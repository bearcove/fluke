use eyre::eyre;
use rfc9113::DEFAULT_FRAME_SIZE;
use std::{collections::VecDeque, rc::Rc, time::Duration};

use enumflags2::{bitflags, BitFlags};
use fluke_buffet::{IntoHalves, Piece, PieceList, Roll, RollMut, WriteOwned};
use fluke_h2_parse::{
    enumflags2,
    nom::{self, Finish},
    ContinuationFlags, DataFlags, ErrorCode, Frame, FrameType, GoAway, HeadersFlags, IntoPiece,
    KnownErrorCode, PingFlags, PrioritySpec, RstStream, SettingPairs, Settings, SettingsFlags,
    StreamId, WindowUpdate, PREFACE,
};
use tokio::time::Instant;
use tracing::{debug, trace};

use crate::rfc9113::default_settings;

pub mod rfc9113;

#[derive(Default)]
pub struct Headers {
    values: VecDeque<(Piece, Piece)>,
}

impl IntoIterator for Headers {
    type Item = (Piece, Piece);
    type IntoIter = std::collections::vec_deque::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl Headers {
    /// Appends a key-value pair to the end of the headers.
    pub fn append(&mut self, key: impl Into<Piece>, value: impl Into<Piece>) {
        self.values.push_back((key.into(), value.into()));
    }

    /// Prepends a key-value pair to the beginning of the headers.
    /// (Useful when inserting pseudo-headers for example)
    pub fn prepend(&mut self, key: impl Into<Piece>, value: impl Into<Piece>) {
        self.values.push_front((key.into(), value.into()));
    }

    /// Replaces all values of the specified key with the provided value.
    pub fn replace(&mut self, key: impl Into<Piece>, value: impl Into<Piece>) {
        let key = key.into();
        let value = value.into();
        self.values.retain(|(k, _)| k != &key);
        self.values.push_back((key, value));
    }

    /// Returns an iterator over the key-value pairs in the headers.
    pub fn iter(&self) -> impl Iterator<Item = &(Piece, Piece)> {
        self.values.iter()
    }

    /// Returns the number of key-value pairs in the headers.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Checks if the headers are empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Gets the first value associated with the specified key.
    /// Returns `None` if the key is not present in the headers.
    pub fn get_first(&self, key: &Piece) -> Option<&Piece> {
        self.values
            .iter()
            .find_map(|(k, v)| if k == key { Some(v) } else { None })
    }

    /// Extends the headers with another set of headers.
    pub fn extend(&mut self, other: Headers) {
        self.values.extend(other);
    }

    /// Remove and return all key-value pairs matching the specified key
    pub fn remove(&mut self, key: &Piece) {
        self.values.retain(|(k, _)| k != key);
    }
}

pub struct Conn<IO: IntoHalves> {
    w: <IO as IntoHalves>::Write,
    scratch: RollMut,
    pub ev_rx: tokio::sync::mpsc::Receiver<Ev>,
    config: Rc<Config>,
    hpack_enc: fluke_hpack::Encoder<'static>,
    hpack_dec: fluke_hpack::Decoder<'static>,
    /// the peer's settings
    pub settings: Settings,
}

pub enum Ev {
    Frame { frame: Frame, payload: Roll },
    IoError { error: std::io::Error },
}

pub enum FrameWaitOutcome {
    Success(Frame, Roll),
    Timeout {
        wanted: BitFlags<FrameT>,
        last_frame: Option<Frame>,
        waited: Duration,
    },
    Eof {
        wanted: BitFlags<FrameT>,
        last_frame: Option<Frame>,
    },
    IoError {
        wanted: BitFlags<FrameT>,
        last_frame: Option<Frame>,
        error: std::io::Error,
    },
}

impl FrameWaitOutcome {
    pub fn unwrap(self) -> (Frame, Roll) {
        match self {
            FrameWaitOutcome::Success(frame, payload) => (frame, payload),
            FrameWaitOutcome::Timeout {
                wanted,
                last_frame,
                waited,
            } => {
                panic!(
                    "Wanted ({wanted:?}), timed out after {waited:?}. Last frame: {last_frame:?}"
                );
            }
            FrameWaitOutcome::Eof { wanted, last_frame } => {
                panic!("Wanted ({wanted:?}), peer hung up. Last frame: {last_frame:?}");
            }
            FrameWaitOutcome::IoError {
                wanted,
                last_frame,
                error,
            } => {
                panic!("Wanted ({wanted:?}), got I/O error {error}. Last frame: {last_frame:?}")
            }
        }
    }
}

/// A "hollow" variant of [FrameType], with no associated data.
/// Useful to expect a certain frame type
#[bitflags]
#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FrameT {
    Data,
    Headers,
    Priority,
    RstStream,
    Settings,
    PushPromise,
    Ping,
    GoAway,
    WindowUpdate,
    Continuation,
    Unknown,
}

impl From<FrameType> for FrameT {
    fn from(value: FrameType) -> Self {
        match value {
            FrameType::Data(_) => Self::Data,
            FrameType::Headers(_) => Self::Headers,
            FrameType::Priority => Self::Priority,
            FrameType::RstStream => Self::RstStream,
            FrameType::Settings(_) => Self::Settings,
            FrameType::PushPromise => Self::PushPromise,
            FrameType::Ping(_) => Self::Ping,
            FrameType::GoAway => Self::GoAway,
            FrameType::WindowUpdate => Self::WindowUpdate,
            FrameType::Continuation(_) => Self::Continuation,
            FrameType::Unknown(_) => Self::Unknown,
        }
    }
}

// A hollow variant of [KnownErrorCode]
#[bitflags]
#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ErrorC {
    NoError,
    ProtocolError,
    InternalError,
    FlowControlError,
    SettingsTimeout,
    StreamClosed,
    FrameSizeError,
    RefusedStream,
    Cancel,
    CompressionError,
    ConnectError,
    EnhanceYourCalm,
    InadequateSecurity,
    Http1_1Required,
}

impl From<ErrorC> for ErrorCode {
    fn from(value: ErrorC) -> Self {
        ErrorCode(value as _)
    }
}

impl From<KnownErrorCode> for ErrorC {
    fn from(value: KnownErrorCode) -> Self {
        match value {
            KnownErrorCode::NoError => Self::NoError,
            KnownErrorCode::ProtocolError => Self::ProtocolError,
            KnownErrorCode::InternalError => Self::InternalError,
            KnownErrorCode::FlowControlError => Self::FlowControlError,
            KnownErrorCode::SettingsTimeout => Self::SettingsTimeout,
            KnownErrorCode::StreamClosed => Self::StreamClosed,
            KnownErrorCode::FrameSizeError => Self::FrameSizeError,
            KnownErrorCode::RefusedStream => Self::RefusedStream,
            KnownErrorCode::Cancel => Self::Cancel,
            KnownErrorCode::CompressionError => Self::CompressionError,
            KnownErrorCode::ConnectError => Self::ConnectError,
            KnownErrorCode::EnhanceYourCalm => Self::EnhanceYourCalm,
            KnownErrorCode::InadequateSecurity => Self::InadequateSecurity,
            KnownErrorCode::Http1_1Required => Self::Http1_1Required,
        }
    }
}

impl<IO: IntoHalves> Conn<IO> {
    pub fn new(config: Rc<Config>, io: IO) -> Self {
        let (mut r, w) = io.into_halves();

        let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<Ev>(1);
        let mut eof = false;

        let recv_fut = {
            let config = config.clone();
            async move {
                let mut res_buf = RollMut::alloc()?;
                'read: loop {
                    trace!("'read loop");

                    match Frame::parse(res_buf.filled()) {
                        Ok((rest, frame)) => {
                            res_buf.keep(rest);
                            debug!("< {frame:?}");

                            // read frame payload
                            let frame_len = frame.len as usize;
                            trace!(?frame_len, "reserving memory");
                            res_buf.reserve_at_least(frame_len)?;

                            let deadline = Instant::now() + config.timeout;
                            trace!(?frame_len, ?deadline, "reading");

                            while res_buf.len() < frame_len {
                                let res;
                                (res, res_buf) = match tokio::time::timeout_at(
                                    deadline,
                                    res_buf.read_into(16384, &mut r),
                                )
                                .await
                                {
                                    Ok(res) => res,
                                    Err(_) => {
                                        debug!(?frame_len, "timed out reading frame payload");
                                        break 'read;
                                    }
                                };
                                let n = res?;
                                trace!(%n, len = %res_buf.len(), "read bytes (reading frame payload)");

                                if n == 0 {
                                    eof = true;
                                    if res_buf.len() < frame_len {
                                        panic!(
                                            "peer frame header, then incomplete payload, then hung up"
                                        )
                                    }
                                }
                            }

                            let payload = if frame_len == 0 {
                                Roll::empty()
                            } else {
                                res_buf.take_at_most(frame_len).unwrap()
                            };
                            assert_eq!(payload.len(), frame_len);

                            trace!(%frame_len, "got frame payload");
                            if ev_tx.send(Ev::Frame { frame, payload }).await.is_err() {
                                // I guess we stopped consuming frames, sure.
                                break 'read;
                            }
                        }
                        Err(nom::Err::Incomplete(_)) => {
                            if eof {
                                if res_buf.is_empty() {
                                    // all good, that's eof!
                                    break 'read;
                                } else {
                                    panic!(
                                        "peer sent incomplete frame header then hung up (buf len: {})",
                                        res_buf.len()
                                    )
                                }
                            }

                            trace!("reserving");
                            res_buf.reserve()?;
                            let res;
                            trace!("re-filling buffer");
                            let deadline = Instant::now() + config.timeout;
                            (res, res_buf) = match tokio::time::timeout_at(
                                deadline,
                                res_buf.read_into(16384, &mut r),
                            )
                            .await
                            {
                                Ok(res) => res,
                                Err(_) => {
                                    debug!("timed out reading frame header");
                                    break 'read;
                                }
                            };
                            let n = res?;
                            if n == 0 {
                                debug!("reached EOF");
                                eof = true;
                            } else {
                                trace!(%n, "read bytes (reading frame header)");
                            }

                            continue;
                        }
                        Err(nom::Err::Failure(err) | nom::Err::Error(err)) => {
                            debug!(?err, "got parse error");
                            break;
                        }
                    }
                }

                Ok::<_, eyre::Report>(())
            }
        };
        fluke_buffet::spawn(async move { recv_fut.await.unwrap() });

        let mut settings: Settings = Default::default();
        for (code, value) in default_settings().0 {
            settings.apply(*code, *value).unwrap();
        }

        Self {
            w,
            scratch: RollMut::alloc().unwrap(),
            ev_rx,
            config,
            hpack_enc: Default::default(),
            hpack_dec: Default::default(),
            settings: Settings {
                initial_window_size: DEFAULT_FRAME_SIZE,
                max_frame_size: DEFAULT_FRAME_SIZE,
                ..Default::default()
            },
        }
    }

    pub async fn write_frame(&mut self, frame: Frame, payload: impl IntoPiece) -> eyre::Result<()> {
        let payload = payload.into_piece(&mut self.scratch)?;
        let frame = frame.with_len(payload.len().try_into().unwrap());

        let header = frame.into_piece(&mut self.scratch)?;
        self.w
            .writev_all_owned(PieceList::single(header).followed_by(payload))
            .await?;
        Ok(())
    }

    pub async fn write_priority(
        &mut self,
        stream_id: StreamId,
        priority_spec: PrioritySpec,
    ) -> eyre::Result<()> {
        self.write_frame(FrameType::Priority.into_frame(stream_id), priority_spec)
            .await
    }

    /// Send a PING frame and wait for the peer to acknowledge it.
    pub async fn verify_connection_still_alive(&mut self) -> eyre::Result<()> {
        let payload = b"pingpong";
        self.write_ping(false, &payload[..]).await?;
        self.verify_ping_frame_with_ack(payload).await?;
        Ok(())
    }

    pub async fn write_ping(&mut self, ack: bool, payload: impl IntoPiece) -> eyre::Result<()> {
        self.write_frame(
            FrameType::Ping(if ack {
                PingFlags::Ack.into()
            } else {
                Default::default()
            })
            .into_frame(StreamId::CONNECTION),
            payload,
        )
        .await
    }

    pub async fn write_and_ack_settings(
        &mut self,
        settings: impl Into<SettingPairs<'_>>,
    ) -> eyre::Result<()> {
        self.write_settings(settings).await?;
        self.verify_settings_frame_with_ack().await?;
        Ok(())
    }

    pub async fn write_settings(
        &mut self,
        settings: impl Into<SettingPairs<'_>>,
    ) -> eyre::Result<()> {
        self.write_frame(
            FrameType::Settings(Default::default()).into_frame(StreamId::CONNECTION),
            settings.into(),
        )
        .await
    }

    /// Waits for a certain kind of frame
    pub async fn wait_for_frame(&mut self, types: impl Into<BitFlags<FrameT>>) -> FrameWaitOutcome {
        let deadline = Instant::now() + self.config.timeout;
        self.wait_for_frame_with_deadline(types, deadline).await
    }

    /// Waits for a certain kind of frame with a specified deadline
    pub async fn wait_for_frame_with_deadline(
        &mut self,
        types: impl Into<BitFlags<FrameT>>,
        deadline: Instant,
    ) -> FrameWaitOutcome {
        let types = types.into();
        let mut last_frame: Option<Frame> = None;

        loop {
            match tokio::time::timeout_at(deadline, self.ev_rx.recv()).await {
                Err(_) => {
                    return FrameWaitOutcome::Timeout {
                        wanted: types,
                        last_frame,
                        waited: self.config.timeout,
                    };
                }
                Ok(maybe_ev) => match maybe_ev {
                    None => {
                        return FrameWaitOutcome::Eof {
                            wanted: types,
                            last_frame,
                        }
                    }
                    Some(ev) => match ev {
                        Ev::Frame { frame, payload } => {
                            if types.contains(FrameT::from(frame.frame_type)) {
                                return FrameWaitOutcome::Success(frame, payload);
                            } else {
                                last_frame = Some(frame)
                            }
                        }
                        Ev::IoError { error } => {
                            return FrameWaitOutcome::IoError {
                                wanted: types,
                                last_frame,
                                error,
                            }
                        }
                    },
                },
            }
        }
    }

    /// Waits for a PING frame with Ack flag and the specified payload.
    /// It will NOT ignore other PING frames, if the first frame it
    /// receives doesn't have the expected payload, it will return an error.
    pub async fn verify_ping_frame_with_ack(&mut self, payload: &[u8]) -> eyre::Result<()> {
        let (frame, received_payload) = self.wait_for_frame(FrameT::Ping).await.unwrap();
        assert!(frame.is_ack(), "expected PING frame to have ACK flag");

        assert_eq!(received_payload, payload, "unexpected PING payload");

        Ok(())
    }

    pub async fn handshake(&mut self) -> eyre::Result<()> {
        // perform an HTTP/2 handshake as a client
        self.w.write_all_owned(PREFACE).await?;

        self.write_settings(default_settings()).await?;

        let (frame, payload) = self.wait_for_frame(FrameT::Settings).await.unwrap();
        assert!(
            !frame.is_ack(),
            "server should send their settings first thing (no ack)"
        );

        Settings::parse(&payload[..], |k, v| self.settings.apply(k, v))?;

        self.write_frame(
            Frame::new(
                FrameType::Settings(SettingsFlags::Ack.into()),
                StreamId::CONNECTION,
            ),
            (),
        )
        .await?;

        // and wait until the server acknowledges our settings
        let (frame, _payload) = self.wait_for_frame(FrameT::Settings).await.unwrap();
        assert!(frame.is_ack(), "server should acknowledge our settings");

        Ok(())
    }

    pub async fn send(&mut self, buf: impl Into<Piece>) -> eyre::Result<()> {
        self.w.write_all_owned(buf.into()).await?;
        Ok(())
    }

    async fn verify_connection_error(
        &mut self,
        codes: impl Into<BitFlags<ErrorC>>,
    ) -> eyre::Result<()> {
        let codes = codes.into();

        match self.wait_for_frame(FrameT::GoAway).await {
            FrameWaitOutcome::Success(_frame, payload) => {
                let (_, goaway) = GoAway::parse(payload).finish().unwrap();
                let error_c: ErrorC = KnownErrorCode::try_from(goaway.error_code)
                    .map_err(|_| eyre::eyre!(
                        "Expected GOAWAY with one of {codes:?}, but got unknown error code {} (0x{:x})",
                        goaway.error_code.as_repr(), goaway.error_code.as_repr()
                    ))?
                    .into();

                if codes.contains(error_c) {
                    // that's what we expected!
                    return Ok(());
                }
                Err(eyre::eyre!(
                    "Expected GOAWAY with one of {codes:?}, but got {error_c:?}"
                ))
            }
            FrameWaitOutcome::Timeout { last_frame, .. } => Err(eyre!(
                "Timed out while waiting for connection error, last frame: ({last_frame:?})"
            )),
            FrameWaitOutcome::Eof { .. } => {
                // that's fine
                Ok(())
            }
            FrameWaitOutcome::IoError { .. } => {
                // TODO: that's fine if it's a connection reset, we should probably check
                Ok(())
            }
        }
    }

    pub async fn verify_connection_close(&mut self) -> eyre::Result<()> {
        match self.wait_for_frame(FrameT::GoAway).await {
            FrameWaitOutcome::Success(_frame, _payload) => {
                // that's what we expected!
                Ok(())
            }
            FrameWaitOutcome::Timeout { last_frame, .. } => Err(eyre!(
                "Timed out while waiting for connection close, last frame: ({last_frame:?})"
            )),
            FrameWaitOutcome::Eof { .. } => {
                // that's fine
                Ok(())
            }
            FrameWaitOutcome::IoError { .. } => {
                // TODO: that's fine if it's a connection reset, we should probably check
                Ok(())
            }
        }
    }

    pub async fn verify_stream_close(&mut self, stream_id: StreamId) -> eyre::Result<()> {
        let mut global_last_frame: Option<Frame> = None;
        let deadline = Instant::now() + self.config.timeout;

        loop {
            match self
                .wait_for_frame_with_deadline(
                    FrameT::Data | FrameT::Headers | FrameT::RstStream,
                    deadline,
                )
                .await
            {
                FrameWaitOutcome::Success(frame, payload) => match frame.frame_type {
                    FrameType::Data(flags) => {
                        if flags.contains(DataFlags::EndStream) {
                            assert_eq!(frame.stream_id, stream_id, "unexpected stream ID");
                            return Ok(());
                        } else {
                            global_last_frame = Some(frame);
                        }
                    }
                    FrameType::Headers(flags) => {
                        if flags.contains(HeadersFlags::EndStream) {
                            assert_eq!(frame.stream_id, stream_id, "unexpected stream ID");
                            return Ok(());
                        } else {
                            global_last_frame = Some(frame);
                        }
                    }
                    FrameType::RstStream => {
                        let (_rest, rst_stream) = RstStream::parse(payload).finish().unwrap();
                        let error_code =
                            KnownErrorCode::try_from(rst_stream.error_code).map_err(|_| {
                                eyre::eyre!("expected NO_ERROR code, but got unknown error code")
                            })?;
                        assert_eq!(
                            error_code,
                            KnownErrorCode::NoError,
                            "expected RST_STREAM frame with NO_ERROR code"
                        );
                        assert_eq!(frame.stream_id, stream_id, "unexpected stream ID");
                        return Ok(());
                    }
                    _ => panic!("unexpected frame type"),
                },
                FrameWaitOutcome::Timeout { last_frame, .. } => {
                    return Err(eyre!(
                        "Timed out while waiting for stream close frame, last frame: ({:?})",
                        last_frame.or(global_last_frame)
                    ));
                }
                FrameWaitOutcome::Eof { .. } => {
                    // that's fine
                    return Ok(());
                }
                FrameWaitOutcome::IoError { .. } => {
                    // TODO: that's fine if it's a connection reset, we should probably check
                    return Ok(());
                }
            }
        }
    }

    /// verify_headers_frame verifies whether a HEADERS frame with specified
    /// stream ID was received.
    pub async fn verify_headers_frame(&mut self, stream_id: StreamId) -> eyre::Result<()> {
        let (frame, _payload) = self.wait_for_frame(FrameT::Headers).await.unwrap();
        assert_eq!(frame.stream_id, stream_id, "unexpected stream ID");
        Ok(())
    }

    pub async fn verify_stream_error(
        &mut self,
        codes: impl Into<BitFlags<ErrorC>>,
    ) -> eyre::Result<()> {
        let codes = codes.into();

        match self
            .wait_for_frame(FrameT::GoAway | FrameT::RstStream)
            .await
        {
            FrameWaitOutcome::Success(frame, payload) => {
                match frame.frame_type {
                    FrameType::GoAway => {
                        let (_, goaway) = GoAway::parse(payload).finish().unwrap();
                        let error_c: ErrorC = KnownErrorCode::try_from(goaway.error_code)
                            .map_err(|_| eyre::eyre!(
                                "Expected GOAWAY or RSTSTREAM with one of {codes:?}, but got unknown error code {} (0x{:x})",
                                goaway.error_code.as_repr(), goaway.error_code.as_repr()
                            ))?
                            .into();

                        if codes.contains(error_c) {
                            // that's what we expected!
                            Ok(())
                        } else {
                            Err(eyre::eyre!(
                                "Expected GOAWAY or RSTSTREAM with one of {codes:?}, but got {error_c:?}"
                            ))
                        }
                    }
                    FrameType::RstStream => {
                        let (_, rststream) = RstStream::parse(payload).finish().unwrap();
                        let error_code = KnownErrorCode::try_from(rststream.error_code)
                            .map_err(|_| eyre::eyre!(
                                "Expected GOAWAY or RSTSTREAM with one of {codes:?}, but got unknown error code {} (0x{:x})",
                                rststream.error_code.as_repr(), rststream.error_code.as_repr()
                            ))?;

                        let error_c: ErrorC = error_code.into();

                        if codes.contains(error_c) {
                            // that's what we expected!
                            Ok(())
                        } else {
                            Err(eyre::eyre!(
                                "Expected GOAWAY or RSTSTREAM with one of {codes:?}, but got {error_c:?}"
                            ))
                        }
                    }
                    _ => unreachable!(),
                }
            }
            FrameWaitOutcome::Timeout { last_frame, .. } => Err(eyre!(
                "Timed out while waiting for stream error, last frame: ({last_frame:?})"
            )),
            FrameWaitOutcome::Eof { .. } => {
                // that's fine
                Ok(())
            }
            FrameWaitOutcome::IoError { .. } => {
                // TODO: that's fine if it's a connection reset, we should probably check
                Ok(())
            }
        }
    }

    fn common_headers(&self, method: &'static str) -> Headers {
        let (scheme, default_port) = if self.config.tls {
            ("https", self.config.port == 443)
        } else {
            ("http", self.config.port == 80)
        };

        let authority = if default_port {
            self.config.host.clone()
        } else {
            format!("{}:{}", self.config.host, self.config.port)
        };

        let mut headers = Headers::default();
        headers.append(":method", method);
        headers.append(":scheme", scheme);
        headers.append(":path", self.config.path.clone().into_bytes());
        headers.append(":authority", authority.into_bytes());
        headers
    }

    pub fn encode_headers(&mut self, headers: &Headers) -> eyre::Result<Piece> {
        // wasteful, but we're doing tests so shrug.
        let mut fragment = Vec::new();
        for (k, v) in headers.iter() {
            self.hpack_enc
                .encode_header_into((k.as_ref(), v.as_ref()), &mut fragment)?;
        }
        Ok(fragment.into())
    }

    /// Note: The buffer should represent the entire block that should be
    /// decoded. For example, in HTTP/2, all continuation frames need to be
    /// concatenated to a single buffer before passing them to the decoder.
    pub fn decode_headers(&mut self, fragment: Piece) -> eyre::Result<Headers> {
        // note: this allocates a lot but, but again, we're doing tests so shrug.
        let mut headers = Headers::default();
        let res = self
            .hpack_dec
            .decode(&fragment[..])
            .map_err(|e| eyre::eyre!("hpack decoder error: {e:?}"))?;
        for (k, v) in res {
            headers.append(k, v);
        }

        tracing::trace!("decoded {} headers:", headers.len());
        for (k, v) in headers.iter() {
            tracing::trace!(
                "    {:?} => {:?}",
                String::from_utf8_lossy(&k[..]),
                String::from_utf8_lossy(&v[..])
            );
        }

        Ok(headers)
    }

    pub async fn send_empty_post_to_root(&mut self, stream_id: StreamId) -> eyre::Result<()> {
        self.encode_and_write_headers(
            stream_id,
            HeadersFlags::EndStream | HeadersFlags::EndHeaders,
            &self.common_headers("POST"),
        )
        .await
    }

    pub async fn encode_and_write_headers(
        &mut self,
        stream_id: StreamId,
        flags: impl Into<BitFlags<HeadersFlags>>,
        headers: &Headers,
    ) -> eyre::Result<()> {
        let flags = flags.into();
        let block_fragment = self.encode_headers(headers)?;
        self.write_headers(stream_id, flags, block_fragment).await?;
        Ok(())
    }

    pub async fn write_headers(
        &mut self,
        stream_id: StreamId,
        flags: impl Into<BitFlags<HeadersFlags>>,
        block_fragment: Piece,
    ) -> eyre::Result<()> {
        let flags = flags.into();
        let frame = Frame::new(FrameType::Headers(flags), stream_id);
        self.write_frame(frame, block_fragment).await?;
        Ok(())
    }

    pub async fn write_headers_with_priority(
        &mut self,
        stream_id: StreamId,
        flags: impl Into<BitFlags<HeadersFlags>>,
        priority_spec: PrioritySpec,
        block_fragment: Piece,
    ) -> eyre::Result<()> {
        let flags = flags.into() | HeadersFlags::Priority;
        let frame = Frame::new(FrameType::Headers(flags), stream_id);

        let payload = block_fragment.into_piece(&mut self.scratch)?;
        let frame = frame.with_len(payload.len().try_into().unwrap());

        let priority_spec_piece = priority_spec.into_piece(&mut self.scratch)?;

        let header = frame.into_piece(&mut self.scratch)?;
        self.w
            .writev_all_owned(
                PieceList::single(header)
                    .followed_by(priority_spec_piece)
                    .followed_by(payload),
            )
            .await?;

        Ok(())
    }

    pub async fn write_continuation(
        &mut self,
        stream_id: StreamId,
        flags: impl Into<BitFlags<ContinuationFlags>>,
        block_fragment: Piece,
    ) -> eyre::Result<()> {
        let flags = flags.into();
        let frame = Frame::new(FrameType::Continuation(flags), stream_id);
        self.write_frame(frame, block_fragment).await?;
        Ok(())
    }

    pub async fn write_data(
        &mut self,
        stream_id: StreamId,
        end_stream: bool,
        data: impl Into<Piece>,
    ) -> eyre::Result<()> {
        let frame = Frame::new(
            FrameType::Data(if end_stream {
                DataFlags::EndStream.into()
            } else {
                Default::default()
            }),
            stream_id,
        );
        self.write_frame(frame, data.into()).await?;
        Ok(())
    }

    /// Generates a set of dummy headers.
    ///
    /// # Parameters
    /// - `len`: The number of headers to generate.
    ///
    /// # Returns
    /// - A `Headers` map containing `len` number of headers, where each header
    ///   has a key in the form of `x-dummy{i}` and a value of `dummy_bytes`
    ///   with a length equal to `self.config.max_header_len`.
    ///
    /// # Properties
    /// - The size of each header value is equal to
    ///   `self.config.max_header_len`.
    /// - The total number of headers in the returned `Headers` map is equal to
    ///   `len`.
    fn dummy_headers(&self, len: usize) -> Headers {
        let mut headers = Headers::default();
        let dummy = dummy_bytes(self.config.max_header_len);

        for i in 0..len {
            let name = format!("x-dummy{}", i);
            headers.append(name.into_bytes(), dummy.clone());
        }

        headers
    }

    pub async fn write_rst_stream(
        &mut self,
        stream_id: StreamId,
        error_code: impl Into<ErrorCode>,
    ) -> eyre::Result<()> {
        let error_code = error_code.into();
        let rst_stream = RstStream { error_code };
        self.write_frame(FrameType::RstStream.into_frame(stream_id), rst_stream)
            .await
    }

    async fn write_window_update(
        &mut self,
        stream_id: StreamId,
        increment: u32,
    ) -> eyre::Result<()> {
        let update = WindowUpdate {
            reserved: 0,
            increment,
        };
        let mut rm = RollMut::alloc().unwrap();
        let piece = update.into_piece(&mut rm).unwrap();
        tracing::debug!(?update, "writing window_update, bytes = {:x?}", &piece[..]);

        self.write_frame(FrameType::WindowUpdate.into_frame(stream_id), update)
            .await
    }

    // verify_settings_frame_with_ack verifies whether a SETTINGS frame with
    // ACK flag was received.
    async fn verify_settings_frame_with_ack(&mut self) -> eyre::Result<()> {
        let (frame, _payload) = self.wait_for_frame(FrameT::Settings).await.unwrap();
        assert!(frame.is_ack());
        Ok(())
    }

    async fn send_req_and_expect_status(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
        expected_status: u16,
    ) -> eyre::Result<()> {
        self.encode_and_write_headers(
            stream_id,
            HeadersFlags::EndHeaders | HeadersFlags::EndStream,
            headers,
        )
        .await?;

        let (frame, payload) = self.wait_for_frame(FrameT::Headers).await.unwrap();
        assert!(
            frame.is_end_headers(),
            "the server is free to answer with headers in several frames but this breaks that test"
        );

        let headers = self.decode_headers(payload.into())?;
        let status = headers
            .get_first(&":status".into())
            .expect("response should contain :status");
        let status = std::str::from_utf8(&status[..])
            .expect("status should be valid utf-8")
            .parse::<u16>()
            .expect("status should be a u16");
        assert_eq!(status, expected_status);

        Ok(())
    }

    async fn send_req_and_expect_stream_rst(
        &mut self,
        stream_id: StreamId,
        headers: &Headers,
    ) -> eyre::Result<()> {
        self.encode_and_write_headers(
            stream_id,
            HeadersFlags::EndHeaders | HeadersFlags::EndStream,
            headers,
        )
        .await?;

        self.verify_stream_error(ErrorC::ProtocolError).await?;

        Ok(())
    }
}

/// Parameters for tests
pub struct Config {
    /// which host to connect to
    pub host: String,

    /// which port to connect to
    pub port: u16,

    /// which path to request
    pub path: String,

    /// whether to use TLS
    pub tls: bool,

    /// how long to wait for a frame
    pub timeout: Duration,

    /// maximum length of a header
    pub max_header_len: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 80,
            path: "/".into(),
            tls: false,

            max_header_len: 4000,

            timeout: Duration::from_millis(100),
        }
    }
}

// DummyString returns a dummy string with specified length.
pub fn dummy_string(len: usize) -> String {
    "x".repeat(len)
}

// DummyBytes returns an array of bytes with specified length.
pub fn dummy_bytes(len: usize) -> Vec<u8> {
    vec![b'x'; len]
}

use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashSet},
    io::Write,
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
};

use byteorder::{BigEndian, WriteBytesExt};
use eyre::Context;
use fluke_buffet::{Piece, PieceList, PieceStr, ReadOwned, Roll, RollMut, WriteOwned};
use fluke_h2_parse::{
    self as parse, enumflags2::BitFlags, nom::Finish, ContinuationFlags, DataFlags, Frame,
    FrameType, HeadersFlags, PingFlags, PrioritySpec, Setting, SettingPairs, Settings,
    SettingsFlags, StreamId, WindowUpdate,
};
use http::{
    header,
    uri::{Authority, PathAndQuery, Scheme},
    HeaderName, StatusCode, Version,
};
use parse::IntoPiece;
use smallvec::{smallvec, SmallVec};
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::{
    h2::{
        body::{H2Body, IncomingMessagesResult, StreamIncoming},
        encode::H2Encoder,
        types::{
            BodyOutgoing, ConnState, H2ConnectionError, H2Event, H2EventPayload, H2RequestError,
            H2StreamError, HeadersOrTrailers, HeadersOutgoing, StreamOutgoing, StreamState,
        },
    },
    util::read_and_parse,
    Headers, Method, Request, Responder, ServerDriver,
};

use super::{
    body::{ChunkPosition, SinglePieceBody},
    types::H2ErrorLevel,
};

pub const MAX_WINDOW_SIZE: i64 = u32::MAX as i64;

/// HTTP/2 server configuration
pub struct ServerConf {
    pub max_streams: Option<u32>,
}

impl Default for ServerConf {
    fn default() -> Self {
        Self {
            max_streams: Some(32),
        }
    }
}

pub async fn serve(
    (transport_r, transport_w): (impl ReadOwned, impl WriteOwned),
    conf: Rc<ServerConf>,
    client_buf: RollMut,
    driver: Rc<impl ServerDriver + 'static>,
) -> eyre::Result<()> {
    let mut state = ConnState::default();
    state.self_settings.max_concurrent_streams = conf.max_streams;

    let mut cx = ServerContext::new(driver.clone(), state, transport_w)?;
    cx.work(client_buf, transport_r).await?;

    debug!("finished serving");
    Ok(())
}

/// Reads and processes h2 frames from the client.
pub(crate) struct ServerContext<D: ServerDriver + 'static, W: WriteOwned> {
    driver: Rc<D>,
    state: ConnState,

    hpack_dec: fluke_hpack::Decoder<'static>,
    hpack_enc: fluke_hpack::Encoder<'static>,
    out_scratch: RollMut,

    /// Whether we've received a GOAWAY frame.
    pub goaway_recv: bool,

    /// TODO: encapsulate into a framer, don't
    /// allow direct access from context methods
    transport_w: W,

    ev_tx: mpsc::Sender<H2Event>,
    ev_rx: mpsc::Receiver<H2Event>,
}

impl<D: ServerDriver + 'static, W: WriteOwned> ServerContext<D, W> {
    pub(crate) fn new(driver: Rc<D>, state: ConnState, transport_w: W) -> eyre::Result<Self> {
        let mut hpack_dec = fluke_hpack::Decoder::new();
        hpack_dec
            .set_max_allowed_table_size(Settings::default().header_table_size.try_into().unwrap());

        let hpack_enc = fluke_hpack::Encoder::new();

        let (ev_tx, ev_rx) = tokio::sync::mpsc::channel::<H2Event>(32);

        Ok(Self {
            driver,
            ev_tx,
            ev_rx,
            state,
            hpack_dec,
            hpack_enc,
            out_scratch: RollMut::alloc()?,
            goaway_recv: false,
            transport_w,
        })
    }

    /// Reads and process h2 frames from the client.
    pub(crate) async fn work(
        &mut self,
        mut client_buf: RollMut,
        mut transport_r: impl ReadOwned,
    ) -> eyre::Result<()> {
        // first read the preface
        {
            (client_buf, _) = match read_and_parse(
                parse::preface,
                &mut transport_r,
                client_buf,
                parse::PREFACE.len(),
            )
            .await?
            {
                Some((client_buf, frame)) => (client_buf, frame),
                None => {
                    debug!("h2 client closed connection before sending preface");
                    return Ok(());
                }
            };
        }

        // then send our initial settings
        {
            debug!("Sending initial settings");
            let setting_payload = {
                let s = &self.state.self_settings;
                SettingPairs(&[
                    (Setting::EnablePush, 0),
                    (Setting::HeaderTableSize, s.header_table_size),
                    (Setting::InitialWindowSize, s.initial_window_size),
                    (
                        Setting::MaxConcurrentStreams,
                        s.max_concurrent_streams.unwrap_or(u32::MAX),
                    ),
                    (Setting::MaxFrameSize, s.max_frame_size),
                    (Setting::MaxHeaderListSize, s.max_header_list_size),
                ])
                .into_piece(&mut self.out_scratch)?
            };
            let frame = Frame::new(
                FrameType::Settings(Default::default()),
                StreamId::CONNECTION,
            );
            self.write_frame(frame, PieceList::single(setting_payload))
                .await?;
        }

        let mut goaway_err: Option<H2ConnectionError> = None;

        {
            // read frames and send them into an mpsc buffer of size 1
            let (tx, rx) = mpsc::channel::<(Frame, Roll)>(32);

            // store max frame size setting as an atomic so we can share it across tasks
            // FIXME: the process_task should update this
            let max_frame_size = Rc::new(AtomicU32::new(self.state.self_settings.max_frame_size));

            let mut deframe_task = std::pin::pin!(Self::deframe_loop(
                client_buf,
                transport_r,
                tx,
                max_frame_size
            ));
            let mut process_task = std::pin::pin!(self.process_loop(rx));

            debug!("Starting both deframe & process tasks");

            tokio::select! {
                res = &mut deframe_task => {
                    debug!(?res, "h2 deframe task finished");

                    if let Err(e) = res {
                        match e {
                            H2ConnectionError::ReadError(e) => {
                                let mut should_ignore_err = false;

                                // if this is a connection reset and we've sent a goaway, ignore it
                                if let Some(io_error) = e.root_cause().downcast_ref::<std::io::Error>() {
                                    if io_error.kind() == std::io::ErrorKind::ConnectionReset {
                                        should_ignore_err = true;
                                    }
                                }

                                debug!(%should_ignore_err, "deciding whether or not to propagate deframer error");
                                if !should_ignore_err {
                                    return Err(e.wrap_err("h2 io"));
                                }
                            },
                            e => {
                                debug!("turning error into GOAWAY");
                                goaway_err = Some(e)
                            }
                        }
                    }

                    if let Err(e) = (&mut process_task).await {
                        // what about the GOAWAY?

                        debug!("h2 process task finished with error: {e}");
                        return Err(e).wrap_err("h2 process");
                    }
                }
                res = &mut process_task => {
                    debug!(?res, "h2 process task finished");

                    if let Err(err) = res {
                        goaway_err = Some(err);
                    }
                }
            }
        }

        if let Some(err) = goaway_err {
            let error_code = err.as_known_error_code();
            debug!("Connection error: {err} ({err:?}) (code {error_code:?})");

            // TODO: don't heap-allocate here
            let additional_debug_data = format!("{err}").into_bytes();

            // TODO: figure out graceful shutdown: this would involve sending a goaway
            // before this point, and processing all the connections we've accepted
            debug!(last_stream_id = %self.state.last_stream_id, ?error_code, "Sending GoAway");
            let payload =
                self.out_scratch
                    .put_to_roll(8 + additional_debug_data.len(), |mut slice| {
                        slice.write_u32::<BigEndian>(self.state.last_stream_id.0)?;
                        slice.write_u32::<BigEndian>(error_code.repr())?;
                        slice.write_all(additional_debug_data.as_slice())?;

                        Ok(())
                    })?;

            let frame = Frame::new(FrameType::GoAway, StreamId::CONNECTION);
            self.write_frame(frame, PieceList::single(payload)).await?;
        }

        Ok(())
    }

    async fn deframe_loop(
        mut client_buf: RollMut,
        mut transport_r: impl ReadOwned,
        tx: mpsc::Sender<(Frame, Roll)>,
        max_frame_size: Rc<AtomicU32>,
    ) -> Result<(), H2ConnectionError> {
        'read_frames: loop {
            const MAX_FRAME_HEADER_SIZE: usize = 128;
            let frame;
            trace!("Reading frame... Buffer length: {}", client_buf.len());
            let frame_res = read_and_parse(
                Frame::parse,
                &mut transport_r,
                client_buf,
                MAX_FRAME_HEADER_SIZE,
            )
            .await;

            let maybe_frame = match frame_res {
                Ok(inner) => inner,
                Err(e) => return Err(H2ConnectionError::ReadError(e)),
            };
            (client_buf, frame) = match maybe_frame {
                Some((client_buf, frame)) => (client_buf, frame),
                None => {
                    debug!("Peer hung up");
                    break 'read_frames;
                }
            };
            trace!(
                "Reading frame... done! New buffer length: {}",
                client_buf.len()
            );
            debug!(?frame, "<");

            let max_frame_size = max_frame_size.load(Ordering::Relaxed);
            if frame.len > max_frame_size {
                return Err(H2ConnectionError::FrameTooLarge {
                    frame_type: frame.frame_type,
                    frame_size: frame.len,
                    max_frame_size,
                });
            }

            trace!(
                "Reading payload of size {}... Buffer length: {}",
                frame.len,
                client_buf.len()
            );
            let mut payload;
            (client_buf, payload) = match read_and_parse(
                nom::bytes::streaming::take(frame.len as usize),
                &mut transport_r,
                client_buf,
                frame.len as usize,
            )
            .await?
            {
                Some((client_buf, payload)) => (client_buf, payload),
                None => {
                    return Err(H2ConnectionError::IncompleteFrame {
                        frame_type: frame.frame_type,
                        frame_size: frame.len,
                    })
                }
            };
            trace!(
                "Reading payload... done! New buffer length: {}",
                client_buf.len()
            );

            let has_padding = match frame.frame_type {
                FrameType::Data(flags) => flags.contains(DataFlags::Padded),
                FrameType::Headers(flags) => flags.contains(HeadersFlags::Padded),
                _ => false,
            };

            if has_padding {
                if payload.is_empty() {
                    return Err(H2ConnectionError::PaddedFrameEmpty {
                        frame_type: frame.frame_type,
                    });
                }

                let padding_length_roll;
                (padding_length_roll, payload) = payload.split_at(1);
                let padding_length = padding_length_roll[0] as usize;
                if payload.len() < padding_length {
                    return Err(H2ConnectionError::PaddedFrameTooShort {
                        frame_type: frame.frame_type,
                        padding_length,
                        frame_size: frame.len,
                    });
                }

                // padding is on the end of the payload
                let at = payload.len() - padding_length;
                (payload, _) = payload.split_at(at);
            }

            if tx.send((frame, payload)).await.is_err() {
                debug!("h2 deframer: receiver dropped, closing connection");
                return Ok(());
            }
        }

        Ok(())
    }

    async fn process_loop(
        &mut self,
        mut rx: mpsc::Receiver<(Frame, Roll)>,
    ) -> Result<(), H2ConnectionError> {
        loop {
            tokio::select! {
                biased;

                maybe_frame = rx.recv() => {
                    if let Some((frame, payload)) = maybe_frame {
                        self.process_frame(frame, payload, &mut rx).await?;
                    } else {
                        debug!("h2 process task: peer hung up");
                        break;
                    }
                }

                ev = self.ev_rx.recv() => {
                    match ev {
                        Some(ev) => self.handle_event(ev).await?,
                        None => unreachable!("the context owns a copy of the sender, and this method has &mut self, so the sender can't be dropped while this method is running"),
                    }
                }

                _ = self.state.send_data_maybe.notified() => {
                    self.send_data_maybe().await?;
                }
            }
        }

        Ok(())
    }

    async fn send_data_maybe(&mut self) -> Result<(), H2ConnectionError> {
        let mut not_pending: HashSet<StreamId> = Default::default();

        // this vec exists for borrow-checker reasons: we can't
        // borrow self mutably twice in 'each_stream
        // TODO: merge those frames! do a single writev_all call!
        let mut frames: Vec<(Frame, PieceList)> = vec![];

        let max_fram = self.state.peer_settings.max_frame_size as usize;

        let streams_with_pending_data: HashSet<_> = self
            .state
            .streams_with_pending_data
            .iter()
            .copied()
            .collect();

        'each_stream: for id in streams_with_pending_data {
            if self.state.outgoing_capacity <= 0 {
                // that's all we can do
                break 'each_stream;
            }

            let outgoing = self
                .state
                .streams
                .get_mut(&id)
                .and_then(|ss| ss.outgoing_mut())
                .expect("stream should not be in streams_with_pending_data if it's already closed / not in an outgoing state");

            debug!(conn_cap = %self.state.outgoing_capacity, strm_cap = %outgoing.capacity, %max_fram, "ready to write");

            if outgoing.headers.has_more_to_write() {
                debug!("writing headers...");

                if matches!(&outgoing.headers, HeadersOutgoing::WaitingForHeaders) {
                    debug!("waiting for headers...");

                    // shouldn't be pending then should it?
                    not_pending.insert(id);
                    continue 'each_stream;
                }

                'queue_header_frames: loop {
                    debug!("writing headers...");

                    let is_continuation =
                        matches!(&outgoing.headers, HeadersOutgoing::WroteSome(_));
                    let piece = outgoing.headers.take_piece();
                    let piece_len = piece.len();

                    if piece_len > max_fram {
                        let write_size = max_fram;
                        let (written, requeued) = piece.split_at(write_size);
                        debug!(%write_size, requeued_len = %requeued.len(), "splitting headers");
                        let frame_type = if is_continuation {
                            FrameType::Continuation(Default::default())
                        } else {
                            FrameType::Headers(Default::default())
                        };
                        outgoing.headers = HeadersOutgoing::WroteSome(requeued);

                        let frame = Frame::new(frame_type, id);
                        frames.push((frame, PieceList::single(written)));
                    } else {
                        let frame_type = if is_continuation {
                            FrameType::Continuation(
                                BitFlags::<ContinuationFlags>::default()
                                    | ContinuationFlags::EndHeaders,
                            )
                        } else {
                            FrameType::Headers(
                                BitFlags::<HeadersFlags>::default() | HeadersFlags::EndHeaders,
                            )
                        };

                        let frame = Frame::new(frame_type, id);
                        frames.push((frame, PieceList::single(piece)));

                        break 'queue_header_frames;
                    }
                }
            }

            let capacity = self.state.outgoing_capacity.min(outgoing.capacity) as usize;
            // bytes written this turn, possibly over multiple frames
            let mut total_bytes_written = 0;

            if outgoing.body.has_more_to_write() {
                'queue_body_frames: while total_bytes_written < capacity {
                    // send as much body data as we can, respecting max frame size and
                    // connection / stream capacity
                    let mut plist = PieceList::default();
                    let mut frame_len = 0;

                    'build_frame: loop {
                        let piece = match outgoing.body.pop_front() {
                            None => break 'build_frame,
                            Some(piece) => piece,
                        };

                        // do we need to split the piece because we don't have
                        // enough capacity left / we hit the max frame size?
                        let piece_len = piece.len();
                        debug!(%piece_len, "popped a piece");

                        let fram_size_if_full_piece = frame_len + piece_len;

                        let cap_left = capacity - total_bytes_written;
                        let max_this_fram = max_fram.min(cap_left);

                        if fram_size_if_full_piece > max_this_fram {
                            // we can't fit this piece in the current frame, so
                            // we have to split it
                            let write_size = max_this_fram - frame_len;
                            let (written, requeued) = piece.split_at(write_size);
                            frame_len += write_size;
                            debug!(written_len = %written.len(), requeued_len = %requeued.len(), "splitting piece");

                            plist.push_back(written);
                            outgoing.body.push_front(requeued);

                            break 'build_frame;
                        } else {
                            // we can write the full piece
                            let write_size = piece_len;
                            frame_len += write_size;

                            plist.push_back(piece);
                        }
                    }

                    let mut flags: BitFlags<DataFlags> = Default::default();
                    if outgoing.body.might_receive_more() {
                        if frame_len == 0 {
                            // the only time we want to send a zero-length frame
                            // is if we have to send END_STREAM separately from
                            // the last chunk.
                            break 'queue_body_frames;
                        }
                    } else {
                        flags |= DataFlags::EndStream;
                    }

                    let frame = Frame::new(FrameType::Data(flags), id);
                    debug!(?frame, %frame_len, "queuing");
                    frames.push((frame, plist));
                    total_bytes_written += frame_len;

                    if flags.contains(DataFlags::EndStream) {
                        break 'queue_body_frames;
                    }
                }
            }
        }

        for (frame, plist) in frames {
            debug!(?frame, plist_len = %plist.len(), "writing");
            self.write_frame(frame, plist).await?;
        }

        for id in not_pending {
            self.state.streams_with_pending_data.remove(&id);
        }

        Ok(())
    }

    async fn handle_event(&mut self, ev: H2Event) -> Result<(), H2ConnectionError> {
        trace!(?ev, "handling event");

        match ev.payload {
            H2EventPayload::Headers(res) => {
                let outgoing = match self
                    .state
                    .streams
                    .get_mut(&ev.stream_id)
                    .and_then(|s| s.outgoing_mut())
                {
                    None => {
                        // ignore the event then, but at this point we should
                        // tell the sender to stop sending chunks, which is not
                        // possible if they all share the same ev_tx
                        // TODO: make it possible to propagate errors to the sender
                        tracing::warn!(stream_id = %ev.stream_id, "ignoring event for stream, since we have no state for it");

                        return Ok(());
                    }
                    Some(outgoing) => outgoing,
                };

                if !matches!(&outgoing.body, BodyOutgoing::StillReceiving(_)) {
                    unreachable!("got headers too late")
                }

                // TODO: don't allocate so much for headers. all `encode_into`
                // wants is an `IntoIter`, we can definitely have a custom iterator
                // that operates on all this instead of using a `Vec`.

                // TODO: enforce max header size
                let mut headers: Vec<(&[u8], &[u8])> = vec![];
                // TODO: prevent overwriting pseudo-headers, especially :status?
                headers.push((b":status", res.status.as_str().as_bytes()));

                for (name, value) in res.headers.iter() {
                    if name == http::header::TRANSFER_ENCODING {
                        // do not set transfer-encoding: chunked when doing HTTP/2
                        continue;
                    }
                    headers.push((name.as_str().as_bytes(), value));
                }

                assert_eq!(self.out_scratch.len(), 0);
                self.hpack_enc
                    .encode_into(headers, &mut self.out_scratch)
                    .map_err(H2ConnectionError::WriteError)?;
                let payload = self.out_scratch.take_all();

                outgoing.headers = HeadersOutgoing::WroteNone(payload.into());
                self.state.streams_with_pending_data.insert(ev.stream_id);
                if self.state.outgoing_capacity > 0 && outgoing.capacity > 0 {
                    // worth revisiting then!
                    self.state.send_data_maybe.notify_one();
                }
            }
            H2EventPayload::BodyChunk(chunk) => {
                let outgoing = match self
                    .state
                    .streams
                    .get_mut(&ev.stream_id)
                    .and_then(|s| s.outgoing_mut())
                {
                    None => {
                        // ignore the event then, but at this point we should
                        // tell the sender to stop sending chunks, which is not
                        // possible if they all share the same ev_tx
                        // TODO: make it possible to propagate errors to the sender
                        return Ok(());
                    }
                    Some(outgoing) => outgoing,
                };

                // FIXME: this isn't great, because, due to biased polling, body pieces can pile
                // up. when we've collected enough pieces for max frame size, we
                // should really send them.
                outgoing.body.push_back(chunk);

                self.state.streams_with_pending_data.insert(ev.stream_id);
                if self.state.outgoing_capacity > 0 && outgoing.capacity > 0 {
                    // worth revisiting then!
                    self.state.send_data_maybe.notify_one();
                }
            }
            H2EventPayload::BodyEnd => {
                let outgoing = match self
                    .state
                    .streams
                    .get_mut(&ev.stream_id)
                    .and_then(|s| s.outgoing_mut())
                {
                    None => return Ok(()),
                    Some(outgoing) => outgoing,
                };

                match &mut outgoing.body {
                    BodyOutgoing::StillReceiving(pieces) => {
                        let pieces = std::mem::take(pieces);
                        if pieces.is_empty() {
                            // we'll need to send a zero-length data frame
                            self.state.send_data_maybe.notify_one();
                        }
                        outgoing.body = BodyOutgoing::DoneReceiving(pieces);
                        debug!(stream_id = %ev.stream_id, outgoing_body = ?outgoing.body, "got body end");
                    }
                    BodyOutgoing::DoneReceiving(_) => {
                        unreachable!("got body end twice")
                    }
                    BodyOutgoing::DoneSending => {
                        unreachable!("got body end after we sent everything")
                    }
                }
            }
        }

        Ok(())
    }

    async fn write_frame(
        &mut self,
        mut frame: Frame,
        payload: PieceList,
    ) -> Result<(), H2ConnectionError> {
        match &frame.frame_type {
            FrameType::Data(flags) => {
                let mut ss = match self.state.streams.entry(frame.stream_id) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry,
                    std::collections::hash_map::Entry::Vacant(_) => unreachable!(
                        "writing DATA frame for non-existent stream, this should never happen"
                    ),
                };

                // update stream flow control window
                {
                    let outgoing = match ss.get_mut().outgoing_mut() {
                        Some(og) => og,
                        None => {
                            unreachable!("writing DATA frame for stream in the wrong state")
                        }
                    };
                    let payload_len: u32 = payload.len().try_into().unwrap();
                    let next_cap = outgoing.capacity - payload_len as i64;

                    if next_cap < 0 {
                        unreachable!(
                            "should never write a frame that makes the stream capacity negative: outgoing.capacity = {}, payload_len = {}",
                            outgoing.capacity, payload.len()
                        )
                    }
                    outgoing.capacity = next_cap;
                }

                // now update connection flow control window
                {
                    let payload_len: u32 = payload.len().try_into().unwrap();
                    let next_cap = self.state.outgoing_capacity - payload_len as i64;

                    if next_cap < 0 {
                        unreachable!(
                            "should never write a frame that makes the connection capacity negative: outgoing_capacity = {}, payload_len = {}",
                            self.state.outgoing_capacity, payload.len()
                        )
                    }
                    self.state.outgoing_capacity = next_cap;
                }

                if flags.contains(DataFlags::EndStream) {
                    // we won't be sending any more data on this stream
                    self.state
                        .streams_with_pending_data
                        .remove(&frame.stream_id);

                    match ss.get_mut() {
                        StreamState::Open { .. } => {
                            let incoming = match std::mem::take(ss.get_mut()) {
                                StreamState::Open { incoming, .. } => incoming,
                                _ => unreachable!(),
                            };
                            // this avoid having to re-insert the stream in the map
                            *ss.get_mut() = StreamState::HalfClosedLocal { incoming };
                        }
                        _ => {
                            // transition to closed
                            ss.remove();
                            debug!(
                                "Closed stream {} (wrote data w/EndStream), now have {} streams",
                                frame.stream_id,
                                self.state.streams.len()
                            );
                        }
                    }
                }
            }
            FrameType::Settings(_) => {
                // TODO: keep track of whether our new settings have been
                // acknowledged
            }
            _ => {
                // muffin.
            }
        };

        // TODO: enforce max_frame_size from the peer settings, not just u32::max
        frame.len = payload
            .len()
            .try_into()
            .map_err(|_| H2ConnectionError::FrameTooLarge {
                frame_type: frame.frame_type,
                frame_size: payload.len() as _,
                max_frame_size: u32::MAX,
            })?;
        debug!(?frame, ">");
        let frame_roll = frame
            .into_piece(&mut self.out_scratch)
            .map_err(|e| eyre::eyre!(e))?;

        if payload.is_empty() {
            trace!("Writing frame without payload");
            self.transport_w
                .write_all_owned(frame_roll)
                .await
                .map_err(H2ConnectionError::WriteError)?;
        } else {
            trace!("Writing frame with payload");
            self.transport_w
                .writev_all_owned(payload.preceded_by(frame_roll))
                .await
                .map_err(H2ConnectionError::WriteError)?;
        }

        Ok(())
    }

    async fn process_frame(
        &mut self,
        frame: Frame,
        mut payload: Roll,
        rx: &mut mpsc::Receiver<(Frame, Roll)>,
    ) -> Result<(), H2ConnectionError> {
        match frame.frame_type {
            FrameType::Data(flags) => {
                if frame.stream_id == StreamId::CONNECTION {
                    return Err(H2ConnectionError::StreamSpecificFrameToConnection {
                        frame_type: frame.frame_type,
                    });
                }

                let ss = self.state.streams.get_mut(&frame.stream_id).ok_or(
                    H2ConnectionError::StreamClosed {
                        stream_id: frame.stream_id,
                    },
                )?;

                match ss {
                    StreamState::Open { incoming, .. }
                    | StreamState::HalfClosedLocal { incoming } => {
                        let next_cap = incoming.capacity - payload.len() as i64;
                        if next_cap < 0 {
                            return Err(H2ConnectionError::WindowUnderflow {
                                stream_id: frame.stream_id,
                            });
                        }
                        incoming.capacity = next_cap;

                        let which = if frame.is_end_stream() {
                            ChunkPosition::Last
                        } else {
                            ChunkPosition::NotLast
                        };

                        // TODO: give back capacity to peer at some point
                        if let Err(e) = incoming.write_chunk(payload.into(), which).await {
                            self.rst(frame.stream_id, e).await?;
                        } else if flags.contains(DataFlags::EndStream) {
                            if let StreamState::Open { .. } = ss {
                                let outgoing = match std::mem::take(ss) {
                                    StreamState::Open { outgoing, .. } => outgoing,
                                    _ => unreachable!(),
                                };
                                *ss = StreamState::HalfClosedRemote { outgoing };
                            } else if self.state.streams.remove(&frame.stream_id).is_some() {
                                debug!(
                                    "Closed stream (read data w/EndStream) {}, now have {} streams",
                                    frame.stream_id,
                                    self.state.streams.len()
                                );
                            }
                        }
                    }
                    StreamState::HalfClosedRemote { .. } => {
                        debug!(
                            stream_id = %frame.stream_id,
                            "Received data for closed stream"
                        );
                        self.rst(frame.stream_id, H2StreamError::StreamClosed)
                            .await?;
                    }
                    StreamState::Transition => unreachable!(),
                }
            }
            FrameType::Headers(flags) => {
                if flags.contains(HeadersFlags::Priority) {
                    let pri_spec;
                    (payload, pri_spec) = PrioritySpec::parse(payload)
                        .finish()
                        .map_err(|err| eyre::eyre!("parsing error: {err:?}"))?;
                    debug!(exclusive = %pri_spec.exclusive, stream_dependency = ?pri_spec.stream_dependency, weight = %pri_spec.weight, "received priority, exclusive");

                    if pri_spec.stream_dependency == frame.stream_id {
                        return Err(H2ConnectionError::HeadersInvalidPriority {
                            stream_id: frame.stream_id,
                        });
                    }
                }

                let headers_or_trailers;
                let mode;

                match self.state.streams.get_mut(&frame.stream_id) {
                    None => {
                        headers_or_trailers = HeadersOrTrailers::Headers;
                        debug!(
                            stream_id = %frame.stream_id,
                            last_stream_id = %self.state.last_stream_id,
                            next_stream_count = %self.state.streams.len() + 1,
                            "Receiving headers",
                        );

                        if frame.stream_id.is_server_initiated() {
                            return Err(H2ConnectionError::ClientSidShouldBeOdd);
                        }

                        match frame.stream_id.cmp(&self.state.last_stream_id) {
                            std::cmp::Ordering::Less => {
                                // we're going back? we can't.
                                return Err(
                                    H2ConnectionError::ClientSidShouldBeNumericallyIncreasing {
                                        stream_id: frame.stream_id,
                                        last_stream_id: self.state.last_stream_id,
                                    },
                                );
                            }
                            std::cmp::Ordering::Equal => {
                                // we already received headers for this stream,
                                // if we're in this branch (no entry in streams)
                                // that means it was closed in-between.
                                return Err(H2ConnectionError::StreamClosed {
                                    stream_id: frame.stream_id,
                                });
                            }
                            std::cmp::Ordering::Greater => {
                                // TODO: if we're shutting down, ignore streams higher
                                // than the last one we accepted.

                                let max_concurrent_streams = self
                                    .state
                                    .self_settings
                                    .max_concurrent_streams
                                    .unwrap_or(u32::MAX);
                                let num_streams_if_accept = self.state.streams.len() + 1;

                                if num_streams_if_accept > max_concurrent_streams as _ {
                                    // reset the stream, indicating we refused it
                                    self.rst(frame.stream_id, H2StreamError::RefusedStream)
                                        .await?;

                                    // but we still need to skip over any continuation frames
                                    mode = ReadHeadersMode::Skip;
                                } else {
                                    self.state.last_stream_id = frame.stream_id;
                                    mode = ReadHeadersMode::Process;
                                }
                            }
                        }
                    }
                    Some(StreamState::Open { .. } | StreamState::HalfClosedLocal { .. }) => {
                        headers_or_trailers = HeadersOrTrailers::Trailers;
                        debug!("Receiving trailers for stream {}", frame.stream_id);

                        if flags.contains(HeadersFlags::EndStream) {
                            // good, that's what we expect
                            mode = ReadHeadersMode::Process;
                        } else {
                            // ignore trailers, we're not accepting the stream
                            mode = ReadHeadersMode::Skip;

                            self.rst(frame.stream_id, H2StreamError::TrailersNotEndStream)
                                .await?;
                        }
                    }
                    Some(StreamState::HalfClosedRemote { .. }) => {
                        return Err(H2ConnectionError::StreamClosed {
                            stream_id: frame.stream_id,
                        });
                    }
                    Some(StreamState::Transition) => unreachable!(),
                }

                if let Err(e) = self
                    .read_headers(
                        headers_or_trailers,
                        mode,
                        flags,
                        frame.stream_id,
                        payload,
                        rx,
                    )
                    .await
                {
                    match e {
                        H2ErrorLevel::Connection(e) => return Err(e),
                        H2ErrorLevel::Stream(e) => {
                            self.rst(frame.stream_id, e).await?;
                        }
                        H2ErrorLevel::Request(e) => {
                            let stream_id = frame.stream_id;

                            tracing::debug!(?e, %stream_id, "Responding to stream with error");
                            // we need to insert it, otherwise `process_event` will ignore us
                            // sending headers, etc.
                            self.state.streams.insert(
                                stream_id,
                                StreamState::HalfClosedRemote {
                                    outgoing: self.state.mk_stream_outgoing(),
                                },
                            );
                            // TODO: inserting/removing here is probably unnecessary.

                            // respond with status code
                            let responder =
                                Responder::new(H2Encoder::new(frame.stream_id, self.ev_tx.clone()));
                            responder
                                .write_final_response_with_body(
                                    crate::Response {
                                        version: Version::HTTP_2,
                                        status: e.status,
                                        headers: Default::default(),
                                    },
                                    &mut SinglePieceBody::new(e.message),
                                )
                                .await?;

                            // don't even store the stream state anywhere, just record the last
                            // stream id since we technically processed the request? maybe?
                            // is returning a 4xx "processing" the request?
                            self.state.last_stream_id = frame.stream_id;
                        }
                    }
                }
            }
            FrameType::Priority => {
                let pri_spec = match PrioritySpec::parse(payload) {
                    Ok((_rest, pri_spec)) => pri_spec,
                    Err(_e) => {
                        self.rst(
                            frame.stream_id,
                            H2StreamError::InvalidPriorityFrameSize {
                                frame_size: frame.len,
                            },
                        )
                        .await?;
                        return Ok(());
                    }
                };
                debug!(?pri_spec, "received priority frame");

                if pri_spec.stream_dependency == frame.stream_id {
                    return Err(H2ConnectionError::HeadersInvalidPriority {
                        stream_id: frame.stream_id,
                    });
                }
            }
            // note: this always unconditionally transitions the stream to closed
            FrameType::RstStream => {
                // error code is a 32bit little-endian integer
                // a frame size of 4 is expected, if not send a PROTOCOL_ERROR
                if frame.len != 4 {
                    self.rst(
                        frame.stream_id,
                        H2StreamError::InvalidRstStreamFrameSize {
                            frame_size: frame.len,
                        },
                    )
                    .await?;
                    return Ok(());
                }
                // TODO: do something with the error code?

                match self.state.streams.remove(&frame.stream_id) {
                    None => {
                        return Err(H2ConnectionError::RstStreamForUnknownStream {
                            stream_id: frame.stream_id,
                        })
                    }
                    Some(ss) => {
                        debug!(
                            "Closed stream (read RstStream) {}, now have {} streams",
                            frame.stream_id,
                            self.state.streams.len()
                        );
                        match ss {
                            StreamState::Open { mut incoming, .. }
                            | StreamState::HalfClosedLocal { mut incoming, .. } => {
                                incoming
                                    .send_error(eyre::eyre!("Received RST_STREAM from peer"))
                                    .await;
                            }
                            StreamState::HalfClosedRemote { .. } => {
                                // good
                            }
                            StreamState::Transition => unreachable!(),
                        }
                    }
                }
            }
            FrameType::Settings(s) => {
                if frame.stream_id != StreamId::CONNECTION {
                    return Err(H2ConnectionError::SettingsWithNonZeroStreamId {
                        stream_id: frame.stream_id,
                    });
                }

                if payload.len() % 6 != 0 {
                    return Err(H2ConnectionError::SettingsInvalidLength {
                        len: payload.len() as _,
                    });
                }

                if s.contains(SettingsFlags::Ack) {
                    debug!("Peer has acknowledged our settings, cool");
                    if !payload.is_empty() {
                        return Err(H2ConnectionError::SettingsInvalidLength {
                            len: payload.len() as _,
                        });
                    }
                } else {
                    let original_initial_window_size = self.state.peer_settings.initial_window_size;
                    let s = &mut self.state.peer_settings;

                    Settings::parse(&payload[..], |code, value| {
                        s.apply(code, value)?;
                        match code {
                            Setting::HeaderTableSize => {
                                self.hpack_enc.set_max_table_size(value as _);
                            }
                            _ => {
                                // nothing to do
                            }
                        }
                        Ok(())
                    })
                    .map_err(H2ConnectionError::BadSettingValue)?;

                    let initial_window_size_delta =
                        (s.initial_window_size as i64) - (original_initial_window_size as i64);

                    let mut maybe_send_data = false;
                    if initial_window_size_delta != 0 {
                        // apply that delta to all streams
                        for (id, stream) in self.state.streams.iter_mut() {
                            if let Some(outgoing) = stream.outgoing_mut() {
                                let next_cap = outgoing.capacity + initial_window_size_delta;
                                if next_cap > MAX_WINDOW_SIZE {
                                    return Err(
                                        H2ConnectionError::StreamWindowSizeOverflowDueToSettings {
                                            stream_id: *id,
                                        },
                                    );
                                }
                                // if capacity was negative or zero, and is now greater than zero,
                                // we need to maybe send data
                                if next_cap > 0 && outgoing.capacity <= 0 {
                                    debug!(?id, %next_cap, "stream capacity was <= 0, now > 0");
                                    maybe_send_data = true;
                                }
                                outgoing.capacity = next_cap;
                            }
                        }
                    }

                    let frame = Frame::new(
                        FrameType::Settings(SettingsFlags::Ack.into()),
                        StreamId::CONNECTION,
                    );
                    self.write_frame(frame, PieceList::default()).await?;
                    debug!("Acknowledged peer settings");

                    if maybe_send_data {
                        self.state.send_data_maybe.notify_one();
                    }
                }
            }
            FrameType::PushPromise => {
                return Err(H2ConnectionError::ClientSentPushPromise);
            }
            FrameType::Ping(flags) => {
                if frame.stream_id != StreamId::CONNECTION {
                    return Err(H2ConnectionError::PingFrameWithNonZeroStreamId {
                        stream_id: frame.stream_id,
                    });
                }

                if frame.len != 8 {
                    return Err(H2ConnectionError::PingFrameInvalidLength { len: frame.len });
                }

                if flags.contains(PingFlags::Ack) {
                    // TODO: check that payload matches the one we sent?
                    return Ok(());
                }

                // send pong frame
                let flags = PingFlags::Ack.into();
                let frame = Frame::new(FrameType::Ping(flags), StreamId::CONNECTION)
                    .with_len(payload.len() as u32);
                self.write_frame(frame, PieceList::default().followed_by(payload))
                    .await?;
            }
            FrameType::GoAway => {
                if frame.stream_id != StreamId::CONNECTION {
                    return Err(H2ConnectionError::GoAwayWithNonZeroStreamId {
                        stream_id: frame.stream_id,
                    });
                }

                self.goaway_recv = true;

                // TODO: this should probably have other effects than setting
                // this flag.
            }
            FrameType::WindowUpdate => {
                if payload.len() != 4 {
                    return Err(H2ConnectionError::WindowUpdateInvalidLength {
                        len: payload.len() as _,
                    });
                }

                let (_, update) = WindowUpdate::parse(payload)
                    .finish()
                    .map_err(|err| eyre::eyre!("parsing error: {err:?}"))?;
                debug!(?update, "Received window update");

                if update.increment == 0 {
                    return Err(H2ConnectionError::WindowUpdateZeroIncrement);
                }

                if frame.stream_id == StreamId::CONNECTION {
                    let new_capacity = self.state.outgoing_capacity + update.increment as i64;
                    if new_capacity > MAX_WINDOW_SIZE {
                        return Err(H2ConnectionError::WindowUpdateOverflow);
                    };

                    debug!(old_capacity = %self.state.outgoing_capacity, %new_capacity, "connection window update");
                    self.state.outgoing_capacity = new_capacity;
                    self.state.send_data_maybe.notify_one();
                } else {
                    let outgoing = match self
                        .state
                        .streams
                        .get_mut(&frame.stream_id)
                        .and_then(|ss| ss.outgoing_mut())
                    {
                        Some(ss) => ss,
                        None => {
                            return Err(H2ConnectionError::WindowUpdateForUnknownOrClosedStream {
                                stream_id: frame.stream_id,
                            });
                        }
                    };

                    let new_capacity = outgoing.capacity + update.increment as i64;
                    if new_capacity > MAX_WINDOW_SIZE {
                        // reset the stream
                        self.rst(frame.stream_id, H2StreamError::WindowUpdateOverflow)
                            .await?;
                        return Ok(());
                    }

                    let old_capacity = outgoing.capacity;
                    debug!(stream_id = %frame.stream_id, %old_capacity, %new_capacity, "stream window update");
                    outgoing.capacity = new_capacity;

                    // insert into streams_with_pending_data if the old capacity was <= zero
                    // and the new capacity is > zero
                    if old_capacity <= 0 && new_capacity > 0 {
                        debug!(conn_capacity = %self.state.outgoing_capacity, "stream capacity is newly positive, inserting in streams_with_pending_data");
                        self.state.streams_with_pending_data.insert(frame.stream_id);

                        // if the connection has capacity, notify!
                        if self.state.outgoing_capacity > 0 {
                            debug!(stream_id = ?frame.stream_id, "stream window update, maybe send data");
                            self.state.send_data_maybe.notify_one();
                        }
                    }
                }
            }
            FrameType::Continuation(_flags) => {
                return Err(H2ConnectionError::UnexpectedContinuationFrame {
                    stream_id: frame.stream_id,
                });
            }
            FrameType::Unknown(ft) => {
                trace!(
                    "ignoring unknown frame with type 0x{:x}, flags 0x{:x}",
                    ft.ty,
                    ft.flags
                );
            }
        }

        Ok(())
    }

    /// Send a RST_STREAM frame to the peer.
    async fn rst(
        &mut self,
        stream_id: StreamId,
        e: H2StreamError,
    ) -> Result<(), H2ConnectionError> {
        self.state.streams.remove(&stream_id);

        let error_code = e.as_known_error_code();
        debug!("Sending rst because: {e} (known error code: {error_code:?})");

        debug!(%stream_id, ?error_code, "Sending RstStream");
        let payload = self
            .out_scratch
            .put_to_roll(4, |mut slice| {
                slice.write_u32::<BigEndian>(error_code.repr())?;
                Ok(())
            })
            .unwrap();

        let frame = Frame::new(FrameType::RstStream, stream_id)
            .with_len((payload.len()).try_into().unwrap());
        self.write_frame(frame, PieceList::single(payload)).await?;

        Ok(())
    }

    async fn read_headers(
        &mut self,
        headers_or_trailers: HeadersOrTrailers,
        mode: ReadHeadersMode,
        flags: BitFlags<HeadersFlags, u8>,
        stream_id: StreamId,
        payload: Roll,
        rx: &mut mpsc::Receiver<(Frame, Roll)>,
    ) -> Result<(), H2ErrorLevel> {
        let end_stream = flags.contains(HeadersFlags::EndStream);

        enum Data {
            Single(Roll),
            Multi(SmallVec<[Roll; 2]>),
        }

        let data = if flags.contains(HeadersFlags::EndHeaders) {
            // good, no continuation frames needed
            Data::Single(payload)
        } else {
            // read continuation frames

            #[allow(unused, clippy::let_unit_value)]
            let flags = (); // don't accidentally use the `flags` variable

            let mut fragments = smallvec![payload];

            loop {
                let (continuation_frame, continuation_payload) = match rx.recv().await {
                    Some(t) => t,
                    None => {
                        // even though this error is "for a stream", it's a
                        // connection error, because it means the peer doesn't
                        // know how to speak HTTP/2.
                        return Err(H2ConnectionError::ExpectedContinuationFrame {
                            stream_id,
                            frame_type: None,
                        }
                        .into());
                    }
                };

                if stream_id != continuation_frame.stream_id {
                    return Err(H2ConnectionError::ExpectedContinuationForStream {
                        stream_id,
                        continuation_stream_id: continuation_frame.stream_id,
                    }
                    .into());
                }

                let cont_flags = match continuation_frame.frame_type {
                    FrameType::Continuation(flags) => flags,
                    other => {
                        return Err(H2ConnectionError::ExpectedContinuationFrame {
                            stream_id,
                            frame_type: Some(other),
                        }
                        .into())
                    }
                };

                // add fragment
                fragments.push(continuation_payload);

                if cont_flags.contains(ContinuationFlags::EndHeaders) {
                    // we're done
                    break;
                }
            }

            Data::Multi(fragments)
        };

        if matches!(mode, ReadHeadersMode::Skip) {
            // that's all we need to do: we're not actually validating the
            // headers, we already send a RST
            return Ok(());
        }

        let mut method: Option<Method> = None;
        let mut scheme: Option<Scheme> = None;
        let mut path: Option<PieceStr> = None;
        let mut authority: Option<Authority> = None;

        let mut headers = Headers::default();

        {
            // we assign to an outer variable because header decoding needs to finish no
            // matter what: if we receive invalid headers for one request, we should still
            // keep reading the next request's headers, and that requires advancing the
            // huffman decoder's state, etc.
            let mut req_error: Option<H2StreamError> = None;
            let mut saw_regular_header = false;

            let on_header_pair = |key: Cow<[u8]>, value: Cow<[u8]>| {
                if req_error.is_some() {
                    return;
                }

                debug!(
                    "{headers_or_trailers:?} | {}: {}",
                    std::str::from_utf8(&key).unwrap_or("<non-utf8-key>"), /* TODO: does this
                                                                            * hurt performance
                                                                            * when debug logging
                                                                            * is disabled? */
                    std::str::from_utf8(&value).unwrap_or("<non-utf8-value>"),
                );

                if &key[..1] == b":" {
                    if saw_regular_header {
                        req_error = Some(H2StreamError::BadRequest(
                                "All pseudo-header fields MUST appear in a field block before all regular field lines (RFC 9113, section 8.3)"
                        ));
                        return;
                    }

                    if matches!(headers_or_trailers, HeadersOrTrailers::Trailers) {
                        req_error = Some(H2StreamError::BadRequest(
                            "Pseudo-header fields MUST NOT appear in a trailer section (RFC 9113, section 8.3)"
                        ));
                        return;
                    }

                    // it's a pseudo-header!
                    // TODO: reject headers that occur after pseudo-headers
                    match &key[1..] {
                        b"method" => {
                            let value: PieceStr = match Piece::from(value.to_vec()).to_str() {
                                Ok(p) => p,
                                Err(_) => {
                                    req_error = Some(H2StreamError::BadRequest(
                                        "invalid ':method' pseudo-header: not valid utf-8, so certainly not a valid method like POST, GET, OPTIONS, CONNECT, PROPFIND, etc.",
                                    ));
                                    return;
                                }
                            };
                            if method.replace(Method::from(value)).is_some() {
                                req_error = Some(H2StreamError::BadRequest("duplicate ':method' pseudo-header. All HTTP/2 requests MUST include _exactly one_ valid value for the ':method', ':scheme', and ':path' pseudo-header fields, unless they are CONNECT requests (RFC 9114, section 8.3.1)"));
                            }
                        }
                        b"scheme" => {
                            let value: PieceStr = match Piece::from(value.to_vec()).to_str() {
                                Ok(p) => p,
                                Err(_) => {
                                    req_error = Some(H2StreamError::BadRequest(
                                        "invalid ':scheme' pseudo-header: not valid utf-8",
                                    ));
                                    return;
                                }
                            };
                            if scheme.replace(value.parse().unwrap()).is_some() {
                                req_error = Some(H2StreamError::BadRequest("duplicate ':scheme' pseudo-header. All HTTP/2 requests MUST include _exactly one_ valid value for the ':method', ':scheme', and ':path' pseudo-header fields, unless they are CONNECT requests (RFC 9113, section 8.3.1)"));
                            }
                        }
                        b"path" => {
                            let value: PieceStr = match Piece::from(value.to_vec()).to_str() {
                                Ok(val) => val,
                                Err(_) => {
                                    req_error = Some(H2StreamError::BadRequest("invalid ':path' pseudo-header (not valid utf-8, which is _certainly_ not a valid URI, as defined by RFC 3986, section 2. See also RFC 9113, section 8.3.1). "));
                                    return;
                                }
                            };

                            if path.replace(value).is_some() {
                                req_error = Some(H2StreamError::BadRequest("duplicate ':path' pseudo-header. All HTTP/2 requests MUST include _exactly one_ valid value for the ':method', ':scheme', and ':path' pseudo-header fields, unless they are CONNECT requests (RFC 9113, section 8.3.1)"));
                            }
                        }
                        b"authority" => {
                            let value: PieceStr = match Piece::from(value.to_vec()).to_str() {
                                Ok(p) => p,
                                Err(_) => {
                                    req_error = Some(H2StreamError::BadRequest(
                                        "invalid ':authority' pseudo-header: not valid utf-8",
                                    ));
                                    return;
                                }
                            };
                            let value: Authority = match value.parse() {
                                Ok(a) => a,
                                Err(_) => {
                                    req_error = Some(H2StreamError::BadRequest("invalid ':authority' pseudo-header: not a valid authority (which is to say: not a valid URI, see RFC 3986, section 3.2)"));
                                    return;
                                }
                            };
                            if authority.replace(value).is_some() {
                                req_error = Some(H2StreamError::BadRequest("duplicate ':authority' pseudo-header. All HTTP/2 requests MUST include _exactly one_ valid value for the ':method', ':scheme', and ':path' pseudo-header fields, unless they are CONNECT requests (RFC 9113, section 8.3.1)"));
                            }
                        }
                        _ => {
                            req_error = Some(H2StreamError::BadRequest(
                                "received invalid pseudo-header. the only defined pseudo-headers are: ':method', ':scheme', ':path', ':authority', ':status' (RFC 9113, section 8.1)",
                            ));
                        }
                    }
                } else {
                    saw_regular_header = true;

                    let name = match HeaderName::from_bytes(&key[..]) {
                        Ok(name) => name,
                        Err(_) => {
                            req_error = Some(H2StreamError::BadRequest(
                                "invalid header name. see RFC 9113, section 8.2.1, 'Field validity'",
                            ));
                            return;
                        }
                    };

                    // Note: An implementation that validates fields according to the definitions in
                    // Sections 5.1 and 5.5 of [HTTP] only needs an additional check that field
                    // names do not include uppercase characters.
                    if key.iter().any(|b: &u8| b.is_ascii_uppercase()) {
                        req_error = Some(H2StreamError::BadRequest(
                            "A field name MUST NOT contain characters in the ranges 0x00-0x20, 0x41-0x5a, or 0x7f-0xff (all ranges inclusive). This specifically excludes all non-visible ASCII characters, ASCII SP (0x20), and uppercase characters ('A' to 'Z', ASCII 0x41 to 0x5a). See RFC9113, section 8.2.1, 'Field Validity'",
                        ));
                        return;
                    }

                    // connection-specific headers are forbidden
                    static KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");
                    static PROXY_CONNECTION: HeaderName =
                        HeaderName::from_static("proxy-connection");

                    if name == http::header::CONNECTION
                        || name == KEEP_ALIVE
                        || name == PROXY_CONNECTION
                        || name == http::header::TRANSFER_ENCODING
                        || name == http::header::UPGRADE
                    {
                        req_error = Some(H2StreamError::BadRequest(
                            "connection-specific headers are forbidden. see RFC 9113, section 8.1.2",
                        ));
                        return;
                    }

                    if name == http::header::TE && &value[..] != b"trailers" {
                        req_error = Some(H2StreamError::BadRequest(
                            "The only exception to this is the TE header field, which MAY be present in an HTTP/2 request; when it is, it MUST NOT contain any value other than 'trailers'. cf. RFC9113, Section 8.2.2",
                        ));
                        return;
                    }

                    // header values aren't allowed to have CR or LF
                    let first = value.first();
                    let last = value.last();
                    if first == Some(&b' ')
                        || first == Some(&b'\x09')
                        || last == Some(&b' ')
                        || last == Some(&b'\x09')
                    {
                        req_error = Some(H2StreamError::BadRequest(
                            "A field value MUST NOT start or end with an ASCII whitespace character (ASCII SP or HTAB, 0x20 or 0x09). (RFC 9113, section 8.2.1, 'Field validity')",
                        ));
                        return;
                    }

                    if value
                        .iter()
                        .any(|&b| b == b'\r' || b == b'\n' || b == b'\0')
                    {
                        req_error = Some(H2StreamError::BadRequest(
                            "A field value MUST NOT contain the zero value (ASCII NUL, 0x00), line feed (ASCII LF, 0x0a), or carriage return (ASCII CR, 0x0d) at any position. See RFC 9113, section 8.2.1, 'Field validity'",
                        ));
                        return;
                    }

                    let value: Piece = value.to_vec().into();
                    headers.append(name, value);
                }
            };

            match data {
                Data::Single(payload) => {
                    self.hpack_dec
                        .decode_with_cb(&payload[..], on_header_pair)
                        .map_err(|e| H2ErrorLevel::Connection(e.into()))?;
                }
                Data::Multi(fragments) => {
                    let total_len = fragments.iter().map(|f| f.len()).sum();
                    // this is a slow path, let's do a little heap allocation. we could
                    // be using `RollMut` for this, but it would probably need to resize
                    // a bunch
                    let mut payload = Vec::with_capacity(total_len);
                    for frag in &fragments {
                        payload.extend_from_slice(&frag[..]);
                    }
                    self.hpack_dec
                        .decode_with_cb(&payload[..], on_header_pair)
                        .map_err(|e| H2ErrorLevel::Connection(e.into()))?;
                }
            };

            if let Some(req_error) = req_error {
                return Err(req_error.into());
            }
        }

        match headers_or_trailers {
            HeadersOrTrailers::Headers => {
                // TODO: cf. https://httpwg.org/specs/rfc9113.html#HttpRequest
                // A server SHOULD treat a request as malformed if it contains a Host header
                // field that identifies an entity that differs from the entity in the
                // ":authority" pseudo-header field.

                let method = match method {
                    Some(method) => {
                        if method == Method::Connect {
                            // RFC 9113, section 8.5 'The CONNECT method': The ":scheme" and ":path"
                            // pseudo-header fields MUST be omitted.
                            if scheme.is_some() {
                                return Err(H2StreamError::BadRequest(
                                    "CONNECT method MUST NOT include ':scheme' pseudo-header",
                                )
                                .into());
                            }
                            if path.is_some() {
                                return Err(H2StreamError::BadRequest(
                                    "CONNECT method MUST NOT include ':path' pseudo-header",
                                )
                                .into());
                            }
                            if authority.is_none() {
                                return Err(H2StreamError::BadRequest(
                                    "CONNECT method MUST include ':authority' pseudo-header",
                                )
                                .into());
                            }

                            // well, also, we just don't support the `CONNECT` method.
                            return Err(H2RequestError {
                                status: StatusCode::NOT_IMPLEMENTED,
                                message: "CONNECT method is not supported".into(),
                            }
                            .into());
                        }

                        method
                    }
                    None => {
                        return Err(
                            H2StreamError::BadRequest("missing :method pseudo-header").into()
                        )
                    }
                };

                let scheme = match scheme {
                    Some(scheme) => scheme,
                    None => {
                        return Err(
                            H2StreamError::BadRequest("missing :scheme pseudo-header").into()
                        );
                    }
                };

                let path = match path {
                    Some(path) => path,
                    None => {
                        return Err(
                            H2StreamError::BadRequest("missing :path pseudo-header, cf. RFC9113, section 8.3.1: This pseudo-header field MUST NOT be empty for 'http' or 'https' URIs; 'http' or 'https' URIs that do not contain a path component MUST include a value of '/'.").into()
                        );
                    }
                };

                if path.len() == 0 && (scheme == Scheme::HTTP || scheme == Scheme::HTTPS) {
                    return Err(H2StreamError::BadRequest(
                        "as per RFC9113, section 8.3.1, ':path' header value MUST NOT be empty for 'http' and 'https' URIs",
                    ).into());
                }

                let path_and_query: PathAndQuery = match path.parse() {
                    Ok(p) => p,
                    Err(_) => {
                        return Err(H2StreamError::BadRequest(
                            "':path' header value is not a valid PathAndQuery",
                        )
                        .into());
                    }
                };

                let authority = match authority {
                    Some(authority) => {
                        // if there's a `host` header, it must match the `:authority` pseudo-header
                        if let Some(host) = headers.get(header::HOST) {
                            let host = std::str::from_utf8(host).map_err(|_| {
                                H2StreamError::BadRequest("'host' header value is not utf-8")
                            })?;
                            let host_authority: Authority = host.parse().map_err(|_| {
                                H2StreamError::BadRequest("'host' header value is not a valid URI")
                            })?;
                            if host_authority != authority {
                                return Err(H2StreamError::BadRequest(
                                    "'host' header value does not match ':authority' pseudo-header value, cf. RFC9113, Section 8.3.1: A server SHOULD treat a request as malformed if it contains a Host header field that identifies an entity that differs from the entity in the ':authority' pseudo-header field"
                                ).into());
                            }
                        }

                        Some(authority)
                    }
                    None => match headers.get(header::HOST) {
                        Some(host) => {
                            let host = std::str::from_utf8(host).map_err(|_| {
                                H2StreamError::BadRequest("'host' header value is not utf-8")
                            })?;
                            let authority: Authority = host.parse().map_err(|_| {
                                H2StreamError::BadRequest("'host' header value is not a valid URI")
                            })?;
                            Some(authority)
                        }
                        None => None,
                    },
                };

                let mut uri_parts: http::uri::Parts = Default::default();
                uri_parts.scheme = Some(scheme);
                uri_parts.authority = authority;
                uri_parts.path_and_query = Some(path_and_query);

                let uri = match http::uri::Uri::from_parts(uri_parts) {
                    Ok(uri) => uri,
                    Err(_) => {
                        return Err(H2RequestError {
                            status: StatusCode::BAD_REQUEST,
                            message: "invalid URI parts".into(),
                        }
                        .into())
                    }
                };

                let req = Request {
                    method,
                    uri,
                    version: Version::HTTP_2,
                    headers,
                };
                let content_length: Option<u64> = match req
                    .headers
                    .get(http::header::CONTENT_LENGTH)
                {
                    Some(len) => {
                        let len = std::str::from_utf8(len).map_err(|_| {
                            H2StreamError::BadRequest("content-length header value is not utf-8")
                        })?;
                        let len = len.parse().map_err(|_| {
                            H2StreamError::BadRequest(
                                "content-length header value is not a valid integer",
                            )
                        })?;
                        Some(len)
                    }
                    None => {
                        if end_stream {
                            Some(0)
                        } else {
                            None
                        }
                    }
                };

                let responder = Responder::new(H2Encoder::new(stream_id, self.ev_tx.clone()));

                let (piece_tx, piece_rx) = mpsc::channel::<IncomingMessagesResult>(1); // TODO: is 1 a sensible value here?

                let req_body = H2Body {
                    content_length,
                    eof: end_stream,
                    rx: piece_rx,
                };

                let incoming = StreamIncoming::new(
                    self.state.self_settings.initial_window_size as _,
                    content_length,
                    piece_tx,
                );
                let outgoing: StreamOutgoing = self.state.mk_stream_outgoing();
                self.state.streams.insert(
                    stream_id,
                    if end_stream {
                        StreamState::HalfClosedRemote { outgoing }
                    } else {
                        StreamState::Open { incoming, outgoing }
                    },
                );
                debug!(
                    "Just accepted stream, now have {} streams",
                    self.state.streams.len()
                );

                // FIXME: don't spawn, just add to an unordered futures
                // instead and poll it in our main loop, to do intra-task
                // concurrency.
                //
                // this lets us freeze the entire http2 server and explore
                // its entire state.
                fluke_buffet::spawn({
                    let driver = self.driver.clone();
                    async move {
                        let mut req_body = req_body;
                        let responder = responder;

                        match driver.handle(req, &mut req_body, responder).await {
                            Ok(_responder) => {
                                debug!("Handler completed successfully, gave us a responder");
                            }
                            Err(e) => {
                                // TODO: actually handle that error.
                                debug!("Handler returned an error: {e}")
                            }
                        }
                    }
                });
            }
            HeadersOrTrailers::Trailers => {
                match self.state.streams.entry(stream_id) {
                    Entry::Occupied(mut slot) => match slot.get_mut() {
                        StreamState::Open { incoming, .. } => {
                            incoming.write_trailers(headers).await?;

                            // set stream state to half closed remote. we do a little
                            // dance to avoid re-inserting.
                            let hcr = slot.insert(StreamState::Transition);
                            slot.insert(match hcr {
                                StreamState::Open { outgoing, .. } => {
                                    StreamState::HalfClosedRemote { outgoing }
                                }
                                _ => unreachable!(),
                            });
                        }
                        _ => {
                            unreachable!("stream state should be open when we receive trailers")
                        }
                    },
                    Entry::Vacant(_) => {
                        // we received trailers for a stream that doesn't exist
                        // anymore, ignore them
                        unreachable!("stream state should be open when we receive trailers")
                    }
                }
            }
        }

        Ok(())
    }
}

enum ReadHeadersMode {
    // we're accepting the stream or processing trailers, we want to
    // process the headers we read.
    Process,
    // we're refusing the stream, we want to skip over the headers we read.
    Skip,
}

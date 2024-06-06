use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
};

use fluke_buffet::Piece;
use fluke_hpack::decoder::DecoderError;
use http::StatusCode;
use tokio::sync::Notify;

use crate::Response;

use super::body::StreamIncoming;
use fluke_h2_parse::{FrameType, KnownErrorCode, Settings, SettingsError, StreamId};

pub(crate) struct ConnState {
    pub(crate) streams: HashMap<StreamId, StreamState>,
    pub(crate) last_stream_id: StreamId,

    pub(crate) self_settings: Settings,
    pub(crate) peer_settings: Settings,

    /// notified when we have data to send, like when:
    /// - an H2Body has been written to, AND
    /// - the corresponding stream has available capacity
    /// - the connection has available capacity
    ///
    /// FIXME: we don't need Notify at all, it uses atomic operations
    /// but all we're doing is single-threaded.
    pub(crate) send_data_maybe: Notify,
    pub(crate) streams_with_pending_data: HashSet<StreamId>,

    pub(crate) incoming_capacity: i64,
    pub(crate) outgoing_capacity: i64,
}

impl Default for ConnState {
    fn default() -> Self {
        let mut s = Self {
            streams: Default::default(),
            last_stream_id: StreamId(0),

            self_settings: Default::default(),
            peer_settings: Default::default(),

            send_data_maybe: Default::default(),
            streams_with_pending_data: Default::default(),

            incoming_capacity: 0,
            outgoing_capacity: 0,
        };
        s.incoming_capacity = s.self_settings.initial_window_size as _;
        s.outgoing_capacity = s.peer_settings.initial_window_size as _;

        s
    }
}

impl ConnState {
    /// create a new [StreamOutgoing] based on our current settings
    pub(crate) fn mk_stream_outgoing(&self) -> StreamOutgoing {
        StreamOutgoing {
            headers: HeadersOutgoing::WaitingForHeaders,
            body: BodyOutgoing::StillReceiving(Default::default()),
            capacity: self.peer_settings.initial_window_size as _,
        }
    }
}

// cf. RFC 9113, 5.1 Stream States:
//
//                               +--------+
//                       send PP |        | recv PP
//                      ,--------+  idle  +--------.
//                     /         |        |         \
//                    v          +--------+          v
//             +----------+          |           +----------+
//             |          |          | send H /  |          |
//      ,------+ reserved |          | recv H    | reserved +------.
//      |      | (local)  |          |           | (remote) |      |
//      |      +---+------+          v           +------+---+      |
//      |          |             +--------+             |          |
//      |          |     recv ES |        | send ES     |          |
//      |   send H |     ,-------+  open  +-------.     | recv H   |
//      |          |    /        |        |        \    |          |
//      |          v   v         +---+----+         v   v          |
//      |      +----------+          |           +----------+      |
//      |      |   half-  |          |           |   half-  |      |
//      |      |  closed  |          | send R /  |  closed  |      |
//      |      | (remote) |          | recv R    | (local)  |      |
//      |      +----+-----+          |           +-----+----+      |
//      |           |                |                 |           |
//      |           | send ES /      |       recv ES / |           |
//      |           |  send R /      v        send R / |           |
//      |           |  recv R    +--------+   recv R   |           |
//      | send R /  `----------->|        |<-----------'  send R / |
//      | recv R                 | closed |               recv R   |
//      `----------------------->|        |<-----------------------'
//                               +--------+
//
//                         Figure 2: Stream States
//
//  send:  endpoint sends this frame
//  recv:  endpoint receives this frame
//  H:  HEADERS frame (with implied CONTINUATION frames)
//  ES:  END_STREAM flag
//  R:  RST_STREAM frame
//  PP:  PUSH_PROMISE frame (with implied CONTINUATION frames); state
//     transitions are for the promised stream
#[derive(Default)]
pub(crate) enum StreamState {
    // we have received full HEADERS
    Open {
        incoming: StreamIncoming,
        outgoing: StreamOutgoing,
    },

    // the peer has sent END_STREAM/RST_STREAM (but we might still send data to the peer)
    HalfClosedRemote {
        outgoing: StreamOutgoing,
    },

    // we have sent END_STREAM/RST_STREAM (but we might still receive data from the peer)
    HalfClosedLocal {
        incoming: StreamIncoming,
    },

    // A transition state used for state machine code
    #[default]
    Transition,
    //
    //
    // Note: the "Closed" state is indicated by not having an entry in the map
}

impl StreamState {
    /// Get the inner `StreamOutgoing` if the state is `Open` or
    /// `HalfClosedRemote`.
    pub(crate) fn outgoing_mut(&mut self) -> Option<&mut StreamOutgoing> {
        match self {
            StreamState::Open { outgoing, .. } => Some(outgoing),
            StreamState::HalfClosedRemote { outgoing, .. } => Some(outgoing),
            _ => None,
        }
    }
}

pub(crate) struct StreamOutgoing {
    pub(crate) headers: HeadersOutgoing,
    pub(crate) body: BodyOutgoing,

    // window size of the stream, ie. how many bytes
    // we can send to the receiver before waiting.
    pub(crate) capacity: i64,
}

#[derive(Default)]
pub(crate) enum HeadersOutgoing {
    // We have not yet sent any headers, and are waiting for the user to send them
    WaitingForHeaders,

    // The user gave us headers to send, but we haven't started yet
    WroteNone(Piece),

    // We have sent some headers, but not all (we're still sending CONTINUATION frames)
    WroteSome(Piece),

    // We've sent everything
    #[default]
    WroteAll,
}

impl HeadersOutgoing {
    #[inline(always)]
    pub(crate) fn has_more_to_write(&self) -> bool {
        match self {
            HeadersOutgoing::WaitingForHeaders => true,
            HeadersOutgoing::WroteNone(_) => true,
            HeadersOutgoing::WroteSome(_) => true,
            HeadersOutgoing::WroteAll => false,
        }
    }

    #[inline(always)]
    pub(crate) fn take_piece(&mut self) -> Piece {
        match std::mem::take(self) {
            Self::WroteNone(piece) => piece,
            Self::WroteSome(piece) => piece,
            _ => Piece::empty(),
        }
    }
}

pub(crate) enum BodyOutgoing {
    /// We are still receiving body pieces from the user
    StillReceiving(VecDeque<Piece>),

    /// We have received all body pieces from the user
    DoneReceiving(VecDeque<Piece>),

    /// We have sent all data to the peer
    DoneSending,
}

impl fmt::Debug for BodyOutgoing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BodyOutgoing::StillReceiving(pieces) => f
                .debug_tuple("BodyOutgoing::StillReceiving")
                .field(&pieces.len())
                .finish(),
            BodyOutgoing::DoneReceiving(pieces) => f
                .debug_tuple("BodyOutgoing::DoneReceiving")
                .field(&pieces.len())
                .finish(),
            BodyOutgoing::DoneSending => f.debug_tuple("BodyOutgoing::DoneSending").finish(),
        }
    }
}

impl BodyOutgoing {
    /// It's still possible for the user to send more data
    #[inline(always)]
    pub(crate) fn might_receive_more(&self) -> bool {
        match self {
            BodyOutgoing::StillReceiving(_) => true,
            BodyOutgoing::DoneReceiving(_) => true,
            BodyOutgoing::DoneSending => false,
        }
    }

    #[inline(always)]
    pub(crate) fn has_more_to_write(&self) -> bool {
        match self {
            BodyOutgoing::StillReceiving(_) => true,
            BodyOutgoing::DoneReceiving(_) => true,
            BodyOutgoing::DoneSending => false,
        }
    }

    #[inline(always)]
    pub(crate) fn pop_front(&mut self) -> Option<Piece> {
        match self {
            BodyOutgoing::StillReceiving(pieces) => pieces.pop_front(),
            BodyOutgoing::DoneReceiving(pieces) => {
                let piece = pieces.pop_front();
                if pieces.is_empty() {
                    *self = BodyOutgoing::DoneSending;
                }
                piece
            }
            BodyOutgoing::DoneSending => None,
        }
    }

    #[inline(always)]
    pub(crate) fn push_front(&mut self, piece: Piece) {
        match self {
            BodyOutgoing::StillReceiving(pieces) => pieces.push_front(piece),
            BodyOutgoing::DoneReceiving(pieces) => pieces.push_front(piece),
            BodyOutgoing::DoneSending => {
                *self = BodyOutgoing::DoneReceiving([piece].into());
            }
        }
    }

    #[inline(always)]
    pub(crate) fn push_back(&mut self, piece: Piece) {
        match self {
            BodyOutgoing::StillReceiving(pieces) => pieces.push_back(piece),
            BodyOutgoing::DoneReceiving(pieces) => pieces.push_back(piece),
            BodyOutgoing::DoneSending => {
                unreachable!("received a piece after we were done sending")
            }
        }
    }
}

/// An error that may either indicate the peer is misbehaving
/// or just a bad request from the client.
#[derive(Debug, thiserror::Error)]
pub(crate) enum H2ErrorLevel {
    #[error("connection error: {0}")]
    Connection(#[from] H2ConnectionError),

    #[error("stream error: {0}")]
    Stream(#[from] H2StreamError),

    #[error("request error: {0}")]
    Request(#[from] H2RequestError),
}

/// The client done goofed, we're returning 4xx most likely
#[derive(thiserror::Error)]
#[error("client error: {status:?}")]
pub(crate) struct H2RequestError {
    pub(crate) status: StatusCode,
    pub(crate) message: Piece,
}

impl fmt::Debug for H2RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("H2RequestError");
        s.field("status", &self.status);
        match std::str::from_utf8(&self.message[..]) {
            Ok(body) => s.field("body", &body),
            Err(_) => s.field("body", &"(not utf-8)"),
        };
        s.finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum H2ConnectionError {
    #[error("frame too large: {frame_type:?} frame of size {frame_size} exceeds max frame size of {max_frame_size}")]
    FrameTooLarge {
        frame_type: FrameType,
        frame_size: u32,
        max_frame_size: u32,
    },

    #[error("remote hung up while reading payload of {frame_type:?} with length {frame_size}")]
    IncompleteFrame {
        frame_type: FrameType,
        frame_size: u32,
    },

    #[error("headers frame had invalid priority: stream {stream_id} depends on itself")]
    HeadersInvalidPriority { stream_id: StreamId },

    #[error("client tried to initiate an even-numbered stream")]
    ClientSidShouldBeOdd,

    #[error("client stream IDs should be numerically increasing")]
    ClientSidShouldBeNumericallyIncreasing {
        stream_id: StreamId,
        last_stream_id: StreamId,
    },

    #[error("received {frame_type:?} frame with Padded flag but empty payload")]
    PaddedFrameEmpty { frame_type: FrameType },

    #[error("received {frame_type:?} with Padded flag but payload was shorter than padding")]
    PaddedFrameTooShort {
        frame_type: FrameType,
        padding_length: usize,
        frame_size: u32,
    },

    #[error("on stream {stream_id}, expected continuation frame, but got {frame_type:?}")]
    ExpectedContinuationFrame {
        stream_id: StreamId,
        frame_type: Option<FrameType>,
    },

    #[error("expected continuation from for stream {stream_id}, but got continuation for stream {continuation_stream_id}")]
    ExpectedContinuationForStream {
        stream_id: StreamId,
        continuation_stream_id: StreamId,
    },

    #[error("on stream {stream_id}, received unexpected continuation frame")]
    UnexpectedContinuationFrame { stream_id: StreamId },

    #[error("hpack decoding error: {0:?}")]
    HpackDecodingError(#[from] DecoderError),

    #[error("client sent a push promise frame, clients aren't allowed to do that, cf. RFC9113 section 8.4")]
    ClientSentPushPromise,

    #[error("received window update for unknown/closed stream {stream_id}")]
    WindowUpdateForUnknownOrClosedStream { stream_id: StreamId },

    #[error("stream-specific frame {frame_type:?} sent to stream ID 0 (connection-wide)")]
    StreamSpecificFrameToConnection { frame_type: FrameType },

    #[error("other error: {0:?}")]
    Internal(#[from] eyre::Report),

    #[error("error reading/parsing H2 frame: {0:?}")]
    ReadError(eyre::Report),

    #[error("error writing H2 frame: {0:?}")]
    WriteError(std::io::Error),

    #[error("received rst frame for unknown stream")]
    RstStreamForUnknownStream { stream_id: StreamId },

    #[error("received frame for closed stream {stream_id}")]
    StreamClosed { stream_id: StreamId },

    #[error("received ping frame frame with non-zero stream id")]
    PingFrameWithNonZeroStreamId { stream_id: StreamId },

    #[error("received ping frame with invalid length {len}")]
    PingFrameInvalidLength { len: u32 },

    #[error("received settings frame with invalid length {len}")]
    SettingsInvalidLength { len: u32 },

    #[error("received settings frame with non-zero stream id")]
    SettingsWithNonZeroStreamId { stream_id: StreamId },

    #[error("received goaway frame with non-zero stream id")]
    GoAwayWithNonZeroStreamId { stream_id: StreamId },

    #[error("zero increment in window update frame for stream")]
    WindowUpdateZeroIncrement,

    #[error("received window update that made the window size overflow")]
    WindowUpdateOverflow,

    #[error("received frame that would cause the window size to underflow")]
    WindowUnderflow { stream_id: StreamId },

    #[error("received initial window size settings update that made the connection window size overflow")]
    StreamWindowSizeOverflowDueToSettings { stream_id: StreamId },

    #[error("received window update frame with invalid length {len}")]
    WindowUpdateInvalidLength { len: usize },

    #[error("bad setting value: {0}")]
    BadSettingValue(SettingsError),
}

impl H2ConnectionError {
    pub(crate) fn as_known_error_code(&self) -> KnownErrorCode {
        match self {
            // frame size errors
            H2ConnectionError::FrameTooLarge { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::PaddedFrameEmpty { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::PingFrameInvalidLength { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::SettingsInvalidLength { .. } => KnownErrorCode::FrameSizeError,
            H2ConnectionError::WindowUpdateInvalidLength { .. } => KnownErrorCode::FrameSizeError,
            // flow control errors
            H2ConnectionError::WindowUpdateOverflow => KnownErrorCode::FlowControlError,
            H2ConnectionError::WindowUnderflow { .. } => KnownErrorCode::FlowControlError,
            H2ConnectionError::StreamWindowSizeOverflowDueToSettings { .. } => {
                KnownErrorCode::FlowControlError
            }
            H2ConnectionError::BadSettingValue(SettingsError::InitialWindowSizeTooLarge {
                ..
            }) => KnownErrorCode::FlowControlError,
            // compression errors
            H2ConnectionError::HpackDecodingError(_) => KnownErrorCode::CompressionError,
            // stream closed error
            H2ConnectionError::StreamClosed { .. } => KnownErrorCode::StreamClosed,
            // internal errors
            H2ConnectionError::Internal(_) => KnownErrorCode::InternalError,
            // protocol errors
            H2ConnectionError::PaddedFrameTooShort { .. } => KnownErrorCode::ProtocolError,
            H2ConnectionError::StreamSpecificFrameToConnection { .. } => {
                KnownErrorCode::ProtocolError
            }
            _ => KnownErrorCode::ProtocolError,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum H2StreamError {
    #[error("received {data_length} bytes in data frames but content-length announced {content_length} bytes")]
    DataLengthDoesNotMatchContentLength {
        data_length: u64,
        content_length: u64,
    },

    #[error("overflow while calculating content length")]
    OverflowWhileCalculatingContentLength,

    #[error("refused stream (would exceed max concurrent streams)")]
    RefusedStream,

    #[error("trailers must have EndStream flag set")]
    TrailersNotEndStream,

    #[error("received PRIORITY frame with invalid size")]
    InvalidPriorityFrameSize { frame_size: u32 },

    #[error("stream closed")]
    StreamClosed,

    #[error("received RST_STREAM frame with invalid size, expected 4 got {frame_size}")]
    InvalidRstStreamFrameSize { frame_size: u32 },

    #[error("received WINDOW_UPDATE that made the window size overflow")]
    WindowUpdateOverflow,

    #[error("bad request: {0}")]
    BadRequest(&'static str),

    #[error("stream reset")]
    Cancel,
}

impl H2StreamError {
    pub(crate) fn as_known_error_code(&self) -> KnownErrorCode {
        use H2StreamError::*;
        use KnownErrorCode as Code;

        match self {
            Cancel => Code::Cancel,
            // stream closed error
            StreamClosed => Code::StreamClosed,
            // stream refused error
            RefusedStream => Code::RefusedStream,
            // frame size errors
            InvalidPriorityFrameSize { .. } => Code::FrameSizeError,
            InvalidRstStreamFrameSize { .. } => Code::FrameSizeError,
            // flow control errors
            WindowUpdateOverflow => Code::FlowControlError,
            _ => Code::ProtocolError,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum HeadersOrTrailers {
    Headers,
    Trailers,
}

#[derive(Debug)]
pub(crate) struct H2Event {
    pub(crate) stream_id: StreamId,
    pub(crate) payload: H2EventPayload,
}

pub(crate) enum H2EventPayload {
    Headers(Response),
    BodyChunk(Piece),
    BodyEnd,
}

impl fmt::Debug for H2EventPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Headers(_) => f.debug_tuple("Headers").finish(),
            Self::BodyChunk(_) => f.debug_tuple("BodyChunk").finish(),
            Self::BodyEnd => write!(f, "BodyEnd"),
        }
    }
}

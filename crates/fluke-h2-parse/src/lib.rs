//! HTTP/2 parser
//!
//! HTTP/2 <https://httpwg.org/specs/rfc9113.html>
//! HTTP semantics <https://httpwg.org/specs/rfc9110.html>

use std::{fmt, io::Write, ops::RangeInclusive};

use byteorder::{BigEndian, WriteBytesExt};
use enum_repr::EnumRepr;

pub use enumflags2;
use enumflags2::{bitflags, BitFlags};

pub use nom;

use nom::{
    combinator::map,
    number::streaming::{be_u24, be_u32, be_u8},
    sequence::tuple,
    IResult,
};

use fluke_buffet::{Piece, Roll, RollMut};

/// This is sent by h2 clients after negotiating over ALPN, or when doing h2c.
pub const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub fn preface(i: Roll) -> IResult<Roll, ()> {
    let (i, _) = nom::bytes::streaming::tag(PREFACE)(i)?;
    Ok((i, ()))
}

pub trait IntoPiece {
    fn into_piece(self, scratch: &mut RollMut) -> std::io::Result<Piece>;
}

/// See https://httpwg.org/specs/rfc9113.html#FrameTypes
#[EnumRepr(type = "u8")]
#[derive(Debug, Clone, Copy)]
pub enum RawFrameType {
    Data = 0x00,
    Headers = 0x01,
    Priority = 0x02,
    RstStream = 0x03,
    Settings = 0x04,
    PushPromise = 0x05,
    Ping = 0x06,
    GoAway = 0x07,
    WindowUpdate = 0x08,
    Continuation = 0x09,
}

/// Typed flags for various frame types
#[derive(Debug, Clone, Copy)]
pub enum FrameType {
    Data(BitFlags<DataFlags>),
    Headers(BitFlags<HeadersFlags>),
    Priority,
    RstStream,
    Settings(BitFlags<SettingsFlags>),
    PushPromise,
    Ping(BitFlags<PingFlags>),
    GoAway,
    WindowUpdate,
    Continuation(BitFlags<ContinuationFlags>),
    Unknown(EncodedFrameType),
}

impl FrameType {
    /// Turn this [FrameType] into a [Frame]
    pub fn into_frame(self, stream_id: StreamId) -> Frame {
        Frame {
            frame_type: self,
            len: 0,
            reserved: 0,
            stream_id,
        }
    }
}

/// See https://httpwg.org/specs/rfc9113.html#DATA
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DataFlags {
    Padded = 0x08,
    EndStream = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#rfc.section.6.2
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HeadersFlags {
    Priority = 0x20,
    Padded = 0x08,
    EndHeaders = 0x04,
    EndStream = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#SETTINGS
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SettingsFlags {
    Ack = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#PING
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PingFlags {
    Ack = 0x01,
}

/// See https://httpwg.org/specs/rfc9113.html#CONTINUATION
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ContinuationFlags {
    EndHeaders = 0x04,
}

#[derive(Debug, Clone, Copy)]
pub struct EncodedFrameType {
    pub ty: u8,
    pub flags: u8,
}

impl EncodedFrameType {
    fn parse(i: Roll) -> IResult<Roll, Self> {
        let (i, (ty, flags)) = tuple((be_u8, be_u8))(i)?;
        Ok((i, Self { ty, flags }))
    }
}

impl From<(RawFrameType, u8)> for EncodedFrameType {
    fn from((ty, flags): (RawFrameType, u8)) -> Self {
        Self {
            ty: ty.repr(),
            flags,
        }
    }
}

impl FrameType {
    pub(crate) fn encode(self) -> EncodedFrameType {
        match self {
            FrameType::Data(f) => (RawFrameType::Data, f.bits()).into(),
            FrameType::Headers(f) => (RawFrameType::Headers, f.bits()).into(),
            FrameType::Priority => (RawFrameType::Priority, 0).into(),
            FrameType::RstStream => (RawFrameType::RstStream, 0).into(),
            FrameType::Settings(f) => (RawFrameType::Settings, f.bits()).into(),
            FrameType::PushPromise => (RawFrameType::PushPromise, 0).into(),
            FrameType::Ping(f) => (RawFrameType::Ping, f.bits()).into(),
            FrameType::GoAway => (RawFrameType::GoAway, 0).into(),
            FrameType::WindowUpdate => (RawFrameType::WindowUpdate, 0).into(),
            FrameType::Continuation(f) => (RawFrameType::Continuation, f.bits()).into(),
            FrameType::Unknown(ft) => ft,
        }
    }

    fn decode(ft: EncodedFrameType) -> Self {
        match RawFrameType::from_repr(ft.ty) {
            Some(ty) => match ty {
                RawFrameType::Data => {
                    FrameType::Data(BitFlags::<DataFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::Headers => {
                    FrameType::Headers(BitFlags::<HeadersFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::Priority => FrameType::Priority,
                RawFrameType::RstStream => FrameType::RstStream,
                RawFrameType::Settings => {
                    FrameType::Settings(BitFlags::<SettingsFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::PushPromise => FrameType::PushPromise,
                RawFrameType::Ping => {
                    FrameType::Ping(BitFlags::<PingFlags>::from_bits_truncate(ft.flags))
                }
                RawFrameType::GoAway => FrameType::GoAway,
                RawFrameType::WindowUpdate => FrameType::WindowUpdate,
                RawFrameType::Continuation => FrameType::Continuation(
                    BitFlags::<ContinuationFlags>::from_bits_truncate(ft.flags),
                ),
            },
            None => FrameType::Unknown(ft),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u32);

impl StreamId {
    /// Stream ID used for connection control frames
    pub const CONNECTION: Self = Self(0);

    /// Server-initiated streams have even IDs
    pub fn is_server_initiated(&self) -> bool {
        self.0 % 2 == 0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid stream id: {0}")]
pub struct StreamIdOutOfRange(u32);

impl TryFrom<u32> for StreamId {
    type Error = StreamIdOutOfRange;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value & 0x8000_0000 != 0 {
            Err(StreamIdOutOfRange(value))
        } else {
            Ok(Self(value))
        }
    }
}

impl fmt::Debug for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// See https://httpwg.org/specs/rfc9113.html#FrameHeader
pub struct Frame {
    pub frame_type: FrameType,
    pub reserved: u8,
    pub stream_id: StreamId,
    pub len: u32,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            frame_type: FrameType::Unknown(EncodedFrameType {
                ty: 0xff,
                flags: 0xff,
            }),
            reserved: 0,
            stream_id: StreamId::CONNECTION,
            len: 0,
        }
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.stream_id.0 == 0 {
            write!(f, "Conn:")?;
        } else {
            write!(f, "#{}:", self.stream_id.0)?;
        }

        let name = match &self.frame_type {
            FrameType::Data(_) => "Data",
            FrameType::Headers(_) => "Headers",
            FrameType::Priority => "Priority",
            FrameType::RstStream => "RstStream",
            FrameType::Settings(_) => "Settings",
            FrameType::PushPromise => "PushPromise",
            FrameType::Ping(_) => "Ping",
            FrameType::GoAway => "GoAway",
            FrameType::WindowUpdate => "WindowUpdate",
            FrameType::Continuation(_) => "Continuation",
            FrameType::Unknown(EncodedFrameType { ty, flags }) => {
                return write!(f, "UnknownFrame({:#x}, {:#x}, len={})", ty, flags, self.len)
            }
        };
        let mut s = f.debug_struct(name);

        if self.reserved != 0 {
            s.field("reserved", &self.reserved);
        }
        if self.len > 0 {
            s.field("len", &self.len);
        }

        // now write flags with DisplayDebug
        struct DisplayDebug<'a, D: fmt::Display>(&'a D);
        impl<'a, D: fmt::Display> fmt::Debug for DisplayDebug<'a, D> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self.0, f)
            }
        }

        // for all the variants with flags, add a flags field, of value
        // &DisplayDebug(flags)
        match &self.frame_type {
            FrameType::Data(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Headers(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Settings(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Ping(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            FrameType::Continuation(flags) => {
                if !flags.is_empty() {
                    s.field("flags", &DisplayDebug(flags));
                }
            }
            _ => {
                // muffin
            }
        }

        s.finish()
    }
}

impl Frame {
    /// Create a new frame with the given type and stream ID.
    pub fn new(frame_type: FrameType, stream_id: StreamId) -> Self {
        Self {
            frame_type,
            reserved: 0,
            stream_id,
            len: 0,
        }
    }

    /// Set the frame's length.
    pub fn with_len(mut self, len: u32) -> Self {
        self.len = len;
        self
    }

    /// Parse a frame from the given slice
    pub fn parse(i: Roll) -> IResult<Roll, Self> {
        let (i, (len, frame_type, (reserved, stream_id))) = tuple((
            be_u24,
            EncodedFrameType::parse,
            parse_reserved_and_stream_id,
        ))(i)?;

        let frame = Frame {
            frame_type: FrameType::decode(frame_type),
            reserved,
            stream_id,
            len,
        };
        Ok((i, frame))
    }

    pub fn write_into(self, mut w: impl std::io::Write) -> std::io::Result<()> {
        use byteorder::{BigEndian, WriteBytesExt};
        w.write_u24::<BigEndian>(self.len as _)?;
        let ft = self.frame_type.encode();
        w.write_u8(ft.ty)?;
        w.write_u8(ft.flags)?;
        w.write_all(&pack_reserved_and_stream_id(self.reserved, self.stream_id))?;

        Ok(())
    }

    /// Returns true if this frame is an ack
    pub fn is_ack(&self) -> bool {
        match self.frame_type {
            FrameType::Settings(flags) => flags.contains(SettingsFlags::Ack),
            FrameType::Ping(flags) => flags.contains(PingFlags::Ack),
            _ => false,
        }
    }

    /// Returns true if this frame has `EndHeaders` set
    pub fn is_end_headers(&self) -> bool {
        match self.frame_type {
            FrameType::Headers(flags) => flags.contains(HeadersFlags::EndHeaders),
            FrameType::Continuation(flags) => flags.contains(ContinuationFlags::EndHeaders),
            _ => false,
        }
    }

    /// Returns true if this frame has `EndStream` set
    pub fn is_end_stream(&self) -> bool {
        match self.frame_type {
            FrameType::Data(flags) => flags.contains(DataFlags::EndStream),
            FrameType::Headers(flags) => flags.contains(HeadersFlags::EndStream),
            _ => false,
        }
    }
}

impl IntoPiece for Frame {
    fn into_piece(self, mut scratch: &mut RollMut) -> std::io::Result<Piece> {
        debug_assert_eq!(scratch.len(), 0);
        self.write_into(&mut scratch)?;
        Ok(scratch.take_all().into())
    }
}

/// See https://httpwg.org/specs/rfc9113.html#FrameHeader - the first bit
/// is reserved, and the rest is a 31-bit stream id
pub fn parse_bit_and_u31(i: Roll) -> IResult<Roll, (u8, u32)> {
    // first, parse a u32:
    let (i, x) = be_u32(i)?;

    let bit = (x >> 31) as u8;
    let val = x & 0x7FFF_FFFF;

    Ok((i, (bit, val)))
}

fn parse_reserved_and_stream_id(i: Roll) -> IResult<Roll, (u8, StreamId)> {
    parse_bit_and_u31(i).map(|(i, (reserved, stream_id))| (i, (reserved, StreamId(stream_id))))
}

/// Pack a bit and a u31 into a 4-byte array (big-endian)
pub fn pack_bit_and_u31(bit: u8, val: u32) -> [u8; 4] {
    // assert val is in range
    assert_eq!(val & 0x7FFF_FFFF, val, "val is too large: {val:x}");

    // assert bit is in range
    assert_eq!(bit & 0x1, bit, "bit should be 0 or 1: {bit:x}");

    // pack
    let mut bytes = val.to_be_bytes();
    if bit != 0 {
        bytes[0] |= 0x80;
    }

    bytes
}

pub fn pack_reserved_and_stream_id(reserved: u8, stream_id: StreamId) -> [u8; 4] {
    pack_bit_and_u31(reserved, stream_id.0)
}

#[test]
fn test_pack_and_parse_bit_and_u31() {
    // Test round-tripping through parse_bit_and_u31 and pack_bit_and_u31
    let test_cases = [
        (0, 0),
        (1, 0),
        (0, 1),
        (1, 1),
        (0, 0x7FFF_FFFF),
        (1, 0x7FFF_FFFF),
    ];

    let mut roll = RollMut::alloc().unwrap();
    for &(bit, number) in &test_cases {
        let packed = pack_bit_and_u31(bit, number);
        roll.reserve_at_least(4).unwrap();
        roll.put(&packed[..]).unwrap();
        let (_, (parsed_bit, parsed_number)) = parse_bit_and_u31(roll.take_all()).unwrap();
        assert_eq!(dbg!(bit), dbg!(parsed_bit));
        assert_eq!(dbg!(number), dbg!(parsed_number));
    }
}

#[test]
#[should_panic(expected = "bit should be 0 or 1: 2")]
fn test_pack_bit_and_u31_panic_not_a_bit() {
    pack_bit_and_u31(2, 0);
}

#[test]
#[should_panic(expected = "val is too large: 80000000")]
fn test_pack_bit_and_u31_panic_val_too_large() {
    pack_bit_and_u31(0, 1 << 31);
}

// cf. https://httpwg.org/specs/rfc9113.html#HEADERS
#[derive(Debug)]
pub struct PrioritySpec {
    pub exclusive: bool,
    pub stream_dependency: StreamId,
    // 0-255 => 1-256
    pub weight: u8,
}

impl PrioritySpec {
    pub fn parse(i: Roll) -> IResult<Roll, Self> {
        map(
            tuple((parse_reserved_and_stream_id, be_u8)),
            |((exclusive, stream_dependency), weight)| Self {
                exclusive: exclusive != 0,
                stream_dependency,
                weight,
            },
        )(i)
    }
}

impl IntoPiece for PrioritySpec {
    fn into_piece(self, scratch: &mut RollMut) -> std::io::Result<Piece> {
        let roll = scratch
            .put_to_roll(5, |mut slice| {
                let reserved_and_stream_id =
                    pack_reserved_and_stream_id(self.exclusive as u8, self.stream_dependency);
                slice.write_all(&reserved_and_stream_id)?;
                slice.write_u8(self.weight)?;
                Ok(())
            })
            .unwrap();
        Ok(roll.into())
    }
}

#[derive(Clone, Copy)]
pub struct ErrorCode(pub u32);

impl ErrorCode {
    /// Returns the underlying u32
    pub fn as_repr(self) -> u32 {
        self.0
    }
}

impl fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match KnownErrorCode::from_repr(self.0) {
            Some(e) => fmt::Debug::fmt(&e, f),
            None => write!(f, "ErrorCode(0x{:02x})", self.0),
        }
    }
}

impl From<KnownErrorCode> for ErrorCode {
    fn from(e: KnownErrorCode) -> Self {
        Self(e as u32)
    }
}

#[EnumRepr(type = "u32")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownErrorCode {
    /// The associated condition is not a result of an error. For example, a
    /// GOAWAY might include this code to indicate graceful shutdown of a
    /// connection.
    NoError = 0x00,

    /// The endpoint detected an unspecific protocol error. This error is for
    /// use when a more specific error code is not available.
    ProtocolError = 0x01,

    /// The endpoint encountered an unexpected internal error.
    InternalError = 0x02,

    /// The endpoint detected that its peer violated the flow-control protocol.
    FlowControlError = 0x03,

    /// The endpoint sent a SETTINGS frame but did not receive a response in a
    /// timely manner. See Section 6.5.3 ("Settings Synchronization").
    /// https://httpwg.org/specs/rfc9113.html#SettingsSync
    SettingsTimeout = 0x04,

    /// The endpoint received a frame after a stream was half-closed.
    StreamClosed = 0x05,

    /// The endpoint received a frame with an invalid size.
    FrameSizeError = 0x06,

    /// The endpoint refused the stream prior to performing any application
    /// processing (see Section 8.7 for details).
    /// https://httpwg.org/specs/rfc9113.html#Reliability
    RefusedStream = 0x07,

    /// The endpoint uses this error code to indicate that the stream is no
    /// longer needed.
    Cancel = 0x08,

    /// The endpoint is unable to maintain the field section compression context
    /// for the connection.
    CompressionError = 0x09,

    /// The connection established in response to a CONNECT request (Section
    /// 8.5) was reset or abnormally closed.
    /// https://httpwg.org/specs/rfc9113.html#CONNECT
    ConnectError = 0x0a,

    /// The endpoint detected that its peer is exhibiting a behavior that might
    /// be generating excessive load.
    EnhanceYourCalm = 0x0b,

    /// The underlying transport has properties that do not meet minimum
    /// security requirements (see Section 9.2).
    /// https://httpwg.org/specs/rfc9113.html#TLSUsage
    InadequateSecurity = 0x0c,

    /// The endpoint requires that HTTP/1.1 be used instead of HTTP/2.
    Http1_1Required = 0x0d,
}

impl TryFrom<ErrorCode> for KnownErrorCode {
    type Error = ();

    fn try_from(e: ErrorCode) -> Result<Self, Self::Error> {
        KnownErrorCode::from_repr(e.0).ok_or(())
    }
}

/// cf. https://httpwg.org/specs/rfc9113.html#SettingValues
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// This setting allows the sender to inform the remote endpoint of the
    /// maximum size of the compression table used to decode field blocks, in
    /// units of octets. The encoder can select any size equal to or less than
    /// this value by using signaling specific to the compression format inside
    /// a field block (see [COMPRESSION]). The initial value is 4,096 octets.
    pub header_table_size: u32,

    /// This setting can be used to enable or disable server push. A server MUST
    /// NOT send a PUSH_PROMISE frame if it receives this parameter set to a
    /// value of 0; see Section 8.4. A client that has both set this parameter
    /// to 0 and had it acknowledged MUST treat the receipt of a PUSH_PROMISE
    /// frame as a connection error (Section 5.4.1) of type PROTOCOL_ERROR.
    ///
    /// The initial value of SETTINGS_ENABLE_PUSH is 1. For a client, this value
    /// indicates that it is willing to receive PUSH_PROMISE frames. For a
    /// server, this initial value has no effect, and is equivalent to the value
    /// 0. Any value other than 0 or 1 MUST be treated as a connection error
    /// (Section 5.4.1) of type PROTOCOL_ERROR.
    ///
    /// A server MUST NOT explicitly set this value to 1. A server MAY choose to
    /// omit this setting when it sends a SETTINGS frame, but if a server does
    /// include a value, it MUST be 0. A client MUST treat receipt of a SETTINGS
    /// frame with SETTINGS_ENABLE_PUSH set to 1 as a connection error (Section
    /// 5.4.1) of type PROTOCOL_ERROR.
    pub enable_push: bool,

    /// This setting indicates the maximum number of concurrent streams that the
    /// sender will allow. This limit is directional: it applies to the number
    /// of streams that the sender permits the receiver to create.
    /// Initially, there is no limit to this value. It is recommended that
    /// this value be no smaller than 100, so as to not unnecessarily limit
    /// parallelism.
    ///
    /// A value of 0 for SETTINGS_MAX_CONCURRENT_STREAMS SHOULD NOT be treated
    /// as special by endpoints. A zero value does prevent the creation of
    /// new streams; however, this can also happen for any limit that is
    /// exhausted with active streams. Servers SHOULD only set a zero value
    /// for short durations; if a server does not wish to accept requests,
    /// closing the connection is more appropriate.
    pub max_concurrent_streams: Option<u32>,

    /// This setting indicates the sender's initial window size (in units of
    /// octets) for stream-level flow control. The initial value is 2^16-1
    /// (65,535) octets.
    ///
    /// This setting affects the window size of all streams (see Section 6.9.2).
    ///
    /// Values above the maximum flow-control window size of 2^31-1 MUST be
    /// treated as a connection error (Section 5.4.1) of type
    /// FLOW_CONTROL_ERROR.
    pub initial_window_size: u32,

    /// This setting indicates the size of the largest frame payload that the
    /// sender is willing to receive, in units of octets.
    ///
    /// The initial value is 2^14 (16,384) octets. The value advertised by an
    /// endpoint MUST be between this initial value and the maximum allowed
    /// frame size (2^24-1 or 16,777,215 octets), inclusive. Values outside
    /// this range MUST be treated as a connection error (Section 5.4.1) of
    /// type PROTOCOL_ERROR.
    pub max_frame_size: u32,

    /// This advisory setting informs a peer of the maximum field section size
    /// that the sender is prepared to accept, in units of octets. The value is
    /// based on the uncompressed size of field lines, including the length of
    /// the name and value in units of octets plus an overhead of 32 octets for
    /// each field line.
    ///
    /// For any given request, a lower limit than what is advertised MAY be
    /// enforced. The initial value of this setting is unlimited.
    pub max_header_list_size: u32,
}

impl Default for Settings {
    fn default() -> Self {
        // cf. https://httpwg.org/specs/rfc9113.html#SettingValues
        Self {
            header_table_size: 4096,
            enable_push: false,
            max_concurrent_streams: Some(100),
            initial_window_size: (1 << 16) - 1,
            max_frame_size: (1 << 14),
            max_header_list_size: 0,
        }
    }
}

impl Settings {
    /// Apply a setting to the current settings, returning an error if the
    /// setting is invalid.
    pub fn apply(&mut self, code: Setting, value: u32) -> Result<(), SettingsError> {
        match code {
            Setting::HeaderTableSize => {
                self.header_table_size = value;
            }
            Setting::EnablePush => match value {
                0 => self.enable_push = false,
                1 => self.enable_push = true,
                _ => return Err(SettingsError::InvalidEnablePushValue { actual: value }),
            },
            Setting::MaxConcurrentStreams => {
                self.max_concurrent_streams = Some(value);
            }
            Setting::InitialWindowSize => {
                if value > Self::MAX_INITIAL_WINDOW_SIZE {
                    return Err(SettingsError::InitialWindowSizeTooLarge { actual: value });
                }
                self.initial_window_size = value;
            }
            Setting::MaxFrameSize => {
                if !Self::MAX_FRAME_SIZE_ALLOWED_RANGE.contains(&value) {
                    return Err(SettingsError::SettingsMaxFrameSizeInvalid { actual: value });
                }
                self.max_frame_size = value;
            }
            Setting::MaxHeaderListSize => {
                self.max_header_list_size = value;
            }
        }

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SettingsError {
    #[error("ENABLE_PUSH setting is supposed to be either 0 or 1, got {actual}")]
    InvalidEnablePushValue { actual: u32 },

    #[error("bad INITIAL_WINDOW_SIZE value {actual}, should be than or equal to 2^31-1")]
    InitialWindowSizeTooLarge { actual: u32 },

    #[error(
        "bad SETTINGS_MAX_FRAME_SIZE value {actual}, should be between 2^14 and 2^24-1 inclusive"
    )]
    SettingsMaxFrameSizeInvalid { actual: u32 },
}

#[EnumRepr(type = "u16")]
#[derive(Debug, Clone, Copy)]
pub enum Setting {
    HeaderTableSize = 0x01,
    EnablePush = 0x02,
    MaxConcurrentStreams = 0x03,
    InitialWindowSize = 0x04,
    MaxFrameSize = 0x05,
    MaxHeaderListSize = 0x06,
}

impl Settings {
    pub const MAX_INITIAL_WINDOW_SIZE: u32 = (1 << 31) - 1;
    pub const MAX_FRAME_SIZE_ALLOWED_RANGE: RangeInclusive<u32> = (1 << 14)..=((1 << 24) - 1);

    /// Parse a series of settings from a buffer, calls the callback for each
    /// known setting found.
    ///
    /// Unknown settings are ignored.
    ///
    /// Panics if the buf isn't a multiple of 6 bytes.
    pub fn parse<E>(
        buf: &[u8],
        mut callback: impl FnMut(Setting, u32) -> Result<(), E>,
    ) -> Result<(), E> {
        assert!(
            buf.len() % 6 == 0,
            "buffer length must be a multiple of 6 bytes"
        );

        for chunk in buf.chunks_exact(6) {
            let id = u16::from_be_bytes([chunk[0], chunk[1]]);
            let value = u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]);
            match Setting::from_repr(id) {
                None => {}
                Some(id) => {
                    callback(id, value)?;
                }
            }
        }

        Ok(())
    }
}

pub struct SettingPairs<'a>(pub &'a [(Setting, u32)]);

impl<'a> From<&'a [(Setting, u32)]> for SettingPairs<'a> {
    fn from(value: &'a [(Setting, u32)]) -> Self {
        Self(value)
    }
}

impl<const N: usize> From<&'static [(Setting, u32); N]> for SettingPairs<'static> {
    fn from(value: &'static [(Setting, u32); N]) -> Self {
        Self(value)
    }
}

impl<'a> IntoPiece for SettingPairs<'a> {
    fn into_piece(self, scratch: &mut RollMut) -> std::io::Result<Piece> {
        let roll = scratch
            .put_to_roll(self.0.len() * 6, |mut slice| {
                for (id, value) in self.0.iter() {
                    slice.write_u16::<BigEndian>(*id as u16)?;
                    slice.write_u32::<BigEndian>(*value)?;
                }
                Ok(())
            })
            .unwrap();
        Ok(roll.into())
    }
}

/// Payload for a GOAWAY frame
pub struct GoAway {
    pub last_stream_id: StreamId,
    pub error_code: ErrorCode,
    pub additional_debug_data: Piece,
}

impl IntoPiece for GoAway {
    fn into_piece(self, scratch: &mut RollMut) -> std::io::Result<Piece> {
        let roll = scratch
            .put_to_roll(8 + self.additional_debug_data.len(), |mut slice| {
                slice.write_u32::<BigEndian>(self.last_stream_id.0)?;
                slice.write_u32::<BigEndian>(self.error_code.0)?;
                slice.write_all(&self.additional_debug_data[..])?;

                Ok(())
            })
            .unwrap();
        Ok(roll.into())
    }
}

impl GoAway {
    pub fn parse(i: Roll) -> IResult<Roll, Self> {
        let (rest, (last_stream_id, error_code)) = tuple((be_u32, be_u32))(i)?;

        let i = Roll::empty();
        Ok((
            i,
            Self {
                last_stream_id: StreamId(last_stream_id),
                error_code: ErrorCode(error_code),
                additional_debug_data: rest.into(),
            },
        ))
    }
}

/// Payload for a RST_STREAM frame
pub struct RstStream {
    pub error_code: ErrorCode,
}

impl IntoPiece for RstStream {
    fn into_piece(self, scratch: &mut RollMut) -> std::io::Result<Piece> {
        let roll = scratch
            .put_to_roll(4, |mut slice| {
                slice.write_u32::<BigEndian>(self.error_code.0)?;
                Ok(())
            })
            .unwrap();
        Ok(roll.into())
    }
}

impl RstStream {
    pub fn parse(i: Roll) -> IResult<Roll, Self> {
        let (rest, error_code) = be_u32(i)?;
        Ok((
            rest,
            Self {
                error_code: ErrorCode(error_code),
            },
        ))
    }
}

/// Payload for a WINDOW_UPDATE frame
#[derive(Debug, Clone, Copy)]
pub struct WindowUpdate {
    pub reserved: u8,
    pub increment: u32,
}

impl IntoPiece for WindowUpdate {
    fn into_piece(self, scratch: &mut RollMut) -> std::io::Result<Piece> {
        let roll = scratch
            .put_to_roll(4, |mut slice| {
                let packed = pack_bit_and_u31(self.reserved, self.increment);
                slice.write_all(&packed)?;
                Ok(())
            })
            .unwrap();
        Ok(roll.into())
    }
}

impl WindowUpdate {
    pub fn parse(i: Roll) -> IResult<Roll, Self> {
        let (rest, (reserved, increment)) = parse_bit_and_u31(i)?;
        Ok((
            rest,
            Self {
                reserved,
                increment,
            },
        ))
    }
}

impl<T> IntoPiece for T
where
    Piece: From<T>,
{
    fn into_piece(self, _scratch: &mut RollMut) -> std::io::Result<Piece> {
        Ok(self.into())
    }
}

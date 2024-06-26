//! Types for performing vectored I/O.

use http::header::HeaderName;
use std::{
    collections::VecDeque,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    rc::Rc,
    str::Utf8Error,
};

use crate::{Roll, RollStr};

/// A piece of data (arbitrary bytes) with a stable address, suitable for
/// passing to the kernel (io_uring writes).
#[derive(Clone)]
pub enum Piece {
    Full {
        core: PieceCore,
    },
    Slice {
        core: PieceCore,
        start: usize,
        len: usize,
    },
}

impl<T: AsRef<[u8]>> PartialEq<T> for Piece {
    fn eq(&self, other: &T) -> bool {
        let s: &[u8] = self.as_ref();
        s.eq(other.as_ref())
    }
}

impl Eq for Piece {}

impl Hash for Piece {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl Piece {
    /// Returns an empty piece
    pub fn empty() -> Self {
        Self::Full {
            core: PieceCore::Static(&[]),
        }
    }
}

#[derive(Clone, Hash)]
pub enum PieceCore {
    Static(&'static [u8]),
    Vec(Rc<Vec<u8>>),
    Roll(Roll),
    HeaderName(HeaderName),
}

impl<T> From<T> for Piece
where
    T: Into<PieceCore>,
{
    #[inline(always)]
    fn from(t: T) -> Self {
        Piece::Full { core: t.into() }
    }
}

impl<const N: usize> From<&'static [u8; N]> for PieceCore {
    #[inline(always)]
    fn from(slie: &'static [u8; N]) -> Self {
        PieceCore::Static(slie)
    }
}

impl From<&'static [u8]> for PieceCore {
    #[inline(always)]
    fn from(slice: &'static [u8]) -> Self {
        PieceCore::Static(slice)
    }
}

impl From<&'static str> for PieceCore {
    #[inline(always)]
    fn from(slice: &'static str) -> Self {
        PieceCore::Static(slice.as_bytes())
    }
}

impl From<Vec<u8>> for PieceCore {
    #[inline(always)]
    fn from(vec: Vec<u8>) -> Self {
        PieceCore::Vec(Rc::new(vec))
    }
}

impl From<Roll> for PieceCore {
    #[inline(always)]
    fn from(roll: Roll) -> Self {
        PieceCore::Roll(roll)
    }
}

impl From<()> for PieceCore {
    #[inline(always)]
    fn from(_empty: ()) -> Self {
        PieceCore::Static(&[])
    }
}

impl From<PieceStr> for Piece {
    fn from(s: PieceStr) -> Self {
        s.piece
    }
}

impl From<HeaderName> for PieceCore {
    #[inline(always)]
    fn from(name: HeaderName) -> Self {
        PieceCore::HeaderName(name)
    }
}

impl Deref for PieceCore {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl Deref for Piece {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for PieceCore {
    fn as_ref(&self) -> &[u8] {
        match self {
            PieceCore::Static(slice) => slice,
            PieceCore::Vec(vec) => vec.as_ref(),
            PieceCore::Roll(roll) => roll.as_ref(),
            PieceCore::HeaderName(name) => name.as_str().as_bytes(),
        }
    }
}

impl Piece {
    fn core(&self) -> &PieceCore {
        match self {
            Piece::Full { core } => core,
            Piece::Slice { core, .. } => core,
        }
    }

    /// Split the piece into two at the given index.
    /// The original piece will be consumed.
    /// Returns a tuple of the two pieces.
    pub fn split_at(self, middle: usize) -> (Self, Self) {
        let len = self.len();
        assert!(middle <= len);

        match self {
            Piece::Full { core } => (
                Self::Slice {
                    core: core.clone(),
                    start: 0,
                    len: middle,
                },
                Self::Slice {
                    core,
                    start: middle,
                    len: len - middle,
                },
            ),
            Piece::Slice { core, start, len } => (
                Self::Slice {
                    core: core.clone(),
                    start,
                    len: middle,
                },
                Self::Slice {
                    core,
                    start: start + middle,
                    len: len - middle,
                },
            ),
        }
    }
}

impl AsRef<[u8]> for Piece {
    fn as_ref(&self) -> &[u8] {
        let ptr = self.core().as_ref();
        if let Piece::Slice { start, len, .. } = self {
            &ptr[*start..][..*len]
        } else {
            ptr
        }
    }
}

impl Piece {
    // Decode as utf-8 (owned)
    pub fn to_str(self) -> Result<PieceStr, Utf8Error> {
        _ = std::str::from_utf8(&self[..])?;
        Ok(PieceStr { piece: self })
    }

    /// Convert to [PieceStr].
    ///
    /// # Safety
    /// UB if not utf-8. Typically only used in parsers.
    pub unsafe fn to_string_unchecked(self) -> PieceStr {
        PieceStr { piece: self }
    }
}

impl Piece {
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A list of [Piece], suitable for issuing vectored writes via io_uring.
#[derive(Default)]
pub struct PieceList {
    // note: we can't use smallvec here, because the address of
    // the piece list must be stable for the kernel to take
    // ownership of it.
    //
    // we could however do our own memory pooling.
    pub(crate) pieces: VecDeque<Piece>,
}

impl PieceList {
    /// Create a new piece list with a single chunk
    pub fn single(piece: impl Into<Piece>) -> Self {
        Self {
            pieces: [piece.into()].into(),
        }
    }

    /// Add a single chunk to the back of the list
    pub fn push_back(&mut self, chunk: impl Into<Piece>) {
        let chunk = chunk.into();
        if !chunk.is_empty() {
            self.pieces.push_back(chunk);
        }
    }

    /// Add a single chunk to the front of the list
    pub fn push_front(&mut self, chunk: impl Into<Piece>) {
        let chunk = chunk.into();
        if !chunk.is_empty() {
            self.pieces.push_front(chunk);
        }
    }

    /// Add a single chunk to the back list and return self
    pub fn followed_by(mut self, chunk: impl Into<Piece>) -> Self {
        self.push_back(chunk);
        self
    }

    /// Add a single chunk to the front of the list and return self
    pub fn preceded_by(mut self, chunk: impl Into<Piece>) -> Self {
        self.push_front(chunk);
        self
    }

    /// Returns total length
    pub fn len(&self) -> usize {
        self.pieces.iter().map(|c| c.len()).sum()
    }

    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pieces.is_empty() || self.len() == 0
    }

    pub fn clear(&mut self) {
        self.pieces.clear();
    }

    pub fn into_vec_deque(self) -> VecDeque<Piece> {
        self.pieces
    }
}

impl From<VecDeque<Piece>> for PieceList {
    fn from(pieces: VecDeque<Piece>) -> Self {
        Self { pieces }
    }
}
impl From<PieceList> for VecDeque<Piece> {
    fn from(list: PieceList) -> Self {
        list.pieces
    }
}

/// A piece of data with a stable address that's _also_
/// valid utf-8.
#[derive(Clone)]
pub struct PieceStr {
    piece: Piece,
}

impl PartialEq for PieceStr {
    fn eq(&self, other: &Self) -> bool {
        self.piece == other.piece
    }
}

impl Eq for PieceStr {}

impl fmt::Debug for PieceStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        fmt::Debug::fmt(&self[..], f)
    }
}

impl fmt::Display for PieceStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(self)
    }
}

impl Deref for PieceStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe { std::str::from_utf8_unchecked(&self.piece) }
    }
}

impl AsRef<str> for PieceStr {
    fn as_ref(&self) -> &str {
        self
    }
}

impl PieceStr {
    /// Returns the underlying bytes (borrowed)
    pub fn as_bytes(&self) -> &[u8] {
        self.piece.as_ref()
    }

    /// Returns the underlying bytes (owned)
    pub fn into_inner(self) -> Piece {
        self.piece
    }
}

impl From<&'static str> for PieceStr {
    fn from(s: &'static str) -> Self {
        PieceStr {
            piece: PieceCore::Static(s.as_bytes()).into(),
        }
    }
}

impl From<String> for PieceStr {
    fn from(s: String) -> Self {
        PieceStr {
            piece: PieceCore::Vec(Rc::new(s.into_bytes())).into(),
        }
    }
}

impl From<RollStr> for PieceStr {
    fn from(s: RollStr) -> Self {
        PieceStr {
            piece: PieceCore::Roll(s.into_inner()).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Piece, PieceCore};

    #[test]
    fn test_slice() {
        // test that slicing works correctly for a
        // piece made from a &'static u8
        let piece: Piece = PieceCore::Static("französisch".as_bytes()).into();
        // split so that "l" is "franz"
        let (first_name, last_name) = piece.split_at(5);
        assert_eq!(&first_name[..], "franz".as_bytes());
        assert_eq!(&last_name[..], "ösisch".as_bytes());

        // test edge cases, zero-length left
        let piece: Piece = PieceCore::Static("französisch".as_bytes()).into();
        let (first_name, last_name) = piece.split_at(0);
        assert_eq!(&first_name[..], "".as_bytes());
        assert_eq!(&last_name[..], "französisch".as_bytes());

        // test edge cases, zero-length right
        let piece: Piece = PieceCore::Static("französisch".as_bytes()).into();
        let (first_name, last_name) = piece.split_at(12);
        assert_eq!(&first_name[..], "französisch".as_bytes());
        assert_eq!(&last_name[..], "".as_bytes());

        // edge case: empty piece being split into two
        let piece: Piece = PieceCore::Static(b"").into();
        let (first_name, last_name) = piece.split_at(0);
        assert_eq!(&first_name[..], "".as_bytes());
        assert_eq!(&last_name[..], "".as_bytes());
    }
}

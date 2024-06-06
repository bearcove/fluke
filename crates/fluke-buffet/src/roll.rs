use std::{
    borrow::Cow,
    cell::UnsafeCell,
    fmt::{Debug, Formatter},
    hash::{Hash, Hasher},
    iter::Enumerate,
    ops::{Bound, Deref, RangeBounds},
    rc::Rc,
    str::Utf8Error,
};

use crate::{io::ReadOwned, Error, IoBufMut};
use nom::{
    Compare, CompareResult, FindSubstring, InputIter, InputLength, InputTake, InputTakeAtPosition,
    Needed, Slice,
};
use tracing::trace;

use crate::{Buf, BufMut, BUF_SIZE};

type Result<T, E = crate::Error> = std::result::Result<T, E>;

/// A "rolling buffer". Uses either one [BufMut] or a `Box<[u8]>` for storage.
/// This buffer never grows, but it can be split, and it can be reallocated so
/// it regains its initical capacity, minus the length of the filled part.
pub struct RollMut {
    storage: StorageMut,
    len: u32,
}

enum StorageMut {
    Buf(BufMut),
    Box(BoxStorage),
}

impl Debug for StorageMut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Buf(bm) => f
                .debug_struct("Buf")
                .field("index", &bm.index)
                .field("off", &bm.off)
                .field("len", &bm.len)
                .finish(),
            Self::Box(bs) => f
                .debug_struct("Box")
                .field("buf", &bs.buf)
                .field("off", &bs.off)
                .finish(),
        }
    }
}

impl StorageMut {
    #[inline(always)]
    fn off(&self) -> u32 {
        match self {
            StorageMut::Buf(b) => b.off as _,
            StorageMut::Box(b) => b.off,
        }
    }

    #[inline(always)]
    fn cap(&self) -> usize {
        match self {
            StorageMut::Buf(_) => BUF_SIZE as usize,
            StorageMut::Box(b) => b.cap(),
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            StorageMut::Buf(b) => b.len(),
            StorageMut::Box(b) => b.len(),
        }
    }

    unsafe fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            StorageMut::Buf(b) => b.as_mut_ptr(),
            StorageMut::Box(b) => b.as_mut_ptr(),
        }
    }
}

#[derive(Clone)]
struct BoxStorage {
    buf: Rc<UnsafeCell<Box<[u8]>>>,
    off: u32,
}

impl BoxStorage {
    #[inline(always)]
    fn len(&self) -> usize {
        let buf = self.buf.get();
        let len = unsafe { (*buf).len() };
        len - self.off as usize
    }

    unsafe fn as_mut_ptr(&self) -> *mut u8 {
        let buf = self.buf.get();
        (*buf).as_mut_ptr().add(self.off as usize)
    }

    /// Returns a slice of bytes into this buffer, of the specified length
    /// Panics if the length is larger than the buffer.
    fn slice(&self, len: u32) -> &[u8] {
        let buf = self.buf.get();
        unsafe { &(*buf)[self.off as usize..][..len as usize] }
    }

    /// Returns a mutable slice of bytes into this buffer, of the specified
    /// length Panics if the length is larger than the buffer.
    fn slice_mut(&mut self, len: u32) -> &mut [u8] {
        let buf = self.buf.get();
        unsafe { &mut (*buf)[self.off as usize..][..len as usize] }
    }

    fn cap(&self) -> usize {
        let buf = self.buf.get();
        unsafe { (*buf).len() }
    }
}

impl RollMut {
    /// Allocate, using a single [BufMut] for storage.
    pub fn alloc() -> Result<Self> {
        Ok(Self {
            storage: StorageMut::Buf(BufMut::alloc()?),
            len: 0,
        })
    }

    /// Double the capacity of this buffer by reallocating it, copying the
    /// filled part into the new buffer. This method always uses a `Box<[u8]>`
    /// for storage.
    ///
    /// This method is somewhat expensive.
    pub fn grow(&mut self) {
        let old_cap = self.storage.cap();
        let new_cap = old_cap * 2;

        tracing::trace!("growing buffer from {} to {}", old_cap, new_cap);

        // TODO: optimize via `MaybeUninit`?
        let b = vec![0; new_cap].into_boxed_slice();
        let mut bs = BoxStorage {
            buf: Rc::new(UnsafeCell::new(b)),
            off: 0,
        };
        let dst_slice = bs.slice_mut(self.len() as u32);
        dst_slice.copy_from_slice(&self[..]);
        let next_storage = StorageMut::Box(bs);

        self.storage = next_storage;
    }

    /// Reallocates the backing storage for this buffer, copying the filled
    /// portion into it. Panics if `len() == storage_size()`, in which case
    /// reallocating won't do much good
    ///
    /// Also panics if we're using buf storage and the offset is zero
    /// (which means reallocating would not free up any space)
    pub fn compact(&mut self) -> Result<()> {
        assert!(self.len() != self.storage_size());
        tracing::trace!("compacting");

        let next_storage = match &self.storage {
            StorageMut::Buf(bm) => {
                assert!(bm.off != 0);
                let mut next_b = BufMut::alloc()?;
                next_b[..self.len()].copy_from_slice(&self[..]);
                StorageMut::Buf(next_b)
            }
            StorageMut::Box(b) => {
                tracing::trace!("reallocating, storage is box");
                if self.len() > BUF_SIZE as usize {
                    // TODO: optimize via `MaybeUninit`?
                    let mut next_b = vec![0; b.cap()].into_boxed_slice();
                    next_b[..self.len()].copy_from_slice(&self[..]);
                    let next_b = BoxStorage {
                        buf: Rc::new(UnsafeCell::new(next_b)),
                        off: 0,
                    };
                    StorageMut::Box(next_b)
                } else {
                    let mut next_b = BufMut::alloc()?;
                    next_b[..self.len()].copy_from_slice(&self[..]);
                    StorageMut::Buf(next_b)
                }
            }
        };

        self.storage = next_storage;

        Ok(())
    }

    /// Reserve more capacity for this buffer if this buffer is full.
    /// If this buffer's size matches the underlying storage size,
    /// this is equivalent to `grow`. Otherwise, it's equivalent
    /// to `realloc`.
    pub fn reserve(&mut self) -> Result<()> {
        if self.len() < self.cap() {
            tracing::trace!("reserve: len < cap, no need to reserve anything");
            return Ok(());
        }

        if self.storage.off() > 0 {
            // let's try to compact first
            trace!(len = %self.len(), cap = %self.cap(), storage_size = %self.storage_size(), "in reserve: reallocating");
            self.compact()?
        } else {
            trace!(len = %self.len(), cap = %self.cap(), storage_size = %self.storage_size(), "in reserve: growing");
            self.grow()
        }

        Ok(())
    }

    /// Make sure we can hold "request_len"
    pub fn reserve_at_least(&mut self, requested_len: usize) -> Result<()> {
        if requested_len <= self.cap() {
            tracing::trace!(%requested_len, cap = %self.cap(), "reserve_at_least: requested_len <= cap, no need to compact");
            return Ok(());
        }

        if self.storage.off() > 0 && requested_len <= (BUF_SIZE as usize - self.len()) {
            // we can compact the filled portion!
            self.compact()?;
        } else {
            // we need to allocate box storage of the right size
            let new_storage_size =
                std::cmp::max(self.storage_size() * 2, requested_len + self.len());
            let mut new_b = vec![0u8; new_storage_size].into_boxed_slice();
            // copy the filled portion
            new_b[..self.len()].copy_from_slice(&self[..]);
            self.storage = StorageMut::Box(BoxStorage {
                buf: Rc::new(UnsafeCell::new(new_b)),
                off: 0,
            });
        }

        assert!(self.cap() >= requested_len);
        Ok(())
    }

    /// The length (filled portion) of this buffer, that can be read
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns true if this is empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The capacity of this buffer, that can be written to
    #[inline(always)]
    pub fn cap(&self) -> usize {
        self.storage.len() - self.len as usize
    }

    /// The size of the underlying storage (to know whether
    /// to reallocate or grow)
    pub fn storage_size(&self) -> usize {
        self.storage.cap()
    }

    /// Read at most `limit` bytes from [ReadOwned] into this buffer.
    ///
    /// This method takes ownership of `self` because it submits an io_uring
    /// operation, where the kernel owns the read buffer - the only way to
    /// gain ownership of `self` again is to complete the read operation.
    ///
    /// Panics if `cap` is zero
    pub async fn read_into(
        self,
        limit: usize,
        r: &mut impl ReadOwned,
    ) -> (std::io::Result<usize>, Self) {
        let read_cap = std::cmp::min(limit, self.cap());
        assert!(read_cap > 0, "refusing to do empty read");
        let read_off = self.len;

        tracing::trace!(%read_off, %read_cap, storage = ?self.storage, len = %self.len, "read_into in progress...");
        let read_into = ReadInto {
            buf: self,
            off: read_off,
            cap: read_cap.try_into().unwrap(),
        };
        let (res, mut read_into) = r.read_owned(read_into).await;
        if let Ok(n) = &res {
            tracing::trace!("read_into got {} bytes", *n);
            read_into.buf.len += *n as u32;
        } else {
            tracing::trace!("read_into failed: {:?}", res);
        }
        (res, read_into.buf)
    }

    /// Put a slice into this buffer, fails if the slice doesn't fit in the
    /// buffer's capacity
    pub fn put(&mut self, s: impl AsRef<[u8]>) -> Result<()> {
        let s = s.as_ref();

        let len = s.len();
        if len > self.cap() {
            return Err(Error::DoesNotFit);
        }
        unsafe {
            let ptr = self.storage.as_mut_ptr().add(self.len as usize);
            std::ptr::copy_nonoverlapping(s.as_ptr(), ptr, len);
        }
        let u32_len: u32 = len.try_into().unwrap();
        self.len += u32_len;
        Ok(())
    }

    /// Put data into this RollMut with a closure. Panics if `len > self.cap()`
    pub fn put_with<T>(&mut self, len: usize, f: impl FnOnce(&mut [u8]) -> Result<T>) -> Result<T> {
        assert!(len <= self.cap());

        let u32_len: u32 = len.try_into().unwrap();
        let slice = unsafe {
            std::slice::from_raw_parts_mut(self.storage.as_mut_ptr().add(self.len as usize), len)
        };
        let res = f(slice);
        if res.is_ok() {
            self.len += u32_len;
        }
        res
    }

    /// Assert that this RollMut isn't filled at all, reserve enough size to put
    /// `len` bytes in it, then fill it with the given closure. If the closure
    /// returns `Err`, it's as if the put never happened.
    pub fn put_to_roll(
        &mut self,
        len: usize,
        f: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<Roll> {
        // TODO: this whole dance is a bit silly: the idea is to have a
        // `RollMut` around that we use whenever we need to serialize something.
        // it's weird that we need to do all this for it to happen but ah well.

        assert_eq!(self.len(), 0);
        self.reserve_at_least(len)?;
        self.put_with(len, f)?;
        let roll = self.take_all();
        debug_assert_eq!(roll.len(), len);
        Ok(roll)
    }

    /// Get a [Roll] corresponding to the filled portion of this buffer
    pub fn filled(&self) -> Roll {
        match &self.storage {
            StorageMut::Buf(b) => b.freeze_slice(0..self.len()).into(),
            StorageMut::Box(b) => RollBox {
                b: b.clone(),
                len: self.len,
            }
            .into(),
        }
    }

    /// Split this [RollMut] at the given index.
    /// Panics if `at > len()`. If `at < len()`, the filled portion will carry
    /// over in the new [RollMut].
    pub fn skip(&mut self, n: usize) {
        let u32_n: u32 = n.try_into().unwrap();
        assert!(u32_n <= self.len);

        match &mut self.storage {
            StorageMut::Buf(b) => b.skip(n),
            StorageMut::Box(b) => b.off += u32_n,
        }
        self.len -= u32_n;
    }

    /// Takes the first `n` bytes (up to `len`) as a `Roll`, and advances
    /// this buffer. Returns `None` if `len` is zero. Panics if `n` is
    /// zero.
    pub fn take_at_most(&mut self, n: usize) -> Option<Roll> {
        assert!(n != 0, "refusing to do empty take_at_most");

        if self.len == 0 {
            return None;
        }

        let n = std::cmp::min(n, self.len as usize);
        let roll = self.filled().slice(..n);
        self.skip(n);
        Some(roll)
    }

    /// Takes the whole `filled` part. Panics if empty, since this is probably
    /// a misuse.
    pub fn take_all(&mut self) -> Roll {
        let roll = self.filled();
        assert!(
            !roll.is_empty(),
            "take_all is pointless if the filled part is empty, check len first"
        );
        self.skip(roll.len());
        roll
    }

    /// Advance this buffer, keeping the filled part only starting with
    /// the given roll. (Useful after parsing something with nom)
    ///
    /// Panics if the roll is not from this buffer
    pub fn keep(&mut self, roll: Roll) {
        match (&mut self.storage, &roll.inner) {
            (StorageMut::Buf(ours), RollInner::Buf(theirs)) => {
                assert_eq!(ours.index, theirs.index, "roll must be from same buffer");
                assert!(theirs.off >= ours.off, "roll must start within buffer");
                let skipped = theirs.off - ours.off;
                tracing::trace!(our_index = %ours.index, their_index = %theirs.index, our_off = %ours.off, their_off = %theirs.off, %skipped, "RollMut::keep");
                self.len -= skipped as u32;
                ours.len -= skipped;
                ours.off = theirs.off;
            }
            (StorageMut::Box(ours), RollInner::Box(theirs)) => {
                assert_eq!(
                    ours.buf.get(),
                    theirs.b.buf.get(),
                    "roll must be from same buffer"
                );
                assert!(theirs.b.off >= ours.off, "roll must start within buffer");
                let skipped = theirs.b.off - ours.off;
                self.len -= skipped;
                ours.off = theirs.b.off;
            }
            _ => {
                panic!("roll must be from same buffer");
            }
        }
    }
}

impl std::io::Write for RollMut {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = buf.len();
        if self.cap() < n {
            // TODO: this is wrong, `reserve` might not reserve _enough data_.
            // we know how much data we need here, so we should reserve exactly
            // that amount (rounded up so it aligns nicely with the default
            // buffer size)
            self.reserve()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
        self.put(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // no need to flush
        Ok(())
    }
}

impl Deref for RollMut {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match &self.storage {
            StorageMut::Buf(b) => &b[..self.len as usize],
            StorageMut::Box(b) => b.slice(self.len),
        }
    }
}

pub(crate) struct ReadInto {
    buf: RollMut,
    off: u32,
    cap: u32,
}

unsafe impl IoBufMut for ReadInto {
    fn io_buf_mut_stable_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.buf.storage.as_mut_ptr().add(self.off as usize) }
    }

    fn io_buf_mut_capacity(&self) -> usize {
        self.cap as _
    }
}

/// An immutable view into a [RollMut]
#[derive(Clone)]
pub struct Roll {
    inner: RollInner,
}

impl Debug for Roll {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self[..], f)
    }
}

impl<T> PartialEq<T> for Roll
where
    T: AsRef<[u8]>,
{
    fn eq(&self, other: &T) -> bool {
        &self[..] == other.as_ref()
    }
}

impl Eq for Roll {}

impl Hash for Roll {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self[..].hash(state)
    }
}

impl From<RollInner> for Roll {
    fn from(inner: RollInner) -> Self {
        Self { inner }
    }
}

impl From<RollBox> for Roll {
    fn from(b: RollBox) -> Self {
        RollInner::Box(b).into()
    }
}

impl From<Buf> for Roll {
    fn from(b: Buf) -> Self {
        RollInner::Buf(b).into()
    }
}

#[derive(Clone)]
enum RollInner {
    Buf(Buf),
    Box(RollBox),
    Empty,
}

#[derive(Clone)]
struct RollBox {
    b: BoxStorage,
    len: u32,
}

impl RollBox {
    #[inline(always)]
    fn split_at(self, at: usize) -> (Self, Self) {
        let at: u32 = at.try_into().unwrap();
        assert!(at <= self.len);

        let left = Self {
            b: self.b.clone(),
            len: at,
        };

        let mut right = Self {
            b: self.b,
            len: self.len - at,
        };
        right.b.off += at;

        (left, right)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len as usize
    }

    fn slice(mut self, range: impl RangeBounds<usize>) -> Self {
        let mut new_start = 0;
        let mut new_end = self.len();

        match range.start_bound() {
            Bound::Included(&n) => new_start = n,
            Bound::Excluded(&n) => new_start = n + 1,
            Bound::Unbounded => {}
        }

        match range.end_bound() {
            Bound::Included(&n) => new_end = n + 1,
            Bound::Excluded(&n) => new_end = n,
            Bound::Unbounded => {}
        }

        assert!(new_start <= new_end);
        assert!(new_end <= self.len());

        self.b.off += new_start as u32;
        self.len = (new_end - new_start) as u32;
        self
    }
}

impl AsRef<[u8]> for RollBox {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.b.slice(self.len)
    }
}

impl AsRef<[u8]> for Roll {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        match &self.inner {
            RollInner::Buf(b) => b.as_ref(),
            RollInner::Box(b) => b.as_ref(),
            RollInner::Empty => &[],
        }
    }
}

impl Deref for Roll {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl RollInner {
    #[inline(always)]
    fn split_at(self, at: usize) -> (Self, Self) {
        match self {
            RollInner::Buf(b) => {
                let (left, right) = b.split_at(at);
                (RollInner::Buf(left), RollInner::Buf(right))
            }
            RollInner::Box(b) => {
                let (left, right) = b.split_at(at);
                (RollInner::Box(left), RollInner::Box(right))
            }
            RollInner::Empty => {
                assert_eq!(at, 0);
                (RollInner::Empty, RollInner::Empty)
            }
        }
    }
}

impl Roll {
    /// Creates an empty roll
    pub fn empty() -> Self {
        RollInner::Empty.into()
    }

    /// Returns the length of this roll
    #[inline(always)]
    pub fn len(&self) -> usize {
        match &self.inner {
            RollInner::Buf(b) => b.len(),
            RollInner::Box(b) => b.len(),
            RollInner::Empty => 0,
        }
    }

    /// Returns true if this roll is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn split_at(self, at: usize) -> (Roll, Roll) {
        let (left, right) = self.inner.split_at(at);
        (left.into(), right.into())
    }

    pub fn slice(self, range: impl RangeBounds<usize>) -> Self {
        match self.inner {
            RollInner::Buf(b) => b.slice(range).into(),
            RollInner::Box(b) => b.slice(range).into(),
            RollInner::Empty => panic!("cannot slice empty roll"),
        }
    }

    pub fn iter(&self) -> RollIter {
        RollIter {
            roll: self.clone(),
            pos: 0,
        }
    }

    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self)
    }

    /// Decode as utf-8
    pub fn to_string(self) -> Result<RollStr, Utf8Error> {
        _ = std::str::from_utf8(&self)?;
        Ok(RollStr { roll: self })
    }

    /// Convert to [RollStr].
    ///
    /// # Safety
    /// UB if not utf-8. Typically only used in parsers.
    pub unsafe fn to_string_unchecked(self) -> RollStr {
        RollStr { roll: self }
    }
}

impl InputIter for Roll {
    type Item = u8;
    type Iter = Enumerate<Self::IterElem>;
    type IterElem = RollIter;

    #[inline]
    fn iter_indices(&self) -> Self::Iter {
        self.iter_elements().enumerate()
    }
    #[inline]
    fn iter_elements(&self) -> Self::IterElem {
        self.iter()
    }
    #[inline]
    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.iter().position(predicate)
    }
    #[inline]
    fn slice_index(&self, count: usize) -> Result<usize, Needed> {
        if self.len() >= count {
            Ok(count)
        } else {
            Err(Needed::new(count - self.len()))
        }
    }
}

/// An iterator over [Roll]
pub struct RollIter {
    roll: Roll,
    pos: usize,
}

impl Iterator for RollIter {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.roll.len() {
            return None;
        }

        let c = self.roll[self.pos];
        self.pos += 1;
        Some(c)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.roll.len() - self.pos;
        (remaining, Some(remaining))
    }
}

impl InputTake for Roll {
    #[inline]
    fn take(&self, count: usize) -> Self {
        self.clone().slice(..count)
    }
    #[inline]
    fn take_split(&self, count: usize) -> (Self, Self) {
        let (prefix, suffix) = self.clone().split_at(count);
        (suffix, prefix)
    }
}

impl InputTakeAtPosition for Roll {
    type Item = u8;

    fn split_at_position<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> nom::IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.iter().position(predicate) {
            Some(i) => Ok(self.clone().take_split(i)),
            None => Err(nom::Err::Incomplete(nom::Needed::new(1))),
        }
    }

    fn split_at_position1<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
        e: nom::error::ErrorKind,
    ) -> nom::IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.iter().position(predicate) {
            Some(0) => Err(nom::Err::Error(E::from_error_kind(self.clone(), e))),
            Some(i) => Ok(self.take_split(i)),
            None => Err(nom::Err::Incomplete(nom::Needed::new(1))),
        }
    }

    fn split_at_position_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> nom::IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.iter().position(predicate) {
            Some(i) => Ok(self.take_split(i)),
            None => Ok(self.take_split(self.input_len())),
        }
    }

    fn split_at_position1_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
        e: nom::error::ErrorKind,
    ) -> nom::IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.iter().position(predicate) {
            Some(0) => Err(nom::Err::Error(E::from_error_kind(self.clone(), e))),
            Some(i) => Ok(self.take_split(i)),
            None => {
                if self.is_empty() {
                    Err(nom::Err::Error(E::from_error_kind(self.clone(), e)))
                } else {
                    Ok(self.take_split(self.input_len()))
                }
            }
        }
    }
}

impl FindSubstring<&[u8]> for Roll {
    fn find_substring(&self, substr: &[u8]) -> Option<usize> {
        if substr.len() > self.len() {
            return None;
        }

        let (&substr_first, substr_rest) = match substr.split_first() {
            Some(split) => split,
            // an empty substring is found at position 0
            // This matches the behavior of str.find("").
            None => return Some(0),
        };

        if substr_rest.is_empty() {
            return memchr::memchr(substr_first, self);
        }

        let mut offset = 0;
        let haystack = &self[..self.len() - substr_rest.len()];

        while let Some(position) = memchr::memchr(substr_first, &haystack[offset..]) {
            offset += position;
            let next_offset = offset + 1;
            if &self[next_offset..][..substr_rest.len()] == substr_rest {
                return Some(offset);
            }

            offset = next_offset;
        }

        None
    }
}

impl Compare<&[u8]> for Roll {
    #[inline(always)]
    fn compare(&self, t: &[u8]) -> CompareResult {
        let pos = self.iter().zip(t.iter()).position(|(a, b)| a != *b);

        match pos {
            Some(_) => CompareResult::Error,
            None => {
                if self.len() >= t.len() {
                    CompareResult::Ok
                } else {
                    CompareResult::Incomplete
                }
            }
        }
    }

    #[inline(always)]
    fn compare_no_case(&self, t: &[u8]) -> CompareResult {
        if self
            .iter()
            .zip(t)
            .any(|(a, b)| lowercase_byte(a) != lowercase_byte(*b))
        {
            CompareResult::Error
        } else if self.len() < t.len() {
            CompareResult::Incomplete
        } else {
            CompareResult::Ok
        }
    }
}

fn lowercase_byte(c: u8) -> u8 {
    match c {
        b'A'..=b'Z' => c - b'A' + b'a',
        _ => c,
    }
}

impl InputLength for Roll {
    #[inline]
    fn input_len(&self) -> usize {
        self.len()
    }
}

impl<S> Slice<S> for Roll
where
    S: RangeBounds<usize>,
{
    fn slice(&self, range: S) -> Self {
        Roll::slice(self.clone(), range)
    }
}

/// A [Roll] that's also a valid utf-8 string.
#[derive(Clone)]
pub struct RollStr {
    roll: Roll,
}

impl Debug for RollStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self[..], f)
    }
}

impl Deref for RollStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe { std::str::from_utf8_unchecked(&self.roll) }
    }
}

impl RollStr {
    pub fn into_inner(self) -> Roll {
        self.roll
    }
}

#[cfg(test)]
mod tests {
    use nom::IResult;
    use tracing::trace;

    use crate::{Roll, RollMut, BUF_SIZE};

    #[test]
    fn test_roll_put() {
        fn test_roll_put_inner(mut rm: RollMut) {
            let initial_size = rm.cap();

            rm.put(b"hello").unwrap();
            assert_eq!(rm.cap(), initial_size - 5);

            let filled = rm.filled();
            assert_eq!(&filled[..], b"hello");

            rm.skip(5);
            assert_eq!(rm.len(), 0);
            assert_eq!(rm.cap(), initial_size - 5);
        }

        let rm = RollMut::alloc().unwrap();
        assert_eq!(rm.cap(), BUF_SIZE as usize);
        test_roll_put_inner(rm);

        let mut rm = RollMut::alloc().unwrap();
        rm.grow();
        test_roll_put_inner(rm);

        let mut rm = RollMut::alloc().unwrap();
        rm.grow();
        rm.grow();
        test_roll_put_inner(rm);
    }

    #[test]
    fn test_roll_put_does_not_fit() {
        let mut rm = RollMut::alloc().unwrap();
        rm.put(" ".repeat(rm.cap())).unwrap();

        let err = rm.put("drop").unwrap_err();
        assert!(format!("{err:?}").contains("DoesNotFit"));
        assert!(format!("{err}").contains("does not fit"));
    }

    #[test]
    fn test_roll_realloc() {
        fn test_roll_realloc_inner(mut rm: RollMut) {
            let init_cap = rm.cap();
            rm.put("hello").unwrap();
            rm.take_all();
            assert_eq!(rm.cap(), init_cap - 5);

            rm.compact().unwrap();
            assert_eq!(rm.cap(), BUF_SIZE as usize);
        }

        let rm = RollMut::alloc().unwrap();
        test_roll_realloc_inner(rm);

        let mut rm = RollMut::alloc().unwrap();
        rm.grow();
        test_roll_realloc_inner(rm);
    }

    #[test]
    fn test_roll_realloc_big() {
        let mut rm = RollMut::alloc().unwrap();
        rm.grow();

        let put = "x".repeat(rm.cap() * 2 / 3);
        rm.put(&put).unwrap();
        rm.compact().unwrap();

        assert_eq!(rm.storage_size(), BUF_SIZE as usize * 2);
        assert_eq!(rm.len(), put.len());
        assert_eq!(&rm[..], put.as_bytes());
    }

    #[test]
    fn test_roll_reserve() {
        let mut rm = RollMut::alloc().unwrap();
        assert_eq!(rm.cap(), BUF_SIZE as usize);
        assert_eq!(rm.len(), 0);
        rm.reserve().unwrap();
        assert_eq!(rm.cap(), BUF_SIZE as usize);
        assert_eq!(rm.len(), 0);

        rm.put("hello").unwrap();
        rm.take_all();

        assert_eq!(rm.cap(), BUF_SIZE as usize - 5);
        assert_eq!(rm.len(), 0);
        rm.reserve().unwrap();
        assert_eq!(rm.cap(), BUF_SIZE as usize - 5);
        assert_eq!(rm.len(), 0);

        let old_cap = rm.cap();
        rm.put(b" ".repeat(old_cap)).unwrap();
        assert_eq!(rm.cap(), 0);
        assert_eq!(rm.len(), old_cap);

        rm.reserve().unwrap();
        assert_eq!(rm.cap(), 5);
        assert_eq!(rm.len(), old_cap);

        rm.put("hello").unwrap();
        rm.reserve().unwrap();
        assert_eq!(rm.cap(), BUF_SIZE as usize);
        assert_eq!(rm.len(), BUF_SIZE as usize);
    }

    #[test]
    fn test_roll_put_then_grow() {
        let mut rm = RollMut::alloc().unwrap();
        assert_eq!(rm.cap(), BUF_SIZE as usize);

        let input = b"I am pretty long";

        rm.put(input).unwrap();
        assert_eq!(rm.len(), input.len());
        assert_eq!(&rm[..], input);

        assert_eq!(rm.cap(), BUF_SIZE as usize - input.len());

        rm.grow();
        assert_eq!(rm.cap(), 2 * (BUF_SIZE as usize) - input.len());
        assert_eq!(&rm[..], input);

        rm.skip(5);
        assert_eq!(&rm[..], b"pretty long");
    }

    #[test]
    #[cfg(not(feature = "miri"))]
    fn test_roll_readfrom_start() {
        use crate::WriteOwned;

        crate::start(async move {
            let mut rm = RollMut::alloc().unwrap();

            let (mut send, mut read) = crate::pipe();
            crate::spawn(async move {
                send.write_all_owned("123456").await.unwrap();
            });

            let mut res;
            (res, rm) = rm.read_into(3, &mut read).await;
            res.unwrap();

            assert_eq!(rm.len(), 3);
            assert_eq!(rm.filled().as_ref(), b"123");

            (res, rm) = rm.read_into(3, &mut read).await;
            res.unwrap();

            assert_eq!(rm.len(), 6);
            assert_eq!(rm.filled().as_ref(), b"123456");
        });
    }

    #[test]
    fn test_roll_keep() {
        fn test_roll_keep_inner(mut rm: RollMut) {
            rm.put(b"helloworld").unwrap();
            assert_eq!(&rm[..], b"helloworld");

            {
                let roll = rm.filled().slice(3..=5);
                assert_eq!(roll, b"low");
            }

            {
                let roll = rm.filled().slice(..5);
                assert_eq!(roll, b"hello");
            }

            let roll = rm.filled().slice(5..);
            assert_eq!(roll, b"world");

            rm.keep(roll);
            assert_eq!(&rm[..], b"world");
        }

        let rm = RollMut::alloc().unwrap();
        test_roll_keep_inner(rm);

        let mut rm = RollMut::alloc().unwrap();
        rm.grow();
        test_roll_keep_inner(rm);
    }

    #[test]
    #[should_panic(expected = "roll must be from same buffer")]
    fn test_roll_keep_different_buf() {
        let mut rm1 = RollMut::alloc().unwrap();
        rm1.put("hello").unwrap();

        let mut rm2 = RollMut::alloc().unwrap();
        rm2.put("hello").unwrap();
        let roll2 = rm2.take_all();

        rm1.keep(roll2);
    }

    #[test]
    #[should_panic(expected = "roll must be from same buffer")]
    fn test_roll_keep_different_box() {
        let mut rm1 = RollMut::alloc().unwrap();
        rm1.grow();
        rm1.put("hello").unwrap();

        let mut rm2 = RollMut::alloc().unwrap();
        rm2.grow();
        rm2.put("hello").unwrap();
        let roll2 = rm2.take_all();

        rm1.keep(roll2);
    }

    #[test]
    #[should_panic(expected = "roll must be from same buffer")]
    fn test_roll_keep_different_type() {
        let mut rm1 = RollMut::alloc().unwrap();
        rm1.grow();
        rm1.put("hello").unwrap();

        let mut rm2 = RollMut::alloc().unwrap();
        rm2.put("hello").unwrap();
        let roll2 = rm2.take_all();

        rm1.keep(roll2);
    }

    #[test]
    #[should_panic(expected = "roll must start within buffer")]
    fn test_roll_keep_before_buf() {
        let mut rm1 = RollMut::alloc().unwrap();
        rm1.put("hello").unwrap();
        let roll = rm1.filled();
        rm1.skip(5);
        rm1.keep(roll);
    }

    #[test]
    #[should_panic(expected = "roll must start within buffer")]
    fn test_roll_keep_before_box() {
        let mut rm1 = RollMut::alloc().unwrap();
        rm1.grow();
        rm1.put("hello").unwrap();
        let roll = rm1.filled();
        rm1.skip(5);
        rm1.keep(roll);
    }

    #[test]
    fn test_roll_iter() {
        let mut rm = RollMut::alloc().unwrap();
        rm.put(b"hello").unwrap();
        let roll = rm.filled();
        let v = roll.iter().collect::<Vec<_>>();
        assert_eq!(v, b"hello");

        assert_eq!(roll.to_string_lossy(), "hello");
    }

    #[test]
    #[cfg(not(feature = "miri"))]
    fn test_roll_iobuf() {
        use crate::{
            io::{IntoHalves, ReadOwned, WriteOwned},
            net::{TcpListener, TcpStream},
        };

        async fn test_roll_iobuf_inner(mut rm: RollMut) -> eyre::Result<()> {
            rm.put(b"hello").unwrap();
            let roll = rm.take_all();

            let ln = TcpListener::bind("127.0.0.1:0".parse()?).await?;
            let local_addr = ln.local_addr()?;

            let send_fut = async move {
                let stream = TcpStream::connect(local_addr).await?;
                let (_stream_r, mut stream_w) = IntoHalves::into_halves(stream);
                stream_w.write_all_owned(roll).await?;
                Ok::<_, eyre::Report>(())
            };

            let recv_fut = async move {
                let (stream, addr) = ln.accept().await?;
                let (mut stream_r, _stream_w) = IntoHalves::into_halves(stream);
                println!("Accepted connection from {addr}");

                let mut buf = vec![0u8; 1024];
                let res;
                (res, buf) = stream_r.read_owned(buf).await;
                let n = res?;

                assert_eq!(&buf[..n], b"hello");

                Ok::<_, eyre::Report>(())
            };

            tokio::try_join!(send_fut, recv_fut)?;
            Ok(())
        }

        crate::start(async move {
            let rm = RollMut::alloc().unwrap();
            test_roll_iobuf_inner(rm).await.unwrap();

            let mut rm = RollMut::alloc().unwrap();
            rm.grow();
            test_roll_iobuf_inner(rm).await.unwrap();
        });
    }

    #[test]
    fn test_roll_take_at_most() {
        let mut rm = RollMut::alloc().unwrap();
        rm.put(b"hello").unwrap();
        let roll = rm.take_at_most(4).unwrap();
        assert_eq!(roll, b"hell");

        let mut rm = RollMut::alloc().unwrap();
        rm.put(b"hello").unwrap();
        let roll = rm.take_at_most(12).unwrap();
        assert_eq!(roll, b"hello");

        let mut rm = RollMut::alloc().unwrap();
        assert!(rm.take_at_most(12).is_none());
    }

    #[test]
    fn test_roll_take_all() {
        let mut rm = RollMut::alloc().unwrap();
        rm.put(b"hello").unwrap();
        let roll = rm.take_all();
        assert_eq!(roll, b"hello");
    }

    #[test]
    #[should_panic(expected = "take_all is pointless if the filled part is empty")]
    fn test_roll_take_all_empty() {
        let mut rm = RollMut::alloc().unwrap();
        rm.take_all();
    }

    #[test]
    #[should_panic(expected = "refusing to do empty take_at_most")]
    fn test_roll_take_at_most_panic() {
        let mut rm = RollMut::alloc().unwrap();
        rm.take_at_most(0);
    }

    #[test]
    fn test_roll_nom_sample() {
        fn parse(i: Roll) -> IResult<Roll, Roll> {
            nom::bytes::streaming::tag(&b"HTTP/1.1 200 OK"[..])(i)
        }

        let mut buf = RollMut::alloc().unwrap();

        let input = b"HTTP/1.1 200 OK".repeat(1000);
        let mut pending = &input[..];

        loop {
            if buf.cap() == 0 {
                trace!("buf had zero cap, growing");
                buf.grow()
            }

            let (rest, version) = match parse(buf.filled()) {
                Ok(t) => t,
                Err(e) => {
                    if e.is_incomplete() {
                        {
                            if pending.is_empty() {
                                println!("ran out of input");
                                break;
                            }

                            let n = std::cmp::min(buf.cap(), pending.len());
                            buf.put(&pending[..n]).unwrap();
                            pending = &pending[n..];

                            println!("advanced by {n}, {} remaining", pending.len());
                        }

                        continue;
                    }
                    panic!("parsing error: {e}");
                }
            };
            assert_eq!(version, b"HTTP/1.1 200 OK");

            buf.keep(rest);
        }
    }

    #[test]
    fn test_roll_io_write() {
        let mut rm = RollMut::alloc().unwrap();
        std::io::Write::write_all(&mut rm, b"hello").unwrap();

        let roll = rm.take_all();
        assert_eq!(std::str::from_utf8(&roll).unwrap(), "hello");
    }

    #[test]
    fn test_reallocate_big() {
        let mut rm = RollMut::alloc().unwrap();
        rm.put(b"baba yaga").unwrap();
        let filled = rm.filled();
        let (_frame, rest) = filled.split_at(4);
        rm.keep(rest);

        rm.reserve_at_least(5263945).unwrap();
        assert!(rm.cap() >= 5263945);
    }
}

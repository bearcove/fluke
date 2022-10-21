use std::{
    cell::{RefCell, RefMut},
    fmt,
    iter::Enumerate,
    ops::Range,
    rc::Rc,
};

use nom::{
    Compare, CompareResult, FindSubstring, InputIter, InputLength, InputTake, InputTakeAtPosition,
};
use smallvec::SmallVec;

use crate::bufpool::{self, BufMut};

use super::{IoChunk, IoChunkable};

macro_rules! dbg2 {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            // 🐉 uncomment to debug tests:
            // dbg!($($arg)*);
        }
    };
}

/// An "aggregate buffer", uses one or more [BufMut]s for storage. Allows
/// writing to uninitialized data, and borrowing ref-counted [AggSlice] of
/// initialized data.
///
/// ```text
///     +-------------------------------+---------------------------------+
///     |             block 0           |            block 1              |
///     +-------------------------------+---------------------------------+
///     |                  |                 |                            |
///     |<----- off ------>|<----------------+- capacity ---------------->|
///     |                  |                 |                            |
///     | used by previous |<----- len ----->|<---------- avail --------->|
///     | bufs and slices. |                 |                            |
///                        |   filled and    |     can be written to      |
///                        |    readable     |                            |
/// ```
///
/// A non-zero `off` indicates that this [AggBuf] was split from another
/// one, re-using `avail` bytes.
///
/// [AggSlice] offsets are relative to the global offset.
pub struct AggBuf {
    inner: Rc<RefCell<AggBufInner>>,
}

/// The inner representation of an [AggBuf].
#[derive(Default)]
pub struct AggBufInner {
    /// storage
    blocks: SmallVec<[BufMut; 5]>,

    /// size of each block - typically a compile-time constant but let's
    /// have a field anyway for now.
    block_size: u32,

    /// global offset: how many bytes to ignore from the first block, in case
    /// this aggregate buffer is re-used from a previous operation.
    off: u32,

    /// length: number of bytes that are initialized (were we read into, and can
    /// be sliced).
    len: u32,
}

pub struct AggBufRead<'a> {
    handle: &'a Rc<RefCell<AggBufInner>>,
    borrow: std::cell::Ref<'a, AggBufInner>,
}

impl Default for AggBuf {
    /// Create an empty [AggBuf].
    ///
    /// [AggBuf::grow_if_needed] must be called before writing to it.
    fn default() -> Self {
        let inner = AggBufInner::new();
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }
}

impl AggBuf {
    /// Borrow this aggregate buffer immutably
    pub fn read(&self) -> AggBufRead<'_> {
        let handle = &self.inner;
        let borrow = handle.borrow();
        AggBufRead { handle, borrow }
    }

    /// Borrow this aggregate buffer mutably
    pub fn write(&self) -> RefMut<AggBufInner> {
        self.inner.borrow_mut()
    }

    /// Split off at the end of the filled portion
    pub fn split(self) -> Self {
        let slice = {
            let read = self.read();
            let len = read.len();
            self.read().slice(len..len)
        };
        self.split_at(slice)
    }

    /// Split off at the start of the given slice, re-using the space in the
    /// given slice, and any unfilled space.
    ///
    /// ```text
    /// Before/after (1 block)
    ///
    ///                                               <-- rest -->
    /// [.............AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBB...........]
    /// <--- off ----><-------------------- len -----------------><-- avail-->
    ///
    /// [.............................................BBBBBBBBBBBB...........]
    /// <------------------- off --------------------><--- len --><-- avail-->
    ///
    /// Before/after (2+ blocks)
    ///
    ///                                               <-- rest -->
    /// [.............AAAAAAAAAAAAAAAAAAAA][AAAAAAAAAABBBBBBBBBBBB...........]
    /// <--- off ----><-------------------- len -----------------><-- avail-->
    ///
    /// [..........BBBBBBBBBBBB...........]
    /// <-- off --><--- len --><-- avail-->
    /// ```
    pub fn split_at(self, rest: AggSlice) -> Self {
        let inner = self.inner.borrow();
        let block_size = inner.block_size;

        dbg2!("split_at", inner.off, inner.len, inner.capacity(),);

        let abs_block_start = (inner.blocks.len() as u32 - 1) * block_size;
        let abs_filled_end = inner.len + inner.off;
        let abs_rest_start = rest.off + inner.off;
        dbg2!("split_at", abs_block_start, abs_filled_end, abs_rest_start);

        // we now have (1 block)
        //
        // [.............AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBB...........]
        // |                                             |          |
        // +- abs_block_start                            |          + abs_filled_end
        //                                               + abs_rest_start
        //
        // or (2+ blocks)
        //
        // [.............AAAAAAAAAAAAAAAAAAAA][AAAAAAAAAABBBBBBBBBBBB...........]
        //                                    |          |          |
        //                    abs_block_start +          |          + abs_filled_end
        //                                               + abs_rest_start

        assert!(
            abs_block_start <= abs_rest_start,
            "rest must start into last block"
        );
        assert!(
            abs_rest_start <= abs_filled_end,
            "rest must start before end of filled part"
        );

        // let's clone get that block and make everything relative to its start
        let reused_block = inner.blocks.iter().last().unwrap().dangerous_clone();

        let rel_filled_end = abs_filled_end - abs_block_start;
        let rel_rest_start = abs_rest_start - abs_block_start;

        // we now have (1 block)
        //
        // <------------------ offset ------------------><--- len --><-- avail-->
        // [.............AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBB...........]
        //                                               |          |
        //                                               |          + rel_filled_end
        //                                               + rel_rest_start
        //
        // or (2+ blocks)
        //
        // <- offset -><-- len --><-- avail-->
        // [AAAAAAAAAABBBBBBBBBBBB...........]
        //            |          |
        //            |          + rel_filled_end
        //            + rel_rest_start

        if rel_rest_start == inner.block_size {
            // there's nothing to re-use here, just return an empty buffer
            return Default::default();
        }

        let mut new_inner = AggBufInner {
            blocks: Default::default(),
            block_size: inner.block_size,
            off: rel_rest_start,
            len: rel_filled_end - rel_rest_start,
        };
        new_inner.blocks.push(reused_block);
        Self {
            inner: Rc::new(RefCell::new(new_inner)),
        }
    }

    /// Return a write slice appropriate for a io_uring read.
    ///
    /// This panics if the write_slice would be empty, to avoid
    /// misuse / no-op reads.
    pub fn write_slice(self) -> AggWriteSlice {
        let ptr;
        let len;

        {
            let mut inner = self.inner.borrow_mut();
            if inner.blocks.is_empty() {
                panic!(
                    "holding AggBuf wrong: please call grow_if_needed before calling write_slice"
                );
            }

            let (block_index, block_range) = inner.contiguous_range(inner.len..inner.capacity());
            ptr = unsafe {
                inner.blocks[block_index]
                    .as_mut_ptr()
                    .add(block_range.start)
            };
            len = block_range.len();
        }

        AggWriteSlice {
            buf: self,
            ptr,
            len: len as _,
            pos: 0,
        }
    }
}

/// A write slice of an [AggBuf] suitable for an io_uring read/write
pub struct AggWriteSlice {
    buf: AggBuf,
    ptr: *mut u8,
    pos: u32,
    len: u32,
}

impl AggWriteSlice {
    pub fn into_inner(self) -> AggBuf {
        self.buf.inner.borrow_mut().len += self.pos;
        self.buf
    }

    /// Limit how many bytes can be read. If the slice was slower
    /// in the first place, don't do anything.
    pub fn limit(mut self, len: u64) -> Self {
        if len < self.len as u64 {
            self.len = len as u32;
        }
        self
    }
}

unsafe impl tokio_uring::buf::IoBuf for AggWriteSlice {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr
    }

    fn bytes_init(&self) -> usize {
        self.pos as _
    }

    fn bytes_total(&self) -> usize {
        self.len as _
    }
}

unsafe impl tokio_uring::buf::IoBufMut for AggWriteSlice {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.pos = pos as _
    }
}

impl AggBufInner {
    fn new() -> Self {
        Self {
            blocks: Default::default(),
            block_size: crate::bufpool::BUF_SIZE as _,
            off: 0,
            len: 0,
        }
    }

    /// Returns the size of each buffer in the pool
    #[inline(always)]
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Returns the total capacity of the buffer
    #[inline(always)]
    pub fn capacity(&self) -> u32 {
        self.blocks.len() as u32 * self.block_size - self.off
    }

    /// Returns the (filled) length of the buffer
    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Return true if this aggregate buf is empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a block index and a slice into it, given a slice into the
    /// aggregate buffer. Doesn't check for the `filled` region
    fn contiguous_range(&self, wanted: Range<u32>) -> (usize, Range<usize>) {
        dbg2!("contiguous_range", &wanted);
        let (start, end) = (wanted.start, wanted.end);
        assert!(start <= end);

        if wanted.start == wanted.end {
            // special case: empty slice
            return (0, 0..0);
        }

        let wanted = end - start;

        // take the global offset into account when indexing into bufs
        let start = start + self.off;
        let block_index = (start / self.block_size) as usize;

        debug_assert!(block_index < self.blocks.len());

        let block_offset = start % self.block_size;
        dbg2!("contiguous_range", block_index, block_offset);
        let avail = self.block_size - block_offset;
        let given = std::cmp::min(avail, wanted);
        dbg2!("contiguous_range", avail, given);

        let block_offset = block_offset as usize;
        let given = given as usize;

        (block_index, block_offset..block_offset + given)
    }

    /// If `len == capacity` (ie. the `unfilled_mut` slice would be empty), try
    /// to add a block to this aggregate buffer. This is fallible, as we might
    /// be out of memory.
    pub fn grow_if_needed(&mut self) -> Result<(), bufpool::Error> {
        if self.len < self.capacity() {
            return Ok(());
        }

        let block = BufMut::alloc()?;
        self.blocks.push(block);
        Ok(())
    }

    /// Writes a slice into the buffer, growing it if needed.
    pub fn put(&mut self, s: impl AsRef<[u8]>) -> Result<(), bufpool::Error> {
        let mut s = s.as_ref();
        while !s.is_empty() {
            self.grow_if_needed()?;

            {
                let unfilled = self.unfilled_mut();
                let unfilled_len = unfilled.len();
                let to_copy = std::cmp::min(unfilled_len, s.len());
                unfilled[..to_copy].copy_from_slice(&s[..to_copy]);
                self.advance(to_copy as u32);
                s = &s[to_copy..];
            }
        }
        Ok(())
    }

    /// Writes an [AggSlice] into this buffer, growing it if needed.
    pub fn put_agg(&mut self, s: &AggSlice) -> Result<(), bufpool::Error> {
        // TODO: this is very, very inefficient
        self.put(&s.to_vec())
    }

    /// Gives a mutable slice that can be written to.
    /// Must call `advance` after writing to the returned slice.
    pub fn unfilled_mut(&mut self) -> &mut [u8] {
        if self.blocks.is_empty() {
            return &mut [];
        }

        let (block_index, range) = self.contiguous_range(self.len()..self.capacity());
        &mut self.blocks[block_index][range]
    }

    /// Called after writing to `unfilled_mut`. Panics if adding `n` brings the
    /// buffer over capacity.
    ///
    /// This isn't unsafe because [BufMut] are zeroed. But you will get
    /// incorrect results if you advance past the end of what's been filled.
    pub fn advance(&mut self, n: u32) {
        assert!(self.len + n <= self.capacity());
        self.len += n;
    }
}

impl<'a> std::ops::Deref for AggBufRead<'a> {
    type Target = AggBufInner;

    fn deref(&self) -> &Self::Target {
        &self.borrow
    }
}

impl AggBufRead<'_> {
    /// Return a slice for the whole length of this buffer
    pub fn read_slice(&self) -> AggSlice {
        self.slice(0..self.len())
    }

    /// Take a slice out of the filled portion of this buffer. Panics if
    /// it is outside the filled portion.
    pub fn slice(&self, range: Range<u32>) -> AggSlice {
        assert!(range.start <= range.end);
        assert!(range.end <= self.len);

        AggSlice {
            parent: AggBuf {
                inner: self.handle.clone(),
            },
            off: range.start as _,
            len: (range.end - range.start) as _,
        }
    }

    /// Returns the biggest continuous slice we can get a the given offset.
    /// Panics if `wanted` is out of bounds. If the requested range spans
    /// multiple buffers, the returned slice will be smaller than requested.
    pub fn filled(&self, wanted: Range<u32>) -> &[u8] {
        // FIXME: this probably shouldn't be pub

        assert!(wanted.end <= self.len);

        let (block_index, given) = self.contiguous_range(wanted);
        &self.blocks[block_index][given]
    }
}

/// A slice of an [AggBuf]. This is a read-only view, it's clonable,
/// it holds a reference to the underlying [AggBuf], so holding it
/// will keep the _whole_ [AggBuf] alive.
pub struct AggSlice {
    parent: AggBuf,
    off: u32,
    len: u32,
}

impl Clone for AggSlice {
    fn clone(&self) -> Self {
        Self {
            parent: AggBuf {
                inner: self.parent.inner.clone(),
            },
            off: self.off,
            len: self.len,
        }
    }
}

impl fmt::Debug for AggSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggSlice")
            .field("off", &self.off)
            .field("len", &self.len)
            .finish()
    }
}

impl IoChunkable for AggSlice {
    /// Returns next contiguous slice
    fn next_chunk(&self, offset: u32) -> Option<IoChunk> {
        if offset == self.len {
            return None;
        }

        let parent = self.parent.read();
        let (block_index, range) = parent.contiguous_range(self.off + offset..self.off + self.len);

        // FIXME: use try_into?
        let range = (range.start as u16)..(range.end as u16);
        Some(parent.blocks[block_index].freeze_slice(range).into())
    }
}

impl AggSlice {
    /// Returns an iterator over the bytes in this slice
    #[inline]
    pub fn iter(&self) -> AggSliceIter {
        AggSliceIter {
            slice: self.clone(),
            pos: 0,
        }
    }

    /// Returns as a vector. This allocates a lot.
    pub fn to_vec(&self) -> Vec<u8> {
        self.iter().collect()
    }

    /// Returns as a string. This allocates a lot.
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.to_vec()).to_string()
    }

    /// Returns the length of this slice
    pub fn len(&self) -> usize {
        self.len as _
    }

    /// Returns true if this is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns true if this slice equals `slice`, ignoring ASCII case
    pub fn eq_ignore_ascii_case(&self, slice: impl AsRef<[u8]>) -> bool {
        let slice = slice.as_ref();

        if self.len() != slice.len() {
            return false;
        }

        // FIXME: this is very naive and could be optimized
        self.iter()
            .zip(slice.iter())
            .all(|(l, r)| l.eq_ignore_ascii_case(r))
    }
}

impl<T> PartialEq<T> for AggSlice
where
    T: AsRef<[u8]>,
{
    fn eq(&self, slice: &T) -> bool {
        let slice = slice.as_ref();

        if self.len() != slice.len() {
            return false;
        }

        // FIXME: this is very naive and could be optimized
        self.iter().eq(slice.iter().copied())
    }
}

impl InputLength for AggSlice {
    fn input_len(&self) -> usize {
        self.len as _
    }
}

impl InputTake for AggSlice {
    fn take(&self, count: usize) -> Self {
        let count: u32 = count.try_into().unwrap();
        if count > self.len {
            panic!("take: count > self.len");
        }

        Self {
            parent: AggBuf {
                inner: self.parent.inner.clone(),
            },
            off: self.off,
            len: count,
        }
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        let count: u32 = count.try_into().unwrap();
        if count > self.len {
            panic!("take_split: count > self.len");
        }

        let prefix = Self {
            parent: AggBuf {
                inner: self.parent.inner.clone(),
            },
            off: self.off,
            len: count,
        };
        let suffix = Self {
            parent: AggBuf {
                inner: self.parent.inner.clone(),
            },
            off: self.off + count,
            len: self.len - count,
        };
        (suffix, prefix)
    }
}

impl Compare<&[u8]> for AggSlice {
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

impl InputTakeAtPosition for AggSlice {
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

impl FindSubstring<&[u8]> for AggSlice {
    fn find_substring(&self, substr: &[u8]) -> Option<usize> {
        let mut offset = None;
        let mut curr_substr = substr;
        for (i, c) in self.iter().enumerate() {
            if c == curr_substr[0] {
                if offset.is_none() {
                    offset = Some(i);
                }
                curr_substr = &curr_substr[1..];
                if curr_substr.is_empty() {
                    return offset;
                }
            } else {
                curr_substr = substr;
            }
        }

        None
    }
}

impl InputIter for AggSlice {
    type Item = u8;
    type Iter = Enumerate<AggSliceIter>;
    type IterElem = AggSliceIter;

    #[inline]
    fn iter_indices(&self) -> Self::Iter {
        self.iter().enumerate()
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
    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        if self.len() >= count {
            Ok(count)
        } else {
            Err(nom::Needed::new(count - self.len()))
        }
    }
}

fn lowercase_byte(c: u8) -> u8 {
    match c {
        b'A'..=b'Z' => c - b'A' + b'a',
        _ => c,
    }
}

pub struct AggSliceIter {
    slice: AggSlice,
    pos: u32,
}

impl Iterator for AggSliceIter {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.slice.len {
            return None;
        }

        dbg2!("AggSliceIter::next", self.pos, self.slice.len);

        // FIXME: this implementation is extremely naive and not efficient at
        // all. we shouldn't have to borrow here or do block math on every
        // iteration: just until we run out of the current block.
        let inner = self.slice.parent.inner.borrow();
        let global_off = self.pos + self.slice.off;
        let (block_index, range) = inner.contiguous_range(global_off..global_off + 1);
        self.pos += 1;
        Some(inner.blocks[block_index][range][0])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.slice.len - self.pos;
        (remaining as _, Some(remaining as _))
    }
}

#[cfg(test)]
mod tests {
    use crate::bufpool::{AggBuf, AggBufInner, AggSlice};
    use nom::IResult;
    use pretty_assertions::assert_eq;

    #[test]
    fn agg_inner_size() {
        assert_eq!(std::mem::size_of::<AggBufInner>(), 64);
    }

    #[test]
    fn agg_slice_size() {
        assert_eq!(std::mem::size_of::<AggSlice>(), 16);
    }

    #[test]
    fn agg_fill() {
        let buf: AggBuf = Default::default();

        let block_size;
        let agg_len;
        {
            let mut buf = buf.write();
            block_size = buf.block_size();

            {
                println!("allocating first block");
                buf.grow_if_needed().unwrap();
            }

            {
                println!("filling first block");
                let slice = buf.unfilled_mut();
                let len = slice.len();
                assert_eq!(len, block_size as usize);
                slice.fill(1);
                buf.advance(len as _);
                assert_eq!(buf.len(), block_size as _);
            }

            {
                println!("unfilled should be empty");
                let slice = buf.unfilled_mut();
                assert_eq!(slice.len(), 0);
            }

            {
                println!("allocating second block");
                buf.grow_if_needed().unwrap();
            }

            {
                println!("filling second block");
                let slice = buf.unfilled_mut();
                let len = slice.len();
                assert_eq!(len, block_size as usize);
                slice.fill(2);
                buf.advance(len as _);
                assert_eq!(buf.len(), block_size * 2);
            }

            {
                println!("unfilled should be empty again");
                let slice = buf.unfilled_mut();
                assert_eq!(slice.len(), 0);
            }

            agg_len = buf.len();
        }

        {
            let buf = buf.read();

            let slice = buf.filled(0..agg_len);
            assert_eq!(slice.len(), block_size as usize);
            for b in slice {
                assert_eq!(*b, 1)
            }

            let slice = buf.filled(15..agg_len);
            assert_eq!(slice.len(), block_size as usize - 15);
            for b in slice {
                assert_eq!(*b, 1)
            }

            let slice = buf.filled((agg_len / 2)..agg_len);
            assert_eq!(slice.len(), block_size as usize);
            for b in slice {
                assert_eq!(*b, 2)
            }
        }
    }

    #[test]
    fn agg_nom_traits() {
        use nom::{Compare, InputLength, InputTake};

        let buf: AggBuf = Default::default();
        let hello = "hello";
        let world = "world";

        {
            let mut buf = buf.write();
            buf.grow_if_needed().unwrap();

            {
                let dst = buf.unfilled_mut();
                let dst_len = dst.len();
                let mut src = b"#".repeat(dst_len);
                src[(dst_len - hello.len())..].copy_from_slice(hello.as_bytes());
                dst.copy_from_slice(&src);

                buf.advance(dst_len as _);
            }

            buf.grow_if_needed().unwrap();

            {
                let dst = buf.unfilled_mut();
                dst[..world.len()].copy_from_slice(world.as_bytes());
                buf.advance(world.len() as _);
            }
        }

        let block_size = buf.write().block_size;
        let start = block_size - hello.len() as u32;
        let end = start + hello.len() as u32 + world.len() as u32;
        let slice = buf.read().slice(start..end);
        assert_eq!(slice.input_len(), hello.len() + world.len());

        eprintln!("to_vec + compare owned");
        assert_eq!(slice.to_string_lossy(), "helloworld");

        assert_eq!(slice.compare(b"that's not it"), nom::CompareResult::Error);
        assert_eq!(slice.compare(b"hello"), nom::CompareResult::Ok);
        eprintln!("helloworld nom::Compare");
        assert_eq!(slice.compare(b"helloworld"), nom::CompareResult::Ok);
        assert_eq!(
            slice.compare(b"helloworldwoops"),
            nom::CompareResult::Incomplete
        );

        {
            let hello_slice = slice.take(5);
            dbg2!(hello_slice.to_string_lossy());
            assert_eq!(hello_slice.compare(b"hello"), nom::CompareResult::Ok);
        }

        {
            let (world_slice, hello_slice) = slice.take_split(5);
            dbg2!(hello_slice.to_string_lossy());
            dbg2!(world_slice.to_string_lossy());
            assert_eq!(hello_slice.compare(b"hello"), nom::CompareResult::Ok);
            assert_eq!(world_slice.compare(b"world"), nom::CompareResult::Ok);
        }

        // TODO: test `compare_no_case`
    }

    #[test]
    fn agg_nom_sample() {
        fn parse(i: AggSlice) -> IResult<AggSlice, AggSlice> {
            nom::bytes::streaming::tag(&b"HTTP/1.1 200 OK"[..])(i)
        }

        let mut buf: AggBuf = Default::default();

        let input = b"HTTP/1.1 200 OK".repeat(1000);
        let mut pending = &input[..];

        loop {
            let slice = buf.read().slice(0..buf.read().len());
            let (rest, version) = match parse(slice) {
                Ok(t) => t,
                Err(e) => {
                    if e.is_incomplete() {
                        {
                            if pending.is_empty() {
                                println!("ran out of input");
                                break;
                            }

                            let mut buf = buf.write();
                            buf.grow_if_needed().unwrap();
                            let unfilled = buf.unfilled_mut();
                            let n = std::cmp::min(unfilled.len(), pending.len());
                            unfilled[..n].copy_from_slice(&pending[..n]);
                            pending = &pending[n..];
                            buf.advance(n as _);
                            println!("advanced by {n}, {} remaining", pending.len());
                        }

                        continue;
                    }
                    panic!("parsing error: {e}");
                }
            };
            assert_eq!(version.to_string_lossy(), "HTTP/1.1 200 OK");

            buf = buf.split_at(rest);
        }
    }
}
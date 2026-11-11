use std::{
    cmp::{Ordering, max},
    collections::{BinaryHeap, binary_heap::PeekMut},
    mem,
};

use bytes::{Buf, Bytes, BytesMut};

use crate::range_set::RangeSet;

/// Helper to assemble unordered stream frames into an ordered stream
#[derive(Debug, Default)]
pub(super) struct Assembler {
    state: State,
    data: BinaryHeap<Buffer>,
    /// Total number of buffered bytes, including duplicates in ordered mode.
    buffered: usize,
    /// Estimated number of allocated bytes, will never be less than `buffered`.
    allocated: usize,
    /// Number of bytes read by the application. When only ordered reads have been used, this is the
    /// length of the contiguous prefix of the stream which has been consumed by the application,
    /// aka the stream offset.
    bytes_read: u64,
    end: u64,
}

impl Assembler {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Reset to the initial state
    pub(super) fn reinit(&mut self) {
        let old_data = mem::take(&mut self.data);
        *self = Self::default();
        self.data = old_data;
        self.data.clear();
        self.shrink_metadata();
    }

    pub(super) fn ensure_ordering(&mut self, ordered: bool) -> Result<(), IllegalOrderedRead> {
        if ordered && !self.state.is_ordered() {
            return Err(IllegalOrderedRead);
        } else if !ordered && self.state.is_ordered() {
            // Enter unordered mode
            if !self.data.is_empty() {
                // Get rid of possible duplicates
                self.defragment();
            }
            let mut recvd = RangeSet::new();
            recvd.insert(0..self.bytes_read);
            for chunk in &self.data {
                recvd.insert(chunk.offset..chunk.offset + chunk.bytes.len() as u64);
            }
            self.state = State::Unordered { recvd };
        }
        Ok(())
    }

    /// Get the the next chunk
    pub(super) fn read(&mut self, max_length: usize, ordered: bool) -> Option<Chunk> {
        loop {
            let mut chunk = self.data.peek_mut()?;

            if ordered {
                if chunk.offset > self.bytes_read {
                    // Next chunk is after current read index
                    return None;
                } else if (chunk.offset + chunk.bytes.len() as u64) <= self.bytes_read {
                    // Next chunk is useless as the read index is beyond its end
                    self.buffered -= chunk.bytes.len();
                    self.allocated -= chunk.allocation_size;
                    PeekMut::pop(chunk);
                    self.shrink_metadata();
                    continue;
                }

                // Determine `start` and `len` of the slice of useful data in chunk
                let start = (self.bytes_read - chunk.offset) as usize;
                if start > 0 {
                    chunk.bytes.advance(start);
                    chunk.offset += start as u64;
                    self.buffered -= start;
                }
            }

            let result = if max_length < chunk.bytes.len() {
                self.bytes_read += max_length as u64;
                let offset = chunk.offset;
                chunk.offset += max_length as u64;
                self.buffered -= max_length;
                let result = Chunk::new(offset, chunk.bytes.split_to(max_length));
                drop(chunk);
                result
            } else {
                self.bytes_read += chunk.bytes.len() as u64;
                self.buffered -= chunk.bytes.len();
                self.allocated -= chunk.allocation_size;
                let chunk = PeekMut::pop(chunk);
                Chunk::new(chunk.offset, chunk.bytes)
            };
            self.shrink_metadata();
            return Some(result);
        }
    }

    /// Bound metadata by the receive-window bytes, with a fixed allowance for
    /// small frames. Before compaction `buffered` may include duplicates, so
    /// cap it by the unread offset span and recompute after deduplication.
    fn chunk_limit(&self) -> usize {
        let span = usize::try_from(self.end - self.bytes_read).unwrap_or(usize::MAX);
        BASE_CHUNKS.saturating_add(self.buffered.min(span) / MIN_RETAINED_CHUNK_SIZE)
    }

    /// Release metadata paid for by bytes that the application has consumed.
    /// Geometric slack amortizes shrinking while a sparse tail remains.
    fn shrink_metadata(&mut self) {
        if self.data.is_empty() {
            self.data.shrink_to(BASE_CHUNKS);
        } else {
            let limit = self.chunk_limit();
            if self.data.capacity() > limit.saturating_mul(4) {
                self.data.shrink_to(limit.saturating_mul(2));
            }
        }
    }

    /// Copy fragmented chunk data to new chunks backed by a single buffer
    ///
    /// This makes sure we're not unnecessarily holding on to many larger allocations.
    /// We merge contiguous chunks in the process of doing so.
    fn defragment(&mut self) {
        // Keep well-utilized large allocations on the common path.
        let min_chunk_size = max(
            self.buffered.div_ceil(self.chunk_limit()),
            MIN_RETAINED_CHUNK_SIZE,
        );
        self.defragment_with_min_chunk_size(min_chunk_size);
        if self.data.len() > self.chunk_limit() {
            // Retained large chunks may be adjacent to each other or to copied
            // small chunks. Count actual disjoint spans before rejecting a valid
            // receive window; the size threshold alone cannot guarantee the cap.
            self.defragment_with_min_chunk_size(usize::MAX);
        }
        self.shrink_metadata();
    }

    fn defragment_with_min_chunk_size(&mut self, min_chunk_size: usize) {
        let new = BinaryHeap::with_capacity(self.data.len());
        let old = mem::replace(&mut self.data, new);
        let mut buffers = old.into_sorted_vec();
        self.buffered = 0;
        let mut fragmented_buffered = 0;
        let mut offset = 0;
        for chunk in buffers.iter_mut().rev() {
            chunk.try_mark_defragment(offset);
            let size = chunk.bytes.len();
            offset = chunk.offset + size as u64;
            self.buffered += size;
            if !chunk.defragmented || size < min_chunk_size {
                fragmented_buffered += size;
            }
        }
        self.allocated = self.buffered;
        let mut buffer = BytesMut::with_capacity(fragmented_buffered);
        let mut offset = 0;
        for chunk in buffers.into_iter().rev() {
            // bytes might be empty after try_mark_defragment
            if chunk.bytes.is_empty() {
                continue;
            }
            if chunk.defragmented && chunk.bytes.len() >= min_chunk_size {
                self.data.push(chunk);
                continue;
            }
            // Overlap is resolved by try_mark_defragment
            if chunk.offset != offset + (buffer.len() as u64) {
                if !buffer.is_empty() {
                    self.data
                        .push(Buffer::new_defragmented(offset, buffer.split().freeze()));
                }
                offset = chunk.offset;
            }
            buffer.extend_from_slice(&chunk.bytes);
        }
        if !buffer.is_empty() {
            self.data
                .push(Buffer::new_defragmented(offset, buffer.split().freeze()));
        }
    }

    // Note: If a packet contains many frames from the same stream, the estimated over-allocation
    // will be much higher because we are counting the same allocation multiple times.
    pub(super) fn insert(
        &mut self,
        mut offset: u64,
        mut bytes: Bytes,
        allocation_size: usize,
    ) -> Result<(), TooManyChunks> {
        debug_assert!(
            bytes.len() <= allocation_size,
            "allocation_size less than bytes.len(): {:?} < {:?}",
            allocation_size,
            bytes.len()
        );
        self.end = self.end.max(offset + bytes.len() as u64);
        if let State::Unordered { ref mut recvd } = self.state {
            // Discard duplicate data
            for duplicate in recvd.replace(offset..offset + bytes.len() as u64) {
                if duplicate.start > offset {
                    let buffer = Buffer::new(
                        offset,
                        bytes.split_to((duplicate.start - offset) as usize),
                        allocation_size,
                    );
                    self.buffered += buffer.bytes.len();
                    self.allocated += buffer.allocation_size;
                    self.data.push(buffer);
                    offset = duplicate.start;
                }
                bytes.advance((duplicate.end - offset) as usize);
                offset = duplicate.end;
            }
        } else if offset < self.bytes_read {
            if (offset + bytes.len() as u64) <= self.bytes_read {
                return Ok(());
            } else {
                let diff = self.bytes_read - offset;
                offset += diff;
                bytes.advance(diff as usize);
            }
        }

        // No early return when empty: the dedup loop above may already have pushed chunks.
        if !bytes.is_empty() {
            let buffer = Buffer::new(offset, bytes, allocation_size);
            self.buffered += buffer.bytes.len();
            self.allocated += buffer.allocation_size;
            self.data.push(buffer);
        }
        // `self.buffered` also counts duplicate bytes, therefore we use
        // `self.end - self.bytes_read` as an upper bound of buffered unique
        // bytes. This will cause a defragmentation if the amount of duplicate
        // bytes exceedes a proportion of the receive window size.
        let buffered = self.buffered.min((self.end - self.bytes_read) as usize);
        let over_allocation = self.allocated - buffered;
        // Rationale: on the one hand, we want to defragment rarely, ideally never
        // in non-pathological scenarios. However, a pathological or malicious
        // peer could send us one-byte frames, and since we use reference-counted
        // buffers in order to prevent copying, this could result in keeping a lot
        // of memory allocated. This limits over-allocation in proportion to the
        // buffered data. The constants are chosen somewhat arbitrarily and try to
        // balance between defragmentation overhead and over-allocation.
        let threshold = 32768.max(buffered * 3 / 2);
        // Small gapped frames hold over-allocation below the threshold, so bound the count too.
        if over_allocation > threshold || self.data.len() > self.chunk_limit().saturating_mul(2) {
            self.defragment();
            // Recompute the byte-funded limit after duplicates and overlaps are removed.
            if self.data.len() > self.chunk_limit() {
                return Err(TooManyChunks);
            }
        }

        Ok(())
    }

    /// Number of bytes consumed by the application
    pub(super) fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Whether ordered consumption has not reached the highest received offset.
    pub(super) fn has_unread_data(&self) -> bool {
        self.end > self.bytes_read
    }

    /// Discard all buffered data
    pub(super) fn clear(&mut self) {
        self.data.clear();
        self.buffered = 0;
        self.allocated = 0;
        self.shrink_metadata();
    }
}

/// A chunk of data from the receive stream
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk {
    /// The offset in the stream
    pub offset: u64,
    /// The contents of the chunk
    pub bytes: Bytes,
}

impl Chunk {
    fn new(offset: u64, bytes: Bytes) -> Self {
        Self { offset, bytes }
    }
}

#[derive(Debug, Eq)]
struct Buffer {
    offset: u64,
    bytes: Bytes,
    /// Size of the allocation behind `bytes`, if `defragmented == false`.
    /// Otherwise this will be set to `bytes.len()` by `try_mark_defragment`.
    /// Will never be less than `bytes.len()`.
    allocation_size: usize,
    defragmented: bool,
}

impl Buffer {
    /// Constructs a new fragmented Buffer
    fn new(offset: u64, bytes: Bytes, allocation_size: usize) -> Self {
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: false,
        }
    }

    /// Constructs a new defragmented Buffer
    fn new_defragmented(offset: u64, bytes: Bytes) -> Self {
        let allocation_size = bytes.len();
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: true,
        }
    }

    /// Discards data before `offset` and flags `self` as defragmented if it has good utilization
    fn try_mark_defragment(&mut self, offset: u64) {
        let duplicate = offset.saturating_sub(self.offset) as usize;
        self.offset = self.offset.max(offset);
        if duplicate >= self.bytes.len() {
            // All bytes are duplicate
            self.bytes = Bytes::new();
            self.defragmented = true;
            self.allocation_size = 0;
            return;
        }
        self.bytes.advance(duplicate);
        // Make sure that fragmented buffers with high utilization become defragmented and
        // defragmented buffers remain defragmented
        self.defragmented = self.defragmented || self.bytes.len() * 6 / 5 >= self.allocation_size;
        if self.defragmented {
            // Make sure that defragmented buffers do not contribute to over-allocation
            self.allocation_size = self.bytes.len();
        }
    }
}

impl Ord for Buffer {
    // Invert ordering based on offset (max-heap, min offset first),
    // prioritize longer chunks at the same offset.
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset
            .cmp(&other.offset)
            .reverse()
            .then(self.bytes.len().cmp(&other.bytes.len()))
    }
}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        (self.offset, self.bytes.len()) == (other.offset, other.bytes.len())
    }
}

#[derive(Debug, Default)]
enum State {
    #[default]
    Ordered,
    Unordered {
        /// The set of offsets that have been received from the peer, including portions not yet
        /// read by the application.
        recvd: RangeSet,
    },
}

impl State {
    fn is_ordered(&self) -> bool {
        matches!(self, Self::Ordered)
    }
}

/// Error indicating that an ordered read was performed on a stream after an unordered read
#[derive(Debug)]
pub struct IllegalOrderedRead;

/// Error indicating that too many chunks are buffered due to maliciously small/gapped frames
#[derive(Debug)]
pub(crate) struct TooManyChunks;

/// Base metadata allowance for a stream, including tiny or malicious frames.
///
/// Every additional MIN_RETAINED_CHUNK_SIZE buffered bytes funds one more node.
/// This keeps metadata proportional to flow-controlled bytes while admitting
/// normal datagram-sized gaps in multi-MiB receive windows.
const BASE_CHUNKS: usize = 1024;

/// Minimum size of a defragmented chunk that is retained without coalescing.
///
/// Chunks below this size will be copied on every compaction. 128 bytes balances
/// being cheap to copy while reducing the overhead of many small buffer nodes.
const MIN_RETAINED_CHUNK_SIZE: usize = 128;

/// Test workload scale. Runtime compaction uses twice the current byte-funded
/// chunk budget, so mergeable frames do not force a compaction on every insert.
#[cfg(test)]
const COMPACT_THRESHOLD: usize = 2 * BASE_CHUNKS;

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn assemble_ordered() {
        let mut x = Assembler::new();
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 1), Some(ref y) if &y[..] == b"1");
        assert_matches!(next(&mut x, 3), Some(ref y) if &y[..] == b"23");
        x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        x.insert(6, Bytes::from_static(b"789"), 3).unwrap();
        x.insert(9, Bytes::from_static(b"10"), 2).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"789");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"10");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_unordered() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains() {
        let mut x = Assembler::new();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains_compact() {
        let mut x = Assembler::new();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"4");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 4).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 4).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_old() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
        x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
        x.insert(9, Bytes::from_static(b"jkl"), 4).unwrap();
        x.insert(12, Bytes::from_static(b"mno"), 4).unwrap();
        x.defragment();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abcdef"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jklmno"))
        );
    }

    #[test]
    fn defrag_with_missing_prefix() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        x.defragment();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
    }

    #[test]
    fn defrag_read_chunk() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
        x.insert(7, Bytes::from_static(b"hij"), 4).unwrap();
        x.insert(11, Bytes::from_static(b"lmn"), 4).unwrap();
        x.defragment();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"abcdef");
        x.insert(5, Bytes::from_static(b"fghijklmn"), 9).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"ghijklmn");
        x.insert(13, Bytes::from_static(b"nopq"), 4).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"opq");
        x.insert(15, Bytes::from_static(b"pqrs"), 4).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"rs");
        assert_matches!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn unordered_after_ordered_read_drops_a_stale_duplicate() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_matches!(next(&mut x, 3), Some(y) if &y[..] == b"abc");
        assert_eq!(x.bytes_read(), 3);
        x.ensure_ordering(false).unwrap();
        assert_eq!(x.read(usize::MAX, false), None);
        assert_eq!(x.bytes_read(), 3);
    }

    #[test]
    fn unordered_after_partial_ordered_read_keeps_only_unread_bytes() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abcdef"), 6).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_matches!(next(&mut x, 4), Some(y) if &y[..] == b"abcd");
        x.ensure_ordering(false).unwrap();
        // Only the unread suffix remains buffered and accounted for
        assert_eq!(x.buffered, 2);
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(4, Bytes::from_static(b"ef"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        assert_eq!(x.buffered, 0);
        assert_eq!(x.bytes_read(), 6);
    }

    #[test]
    fn unordered_happy_path() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abc"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn unordered_dedup() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"abcdefghi"), 9).unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"a"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(1, Bytes::from_static(b"bc"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(6, Bytes::from_static(b"ghi"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jkl"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(12, Bytes::from_static(b"mno"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn chunks_dedup() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        x.insert(1, Bytes::from_static(b"bcdefghi"), 9).unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abcd")))
        );
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(4, Bytes::from_static(b"efghi")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(9, Bytes::from_static(b"jkl")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(12, Bytes::from_static(b"mno")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn ordered_eager_discard() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_eq!(x.data.len(), 1);
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        x.insert(0, Bytes::from_static(b"ab"), 2).unwrap();
        assert_eq!(x.data.len(), 0);
        x.insert(2, Bytes::from_static(b"cd"), 2).unwrap();
        assert_eq!(
            x.data.peek(),
            Some(&Buffer::new(3, Bytes::from_static(b"d"), 2))
        );
    }

    #[test]
    fn ordered_insert_unordered_read() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        x.ensure_ordering(false).unwrap();
        assert_eq!(
            x.read(3, false),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        assert_eq!(x.read(3, false), None);
    }

    #[test]
    fn ordered_lossless_stream_is_not_rejected() {
        const FRAME_LEN: usize = 32;

        let mut x = Assembler::new();
        let data = Bytes::from(vec![0; FRAME_LEN]);
        let mut offset = 0u64;
        while offset < (2 * COMPACT_THRESHOLD * FRAME_LEN) as u64 {
            x.insert(offset, data.clone(), FRAME_LEN)
                .expect("contiguous lossless data must not be rejected");
            offset += FRAME_LEN as u64;
        }
    }

    #[test]
    fn defrag_respects_byte_funded_chunk_budget() {
        let mut x = Assembler::new();
        let data = Bytes::from(vec![0; MIN_RETAINED_CHUNK_SIZE]);
        let mut offset = 0;
        // Fill with minimium sized chunks.
        for _ in 0..BASE_CHUNKS {
            x.insert(offset, data.clone(), data.len()).unwrap();
            offset += data.len() as u64;
        }
        // Add one extra chunk. Metadata remains within the byte-funded budget,
        // whether contiguous retained buffers need copying or not.
        x.insert(offset, Bytes::from_static(b"x"), 1).unwrap();
        x.defragment();
        assert!(x.data.len() <= x.chunk_limit());
    }

    #[test]
    fn bounded_chunks_under_low_over_allocation() {
        // Gapped frames whose `allocation_size` equals their length hold
        // `over_allocation` at zero, so that trigger never fires.
        let mut x = Assembler::new();
        // Withhold offset 0 so an ordered reader can never drain anything.
        let mut offset = 1u64;
        let mut result = Ok(());
        for _ in 0..(BASE_CHUNKS * 8) {
            result = x.insert(offset, Bytes::from_static(b"gap"), 3);
            if result.is_err() {
                break;
            }
            offset += 3 + 1; // 3 data bytes, 1 byte gap
        }
        assert_matches!(result, Err(TooManyChunks));
        assert!(
            x.data.len() <= x.chunk_limit().saturating_mul(2) + 1,
            "chunk count {} exceeded the bound",
            x.data.len()
        );
    }

    #[test]
    fn bounded_chunks_unordered_overlap_flood() {
        // Overlapping frames whose tail is already received: the dedup loop pushes
        // the fresh head byte and leaves `bytes` empty, and `end` never rises, so
        // the flood costs the peer no flow control.
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        let top = 1_000_000u64;
        x.insert(top, Bytes::from_static(b"ab"), 2).unwrap();
        for k in 0..(4 * BASE_CHUNKS as u64) {
            x.insert(top - k - 1, Bytes::from_static(b"ab"), 2).unwrap();
            assert!(
                x.data.len() <= x.chunk_limit().saturating_mul(2) + 1,
                "chunk count {} exceeded the bound at k={k}",
                x.data.len()
            );
        }
    }

    #[test]
    fn bounded_chunks_duplicate_flood() {
        // Duplicates against a stream already at the cap. Ordered mode does not dedup,
        // so each one pushes a chunk; they must not be rejected, or compact every frame.
        let mut x = Assembler::new();
        // Withhold offset 0 so nothing can be drained.
        for i in 0..BASE_CHUNKS as u64 {
            x.insert(1 + i * 4, Bytes::from_static(b"abc"), 3).unwrap();
        }
        let mut max_len = x.data.len();
        for _ in 0..(3 * BASE_CHUNKS) {
            x.insert(1, Bytes::from_static(b"abc"), 3)
                .expect("duplicate flood must not be rejected");
            max_len = max_len.max(x.data.len());
            assert!(
                x.data.len() <= x.chunk_limit().saturating_mul(2) + 1,
                "chunk count {} exceeded the bound",
                x.data.len()
            );
        }
        // Compacting on every frame would pin the count at `BASE_CHUNKS`.
        assert!(
            max_len > BASE_CHUNKS,
            "buffer compacted on every frame (max observed len {max_len})"
        );
    }

    #[test]
    fn large_contiguous_prefix_with_sparse_tail_survives_compaction() {
        let mut x = Assembler::new();
        let large = Bytes::from(vec![0x61; 4096]);
        let small = Bytes::from(vec![0x62; 32]);
        // A delayed first byte holds a valid receive window. The large prefix
        // is contiguous; the tail adds only 600 genuine one-byte holes.
        let mut offset = 1u64;
        for _ in 0..600 {
            x.insert(offset, large.clone(), large.len()).unwrap();
            offset += large.len() as u64;
        }
        for _ in 0..600 {
            offset += 1;
            x.insert(offset, small.clone(), small.len()).unwrap();
            offset += small.len() as u64;
        }
        let last = offset - small.len() as u64;
        // Retransmissions may duplicate stream bytes with fresh packet numbers.
        // They must not turn 601 genuine spans into a connection-fatal limit.
        for _ in 0..1000 {
            x.insert(last, small.clone(), small.len())
                .expect("legal stream with 601 receive spans must survive compaction");
        }
        x.defragment();
        assert!(x.data.len() <= x.chunk_limit());
        x.insert(0, Bytes::from_static(b"p"), 1).unwrap();
        let mut gap = 1 + 600 * large.len() as u64;
        for _ in 0..600 {
            x.insert(gap, Bytes::from_static(b"g"), 1).unwrap();
            gap += 1 + small.len() as u64;
        }
        let mut received = Vec::new();
        while let Some(chunk) = x.read(usize::MAX, true) {
            assert_eq!(chunk.offset, received.len() as u64);
            received.extend_from_slice(&chunk.bytes);
        }
        let mut expected = vec![b'p'];
        expected.extend(std::iter::repeat_n(b'a', 600 * large.len()));
        for _ in 0..600 {
            expected.push(b'g');
            expected.extend_from_slice(&small);
        }
        assert_eq!(received, expected);
        assert_eq!(x.buffered, 0);
        assert_eq!(x.allocated, 0);
    }

    #[test]
    fn byte_funded_metadata_is_released_after_drain_clear_and_reinit() {
        for action in ["drain", "clear", "reinit"] {
            let mut assembler = Assembler::new();
            let payload = Bytes::from(vec![0x61; 1200]);
            for index in 0..5000u64 {
                assembler
                    .insert(1 + index * 2400, payload.clone(), payload.len())
                    .unwrap();
            }
            assert!(assembler.data.capacity() > BASE_CHUNKS);
            match action {
                "drain" => {
                    assembler.insert(0, Bytes::from_static(b"p"), 1).unwrap();
                    for index in 0..5000u64 {
                        assembler
                            .insert(1201 + index * 2400, payload.clone(), payload.len())
                            .unwrap();
                    }
                    let mut total = 0;
                    while let Some(chunk) = assembler.read(usize::MAX, true) {
                        assert_eq!(chunk.offset, total);
                        total += chunk.bytes.len() as u64;
                    }
                    assert_eq!(total, 1 + 5000 * 2400);
                }
                "clear" => assembler.clear(),
                "reinit" => assembler.reinit(),
                _ => unreachable!(),
            }
            assert_eq!(assembler.buffered, 0);
            assert_eq!(assembler.allocated, 0);
            assert!(
                assembler.data.capacity() <= BASE_CHUNKS,
                "{action} retained metadata without payload"
            );
        }
    }

    #[test]
    fn empty_offset_space_does_not_fund_tiny_gap_metadata() {
        let mut assembler = Assembler::new();
        assembler
            .insert(1 << 30, Bytes::from_static(b"x"), 1)
            .unwrap();
        let mut rejected = false;
        for index in 0..4096u64 {
            if assembler
                .insert(1 + index * 4, Bytes::from_static(b"abc"), 3)
                .is_err()
            {
                rejected = true;
                break;
            }
        }
        assert!(
            rejected,
            "offset space without payload must not bypass the small-frame bound"
        );
        assert!(
            assembler.data.len() <= 4096,
            "tiny-frame metadata must remain bounded"
        );
    }

    #[test]
    fn receive_window_sized_datagrams_survive_sparse_delivery() {
        let mut assembler = Assembler::new();
        let payload = Bytes::from(vec![0x61; 1200]);
        // Three MiB of received payload, with a delayed datagram between each
        // pair. This geometry fits the HY2 receive window and comes from a
        // fixed large-datagram loss pattern, not tiny malicious stream frames.
        for index in 0..2500u64 {
            assembler
                .insert(1 + index * 2400, payload.clone(), payload.len())
                .expect("legal datagram-sized sparse delivery must survive");
        }
        assembler.insert(0, Bytes::from_static(b"p"), 1).unwrap();
        for index in 0..2500u64 {
            assembler
                .insert(1201 + index * 2400, payload.clone(), payload.len())
                .unwrap();
        }
        let mut total = 0;
        while let Some(chunk) = assembler.read(usize::MAX, true) {
            assert_eq!(chunk.offset, total);
            if total == 0 {
                assert_eq!(chunk.bytes[0], b'p');
                assert!(chunk.bytes[1..].iter().all(|b| *b == 0x61));
            } else {
                assert!(chunk.bytes.iter().all(|b| *b == 0x61));
            }
            total += chunk.bytes.len() as u64;
        }
        assert_eq!(total, 1 + 2500 * 2400);
        assert_eq!(assembler.buffered, 0);
        assert_eq!(assembler.allocated, 0);
    }

    #[test]
    fn retained_prefix_funds_bounded_sparse_tail_metadata() {
        let mut x = Assembler::new();
        let large = Bytes::from(vec![0x61; 4096]);
        let small = Bytes::from(vec![0x62; 32]);
        let mut offset = 1;
        for _ in 0..600 {
            x.insert(offset, large.clone(), large.len()).unwrap();
            offset += large.len() as u64;
        }
        for _ in 0..1100 {
            offset += 1;
            x.insert(offset, small.clone(), small.len()).unwrap();
            offset += small.len() as u64;
        }
        for _ in 0..1000 {
            x.insert(offset - small.len() as u64, small.clone(), small.len())
                .expect("the large received prefix funds bounded tail metadata");
        }
        x.defragment();
        assert!(x.data.len() <= x.chunk_limit());
        // Duplicates do not contribute to the compacted byte budget.
        assert_eq!(x.buffered, 600 * large.len() + 1100 * small.len());
        assert!(x.chunk_limit() <= BASE_CHUNKS + x.buffered / MIN_RETAINED_CHUNK_SIZE);
    }

    fn next_unordered(x: &mut Assembler) -> Chunk {
        x.read(usize::MAX, false).unwrap()
    }

    fn next(x: &mut Assembler, size: usize) -> Option<Bytes> {
        x.read(size, true).map(|chunk| chunk.bytes)
    }
}

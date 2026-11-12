//! EXPERIMENT: a single-producer single-consumer ring of fixed-size slots in a shared file, ported from
//! the in-process `SpscQueue` of github.com/bruno-medeiros/rust-low-latency-hft (lockfree-queue/src/spsc.rs).
//! The provider owns `tail`, the strategy owns `head`; one slot stays empty to tell full from empty.
//!
//! ```text
//! offset 0          64         128              192                         192 + SLOT_COUNT * 128
//!        +----------+----------+----------------+---------+---------+-------+
//!        | tail u32 | head u32 | metadata, 64 B | slot 0  | slot 1  | ...   |
//!        | (writer) | (reader) |                | 128 B   | 128 B   |       |
//!        +----------+----------+----------------+---------+---------+-------+
//!
//! metadata: MDPRING magic (8 B), FORMAT_VERSION (u32 LE), MarketDataType (u8), zeros.
//! slot:     payload length (u32), zeros to byte 16, then the rkyv Bbo or Trade (at most 112 B).
//! ```
use std::fs::File;
use std::ops::Range;
use std::ptr::copy_nonoverlapping;
use std::slice;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{Result, anyhow, ensure};
use memmap2::{MmapMut, MmapOptions};

use crate::types::MarketDataType;

const CACHE_LINE_SIZE: usize = 64;
const TAIL_OFFSET: usize = 0;
const HEAD_OFFSET: usize = CACHE_LINE_SIZE;
const METADATA_LENGTH: usize = CACHE_LINE_SIZE;
const METADATA_RANGE: Range<usize> = 2 * CACHE_LINE_SIZE..2 * CACHE_LINE_SIZE + METADATA_LENGTH;
const SLOTS_OFFSET: usize = METADATA_RANGE.end;

// Two cache lines; rkyv needs the payload 16-byte aligned, so the length field is padded to 16 bytes.
const SLOT_SIZE: usize = 2 * CACHE_LINE_SIZE;
const SLOT_HEADER_LENGTH: usize = 16;
/// Largest rkyv payload a slot holds.
pub const SLOT_PAYLOAD_LENGTH: usize = SLOT_SIZE - SLOT_HEADER_LENGTH;

// 8 MiB of slots, the size of the data area of the bcast ring this replaces.
pub(super) const SLOT_COUNT: u32 = 1 << 16;
const INDEX_MASK: u32 = SLOT_COUNT - 1;

// Must never change while strategies run: they keep their old mapping and would break.
pub(super) const RING_FILE_LENGTH: u64 = (SLOTS_OFFSET + SLOT_COUNT as usize * SLOT_SIZE) as u64;

const METADATA_MAGIC: [u8; 8] = *b"MDPRING\0";
const METADATA_MAGIC_RANGE: Range<usize> = 0..8;
const METADATA_VERSION_RANGE: Range<usize> = 8..12;
const METADATA_MARKET_DATA_TYPE_INDEX: usize = 12;
const FORMAT_VERSION: u32 = 2;

/// The mapped ring file. The writer side calls only `try_push`, the reader side only `try_pop` and
/// `skip_to_tail`: one thread each.
pub(super) struct Ring {
    memory_map: MmapMut,
}

impl Ring {
    /// Maps a file that is already `RING_FILE_LENGTH` bytes long.
    pub(super) fn map(file: &File) -> Result<Self> {
        let memory_map = unsafe { MmapOptions::new().map_mut(file)? };
        Ok(Self { memory_map })
    }

    /// The market data type the ring holds; fails unless this format version wrote it.
    pub(super) fn market_data_type(&self) -> Result<MarketDataType> {
        read_market_data_type(&self.memory_map[METADATA_RANGE])
    }

    /// Empties the ring and marks it as holding `market_data_type`.
    pub(super) fn reset(&mut self, market_data_type: MarketDataType) {
        self.tail().store(0, Ordering::Release);
        self.head().store(0, Ordering::Release);
        write_metadata(&mut self.memory_map[METADATA_RANGE], market_data_type);
    }

    /// Returns false, writing nothing, when the reader has left no free slot.
    pub(super) fn try_push(&self, payload: &[u8]) -> bool {
        let length = payload.len();
        assert!(
            length <= SLOT_PAYLOAD_LENGTH,
            "a {length} byte payload does not fit a slot"
        );
        let tail = self.tail().load(Ordering::Relaxed);
        let next_tail = tail.wrapping_add(1) & INDEX_MASK;
        if next_tail == self.head().load(Ordering::Acquire) {
            return false;
        }
        let slot = self.slot(tail);
        // SAFETY: the slot lies inside the mapping, and the reader does not touch it until `tail` moves past it.
        unsafe {
            slot.cast::<u32>().write(length as u32);
            copy_nonoverlapping(payload.as_ptr(), slot.add(SLOT_HEADER_LENGTH), length);
        }
        self.tail().store(next_tail, Ordering::Release);
        true
    }

    /// Hands the oldest unread payload to `read` in place, then frees its slot.
    pub(super) fn try_pop<Value>(&self, read: impl FnOnce(&[u8]) -> Value) -> Option<Value> {
        let head = self.head().load(Ordering::Relaxed);
        if head == self.tail().load(Ordering::Acquire) {
            return None;
        }
        let slot = self.slot(head);
        // SAFETY: the slot lies inside the mapping, and the writer does not reuse it until `head` moves past it.
        let payload = unsafe {
            let length = (slot.cast::<u32>().read() as usize).min(SLOT_PAYLOAD_LENGTH);
            slice::from_raw_parts(slot.add(SLOT_HEADER_LENGTH), length)
        };
        let value = read(payload);
        self.head()
            .store(head.wrapping_add(1) & INDEX_MASK, Ordering::Release);
        Some(value)
    }

    /// Drops everything unread, so a starting reader sees only what is published from now on.
    pub(super) fn skip_to_tail(&self) {
        let tail = self.tail().load(Ordering::Acquire);
        self.head().store(tail, Ordering::Release);
    }

    fn tail(&self) -> &AtomicU32 {
        self.index(TAIL_OFFSET)
    }

    fn head(&self) -> &AtomicU32 {
        self.index(HEAD_OFFSET)
    }

    fn index(&self, offset: usize) -> &AtomicU32 {
        // SAFETY: the offset is cache-line aligned inside the page-aligned mapping, and both
        // processes access these four bytes only as an `AtomicU32`.
        unsafe { &*self.memory_map.as_ptr().add(offset).cast::<AtomicU32>() }
    }

    fn slot(&self, index: u32) -> *mut u8 {
        // SAFETY: `index` is masked to `SLOT_COUNT`, so the slot lies inside the mapping.
        unsafe {
            self.memory_map
                .as_ptr()
                .cast_mut()
                .add(SLOTS_OFFSET + index as usize * SLOT_SIZE)
        }
    }
}

fn write_metadata(metadata: &mut [u8], market_data_type: MarketDataType) {
    metadata[METADATA_MAGIC_RANGE].copy_from_slice(&METADATA_MAGIC);
    metadata[METADATA_VERSION_RANGE].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    metadata[METADATA_MARKET_DATA_TYPE_INDEX] = market_data_type as u8;
}

fn read_market_data_type(metadata: &[u8]) -> Result<MarketDataType> {
    ensure!(
        metadata[METADATA_MAGIC_RANGE] == METADATA_MAGIC,
        "metadata lacks the ring magic"
    );
    ensure!(
        metadata[METADATA_VERSION_RANGE] == FORMAT_VERSION.to_le_bytes(),
        "metadata is not format version {FORMAT_VERSION}"
    );
    let code = metadata[METADATA_MARKET_DATA_TYPE_INDEX];
    [
        MarketDataType::Bbo,
        MarketDataType::Trade,
        MarketDataType::AggregateTrade,
    ]
    .into_iter()
    .find(|market_data_type| *market_data_type as u8 == code)
    .ok_or_else(|| anyhow!("metadata holds unknown market data type code {code}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trips_each_data_type() {
        for market_data_type in [
            MarketDataType::Bbo,
            MarketDataType::Trade,
            MarketDataType::AggregateTrade,
        ] {
            let mut metadata = [0; METADATA_LENGTH];
            write_metadata(&mut metadata, market_data_type);
            assert_eq!(
                market_data_type,
                read_market_data_type(&metadata).expect("written metadata is expected to be read")
            );
        }
    }

    #[test]
    fn foreign_or_corrupt_metadata_is_refused() {
        let blank = [0; METADATA_LENGTH];
        assert!(read_market_data_type(&blank).is_err());

        let mut wrong_version = [0; METADATA_LENGTH];
        write_metadata(&mut wrong_version, MarketDataType::Bbo);
        wrong_version[METADATA_VERSION_RANGE].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        assert!(read_market_data_type(&wrong_version).is_err());

        let mut unknown_data_type = [0; METADATA_LENGTH];
        write_metadata(&mut unknown_data_type, MarketDataType::Bbo);
        unknown_data_type[METADATA_MARKET_DATA_TYPE_INDEX] = u8::MAX;
        assert!(read_market_data_type(&unknown_data_type).is_err());
    }
}

//! Layout of one topic's ring file; offsets are for x86_64 and aarch64, where bcast pads header fields to 128 bytes.
//!
//! ```text
//! offset 0            128          256                                 1280                      1280 + 64 KiB
//!        +------------+------------+-----------------------------------+-------------------------+
//!        | producer   | ready flag | metadata, 1024 B                  | data: RING_CAPACITY     |
//!        | position   | (writer    |                                   | bytes of frames         |
//!        | (u64, only | initialised|                                   |                         |
//!        |  grows)    |  the ring) |                                   |                         |
//!        +------------+------------+-----------------------------------+-------------------------+
//!        |<------------------- bcast header, HEADER_SIZE ------------->|
//!
//! metadata (METADATA_RANGE), written by this module:
//!        0          8               12                13                                    1024
//!        +----------+---------------+-----------------+-------------------------------------+
//!        | MDPRING  | FORMAT_VERSION| MarketDataType  | unused (zero)                       |
//!        | magic    | u32 LE        | u8 (1 BBO,      |                                     |
//!        |          |               |     2 Trade)    |                                     |
//!        +----------+---------------+-----------------+-------------------------------------+
//!
//! data: one frame per message, 8-byte aligned; a frame that would cross the end is preceded by a padding frame.
//!        +--------------------------+------------------+---------------------+---------+----------
//!        | fields: u32              | user_defined: u32| rkyv Bbo or Trade   | pad to  | next
//!        | bit 31 fin, 30 continued,| (unused, 0)      | (at most 512 B)     | 8 bytes | frame ...
//!        | 29 padding, 28 heartbeat,|                  |                     |         |
//!        | bits 0-27 payload length |                  |                     |         |
//!        +--------------------------+------------------+---------------------+---------+----------
//!        a frame starts at producer position % RING_CAPACITY
//! ```
use crate::types::MarketDataType;
use bcast::{HEADER_SIZE, METADATA_BUFFER_SIZE};
use std::ops::Range;

pub(super) const RING_CAPACITY: usize = 1 << 16;

// Must never change while strategies run: they keep their old mapping and would break.
pub(super) const RING_FILE_LENGTH: u64 = (HEADER_SIZE + RING_CAPACITY) as u64;

// bcast places its metadata buffer at the end of the header.
pub(super) const METADATA_RANGE: Range<usize> = HEADER_SIZE - METADATA_BUFFER_SIZE..HEADER_SIZE;

const METADATA_MAGIC: [u8; 8] = *b"MDPRING\0";
const METADATA_MAGIC_RANGE: Range<usize> = 0..8;
const METADATA_VERSION_RANGE: Range<usize> = 8..12;
const METADATA_MARKET_DATA_TYPE_INDEX: usize = 12;
const FORMAT_VERSION: u32 = 1;

pub(super) fn write_metadata(metadata: &mut [u8], market_data_type: MarketDataType) {
    metadata[METADATA_MAGIC_RANGE].copy_from_slice(&METADATA_MAGIC);
    metadata[METADATA_VERSION_RANGE].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    metadata[METADATA_MARKET_DATA_TYPE_INDEX] = market_data_type as u8;
}

/// Returns `None` unless the metadata was written by this format version.
pub(super) fn read_metadata(metadata: &[u8]) -> Option<MarketDataType> {
    if metadata[METADATA_MAGIC_RANGE] != METADATA_MAGIC
        || metadata[METADATA_VERSION_RANGE] != FORMAT_VERSION.to_le_bytes()
    {
        return None;
    }
    let code = metadata[METADATA_MARKET_DATA_TYPE_INDEX];
    [MarketDataType::Bbo, MarketDataType::Trade]
        .into_iter()
        .find(|market_data_type| *market_data_type as u8 == code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trips_each_data_type() {
        for market_data_type in [MarketDataType::Bbo, MarketDataType::Trade] {
            let mut metadata = [0; METADATA_BUFFER_SIZE];
            write_metadata(&mut metadata, market_data_type);
            assert_eq!(Some(market_data_type), read_metadata(&metadata));
        }
    }

    #[test]
    fn foreign_or_corrupt_metadata_is_refused() {
        let blank = [0; METADATA_BUFFER_SIZE];
        assert_eq!(None, read_metadata(&blank));

        let mut wrong_version = [0; METADATA_BUFFER_SIZE];
        write_metadata(&mut wrong_version, MarketDataType::Bbo);
        wrong_version[METADATA_VERSION_RANGE].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        assert_eq!(None, read_metadata(&wrong_version));

        let mut unknown_data_type = [0; METADATA_BUFFER_SIZE];
        write_metadata(&mut unknown_data_type, MarketDataType::Bbo);
        unknown_data_type[METADATA_MARKET_DATA_TYPE_INDEX] = u8::MAX;
        assert_eq!(None, read_metadata(&unknown_data_type));
    }
}

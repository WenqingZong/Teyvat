//! Each strategy reads its own rings, one per `RingKey`: the provider owns the writers, each strategy its reader.
/// Largest rkyv payload a frame may carry; the reader copies each frame into a buffer this size.
pub const MAX_PAYLOAD_LENGTH: usize = 512;

mod reader;
mod ring_format;
mod writer;

pub use reader::MarketDataReader;
pub use writer::{MarketDataWriter, SerializedMarketData};

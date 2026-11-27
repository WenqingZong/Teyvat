//! Each strategy reads its own rings, one per `RingKey`: the provider owns the writers, each strategy its reader.
/// Largest rkyv payload a frame may carry; the reader copies each frame into a buffer this size.
/// An order book's ten levels per side need about 700 bytes.
pub const MAX_PAYLOAD_LENGTH: usize = 1024;

mod reader;
mod ring_format;
mod writer;

pub use reader::MarketDataReader;
pub use writer::{MarketDataWriter, SerializedMarketData};

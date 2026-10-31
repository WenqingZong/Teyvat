//! One ring per topic: the provider owns the writer, strategies own the reader.
/// Largest rkyv payload a frame may carry; the reader copies each frame into a buffer this size.
pub const MAX_PAYLOAD_LENGTH: usize = 512;

mod reader;
mod ring_format;
mod writer;

pub use reader::MarketDataReader;
#[cfg(feature = "bench")]
pub use writer::MarketDataWriter;
#[cfg(not(feature = "bench"))]
pub(crate) use writer::MarketDataWriter;

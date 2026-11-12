//! Each strategy reads its own rings, one per `RingKey`: the provider owns the writers, each strategy its reader.
mod reader;
mod ring_format;
mod writer;

pub use reader::MarketDataReader;
pub use ring_format::SLOT_PAYLOAD_LENGTH as MAX_PAYLOAD_LENGTH;
pub use writer::{MarketDataWriter, SerializedMarketData};

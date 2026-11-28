use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize, Serialize};
use rust_decimal::Decimal;

use crate::types::rkyv_decimal::RkyvDecimal;
use crate::types::{Instrument, LatencyMetric};

/// The number of levels kept on each side; strategies need only the top of the book.
pub const ORDER_BOOK_DEPTH: u8 = 10;
/// `bid_count` and `ask_count` are `u8`, so a full side must fit one.
const _: () = assert!(ORDER_BOOK_DEPTH <= u8::MAX);

#[derive(Archive, Clone, Copy, Debug, Deserialize, Serialize)]
pub struct PriceLevel {
    #[rkyv(with = RkyvDecimal)]
    pub price: Decimal,
    #[rkyv(with = RkyvDecimal)]
    pub size: Decimal,
}

impl PriceLevel {
    pub const EMPTY: Self = Self {
        price: Decimal::ZERO,
        size: Decimal::ZERO,
    };
}

/// Venues send each level as a positional `(price, size)` pair.
impl From<(Decimal, Decimal)> for PriceLevel {
    fn from((price, size): (Decimal, Decimal)) -> Self {
        Self { price, size }
    }
}

/// The top `ORDER_BOOK_DEPTH` levels of each side at one moment. Only the first `bid_count` and
/// `ask_count` entries are valid; level 0 is the best price, bids descend and asks ascend.
///
/// This is the wire message; a connector's local book is reduced to it just before publishing.
#[derive(Archive, Clone, Copy, Debug, Deserialize, Serialize)]
pub struct OrderBook {
    pub instrument: Instrument,
    pub exchange_time_nanoseconds: Option<NonZeroU64>,
    pub latency_metric: LatencyMetric,
    pub bid_levels: [PriceLevel; ORDER_BOOK_DEPTH as usize],
    pub ask_levels: [PriceLevel; ORDER_BOOK_DEPTH as usize],
    pub bid_count: u8,
    pub ask_count: u8,
    pub sequence: usize,
}

impl OrderBook {
    pub fn bids(&self) -> &[PriceLevel] {
        &self.bid_levels[..usize::from(self.bid_count)]
    }

    pub fn asks(&self) -> &[PriceLevel] {
        &self.ask_levels[..usize::from(self.ask_count)]
    }
}

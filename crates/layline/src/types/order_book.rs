use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize, Serialize};
use rust_decimal::Decimal;

use crate::types::rkyv_decimal::RkyvDecimal;
use crate::types::{Instrument, LatencyMetric};

/// The number of levels kept on each side; strategies need only the top of the book.
pub const ORDER_BOOK_DEPTH: usize = 10;

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

/// The top `ORDER_BOOK_DEPTH` levels of each side at one moment. Only the first `bid_count` and
/// `ask_count` entries are valid; level 0 is the best price, bids descend and asks ascend.
#[derive(Archive, Clone, Copy, Debug, Deserialize, Serialize)]
pub struct OrderBook {
    pub instrument: Instrument,
    pub exchange_time_nanoseconds: Option<NonZeroU64>,
    pub latency_metric: LatencyMetric,
    pub bid_levels: [PriceLevel; ORDER_BOOK_DEPTH],
    pub ask_levels: [PriceLevel; ORDER_BOOK_DEPTH],
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

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use crate::testing::{test_instrument, test_order_book};
    use crate::types::{Currency, Exchange, UnderlyingType};

    #[test]
    fn accessors_expose_only_the_valid_levels() {
        let mut order_book = test_order_book(
            test_instrument(
                Exchange::Binance,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Perpetual,
            ),
            1,
        );
        order_book.bid_count = 2;
        order_book.ask_count = 3;
        order_book.bid_levels[0].price = dec!(100);
        order_book.ask_levels[2].price = dec!(103);
        assert_eq!(2, order_book.bids().len());
        assert_eq!(3, order_book.asks().len());
        assert_eq!(dec!(100), order_book.bids()[0].price);
        assert_eq!(dec!(103), order_book.asks()[2].price);
    }
}

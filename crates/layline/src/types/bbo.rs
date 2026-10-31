use crate::types::rkyv_decimal::RkyvDecimal;
use crate::types::{Instrument, LatencyMetric};
use rkyv::{Archive, Deserialize, Serialize};
use rust_decimal::Decimal;
use std::num::NonZeroU64;

#[derive(Archive, Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Bbo {
    pub instrument: Instrument,
    pub exchange_time_nanoseconds: Option<NonZeroU64>,
    pub latency_metric: LatencyMetric,
    #[rkyv(with = RkyvDecimal)]
    pub ask_price: Decimal,
    #[rkyv(with = RkyvDecimal)]
    pub ask_size: Decimal,
    #[rkyv(with = RkyvDecimal)]
    pub bid_price: Decimal,
    #[rkyv(with = RkyvDecimal)]
    pub bid_size: Decimal,
    pub sequence: usize,
}

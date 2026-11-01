use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize, Serialize};
use rust_decimal::Decimal;

use crate::types::rkyv_decimal::RkyvDecimal;
use crate::types::{Instrument, LatencyMetric, Side};

#[derive(Archive, Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Trade {
    pub instrument: Instrument,
    pub exchange_time_nanoseconds: Option<NonZeroU64>,
    pub latency_metric: LatencyMetric,
    #[rkyv(with = RkyvDecimal)]
    pub price: Decimal,
    #[rkyv(with = RkyvDecimal)]
    pub size: Decimal,
    pub taker_side: Side,
    pub sequence: usize,
}

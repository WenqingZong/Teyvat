use serde::{Deserialize, Serialize};
use strum::Display;

/// Serde spelling matches `Display`, so JSON keys read like ring paths.
#[derive(
    Clone, Copy, Debug, Deserialize, Display, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[repr(u8)]
pub enum MarketDataType {
    Bbo = 1,
    Trade = 2,
    /// Fills merged into one `Trade`: the venue's own aggregate stream as it is, or, where the venue
    /// has none, one taker order's fills merged by the provider at their volume-weighted average price.
    AggregateTrade = 3,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_trade_is_spelled_with_an_underscore() {
        assert_eq!(
            "AGGREGATE_TRADE",
            MarketDataType::AggregateTrade.to_string()
        );
        assert_eq!(
            MarketDataType::AggregateTrade,
            serde_json::from_str::<MarketDataType>("\"AGGREGATE_TRADE\"")
                .expect("deserialization is expected to not fail")
        );
    }
}

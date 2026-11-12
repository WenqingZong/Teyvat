use crate::types::{Bbo, MarketDataType, Topic, Trade};

#[derive(Clone, Copy, Debug)]
pub enum MarketData {
    Bbo(Bbo),
    Trade(Trade),
    /// Shares `Trade`'s layout; the variant tells which ring and topic it belongs to.
    AggregateTrade(Trade),
}

impl MarketData {
    pub fn topic(&self) -> Topic {
        match self {
            MarketData::Bbo(bbo) => Topic {
                instrument: bbo.instrument,
                market_data_type: MarketDataType::Bbo,
            },
            MarketData::Trade(trade) => Topic {
                instrument: trade.instrument,
                market_data_type: MarketDataType::Trade,
            },
            MarketData::AggregateTrade(trade) => Topic {
                instrument: trade.instrument,
                market_data_type: MarketDataType::AggregateTrade,
            },
        }
    }
}

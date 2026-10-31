use crate::types::{Bbo, MarketDataType, Topic, Trade};

#[derive(Clone, Copy, Debug)]
pub enum MarketData {
    Bbo(Bbo),
    Trade(Trade),
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
        }
    }
}

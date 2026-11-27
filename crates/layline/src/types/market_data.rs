use crate::types::{Bbo, MarketDataType, OrderBook, Topic, Trade};

// Deliberately inline: boxing the order book would add an allocation on the market data path. The
// enum is about 700 bytes; revisit if the latency benches show the copies matter.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug)]
pub enum MarketData {
    Bbo(Bbo),
    Trade(Trade),
    /// Shares `Trade`'s layout; the variant tells which ring and topic it belongs to.
    AggregateTrade(Trade),
    OrderBook(OrderBook),
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
            MarketData::OrderBook(order_book) => Topic {
                instrument: order_book.instrument,
                market_data_type: MarketDataType::OrderBook,
            },
        }
    }
}

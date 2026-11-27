//! How topics map onto Binance endpoints and stream names.

// The symbol spelling is Binance connector knowledge, like the channel and endpoint spelling.
// `exchangeInfo` returns the same symbol, which `instrument_rules::build_rules` checks before
// anything is subscribed.

use std::str::from_utf8;

use super::endpoint::BinanceEndpoint;
use layline::types::{Currency, Instrument, MarketDataType, Topic, UnderlyingType};

/// Room for the longest symbol plus channel, so per-frame names need no allocation.
pub(super) const STREAM_NAME_CAPACITY: usize = 64;
pub(super) const SBE_BEST_BID_ASK_CHANNEL: &str = "bestBidAsk";
pub(super) const SBE_DEPTH_CHANNEL: &str = "depth";
pub(super) const TRADE_CHANNEL: &str = "trade";
const BOOK_TICKER_CHANNEL: &str = "bookTicker";
const AGGREGATE_TRADE_CHANNEL: &str = "aggTrade";
const DEPTH_CHANNEL: &str = "depth10@100ms";

/// The endpoint and stream `topic` needs. Binance accepts any stream name on any path and then
/// sends nothing, so this mapping must stay under test.
pub(super) fn route(topic: &Topic) -> (BinanceEndpoint, String) {
    let (endpoint, channel) = match (topic.instrument.underlying_type, topic.market_data_type) {
        (UnderlyingType::Spot, MarketDataType::Bbo) => {
            (BinanceEndpoint::SpotSbe, SBE_BEST_BID_ASK_CHANNEL)
        }
        (UnderlyingType::Spot, MarketDataType::Trade | MarketDataType::AggregateTrade) => {
            (BinanceEndpoint::SpotSbe, TRADE_CHANNEL)
        }
        (UnderlyingType::Spot, MarketDataType::OrderBook) => {
            (BinanceEndpoint::SpotSbe, SBE_DEPTH_CHANNEL)
        }
        (UnderlyingType::Perpetual, MarketDataType::Bbo) => {
            (BinanceEndpoint::FuturesPublic, BOOK_TICKER_CHANNEL)
        }
        (UnderlyingType::Perpetual, MarketDataType::Trade) => {
            (BinanceEndpoint::FuturesPublic, TRADE_CHANNEL)
        }
        (UnderlyingType::Perpetual, MarketDataType::AggregateTrade) => {
            (BinanceEndpoint::FuturesMarket, AGGREGATE_TRADE_CHANNEL)
        }
        (UnderlyingType::Perpetual, MarketDataType::OrderBook) => {
            (BinanceEndpoint::FuturesPublic, DEPTH_CHANNEL)
        }
    };
    let symbol = venue_symbol(topic.instrument).to_lowercase();
    (endpoint, format!("{symbol}@{channel}"))
}

/// How the venue spells the instrument. A stock's spot pair is its bStock: `AAPL-USDT-SPOT` is
/// `AAPLBUSDT`, while `AAPL-USDT-PERP` is `AAPLUSDT`.
pub(super) fn venue_symbol(instrument: Instrument) -> String {
    let Instrument {
        base,
        quote,
        underlying_type,
        ..
    } = instrument;
    let base = match (underlying_type, base) {
        (
            UnderlyingType::Spot,
            Currency::Aapl
            | Currency::Amzn
            | Currency::Googl
            | Currency::Meta
            | Currency::Msft
            | Currency::Nvda
            | Currency::Tsla,
        ) => format!("{base}B"),
        _ => base.to_string(),
    };
    format!("{base}{quote}")
}

/// The stream name of an SBE event, which carries its symbol but not its stream. Written into `buffer`,
/// since it runs for every frame; `None` if it does not fit.
pub(super) fn sbe_stream_name<'a>(
    symbol: &str,
    channel: &str,
    buffer: &'a mut [u8; STREAM_NAME_CAPACITY],
) -> Option<&'a str> {
    let symbol_length = symbol.len();
    let name = buffer.get_mut(..symbol_length + 1 + channel.len())?;
    let (symbol_part, channel_part) = name.split_at_mut(symbol_length);
    symbol_part.copy_from_slice(symbol.as_bytes());
    symbol_part.make_ascii_lowercase();
    channel_part[0] = b'@';
    channel_part[1..].copy_from_slice(channel.as_bytes());
    from_utf8(name).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use layline::types::{Exchange, Instrument};

    fn topic(underlying_type: UnderlyingType, market_data_type: MarketDataType) -> Topic {
        Topic {
            instrument: Instrument {
                exchange: Exchange::Binance,
                base: Currency::Btc,
                quote: Currency::Usdt,
                underlying_type,
            },
            market_data_type,
        }
    }

    fn stock_topic(underlying_type: UnderlyingType, market_data_type: MarketDataType) -> Topic {
        Topic {
            instrument: Instrument {
                exchange: Exchange::Binance,
                base: Currency::Aapl,
                quote: Currency::Usdt,
                underlying_type,
            },
            market_data_type,
        }
    }

    #[test]
    fn topics_map_to_endpoints() {
        let cases = [
            (
                UnderlyingType::Spot,
                MarketDataType::Bbo,
                BinanceEndpoint::SpotSbe,
            ),
            (
                UnderlyingType::Spot,
                MarketDataType::Trade,
                BinanceEndpoint::SpotSbe,
            ),
            (
                UnderlyingType::Spot,
                MarketDataType::AggregateTrade,
                BinanceEndpoint::SpotSbe,
            ),
            (
                UnderlyingType::Perpetual,
                MarketDataType::Bbo,
                BinanceEndpoint::FuturesPublic,
            ),
            (
                UnderlyingType::Perpetual,
                MarketDataType::Trade,
                BinanceEndpoint::FuturesPublic,
            ),
            (
                UnderlyingType::Perpetual,
                MarketDataType::AggregateTrade,
                BinanceEndpoint::FuturesMarket,
            ),
            (
                UnderlyingType::Spot,
                MarketDataType::OrderBook,
                BinanceEndpoint::SpotSbe,
            ),
            (
                UnderlyingType::Perpetual,
                MarketDataType::OrderBook,
                BinanceEndpoint::FuturesPublic,
            ),
        ];
        for (underlying_type, market_data_type, endpoint) in cases {
            assert_eq!(endpoint, route(&topic(underlying_type, market_data_type)).0);
        }
    }

    #[test]
    fn stock_tokens_are_spelled_with_a_b_only_on_spot() {
        let spot = stock_topic(UnderlyingType::Spot, MarketDataType::Bbo);
        assert_eq!("AAPLBUSDT", venue_symbol(spot.instrument));
        let perpetual = stock_topic(UnderlyingType::Perpetual, MarketDataType::Bbo);
        assert_eq!("AAPLUSDT", venue_symbol(perpetual.instrument));
        let bitcoin = topic(UnderlyingType::Spot, MarketDataType::Bbo);
        assert_eq!("BTCUSDT", venue_symbol(bitcoin.instrument));
    }

    #[test]
    fn stream_names_follow_binance_spelling() {
        let (spot_endpoint, spot_bbo) = route(&topic(UnderlyingType::Spot, MarketDataType::Bbo));
        assert_eq!(BinanceEndpoint::SpotSbe, spot_endpoint);
        assert_eq!("btcusdt@bestBidAsk", spot_bbo);
        let (_, spot_trade) = route(&topic(UnderlyingType::Spot, MarketDataType::Trade));
        assert_eq!("btcusdt@trade", spot_trade);
        assert_eq!(
            spot_trade,
            route(&topic(UnderlyingType::Spot, MarketDataType::AggregateTrade)).1
        );
        let mut buffer = [0; STREAM_NAME_CAPACITY];
        assert_eq!(
            Some(spot_bbo.as_str()),
            sbe_stream_name("BTCUSDT", SBE_BEST_BID_ASK_CHANNEL, &mut buffer)
        );
        assert_eq!(
            Some(spot_trade.as_str()),
            sbe_stream_name("BTCUSDT", TRADE_CHANNEL, &mut buffer)
        );
        assert_eq!(
            None,
            sbe_stream_name(
                &"X".repeat(STREAM_NAME_CAPACITY),
                TRADE_CHANNEL,
                &mut buffer
            )
        );
        assert_eq!(
            "btcusdt@bookTicker",
            route(&topic(UnderlyingType::Perpetual, MarketDataType::Bbo)).1
        );
        assert_eq!(
            "btcusdt@trade",
            route(&topic(UnderlyingType::Perpetual, MarketDataType::Trade)).1
        );
        assert_eq!(
            "btcusdt@aggTrade",
            route(&topic(
                UnderlyingType::Perpetual,
                MarketDataType::AggregateTrade
            ))
            .1
        );
        assert_eq!(
            "btcusdt@depth",
            route(&topic(UnderlyingType::Spot, MarketDataType::OrderBook)).1
        );
        assert_eq!(
            "btcusdt@depth10@100ms",
            route(&topic(UnderlyingType::Perpetual, MarketDataType::OrderBook)).1
        );
        assert_eq!(
            "aaplbusdt@bestBidAsk",
            route(&stock_topic(UnderlyingType::Spot, MarketDataType::Bbo)).1
        );
        assert_eq!(
            "aaplusdt@bookTicker",
            route(&stock_topic(UnderlyingType::Perpetual, MarketDataType::Bbo)).1
        );
        assert_eq!(
            "aaplusdt@depth10@100ms",
            route(&stock_topic(
                UnderlyingType::Perpetual,
                MarketDataType::OrderBook
            ))
            .1
        );
    }
}

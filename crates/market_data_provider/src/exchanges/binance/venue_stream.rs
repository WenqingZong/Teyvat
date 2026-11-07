//! How topics map onto Binance endpoints and stream names.

// TODO: an InstrumentManager (not implemented yet) should own how each venue spells an instrument,
// learnt through an InstrumentManager; `venue_symbol` and `stream_name` then move there.

use std::str::from_utf8;

use super::endpoint::BinanceEndpoint;
use layline::types::{Instrument, MarketDataType, Topic, UnderlyingType};

/// Room for the longest symbol plus channel, so per-frame names need no allocation.
pub(super) const STREAM_NAME_CAPACITY: usize = 64;
pub(super) const SBE_BEST_BID_ASK_CHANNEL: &str = "bestBidAsk";
pub(super) const TRADE_CHANNEL: &str = "trade";
const BOOK_TICKER_CHANNEL: &str = "bookTicker";

/// Binance accepts any stream name on any path and then sends nothing, so this mapping must stay under test.
pub(super) fn endpoint_for_topic(topic: &Topic) -> BinanceEndpoint {
    match topic.instrument.underlying_type {
        UnderlyingType::Spot => BinanceEndpoint::SpotSbe,
        UnderlyingType::Perpetual => BinanceEndpoint::FuturesPublic,
    }
}

/// Spot and USDⓈ-M perpetuals share the symbol; the endpoint tells them apart.
pub(super) fn venue_symbol(instrument: Instrument) -> String {
    format!("{}{}", instrument.base, instrument.quote)
}

/// Unique on its endpoint, as Binance names it in SUBSCRIBE and in each event.
pub(super) fn stream_name(topic: &Topic) -> String {
    let channel = match (topic.instrument.underlying_type, topic.market_data_type) {
        (UnderlyingType::Spot, MarketDataType::Bbo) => SBE_BEST_BID_ASK_CHANNEL,
        (UnderlyingType::Perpetual, MarketDataType::Bbo) => BOOK_TICKER_CHANNEL,
        (_, MarketDataType::Trade) => TRADE_CHANNEL,
    };
    let symbol = venue_symbol(topic.instrument).to_lowercase();
    format!("{symbol}@{channel}")
}

/// `stream_name` of an SBE event, which carries its symbol but not its stream. Written into `buffer`,
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
    use layline::types::{Currency, Exchange};

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
                UnderlyingType::Perpetual,
                MarketDataType::Bbo,
                BinanceEndpoint::FuturesPublic,
            ),
            (
                UnderlyingType::Perpetual,
                MarketDataType::Trade,
                BinanceEndpoint::FuturesPublic,
            ),
        ];
        for (underlying_type, market_data_type, endpoint) in cases {
            assert_eq!(
                endpoint,
                endpoint_for_topic(&topic(underlying_type, market_data_type))
            );
        }
    }

    #[test]
    fn stream_names_follow_binance_spelling() {
        assert_eq!(
            "BTCUSDT",
            venue_symbol(topic(UnderlyingType::Perpetual, MarketDataType::Bbo).instrument)
        );
        let spot_bbo = stream_name(&topic(UnderlyingType::Spot, MarketDataType::Bbo));
        assert_eq!("btcusdt@bestBidAsk", spot_bbo);
        let spot_trade = stream_name(&topic(UnderlyingType::Spot, MarketDataType::Trade));
        assert_eq!("btcusdt@trade", spot_trade);
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
            stream_name(&topic(UnderlyingType::Perpetual, MarketDataType::Bbo))
        );
        assert_eq!(
            "btcusdt@trade",
            stream_name(&topic(UnderlyingType::Perpetual, MarketDataType::Trade))
        );
    }
}

//! Binance JSON text frames on combined `/stream` paths: replies to our requests, and stream events.
//! Events and acknowledgements take a hand-written path for Binance's compact layout (serde_json costs
//! ~400 ns a frame); anything it does not recognise is logged as an error, so the parser can learn it,
//! and falls back to serde_json, so odd input is never misread.

use std::borrow::Cow;
use std::fmt::{Display, Formatter, Result as FormatResult};
use std::num::NonZeroU64;

use anyhow::{Context, Result, anyhow};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::time::milliseconds_to_nanoseconds;
use layline::types::{Bbo, LatencyMetric, MarketData, MarketDataType, Side, Topic, Trade};

/// Futures `@trade` also reports insurance fund and ADL fills, which are not market prints.
const MARKET_ORDER_TYPE: &str = "MARKET";
const COMPACT_STREAM_PREFIX: &str = "{\"stream\":\"";
const COMPACT_DATA_PREFIX: &str = ",\"data\":{";
const COMPACT_SUFFIX: &str = "}}";
const COMPACT_ACKNOWLEDGEMENT_PREFIX: &str = "{\"result\":null,\"id\":";
const COMPACT_ACKNOWLEDGEMENT_SUFFIX: &str = "}";
const SERVER_SHUTDOWN_PREFIX: &str = "{\"e\":\"serverShutdown\"";

#[derive(Serialize)]
pub(super) struct Request<'a> {
    pub(super) method: &'a str,
    pub(super) params: &'a [String],
    pub(super) id: u64,
}

/// What one text frame is, before any handling. `classify_text_frame` is the single entry point.
pub(super) enum TextFrame<'a> {
    Response(Response),
    Event {
        stream: Cow<'a, str>,
        data: EventData<'a>,
    },
    /// The venue is going down; reconnect at once.
    ServerShutdown,
}

#[derive(Debug)]
pub(super) struct Response {
    /// Missing when the venue could not parse the request at all.
    pub(super) id: Option<u64>,
    pub(super) error: Option<VenueError>,
}

#[derive(Debug, Deserialize)]
pub(super) struct VenueError {
    code: i64,
    msg: String,
}

impl Display for VenueError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        write!(formatter, "code {}: {}", self.code, self.msg)
    }
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: Option<Cow<'a, str>>,
    #[serde(borrow)]
    data: Option<EventData<'a>>,
    id: Option<u64>,
    error: Option<VenueError>,
}

/// Every field any parsed event uses, so one pass reads the frame whatever its stream.
#[derive(Default, Deserialize)]
pub(super) struct EventData<'a> {
    /// Book update id of a `bookTicker`.
    #[serde(rename = "u")]
    update_id: Option<u64>,
    #[serde(rename = "b")]
    bid_price: Option<Decimal>,
    #[serde(rename = "B")]
    bid_size: Option<Decimal>,
    #[serde(rename = "a")]
    ask_price: Option<Decimal>,
    #[serde(rename = "A")]
    ask_size: Option<Decimal>,
    /// Transaction time in milliseconds; spot `bookTicker` has none.
    #[serde(rename = "T")]
    transaction_time_milliseconds: Option<u64>,
    #[serde(rename = "t")]
    trade_id: Option<u64>,
    #[serde(rename = "p")]
    price: Option<Decimal>,
    #[serde(rename = "q")]
    size: Option<Decimal>,
    #[serde(rename = "m")]
    buyer_is_maker: Option<bool>,
    #[serde(rename = "X", borrow)]
    order_type: Option<Cow<'a, str>>,
}

impl<'a> EventData<'a> {
    /// `None` if a known field holds something unexpected.
    fn set(&mut self, key: &str, value: &'a str) -> Option<()> {
        match key {
            "u" => self.update_id = Some(value.parse().ok()?),
            "b" => self.bid_price = Some(value.parse().ok()?),
            "B" => self.bid_size = Some(value.parse().ok()?),
            "a" => self.ask_price = Some(value.parse().ok()?),
            "A" => self.ask_size = Some(value.parse().ok()?),
            "T" => self.transaction_time_milliseconds = Some(value.parse().ok()?),
            "t" => self.trade_id = Some(value.parse().ok()?),
            "p" => self.price = Some(value.parse().ok()?),
            "q" => self.size = Some(value.parse().ok()?),
            "m" => self.buyer_is_maker = Some(value.parse().ok()?),
            "X" => self.order_type = Some(Cow::Borrowed(value)),
            _ => {}
        }
        Some(())
    }
}

pub(super) fn classify_text_frame(text: &str) -> Result<TextFrame<'_>> {
    if text.starts_with(SERVER_SHUTDOWN_PREFIX) {
        return Ok(TextFrame::ServerShutdown);
    }
    if let Some((stream, data)) = parse_compact_event(text) {
        return Ok(TextFrame::Event {
            stream: Cow::Borrowed(stream),
            data,
        });
    }
    if let Some(id) = parse_compact_acknowledgement(text) {
        return Ok(TextFrame::Response(Response {
            id: Some(id),
            error: None,
        }));
    }
    error!("the hand-written JSON parser does not recognise {text}; falling back to serde_json");
    parse_with_serde(text)
}

/// Reads `{"result":null,"id":N}`, Binance's answer to a request that succeeded.
fn parse_compact_acknowledgement(text: &str) -> Option<u64> {
    text.strip_prefix(COMPACT_ACKNOWLEDGEMENT_PREFIX)?
        .strip_suffix(COMPACT_ACKNOWLEDGEMENT_SUFFIX)?
        .parse()
        .ok()
}

/// Reads `{"stream":"…","data":{…}}` with a flat `data` object and no whitespace, escapes or nesting,
/// which is how Binance sends every event. One pass over the bytes, no substring search; `None` hands
/// anything else to serde_json.
fn parse_compact_event(text: &str) -> Option<(&str, EventData<'_>)> {
    let mut cursor = Cursor { text, position: 0 };
    cursor.expect(COMPACT_STREAM_PREFIX)?;
    let stream = cursor.string()?;
    cursor.expect(COMPACT_DATA_PREFIX)?;
    let mut data = EventData::default();
    loop {
        cursor.expect("\"")?;
        let key = cursor.string()?;
        cursor.expect(":")?;
        let value = match cursor.peek()? {
            b'"' => {
                cursor.position += 1;
                cursor.string()?
            }
            b'{' | b'[' => return None,
            _ => cursor.scalar()?,
        };
        data.set(key, value)?;
        match cursor.peek()? {
            b',' => cursor.position += 1,
            _ => return (cursor.rest() == COMPACT_SUFFIX).then_some((stream, data)),
        }
    }
}

struct Cursor<'a> {
    text: &'a str,
    position: usize,
}

impl<'a> Cursor<'a> {
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.position).copied()
    }

    fn rest(&self) -> &'a str {
        self.text.get(self.position..).unwrap_or_default()
    }

    fn expect(&mut self, literal: &str) -> Option<()> {
        self.rest()
            .starts_with(literal)
            .then(|| self.position += literal.len())
    }

    /// The rest of a string whose opening quote is consumed, up to its closing quote; `None` on an escape.
    fn string(&mut self) -> Option<&'a str> {
        let start = self.position;
        loop {
            match self.peek()? {
                b'"' => break,
                b'\\' => return None,
                _ => self.position += 1,
            }
        }
        let value = self.text.get(start..self.position)?;
        self.position += 1;
        Some(value)
    }

    /// A number, boolean or null, up to the `,` or `}` after it.
    fn scalar(&mut self) -> Option<&'a str> {
        let start = self.position;
        while !matches!(self.peek()?, b',' | b'}') {
            self.position += 1;
        }
        self.text.get(start..self.position)
    }
}

fn parse_with_serde(text: &str) -> Result<TextFrame<'_>> {
    let envelope: Envelope = serde_json::from_str(text)?;
    match (envelope.stream, envelope.data) {
        (Some(stream), Some(data)) => Ok(TextFrame::Event { stream, data }),
        _ if envelope.id.is_some() || envelope.error.is_some() => {
            Ok(TextFrame::Response(Response {
                id: envelope.id,
                error: envelope.error,
            }))
        }
        _ => Err(anyhow!("neither an event nor a response")),
    }
}

/// `None` for a futures fill that is not a market trade.
pub(super) fn event_to_market_data(
    topic: Topic,
    data: &EventData,
    received_time_nanoseconds: NonZeroU64,
) -> Result<Option<MarketData>> {
    let latency_metric = LatencyMetric::received(received_time_nanoseconds);
    let exchange_time_nanoseconds_option = data
        .transaction_time_milliseconds
        .map(milliseconds_to_nanoseconds);
    let market_data = match topic.market_data_type {
        MarketDataType::Bbo => MarketData::Bbo(Bbo {
            instrument: topic.instrument,
            exchange_time_nanoseconds: exchange_time_nanoseconds_option,
            latency_metric,
            ask_price: required(data.ask_price, "a")?,
            ask_size: required(data.ask_size, "A")?,
            bid_price: required(data.bid_price, "b")?,
            bid_size: required(data.bid_size, "B")?,
            sequence: to_sequence(required(data.update_id, "u")?)?,
        }),
        MarketDataType::Trade => {
            if data.order_type.as_deref() != Some(MARKET_ORDER_TYPE) {
                return Ok(None);
            }
            MarketData::Trade(Trade {
                instrument: topic.instrument,
                exchange_time_nanoseconds: exchange_time_nanoseconds_option,
                latency_metric,
                price: required(data.price, "p")?,
                size: required(data.size, "q")?,
                taker_side: taker_side(required(data.buyer_is_maker, "m")?),
                sequence: to_sequence(required(data.trade_id, "t")?)?,
            })
        }
    };
    Ok(Some(market_data))
}

fn required<Value>(value_option: Option<Value>, field: &str) -> Result<Value> {
    value_option.with_context(|| format!("missing field {field}"))
}

pub(super) fn to_sequence(id: u64) -> Result<usize> {
    usize::try_from(id).with_context(|| format!("id {id} does not fit usize"))
}

/// The buyer resting on the book means the seller took liquidity.
pub(super) fn taker_side(buyer_is_maker: bool) -> Side {
    if buyer_is_maker {
        Side::Sell
    } else {
        Side::Buy
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;
    use layline::types::{Currency, Exchange, Instrument, UnderlyingType};

    const RECEIVED_TIME_NANOSECONDS: NonZeroU64 =
        NonZeroU64::new(1_792_881_786_600_000_000).expect("not zero");
    /// Captured on the demo endpoints.
    const SPOT_BOOK_TICKER: &str = r#"{"stream":"btcusdt@bookTicker","data":{"u":6230799530,"s":"BTCUSDT","b":"80586.30000000","B":"3.17369000","a":"80586.31000000","A":"10.52077000"}}"#;
    const FUTURES_BOOK_TICKER: &str = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":462733955406,"s":"BTCUSDT","ps":"BTCUSDT","b":"80570.70","B":"2.6914","a":"80570.80","A":"187.5753","T":1792881786559,"E":1792881786571,"st":1}}"#;
    const FUTURES_TRADE: &str = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1792881786571,"T":1792881786559,"s":"BTCUSDT","t":550507694,"p":"80570.70","q":"0.0015","X":"MARKET","m":true,"st":1}}"#;

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

    fn parse_event(text: &str, topic: Topic) -> Result<Option<MarketData>> {
        let TextFrame::Event { stream, data } =
            classify_text_frame(text).expect("the frame is expected to parse")
        else {
            panic!("an event is expected");
        };
        assert_eq!("btcusdt", stream.split('@').next().expect("named stream"));
        event_to_market_data(topic, &data, RECEIVED_TIME_NANOSECONDS)
    }

    #[test]
    fn spot_book_ticker_has_no_exchange_time() {
        let topic = topic(UnderlyingType::Spot, MarketDataType::Bbo);
        let Some(MarketData::Bbo(bbo)) =
            parse_event(SPOT_BOOK_TICKER, topic).expect("a valid event")
        else {
            panic!("a BBO is expected");
        };
        assert_eq!(topic.instrument, bbo.instrument);
        assert_eq!(None, bbo.exchange_time_nanoseconds);
        assert_eq!(
            RECEIVED_TIME_NANOSECONDS,
            bbo.latency_metric.provider_receive_time_nanoseconds
        );
        assert_eq!(dec!(80586.30000000), bbo.bid_price);
        assert_eq!(dec!(3.17369), bbo.bid_size);
        assert_eq!(dec!(80586.31), bbo.ask_price);
        assert_eq!(dec!(10.52077), bbo.ask_size);
        assert_eq!(6_230_799_530, bbo.sequence);
    }

    #[test]
    fn futures_book_ticker_carries_its_transaction_time() {
        let topic = topic(UnderlyingType::Perpetual, MarketDataType::Bbo);
        let Some(MarketData::Bbo(bbo)) =
            parse_event(FUTURES_BOOK_TICKER, topic).expect("a valid event")
        else {
            panic!("a BBO is expected");
        };
        assert_eq!(
            NonZeroU64::new(1_792_881_786_559_000_000),
            bbo.exchange_time_nanoseconds
        );
        assert_eq!(dec!(80570.70), bbo.bid_price);
        assert_eq!(dec!(187.5753), bbo.ask_size);
        assert_eq!(462_733_955_406, bbo.sequence);
    }

    #[test]
    fn futures_market_trade_is_parsed_and_other_fills_skipped() {
        let topic = topic(UnderlyingType::Perpetual, MarketDataType::Trade);
        let Some(MarketData::Trade(trade)) =
            parse_event(FUTURES_TRADE, topic).expect("a valid event")
        else {
            panic!("a trade is expected");
        };
        assert_eq!(
            NonZeroU64::new(1_792_881_786_559_000_000),
            trade.exchange_time_nanoseconds
        );
        assert_eq!(dec!(80570.70), trade.price);
        assert_eq!(dec!(0.0015), trade.size);
        assert_eq!(Side::Sell, trade.taker_side);
        assert_eq!(550_507_694, trade.sequence);

        let liquidation = FUTURES_TRADE.replace("MARKET", "INSURANCE_FUND");
        assert!(
            parse_event(&liquidation, topic)
                .expect("a valid event")
                .is_none()
        );
    }

    /// Both paths turn `text` into the same market data, or the fast path declines it.
    fn assert_paths_agree(text: &str, topic: Topic) {
        let fast_option = parse_compact_event(text).map(|(stream, data)| {
            (
                stream.to_owned(),
                format!(
                    "{:?}",
                    event_to_market_data(topic, &data, RECEIVED_TIME_NANOSECONDS).ok()
                ),
            )
        });
        let Ok(TextFrame::Event { stream, data }) = parse_with_serde(text) else {
            assert!(
                fast_option.is_none(),
                "the fast path read what serde refuses: {text}"
            );
            return;
        };
        let slow = (
            stream.into_owned(),
            format!(
                "{:?}",
                event_to_market_data(topic, &data, RECEIVED_TIME_NANOSECONDS).ok()
            ),
        );
        if let Some(fast) = fast_option {
            assert_eq!(slow, fast, "{text}");
        }
    }

    #[test]
    fn compact_events_take_the_fast_path_and_match_serde() {
        let bbo = topic(UnderlyingType::Perpetual, MarketDataType::Bbo);
        let trade = topic(UnderlyingType::Perpetual, MarketDataType::Trade);
        for (text, topic) in [
            (FUTURES_BOOK_TICKER, bbo),
            (SPOT_BOOK_TICKER, bbo),
            (FUTURES_TRADE, trade),
        ] {
            assert!(parse_compact_event(text).is_some(), "{text}");
            assert_paths_agree(text, topic);
        }
        let reordered = r#"{"stream":"btcusdt@trade","data":{"m":false,"X":"MARKET","q":"2","p":"3","t":4,"T":5,"extra":null}}"#;
        let (_, data) =
            parse_compact_event(reordered).expect("reordered keys stay on the fast path");
        let Some(MarketData::Trade(parsed)) =
            event_to_market_data(trade, &data, RECEIVED_TIME_NANOSECONDS).expect("a valid trade")
        else {
            panic!("a trade is expected");
        };
        assert_eq!(
            (dec!(3), dec!(2), Side::Buy, 4),
            (
                parsed.price,
                parsed.size,
                parsed.taker_side,
                parsed.sequence
            )
        );
        assert_paths_agree(reordered, trade);
    }

    #[test]
    fn unusual_frames_fall_back_to_serde() {
        let trade = topic(UnderlyingType::Perpetual, MarketDataType::Trade);
        let unusual = [
            FUTURES_TRADE.replace(",\"data\"", ", \"data\""),
            FUTURES_TRADE.replace("\"p\":\"80570.70\"", "\"p\": \"80570.70\""),
            FUTURES_TRADE.replace("\"X\":\"MARKET\"", "\"X\":\"MAR\\u004bET\""),
            FUTURES_TRADE.replace("btcusdt@trade", "btcusdt\\u0040trade"),
            FUTURES_TRADE.replace("\"st\":1", "\"st\":{\"nested\":1}"),
            FUTURES_TRADE.replace("\"st\":1", "\"st\":[1,2]"),
            FUTURES_TRADE.replace("\"t\":550507694", "\"t\":null"),
            FUTURES_TRADE.replace("\"m\":true", "\"m\":1"),
            FUTURES_TRADE.replace("}}", "} }"),
            format!("{FUTURES_TRADE} "),
            r#"{"stream":"btcusdt@trade","data":{}}"#.to_owned(),
            r#"{"stream":"btcusdt@trade","data":{"p"}}"#.to_owned(),
            r#"{"stream":"btcusdt@trade","data":{"p":"#.to_owned(),
            r#"{"result":null,"id":7}"#.to_owned(),
        ];
        for text in &unusual {
            assert!(parse_compact_event(text).is_none(), "{text}");
            assert_paths_agree(text, trade);
        }
        let Ok(TextFrame::Event { data, .. }) = classify_text_frame(&unusual[2]) else {
            panic!("serde is expected to read the escaped frame");
        };
        assert_eq!(Some("MARKET"), data.order_type.as_deref());
    }

    #[test]
    fn missing_field_is_reported() {
        let topic = topic(UnderlyingType::Spot, MarketDataType::Bbo);
        let error = parse_event(&SPOT_BOOK_TICKER.replace("\"A\"", "\"Z\""), topic)
            .expect_err("a BBO without ask size is expected to fail");
        assert_eq!("missing field A", error.to_string());
    }

    #[test]
    fn responses_carry_their_id_and_error() {
        let TextFrame::Response(Response {
            id: id_option,
            error: error_option,
        }) = classify_text_frame(r#"{"result":null,"id":7}"#).expect("a valid response")
        else {
            panic!("a response is expected");
        };
        assert_eq!(Some(7), id_option);
        assert!(error_option.is_none());

        let TextFrame::Response(Response {
            id: id_option,
            error: error_option,
        }) = classify_text_frame(
            r#"{"error":{"code":3,"msg":"Invalid JSON: expected ident at line 1 column 2"}}"#,
        )
        .expect("a valid response")
        else {
            panic!("a response is expected");
        };
        assert_eq!(None, id_option);
        assert_eq!(
            "code 3: Invalid JSON: expected ident at line 1 column 2",
            error_option.expect("an error").to_string()
        );
    }

    #[test]
    fn server_shutdown_is_classified() {
        assert!(matches!(
            classify_text_frame(r#"{"e":"serverShutdown"}"#).expect("a shutdown frame"),
            TextFrame::ServerShutdown
        ));
    }

    #[test]
    fn acknowledgements_take_the_fast_path() {
        assert_eq!(
            Some(7),
            parse_compact_acknowledgement(r#"{"result":null,"id":7}"#)
        );
        for unusual in [
            r#"{"result": null,"id":7}"#,
            r#"{"result":null,"id":7} "#,
            r#"{"result":null,"id":-7}"#,
            r#"{"result":[],"id":7}"#,
            r#"{"error":{"code":2,"msg":"Invalid request"},"id":7}"#,
        ] {
            assert_eq!(None, parse_compact_acknowledgement(unusual), "{unusual}");
        }
        let TextFrame::Response(Response {
            id: id_option,
            error: error_option,
        }) = classify_text_frame(r#"{"result": null,"id":7}"#).expect("serde reads it")
        else {
            panic!("a response is expected");
        };
        assert_eq!((Some(7), true), (id_option, error_option.is_none()));
    }

    #[test]
    fn unknown_frames_are_refused() {
        assert!(classify_text_frame("{}").is_err());
        assert!(classify_text_frame("not json").is_err());
    }

    #[test]
    fn requests_serialize_as_binance_expects() {
        let streams = ["btcusdt@trade".to_owned()];
        let request = Request {
            method: "SUBSCRIBE",
            params: &streams,
            id: 1,
        };
        assert_eq!(
            r#"{"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}"#,
            serde_json::to_string(&request).expect("serializing is expected to not fail")
        );
    }

    #[test]
    fn buyer_as_maker_means_seller_took() {
        assert_eq!(Side::Sell, taker_side(true));
        assert_eq!(Side::Buy, taker_side(false));
    }
}

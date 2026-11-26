//! The market data provider's HTTP contract with strategies: JSON over HTTP. Topics nest as
//! exchange, then instrument, then market data type, spelled as in ring paths. The documents below
//! are pretty-printed so the field hierarchy is visible at a glance.
//!
//! ```text
//! POST /subscribe
//! {
//!   "strategy_name": "dummy_strategy",
//!   "demo": false,
//!   "topics": {
//!     "Binance": {
//!       "BTC-USDT-SPOT": ["BBO", "TRADE"]
//!     },
//!     "OKX": {
//!       "BTC-USDT-PERP": ["BBO"]
//!     }
//!   }
//! }
//!
//! 200 OK
//! {
//!   "results": {
//!     "Binance": {
//!       "BTC-USDT-SPOT": {
//!         "instrument_rule": {
//!           "exchange_symbol": "BTCUSDT",
//!           "price_step": "0.01",
//!           "quantity_step": "0.00001",
//!           "minimum_quantity": "0.00001",
//!           "minimum_notional_size": "5"
//!         },
//!         // AI TODO: 这里虽然只是文档, 但也要写全的. 请求里还有okx呢
//!         "BBO": {
//!           "Ok": "/dev/shm/market_data_provider/dummy_strategy/Binance/SPOT/BBO.dat"
//!         },
//!         "TRADE": {
//!           "Err": "reason"
//!         }
//!       }
//!     }
//!   }
//! }
//!
//! GET /subscribed
//! {
//!   "topics": {
//!     "Binance": {
//! // AI TODO: 这里也是, 要写全的, 还有TRADE呢
//!       "BTC-USDT-SPOT": ["BBO"]
//!     }
//!   }
//! }
//!
//! GET /connected_apis
//! {
//!   "Binance": [
//!     {
//!       "description": "...",
//!       "api": "wss://...",
//!       "doc": "https://..."
//!     }
//!   ],
//!   "OKX": []
//! }
//! ```
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter, Result as FormatResult};
use std::ops::Deref;
use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize};

pub use crate::types::{ConnectedApi, ConnectedApis, Subscription};
use crate::types::{Instrument, InstrumentRule, MarketDataType, StrategyName, Topic};

pub const HEALTH_PATH: &str = "/health";
pub const INFO_PATH: &str = "/info";
/// Adds topics to the strategy's set and renews its lease.
pub const SUBSCRIBE_PATH: &str = "/subscribe";
/// Replaces the strategy's whole set and renews its lease; strategies call it periodically.
pub const RESUBSCRIBE_PATH: &str = "/resubscribe";
pub const UNSUBSCRIBE_PATH: &str = "/unsubscribe";
pub const SUBSCRIBED_PATH: &str = "/subscribed";
pub const CONNECTED_APIS_PATH: &str = "/connected_apis";

/// Body of `/subscribe`, `/resubscribe` and `/unsubscribe`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SubscriptionRequest {
    pub strategy_name: StrategyName,
    /// The venues' demo-trading data instead of live; each exchange serves one environment at a time.
    pub demo: bool,
    #[serde(with = "topic_set")]
    pub topics: HashSet<Topic>,
}

/// One instrument's outcome and, when it has one, its rule.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InstrumentSubscription {
    /// `None` when no rule applies: the venue has no rule for the instrument (a synthetic feed or
    /// an unsupported instrument), or every requested topic for it was refused.
    #[serde(rename = "instrument_rule", skip_serializing_if = "Option::is_none")]
    pub instrument_rule_option: Option<InstrumentRule>,
    #[serde(flatten)]
    pub results: HashMap<MarketDataType, SubscriptionResult>,
}

/// Body returned by `/subscribe` and `/resubscribe`: one entry per requested instrument.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SubscriptionResponse {
    #[serde(with = "instrument_subscriptions")]
    pub results: HashMap<Instrument, InstrumentSubscription>,
}

/// The ring path to read, or why the topic was refused.
pub type SubscriptionResult = Result<PathBuf, String>;

/// A strategy's topics, printed sorted one per line by `#[derive(Debug)]`; deserialized from the
/// same nested tree the HTTP API uses.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SortedTopics(pub HashSet<Topic>);

impl Debug for SortedTopics {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let mut topics: Vec<Topic> = self.0.iter().copied().collect();
        topics.sort_by_cached_key(Topic::to_string);
        formatter
            .debug_list()
            .entries(topics.iter().map(DisplayedTopic))
            .finish()
    }
}

struct DisplayedTopic<'a>(&'a Topic);

impl Debug for DisplayedTopic<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        write!(formatter, "{}", self.0)
    }
}

impl Deref for SortedTopics {
    type Target = HashSet<Topic>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<HashSet<Topic>> for SortedTopics {
    fn from(topics: HashSet<Topic>) -> Self {
        Self(topics)
    }
}

impl From<SortedTopics> for HashSet<Topic> {
    fn from(topics: SortedTopics) -> Self {
        topics.0
    }
}

impl<'de> Deserialize<'de> for SortedTopics {
    fn deserialize<DeserializerType: Deserializer<'de>>(
        deserializer: DeserializerType,
    ) -> Result<Self, DeserializerType::Error> {
        Ok(Self(topic_set::deserialize(deserializer)?))
    }
}

/// Refused topics grouped by reason, reasons sorted and topics sorted by name, so a cause shared by
/// many topics is reported once.
pub fn group_by_reason(refusals: &HashMap<Topic, String>) -> BTreeMap<&str, Vec<Topic>> {
    let mut groups: BTreeMap<&str, Vec<Topic>> = BTreeMap::new();
    for (topic, reason) in refusals {
        groups.entry(reason).or_default().push(*topic);
    }
    for topics in groups.values_mut() {
        topics.sort_by_cached_key(Topic::to_string);
    }
    groups
}

/// Body of `/subscribed`: every topic the provider currently publishes.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SubscribedResponse {
    #[serde(with = "topic_set")]
    pub topics: HashSet<Topic>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProviderInfo {
    /// UTC to the millisecond, such as `2026-10-15 13:30:05.123 UTC`.
    pub server_time: String,
    pub version: String,
    /// `release` or `debug`.
    pub build: String,
    /// The commit the provider was built from.
    pub git_commit_hash: String,
    pub lease_duration_seconds: u64,
}

/// `HashSet<Topic>` as `{exchange: {instrument: [market_data_type, ...]}}`, for
/// `#[serde(with = "topic_set")]`; strategy configs list their topics the same way.
pub mod topic_set {
    use std::collections::{BTreeMap, BTreeSet, HashSet};

    use serde::de::Error as DeserializeError;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::types::{Exchange, Instrument, MarketDataType, Topic};

    type Tree = BTreeMap<Exchange, BTreeMap<String, BTreeSet<MarketDataType>>>;

    pub fn serialize<SerializerType: Serializer>(
        topics: &HashSet<Topic>,
        serializer: SerializerType,
    ) -> Result<SerializerType::Ok, SerializerType::Error> {
        let mut tree: Tree = BTreeMap::new();
        for topic in topics {
            tree.entry(topic.instrument.exchange)
                .or_default()
                .entry(topic.instrument.key())
                .or_default()
                .insert(topic.market_data_type);
        }
        tree.serialize(serializer)
    }

    pub fn deserialize<'de, DeserializerType: Deserializer<'de>>(
        deserializer: DeserializerType,
    ) -> Result<HashSet<Topic>, DeserializerType::Error> {
        let tree = Tree::deserialize(deserializer)?;
        let mut topics = HashSet::new();
        for (exchange, instruments) in tree {
            for (key, market_data_types) in instruments {
                let instrument =
                    Instrument::from_key(exchange, &key).map_err(DeserializeError::custom)?;
                for market_data_type in market_data_types {
                    topics.insert(Topic {
                        instrument,
                        market_data_type,
                    });
                }
            }
        }
        Ok(topics)
    }
}

/// `HashMap<Instrument, InstrumentSubscription>` as
/// `{exchange: {instrument: {instrument_rule: ..., market_data_type: result}}}`.
mod instrument_subscriptions {
    use std::collections::{BTreeMap, HashMap};

    use serde::de::Error as DeserializeError;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::InstrumentSubscription;
    use crate::types::{Exchange, Instrument};

    pub fn serialize<SerializerType: Serializer>(
        subscriptions: &HashMap<Instrument, InstrumentSubscription>,
        serializer: SerializerType,
    ) -> Result<SerializerType::Ok, SerializerType::Error> {
        let mut tree: BTreeMap<Exchange, BTreeMap<String, &InstrumentSubscription>> =
            BTreeMap::new();
        for (instrument, subscription) in subscriptions {
            tree.entry(instrument.exchange)
                .or_default()
                .insert(instrument.key(), subscription);
        }
        tree.serialize(serializer)
    }

    pub fn deserialize<'de, DeserializerType: Deserializer<'de>>(
        deserializer: DeserializerType,
    ) -> Result<HashMap<Instrument, InstrumentSubscription>, DeserializerType::Error> {
        let tree = BTreeMap::<Exchange, BTreeMap<String, InstrumentSubscription>>::deserialize(
            deserializer,
        )?;
        let mut subscriptions = HashMap::new();
        for (exchange, instruments) in tree {
            for (key, subscription) in instruments {
                let instrument =
                    Instrument::from_key(exchange, &key).map_err(DeserializeError::custom)?;
                subscriptions.insert(instrument, subscription);
            }
        }
        Ok(subscriptions)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::{Value, json};

    use super::*;
    use crate::testing::{test_instrument, test_topic};
    use crate::types::{Currency, Exchange, InstrumentRule, MarketDataType, UnderlyingType};

    fn topic(
        exchange: Exchange,
        underlying_type: UnderlyingType,
        market_data_type: MarketDataType,
    ) -> Topic {
        test_topic(
            test_instrument(exchange, Currency::Btc, Currency::Usdt, underlying_type),
            market_data_type,
        )
    }

    fn to_json<Message: Serialize>(message: &Message) -> Value {
        serde_json::to_value(message).expect("serialization is expected to not fail")
    }

    #[test]
    fn refusals_are_grouped_by_reason_and_sorted() {
        let spot_bbo = topic(Exchange::Binance, UnderlyingType::Spot, MarketDataType::Bbo);
        let spot_trade = topic(
            Exchange::Binance,
            UnderlyingType::Spot,
            MarketDataType::Trade,
        );
        let okx_bbo = topic(
            Exchange::Okx,
            UnderlyingType::Perpetual,
            MarketDataType::Bbo,
        );
        let refusals = HashMap::from([
            (spot_trade, "venue down".to_owned()),
            (okx_bbo, "worker stopped".to_owned()),
            (spot_bbo, "venue down".to_owned()),
        ]);
        let groups = group_by_reason(&refusals);
        assert_eq!(
            BTreeMap::from([
                ("venue down", vec![spot_bbo, spot_trade]),
                ("worker stopped", vec![okx_bbo]),
            ]),
            groups
        );
    }

    #[test]
    fn request_nests_topics_by_exchange_and_instrument() {
        let request = SubscriptionRequest {
            strategy_name: "dummy_strategy".to_owned(),
            demo: true,
            topics: HashSet::from([
                topic(
                    Exchange::Okx,
                    UnderlyingType::Perpetual,
                    MarketDataType::Bbo,
                ),
                topic(
                    Exchange::Binance,
                    UnderlyingType::Spot,
                    MarketDataType::Trade,
                ),
                topic(Exchange::Binance, UnderlyingType::Spot, MarketDataType::Bbo),
            ]),
        };
        let expected = json!({
            "strategy_name": "dummy_strategy",
            "demo": true,
            "topics": {
                "Binance": {"BTC-USDT-SPOT": ["BBO", "TRADE"]},
                "OKX": {"BTC-USDT-PERP": ["BBO"]},
            },
        });
        assert_eq!(expected, to_json(&request));
        let decoded: SubscriptionRequest =
            serde_json::from_value(expected).expect("deserialization is expected to not fail");
        assert_eq!(request, decoded);
    }

    #[test]
    fn response_nests_instrument_rules_and_outcomes_and_round_trips() {
        let instrument =
            topic(Exchange::Binance, UnderlyingType::Spot, MarketDataType::Bbo).instrument;
        let response = SubscriptionResponse {
            results: HashMap::from([(
                instrument,
                InstrumentSubscription {
                    instrument_rule_option: Some(InstrumentRule {
                        exchange_symbol: "BTCUSDT".to_owned(),
                        price_step: dec!(0.01),
                        quantity_step: dec!(0.00001),
                        minimum_quantity: dec!(0.00001),
                        minimum_notional_size: dec!(5),
                    }),
                    results: HashMap::from([
                        (MarketDataType::Bbo, Ok(PathBuf::from("/dev/shm/a.dat"))),
                        (MarketDataType::Trade, Err("rejected".to_owned())),
                    ]),
                },
            )]),
        };
        let expected = json!({
            "results": {"Binance": {"BTC-USDT-SPOT": {
                "instrument_rule": {
                    "exchange_symbol": "BTCUSDT",
                    "price_step": "0.01",
                    "quantity_step": "0.00001",
                    "minimum_quantity": "0.00001",
                    "minimum_notional_size": "5",
                },
                "BBO": {"Ok": "/dev/shm/a.dat"},
                "TRADE": {"Err": "rejected"},
            }}},
        });
        assert_eq!(expected, to_json(&response));
        let decoded: SubscriptionResponse =
            serde_json::from_value(expected).expect("deserialization is expected to not fail");
        assert_eq!(response, decoded);
    }

    #[test]
    fn synthetic_instruments_have_no_instrument_rule() {
        let instrument = topic(
            Exchange::Okx,
            UnderlyingType::Perpetual,
            MarketDataType::Bbo,
        )
        .instrument;
        let response = SubscriptionResponse {
            results: HashMap::from([(
                instrument,
                InstrumentSubscription {
                    instrument_rule_option: None,
                    results: HashMap::from([(
                        MarketDataType::Bbo,
                        Ok(PathBuf::from("/dev/shm/okx.dat")),
                    )]),
                },
            )]),
        };
        let expected = json!({
            "results": {"OKX": {"BTC-USDT-PERP": {
                "BBO": {"Ok": "/dev/shm/okx.dat"},
            }}},
        });
        assert_eq!(expected, to_json(&response));
        let decoded: SubscriptionResponse =
            serde_json::from_value(expected).expect("deserialization is expected to not fail");
        assert_eq!(response, decoded);
    }

    #[test]
    fn subscribed_response_round_trips() {
        let response = SubscribedResponse {
            topics: HashSet::from([topic(
                Exchange::Okx,
                UnderlyingType::Perpetual,
                MarketDataType::Trade,
            )]),
        };
        let expected = json!({"topics": {"OKX": {"BTC-USDT-PERP": ["TRADE"]}}});
        assert_eq!(expected, to_json(&response));
        let decoded: SubscribedResponse =
            serde_json::from_value(expected).expect("deserialization is expected to not fail");
        assert_eq!(response, decoded);
    }

    #[test]
    fn connected_apis_list_each_exchange() {
        const BINANCE_APIS: &[ConnectedApi] = &[ConnectedApi {
            description: "Connect to websocket",
            api: "wss://stream.binance.com:9443/ws",
            doc: "https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams",
        }];
        let connected_apis =
            ConnectedApis::from([(Exchange::Binance, BINANCE_APIS), (Exchange::Okx, &[])]);
        let expected = json!({
            "Binance": [{
                "description": "Connect to websocket",
                "api": "wss://stream.binance.com:9443/ws",
                "doc": "https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams",
            }],
            "OKX": [],
        });
        assert_eq!(expected, to_json(&connected_apis));
    }

    #[test]
    fn malformed_topics_are_rejected() {
        for topics in [
            json!({"Bybit": {"BTC-USDT-SPOT": ["BBO"]}}),
            json!({"Binance": {"BTC-USDT": ["BBO"]}}),
            json!({"Binance": {"BTC-DOGE-SPOT": ["BBO"]}}),
            json!({"Binance": {"BTC-USDT-SPOT": ["bbo"]}}),
        ] {
            let request =
                json!({"strategy_name": "dummy_strategy", "demo": false, "topics": topics});
            assert!(serde_json::from_value::<SubscriptionRequest>(request).is_err());
        }
    }

    #[test]
    fn request_without_demo_is_rejected() {
        let request = json!({"strategy_name": "dummy_strategy", "topics": {}});
        assert!(serde_json::from_value::<SubscriptionRequest>(request).is_err());
    }
}

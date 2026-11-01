//! The market data provider's HTTP contract with strategies: JSON over HTTP. Topics nest as exchange, then instrument, then market data type, spelled as in ring paths:
//!
//! ```text
//! POST /subscribe   {"strategy_name": "dummy_strategy", "demo": false,
//!                    "topics": {"Binance": {"BTC-USDT-SPOT": ["BBO", "TRADE"]}, "OKX": {"BTC-USDT-PERP": ["BBO"]}}}
//! 200 OK            {"results": {"Binance": {"BTC-USDT-SPOT": {"BBO": {"Ok": "/dev/shm/market_data_provider/Binance/BTC-USDT-SPOT/BBO.dat"},
//!                                                          "TRADE": {"Err": "reason"}}}}}
//! GET /subscribed   {"topics": {"Binance": {"BTC-USDT-SPOT": {"BBO": {"path": "...", "strategy_names": ["dummy_strategy"]}}}}}
//! GET /connected_apis {"Binance": [{"description": "...", "api": "wss://...", "doc": "https://..."}], "OKX": []}
//! ```
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use crate::types::{ConnectedApi, ConnectedApis, Subscription};
use crate::types::{Exchange, Instrument, MarketDataType, Topic};

pub const DEFAULT_PROVIDER_ADDRESS: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));

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
    pub strategy_name: String,
    /// The venues' demo-trading data instead of live; each exchange serves one environment at a time.
    pub demo: bool,
    #[serde(with = "topic_set")]
    pub topics: HashSet<Topic>,
}

/// Body returned by `/subscribe` and `/resubscribe`: one result per requested topic.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SubscriptionResponse {
    #[serde(with = "topic_map")]
    pub results: HashMap<Topic, SubscriptionResult>,
}

/// The ring path to read, or why the topic was refused.
pub type SubscriptionResult = Result<PathBuf, String>;

/// Body of `/subscribed`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SubscribedResponse {
    #[serde(with = "topic_map")]
    pub topics: HashMap<Topic, TopicStatus>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TopicStatus {
    pub path: PathBuf,
    pub strategy_names: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProviderInfo {
    /// UTC to the millisecond, such as `2026-10-15 13:30:05.123 UTC`.
    pub server_time: String,
    pub version: String,
    /// `release` or `debug`.
    pub build: String,
    pub lease_duration_seconds: u64,
}

/// The wire tree: exchange, then instrument key, then one leaf per market data type.
type TopicTree<Leaf> = BTreeMap<Exchange, BTreeMap<String, Leaf>>;

/// Lets the set adapter reuse the map adapter's tree walk with a different leaf shape.
trait TopicLeaves<Value>: Default {
    fn insert(&mut self, market_data_type: MarketDataType, value: Value);
    fn into_entries(self) -> Vec<(MarketDataType, Value)>;
}

impl TopicLeaves<()> for BTreeSet<MarketDataType> {
    fn insert(&mut self, market_data_type: MarketDataType, _value: ()) {
        self.insert(market_data_type);
    }

    fn into_entries(self) -> Vec<(MarketDataType, ())> {
        self.into_iter()
            .map(|market_data_type| (market_data_type, ()))
            .collect()
    }
}

impl<Value> TopicLeaves<Value> for BTreeMap<MarketDataType, Value> {
    fn insert(&mut self, market_data_type: MarketDataType, value: Value) {
        self.insert(market_data_type, value);
    }

    fn into_entries(self) -> Vec<(MarketDataType, Value)> {
        self.into_iter().collect()
    }
}

fn nest<Leaf, Value>(entries: impl Iterator<Item = (Topic, Value)>) -> TopicTree<Leaf>
where
    Leaf: Default + TopicLeaves<Value>,
{
    let mut tree: TopicTree<Leaf> = BTreeMap::new();
    for (topic, value) in entries {
        tree.entry(topic.instrument.exchange)
            .or_default()
            .entry(topic.instrument.key())
            .or_default()
            .insert(topic.market_data_type, value);
    }
    tree
}

fn unnest<Leaf, Value>(tree: TopicTree<Leaf>) -> Result<HashMap<Topic, Value>, String>
where
    Leaf: TopicLeaves<Value>,
{
    let mut values = HashMap::new();
    for (exchange, instruments) in tree {
        for (key, leaves) in instruments {
            let instrument = Instrument::from_key(exchange, &key)?;
            for (market_data_type, value) in leaves.into_entries() {
                values.insert(
                    Topic {
                        instrument,
                        market_data_type,
                    },
                    value,
                );
            }
        }
    }
    Ok(values)
}

/// `HashSet<Topic>` as `{exchange: {instrument: [market_data_type, ...]}}`.
mod topic_set {
    use std::collections::{BTreeSet, HashSet};

    use serde::de::Error as DeserializeError;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{TopicTree, nest, unnest};
    use crate::types::{MarketDataType, Topic};

    pub(super) fn serialize<S: Serializer>(
        topics: &HashSet<Topic>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let tree: TopicTree<BTreeSet<MarketDataType>> =
            nest(topics.iter().map(|topic| (*topic, ())));
        tree.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashSet<Topic>, D::Error> {
        let tree = TopicTree::<BTreeSet<MarketDataType>>::deserialize(deserializer)?;
        let topics = unnest(tree).map_err(DeserializeError::custom)?;
        Ok(topics.into_iter().map(|(topic, ())| topic).collect())
    }
}

/// `HashMap<Topic, Value>` as `{exchange: {instrument: {market_data_type: value}}}`.
mod topic_map {
    use std::collections::{BTreeMap, HashMap};

    use serde::de::Error as DeserializeError;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{TopicTree, nest, unnest};
    use crate::types::{MarketDataType, Topic};

    pub(super) fn serialize<Value: Serialize, S: Serializer>(
        values: &HashMap<Topic, Value>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let tree: TopicTree<BTreeMap<MarketDataType, &Value>> =
            nest(values.iter().map(|(topic, value)| (*topic, value)));
        tree.serialize(serializer)
    }

    pub(super) fn deserialize<'de, Value: Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<HashMap<Topic, Value>, D::Error> {
        let tree = TopicTree::<BTreeMap<MarketDataType, Value>>::deserialize(deserializer)?;
        unnest(tree).map_err(DeserializeError::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::types::{Currency, MarketDataType, UnderlyingType};

    fn topic(
        exchange: Exchange,
        underlying_type: UnderlyingType,
        market_data_type: MarketDataType,
    ) -> Topic {
        crate::testing::topic(
            crate::testing::instrument(exchange, Currency::Btc, Currency::Usdt, underlying_type),
            market_data_type,
        )
    }

    fn to_json<Message: Serialize>(message: &Message) -> Value {
        serde_json::to_value(message).expect("serialization is expected to not fail")
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
    fn response_nests_outcomes_and_round_trips() {
        let response = SubscriptionResponse {
            results: HashMap::from([
                (
                    topic(Exchange::Binance, UnderlyingType::Spot, MarketDataType::Bbo),
                    Ok(PathBuf::from("/dev/shm/a.dat")),
                ),
                (
                    topic(
                        Exchange::Binance,
                        UnderlyingType::Spot,
                        MarketDataType::Trade,
                    ),
                    Err("rejected".to_owned()),
                ),
            ]),
        };
        let expected = json!({
            "results": {"Binance": {"BTC-USDT-SPOT": {
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
    fn subscribed_response_round_trips() {
        let response = SubscribedResponse {
            topics: HashMap::from([(
                topic(
                    Exchange::Okx,
                    UnderlyingType::Perpetual,
                    MarketDataType::Trade,
                ),
                TopicStatus {
                    path: PathBuf::from("/dev/shm/b.dat"),
                    strategy_names: BTreeSet::from(["first".to_owned(), "second".to_owned()]),
                },
            )]),
        };
        let expected = json!({
            "topics": {"OKX": {"BTC-USDT-PERP": {"TRADE": {
                "path": "/dev/shm/b.dat",
                "strategy_names": ["first", "second"],
            }}}},
        });
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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::{Duration, Instant};

use chrono::{NaiveDateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::task::spawn_blocking;
use ureq::http::Response;
use ureq::{Body, Error};

use layline::build::GIT_COMMIT_HASH;
use layline::provider_api::{
    CONNECTED_APIS_PATH, HEALTH_PATH, INFO_PATH, ProviderInfo, RESUBSCRIBE_PATH, SUBSCRIBE_PATH,
    SUBSCRIBED_PATH, SubscribedResponse, Subscription, SubscriptionRequest, SubscriptionResponse,
    UNSUBSCRIBE_PATH,
};
use layline::shared_memory::MarketDataReader;
use layline::testing::write_secret_file;
use layline::types::{
    Currency, Exchange, Instrument, MarketData, MarketDataType, StrategyName, Topic, UnderlyingType,
};
use market_data_provider::config::MarketDataProviderConfig;
use market_data_provider::server::{run, serve};

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(5);
const LONG_LEASE: Duration = Duration::from_secs(300);
const SHORT_LEASE: Duration = Duration::from_millis(1);
// Longer than the lease sweep interval plus the short lease.
const EXPIRY_WAIT: Duration = Duration::from_millis(1500);
const FIRST_STRATEGY_NAME: &str = "first_strategy";
const SECOND_STRATEGY_NAME: &str = "second_strategy";
/// Any 32 bytes, base64-encoded, make a valid X25519 key for parsing.
const DUMMY_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

fn instrument(exchange: Exchange, base: Currency, underlying_type: UnderlyingType) -> Instrument {
    Instrument {
        exchange,
        base,
        quote: Currency::Usdt,
        underlying_type,
    }
}

fn okx_bitcoin_spot_bbo() -> Topic {
    Topic {
        instrument: instrument(Exchange::Okx, Currency::Btc, UnderlyingType::Spot),
        market_data_type: MarketDataType::Bbo,
    }
}

fn okx_ether_spot_bbo() -> Topic {
    Topic {
        instrument: instrument(Exchange::Okx, Currency::Eth, UnderlyingType::Spot),
        market_data_type: MarketDataType::Bbo,
    }
}

/// OKX only: its connector is still a dummy, while Binance's would reach the venue and Phanes.
/// The two spot BBO topics share one ring.
fn topics() -> HashSet<Topic> {
    HashSet::from([
        okx_bitcoin_spot_bbo(),
        okx_ether_spot_bbo(),
        Topic {
            instrument: instrument(Exchange::Okx, Currency::Btc, UnderlyingType::Spot),
            market_data_type: MarketDataType::Trade,
        },
        Topic {
            instrument: instrument(Exchange::Okx, Currency::Btc, UnderlyingType::Perpetual),
            market_data_type: MarketDataType::Bbo,
        },
    ])
}

/// The `[gnosis_client]` and `[celestia_client]` tables only have to parse: OKX never asks for a key, and
/// `serve` reports nothing to Celestia.
fn config(port: u16, demo: bool) -> MarketDataProviderConfig {
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let config_file = directory.path().join("market_data_provider.toml");
    let text = format!(
        "port = {port}\ndemo = {demo}\nprocess_metrics_interval_seconds = 10\n\n[gnosis_client]\nurl = \"https://phanes.invalid\"\nclient_gnosis_private_key = \"{DUMMY_KEY}\"\nphanes_gnosis_public_key = \"{DUMMY_KEY}\"\n\n[celestia_client]\nurl = \"https://phanes.invalid\"\nclient_celestia_private_key = \"{DUMMY_KEY}\"\nphanes_celestia_public_key = \"{DUMMY_KEY}\"\n"
    );
    write_secret_file(&config_file, &text);
    MarketDataProviderConfig::load(&config_file).expect("the config is expected to load")
}

async fn start_provider(root_directory: &Path, lease_duration: Duration) -> SocketAddr {
    start_provider_in_environment(root_directory, lease_duration, false).await
}

/// Listens on a free port.
async fn start_provider_in_environment(
    root_directory: &Path,
    lease_duration: Duration,
    demo: bool,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind is expected to not fail");
    let address = listener
        .local_addr()
        .expect("local address is expected to be known");
    tokio::spawn(serve(
        config(0, demo),
        listener,
        root_directory.to_owned(),
        lease_duration,
    ));
    address
}

fn url(address: SocketAddr, path: &str) -> String {
    format!("http://{address}{path}")
}

fn post(
    address: SocketAddr,
    path: &str,
    strategy_name: &StrategyName,
    topics: &HashSet<Topic>,
) -> Response<Body> {
    post_request(
        address,
        path,
        &SubscriptionRequest {
            strategy_name: strategy_name.clone(),
            demo: false,
            topics: topics.clone(),
        },
    )
}

fn post_request(address: SocketAddr, path: &str, request: &SubscriptionRequest) -> Response<Body> {
    ureq::post(&url(address, path))
        .send_json(request)
        .expect("request is expected to not fail")
}

fn accepted_paths(mut response: Response<Body>) -> HashMap<Topic, PathBuf> {
    let response: SubscriptionResponse = response
        .body_mut()
        .read_json()
        .expect("response is expected to be json");
    response
        .results
        .into_iter()
        .map(|(topic, subscription_result)| {
            let path = subscription_result.expect("subscription is expected to succeed");
            (topic, path)
        })
        .collect()
}

fn subscribed_topics(address: SocketAddr) -> HashSet<Topic> {
    let response: SubscribedResponse = ureq::get(&url(address, SUBSCRIBED_PATH))
        .call()
        .expect("request is expected to not fail")
        .body_mut()
        .read_json()
        .expect("response is expected to be json");
    response.topics
}

fn assert_server_time_is_utc_milliseconds(server_time: &str) {
    let time = server_time
        .strip_suffix(" UTC")
        .expect("server time is expected to end with UTC");
    let (_, milliseconds) = time
        .rsplit_once('.')
        .expect("server time is expected to have a fraction");
    assert_eq!(3, milliseconds.len());
    let parsed = NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S%.f")
        .expect("server time is expected to parse")
        .and_utc();
    let drift = (Utc::now() - parsed).abs();
    assert!(
        drift < TimeDelta::seconds(5),
        "server time is {drift} off UTC"
    );
}

fn receive_all(subscriptions: &[Subscription]) -> HashSet<Topic> {
    let market_data_reader = MarketDataReader::open(subscriptions)
        .expect("market_data_reader open is expected to not fail");
    let mut received = HashSet::new();
    let deadline = Instant::now() + RECEIVE_TIMEOUT;
    while received.len() < subscriptions.len() && Instant::now() < deadline {
        market_data_reader
            .poll(|data: MarketData, _received_at_nanoseconds| {
                received.insert(data.topic());
            })
            .expect("poll is expected to not fail");
    }
    received
}

#[tokio::test(flavor = "multi_thread")]
async fn strategy_receives_every_subscribed_topic_from_each_exchange() {
    let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), LONG_LEASE).await;

    let received = spawn_blocking(move || {
        let subscriptions: Vec<Subscription> = accepted_paths(post(
            address,
            SUBSCRIBE_PATH,
            &first_strategy_name,
            &topics(),
        ))
        .into_iter()
        .map(|(topic, path)| Subscription { topic, path })
        .collect();
        receive_all(&subscriptions)
    })
    .await
    .expect("strategy task is expected to not panic");

    assert_eq!(topics(), received);
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_stays_subscribed_until_its_last_strategy_leaves() {
    let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
    let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), LONG_LEASE).await;

    spawn_blocking(move || {
        let first = accepted_paths(post(
            address,
            SUBSCRIBE_PATH,
            &first_strategy_name,
            &topics(),
        ));
        let second = accepted_paths(post(
            address,
            SUBSCRIBE_PATH,
            &second_strategy_name,
            &topics(),
        ));
        assert!(
            first.iter().all(|(topic, path)| second[topic] != *path),
            "each strategy is expected to read its own rings"
        );
        assert_eq!(topics(), subscribed_topics(address));

        let response = post(address, UNSUBSCRIBE_PATH, &first_strategy_name, &topics());
        assert_eq!(204, response.status());
        assert_eq!(topics(), subscribed_topics(address));

        post(address, UNSUBSCRIBE_PATH, &second_strategy_name, &topics());
        assert!(subscribed_topics(address).is_empty());
    })
    .await
    .expect("strategy task is expected to not panic");
}

#[tokio::test(flavor = "multi_thread")]
async fn resubscribe_replaces_the_strategy_set() {
    let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), LONG_LEASE).await;

    spawn_blocking(move || {
        post(address, SUBSCRIBE_PATH, &first_strategy_name, &topics());
        post(
            address,
            RESUBSCRIBE_PATH,
            &first_strategy_name,
            &HashSet::from([okx_bitcoin_spot_bbo()]),
        );
        assert_eq!(
            HashSet::from([okx_bitcoin_spot_bbo()]),
            subscribed_topics(address)
        );
    })
    .await
    .expect("strategy task is expected to not panic");
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_lease_releases_topics() {
    let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), SHORT_LEASE).await;

    spawn_blocking(move || {
        let first = accepted_paths(post(
            address,
            SUBSCRIBE_PATH,
            &first_strategy_name,
            &topics(),
        ));
        sleep(EXPIRY_WAIT);
        assert!(subscribed_topics(address).is_empty());

        let returning = accepted_paths(post(
            address,
            SUBSCRIBE_PATH,
            &first_strategy_name,
            &topics(),
        ));
        assert_eq!(first, returning);
    })
    .await
    .expect("strategy task is expected to not panic");
}

#[tokio::test(flavor = "multi_thread")]
async fn health_info_and_validation_endpoints_respond() {
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), LONG_LEASE).await;

    spawn_blocking(move || {
        let health = ureq::get(&url(address, HEALTH_PATH))
            .call()
            .expect("health check is expected to not fail")
            .body_mut()
            .read_to_string()
            .expect("health body is expected to be text");
        assert_eq!("ok", health);

        let info: ProviderInfo = ureq::get(&url(address, INFO_PATH))
            .call()
            .expect("info request is expected to not fail")
            .body_mut()
            .read_json()
            .expect("info is expected to be json");
        assert_eq!(env!("CARGO_PKG_VERSION"), info.version);
        assert_eq!("debug", info.build);
        assert_eq!(GIT_COMMIT_HASH, info.git_commit_hash);
        assert_eq!(LONG_LEASE.as_secs(), info.lease_duration_seconds);
        assert_server_time_is_utc_milliseconds(&info.server_time);

        let rejection_result =
            ureq::post(&url(address, SUBSCRIBE_PATH)).send_json(SubscriptionRequest {
                strategy_name: " ".to_owned(),
                demo: false,
                topics: topics(),
            });
        assert!(matches!(rejection_result, Err(Error::StatusCode(400))));
    })
    .await
    .expect("strategy task is expected to not panic");
}

#[tokio::test(flavor = "multi_thread")]
async fn demo_request_is_refused_while_provider_serves_live() {
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), LONG_LEASE).await;

    spawn_blocking(move || {
        let request = SubscriptionRequest {
            strategy_name: StrategyName::from(FIRST_STRATEGY_NAME),
            demo: true,
            topics: HashSet::from([okx_bitcoin_spot_bbo()]),
        };
        let rejection_result = ureq::post(&url(address, SUBSCRIBE_PATH)).send_json(request);
        assert!(matches!(rejection_result, Err(Error::StatusCode(400))));
        assert!(subscribed_topics(address).is_empty());
    })
    .await
    .expect("strategy task is expected to not panic");
}

#[tokio::test(flavor = "multi_thread")]
async fn demo_provider_accepts_demo_requests() {
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider_in_environment(directory.path(), LONG_LEASE, true).await;

    spawn_blocking(move || {
        let request = SubscriptionRequest {
            strategy_name: StrategyName::from(FIRST_STRATEGY_NAME),
            demo: true,
            topics: HashSet::from([okx_bitcoin_spot_bbo()]),
        };
        accepted_paths(post_request(address, SUBSCRIBE_PATH, &request));
        assert!(subscribed_topics(address).contains(&okx_bitcoin_spot_bbo()));
    })
    .await
    .expect("strategy task is expected to not panic");
}

#[tokio::test(flavor = "multi_thread")]
async fn connected_apis_lists_every_exchange() {
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), LONG_LEASE).await;

    spawn_blocking(move || {
        let connected_apis: Value = ureq::get(&url(address, CONNECTED_APIS_PATH))
            .call()
            .expect("connected apis request is expected to not fail")
            .body_mut()
            .read_json()
            .expect("connected apis is expected to be json");
        assert_eq!(json!([]), connected_apis["OKX"]);
        let binance_apis = connected_apis["Binance"]
            .as_array()
            .expect("Binance lists its venue APIs");
        assert!(
            binance_apis
                .iter()
                .any(|api| api["api"] == "wss://stream-sbe.binance.com:9443/stream")
        );
    })
    .await
    .expect("strategy task is not expected to panic");
}

#[tokio::test(flavor = "multi_thread")]
async fn second_provider_refuses_a_taken_address() {
    let directory = tempdir().expect("temp dir creation is expected to not fail");
    let address = start_provider(directory.path(), LONG_LEASE).await;

    assert!(run(config(address.port(), false)).await.is_err());
}

//! The strategy's side of the provider's HTTP API: subscribe once, then keep the lease by
//! resubscribing periodically. Both run on the strategy's background runtime.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Result, bail};
use reqwest::{Client, StatusCode};
use tokio::time::sleep;
use tracing::{debug, error, warn};
use url::Url;

use layline::backoff::exponential_backoff;
use layline::provider_api::{
    RESUBSCRIBE_PATH, SUBSCRIBE_PATH, Subscription, SubscriptionRequest, SubscriptionResponse,
    SubscriptionResult, group_by_reason,
};
use layline::types::{StrategyName, Topic};

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
// Far below the provider's lease; a restarted provider gets the topics back within this interval.
const RESUBSCRIBE_INTERVAL: Duration = Duration::from_secs(10);

/// One strategy's subscription to the provider: the same request, first to subscribe, then to
/// resubscribe.
pub struct ProviderSession {
    client: Client,
    /// `http://<provider_address>/`; each request joins its path to it.
    base_url: Url,
    request: SubscriptionRequest,
    resubscribe_interval: Duration,
}

impl ProviderSession {
    pub fn new(
        provider_address: SocketAddr,
        strategy_name: StrategyName,
        demo: bool,
        topics: HashSet<Topic>,
    ) -> Result<Self> {
        Self::with_resubscribe_interval(
            provider_address,
            strategy_name,
            demo,
            topics,
            RESUBSCRIBE_INTERVAL,
        )
    }

    fn with_resubscribe_interval(
        provider_address: SocketAddr,
        strategy_name: StrategyName,
        demo: bool,
        topics: HashSet<Topic>,
        resubscribe_interval: Duration,
    ) -> Result<Self> {
        Ok(Self {
            client: Client::builder().timeout(REQUEST_TIMEOUT).build()?,
            base_url: Url::parse(&format!("http://{provider_address}"))?,
            request: SubscriptionRequest {
                strategy_name,
                demo,
                topics,
            },
            resubscribe_interval,
        })
    }

    /// Retries until the provider accepts every topic; fails at once if it rejects a topic or
    /// refuses the request itself.
    pub async fn subscribe(&self) -> Result<Vec<Subscription>> {
        extract_successful_subscriptions(self.post_with_retry(SUBSCRIBE_PATH).await?)
    }

    /// Keeps the lease, and restores the subscription after the provider restarts. Never returns;
    /// panics when the provider refuses the request or its answer shows the two sides disagree.
    pub async fn resubscribe_forever(self, subscriptions: Vec<Subscription>) {
        loop {
            sleep(self.resubscribe_interval).await;
            match self.post(RESUBSCRIBE_PATH).await {
                Ok(results) => report_changes(&subscriptions, &results),
                Err(error) if error.is::<Refusal>() => {
                    panic!("provider refused to resubscribe: {error:#}")
                }
                Err(error) => warn!("resubscribing failed: {error:#}"),
            }
        }
    }

    /// The URL of the API endpoint at `path`, one of the `*_PATH` constants.
    fn url_of(&self, path: &str) -> Url {
        self.base_url
            .join(path)
            .expect("an absolute API path joins to any http URL")
    }

    async fn post(&self, path: &str) -> Result<HashMap<Topic, SubscriptionResult>> {
        let response = self
            .client
            .post(self.url_of(path))
            .json(&self.request)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            // The body of an error answer carries the provider's reason.
            let reason = response.text().await?;
            if status.is_client_error() {
                return Err(Refusal { status, reason }.into());
            }
            bail!("provider answered {status}: {reason}");
        }
        let response: SubscriptionResponse = response.json().await?;
        Ok(response.results)
    }

    /// Retries until the provider answers, unless it refuses the request itself.
    async fn post_with_retry(&self, path: &str) -> Result<HashMap<Topic, SubscriptionResult>> {
        let mut failures = 0;
        loop {
            match self.post(path).await {
                Ok(results) => return Ok(results),
                Err(error) if error.is::<Refusal>() => return Err(error),
                Err(error) => {
                    failures += 1;
                    let delay = exponential_backoff(failures, INITIAL_RETRY_DELAY, MAX_RETRY_DELAY);
                    let url = self.url_of(path);
                    warn!("{url} failed, retrying in {delay:?}: {error:#}");
                    sleep(delay).await;
                }
            }
        }
    }
}

/// A 4xx answer, such as a `demo` the provider does not serve: sending the request again cannot help.
#[derive(Debug, thiserror::Error)]
#[error("provider refused the request with {status}: {reason}")]
struct Refusal {
    status: StatusCode,
    reason: String,
}

fn extract_successful_subscriptions(
    results: HashMap<Topic, SubscriptionResult>,
) -> Result<Vec<Subscription>> {
    let mut subscriptions = Vec::with_capacity(results.len());
    let mut rejections = HashMap::new();
    for (topic, subscription_result) in results {
        match subscription_result {
            Ok(path) => subscriptions.push(Subscription { topic, path }),
            Err(reason) => {
                rejections.insert(topic, reason);
            }
        }
    }
    if !rejections.is_empty() {
        bail!(describe_rejections(&rejections));
    }
    Ok(subscriptions)
}

/// One line per reason, then its topics one per line, indented under it:
///
/// ```text
/// provider rejected 2 topics:
///   SpotSbe: Binance refused the request
///     Binance/BTC-USDT-SPOT/BBO
///     Binance/BTC-USDT-SPOT/TRADE
/// ```
fn describe_rejections(rejections: &HashMap<Topic, String>) -> String {
    let mut lines = vec![format!("provider rejected {} topics:", rejections.len())];
    for (reason, topics) in group_by_reason(rejections) {
        lines.push(format!("  {reason}"));
        lines.extend(topics.iter().map(|topic| format!("    {topic}")));
    }
    lines.join("\n")
}

fn report_changes(subscriptions: &[Subscription], results: &HashMap<Topic, SubscriptionResult>) {
    for subscription in subscriptions {
        match results.get(&subscription.topic) {
            // The provider derives ring paths from the strategy and the topic, so they never move.
            Some(Ok(path)) => assert_eq!(
                subscription.path, *path,
                "provider moved {}",
                subscription.topic
            ),
            Some(Err(reason)) => {
                let topic = subscription.topic;
                error!("provider rejected {topic} on resubscription: {reason}");
            }
            None => panic!(
                "provider answers every topic, but not {} on resubscription",
                subscription.topic
            ),
        }
    }
    debug!("resubscribed to {} topics", results.len());
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::mpsc::{Sender, channel};
    use std::thread::spawn as spawn_thread;

    use tokio::spawn;
    use tokio::task::spawn_blocking;

    use super::*;
    use layline::types::{Currency, Exchange, Instrument, MarketDataType, UnderlyingType};

    const STRATEGY_NAME: &str = "test_strategy";
    const TEST_RESUBSCRIBE_INTERVAL: Duration = Duration::from_millis(50);

    fn topic() -> Topic {
        Topic {
            instrument: Instrument {
                exchange: Exchange::Binance,
                base: Currency::Eth,
                quote: Currency::Usdt,
                underlying_type: UnderlyingType::Spot,
            },
            market_data_type: MarketDataType::Trade,
        }
    }

    /// Answers one HTTP request as a provider would, forwarding its path and body.
    fn answer(
        listener: &TcpListener,
        subscription_result: &Result<PathBuf, String>,
        requests: &Sender<(String, SubscriptionRequest)>,
    ) {
        let (stream, _) = listener.accept().expect("accept is expected to not fail");
        let (path, request) = read_request(&stream);
        let results = request
            .topics
            .iter()
            .map(|topic| (*topic, subscription_result.clone()))
            .collect();
        let body = serde_json::to_string(&SubscriptionResponse { results })
            .expect("serialization is expected to not fail");
        let content_length = body.len();
        write!(
            &stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n{body}"
        )
        .expect("write is expected to not fail");
        requests
            .send((path, request))
            .expect("test receiver is expected to be alive");
    }

    fn read_request(stream: &TcpStream) -> (String, SubscriptionRequest) {
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .expect("read is expected to not fail");
        let path = request_line
            .split_whitespace()
            .nth(1)
            .expect("request line is expected to hold a path")
            .to_owned();
        let mut content_length = 0;
        loop {
            let mut header = String::new();
            reader
                .read_line(&mut header)
                .expect("read is expected to not fail");
            if header.trim().is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = value
                    .trim()
                    .parse()
                    .expect("content length is expected to be a number");
            }
        }
        let mut body = vec![0; content_length];
        reader
            .read_exact(&mut body)
            .expect("read is expected to not fail");
        let request = serde_json::from_slice(&body).expect("body is expected to be a request");
        (path, request)
    }

    #[tokio::test]
    async fn client_subscribes_then_keeps_resubscribing() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind is expected to not fail");
        let address = listener
            .local_addr()
            .expect("local address is expected to be known");
        let subscription_result = Ok(PathBuf::from("/dev/shm/TRADE.dat"));
        let (requests, received) = channel();
        let provider = spawn_thread(move || {
            for _ in 0..2 {
                answer(&listener, &subscription_result, &requests);
            }
        });

        let session = ProviderSession::with_resubscribe_interval(
            address,
            STRATEGY_NAME.to_owned(),
            true,
            HashSet::from([topic()]),
            TEST_RESUBSCRIBE_INTERVAL,
        )
        .expect("the session is expected to build");
        let subscriptions = session
            .subscribe()
            .await
            .expect("subscribe is expected to not fail");
        spawn(session.resubscribe_forever(subscriptions.clone()));
        assert_eq!(
            vec![Subscription {
                topic: topic(),
                path: PathBuf::from("/dev/shm/TRADE.dat"),
            }],
            subscriptions
        );
        let expected_request = SubscriptionRequest {
            strategy_name: STRATEGY_NAME.to_owned(),
            demo: true,
            topics: HashSet::from([topic()]),
        };
        // The fake provider answers on its own thread; waiting for it must not stall the runtime.
        let received_requests = spawn_blocking(move || {
            [received.recv(), received.recv()]
                .map(|request_result| request_result.expect("a request is expected"))
        })
        .await
        .expect("the receiving task is expected to not panic");
        for ((path, request), expected_path) in received_requests
            .into_iter()
            .zip([SUBSCRIBE_PATH, RESUBSCRIBE_PATH])
        {
            assert_eq!(expected_path, path);
            assert_eq!(expected_request, request);
        }
        provider
            .join()
            .expect("provider thread is expected to not panic");
    }

    #[tokio::test]
    async fn rejected_topic_fails_subscribe() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind is expected to not fail");
        let address = listener
            .local_addr()
            .expect("local address is expected to be known");
        let (requests, _received) = channel();
        let provider = spawn_thread(move || {
            answer(&listener, &Err("unmapped instrument".to_owned()), &requests);
        });

        let error = ProviderSession::new(
            address,
            STRATEGY_NAME.to_owned(),
            false,
            HashSet::from([topic()]),
        )
        .expect("the session is expected to build")
        .subscribe()
        .await
        .expect_err("rejected subscription is expected to fail");
        assert!(error.to_string().contains("unmapped instrument"));
        provider
            .join()
            .expect("provider thread is expected to not panic");
    }

    #[test]
    fn rejections_are_listed_one_line_each_under_their_reason() {
        let spot_trade = topic();
        let spot_bbo = Topic {
            market_data_type: MarketDataType::Bbo,
            ..spot_trade
        };
        let perpetual_trade = Topic {
            instrument: Instrument {
                underlying_type: UnderlyingType::Perpetual,
                ..spot_trade.instrument
            },
            ..spot_trade
        };
        let description = describe_rejections(&HashMap::from([
            (spot_trade, "SpotSbe: refused".to_owned()),
            (perpetual_trade, "FuturesPublic: timed out".to_owned()),
            (spot_bbo, "SpotSbe: refused".to_owned()),
        ]));
        assert_eq!(
            "provider rejected 3 topics:\n  \
             FuturesPublic: timed out\n    \
             Binance/ETH-USDT-PERP/TRADE\n  \
             SpotSbe: refused\n    \
             Binance/ETH-USDT-SPOT/BBO\n    \
             Binance/ETH-USDT-SPOT/TRADE",
            description
        );
    }

    #[test]
    fn unchanged_or_rejected_resubscription_keeps_running() {
        let subscriptions = [Subscription {
            topic: topic(),
            path: PathBuf::from("/dev/shm/old.dat"),
        }];
        for subscription_result in [
            Ok(PathBuf::from("/dev/shm/old.dat")),
            Err("gone".to_owned()),
        ] {
            report_changes(
                &subscriptions,
                &HashMap::from([(topic(), subscription_result)]),
            );
        }
    }

    #[test]
    #[should_panic(expected = "provider moved")]
    fn moved_ring_path_panics() {
        let subscriptions = [Subscription {
            topic: topic(),
            path: PathBuf::from("/dev/shm/old.dat"),
        }];
        let results = HashMap::from([(topic(), Ok(PathBuf::from("/dev/shm/new.dat")))]);
        report_changes(&subscriptions, &results);
    }

    #[test]
    #[should_panic(expected = "provider answers every topic")]
    fn unanswered_topic_panics() {
        let subscriptions = [Subscription {
            topic: topic(),
            path: PathBuf::from("/dev/shm/old.dat"),
        }];
        report_changes(&subscriptions, &HashMap::new());
    }

    /// Answers one request with `status_line` and a plain-text `reason`, as the provider refuses one.
    fn answer_with_status(listener: &TcpListener, status_line: &str, reason: &str) {
        let (stream, _) = listener.accept().expect("accept is expected to not fail");
        read_request(&stream);
        let content_length = reason.len();
        write!(
            &stream,
            "HTTP/1.1 {status_line}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n{reason}"
        )
        .expect("write is expected to not fail");
    }

    #[tokio::test]
    async fn client_error_fails_subscribe_with_the_providers_reason() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind is expected to not fail");
        let address = listener
            .local_addr()
            .expect("local address is expected to be known");
        // Answers only once: a retry would block on a connection nobody accepts.
        let provider = spawn_thread(move || {
            answer_with_status(
                &listener,
                "400 Bad Request",
                "provider serves demo data, not live",
            );
        });

        let error = ProviderSession::new(
            address,
            STRATEGY_NAME.to_owned(),
            false,
            HashSet::from([topic()]),
        )
        .expect("the session is expected to build")
        .subscribe()
        .await
        .expect_err("a refused subscription is expected to fail");
        assert!(error.is::<Refusal>(), "{error:#}");
        assert!(
            error
                .to_string()
                .contains("provider serves demo data, not live"),
            "{error:#}"
        );
        provider
            .join()
            .expect("provider thread is expected to not panic");
    }

    #[tokio::test]
    async fn server_error_is_retried() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind is expected to not fail");
        let address = listener
            .local_addr()
            .expect("local address is expected to be known");
        let subscription_result = Ok(PathBuf::from("/dev/shm/TRADE.dat"));
        let (requests, _received) = channel();
        let provider = spawn_thread(move || {
            answer_with_status(&listener, "500 Internal Server Error", "busy");
            answer(&listener, &subscription_result, &requests);
        });

        let subscriptions = ProviderSession::new(
            address,
            STRATEGY_NAME.to_owned(),
            true,
            HashSet::from([topic()]),
        )
        .expect("the session is expected to build")
        .subscribe()
        .await
        .expect("subscribe is expected to succeed once the provider recovers");
        assert_eq!(1, subscriptions.len());
        provider
            .join()
            .expect("provider thread is expected to not panic");
    }
}

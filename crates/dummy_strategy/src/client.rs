use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use anyhow::{Result, bail};
use tracing::{debug, error, warn};
use ureq::Agent;

use layline::backoff::exponential_backoff;
use layline::provider_api::{
    RESUBSCRIBE_PATH, SUBSCRIBE_PATH, Subscription, SubscriptionRequest, SubscriptionResponse,
    SubscriptionResult, group_by_reason,
};
use layline::types::Topic;

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
// Far below the provider's lease; a restarted provider gets the topics back within this interval.
const RESUBSCRIBE_INTERVAL: Duration = Duration::from_secs(10);

/// Blocks until the provider accepts every topic, then resubscribes periodically on a background thread.
pub fn subscribe(
    provider_address: SocketAddr,
    strategy_name: &str,
    demo: bool,
    topics: HashSet<Topic>,
) -> Result<Vec<Subscription>> {
    subscribe_with_resubscribe_interval(
        provider_address,
        strategy_name,
        demo,
        topics,
        RESUBSCRIBE_INTERVAL,
    )
}

fn subscribe_with_resubscribe_interval(
    provider_address: SocketAddr,
    strategy_name: &str,
    demo: bool,
    topics: HashSet<Topic>,
    resubscribe_interval: Duration,
) -> Result<Vec<Subscription>> {
    let session = ProviderSession {
        agent: Agent::config_builder()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .build()
            .into(),
        base_url: format!("http://{provider_address}"),
        request: SubscriptionRequest {
            strategy_name: strategy_name.to_owned(),
            demo,
            topics,
        },
        resubscribe_interval,
    };
    let subscriptions = into_subscriptions(session.post_with_retry(SUBSCRIBE_PATH))?;
    let renewed_subscriptions = subscriptions.clone();
    thread::Builder::new()
        .name("provider-session".to_owned())
        .spawn(move || session.resubscribe_forever(&renewed_subscriptions))?;
    Ok(subscriptions)
}

struct ProviderSession {
    agent: Agent,
    base_url: String,
    request: SubscriptionRequest,
    resubscribe_interval: Duration,
}

impl ProviderSession {
    fn post(&self, path: &str) -> Result<HashMap<Topic, SubscriptionResult>> {
        let response: SubscriptionResponse = self
            .agent
            .post(&format!("{}{path}", self.base_url))
            .send_json(&self.request)?
            .body_mut()
            .read_json()?;
        Ok(response.results)
    }

    fn post_with_retry(&self, path: &str) -> HashMap<Topic, SubscriptionResult> {
        let mut failures = 0;
        loop {
            match self.post(path) {
                Ok(results) => return results,
                Err(error) => {
                    failures += 1;
                    let delay = exponential_backoff(failures, INITIAL_RETRY_DELAY, MAX_RETRY_DELAY);
                    warn!(
                        "{}{path} failed, retrying in {delay:?}: {error:#}",
                        self.base_url
                    );
                    thread::sleep(delay);
                }
            }
        }
    }

    fn resubscribe_forever(&self, subscriptions: &[Subscription]) {
        loop {
            thread::sleep(self.resubscribe_interval);
            match self.post(RESUBSCRIBE_PATH) {
                Ok(results) => report_changes(subscriptions, &results),
                Err(error) => warn!("resubscribing failed: {error:#}"),
            }
        }
    }
}

// Claude TODO: 这个方法的名字太迷惑了, 应该叫extract_succesful_subscriptions.
fn into_subscriptions(results: HashMap<Topic, SubscriptionResult>) -> Result<Vec<Subscription>> {
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
    for (reason, topic_names) in group_by_reason(rejections) {
        lines.push(format!("  {reason}"));
        lines.extend(topic_names.iter().map(|name| format!("    {name}")));
    }
    lines.join("\n")
}

fn report_changes(subscriptions: &[Subscription], results: &HashMap<Topic, SubscriptionResult>) {
    for subscription in subscriptions {
        match results.get(&subscription.topic) {
            Some(Ok(path)) if *path == subscription.path => {}
            Some(Ok(path)) => error!(
                "provider moved {} to {}, still reading {}",
                subscription.topic,
                path.display(),
                subscription.path.display()
            ),
            Some(Err(reason)) => error!(
                "provider rejected {} on resubscription: {reason}",
                subscription.topic
            ),
            None => error!(
                "provider did not answer for {} on resubscription",
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
        write!(
            &stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
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

    #[test]
    fn client_subscribes_then_keeps_resubscribing() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind is expected to not fail");
        let address = listener
            .local_addr()
            .expect("local address is expected to be known");
        let subscription_result = Ok(PathBuf::from("/dev/shm/TRADE.dat"));
        let (requests, received) = channel();
        let provider = thread::spawn(move || {
            for _ in 0..2 {
                answer(&listener, &subscription_result, &requests);
            }
        });

        let subscriptions = subscribe_with_resubscribe_interval(
            address,
            STRATEGY_NAME,
            true,
            HashSet::from([topic()]),
            TEST_RESUBSCRIBE_INTERVAL,
        )
        .expect("subscribe is expected to not fail");
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
        for expected_path in [SUBSCRIBE_PATH, RESUBSCRIBE_PATH] {
            let (path, request) = received.recv().expect("a request is expected");
            assert_eq!(expected_path, path);
            assert_eq!(expected_request, request);
        }
        provider
            .join()
            .expect("provider thread is expected to not panic");
    }

    #[test]
    fn rejected_topic_fails_subscribe() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind is expected to not fail");
        let address = listener
            .local_addr()
            .expect("local address is expected to be known");
        let (requests, _received) = channel();
        let provider = thread::spawn(move || {
            answer(&listener, &Err("unmapped instrument".to_owned()), &requests);
        });

        let error = subscribe(address, STRATEGY_NAME, false, HashSet::from([topic()]))
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
    fn changed_or_rejected_resubscription_is_reported() {
        let subscriptions = [Subscription {
            topic: topic(),
            path: PathBuf::from("/dev/shm/old.dat"),
        }];
        for subscription_result in [
            Ok(PathBuf::from("/dev/shm/old.dat")),
            Ok(PathBuf::from("/dev/shm/new.dat")),
            Err("gone".to_owned()),
        ] {
            report_changes(
                &subscriptions,
                &HashMap::from([(topic(), subscription_result)]),
            );
        }
        report_changes(&subscriptions, &HashMap::new());
    }
}

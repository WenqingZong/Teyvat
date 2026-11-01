use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use futures::future::join_all;

use super::websocket::TungsteniteClient;
use super::worker::{WorkerHandle, WorkerReply, failed_reply, spawn_worker};
use super::{BinanceConnector, ConnectorContext, OkxConnector};
use crate::utils::RealClock;
use layline::types::{Exchange, Topic};

/// Routes each topic to the worker of its exchange. A worker starts with its first topic and stops with its last.
pub(crate) struct ExchangeRouter {
    root_directory: PathBuf,
    demo: bool,
    context: ConnectorContext,
    workers: HashMap<Exchange, WorkerHandle>,
}

impl ExchangeRouter {
    /// `demo` selects the venues' demo-trading data for every worker this router starts.
    pub(crate) fn new(root_directory: &Path, demo: bool, context: ConnectorContext) -> Self {
        Self {
            root_directory: root_directory.to_owned(),
            demo,
            context,
            workers: HashMap::new(),
        }
    }

    pub(crate) fn root_directory(&self) -> &Path {
        &self.root_directory
    }

    /// Whether an exchange's worker is alive, so active topics are not left without a publisher.
    pub(crate) fn is_running(&self, exchange: Exchange) -> bool {
        self.workers
            .get(&exchange)
            .is_some_and(|worker| !worker.has_stopped())
    }

    /// Returns why each topic was refused; topics missing from the map succeeded.
    pub(crate) async fn subscribe(&mut self, topics: &HashSet<Topic>) -> HashMap<Topic, String> {
        let by_exchange = group_by_exchange(topics);
        let start_failures = self.start_missing_workers(by_exchange.keys().copied());
        let workers = &self.workers;
        let start_failures = &start_failures;
        let replies = join_all(by_exchange.iter().map(|(exchange, topics)| {
            let exchange = *exchange;
            async move {
                match (workers.get(&exchange), start_failures.get(&exchange)) {
                    (Some(worker), None) => worker.subscribe(topics).await,
                    (_, reason) => failed_reply(
                        topics,
                        reason.map_or("worker is not running", String::as_str),
                    ),
                }
            }
        }))
        .await;
        merge_failures(replies)
    }

    /// Returns why each topic could not be unsubscribed; topics missing from the map succeeded.
    pub(crate) async fn unsubscribe(&mut self, topics: &HashSet<Topic>) -> HashMap<Topic, String> {
        let by_exchange = group_by_exchange(topics);
        let workers = &self.workers;
        let replies = join_all(by_exchange.iter().map(|(exchange, topics)| {
            let exchange = *exchange;
            async move {
                match workers.get(&exchange) {
                    Some(worker) if !worker.has_stopped() => worker.unsubscribe(topics).await,
                    _ => WorkerReply::new(),
                }
            }
        }))
        .await;
        merge_failures(replies)
    }

    /// Returns the reason for each worker that failed to start.
    fn start_missing_workers(
        &mut self,
        exchanges: impl IntoIterator<Item = Exchange>,
    ) -> HashMap<Exchange, String> {
        let missing: Vec<Exchange> = exchanges
            .into_iter()
            .filter(|exchange| {
                self.workers
                    .get(exchange)
                    .is_none_or(WorkerHandle::has_stopped)
            })
            .collect();
        let mut failures = HashMap::new();
        for exchange in missing {
            match spawn_exchange_worker(exchange, &self.root_directory, self.demo, &self.context) {
                Ok(worker) => {
                    self.workers.insert(exchange, worker);
                }
                Err(error) => {
                    self.workers.remove(&exchange);
                    failures.insert(
                        exchange,
                        format!("failed to start {exchange} worker: {error:#}"),
                    );
                }
            }
        }
        failures
    }
}

fn merge_failures(replies: Vec<WorkerReply>) -> HashMap<Topic, String> {
    replies.into_iter().flatten().collect()
}

/// The only per-exchange code in routing: add an arm here when adding an exchange.
fn spawn_exchange_worker(
    exchange: Exchange,
    root_directory: &Path,
    demo: bool,
    context: &ConnectorContext,
) -> Result<WorkerHandle> {
    match exchange {
        Exchange::Binance => {
            let context = context.clone();
            spawn_worker(root_directory, demo, move || {
                BinanceConnector::new(
                    demo,
                    |_| TungsteniteClient::default(),
                    context.phanes_client,
                    context.notifier,
                    RealClock,
                )
            })
        }
        Exchange::Okx => spawn_worker(root_directory, demo, OkxConnector::new),
    }
}

fn group_by_exchange(topics: &HashSet<Topic>) -> HashMap<Exchange, HashSet<Topic>> {
    let mut groups: HashMap<Exchange, HashSet<Topic>> = HashMap::new();
    for topic in topics {
        groups
            .entry(topic.instrument.exchange)
            .or_default()
            .insert(*topic);
    }
    groups
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::exchanges::tests::test_connector_context;
    use crate::exchanges::worker::tests::wait_until_stopped;
    use layline::types::{Currency, MarketDataType, UnderlyingType};

    fn topic(exchange: Exchange, market_data_type: MarketDataType) -> Topic {
        layline::testing::topic(
            layline::testing::instrument(
                exchange,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Spot,
            ),
            market_data_type,
        )
    }

    fn running_exchanges(router: &ExchangeRouter) -> HashSet<Exchange> {
        router
            .workers
            .iter()
            .filter(|(_, worker)| !worker.has_stopped())
            .map(|(exchange, _)| *exchange)
            .collect()
    }

    async fn wait_until_idle(router: &ExchangeRouter) {
        for worker in router.workers.values() {
            wait_until_stopped(worker).await;
        }
    }

    #[test]
    fn topics_group_by_exchange() {
        let topics = HashSet::from([
            topic(Exchange::Okx, MarketDataType::Bbo),
            topic(Exchange::Binance, MarketDataType::Bbo),
            topic(Exchange::Okx, MarketDataType::Trade),
        ]);
        assert_eq!(
            HashMap::from([
                (
                    Exchange::Okx,
                    HashSet::from([
                        topic(Exchange::Okx, MarketDataType::Bbo),
                        topic(Exchange::Okx, MarketDataType::Trade),
                    ])
                ),
                (
                    Exchange::Binance,
                    HashSet::from([topic(Exchange::Binance, MarketDataType::Bbo)])
                ),
            ]),
            group_by_exchange(&topics)
        );
    }

    /// OKX only: starting Binance's worker would reach the venue.
    #[tokio::test]
    async fn router_answers_every_topic() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let mut router = ExchangeRouter::new(directory.path(), false, test_connector_context());
        let topics = HashSet::from([
            topic(Exchange::Okx, MarketDataType::Bbo),
            topic(Exchange::Okx, MarketDataType::Trade),
        ]);

        let failures = router.subscribe(&topics).await;
        assert!(failures.is_empty(), "{failures:?}");
        assert!(
            topics
                .iter()
                .all(|topic| topic.file_path(directory.path()).exists())
        );
        assert!(router.unsubscribe(&topics).await.is_empty());
    }

    #[tokio::test]
    async fn workers_start_on_demand_and_stop_when_idle() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let mut router = ExchangeRouter::new(directory.path(), false, test_connector_context());
        assert!(running_exchanges(&router).is_empty());

        let okx_bbo = HashSet::from([topic(Exchange::Okx, MarketDataType::Bbo)]);
        let okx_trade = HashSet::from([topic(Exchange::Okx, MarketDataType::Trade)]);
        let okx_topics: HashSet<Topic> = okx_bbo.union(&okx_trade).copied().collect();
        assert!(router.subscribe(&okx_topics).await.is_empty());
        assert_eq!(HashSet::from([Exchange::Okx]), running_exchanges(&router));

        assert!(router.unsubscribe(&okx_bbo).await.is_empty());
        assert_eq!(HashSet::from([Exchange::Okx]), running_exchanges(&router));
        assert!(router.unsubscribe(&okx_trade).await.is_empty());
        wait_until_idle(&router).await;

        let okx_bbo = HashSet::from([topic(Exchange::Okx, MarketDataType::Bbo)]);
        assert!(router.unsubscribe(&okx_bbo).await.is_empty());
        assert!(running_exchanges(&router).is_empty());
    }

    #[tokio::test]
    async fn stopped_worker_is_replaced_on_next_subscribe() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let mut router = ExchangeRouter::new(directory.path(), false, test_connector_context());
        router
            .workers
            .insert(Exchange::Okx, WorkerHandle::stopped_for_test(Exchange::Okx));
        assert!(!router.is_running(Exchange::Okx));

        let okx_trade = HashSet::from([topic(Exchange::Okx, MarketDataType::Trade)]);
        assert!(router.subscribe(&okx_trade).await.is_empty());
        assert!(
            router
                .workers
                .get(&Exchange::Okx)
                .is_some_and(|worker| !worker.has_stopped())
        );
    }
}

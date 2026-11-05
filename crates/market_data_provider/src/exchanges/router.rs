use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use strum::IntoEnumIterator;

use super::websocket::TungsteniteClient;
use super::worker::{WorkerHandle, failed_reply, spawn_worker};
use super::{BinanceConnector, ConnectorContext, OkxConnector};
use crate::utils::RealClock;
use layline::types::{Exchange, StrategyName, Topic};

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

    /// Moves the strategy from `held` to `target`, one exchange after another. Returns why each topic
    /// could not be added or released; topics missing from the map succeeded.
    pub(crate) async fn set_topics(
        &mut self,
        strategy_name: &StrategyName,
        held: &HashSet<Topic>,
        target: &HashSet<Topic>,
    ) -> HashMap<Topic, String> {
        let mut held_by_exchange = group_by_exchange(held);
        let mut target_by_exchange = group_by_exchange(target);
        let mut failures = HashMap::new();
        for exchange in Exchange::iter() {
            let held = held_by_exchange.remove(&exchange).unwrap_or_default();
            let target = target_by_exchange.remove(&exchange).unwrap_or_default();
            let running = self
                .workers
                .get(&exchange)
                .is_some_and(|worker| !worker.has_stopped());
            // A stopped worker took the strategy's rings with it, so a new one is sent the whole target.
            if (running && held == target) || (!running && target.is_empty()) {
                continue;
            }
            if !running {
                match spawn_exchange_worker(
                    exchange,
                    &self.root_directory,
                    self.demo,
                    &self.context,
                ) {
                    Ok(worker) => {
                        self.workers.insert(exchange, worker);
                    }
                    Err(error) => {
                        self.workers.remove(&exchange);
                        let reason = format!("failed to start {exchange} worker: {error:#}");
                        failures.extend(failed_reply(&target, &reason));
                        continue;
                    }
                }
            }
            let worker = &self.workers[&exchange];
            failures.extend(worker.set_topics(strategy_name, &target).await);
        }
        failures
    }
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
    use crate::exchanges::worker::tests::{stopped_worker, wait_until_stopped};
    use layline::testing::{test_instrument, test_topic};
    use layline::types::{Currency, MarketDataType, UnderlyingType};

    const STRATEGY_NAME: &str = "test_strategy";

    fn topic(exchange: Exchange, market_data_type: MarketDataType) -> Topic {
        test_topic(
            test_instrument(
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
        let strategy_name = StrategyName::from(STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let mut router = ExchangeRouter::new(directory.path(), false, test_connector_context());
        let topics = HashSet::from([
            topic(Exchange::Okx, MarketDataType::Bbo),
            topic(Exchange::Okx, MarketDataType::Trade),
        ]);

        let failures = router
            .set_topics(&strategy_name, &HashSet::new(), &topics)
            .await;
        assert!(failures.is_empty(), "{failures:?}");
        assert!(topics.iter().all(|topic| {
            topic
                .ring_key()
                .file_path(directory.path(), &strategy_name)
                .exists()
        }));
        assert!(
            router
                .set_topics(&strategy_name, &topics, &HashSet::new())
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn workers_start_on_demand_and_stop_when_idle() {
        let strategy_name = StrategyName::from(STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let mut router = ExchangeRouter::new(directory.path(), false, test_connector_context());
        assert!(running_exchanges(&router).is_empty());

        let okx_bbo = HashSet::from([topic(Exchange::Okx, MarketDataType::Bbo)]);
        let okx_trade = HashSet::from([topic(Exchange::Okx, MarketDataType::Trade)]);
        let okx_topics: HashSet<Topic> = okx_bbo.union(&okx_trade).copied().collect();
        assert!(
            router
                .set_topics(&strategy_name, &HashSet::new(), &okx_topics)
                .await
                .is_empty()
        );
        assert_eq!(HashSet::from([Exchange::Okx]), running_exchanges(&router));

        assert!(
            router
                .set_topics(&strategy_name, &okx_topics, &okx_trade)
                .await
                .is_empty()
        );
        assert_eq!(HashSet::from([Exchange::Okx]), running_exchanges(&router));
        assert!(
            router
                .set_topics(&strategy_name, &okx_trade, &HashSet::new())
                .await
                .is_empty()
        );
        wait_until_idle(&router).await;

        // Releasing from a stopped worker neither fails nor starts a new one.
        assert!(
            router
                .set_topics(&strategy_name, &okx_bbo, &HashSet::new())
                .await
                .is_empty()
        );
        assert!(running_exchanges(&router).is_empty());
    }

    #[tokio::test]
    async fn stopped_worker_is_replaced_and_sent_the_held_topics() {
        let strategy_name = StrategyName::from(STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let mut router = ExchangeRouter::new(directory.path(), false, test_connector_context());
        router
            .workers
            .insert(Exchange::Okx, stopped_worker(Exchange::Okx));

        // The strategy already held the topic, so only the stopped worker makes the router send it.
        let okx_trade = HashSet::from([topic(Exchange::Okx, MarketDataType::Trade)]);
        assert!(
            router
                .set_topics(&strategy_name, &okx_trade, &okx_trade)
                .await
                .is_empty()
        );
        assert!(
            router
                .workers
                .get(&Exchange::Okx)
                .is_some_and(|worker| !worker.has_stopped())
        );
    }
}

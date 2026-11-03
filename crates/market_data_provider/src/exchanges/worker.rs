use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{error, info, warn};

use super::ExchangeConnector;
use layline::shared_memory::{MarketDataWriter, SerializedMarketData};
use layline::types::{Exchange, MarketData, RingKey, StrategyName, Topic};

enum WorkerCommand {
    Subscribe {
        strategy_name: StrategyName,
        topics: HashSet<Topic>,
        reply_sender: oneshot::Sender<WorkerReply>,
    },
    Unsubscribe {
        strategy_name: StrategyName,
        topics: HashSet<Topic>,
        reply_sender: oneshot::Sender<WorkerReply>,
    },
}

/// Why each topic could not be subscribed or unsubscribed; topics missing from the map succeeded.
pub(super) type WorkerReply = HashMap<Topic, String>;

pub(super) fn failed_reply(topics: &HashSet<Topic>, reason: &str) -> WorkerReply {
    topics
        .iter()
        .map(|topic| (*topic, reason.to_owned()))
        .collect()
}

const WORKER_REPLY_TIMEOUT: Duration = Duration::from_secs(15);

/// Thread-safe entry point to one exchange worker; dropping it stops the worker.
pub(super) struct WorkerHandle {
    exchange: Exchange,
    commands: UnboundedSender<WorkerCommand>,
}

impl WorkerHandle {
    pub(super) async fn subscribe(
        &self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
    ) -> WorkerReply {
        let (reply_sender, reply_receiver) = oneshot::channel();
        let command = WorkerCommand::Subscribe {
            strategy_name: strategy_name.clone(),
            topics: topics.clone(),
            reply_sender,
        };
        if self.commands.send(command).is_err() {
            return self.stopped(topics);
        }
        match timeout(WORKER_REPLY_TIMEOUT, reply_receiver).await {
            Ok(Ok(worker_reply)) => worker_reply,
            Ok(Err(_)) => self.stopped(topics),
            Err(_) => self.unanswered(topics),
        }
    }

    /// Unknown topics succeed, so unsubscribing is idempotent.
    pub(super) async fn unsubscribe(
        &self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
    ) -> WorkerReply {
        let (reply_sender, reply_receiver) = oneshot::channel();
        let command = WorkerCommand::Unsubscribe {
            strategy_name: strategy_name.clone(),
            topics: topics.clone(),
            reply_sender,
        };
        if self.commands.send(command).is_err() {
            return WorkerReply::new();
        }
        match timeout(WORKER_REPLY_TIMEOUT, reply_receiver).await {
            Ok(Ok(worker_reply)) => worker_reply,
            Ok(Err(_)) => WorkerReply::new(),
            Err(_) => self.unanswered(topics),
        }
    }

    pub(super) fn has_stopped(&self) -> bool {
        self.commands.is_closed()
    }

    #[cfg(test)]
    pub(super) fn stopped_for_test(exchange: Exchange) -> Self {
        let (commands, receiver) = unbounded_channel();
        drop(receiver);
        Self { exchange, commands }
    }

    fn stopped(&self, topics: &HashSet<Topic>) -> WorkerReply {
        failed_reply(topics, &format!("{} worker has stopped", self.exchange))
    }

    fn unanswered(&self, topics: &HashSet<Topic>) -> WorkerReply {
        failed_reply(
            topics,
            &format!(
                "{} worker did not answer within {WORKER_REPLY_TIMEOUT:?}",
                self.exchange
            ),
        )
    }
}

/// bcast writers are `!Send`, so each exchange gets its own thread that owns its connector and every strategy's rings.
/// The connector need not be `Send` either, so `new_connector` builds it on that thread.
pub(super) fn spawn_worker<Connector: ExchangeConnector>(
    root_directory: &Path,
    demo: bool,
    new_connector: impl FnOnce() -> Connector + Send + 'static,
) -> Result<WorkerHandle> {
    let runtime = RuntimeBuilder::new_current_thread().enable_all().build()?;
    let (commands, receiver) = unbounded_channel();
    let root_directory = root_directory.to_owned();
    thread::Builder::new()
        .name(format!("{}-worker", Connector::EXCHANGE))
        .spawn(move || {
            runtime.block_on(run_worker(root_directory, new_connector, receiver));
            info!("{} worker stopped", Connector::EXCHANGE);
        })?;
    info!("{} worker started, demo: {demo}", Connector::EXCHANGE);
    Ok(WorkerHandle {
        exchange: Connector::EXCHANGE,
        commands,
    })
}

async fn run_worker<Connector: ExchangeConnector>(
    root_directory: PathBuf,
    new_connector: impl FnOnce() -> Connector,
    commands: UnboundedReceiver<WorkerCommand>,
) {
    let worker = ExchangeWorker {
        connector: new_connector(),
        root_directory,
        strategies: HashMap::new(),
    };
    worker.run(commands).await;
}

struct ExchangeWorker<Connector> {
    connector: Connector,
    root_directory: PathBuf,
    /// Every strategy this worker publishes to; a strategy leaves with its last topic.
    strategies: HashMap<StrategyName, StrategyRings>,
}

/// One strategy's topics on this exchange and the rings they are written into.
#[derive(Default)]
struct StrategyRings {
    topics: HashSet<Topic>,
    writers: HashMap<RingKey, MarketDataWriter>,
}

impl StrategyRings {
    fn publish(&self, serialized: &SerializedMarketData) {
        self.writers
            .get(&serialized.topic().ring_key())
            .expect("every subscribed topic's ring is open")
            .publish(serialized);
    }
}

impl<Connector: ExchangeConnector> ExchangeWorker<Connector> {
    async fn run(mut self, mut commands: UnboundedReceiver<WorkerCommand>) {
        loop {
            select! {
                command_option = commands.recv() => match command_option {
                    Some(WorkerCommand::Subscribe { strategy_name, topics, reply_sender }) => {
                        let failures = self.subscribe(&strategy_name, &topics).await;
                        self.reply(reply_sender, failures);
                    }
                    Some(WorkerCommand::Unsubscribe { strategy_name, topics, reply_sender }) => {
                        let failures = self.unsubscribe(&strategy_name, &topics).await;
                        self.reply(reply_sender, failures);
                    }
                    None => break,
                },
                data = self.connector.next_event() => self.publish(&data),
            }
            if self.strategies.is_empty() {
                break;
            }
        }
        self.connector.disconnect().await;
    }

    /// Opens the strategy's rings first, then subscribes the exchange to the topics no strategy wanted before.
    async fn subscribe(
        &mut self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
    ) -> WorkerReply {
        let (opened, mut failures) = self.open_rings(strategy_name, topics);
        let newly_wanted: HashSet<Topic> = opened
            .iter()
            .filter(|topic| !self.is_wanted(topic))
            .copied()
            .collect();
        if !newly_wanted.is_empty()
            && let Err(error) = self.connector.subscribe(&newly_wanted).await
        {
            failures.extend(failed_reply(&newly_wanted, &format!("{error:#}")));
        }

        let rings = self
            .strategies
            .get_mut(strategy_name)
            .expect("open_rings registers the strategy");
        for topic in opened
            .into_iter()
            .filter(|topic| !failures.contains_key(topic))
        {
            info!(
                "publishing {topic} to {}",
                topic
                    .file_path(&self.root_directory, strategy_name)
                    .display()
            );
            rings.topics.insert(topic);
        }
        self.close_unused_rings(strategy_name);
        failures
    }

    /// Opens the strategy's ring of every topic it does not have yet. Returns those topics, and why the others were refused.
    fn open_rings(
        &mut self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
    ) -> (HashSet<Topic>, WorkerReply) {
        let rings = self.strategies.entry(strategy_name.clone()).or_default();
        let mut opened = HashSet::new();
        let mut failures = WorkerReply::new();
        for topic in topics.difference(&rings.topics) {
            if topic.instrument.exchange != Connector::EXCHANGE {
                let reason = format!(
                    "{} worker cannot serve {}",
                    Connector::EXCHANGE,
                    topic.instrument.exchange
                );
                failures.insert(*topic, reason);
                continue;
            }
            let ring_key = topic.ring_key();
            if let Entry::Vacant(entry) = rings.writers.entry(ring_key) {
                match MarketDataWriter::open(&self.root_directory, strategy_name, ring_key) {
                    Ok(writer) => {
                        entry.insert(writer);
                    }
                    Err(error) => {
                        failures.insert(*topic, format!("{error:#}"));
                        continue;
                    }
                }
            }
            opened.insert(*topic);
        }
        (opened, failures)
    }

    /// Unknown topics succeed, so unsubscribing is idempotent. The exchange stops sending the topics no strategy wants any more.
    async fn unsubscribe(
        &mut self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
    ) -> WorkerReply {
        let Some(rings) = self.strategies.get_mut(strategy_name) else {
            return WorkerReply::new();
        };
        let removed: Vec<Topic> = topics
            .iter()
            .filter(|topic| rings.topics.remove(topic))
            .copied()
            .collect();
        self.close_unused_rings(strategy_name);

        let unwanted: HashSet<Topic> = removed
            .into_iter()
            .filter(|topic| !self.is_wanted(topic))
            .collect();
        if unwanted.is_empty() {
            return WorkerReply::new();
        }
        match self.connector.unsubscribe(&unwanted).await {
            Ok(()) => {
                for topic in &unwanted {
                    info!("stopped publishing {topic}, no strategy wants it");
                }
                WorkerReply::new()
            }
            Err(error) => failed_reply(&unwanted, &format!("{error:#}")),
        }
    }

    /// Whether any strategy reads `topic`, so the exchange must keep sending it.
    fn is_wanted(&self, topic: &Topic) -> bool {
        self.strategies
            .values()
            .any(|rings| rings.topics.contains(topic))
    }

    /// Closes the strategy's rings that no topic is written into, and forgets the strategy once it has no topic left.
    fn close_unused_rings(&mut self, strategy_name: &StrategyName) {
        let Some(rings) = self.strategies.get_mut(strategy_name) else {
            return;
        };
        let used: HashSet<RingKey> = rings.topics.iter().map(Topic::ring_key).collect();
        rings.writers.retain(|ring_key, _| used.contains(ring_key));
        if rings.topics.is_empty() {
            self.strategies.remove(strategy_name);
        }
    }

    fn reply(&self, reply_sender: oneshot::Sender<WorkerReply>, failures: WorkerReply) {
        if reply_sender.send(failures).is_err() {
            warn!(
                "{} requester went away before the reply",
                Connector::EXCHANGE
            );
        }
    }

    /// Serializes once, then writes the same bytes into the ring of every strategy that reads the topic.
    fn publish(&self, data: &MarketData) {
        let serialized = match SerializedMarketData::new(data) {
            Ok(serialized) => serialized,
            Err(error) => {
                error!("failed to serialize {}: {error:#}", data.topic());
                return;
            }
        };
        for rings in self
            .strategies
            .values()
            .filter(|rings| rings.topics.contains(&serialized.topic()))
        {
            rings.publish(&serialized);
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::fs::create_dir_all;
    use std::future::pending;

    use anyhow::bail;
    use tempfile::tempdir;
    use tokio::time::sleep;

    use super::*;
    use crate::exchanges::OkxConnector;
    use layline::shared_memory::MarketDataReader;
    use layline::testing::{test_instrument, test_subscription, test_topic};
    use layline::types::ConnectedApi;
    use layline::types::{Currency, Instrument, MarketDataType, UnderlyingType};

    const FIRST_STRATEGY_NAME: &str = "first";
    const SECOND_STRATEGY_NAME: &str = "second";
    const STOP_TIMEOUT: Duration = Duration::from_secs(5);
    const DATA_TIMEOUT: Duration = Duration::from_secs(5);
    const POLL_INTERVAL: Duration = Duration::from_millis(1);
    /// Two synthetic ticks, so every topic a ring carries shows up at least once.
    const SAMPLED_MESSAGE_COUNT: usize = 4;

    /// Resolves once the worker thread has dropped its command receiver.
    pub(in crate::exchanges) async fn wait_until_stopped(worker: &WorkerHandle) {
        timeout(STOP_TIMEOUT, worker.commands.closed())
            .await
            .expect("worker is expected to stop");
    }

    /// Always rejects unsubscribing; accepts subscribing only if `ACCEPTS_SUBSCRIBE`.
    struct ScriptedConnector<const ACCEPTS_SUBSCRIBE: bool>;

    impl<const ACCEPTS_SUBSCRIBE: bool> ExchangeConnector for ScriptedConnector<ACCEPTS_SUBSCRIBE> {
        const EXCHANGE: Exchange = Exchange::Okx;
        const CONNECTED_APIS: &'static [ConnectedApi] = &[];

        async fn subscribe(&mut self, _topics: &HashSet<Topic>) -> Result<()> {
            if ACCEPTS_SUBSCRIBE {
                return Ok(());
            }
            bail!("venue is down")
        }

        async fn unsubscribe(&mut self, _topics: &HashSet<Topic>) -> Result<()> {
            bail!("venue is down")
        }

        async fn next_event(&mut self) -> MarketData {
            pending().await
        }

        async fn disconnect(&mut self) {}
    }

    fn bitcoin_topic(market_data_type: MarketDataType) -> Topic {
        okx_spot_topic(Currency::Btc, market_data_type)
    }

    fn ether_topic(market_data_type: MarketDataType) -> Topic {
        okx_spot_topic(Currency::Eth, market_data_type)
    }

    fn okx_spot_topic(base: Currency, market_data_type: MarketDataType) -> Topic {
        test_topic(
            test_instrument(Exchange::Okx, base, Currency::Usdt, UnderlyingType::Spot),
            market_data_type,
        )
    }

    fn spawn<Connector: ExchangeConnector>(
        root_directory: &Path,
        new_connector: impl FnOnce() -> Connector + Send + 'static,
    ) -> WorkerHandle {
        spawn_worker(root_directory, false, new_connector)
            .expect("spawning a worker is expected to not fail")
    }

    /// The distinct topics among the next messages of the strategy's ring that carries `topic`.
    async fn next_topics(
        root_directory: &Path,
        strategy_name: &StrategyName,
        topic: Topic,
    ) -> HashSet<Topic> {
        let reader =
            MarketDataReader::open(&[test_subscription(root_directory, strategy_name, topic)])
                .expect("reader open is expected to not fail");
        let mut topics = Vec::new();
        timeout(DATA_TIMEOUT, async {
            while topics.len() < SAMPLED_MESSAGE_COUNT {
                reader
                    .poll(|data, _received_at_nanoseconds| topics.push(data.topic()))
                    .expect("poll is expected to not fail");
                sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("market data is expected to keep arriving");
        topics.into_iter().collect()
    }

    #[tokio::test]
    async fn worker_subscribes_batch_and_reports_failures() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let bbo = HashSet::from([bitcoin_topic(MarketDataType::Bbo)]);
        let topics = HashSet::from([
            bitcoin_topic(MarketDataType::Bbo),
            bitcoin_topic(MarketDataType::Trade),
        ]);

        let reply = worker.subscribe(&first_strategy_name, &topics).await;
        assert!(reply.is_empty(), "{reply:?}");
        assert!(topics.iter().all(|topic| {
            topic
                .file_path(directory.path(), &first_strategy_name)
                .exists()
        }));
        assert!(
            worker
                .subscribe(&first_strategy_name, &bbo)
                .await
                .is_empty()
        );

        assert!(
            worker
                .unsubscribe(&first_strategy_name, &bbo)
                .await
                .is_empty()
        );
        assert!(
            worker
                .unsubscribe(&first_strategy_name, &topics)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn each_strategy_receives_only_its_own_topics() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let bitcoin = bitcoin_topic(MarketDataType::Bbo);
        let ether = ether_topic(MarketDataType::Bbo);
        assert!(
            worker
                .subscribe(&first_strategy_name, &HashSet::from([bitcoin, ether]))
                .await
                .is_empty()
        );
        assert!(
            worker
                .subscribe(&second_strategy_name, &HashSet::from([bitcoin]))
                .await
                .is_empty()
        );

        assert_eq!(
            HashSet::from([bitcoin, ether]),
            next_topics(directory.path(), &first_strategy_name, bitcoin).await
        );
        assert_eq!(
            HashSet::from([bitcoin]),
            next_topics(directory.path(), &second_strategy_name, bitcoin).await
        );

        assert!(
            worker
                .unsubscribe(&first_strategy_name, &HashSet::from([bitcoin]))
                .await
                .is_empty()
        );
        assert_eq!(
            HashSet::from([bitcoin]),
            next_topics(directory.path(), &second_strategy_name, bitcoin).await
        );
    }

    #[tokio::test]
    async fn exchange_stops_sending_a_topic_only_after_its_last_strategy() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo = HashSet::from([bitcoin_topic(MarketDataType::Bbo)]);
        let worker = spawn(directory.path(), || ScriptedConnector::<true>);
        assert!(
            worker
                .subscribe(&first_strategy_name, &bbo)
                .await
                .is_empty()
        );
        assert!(
            worker
                .subscribe(&second_strategy_name, &bbo)
                .await
                .is_empty()
        );

        // The connector rejects every unsubscription, so a reply without failures means it was never asked.
        assert!(
            worker
                .unsubscribe(&first_strategy_name, &bbo)
                .await
                .is_empty()
        );
        assert_eq!(
            bbo.len(),
            worker.unsubscribe(&second_strategy_name, &bbo).await.len()
        );
    }

    #[tokio::test]
    async fn worker_stops_after_its_last_strategy() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let bbo = HashSet::from([bitcoin_topic(MarketDataType::Bbo)]);
        assert!(
            worker
                .subscribe(&first_strategy_name, &bbo)
                .await
                .is_empty()
        );
        assert!(
            worker
                .subscribe(&second_strategy_name, &bbo)
                .await
                .is_empty()
        );
        assert!(
            worker
                .unsubscribe(&first_strategy_name, &bbo)
                .await
                .is_empty()
        );
        assert!(!worker.has_stopped());
        assert!(
            worker
                .unsubscribe(&second_strategy_name, &bbo)
                .await
                .is_empty()
        );

        wait_until_stopped(&worker).await;
    }

    #[tokio::test]
    async fn wrong_exchange_is_refused() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let binance_topic = Topic {
            instrument: Instrument {
                exchange: Exchange::Binance,
                ..bitcoin_topic(MarketDataType::Bbo).instrument
            },
            market_data_type: MarketDataType::Bbo,
        };
        let reply = worker
            .subscribe(&first_strategy_name, &HashSet::from([binance_topic]))
            .await;
        assert!(reply.contains_key(&binance_topic));
    }

    #[tokio::test]
    async fn ring_open_failure_is_reported() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo = bitcoin_topic(MarketDataType::Bbo);
        create_dir_all(bbo.file_path(directory.path(), &first_strategy_name))
            .expect("creating the ring path as a directory is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let reply = worker
            .subscribe(&first_strategy_name, &HashSet::from([bbo]))
            .await;
        assert!(reply.contains_key(&bbo));
    }

    #[tokio::test]
    async fn exchange_failures_fail_only_affected_topics() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo = bitcoin_topic(MarketDataType::Bbo);
        let trade = bitcoin_topic(MarketDataType::Trade);
        let rejecting = spawn(directory.path(), || ScriptedConnector::<false>);
        let reply = rejecting
            .subscribe(&first_strategy_name, &HashSet::from([bbo]))
            .await;
        assert!(reply.contains_key(&bbo));
        assert!(
            rejecting
                .unsubscribe(&first_strategy_name, &HashSet::from([bbo]))
                .await
                .is_empty()
        );

        let sticky = spawn(directory.path(), || ScriptedConnector::<true>);
        assert!(
            sticky
                .subscribe(&first_strategy_name, &HashSet::from([bbo]))
                .await
                .is_empty()
        );
        let reply = sticky
            .unsubscribe(&first_strategy_name, &HashSet::from([bbo, trade]))
            .await;
        assert!(reply.contains_key(&bbo));
        assert!(!reply.contains_key(&trade));
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_worker_times_out() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let (commands, _receiver) = unbounded_channel();
        let worker = WorkerHandle {
            exchange: Exchange::Okx,
            commands,
        };
        let topics = HashSet::from([bitcoin_topic(MarketDataType::Bbo)]);
        let reply = worker.subscribe(&first_strategy_name, &topics).await;
        assert!(
            reply
                .values()
                .all(|reason| reason.contains("did not answer"))
        );
    }

    #[tokio::test]
    async fn stopped_worker_fails_every_topic() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let worker = WorkerHandle::stopped_for_test(Exchange::Okx);
        assert!(worker.has_stopped());
        let topics = HashSet::from([
            bitcoin_topic(MarketDataType::Bbo),
            bitcoin_topic(MarketDataType::Trade),
        ]);
        let reply = worker.subscribe(&first_strategy_name, &topics).await;
        assert_eq!(topics.len(), reply.len());
        assert!(reply.values().all(|reason| reason.contains("has stopped")));
        assert!(
            worker
                .unsubscribe(&first_strategy_name, &topics)
                .await
                .is_empty()
        );
    }
}

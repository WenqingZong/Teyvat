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
use tracing::{debug, error, info, warn};

use super::ExchangeConnector;
use layline::shared_memory::MarketDataWriter;
use layline::types::{Exchange, MarketData, Topic};

enum WorkerCommand {
    Subscribe {
        topics: HashSet<Topic>,
        reply: oneshot::Sender<WorkerReply>,
    },
    Unsubscribe {
        topics: HashSet<Topic>,
        reply: oneshot::Sender<WorkerReply>,
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

// TODO: one worker per exchange suits the current Oracle free-tier AMD instance. On a bigger machine, revisit splitting workers by (exchange, market_data_type).
const WORKER_REPLY_TIMEOUT: Duration = Duration::from_secs(15);

/// Thread-safe entry point to one exchange worker; dropping it stops the worker.
pub(super) struct WorkerHandle {
    exchange: Exchange,
    commands: UnboundedSender<WorkerCommand>,
}

impl WorkerHandle {
    pub(super) async fn subscribe(&self, topics: &HashSet<Topic>) -> WorkerReply {
        let (reply, response) = oneshot::channel();
        let command = WorkerCommand::Subscribe {
            topics: topics.clone(),
            reply,
        };
        if self.commands.send(command).is_err() {
            return self.stopped(topics);
        }
        match timeout(WORKER_REPLY_TIMEOUT, response).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => self.stopped(topics),
            Err(_) => self.unanswered(topics),
        }
    }

    /// Unknown topics succeed, so unsubscribing is idempotent.
    pub(super) async fn unsubscribe(&self, topics: &HashSet<Topic>) -> WorkerReply {
        let (reply, response) = oneshot::channel();
        let command = WorkerCommand::Unsubscribe {
            topics: topics.clone(),
            reply,
        };
        if self.commands.send(command).is_err() {
            return WorkerReply::new();
        }
        match timeout(WORKER_REPLY_TIMEOUT, response).await {
            Ok(Ok(reply)) => reply,
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

/// bcast writers are `!Send`, so each exchange gets its own thread that owns its connector and every writer of its topics.
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
        market_data_writers: HashMap::new(),
    };
    worker.run(commands).await;
}

struct ExchangeWorker<Connector> {
    connector: Connector,
    root_directory: PathBuf,
    market_data_writers: HashMap<Topic, MarketDataWriter>,
}

impl<Connector: ExchangeConnector> ExchangeWorker<Connector> {
    async fn run(mut self, mut commands: UnboundedReceiver<WorkerCommand>) {
        loop {
            select! {
                command_option = commands.recv() => match command_option {
                    Some(WorkerCommand::Subscribe { topics, reply }) => {
                        let failures = self.subscribe(&topics).await;
                        self.reply(reply, failures);
                    }
                    Some(WorkerCommand::Unsubscribe { topics, reply }) => {
                        let failures = self.unsubscribe(&topics).await;
                        self.reply(reply, failures);
                    }
                    None => break,
                },
                data = self.connector.next_event() => self.publish(&data),
            }
            if self.market_data_writers.is_empty() {
                break;
            }
        }
        self.connector.disconnect().await;
    }

    /// Opens every new ring first, then subscribes all of them on the exchange in one call.
    async fn subscribe(&mut self, topics: &HashSet<Topic>) -> HashMap<Topic, String> {
        let mut opened = HashMap::new();
        let mut failures = HashMap::new();
        for topic in topics {
            if topic.instrument.exchange != Connector::EXCHANGE {
                failures.insert(
                    *topic,
                    format!(
                        "{} worker cannot serve {}",
                        Connector::EXCHANGE,
                        topic.instrument.exchange
                    ),
                );
                continue;
            }
            if self.market_data_writers.contains_key(topic) {
                continue;
            }
            let path = topic.file_path(&self.root_directory);
            match MarketDataWriter::open(&path, topic.market_data_type) {
                Ok(writer) => {
                    opened.insert(*topic, writer);
                }
                Err(error) => {
                    failures.insert(*topic, format!("{error:#}"));
                }
            }
        }
        if opened.is_empty() {
            return failures;
        }

        let new_topics: HashSet<Topic> = opened.keys().copied().collect();
        if let Err(error) = self.connector.subscribe(&new_topics).await {
            let reason = format!("{error:#}");
            for topic in new_topics {
                failures.insert(topic, reason.clone());
            }
            return failures;
        }
        for (topic, writer) in opened {
            info!(
                "publishing {topic} to {}",
                topic.file_path(&self.root_directory).display()
            );
            self.market_data_writers.insert(topic, writer);
        }
        failures
    }

    /// Unknown topics succeed, so unsubscribing is idempotent.
    async fn unsubscribe(&mut self, topics: &HashSet<Topic>) -> HashMap<Topic, String> {
        let removed: HashSet<Topic> = topics
            .iter()
            .filter(|topic| self.market_data_writers.remove(topic).is_some())
            .copied()
            .collect();
        if removed.is_empty() {
            return HashMap::new();
        }
        let mut failures = HashMap::new();
        match self.connector.unsubscribe(&removed).await {
            Ok(()) => {
                for topic in &removed {
                    info!("stopped publishing {topic}");
                }
            }
            Err(error) => {
                let reason = format!("{error:#}");
                for topic in removed {
                    failures.insert(topic, reason.clone());
                }
            }
        }
        failures
    }

    fn reply(&self, reply: oneshot::Sender<WorkerReply>, failures: WorkerReply) {
        if reply.send(failures).is_err() {
            warn!(
                "{} requester went away before the reply",
                Connector::EXCHANGE
            );
        }
    }

    fn publish(&self, data: &MarketData) {
        let topic = data.topic();
        let Some(writer) = self.market_data_writers.get(&topic) else {
            debug!("dropping event of unsubscribed {topic}");
            return;
        };
        if let Err(error) = writer.publish(data) {
            error!("failed to publish {topic}: {error:#}");
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::fs::create_dir_all;
    use std::future::pending;

    use anyhow::bail;
    use tempfile::tempdir;

    use super::*;
    use crate::exchanges::OkxConnector;
    use layline::types::ConnectedApi;
    use layline::types::{Currency, Instrument, MarketDataType, UnderlyingType};

    /// Resolves once the worker thread has dropped its command receiver.
    pub(in crate::exchanges) async fn wait_until_stopped(worker: &WorkerHandle) {
        timeout(Duration::from_secs(5), worker.commands.closed())
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

    fn topic(market_data_type: MarketDataType) -> Topic {
        layline::testing::topic(
            layline::testing::instrument(
                Exchange::Okx,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Spot,
            ),
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

    #[tokio::test]
    async fn worker_subscribes_batch_and_reports_failures() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let bbo = HashSet::from([topic(MarketDataType::Bbo)]);
        let topics = HashSet::from([topic(MarketDataType::Bbo), topic(MarketDataType::Trade)]);

        let reply = worker.subscribe(&topics).await;
        assert!(reply.is_empty(), "{reply:?}");
        assert!(
            topics
                .iter()
                .all(|topic| topic.file_path(directory.path()).exists())
        );
        assert!(worker.subscribe(&bbo).await.is_empty());

        assert!(worker.unsubscribe(&bbo).await.is_empty());
        assert!(worker.unsubscribe(&topics).await.is_empty());
    }

    #[tokio::test]
    async fn worker_stops_after_its_last_topic() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let bbo = HashSet::from([topic(MarketDataType::Bbo)]);
        assert!(worker.subscribe(&bbo).await.is_empty());
        assert!(worker.unsubscribe(&bbo).await.is_empty());

        wait_until_stopped(&worker).await;
    }

    #[tokio::test]
    async fn wrong_exchange_is_refused() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let binance_topic = Topic {
            instrument: Instrument {
                exchange: Exchange::Binance,
                ..topic(MarketDataType::Bbo).instrument
            },
            market_data_type: MarketDataType::Bbo,
        };
        let reply = worker.subscribe(&HashSet::from([binance_topic])).await;
        assert!(reply.contains_key(&binance_topic));
    }

    #[tokio::test]
    async fn ring_open_failure_is_reported() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo = topic(MarketDataType::Bbo);
        create_dir_all(bbo.file_path(directory.path()))
            .expect("creating the ring path as a directory is expected to not fail");
        let worker = spawn(directory.path(), OkxConnector::new);
        let reply = worker.subscribe(&HashSet::from([bbo])).await;
        assert!(reply.contains_key(&bbo));
    }

    #[tokio::test]
    async fn exchange_failures_fail_only_affected_topics() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo = topic(MarketDataType::Bbo);
        let trade = topic(MarketDataType::Trade);
        let rejecting = spawn(directory.path(), || ScriptedConnector::<false>);
        let reply = rejecting.subscribe(&HashSet::from([bbo])).await;
        assert!(reply.contains_key(&bbo));
        assert!(
            rejecting
                .unsubscribe(&HashSet::from([bbo]))
                .await
                .is_empty()
        );

        let sticky = spawn(directory.path(), || ScriptedConnector::<true>);
        assert!(sticky.subscribe(&HashSet::from([bbo])).await.is_empty());
        let reply = sticky.unsubscribe(&HashSet::from([bbo, trade])).await;
        assert!(reply.contains_key(&bbo));
        assert!(!reply.contains_key(&trade));
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_worker_times_out() {
        let (commands, _receiver) = unbounded_channel();
        let worker = WorkerHandle {
            exchange: Exchange::Okx,
            commands,
        };
        let topics = HashSet::from([topic(MarketDataType::Bbo)]);
        let reply = worker.subscribe(&topics).await;
        assert!(
            reply
                .values()
                .all(|reason| reason.contains("did not answer"))
        );
    }

    #[tokio::test]
    async fn stopped_worker_fails_every_topic() {
        let worker = WorkerHandle::stopped_for_test(Exchange::Okx);
        assert!(worker.has_stopped());
        let topics = HashSet::from([topic(MarketDataType::Bbo), topic(MarketDataType::Trade)]);
        let reply = worker.subscribe(&topics).await;
        assert_eq!(topics.len(), reply.len());
        assert!(reply.values().all(|reason| reason.contains("has stopped")));
        assert!(worker.unsubscribe(&topics).await.is_empty());
    }
}

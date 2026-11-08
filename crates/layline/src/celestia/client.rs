//! Reports a process's samples to Phanes Celestia, as a task on the process's tokio runtime.
//! Every sample goes into one outbox the moment it arrives, connected or not, and leaves it only
//! when Phanes acknowledges it; after a reconnect, everything still in it is sent again.
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter, Result as FormatResult};
use std::pin::pin;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretBox};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{info, warn};
use url::{ParseError, Url};

use super::{Ack, Batch, CELESTIA_PATH, CelestiaError, Hello, Initiator, Session};
use crate::backoff::exponential_backoff;
use crate::noise::{StaticKey, deserialize_key};
use crate::process_metrics::ProcessSample;
use crate::time::since_unix_epoch;

/// The reporter takes each sample as it arrives, so the channel from the monitor needs no room for
/// more; samples wait in the outbox instead.
pub const SAMPLE_CHANNEL_CAPACITY: usize = 1;
/// How much reporting the outbox holds while Phanes does not acknowledge it; beyond that the oldest
/// batch is dropped.
const BUFFERED_DURATION: Duration = Duration::from_secs(3600);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Keeps Cloudflare from closing the connection while no samples flow.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("websocket: {0}")]
    WebSocket(#[from] WebSocketError),
    #[error(transparent)]
    Celestia(#[from] CelestiaError),
    #[error("server did not finish the handshake in time")]
    HandshakeTimeout,
    #[error("server closed the connection")]
    Closed,
    #[error("unexpected websocket message")]
    UnexpectedMessage,
}

#[derive(Debug, thiserror::Error)]
pub enum CelestiaUrlError {
    #[error("invalid celestia url: {0}")]
    Parse(#[from] ParseError),
    #[error("{0} is not an http(s) or ws(s) url")]
    UnsupportedScheme(Url),
}

/// The `[celestia_client]` table of every reporting process's config. The Noise key authenticates the
/// machine, so every process on it holds the same `client_celestia_private_key`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CelestiaClientConfig {
    /// Phanes, as Cloudflare exposes it; the reporter appends `/celestia`.
    pub url: Url,
    /// Base64 X25519 private key this machine authenticates to Celestia with.
    #[serde(deserialize_with = "deserialize_key")]
    pub client_celestia_private_key: SecretBox<StaticKey>,
    /// Base64 X25519 public key of Phanes Celestia.
    #[serde(deserialize_with = "deserialize_key")]
    pub phanes_celestia_public_key: SecretBox<StaticKey>,
}

/// Written by hand so `url` prints as one URL rather than `Url`'s fields; the keys print as
/// `[REDACTED]`.
impl Debug for CelestiaClientConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let Self {
            url,
            client_celestia_private_key,
            phanes_celestia_public_key,
        } = self;
        formatter
            .debug_struct("CelestiaClientConfig")
            .field("url", &format_args!("{url}"))
            .field("client_celestia_private_key", client_celestia_private_key)
            .field("phanes_celestia_public_key", phanes_celestia_public_key)
            .finish()
    }
}

/// Written by hand because `SecretBox` of a key is not `Clone`: the copy holds the same keys in
/// new `SecretBox`es, which zero their memory on drop like the originals. A service whose config
/// must stay whole hands the reporter a copy.
impl Clone for CelestiaClientConfig {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            client_celestia_private_key: copy_key(&self.client_celestia_private_key),
            phanes_celestia_public_key: copy_key(&self.phanes_celestia_public_key),
        }
    }
}

fn copy_key(key: &SecretBox<StaticKey>) -> SecretBox<StaticKey> {
    SecretBox::new(Box::new(*key.expose_secret()))
}

/// Where to report, and as which process.
pub struct ReporterConfig {
    celestia_client: CelestiaClientConfig,
    process_name: String,
}

impl ReporterConfig {
    /// Fails on a URL that is not http(s) or ws(s), so a process refuses to start with one.
    pub fn new(
        celestia_client: CelestiaClientConfig,
        process_name: String,
    ) -> Result<Self, CelestiaUrlError> {
        build_celestia_url(&celestia_client.url)?;
        Ok(Self {
            celestia_client,
            process_name,
        })
    }
}

/// An established connection: the socket and its Noise session.
struct Connection {
    socket: Socket,
    session: Session,
}

/// Reports every sample from `sample_receiver`, which arrive one every `sample_interval`. Returns
/// only once every sample sender is gone. It never stops the process: a failed connection is
/// logged and retried with exponential backoff, and samples keep going into the outbox meanwhile.
pub async fn run_celestia_reporter(
    config: ReporterConfig,
    sample_interval: Duration,
    mut sample_receiver: Receiver<ProcessSample>,
) {
    let buffered_batches =
        (BUFFERED_DURATION.as_secs_f64() / sample_interval.as_secs_f64()).ceil() as usize;
    // Seeding from the clock in milliseconds keeps sequences increasing across restarts.
    let first_sequence =
        u64::try_from(since_unix_epoch().as_millis()).expect("milliseconds since 1970 fit in u64");
    let ReporterConfig {
        celestia_client,
        process_name,
    } = config;
    let celestia_url =
        build_celestia_url(&celestia_client.url).expect("ReporterConfig::new accepted the URL");
    let hello = Hello { process_name };
    let mut outbox = Outbox::new(first_sequence, buffered_batches);
    let mut failures = 0;
    loop {
        let Some(connect_result) = take_samples_while(
            connect(&celestia_url, &celestia_client, &hello),
            &mut sample_receiver,
            &mut outbox,
        )
        .await
        else {
            break;
        };
        let session_result = match connect_result {
            Ok(connection) => {
                let started = Instant::now();
                let session_result =
                    exchange_batches(connection, &mut outbox, &mut sample_receiver).await;
                // A session that lasted a while means the link was healthy, so retry quickly.
                if started.elapsed() > MAX_BACKOFF {
                    failures = 0;
                }
                session_result
            }
            Err(error) => Err(error),
        };
        match session_result {
            Ok(()) => break,
            Err(error) => warn!("Celestia session failed: {error}"),
        }
        failures += 1;
        let backoff = exponential_backoff(failures, INITIAL_BACKOFF, MAX_BACKOFF);
        info!("reconnecting to Celestia in {backoff:?}");
        if take_samples_while(sleep(backoff), &mut sample_receiver, &mut outbox)
            .await
            .is_none()
        {
            break;
        }
    }
    info!("no more samples to report, Celestia reporter stopped");
}

/// Drives `future` to completion while moving every sample that arrives meanwhile into the
/// outbox, so no sample waits on a connection. `None` once every sample sender is gone.
async fn take_samples_while<Output>(
    future: impl Future<Output = Output>,
    sample_receiver: &mut Receiver<ProcessSample>,
    outbox: &mut Outbox,
) -> Option<Output> {
    let mut future = pin!(future);
    loop {
        select! {
            output = &mut future => return Some(output),
            sample_option = sample_receiver.recv() => {
                outbox.push(vec![sample_option?]);
            }
        }
    }
}

/// Batches waiting for an ack, oldest first. When full, the oldest batch is dropped.
pub struct Outbox {
    next_sequence: u64,
    pending: VecDeque<Batch>,
    capacity: usize,
}

impl Outbox {
    /// Seed `first_sequence` from the clock so a restarted client never reuses a sequence.
    pub fn new(first_sequence: u64, capacity: usize) -> Self {
        Self {
            next_sequence: first_sequence,
            pending: VecDeque::new(),
            capacity,
        }
    }

    pub fn push(&mut self, samples: Vec<ProcessSample>) -> &Batch {
        if self.pending.len() >= self.capacity
            && let Some(dropped) = self.pending.pop_front()
        {
            let sequence = dropped.sequence;
            warn!("outbox full, dropped batch {sequence}");
        }
        self.pending.push_back(Batch {
            sequence: self.next_sequence,
            samples,
        });
        self.next_sequence += 1;
        self.pending.back().expect("a batch was just pushed")
    }

    pub fn acknowledge(&mut self, sequence: u64) {
        while self
            .pending
            .front()
            .is_some_and(|batch| batch.sequence <= sequence)
        {
            self.pending.pop_front();
        }
    }

    pub fn pending(&self) -> impl Iterator<Item = &Batch> {
        self.pending.iter()
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// `https://host` becomes `wss://host/celestia`, `http://host` becomes `ws://host/celestia`.
fn build_celestia_url(base_url: &Url) -> Result<Url, CelestiaUrlError> {
    let mut url = base_url.join(CELESTIA_PATH)?;
    let scheme = match url.scheme() {
        "https" | "wss" => "wss",
        "http" | "ws" => "ws",
        _ => return Err(CelestiaUrlError::UnsupportedScheme(base_url.clone())),
    };
    url.set_scheme(scheme)
        .map_err(|()| CelestiaUrlError::UnsupportedScheme(base_url.clone()))?;
    Ok(url)
}

/// `celestia_url` is the WebSocket URL of `/celestia` on Phanes, made from `celestia_client.url`;
/// `hello` names this process to Phanes.
async fn connect(
    celestia_url: &Url,
    celestia_client: &CelestiaClientConfig,
    hello: &Hello,
) -> Result<Connection, ClientError> {
    let (mut socket, _) = connect_async(celestia_url.as_str()).await?;
    let session = handshake(
        &mut socket,
        &celestia_client.client_celestia_private_key,
        &celestia_client.phanes_celestia_public_key,
        hello,
    )
    .await?;
    Ok(Connection { socket, session })
}

/// Resends everything pending, then sends each new sample as it arrives, until an error. Returns
/// `Ok` only after every sample sender is gone and the pending acks are drained.
async fn exchange_batches(
    connection: Connection,
    outbox: &mut Outbox,
    sample_receiver: &mut Receiver<ProcessSample>,
) -> Result<(), ClientError> {
    let Connection {
        mut socket,
        mut session,
    } = connection;
    let pending_batches = outbox.len();
    info!("Celestia session established, resending {pending_batches} pending batches");
    for batch in outbox.pending() {
        socket
            .feed(Message::Binary(session.encrypt(batch)?.into()))
            .await?;
    }
    socket.flush().await?;

    let mut heartbeat = interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        select! {
            sample_option = sample_receiver.recv() => {
                let Some(sample) = sample_option else {
                    return drain(socket, session, outbox).await;
                };
                let ciphertext = session.encrypt(outbox.push(vec![sample]))?;
                socket.send(Message::Binary(ciphertext.into())).await?;
            }
            message = socket.next() => receive_ack(message, &mut session, outbox)?,
            _ = heartbeat.tick() => socket.send(Message::Ping(Vec::new().into())).await?,
        }
    }
}

fn receive_ack(
    message: Option<Result<Message, WebSocketError>>,
    session: &mut Session,
    outbox: &mut Outbox,
) -> Result<(), ClientError> {
    match message {
        Some(Ok(Message::Binary(ciphertext))) => {
            let ack: Ack = session.decrypt(&ciphertext)?;
            outbox.acknowledge(ack.sequence);
            Ok(())
        }
        Some(Ok(Message::Ping(_) | Message::Pong(_))) => Ok(()),
        Some(Ok(Message::Close(_))) | None => Err(ClientError::Closed),
        Some(Ok(_)) => Err(ClientError::UnexpectedMessage),
        Some(Err(error)) => Err(error.into()),
    }
}

/// On shutdown, waits briefly for outstanding acks before closing.
async fn drain(
    mut socket: Socket,
    mut session: Session,
    outbox: &mut Outbox,
) -> Result<(), ClientError> {
    let waiting = async {
        while !outbox.is_empty() {
            receive_ack(socket.next().await, &mut session, outbox)?;
        }
        Ok::<(), ClientError>(())
    };
    match timeout(DRAIN_TIMEOUT, waiting).await {
        Ok(result) => result?,
        Err(_) => {
            let unacknowledged_batches = outbox.len();
            warn!("{unacknowledged_batches} batches still unacknowledged at shutdown")
        }
    }
    socket.close(None).await?;
    Ok(())
}

async fn handshake(
    socket: &mut Socket,
    client_private_key: &SecretBox<StaticKey>,
    celestia_public_key: &SecretBox<StaticKey>,
    hello: &Hello,
) -> Result<Session, ClientError> {
    let (initiator, first_message) =
        Initiator::start(client_private_key, celestia_public_key, hello)?;
    socket.send(Message::Binary(first_message.into())).await?;
    let reply = timeout(HANDSHAKE_TIMEOUT, socket.next())
        .await
        .map_err(|_| ClientError::HandshakeTimeout)?;
    match reply {
        Some(Ok(Message::Binary(second_message))) => Ok(initiator.finish(&second_message)?),
        Some(Ok(Message::Close(_))) | None => Err(ClientError::Closed),
        Some(Ok(_)) => Err(ClientError::UnexpectedMessage),
        Some(Err(error)) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;

    use tokio::net::{TcpListener, TcpStream};
    use tokio::spawn;
    use tokio::sync::mpsc::channel;
    use tokio_tungstenite::accept_async;

    use super::*;
    use crate::celestia::{Accepted, Responder};
    use crate::testing::{test_keypair, test_process_sample};

    const PROCESS_NAME: &str = "celestia_test";
    const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
    /// Below `DRAIN_TIMEOUT`, so stopping in time means every sample was acknowledged.
    const REPORTER_STOP_WAIT: Duration = Duration::from_secs(3);
    /// Any 32 bytes, base64-encoded, make a valid X25519 key for parsing.
    const DUMMY_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    /// A Celestia server as Phanes runs one, except that it serves any client: it completes every
    /// handshake made for its key and acknowledges every batch. Returns its base URL and the keys a
    /// client needs.
    async fn celestia_server() -> (Url, SecretBox<StaticKey>, SecretBox<StaticKey>) {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (client_private_key, _) = test_keypair();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener binds");
        let address = listener.local_addr().expect("listener has an address");
        let celestia_private_key = Arc::new(celestia_private_key);
        spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                spawn(answer_connection(stream, Arc::clone(&celestia_private_key)));
            }
        });
        let base_url = url(&format!("http://{address}"));
        (base_url, client_private_key, celestia_public_key)
    }

    /// Ends the connection on anything Phanes would refuse.
    async fn answer_connection(stream: TcpStream, celestia_private_key: Arc<SecretBox<StaticKey>>) {
        let Ok(mut socket) = accept_async(stream).await else {
            return;
        };
        let Some(Ok(Message::Binary(first_message))) = socket.next().await else {
            return;
        };
        let Ok(responder) = Responder::read_first_message(&celestia_private_key, &first_message)
        else {
            return;
        };
        let Accepted {
            mut session,
            second_message,
            ..
        } = responder.finish().expect("the client sends a hello");
        if socket
            .send(Message::Binary(second_message.into()))
            .await
            .is_err()
        {
            return;
        }
        while let Some(Ok(message)) = socket.next().await {
            if let Message::Binary(ciphertext) = message {
                let batch: Batch = session
                    .decrypt(&ciphertext)
                    .expect("the client sends batches");
                let ack = session
                    .encrypt(&Ack {
                        sequence: batch.sequence,
                    })
                    .expect("an ack encrypts");
                if socket.send(Message::Binary(ack.into())).await.is_err() {
                    return;
                }
            }
        }
    }

    fn client_config(
        url: Url,
        client_celestia_private_key: SecretBox<StaticKey>,
        phanes_celestia_public_key: SecretBox<StaticKey>,
    ) -> CelestiaClientConfig {
        CelestiaClientConfig {
            url,
            client_celestia_private_key,
            phanes_celestia_public_key,
        }
    }

    /// Phanes on a port where nobody listens.
    fn unreachable_client_config() -> CelestiaClientConfig {
        let (client_private_key, celestia_public_key) = test_keypair();
        client_config(
            url("http://127.0.0.1:1"),
            client_private_key,
            celestia_public_key,
        )
    }

    fn reporter_config(celestia_client: CelestiaClientConfig) -> ReporterConfig {
        ReporterConfig::new(celestia_client, PROCESS_NAME.to_owned()).expect("the url is http")
    }

    async fn connect_to(celestia_client: &CelestiaClientConfig) -> Result<Connection, ClientError> {
        let celestia_url = build_celestia_url(&celestia_client.url).expect("the url is http");
        let hello = Hello {
            process_name: PROCESS_NAME.to_owned(),
        };
        connect(&celestia_url, celestia_client, &hello).await
    }

    #[test]
    fn config_table_prints_the_url_on_one_line_and_redacts_keys() {
        let client_config: CelestiaClientConfig = toml::from_str(&format!(
            "url = \"https://phanes.example\"\nclient_celestia_private_key = \"{DUMMY_KEY}\"\nphanes_celestia_public_key = \"{DUMMY_KEY}\"\n"
        ))
        .expect("the table is valid");
        let expected = r#"CelestiaClientConfig {
    url: https://phanes.example/,
    client_celestia_private_key: SecretBox<[u8; 32]>([REDACTED]),
    phanes_celestia_public_key: SecretBox<[u8; 32]>([REDACTED]),
}"#;
        assert_eq!(expected, format!("{client_config:#?}"));
    }

    #[test]
    fn reporter_config_refuses_a_url_that_is_not_http_or_websocket() {
        let (client_private_key, celestia_public_key) = test_keypair();
        let celestia_client = client_config(
            url("ftp://phanes.example"),
            client_private_key,
            celestia_public_key,
        );
        assert!(ReporterConfig::new(celestia_client, PROCESS_NAME.to_owned()).is_err());
    }

    #[test]
    fn clone_holds_the_same_url_and_keys() {
        let original = unreachable_client_config();
        let copy = original.clone();
        assert_eq!(original.url, copy.url);
        assert_eq!(
            original.client_celestia_private_key.expose_secret(),
            copy.client_celestia_private_key.expose_secret()
        );
        assert_eq!(
            original.phanes_celestia_public_key.expose_secret(),
            copy.phanes_celestia_public_key.expose_secret()
        );
    }

    #[tokio::test]
    async fn session_delivers_pending_and_new_batches() {
        let (url, client_private_key, celestia_public_key) = celestia_server().await;
        let mut outbox = Outbox::new(10, 100);
        outbox.push(vec![test_process_sample()]);
        let (sender, mut receiver) = channel(8);
        for _ in 0..2 {
            sender
                .send(test_process_sample())
                .await
                .expect("sample is queued");
        }
        drop(sender);

        let connection = connect_to(&client_config(url, client_private_key, celestia_public_key))
            .await
            .expect("the client connects");
        exchange_batches(connection, &mut outbox, &mut receiver)
            .await
            .expect("session ends cleanly after the channel closes");
        assert!(outbox.is_empty(), "every batch was acknowledged");
    }

    #[tokio::test]
    async fn handshake_for_another_server_fails() {
        let (url, client_private_key, _) = celestia_server().await;
        let (_, other_celestia_public_key) = test_keypair();
        let result = connect_to(&client_config(
            url,
            client_private_key,
            other_celestia_public_key,
        ))
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn unreachable_server_is_an_error() {
        let result = connect_to(&unreachable_client_config()).await;
        assert!(matches!(result, Err(ClientError::WebSocket(_))));
    }

    /// Samples go into the outbox while the reporter connects or backs off.
    #[tokio::test]
    async fn samples_wait_in_the_outbox_while_disconnected() {
        let mut outbox = Outbox::new(0, 10);
        let (sender, mut receiver) = channel(SAMPLE_CHANNEL_CAPACITY);
        // The test holds `sender` until the end, so a sender stays alive while `monitor` is done.
        let monitor_sender = sender.clone();
        let monitor = spawn(async move {
            for _ in 0..3 {
                monitor_sender
                    .send(test_process_sample())
                    .await
                    .expect("the reporter takes samples");
            }
        });
        let output = take_samples_while(sleep(SAMPLE_INTERVAL), &mut receiver, &mut outbox).await;
        assert_eq!(Some(()), output);
        assert_eq!(3, outbox.len());
        monitor.await.expect("the monitor is expected to not panic");
        drop(sender);
        let output = take_samples_while(pending::<()>(), &mut receiver, &mut outbox).await;
        assert_eq!(None, output, "every sender is gone");
    }

    /// The reporter stops once the senders are gone, and before `DRAIN_TIMEOUT` only if Phanes
    /// acknowledged every sample.
    #[tokio::test]
    async fn reporter_delivers_every_sample_then_stops() {
        let (url, client_private_key, celestia_public_key) = celestia_server().await;
        let (sender, receiver) = channel(SAMPLE_CHANNEL_CAPACITY);
        let reporter = spawn(run_celestia_reporter(
            reporter_config(client_config(url, client_private_key, celestia_public_key)),
            SAMPLE_INTERVAL,
            receiver,
        ));
        for _ in 0..3 {
            sender
                .send(test_process_sample())
                .await
                .expect("the reporter takes samples");
        }
        drop(sender);
        timeout(REPORTER_STOP_WAIT, reporter)
            .await
            .expect("the reporter drains its outbox before the drain timeout")
            .expect("the reporter is expected to not panic");
    }

    #[tokio::test]
    async fn reporter_takes_samples_while_phanes_is_unreachable() {
        let (sender, receiver) = channel(SAMPLE_CHANNEL_CAPACITY);
        let reporter = spawn(run_celestia_reporter(
            reporter_config(unreachable_client_config()),
            SAMPLE_INTERVAL,
            receiver,
        ));
        for _ in 0..3 {
            timeout(REPORTER_STOP_WAIT, sender.send(test_process_sample()))
                .await
                .expect("the reporter takes a sample at once")
                .expect("the reporter is running");
        }
        assert!(!reporter.is_finished());
        drop(sender);
        timeout(REPORTER_STOP_WAIT, reporter)
            .await
            .expect("the reporter stops once the senders are gone")
            .expect("the reporter is expected to not panic");
    }

    fn sequences(outbox: &Outbox) -> Vec<u64> {
        outbox.pending().map(|batch| batch.sequence).collect()
    }

    fn url(text: &str) -> Url {
        Url::parse(text).expect("test url is valid")
    }

    #[test]
    fn outbox_assigns_increasing_sequences_and_drops_acknowledged() {
        let mut outbox = Outbox::new(100, 10);
        assert!(outbox.is_empty());
        for _ in 0..3 {
            outbox.push(vec![test_process_sample()]);
        }
        assert_eq!(sequences(&outbox), [100, 101, 102]);
        outbox.acknowledge(101);
        assert_eq!(sequences(&outbox), [102]);
        outbox.acknowledge(50);
        assert_eq!(outbox.len(), 1);
    }

    #[test]
    fn full_outbox_drops_oldest() {
        let mut outbox = Outbox::new(0, 2);
        for _ in 0..3 {
            outbox.push(Vec::new());
        }
        assert_eq!(sequences(&outbox), [1, 2]);
    }

    #[test]
    fn builds_celestia_url() {
        let cases = [
            (
                "https://phanes.wenqing.dev/",
                "wss://phanes.wenqing.dev/celestia",
            ),
            ("http://127.0.0.1:8888", "ws://127.0.0.1:8888/celestia"),
            ("ws://127.0.0.1:8888", "ws://127.0.0.1:8888/celestia"),
        ];
        for (base_url, expected) in cases {
            assert_eq!(
                build_celestia_url(&url(base_url))
                    .expect("url converts")
                    .as_str(),
                expected
            );
        }
        assert!(build_celestia_url(&url("ftp://example.com")).is_err());
    }
}

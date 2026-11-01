//! Keeps unacknowledged batches in memory and streams them to Phanes.
use std::collections::VecDeque;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use secrecy::SecretBox;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{info, warn};
use url::{ParseError, Url};

use layline::noise::StaticKey;
use phanes::celestia::proto::{
    Ack, Batch, CELESTIA_PATH, CelestiaError, Initiator, ProcessSample, Session,
};

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
pub fn build_celestia_url(base_url: &Url) -> Result<Url, CelestiaUrlError> {
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

/// Connects, resends everything pending, then streams new samples until an error.
/// Returns `Ok` only after the sample channel is closed and pending acks are drained.
pub async fn run_session(
    url: &Url,
    client_private_key: &SecretBox<StaticKey>,
    celestia_public_key: &StaticKey,
    outbox: &mut Outbox,
    samples: &mut Receiver<ProcessSample>,
) -> Result<(), ClientError> {
    let (mut socket, _) = connect_async(url.as_str()).await?;
    let mut session = handshake(&mut socket, client_private_key, celestia_public_key).await?;
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
        tokio::select! {
            sample = samples.recv() => {
                let Some(sample) = sample else {
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
    celestia_public_key: &StaticKey,
) -> Result<Session, ClientError> {
    let (initiator, first_message) = Initiator::start(client_private_key, celestia_public_key)?;
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
    use std::sync::Arc;

    use axum::serve;
    use tokio::net::TcpListener;
    use tokio::spawn;
    use tokio::sync::mpsc::{channel, unbounded_channel};

    use super::*;
    use layline::noise::generate_keypair;
    use phanes::celestia::proto::NetworkRate;
    use phanes::celestia::server::{CelestiaState, router};

    fn keypair() -> (SecretBox<StaticKey>, StaticKey) {
        let keypair = generate_keypair().expect("keypair generation succeeds");
        (
            SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.private)),
            keypair.public.try_into().expect("public key is 32 bytes"),
        )
    }

    /// A real Phanes Celestia endpoint; returns its URL and the keys a client needs.
    async fn phanes() -> (Url, SecretBox<StaticKey>, StaticKey) {
        let (celestia_private_key, celestia_public_key) = keypair();
        let (client_private_key, client_public_key) = keypair();
        let (notifications, _) = unbounded_channel();
        let state = Arc::new(CelestiaState::new(
            celestia_private_key,
            client_public_key,
            notifications,
        ));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener binds");
        let address = listener.local_addr().expect("listener has an address");
        spawn(async move { serve(listener, router(state)).await });
        let url = build_celestia_url(&url(&format!("http://{address}"))).expect("url converts");
        (url, client_private_key, celestia_public_key)
    }

    #[tokio::test]
    async fn session_delivers_pending_and_new_batches() {
        let (url, client_private_key, celestia_public_key) = phanes().await;
        let mut outbox = Outbox::new(10, 100);
        outbox.push(vec![sample()]);
        let (sender, mut receiver) = channel(8);
        for _ in 0..2 {
            sender.send(sample()).await.expect("sample is queued");
        }
        drop(sender);

        run_session(
            &url,
            &client_private_key,
            &celestia_public_key,
            &mut outbox,
            &mut receiver,
        )
        .await
        .expect("session ends cleanly after the channel closes");
        assert!(outbox.is_empty(), "every batch was acknowledged");
    }

    #[tokio::test]
    async fn wrong_key_fails_the_session() {
        let (url, _, celestia_public_key) = phanes().await;
        let (impostor_private_key, _) = keypair();
        let (_sender, mut receiver) = channel(1);
        let result = run_session(
            &url,
            &impostor_private_key,
            &celestia_public_key,
            &mut Outbox::new(0, 1),
            &mut receiver,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn unreachable_server_is_an_error() {
        let (client_private_key, celestia_public_key) = keypair();
        let (_sender, mut receiver) = channel(1);
        let result = run_session(
            &url("ws://127.0.0.1:1/celestia"),
            &client_private_key,
            &celestia_public_key,
            &mut Outbox::new(0, 1),
            &mut receiver,
        )
        .await;
        assert!(matches!(result, Err(ClientError::WebSocket(_))));
    }

    fn sample() -> ProcessSample {
        ProcessSample {
            timestamp: Duration::ZERO,
            cpu_percent: 0.0,
            resident_memory_bytes: 0,
            network: NetworkRate::default(),
        }
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
            outbox.push(vec![sample()]);
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

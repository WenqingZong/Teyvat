//! Phanes side: accepts Osaka's Celestia WebSocket, logs every sample and acks each batch.

use log::{error, info, warn};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Error as AxumError;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response as HttpResponse;
use axum::routing::get;
use futures::SinkExt;
use secrecy::SecretBox;
use snow::Error as NoiseError;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::timeout;

use crate::celestia::proto::{Ack, Batch, CELESTIA_PATH, CelestiaError, Session, respond};
use crate::client_filter::ClientName;
use crate::noise::{MAX_NOISE_MESSAGE_LENGTH, StaticKey};
use crate::notify::{Notification, UnauthenticatedThrottle};
use crate::request_details::{RequestDetails, RequestHead};
use crate::server::Service;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// The client sends a sample every second and pings when idle, so silence means it is gone.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub struct CelestiaState {
    celestia_private_key: SecretBox<StaticKey>,
    client_public_key: StaticKey,
    notifications: UnboundedSender<Notification>,
    unauthenticated_throttle: Mutex<UnauthenticatedThrottle>,
}

#[derive(Debug, thiserror::Error)]
enum ConnectionError {
    #[error("websocket: {0}")]
    WebSocket(#[from] AxumError),
    /// Security events, pushed to the phone with the rejected frame: wrong key or forged frames.
    #[error("unauthenticated: {error}")]
    Unauthenticated { error: CelestiaError, frame: Bytes },
    #[error("frame failed decryption: {error}")]
    Forged { error: NoiseError, frame: Bytes },
    #[error(transparent)]
    Celestia(#[from] CelestiaError),
    #[error("no message within {0:?}")]
    Timeout(Duration),
    #[error("client closed the connection during the handshake")]
    Closed,
    #[error("unexpected text frame")]
    UnexpectedText,
}

impl ConnectionError {
    /// `None` for runtime errors, which are not security events.
    fn get_rejected_frame(&self) -> Option<&[u8]> {
        match self {
            Self::Unauthenticated { frame, .. } | Self::Forged { frame, .. } => Some(frame),
            _ => None,
        }
    }
}

impl CelestiaState {
    pub fn new(
        celestia_private_key: SecretBox<StaticKey>,
        client_public_key: StaticKey,
        notifications: UnboundedSender<Notification>,
    ) -> Self {
        Self {
            celestia_private_key,
            client_public_key,
            notifications,
            unauthenticated_throttle: Mutex::default(),
        }
    }

    fn notify_unauthenticated(&self, request: RequestDetails) {
        let suppressed = self
            .unauthenticated_throttle
            .lock()
            .expect("unauthenticated throttle mutex is not poisoned")
            .record(Instant::now());
        let Some(suppressed) = suppressed else {
            return;
        };
        let notification = Notification::Unauthenticated {
            service: Service::Celestia,
            request,
            suppressed,
        };
        if self.notifications.send(notification).is_err() {
            error!("notification dispatcher is gone; push dropped");
        }
    }
}

pub fn router(state: Arc<CelestiaState>) -> Router {
    Router::new()
        .route(CELESTIA_PATH, get(upgrade))
        .with_state(state)
}

async fn upgrade(
    State(state): State<Arc<CelestiaState>>,
    client: ClientName,
    request_head: RequestHead,
    websocket: WebSocketUpgrade,
) -> HttpResponse {
    websocket
        .max_message_size(MAX_NOISE_MESSAGE_LENGTH)
        .on_upgrade(move |socket| async move {
            match serve_connection(socket, &state, &client).await {
                Ok(()) => info!("Celestia client {client} disconnected"),
                Err(error) => report_connection_error(&state, &client, request_head, &error),
            }
        })
}

/// Security events are logged with the whole request and pushed; the rest only logged.
fn report_connection_error(
    state: &CelestiaState,
    client: &ClientName,
    request_head: RequestHead,
    error: &ConnectionError,
) {
    match error.get_rejected_frame() {
        Some(frame) => {
            let request = RequestDetails::new(request_head, frame);
            warn!("Celestia security event from client {client}: {error}; {request}");
            state.notify_unauthenticated(request);
        }
        None => warn!("Celestia connection of client {client} ended: {error}"),
    }
}

/// Runtime errors only end this connection; the client reconnects and resends unacked batches.
async fn serve_connection(
    mut socket: WebSocket,
    state: &CelestiaState,
    client: &ClientName,
) -> Result<(), ConnectionError> {
    let mut session = handshake(&mut socket, state).await?;
    info!("Celestia client {client} connected");
    loop {
        let Some(message) = receive(&mut socket, IDLE_TIMEOUT).await? else {
            return Ok(());
        };
        match message {
            Message::Binary(ciphertext) => {
                let batch: Batch = session.decrypt(&ciphertext).map_err(|error| match error {
                    CelestiaError::Noise(error) => ConnectionError::Forged {
                        error,
                        frame: ciphertext,
                    },
                    CelestiaError::Payload(_) => ConnectionError::Celestia(error),
                })?;
                let sequence = batch.sequence;
                for sample in &batch.samples {
                    info!("{client} #{sequence}: {sample}");
                }
                let ack = session.encrypt(&Ack { sequence })?;
                socket.send(Message::Binary(ack.into())).await?;
            }
            // tungstenite queues the pong / close reply itself but only sends it on the next
            // read, write or flush; flush now, a dropped socket would never send the close.
            Message::Ping(_) => socket.flush().await?,
            Message::Pong(_) => {}
            Message::Close(_) => {
                socket.flush().await?;
                return Ok(());
            }
            Message::Text(_) => return Err(ConnectionError::UnexpectedText),
        }
    }
}

async fn handshake(
    socket: &mut WebSocket,
    state: &CelestiaState,
) -> Result<Session, ConnectionError> {
    let first_message = match receive(socket, HANDSHAKE_TIMEOUT).await? {
        Some(Message::Binary(first_message)) => first_message,
        Some(Message::Text(_)) => return Err(ConnectionError::UnexpectedText),
        Some(_) | None => return Err(ConnectionError::Closed),
    };
    let (session, second_message) = respond(
        &state.celestia_private_key,
        &state.client_public_key,
        &first_message,
    )
    .map_err(|error| ConnectionError::Unauthenticated {
        error,
        frame: first_message,
    })?;
    socket.send(Message::Binary(second_message.into())).await?;
    Ok(session)
}

/// `None` once the client has gone away.
async fn receive(
    socket: &mut WebSocket,
    limit: Duration,
) -> Result<Option<Message>, ConnectionError> {
    match timeout(limit, socket.recv()).await {
        Ok(message) => Ok(message.transpose()?),
        Err(_) => Err(ConnectionError::Timeout(limit)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::celestia::proto::Initiator;
    use crate::celestia::proto::tests::sample;
    use crate::noise::tests::test_keypair;
    use axum::serve;
    use futures::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::spawn;
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use tokio_tungstenite::tungstenite::Message as ClientMessage;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

    /// Long enough for the server task to push after the socket closes.
    const NOTIFICATION_WAIT: Duration = Duration::from_millis(500);
    /// Longer than the Noise tag, so the server attempts decryption.
    const FORGED_FRAME_LENGTH: usize = 64;
    const PING_PAYLOAD: &[u8] = b"heartbeat";

    type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

    struct Harness {
        url: String,
        client_private_key: SecretBox<StaticKey>,
        celestia_public_key: StaticKey,
        notifications: UnboundedReceiver<Notification>,
    }

    async fn harness() -> Harness {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let (sender, notifications) = unbounded_channel();
        let state = Arc::new(CelestiaState::new(
            celestia_private_key,
            client_public_key,
            sender,
        ));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener binds");
        let address = listener.local_addr().expect("listener has an address");
        spawn(async move { serve(listener, router(state)).await });
        Harness {
            url: format!("ws://{address}{CELESTIA_PATH}"),
            client_private_key,
            celestia_public_key,
            notifications,
        }
    }

    impl Harness {
        /// Sends handshake message 1 signed with `client_private_key`.
        async fn start(
            &self,
            client_private_key: &SecretBox<StaticKey>,
        ) -> (ClientSocket, Initiator) {
            let (mut socket, _) = connect_async(self.url.as_str())
                .await
                .expect("client connects");
            let (initiator, first_message) =
                Initiator::start(client_private_key, &self.celestia_public_key)
                    .expect("client starts handshake");
            socket
                .send(ClientMessage::Binary(first_message.into()))
                .await
                .expect("first message is sent");
            (socket, initiator)
        }

        async fn connect(&self) -> (ClientSocket, Session) {
            let (mut socket, initiator) = self.start(&self.client_private_key).await;
            let Some(Ok(ClientMessage::Binary(second_message))) = socket.next().await else {
                panic!("server should answer the handshake");
            };
            let session = initiator
                .finish(&second_message)
                .expect("client finishes handshake");
            (socket, session)
        }

        async fn assert_disconnected_after(&self, mut socket: ClientSocket, frame: ClientMessage) {
            socket.send(frame).await.expect("frame is sent");
            assert_disconnected(&mut socket).await;
        }

        async fn next_notification(&mut self) -> Option<Notification> {
            timeout(NOTIFICATION_WAIT, self.notifications.recv())
                .await
                .ok()
                .flatten()
        }
    }

    async fn assert_disconnected(socket: &mut ClientSocket) {
        assert!(matches!(
            socket.next().await,
            None | Some(Ok(ClientMessage::Close(_))) | Some(Err(_))
        ));
    }

    /// Checks the push and returns the rejected frame it carries.
    fn expect_security_event(notification: Option<Notification>) -> Bytes {
        let Some(Notification::Unauthenticated {
            service,
            request,
            suppressed,
        }) = notification
        else {
            panic!("expected a security event, got {notification:?}");
        };
        assert_eq!((service, suppressed), (Service::Celestia, 0));
        assert_eq!(request.head.uri.path(), CELESTIA_PATH);
        request.payload
    }

    #[tokio::test]
    async fn batches_are_acknowledged() {
        let mut harness = harness().await;
        let (mut socket, mut session) = harness.connect().await;
        for sequence in [7, 8] {
            let batch = Batch {
                sequence,
                samples: vec![sample()],
            };
            let ciphertext = session.encrypt(&batch).expect("client encrypts batch");
            socket
                .send(ClientMessage::Binary(ciphertext.into()))
                .await
                .expect("batch is sent");
            let Some(Ok(ClientMessage::Binary(reply))) = socket.next().await else {
                panic!("server should ack the batch");
            };
            let ack: Ack = session.decrypt(&reply).expect("client decrypts ack");
            assert_eq!(ack, Ack { sequence });
        }
        socket.close(None).await.expect("client closes");
        assert_eq!(harness.next_notification().await, None);
    }

    #[tokio::test]
    async fn ping_is_answered_with_pong() {
        let harness = harness().await;
        let (mut socket, _) = harness.connect().await;
        socket
            .send(ClientMessage::Ping(PING_PAYLOAD.into()))
            .await
            .expect("ping is sent");
        let Some(Ok(ClientMessage::Pong(payload))) = socket.next().await else {
            panic!("server should answer the ping");
        };
        assert_eq!(payload.as_ref(), PING_PAYLOAD);
    }

    #[tokio::test]
    async fn close_is_echoed() {
        let mut harness = harness().await;
        let (mut socket, _) = harness.connect().await;
        socket
            .send(ClientMessage::Close(None))
            .await
            .expect("close is sent");
        assert!(matches!(
            socket.next().await,
            Some(Ok(ClientMessage::Close(None)))
        ));
        assert_eq!(harness.next_notification().await, None);
    }

    #[tokio::test]
    async fn wrong_client_key_ends_the_connection_and_notifies() {
        let mut harness = harness().await;
        let (impostor_private_key, _) = test_keypair();
        let (mut socket, _) = harness.start(&impostor_private_key).await;
        assert_disconnected(&mut socket).await;
        let frame = expect_security_event(harness.next_notification().await);
        assert!(!frame.is_empty());
    }

    #[tokio::test]
    async fn forged_frame_ends_the_connection_and_notifies() {
        let mut harness = harness().await;
        let (socket, _) = harness.connect().await;
        let forged_frame = vec![0u8; FORGED_FRAME_LENGTH];
        let forged = ClientMessage::Binary(forged_frame.clone().into());
        harness.assert_disconnected_after(socket, forged).await;
        let frame = expect_security_event(harness.next_notification().await);
        assert_eq!(frame.as_ref(), forged_frame.as_slice());
    }

    #[tokio::test]
    async fn text_frame_ends_the_connection_quietly() {
        let mut harness = harness().await;
        let (socket, _) = harness.connect().await;
        harness
            .assert_disconnected_after(socket, ClientMessage::Text("hello".into()))
            .await;
        assert_eq!(harness.next_notification().await, None);
    }

    #[tokio::test]
    async fn malformed_payload_ends_the_connection_quietly() {
        let mut harness = harness().await;
        let (socket, mut session) = harness.connect().await;
        let not_a_batch = session
            .encrypt(&Ack { sequence: 1 })
            .expect("client encrypts");
        harness
            .assert_disconnected_after(socket, ClientMessage::Binary(not_a_batch.into()))
            .await;
        assert_eq!(harness.next_notification().await, None);
    }
}

//! Phanes side: accepts a client's Celestia WebSocket, identifies the client by the public key of
//! its handshake, logs every sample and acks each batch.

use std::sync::Arc;
use std::time::Duration;

use axum::Error as AxumError;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response as HttpResponse;
use axum::routing::get;
use futures::SinkExt;
use secrecy::SecretBox;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::timeout;
use tracing::{info, warn};

use crate::celestia::proto::{Ack, Batch, CELESTIA_PATH, CelestiaError, Responder, Session};
use crate::client_filter::IpClientName;
use crate::clients::{ClientName, Clients, identify};
use crate::notify::{Notification, ThrottledNotificationSender};
use crate::request_details::{RequestDetails, RequestHead};
use crate::service::Service;
use layline::noise::{MAX_NOISE_MESSAGE_LENGTH, StaticKey};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// The client sends a sample every second and pings when idle, so silence means it is gone.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub struct CelestiaState {
    celestia_private_key: SecretBox<StaticKey>,
    clients: Arc<Clients>,
    unauthenticated_notification_sender: ThrottledNotificationSender,
}

#[derive(Debug, thiserror::Error)]
enum ConnectionError {
    #[error("websocket: {0}")]
    WebSocket(#[from] AxumError),
    /// Security events, pushed to the phone with the rejected frame: a failed handshake, a key of
    /// no client or of a client the address does not belong to, or a forged frame.
    #[error("{reason}")]
    Rejected { reason: String, frame: Bytes },
    #[error(transparent)]
    Celestia(#[from] CelestiaError),
    #[error("no message within {0:?}")]
    Timeout(Duration),
    #[error("client closed the connection during the handshake")]
    Closed,
    #[error("unexpected text frame")]
    UnexpectedText,
}

impl CelestiaState {
    pub fn new(
        celestia_private_key: SecretBox<StaticKey>,
        clients: Arc<Clients>,
        notification_sender: UnboundedSender<Notification>,
    ) -> Self {
        Self {
            celestia_private_key,
            clients,
            unauthenticated_notification_sender: ThrottledNotificationSender::new(
                notification_sender,
            ),
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
    IpClientName(ip_client_name_option): IpClientName,
    request_head: RequestHead,
    websocket: WebSocketUpgrade,
) -> HttpResponse {
    websocket
        .max_message_size(MAX_NOISE_MESSAGE_LENGTH)
        .on_upgrade(move |mut socket| async move {
            let (mut session, client) =
                match handshake(&mut socket, &state, ip_client_name_option.as_ref()).await {
                    Ok(established) => established,
                    Err(error) => {
                        return report_connection_error(&state, None, request_head, &error);
                    }
                };
            info!("Celestia client {client} connected");
            match exchange_batches(socket, &mut session, &client).await {
                Ok(()) => info!("Celestia client {client} disconnected"),
                Err(error) => report_connection_error(&state, Some(&client), request_head, &error),
            }
        })
}

/// Security events are logged with the whole request and pushed; the rest only logged.
/// `client_option` is `None` while the handshake has not yet identified the client.
fn report_connection_error(
    state: &CelestiaState,
    client_option: Option<&ClientName>,
    request_head: RequestHead,
    error: &ConnectionError,
) {
    let caller = client_option.map_or_else(
        || "an unidentified client".to_owned(),
        |client| format!("client {client}"),
    );
    match error {
        ConnectionError::Rejected { frame, .. } => {
            let request = RequestDetails::new(request_head, frame);
            warn!("Celestia security event from {caller}: {error}; {request}");
            state
                .unauthenticated_notification_sender
                .enqueue(|suppressed| Notification::Unauthenticated {
                    service: Service::Celestia,
                    request,
                    suppressed,
                });
        }
        _ => warn!("Celestia connection of {caller} ended: {error}"),
    }
}

/// Runtime errors only end this connection; the client reconnects and resends unacked batches.
async fn exchange_batches(
    mut socket: WebSocket,
    session: &mut Session,
    client: &ClientName,
) -> Result<(), ConnectionError> {
    loop {
        let Some(message) = receive(&mut socket, IDLE_TIMEOUT).await? else {
            return Ok(());
        };
        match message {
            Message::Binary(ciphertext) => {
                let batch: Batch = session.decrypt(&ciphertext).map_err(|error| match error {
                    CelestiaError::Noise(error) => ConnectionError::Rejected {
                        reason: format!("frame failed decryption: {error}"),
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

/// The session, and the client its key belongs to. `ip_client_name_option` is the client the
/// address belongs to, `None` in debug builds.
async fn handshake(
    socket: &mut WebSocket,
    state: &CelestiaState,
    ip_client_name_option: Option<&ClientName>,
) -> Result<(Session, ClientName), ConnectionError> {
    let first_message = match receive(socket, HANDSHAKE_TIMEOUT).await? {
        Some(Message::Binary(first_message)) => first_message,
        Some(Message::Text(_)) => return Err(ConnectionError::UnexpectedText),
        Some(_) | None => return Err(ConnectionError::Closed),
    };
    let responder = match Responder::read_first_message(&state.celestia_private_key, &first_message)
    {
        Ok(responder) => responder,
        Err(error) => {
            return Err(ConnectionError::Rejected {
                reason: format!("unauthenticated: {error}"),
                frame: first_message,
            });
        }
    };
    let client = match identify(
        &state.clients,
        Service::Celestia,
        responder.client_public_key(),
        ip_client_name_option,
    ) {
        Ok((client_name, _)) => client_name.clone(),
        Err(error) => {
            return Err(ConnectionError::Rejected {
                reason: error.to_string(),
                frame: first_message,
            });
        }
    };
    let (session, second_message) = responder.finish()?;
    socket.send(Message::Binary(second_message.into())).await?;
    Ok((session, client))
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
    use std::net::Ipv4Addr;

    use axum::serve;
    use futures::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::spawn;
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use tokio_tungstenite::tungstenite::Message as ClientMessage;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

    use super::*;
    use crate::celestia::proto::Initiator;
    use crate::celestia::proto::tests::sample;
    use crate::clients::ClientConfig;
    use crate::clients::tests::test_client;
    use layline::testing::test_keypair;

    /// Long enough for the server task to push after the socket closes.
    const NOTIFICATION_WAIT: Duration = Duration::from_millis(500);
    /// Longer than the Noise tag, so the server attempts decryption.
    const FORGED_FRAME_LENGTH: usize = 64;
    const PING_PAYLOAD: &[u8] = b"heartbeat";
    const CLIENT_NAME: &str = "Osaka";
    /// Arbitrary documentation address (RFC 5737) with no business meaning.
    const CLIENT_IPV4: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);

    type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

    struct Harness {
        url: String,
        client_private_key: SecretBox<StaticKey>,
        celestia_public_key: SecretBox<StaticKey>,
        notification_receiver: UnboundedReceiver<Notification>,
    }

    async fn harness() -> Harness {
        let (celestia_private_key, celestia_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let (notification_sender, notification_receiver) = unbounded_channel();
        let client = ClientConfig {
            client_celestia_service_public_key: client_public_key,
            ..test_client(CLIENT_IPV4)
        };
        let clients = Clients::from([(ClientName::from(CLIENT_NAME), client)]);
        let state = Arc::new(CelestiaState::new(
            celestia_private_key,
            Arc::new(clients),
            notification_sender,
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
            notification_receiver,
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
            timeout(NOTIFICATION_WAIT, self.notification_receiver.recv())
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
    async fn key_of_no_client_ends_the_connection_and_notifies() {
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

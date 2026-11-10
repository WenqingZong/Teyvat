//! Phanes side: accepts a client's Celestia WebSocket, identifies the client by the public key of
//! its handshake and the process by the handshake's `Hello`, logs every sample and acks each batch.

use std::collections::HashMap;
use std::fmt::{Display, Formatter, Result as FormatResult};
use std::sync::{Arc, Mutex, MutexGuard};
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
use tokio::select;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender, channel};
use tokio::time::timeout;
use tracing::{info, warn};

use crate::client_filter::IpClientName;
use crate::clients::{ClientName, Clients, identify};
use crate::notify::{Notification, ThrottledNotificationSender, enqueue};
use crate::request_details::{LOG_INDENT, RequestDetails, RequestHead};
use crate::service::Service;
use layline::celestia::{
    Accepted, Ack, Batch, CELESTIA_PATH, CelestiaError, Hello, Responder, Session,
};
use layline::noise::{MAX_NOISE_MESSAGE_LENGTH, StaticKey};
use layline::process_metrics::THREAD_METRICS_LEGEND;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// The client pings every 30 seconds besides sending its samples, so silence means it is gone.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
/// The reason the close push gives when the client closed the connection without naming one.
const CLOSED_BY_CLIENT_REASON: &str = "原因未汇报";

pub struct CelestiaState {
    celestia_private_key: SecretBox<StaticKey>,
    clients: Arc<Clients>,
    notification_sender: UnboundedSender<Notification>,
    unauthenticated_notification_sender: ThrottledNotificationSender,
    /// The open connection of each reporter. A reporter that connects again while Phanes still
    /// holds its old connection, one whose link died unnoticed, replaces it: dropping the old
    /// connection's sender ends it.
    connections: Mutex<HashMap<Reporter, OneshotSender<()>>>,
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
    Celestia(CelestiaError),
    /// The client is known, so a malformed `Hello` is a bug on its side, not a security event.
    #[error("client {client} sent a malformed hello: {error}")]
    MalformedHello {
        client: ClientName,
        error: CelestiaError,
    },
    #[error("no message within {0:?}")]
    Timeout(Duration),
    #[error("replaced by a new connection of the same reporter")]
    Replaced,
    #[error("client closed the connection during the handshake")]
    Closed,
    #[error("unexpected text frame")]
    UnexpectedText,
}

/// One reporting process: the client its key belongs to and the process its `Hello` named.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Reporter {
    client: ClientName,
    process_name: String,
}

/// `Osaka/market_data_provider`, as every log line about the reporter names it.
impl Display for Reporter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let Self {
            client,
            process_name,
        } = self;
        write!(formatter, "{client}/{process_name}")
    }
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
                notification_sender.clone(),
            ),
            notification_sender,
            connections: Mutex::default(),
        }
    }

    /// Registers the connection whose receiver `new_sender` pairs with. Returns the sender of the
    /// reporter's previous connection, which this one replaces; dropping it tells that connection.
    fn register(
        &self,
        reporter: &Reporter,
        new_sender: OneshotSender<()>,
    ) -> Option<OneshotSender<()>> {
        self.lock_connections().insert(reporter.clone(), new_sender)
    }

    /// Forgets the connection whose sender `new_receiver` pairs with and returns that sender;
    /// `None` if a newer connection of the reporter has already replaced it.
    fn unregister(
        &self,
        reporter: &Reporter,
        new_receiver: &mut OneshotReceiver<()>,
    ) -> Option<OneshotSender<()>> {
        let mut connections = self.lock_connections();
        // Only a replacement drops this connection's sender while it is registered.
        if matches!(new_receiver.try_recv(), Err(TryRecvError::Closed)) {
            return None;
        }
        connections.remove(reporter)
    }

    fn lock_connections(&self) -> MutexGuard<'_, HashMap<Reporter, OneshotSender<()>>> {
        self.connections
            .lock()
            .expect("nothing panics while holding the connections")
    }
}

pub fn router(state: Arc<CelestiaState>) -> Router {
    Router::new()
        .route(CELESTIA_PATH, get(upgrade))
        .with_state(state)
}

async fn upgrade(
    State(state): State<Arc<CelestiaState>>,
    ip_client_name: IpClientName,
    request_head: RequestHead,
    websocket: WebSocketUpgrade,
) -> HttpResponse {
    websocket
        .max_message_size(MAX_NOISE_MESSAGE_LENGTH)
        .on_upgrade(move |mut socket| async move {
            let (mut session, reporter) =
                match handshake(&mut socket, &state, ip_client_name.0.as_ref()).await {
                    Ok(established) => established,
                    Err(error) => {
                        return report_connection_error(
                            &state,
                            None,
                            ip_client_name,
                            request_head,
                            &error,
                        );
                    }
                };
            let (new_sender, mut new_receiver) = channel();
            if let Some(previous_sender) = state.register(&reporter, new_sender) {
                // Dropping it tells the previous connection that it was replaced; that connection
                // pushes the reconnect once it has ended, so the phone never reads a loss after
                // the reconnect.
                drop(previous_sender);
                info!("Celestia reporter {reporter} reconnected, replacing its old connection");
            } else {
                info!("Celestia reporter {reporter} connected");
                enqueue(
                    &state.notification_sender,
                    Notification::CelestiaConnected {
                        client: reporter.client.clone(),
                        process_name: reporter.process_name.clone(),
                    },
                );
            }
            let mut last_message_at = Instant::now();
            let exchange = exchange_batches(socket, &mut session, &reporter, &mut last_message_at);
            let exchange_result = select! {
                exchange_result = exchange => exchange_result,
                _ = &mut new_receiver => Err(ConnectionError::Replaced),
            };
            match &exchange_result {
                Ok(reason) => info!("Celestia reporter {reporter} disconnected: {reason}"),
                Err(error) => report_connection_error(
                    &state,
                    Some(&reporter),
                    ip_client_name,
                    request_head,
                    error,
                ),
            }
            // Checked after the exchange ends whatever ended it: a connection that ended on its
            // own just as its replacement arrived still announces the reconnect.
            let own_sender_option = state.unregister(&reporter, &mut new_receiver);
            let Reporter {
                client,
                process_name,
            } = reporter;
            let notification = match own_sender_option {
                // A newer connection of the reporter replaced this one.
                None => Notification::CelestiaReconnected {
                    client,
                    process_name,
                    silence: last_message_at.elapsed(),
                },
                Some(_own_sender) => match exchange_result {
                    Ok(reason) => Notification::CelestiaClosed {
                        client,
                        process_name,
                        reason,
                    },
                    Err(error) => Notification::CelestiaDisconnected {
                        client,
                        process_name,
                        reason: error.to_string(),
                    },
                },
            };
            enqueue(&state.notification_sender, notification);
        })
}

/// Security events are logged with the whole request and pushed; the rest only logged.
/// `reporter_option` is `None` until the handshake has finished.
fn report_connection_error(
    state: &CelestiaState,
    reporter_option: Option<&Reporter>,
    ip_client_name: IpClientName,
    request_head: RequestHead,
    error: &ConnectionError,
) {
    let caller = reporter_option.map_or_else(
        || "an unidentified reporter".to_owned(),
        |reporter| format!("reporter {reporter}"),
    );
    match error {
        ConnectionError::Rejected { reason, frame } => {
            let request = RequestDetails::new(request_head, frame);
            warn!(
                "Celestia security event from {caller}\n{LOG_INDENT}reason: {reason}\n\
                 {LOG_INDENT}client by IP: {ip_client_name}\n{request}"
            );
            state
                .unauthenticated_notification_sender
                .enqueue(|suppressed| Notification::Unauthenticated {
                    service: Service::Celestia,
                    reason: reason.clone(),
                    ip_client_name_option: ip_client_name.0,
                    request,
                    suppressed,
                });
        }
        _ => warn!("Celestia connection of {caller} ended: {error}"),
    }
}

/// Runs until the client closes the connection, returning the reason it gave, such as the signal
/// that stopped it. Runtime errors only end this connection; the client reconnects and resends
/// unacked batches. `last_message_at` is when the client last sent anything.
async fn exchange_batches(
    mut socket: WebSocket,
    session: &mut Session,
    reporter: &Reporter,
    last_message_at: &mut Instant,
) -> Result<String, ConnectionError> {
    loop {
        let Some(message) = receive(&mut socket, IDLE_TIMEOUT).await? else {
            return Ok(CLOSED_BY_CLIENT_REASON.to_owned());
        };
        *last_message_at = Instant::now();
        match message {
            Message::Binary(ciphertext) => {
                let batch: Batch = session.decrypt(&ciphertext).map_err(|error| match error {
                    CelestiaError::Noise(error) => ConnectionError::Rejected {
                        reason: format!("frame failed decryption: {error}"),
                        frame: ciphertext,
                    },
                    CelestiaError::Payload(_) => ConnectionError::Celestia(error),
                })?;
                let Batch { sequence, samples } = batch;
                // The timestamp tells a resent sample from a fresh one.
                for sample in &samples {
                    let timestamp_seconds = sample.timestamp_seconds;
                    info!(
                        "{reporter} #{sequence} taken at {timestamp_seconds}: process metrics: {sample}"
                    );
                    let threads = sample.format_threads();
                    info!(
                        "{reporter} #{sequence}: thread metrics ({THREAD_METRICS_LEGEND}): {threads}"
                    );
                }
                let ack = session
                    .encrypt(&Ack { sequence })
                    .expect("an ack always encrypts");
                socket.send(Message::Binary(ack.into())).await?;
            }
            // tungstenite queues the pong / close reply itself but only sends it on the next
            // read, write or flush; flush now, a dropped socket would never send the close.
            Message::Ping(_) => socket.flush().await?,
            Message::Pong(_) => {}
            Message::Close(close_frame_option) => {
                socket.flush().await?;
                return Ok(close_frame_option.map_or_else(
                    || CLOSED_BY_CLIENT_REASON.to_owned(),
                    |close_frame| close_frame.reason.to_string(),
                ));
            }
            Message::Text(_) => return Err(ConnectionError::UnexpectedText),
        }
    }
}

/// The session, and the reporter: the client its key belongs to and the process its `Hello` names.
/// `ip_client_name_option` is the client the address belongs to, `None` in debug builds.
async fn handshake(
    socket: &mut WebSocket,
    state: &CelestiaState,
    ip_client_name_option: Option<&ClientName>,
) -> Result<(Session, Reporter), ConnectionError> {
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
    let Accepted {
        session,
        hello: Hello { process_name },
        second_message,
    } = match responder.finish() {
        Ok(accepted) => accepted,
        Err(error) => return Err(ConnectionError::MalformedHello { client, error }),
    };
    socket.send(Message::Binary(second_message.into())).await?;
    Ok((
        session,
        Reporter {
            client,
            process_name,
        },
    ))
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
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

    use super::*;
    use crate::clients::ClientConfig;
    use crate::clients::tests::test_client;
    use layline::celestia::{CELESTIA_PROLOGUE, Initiator};
    use layline::noise::{build_initiator, encrypt};
    use layline::testing::{test_keypair, test_process_sample};

    /// Long enough for the server task to push after the socket closes.
    const NOTIFICATION_WAIT: Duration = Duration::from_millis(500);
    /// Longer than the Noise tag, so the server attempts decryption.
    const FORGED_FRAME_LENGTH: usize = 64;
    const PING_PAYLOAD: &[u8] = b"heartbeat";
    const CLIENT_NAME: &str = "Osaka";
    const PROCESS_NAME: &str = "market_data_provider";
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
            let hello = Hello {
                process_name: PROCESS_NAME.to_owned(),
            };
            let (initiator, first_message) =
                Initiator::start(client_private_key, &self.celestia_public_key, &hello)
                    .expect("client starts handshake");
            socket
                .send(ClientMessage::Binary(first_message.into()))
                .await
                .expect("first message is sent");
            (socket, initiator)
        }

        async fn handshake(&self) -> (ClientSocket, Session) {
            let (mut socket, initiator) = self.start(&self.client_private_key).await;
            let Some(Ok(ClientMessage::Binary(second_message))) = socket.next().await else {
                panic!("server should answer the handshake");
            };
            let session = initiator
                .finish(&second_message)
                .expect("client finishes handshake");
            (socket, session)
        }

        /// Completes the handshake and checks the push it triggers, so later checks see only
        /// what happens after the connection.
        async fn connect(&mut self) -> (ClientSocket, Session) {
            let (socket, session) = self.handshake().await;
            assert_eq!(
                self.next_notification().await,
                Some(Notification::CelestiaConnected {
                    client: ClientName::from(CLIENT_NAME),
                    process_name: PROCESS_NAME.to_owned(),
                })
            );
            (socket, session)
        }

        /// The connection `connect` opened has ended: checks that its loss is pushed for the
        /// right reporter and nothing after it, and returns the reason the push gives.
        async fn loss_reason(&mut self) -> String {
            let notification = self.next_notification().await;
            let Some(Notification::CelestiaDisconnected {
                client,
                process_name,
                reason,
            }) = notification
            else {
                panic!("expected the loss to be pushed, got {notification:?}");
            };
            assert_eq!(
                (client, process_name.as_str()),
                (ClientName::from(CLIENT_NAME), PROCESS_NAME)
            );
            assert_eq!(self.next_notification().await, None);
            reason
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

    /// The push of a connection the client closed on purpose.
    fn closed(reason: &str) -> Notification {
        Notification::CelestiaClosed {
            client: ClientName::from(CLIENT_NAME),
            process_name: PROCESS_NAME.to_owned(),
            reason: reason.to_owned(),
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
            ..
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
                samples: vec![test_process_sample()],
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
        assert_eq!(
            harness.next_notification().await,
            Some(closed(CLOSED_BY_CLIENT_REASON))
        );
    }

    #[tokio::test]
    async fn ping_is_answered_with_pong() {
        let mut harness = harness().await;
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
        assert_eq!(
            harness.next_notification().await,
            Some(closed(CLOSED_BY_CLIENT_REASON))
        );
    }

    #[tokio::test]
    async fn close_reason_is_pushed() {
        let mut harness = harness().await;
        let (mut socket, _) = harness.connect().await;
        let close_frame = CloseFrame {
            code: CloseCode::Normal,
            reason: "SIGINT".into(),
        };
        socket
            .close(Some(close_frame))
            .await
            .expect("client closes");
        assert_eq!(harness.next_notification().await, Some(closed("SIGINT")));
    }

    /// A link that died without Phanes noticing: the reporter connects again, and the phone reads
    /// one reconnect instead of a connect followed by the old connection's loss.
    #[tokio::test]
    async fn new_connection_replaces_the_old_one_and_pushes_one_reconnect() {
        let mut harness = harness().await;
        let (mut old_socket, _) = harness.connect().await;
        let (mut new_socket, _) = harness.handshake().await;
        assert_disconnected(&mut old_socket).await;
        let notification = harness.next_notification().await;
        let Some(Notification::CelestiaReconnected {
            client,
            process_name,
            silence,
        }) = notification
        else {
            panic!("expected the reconnect to be pushed, got {notification:?}");
        };
        assert_eq!(
            (client, process_name.as_str()),
            (ClientName::from(CLIENT_NAME), PROCESS_NAME)
        );
        assert!(silence < NOTIFICATION_WAIT, "{silence:?}");
        assert_eq!(harness.next_notification().await, None);
        // The new connection is the registered one, so its own end is pushed as usual.
        new_socket.close(None).await.expect("client closes");
        assert_eq!(
            harness.next_notification().await,
            Some(closed(CLOSED_BY_CLIENT_REASON))
        );
    }

    #[tokio::test]
    async fn dropped_connection_is_notified_as_lost() {
        let mut harness = harness().await;
        let (socket, _) = harness.connect().await;
        drop(socket);
        // A socket gone without a close frame is a websocket error, not a clean close.
        let reason = harness.loss_reason().await;
        assert!(reason.starts_with("websocket:"), "{reason}");
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

    #[test]
    fn reporter_prints_as_client_slash_process() {
        let reporter = Reporter {
            client: ClientName::from(CLIENT_NAME),
            process_name: PROCESS_NAME.to_owned(),
        };
        assert_eq!(reporter.to_string(), "Osaka/market_data_provider");
    }

    /// A known client whose first message carries no `Hello` is a bug on its side, not an attack.
    #[tokio::test]
    async fn first_message_without_a_hello_ends_the_connection_quietly() {
        let mut harness = harness().await;
        let (mut socket, _) = connect_async(harness.url.as_str())
            .await
            .expect("client connects");
        let mut state = build_initiator(
            CELESTIA_PROLOGUE,
            &harness.client_private_key,
            &harness.celestia_public_key,
        )
        .expect("initiator builds");
        let first_message = encrypt(&mut state, &[]).expect("empty message 1 encrypts");
        socket
            .send(ClientMessage::Binary(first_message.into()))
            .await
            .expect("first message is sent");
        assert_disconnected(&mut socket).await;
        assert_eq!(harness.next_notification().await, None);
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
        let reason = harness.loss_reason().await;
        assert!(reason.starts_with("frame failed decryption"), "{reason}");
    }

    #[tokio::test]
    async fn text_frame_ends_the_connection_without_a_security_event() {
        let mut harness = harness().await;
        let (socket, _) = harness.connect().await;
        harness
            .assert_disconnected_after(socket, ClientMessage::Text("hello".into()))
            .await;
        assert_eq!(
            harness.loss_reason().await,
            ConnectionError::UnexpectedText.to_string()
        );
    }

    #[tokio::test]
    async fn malformed_payload_ends_the_connection_without_a_security_event() {
        let mut harness = harness().await;
        let (socket, mut session) = harness.connect().await;
        let not_a_batch = session
            .encrypt(&Ack { sequence: 1 })
            .expect("client encrypts");
        harness
            .assert_disconnected_after(socket, ClientMessage::Binary(not_a_batch.into()))
            .await;
        let reason = harness.loss_reason().await;
        assert!(reason.starts_with("payload:"), "{reason}");
    }
}

//! Phanes side of Gnosis: identifies the client by the public key of its handshake and releases
//! the gnoses its `allowed_gnosis_ids` lists.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::post;
use secrecy::SecretBox;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

use crate::client_filter::IpClientName;
use crate::clients::{ClientConfig, Clients, identify};
use crate::gnosis::config::{AnyGnosis, Gnoses};
use crate::notify::{Notification, ThrottledNotificationSender, enqueue};
use crate::request_details::{RequestDetails, RequestHead};
use crate::service::Service;
use layline::gnosis::{GNOSIS_PATH, NONCE_LENGTH, Request, Responder, Response};
use layline::noise::StaticKey;
use layline::time::since_unix_epoch;

/// Noise message 1 carries a small JSON payload; anything bigger is not a real client.
const MAX_REQUEST_BODY_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DenialReason {
    #[error("malformed request payload")]
    MalformedRequest,
    #[error("timestamp outside allowed clock skew ({skew_seconds}s)")]
    ClockSkew { skew_seconds: u64 },
    #[error("malformed nonce")]
    MalformedNonce,
    #[error("replayed nonce")]
    Replay,
    #[error("gnosis not allowed for this client")]
    NotAllowed,
}

pub struct GnosisState {
    gnosis_private_key: SecretBox<StaticKey>,
    clients: Arc<Clients>,
    gnoses: Gnoses,
    max_clock_skew_seconds: u64,
    notification_sender: UnboundedSender<Notification>,
    unauthenticated_notification_sender: ThrottledNotificationSender,
    /// nonce -> unix second after which the nonce's timestamp is out of window anyway.
    seen_nonces: Mutex<HashMap<String, u64>>,
}

impl GnosisState {
    pub fn new(
        gnosis_private_key: SecretBox<StaticKey>,
        clients: Arc<Clients>,
        gnoses: Gnoses,
        max_clock_skew_seconds: u64,
        notification_sender: UnboundedSender<Notification>,
    ) -> Self {
        Self {
            gnosis_private_key,
            clients,
            gnoses,
            max_clock_skew_seconds,
            unauthenticated_notification_sender: ThrottledNotificationSender::new(
                notification_sender.clone(),
            ),
            notification_sender,
            seen_nonces: Mutex::default(),
        }
    }

    /// Logs the whole request, pushes (throttled) and answers with an empty 404.
    fn reject_unauthenticated(
        &self,
        reason: &str,
        request_head: RequestHead,
        body: &[u8],
    ) -> HttpResponse {
        let request = RequestDetails::new(request_head, body);
        warn!("rejected unauthenticated Gnosis request: {reason}; {request}");
        self.unauthenticated_notification_sender
            .enqueue(|suppressed| Notification::Unauthenticated {
                service: Service::Gnosis,
                request,
                suppressed,
            });
        StatusCode::NOT_FOUND.into_response()
    }

    fn decide(&self, client: &ClientConfig, request: &Request) -> Result<AnyGnosis, DenialReason> {
        let now = since_unix_epoch().as_secs();
        let skew_seconds = now.abs_diff(request.timestamp);
        if skew_seconds > self.max_clock_skew_seconds {
            return Err(DenialReason::ClockSkew { skew_seconds });
        }
        self.check_and_record_nonce(&request.nonce, request.timestamp, now)?;
        if !client.allowed_gnosis_ids.contains(&request.gnosis_id) {
            return Err(DenialReason::NotAllowed);
        }
        Ok(self
            .gnoses
            .get(request.gnosis_id)
            .expect("PhanesConfig::load checked every allowed gnosis has a table"))
    }

    fn check_and_record_nonce(
        &self,
        nonce: &str,
        timestamp: u64,
        now: u64,
    ) -> Result<(), DenialReason> {
        let is_well_formed =
            nonce.len() == NONCE_LENGTH * 2 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit());
        if !is_well_formed {
            return Err(DenialReason::MalformedNonce);
        }
        let mut seen_nonces = self
            .seen_nonces
            .lock()
            .expect("nonce cache mutex is not poisoned");
        seen_nonces.retain(|_, expiry| *expiry >= now);
        if seen_nonces.contains_key(nonce) {
            return Err(DenialReason::Replay);
        }
        seen_nonces.insert(
            nonce.to_ascii_lowercase(),
            timestamp + self.max_clock_skew_seconds,
        );
        Ok(())
    }
}

pub fn router(state: Arc<GnosisState>) -> Router {
    Router::new()
        .route(GNOSIS_PATH, post(gnosis))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

async fn gnosis(
    State(state): State<Arc<GnosisState>>,
    IpClientName(ip_client_name_option): IpClientName,
    request_head: RequestHead,
    body: Bytes,
) -> HttpResponse {
    let responder = match Responder::read_request(&state.gnosis_private_key, &body) {
        Ok(responder) => responder,
        Err(error) => {
            return state.reject_unauthenticated(&error.to_string(), request_head, &body);
        }
    };
    let (client_name, client) = match identify(
        &state.clients,
        Service::Gnosis,
        responder.client_public_key(),
        ip_client_name_option.as_ref(),
    ) {
        Ok(identified) => identified,
        Err(error) => {
            return state.reject_unauthenticated(&error.to_string(), request_head, &body);
        }
    };

    // The gnosis id is `None` when the payload did not parse.
    let decision = match responder.parse_request() {
        Ok(request) => match state.decide(client, &request) {
            Ok(gnosis) => Ok((request.gnosis_id, gnosis)),
            Err(reason) => Err((Some(request.gnosis_id), reason)),
        },
        Err(error) => {
            warn!("client {client_name} sent a malformed payload: {error}");
            Err((None, DenialReason::MalformedRequest))
        }
    };
    let (response, notification) = match decision {
        Ok((gnosis_id, gnosis)) => {
            info!("granted gnosis {gnosis_id} to client {client_name}");
            let notification = Notification::Granted {
                client: client_name.clone(),
                gnosis_id,
            };
            (Response::Granted { gnosis }, notification)
        }
        Err((gnosis_id_option, reason)) => {
            let reason = reason.to_string();
            match gnosis_id_option {
                Some(gnosis_id) => {
                    warn!("denied gnosis {gnosis_id} to client {client_name}: {reason}");
                }
                None => warn!("denied an unparsable request of client {client_name}: {reason}"),
            }
            let notification = Notification::Denied {
                client: client_name.clone(),
                gnosis_id: gnosis_id_option,
                reason: reason.clone(),
            };
            (Response::Denied { reason }, notification)
        }
    };

    // A small JSON payload in Noise message 2 cannot fail to encrypt, so a failure is a bug.
    let second_message = responder
        .write_response(&response)
        .expect("a Gnosis response always encrypts");
    // The push goes out only once the response is on its way.
    enqueue(&state.notification_sender, notification);
    (
        [(header::CONTENT_TYPE, "application/octet-stream")],
        second_message,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request as HttpRequest};
    use secrecy::ExposeSecret;
    use serde_json::{from_slice, json};
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use tower::ServiceExt;

    use super::*;
    use crate::clients::ClientName;
    use crate::clients::tests::test_client;
    use crate::config::DEFAULT_MAX_CLOCK_SKEW_SECONDS;
    use layline::gnosis::binance::BinanceApiKey;
    use layline::gnosis::{GNOSIS_PROLOGUE, GnosisId, Initiator};
    use layline::noise::{build_initiator, decrypt, encrypt};
    use layline::testing::test_keypair;
    use layline::time::get_current_timestamp_nanoseconds;

    const CLIENT_NAME: &str = "Osaka";
    const OTHER_CLIENT_NAME: &str = "Tokyo";
    /// The one gnosis the client may fetch.
    const ALLOWED_GNOSIS_ID: GnosisId = GnosisId::BinanceDemo;
    /// Not in the client's `allowed_gnosis_ids`.
    const NOT_ALLOWED_GNOSIS_ID: GnosisId = GnosisId::BinanceReal;
    const API_KEY: &str = "天上永恒的王座到来";
    const SECRET_KEY: &str = "世界为之焕然一新";
    /// Arbitrary documentation address (RFC 5737) with no business meaning.
    const CLIENT_IPV4: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);

    struct Harness {
        app: Router,
        client_private_key: SecretBox<StaticKey>,
        gnosis_public_key: SecretBox<StaticKey>,
        notification_receiver: UnboundedReceiver<Notification>,
    }

    /// Phanes holds both gnoses; the client may fetch only `ALLOWED_GNOSIS_ID`.
    fn gnoses() -> Gnoses {
        toml::from_str(&format!(
            "[{ALLOWED_GNOSIS_ID}]\napi_key = \"{API_KEY}\"\nsecret_key = \"{SECRET_KEY}\"\n\
             [{NOT_ALLOWED_GNOSIS_ID}]\napi_key = \"never\"\nsecret_key = \"released\"\n"
        ))
        .expect("the gnoses parse")
    }

    fn harness() -> Harness {
        let (gnosis_private_key, gnosis_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let client = ClientConfig {
            client_gnosis_service_public_key: client_public_key,
            allowed_gnosis_ids: vec![ALLOWED_GNOSIS_ID],
            ..test_client(CLIENT_IPV4)
        };
        let clients = Clients::from([(ClientName::from(CLIENT_NAME), client)]);
        let (notification_sender, notification_receiver) = unbounded_channel();
        let state = GnosisState::new(
            gnosis_private_key,
            Arc::new(clients),
            gnoses(),
            DEFAULT_MAX_CLOCK_SKEW_SECONDS,
            notification_sender,
        );
        Harness {
            app: router(Arc::new(state)),
            client_private_key,
            gnosis_public_key,
            notification_receiver,
        }
    }

    /// Posts `body` as if the IP filter had found `ip_client_name_option` (`None`: no filter).
    async fn post(
        app: &Router,
        body: Vec<u8>,
        ip_client_name_option: Option<&str>,
    ) -> (StatusCode, Vec<u8>) {
        let mut request = HttpRequest::post(GNOSIS_PATH)
            .body(Body::from(body))
            .expect("http request is valid");
        if let Some(ip_client_name) = ip_client_name_option {
            request
                .extensions_mut()
                .insert(ClientName::from(ip_client_name));
        }
        let response = app
            .clone()
            .oneshot(request)
            .await
            .expect("router is infallible");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body is readable");
        (status, bytes.to_vec())
    }

    impl Harness {
        async fn gnosis(&self, request: &Request) -> Response<BinanceApiKey> {
            let (initiator, first_message) = Initiator::write_request(
                &self.client_private_key,
                &self.gnosis_public_key,
                request,
            )
            .expect("client writes request");
            let (status, body) = post(&self.app, first_message, None).await;
            assert_eq!(status, StatusCode::OK);
            initiator
                .read_response(&body)
                .expect("client decrypts response")
        }

        fn next_notification(&mut self) -> Notification {
            self.notification_receiver
                .try_recv()
                .expect("a notification was queued")
        }

        /// Message 1 for `ALLOWED_GNOSIS_ID`, signed with `client_private_key`.
        fn first_message(&self, client_private_key: &SecretBox<StaticKey>) -> Vec<u8> {
            let request = Request::new(ALLOWED_GNOSIS_ID, get_current_timestamp_nanoseconds());
            let (_, first_message) =
                Initiator::write_request(client_private_key, &self.gnosis_public_key, &request)
                    .expect("client writes request");
            first_message
        }

        /// Expects an empty 404 and one unauthenticated push carrying `first_message`.
        async fn expect_unauthenticated(
            &mut self,
            first_message: Vec<u8>,
            ip_client_name_option: Option<&str>,
        ) {
            let (status, body) =
                post(&self.app, first_message.clone(), ip_client_name_option).await;
            assert_eq!((status, body.is_empty()), (StatusCode::NOT_FOUND, true));
            let Notification::Unauthenticated {
                service,
                request,
                suppressed,
            } = self.next_notification()
            else {
                panic!("expected an unauthenticated notification");
            };
            assert_eq!((service, suppressed), (Service::Gnosis, 0));
            assert_eq!(request.head.method, Method::POST);
            assert_eq!(request.head.uri.path(), GNOSIS_PATH);
            assert_eq!(request.payload.as_ref(), first_message.as_slice());
        }
    }

    fn assert_denied(response: Response<BinanceApiKey>, expected: &str) {
        match response {
            Response::Denied { reason } => assert!(
                reason.contains(expected),
                "reason {reason:?} should contain {expected:?}"
            ),
            Response::Granted { .. } => panic!("expected denial containing {expected:?}"),
        }
    }

    #[tokio::test]
    async fn grants_allowed_gnosis_and_notifies() {
        let mut harness = harness();
        match harness
            .gnosis(&Request::new(
                ALLOWED_GNOSIS_ID,
                get_current_timestamp_nanoseconds(),
            ))
            .await
        {
            Response::Granted { gnosis } => {
                assert_eq!(API_KEY, gnosis.api_key().expose_secret());
                assert_eq!(SECRET_KEY, gnosis.secret_key().expose_secret());
            }
            Response::Denied { reason } => panic!("unexpected denial: {reason}"),
        }
        assert_eq!(
            harness.next_notification(),
            Notification::Granted {
                client: CLIENT_NAME.into(),
                gnosis_id: ALLOWED_GNOSIS_ID,
            }
        );
    }

    #[tokio::test]
    async fn denies_gnosis_the_client_is_not_allowed() {
        let mut harness = harness();
        assert_denied(
            harness
                .gnosis(&Request::new(
                    NOT_ALLOWED_GNOSIS_ID,
                    get_current_timestamp_nanoseconds(),
                ))
                .await,
            "not allowed",
        );
        assert_eq!(
            harness.next_notification(),
            Notification::Denied {
                client: CLIENT_NAME.into(),
                gnosis_id: Some(NOT_ALLOWED_GNOSIS_ID),
                reason: DenialReason::NotAllowed.to_string(),
            }
        );
    }

    /// `Request` cannot hold an unknown id, so this writes the payload by hand.
    #[tokio::test]
    async fn unknown_gnosis_id_is_a_malformed_request() {
        let mut harness = harness();
        let mut state = build_initiator(
            GNOSIS_PROLOGUE,
            &harness.client_private_key,
            &harness.gnosis_public_key,
        )
        .expect("client builds handshake");
        let payload = json!({
            "gnosis_id": "../binance_demo",
            "timestamp": since_unix_epoch().as_secs(),
            "nonce": "00".repeat(NONCE_LENGTH),
        });
        let first_message =
            encrypt(&mut state, payload.to_string().as_bytes()).expect("client encrypts");
        let (status, body) = post(&harness.app, first_message, None).await;
        assert_eq!(status, StatusCode::OK);
        let response: Response<BinanceApiKey> = from_slice(
            decrypt(&mut state, &body)
                .expect("client decrypts response")
                .expose_secret(),
        )
        .expect("response parses");
        assert_denied(response, "malformed request payload");
        assert_eq!(
            harness.next_notification(),
            Notification::Denied {
                client: CLIENT_NAME.into(),
                gnosis_id: None,
                reason: DenialReason::MalformedRequest.to_string(),
            }
        );
    }

    #[tokio::test]
    async fn denies_replayed_nonce() {
        let harness = harness();
        let request = Request::new(ALLOWED_GNOSIS_ID, get_current_timestamp_nanoseconds());
        assert!(matches!(
            harness.gnosis(&request).await,
            Response::Granted { .. }
        ));
        assert_denied(harness.gnosis(&request).await, "replayed");
    }

    #[tokio::test]
    async fn denies_stale_timestamp() {
        let harness = harness();
        let mut request = Request::new(ALLOWED_GNOSIS_ID, get_current_timestamp_nanoseconds());
        // One second past the allowed skew.
        request.timestamp = since_unix_epoch().as_secs() - (DEFAULT_MAX_CLOCK_SKEW_SECONDS + 1);
        assert_denied(harness.gnosis(&request).await, "clock skew");
    }

    #[tokio::test]
    async fn denies_malformed_nonce() {
        let harness = harness();
        let mut request = Request::new(ALLOWED_GNOSIS_ID, get_current_timestamp_nanoseconds());
        request.nonce = "abc".into();
        assert_denied(harness.gnosis(&request).await, "malformed nonce");
    }

    #[tokio::test]
    async fn key_of_no_client_gets_empty_404_and_throttled_notification() {
        let mut harness = harness();
        let (stranger_private_key, _) = test_keypair();
        let first_message = harness.first_message(&stranger_private_key);
        harness.expect_unauthenticated(first_message, None).await;
        for _ in 0..2 {
            let first_message = harness.first_message(&stranger_private_key);
            let (status, _) = post(&harness.app, first_message, None).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
        assert!(harness.notification_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn junk_gets_empty_404_and_a_notification() {
        let mut harness = harness();
        harness.expect_unauthenticated(b"junk".to_vec(), None).await;
    }

    #[tokio::test]
    async fn key_sent_from_another_clients_address_gets_empty_404() {
        let mut harness = harness();
        let first_message = harness.first_message(&harness.client_private_key);
        harness
            .expect_unauthenticated(first_message, Some(OTHER_CLIENT_NAME))
            .await;
    }

    #[tokio::test]
    async fn key_sent_from_its_own_clients_address_is_served() {
        let mut harness = harness();
        let first_message = harness.first_message(&harness.client_private_key);
        let (status, _) = post(&harness.app, first_message, Some(CLIENT_NAME)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(matches!(
            harness.next_notification(),
            Notification::Granted { .. }
        ));
    }
}

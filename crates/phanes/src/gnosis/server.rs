//! Phanes side of Gnosis: authenticates the client and releases API keys from disk.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{error, info, warn};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::post;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use tokio::fs::read_to_string;
use tokio::sync::mpsc::UnboundedSender;

use crate::client_filter::ClientName;
use crate::notify::{Notification, UnauthenticatedThrottle};
use crate::request_details::{RequestDetails, RequestHead};
use crate::service::Service;
use layline::gnosis::{GNOSIS_PATH, NONCE_LENGTH, Request, Responder, Response};
use layline::noise::StaticKey;
use layline::time::since_unix_epoch;

/// Noise message 1 carries a small JSON payload; anything bigger is not a real client.
const MAX_REQUEST_BODY_BYTES: usize = 4096;
const MAX_KEY_ID_LENGTH: usize = 64;

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
    #[error("invalid key id")]
    InvalidKeyId,
    #[error("key id not allowed for this client")]
    NotAllowed,
    #[error("key not found on server")]
    KeyNotFound,
}

pub struct GnosisState {
    pub gnosis_private_key: SecretBox<StaticKey>,
    pub client_public_key: StaticKey,
    pub allowed_key_ids: HashSet<String>,
    pub api_keys_directory: PathBuf,
    pub max_clock_skew_seconds: u64,
    pub notifications: UnboundedSender<Notification>,
    /// nonce -> unix second after which the nonce's timestamp is out of window anyway.
    seen_nonces: Mutex<HashMap<String, u64>>,
    unauthenticated_throttle: Mutex<UnauthenticatedThrottle>,
}

impl GnosisState {
    pub fn new(
        gnosis_private_key: SecretBox<StaticKey>,
        client_public_key: StaticKey,
        allowed_key_ids: HashSet<String>,
        api_keys_directory: PathBuf,
        max_clock_skew_seconds: u64,
        notifications: UnboundedSender<Notification>,
    ) -> Self {
        Self {
            gnosis_private_key,
            client_public_key,
            allowed_key_ids,
            api_keys_directory,
            max_clock_skew_seconds,
            notifications,
            seen_nonces: Mutex::default(),
            unauthenticated_throttle: Mutex::default(),
        }
    }

    fn notify(&self, notification: Notification) {
        if self.notifications.send(notification).is_err() {
            error!("notification dispatcher is gone; push dropped");
        }
    }

    fn notify_unauthenticated(&self, request: RequestDetails) {
        let suppressed = self
            .unauthenticated_throttle
            .lock()
            .expect("unauthenticated throttle mutex is not poisoned")
            .record(Instant::now());
        if let Some(suppressed) = suppressed {
            self.notify(Notification::Unauthenticated {
                service: Service::Gnosis,
                request,
                suppressed,
            });
        }
    }

    async fn decide(&self, request: &Request) -> Result<SecretString, DenialReason> {
        let now = since_unix_epoch().as_secs();
        let skew_seconds = now.abs_diff(request.timestamp);
        if skew_seconds > self.max_clock_skew_seconds {
            return Err(DenialReason::ClockSkew { skew_seconds });
        }
        self.check_and_record_nonce(&request.nonce, request.timestamp, now)?;

        if !is_valid_key_id(&request.key_id) {
            return Err(DenialReason::InvalidKeyId);
        }
        if !self.allowed_key_ids.contains(&request.key_id) {
            return Err(DenialReason::NotAllowed);
        }

        let key_id = &request.key_id;
        let path = self.api_keys_directory.join(format!("{key_id}.txt"));
        let printable_path = path.display();
        let contents = SecretString::from(read_to_string(&path).await.map_err(|error| {
            error!("cannot read key file {printable_path}: {error}");
            DenialReason::KeyNotFound
        })?);
        let secret = contents.expose_secret().trim();
        if secret.is_empty() {
            error!("key file {printable_path} is empty");
            return Err(DenialReason::KeyNotFound);
        }
        Ok(SecretString::from(secret))
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

pub fn is_valid_key_id(key_id: &str) -> bool {
    !key_id.is_empty()
        && key_id.len() <= MAX_KEY_ID_LENGTH
        && key_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

pub fn router(state: Arc<GnosisState>) -> Router {
    Router::new()
        .route(GNOSIS_PATH, post(gnosis))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

async fn gnosis(
    State(state): State<Arc<GnosisState>>,
    ClientName(client): ClientName,
    request_head: RequestHead,
    body: Bytes,
) -> HttpResponse {
    let responder =
        match Responder::read_request(&state.gnosis_private_key, &state.client_public_key, &body) {
            Ok(responder) => responder,
            Err(error) => {
                let request = RequestDetails::new(request_head, &body);
                warn!("rejected unauthenticated request from client {client}: {error}; {request}");
                state.notify_unauthenticated(request);
                return StatusCode::NOT_FOUND.into_response();
            }
        };

    let (key_id, decision) = match responder.parse_request() {
        Ok(request) => {
            let decision = state.decide(&request).await;
            (Some(request.key_id), decision)
        }
        Err(error) => {
            warn!("client {client} sent a malformed payload: {error}");
            (None, Err(DenialReason::MalformedRequest))
        }
    };

    let (response, notification) = match (decision, key_id) {
        (Ok(secret), Some(key_id)) => {
            info!("granted key {key_id} to client {client}");
            (
                Response::Granted { secret },
                Notification::Granted {
                    client: client.clone(),
                    key_id,
                },
            )
        }
        (decision, key_id) => {
            let reason = decision
                .err()
                .unwrap_or(DenialReason::MalformedRequest)
                .to_string();
            warn!("denied key {key_id:?} to client {client}: {reason}");
            (
                Response::Denied {
                    reason: reason.clone(),
                },
                Notification::Denied {
                    client: client.clone(),
                    key_id,
                    reason,
                },
            )
        }
    };

    match responder.write_response(&response) {
        Ok(second_message) => {
            state.notify(notification);
            (
                [(header::CONTENT_TYPE, "application/octet-stream")],
                second_message,
            )
                .into_response()
        }
        Err(error) => {
            error!("failed to encrypt response: {error}");
            state.notify(Notification::Denied {
                client,
                key_id: None,
                reason: format!("server failed to encrypt response: {error}"),
            });
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_filter::UNFILTERED_CLIENT_NAME;
    use crate::config::DEFAULT_MAX_CLOCK_SKEW_SECONDS;
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request as HttpRequest};
    use layline::gnosis::Initiator;
    use layline::testing::test_keypair;
    use layline::time::get_current_timestamp_nanoseconds;
    use std::fs::write;
    use tempfile::{TempDir, tempdir};
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use tower::ServiceExt;

    const KEY_ID: &str = "binance_demo";
    const SECRET: &str = "世界为之焕然一新";
    /// Has a key file but is not in the allow list.
    const NOT_ALLOWED_KEY_ID: &str = "other_key";
    const NOT_ALLOWED_SECRET: &str = "never released";
    /// In the allow list but has no key file.
    const MISSING_KEY_ID: &str = "missing_key";

    struct Harness {
        app: Router,
        client_private_key: SecretBox<StaticKey>,
        gnosis_public_key: StaticKey,
        notifications: UnboundedReceiver<Notification>,
        _api_keys_directory: TempDir,
    }

    fn harness() -> Harness {
        let (gnosis_private_key, gnosis_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let api_keys_directory = tempdir().expect("temporary directory is created");
        write(
            api_keys_directory.path().join(format!("{KEY_ID}.txt")),
            format!("{SECRET}\n"),
        )
        .expect("key file is written");
        write(
            api_keys_directory
                .path()
                .join(format!("{NOT_ALLOWED_KEY_ID}.txt")),
            NOT_ALLOWED_SECRET,
        )
        .expect("key file is written");

        let (sender, notifications) = unbounded_channel();
        let state = GnosisState::new(
            gnosis_private_key,
            client_public_key,
            [KEY_ID.to_owned(), MISSING_KEY_ID.to_owned()]
                .into_iter()
                .collect(),
            api_keys_directory.path().to_owned(),
            DEFAULT_MAX_CLOCK_SKEW_SECONDS,
            sender,
        );
        Harness {
            app: router(Arc::new(state)),
            client_private_key,
            gnosis_public_key,
            notifications,
            _api_keys_directory: api_keys_directory,
        }
    }

    async fn post(app: &Router, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
        let request = HttpRequest::post(GNOSIS_PATH)
            .body(Body::from(body))
            .expect("http request is valid");
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
        async fn gnosis(&self, request: &Request) -> Response {
            let (initiator, first_message) = Initiator::write_request(
                &self.client_private_key,
                &self.gnosis_public_key,
                request,
            )
            .expect("client writes request");
            let (status, body) = post(&self.app, first_message).await;
            assert_eq!(status, StatusCode::OK);
            initiator
                .read_response(&body)
                .expect("client decrypts response")
        }

        fn next_notification(&mut self) -> Notification {
            self.notifications
                .try_recv()
                .expect("a notification was queued")
        }
    }

    fn assert_denied(response: Response, expected: &str) {
        match response {
            Response::Denied { reason } => assert!(
                reason.contains(expected),
                "reason {reason:?} should contain {expected:?}"
            ),
            Response::Granted { .. } => panic!("expected denial containing {expected:?}"),
        }
    }

    #[tokio::test]
    async fn grants_allowed_key_and_notifies() {
        let mut harness = harness();
        match harness
            .gnosis(&Request::new(KEY_ID, get_current_timestamp_nanoseconds()))
            .await
        {
            Response::Granted { secret } => assert_eq!(secret.expose_secret(), SECRET),
            Response::Denied { reason } => panic!("unexpected denial: {reason}"),
        }
        assert_eq!(
            harness.next_notification(),
            Notification::Granted {
                client: UNFILTERED_CLIENT_NAME.into(),
                key_id: KEY_ID.into()
            }
        );
    }

    #[tokio::test]
    async fn denies_key_not_in_allow_list() {
        let mut harness = harness();
        assert_denied(
            harness
                .gnosis(&Request::new(
                    NOT_ALLOWED_KEY_ID,
                    get_current_timestamp_nanoseconds(),
                ))
                .await,
            "not allowed",
        );
        assert_eq!(
            harness.next_notification(),
            Notification::Denied {
                client: UNFILTERED_CLIENT_NAME.into(),
                key_id: Some(NOT_ALLOWED_KEY_ID.into()),
                reason: DenialReason::NotAllowed.to_string(),
            }
        );
    }

    #[tokio::test]
    async fn denies_missing_key_file() {
        let harness = harness();
        assert_denied(
            harness
                .gnosis(&Request::new(
                    MISSING_KEY_ID,
                    get_current_timestamp_nanoseconds(),
                ))
                .await,
            "not found",
        );
    }

    #[tokio::test]
    async fn denies_path_traversal_key_id() {
        let harness = harness();
        assert_denied(
            harness
                .gnosis(&Request::new(
                    "../binance_demo_trading",
                    get_current_timestamp_nanoseconds(),
                ))
                .await,
            "invalid key id",
        );
    }

    #[tokio::test]
    async fn denies_replayed_nonce() {
        let harness = harness();
        let request = Request::new(KEY_ID, get_current_timestamp_nanoseconds());
        assert!(matches!(
            harness.gnosis(&request).await,
            Response::Granted { .. }
        ));
        assert_denied(harness.gnosis(&request).await, "replayed");
    }

    #[tokio::test]
    async fn denies_stale_timestamp() {
        let harness = harness();
        let mut request = Request::new(KEY_ID, get_current_timestamp_nanoseconds());
        // One second past the allowed skew.
        request.timestamp = since_unix_epoch().as_secs() - (DEFAULT_MAX_CLOCK_SKEW_SECONDS + 1);
        assert_denied(harness.gnosis(&request).await, "clock skew");
    }

    #[tokio::test]
    async fn denies_malformed_nonce() {
        let harness = harness();
        let mut request = Request::new(KEY_ID, get_current_timestamp_nanoseconds());
        request.nonce = "abc".into();
        assert_denied(harness.gnosis(&request).await, "malformed nonce");
    }

    #[tokio::test]
    async fn unauthenticated_request_gets_empty_404_and_throttled_notification() {
        let mut harness = harness();
        let (impostor_private_key, _) = test_keypair();
        let mut first_messages = Vec::new();
        for _ in 0..3 {
            let (_, first_message) = Initiator::write_request(
                &impostor_private_key,
                &harness.gnosis_public_key,
                &Request::new(KEY_ID, get_current_timestamp_nanoseconds()),
            )
            .expect("impostor writes request");
            first_messages.push(first_message.clone());
            let (status, body) = post(&harness.app, first_message).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert!(body.is_empty());
        }
        let Notification::Unauthenticated {
            service,
            request,
            suppressed,
        } = harness.next_notification()
        else {
            panic!("expected an unauthenticated notification");
        };
        assert_eq!((service, suppressed), (Service::Gnosis, 0));
        assert_eq!(request.head.method, Method::POST);
        assert_eq!(request.head.uri.path(), GNOSIS_PATH);
        assert_eq!(request.payload.as_ref(), first_messages[0].as_slice());
        assert!(harness.notifications.try_recv().is_err());
    }

    #[test]
    fn key_id_validation() {
        assert!(is_valid_key_id("binance_demo-trading2"));
        assert!(!is_valid_key_id(""));
        assert!(!is_valid_key_id("a/b"));
        assert!(!is_valid_key_id("a.txt"));
        assert!(!is_valid_key_id(&"a".repeat(MAX_KEY_ID_LENGTH + 1)));
    }
}

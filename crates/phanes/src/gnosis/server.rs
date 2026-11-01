//! Phanes side of Gnosis: authenticates the client and releases API key files from disk.

use std::collections::{HashMap, HashSet};
use std::fs::read_to_string as read_file_to_string;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::post;
use secrecy::{SecretBox, SecretString};
use tokio::fs::read_to_string;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info, warn};

use crate::client_filter::ClientName;
use crate::notify::{Notification, UnauthenticatedThrottle};
use crate::request_details::{RequestDetails, RequestHead};
use crate::service::Service;
use layline::api_keys::{ApiKeyFormatError, ApiKeyId};
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
    #[error("key id not allowed for this client")]
    NotAllowed,
    #[error("key not found on server")]
    KeyNotFound,
}

/// Why an allowed key's file fails the startup check. Never quotes the file content.
#[derive(Debug, thiserror::Error)]
pub enum KeyFileError {
    #[error("cannot read key file {}: {source}", path.display())]
    Unreadable { path: PathBuf, source: IoError },
    #[error("key file {} does not match its key id: {source}", path.display())]
    Mismatched {
        path: PathBuf,
        source: ApiKeyFormatError,
    },
}

/// `<gnoses_directory>/<key_id>.json`.
pub fn api_key_file_path(gnoses_directory: &Path, key_id: ApiKeyId) -> PathBuf {
    gnoses_directory.join(key_id.file_name())
}

/// Startup check of an allowed key's file: it exists and holds the fields of `key_id`'s type.
pub fn check_api_key_file(gnoses_directory: &Path, key_id: ApiKeyId) -> Result<(), KeyFileError> {
    let path = api_key_file_path(gnoses_directory, key_id);
    let key_file = SecretString::from(read_file_to_string(&path).map_err(|source| {
        KeyFileError::Unreadable {
            path: path.clone(),
            source,
        }
    })?);
    key_id
        .check_key_file(&key_file)
        .map_err(|source| KeyFileError::Mismatched { path, source })
}

pub struct GnosisState {
    pub gnosis_private_key: SecretBox<StaticKey>,
    pub client_public_key: StaticKey,
    pub allowed_key_ids: HashSet<ApiKeyId>,
    pub gnoses_directory: PathBuf,
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
        allowed_key_ids: HashSet<ApiKeyId>,
        gnoses_directory: PathBuf,
        max_clock_skew_seconds: u64,
        notifications: UnboundedSender<Notification>,
    ) -> Self {
        Self {
            gnosis_private_key,
            client_public_key,
            allowed_key_ids,
            gnoses_directory,
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

        if !self.allowed_key_ids.contains(&request.key_id) {
            return Err(DenialReason::NotAllowed);
        }

        let path = api_key_file_path(&self.gnoses_directory, request.key_id);
        let key_file = read_to_string(&path).await.map_err(|error| {
            error!("cannot read key file {}: {error}", path.display());
            DenialReason::KeyNotFound
        })?;
        Ok(SecretString::from(key_file))
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
    use std::fs::write;

    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request as HttpRequest};
    use secrecy::ExposeSecret;
    use serde_json::{from_slice, json};
    use tempfile::{TempDir, tempdir};
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use tower::ServiceExt;

    use super::*;
    use crate::client_filter::UNFILTERED_CLIENT_NAME;
    use crate::config::DEFAULT_MAX_CLOCK_SKEW_SECONDS;
    use layline::gnosis::{GNOSIS_PROLOGUE, Initiator};
    use layline::noise::{build_handshake, decrypt, encrypt};
    use layline::testing::test_keypair;
    use layline::time::get_current_timestamp_nanoseconds;

    const KEY_ID: ApiKeyId = ApiKeyId::BinanceDemo;
    /// Served byte for byte, whitespace included.
    const KEY_FILE: &str = "{\n    \"api_key\": \"天上永恒的王座到来\",\n    \"secret_key\": \"世界为之焕然一新\"\n}\n";
    /// Has a key file but is not in the allow list.
    const NOT_ALLOWED_KEY_ID: ApiKeyId = ApiKeyId::BinanceReal;
    const NOT_ALLOWED_KEY_FILE: &str = r#"{"api_key": "never", "secret_key": "released"}"#;
    /// In the allow list but has no key file.
    const MISSING_KEY_ID: ApiKeyId = ApiKeyId::BinanceSbeDemo;

    struct Harness {
        app: Router,
        client_private_key: SecretBox<StaticKey>,
        gnosis_public_key: StaticKey,
        notifications: UnboundedReceiver<Notification>,
        _gnoses_directory: TempDir,
    }

    fn harness() -> Harness {
        let (gnosis_private_key, gnosis_public_key) = test_keypair();
        let (client_private_key, client_public_key) = test_keypair();
        let gnoses_directory = tempdir().expect("temporary directory is created");
        for (key_id, key_file) in [
            (KEY_ID, KEY_FILE),
            (NOT_ALLOWED_KEY_ID, NOT_ALLOWED_KEY_FILE),
        ] {
            write(api_key_file_path(gnoses_directory.path(), key_id), key_file)
                .expect("key file is written");
        }

        let (sender, notifications) = unbounded_channel();
        let state = GnosisState::new(
            gnosis_private_key,
            client_public_key,
            HashSet::from([KEY_ID, MISSING_KEY_ID]),
            gnoses_directory.path().to_owned(),
            DEFAULT_MAX_CLOCK_SKEW_SECONDS,
            sender,
        );
        Harness {
            app: router(Arc::new(state)),
            client_private_key,
            gnosis_public_key,
            notifications,
            _gnoses_directory: gnoses_directory,
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
            Response::Granted { secret } => assert_eq!(secret.expose_secret(), KEY_FILE),
            Response::Denied { reason } => panic!("unexpected denial: {reason}"),
        }
        assert_eq!(
            harness.next_notification(),
            Notification::Granted {
                client: UNFILTERED_CLIENT_NAME.into(),
                key_id: KEY_ID
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
                key_id: Some(NOT_ALLOWED_KEY_ID),
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

    #[test]
    fn startup_check_wants_the_fields_of_the_key_id_and_never_quotes_the_file() {
        let directory = tempdir().expect("temporary directory is created");
        let path = api_key_file_path(directory.path(), KEY_ID);
        assert_eq!(directory.path().join("binance_demo.json"), path);
        assert!(matches!(
            check_api_key_file(directory.path(), KEY_ID),
            Err(KeyFileError::Unreadable { .. })
        ));

        const PLAIN_TEXT: &str = "secret written as plain text";
        const SBE_KEY_FILE: &str = r#"{"api_key": "only the SBE field"}"#;
        for mismatched in [PLAIN_TEXT, "{}", SBE_KEY_FILE] {
            write(&path, mismatched).expect("key file is written");
            let error = check_api_key_file(directory.path(), KEY_ID)
                .expect_err("the file lacks the fields of a Binance trading key");
            assert!(matches!(error, KeyFileError::Mismatched { .. }));
            let message = error.to_string();
            assert!(message.contains("binance_demo.json"), "{message}");
            assert!(!message.contains(mismatched), "{message}");
        }

        write(&path, KEY_FILE).expect("key file is written");
        check_api_key_file(directory.path(), KEY_ID).expect("the fields match");
    }

    /// `Request` cannot hold an unknown id, so this writes the payload by hand.
    #[tokio::test]
    async fn unknown_key_id_is_a_malformed_request() {
        let mut harness = harness();
        let mut state = build_handshake(
            GNOSIS_PROLOGUE,
            &harness.client_private_key,
            &harness.gnosis_public_key,
            true,
        )
        .expect("client builds handshake");
        let payload = json!({
            "key_id": "../binance_demo",
            "timestamp": since_unix_epoch().as_secs(),
            "nonce": "00".repeat(NONCE_LENGTH),
        });
        let first_message =
            encrypt(&mut state, payload.to_string().as_bytes()).expect("client encrypts");
        let (status, body) = post(&harness.app, first_message).await;
        assert_eq!(status, StatusCode::OK);
        let response: Response = from_slice(
            decrypt(&mut state, &body)
                .expect("client decrypts response")
                .expose_secret(),
        )
        .expect("response parses");
        assert_denied(response, "malformed request payload");
        assert_eq!(
            harness.next_notification(),
            Notification::Denied {
                client: UNFILTERED_CLIENT_NAME.into(),
                key_id: None,
                reason: DenialReason::MalformedRequest.to_string(),
            }
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
}

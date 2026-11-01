//! Client of Phanes Gnosis, which releases API keys over an end-to-end encrypted HTTP round trip.

use std::panic::panic_any;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode};
use secrecy::{SecretBox, SecretString};
use serde::Deserialize;
use tracing::{error, info};
use url::Url;

use crate::alert::SendAlert;
use crate::exit_reason::ExitReason;
use crate::utils::{Clock, RealClock};
use layline::gnosis::{GNOSIS_PATH, Initiator, Request, Response};
use layline::noise::{StaticKey, decode_private_key, decode_public_key};
use layline::secrets::read_secret_file;

const GNOSIS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The `[phanes]` table of the config. Like Phanes itself, each service has its own key pair.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhanesClientConfig {
    url: Url,
    /// Base64 X25519 private key this client authenticates to Gnosis with, from `phanes keygen`; chmod 600.
    gnosis_private_key_file: PathBuf,
    /// Base64 X25519 public key of Phanes Gnosis.
    gnosis_public_key: String,
}

/// Where API keys come from; the connector asks only when a topic needs one.
pub(crate) trait ApiKeySource {
    async fn fetch_api_key(&self, key_id: &str) -> Result<SecretString>;
}

/// Failing to get an API key is an unrecoverable startup error: alerts the phone, then panics with
/// `ExitReason::ApiKeyUnavailable`, which the binary's panic hook turns into the exit code.
pub(crate) async fn fetch_api_key_or_panic(
    source: &impl ApiKeySource,
    alert: &impl SendAlert,
    key_id: &str,
) -> SecretString {
    match source.fetch_api_key(key_id).await {
        Ok(api_key) => {
            info!("fetched API key {key_id} from Phanes");
            api_key
        }
        Err(error) => {
            let reason = format!("failed to fetch API key {key_id} from Phanes: {error:#}");
            error!("{reason}");
            alert
                .send_alert(
                    &format!("Market Data Provider Cannot Query API key {key_id}"),
                    &reason,
                )
                .await;
            panic_any(ExitReason::ApiKeyUnavailable);
        }
    }
}

/// The one HTTP round trip of Gnosis, so tests can answer it in process.
pub(crate) trait GnosisHttp {
    async fn post(&self, url: Url, body: Vec<u8>) -> Result<(StatusCode, Vec<u8>)>;
}

#[derive(Clone)]
pub(crate) struct ReqwestGnosisHttp {
    client: Client,
}

impl GnosisHttp for ReqwestGnosisHttp {
    async fn post(&self, url: Url, body: Vec<u8>) -> Result<(StatusCode, Vec<u8>)> {
        let response = self.client.post(url).body(body).send().await?;
        let status = response.status();
        Ok((status, response.bytes().await?.to_vec()))
    }
}

/// Clones share the private key, so every exchange worker can hold one.
#[derive(Clone)]
pub(crate) struct PhanesClient<Http = ReqwestGnosisHttp, TimeSource = RealClock> {
    url: Url,
    gnosis_private_key: Arc<SecretBox<StaticKey>>,
    gnosis_public_key: StaticKey,
    http: Http,
    clock: TimeSource,
}

impl PhanesClient {
    /// Reads the private key and checks both keys, so a bad `[phanes]` table stops the provider at startup.
    pub(crate) fn new(config: &PhanesClientConfig) -> Result<Self> {
        let key_file = &config.gnosis_private_key_file;
        let encoded_private_key = read_secret_file(key_file)?;
        let gnosis_private_key = decode_private_key(&encoded_private_key)
            .with_context(|| format!("invalid private key in {}", key_file.display()))?;
        let gnosis_public_key = decode_public_key(&config.gnosis_public_key)
            .context("invalid phanes.gnosis_public_key")?;
        let client = Client::builder().timeout(GNOSIS_REQUEST_TIMEOUT).build()?;
        Ok(Self {
            url: config.url.clone(),
            gnosis_private_key: Arc::new(gnosis_private_key),
            gnosis_public_key,
            http: ReqwestGnosisHttp { client },
            clock: RealClock,
        })
    }
}

impl<Http: GnosisHttp, TimeSource: Clock> ApiKeySource for PhanesClient<Http, TimeSource> {
    async fn fetch_api_key(&self, key_id: &str) -> Result<SecretString> {
        let (initiator, first_message) = Initiator::write_request(
            &self.gnosis_private_key,
            &self.gnosis_public_key,
            &Request::new(key_id, self.clock.now()),
        )?;
        let (status, second_message) = self
            .http
            .post(self.url.join(GNOSIS_PATH)?, first_message)
            .await?;
        if !status.is_success() {
            bail!("Phanes answered {status}");
        }
        match initiator.read_response(&second_message)? {
            Response::Granted { secret } => Ok(secret),
            Response::Denied { reason } => bail!("Phanes denied it: {reason}"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs::{Permissions, set_permissions, write};
    use std::num::NonZeroU64;
    use std::os::unix::fs::PermissionsExt;
    use std::panic::AssertUnwindSafe;
    use std::path::Path;

    use futures::FutureExt;
    use secrecy::ExposeSecret;
    use tempfile::tempdir;

    use super::*;
    use crate::alert::tests::RecordingAlert;
    use crate::utils::tests::MockClock;
    use layline::gnosis::Responder;
    use layline::noise::encode_key;
    use layline::testing::test_keypair;

    const PHANES_URL: &str = "https://phanes.example";
    const OWNER_ONLY_MODE: u32 = 0o600;
    const WORLD_READABLE_MODE: u32 = 0o644;
    const GRANTED_KEY_ID: &str = "granted_key";
    const GRANTED_SECRET: &str = "dummy-granted-secret";
    const DENIED_KEY_ID: &str = "denied_key";
    const DENIAL_REASON: &str = "key not allowed";
    /// 2026-10-24 00:00:00 UTC.
    const NOW_NANOSECONDS: NonZeroU64 =
        NonZeroU64::new(1_792_800_000_000_000_000).expect("not zero");

    /// For code that needs a production `PhanesClient` but never fetches a key.
    pub(crate) fn unreachable_phanes_client() -> PhanesClient {
        let (gnosis_private_key, _) = test_keypair();
        let (_, gnosis_public_key) = test_keypair();
        PhanesClient {
            url: Url::parse("https://phanes.invalid").expect("the test URL is valid"),
            gnosis_private_key: Arc::new(gnosis_private_key),
            gnosis_public_key,
            http: ReqwestGnosisHttp {
                client: Client::new(),
            },
            clock: RealClock,
        }
    }

    /// Grants the keys in `grants`, denies the rest, and records every key id asked for.
    #[derive(Default)]
    pub(crate) struct MockApiKeySource {
        pub(crate) grants: HashMap<String, String>,
        pub(crate) offline: bool,
        pub(crate) requested_key_ids: RefCell<Vec<String>>,
    }

    impl ApiKeySource for MockApiKeySource {
        async fn fetch_api_key(&self, key_id: &str) -> Result<SecretString> {
            self.requested_key_ids.borrow_mut().push(key_id.to_owned());
            if self.offline {
                bail!("Phanes is unreachable");
            }
            match self.grants.get(key_id) {
                Some(secret) => Ok(SecretString::from(secret.as_str())),
                None => bail!("Phanes denied it: {DENIAL_REASON}"),
            }
        }
    }

    #[derive(Clone, Copy, PartialEq)]
    enum PhanesBehaviour {
        Normal,
        Offline,
        /// Fails every handshake, as for a client whose public key Phanes does not hold.
        ForgetClient,
        Garbage,
    }

    /// Phanes' end of the HTTP round trip, answered in process with the server half of the protocol.
    struct GnosisHttpMock {
        gnosis_private_key: SecretBox<StaticKey>,
        client_public_key: StaticKey,
        grants: HashMap<String, String>,
        behaviour: PhanesBehaviour,
        requests: RefCell<Vec<Request>>,
    }

    impl GnosisHttp for GnosisHttpMock {
        async fn post(&self, url: Url, body: Vec<u8>) -> Result<(StatusCode, Vec<u8>)> {
            if self.behaviour == PhanesBehaviour::Offline {
                bail!("Phanes is unreachable");
            }
            if url.path() != GNOSIS_PATH {
                return Ok((StatusCode::NOT_FOUND, Vec::new()));
            }
            let client_public_key = match self.behaviour {
                PhanesBehaviour::ForgetClient => test_keypair().1,
                _ => self.client_public_key,
            };
            let Ok(responder) =
                Responder::read_request(&self.gnosis_private_key, &client_public_key, &body)
            else {
                return Ok((StatusCode::NOT_FOUND, Vec::new()));
            };
            let request = responder.parse_request()?;
            let response = match self.grants.get(&request.key_id) {
                Some(secret) => Response::Granted {
                    secret: SecretString::from(secret.as_str()),
                },
                None => Response::Denied {
                    reason: DENIAL_REASON.to_owned(),
                },
            };
            self.requests.borrow_mut().push(request);
            if self.behaviour == PhanesBehaviour::Garbage {
                return Ok((StatusCode::OK, b"garbage".to_vec()));
            }
            Ok((StatusCode::OK, responder.write_response(&response)?))
        }
    }

    /// A client wired to an in-process Phanes that grants `GRANTED_KEY_ID`, and a mock clock.
    struct PhanesFixture {
        client: PhanesClient<GnosisHttpMock, MockClock>,
        clock: MockClock,
    }

    impl PhanesFixture {
        fn new() -> Self {
            let (gnosis_private_key, gnosis_public_key) = test_keypair();
            let (client_private_key, client_public_key) = test_keypair();
            let clock = MockClock::new(NOW_NANOSECONDS);
            let phanes = GnosisHttpMock {
                gnosis_private_key,
                client_public_key,
                grants: HashMap::from([(GRANTED_KEY_ID.to_owned(), GRANTED_SECRET.to_owned())]),
                behaviour: PhanesBehaviour::Normal,
                requests: RefCell::new(Vec::new()),
            };
            let client = PhanesClient {
                url: Url::parse(PHANES_URL).expect("PHANES_URL is a valid URL"),
                gnosis_private_key: Arc::new(client_private_key),
                gnosis_public_key,
                http: phanes,
                clock: clock.clone(),
            };
            Self { client, clock }
        }

        fn phanes_behaves(&mut self, behaviour: PhanesBehaviour) {
            self.client.http.behaviour = behaviour;
        }

        fn expect_phanes_received(&self, key_ids: &[&str]) -> Vec<Request> {
            let requests = self.client.http.requests.borrow().clone();
            let received: Vec<&str> = requests
                .iter()
                .map(|request| request.key_id.as_str())
                .collect();
            assert_eq!(key_ids, received);
            requests
        }

        async fn expect_error(&self, key_id: &str, expected: &str) {
            let error = self
                .client
                .fetch_api_key(key_id)
                .await
                .expect_err("the fetch is expected to fail");
            let message = format!("{error:#}");
            assert!(message.contains(expected), "{message}");
        }
    }

    #[tokio::test]
    async fn granted_key_is_returned_and_request_carries_clock_time() {
        let fixture = PhanesFixture::new();
        fixture.clock.advance(Duration::from_secs(5));
        let api_key = fixture
            .client
            .fetch_api_key(GRANTED_KEY_ID)
            .await
            .expect("Phanes is expected to grant the key");
        assert_eq!(GRANTED_SECRET, api_key.expose_secret());
        let requests = fixture.expect_phanes_received(&[GRANTED_KEY_ID]);
        assert_eq!(1_792_800_005, requests[0].timestamp);
    }

    #[tokio::test]
    async fn denied_key_is_an_error() {
        PhanesFixture::new()
            .expect_error(DENIED_KEY_ID, DENIAL_REASON)
            .await;
    }

    #[tokio::test]
    async fn unknown_client_gets_not_found() {
        let mut fixture = PhanesFixture::new();
        fixture.phanes_behaves(PhanesBehaviour::ForgetClient);
        fixture.expect_error(GRANTED_KEY_ID, "404").await;
        fixture.expect_phanes_received(&[]);
    }

    #[tokio::test]
    async fn unreachable_phanes_is_an_error() {
        let mut fixture = PhanesFixture::new();
        fixture.phanes_behaves(PhanesBehaviour::Offline);
        fixture.expect_error(GRANTED_KEY_ID, "unreachable").await;
    }

    #[tokio::test]
    async fn garbled_reply_is_rejected() {
        let mut fixture = PhanesFixture::new();
        fixture.phanes_behaves(PhanesBehaviour::Garbage);
        fixture.expect_error(GRANTED_KEY_ID, "noise").await;
    }

    #[tokio::test]
    async fn fetch_or_panic_returns_key_without_alerting() {
        let key_source = MockApiKeySource {
            grants: HashMap::from([(GRANTED_KEY_ID.to_owned(), GRANTED_SECRET.to_owned())]),
            ..MockApiKeySource::default()
        };
        let alert = RecordingAlert::default();
        let api_key = fetch_api_key_or_panic(&key_source, &alert, GRANTED_KEY_ID).await;
        assert_eq!(GRANTED_SECRET, api_key.expose_secret());
        assert!(alert.alerts().is_empty());
    }

    #[tokio::test]
    async fn fetch_or_panic_alerts_the_key_id_then_panics_with_its_exit_reason() {
        let key_source = MockApiKeySource::default();
        let alert = RecordingAlert::default();
        let payload = AssertUnwindSafe(fetch_api_key_or_panic(&key_source, &alert, DENIED_KEY_ID))
            .catch_unwind()
            .await
            .expect_err("a denied key is expected to panic");
        assert_eq!(
            Some(&ExitReason::ApiKeyUnavailable),
            payload.downcast_ref::<ExitReason>()
        );
        let alerts = alert.alerts();
        assert_eq!(1, alerts.len());
        assert!(alerts[0].starts_with(&format!(
            "Market Data Provider Cannot Query API key {DENIED_KEY_ID}"
        )));
        assert!(alerts[0].contains(DENIAL_REASON));
    }

    /// Writes a chmod 600 private key and returns the `[phanes]` table that points at it.
    fn config_with_key_file(directory: &Path, gnosis_public_key: &str) -> PhanesClientConfig {
        let key_file = directory.join("gnosis.key");
        let (private_key, _) = test_keypair();
        write(&key_file, encode_key(private_key.expose_secret()))
            .expect("writing the key file is expected to not fail");
        set_permissions(&key_file, Permissions::from_mode(OWNER_ONLY_MODE))
            .expect("setting permissions is expected to not fail");
        toml::from_str(&format!(
            "url = \"{PHANES_URL}\"\ngnosis_private_key_file = \"{}\"\ngnosis_public_key = \"{gnosis_public_key}\"",
            key_file.display()
        ))
        .expect("the [phanes] table is expected to parse")
    }

    #[test]
    fn client_reads_its_key_file_and_checks_both_keys() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let (_, gnosis_public_key) = test_keypair();
        let config = config_with_key_file(directory.path(), &encode_key(&gnosis_public_key));
        let client = PhanesClient::new(&config).expect("the config is valid");
        assert_eq!(gnosis_public_key, client.gnosis_public_key);

        let invalid_public_key = config_with_key_file(directory.path(), "not base64!");
        assert!(PhanesClient::new(&invalid_public_key).is_err());

        set_permissions(
            &config.gnosis_private_key_file,
            Permissions::from_mode(WORLD_READABLE_MODE),
        )
        .expect("setting permissions is expected to not fail");
        let error = PhanesClient::new(&config)
            .err()
            .expect("a world-readable key file is expected to be refused");
        assert!(error.to_string().contains("chmod 600"), "{error}");
    }

    #[test]
    fn missing_key_file_is_refused() {
        let config: PhanesClientConfig = toml::from_str(&format!(
            "url = \"{PHANES_URL}\"\ngnosis_private_key_file = \"/nonexistent/gnosis.key\"\ngnosis_public_key = \"\""
        ))
        .expect("the [phanes] table is expected to parse");
        assert!(PhanesClient::new(&config).is_err());
    }
}

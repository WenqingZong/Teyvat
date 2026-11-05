//! Client of Phanes Gnosis, which releases gnoses (API keys) over an end-to-end encrypted HTTP
//! round trip.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use reqwest::{Client, StatusCode};
use secrecy::SecretBox;
use serde::Deserialize;
use tracing::info;
use url::Url;

use crate::alert::{SendAlert, alert_and_panic};
use crate::exit_reason::ExitReason;
use crate::utils::{Clock, RealClock};
use layline::gnosis::{GNOSIS_PATH, Gnosis, Initiator, Request, Response};
use layline::noise::{StaticKey, deserialize_key};

const GNOSIS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The `[phanes]` table of the config. Like Phanes itself, each service has its own key pair.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhanesClientConfig {
    url: Url,
    /// Base64 X25519 private key this client authenticates to Gnosis with.
    #[serde(deserialize_with = "deserialize_key")]
    gnosis_private_key: SecretBox<StaticKey>,
    /// Base64 X25519 public key of Phanes Gnosis.
    #[serde(deserialize_with = "deserialize_key")]
    gnosis_public_key: SecretBox<StaticKey>,
}

/// Where gnoses come from; the connector asks only when a topic needs one.
pub(crate) trait GnosisSource {
    /// The live or demo gnosis of `Key`, whose type picks the gnosis id.
    async fn fetch_gnosis<Key: Gnosis>(&self, demo: bool) -> Result<Key>;
}

/// Fetches the live or demo gnosis of `Key`. Failing to get it is an unrecoverable startup error:
/// alerts the phone, then panics with `ExitReason::GnosisUnavailable`, which the binary's panic
/// hook turns into the exit code.
pub(crate) async fn fetch_gnosis_or_panic<Key: Gnosis>(
    source: &impl GnosisSource,
    alert: &impl SendAlert,
    demo: bool,
) -> Key {
    let gnosis_id = Key::gnosis_id(demo);
    match source.fetch_gnosis::<Key>(demo).await {
        Ok(gnosis) => {
            info!("fetched gnosis {gnosis_id} from Phanes");
            gnosis
        }
        Err(error) => match alert_and_panic(
            alert,
            &format!("Market Data Provider Cannot Query Gnosis {gnosis_id}"),
            &format!("cannot get gnosis {gnosis_id} from Phanes: {error:#}"),
            ExitReason::GnosisUnavailable,
        )
        .await {},
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

/// Clones share the keys, so every exchange worker can hold one.
#[derive(Clone)]
pub(crate) struct PhanesClient<Http = ReqwestGnosisHttp, TimeSource = RealClock> {
    url: Url,
    gnosis_private_key: Arc<SecretBox<StaticKey>>,
    gnosis_public_key: Arc<SecretBox<StaticKey>>,
    http: Http,
    clock: TimeSource,
}

impl PhanesClient {
    /// Both keys were checked when the config was parsed.
    pub(crate) fn new(config: PhanesClientConfig) -> Result<Self> {
        let client = Client::builder().timeout(GNOSIS_REQUEST_TIMEOUT).build()?;
        Ok(Self {
            url: config.url,
            gnosis_private_key: Arc::new(config.gnosis_private_key),
            gnosis_public_key: Arc::new(config.gnosis_public_key),
            http: ReqwestGnosisHttp { client },
            clock: RealClock,
        })
    }
}

impl<Http: GnosisHttp, TimeSource: Clock> GnosisSource for PhanesClient<Http, TimeSource> {
    async fn fetch_gnosis<Key: Gnosis>(&self, demo: bool) -> Result<Key> {
        let (initiator, first_message) = Initiator::write_request(
            &self.gnosis_private_key,
            &self.gnosis_public_key,
            &Request::new(Key::gnosis_id(demo), self.clock.now()),
        )?;
        let (status, second_message) = self
            .http
            .post(self.url.join(GNOSIS_PATH)?, first_message)
            .await?;
        if !status.is_success() {
            bail!("Phanes answered {status}");
        }
        match initiator.read_response(&second_message)? {
            Response::Granted { gnosis } => Ok(gnosis),
            Response::Denied { reason } => bail!("Phanes denied it: {reason}"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::num::NonZeroU64;

    use secrecy::ExposeSecret;
    use serde_json::{Value, from_str};
    use toml::de::Error as TomlError;

    use super::*;
    use crate::alert::tests::{RecordingAlert, expect_alert_and_panic};
    use crate::utils::tests::MockClock;
    use layline::gnosis::binance::{BinanceApiKey, BinanceSbeApiKey};
    use layline::gnosis::{GnosisId, Responder};
    use layline::noise::encode_key;
    use layline::testing::test_keypair;

    const PHANES_URL: &str = "https://phanes.example";
    const GRANTED_SECRET: &str = "dummy-granted-secret";
    /// The live SBE gnosis, granted as `GRANTED_GNOSIS`.
    const GRANTED_GNOSIS_ID: GnosisId = GnosisId::BinanceSbeReal;
    /// The live trading gnosis, never granted.
    const DENIED_GNOSIS_ID: GnosisId = GnosisId::BinanceReal;
    /// The demo SBE gnosis, granted with fields that are not those of `BinanceSbeApiKey`.
    const MISNAMED_GNOSIS_ID: GnosisId = GnosisId::BinanceSbeDemo;
    const MISNAMED_GNOSIS: &str = r#"{"secret_key": "dummy-misnamed-secret"}"#;
    const DENIAL_REASON: &str = "gnosis not allowed";
    /// 2026-10-24 00:00:00 UTC.
    const NOW_NANOSECONDS: NonZeroU64 =
        NonZeroU64::new(1_792_800_000_000_000_000).expect("not zero");

    /// `GRANTED_GNOSIS_ID` as Phanes sends it.
    fn granted_gnosis() -> String {
        format!(r#"{{"api_key": "{GRANTED_SECRET}"}}"#)
    }

    /// For code that needs a production `PhanesClient` but never fetches a gnosis.
    pub(crate) fn unreachable_phanes_client() -> PhanesClient {
        let (gnosis_private_key, _) = test_keypair();
        let (_, gnosis_public_key) = test_keypair();
        PhanesClient {
            url: Url::parse("https://phanes.invalid").expect("the test URL is valid"),
            gnosis_private_key: Arc::new(gnosis_private_key),
            gnosis_public_key: Arc::new(gnosis_public_key),
            http: ReqwestGnosisHttp {
                client: Client::new(),
            },
            clock: RealClock,
        }
    }

    /// Grants the gnoses in `grants`, each a JSON object as Phanes sends it, denies the rest, and
    /// records every gnosis id asked for.
    #[derive(Default)]
    pub(crate) struct MockGnosisSource {
        pub(crate) grants: HashMap<GnosisId, String>,
        pub(crate) offline: bool,
        pub(crate) requested_gnosis_ids: RefCell<Vec<GnosisId>>,
    }

    impl GnosisSource for MockGnosisSource {
        async fn fetch_gnosis<Key: Gnosis>(&self, demo: bool) -> Result<Key> {
            let gnosis_id = Key::gnosis_id(demo);
            self.requested_gnosis_ids.borrow_mut().push(gnosis_id);
            if self.offline {
                bail!("Phanes is unreachable");
            }
            match self.grants.get(&gnosis_id) {
                Some(gnosis) => Ok(from_str(gnosis)?),
                None => bail!("Phanes denied it: {DENIAL_REASON}"),
            }
        }
    }

    #[derive(Clone, Copy, PartialEq)]
    enum PhanesBehaviour {
        Normal,
        Offline,
        /// Answers every request with an empty 404, as for a client whose public key Phanes does not hold.
        ForgetClient,
        Garbage,
    }

    /// Phanes' end of the HTTP round trip, answered in process with the server half of the protocol.
    struct GnosisHttpMock {
        gnosis_private_key: SecretBox<StaticKey>,
        client_public_key: SecretBox<StaticKey>,
        grants: HashMap<GnosisId, String>,
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
            let Ok(responder) = Responder::read_request(&self.gnosis_private_key, &body) else {
                return Ok((StatusCode::NOT_FOUND, Vec::new()));
            };
            let is_known_client = responder.client_public_key().expose_secret()
                == self.client_public_key.expose_secret();
            if self.behaviour == PhanesBehaviour::ForgetClient || !is_known_client {
                return Ok((StatusCode::NOT_FOUND, Vec::new()));
            }
            let request = responder.parse_request()?;
            let response: Response<Value> = match self.grants.get(&request.gnosis_id) {
                Some(gnosis) => Response::Granted {
                    gnosis: from_str(gnosis)?,
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

    /// A client wired to an in-process Phanes that grants `GRANTED_GNOSIS_ID`, and a mock clock.
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
                grants: HashMap::from([(GRANTED_GNOSIS_ID, granted_gnosis())]),
                behaviour: PhanesBehaviour::Normal,
                requests: RefCell::new(Vec::new()),
            };
            let client = PhanesClient {
                url: Url::parse(PHANES_URL).expect("PHANES_URL is a valid URL"),
                gnosis_private_key: Arc::new(client_private_key),
                gnosis_public_key: Arc::new(gnosis_public_key),
                http: phanes,
                clock: clock.clone(),
            };
            Self { client, clock }
        }

        fn phanes_behaves(&mut self, behaviour: PhanesBehaviour) {
            self.client.http.behaviour = behaviour;
        }

        fn expect_phanes_received(&self, gnosis_ids: &[GnosisId]) -> Vec<Request> {
            let requests = self.client.http.requests.borrow().clone();
            let received: Vec<GnosisId> =
                requests.iter().map(|request| request.gnosis_id).collect();
            assert_eq!(gnosis_ids, received);
            requests
        }

        /// Fetches the live gnosis of `Key` and expects an error containing `expected`.
        async fn expect_error<Key: Gnosis + Debug>(&self, expected: &str) {
            let error = self
                .client
                .fetch_gnosis::<Key>(false)
                .await
                .expect_err("the fetch is expected to fail");
            let message = format!("{error:#}");
            assert!(message.contains(expected), "{message}");
        }
    }

    #[tokio::test]
    async fn granted_gnosis_is_returned_and_request_carries_clock_time() {
        let fixture = PhanesFixture::new();
        fixture.clock.advance(Duration::from_secs(5));
        let sbe_api_key: BinanceSbeApiKey = fixture
            .client
            .fetch_gnosis(false)
            .await
            .expect("Phanes is expected to grant the gnosis");
        assert_eq!(GRANTED_SECRET, sbe_api_key.api_key().expose_secret());
        let requests = fixture.expect_phanes_received(&[GRANTED_GNOSIS_ID]);
        assert_eq!(1_792_800_005, requests[0].timestamp);
    }

    #[tokio::test]
    async fn denied_gnosis_is_an_error() {
        assert_eq!(DENIED_GNOSIS_ID, BinanceApiKey::gnosis_id(false));
        PhanesFixture::new()
            .expect_error::<BinanceApiKey>(DENIAL_REASON)
            .await;
    }

    #[tokio::test]
    async fn unknown_client_gets_not_found() {
        let mut fixture = PhanesFixture::new();
        fixture.phanes_behaves(PhanesBehaviour::ForgetClient);
        fixture.expect_error::<BinanceSbeApiKey>("404").await;
        fixture.expect_phanes_received(&[]);
    }

    #[tokio::test]
    async fn unreachable_phanes_is_an_error() {
        let mut fixture = PhanesFixture::new();
        fixture.phanes_behaves(PhanesBehaviour::Offline);
        fixture
            .expect_error::<BinanceSbeApiKey>("unreachable")
            .await;
    }

    #[tokio::test]
    async fn garbled_reply_is_rejected() {
        let mut fixture = PhanesFixture::new();
        fixture.phanes_behaves(PhanesBehaviour::Garbage);
        fixture.expect_error::<BinanceSbeApiKey>("noise").await;
    }

    fn gnosis_source_granting_test_gnoses() -> MockGnosisSource {
        MockGnosisSource {
            grants: HashMap::from([
                (GRANTED_GNOSIS_ID, granted_gnosis()),
                (MISNAMED_GNOSIS_ID, MISNAMED_GNOSIS.to_owned()),
            ]),
            ..MockGnosisSource::default()
        }
    }

    /// Fetches the live or demo gnosis of `Key`, expects it to panic, and returns the one alert.
    async fn expect_fetch_or_panic_to_alert<Key: Gnosis + Debug>(demo: bool) -> String {
        let gnosis_source = gnosis_source_granting_test_gnoses();
        let alert = RecordingAlert::default();
        let recorded = expect_alert_and_panic(
            fetch_gnosis_or_panic::<Key>(&gnosis_source, &alert, demo),
            &alert,
            ExitReason::GnosisUnavailable,
        )
        .await;
        let gnosis_id = Key::gnosis_id(demo);
        assert!(
            recorded.starts_with(&format!(
                "Market Data Provider Cannot Query Gnosis {gnosis_id}"
            )),
            "{recorded}"
        );
        recorded
    }

    #[tokio::test]
    async fn fetch_or_panic_returns_the_gnosis_without_alerting() {
        let alert = RecordingAlert::default();
        let sbe_api_key: BinanceSbeApiKey =
            fetch_gnosis_or_panic(&gnosis_source_granting_test_gnoses(), &alert, false).await;
        assert_eq!(GRANTED_SECRET, sbe_api_key.api_key().expose_secret());
        assert!(alert.alerts().is_empty());
    }

    #[tokio::test]
    async fn fetch_or_panic_alerts_the_gnosis_id_then_panics_with_its_exit_reason() {
        let alert = expect_fetch_or_panic_to_alert::<BinanceApiKey>(false).await;
        assert!(alert.contains(DENIAL_REASON), "{alert}");
    }

    #[tokio::test]
    async fn gnosis_with_other_fields_alerts_its_field_names_but_never_its_values() {
        assert_eq!(MISNAMED_GNOSIS_ID, BinanceSbeApiKey::gnosis_id(true));
        let alert = expect_fetch_or_panic_to_alert::<BinanceSbeApiKey>(true).await;
        assert!(alert.contains("secret_key"), "{alert}");
        assert!(!alert.contains("dummy-misnamed-secret"), "{alert}");
    }

    /// The `[phanes]` table holding `gnosis_private_key` and `gnosis_public_key`, as written.
    fn phanes_table(
        gnosis_private_key: &str,
        gnosis_public_key: &str,
    ) -> Result<PhanesClientConfig, TomlError> {
        toml::from_str(&format!(
            "url = \"{PHANES_URL}\"\ngnosis_private_key = \"{gnosis_private_key}\"\ngnosis_public_key = \"{gnosis_public_key}\""
        ))
    }

    #[test]
    fn table_holds_both_keys_and_refuses_invalid_ones() {
        let (private_key, _) = test_keypair();
        let (_, public_key) = test_keypair();
        let encoded_private_key = encode_key(private_key.expose_secret());
        let encoded_public_key = encode_key(public_key.expose_secret());
        let config = phanes_table(&encoded_private_key, &encoded_public_key)
            .expect("the [phanes] table is expected to parse");
        let client = PhanesClient::new(config).expect("the config is valid");
        assert_eq!(
            private_key.expose_secret(),
            client.gnosis_private_key.expose_secret()
        );
        assert_eq!(
            public_key.expose_secret(),
            client.gnosis_public_key.expose_secret()
        );
        assert!(phanes_table("not base64!", &encoded_public_key).is_err());
        assert!(phanes_table(&encoded_private_key, "not base64!").is_err());
    }
}

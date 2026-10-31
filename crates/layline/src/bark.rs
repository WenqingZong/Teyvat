//! Encrypted push notifications via [Bark](https://bark.day.app/#/encryption).
//!
//! The payload is encrypted with AES-256-CBC (PKCS#7) before leaving this machine,
//! so the Bark relay server only sees ciphertext. The key and IV are used as raw
//! ASCII bytes, matching the Bark app's "AES-256-CBC" encryption setting.

use aes::Aes256;
use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use cbc::Encryptor;
use rand::{RngExt, distr::Alphanumeric, rng};
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Arc;
use tracing::info;
use url::Url;

type Aes256CbcEncryptor = Encryptor<Aes256>;

const KEY_LENGTH: usize = 32;
const INITIALIZATION_VECTOR_LENGTH: usize = 16;
const BARK_SERVER: &str = "https://api.day.app";

#[derive(Debug, thiserror::Error)]
pub enum BarkError {
    #[error("missing [bark] section in the config")]
    MissingConfig,
    #[error("encryption key must be exactly {KEY_LENGTH} bytes, got {0}")]
    InvalidKeyLength(usize),
    #[error("Bark server {0} cannot be used as a base URL")]
    InvalidServer(Url),
    #[error("failed to serialize payload: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The URL is stripped because it contains the device key.
    #[error("request to Bark failed: {0}")]
    Http(reqwest::Error),
    #[error("Bark rejected the push ({code}): {message}")]
    Rejected { code: StatusCode, message: String },
}

/// The `[bark]` table of the config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarkConfig {
    #[serde(default = "default_bark_server")]
    server: Url,
    device_key: SecretString,
    encryption_key: SecretString,
}

fn default_bark_server() -> Url {
    Url::parse(BARK_SERVER).expect("BARK_SERVER is a valid URL")
}

/// Fields of the Bark push payload; see the Bark docs for the full list. Empty fields are omitted.
#[derive(Debug, Default, Serialize)]
pub struct BarkMessage {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub title: String,
    pub body: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sound: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub group: String,
}

#[derive(Deserialize)]
struct BarkResponse {
    /// HTTP-style status code in the JSON body.
    #[serde(deserialize_with = "deserialize_status_code")]
    code: StatusCode,
    #[serde(default)]
    message: String,
}

/// `StatusCode` has no serde support; Bark sends it as a JSON number.
fn deserialize_status_code<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<StatusCode, D::Error> {
    let code = u16::deserialize(deserializer)?;
    StatusCode::from_u16(code).map_err(DeserializeError::custom)
}

pub struct BarkNotifier {
    client: Client,
    config: BarkConfig,
}

impl BarkNotifier {
    /// Validates the config up front so a bad key fails at startup, not at the first push.
    pub fn new(config: BarkConfig) -> Result<Self, BarkError> {
        let key_length = config.encryption_key.expose_secret().len();
        if key_length != KEY_LENGTH {
            return Err(BarkError::InvalidKeyLength(key_length));
        }
        if config.server.cannot_be_a_base() {
            return Err(BarkError::InvalidServer(config.server));
        }
        Ok(Self {
            client: Client::new(),
            config,
        })
    }

    pub async fn send(&self, message: &BarkMessage) -> Result<(), BarkError> {
        let plaintext = serde_json::to_string(message)?;
        let initialization_vector = random_initialization_vector();
        let ciphertext = encrypt(
            self.config.encryption_key.expose_secret(),
            &initialization_vector,
            &plaintext,
        );

        let mut url = self.config.server.clone();
        url.path_segments_mut()
            .expect("server is checked in BarkNotifier::new")
            .pop_if_empty()
            .push(self.config.device_key.expose_secret());
        let response: BarkResponse = self
            .client
            .post(url)
            .form(&[
                ("ciphertext", ciphertext.as_str()),
                ("iv", initialization_vector.as_str()),
            ])
            .send()
            .await
            .map_err(http_error)?
            .json()
            .await
            .map_err(http_error)?;

        if response.code != StatusCode::OK {
            return Err(BarkError::Rejected {
                code: response.code,
                message: response.message,
            });
        }
        Ok(())
    }
}

/// Where pushes go: Bark in release builds, the log in debug builds. Clones share one Bark client.
#[derive(Clone)]
pub enum Notifier {
    Bark(Arc<BarkNotifier>),
    /// Local development: no Bark credentials needed, every push becomes an `info!` line.
    Log,
}

impl Notifier {
    /// Release builds require the `[bark]` section of the config; debug builds always log.
    pub fn new(release_build: bool, bark_config: Option<BarkConfig>) -> Result<Self, BarkError> {
        if !release_build {
            return Ok(Self::Log);
        }
        let bark_config = bark_config.ok_or(BarkError::MissingConfig)?;
        Ok(Self::Bark(Arc::new(BarkNotifier::new(bark_config)?)))
    }

    pub async fn send(&self, message: &BarkMessage) -> Result<(), BarkError> {
        match self {
            Self::Bark(bark) => bark.send(message).await,
            Self::Log => {
                let title = &message.title;
                let body = message.body.replace('\n', " | ");
                info!("[push] {title}: {body}");
                Ok(())
            }
        }
    }
}

fn http_error(error: reqwest::Error) -> BarkError {
    BarkError::Http(error.without_url())
}

/// Bark transmits the IV as a 16-character string whose ASCII bytes are the IV.
fn random_initialization_vector() -> String {
    rng()
        .sample_iter(Alphanumeric)
        .take(INITIALIZATION_VECTOR_LENGTH)
        .map(char::from)
        .collect()
}

/// AES-256-CBC with PKCS#7 padding, base64-encoded.
fn encrypt(key: &str, initialization_vector: &str, plaintext: &str) -> String {
    let key: &[u8; KEY_LENGTH] = key
        .as_bytes()
        .try_into()
        .expect("encryption key length is checked in BarkNotifier::new");
    let initialization_vector: &[u8; INITIALIZATION_VECTOR_LENGTH] = initialization_vector
        .as_bytes()
        .try_into()
        .expect("initialization vector must be 16 bytes");
    let ciphertext = Aes256CbcEncryptor::new(key.into(), initialization_vector.into())
        .encrypt_padded_vec::<Pkcs7>(plaintext.as_bytes());
    BASE64.encode(ciphertext)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Dummy key, not a real Bark key.
    const TEST_KEY: &str = "0123456789abcdefghijklmnopqrstuv";

    fn test_config(server: &str, encryption_key: &str) -> BarkConfig {
        BarkConfig {
            server: Url::parse(server).expect("test server is a valid URL"),
            device_key: SecretString::from("device"),
            encryption_key: SecretString::from(encryption_key),
        }
    }

    #[test]
    fn encrypt_matches_openssl() {
        // Generated with the reference script:
        // echo -n '{"body":"test","sound":"birdsong"}' \
        //   | openssl enc -aes-256-cbc -K <hex key> -iv <hex iv> | base64 -w0
        let ciphertext = encrypt(
            TEST_KEY,
            "ABCDEFGHIJKLMNOP",
            r#"{"body":"test","sound":"birdsong"}"#,
        );
        assert_eq!(
            ciphertext,
            "odL/VIsSeyNRmhny8Ed/esIgDXzw3e5t4fkAx5SoFqTMDr8tLBbGQXyrADym2nnl"
        );
    }

    #[test]
    fn message_serializes_without_empty_fields() {
        let message = BarkMessage {
            body: "test".into(),
            sound: "birdsong".into(),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&message).expect("message serializes"),
            r#"{"body":"test","sound":"birdsong"}"#
        );
    }

    #[test]
    fn random_initialization_vector_is_16_alphanumeric_chars() {
        let initialization_vector = random_initialization_vector();
        assert_eq!(initialization_vector.len(), INITIALIZATION_VECTOR_LENGTH);
        assert!(
            initialization_vector
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        );
    }

    #[test]
    fn notifier_rejects_wrong_key_length() {
        let result = BarkNotifier::new(test_config(BARK_SERVER, "too-short"));
        assert!(matches!(result, Err(BarkError::InvalidKeyLength(9))));
    }

    #[test]
    fn notifier_rejects_non_base_server() {
        let result = BarkNotifier::new(test_config("mailto:someone@example.com", TEST_KEY));
        assert!(matches!(result, Err(BarkError::InvalidServer(_))));
    }

    #[test]
    fn response_code_parses_as_status_code() {
        let response: BarkResponse =
            serde_json::from_str(r#"{"code":400,"message":"failed to get device token"}"#)
                .expect("response parses");
        assert_eq!(response.code, StatusCode::BAD_REQUEST);
        assert_eq!(response.message, "failed to get device token");
    }

    #[test]
    fn response_code_rejects_invalid_status_code() {
        assert!(serde_json::from_str::<BarkResponse>(r#"{"code":42}"#).is_err());
    }

    #[test]
    fn debug_build_logs_without_bark_config() {
        let notifier = Notifier::new(false, None).expect("debug builds never need Bark");
        assert!(matches!(notifier, Notifier::Log));
    }

    #[test]
    fn release_build_requires_bark_config() {
        assert!(matches!(
            Notifier::new(true, None),
            Err(BarkError::MissingConfig)
        ));
    }

    #[test]
    fn release_build_pushes_through_bark() {
        let bark_config = test_config(BARK_SERVER, TEST_KEY);
        let notifier = Notifier::new(true, Some(bark_config)).expect("the Bark config is valid");
        assert!(matches!(notifier, Notifier::Bark(_)));
    }

    #[tokio::test]
    async fn log_notifier_always_succeeds() {
        let message = BarkMessage {
            title: "title".to_owned(),
            body: "first line\nsecond line".to_owned(),
            ..Default::default()
        };
        assert!(Notifier::Log.send(&message).await.is_ok());
    }

    #[test]
    fn config_defaults_to_official_server() {
        let config: BarkConfig =
            toml::from_str("device_key = \"device\"\nencryption_key = \"key\"")
                .expect("config parses");
        assert_eq!(config.server.as_str(), "https://api.day.app/");
    }
}

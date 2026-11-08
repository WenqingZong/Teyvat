//! The client side's config of Gnosis: the `[gnosis_client]` table of every service that fetches
//! gnoses from Phanes, beside `celestia::client::CelestiaClientConfig`, the table of every
//! service that reports to Celestia.

use std::fmt::{Debug, Formatter, Result as FormatResult};

use secrecy::SecretBox;
use serde::Deserialize;
use url::Url;

use crate::noise::{StaticKey, deserialize_key};

/// The `[gnosis_client]` table of the config. Like Phanes itself, each service has its own key
/// pair. The client's public key is not needed: Noise derives it from the private key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GnosisClientConfig {
    /// Phanes, as Cloudflare exposes it.
    pub url: Url,
    /// Base64 X25519 private key this client authenticates to Gnosis with.
    #[serde(deserialize_with = "deserialize_key")]
    pub client_gnosis_private_key: SecretBox<StaticKey>,
    /// Base64 X25519 public key of Phanes Gnosis.
    #[serde(deserialize_with = "deserialize_key")]
    pub phanes_gnosis_public_key: SecretBox<StaticKey>,
}

/// Written by hand so `url` prints as one URL rather than `Url`'s fields; the keys print as
/// `[REDACTED]`.
impl Debug for GnosisClientConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let Self {
            url,
            client_gnosis_private_key,
            phanes_gnosis_public_key,
        } = self;
        formatter
            .debug_struct("GnosisClientConfig")
            .field("url", &format_args!("{url}"))
            .field("client_gnosis_private_key", client_gnosis_private_key)
            .field("phanes_gnosis_public_key", phanes_gnosis_public_key)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use toml::de::Error as TomlError;

    use super::*;

    /// Any 32 bytes, base64-encoded, make a valid X25519 key for parsing.
    const DUMMY_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    fn table_text(client_gnosis_private_key: &str) -> String {
        format!(
            "url = \"https://phanes.example\"\nclient_gnosis_private_key = \"{client_gnosis_private_key}\"\nphanes_gnosis_public_key = \"{DUMMY_KEY}\"\n"
        )
    }

    fn table(client_gnosis_private_key: &str) -> Result<GnosisClientConfig, TomlError> {
        toml::from_str(&table_text(client_gnosis_private_key))
    }

    #[test]
    fn table_prints_the_url_on_one_line_and_redacts_keys() {
        let config = table(DUMMY_KEY).expect("the table is valid");
        let expected = r#"GnosisClientConfig {
    url: https://phanes.example/,
    client_gnosis_private_key: SecretBox<[u8; 32]>([REDACTED]),
    phanes_gnosis_public_key: SecretBox<[u8; 32]>([REDACTED]),
}"#;
        assert_eq!(expected, format!("{config:#?}"));
    }

    #[test]
    fn invalid_key_or_unknown_field_is_refused() {
        assert!(table("not base64!").is_err());
        let valid_text = table_text(DUMMY_KEY);
        let unknown_field = format!("{valid_text}unknown = 1\n");
        assert!(toml::from_str::<GnosisClientConfig>(&unknown_field).is_err());
    }
}

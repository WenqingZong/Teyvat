//! phanes.toml, read once at startup; see `deploy/phanes/phanes.example.toml`. Every Rust field
//! is named as its TOML key.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;

use secrecy::{ExposeSecret, SecretBox};
use serde::Deserialize;
use strum::IntoEnumIterator;
use tracing::info;

use crate::clients::{ClientName, Clients};
use crate::gnosis::config::Gnoses;
use crate::service::Service;
use layline::bark::BarkConfig;
use layline::gnosis::GnosisId;
use layline::noise::{StaticKey, deserialize_key};
use layline::secrets::{SecretFileError, read_secret_toml};

pub const DEFAULT_MAX_CLOCK_SKEW_SECONDS: u64 = 60;

#[derive(Debug, thiserror::Error)]
pub enum PhanesConfigError {
    #[error(transparent)]
    SecretFile(#[from] SecretFileError),
    #[error("{address} is listed under both clients {first} and {second}")]
    DuplicateClientIp {
        address: IpAddr,
        first: ClientName,
        second: ClientName,
    },
    #[error(
        "client {second} reuses a public key of client {first}; every client needs its own key for each service"
    )]
    DuplicatePublicKey {
        first: ClientName,
        second: ClientName,
    },
    #[error(
        "clients.{client}.allowed_gnosis_ids lists {gnosis_id}, but [gnoses.{gnosis_id}] is missing"
    )]
    MissingGnosis {
        client: ClientName,
        gnosis_id: GnosisId,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhanesConfig {
    pub listen_address: SocketAddr,
    /// Base64, like every key in this file.
    #[serde(deserialize_with = "deserialize_key")]
    pub gnosis_private_key: SecretBox<StaticKey>,
    #[serde(deserialize_with = "deserialize_key")]
    pub celestia_private_key: SecretBox<StaticKey>,
    #[serde(default = "default_max_clock_skew_seconds")]
    pub max_clock_skew_seconds: u64,
    pub clients: Clients,
    /// Phanes may hold gnoses no client is allowed yet.
    #[serde(default)]
    pub gnoses: Gnoses,
    /// Required by release builds only; debug builds log notifications instead.
    pub bark: Option<BarkConfig>,
}

fn default_max_clock_skew_seconds() -> u64 {
    DEFAULT_MAX_CLOCK_SKEW_SECONDS
}

impl PhanesConfig {
    /// Parses the file and checks it, so the services can take a loaded config as valid. It holds
    /// the private keys, the gnoses and the Bark credentials, so it must be chmod 600; parse
    /// errors never quote it.
    pub fn load(path: &Path) -> Result<Self, PhanesConfigError> {
        let config: Self = read_secret_toml(path)?;
        config.check_addresses_are_distinct()?;
        config.check_public_keys_are_distinct()?;
        config.check_allowed_gnoses_exist()?;
        // Every key, gnosis and Bark credential is a `secrecy` type, which `Debug` redacts.
        let path = path.display();
        info!("loaded {path}: {config:#?}");
        Ok(config)
    }

    fn check_addresses_are_distinct(&self) -> Result<(), PhanesConfigError> {
        let mut clients_by_address = HashMap::new();
        for (name, client) in &self.clients {
            for address in client.addresses() {
                if let Some(first) = clients_by_address.insert(address, name) {
                    return Err(PhanesConfigError::DuplicateClientIp {
                        address,
                        first: first.clone(),
                        second: name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Across clients and services alike, so a key names one client of one service.
    fn check_public_keys_are_distinct(&self) -> Result<(), PhanesConfigError> {
        let mut seen_public_keys: Vec<(&StaticKey, &ClientName)> = Vec::new();
        for (name, client) in &self.clients {
            for service in Service::iter() {
                let public_key = client.public_key(service).expose_secret();
                if let Some((_, first)) = seen_public_keys
                    .iter()
                    .find(|(seen, _)| *seen == public_key)
                {
                    return Err(PhanesConfigError::DuplicatePublicKey {
                        first: (*first).clone(),
                        second: name.clone(),
                    });
                }
                seen_public_keys.push((public_key, name));
            }
        }
        Ok(())
    }

    fn check_allowed_gnoses_exist(&self) -> Result<(), PhanesConfigError> {
        for (name, client) in &self.clients {
            for &gnosis_id in &client.allowed_gnosis_ids {
                if self.gnoses.get(gnosis_id).is_none() {
                    return Err(PhanesConfigError::MissingGnosis {
                        client: name.clone(),
                        gnosis_id,
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::{TempDir, tempdir};

    use super::*;
    use layline::noise::encode_key;
    use layline::testing::{test_keypair, write_secret_file};

    /// Arbitrary documentation addresses (RFC 5737 / RFC 3849) with no business meaning.
    const OSAKA_IPV4: &str = "192.0.2.1";
    const OSAKA_IPV6: &str = "2001:db8::1";
    const TOKYO_IPV4: &str = "198.51.100.7";
    const OSAKA: &str = "Osaka";
    const TOKYO: &str = "Tokyo";
    const GNOSIS_API_KEY: &str = "天上永恒的王座到来";
    const GNOSIS_SECRET_KEY: &str = "世界为之焕然一新";

    /// Where phanes.toml is written, Phanes' private keys, and the public keys of two clients.
    struct Fixture {
        directory: TempDir,
        gnosis_private_key: SecretBox<StaticKey>,
        celestia_private_key: SecretBox<StaticKey>,
        osaka_gnosis_public_key: SecretBox<StaticKey>,
        osaka_celestia_public_key: SecretBox<StaticKey>,
        tokyo_gnosis_public_key: SecretBox<StaticKey>,
        tokyo_celestia_public_key: SecretBox<StaticKey>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                directory: tempdir().expect("temporary directory is created"),
                gnosis_private_key: test_keypair().0,
                celestia_private_key: test_keypair().0,
                osaka_gnosis_public_key: test_keypair().1,
                osaka_celestia_public_key: test_keypair().1,
                tokyo_gnosis_public_key: test_keypair().1,
                tokyo_celestia_public_key: test_keypair().1,
            }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.path().join(name)
        }

        /// Osaka may fetch binance_demo; Tokyo only uses Celestia. binance_real is held for nobody.
        fn valid_text(&self) -> String {
            let gnosis_private_key = encode_key(self.gnosis_private_key.expose_secret());
            let celestia_private_key = encode_key(self.celestia_private_key.expose_secret());
            let osaka_gnosis_public_key = encode_key(self.osaka_gnosis_public_key.expose_secret());
            let osaka_celestia_public_key =
                encode_key(self.osaka_celestia_public_key.expose_secret());
            let tokyo_gnosis_public_key = encode_key(self.tokyo_gnosis_public_key.expose_secret());
            let tokyo_celestia_public_key =
                encode_key(self.tokyo_celestia_public_key.expose_secret());
            format!(
                r#"
listen_address = "127.0.0.1:8888"
gnosis_private_key = "{gnosis_private_key}"
celestia_private_key = "{celestia_private_key}"

[clients.Osaka]
ipv4 = "{OSAKA_IPV4}"
ipv6 = "{OSAKA_IPV6}"
client_gnosis_service_public_key = "{osaka_gnosis_public_key}"
client_celestia_service_public_key = "{osaka_celestia_public_key}"
allowed_gnosis_ids = ["binance_demo"]

[clients.Tokyo]
ipv4 = "{TOKYO_IPV4}"
client_gnosis_service_public_key = "{tokyo_gnosis_public_key}"
client_celestia_service_public_key = "{tokyo_celestia_public_key}"
allowed_gnosis_ids = []

[gnoses.binance_demo]
api_key = "{GNOSIS_API_KEY}"
secret_key = "{GNOSIS_SECRET_KEY}"

[gnoses.binance_real]
api_key = "never"
secret_key = "released"
"#
            )
        }

        fn load(&self, text: &str) -> Result<PhanesConfig, PhanesConfigError> {
            let path = self.path("phanes.toml");
            write_secret_file(&path, text);
            PhanesConfig::load(&path)
        }

        fn load_valid(&self) -> PhanesConfig {
            self.load(&self.valid_text()).expect("config loads")
        }

        /// Expects `text` to fail parsing, and returns the message.
        fn expect_invalid_toml(&self, text: &str) -> String {
            let error = self.load(text).expect_err("config is refused");
            assert!(
                matches!(
                    error,
                    PhanesConfigError::SecretFile(SecretFileError::InvalidToml { .. })
                ),
                "{error}"
            );
            error.to_string()
        }
    }

    #[test]
    fn loads_clients_keys_and_defaults() {
        let fixture = Fixture::new();
        let config = fixture.load_valid();
        assert_eq!(
            vec![OSAKA, TOKYO],
            config
                .clients
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            config.max_clock_skew_seconds,
            DEFAULT_MAX_CLOCK_SKEW_SECONDS
        );
        assert!(config.bark.is_none());
        assert_eq!(
            fixture.gnosis_private_key.expose_secret(),
            config.gnosis_private_key.expose_secret()
        );
        assert_eq!(
            fixture.celestia_private_key.expose_secret(),
            config.celestia_private_key.expose_secret()
        );
    }

    #[test]
    fn debug_output_redacts_keys_and_gnoses() {
        let fixture = Fixture::new();
        let debug_output = format!("{:?}", fixture.load_valid());
        let keys = [
            &fixture.gnosis_private_key,
            &fixture.celestia_private_key,
            &fixture.osaka_gnosis_public_key,
            &fixture.osaka_celestia_public_key,
            &fixture.tokyo_gnosis_public_key,
            &fixture.tokyo_celestia_public_key,
        ];
        for key in keys {
            let key_bytes = format!("{:?}", key.expose_secret());
            assert!(!debug_output.contains(&key_bytes), "{debug_output}");
            let encoded_key = encode_key(key.expose_secret());
            assert!(!debug_output.contains(&encoded_key), "{debug_output}");
        }
        assert!(!debug_output.contains(GNOSIS_API_KEY), "{debug_output}");
        assert!(!debug_output.contains(GNOSIS_SECRET_KEY), "{debug_output}");
    }

    #[test]
    fn allowed_gnosis_without_a_table_is_rejected() {
        let fixture = Fixture::new();
        let text = fixture.valid_text().replace(
            "allowed_gnosis_ids = []",
            "allowed_gnosis_ids = [\"binance_sbe_demo\"]",
        );
        assert!(matches!(
            fixture.load(&text),
            Err(PhanesConfigError::MissingGnosis { ref client, gnosis_id: GnosisId::BinanceSbeDemo })
                if client == TOKYO
        ));
    }

    #[test]
    fn public_key_reused_by_another_client_or_service_is_rejected() {
        let fixture = Fixture::new();
        let osaka_celestia_public_key =
            encode_key(fixture.osaka_celestia_public_key.expose_secret());
        for reused_by in [
            &fixture.tokyo_celestia_public_key,
            &fixture.osaka_gnosis_public_key,
        ] {
            let text = fixture.valid_text().replace(
                &encode_key(reused_by.expose_secret()),
                &osaka_celestia_public_key,
            );
            assert!(matches!(
                fixture.load(&text),
                Err(PhanesConfigError::DuplicatePublicKey { .. })
            ));
        }
    }

    #[test]
    fn address_shared_by_two_clients_is_rejected() {
        let fixture = Fixture::new();
        let text = fixture.valid_text().replace(
            &format!("ipv4 = \"{TOKYO_IPV4}\"\n"),
            &format!("ipv4 = \"{TOKYO_IPV4}\"\nipv6 = \"{OSAKA_IPV6}\"\n"),
        );
        assert!(matches!(
            fixture.load(&text),
            Err(PhanesConfigError::DuplicateClientIp { address, .. })
                if address == OSAKA_IPV6.parse::<IpAddr>().expect("test address is valid")
        ));
    }

    #[test]
    fn malformed_values_are_rejected() {
        /// The last octet is out of range.
        const INVALID_IPV4: &str = "198.51.100.300";
        let fixture = Fixture::new();
        let valid_text = fixture.valid_text();
        let without_ipv4 = valid_text.replace(&format!("ipv4 = \"{TOKYO_IPV4}\"\n"), "");
        let without_allowed_gnosis_ids = valid_text.replace("allowed_gnosis_ids = []\n", "");
        let replacements = [
            (TOKYO_IPV4.to_owned(), INVALID_IPV4.to_owned()),
            (
                format!("ipv4 = \"{OSAKA_IPV4}\""),
                format!("ipv4 = \"{OSAKA_IPV6}\""),
            ),
            (
                format!("ipv6 = \"{OSAKA_IPV6}\""),
                format!("ipv6 = \"{OSAKA_IPV4}\""),
            ),
            (
                encode_key(fixture.tokyo_gnosis_public_key.expose_secret()),
                "not a key".to_owned(),
            ),
            (
                encode_key(fixture.celestia_private_key.expose_secret()),
                "not a key".to_owned(),
            ),
            (
                "[\"binance_demo\"]".to_owned(),
                "[\"binance_demo_trading\"]".to_owned(),
            ),
            (
                "[clients.Tokyo]\n".to_owned(),
                "[clients.Tokyo]\nname = \"Tokyo\"\n".to_owned(),
            ),
        ];
        for (valid, invalid) in replacements {
            fixture.expect_invalid_toml(&valid_text.replace(&valid, &invalid));
        }
        fixture.expect_invalid_toml(&without_ipv4);
        fixture.expect_invalid_toml(&without_allowed_gnosis_ids);
    }

    #[test]
    fn misnamed_gnosis_field_is_rejected_without_quoting_the_secret() {
        let fixture = Fixture::new();
        let text = fixture.valid_text().replacen("secret_key", "secret_kye", 1);
        let message = fixture.expect_invalid_toml(&text);
        assert!(message.contains("secret_kye"), "{message}");
        assert!(!message.contains(GNOSIS_SECRET_KEY), "{message}");
    }

    #[test]
    fn missing_config_file_is_a_read_error() {
        let fixture = Fixture::new();
        assert!(matches!(
            PhanesConfig::load(&fixture.path("absent.toml")),
            Err(PhanesConfigError::SecretFile(SecretFileError::Read { .. }))
        ));
    }
}

//! phanes.toml, read once at startup; see `deploy/phanes/phanes.example.toml`. Every Rust field
//! is named as its TOML key.

use std::collections::{BTreeMap, HashMap};
use std::iter::once;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};

use secrecy::{SecretBox, SecretString};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer};

use crate::client_filter::ClientsByIp;
use crate::clients::{ClientName, ClientsByPublicKey};
use crate::gnosis::config::Gnoses;
use crate::gnosis::server::AllowedGnoses;
use layline::bark::BarkConfig;
use layline::gnosis::GnosisId;
use layline::noise::{StaticKey, copy_key, decode_key};
use layline::secrets::{SecretFileError, read_secret_file, read_secret_toml};

pub const DEFAULT_MAX_CLOCK_SKEW_SECONDS: u64 = 60;

#[derive(Debug, thiserror::Error)]
pub enum PhanesConfigError {
    #[error(transparent)]
    SecretFile(#[from] SecretFileError),
    #[error("invalid key in {0}")]
    InvalidKey(String),
    #[error("{address} is listed under both clients {first} and {second}")]
    DuplicateClientIp {
        address: IpAddr,
        first: ClientName,
        second: ClientName,
    },
    #[error("clients {first} and {second} have the same {field}")]
    DuplicatePublicKey {
        field: &'static str,
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
    pub gnosis_private_key_file: PathBuf,
    pub celestia_private_key_file: PathBuf,
    #[serde(default = "default_max_clock_skew_seconds")]
    pub max_clock_skew_seconds: u64,
    pub clients: BTreeMap<ClientName, ClientConfig>,
    /// Phanes may hold gnoses no client is allowed yet.
    #[serde(default)]
    pub gnoses: Gnoses,
    /// Required by release builds only; debug builds log notifications instead.
    pub bark: Option<BarkConfig>,
}

/// A `[clients.<name>]` table: where the client calls from, the public key it uses for each
/// service, and the gnoses it may fetch.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// The addresses Cloudflare reports in `CF-Connecting-IP`.
    pub ipv4: Ipv4Addr,
    pub ipv6: Option<Ipv6Addr>,
    #[serde(deserialize_with = "deserialize_public_key")]
    pub client_gnosis_service_public_key: SecretBox<StaticKey>,
    #[serde(deserialize_with = "deserialize_public_key")]
    pub client_celestia_service_public_key: SecretBox<StaticKey>,
    /// May be empty: such a client only uses Celestia.
    pub allowed_gnosis_ids: Vec<GnosisId>,
}

impl ClientConfig {
    fn addresses(&self) -> impl Iterator<Item = IpAddr> {
        once(IpAddr::V4(self.ipv4)).chain(self.ipv6.map(IpAddr::V6))
    }
}

/// A base64 X25519 public key, refused when parsing if it does not decode.
fn deserialize_public_key<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<SecretBox<StaticKey>, D::Error> {
    let encoded_public_key = SecretString::deserialize(deserializer)?;
    decode_key(&encoded_public_key)
        .map_err(|_| DeserializeError::custom("expected a base64 X25519 public key"))
}

fn default_max_clock_skew_seconds() -> u64 {
    DEFAULT_MAX_CLOCK_SKEW_SECONDS
}

impl PhanesConfig {
    /// The config holds the gnoses and Bark credentials, so it must be chmod 600; parse errors
    /// never quote it.
    pub fn load(path: &Path) -> Result<Self, PhanesConfigError> {
        Ok(read_secret_toml(path)?)
    }

    pub fn load_gnosis_private_key(&self) -> Result<SecretBox<StaticKey>, PhanesConfigError> {
        load_private_key(&self.gnosis_private_key_file)
    }

    pub fn load_celestia_private_key(&self) -> Result<SecretBox<StaticKey>, PhanesConfigError> {
        load_private_key(&self.celestia_private_key_file)
    }

    /// Every address of every client; refuses an address listed under two clients.
    pub fn clients_by_ip(&self) -> Result<ClientsByIp, PhanesConfigError> {
        let mut clients_by_ip = HashMap::new();
        for (name, client) in &self.clients {
            for address in client.addresses() {
                if let Some(first) = clients_by_ip.insert(address, name.clone()) {
                    return Err(PhanesConfigError::DuplicateClientIp {
                        address,
                        first,
                        second: name.clone(),
                    });
                }
            }
        }
        Ok(clients_by_ip)
    }

    pub fn gnosis_clients(&self) -> Result<ClientsByPublicKey, PhanesConfigError> {
        self.clients_by_public_key("client_gnosis_service_public_key", |client| {
            &client.client_gnosis_service_public_key
        })
    }

    pub fn celestia_clients(&self) -> Result<ClientsByPublicKey, PhanesConfigError> {
        self.clients_by_public_key("client_celestia_service_public_key", |client| {
            &client.client_celestia_service_public_key
        })
    }

    /// A service tells its clients apart by key, so two clients may not share one.
    fn clients_by_public_key(
        &self,
        field: &'static str,
        public_key_of: fn(&ClientConfig) -> &SecretBox<StaticKey>,
    ) -> Result<ClientsByPublicKey, PhanesConfigError> {
        let mut clients = ClientsByPublicKey::default();
        for (name, client) in &self.clients {
            clients
                .insert(copy_key(public_key_of(client)), name.clone())
                .map_err(|first| PhanesConfigError::DuplicatePublicKey {
                    field,
                    first,
                    second: name.clone(),
                })?;
        }
        Ok(clients)
    }

    /// Each client's `allowed_gnosis_ids` with their gnoses; refuses an id without a `[gnoses]` table.
    pub fn allowed_gnoses(&self) -> Result<AllowedGnoses, PhanesConfigError> {
        let mut allowed_gnoses = HashMap::new();
        for (name, client) in &self.clients {
            let mut gnoses = HashMap::new();
            for &gnosis_id in &client.allowed_gnosis_ids {
                let gnosis =
                    self.gnoses
                        .get(gnosis_id)
                        .ok_or_else(|| PhanesConfigError::MissingGnosis {
                            client: name.clone(),
                            gnosis_id,
                        })?;
                gnoses.insert(gnosis_id, gnosis);
            }
            allowed_gnoses.insert(name.clone(), gnoses);
        }
        Ok(allowed_gnoses)
    }
}

fn load_private_key(path: &Path) -> Result<SecretBox<StaticKey>, PhanesConfigError> {
    let encoded = read_secret_file(path)?;
    decode_key(&encoded).map_err(|_| PhanesConfigError::InvalidKey(path.display().to_string()))
}

#[cfg(test)]
mod tests {
    use std::fs::{Permissions, set_permissions};
    use std::os::unix::fs::PermissionsExt;

    use secrecy::ExposeSecret;
    use tempfile::{TempDir, tempdir};

    use super::*;
    use layline::noise::encode_key;
    use layline::testing::{test_keypair, write_secret_file};

    const WORLD_READABLE_MODE: u32 = 0o644;
    /// Arbitrary documentation addresses (RFC 5737 / RFC 3849) with no business meaning.
    const OSAKA_IPV4: &str = "192.0.2.1";
    const OSAKA_IPV6: &str = "2001:db8::1";
    const TOKYO_IPV4: &str = "198.51.100.7";
    const OSAKA: &str = "Osaka";
    const TOKYO: &str = "Tokyo";
    const GNOSIS_API_KEY: &str = "天上永恒的王座到来";
    const GNOSIS_SECRET_KEY: &str = "世界为之焕然一新";

    /// Private key files on disk, and the public keys of two clients.
    struct Fixture {
        directory: TempDir,
        osaka_gnosis_public_key: SecretBox<StaticKey>,
        osaka_celestia_public_key: SecretBox<StaticKey>,
        tokyo_gnosis_public_key: SecretBox<StaticKey>,
        tokyo_celestia_public_key: SecretBox<StaticKey>,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempdir().expect("temporary directory is created");
            for name in ["gnosis.key", "celestia.key"] {
                let (private_key, _) = test_keypair();
                write_secret_file(
                    &directory.path().join(name),
                    &encode_key(private_key.expose_secret()),
                );
            }
            Self {
                directory,
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
            format!(
                r#"
listen_address = "127.0.0.1:8888"
gnosis_private_key_file = "{}"
celestia_private_key_file = "{}"

[clients.Osaka]
ipv4 = "{OSAKA_IPV4}"
ipv6 = "{OSAKA_IPV6}"
client_gnosis_service_public_key = "{}"
client_celestia_service_public_key = "{}"
allowed_gnosis_ids = ["binance_demo"]

[clients.Tokyo]
ipv4 = "{TOKYO_IPV4}"
client_gnosis_service_public_key = "{}"
client_celestia_service_public_key = "{}"
allowed_gnosis_ids = []

[gnoses.binance_demo]
api_key = "{GNOSIS_API_KEY}"
secret_key = "{GNOSIS_SECRET_KEY}"

[gnoses.binance_real]
api_key = "never"
secret_key = "released"
"#,
                self.path("gnosis.key").display(),
                self.path("celestia.key").display(),
                encode_key(self.osaka_gnosis_public_key.expose_secret()),
                encode_key(self.osaka_celestia_public_key.expose_secret()),
                encode_key(self.tokyo_gnosis_public_key.expose_secret()),
                encode_key(self.tokyo_celestia_public_key.expose_secret()),
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

    fn address(text: &str) -> IpAddr {
        text.parse().expect("test address is valid")
    }

    #[test]
    fn loads_keys_and_defaults() {
        let fixture = Fixture::new();
        let config = fixture.load_valid();
        assert_eq!(
            config.max_clock_skew_seconds,
            DEFAULT_MAX_CLOCK_SKEW_SECONDS
        );
        assert!(config.bark.is_none());
        config.load_gnosis_private_key().expect("gnosis key loads");
        config
            .load_celestia_private_key()
            .expect("celestia key loads");
    }

    #[test]
    fn every_address_names_its_client() {
        let config = Fixture::new().load_valid();
        let expected: ClientsByIp = [
            (OSAKA_IPV4, OSAKA),
            (OSAKA_IPV6, OSAKA),
            (TOKYO_IPV4, TOKYO),
        ]
        .into_iter()
        .map(|(text, name)| (address(text), ClientName::from(name)))
        .collect();
        assert_eq!(
            config.clients_by_ip().expect("addresses are distinct"),
            expected
        );
    }

    #[test]
    fn each_service_knows_its_clients_by_their_key_for_it() {
        let fixture = Fixture::new();
        let config = fixture.load_valid();
        let gnosis_clients = config.gnosis_clients().expect("keys are distinct");
        let celestia_clients = config.celestia_clients().expect("keys are distinct");
        let osaka = ClientName::from(OSAKA);
        let tokyo = ClientName::from(TOKYO);
        assert_eq!(
            Ok(osaka.clone()),
            gnosis_clients.identify(&fixture.osaka_gnosis_public_key, None)
        );
        assert_eq!(
            Ok(tokyo),
            celestia_clients.identify(&fixture.tokyo_celestia_public_key, None)
        );
        assert!(
            gnosis_clients
                .identify(&fixture.osaka_celestia_public_key, None)
                .is_err()
        );
        assert!(
            celestia_clients
                .identify(&fixture.osaka_gnosis_public_key, Some(&osaka))
                .is_err()
        );
    }

    #[test]
    fn each_client_gets_only_its_allowed_gnoses() {
        let config = Fixture::new().load_valid();
        let allowed_gnoses = config
            .allowed_gnoses()
            .expect("every allowed id has a table");
        let osaka_gnoses = &allowed_gnoses[OSAKA];
        assert_eq!(1, osaka_gnoses.len());
        assert!(osaka_gnoses.contains_key(&GnosisId::BinanceDemo));
        assert!(allowed_gnoses[TOKYO].is_empty());
    }

    #[test]
    fn allowed_gnosis_without_a_table_is_rejected() {
        let fixture = Fixture::new();
        let text = fixture.valid_text().replace(
            "allowed_gnosis_ids = []",
            "allowed_gnosis_ids = [\"binance_sbe_demo\"]",
        );
        let error = fixture
            .load(&text)
            .expect("config parses")
            .allowed_gnoses()
            .expect_err("binance_sbe_demo has no table");
        assert!(matches!(
            error,
            PhanesConfigError::MissingGnosis { ref client, gnosis_id: GnosisId::BinanceSbeDemo }
                if client == TOKYO
        ));
    }

    #[test]
    fn public_key_shared_by_two_clients_is_rejected() {
        let fixture = Fixture::new();
        let text = fixture.valid_text().replace(
            &encode_key(fixture.tokyo_celestia_public_key.expose_secret()),
            &encode_key(fixture.osaka_celestia_public_key.expose_secret()),
        );
        let config = fixture.load(&text).expect("config parses");
        config
            .gnosis_clients()
            .expect("gnosis keys are still distinct");
        assert!(matches!(
            config.celestia_clients(),
            Err(PhanesConfigError::DuplicatePublicKey {
                field: "client_celestia_service_public_key",
                ..
            })
        ));
    }

    #[test]
    fn address_shared_by_two_clients_is_rejected() {
        let fixture = Fixture::new();
        let text = fixture.valid_text().replace(
            &format!("ipv4 = \"{TOKYO_IPV4}\"\n"),
            &format!("ipv4 = \"{TOKYO_IPV4}\"\nipv6 = \"{OSAKA_IPV6}\"\n"),
        );
        let config = fixture.load(&text).expect("config parses");
        assert!(matches!(
            config.clients_by_ip(),
            Err(PhanesConfigError::DuplicateClientIp { address: duplicate, .. })
                if duplicate == address(OSAKA_IPV6)
        ));
    }

    #[test]
    fn malformed_client_tables_are_rejected() {
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
    fn world_readable_private_key_is_rejected() {
        let fixture = Fixture::new();
        let config = fixture.load_valid();
        set_permissions(
            fixture.path("celestia.key"),
            Permissions::from_mode(WORLD_READABLE_MODE),
        )
        .expect("permissions are set");
        assert!(matches!(
            config.load_celestia_private_key(),
            Err(PhanesConfigError::SecretFile(
                SecretFileError::InsecurePermissions(_)
            ))
        ));
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

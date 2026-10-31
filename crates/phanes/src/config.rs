use std::collections::{BTreeMap, HashMap};
use std::fs::{metadata, read_to_string};
use std::io::Error as IoError;
use std::iter::once;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use secrecy::{ExposeSecret, SecretBox, SecretString};
use serde::Deserialize;
use toml::de::Error as TomlError;

use crate::bark::BarkConfig;
use crate::client_filter::ClientsByIp;
use crate::noise::{StaticKey, decode_private_key, decode_public_key};

pub const DEFAULT_MAX_CLOCK_SKEW_SECONDS: u64 = 60;
/// Any of these bits set means group or others can access the file; secrets must be chmod 600.
const GROUP_AND_OTHERS_PERMISSION_BITS: u32 = 0o077;

#[derive(Debug, thiserror::Error)]
pub enum PhanesConfigError {
    #[error("cannot read {path}: {source}")]
    Read { path: PathBuf, source: IoError },
    #[error("invalid config {path}: {source}")]
    Parse { path: PathBuf, source: TomlError },
    #[error("{0} must not be readable by group or others (chmod 600)")]
    InsecurePermissions(PathBuf),
    #[error("invalid key in {0}")]
    InvalidKey(String),
    #[error("{address} is listed under both clients {first} and {second}")]
    DuplicateClientIp {
        address: IpAddr,
        first: String,
        second: String,
    },
    #[error("{field} names client {name:?}, which is not in [clients]")]
    UnknownClient { field: String, name: String },
    #[error("{0} is empty")]
    NoAllowedClients(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhanesConfig {
    pub listen_address: SocketAddr,
    pub gnosis_private_key_file: PathBuf,
    pub celestia_private_key_file: PathBuf,
    /// `<api_keys_directory>/<key_id>.txt` holds the secret for `key_id`.
    pub api_keys_directory: PathBuf,
    #[serde(default = "default_max_clock_skew_seconds")]
    pub max_clock_skew_seconds: u64,
    /// Client name → the addresses Cloudflare may report for it.
    pub clients: BTreeMap<String, ClientAddresses>,
    pub gnosis_service_config: GnosisServiceConfig,
    pub celestia_service_config: CelestiaServiceConfig,
    /// Required by release builds only; debug builds log notifications instead.
    pub bark: Option<BarkConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientAddresses {
    pub ipv4: Ipv4Addr,
    pub ipv6: Option<Ipv6Addr>,
}

impl ClientAddresses {
    fn iter(&self) -> impl Iterator<Item = IpAddr> {
        once(IpAddr::V4(self.ipv4)).chain(self.ipv6.map(IpAddr::V6))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GnosisServiceConfig {
    pub public_key: String,
    pub allowed_key_ids: Vec<String>,
    pub allowed_clients: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CelestiaServiceConfig {
    pub public_key: String,
    pub allowed_clients: Vec<String>,
}

fn default_max_clock_skew_seconds() -> u64 {
    DEFAULT_MAX_CLOCK_SKEW_SECONDS
}

impl PhanesConfig {
    /// The config holds Bark credentials, so it must be chmod 600 like the private key.
    pub fn load(path: &Path) -> Result<Self, PhanesConfigError> {
        let text = read_secret_file(path)?;
        toml::from_str(text.expose_secret()).map_err(|source| PhanesConfigError::Parse {
            path: path.to_owned(),
            source,
        })
    }

    pub fn load_gnosis_private_key(&self) -> Result<SecretBox<StaticKey>, PhanesConfigError> {
        load_private_key(&self.gnosis_private_key_file)
    }

    pub fn load_celestia_private_key(&self) -> Result<SecretBox<StaticKey>, PhanesConfigError> {
        load_private_key(&self.celestia_private_key_file)
    }

    pub fn parse_gnosis_client_public_key_from_toml(&self) -> Result<StaticKey, PhanesConfigError> {
        decode_client_public_key(
            &self.gnosis_service_config.public_key,
            "gnosis_service_config.public_key",
        )
    }

    pub fn parse_celestia_client_public_key_from_toml(
        &self,
    ) -> Result<StaticKey, PhanesConfigError> {
        decode_client_public_key(
            &self.celestia_service_config.public_key,
            "celestia_service_config.public_key",
        )
    }

    /// Every client in `[clients]`; rejects an address listed under two names.
    fn parse_all_clients_from_toml(&self) -> Result<ClientsByIp, PhanesConfigError> {
        let mut clients_by_ip = HashMap::new();
        for (name, addresses) in &self.clients {
            for address in addresses.iter() {
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

    pub fn parse_gnosis_clients_from_toml(&self) -> Result<ClientsByIp, PhanesConfigError> {
        self.parse_allowed_clients_from_toml(
            &self.gnosis_service_config.allowed_clients,
            "gnosis_service_config.allowed_clients",
        )
    }

    pub fn parse_celestia_clients_from_toml(&self) -> Result<ClientsByIp, PhanesConfigError> {
        self.parse_allowed_clients_from_toml(
            &self.celestia_service_config.allowed_clients,
            "celestia_service_config.allowed_clients",
        )
    }

    fn parse_allowed_clients_from_toml(
        &self,
        names: &[String],
        field: &str,
    ) -> Result<ClientsByIp, PhanesConfigError> {
        if names.is_empty() {
            return Err(PhanesConfigError::NoAllowedClients(field.to_owned()));
        }
        if let Some(name) = names.iter().find(|name| !self.clients.contains_key(*name)) {
            return Err(PhanesConfigError::UnknownClient {
                field: field.to_owned(),
                name: name.clone(),
            });
        }
        let mut clients_by_ip = self.parse_all_clients_from_toml()?;
        clients_by_ip.retain(|_, name| names.contains(name));
        Ok(clients_by_ip)
    }
}

fn load_private_key(path: &Path) -> Result<SecretBox<StaticKey>, PhanesConfigError> {
    let encoded = read_secret_file(path)?;
    decode_private_key(&encoded)
        .map_err(|_| PhanesConfigError::InvalidKey(path.display().to_string()))
}

fn decode_client_public_key(encoded: &str, field: &str) -> Result<StaticKey, PhanesConfigError> {
    decode_public_key(encoded).map_err(|_| PhanesConfigError::InvalidKey(field.to_owned()))
}

fn read_secret_file(path: &Path) -> Result<SecretString, PhanesConfigError> {
    let read_error = |source| PhanesConfigError::Read {
        path: path.to_owned(),
        source,
    };
    let mode = metadata(path).map_err(read_error)?.permissions().mode();
    if mode & GROUP_AND_OTHERS_PERMISSION_BITS != 0 {
        return Err(PhanesConfigError::InsecurePermissions(path.to_owned()));
    }
    Ok(SecretString::from(
        read_to_string(path).map_err(read_error)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::noise::encode_key;
    use crate::noise::tests::test_keypair;
    use std::fs::{Permissions, set_permissions, write};
    use tempfile::{TempDir, tempdir};

    const OWNER_ONLY_MODE: u32 = 0o600;
    const WORLD_READABLE_MODE: u32 = 0o644;
    /// Arbitrary documentation addresses (RFC 5737 / RFC 3849) with no business meaning.
    const OSAKA_IPV4: &str = "192.0.2.1";
    const OSAKA_IPV6: &str = "2001:db8::1";
    const TOKYO_IPV4: &str = "198.51.100.7";

    struct Fixture {
        directory: TempDir,
        client_public_key: StaticKey,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempdir().expect("temporary directory is created");
            for name in ["gnosis.key", "celestia.key"] {
                let (private_key, _) = test_keypair();
                write_secret(
                    &directory.path().join(name),
                    &encode_key(private_key.expose_secret()),
                );
            }
            let (_, client_public_key) = test_keypair();
            Self {
                directory,
                client_public_key,
            }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.path().join(name)
        }

        fn config_text(&self, celestia_public_key: &str) -> String {
            format!(
                r#"
listen_address = "127.0.0.1:8888"
gnosis_private_key_file = "{}"
celestia_private_key_file = "{}"
api_keys_directory = "{}"

[clients.Osaka]
ipv4 = "{OSAKA_IPV4}"
ipv6 = "{OSAKA_IPV6}"

[clients.Tokyo]
ipv4 = "{TOKYO_IPV4}"

[gnosis_service_config]
public_key = "{}"
allowed_key_ids = ["binance_demo"]
allowed_clients = ["Osaka"]

[celestia_service_config]
public_key = "{celestia_public_key}"
allowed_clients = ["Osaka", "Tokyo"]
"#,
                self.path("gnosis.key").display(),
                self.path("celestia.key").display(),
                self.directory.path().display(),
                encode_key(&self.client_public_key),
            )
        }

        fn load(&self, text: &str) -> Result<PhanesConfig, PhanesConfigError> {
            let path = self.path("phanes.toml");
            write_secret(&path, text);
            PhanesConfig::load(&path)
        }
    }

    fn write_secret(path: &Path, contents: &str) {
        write(path, contents).expect("file is written");
        set_permissions(path, Permissions::from_mode(OWNER_ONLY_MODE))
            .expect("permissions are set");
    }

    #[test]
    fn loads_both_services() {
        let fixture = Fixture::new();
        let encoded_client_key = encode_key(&fixture.client_public_key);
        let config = fixture
            .load(&fixture.config_text(&encoded_client_key))
            .expect("config loads");
        assert_eq!(
            config.max_clock_skew_seconds,
            DEFAULT_MAX_CLOCK_SKEW_SECONDS
        );
        assert!(config.bark.is_none());
        config.load_gnosis_private_key().expect("gnosis key loads");
        config
            .load_celestia_private_key()
            .expect("celestia key loads");
        assert_eq!(
            config
                .parse_gnosis_client_public_key_from_toml()
                .expect("gnosis client key"),
            fixture.client_public_key
        );
        assert_eq!(
            config
                .parse_celestia_client_public_key_from_toml()
                .expect("celestia client key"),
            fixture.client_public_key
        );
    }

    fn address(text: &str) -> IpAddr {
        text.parse().expect("test address is valid")
    }

    fn valid_config(fixture: &Fixture) -> String {
        fixture.config_text(&encode_key(&fixture.client_public_key))
    }

    #[test]
    fn each_service_gets_its_allowed_clients() {
        let fixture = Fixture::new();
        let config = fixture.load(&valid_config(&fixture)).expect("config loads");
        let osaka_only: ClientsByIp = [OSAKA_IPV4, OSAKA_IPV6]
            .into_iter()
            .map(|text| (address(text), "Osaka".to_owned()))
            .collect();
        let mut everyone = osaka_only.clone();
        everyone.insert(address(TOKYO_IPV4), "Tokyo".to_owned());
        assert_eq!(
            config
                .parse_gnosis_clients_from_toml()
                .expect("gnosis clients"),
            osaka_only
        );
        assert_eq!(
            config
                .parse_celestia_clients_from_toml()
                .expect("celestia clients"),
            everyone
        );
        assert_eq!(
            config.parse_all_clients_from_toml().expect("all clients"),
            everyone
        );
    }

    #[test]
    fn unknown_allowed_client_is_rejected() {
        /// Any name missing from `[clients]`.
        const UNKNOWN_CLIENT: &str = "Kyoto";
        let fixture = Fixture::new();
        let text = valid_config(&fixture).replace(
            r#"allowed_clients = ["Osaka"]"#,
            &format!(r#"allowed_clients = ["Osaka", "{UNKNOWN_CLIENT}"]"#),
        );
        let config = fixture.load(&text).expect("config parses");
        assert!(matches!(
            config.parse_gnosis_clients_from_toml(),
            Err(PhanesConfigError::UnknownClient { field, name })
                if field == "gnosis_service_config.allowed_clients" && name == UNKNOWN_CLIENT
        ));
    }

    #[test]
    fn empty_allowed_clients_is_rejected() {
        let fixture = Fixture::new();
        let text = valid_config(&fixture).replace(
            r#"allowed_clients = ["Osaka", "Tokyo"]"#,
            "allowed_clients = []",
        );
        let config = fixture.load(&text).expect("config parses");
        assert!(matches!(
            config.parse_celestia_clients_from_toml(),
            Err(PhanesConfigError::NoAllowedClients(field))
                if field == "celestia_service_config.allowed_clients"
        ));
    }

    #[test]
    fn address_shared_by_two_clients_is_rejected() {
        let fixture = Fixture::new();
        let text = valid_config(&fixture).replace(
            &format!("ipv4 = \"{TOKYO_IPV4}\"\n"),
            &format!("ipv4 = \"{TOKYO_IPV4}\"\nipv6 = \"{OSAKA_IPV6}\"\n"),
        );
        let config = fixture.load(&text).expect("config parses");
        assert!(matches!(
            config.parse_gnosis_clients_from_toml(),
            Err(PhanesConfigError::DuplicateClientIp { address: duplicate, .. })
                if duplicate == address(OSAKA_IPV6)
        ));
    }

    #[test]
    fn invalid_client_address_is_rejected() {
        /// The last octet is out of range.
        const INVALID_IPV4: &str = "198.51.100.300";
        let fixture = Fixture::new();
        let text = valid_config(&fixture).replace(TOKYO_IPV4, INVALID_IPV4);
        assert!(matches!(
            fixture.load(&text),
            Err(PhanesConfigError::Parse { .. })
        ));
    }

    #[test]
    fn client_without_ipv4_is_rejected() {
        let fixture = Fixture::new();
        let text = valid_config(&fixture).replace(&format!("ipv4 = \"{TOKYO_IPV4}\"\n"), "");
        assert!(matches!(
            fixture.load(&text),
            Err(PhanesConfigError::Parse { .. })
        ));
    }

    #[test]
    fn address_of_the_wrong_family_is_rejected() {
        let fixture = Fixture::new();
        let swaps = [
            (
                format!("ipv4 = \"{OSAKA_IPV4}\""),
                format!("ipv4 = \"{OSAKA_IPV6}\""),
            ),
            (
                format!("ipv6 = \"{OSAKA_IPV6}\""),
                format!("ipv6 = \"{OSAKA_IPV4}\""),
            ),
        ];
        for (valid, wrong_family) in swaps {
            let text = valid_config(&fixture).replace(&valid, &wrong_family);
            assert!(matches!(
                fixture.load(&text),
                Err(PhanesConfigError::Parse { .. })
            ));
        }
    }

    #[test]
    fn address_list_instead_of_table_is_rejected() {
        let fixture = Fixture::new();
        let text = valid_config(&fixture).replace(
            &format!("[clients.Tokyo]\nipv4 = \"{TOKYO_IPV4}\"\n"),
            &format!("[clients]\nTokyo = [\"{TOKYO_IPV4}\"]\n"),
        );
        assert!(matches!(
            fixture.load(&text),
            Err(PhanesConfigError::Parse { .. })
        ));
    }

    #[test]
    fn name_field_in_service_config_is_rejected() {
        let fixture = Fixture::new();
        let text = valid_config(&fixture).replace(
            "[gnosis_service_config]\n",
            "[gnosis_service_config]\nname = \"Osaka\"\n",
        );
        assert!(matches!(
            fixture.load(&text),
            Err(PhanesConfigError::Parse { .. })
        ));
    }

    #[test]
    fn missing_celestia_section_is_rejected() {
        let fixture = Fixture::new();
        let text = fixture.config_text("unused");
        let without_celestia = text
            .split("[celestia_service_config]")
            .next()
            .expect("split yields the first part");
        assert!(matches!(
            fixture.load(without_celestia),
            Err(PhanesConfigError::Parse { .. })
        ));
    }

    #[test]
    fn invalid_celestia_public_key_is_rejected() {
        let fixture = Fixture::new();
        let config = fixture
            .load(&fixture.config_text("not a key"))
            .expect("config parses");
        assert!(matches!(
            config.parse_celestia_client_public_key_from_toml(),
            Err(PhanesConfigError::InvalidKey(field)) if field == "celestia_service_config.public_key"
        ));
    }

    #[test]
    fn world_readable_private_key_is_rejected() {
        let fixture = Fixture::new();
        let encoded_client_key = encode_key(&fixture.client_public_key);
        let config = fixture
            .load(&fixture.config_text(&encoded_client_key))
            .expect("config loads");
        set_permissions(
            fixture.path("celestia.key"),
            Permissions::from_mode(WORLD_READABLE_MODE),
        )
        .expect("permissions are set");
        assert!(matches!(
            config.load_celestia_private_key(),
            Err(PhanesConfigError::InsecurePermissions(_))
        ));
    }

    #[test]
    fn missing_config_file_is_a_read_error() {
        let fixture = Fixture::new();
        assert!(matches!(
            PhanesConfig::load(&fixture.path("absent.toml")),
            Err(PhanesConfigError::Read { .. })
        ));
    }
}

//! The provider's TOML config, read once at startup; see `deploy/market_data_provider.example.toml`.

use crate::DEFAULT_ROOT_DIRECTORY;
use crate::phanes_client::{PhanesClient, PhanesClientConfig};
use anyhow::{Context, Result};
use layline::bark::{BarkConfig, Notifier};
use layline::build::RELEASE_BUILD;
use layline::secrets::read_secret_file;
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    /// The HTTP API listens on `127.0.0.1:port`.
    port: u16,
    /// Serve the venues' demo-trading data; every subscription request must ask for the same environment.
    demo: bool,
    phanes: PhanesClientConfig,
    /// Required by release builds only; debug builds log alerts instead.
    bark: Option<BarkConfig>,
}

pub struct ProviderConfig {
    pub root_directory: PathBuf,
    pub listen_address: SocketAddr,
    /// How long a strategy keeps its topics without resubscribing, so restarting strategies do not churn the exchange.
    pub lease_duration: Duration,
    /// Serve the venues' demo-trading data; every subscription request must ask for the same environment.
    pub demo: bool,
    pub(crate) phanes_client: PhanesClient,
    pub(crate) notifier: Notifier,
}

impl ProviderConfig {
    /// Checks everything up front, so a bad key or a release build without `[bark]` refuses to start.
    /// The file must be chmod 600, since it holds the Bark credentials.
    pub fn load(path: &Path) -> Result<Self> {
        let text = read_secret_file(path)?;
        let config: ConfigFile = toml::from_str(text.expose_secret())
            .with_context(|| format!("invalid config {}", path.display()))?;
        Ok(Self {
            root_directory: PathBuf::from(DEFAULT_ROOT_DIRECTORY),
            listen_address: SocketAddr::from((Ipv4Addr::LOCALHOST, config.port)),
            lease_duration: DEFAULT_LEASE_DURATION,
            demo: config.demo,
            phanes_client: PhanesClient::new(&config.phanes)?,
            notifier: Notifier::new(RELEASE_BUILD, config.bark)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{Permissions, set_permissions, write};
    use std::os::unix::fs::PermissionsExt;
    use tempfile::{TempDir, tempdir};

    const OWNER_ONLY_MODE: u32 = 0o600;
    const WORLD_READABLE_MODE: u32 = 0o644;
    /// Any 32 bytes, base64-encoded, make a valid X25519 key for parsing.
    const DUMMY_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    struct ConfigFixture {
        directory: TempDir,
    }

    impl ConfigFixture {
        fn new() -> Self {
            let directory = tempdir().expect("temp dir creation is expected to not fail");
            let fixture = Self { directory };
            fixture.write_secret("gnosis.key", DUMMY_KEY);
            fixture
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.path().join(name)
        }

        fn write_secret(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.path(name);
            write(&path, contents).expect("writing is expected to not fail");
            set_permissions(&path, Permissions::from_mode(OWNER_ONLY_MODE))
                .expect("setting permissions is expected to not fail");
            path
        }

        fn valid_text(&self) -> String {
            format!(
                "port = 6666\ndemo = false\n\n[phanes]\nurl = \"https://phanes.example\"\ngnosis_private_key_file = \"{}\"\ngnosis_public_key = \"{DUMMY_KEY}\"\n",
                self.path("gnosis.key").display()
            )
        }

        fn load(&self, text: &str) -> Result<ProviderConfig> {
            ProviderConfig::load(&self.write_secret("market_data_provider.toml", text))
        }
    }

    #[test]
    fn valid_config_listens_on_localhost_with_defaults() {
        let fixture = ConfigFixture::new();
        let config = fixture
            .load(&fixture.valid_text())
            .expect("the config is valid");
        assert_eq!(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 6666)),
            config.listen_address
        );
        assert_eq!(PathBuf::from(DEFAULT_ROOT_DIRECTORY), config.root_directory);
        assert_eq!(DEFAULT_LEASE_DURATION, config.lease_duration);
        assert!(matches!(config.notifier, Notifier::Log), "debug build");
    }

    #[test]
    fn missing_phanes_or_unknown_field_is_refused() {
        let fixture = ConfigFixture::new();
        assert!(fixture.load("port = 6666\n").is_err());
        let unknown_field = format!("unknown = 1\n{}", fixture.valid_text());
        assert!(fixture.load(&unknown_field).is_err());
    }

    #[test]
    fn invalid_phanes_key_is_refused() {
        let fixture = ConfigFixture::new();
        let text = fixture.valid_text().replace(
            &format!("gnosis_public_key = \"{DUMMY_KEY}\""),
            "gnosis_public_key = \"short\"",
        );
        assert!(fixture.load(&text).is_err());
    }

    #[test]
    fn world_readable_config_is_refused() {
        let fixture = ConfigFixture::new();
        let path = fixture.write_secret("market_data_provider.toml", &fixture.valid_text());
        set_permissions(&path, Permissions::from_mode(WORLD_READABLE_MODE))
            .expect("setting permissions is expected to not fail");
        let error = ProviderConfig::load(&path)
            .err()
            .expect("a world-readable config is expected to be refused");
        assert!(error.to_string().contains("chmod 600"), "{error}");
    }

    #[test]
    fn missing_config_is_refused() {
        let fixture = ConfigFixture::new();
        assert!(ProviderConfig::load(&fixture.path("absent.toml")).is_err());
    }
}

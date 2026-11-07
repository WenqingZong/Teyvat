//! The provider's TOML config, read once at startup; see `deploy/market_data_provider/market_data_provider.example.toml`.

use std::path::Path;

use anyhow::Result;
use serde::Deserialize;
use tracing::info;

use crate::phanes_client::PhanesClientConfig;
use layline::bark::BarkConfig;
use layline::secrets::read_secret_toml;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataProviderConfig {
    /// The HTTP API listens on `127.0.0.1:port`.
    pub port: u16,
    /// Serve the venues' demo-trading data; every subscription request must ask for the same environment.
    pub demo: bool,
    pub(crate) phanes_client: PhanesClientConfig,
    /// Required by release builds only; debug builds log alerts instead.
    pub(crate) bark: Option<BarkConfig>,
}

impl MarketDataProviderConfig {
    /// The keys are decoded while parsing, so a bad one refuses to start here; `server::serve`
    /// refuses a release build without `[bark]`. The file must be chmod 600, since it holds the
    /// Gnosis private key and the Bark credentials.
    pub fn load(path: &Path) -> Result<Self> {
        let config: Self = read_secret_toml(path)?;
        // The keys and the Bark credentials are `secrecy` types, which `Debug` redacts.
        let path = path.display();
        info!("loaded {path}: {config:#?}");
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{Permissions, set_permissions};
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use tempfile::{TempDir, tempdir};

    use super::*;
    use layline::testing::write_secret_file;

    const WORLD_READABLE_MODE: u32 = 0o644;
    /// Any 32 bytes, base64-encoded, make a valid X25519 key for parsing.
    const DUMMY_KEY: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=";

    struct ConfigFixture {
        directory: TempDir,
    }

    impl ConfigFixture {
        fn new() -> Self {
            Self {
                directory: tempdir().expect("temp dir creation is expected to not fail"),
            }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.path().join(name)
        }

        fn write_secret(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.path(name);
            write_secret_file(&path, contents);
            path
        }

        fn valid_text(&self) -> String {
            format!(
                "port = 6666\ndemo = false\n\n[phanes_client]\nurl = \"https://phanes.example\"\nclient_gnosis_private_key = \"{DUMMY_KEY}\"\nphanes_gnosis_public_key = \"{DUMMY_KEY}\"\n"
            )
        }

        fn load(&self, text: &str) -> Result<MarketDataProviderConfig> {
            MarketDataProviderConfig::load(&self.write_secret("market_data_provider.toml", text))
        }
    }

    /// Each URL prints on one line; every key and Bark credential prints as `[REDACTED]`.
    #[test]
    fn debug_output_shows_urls_on_one_line_and_redacts_secrets() {
        const BARK_DEVICE_KEY: &str = "龙族科技";
        const BARK_ENCRYPTION_KEY: &str = "望月者计划";
        let fixture = ConfigFixture::new();
        let valid_text = fixture.valid_text();
        let text = format!(
            "{valid_text}\n[bark]\ndevice_key = \"{BARK_DEVICE_KEY}\"\nencryption_key = \"{BARK_ENCRYPTION_KEY}\"\n"
        );
        let config = fixture.load(&text).expect("the config is valid");
        let expected = r#"MarketDataProviderConfig {
    port: 6666,
    demo: false,
    phanes_client: PhanesClientConfig {
        url: https://phanes.example/,
        client_gnosis_private_key: SecretBox<[u8; 32]>([REDACTED]),
        phanes_gnosis_public_key: SecretBox<[u8; 32]>([REDACTED]),
    },
    bark: Some(
        BarkConfig {
            server: https://api.day.app/,
            device_key: SecretBox<str>([REDACTED]),
            encryption_key: SecretBox<str>([REDACTED]),
        },
    ),
}"#;
        assert_eq!(expected, format!("{config:#?}"));
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
            &format!("phanes_gnosis_public_key = \"{DUMMY_KEY}\""),
            "phanes_gnosis_public_key = \"short\"",
        );
        assert!(fixture.load(&text).is_err());
    }

    #[test]
    fn world_readable_config_is_refused() {
        let fixture = ConfigFixture::new();
        let path = fixture.write_secret("market_data_provider.toml", &fixture.valid_text());
        set_permissions(&path, Permissions::from_mode(WORLD_READABLE_MODE))
            .expect("setting permissions is expected to not fail");
        let error = MarketDataProviderConfig::load(&path)
            .expect_err("a world-readable config is expected to be refused");
        assert!(error.to_string().contains("chmod 600"), "{error}");
    }

    #[test]
    fn missing_config_is_refused() {
        let fixture = ConfigFixture::new();
        assert!(MarketDataProviderConfig::load(&fixture.path("absent.toml")).is_err());
    }
}

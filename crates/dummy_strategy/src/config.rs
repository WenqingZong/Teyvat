//! The strategy's TOML config, read once at startup; see `deploy/dummy_strategy/strategy.example.toml`.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Result, ensure};
use serde::Deserialize;

use layline::provider_api::topic_set;
use layline::secrets::read_secret_toml;
use layline::types::{StrategyName, Topic};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    pub strategy_name: StrategyName,
    /// Where the market data provider's HTTP API listens.
    pub provider_address: SocketAddr,
    /// Trade on the venues' demo accounts instead of live ones, on their demo market data; the
    /// provider refuses a strategy whose `demo` differs from its own.
    pub demo: bool,
    /// Exchange, then instrument, then market data types, as in the provider's HTTP API.
    #[serde(with = "topic_set")]
    pub topics: HashSet<Topic>,
}

impl StrategyConfig {
    /// The file must be chmod 600, like every service config.
    pub fn load(path: &Path) -> Result<Self> {
        let config: Self = read_secret_toml(path)?;
        ensure!(
            !config.topics.is_empty(),
            "{} subscribes to no topics",
            path.display()
        );
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use layline::testing::{test_instrument, test_topic, write_secret_file};
    use layline::types::{Currency, Exchange, MarketDataType, UnderlyingType};

    const MINIMAL_CONFIG: &str = r#"
strategy_name = "test_strategy"
provider_address = "127.0.0.1:6666"
demo = true

[topics.OKX]
ETH-USDC-PERP = ["TRADE"]
"#;

    fn load(toml_content: &str) -> Result<StrategyConfig> {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let path = directory.path().join("strategy.toml");
        write_secret_file(&path, toml_content);
        StrategyConfig::load(&path)
    }

    #[test]
    fn config_reads_every_field() {
        let config = load(MINIMAL_CONFIG).expect("the config is valid");
        assert_eq!("test_strategy", config.strategy_name);
        assert_eq!(
            "127.0.0.1:6666"
                .parse::<SocketAddr>()
                .expect("the address is valid"),
            config.provider_address
        );
        assert!(config.demo);
        let okx_ether_perpetual_trade = test_topic(
            test_instrument(
                Exchange::Okx,
                Currency::Eth,
                Currency::Usdc,
                UnderlyingType::Perpetual,
            ),
            MarketDataType::Trade,
        );
        assert_eq!(HashSet::from([okx_ether_perpetual_trade]), config.topics);
    }

    #[test]
    fn unknown_field_is_refused() {
        assert!(load(&format!("unknown = 1\n{MINIMAL_CONFIG}")).is_err());
    }

    #[test]
    fn misspelled_instrument_is_refused() {
        let misspelled = MINIMAL_CONFIG.replace("ETH-USDC-PERP", "ETH-USDC-PERPETUAL");
        let error = load(&misspelled).expect_err("an unknown underlying type is refused");
        assert!(error.to_string().contains("PERPETUAL"), "{error}");
    }

    #[test]
    fn config_without_topics_is_refused() {
        let without_topics = MINIMAL_CONFIG.replace("ETH-USDC-PERP = [\"TRADE\"]", "");
        let error = load(&without_topics).expect_err("no topics is refused");
        assert!(error.to_string().contains("no topics"), "{error}");
    }
}

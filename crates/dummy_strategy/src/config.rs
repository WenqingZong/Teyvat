//! The strategy's TOML config, read once at startup; see `deploy/dummy_strategy/strategy.example.toml`.

use std::collections::HashSet;
use std::fmt::{Debug, Formatter, Result as FormatResult, from_fn};
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::Path;

use anyhow::{Result, ensure};
use serde::Deserialize;
use tracing::info;

use layline::provider_api::topic_set;
use layline::secrets::read_secret_toml;
use layline::types::{StrategyName, Topic};

#[derive(Deserialize)]
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
    /// How often the process logs its own CPU, memory, threads and TCP traffic.
    pub process_metrics_interval_seconds: NonZeroU64,
}

/// Written by hand so each topic prints on one line as its `Display`, sorted by it, rather than
/// as nested structs in `HashSet` order.
impl Debug for StrategyConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let Self {
            strategy_name,
            provider_address,
            demo,
            topics,
            process_metrics_interval_seconds,
        } = self;
        let mut topic_names: Vec<String> = topics.iter().map(Topic::to_string).collect();
        topic_names.sort();
        let unquoted_topic_names = topic_names
            .iter()
            .map(|topic_name| from_fn(move |formatter| formatter.write_str(topic_name)));
        formatter
            .debug_struct("StrategyConfig")
            .field("strategy_name", strategy_name)
            .field("provider_address", provider_address)
            .field("demo", demo)
            .field(
                "topics",
                &from_fn(|formatter| {
                    formatter
                        .debug_list()
                        .entries(unquoted_topic_names.clone())
                        .finish()
                }),
            )
            .field(
                "process_metrics_interval_seconds",
                process_metrics_interval_seconds,
            )
            .finish()
    }
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
        let path = path.display();
        info!("loaded {path}: {config:#?}");
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
process_metrics_interval_seconds = 10

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
        assert_eq!(10, config.process_metrics_interval_seconds.get());
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

    /// Topics print one per line, sorted by name, whatever order the file and the set hold them in.
    #[test]
    fn debug_output_lists_topics_sorted_one_per_line() {
        let config = load(&format!(
            "{MINIMAL_CONFIG}ETH-USDC-SPOT = [\"TRADE\", \"BBO\"]\n\n[topics.Binance]\nBTC-USDT-PERP = [\"BBO\"]\n"
        ))
        .expect("the config is valid");
        let expected = r#"StrategyConfig {
    strategy_name: "test_strategy",
    provider_address: 127.0.0.1:6666,
    demo: true,
    topics: [
        Binance/BTC-USDT-PERP/BBO,
        OKX/ETH-USDC-PERP/TRADE,
        OKX/ETH-USDC-SPOT/BBO,
        OKX/ETH-USDC-SPOT/TRADE,
    ],
    process_metrics_interval_seconds: 10,
}"#;
        assert_eq!(expected, format!("{config:#?}"));
    }

    #[test]
    fn unknown_field_is_refused() {
        assert!(load(&format!("unknown = 1\n{MINIMAL_CONFIG}")).is_err());
    }

    #[test]
    fn zero_process_metrics_interval_is_refused() {
        let zero_interval = MINIMAL_CONFIG.replace(
            "process_metrics_interval_seconds = 10",
            "process_metrics_interval_seconds = 0",
        );
        assert!(load(&zero_interval).is_err());
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

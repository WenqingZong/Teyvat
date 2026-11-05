use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing::error;

use dummy_strategy::client::subscribe;
use dummy_strategy::config::StrategyConfig;
use dummy_strategy::latency_bench::LatencyBench;
use dummy_strategy::strategy::Strategy;
use layline::logging::init_tracing;
use layline::shared_memory::MarketDataReader;

#[derive(Parser)]
#[command(
    about = "Development stand-in strategy: reads the topics of its config and logs latency percentiles"
)]
struct Arguments {
    /// TOML config, chmod 600: strategy name, provider address, demo and topics.
    #[arg(long, default_value = "strategy.toml")]
    config: PathBuf,
    /// How often to log latency percentiles over the samples since the last report.
    #[arg(long, default_value_t = 60)]
    report_interval_seconds: u64,
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    let _guard = init_tracing(); // 持有 WorkerGuard，确保退出前 flush
    match run(
        &arguments.config,
        Duration::from_secs(arguments.report_interval_seconds),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!("fatal: {error:?}"); // {:?} 保留完整 context 链
            ExitCode::FAILURE
        }
    }
}

fn run(config_path: &Path, report_interval: Duration) -> Result<()> {
    let StrategyConfig {
        strategy_name,
        provider_address,
        demo,
        topics,
    } = StrategyConfig::load(config_path)?;
    let subscriptions = subscribe(provider_address, strategy_name, demo, topics)?;
    let mut strategy = LatencyBench::new(report_interval);
    MarketDataReader::open(&subscriptions)?.run(|data, received_at_nanoseconds| {
        strategy.on_market_data(&data, received_at_nanoseconds)
    })
}

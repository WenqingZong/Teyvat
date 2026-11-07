use std::panic::set_hook;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;

use dummy_strategy::client::subscribe;
use dummy_strategy::config::StrategyConfig;
use dummy_strategy::latency_bench::LatencyBench;
use dummy_strategy::strategy::Strategy;
use layline::build::{GIT_COMMIT_HASH, build_profile};
use layline::logging::init_tracing;
use layline::shared_memory::MarketDataReader;

const FAILURE_EXIT_CODE: i32 = 1;

/// Dropping the guard flushes buffered log lines; `exit` runs no destructors, so it is dropped by hand.
static LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);

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

fn main() {
    let arguments = Arguments::parse();
    *LOG_GUARD
        .lock()
        .expect("nothing panics while holding LOG_GUARD") = Some(init_tracing());
    exit_on_panic();
    let build_profile = build_profile();
    info!("{build_profile} build of commit {GIT_COMMIT_HASH}");
    let report_interval = Duration::from_secs(arguments.report_interval_seconds);
    if let Err(error) = run(&arguments.config, report_interval) {
        // `{:?}` keeps the whole context chain.
        exit_with_error(&format!("fatal: {error:?}"));
    }
}

/// A panic on any thread, such as the resubscribing or the reporting one, stops the whole strategy.
fn exit_on_panic() {
    set_hook(Box::new(|panic_info| {
        exit_with_error(&format!("fatal panic: {panic_info}"))
    }));
}

fn exit_with_error(message: &str) -> ! {
    error!("{message}");
    if let Ok(mut log_guard_option) = LOG_GUARD.lock() {
        drop(log_guard_option.take());
    }
    exit(FAILURE_EXIT_CODE);
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

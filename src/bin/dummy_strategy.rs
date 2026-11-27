use std::collections::HashMap;
use std::panic::set_hook;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::mpsc::channel;
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;

use dummy_strategy::client::ProviderSession;
use dummy_strategy::config::StrategyConfig;
use dummy_strategy::latency_bench::LatencyBench;
use dummy_strategy::strategy::Strategy;
use layline::build::{GIT_COMMIT_HASH, build_profile};
use layline::celestia::client::{ReporterConfig, SAMPLE_CHANNEL_CAPACITY, run_celestia_reporter};
use layline::logging::init_tracing;
use layline::process_metrics::run_process_monitor;
use layline::shared_memory::MarketDataReader;
use layline::shutdown::shutdown_signal;
use layline::types::{MarketData, Topic};

const SUCCESS_EXIT_CODE: i32 = 0;
const FAILURE_EXIT_CODE: i32 = 1;
/// Everything but the hot path runs as tasks on this one thread: none of it is busy.
const BACKGROUND_WORKER_THREADS: usize = 1;
const BACKGROUND_THREAD_NAME: &str = "background";
/// One sample of each topic per minute is enough for a human to check the data.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);

/// Dropping the guard flushes buffered log lines; `exit` runs no destructors, so it is dropped by hand.
static LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);

/// Logs each topic's newest message at most once per `SNAPSHOT_INTERVAL`, so a human can check the
/// data without the high-frequency streams flooding the log.
#[derive(Default)]
struct DataSnapshots {
    last_printed_at: HashMap<Topic, Instant>,
}

impl DataSnapshots {
    fn print_if_due(&mut self, data: &MarketData, now: Instant) {
        let topic = data.topic();
        if self
            .last_printed_at
            .get(&topic)
            .is_none_or(|last| now.duration_since(*last) >= SNAPSHOT_INTERVAL)
        {
            info!("{topic}: {data:?}");
            self.last_printed_at.insert(topic, now);
        }
    }
}

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

/// A panic anywhere, the hot path or a background task such as resubscribing, stops the whole
/// strategy: the hook runs before tokio would only end the panicking task.
fn exit_on_panic() {
    set_hook(Box::new(|panic_info| {
        exit_with_error(&format!("fatal panic: {panic_info}"))
    }));
}

fn exit_with_error(message: &str) -> ! {
    error!("{message}");
    flush_logs_and_exit(FAILURE_EXIT_CODE);
}

fn flush_logs_and_exit(code: i32) -> ! {
    if let Ok(mut log_guard_option) = LOG_GUARD.lock() {
        drop(log_guard_option.take());
    }
    exit(code);
}

fn run(config_path: &Path, report_interval: Duration) -> Result<()> {
    let StrategyConfig {
        strategy_name,
        provider_address,
        demo,
        topics,
        process_metrics_interval_seconds,
        celestia_client,
    } = StrategyConfig::load(config_path)?;
    // Phanes' log names the process after the strategy.
    let reporter_config = ReporterConfig::new(celestia_client, strategy_name.clone())?;
    // The main thread polls the rings; the background runtime does everything else. It lives as
    // long as `run`, which returns only on a fatal error.
    let background = RuntimeBuilder::new_multi_thread()
        .worker_threads(BACKGROUND_WORKER_THREADS)
        .thread_name(BACKGROUND_THREAD_NAME)
        .enable_all()
        .build()?;
    let process_metrics_interval = Duration::from_secs(process_metrics_interval_seconds.get());
    let (sample_sender, sample_receiver) = channel(SAMPLE_CHANNEL_CAPACITY);
    background.spawn(run_process_monitor(
        process_metrics_interval,
        Some(sample_sender),
    ));
    let reporter = run_celestia_reporter(
        reporter_config,
        process_metrics_interval,
        sample_receiver,
        shutdown_signal(),
    );
    // The reporter stops on SIGINT or SIGTERM once it has told Phanes why; the strategy stops with
    // it, from here because the main thread never leaves the ring loop.
    background.spawn(async {
        reporter.await;
        info!("shutting down");
        flush_logs_and_exit(SUCCESS_EXIT_CODE);
    });
    let provider_session =
        ProviderSession::new(provider_address, strategy_name, demo, topics.into())?;
    let instrument_rules = provider_session.instrument_rules();
    let subscriptions = background.block_on(provider_session.subscribe())?;
    let initial_instrument_count = instrument_rules.borrow().len();
    info!("received instrument rules for {initial_instrument_count} instruments");
    background.spawn(provider_session.resubscribe_forever(subscriptions.clone()));
    let mut strategy = LatencyBench::new(report_interval, background.handle());
    let mut snapshots = DataSnapshots::default();
    MarketDataReader::open(&subscriptions)?.run(|data, received_at_nanoseconds| {
        snapshots.print_if_due(&data, Instant::now());
        strategy.on_market_data(&data, received_at_nanoseconds)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use layline::testing::{test_bbo, test_instrument, test_topic};
    use layline::types::{Currency, Exchange, MarketDataType, UnderlyingType};

    #[test]
    fn snapshots_are_limited_per_topic() {
        let topic = test_topic(
            test_instrument(
                Exchange::Binance,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Perpetual,
            ),
            MarketDataType::Bbo,
        );
        let data = MarketData::Bbo(test_bbo(topic.instrument, 1));
        let mut snapshots = DataSnapshots::default();
        let start = Instant::now();

        snapshots.print_if_due(&data, start);
        assert_eq!(Some(&start), snapshots.last_printed_at.get(&topic));

        snapshots.print_if_due(&data, start + Duration::from_secs(1));
        assert_eq!(Some(&start), snapshots.last_printed_at.get(&topic));

        let due = start + SNAPSHOT_INTERVAL;
        snapshots.print_if_due(&data, due);
        assert_eq!(Some(&due), snapshots.last_printed_at.get(&topic));
    }
}

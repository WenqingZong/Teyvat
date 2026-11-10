use std::panic::{panic_any, set_hook};
use std::path::PathBuf;
use std::process::{ExitCode, exit};
use std::sync::Mutex;
use std::time::Duration;

use clap::Parser;
use tokio::sync::mpsc::channel;
use tokio::{select, spawn};
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;

use layline::build::{GIT_COMMIT_HASH, build_profile};
use layline::celestia::client::{ReporterConfig, SAMPLE_CHANNEL_CAPACITY, run_celestia_reporter};
use layline::logging::init_tracing;
use layline::process_metrics::run_process_monitor;
use layline::shutdown::shutdown_signal;
use market_data_provider::config::MarketDataProviderConfig;
use market_data_provider::exit_reason::ExitReason;
use market_data_provider::server::run;

/// How Phanes' log names this process.
const PROCESS_NAME: &str = "market_data_provider";

/// Dropping the guard flushes buffered log lines, on a normal exit and in the panic hook alike.
static LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);

#[derive(Parser)]
#[command(about = "Publishes exchange market data to strategies over shared memory")]
struct Arguments {
    /// TOML config, chmod 600 since it holds the Bark credentials.
    #[arg(long, default_value = "market_data_provider.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();
    *LOG_GUARD
        .lock()
        .expect("nothing panics while holding LOG_GUARD") = Some(init_tracing());
    exit_on_panic();
    let build_profile = build_profile();
    info!("{build_profile} build of commit {GIT_COMMIT_HASH}");
    let config =
        MarketDataProviderConfig::load(&arguments.config).unwrap_or_else(|error| fatal(error));
    let reporter_config =
        ReporterConfig::new(config.celestia_client.clone(), PROCESS_NAME.to_owned())
            .unwrap_or_else(|error| fatal(error.into()));
    let process_metrics_interval =
        Duration::from_secs(config.process_metrics_interval_seconds.get());
    let (sample_sender, sample_receiver) = channel(SAMPLE_CHANNEL_CAPACITY);
    spawn(run_process_monitor(
        process_metrics_interval,
        Some(sample_sender),
    ));
    let reporter = spawn(run_celestia_reporter(
        reporter_config,
        process_metrics_interval,
        sample_receiver,
        shutdown_signal(),
    ));
    // The reporter stops on SIGINT or SIGTERM once it has told Phanes why; the provider stops with it.
    select! {
        run_result = run(config) => run_result.unwrap_or_else(|error| fatal(error)),
        reporter_result = reporter => reporter_result.expect("a panic exits through the hook"),
    }
    info!("shutting down");
    flush_logs();
    ExitCode::SUCCESS
}

/// Startup failures stop the process at once; nothing runs with a half-valid configuration.
fn fatal(error: anyhow::Error) -> ! {
    error!("fatal: {error:?}");
    panic_any(ExitReason::FatalError);
}

/// A panic on any thread or task, the process monitor's included, stops the whole provider. A panic raised with an `ExitReason` payload,
/// such as an exchange worker failing to get its API key, exits with that reason's code.
fn exit_on_panic() {
    set_hook(Box::new(|panic_info| {
        let exit_reason = match panic_info.payload().downcast_ref::<ExitReason>() {
            Some(exit_reason) => {
                let location = panic_info
                    .location()
                    .map_or_else(|| "unknown location".to_owned(), ToString::to_string);
                error!("fatal: {exit_reason:?} at {location}");
                *exit_reason
            }
            None => {
                error!("fatal panic: {panic_info}");
                ExitReason::UnexpectedPanic
            }
        };
        flush_logs();
        exit(i32::from(exit_reason.code()));
    }));
}

fn flush_logs() {
    if let Ok(mut guard) = LOG_GUARD.lock() {
        drop(guard.take());
    }
}

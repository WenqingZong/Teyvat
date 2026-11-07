use std::panic::{panic_any, set_hook};
use std::path::PathBuf;
use std::process::{ExitCode, exit};
use std::sync::Mutex;

use clap::Parser;
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;

use layline::build::{GIT_COMMIT_HASH, build_profile};
use layline::logging::init_tracing;
use market_data_provider::config::MarketDataProviderConfig;
use market_data_provider::exit_reason::ExitReason;
use market_data_provider::server::run;

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
    run(config).await.unwrap_or_else(|error| fatal(error));
    ExitCode::SUCCESS
}

/// Startup failures stop the process at once; nothing runs with a half-valid configuration.
fn fatal(error: anyhow::Error) -> ! {
    error!("fatal: {error:?}");
    panic_any(ExitReason::FatalError);
}

/// A panic on any thread stops the whole provider. A panic raised with an `ExitReason` payload,
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

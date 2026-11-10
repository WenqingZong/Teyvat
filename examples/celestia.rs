//! Celestia smoke test: a process that monitors itself, as every Teyvat service does, and reports
//! its samples to Phanes through `layline::celestia::client`. Unacknowledged samples stay in
//! memory (up to an hour) and are resent after a reconnect.
//!
//! cargo run --example celestia -- --url https://phanes.wenqing.dev \
//!     --client-private-key-file celestia_client.key --celestia-public-key <base64>

use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use secrecy::SecretString;
use tokio::spawn;
use tokio::sync::mpsc::channel;
use tracing::info;
use url::Url;

use layline::celestia::client::{
    CelestiaClientConfig, ReporterConfig, SAMPLE_CHANNEL_CAPACITY, run_celestia_reporter,
};
use layline::logging::init_tracing;
use layline::noise::decode_key;
use layline::process_metrics::run_process_monitor;
use layline::secrets::read_secret_file;
use layline::shutdown::shutdown_signal;

#[derive(Parser)]
struct Arguments {
    /// Phanes, as Cloudflare exposes it; `/celestia` is appended.
    #[arg(long)]
    url: Url,
    /// chmod 600; the machine's Celestia private key in base64.
    #[arg(long)]
    client_private_key_file: PathBuf,
    #[arg(long)]
    celestia_public_key: SecretString,
    /// How Phanes' log names this process.
    #[arg(long, default_value = "celestia_example")]
    process_name: String,
    #[arg(long, default_value = "10")]
    process_metrics_interval_seconds: NonZeroU64,
}

/// One thread runs everything, as a service's background runtime would.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _log_guard = init_tracing();
    let arguments = Arguments::parse();
    let celestia_client = CelestiaClientConfig {
        url: arguments.url,
        client_celestia_private_key: decode_key(&read_secret_file(
            &arguments.client_private_key_file,
        )?)?,
        phanes_celestia_public_key: decode_key(&arguments.celestia_public_key)?,
    };
    let reporter_config = ReporterConfig::new(celestia_client, arguments.process_name)?;
    let interval = Duration::from_secs(arguments.process_metrics_interval_seconds.get());
    let (sample_sender, sample_receiver) = channel(SAMPLE_CHANNEL_CAPACITY);
    spawn(run_process_monitor(interval, Some(sample_sender)));
    run_celestia_reporter(
        reporter_config,
        interval,
        sample_receiver,
        shutdown_signal(),
    )
    .await;
    info!("exiting");
    Ok(())
}

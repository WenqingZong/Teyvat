//! Simplified Osaka process monitor: samples one process every second and streams it to Phanes.
//! Unacknowledged batches stay in memory (up to an hour) and are resent after a reconnect.
//!
//! cargo run --example celestia -- --url https://phanes.wenqing.dev \
//!     --client-private-key-file celestia_client.key --celestia-public-key <base64> \
//!     --systemd-unit my_app.service      # or --process-id <pid>, network then reads 0

use std::fs::read_to_string;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::Parser;
use secrecy::{SecretBox, SecretString};
use tokio::signal::ctrl_c;
use tokio::spawn;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::{info, warn};
use url::Url;

use client::{Outbox, build_celestia_url, run_session};
use layline::logging::init_tracing;
use layline::noise::{StaticKey, decode_private_key, decode_public_key};
use layline::time::since_unix_epoch;
use phanes::celestia::proto::ProcessSample;
use sampler::{Sampler, Target};

mod client;
mod sampler;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
/// One hour of samples at one per second.
const BUFFERED_SAMPLES: usize = 3600;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Parser)]
struct Arguments {
    #[arg(long)]
    url: Url,
    #[arg(long)]
    client_private_key_file: PathBuf,
    #[arg(long)]
    celestia_public_key: String,
    #[arg(long)]
    process_id: Option<u32>,
    /// Needs `IPAccounting=yes` on the unit for network rates.
    #[arg(long)]
    systemd_unit: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _log_guard = init_tracing();
    let arguments = Arguments::parse();
    let target = match (arguments.process_id, arguments.systemd_unit) {
        (Some(process_id), None) => Target::ProcessId(process_id),
        (None, Some(unit)) => Target::SystemdUnit(unit),
        (Some(_), Some(_)) => bail!("--process-id and --systemd-unit are mutually exclusive"),
        (None, None) => bail!("--process-id or --systemd-unit is required"),
    };
    let encoded_private_key =
        SecretString::from(read_to_string(&arguments.client_private_key_file)?);
    let client_private_key = decode_private_key(&encoded_private_key)?;
    let celestia_public_key = decode_public_key(&arguments.celestia_public_key)?;
    let url = build_celestia_url(&arguments.url)?;

    let (sender, mut receiver) = channel(BUFFERED_SAMPLES);
    spawn(sample_forever(Sampler::new(target), sender));

    // Seeding from the clock in milliseconds keeps sequences increasing across restarts.
    let first_sequence =
        u64::try_from(since_unix_epoch().as_millis()).expect("milliseconds since 1970 fit in u64");
    let mut outbox = Outbox::new(first_sequence, BUFFERED_SAMPLES);
    // Raced as a whole so ctrl-c also works while waiting to reconnect.
    tokio::select! {
        () = reconnect_forever(&url, &client_private_key, &celestia_public_key, &mut outbox, &mut receiver) => {}
        result = ctrl_c() => result?,
    }
    let unacknowledged_batches = outbox.len();
    info!("ctrl-c, exiting with {unacknowledged_batches} unacknowledged batches");
    Ok(())
}

async fn reconnect_forever(
    url: &Url,
    client_private_key: &SecretBox<StaticKey>,
    celestia_public_key: &StaticKey,
    outbox: &mut Outbox,
    receiver: &mut Receiver<ProcessSample>,
) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let started = Instant::now();
        if let Err(error) = run_session(
            url,
            client_private_key,
            celestia_public_key,
            outbox,
            receiver,
        )
        .await
        {
            warn!("Celestia session failed: {error}");
        }
        // A session that lasted a while means the link was healthy, so retry quickly.
        if started.elapsed() > MAX_BACKOFF {
            backoff = INITIAL_BACKOFF;
        }
        info!("reconnecting in {backoff:?}");
        sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn sample_forever(mut sampler: Sampler, sender: Sender<ProcessSample>) {
    let mut ticker = interval(SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match sampler.sample().await {
            Ok(Some(sample)) => match sender.try_send(sample) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => warn!("sample buffer full, dropping sample"),
                Err(TrySendError::Closed(_)) => return,
            },
            Ok(None) => {}
            Err(error) => warn!("sampling failed: {error}"),
        }
    }
}

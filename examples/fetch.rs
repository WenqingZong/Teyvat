//! Minimal client playing the Osaka side, for end-to-end testing and as a reference
//! for the real client. Logs the received gnosis, so only use it with dummy keys.
//!
//! cargo run --example fetch -- --url https://phanes.wenqing.dev \
//!     --client-private-key-file client.key --gnosis-public-key <base64> --gnosis-id binance_demo

use std::fs::read_to_string;
use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;
use reqwest::Client;
use secrecy::SecretString;
use serde_json::Value;
use tracing::{error, info, warn};
use url::Url;

use layline::gnosis::{GNOSIS_PATH, GnosisId, Initiator, Request, Response};
use layline::logging::init_tracing;
use layline::noise::decode_key;
use layline::time::get_current_timestamp_nanoseconds;

#[derive(Parser)]
struct Arguments {
    #[arg(long)]
    url: Url,
    #[arg(long)]
    client_private_key_file: PathBuf,
    #[arg(long)]
    gnosis_public_key: SecretString,
    /// Such as `binance_demo`; see `GnosisId`.
    #[arg(long)]
    gnosis_id: GnosisId,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _log_guard = init_tracing();
    let arguments = Arguments::parse();
    let encoded_private_key =
        SecretString::from(read_to_string(&arguments.client_private_key_file)?);
    let client_private_key = decode_key(&encoded_private_key)?;
    let gnosis_public_key = decode_key(&arguments.gnosis_public_key)?;

    let (initiator, first_message) = Initiator::write_request(
        &client_private_key,
        &gnosis_public_key,
        &Request::new(arguments.gnosis_id, get_current_timestamp_nanoseconds()),
    )?;
    let http_response = Client::new()
        .post(arguments.url.join(GNOSIS_PATH)?)
        .body(first_message)
        .send()
        .await?;
    let status = http_response.status();
    if !status.is_success() {
        error!("server answered {status}");
        bail!("server answered {status}");
    }
    let second_message = http_response.bytes().await?;
    let gnosis_id = arguments.gnosis_id;
    // Any id may be asked for, so the gnosis is read as plain JSON rather than as its key type.
    match initiator.read_response::<Value>(&second_message)? {
        Response::Granted { gnosis } => info!("granted {gnosis_id}: {gnosis}"),
        Response::Denied { reason } => warn!("denied {gnosis_id}: {reason}"),
    }
    Ok(())
}

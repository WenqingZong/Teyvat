//! Sends a test push using the `[bark]` section of phanes.toml. Usage:
//! cargo run --example bark_push -- [path/to/phanes.toml] ["message"]

use std::env::args;
use std::path::PathBuf;
use tracing::info;

use anyhow::{Context, Result};

use layline::bark::{BarkMessage, BarkNotifier};
use layline::logging::init_tracing;
use phanes::config::PhanesConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let _log_guard = init_tracing();
    let mut arguments = args().skip(1);
    let config_path = PathBuf::from(arguments.next().unwrap_or_else(|| "phanes.toml".to_owned()));
    let body = arguments.next().unwrap_or_else(|| "test".to_owned());

    let config = PhanesConfig::load(&config_path)?;
    let printable_path = config_path.display();
    let bark_config = config
        .bark
        .with_context(|| format!("no [bark] section in {printable_path}"))?;
    let notifier = BarkNotifier::new(bark_config)?;
    notifier
        .send(&BarkMessage {
            title: "Phanes Test Message".into(),
            body,
            sound: "birdsong".into(),
            ..Default::default()
        })
        .await?;
    info!("push sent");
    Ok(())
}

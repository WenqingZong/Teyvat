//! Sends a test push using the `[bark]` section of phanes.toml. Usage:
//! cargo run --example bark_push -- [path/to/phanes.toml] ["message"]

use log::info;
use std::env::args;
use std::path::PathBuf;

use anyhow::{Context, Result};

use phanes::bark::{BarkMessage, BarkNotifier};
use phanes::config::PhanesConfig;
use phanes::utils::init_logger;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
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

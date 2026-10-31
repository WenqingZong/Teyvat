pub mod api;
mod coordinator;
mod http;
mod registry;

use crate::config::ProviderConfig;
use crate::exchanges::{ConnectorContext, ExchangeRouter};
use crate::types::Topic;
use anyhow::{Context, Result};
use coordinator::Coordinator;
use std::fs::create_dir_all;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::Mutex;
use tokio::time::interval;

const LEASE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Fails fast if another provider already listens on the address.
pub async fn run(config: ProviderConfig) -> Result<()> {
    let listener = TcpListener::bind(config.listen_address)
        .await
        .with_context(|| format!("failed to listen on {}", config.listen_address))?;
    serve(config, listener).await
}

/// Like `run`, on a listener the caller already bound; `config.listen_address` is ignored.
pub async fn serve(config: ProviderConfig, listener: TcpListener) -> Result<()> {
    create_dir_all(&config.root_directory)
        .with_context(|| format!("failed to create {}", config.root_directory.display()))?;
    let context = ConnectorContext {
        phanes_client: config.phanes_client,
        notifier: config.notifier,
    };
    let router = ExchangeRouter::new(&config.root_directory, config.demo, context);
    let coordinator = Arc::new(Mutex::new(Coordinator::new(router, config.lease_duration)));
    select! {
        serve_result = http::serve(listener, coordinator.clone(), config.demo) => serve_result,
        () = expire_leases_periodically(coordinator) => Ok(()),
    }
}

/// Sorted topics for log lines, such as `[Binance/BTC-USDT-SPOT/BBO, OKX/BTC-USDT-PERP/TRADE]`.
fn format_topics<'topic>(topics: impl IntoIterator<Item = &'topic Topic>) -> String {
    let mut names: Vec<String> = topics.into_iter().map(Topic::to_string).collect();
    names.sort();
    format!("[{}]", names.join(", "))
}

async fn expire_leases_periodically(coordinator: Arc<Mutex<Coordinator>>) {
    let mut sweep = interval(LEASE_SWEEP_INTERVAL);
    loop {
        sweep.tick().await;
        coordinator.lock().await.expire_leases(Instant::now()).await;
    }
}

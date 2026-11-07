mod coordinator;
mod http;
mod registry;

use std::fs::create_dir_all;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::Mutex;
use tokio::time::interval;

use crate::DEFAULT_ROOT_DIRECTORY;
use crate::config::MarketDataProviderConfig;
use crate::exchanges::{ConnectorContext, ExchangeRouter};
use crate::phanes_client::PhanesClient;
use coordinator::Coordinator;
use layline::bark::Notifier;
use layline::build::RELEASE_BUILD;
use layline::types::Topic;

const LEASE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
/// How long a strategy keeps its topics without resubscribing, so restarting strategies do not churn the exchange.
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// Listens on `127.0.0.1:port` and publishes under `DEFAULT_ROOT_DIRECTORY`. Fails fast if another
/// provider already listens on the address.
pub async fn run(config: MarketDataProviderConfig) -> Result<()> {
    let listen_address = SocketAddr::from((Ipv4Addr::LOCALHOST, config.port));
    let listener = TcpListener::bind(listen_address)
        .await
        .with_context(|| format!("failed to listen on {listen_address}"))?;
    serve(
        config,
        listener,
        PathBuf::from(DEFAULT_ROOT_DIRECTORY),
        DEFAULT_LEASE_DURATION,
    )
    .await
}

/// Like `run`, on a listener the caller already bound (`config.port` is ignored), with the ring
/// directory and lease duration chosen by the caller. A release build without `[bark]` refuses to
/// start here.
pub async fn serve(
    config: MarketDataProviderConfig,
    listener: TcpListener,
    root_directory: PathBuf,
    lease_duration: Duration,
) -> Result<()> {
    let context = ConnectorContext {
        phanes_client: PhanesClient::new(config.phanes_client)?,
        notifier: Notifier::new(RELEASE_BUILD, config.bark)?,
    };
    create_dir_all(&root_directory)
        .with_context(|| format!("failed to create {}", root_directory.display()))?;
    let router = ExchangeRouter::new(&root_directory, config.demo, context);
    let coordinator = Arc::new(Mutex::new(Coordinator::new(router, lease_duration)));
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

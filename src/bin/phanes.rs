use std::any::Any;
use std::backtrace::Backtrace;
use std::future::pending;
use std::io::{Error as IoError, Write, stdout};
use std::net::SocketAddr;
use std::panic::set_hook;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use secrecy::{ExposeSecret, SecretSlice, SecretString};
use tokio::net::TcpListener;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::signal::ctrl_c;
use tokio::signal::unix::{SignalKind, signal};
use tokio::spawn;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::timeout;
use tracing::{error, info, warn};

use layline::bark::Notifier;
use layline::build::RELEASE_BUILD;
use layline::logging::init_tracing;
use layline::noise::{encode_key, generate_keypair};
use phanes::celestia::server::CelestiaState;
use phanes::client_filter::{CLIENT_IP_HEADER, ClientFilter};
use phanes::clients::ClientName;
use phanes::config::PhanesConfig;
use phanes::gnosis::server::GnosisState;
use phanes::notify::{self, Notification, enqueue};
use phanes::server;

const CRASH_NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Parser)]
#[command(about = "Phanes, the Control Center of a Quant Trading Bot")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run Phanes. Release builds push through the `[bark]` section of the config;
    /// debug builds log notifications instead.
    Serve {
        /// TOML config, chmod 600: it holds the private keys, the gnoses and the Bark credentials.
        #[arg(long, default_value = "phanes.toml")]
        config: PathBuf,
    },
    /// Print a fresh static X25519 key pair in base64, as configs hold keys: the private key goes
    /// into its owner's config only, the public key to whoever talks to it.
    Keygen,
}

#[derive(Debug, thiserror::Error)]
enum ServiceError {
    #[error(
        "listen_address {0} is not loopback: LAN devices could bypass Cloudflare and forge {CLIENT_IP_HEADER}"
    )]
    ListenAddressNotLoopback(SocketAddr),
    #[error("cannot listen on {address}: {source}")]
    Bind {
        address: SocketAddr,
        source: IoError,
    },
    #[error("server stopped: {0}")]
    Serve(IoError),
    #[error("server stopped unexpectedly")]
    Stopped,
}

fn main() -> ExitCode {
    let _log_guard = init_tracing();
    match Cli::parse().command {
        Command::Keygen => match print_key_pair() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                error!("keygen failed: {error}");
                ExitCode::FAILURE
            }
        },
        Command::Serve { config } => {
            let runtime = match RuntimeBuilder::new_multi_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    error!("cannot start tokio runtime: {error}");
                    return ExitCode::FAILURE;
                }
            };
            runtime.block_on(supervise(&config))
        }
    }
}

/// Written to the terminal, not logged: logs must never hold a private key.
fn print_key_pair() -> Result<()> {
    let keypair = generate_keypair()?;
    let private_key = SecretSlice::from(keypair.private);
    let encoded_private_key = SecretString::from(encode_key(private_key.expose_secret()));
    let encoded_public_key = encode_key(&keypair.public);
    let mut terminal = stdout().lock();
    writeln!(
        terminal,
        "private key: {}",
        encoded_private_key.expose_secret()
    )?;
    writeln!(terminal, "public key:  {encoded_public_key}")?;
    Ok(())
}

/// Runs the service and turns any panic or fatal error into a log entry, a push
/// notification and a non-zero exit code. Config errors can only be logged, since
/// the Bark credentials live in the config.
async fn supervise(config_path: &Path) -> ExitCode {
    let mut config = match PhanesConfig::load(config_path) {
        Ok(config) => config,
        Err(error) => {
            error!("cannot load config, refusing to start: {error}");
            return ExitCode::FAILURE;
        }
    };
    let notifier = match Notifier::new(RELEASE_BUILD, config.bark.take()) {
        Ok(notifier) => Arc::new(notifier),
        Err(error) => {
            error!("cannot set up Bark notifications, refusing to start: {error}");
            return ExitCode::FAILURE;
        }
    };
    if matches!(*notifier, Notifier::Log) {
        warn!("debug build: Bark disabled, notifications are logged only");
    }

    let (panic_sender, mut panic_receiver) = unbounded_channel();
    install_panic_hook(panic_sender);

    let mut service = spawn(run_service(config, Arc::clone(&notifier)));
    let crash_reason = tokio::select! {
        result = &mut service => match result {
            Ok(Ok(())) => ServiceError::Stopped.to_string(),
            Ok(Err(error)) => error.to_string(),
            Err(join_error) if join_error.is_panic() => {
                let payload = describe_panic_payload(join_error.into_panic().as_ref());
                format!("panic: {payload}")
            }
            Err(join_error) => format!("service task failed: {join_error}"),
        },
        Some(panic_message) = panic_receiver.recv() => format!("panic: {panic_message}"),
        signal = shutdown_signal() => {
            info!("received {signal}, shutting down");
            return ExitCode::SUCCESS;
        }
    };
    service.abort();
    report_crash(&notifier, crash_reason).await
}

async fn run_service(config: PhanesConfig, notifier: Arc<Notifier>) -> Result<(), ServiceError> {
    // The IP filter trusts CF-Connecting-IP, which only cloudflared on this machine may set.
    if RELEASE_BUILD && !config.listen_address.ip().is_loopback() {
        return Err(ServiceError::ListenAddressNotLoopback(
            config.listen_address,
        ));
    }

    let listener = TcpListener::bind(config.listen_address)
        .await
        .map_err(|source| ServiceError::Bind {
            address: config.listen_address,
            source,
        })?;

    let (notification_sender, notification_receiver) = unbounded_channel();
    spawn(notify::run_dispatcher(notifier, notification_receiver));

    let clients = Arc::new(config.clients);
    let gnosis_state = GnosisState::new(
        config.gnosis_private_key,
        Arc::clone(&clients),
        config.gnoses,
        config.max_clock_skew_seconds,
        notification_sender.clone(),
    );
    let celestia_state = CelestiaState::new(
        config.celestia_private_key,
        Arc::clone(&clients),
        notification_sender.clone(),
    );
    let client_filter = Arc::new(ClientFilter::new(
        Arc::clone(&clients),
        notification_sender.clone(),
    ));
    if !RELEASE_BUILD {
        warn!("debug build: client IP filter disabled");
    }

    let listen_address = config.listen_address;
    let client_names: Vec<&ClientName> = clients.keys().collect();
    info!("Phanes listening on {listen_address} for clients {client_names:?}");
    let listen_address = listen_address.to_string();
    enqueue(
        &notification_sender,
        Notification::Started { listen_address },
    );

    server::run(
        listener,
        Arc::new(gnosis_state),
        Arc::new(celestia_state),
        RELEASE_BUILD.then_some(client_filter),
    )
    .await
    .map_err(ServiceError::Serve)
}

fn install_panic_hook(panic_sender: UnboundedSender<String>) {
    set_hook(Box::new(move |panic_info| {
        let location = panic_info
            .location()
            .map(|location| location.to_string())
            .unwrap_or_else(|| "unknown location".to_owned());
        let payload = describe_panic_payload(panic_info.payload());
        let message = format!("{payload} at {location}");
        let backtrace = Backtrace::force_capture();
        error!("panic: {message}\n{backtrace}");
        if panic_sender.send(message).is_err() {
            error!("supervisor is gone; panic not reported");
        }
    }));
}

fn describe_panic_payload(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

async fn report_crash(notifier: &Notifier, reason: String) -> ExitCode {
    error!("Phanes service died: {reason}");
    let notification = Notification::Crashed { reason };
    match timeout(
        CRASH_NOTIFICATION_TIMEOUT,
        notifier.send(&notification.to_bark_message()),
    )
    .await
    {
        Ok(Ok(())) => info!("crash notification sent"),
        Ok(Err(error)) => error!("failed to send crash notification: {error}"),
        Err(_) => error!("timed out sending crash notification"),
    }
    ExitCode::FAILURE
}

async fn shutdown_signal() -> &'static str {
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            error!("cannot listen for SIGTERM: {error}");
            return match ctrl_c().await {
                Ok(()) => "ctrl-c",
                Err(_) => pending().await,
            };
        }
    };
    tokio::select! {
        _ = terminate.recv() => "SIGTERM",
        result = ctrl_c() => match result {
            Ok(()) => "ctrl-c",
            Err(error) => {
                error!("cannot listen for ctrl-c: {error}");
                pending().await
            }
        },
    }
}

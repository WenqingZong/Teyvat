use log::{error, info, warn};
use std::any::Any;
use std::backtrace::Backtrace;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::future::pending;
use std::io::{Error as IoError, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
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

use phanes::celestia::server::CelestiaState;
use phanes::client_filter::{CLIENT_IP_HEADER, ClientFilter, ClientsByIp};
use phanes::config::{PhanesConfig, PhanesConfigError};
use phanes::gnosis::server::{self as gnosis_server, GnosisState};
use phanes::noise::{encode_key, generate_keypair};
use phanes::notify::{self, Notification, Notifier};
use phanes::server::{self, ClientFilters, Service};
use phanes::utils::{RELEASE_BUILD, init_logger};

const CRASH_NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(15);
/// Owner read/write only.
const PRIVATE_KEY_FILE_MODE: u32 = 0o600;

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
        #[arg(long, default_value = "phanes.toml")]
        config: PathBuf,
    },
    /// Generate a static X25519 key pair: the private key is written to a new file
    /// (mode 600), the public key is logged.
    Keygen {
        #[arg(long)]
        private_key_file: PathBuf,
    },
}

#[derive(Debug, thiserror::Error)]
enum ServiceError {
    #[error(transparent)]
    Config(#[from] PhanesConfigError),
    #[error("invalid key id {0:?} in gnosis_service_config.allowed_key_ids")]
    InvalidAllowedKeyId(String),
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
    init_logger();
    match Cli::parse().command {
        Command::Keygen { private_key_file } => match generate_key_file(&private_key_file) {
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

fn generate_key_file(path: &Path) -> Result<()> {
    let keypair = generate_keypair()?;
    let private_key = SecretSlice::from(keypair.private);
    let encoded_private_key = SecretString::from(encode_key(private_key.expose_secret()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(PRIVATE_KEY_FILE_MODE)
        .open(path)?;
    let encoded_private_key = encoded_private_key.expose_secret();
    writeln!(file, "{encoded_private_key}")?;
    let printable_path = path.display();
    info!("private key written to {printable_path}");
    let encoded_public_key = encode_key(&keypair.public);
    info!("public key: {encoded_public_key}");
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
    let gnosis_private_key = config.load_gnosis_private_key()?;
    let gnosis_client_public_key = config.parse_gnosis_client_public_key_from_toml()?;
    let celestia_private_key = config.load_celestia_private_key()?;
    let celestia_client_public_key = config.parse_celestia_client_public_key_from_toml()?;
    let gnosis_clients = config.parse_gnosis_clients_from_toml()?;
    let celestia_clients = config.parse_celestia_clients_from_toml()?;
    // The IP filter trusts CF-Connecting-IP, which only cloudflared on this machine may set.
    if RELEASE_BUILD && !config.listen_address.ip().is_loopback() {
        return Err(ServiceError::ListenAddressNotLoopback(
            config.listen_address,
        ));
    }

    let mut allowed_key_ids = HashSet::new();
    for key_id in &config.gnosis_service_config.allowed_key_ids {
        if !gnosis_server::is_valid_key_id(key_id) {
            return Err(ServiceError::InvalidAllowedKeyId(key_id.clone()));
        }
        let key_file = config.api_keys_directory.join(format!("{key_id}.txt"));
        if !key_file.is_file() {
            let printable_path = key_file.display();
            warn!("allowed key {key_id} has no file at {printable_path}");
        }
        allowed_key_ids.insert(key_id.clone());
    }

    let listener = TcpListener::bind(config.listen_address)
        .await
        .map_err(|source| ServiceError::Bind {
            address: config.listen_address,
            source,
        })?;

    let (notification_sender, notification_receiver) = unbounded_channel();
    spawn(notify::run_dispatcher(notifier, notification_receiver));

    let gnosis_state = GnosisState::new(
        gnosis_private_key,
        gnosis_client_public_key,
        allowed_key_ids,
        config.api_keys_directory.clone(),
        config.max_clock_skew_seconds,
        notification_sender.clone(),
    );
    let celestia_state = CelestiaState::new(
        celestia_private_key,
        celestia_client_public_key,
        notification_sender.clone(),
    );
    let client_filters = ClientFilters {
        gnosis: build_client_filter(Service::Gnosis, gnosis_clients, &notification_sender),
        celestia: build_client_filter(Service::Celestia, celestia_clients, &notification_sender),
    };
    if !RELEASE_BUILD {
        warn!("debug build: client IP filter disabled");
    }

    let listen_address = config.listen_address;
    let gnosis_client_names = &config.gnosis_service_config.allowed_clients;
    let celestia_client_names = &config.celestia_service_config.allowed_clients;
    info!(
        "Phanes listening on {listen_address}: Gnosis for {gnosis_client_names:?}, Celestia for {celestia_client_names:?}"
    );
    if notification_sender
        .send(Notification::Started {
            listen_address: config.listen_address.to_string(),
        })
        .is_err()
    {
        error!("notification dispatcher is gone; startup push dropped");
    }

    server::run(
        listener,
        Arc::new(gnosis_state),
        Arc::new(celestia_state),
        RELEASE_BUILD.then_some(client_filters),
    )
    .await
    .map_err(ServiceError::Serve)
}

fn build_client_filter(
    service: Service,
    clients: ClientsByIp,
    notification_sender: &UnboundedSender<Notification>,
) -> Arc<ClientFilter> {
    Arc::new(ClientFilter::new(
        service,
        clients,
        notification_sender.clone(),
    ))
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
    match timeout(CRASH_NOTIFICATION_TIMEOUT, notifier.send(&notification)).await {
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

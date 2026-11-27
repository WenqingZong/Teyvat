use std::mem::size_of;

pub mod aggregation;
mod alert;
pub mod config;
mod exchanges;
pub mod exit_reason;
mod gnosis_client;
pub mod server;
mod time;
mod utils;

/// Where `server::run` creates the rings. Linux keeps them on tmpfs; every other platform is a
/// local development build without one, so the rings go under the repository's `tmp/`, which git
/// ignores. Production is always Linux.
#[cfg(target_os = "linux")]
pub const DEFAULT_ROOT_DIRECTORY: &str = "/dev/shm/market_data_provider";
#[cfg(not(target_os = "linux"))]
pub const DEFAULT_ROOT_DIRECTORY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tmp/market_data_provider"
);

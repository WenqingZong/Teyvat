mod alert;
mod bark;
pub mod config;
mod exchanges;
pub mod exit_reason;
pub mod logging;
mod phanes_client;
mod secrets;
pub mod server;
pub mod shared_memory;
#[cfg(test)]
mod testing;
mod time;
pub mod types;
pub mod utils;

pub const DEFAULT_ROOT_DIRECTORY: &str = "/dev/shm/market_data_provider";

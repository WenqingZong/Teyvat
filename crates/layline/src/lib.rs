//! LayLine (地脉): the code every Teyvat crate shares. Only code that two or more crates use lives
//! here; the top-level modules must not import each other in a cycle (see `tests/module_dependencies.rs`).

pub mod backoff;
pub mod bark;
pub mod build;
pub mod gnosis;
pub mod logging;
pub mod noise;
pub mod provider_api;
pub mod secrets;
pub mod shared_memory;
pub mod testing;
pub mod time;
pub mod types;

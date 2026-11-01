//! Development stand-in strategy: subscribes through the provider's HTTP API, reads the rings and
//! measures latency. The binary is `src/bin/dummy_strategy.rs` at the workspace root.

pub mod client;
pub mod latency_bench;
pub mod strategy;

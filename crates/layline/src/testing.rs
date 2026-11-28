//! Fixtures for the tests of every crate, so each test does not spell out the same instrument.
//! Production code never calls these.
use std::fs::{Permissions, set_permissions, write};
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use rust_decimal::{Decimal, dec};
use secrecy::SecretBox;

use crate::noise::{StaticKey, generate_keypair};
use crate::process_metrics::{MemoryUsage, ProcessSample, ThreadSample};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::{
    Bbo, Currency, Exchange, Instrument, LatencyMetric, MarketDataType, ORDER_BOOK_DEPTH,
    OrderBook, PriceLevel, Side, StrategyName, Subscription, Topic, Trade, UnderlyingType,
};

pub fn test_instrument(
    exchange: Exchange,
    base: Currency,
    quote: Currency,
    underlying_type: UnderlyingType,
) -> Instrument {
    Instrument {
        exchange,
        base,
        quote,
        underlying_type,
    }
}

pub fn test_topic(instrument: Instrument, market_data_type: MarketDataType) -> Topic {
    Topic {
        instrument,
        market_data_type,
    }
}

pub fn test_subscription(
    root_directory: &Path,
    strategy_name: &StrategyName,
    topic: Topic,
) -> Subscription {
    Subscription {
        topic,
        path: topic.ring_key().file_path(root_directory, strategy_name),
    }
}

/// A two-thread sample with every field set, as Celestia sends it.
pub fn test_process_sample() -> ProcessSample {
    ProcessSample {
        timestamp_seconds: NonZeroU32::new(1_800_000_000).expect("not zero"),
        cpu_percent: 12.5,
        memory: MemoryUsage {
            anonymous_kibibytes: 4096,
            file_kibibytes: 8192,
            shared_memory_kibibytes: 1024,
        },
        open_file_descriptors: 12,
        voluntary_context_switches_per_second: 3.5,
        involuntary_context_switches_per_second: 0.5,
        tcp_received_bytes_per_second: 1024.0,
        tcp_sent_bytes_per_second: 512.0,
        threads: vec![
            ThreadSample {
                thread_id: 100,
                name: "main".to_owned(),
                cpu_percent: 12.0,
                voluntary_context_switches_per_second: 3.0,
                involuntary_context_switches_per_second: 0.5,
            },
            ThreadSample {
                thread_id: 101,
                name: "process_monitor".to_owned(),
                cpu_percent: 0.5,
                voluntary_context_switches_per_second: 0.5,
                involuntary_context_switches_per_second: 0.0,
            },
        ],
        sampling_duration_microseconds: 2000,
    }
}

pub fn test_bbo(instrument: Instrument, sequence: usize) -> Bbo {
    Bbo {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        ask_price: dec!(101.5),
        ask_size: dec!(2),
        bid_price: dec!(101.4),
        bid_size: dec!(4),
        sequence,
    }
}

pub fn test_trade(instrument: Instrument, sequence: usize) -> Trade {
    Trade {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        price: dec!(101.5),
        size: dec!(0.25),
        taker_side: Side::Buy,
        sequence,
    }
}

pub fn test_order_book(instrument: Instrument, sequence: usize) -> OrderBook {
    let mut bid_levels = [PriceLevel::EMPTY; ORDER_BOOK_DEPTH as usize];
    let mut ask_levels = [PriceLevel::EMPTY; ORDER_BOOK_DEPTH as usize];
    for (index, level) in bid_levels.iter_mut().enumerate() {
        level.price = dec!(101.4) - Decimal::from(index) * dec!(0.1);
        level.size = dec!(4);
    }
    for (index, level) in ask_levels.iter_mut().enumerate() {
        level.price = dec!(101.5) + Decimal::from(index) * dec!(0.1);
        level.size = dec!(2);
    }
    OrderBook {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        bid_levels,
        ask_levels,
        bid_count: ORDER_BOOK_DEPTH,
        ask_count: ORDER_BOOK_DEPTH,
        sequence,
    }
}

/// A fresh static X25519 key pair, private and public half, both wrapped as services hold them.
pub fn test_keypair() -> (SecretBox<StaticKey>, SecretBox<StaticKey>) {
    let keypair = generate_keypair().expect("keypair generation succeeds");
    (
        SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.private)),
        SecretBox::init_with_mut(|key: &mut StaticKey| key.copy_from_slice(&keypair.public)),
    )
}

/// The mode `crate::secrets` requires of config and key files.
const OWNER_ONLY_MODE: u32 = 0o600;

/// Writes `content` to `path` as a chmod 600 file, as configs and private keys must be.
pub fn write_secret_file(path: &Path, content: &str) {
    write(path, content).expect("writing the secret file is expected to not fail");
    set_permissions(path, Permissions::from_mode(OWNER_ONLY_MODE))
        .expect("setting permissions is expected to not fail");
}

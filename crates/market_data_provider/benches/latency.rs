//! Latency of the market data path, bottom up: codec, publish, reader fan-out, cross-thread delivery.
//! Run with `cargo bench -p market_data_provider`.
use std::hint::{black_box, spin_loop};
use std::num::NonZeroU64;
use std::path::Path;
use std::slice;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rkyv::rancor::Error as RkyvError;
use rust_decimal::dec;
use tempfile::{TempDir, tempdir, tempdir_in};

use layline::provider_api::Subscription;
use layline::shared_memory::{MAX_PAYLOAD_LENGTH, MarketDataReader, MarketDataWriter};
use layline::time::get_current_timestamp_nanoseconds;
use layline::types::{
    Bbo, Currency, Exchange, Instrument, LatencyMetric, MarketData, MarketDataType, Side, Topic,
    Trade, UnderlyingType,
};

const SHARED_MEMORY_DIRECTORY: &str = "/dev/shm";
const FAN_OUT_TOPIC_COUNTS: [usize; 3] = [1, 8, 64];

// Mirrors the reader's buffer: rkyv requires aligned input.
#[repr(C, align(16))]
struct PayloadBuffer([u8; MAX_PAYLOAD_LENGTH]);

/// Rings live in `/dev/shm` where it exists, as in production.
fn ring_directory() -> TempDir {
    let directory_result = if Path::new(SHARED_MEMORY_DIRECTORY).is_dir() {
        tempdir_in(SHARED_MEMORY_DIRECTORY)
    } else {
        tempdir()
    };
    directory_result.expect("temp dir creation is expected to not fail")
}

/// Distinct topics across every exchange, pair, underlying type and market data type.
fn topics(count: usize) -> Vec<Topic> {
    let currencies = [Currency::Btc, Currency::Eth, Currency::Usdt, Currency::Usdc];
    let mut topics = Vec::with_capacity(count);
    for exchange in [Exchange::Binance, Exchange::Okx] {
        for base in currencies {
            for quote in currencies {
                for underlying_type in [UnderlyingType::Spot, UnderlyingType::Perpetual] {
                    for market_data_type in [MarketDataType::Bbo, MarketDataType::Trade] {
                        topics.push(Topic {
                            instrument: Instrument {
                                exchange,
                                base,
                                quote,
                                underlying_type,
                            },
                            market_data_type,
                        });
                    }
                }
            }
        }
    }
    assert!(count <= topics.len(), "only {} topics exist", topics.len());
    topics.truncate(count);
    topics
}

fn subscription(directory: &TempDir, topic: Topic) -> Subscription {
    Subscription {
        topic,
        path: topic.file_path(directory.path()),
    }
}

fn writer(subscription: &Subscription) -> MarketDataWriter {
    MarketDataWriter::open(&subscription.path, subscription.topic.market_data_type)
        .expect("writer open is expected to not fail")
}

fn reader(subscriptions: &[Subscription]) -> MarketDataReader {
    MarketDataReader::open(subscriptions).expect("reader open is expected to not fail")
}

/// One BBO topic and one trade topic of the same instrument.
fn bbo_and_trade_topics() -> [Topic; 2] {
    [MarketDataType::Bbo, MarketDataType::Trade].map(|market_data_type| Topic {
        market_data_type,
        ..topics(1)[0]
    })
}

fn bbo(instrument: Instrument, sequence: usize) -> Bbo {
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

fn trade(instrument: Instrument, sequence: usize) -> Trade {
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

fn market_data(topic: Topic, sequence: usize) -> MarketData {
    match topic.market_data_type {
        MarketDataType::Bbo => MarketData::Bbo(bbo(topic.instrument, sequence)),
        MarketDataType::Trade => MarketData::Trade(trade(topic.instrument, sequence)),
    }
}

fn publish(writer: &MarketDataWriter, data: &MarketData) {
    writer
        .publish(data)
        .expect("publish is expected to not fail");
}

fn write_time(data: &MarketData) -> NonZeroU64 {
    match data {
        MarketData::Bbo(bbo) => bbo.latency_metric.provider_write_time_nanoseconds,
        MarketData::Trade(trade) => trade.latency_metric.provider_write_time_nanoseconds,
    }
}

fn codec(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("codec");
    let instrument = topics(1)[0].instrument;
    let bbo = bbo(instrument, 1);
    let trade = trade(instrument, 1);

    group.bench_function("serialize/BBO", |bencher| {
        bencher.iter(|| {
            rkyv::to_bytes::<RkyvError>(black_box(&bbo))
                .expect("serialization is expected to not fail")
        });
    });
    group.bench_function("serialize/TRADE", |bencher| {
        bencher.iter(|| {
            rkyv::to_bytes::<RkyvError>(black_box(&trade))
                .expect("serialization is expected to not fail")
        });
    });

    let mut buffer = PayloadBuffer([0; MAX_PAYLOAD_LENGTH]);
    let bbo_bytes =
        rkyv::to_bytes::<RkyvError>(&bbo).expect("serialization is expected to not fail");
    group.bench_function("copy_and_deserialize/BBO", |bencher| {
        bencher.iter(|| {
            let payload = &mut buffer.0[..bbo_bytes.len()];
            payload.copy_from_slice(black_box(&bbo_bytes));
            rkyv::from_bytes::<Bbo, RkyvError>(payload)
                .expect("deserialization is expected to not fail")
        });
    });
    let trade_bytes =
        rkyv::to_bytes::<RkyvError>(&trade).expect("serialization is expected to not fail");
    group.bench_function("copy_and_deserialize/TRADE", |bencher| {
        bencher.iter(|| {
            let payload = &mut buffer.0[..trade_bytes.len()];
            payload.copy_from_slice(black_box(&trade_bytes));
            rkyv::from_bytes::<Trade, RkyvError>(payload)
                .expect("deserialization is expected to not fail")
        });
    });
    group.finish();
}

fn publish_to_ring(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("publish");
    let directory = ring_directory();
    for topic in bbo_and_trade_topics() {
        let writer = writer(&subscription(&directory, topic));
        let data = market_data(topic, 1);
        group.bench_function(topic.market_data_type.to_string(), |bencher| {
            bencher.iter(|| publish(&writer, black_box(&data)));
        });
    }
    group.finish();
}

/// `idle` is the pure sweep over rings without data; `one_active` adds one message on the first topic.
fn poll_fan_out(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("poll_fan_out");
    for topic_count in FAN_OUT_TOPIC_COUNTS {
        let directory = ring_directory();
        let subscriptions: Vec<Subscription> = topics(topic_count)
            .into_iter()
            .map(|topic| subscription(&directory, topic))
            .collect();
        let writers: Vec<MarketDataWriter> = subscriptions.iter().map(writer).collect();
        let reader = reader(&subscriptions);
        let data = market_data(subscriptions[0].topic, 1);

        group.bench_with_input(
            BenchmarkId::new("idle", topic_count),
            &reader,
            |bencher, reader| {
                bencher.iter(|| {
                    reader
                        .poll(|data, received_at_nanoseconds| {
                            black_box((data, received_at_nanoseconds));
                        })
                        .expect("poll is expected to not fail")
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("one_active", topic_count),
            &reader,
            |bencher, reader| {
                bencher.iter(|| {
                    publish(&writers[0], &data);
                    reader
                        .poll(|data, received_at_nanoseconds| {
                            black_box((data, received_at_nanoseconds));
                        })
                        .expect("poll is expected to not fail")
                });
            },
        );
    }
    group.finish();
}

/// One-way delivery: provider write time to the reader's receive time, as `LatencyMetric` reports it.
/// The writer waits for each acknowledgement before the next message, so no queueing is measured.
fn cross_thread_latency(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("cross_thread_latency");
    let directory = ring_directory();
    for topic in bbo_and_trade_topics() {
        let subscription = subscription(&directory, topic);
        let writer = writer(&subscription);
        group.bench_function(topic.market_data_type.to_string(), |bencher| {
            bencher.iter_custom(|iterations| deliver(&writer, &subscription, iterations));
        });
    }
    group.finish();
}

fn deliver(writer: &MarketDataWriter, subscription: &Subscription, iterations: u64) -> Duration {
    let acknowledged = AtomicU64::new(0);
    let total_latency_nanoseconds = AtomicU64::new(0);
    let reader_ready = Barrier::new(2);
    thread::scope(|scope| {
        scope.spawn(|| {
            let reader = reader(slice::from_ref(subscription));
            reader_ready.wait();
            while acknowledged.load(Ordering::Acquire) < iterations {
                reader
                    .poll(|data, received_at_nanoseconds| {
                        let latency_nanoseconds = received_at_nanoseconds
                            .get()
                            .saturating_sub(write_time(&data).get());
                        total_latency_nanoseconds.fetch_add(latency_nanoseconds, Ordering::Relaxed);
                        acknowledged.fetch_add(1, Ordering::Release);
                    })
                    .expect("poll is expected to not fail");
            }
        });
        reader_ready.wait();
        for (sequence, delivered) in (1..=iterations).enumerate() {
            publish(writer, &market_data(subscription.topic, sequence));
            while acknowledged.load(Ordering::Acquire) < delivered {
                spin_loop();
            }
        }
    });
    Duration::from_nanos(total_latency_nanoseconds.load(Ordering::Relaxed))
}

criterion_group!(
    latency,
    codec,
    publish_to_ring,
    poll_fan_out,
    cross_thread_latency
);
criterion_main!(latency);

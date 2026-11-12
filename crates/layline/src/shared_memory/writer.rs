use std::cell::Cell;
use std::fs::{OpenOptions, create_dir_all};
use std::path::Path;

use anyhow::{Context, Result};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;
use tracing::warn;

use super::ring_format::{RING_FILE_LENGTH, Ring};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::{MarketData, RingKey, StrategyName, Topic};

/// A message serialized once, then written into the ring of every strategy that reads its topic.
pub struct SerializedMarketData {
    topic: Topic,
    bytes: AlignedVec,
}

impl SerializedMarketData {
    /// Stamps the provider write time first, so serialization and every ring copy count as transport.
    /// Panics if serialization fails: a `Bbo` or `Trade` always fits, so a failure is a bug.
    pub fn new(data: &MarketData) -> Self {
        let provider_write_time_nanoseconds = get_current_timestamp_nanoseconds();
        let bytes = match *data {
            MarketData::Bbo(mut bbo) => {
                bbo.latency_metric.provider_write_time_nanoseconds =
                    provider_write_time_nanoseconds;
                rkyv::to_bytes::<RkyvError>(&bbo).expect("a Bbo always serializes")
            }
            MarketData::Trade(mut trade) | MarketData::AggregateTrade(mut trade) => {
                trade.latency_metric.provider_write_time_nanoseconds =
                    provider_write_time_nanoseconds;
                rkyv::to_bytes::<RkyvError>(&trade).expect("a Trade always serializes")
            }
        };
        Self {
            topic: data.topic(),
            bytes,
        }
    }

    pub fn topic(&self) -> Topic {
        self.topic
    }
}

/// The single writer of one strategy's ring.
pub struct MarketDataWriter {
    ring_key: RingKey,
    ring: Ring,
    /// Whether the last publish found the ring full, so a stalled strategy is logged once, not per message.
    is_dropping: Cell<bool>,
}

impl MarketDataWriter {
    /// Joins an existing ring of the same format so a running strategy sees a continuous stream; otherwise resets it.
    pub fn open(
        root_directory: &Path,
        strategy_name: &StrategyName,
        ring_key: RingKey,
    ) -> Result<Self> {
        let path = ring_key.file_path(root_directory, strategy_name);
        if let Some(directory) = path.parent() {
            create_dir_all(directory)
                .with_context(|| format!("failed to create {}", directory.display()))?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let resized = file.metadata()?.len() != RING_FILE_LENGTH;
        if resized {
            file.set_len(RING_FILE_LENGTH)?;
        }

        let mut ring = Ring::map(&file)?;
        let market_data_type = ring_key.market_data_type;
        // A new, resized or foreign file is reset; only a ring of this format and type is joined.
        let reusable = !resized
            && ring
                .market_data_type()
                .is_ok_and(|existing_market_data_type| {
                    existing_market_data_type == market_data_type
                });
        if !reusable {
            ring.reset(market_data_type);
        }
        Ok(Self {
            ring_key,
            ring,
            is_dropping: Cell::new(false),
        })
    }

    /// Drops the message when the strategy has left no free slot: the writer never waits for a reader.
    pub fn publish(&self, serialized: &SerializedMarketData) {
        debug_assert_eq!(
            serialized.topic.ring_key(),
            self.ring_key,
            "cannot publish {} to the {} ring",
            serialized.topic,
            self.ring_key
        );
        let published = self.ring.try_push(&serialized.bytes);
        if published == self.is_dropping.get() {
            self.is_dropping.set(!published);
            let ring_key = self.ring_key;
            if published {
                warn!("the {ring_key} ring has free slots again");
            } else {
                warn!("the {ring_key} ring is full, dropping messages until its strategy reads");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::rename;
    use std::num::NonZeroU64;
    use std::slice;

    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::shared_memory::MarketDataReader;
    use crate::shared_memory::ring_format::SLOT_COUNT;
    use crate::testing::{test_bbo, test_instrument, test_subscription, test_topic, test_trade};
    use crate::types::Subscription;
    use crate::types::{
        Currency, Exchange, Instrument, LatencyMetric, MarketDataType, UnderlyingType,
    };

    const STRATEGY_NAME: &str = "test_strategy";
    const OTHER_STRATEGY_NAME: &str = "other_strategy";

    #[derive(Default)]
    struct Collector {
        bbo_sequences: Vec<usize>,
        trade_sequences: Vec<usize>,
        aggregate_trade_sequences: Vec<usize>,
    }

    impl Collector {
        fn collect(&mut self, data: MarketData, _received_at_nanoseconds: NonZeroU64) {
            match data {
                MarketData::Bbo(bbo) => self.bbo_sequences.push(bbo.sequence),
                MarketData::Trade(trade) => self.trade_sequences.push(trade.sequence),
                MarketData::AggregateTrade(trade) => {
                    self.aggregate_trade_sequences.push(trade.sequence)
                }
            }
        }
    }

    fn instrument() -> Instrument {
        test_instrument(
            Exchange::Binance,
            Currency::Btc,
            Currency::Usdt,
            UnderlyingType::Spot,
        )
    }

    fn topic(market_data_type: MarketDataType) -> Topic {
        test_topic(instrument(), market_data_type)
    }

    fn subscription(directory: &TempDir, market_data_type: MarketDataType) -> Subscription {
        let strategy_name = StrategyName::from(STRATEGY_NAME);
        test_subscription(directory.path(), &strategy_name, topic(market_data_type))
    }

    fn writer(directory: &TempDir, market_data_type: MarketDataType) -> MarketDataWriter {
        let strategy_name = StrategyName::from(STRATEGY_NAME);
        MarketDataWriter::open(
            directory.path(),
            &strategy_name,
            topic(market_data_type).ring_key(),
        )
        .expect("writer open is expected to not fail")
    }

    fn market_data_reader(subscriptions: &[Subscription]) -> MarketDataReader {
        MarketDataReader::open(subscriptions)
            .expect("market_data_reader open is expected to not fail")
    }

    fn bbo(sequence: usize) -> MarketData {
        MarketData::Bbo(test_bbo(instrument(), sequence))
    }

    fn trade(sequence: usize) -> MarketData {
        MarketData::Trade(test_trade(instrument(), sequence))
    }

    fn aggregate_trade(sequence: usize) -> MarketData {
        MarketData::AggregateTrade(test_trade(instrument(), sequence))
    }

    fn ether_instrument() -> Instrument {
        test_instrument(
            Exchange::Binance,
            Currency::Eth,
            Currency::Usdt,
            UnderlyingType::Spot,
        )
    }

    fn ether_bbo(sequence: usize) -> MarketData {
        MarketData::Bbo(test_bbo(ether_instrument(), sequence))
    }

    fn publish(writer: &MarketDataWriter, data: MarketData) {
        writer.publish(&serialized(&data));
    }

    fn serialized(data: &MarketData) -> SerializedMarketData {
        SerializedMarketData::new(data)
    }

    fn poll(market_data_reader: &MarketDataReader, collector: &mut Collector) -> usize {
        market_data_reader
            .poll(|data, received_at_nanoseconds| collector.collect(data, received_at_nanoseconds))
            .expect("poll is expected to not fail")
    }

    #[test]
    fn reader_dispatches_every_topic() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo_subscription = subscription(&directory, MarketDataType::Bbo);
        let trade_subscription = subscription(&directory, MarketDataType::Trade);
        let aggregate_trade_subscription = subscription(&directory, MarketDataType::AggregateTrade);
        let bbo_writer = writer(&directory, MarketDataType::Bbo);
        let trade_writer = writer(&directory, MarketDataType::Trade);
        let aggregate_trade_writer = writer(&directory, MarketDataType::AggregateTrade);
        let market_data_reader = market_data_reader(&[
            bbo_subscription,
            trade_subscription,
            aggregate_trade_subscription,
        ]);
        let mut collector = Collector::default();

        assert_eq!(0, poll(&market_data_reader, &mut collector));
        publish(&bbo_writer, bbo(7));
        publish(&bbo_writer, bbo(8));
        publish(&trade_writer, trade(9));
        publish(&aggregate_trade_writer, aggregate_trade(10));

        assert_eq!(4, poll(&market_data_reader, &mut collector));
        assert_eq!(vec![7, 8], collector.bbo_sequences);
        assert_eq!(vec![9], collector.trade_sequences);
        assert_eq!(vec![10], collector.aggregate_trade_sequences);
    }

    #[test]
    fn instruments_sharing_a_ring_are_each_read_once() {
        let strategy_name = StrategyName::from(STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bitcoin_subscription = subscription(&directory, MarketDataType::Bbo);
        let ether_subscription = test_subscription(
            directory.path(),
            &strategy_name,
            test_topic(ether_instrument(), MarketDataType::Bbo),
        );
        assert_eq!(bitcoin_subscription.path, ether_subscription.path);
        let writer = writer(&directory, MarketDataType::Bbo);
        let market_data_reader = market_data_reader(&[bitcoin_subscription, ether_subscription]);
        let mut collector = Collector::default();

        publish(&writer, bbo(1));
        publish(&writer, ether_bbo(2));

        assert_eq!(2, poll(&market_data_reader, &mut collector));
        assert_eq!(vec![1, 2], collector.bbo_sequences);
    }

    #[test]
    fn one_serialization_reaches_every_strategy_ring() {
        let other_strategy_name = StrategyName::from(OTHER_STRATEGY_NAME);
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let ring_key = topic(MarketDataType::Bbo).ring_key();
        let other_writer = MarketDataWriter::open(directory.path(), &other_strategy_name, ring_key)
            .expect("writer open is expected to not fail");
        let writer = writer(&directory, MarketDataType::Bbo);
        let other_subscription = test_subscription(
            directory.path(),
            &other_strategy_name,
            topic(MarketDataType::Bbo),
        );
        let reader = market_data_reader(&[subscription(&directory, MarketDataType::Bbo)]);
        let other_reader = market_data_reader(&[other_subscription]);

        let serialized = serialized(&bbo(5));
        writer.publish(&serialized);
        other_writer.publish(&serialized);

        for reader in [&reader, &other_reader] {
            let mut collector = Collector::default();
            poll(reader, &mut collector);
            assert_eq!(vec![5], collector.bbo_sequences);
        }
    }

    #[test]
    fn writer_stamps_write_time_and_keeps_receive_time() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let subscription = subscription(&directory, MarketDataType::Trade);
        let writer = writer(&directory, MarketDataType::Trade);
        let market_data_reader = market_data_reader(slice::from_ref(&subscription));
        let MarketData::Trade(mut sent) = trade(1) else {
            panic!("trade() is expected to build a trade");
        };
        sent.latency_metric = LatencyMetric::received(NonZeroU64::MIN);
        publish(&writer, MarketData::Trade(sent));

        let mut received = Vec::new();
        market_data_reader
            .poll(|data, _received_at_nanoseconds| {
                if let MarketData::Trade(trade) = data {
                    received.push(trade.latency_metric);
                }
            })
            .expect("poll is expected to not fail");
        assert_eq!(1, received.len());
        assert_eq!(
            NonZeroU64::MIN,
            received[0].provider_receive_time_nanoseconds
        );
        assert!(received[0].provider_write_time_nanoseconds > NonZeroU64::MIN);
    }

    #[test]
    fn running_reader_survives_writer_restart() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let subscription = subscription(&directory, MarketDataType::Bbo);
        let first_writer = writer(&directory, MarketDataType::Bbo);
        let market_data_reader = market_data_reader(slice::from_ref(&subscription));
        let mut collector = Collector::default();

        publish(&first_writer, bbo(1));
        poll(&market_data_reader, &mut collector);
        drop(first_writer);

        let second_writer = writer(&directory, MarketDataType::Bbo);
        publish(&second_writer, bbo(2));
        poll(&market_data_reader, &mut collector);

        assert_eq!(vec![1, 2], collector.bbo_sequences);
    }

    #[test]
    fn late_reader_starts_from_current_position() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let subscription = subscription(&directory, MarketDataType::Bbo);
        let writer = writer(&directory, MarketDataType::Bbo);
        publish(&writer, bbo(1));

        let market_data_reader = market_data_reader(slice::from_ref(&subscription));
        let mut collector = Collector::default();
        publish(&writer, bbo(2));
        poll(&market_data_reader, &mut collector);

        assert_eq!(vec![2], collector.bbo_sequences);
    }

    #[test]
    fn full_ring_drops_new_messages_until_the_reader_catches_up() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let subscription = subscription(&directory, MarketDataType::Bbo);
        let writer = writer(&directory, MarketDataType::Bbo);
        let market_data_reader = market_data_reader(slice::from_ref(&subscription));
        let mut collector = Collector::default();

        // One slot stays empty, so the ring holds one message less than it has slots.
        let held_count = SLOT_COUNT as usize - 1;
        for sequence in 0..held_count + 2 {
            publish(&writer, bbo(sequence));
        }
        assert_eq!(held_count, poll(&market_data_reader, &mut collector));
        assert_eq!(
            (0..held_count).collect::<Vec<usize>>(),
            collector.bbo_sequences
        );

        publish(&writer, bbo(held_count + 2));
        assert_eq!(1, poll(&market_data_reader, &mut collector));
        assert_eq!(Some(&(held_count + 2)), collector.bbo_sequences.last());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "cannot publish")]
    fn writer_rejects_data_of_another_type_in_debug() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        writer(&directory, MarketDataType::Bbo).publish(&serialized(&trade(1)));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "cannot publish")]
    fn writer_rejects_data_of_another_exchange_in_debug() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let okx_instrument = test_instrument(
            Exchange::Okx,
            Currency::Btc,
            Currency::Usdt,
            UnderlyingType::Spot,
        );
        writer(&directory, MarketDataType::Bbo)
            .publish(&serialized(&MarketData::Bbo(test_bbo(okx_instrument, 1))));
    }

    #[test]
    fn reader_rejects_ring_of_another_type() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo_subscription = subscription(&directory, MarketDataType::Bbo);
        let _writer = writer(&directory, MarketDataType::Bbo);
        let mismatched = Subscription {
            topic: topic(MarketDataType::Trade),
            path: bbo_subscription.path,
        };
        assert!(MarketDataReader::open(&[mismatched]).is_err());
    }

    #[test]
    fn writer_resets_ring_written_with_another_type() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        drop(writer(&directory, MarketDataType::Bbo));
        let trade_subscription = subscription(&directory, MarketDataType::Trade);
        rename(
            subscription(&directory, MarketDataType::Bbo).path,
            &trade_subscription.path,
        )
        .expect("renaming the bbo ring is expected to not fail");

        let trade_writer = writer(&directory, MarketDataType::Trade);
        let market_data_reader = market_data_reader(slice::from_ref(&trade_subscription));
        let mut collector = Collector::default();
        publish(&trade_writer, trade(3));
        poll(&market_data_reader, &mut collector);
        assert_eq!(vec![3], collector.trade_sequences);
    }
}

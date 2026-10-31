use super::ring_format::{METADATA_RANGE, RING_FILE_LENGTH, read_metadata, write_metadata};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::{MarketData, MarketDataType};
use anyhow::{Context, Result};
use bcast::{RingBuffer, Writer};
use memmap2::{MmapMut, MmapOptions};
use rkyv::rancor::Error as RkyvError;
use std::fs::{OpenOptions, create_dir_all};
use std::path::Path;

/// The single writer of one topic's ring.
pub struct MarketDataWriter {
    market_data_type: MarketDataType,
    writer: Writer,
    _memory_map: MmapMut,
}

impl MarketDataWriter {
    /// Joins an existing ring of the same format so running strategies see a continuous stream; otherwise resets it.
    pub fn open(path: &Path, market_data_type: MarketDataType) -> Result<Self> {
        if let Some(directory) = path.parent() {
            create_dir_all(directory)
                .with_context(|| format!("failed to create {}", directory.display()))?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let resized = file.metadata()?.len() != RING_FILE_LENGTH;
        if resized {
            file.set_len(RING_FILE_LENGTH)?;
        }

        let memory_map = unsafe { MmapOptions::new().map_mut(&file)? };
        let reusable =
            !resized && read_metadata(&memory_map[METADATA_RANGE]) == Some(market_data_type);
        let ring = RingBuffer::new(memory_map.as_ref());
        let writer = if reusable {
            ring.join_writer()
        } else {
            ring.into_writer_with_metadata(|metadata| write_metadata(metadata, market_data_type))
        };
        Ok(Self {
            market_data_type,
            writer,
            _memory_map: memory_map,
        })
    }

    pub fn publish(&self, data: &MarketData) -> Result<()> {
        debug_assert_eq!(
            data.topic().market_data_type,
            self.market_data_type,
            "cannot publish {} to a {} ring",
            data.topic().market_data_type,
            self.market_data_type
        );
        let provider_write_time_nanoseconds = get_current_timestamp_nanoseconds();
        let bytes = match *data {
            MarketData::Bbo(mut bbo) => {
                bbo.latency_metric.provider_write_time_nanoseconds =
                    provider_write_time_nanoseconds;
                rkyv::to_bytes::<RkyvError>(&bbo)?
            }
            MarketData::Trade(mut trade) => {
                trade.latency_metric.provider_write_time_nanoseconds =
                    provider_write_time_nanoseconds;
                rkyv::to_bytes::<RkyvError>(&trade)?
            }
        };
        let mut claim = self.writer.claim(bytes.len(), true);
        claim.get_buffer_mut().copy_from_slice(&bytes);
        claim.commit();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_memory::MarketDataReader;
    use crate::shared_memory::ring_format::RING_CAPACITY;
    use crate::types::Subscription;
    use crate::types::{
        Bbo, Currency, Exchange, Instrument, LatencyMetric, MarketDataType, Topic, UnderlyingType,
    };
    use std::num::NonZeroU64;
    use std::path::PathBuf;
    use std::slice;
    use tempfile::{TempDir, tempdir};

    #[derive(Default)]
    struct Collector {
        bbo_sequences: Vec<usize>,
        trade_sequences: Vec<usize>,
    }

    impl Collector {
        fn collect(&mut self, data: MarketData, _received_at_nanoseconds: NonZeroU64) {
            match data {
                MarketData::Bbo(bbo) => self.bbo_sequences.push(bbo.sequence),
                MarketData::Trade(trade) => self.trade_sequences.push(trade.sequence),
            }
        }
    }

    fn instrument() -> Instrument {
        crate::testing::instrument(
            Exchange::Binance,
            Currency::Btc,
            Currency::Usdt,
            UnderlyingType::Spot,
        )
    }

    fn topic(market_data_type: MarketDataType) -> Topic {
        crate::testing::topic(instrument(), market_data_type)
    }

    fn subscription(directory: &TempDir, market_data_type: MarketDataType) -> Subscription {
        crate::testing::subscription(directory.path(), topic(market_data_type))
    }

    fn writer(subscription: &Subscription) -> MarketDataWriter {
        MarketDataWriter::open(&subscription.path, subscription.topic.market_data_type)
            .expect("writer open is expected to not fail")
    }

    fn market_data_reader(subscriptions: &[Subscription]) -> MarketDataReader {
        MarketDataReader::open(subscriptions)
            .expect("market_data_reader open is expected to not fail")
    }

    fn bbo(sequence: usize) -> MarketData {
        MarketData::Bbo(crate::testing::bbo(instrument(), sequence))
    }

    fn trade(sequence: usize) -> MarketData {
        MarketData::Trade(crate::testing::trade(instrument(), sequence))
    }

    fn publish(writer: &MarketDataWriter, data: MarketData) {
        writer
            .publish(&data)
            .expect("publish is expected to not fail");
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
        let bbo_writer = writer(&bbo_subscription);
        let trade_writer = writer(&trade_subscription);
        let market_data_reader = market_data_reader(&[bbo_subscription, trade_subscription]);
        let mut collector = Collector::default();

        assert_eq!(0, poll(&market_data_reader, &mut collector));
        publish(&bbo_writer, bbo(7));
        publish(&bbo_writer, bbo(8));
        publish(&trade_writer, trade(9));

        assert_eq!(3, poll(&market_data_reader, &mut collector));
        assert_eq!(vec![7, 8], collector.bbo_sequences);
        assert_eq!(vec![9], collector.trade_sequences);
    }

    #[test]
    fn writer_stamps_write_time_and_keeps_receive_time() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let subscription = subscription(&directory, MarketDataType::Trade);
        let writer = writer(&subscription);
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
        let first_writer = writer(&subscription);
        let market_data_reader = market_data_reader(slice::from_ref(&subscription));
        let mut collector = Collector::default();

        publish(&first_writer, bbo(1));
        poll(&market_data_reader, &mut collector);
        drop(first_writer);

        let second_writer = writer(&subscription);
        publish(&second_writer, bbo(2));
        poll(&market_data_reader, &mut collector);

        assert_eq!(vec![1, 2], collector.bbo_sequences);
    }

    #[test]
    fn late_reader_starts_from_current_position() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let subscription = subscription(&directory, MarketDataType::Bbo);
        let writer = writer(&subscription);
        publish(&writer, bbo(1));

        let market_data_reader = market_data_reader(slice::from_ref(&subscription));
        let mut collector = Collector::default();
        publish(&writer, bbo(2));
        poll(&market_data_reader, &mut collector);

        assert_eq!(vec![2], collector.bbo_sequences);
    }

    #[test]
    fn overrun_reader_recovers_to_latest() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let subscription = subscription(&directory, MarketDataType::Bbo);
        let writer = writer(&subscription);
        let market_data_reader = market_data_reader(slice::from_ref(&subscription));
        let mut collector = Collector::default();

        let lapping_count = RING_CAPACITY / size_of::<Bbo>() * 2;
        for sequence in 0..lapping_count {
            publish(&writer, bbo(sequence));
        }
        assert_eq!(0, poll(&market_data_reader, &mut collector));

        publish(&writer, bbo(lapping_count));
        poll(&market_data_reader, &mut collector);
        assert_eq!(vec![lapping_count], collector.bbo_sequences);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "cannot publish")]
    fn writer_rejects_data_of_another_type_in_debug() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let writer = writer(&subscription(&directory, MarketDataType::Bbo));
        writer
            .publish(&trade(1))
            .expect("publishing a trade to a bbo ring is expected to panic");
    }

    #[test]
    fn reader_rejects_ring_of_another_type() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let bbo_subscription = subscription(&directory, MarketDataType::Bbo);
        let _writer = writer(&bbo_subscription);
        let mismatched = Subscription {
            topic: topic(MarketDataType::Trade),
            path: bbo_subscription.path,
        };
        assert!(MarketDataReader::open(&[mismatched]).is_err());
    }

    #[test]
    fn writer_resets_ring_written_with_another_type() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let path: PathBuf = subscription(&directory, MarketDataType::Bbo).path;
        drop(
            MarketDataWriter::open(&path, MarketDataType::Bbo)
                .expect("writer open is expected to not fail"),
        );

        let trade_writer = MarketDataWriter::open(&path, MarketDataType::Trade)
            .expect("writer open is expected to not fail");
        let reused = Subscription {
            topic: topic(MarketDataType::Trade),
            path,
        };
        let market_data_reader = market_data_reader(&[reused]);
        let mut collector = Collector::default();
        publish(&trade_writer, trade(3));
        poll(&market_data_reader, &mut collector);
        assert_eq!(vec![3], collector.trade_sequences);
    }
}

use super::MAX_PAYLOAD_LENGTH;
use super::ring_format::{RING_FILE_LENGTH, read_metadata};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::Subscription;
use crate::types::{Bbo, MarketData, MarketDataType, Topic, Trade};
use anyhow::{Context, Result, ensure};
use bcast::error::Error::Overrun;
use bcast::{Reader, RingBuffer};
use memmap2::{Mmap, MmapOptions};
use rkyv::rancor::Error as RkyvError;
use std::fs::OpenOptions;
use std::num::NonZeroU64;
use tracing::{error, warn};

// rkyv requires the archived bytes to be aligned.
#[repr(C, align(16))]
struct PayloadBuffer([u8; MAX_PAYLOAD_LENGTH]);

struct TopicSharedMemoryRingBufferReader {
    topic: Topic,
    reader: Reader,
    _memory_map: Mmap,
}

impl TopicSharedMemoryRingBufferReader {
    fn open(subscription: &Subscription) -> Result<Self> {
        let path = &subscription.path;
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let length = file.metadata()?.len();
        ensure!(
            length == RING_FILE_LENGTH,
            "{} is {length} bytes, expected {RING_FILE_LENGTH}",
            path.display()
        );

        let memory_map = unsafe { MmapOptions::new().map(&file)? };
        let reader = RingBuffer::new(memory_map.as_ref()).into_reader();
        let stored_option = read_metadata(reader.metadata());
        let expected = subscription.topic.market_data_type;
        ensure!(
            stored_option == Some(expected),
            "{} holds {stored_option:?}, expected {expected}",
            path.display()
        );
        Ok(Self {
            topic: subscription.topic,
            reader,
            _memory_map: memory_map,
        })
    }

    fn poll<OnData: FnMut(MarketData, NonZeroU64)>(&self, on_data: &mut OnData) -> Result<usize> {
        let Some(batch) = self.reader.read_batch() else {
            return Ok(0);
        };

        let mut handled = 0;
        let mut buffer = PayloadBuffer([0; MAX_PAYLOAD_LENGTH]);
        for message_result in batch {
            let length = match message_result.and_then(|message| message.read(&mut buffer.0)) {
                Ok(length) => length,
                Err(Overrun(position)) => {
                    warn!("{} overrun at position {position}", self.topic);
                    self.reader.reset();
                    break;
                }
                Err(error) => {
                    error!("dropping {} message: {error}", self.topic);
                    continue;
                }
            };
            let received_at_nanoseconds = get_current_timestamp_nanoseconds();
            let payload = &buffer.0[..length];
            let data = match self.topic.market_data_type {
                MarketDataType::Bbo => {
                    MarketData::Bbo(rkyv::from_bytes::<Bbo, RkyvError>(payload)?)
                }
                MarketDataType::Trade => {
                    MarketData::Trade(rkyv::from_bytes::<Trade, RkyvError>(payload)?)
                }
            };
            on_data(data, received_at_nanoseconds);
            handled += 1;
        }
        Ok(handled)
    }
}

/// Reads every subscribed topic's ring in turn and hands each message to the caller with its receive time.
pub struct MarketDataReader {
    readers: Vec<TopicSharedMemoryRingBufferReader>,
}

impl MarketDataReader {
    pub fn open(subscriptions: &[Subscription]) -> Result<Self> {
        let readers = subscriptions
            .iter()
            .map(TopicSharedMemoryRingBufferReader::open)
            .collect::<Result<_>>()?;
        Ok(Self { readers })
    }

    pub fn run<OnData: FnMut(MarketData, NonZeroU64)>(&self, mut on_data: OnData) -> Result<()> {
        loop {
            self.poll(&mut on_data)?;
        }
    }

    /// Dispatches every pending message across all topics and returns how many were handled.
    pub fn poll<OnData: FnMut(MarketData, NonZeroU64)>(
        &self,
        mut on_data: OnData,
    ) -> Result<usize> {
        let mut handled = 0;
        // PERFORMANCE: possible bottleneck.
        // Why: one ring per topic keeps the provider single-writer per ring and lets strategies pick topics; sweeping them in turn is lock- and syscall-free.
        // Cost: every sweep loads each ring's producer position, so idle topics still cost a cache-line read; one topic's latency grows with the topic count, a busy topic's batch delays the others, and there is no cross-topic ordering.
        // Later: a per-strategy notification ring (or one multiplexed ring) to touch only changed topics, zero-copy rkyv access instead of deserializing, and pinning the polling thread to a core.
        for reader in &self.readers {
            handled += reader.poll(&mut on_data)?;
        }
        Ok(handled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Currency, Exchange, UnderlyingType};
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn subscription(path: PathBuf) -> Subscription {
        Subscription {
            topic: crate::testing::topic(
                crate::testing::instrument(
                    Exchange::Binance,
                    Currency::Btc,
                    Currency::Usdt,
                    UnderlyingType::Spot,
                ),
                MarketDataType::Bbo,
            ),
            path,
        }
    }

    #[test]
    fn reader_rejects_missing_and_unsized_files() {
        let directory = tempdir().expect("temp dir creation is expected to not fail");
        let missing = subscription(directory.path().join("missing.dat"));
        assert!(MarketDataReader::open(&[missing]).is_err());

        let unsized_path = directory.path().join("empty.dat");
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&unsized_path)
            .expect("file creation is expected to not fail");
        assert!(MarketDataReader::open(&[subscription(unsized_path)]).is_err());
    }
}

use std::fs::OpenOptions;
use std::num::NonZeroU64;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use bcast::error::Error::Overrun;
use bcast::{Reader, RingBuffer};
use memmap2::{Mmap, MmapOptions};
use rkyv::rancor::Error as RkyvError;
use tracing::{error, warn};

use super::MAX_PAYLOAD_LENGTH;
use super::ring_format::{METADATA_RANGE, RING_FILE_LENGTH, read_market_data_type};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::Subscription;
use crate::types::{Bbo, MarketData, MarketDataType, Trade};

// rkyv requires the archived bytes to be aligned.
#[repr(C, align(16))]
struct PayloadBuffer([u8; MAX_PAYLOAD_LENGTH]);

/// One of the strategy's rings; it carries only topics this strategy subscribed to.
struct RingReader {
    path: PathBuf,
    market_data_type: MarketDataType,
    reader: Reader,
    // Never read, only kept alive: `reader` points into this mapping, and dropping it would unmap the
    // ring under the reader. Declared after `reader`, so it is dropped after it.
    _memory_map: Mmap,
}

impl RingReader {
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
        // Checked before `into_reader`, which spins until a writer marks the ring ready and would
        // hang forever on a file no writer initialized.
        let expected_market_data_type = subscription.topic.market_data_type;
        let actual_market_data_type = read_market_data_type(&memory_map[METADATA_RANGE])
            .with_context(|| format!("{} is not a market data ring", path.display()))?;
        ensure!(
            actual_market_data_type == expected_market_data_type,
            "{} holds {actual_market_data_type}, expected {expected_market_data_type}",
            path.display()
        );
        let reader = RingBuffer::new(memory_map.as_ref()).into_reader();
        Ok(Self {
            path: path.clone(),
            market_data_type: expected_market_data_type,
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
                    warn!("{} overrun at position {position}", self.path.display());
                    self.reader.reset();
                    break;
                }
                Err(error) => {
                    error!("dropping a message of {}: {error}", self.path.display());
                    continue;
                }
            };
            let received_at_nanoseconds = get_current_timestamp_nanoseconds();
            let payload = &buffer.0[..length];
            let data = match self.market_data_type {
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

/// Reads every ring of the strategy in turn and hands each message to the caller with its receive time.
pub struct MarketDataReader {
    readers: Vec<RingReader>,
}

impl MarketDataReader {
    /// Topics sharing a ring share its path, so each path is opened once.
    pub fn open(subscriptions: &[Subscription]) -> Result<Self> {
        let mut readers: Vec<RingReader> = Vec::new();
        for subscription in subscriptions {
            if readers
                .iter()
                .all(|reader| reader.path != subscription.path)
            {
                readers.push(RingReader::open(subscription)?);
            }
        }
        Ok(Self { readers })
    }

    pub fn run<OnData: FnMut(MarketData, NonZeroU64)>(&self, mut on_data: OnData) -> Result<()> {
        loop {
            self.poll(&mut on_data)?;
        }
    }

    /// Dispatches every pending message across all rings and returns how many were handled.
    pub fn poll<OnData: FnMut(MarketData, NonZeroU64)>(
        &self,
        mut on_data: OnData,
    ) -> Result<usize> {
        let mut handled = 0;
        // PERFORMANCE: possible bottleneck. Consider zero-copy rkyv access instead of deserializing, and pinning the polling thread to a core.
        for reader in &self.readers {
            handled += reader.poll(&mut on_data)?;
        }
        Ok(handled)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::testing::{test_instrument, test_topic};
    use crate::types::{Currency, Exchange, UnderlyingType};

    fn subscription(path: PathBuf) -> Subscription {
        Subscription {
            topic: test_topic(
                test_instrument(
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
    fn reader_rejects_missing_unsized_and_unformatted_files() {
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

        let unformatted_path = directory.path().join("zeros.dat");
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&unformatted_path)
            .expect("file creation is expected to not fail")
            .set_len(RING_FILE_LENGTH)
            .expect("sizing the file is expected to not fail");
        let error = MarketDataReader::open(&[subscription(unformatted_path)])
            .err()
            .expect("a file without ring metadata is expected to be refused");
        assert!(format!("{error:#}").contains("not a market data ring"));
    }
}

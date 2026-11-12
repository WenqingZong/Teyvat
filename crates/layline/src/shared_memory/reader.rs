use std::fs::OpenOptions;
use std::num::NonZeroU64;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use rkyv::rancor::Error as RkyvError;

use super::ring_format::{RING_FILE_LENGTH, Ring};
use crate::time::get_current_timestamp_nanoseconds;
use crate::types::Subscription;
use crate::types::{Bbo, MarketData, MarketDataType, Trade};

/// One of the strategy's rings; it carries only topics this strategy subscribed to.
struct RingReader {
    path: PathBuf,
    market_data_type: MarketDataType,
    ring: Ring,
}

impl RingReader {
    fn open(subscription: &Subscription) -> Result<Self> {
        let path = &subscription.path;
        // Writable: the reader owns the ring's head index.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let length = file.metadata()?.len();
        let displayed_path = path.display();
        ensure!(
            length == RING_FILE_LENGTH,
            "{displayed_path} is {length} bytes, expected {RING_FILE_LENGTH}"
        );

        let ring = Ring::map(&file)?;
        let expected_market_data_type = subscription.topic.market_data_type;
        let actual_market_data_type = ring
            .market_data_type()
            .with_context(|| format!("{} is not a market data ring", path.display()))?;
        ensure!(
            actual_market_data_type == expected_market_data_type,
            "{displayed_path} holds {actual_market_data_type}, expected {expected_market_data_type}"
        );
        ring.skip_to_tail();
        Ok(Self {
            path: path.clone(),
            market_data_type: expected_market_data_type,
            ring,
        })
    }

    fn poll<OnData: FnMut(MarketData, NonZeroU64)>(&self, on_data: &mut OnData) -> Result<usize> {
        let mut handled = 0;
        while let Some(received_result) = self.ring.try_pop(|payload| self.decode(payload)) {
            let (data, received_at_nanoseconds) = received_result
                .with_context(|| format!("cannot decode a message of {}", self.path.display()))?;
            on_data(data, received_at_nanoseconds);
            handled += 1;
        }
        Ok(handled)
    }

    /// Deserializes straight from the slot: the writer cannot reuse it before it is popped.
    fn decode(&self, payload: &[u8]) -> Result<(MarketData, NonZeroU64)> {
        let received_at_nanoseconds = get_current_timestamp_nanoseconds();
        let data = match self.market_data_type {
            MarketDataType::Bbo => MarketData::Bbo(rkyv::from_bytes::<Bbo, RkyvError>(payload)?),
            MarketDataType::Trade => {
                MarketData::Trade(rkyv::from_bytes::<Trade, RkyvError>(payload)?)
            }
            MarketDataType::AggregateTrade => {
                MarketData::AggregateTrade(rkyv::from_bytes::<Trade, RkyvError>(payload)?)
            }
        };
        Ok((data, received_at_nanoseconds))
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

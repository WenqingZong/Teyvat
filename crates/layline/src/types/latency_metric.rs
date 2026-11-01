use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize, Serialize};

/// Provider-side timestamps; `exchange_time_nanoseconds` stays outside because the venue supplies it.
#[derive(Archive, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LatencyMetric {
    /// When the connector read the venue message, before parsing.
    pub provider_receive_time_nanoseconds: NonZeroU64,
    /// When the writer began publishing, before serialization.
    pub provider_write_time_nanoseconds: NonZeroU64,
}

impl LatencyMetric {
    /// The write time is a placeholder until `MarketDataWriter::publish` overwrites it.
    pub fn received(provider_receive_time_nanoseconds: NonZeroU64) -> Self {
        Self {
            provider_receive_time_nanoseconds,
            provider_write_time_nanoseconds: provider_receive_time_nanoseconds,
        }
    }
}

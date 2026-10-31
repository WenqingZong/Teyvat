use crate::types::{Instrument, MarketDataType};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FormatResult};
use std::path::{Path, PathBuf};

/// Unit of subscription and deduplication; each topic owns one shared-memory ring.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Topic {
    pub instrument: Instrument,
    pub market_data_type: MarketDataType,
}

impl Topic {
    pub fn file_path(&self, root_directory: &Path) -> PathBuf {
        root_directory.join(format!("{self}.dat"))
    }
}

impl Display for Topic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        write!(formatter, "{}/{}", self.instrument, self.market_data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Currency, Exchange, UnderlyingType};

    fn topic() -> Topic {
        Topic {
            instrument: Instrument {
                exchange: Exchange::Okx,
                base: Currency::Eth,
                quote: Currency::Usdc,
                underlying_type: UnderlyingType::Spot,
            },
            market_data_type: MarketDataType::Trade,
        }
    }

    #[test]
    fn file_path_nests_exchange_symbol_and_market_data_type() {
        assert_eq!(
            PathBuf::from("/dev/shm/root/OKX/ETH-USDC-SPOT/TRADE.dat"),
            topic().file_path(Path::new("/dev/shm/root"))
        );
    }

    #[test]
    fn topic_round_trips_through_json() {
        let encoded =
            serde_json::to_string(&topic()).expect("serialization is expected to not fail");
        let decoded: Topic =
            serde_json::from_str(&encoded).expect("deserialization is expected to not fail");
        assert_eq!(topic(), decoded);
        assert_eq!("OKX/ETH-USDC-SPOT/TRADE", decoded.to_string());
    }
}

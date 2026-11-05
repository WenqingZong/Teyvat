use std::fmt::{Display, Formatter, Result as FormatResult};

use serde::{Deserialize, Serialize};

use crate::types::{Instrument, MarketDataType, RingKey};

/// Unit of subscription and deduplication; a strategy's topics of one `RingKey` share a ring.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Topic {
    pub instrument: Instrument,
    pub market_data_type: MarketDataType,
}

impl Topic {
    pub fn ring_key(&self) -> RingKey {
        RingKey {
            exchange: self.instrument.exchange,
            underlying_type: self.instrument.underlying_type,
            market_data_type: self.market_data_type,
        }
    }
}

impl Display for Topic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        write!(formatter, "{}/{}", self.instrument, self.market_data_type)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::testing::{test_instrument, test_topic};
    use crate::types::{Currency, Exchange, StrategyName, UnderlyingType};

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
    fn a_strategys_instruments_of_one_exchange_and_underlying_type_share_a_ring() {
        let other_pair = test_topic(
            test_instrument(
                Exchange::Okx,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Spot,
            ),
            MarketDataType::Trade,
        );
        let root_directory = Path::new("/dev/shm/root");
        let first_strategy_name = StrategyName::from("first");
        let second_strategy_name = StrategyName::from("second");
        assert_eq!(
            topic()
                .ring_key()
                .file_path(root_directory, &first_strategy_name),
            other_pair
                .ring_key()
                .file_path(root_directory, &first_strategy_name)
        );
        assert_ne!(
            topic()
                .ring_key()
                .file_path(root_directory, &first_strategy_name),
            topic()
                .ring_key()
                .file_path(root_directory, &second_strategy_name)
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

use std::fmt::{Display, Formatter, Result as FormatResult};
use std::path::{Path, PathBuf};

use crate::types::{Exchange, MarketDataType, StrategyName, UnderlyingType};

/// Identifies one of a strategy's shared-memory rings: every instrument the strategy reads of the
/// same exchange, underlying type and market data type shares it, so a strategy polls a few rings
/// however many instruments it reads.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RingKey {
    pub exchange: Exchange,
    pub underlying_type: UnderlyingType,
    pub market_data_type: MarketDataType,
}

impl RingKey {
    /// Each strategy reads only its own rings, so the path starts with the strategy's name.
    pub fn file_path(&self, root_directory: &Path, strategy_name: &StrategyName) -> PathBuf {
        root_directory
            .join(strategy_name)
            .join(format!("{self}.dat"))
    }
}

impl Display for RingKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        write!(
            formatter,
            "{}/{}/{}",
            self.exchange, self.underlying_type, self.market_data_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_path_nests_strategy_exchange_underlying_type_and_market_data_type() {
        let ring_key = RingKey {
            exchange: Exchange::Okx,
            underlying_type: UnderlyingType::Perpetual,
            market_data_type: MarketDataType::Trade,
        };
        assert_eq!(
            PathBuf::from("/dev/shm/dummy_strategy/OKX/PERP/TRADE.dat"),
            ring_key.file_path(Path::new("/dev/shm"), &StrategyName::from("dummy_strategy"))
        );
    }
}

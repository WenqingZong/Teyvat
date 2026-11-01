use std::fmt::{Display, Formatter, Result as FormatResult};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::types::{Currency, Exchange, UnderlyingType};

#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    PartialEq,
    RkyvDeserialize,
    RkyvSerialize,
    Serialize,
)]
pub struct Instrument {
    pub exchange: Exchange,
    pub base: Currency,
    pub quote: Currency,
    pub underlying_type: UnderlyingType,
}

impl Instrument {
    /// `BTC-USDT-SPOT`, the instrument spelling used by the HTTP API and ring paths.
    pub fn key(self) -> String {
        format!("{}-{}-{}", self.base, self.quote, self.underlying_type)
    }

    /// Parses `BTC-USDT-SPOT` back into an instrument of `exchange`.
    pub fn from_key(exchange: Exchange, key: &str) -> Result<Self, String> {
        let parts: Vec<&str> = key.split('-').collect();
        if parts.len() != 3 {
            return Err(format!(
                "instrument {key:?} is expected to look like BTC-USDT-SPOT"
            ));
        }
        Ok(Self {
            exchange,
            base: parts[0]
                .parse()
                .map_err(|_| format!("unknown currency {:?}", parts[0]))?,
            quote: parts[1]
                .parse()
                .map_err(|_| format!("unknown currency {:?}", parts[1]))?,
            underlying_type: parts[2]
                .parse()
                .map_err(|_| format!("unknown underlying type {:?}", parts[2]))?,
        })
    }
}

impl Display for Instrument {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        write!(formatter, "{}/{}", self.exchange, self.key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument() -> Instrument {
        Instrument {
            exchange: Exchange::Okx,
            base: Currency::Btc,
            quote: Currency::Usdt,
            underlying_type: UnderlyingType::Perpetual,
        }
    }

    #[test]
    fn instrument_displays_exchange_and_symbol() {
        assert_eq!("OKX/BTC-USDT-PERP", instrument().to_string());
    }

    #[test]
    fn instrument_key_round_trips() {
        let instrument = instrument();
        let key = instrument.key();
        assert_eq!("BTC-USDT-PERP", key);
        assert_eq!(
            instrument,
            Instrument::from_key(Exchange::Okx, &key).expect("a valid key is expected to parse")
        );
    }

    #[test]
    fn malformed_key_is_refused() {
        for key in ["BTC-USDT", "BTC-DOGE-SPOT", "BTC-USDT-bbo"] {
            assert!(Instrument::from_key(Exchange::Binance, key).is_err());
        }
    }

    #[test]
    fn instrument_round_trips_through_json() {
        let instrument = Instrument {
            exchange: Exchange::Binance,
            base: Currency::Eth,
            quote: Currency::Usdc,
            underlying_type: UnderlyingType::Spot,
        };
        let encoded =
            serde_json::to_string(&instrument).expect("serialization is expected to not fail");
        let decoded: Instrument =
            serde_json::from_str(&encoded).expect("deserialization is expected to not fail");
        assert_eq!(instrument, decoded);
    }
}

//! Binance gnoses.

use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use super::{Gnosis, GnosisId, serialize_secret};

/// `binance_{real,demo}`: the trading key.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceApiKey {
    #[serde(serialize_with = "serialize_secret")]
    api_key: SecretString,
    #[serde(serialize_with = "serialize_secret")]
    secret_key: SecretString,
}

impl BinanceApiKey {
    pub fn api_key(&self) -> SecretString {
        self.api_key.clone()
    }

    pub fn secret_key(&self) -> SecretString {
        self.secret_key.clone()
    }
}

impl Gnosis for BinanceApiKey {
    fn gnosis_id(demo: bool) -> GnosisId {
        if demo {
            GnosisId::BinanceDemo
        } else {
            GnosisId::BinanceReal
        }
    }
}

/// `binance_sbe_{real,demo}`: SBE market data streams need only the API key, sent in the
/// `X-MBX-APIKEY` header.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceSbeApiKey {
    #[serde(serialize_with = "serialize_secret")]
    api_key: SecretString,
}

impl BinanceSbeApiKey {
    pub fn api_key(&self) -> SecretString {
        self.api_key.clone()
    }
}

impl Gnosis for BinanceSbeApiKey {
    fn gnosis_id(demo: bool) -> GnosisId {
        if demo {
            GnosisId::BinanceSbeDemo
        } else {
            GnosisId::BinanceSbeReal
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::from_str;

    use super::*;

    const API_KEY: &str = "dummy-api-key";
    const SECRET_KEY: &str = "dummy-secret-key";

    #[test]
    fn key_types_deny_unknown_fields() {
        let with_unknown_field =
            format!(r#"{{"api_key": "{API_KEY}", "unknown_field": "{SECRET_KEY}"}}"#);
        assert!(from_str::<BinanceSbeApiKey>(&with_unknown_field).is_err());
        assert!(from_str::<BinanceApiKey>(&with_unknown_field).is_err());
    }

    #[test]
    fn live_and_demo_gnoses_of_a_type_differ() {
        assert_ne!(
            BinanceApiKey::gnosis_id(false),
            BinanceApiKey::gnosis_id(true)
        );
        assert_ne!(
            BinanceSbeApiKey::gnosis_id(false),
            BinanceSbeApiKey::gnosis_id(true)
        );
    }
}

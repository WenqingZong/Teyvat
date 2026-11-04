//! The `[gnoses]` table of phanes.toml: one table per `GnosisId`, parsed as the key type that id
//! holds, so a misspelled id, a missing or unknown field, or a key of the wrong type fails to parse.

use serde::{Deserialize, Serialize};

use layline::gnosis::GnosisId;
use layline::gnosis::binance::{BinanceApiKey, BinanceSbeApiKey};

/// Each field is named exactly as its `GnosisId` is spelled.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gnoses {
    binance_real: Option<BinanceApiKey>,
    binance_demo: Option<BinanceApiKey>,
    binance_sbe_real: Option<BinanceSbeApiKey>,
    binance_sbe_demo: Option<BinanceSbeApiKey>,
}

impl Gnoses {
    /// The gnosis of `gnosis_id`, or `None` if phanes.toml has no table for it.
    pub fn get(&self, gnosis_id: GnosisId) -> Option<AnyGnosis> {
        match gnosis_id {
            GnosisId::BinanceReal => self.binance_real.clone().map(AnyGnosis::Binance),
            GnosisId::BinanceDemo => self.binance_demo.clone().map(AnyGnosis::Binance),
            GnosisId::BinanceSbeReal => self.binance_sbe_real.clone().map(AnyGnosis::BinanceSbe),
            GnosisId::BinanceSbeDemo => self.binance_sbe_demo.clone().map(AnyGnosis::BinanceSbe),
        }
    }
}

/// A gnosis of any key type. It serializes as the bare key, so the client reads it as its type.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum AnyGnosis {
    Binance(BinanceApiKey),
    BinanceSbe(BinanceSbeApiKey),
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;
    use serde_json::to_value;
    use strum::IntoEnumIterator;

    use super::*;

    /// A table for every id, each with fields of its own type.
    const EVERY_GNOSIS: &str = r#"
[binance_real]
api_key = "real api key"
secret_key = "real secret key"

[binance_demo]
api_key = "demo api key"
secret_key = "demo secret key"

[binance_sbe_real]
api_key = "real sbe api key"

[binance_sbe_demo]
api_key = "demo sbe api key"
"#;

    #[test]
    fn every_gnosis_id_has_a_table_of_its_own_spelling() {
        let gnoses: Gnoses = toml::from_str(EVERY_GNOSIS).expect("every table parses");
        for gnosis_id in GnosisId::iter() {
            assert!(gnoses.get(gnosis_id).is_some(), "no table for {gnosis_id}");
        }
    }

    #[test]
    fn gnosis_serializes_as_the_bare_key() {
        let gnoses: Gnoses = toml::from_str(EVERY_GNOSIS).expect("every table parses");
        let gnosis = gnoses
            .get(GnosisId::BinanceSbeDemo)
            .expect("the table is there");
        let AnyGnosis::BinanceSbe(sbe_api_key) = &gnosis else {
            panic!("binance_sbe_demo holds an SBE key, got {gnosis:?}");
        };
        assert_eq!("demo sbe api key", sbe_api_key.api_key().expose_secret());
        assert_eq!(
            serde_json::json!({"api_key": "demo sbe api key"}),
            to_value(&gnosis).expect("serializes")
        );
    }

    #[test]
    fn missing_table_is_none() {
        assert!(Gnoses::default().get(GnosisId::BinanceReal).is_none());
    }

    #[test]
    fn key_of_the_wrong_type_or_unknown_id_is_refused() {
        for text in [
            "[binance_demo]\napi_key = \"only the SBE field\"\n",
            "[binance_sbe_demo]\napi_key = \"a\"\nsecret_key = \"b\"\n",
            "[binance_demo_trading]\napi_key = \"a\"\nsecret_key = \"b\"\n",
        ] {
            assert!(toml::from_str::<Gnoses>(text).is_err(), "{text}");
        }
    }
}

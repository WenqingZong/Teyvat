//! The API keys Phanes Gnosis releases. Each `ApiKeyId` names one `<key_id>.json` key file, a JSON
//! object holding one of the key types below: Phanes checks every allowed file against its type at
//! startup, and a client fetches a key by its type, which picks the id.

use std::any::type_name;
use std::collections::BTreeMap;

use secrecy::{ExposeSecret, SecretString};
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use strum::{Display, EnumIter, EnumString};

const API_KEY_FILE_EXTENSION: &str = "json";

/// Every key Phanes can release. Serde, `Display` and `FromStr` all spell it in snake_case, as in
/// the key file names of `gnoses/`.
#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumIter, EnumString, Eq, Hash, PartialEq, Serialize,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ApiKeyId {
    BinanceReal,
    BinanceDemo,
    BinanceSbeReal,
    BinanceSbeDemo,
}

impl ApiKeyId {
    /// `<key_id>.json`, in Phanes' key directory and in `gnoses/`.
    pub fn file_name(self) -> String {
        format!("{self}.{API_KEY_FILE_EXTENSION}")
    }

    /// Parses `key_file_content` as the type this id holds, then drops it.
    pub fn check_key_file(self, key_file_content: &SecretString) -> Result<(), ApiKeyFormatError> {
        match self {
            Self::BinanceReal | Self::BinanceDemo => {
                parse_api_key::<BinanceApiKey>(key_file_content).map(drop)
            }
            Self::BinanceSbeReal | Self::BinanceSbeDemo => {
                parse_api_key::<BinanceSbeApiKey>(key_file_content).map(drop)
            }
        }
    }
}

/// A key type, and the id of its live and demo key, so a client cannot fetch one id as another type.
pub trait ApiKey: DeserializeOwned {
    fn key_id(demo: bool) -> ApiKeyId;
}

/// `binance_{real,demo}.json`: the trading key.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceApiKey {
    api_key: SecretString,
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

impl ApiKey for BinanceApiKey {
    fn key_id(demo: bool) -> ApiKeyId {
        if demo {
            ApiKeyId::BinanceDemo
        } else {
            ApiKeyId::BinanceReal
        }
    }
}

/// `binance_sbe_{real,demo}.json`: SBE market data streams need only the API key, sent in the
/// `X-MBX-APIKEY` header.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceSbeApiKey {
    api_key: SecretString,
}

impl BinanceSbeApiKey {
    pub fn api_key(&self) -> SecretString {
        self.api_key.clone()
    }
}

impl ApiKey for BinanceSbeApiKey {
    fn key_id(demo: bool) -> ApiKeyId {
        if demo {
            ApiKeyId::BinanceSbeDemo
        } else {
            ApiKeyId::BinanceSbeReal
        }
    }
}

/// Why a key file does not parse as the expected type. It names the fields the file has but
/// never quotes the file, whose values are secrets: serde's own messages may echo a value.
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("expected {expected}, the key file {found}")]
pub struct ApiKeyFormatError {
    expected: &'static str,
    found: String,
}

pub fn parse_api_key<KeyType: ApiKey>(
    key_file_content: &SecretString,
) -> Result<KeyType, ApiKeyFormatError> {
    let json_str = key_file_content.expose_secret();
    from_str(json_str).map_err(|error| {
        let position = format!("line {} column {}", error.line(), error.column());
        let found = match from_str::<BTreeMap<String, IgnoredAny>>(json_str) {
            Ok(fields) => {
                let names: Vec<&str> = fields.keys().map(String::as_str).collect();
                format!("has fields [{}] (error at {position})", names.join(", "))
            }
            Err(_) => format!("is not a JSON object (error at {position})"),
        };
        ApiKeyFormatError {
            expected: short_type_name::<KeyType>(),
            found,
        }
    })
}

/// `BinanceSbeApiKey` rather than its whole module path.
fn short_type_name<Type>() -> &'static str {
    let full_name = type_name::<Type>();
    full_name.rsplit("::").next().unwrap_or(full_name)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::to_string;
    use strum::IntoEnumIterator;

    use super::*;

    const BINANCE_KEY_FILE: &str = r#"{"api_key": "public", "secret_key": "private"}"#;
    const BINANCE_SBE_KEY_FILE: &str = r#"{"api_key": "public"}"#;

    /// A valid file for every id; the match is exhaustive, so a new id needs one here.
    fn sample_key_file(key_id: ApiKeyId) -> &'static str {
        match key_id {
            ApiKeyId::BinanceReal | ApiKeyId::BinanceDemo => BINANCE_KEY_FILE,
            ApiKeyId::BinanceSbeReal | ApiKeyId::BinanceSbeDemo => BINANCE_SBE_KEY_FILE,
        }
    }

    #[test]
    fn every_key_type_accepts_its_fields_and_denies_unknown_ones() {
        for key_id in ApiKeyId::iter() {
            let sample = sample_key_file(key_id);
            key_id
                .check_key_file(&SecretString::from(sample))
                .expect("the sample has the fields of its type");
            let with_unknown_field = sample.replacen('{', r#"{"unknown_field": "x", "#, 1);
            assert!(
                key_id
                    .check_key_file(&SecretString::from(with_unknown_field))
                    .is_err(),
                "{key_id} must deny unknown fields"
            );
        }
    }

    /// `ApiKey::key_id` and `ApiKeyId::check_key_file` agree on which type each id holds.
    fn assert_type_holds_its_ids<KeyType: ApiKey>(sample: &str) {
        assert_ne!(KeyType::key_id(false), KeyType::key_id(true));
        for demo in [false, true] {
            KeyType::key_id(demo)
                .check_key_file(&SecretString::from(sample))
                .expect("the id holds this type");
        }
    }

    #[test]
    fn key_types_and_their_ids_agree() {
        assert_type_holds_its_ids::<BinanceApiKey>(BINANCE_KEY_FILE);
        assert_type_holds_its_ids::<BinanceSbeApiKey>(BINANCE_SBE_KEY_FILE);
    }

    #[test]
    fn key_id_spellings_agree() {
        for key_id in ApiKeyId::iter() {
            let name = key_id.to_string();
            assert_eq!(
                format!("\"{name}\""),
                to_string(&key_id).expect("serializes")
            );
            assert_eq!(Ok(key_id), ApiKeyId::from_str(&name));
            assert_eq!(format!("{name}.json"), key_id.file_name());
        }
        assert_eq!("binance_sbe_demo", ApiKeyId::BinanceSbeDemo.to_string());
    }

    #[test]
    fn key_file_parses_into_its_type() {
        let api_key: BinanceApiKey =
            parse_api_key(&SecretString::from(BINANCE_KEY_FILE)).expect("the fields match");
        assert_eq!("public", api_key.api_key().expose_secret());
        assert_eq!("private", api_key.secret_key().expose_secret());
    }

    #[test]
    fn mismatched_key_file_names_its_fields_but_never_its_values() {
        let misnamed = SecretString::from(r#"{"public_key": "needle", "secret_key": "needle"}"#);
        let message = parse_api_key::<BinanceApiKey>(&misnamed)
            .expect_err("public_key is not a field of BinanceApiKey")
            .to_string();
        assert!(message.starts_with("expected BinanceApiKey,"), "{message}");
        assert!(message.contains("[public_key, secret_key]"), "{message}");
        assert!(!message.contains("needle"), "{message}");

        let wrong_type = SecretString::from(r#"{"api_key": 12345, "secret_key": "needle"}"#);
        let message = parse_api_key::<BinanceApiKey>(&wrong_type)
            .expect_err("api_key must be a string")
            .to_string();
        assert!(message.contains("[api_key, secret_key]"), "{message}");
        assert!(!message.contains("12345"), "{message}");
        assert!(!message.contains("needle"), "{message}");
    }

    #[test]
    fn key_file_that_is_not_an_object_is_refused_without_echoing_it() {
        for json in [r#""needle""#, "needle", ""] {
            let message = parse_api_key::<BinanceApiKey>(&SecretString::from(json))
                .expect_err("only an object is a key file")
                .to_string();
            assert!(message.contains("is not a JSON object"), "{message}");
            assert!(!message.contains("needle"), "{message}");
        }
    }
}

use serde::{Deserialize, Serialize};
use strum::Display;

/// Serde spelling matches `Display`, so JSON keys read like ring paths.
#[derive(
    Clone, Copy, Debug, Deserialize, Display, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "UPPERCASE")]
#[strum(serialize_all = "UPPERCASE")]
#[repr(u8)]
pub enum MarketDataType {
    Bbo = 1,
    Trade = 2,
}

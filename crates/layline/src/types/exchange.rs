use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter};

/// Serde spelling matches `Display`, so JSON keys read like ring paths.
#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    RkyvDeserialize,
    RkyvSerialize,
    Serialize,
)]
pub enum Exchange {
    Binance,
    #[serde(rename = "OKX")]
    #[strum(to_string = "OKX")]
    Okx,
}

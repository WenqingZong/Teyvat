use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Display,
    EnumString,
    Eq,
    Hash,
    PartialEq,
    RkyvDeserialize,
    RkyvSerialize,
    Serialize,
)]
pub enum UnderlyingType {
    #[serde(rename = "SPOT")]
    #[strum(to_string = "SPOT")]
    Spot,
    /// Straight (linear) perpetual only; inverse perpetuals are not supported yet.
    #[serde(rename = "PERP")]
    #[strum(to_string = "PERP")]
    Perpetual,
}

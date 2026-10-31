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
#[serde(rename_all = "UPPERCASE")]
#[strum(serialize_all = "UPPERCASE")]
pub enum Currency {
    Btc,
    Eth,
    Usdt,
    Usdc,
}

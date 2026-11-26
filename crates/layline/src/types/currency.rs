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
    Aapl,
    Amzn,
    Btc,
    Eth,
    Googl,
    Meta,
    Msft,
    Nvda,
    Tsla,
    Usdc,
    Usdt,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn every_currency_round_trips_between_display_and_parse() {
        for (currency, text) in [
            (Currency::Aapl, "AAPL"),
            (Currency::Amzn, "AMZN"),
            (Currency::Btc, "BTC"),
            (Currency::Eth, "ETH"),
            (Currency::Googl, "GOOGL"),
            (Currency::Meta, "META"),
            (Currency::Msft, "MSFT"),
            (Currency::Nvda, "NVDA"),
            (Currency::Tsla, "TSLA"),
            (Currency::Usdc, "USDC"),
            (Currency::Usdt, "USDT"),
        ] {
            assert_eq!(text, currency.to_string());
            assert_eq!(Ok(currency), Currency::from_str(text));
        }
    }
}

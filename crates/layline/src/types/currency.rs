use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString};

#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Display,
    EnumIter,
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

impl Currency {
    /// Every exchange we support quotes in a stablecoin; we support spot and U-margined
    /// perpetuals only, so USDT and USDC are the quote currencies.
    pub const fn is_quote_currency(self) -> bool {
        matches!(self, Self::Usdt | Self::Usdc)
    }

    /// An asset that can be a base; stablecoins quote, they never trade as the base.
    pub const fn is_base_currency(self) -> bool {
        !self.is_quote_currency()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use strum::IntoEnumIterator;

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

    #[test]
    fn stablecoins_are_the_only_quote_currencies() {
        for currency in Currency::iter() {
            assert_eq!(
                matches!(currency, Currency::Usdt | Currency::Usdc),
                currency.is_quote_currency()
            );
            assert_eq!(!currency.is_quote_currency(), currency.is_base_currency());
        }
    }
}

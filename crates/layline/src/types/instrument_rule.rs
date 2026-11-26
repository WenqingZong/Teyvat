use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::Instrument;

/// One instrument's trading rules, fetched from its exchange.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InstrumentRule {
    /// The symbol the exchange expects in subscriptions and orders.
    pub exchange_symbol: String,
    /// The smallest price increment.
    #[serde(with = "rust_decimal::serde::str")]
    pub price_step: Decimal,
    /// The smallest quantity increment.
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity_step: Decimal,
    /// The smallest order quantity.
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_quantity: Decimal,
    /// The smallest order value in the quote currency.
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_notional_size: Decimal,
}

/// Every instrument rule the provider currently knows.
pub type InstrumentRules = HashMap<Instrument, InstrumentRule>;

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::json;

    use super::*;

    fn rule() -> InstrumentRule {
        InstrumentRule {
            exchange_symbol: "AAPLBUSDT".to_owned(),
            price_step: dec!(0.01),
            quantity_step: dec!(0.001),
            minimum_quantity: dec!(0.001),
            minimum_notional_size: dec!(5),
        }
    }

    #[test]
    fn rules_round_trip_through_json_as_exact_decimal_strings() {
        let encoded = serde_json::to_value(rule()).expect("serialization is expected to not fail");
        assert_eq!(
            json!({
                "exchange_symbol": "AAPLBUSDT",
                "price_step": "0.01",
                "quantity_step": "0.001",
                "minimum_quantity": "0.001",
                "minimum_notional_size": "5",
            }),
            encoded
        );
        let decoded: InstrumentRule =
            serde_json::from_value(encoded).expect("deserialization is expected to not fail");
        assert_eq!(rule(), decoded);
    }
}

//! Merging fills into one. Only the provider merges fills today, so this lives here; it may move to
//! layline once another crate, such as a strategy, needs it.

use anyhow::{Context, Result};
use rust_decimal::Decimal;

/// Fills added one by one, read back as a single fill: their total size at their volume-weighted
/// average price. Accumulates in place, so merging allocates nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct VolumeWeightedAveragePrice {
    notional: Decimal,
    size: Decimal,
}

impl VolumeWeightedAveragePrice {
    /// Fails only on a `Decimal` overflow, which no real fill reaches.
    pub fn add(&mut self, price: Decimal, size: Decimal) -> Result<()> {
        self.notional = price
            .checked_mul(size)
            .and_then(|notional| self.notional.checked_add(notional))
            .context("notional overflows Decimal")?;
        self.size = self
            .size
            .checked_add(size)
            .context("size overflows Decimal")?;
        Ok(())
    }

    pub fn size(&self) -> Decimal {
        self.size
    }

    /// Unrounded; `None` while the total size is zero.
    pub fn price(&self) -> Option<Decimal> {
        self.notional.checked_div(self.size)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn fills_are_weighted_by_size() {
        let mut average = VolumeWeightedAveragePrice::default();
        average.add(dec!(100), dec!(1)).expect("no overflow");
        average.add(dec!(103), dec!(2)).expect("no overflow");
        assert_eq!(dec!(3), average.size());
        assert_eq!(Some(dec!(102)), average.price());
    }

    #[test]
    fn a_single_fill_keeps_its_price() {
        let mut average = VolumeWeightedAveragePrice::default();
        average
            .add(dec!(74547.6), dec!(0.024))
            .expect("no overflow");
        assert_eq!(dec!(0.024), average.size());
        assert_eq!(Some(dec!(74547.6)), average.price());
    }

    #[test]
    fn no_size_has_no_price() {
        let mut average = VolumeWeightedAveragePrice::default();
        assert_eq!(None, average.price());
        average.add(dec!(100), Decimal::ZERO).expect("no overflow");
        assert_eq!(None, average.price());
    }

    #[test]
    fn overflow_is_an_error() {
        let mut average = VolumeWeightedAveragePrice::default();
        assert!(average.add(Decimal::MAX, dec!(2)).is_err());
        let mut average = VolumeWeightedAveragePrice::default();
        average
            .add(Decimal::ONE, Decimal::MAX)
            .expect("no overflow");
        assert!(average.add(Decimal::ZERO, Decimal::ONE).is_err());
    }
}

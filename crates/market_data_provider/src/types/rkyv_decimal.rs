use rkyv::{Archive, Deserialize, Serialize};
use rust_decimal::Decimal;

/// rust_decimal has no rkyv 0.8 support, so archive it as its 16-byte representation.
#[derive(Archive, Deserialize, Serialize)]
#[rkyv(remote = Decimal)]
pub struct RkyvDecimal {
    #[rkyv(getter = Decimal::serialize)]
    bytes: [u8; 16],
}

impl From<RkyvDecimal> for Decimal {
    fn from(RkyvDecimal { bytes }: RkyvDecimal) -> Self {
        Self::deserialize(bytes)
    }
}

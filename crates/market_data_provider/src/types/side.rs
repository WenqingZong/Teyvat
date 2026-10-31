use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Side {
    Buy,
    Sell,
}

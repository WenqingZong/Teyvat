use crate::types::Topic;
use std::path::PathBuf;

/// An accepted topic and the ring file to read it from.
#[derive(Clone, Debug, PartialEq)]
pub struct Subscription {
    pub topic: Topic,
    pub path: PathBuf,
}

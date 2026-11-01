use std::collections::BTreeMap;

use serde::Serialize;

use crate::types::Exchange;

/// The venue APIs each exchange's connector calls, so their changelogs can be watched.
pub type ConnectedApis = BTreeMap<Exchange, &'static [ConnectedApi]>;

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct ConnectedApi {
    pub description: &'static str,
    pub api: &'static str,
    pub doc: &'static str,
}

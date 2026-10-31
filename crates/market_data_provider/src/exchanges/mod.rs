mod binance;
mod okx;
mod router;
// Temporary development stand-in for venue feeds; the whole module will be deleted.
mod synthetic;
mod websocket;
mod worker;

use binance::BinanceConnector;
use okx::OkxConnector;
pub(crate) use router::ExchangeRouter;

use crate::alert::Notifier;
use crate::phanes_client::PhanesClient;
use crate::types::{ConnectedApi, ConnectedApis};
use crate::types::{Exchange, MarketData, Topic};
use anyhow::Result;
use std::collections::HashSet;
use strum::IntoEnumIterator;

/// What every connector may need besides its venue; each worker gets its own clone.
#[derive(Clone)]
pub(crate) struct ConnectorContext {
    pub(crate) phanes_client: PhanesClient,
    pub(crate) notifier: Notifier,
}

/// One venue connection, driven by its exchange's dedicated single-threaded runtime, so it need not be `Send`.
/// Each connector is built by its own `new`, which connects nothing; venue connections open in `subscribe`.
pub(crate) trait ExchangeConnector {
    const EXCHANGE: Exchange;
    /// Venue APIs this connector calls, served by `/connected_apis`. Hardcoded, so expect to edit it by hand
    /// whenever the connector starts or stops calling a venue API.
    const CONNECTED_APIS: &'static [ConnectedApi];

    async fn subscribe(&mut self, topics: &HashSet<Topic>) -> Result<()>;

    async fn unsubscribe(&mut self, topics: &HashSet<Topic>) -> Result<()>;

    /// Must be cancel-safe: the worker races it against subscription commands.
    // TODO: provisional shape; its design still needs a detailed discussion.
    async fn next_event(&mut self) -> MarketData;

    /// Called once before the worker stops, to close venue connections gracefully.
    async fn disconnect(&mut self);
}

/// The exhaustive match stops a new exchange from compiling until its APIs are listed.
pub(crate) fn connected_apis() -> ConnectedApis {
    Exchange::iter()
        .map(|exchange| {
            let apis = match exchange {
                Exchange::Binance => <BinanceConnector>::CONNECTED_APIS,
                Exchange::Okx => OkxConnector::CONNECTED_APIS,
            };
            (exchange, apis)
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::phanes_client::tests::unreachable_phanes_client;

    /// For connectors that never fetch a key or alert, such as OKX's.
    pub(crate) fn test_connector_context() -> ConnectorContext {
        ConnectorContext {
            phanes_client: unreachable_phanes_client(),
            notifier: Notifier::Log,
        }
    }
}

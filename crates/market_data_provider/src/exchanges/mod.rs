mod binance;
mod okx;
mod router;
// Temporary development stand-in for venue feeds; the whole module will be deleted.
mod synthetic;
mod websocket;
mod worker;

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use strum::IntoEnumIterator;

use crate::gnosis_client::GnosisClient;
use binance::BinanceConnector;
use layline::bark::Notifier;
use layline::types::{ConnectedApi, ConnectedApis};
use layline::types::{Exchange, Instrument, InstrumentRules, MarketData, Topic};
use okx::OkxConnector;
pub(crate) use router::ExchangeRouter;

/// Fetches the rules for `instruments` from their exchange. OKX is still synthetic and has no
/// rules; its dummy arm keeps this dispatch exchange-agnostic until the real connector lands.
pub(crate) async fn fetch_instrument_rules(
    exchange: Exchange,
    demo: bool,
    instruments: &HashSet<Instrument>,
) -> Result<InstrumentRules> {
    match exchange {
        Exchange::Binance => {
            binance::instrument_rules::fetch_instrument_rules(demo, instruments).await
        }
        Exchange::Okx => Ok(InstrumentRules::new()),
    }
}

/// What every connector may need besides its venue; each worker gets its own clone.
#[derive(Clone)]
pub(crate) struct ConnectorContext {
    pub(crate) gnosis_client: GnosisClient,
    pub(crate) notifier: Notifier,
    pub(crate) instrument_rules: Arc<RwLock<InstrumentRules>>,
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
    use crate::gnosis_client::tests::unreachable_gnosis_client;
    use layline::testing::test_instrument;
    use layline::types::{Currency, UnderlyingType};

    #[tokio::test]
    async fn okx_has_no_instrument_rules() {
        let instrument = test_instrument(
            Exchange::Okx,
            Currency::Btc,
            Currency::Usdt,
            UnderlyingType::Spot,
        );
        assert!(
            fetch_instrument_rules(Exchange::Okx, false, &HashSet::from([instrument]))
                .await
                .expect("the OKX dummy never fails")
                .is_empty()
        );
    }

    /// For connectors that never fetch a key or alert, such as OKX's.
    pub(crate) fn test_connector_context() -> ConnectorContext {
        ConnectorContext {
            gnosis_client: unreachable_gnosis_client(),
            notifier: Notifier::Log,
            instrument_rules: Arc::new(RwLock::new(InstrumentRules::new())),
        }
    }
}

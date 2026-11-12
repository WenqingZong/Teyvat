//! Hardcoded facts about the Binance APIs this connector calls: what each is, where, and its docs.
//! `/connected_apis` serves them so an agent can watch the venue's changelogs; no logic belongs here.

use strum::{Display, EnumCount, EnumIter, FromRepr};

use layline::types::ConnectedApi;

/// One websocket connection each, opened in declaration order.
#[derive(
    Clone, Copy, Debug, Display, EnumCount, EnumIter, Eq, FromRepr, Hash, Ord, PartialEq, PartialOrd,
)]
pub(in crate::exchanges) enum BinanceEndpoint {
    /// Binary spot `@bestBidAsk` and `@trade`; needs a live or demo Ed25519 API key in the `X-MBX-APIKEY` header.
    SpotSbe,
    /// JSON `/public`: perpetual `@bookTicker` and `@trade`; `/market` carries neither. Futures have no SBE streams.
    FuturesPublic,
    /// JSON `/market`: perpetual `@aggTrade`, which `/public` accepts but never sends.
    FuturesMarket,
}

pub(super) const API_KEY_HEADER: &str = "X-MBX-APIKEY";

const SPOT_SBE_DOC: &str =
    "https://developers.binance.com/docs/binance-spot-api-docs/sbe-market-data-streams";
const FUTURES_DOC: &str = "https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams";

impl BinanceEndpoint {
    /// Position in declaration order, which indexes per-endpoint arrays.
    pub(super) fn index(self) -> usize {
        self as usize
    }

    pub(super) fn url(self, demo: bool) -> &'static str {
        match (self, demo) {
            (Self::SpotSbe, false) => "wss://stream-sbe.binance.com:9443/stream",
            (Self::SpotSbe, true) => "wss://demo-stream-sbe.binance.com:9443/stream",
            (Self::FuturesPublic, false) => "wss://fstream.binance.com/public/stream",
            (Self::FuturesPublic, true) => "wss://demo-fstream.binance.com/public/stream",
            (Self::FuturesMarket, false) => "wss://fstream.binance.com/market/stream",
            (Self::FuturesMarket, true) => "wss://demo-fstream.binance.com/market/stream",
        }
    }
}

pub(super) const CONNECTED_APIS: &[ConnectedApi] = &[
    ConnectedApi {
        description: "Spot SBE market data websocket, live, API key required",
        api: "wss://stream-sbe.binance.com:9443/stream",
        doc: SPOT_SBE_DOC,
    },
    ConnectedApi {
        description: "Spot SBE market data websocket, demo, API key required",
        api: "wss://demo-stream-sbe.binance.com:9443/stream",
        doc: SPOT_SBE_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures public market data websocket, live",
        api: "wss://fstream.binance.com/public/stream",
        doc: FUTURES_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures public market data websocket, demo",
        api: "wss://demo-fstream.binance.com/public/stream",
        doc: FUTURES_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures market data websocket, live",
        api: "wss://fstream.binance.com/market/stream",
        doc: FUTURES_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures market data websocket, demo",
        api: "wss://demo-fstream.binance.com/market/stream",
        doc: FUTURES_DOC,
    },
];

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn index_follows_declaration_order() {
        for (position, endpoint) in BinanceEndpoint::iter().enumerate() {
            assert_eq!(position, endpoint.index());
            assert_eq!(Some(endpoint), BinanceEndpoint::from_repr(position));
        }
        assert_eq!(BinanceEndpoint::COUNT, BinanceEndpoint::iter().count());
    }

    #[test]
    fn every_endpoint_url_is_listed_in_connected_apis() {
        for endpoint in BinanceEndpoint::iter() {
            for demo in [false, true] {
                let url = endpoint.url(demo);
                assert!(
                    url.contains("demo") == demo,
                    "{endpoint} demo {demo}: {url}"
                );
                assert!(CONNECTED_APIS.iter().any(|api| api.api == url), "{url}");
            }
        }
    }
}

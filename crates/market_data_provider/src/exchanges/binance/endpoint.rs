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

const SPOT_SBE_LIVE: &str = "wss://stream-sbe.binance.com:9443/stream";
const SPOT_SBE_DEMO: &str = "wss://demo-stream-sbe.binance.com:9443/stream";
const FUTURES_PUBLIC_LIVE: &str = "wss://fstream.binance.com/public/stream";
const FUTURES_PUBLIC_DEMO: &str = "wss://demo-fstream.binance.com/public/stream";
const FUTURES_MARKET_LIVE: &str = "wss://fstream.binance.com/market/stream";
const FUTURES_MARKET_DEMO: &str = "wss://demo-fstream.binance.com/market/stream";

const SPOT_SBE_DOC: &str =
    "https://developers.binance.com/docs/binance-spot-api-docs/sbe-market-data-streams";
const FUTURES_DOC: &str = "https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams";

pub(super) const SPOT_EXCHANGE_INFO_LIVE: &str = "https://api.binance.com/api/v3/exchangeInfo";
/// `testnet.binance.vision` is Binance's spot testnet, not a typo: the demo exchangeInfo is served
/// from that domain.
pub(super) const SPOT_EXCHANGE_INFO_DEMO: &str =
    "https://testnet.binance.vision/api/v3/exchangeInfo";
pub(super) const FUTURES_EXCHANGE_INFO_LIVE: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
pub(super) const FUTURES_EXCHANGE_INFO_DEMO: &str =
    "https://testnet.binancefuture.com/fapi/v1/exchangeInfo";
pub(super) const SPOT_EXCHANGE_INFO_DOC: &str = "https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general#exchange-info";
pub(super) const FUTURES_EXCHANGE_INFO_DOC: &str = "https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information";

impl BinanceEndpoint {
    /// Position in declaration order, which indexes per-endpoint arrays.
    pub(super) fn index(self) -> usize {
        self as usize
    }

    pub(super) fn url(self, demo: bool) -> &'static str {
        match (self, demo) {
            (Self::SpotSbe, false) => SPOT_SBE_LIVE,
            (Self::SpotSbe, true) => SPOT_SBE_DEMO,
            (Self::FuturesPublic, false) => FUTURES_PUBLIC_LIVE,
            (Self::FuturesPublic, true) => FUTURES_PUBLIC_DEMO,
            (Self::FuturesMarket, false) => FUTURES_MARKET_LIVE,
            (Self::FuturesMarket, true) => FUTURES_MARKET_DEMO,
        }
    }
}

pub(super) const CONNECTED_APIS: &[ConnectedApi] = &[
    ConnectedApi {
        description: "Spot SBE market data websocket, live, API key required",
        api: SPOT_SBE_LIVE,
        doc: SPOT_SBE_DOC,
    },
    ConnectedApi {
        description: "Spot SBE market data websocket, demo, API key required",
        api: SPOT_SBE_DEMO,
        doc: SPOT_SBE_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures public market data websocket, live",
        api: FUTURES_PUBLIC_LIVE,
        doc: FUTURES_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures public market data websocket, demo",
        api: FUTURES_PUBLIC_DEMO,
        doc: FUTURES_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures market data websocket, live",
        api: FUTURES_MARKET_LIVE,
        doc: FUTURES_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures market data websocket, demo",
        api: FUTURES_MARKET_DEMO,
        doc: FUTURES_DOC,
    },
    ConnectedApi {
        description: "Spot exchange information, live",
        api: SPOT_EXCHANGE_INFO_LIVE,
        doc: SPOT_EXCHANGE_INFO_DOC,
    },
    ConnectedApi {
        description: "Spot exchange information, demo",
        api: SPOT_EXCHANGE_INFO_DEMO,
        doc: SPOT_EXCHANGE_INFO_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures exchange information, live",
        api: FUTURES_EXCHANGE_INFO_LIVE,
        doc: FUTURES_EXCHANGE_INFO_DOC,
    },
    ConnectedApi {
        description: "USDⓈ-M futures exchange information, demo",
        api: FUTURES_EXCHANGE_INFO_DEMO,
        doc: FUTURES_EXCHANGE_INFO_DOC,
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
    fn exchange_info_urls_are_listed_in_connected_apis() {
        for url in [
            SPOT_EXCHANGE_INFO_LIVE,
            SPOT_EXCHANGE_INFO_DEMO,
            FUTURES_EXCHANGE_INFO_LIVE,
            FUTURES_EXCHANGE_INFO_DEMO,
        ] {
            assert!(CONNECTED_APIS.iter().any(|api| api.api == url), "{url}");
        }
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

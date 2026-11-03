use std::collections::HashSet;

use anyhow::Result;
use tracing::{info, trace};

use super::ExchangeConnector;
use super::synthetic::SyntheticFeed;
use crate::utils::RealClock;
use layline::types::{ConnectedApi, Exchange, MarketData, Topic};

/// Dummy: logs each call and serves synthetic data until the real websocket client lands.
pub(crate) struct OkxConnector {
    feed: SyntheticFeed,
}

impl OkxConnector {
    pub(super) fn new() -> Self {
        info!("OkxConnector::new is called");
        Self {
            feed: SyntheticFeed::new(),
        }
    }
}

impl ExchangeConnector for OkxConnector {
    const EXCHANGE: Exchange = Exchange::Okx;
    // Edited by hand: list every venue API this connector calls.
    const CONNECTED_APIS: &'static [ConnectedApi] = &[];

    async fn subscribe(&mut self, topics: &HashSet<Topic>) -> Result<()> {
        info!("OkxConnector::subscribe is called: {topics:?}");
        self.feed.add(topics);
        Ok(())
    }

    async fn unsubscribe(&mut self, topics: &HashSet<Topic>) -> Result<()> {
        info!("OkxConnector::unsubscribe is called: {topics:?}");
        self.feed.remove(topics);
        Ok(())
    }

    async fn next_event(&mut self) -> MarketData {
        trace!("OkxConnector::next_event is called");
        self.feed.next_event(&RealClock).await
    }

    async fn disconnect(&mut self) {
        info!("OkxConnector::disconnect is called");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use layline::testing::test_instrument;
    use layline::types::{Currency, Instrument, MarketDataType, UnderlyingType};

    fn instrument(underlying_type: UnderlyingType) -> Instrument {
        test_instrument(
            Exchange::Okx,
            Currency::Btc,
            Currency::Usdt,
            underlying_type,
        )
    }

    #[tokio::test]
    async fn dummy_connector_serves_subscribed_topics() {
        let topic = Topic {
            instrument: instrument(UnderlyingType::Spot),
            market_data_type: MarketDataType::Bbo,
        };
        let mut connector = OkxConnector::new();
        connector
            .subscribe(&HashSet::from([topic]))
            .await
            .expect("subscribe is expected to not fail");
        assert_eq!(topic, connector.next_event().await.topic());
        connector
            .unsubscribe(&HashSet::from([topic]))
            .await
            .expect("unsubscribe is expected to not fail");
    }
}

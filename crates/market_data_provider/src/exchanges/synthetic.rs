//! TEMPORARY, development only: synthetic data standing in for venue feeds.
//! Never human-reviewed; the whole module will be deleted once real connectors land.
use crate::utils::Clock;
use layline::types::{Bbo, LatencyMetric, MarketData, MarketDataType, Side, Topic, Trade};
use rust_decimal::{Decimal, dec};
use std::collections::{HashSet, VecDeque};
use tokio::time::{Duration, Interval, MissedTickBehavior, interval};

const SYNTHETIC_INTERVAL: Duration = Duration::from_millis(10);
const TICK_SIZE: Decimal = dec!(0.01);
const LOT_SIZE: Decimal = dec!(0.001);
const SYNTHETIC_BID_PRICE: Decimal = dec!(99.99);
const SYNTHETIC_LOTS: Decimal = dec!(1000);

/// Stand-in for a venue stream: emits one message per subscription every tick.
pub(super) struct SyntheticFeed {
    subscriptions: Vec<Topic>,
    pending: VecDeque<MarketData>,
    ticker: Interval,
    sequence: usize,
}

impl SyntheticFeed {
    pub(super) fn new() -> Self {
        let mut ticker = interval(SYNTHETIC_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            subscriptions: Vec::new(),
            pending: VecDeque::new(),
            ticker,
            sequence: 0,
        }
    }

    pub(super) fn add(&mut self, topics: &HashSet<Topic>) {
        for topic in topics {
            if !self.subscriptions.contains(topic) {
                self.subscriptions.push(*topic);
            }
        }
    }

    pub(super) fn remove(&mut self, topics: &HashSet<Topic>) {
        self.subscriptions
            .retain(|subscribed| !topics.contains(subscribed));
        self.pending.retain(|data| !topics.contains(&data.topic()));
    }

    pub(super) async fn next_event(&mut self, clock: &impl Clock) -> MarketData {
        loop {
            if let Some(data) = self.pending.pop_front() {
                return data;
            }
            self.ticker.tick().await;
            self.sequence += 1;
            let latency_metric = LatencyMetric::received(clock.now());
            for topic in &self.subscriptions {
                self.pending
                    .push_back(synthetic_data(*topic, latency_metric, self.sequence));
            }
        }
    }
}

/// Prices and sizes sit on the dummy trading rule's grid, as a venue's would.
fn synthetic_data(topic: Topic, latency_metric: LatencyMetric, sequence: usize) -> MarketData {
    let ask_price = SYNTHETIC_BID_PRICE + TICK_SIZE;
    let size = SYNTHETIC_LOTS * LOT_SIZE;
    match topic.market_data_type {
        MarketDataType::Bbo => MarketData::Bbo(Bbo {
            instrument: topic.instrument,
            exchange_time_nanoseconds: None,
            latency_metric,
            ask_price,
            ask_size: size,
            bid_price: SYNTHETIC_BID_PRICE,
            bid_size: size,
            sequence,
        }),
        MarketDataType::Trade => MarketData::Trade(Trade {
            instrument: topic.instrument,
            exchange_time_nanoseconds: None,
            latency_metric,
            price: ask_price,
            size,
            taker_side: Side::Buy,
            sequence,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::RealClock;
    use layline::time::get_current_timestamp_nanoseconds;
    use layline::types::{Currency, Exchange, Instrument, UnderlyingType};

    fn instrument() -> Instrument {
        layline::testing::instrument(
            Exchange::Binance,
            Currency::Eth,
            Currency::Usdt,
            UnderlyingType::Spot,
        )
    }

    fn topic(market_data_type: MarketDataType) -> Topic {
        layline::testing::topic(instrument(), market_data_type)
    }

    #[tokio::test]
    async fn feed_emits_one_message_per_subscription_per_tick() {
        let mut feed = SyntheticFeed::new();
        feed.add(&HashSet::from([topic(MarketDataType::Bbo)]));
        feed.add(&HashSet::from([topic(MarketDataType::Bbo)]));
        feed.add(&HashSet::from([topic(MarketDataType::Trade)]));

        let first = feed.next_event(&RealClock).await.topic();
        let second = feed.next_event(&RealClock).await.topic();
        assert_eq!(instrument(), first.instrument);
        assert_eq!(MarketDataType::Bbo, first.market_data_type);
        assert_eq!(MarketDataType::Trade, second.market_data_type);
        assert!(feed.pending.is_empty());
    }

    #[tokio::test]
    async fn removed_subscription_stops_emitting() {
        let mut feed = SyntheticFeed::new();
        feed.add(&HashSet::from([topic(MarketDataType::Bbo)]));
        feed.add(&HashSet::from([topic(MarketDataType::Trade)]));
        assert_eq!(
            MarketDataType::Bbo,
            feed.next_event(&RealClock).await.topic().market_data_type
        );

        feed.remove(&HashSet::from([topic(MarketDataType::Trade)]));
        assert!(feed.pending.is_empty());
        for _ in 0..3 {
            assert_eq!(
                MarketDataType::Bbo,
                feed.next_event(&RealClock).await.topic().market_data_type
            );
        }
    }

    #[test]
    fn synthetic_prices_follow_trading_rule() {
        let MarketData::Bbo(bbo) = synthetic_data(
            topic(MarketDataType::Bbo),
            LatencyMetric::received(get_current_timestamp_nanoseconds()),
            1,
        ) else {
            panic!("a bbo topic is expected to produce a bbo");
        };
        assert_eq!(TICK_SIZE, bbo.ask_price - bbo.bid_price);
        assert_eq!(dec!(1), bbo.bid_size);
    }
}

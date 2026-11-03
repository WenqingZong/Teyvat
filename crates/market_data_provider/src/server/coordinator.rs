use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tracing::warn;

use super::format_topics;
use super::registry::SubscriptionRegistry;
use crate::exchanges::ExchangeRouter;
use layline::provider_api::{SubscriptionResult, group_by_reason};
use layline::types::{StrategyName, Topic};

/// Owns all subscription state; the server serialises calls through one lock.
pub(crate) struct Coordinator {
    registry: SubscriptionRegistry,
    router: ExchangeRouter,
    lease_duration: Duration,
}

impl Coordinator {
    pub(crate) fn new(router: ExchangeRouter, lease_duration: Duration) -> Self {
        Self {
            registry: SubscriptionRegistry::default(),
            router,
            lease_duration,
        }
    }

    pub(crate) async fn subscribe(
        &mut self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
        now: Instant,
    ) -> HashMap<Topic, SubscriptionResult> {
        let mut target = self.registry.held_by(strategy_name);
        target.extend(topics.iter().copied());
        self.apply(strategy_name, &target, topics, now).await
    }

    /// Makes `topics` the strategy's whole set, releasing whatever else it held. Silent unless the set changed.
    pub(crate) async fn resubscribe(
        &mut self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
        now: Instant,
    ) -> HashMap<Topic, SubscriptionResult> {
        let change = TopicSetChange::between(&self.registry.held_by(strategy_name), topics);
        if !change.is_empty() {
            warn!(
                "{strategy_name} resubscribed with a different set: added {}, dropped {}",
                format_topics(&change.added),
                format_topics(&change.dropped)
            );
        }
        self.apply(strategy_name, topics, topics, now).await
    }

    pub(crate) async fn unsubscribe(
        &mut self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
        now: Instant,
    ) {
        self.registry
            .renew_lease(strategy_name, now + self.lease_duration);
        self.release(strategy_name, topics).await;
    }

    pub(crate) async fn expire_leases(&mut self, now: Instant) {
        for (strategy_name, topics) in self.registry.expire_leases(now) {
            self.release(&strategy_name, &topics).await;
        }
    }

    pub(crate) fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) fn subscribed_topics(&self) -> HashSet<Topic> {
        self.registry.subscribed_topics()
    }

    /// Moves the strategy's held set to `target`. Returns the ring path, or why each requested topic was refused.
    async fn apply(
        &mut self,
        strategy_name: &StrategyName,
        target: &HashSet<Topic>,
        requested: &HashSet<Topic>,
        now: Instant,
    ) -> HashMap<Topic, SubscriptionResult> {
        self.registry
            .renew_lease(strategy_name, now + self.lease_duration);
        let change = TopicSetChange::between(&self.registry.held_by(strategy_name), target);

        // A stopped worker took the strategy's rings with it, so its held topics are subscribed again.
        let to_subscribe: HashSet<Topic> = target
            .iter()
            .filter(|topic| {
                change.added.contains(topic) || !self.router.is_running(topic.instrument.exchange)
            })
            .copied()
            .collect();
        let failures = self.router.subscribe(strategy_name, &to_subscribe).await;
        let subscribed: HashSet<Topic> = to_subscribe
            .into_iter()
            .filter(|topic| !failures.contains_key(topic))
            .collect();
        self.registry.add(strategy_name, &subscribed);
        self.release(strategy_name, &change.dropped).await;

        for (reason, topics) in group_by_reason(&failures) {
            warn!(
                "{strategy_name} was refused {}: {reason}",
                format_topics(&topics)
            );
        }
        let root_directory = self.router.root_directory();
        requested
            .iter()
            .map(|topic| {
                let subscription_result = match failures.get(topic) {
                    Some(reason) => Err(reason.clone()),
                    None => Ok(topic.file_path(root_directory, strategy_name)),
                };
                (*topic, subscription_result)
            })
            .collect()
    }

    /// Stops delivering `topics` to the strategy; the exchange stops sending those no strategy holds any more.
    async fn release(&mut self, strategy_name: &StrategyName, topics: &HashSet<Topic>) {
        self.registry.remove(strategy_name, topics);
        let failures = self.router.unsubscribe(strategy_name, topics).await;
        for (reason, topics) in group_by_reason(&failures) {
            warn!(
                "failed to release {} of {strategy_name}: {reason}",
                format_topics(&topics)
            );
        }
    }
}

/// What a strategy must add to, and drop from, its held set to reach `requested`.
#[derive(Debug, PartialEq)]
struct TopicSetChange {
    added: HashSet<Topic>,
    dropped: HashSet<Topic>,
}

impl TopicSetChange {
    fn between(held: &HashSet<Topic>, requested: &HashSet<Topic>) -> Self {
        Self {
            added: requested.difference(held).copied().collect(),
            dropped: held.difference(requested).copied().collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.added.is_empty() && self.dropped.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use layline::testing::{test_instrument, test_topic};
    use layline::types::{Currency, Exchange, MarketDataType, UnderlyingType};

    fn topic(exchange: Exchange, market_data_type: MarketDataType) -> Topic {
        test_topic(
            test_instrument(
                exchange,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Spot,
            ),
            market_data_type,
        )
    }

    #[test]
    fn change_lists_added_and_dropped_topics() {
        let binance_bbo = topic(Exchange::Binance, MarketDataType::Bbo);
        let binance_trade = topic(Exchange::Binance, MarketDataType::Trade);
        let okx_trade = topic(Exchange::Okx, MarketDataType::Trade);
        let held = HashSet::from([binance_bbo, okx_trade]);
        let requested = HashSet::from([binance_bbo, binance_trade]);
        let change = TopicSetChange::between(&held, &requested);
        assert_eq!(
            TopicSetChange {
                added: HashSet::from([binance_trade]),
                dropped: HashSet::from([okx_trade]),
            },
            change
        );
        assert!(!change.is_empty());
        assert!(TopicSetChange::between(&held, &held).is_empty());
    }
}

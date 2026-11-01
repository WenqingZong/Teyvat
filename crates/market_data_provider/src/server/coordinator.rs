use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use super::format_topics;
use super::registry::SubscriptionRegistry;
use crate::exchanges::ExchangeRouter;
use layline::provider_api::{SubscriptionResult, TopicStatus, group_by_reason};
use layline::types::Topic;

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
        strategy_name: &str,
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
        strategy_name: &str,
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
        strategy_name: &str,
        topics: &HashSet<Topic>,
        now: Instant,
    ) {
        self.registry
            .renew_lease(strategy_name, now + self.lease_duration);
        let orphaned = self.registry.remove_holder(strategy_name, topics);
        self.release(orphaned).await;
    }

    pub(crate) async fn expire_leases(&mut self, now: Instant) {
        let orphaned = self.registry.expire_leases(now);
        self.release(orphaned).await;
    }

    pub(crate) fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) fn statuses(&self) -> HashMap<Topic, TopicStatus> {
        self.registry.statuses(self.router.root_directory())
    }

    /// Moves the strategy's held set to `target`, activating and releasing topics as needed.
    /// Returns the ring path, or why each requested topic was refused.
    async fn apply(
        &mut self,
        strategy_name: &str,
        target: &HashSet<Topic>,
        requested: &HashSet<Topic>,
        now: Instant,
    ) -> HashMap<Topic, SubscriptionResult> {
        self.registry
            .renew_lease(strategy_name, now + self.lease_duration);
        let change = TopicSetChange::between(&self.registry.held_by(strategy_name), target);

        let mut failures: HashMap<Topic, String> = HashMap::new();
        let mut to_activate: HashSet<Topic> = HashSet::new();
        for topic in &change.added {
            if self.registry.is_active(topic) && self.router.is_running(topic.instrument.exchange) {
                self.registry.add_holder(topic, strategy_name);
            } else {
                to_activate.insert(*topic);
            }
        }
        for topic in target {
            if self.registry.is_active(topic) && !self.router.is_running(topic.instrument.exchange)
            {
                to_activate.insert(*topic);
            }
        }

        if !to_activate.is_empty() {
            failures.extend(self.router.subscribe(&to_activate).await);
            for topic in &to_activate {
                if !failures.contains_key(topic) {
                    self.registry.add_holder(topic, strategy_name);
                }
            }
        }

        let orphaned = self.registry.remove_holder(strategy_name, &change.dropped);
        self.release(orphaned).await;
        for (reason, topic_names) in group_by_reason(&failures) {
            warn!(
                "{strategy_name} was refused [{}]: {reason}",
                topic_names.join(", ")
            );
        }
        requested
            .iter()
            .map(|topic| {
                let subscription_result = match failures.get(topic) {
                    Some(reason) => Err(reason.clone()),
                    None => Ok(topic.file_path(self.router.root_directory())),
                };
                (*topic, subscription_result)
            })
            .collect()
    }

    /// Unsubscribes topics that no strategy holds any more.
    async fn release(&mut self, orphaned: HashSet<Topic>) {
        let failures = self.router.unsubscribe(&orphaned).await;
        for topic in &orphaned {
            match failures.get(topic) {
                Some(reason) => warn!("failed to unsubscribe {topic}: {reason}"),
                None => info!("released {topic}, no strategy holds it"),
            }
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
    use layline::types::{Currency, Exchange, MarketDataType, UnderlyingType};

    fn topic(exchange: Exchange, market_data_type: MarketDataType) -> Topic {
        layline::testing::topic(
            layline::testing::instrument(
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

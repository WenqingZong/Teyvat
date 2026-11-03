use std::collections::{HashMap, HashSet};
use std::time::Instant;

use layline::types::{StrategyName, Topic};

/// The topics each strategy holds and when its lease ends; a strategy holds its topics until it
/// unsubscribes or its lease expires.
#[derive(Default)]
pub(crate) struct SubscriptionRegistry {
    topics_by_strategy: HashMap<StrategyName, HashSet<Topic>>,
    lease_expiries: HashMap<StrategyName, Instant>,
}

impl SubscriptionRegistry {
    pub(crate) fn held_by(&self, strategy_name: &StrategyName) -> HashSet<Topic> {
        self.topics_by_strategy
            .get(strategy_name)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn add(&mut self, strategy_name: &StrategyName, topics: &HashSet<Topic>) {
        self.topics_by_strategy
            .entry(strategy_name.to_owned())
            .or_default()
            .extend(topics);
    }

    pub(crate) fn remove(&mut self, strategy_name: &StrategyName, topics: &HashSet<Topic>) {
        let Some(held) = self.topics_by_strategy.get_mut(strategy_name) else {
            return;
        };
        held.retain(|topic| !topics.contains(topic));
        if held.is_empty() {
            self.topics_by_strategy.remove(strategy_name);
        }
    }

    pub(crate) fn renew_lease(&mut self, strategy_name: &StrategyName, expires_at: Instant) {
        self.lease_expiries
            .insert(strategy_name.to_owned(), expires_at);
    }

    /// Forgets every strategy whose lease ended by `now`, returning the topics each one held.
    pub(crate) fn expire_leases(&mut self, now: Instant) -> HashMap<StrategyName, HashSet<Topic>> {
        let expired: Vec<StrategyName> = self
            .lease_expiries
            .iter()
            .filter(|(_, expires_at)| **expires_at <= now)
            .map(|(strategy_name, _)| strategy_name.clone())
            .collect();
        expired
            .into_iter()
            .map(|strategy_name| {
                self.lease_expiries.remove(&strategy_name);
                let held = self
                    .topics_by_strategy
                    .remove(&strategy_name)
                    .unwrap_or_default();
                (strategy_name, held)
            })
            .collect()
    }

    /// Every topic some strategy holds, which is what the provider publishes.
    pub(crate) fn subscribed_topics(&self) -> HashSet<Topic> {
        self.topics_by_strategy
            .values()
            .flatten()
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use layline::testing::{test_instrument, test_topic};
    use layline::types::{Currency, Exchange, MarketDataType, UnderlyingType};

    const FIRST_STRATEGY_NAME: &str = "first";
    const SECOND_STRATEGY_NAME: &str = "second";
    const LEASE: Duration = Duration::from_secs(60);

    fn topic(market_data_type: MarketDataType) -> Topic {
        test_topic(
            test_instrument(
                Exchange::Okx,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Perpetual,
            ),
            market_data_type,
        )
    }

    fn bbo() -> HashSet<Topic> {
        HashSet::from([topic(MarketDataType::Bbo)])
    }

    fn trade() -> HashSet<Topic> {
        HashSet::from([topic(MarketDataType::Trade)])
    }

    #[test]
    fn strategies_hold_what_they_add_until_they_remove_it() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let mut registry = SubscriptionRegistry::default();
        registry.add(&first_strategy_name, &bbo());
        registry.add(&first_strategy_name, &trade());
        registry.add(&second_strategy_name, &bbo());
        assert_eq!(&bbo() | &trade(), registry.held_by(&first_strategy_name));

        registry.remove(&first_strategy_name, &bbo());
        assert_eq!(trade(), registry.held_by(&first_strategy_name));
        assert_eq!(bbo(), registry.held_by(&second_strategy_name));
    }

    #[test]
    fn subscribed_topics_are_every_strategys_topics() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let mut registry = SubscriptionRegistry::default();
        registry.add(&first_strategy_name, &bbo());
        registry.add(&second_strategy_name, &bbo());
        registry.add(&second_strategy_name, &trade());
        assert_eq!(&bbo() | &trade(), registry.subscribed_topics());

        registry.remove(&second_strategy_name, &(&bbo() | &trade()));
        registry.remove(&first_strategy_name, &bbo());
        assert!(registry.subscribed_topics().is_empty());
    }

    #[test]
    fn expired_lease_releases_only_that_strategy() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let mut registry = SubscriptionRegistry::default();
        registry.add(&first_strategy_name, &(&bbo() | &trade()));
        registry.add(&second_strategy_name, &bbo());
        let start = Instant::now();
        registry.renew_lease(&first_strategy_name, start + LEASE);
        registry.renew_lease(&second_strategy_name, start + LEASE * 2);

        assert!(
            registry
                .expire_leases(start + LEASE - Duration::from_secs(1))
                .is_empty()
        );
        assert_eq!(
            HashMap::from([(StrategyName::from(FIRST_STRATEGY_NAME), &bbo() | &trade())]),
            registry.expire_leases(start + LEASE)
        );
        assert!(registry.held_by(&first_strategy_name).is_empty());
        assert_eq!(bbo(), registry.held_by(&second_strategy_name));
    }

    #[test]
    fn renewed_lease_postpones_expiry() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let mut registry = SubscriptionRegistry::default();
        registry.add(&first_strategy_name, &bbo());
        let start = Instant::now();
        registry.renew_lease(&first_strategy_name, start + LEASE);
        registry.renew_lease(&first_strategy_name, start + LEASE * 2);
        assert!(registry.expire_leases(start + LEASE).is_empty());
    }
}

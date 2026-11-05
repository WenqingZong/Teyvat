use std::collections::{HashMap, HashSet};
use std::time::Instant;

use layline::types::{StrategyName, Topic};

/// The topics each strategy holds and when its lease ends; a strategy holds its topics until it
/// unsubscribes or its lease expires.
#[derive(Default)]
pub(crate) struct SubscriptionRegistry {
    holdings: HashMap<StrategyName, Holding>,
}

struct Holding {
    topics: HashSet<Topic>,
    lease_expires_at: Instant,
}

impl SubscriptionRegistry {
    pub(crate) fn held_by(&self, strategy_name: &StrategyName) -> HashSet<Topic> {
        self.holdings
            .get(strategy_name)
            .map(|holding| holding.topics.clone())
            .unwrap_or_default()
    }

    /// Replaces the strategy's topics and lease; a strategy without topics is forgotten.
    pub(crate) fn set(
        &mut self,
        strategy_name: &StrategyName,
        topics: HashSet<Topic>,
        lease_expires_at: Instant,
    ) {
        if topics.is_empty() {
            self.holdings.remove(strategy_name);
            return;
        }
        let holding = Holding {
            topics,
            lease_expires_at,
        };
        self.holdings.insert(strategy_name.clone(), holding);
    }

    /// Forgets every strategy whose lease ended by `now`, returning the topics each one held.
    pub(crate) fn expire_leases(&mut self, now: Instant) -> HashMap<StrategyName, HashSet<Topic>> {
        self.holdings
            .extract_if(|_, holding| holding.lease_expires_at <= now)
            .map(|(strategy_name, holding)| (strategy_name, holding.topics))
            .collect()
    }

    /// Every topic some strategy holds, which is what the provider publishes.
    pub(crate) fn subscribed_topics(&self) -> HashSet<Topic> {
        self.holdings
            .values()
            .flat_map(|holding| &holding.topics)
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
    fn strategies_hold_what_they_were_set_to() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let lease_expires_at = Instant::now() + LEASE;
        let mut registry = SubscriptionRegistry::default();
        registry.set(&first_strategy_name, &bbo() | &trade(), lease_expires_at);
        registry.set(&second_strategy_name, bbo(), lease_expires_at);
        assert_eq!(&bbo() | &trade(), registry.held_by(&first_strategy_name));
        assert_eq!(&bbo() | &trade(), registry.subscribed_topics());

        registry.set(&first_strategy_name, trade(), lease_expires_at);
        assert_eq!(trade(), registry.held_by(&first_strategy_name));
        assert_eq!(bbo(), registry.held_by(&second_strategy_name));

        registry.set(&first_strategy_name, HashSet::new(), lease_expires_at);
        registry.set(&second_strategy_name, HashSet::new(), lease_expires_at);
        assert!(registry.subscribed_topics().is_empty());
        assert!(registry.expire_leases(lease_expires_at).is_empty());
    }

    #[test]
    fn expired_lease_releases_only_that_strategy() {
        let first_strategy_name = StrategyName::from(FIRST_STRATEGY_NAME);
        let second_strategy_name = StrategyName::from(SECOND_STRATEGY_NAME);
        let start = Instant::now();
        let mut registry = SubscriptionRegistry::default();
        registry.set(&first_strategy_name, &bbo() | &trade(), start + LEASE);
        registry.set(&second_strategy_name, bbo(), start + LEASE * 2);

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
        let start = Instant::now();
        let mut registry = SubscriptionRegistry::default();
        registry.set(&first_strategy_name, bbo(), start + LEASE);
        registry.set(&first_strategy_name, bbo(), start + LEASE * 2);
        assert!(registry.expire_leases(start + LEASE).is_empty());
    }
}

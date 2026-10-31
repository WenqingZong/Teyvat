use crate::server::api::TopicStatus;
use crate::types::Topic;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

struct TopicState {
    holders: HashSet<String>,
}

/// Reference counts exchange subscriptions by strategy name; a strategy holds its topics until it unsubscribes or its lease expires.
#[derive(Default)]
pub(crate) struct SubscriptionRegistry {
    topics: HashMap<Topic, TopicState>,
    lease_expiries: HashMap<String, Instant>,
}

impl SubscriptionRegistry {
    pub(crate) fn is_active(&self, topic: &Topic) -> bool {
        self.topics.contains_key(topic)
    }

    /// Adds `strategy_name` to `topic`, activating the topic when it is not active yet.
    pub(crate) fn add_holder(&mut self, topic: &Topic, strategy_name: &str) {
        self.topics
            .entry(*topic)
            .or_insert_with(|| TopicState {
                holders: HashSet::new(),
            })
            .holders
            .insert(strategy_name.to_owned());
    }

    pub(crate) fn renew_lease(&mut self, strategy_name: &str, expires_at: Instant) {
        self.lease_expiries
            .insert(strategy_name.to_owned(), expires_at);
    }

    pub(crate) fn held_by(&self, strategy_name: &str) -> HashSet<Topic> {
        self.topics
            .iter()
            .filter(|(_, state)| state.holders.contains(strategy_name))
            .map(|(topic, _)| *topic)
            .collect()
    }

    /// Forgets and returns the topics left without holders.
    pub(crate) fn remove_holder(
        &mut self,
        strategy_name: &str,
        topics: &HashSet<Topic>,
    ) -> HashSet<Topic> {
        let mut orphaned = HashSet::new();
        for topic in topics {
            let Some(state) = self.topics.get_mut(topic) else {
                continue;
            };
            if !state.holders.remove(strategy_name) || !state.holders.is_empty() {
                continue;
            }
            self.topics.remove(topic);
            orphaned.insert(*topic);
        }
        orphaned
    }

    /// Drops every strategy whose lease ended by `now`; returns the topics left without holders.
    pub(crate) fn expire_leases(&mut self, now: Instant) -> HashSet<Topic> {
        let expired: Vec<String> = self
            .lease_expiries
            .iter()
            .filter(|(_, expires_at)| **expires_at <= now)
            .map(|(strategy_name, _)| strategy_name.clone())
            .collect();
        let mut orphaned = HashSet::new();
        for strategy_name in expired {
            self.lease_expiries.remove(&strategy_name);
            let held = self.held_by(&strategy_name);
            orphaned.extend(self.remove_holder(&strategy_name, &held));
        }
        orphaned
    }

    /// Every active topic, its ring path and the strategies holding it.
    pub(crate) fn statuses(&self, root_directory: &Path) -> HashMap<Topic, TopicStatus> {
        self.topics
            .iter()
            .map(|(topic, state)| {
                let status = TopicStatus {
                    path: topic.file_path(root_directory),
                    strategy_names: state.holders.iter().cloned().collect(),
                };
                (*topic, status)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Currency, Exchange, MarketDataType, UnderlyingType};
    use std::time::Duration;

    const FIRST: &str = "first";
    const SECOND: &str = "second";
    const LEASE: Duration = Duration::from_secs(60);

    fn topic(market_data_type: MarketDataType) -> Topic {
        crate::testing::topic(
            crate::testing::instrument(
                Exchange::Okx,
                Currency::Btc,
                Currency::Usdt,
                UnderlyingType::Perpetual,
            ),
            market_data_type,
        )
    }

    fn registry_held_by(strategy_names: &[&str]) -> SubscriptionRegistry {
        let mut registry = SubscriptionRegistry::default();
        for strategy_name in strategy_names {
            registry.add_holder(&topic(MarketDataType::Bbo), strategy_name);
        }
        registry
    }

    #[test]
    fn holding_a_topic_activates_it() {
        let mut registry = SubscriptionRegistry::default();
        let bbo = topic(MarketDataType::Bbo);
        assert!(!registry.is_active(&bbo));
        registry.add_holder(&bbo, FIRST);
        assert!(registry.is_active(&bbo));
        assert_eq!(HashSet::from([bbo]), registry.held_by(FIRST));
    }

    #[test]
    fn topic_stays_active_while_any_strategy_holds_it() {
        let mut registry = registry_held_by(&[FIRST, SECOND]);

        assert!(
            registry
                .remove_holder(FIRST, &HashSet::from([topic(MarketDataType::Bbo)]))
                .is_empty()
        );
        assert!(registry.is_active(&topic(MarketDataType::Bbo)));
        assert_eq!(
            HashSet::from([topic(MarketDataType::Bbo)]),
            registry.remove_holder(SECOND, &HashSet::from([topic(MarketDataType::Bbo)]))
        );
        assert!(!registry.is_active(&topic(MarketDataType::Bbo)));
    }

    #[test]
    fn removing_unheld_topic_changes_nothing() {
        let mut registry = registry_held_by(&[FIRST]);
        assert!(
            registry
                .remove_holder(
                    SECOND,
                    &HashSet::from([topic(MarketDataType::Bbo), topic(MarketDataType::Trade)])
                )
                .is_empty()
        );
        assert_eq!(
            HashSet::from([topic(MarketDataType::Bbo)]),
            registry.held_by(FIRST)
        );
    }

    #[test]
    fn expired_lease_releases_only_that_strategy() {
        let mut registry = registry_held_by(&[FIRST, SECOND]);
        registry.add_holder(&topic(MarketDataType::Trade), FIRST);
        let start = Instant::now();
        registry.renew_lease(FIRST, start + LEASE);
        registry.renew_lease(SECOND, start + LEASE * 2);

        assert!(
            registry
                .expire_leases(start + LEASE - Duration::from_secs(1))
                .is_empty()
        );
        assert_eq!(
            HashSet::from([topic(MarketDataType::Trade)]),
            registry.expire_leases(start + LEASE)
        );
        assert_eq!(
            HashSet::from([topic(MarketDataType::Bbo)]),
            registry.held_by(SECOND)
        );
        assert!(registry.held_by(FIRST).is_empty());
    }

    #[test]
    fn renewed_lease_postpones_expiry() {
        let mut registry = registry_held_by(&[FIRST]);
        let start = Instant::now();
        registry.renew_lease(FIRST, start + LEASE);
        registry.renew_lease(FIRST, start + LEASE * 2);
        assert!(registry.expire_leases(start + LEASE).is_empty());
    }

    #[test]
    fn statuses_list_every_holder() {
        let registry = registry_held_by(&[SECOND, FIRST]);
        assert_eq!(
            HashMap::from([(
                topic(MarketDataType::Bbo),
                TopicStatus {
                    path: topic(MarketDataType::Bbo).file_path(Path::new("/dev/shm")),
                    strategy_names: [FIRST.to_owned(), SECOND.to_owned()].into(),
                },
            )]),
            registry.statuses(Path::new("/dev/shm"))
        );
    }
}

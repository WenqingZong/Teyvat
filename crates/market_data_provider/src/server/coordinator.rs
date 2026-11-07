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
        let target = &self.registry.held_by(strategy_name) | topics;
        self.apply(strategy_name, &target, topics, now).await
    }

    /// Makes `topics` the strategy's whole set, releasing whatever else it held. Silent unless the set changed.
    pub(crate) async fn resubscribe(
        &mut self,
        strategy_name: &StrategyName,
        topics: &HashSet<Topic>,
        now: Instant,
    ) -> HashMap<Topic, SubscriptionResult> {
        let held = self.registry.held_by(strategy_name);
        if held != *topics {
            let added_topics = format_topics(topics.difference(&held));
            let dropped_topics = format_topics(held.difference(topics));
            warn!(
                "{strategy_name} resubscribed with a different set: added {added_topics}, dropped {dropped_topics}"
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
        let target = &self.registry.held_by(strategy_name) - topics;
        self.apply(strategy_name, &target, &HashSet::new(), now)
            .await;
    }

    pub(crate) async fn expire_leases(&mut self, now: Instant) {
        for (strategy_name, held) in self.registry.expire_leases(now) {
            self.move_topics(&strategy_name, &held, &HashSet::new())
                .await;
        }
    }

    pub(crate) fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) fn subscribed_topics(&self) -> HashSet<Topic> {
        self.registry.subscribed_topics()
    }

    /// Moves the strategy's held set to `target` and renews its lease. Returns the ring path, or why
    /// each requested topic was refused.
    async fn apply(
        &mut self,
        strategy_name: &StrategyName,
        target: &HashSet<Topic>,
        requested: &HashSet<Topic>,
        now: Instant,
    ) -> HashMap<Topic, SubscriptionResult> {
        let held = self.registry.held_by(strategy_name);
        let failures = self.move_topics(strategy_name, &held, target).await;
        // A held topic a new worker refused stays held, so the next request sends it again.
        let new_held = target
            .iter()
            .filter(|topic| held.contains(topic) || !failures.contains_key(topic))
            .copied()
            .collect();
        self.registry
            .set(strategy_name, new_held, now + self.lease_duration);

        let root_directory = self.router.root_directory();
        requested
            .iter()
            .map(|topic| {
                let subscription_result = match failures.get(topic) {
                    Some(reason) => Err(reason.clone()),
                    None => Ok(topic.ring_key().file_path(root_directory, strategy_name)),
                };
                (*topic, subscription_result)
            })
            .collect()
    }

    /// Has the router move the strategy from `held` to `target`, and logs every failure.
    async fn move_topics(
        &mut self,
        strategy_name: &StrategyName,
        held: &HashSet<Topic>,
        target: &HashSet<Topic>,
    ) -> HashMap<Topic, String> {
        let failures = self.router.set_topics(strategy_name, held, target).await;
        let (refused, unreleased): (HashMap<Topic, String>, HashMap<Topic, String>) = failures
            .clone()
            .into_iter()
            .partition(|(topic, _)| target.contains(topic));
        for (reason, topics) in group_by_reason(&unreleased) {
            let formatted_topics = format_topics(&topics);
            warn!("failed to release {formatted_topics} of {strategy_name}: {reason}");
        }
        for (reason, topics) in group_by_reason(&refused) {
            let formatted_topics = format_topics(&topics);
            warn!("{strategy_name} was refused {formatted_topics}: {reason}");
        }
        failures
    }
}

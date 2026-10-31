//! Venue-neutral state for a connector that owns one websocket per endpoint.
use super::client::WebSocketClient;
use super::connection::{Heartbeat, WebSocketConnection};
use layline::backoff::exponential_backoff;
use layline::types::Topic;
use std::array::from_fn;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::Instant;

/// A venue's websocket endpoint enum.
pub(crate) trait EndpointId: Copy + Eq + Ord + Display {
    fn index(self) -> usize;
    fn from_index(index: usize) -> Self;
}

pub(crate) struct EndpointState<Client> {
    pub(crate) connection: WebSocketConnection<Client>,
    /// The venue's subscription key (Binance stream name, OKX channel) mapped to its topic.
    pub(crate) topics_by_key: HashMap<String, Topic>,
    /// Set while the venue may lack some of `topics_by_key`: after a lost connection or failed request.
    pub(crate) restore_plan: Option<RestorePlan>,
}

#[derive(Clone, Copy)]
pub(crate) struct RestorePlan {
    pub(crate) scheduled_at: Instant,
    pub(crate) consecutive_failures: u32,
}

/// One websocket connection per endpoint, with its topics and restore schedule.
pub(crate) struct EndpointSet<const COUNT: usize, Endpoint: EndpointId, Client> {
    endpoints: [EndpointState<Client>; COUNT],
    _endpoint: PhantomData<Endpoint>,
}

impl<const COUNT: usize, Endpoint: EndpointId, Client: WebSocketClient>
    EndpointSet<COUNT, Endpoint, Client>
{
    pub(crate) fn new(
        mut create_websocket: impl FnMut(Endpoint) -> Client,
        heartbeat: Heartbeat,
    ) -> Self {
        let endpoints = from_fn(|index| EndpointState {
            connection: WebSocketConnection::new(
                create_websocket(Endpoint::from_index(index)),
                heartbeat,
            ),
            topics_by_key: HashMap::new(),
            restore_plan: None,
        });
        Self {
            endpoints,
            _endpoint: PhantomData,
        }
    }

    /// Disjoint mutable borrows, as `next_event` reads two endpoints at once.
    pub(crate) fn states_mut(&mut self) -> &mut [EndpointState<Client>; COUNT] {
        &mut self.endpoints
    }

    pub(crate) fn state(&self, endpoint: Endpoint) -> &EndpointState<Client> {
        &self.endpoints[endpoint.index()]
    }

    pub(crate) fn state_mut(&mut self, endpoint: Endpoint) -> &mut EndpointState<Client> {
        &mut self.endpoints[endpoint.index()]
    }

    pub(crate) fn next_restore_at(&self) -> Option<Instant> {
        self.endpoints
            .iter()
            .filter_map(|state| {
                state
                    .restore_plan
                    .map(|restore_plan| restore_plan.scheduled_at)
            })
            .min()
    }

    pub(crate) fn due_endpoints(&self, now: Instant) -> Vec<Endpoint> {
        self.endpoints
            .iter()
            .enumerate()
            .filter(|(_, state)| {
                state
                    .restore_plan
                    .is_some_and(|restore_plan| restore_plan.scheduled_at <= now)
            })
            .map(|(index, _)| Endpoint::from_index(index))
            .collect()
    }

    /// Records topics by venue key; returns the keys that were new and whether every key must be resent.
    pub(crate) fn record_topics(
        &mut self,
        endpoint: Endpoint,
        topics_by_key: HashMap<String, Topic>,
    ) -> (Vec<String>, bool) {
        let state = self.state_mut(endpoint);
        let added_keys: Vec<String> = topics_by_key
            .keys()
            .filter(|key| !state.topics_by_key.contains_key(*key))
            .cloned()
            .collect();
        if added_keys.is_empty() {
            return (added_keys, false);
        }
        let resubscribe_all = !state.connection.is_connected() || state.restore_plan.is_some();
        state.topics_by_key.extend(topics_by_key);
        (added_keys, resubscribe_all)
    }

    /// Forgets topics a failed subscribe added, so the venue state matches ours again.
    pub(crate) fn rollback_topics(&mut self, endpoint: Endpoint, keys: &[String]) {
        let state = self.state_mut(endpoint);
        for key in keys {
            state.topics_by_key.remove(key);
        }
    }

    /// Forgets topics and returns the keys that were actually present.
    pub(crate) fn forget_topics(&mut self, endpoint: Endpoint, keys: &[String]) -> Vec<String> {
        let state = self.state_mut(endpoint);
        keys.iter()
            .filter(|key| state.topics_by_key.remove(*key).is_some())
            .cloned()
            .collect()
    }

    pub(crate) fn clear_restore(&mut self, endpoint: Endpoint) {
        self.state_mut(endpoint).restore_plan = None;
    }

    pub(crate) fn restore_failures(&self, endpoint: Endpoint) -> u32 {
        self.state(endpoint)
            .restore_plan
            .map_or(0, |restore_plan| restore_plan.consecutive_failures)
    }

    /// Schedules a restore with backoff; returns the consecutive failure count.
    pub(crate) fn schedule_restore(
        &mut self,
        endpoint: Endpoint,
        initial: Duration,
        maximum: Duration,
    ) -> u32 {
        let state = self.state_mut(endpoint);
        let consecutive_failures = state
            .restore_plan
            .map_or(0, |restore_plan| restore_plan.consecutive_failures)
            + 1;
        state.restore_plan = if state.topics_by_key.is_empty() {
            None
        } else {
            Some(RestorePlan {
                scheduled_at: Instant::now()
                    + exponential_backoff(consecutive_failures, initial, maximum),
                consecutive_failures,
            })
        };
        consecutive_failures
    }

    /// Schedules an immediate restore without counting a failure.
    pub(crate) fn schedule_immediate_restore(&mut self, endpoint: Endpoint) {
        let state = self.state_mut(endpoint);
        state.restore_plan = if state.topics_by_key.is_empty() {
            None
        } else {
            Some(RestorePlan {
                scheduled_at: Instant::now(),
                consecutive_failures: 0,
            })
        };
    }

    pub(crate) async fn disconnect_all(&mut self) {
        for state in &mut self.endpoints {
            state.topics_by_key.clear();
            state.restore_plan = None;
            state.connection.close().await;
        }
    }
}

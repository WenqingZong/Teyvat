use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Router, serve as serve_http};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::coordinator::Coordinator;
use super::format_topics;
use crate::exchanges::{connected_apis, fetch_instrument_rules};
use layline::backoff::exponential_backoff;
use layline::build::{GIT_COMMIT_HASH, build_profile};
use layline::provider_api::{
    CONNECTED_APIS_PATH, ConnectedApis, HEALTH_PATH, INFO_PATH, InstrumentSubscription,
    ProviderInfo, RESUBSCRIBE_PATH, SUBSCRIBE_PATH, SUBSCRIBED_PATH, SubscribedResponse,
    SubscriptionRequest, SubscriptionResponse, SubscriptionResult, UNSUBSCRIBE_PATH,
};
use layline::time::server_time;
use layline::types::{Instrument, InstrumentRules, Topic};

/// How long a fetched rule is trusted. Every strategy resubscribes every ten seconds, so the first
/// request after this period refreshes every rule the provider holds; the requests in between fetch
/// only rules the cache lacks.
const RULES_FRESHNESS: Duration = Duration::from_hours(24);
/// A failed refresh waits on later requests, starting at a minute and doubling up to an hour.
const REFRESH_RETRY_INITIAL: Duration = Duration::from_mins(1);
const REFRESH_RETRY_MAXIMUM: Duration = Duration::from_hours(1);

type Rejection = (StatusCode, String);

/// The rules the provider has fetched, and when it fetched them.
#[derive(Default)]
struct RulesCache {
    fetched_at: Option<Instant>,
    /// When a failed refresh may be tried again; `None` until one fails.
    retry_at: Option<Instant>,
    /// Consecutive failed refreshes, which set the retry delay.
    failures: u32,
    rules: InstrumentRules,
}

impl RulesCache {
    /// Whether every rule the cache holds is due for a refresh. A failed refresh keeps quiet until
    /// its retry, so a broken `exchangeInfo` is not asked again on every resubscription.
    fn is_stale(&self, now: Instant) -> bool {
        let due = self
            .fetched_at
            .is_none_or(|fetched_at| now.duration_since(fetched_at) > RULES_FRESHNESS);
        due && self.retry_at.is_none_or(|retry_at| now >= retry_at)
    }

    /// The instruments to fetch, and whether the request depends on them: an instrument the cache
    /// lacks must arrive for the request to be answered, while a stale map is only refreshed.
    /// Fetching one new instrument does not count as refreshing the others.
    fn wanted(&self, requested: &HashSet<Instrument>, stale: bool) -> (HashSet<Instrument>, bool) {
        let missing: HashSet<Instrument> = requested
            .iter()
            .filter(|instrument| !self.rules.contains_key(instrument))
            .copied()
            .collect();
        let must_answer = !missing.is_empty();
        let mut wanted = missing;
        if stale {
            wanted.extend(self.rules.keys().copied());
        }
        (wanted, must_answer)
    }

    /// Adds freshly fetched rules; `full_refresh` is true when they cover every cached instrument,
    /// which is what restarts the freshness period.
    fn store(&mut self, fetched: InstrumentRules, full_refresh: bool) {
        self.rules.extend(fetched);
        if full_refresh {
            self.fetched_at = Some(Instant::now());
            self.retry_at = None;
            self.failures = 0;
        }
    }

    /// Postpones the next refresh after a failed one, backing off with each failure in a row.
    fn retry_later(&mut self) {
        self.failures += 1;
        let delay =
            exponential_backoff(self.failures, REFRESH_RETRY_INITIAL, REFRESH_RETRY_MAXIMUM);
        self.retry_at = Some(Instant::now() + delay);
    }
}

#[derive(Clone)]
struct ServerState {
    coordinator: Arc<Mutex<Coordinator>>,
    demo: bool,
    rules: Arc<RwLock<RulesCache>>,
}

impl ServerState {
    /// The rules cache; no code panics while holding its lock.
    fn cached_rules(&self) -> RwLockReadGuard<'_, RulesCache> {
        self.rules
            .read()
            .expect("no code panics while holding the instrument rules")
    }

    fn cached_rules_mut(&self) -> RwLockWriteGuard<'_, RulesCache> {
        self.rules
            .write()
            .expect("no code panics while holding the instrument rules")
    }
}

pub(crate) async fn serve(
    listener: TcpListener,
    coordinator: Arc<Mutex<Coordinator>>,
    demo: bool,
) -> Result<()> {
    let state = ServerState {
        coordinator,
        demo,
        rules: Arc::new(RwLock::new(RulesCache::default())),
    };
    let routes = Router::new()
        .route(HEALTH_PATH, get(health))
        .route(INFO_PATH, get(provider_info))
        .route(SUBSCRIBE_PATH, post(subscribe))
        .route(RESUBSCRIBE_PATH, post(resubscribe))
        .route(UNSUBSCRIBE_PATH, post(unsubscribe))
        .route(SUBSCRIBED_PATH, get(subscribed))
        .route(CONNECTED_APIS_PATH, get(list_connected_apis))
        .with_state(state);
    info!("accepting strategies on http://{}", listener.local_addr()?);
    serve_http(listener, routes)
        .await
        .context("control server failed")
}

async fn health() -> &'static str {
    "ok"
}

async fn provider_info(State(state): State<ServerState>) -> Json<ProviderInfo> {
    let lease_duration = state.coordinator.lock().await.lease_duration();
    Json(ProviderInfo {
        server_time: server_time(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        build: build_profile().to_owned(),
        git_commit_hash: GIT_COMMIT_HASH.to_owned(),
        lease_duration_seconds: lease_duration.as_secs(),
    })
}

async fn subscribe(
    State(state): State<ServerState>,
    Json(request): Json<SubscriptionRequest>,
) -> Result<Json<SubscriptionResponse>, Rejection> {
    info!(
        "{} requested subscribe, demo: {}: {}",
        request.strategy_name,
        request.demo,
        format_topics(&request.topics)
    );
    validate(&request, state.demo)?;
    fetch_needed_instrument_rules(&state, &request.topics).await?;
    let results = state
        .coordinator
        .lock()
        .await
        .subscribe(&request.strategy_name, &request.topics, Instant::now())
        .await;
    Ok(respond(&state, results))
}

async fn resubscribe(
    State(state): State<ServerState>,
    Json(request): Json<SubscriptionRequest>,
) -> Result<Json<SubscriptionResponse>, Rejection> {
    validate(&request, state.demo)?;
    fetch_needed_instrument_rules(&state, &request.topics).await?;
    let results = state
        .coordinator
        .lock()
        .await
        .resubscribe(&request.strategy_name, &request.topics, Instant::now())
        .await;
    Ok(respond(&state, results))
}

/// Attaches each instrument's rule to its outcomes.
fn respond(
    state: &ServerState,
    results: HashMap<Topic, SubscriptionResult>,
) -> Json<SubscriptionResponse> {
    Json(SubscriptionResponse {
        results: group_subscription_results(results, &state.cached_rules().rules),
    })
}

async fn unsubscribe(
    State(state): State<ServerState>,
    Json(request): Json<SubscriptionRequest>,
) -> Result<StatusCode, Rejection> {
    info!(
        "{} requested unsubscribe: {}",
        request.strategy_name,
        format_topics(&request.topics)
    );
    validate(&request, state.demo)?;
    state
        .coordinator
        .lock()
        .await
        .unsubscribe(&request.strategy_name, &request.topics, Instant::now())
        .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn subscribed(State(state): State<ServerState>) -> Json<SubscribedResponse> {
    Json(SubscribedResponse {
        topics: state.coordinator.lock().await.subscribed_topics(),
    })
}

async fn list_connected_apis() -> Json<ConnectedApis> {
    Json(connected_apis())
}

/// Fetches the rules this request needs: the ones the cache lacks, plus every cached rule when the
/// map is stale. A rule the request depends on must arrive, or the request fails so the strategy
/// retries. A stale refresh is best effort: its failure keeps the cached rules, does not fail the
/// request, and waits for its retry instead of asking the exchange on every resubscription. A
/// missing or non-trading instrument panics inside the venue's fetch, because it is a configuration
/// error.
async fn fetch_needed_instrument_rules(
    state: &ServerState,
    topics: &HashSet<Topic>,
) -> Result<(), Rejection> {
    let requested_instruments: HashSet<Instrument> =
        topics.iter().map(|topic| topic.instrument).collect();
    if requested_instruments.is_empty() {
        return Ok(());
    }
    let (wanted, stale, must_answer) = {
        let rules = state.cached_rules();
        let stale = rules.is_stale(Instant::now());
        let (wanted, must_answer) = rules.wanted(&requested_instruments, stale);
        (wanted, stale, must_answer)
    };
    if wanted.is_empty() {
        return Ok(());
    }
    let fetched = match fetch_instrument_rules(state.demo, &wanted).await {
        Ok(fetched) => fetched,
        Err(error) if must_answer => {
            let reason = format!("failed to fetch the instrument rules: {error:#}");
            warn!("{reason}");
            return Err((StatusCode::SERVICE_UNAVAILABLE, reason));
        }
        Err(error) => {
            warn!("refreshing the instrument rules failed, keeping the cached ones: {error:#}");
            state.cached_rules_mut().retry_later();
            return Ok(());
        }
    };
    state.cached_rules_mut().store(fetched, stale);
    Ok(())
}

/// Keys the outcomes by instrument, next to the instrument's rule, which the response carries once
/// per instrument instead of once per topic. A venue without rules, such as the synthetic OKX feed,
/// contributes no entry to `instrument_rules`, so its instruments keep `None`.
fn group_subscription_results(
    results: HashMap<Topic, SubscriptionResult>,
    instrument_rules: &InstrumentRules,
) -> HashMap<Instrument, InstrumentSubscription> {
    let mut grouped: HashMap<Instrument, InstrumentSubscription> = HashMap::new();
    for (topic, result) in results {
        grouped
            .entry(topic.instrument)
            .or_insert_with(|| InstrumentSubscription {
                instrument_rule_option: instrument_rules.get(&topic.instrument).cloned(),
                results: HashMap::new(),
            })
            .results
            .insert(topic.market_data_type, result);
    }
    grouped
}

fn validate(request: &SubscriptionRequest, demo: bool) -> Result<(), Rejection> {
    if request.strategy_name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "strategy_name must not be empty".to_owned(),
        ));
    }
    if request.demo != demo {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "provider serves {} data, not {}",
                environment_name(demo),
                environment_name(request.demo)
            ),
        ));
    }
    Ok(())
}

fn environment_name(demo: bool) -> &'static str {
    if demo { "demo" } else { "live" }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rust_decimal::dec;

    use super::*;
    use layline::types::{Currency, Exchange, InstrumentRule, MarketDataType, UnderlyingType};

    fn instrument(exchange: Exchange, underlying_type: UnderlyingType) -> Instrument {
        Instrument {
            exchange,
            base: Currency::Btc,
            quote: Currency::Usdt,
            underlying_type,
        }
    }

    fn rule() -> InstrumentRule {
        InstrumentRule {
            exchange_symbol: "BTCUSDT".to_owned(),
            price_step: dec!(0.01),
            quantity_step: dec!(0.001),
            minimum_quantity: dec!(0.001),
            minimum_notional_size: dec!(5),
        }
    }

    fn topic(instrument: Instrument, market_data_type: MarketDataType) -> Topic {
        Topic {
            instrument,
            market_data_type,
        }
    }

    #[test]
    fn results_group_per_instrument_and_keep_their_rule() {
        let binance = instrument(Exchange::Binance, UnderlyingType::Spot);
        let okx = instrument(Exchange::Okx, UnderlyingType::Perpetual);
        let binance_rule = rule();
        let instrument_rules = InstrumentRules::from([(binance, binance_rule.clone())]);
        let results = HashMap::from([
            (
                topic(binance, MarketDataType::Bbo),
                Ok(PathBuf::from("/dev/shm/binance.dat")),
            ),
            (
                topic(okx, MarketDataType::Bbo),
                Ok(PathBuf::from("/dev/shm/okx.dat")),
            ),
        ]);
        let grouped = group_subscription_results(results, &instrument_rules);
        assert_eq!(Some(binance_rule), grouped[&binance].instrument_rule_option);
        assert_eq!(None, grouped[&okx].instrument_rule_option);
        assert_eq!(1, grouped[&binance].results.len());
        assert_eq!(1, grouped[&okx].results.len());
    }

    /// A refused topic must not hide its instrument's rule: the strategy keeps the rule it already
    /// has while it reports the refusal.
    #[test]
    fn a_refused_topic_keeps_its_instruments_rule() {
        let binance = instrument(Exchange::Binance, UnderlyingType::Spot);
        let binance_rule = rule();
        let instrument_rules = InstrumentRules::from([(binance, binance_rule.clone())]);
        let results = HashMap::from([(
            topic(binance, MarketDataType::Bbo),
            Err("ring creation failed".to_owned()),
        )]);
        let grouped = group_subscription_results(results, &instrument_rules);
        assert_eq!(Some(binance_rule), grouped[&binance].instrument_rule_option);
    }

    #[test]
    fn rules_are_fetched_when_missing_or_stale() {
        let binance = instrument(Exchange::Binance, UnderlyingType::Spot);
        let okx = instrument(Exchange::Okx, UnderlyingType::Perpetual);
        let requested = HashSet::from([binance, okx]);
        let fetched_at = Instant::now();
        let cache = RulesCache {
            fetched_at: Some(fetched_at),
            rules: InstrumentRules::from([(binance, rule())]),
            ..RulesCache::default()
        };
        let soon = fetched_at + Duration::from_secs(5);
        assert!(!cache.is_stale(soon));
        let (wanted, must_answer) = cache.wanted(&requested, false);
        assert_eq!(HashSet::from([okx]), wanted);
        assert!(must_answer);

        // A resubscription while every requested rule is cached must not fetch anything.
        let fully_cached = RulesCache {
            fetched_at: Some(fetched_at),
            rules: InstrumentRules::from([(binance, rule()), (okx, rule())]),
            ..RulesCache::default()
        };
        assert_eq!(
            (HashSet::new(), false),
            fully_cached.wanted(&requested, false)
        );

        let later = fetched_at + RULES_FRESHNESS + Duration::from_secs(1);
        assert!(cache.is_stale(later));
        let (wanted, must_answer) = cache.wanted(&requested, true);
        assert_eq!(requested, wanted);
        assert!(must_answer);

        let empty = RulesCache::default();
        assert!(empty.is_stale(soon));
        let (wanted, must_answer) = empty.wanted(&requested, true);
        assert_eq!(requested, wanted);
        assert!(must_answer);
    }

    /// A failed refresh must not be retried on every resubscription.
    #[test]
    fn a_failed_refresh_waits_for_its_retry_and_a_success_clears_it() {
        let mut cache = RulesCache::default();
        let now = Instant::now();
        assert!(cache.is_stale(now));
        cache.retry_later();
        assert!(!cache.is_stale(now + Duration::from_secs(30)));
        assert!(cache.is_stale(now + REFRESH_RETRY_INITIAL + Duration::from_secs(1)));

        cache.retry_later();
        assert!(!cache.is_stale(now + REFRESH_RETRY_INITIAL + Duration::from_secs(30)));
        assert!(cache.is_stale(now + REFRESH_RETRY_INITIAL * 2 + Duration::from_secs(1)));

        cache.store(InstrumentRules::new(), true);
        assert!(!cache.is_stale(Instant::now()));
        assert_eq!((None, 0), (cache.retry_at, cache.failures));
    }

    /// A fetch that only adds a newly requested instrument must not postpone the full refresh.
    #[test]
    fn only_a_full_refresh_restarts_the_freshness_period() {
        let binance = instrument(Exchange::Binance, UnderlyingType::Spot);
        let okx = instrument(Exchange::Okx, UnderlyingType::Perpetual);
        let fetched_at = Instant::now();
        let mut cache = RulesCache {
            fetched_at: Some(fetched_at),
            rules: InstrumentRules::from([(binance, rule())]),
            ..RulesCache::default()
        };
        let later = fetched_at + RULES_FRESHNESS + Duration::from_secs(1);
        cache.store(InstrumentRules::from([(okx, rule())]), false);
        assert!(cache.is_stale(later));

        cache.store(InstrumentRules::new(), true);
        assert!(!cache.is_stale(Instant::now()));
    }
}

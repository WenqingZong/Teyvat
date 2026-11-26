use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use anyhow::{Context, Result};
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Router, serve as serve_http};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::info;

use super::coordinator::Coordinator;
use super::format_topics;
use crate::exchanges::{connected_apis, fetch_instrument_rules};
use layline::build::{GIT_COMMIT_HASH, build_profile};
use layline::provider_api::{
    CONNECTED_APIS_PATH, ConnectedApis, HEALTH_PATH, INFO_PATH, InstrumentSubscription,
    ProviderInfo, RESUBSCRIBE_PATH, SUBSCRIBE_PATH, SUBSCRIBED_PATH, SubscribedResponse,
    SubscriptionRequest, SubscriptionResponse, SubscriptionResult, UNSUBSCRIBE_PATH,
};
use layline::time::server_time;
use layline::types::{Exchange, Instrument, InstrumentRules, MarketDataType, Topic};

type Rejection = (StatusCode, String);

#[derive(Clone)]
struct ServerState {
    coordinator: Arc<Mutex<Coordinator>>,
    demo: bool,
    instrument_rules: Arc<RwLock<InstrumentRules>>,
}

pub(crate) async fn serve(
    listener: TcpListener,
    coordinator: Arc<Mutex<Coordinator>>,
    demo: bool,
    instrument_rules: Arc<RwLock<InstrumentRules>>,
) -> Result<()> {
    let state = ServerState {
        coordinator,
        demo,
        instrument_rules,
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
    fetch_missing_instrument_rules(&state, &request.topics).await?;
    let results = state
        .coordinator
        .lock()
        .await
        .subscribe(&request.strategy_name, &request.topics, Instant::now())
        .await;
    let results = group_subscription_results(results, &state.instrument_rules);
    Ok(Json(SubscriptionResponse { results }))
}

async fn resubscribe(
    State(state): State<ServerState>,
    Json(request): Json<SubscriptionRequest>,
) -> Result<Json<SubscriptionResponse>, Rejection> {
    validate(&request, state.demo)?;
    fetch_missing_instrument_rules(&state, &request.topics).await?;
    let results = state
        .coordinator
        .lock()
        .await
        .resubscribe(&request.strategy_name, &request.topics, Instant::now())
        .await;
    let results = group_subscription_results(results, &state.instrument_rules);
    Ok(Json(SubscriptionResponse { results }))
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

/// Fetches rules for the requested instruments the map does not know yet. A missing or
/// non-trading instrument panics inside the venue's fetch, because it is a configuration error,
/// while a failed request is returned so the strategy retries. Known instruments are skipped so a
/// rule-bearing venue is not queried on every resubscription; the 24-hour refresh keeps the rules
/// current.
async fn fetch_missing_instrument_rules(
    state: &ServerState,
    topics: &HashSet<Topic>,
) -> Result<(), Rejection> {
    let requested_instruments: HashSet<Instrument> =
        topics.iter().map(|topic| topic.instrument).collect();
    let missing_instruments: HashSet<Instrument> = {
        let instrument_rules = state
            .instrument_rules
            .read()
            .expect("no code panics while holding the instrument rules");
        requested_instruments
            .iter()
            .filter(|instrument| !instrument_rules.contains_key(instrument))
            .copied()
            .collect()
    };
    if missing_instruments.is_empty() {
        return Ok(());
    }
    let mut missing_by_exchange: HashMap<Exchange, HashSet<Instrument>> = HashMap::new();
    for instrument in missing_instruments {
        missing_by_exchange
            .entry(instrument.exchange)
            .or_default()
            .insert(instrument);
    }
    for (exchange, instruments) in missing_by_exchange {
        let fetched = fetch_instrument_rules(exchange, state.demo, &instruments)
            .await
            .map_err(|error| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("failed to fetch instrument rules for {exchange}: {error:#}"),
                )
            })?;
        state
            .instrument_rules
            .write()
            .expect("no code panics while holding the instrument rules")
            .extend(fetched);
    }
    Ok(())
}

fn group_subscription_results(
    results: HashMap<Topic, SubscriptionResult>,
    instrument_rules: &RwLock<InstrumentRules>,
) -> HashMap<Instrument, InstrumentSubscription> {
    let mut grouped: HashMap<Instrument, HashMap<MarketDataType, SubscriptionResult>> =
        HashMap::new();
    for (topic, result) in results {
        grouped
            .entry(topic.instrument)
            .or_default()
            .insert(topic.market_data_type, result);
    }
    let instrument_rules = instrument_rules
        .read()
        .expect("no code panics while holding the instrument rules");
    grouped
        .into_iter()
        .map(|(instrument, results)| {
            let instrument_rule_option = if results.values().any(|result| result.is_ok()) {
                instrument_rules.get(&instrument).cloned()
            } else {
                None
            };
            (
                instrument,
                InstrumentSubscription {
                    instrument_rule_option,
                    results,
                },
            )
        })
        .collect()
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
    use layline::types::{Currency, InstrumentRule, MarketDataType, UnderlyingType};

    #[test]
    fn results_group_per_instrument_and_keep_their_rule() {
        let binance = Instrument {
            exchange: Exchange::Binance,
            base: Currency::Btc,
            quote: Currency::Usdt,
            underlying_type: UnderlyingType::Spot,
        };
        let okx = Instrument {
            exchange: Exchange::Okx,
            base: Currency::Btc,
            quote: Currency::Usdt,
            underlying_type: UnderlyingType::Perpetual,
        };
        let rule = InstrumentRule {
            exchange_symbol: "BTCUSDT".to_owned(),
            price_step: dec!(0.01),
            quantity_step: dec!(0.001),
            minimum_quantity: dec!(0.001),
            minimum_notional_size: dec!(5),
        };
        let instrument_rules = RwLock::new(InstrumentRules::from([(binance, rule.clone())]));
        let results = HashMap::from([
            (
                Topic {
                    instrument: binance,
                    market_data_type: MarketDataType::Bbo,
                },
                Ok(PathBuf::from("/dev/shm/binance.dat")),
            ),
            (
                Topic {
                    instrument: okx,
                    market_data_type: MarketDataType::Bbo,
                },
                Ok(PathBuf::from("/dev/shm/okx.dat")),
            ),
        ]);
        let grouped = group_subscription_results(results, &instrument_rules);
        assert_eq!(Some(rule), grouped[&binance].instrument_rule_option);
        assert_eq!(None, grouped[&okx].instrument_rule_option);
        assert_eq!(1, grouped[&binance].results.len());
        assert_eq!(1, grouped[&okx].results.len());
    }

    #[test]
    fn a_failed_instrument_has_no_rule() {
        let instrument = Instrument {
            exchange: Exchange::Binance,
            base: Currency::Btc,
            quote: Currency::Usdt,
            underlying_type: UnderlyingType::Spot,
        };
        let rule = InstrumentRule {
            exchange_symbol: "BTCUSDT".to_owned(),
            price_step: dec!(0.01),
            quantity_step: dec!(0.001),
            minimum_quantity: dec!(0.001),
            minimum_notional_size: dec!(5),
        };
        let instrument_rules = RwLock::new(InstrumentRules::from([(instrument, rule)]));
        let results = HashMap::from([(
            Topic {
                instrument,
                market_data_type: MarketDataType::Bbo,
            },
            Err("ring creation failed".to_owned()),
        )]);
        let grouped = group_subscription_results(results, &instrument_rules);
        assert_eq!(None, grouped[&instrument].instrument_rule_option);
    }
}

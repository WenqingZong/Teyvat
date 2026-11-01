use std::sync::Arc;
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
use crate::exchanges::connected_apis;
use layline::build::build_profile;
use layline::provider_api::{
    CONNECTED_APIS_PATH, ConnectedApis, HEALTH_PATH, INFO_PATH, ProviderInfo, RESUBSCRIBE_PATH,
    SUBSCRIBE_PATH, SUBSCRIBED_PATH, SubscribedResponse, SubscriptionRequest, SubscriptionResponse,
    UNSUBSCRIBE_PATH,
};
use layline::time::server_time;

type Rejection = (StatusCode, String);

#[derive(Clone)]
struct ServerState {
    coordinator: Arc<Mutex<Coordinator>>,
    demo: bool,
}

pub(crate) async fn serve(
    listener: TcpListener,
    coordinator: Arc<Mutex<Coordinator>>,
    demo: bool,
) -> Result<()> {
    let state = ServerState { coordinator, demo };
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
    let results = state
        .coordinator
        .lock()
        .await
        .subscribe(&request.strategy_name, &request.topics, Instant::now())
        .await;
    Ok(Json(SubscriptionResponse { results }))
}

async fn resubscribe(
    State(state): State<ServerState>,
    Json(request): Json<SubscriptionRequest>,
) -> Result<Json<SubscriptionResponse>, Rejection> {
    validate(&request, state.demo)?;
    let results = state
        .coordinator
        .lock()
        .await
        .resubscribe(&request.strategy_name, &request.topics, Instant::now())
        .await;
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
        topics: state.coordinator.lock().await.statuses(),
    })
}

async fn list_connected_apis() -> Json<ConnectedApis> {
    Json(connected_apis())
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

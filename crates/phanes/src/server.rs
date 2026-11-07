//! All Phanes services on one listener: Gnosis, Celestia and `GET /info`.

use std::io::Result as IoResult;
use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::routing::get;
use axum::{Json, Router, serve};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::warn;

use crate::celestia::server::{self as celestia_server, CelestiaState};
use crate::client_filter::{ClientFilter, filter_clients};
use crate::gnosis::server::{self as gnosis_server, GnosisState};
use crate::request_details::RequestDetails;
use layline::build::{GIT_COMMIT_HASH, build_profile};
use layline::time::server_time;

pub const INFO_PATH: &str = "/info";

/// Body of `GET /info`, for liveness checks.
#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    pub status: String,
    pub server_time: String,
    pub version: String,
    pub build: String,
    pub git_commit_hash: String,
}

/// One IP allow list in front of every service; `GET /info` is never filtered. `client_filter` is
/// `None` in debug builds, where local tests connect without Cloudflare.
pub fn router(
    gnosis_state: Arc<GnosisState>,
    celestia_state: Arc<CelestiaState>,
    client_filter: Option<Arc<ClientFilter>>,
) -> Router {
    let services =
        gnosis_server::router(gnosis_state).merge(celestia_server::router(celestia_state));
    let services = match client_filter {
        // `route_layer`, not `layer`: the latter also wraps the fallback, which would report
        // every unknown path as a blocked request.
        Some(filter) => services.route_layer(from_fn_with_state(filter, filter_clients)),
        None => services,
    };
    services
        .route(INFO_PATH, get(info))
        .fallback(log_unknown_path)
}

/// Unknown paths belong to no service: logged in full for later analysis, never pushed.
async fn log_unknown_path(request: Request) -> StatusCode {
    let request = RequestDetails::read_request(request).await;
    warn!("request to unknown path\n{request}");
    StatusCode::NOT_FOUND
}

pub async fn run(
    listener: TcpListener,
    gnosis_state: Arc<GnosisState>,
    celestia_state: Arc<CelestiaState>,
    client_filter: Option<Arc<ClientFilter>>,
) -> IoResult<()> {
    serve(
        listener,
        router(gnosis_state, celestia_state, client_filter),
    )
    .await
}

async fn info() -> Json<Info> {
    Json(Info {
        status: "ok".to_owned(),
        server_time: server_time(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        build: build_profile().to_owned(),
        git_commit_hash: GIT_COMMIT_HASH.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
    use tower::ServiceExt;

    use super::*;
    use crate::celestia::proto::CELESTIA_PATH;
    use crate::client_filter::CLIENT_IP_HEADER;
    use crate::clients::tests::test_client;
    use crate::clients::{ClientName, Clients};
    use crate::config::DEFAULT_MAX_CLOCK_SKEW_SECONDS;
    use crate::gnosis::config::Gnoses;
    use crate::notify::Notification;
    use layline::gnosis::GNOSIS_PATH;
    use layline::testing::test_keypair;

    /// Arbitrary documentation addresses (RFC 5737) with no business meaning.
    const OSAKA_IPV4: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
    const TOKYO_IPV4: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 7);
    const STRANGER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));

    fn phanes_router(
        notification_sender: UnboundedSender<Notification>,
        client_filter: Option<Arc<ClientFilter>>,
    ) -> Router {
        let (gnosis_private_key, _) = test_keypair();
        let (celestia_private_key, _) = test_keypair();
        let gnosis_state = GnosisState::new(
            gnosis_private_key,
            Arc::default(),
            Gnoses::default(),
            DEFAULT_MAX_CLOCK_SKEW_SECONDS,
            notification_sender.clone(),
        );
        let celestia_state =
            CelestiaState::new(celestia_private_key, Arc::default(), notification_sender);
        router(
            Arc::new(gnosis_state),
            Arc::new(celestia_state),
            client_filter,
        )
    }

    fn client_filter(notification_sender: &UnboundedSender<Notification>) -> Arc<ClientFilter> {
        let clients = Clients::from([
            (ClientName::from("Osaka"), test_client(OSAKA_IPV4)),
            (ClientName::from("Tokyo"), test_client(TOKYO_IPV4)),
        ]);
        Arc::new(ClientFilter::new(
            Arc::new(clients),
            notification_sender.clone(),
        ))
    }

    async fn status(router: &Router, request: HttpRequest<Body>) -> StatusCode {
        router
            .clone()
            .oneshot(request)
            .await
            .expect("router is infallible")
            .status()
    }

    fn get(path: &str, client_ip: Option<IpAddr>) -> HttpRequest<Body> {
        let mut request = HttpRequest::get(path);
        if let Some(client_ip) = client_ip {
            request = request.header(CLIENT_IP_HEADER, client_ip.to_string());
        }
        request.body(Body::empty()).expect("http request is valid")
    }

    #[tokio::test]
    async fn serves_every_service() {
        let (notification_sender, _) = unbounded_channel();
        let router = phanes_router(notification_sender, None);
        assert_eq!(status(&router, get(INFO_PATH, None)).await, StatusCode::OK);
        // A plain GET without the upgrade headers is rejected by the WebSocket extractor, not 404.
        assert_ne!(
            status(&router, get(CELESTIA_PATH, None)).await,
            StatusCode::NOT_FOUND
        );
        let junk = HttpRequest::post(GNOSIS_PATH)
            .body(Body::from("junk"))
            .expect("http request is valid");
        // Gnosis answers a failed handshake with an empty 404, so only the method proves the route.
        assert_eq!(status(&router, junk).await, StatusCode::NOT_FOUND);
        assert_eq!(
            status(&router, get(GNOSIS_PATH, None)).await,
            StatusCode::METHOD_NOT_ALLOWED
        );
    }

    #[tokio::test]
    async fn every_client_reaches_every_service_and_strangers_none() {
        let (notification_sender, mut notification_receiver) = unbounded_channel();
        let router = phanes_router(
            notification_sender.clone(),
            Some(client_filter(&notification_sender)),
        );
        // Past the filter, GET /gnosis is 405 and a plain GET /celestia is not 404.
        for client_ip in [IpAddr::V4(OSAKA_IPV4), IpAddr::V4(TOKYO_IPV4)] {
            assert_eq!(
                status(&router, get(GNOSIS_PATH, Some(client_ip))).await,
                StatusCode::METHOD_NOT_ALLOWED
            );
            assert_ne!(
                status(&router, get(CELESTIA_PATH, Some(client_ip))).await,
                StatusCode::NOT_FOUND
            );
        }
        assert!(notification_receiver.try_recv().is_err());

        assert_eq!(
            status(&router, get(CELESTIA_PATH, Some(STRANGER_IP))).await,
            StatusCode::NOT_FOUND
        );
        let Some(Notification::Blocked {
            client_ip,
            request,
            suppressed,
        }) = notification_receiver.try_recv().ok()
        else {
            panic!("expected a blocked notification");
        };
        assert_eq!((client_ip, suppressed), (Some(STRANGER_IP), 0));
        assert_eq!(request.head.uri.path(), CELESTIA_PATH);
    }

    #[tokio::test]
    async fn unknown_paths_are_not_blocked_requests() {
        /// Arbitrary paths a scanner might try.
        const UNKNOWN_PATHS: [&str; 2] = ["/", "/.env"];
        let (notification_sender, mut notification_receiver) = unbounded_channel();
        let router = phanes_router(
            notification_sender.clone(),
            Some(client_filter(&notification_sender)),
        );
        for path in UNKNOWN_PATHS {
            assert_eq!(
                status(&router, get(path, Some(STRANGER_IP))).await,
                StatusCode::NOT_FOUND
            );
        }
        assert!(notification_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn info_is_open_to_everyone() {
        let (notification_sender, mut notification_receiver) = unbounded_channel();
        let router = phanes_router(
            notification_sender.clone(),
            Some(client_filter(&notification_sender)),
        );
        for client_ip in [Some(STRANGER_IP), None] {
            assert_eq!(
                status(&router, get(INFO_PATH, client_ip)).await,
                StatusCode::OK
            );
        }
        assert!(notification_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn info_reports_status_version_and_build() {
        let Json(info) = info().await;
        assert_eq!(info.status, "ok");
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(info.build, "debug");
        assert_eq!(info.git_commit_hash, GIT_COMMIT_HASH);
    }
}

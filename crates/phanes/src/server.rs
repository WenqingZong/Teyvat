//! All Phanes services on one listener: Gnosis, Celestia and `GET /info`.

use log::warn;
use std::io::Result as IoResult;
use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::routing::get;
use axum::{Json, Router, serve};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::celestia::server::{self as celestia_server, CelestiaState};
use crate::client_filter::{ClientFilter, filter_clients};
use crate::gnosis::server::{self as gnosis_server, GnosisState};
use crate::request_details::RequestDetails;
use crate::utils::RELEASE_BUILD;

pub const INFO_PATH: &str = "/info";
/// UTC, e.g. `2026-10-15 13:30:05.123 UTC`.
const SERVER_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f UTC";

/// The services behind the IP filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    Gnosis,
    Celestia,
}

/// Logs are English, push notifications Chinese.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    English,
    Chinese,
}

impl Service {
    pub fn to_string(self, language: Language) -> String {
        match (self, language) {
            (Self::Gnosis, Language::English) => "Gnosis",
            (Self::Gnosis, Language::Chinese) => "神之心请求",
            (Self::Celestia, Language::English) => "Celestia",
            (Self::Celestia, Language::Chinese) => "天空岛监控连接",
        }
        .to_owned()
    }
}

/// Body of `GET /info`, for liveness checks.
#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    pub status: String,
    pub server_time: String,
    pub version: String,
    pub build: String,
}

/// One IP allow list per service. `GET /info` is never filtered.
pub struct ClientFilters {
    pub gnosis: Arc<ClientFilter>,
    pub celestia: Arc<ClientFilter>,
}

/// `client_filters` is `None` in debug builds, where local tests connect without Cloudflare.
pub fn router(
    gnosis_state: Arc<GnosisState>,
    celestia_state: Arc<CelestiaState>,
    client_filters: Option<ClientFilters>,
) -> Router {
    let gnosis = gnosis_server::router(gnosis_state);
    let celestia = celestia_server::router(celestia_state);
    let services = match client_filters {
        Some(filters) => apply_client_filter(gnosis, filters.gnosis)
            .merge(apply_client_filter(celestia, filters.celestia)),
        None => gnosis.merge(celestia),
    };
    services
        .route(INFO_PATH, get(info))
        .fallback(log_unknown_path)
}

/// `route_layer`, not `layer`: the latter also wraps the fallback, which would report every
/// unknown path as a blocked request to this service.
fn apply_client_filter(router: Router, filter: Arc<ClientFilter>) -> Router {
    router.route_layer(from_fn_with_state(filter, filter_clients))
}

/// Unknown paths belong to no service: logged in full for later analysis, never pushed.
async fn log_unknown_path(request: Request) -> StatusCode {
    let request = RequestDetails::read_request(request).await;
    warn!("request to unknown path: {request}");
    StatusCode::NOT_FOUND
}

pub async fn run(
    listener: TcpListener,
    gnosis_state: Arc<GnosisState>,
    celestia_state: Arc<CelestiaState>,
    client_filters: Option<ClientFilters>,
) -> IoResult<()> {
    serve(
        listener,
        router(gnosis_state, celestia_state, client_filters),
    )
    .await
}

async fn info() -> Json<Info> {
    Json(Info {
        status: "ok".to_owned(),
        server_time: Timestamp::now().strftime(SERVER_TIME_FORMAT).to_string(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        build: if RELEASE_BUILD { "release" } else { "debug" }.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::celestia::proto::CELESTIA_PATH;
    use crate::client_filter::{CLIENT_IP_HEADER, ClientsByIp};
    use crate::config::DEFAULT_MAX_CLOCK_SKEW_SECONDS;
    use crate::gnosis::proto::GNOSIS_PATH;
    use crate::noise::tests::test_keypair;
    use crate::notify::Notification;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use jiff::civil::DateTime;
    use jiff::tz::TimeZone;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr};
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
    use tower::ServiceExt;

    /// Arbitrary documentation addresses (RFC 5737) with no business meaning.
    const OSAKA_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    const TOKYO_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7));
    const STRANGER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
    /// Generous bound between building the response and checking it.
    const MAX_SERVER_TIME_DRIFT: Duration = Duration::from_secs(5);
    const MILLISECOND_DIGITS: usize = 3;
    /// The literal tail of `SERVER_TIME_FORMAT`.
    const UTC_SUFFIX: &str = " UTC";

    fn phanes_router(
        notifications: UnboundedSender<Notification>,
        client_filters: Option<ClientFilters>,
    ) -> Router {
        let (gnosis_private_key, _) = test_keypair();
        let (celestia_private_key, _) = test_keypair();
        let (_, client_public_key) = test_keypair();
        let gnosis_state = GnosisState::new(
            gnosis_private_key,
            client_public_key,
            HashSet::new(),
            PathBuf::new(),
            DEFAULT_MAX_CLOCK_SKEW_SECONDS,
            notifications.clone(),
        );
        let celestia_state =
            CelestiaState::new(celestia_private_key, client_public_key, notifications);
        router(
            Arc::new(gnosis_state),
            Arc::new(celestia_state),
            client_filters,
        )
    }

    /// Osaka may use Gnosis, Tokyo only celestia.
    fn client_filters(notifications: &UnboundedSender<Notification>) -> ClientFilters {
        let clients = |entries: &[(IpAddr, &str)]| -> ClientsByIp {
            entries
                .iter()
                .map(|(address, name)| (*address, (*name).to_owned()))
                .collect()
        };
        let filter = |service, entries: &[(IpAddr, &str)]| {
            Arc::new(ClientFilter::new(
                service,
                clients(entries),
                notifications.clone(),
            ))
        };
        ClientFilters {
            gnosis: filter(Service::Gnosis, &[(OSAKA_IP, "Osaka")]),
            celestia: filter(Service::Celestia, &[(TOKYO_IP, "Tokyo")]),
        }
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
        let (notifications, _) = unbounded_channel();
        let router = phanes_router(notifications, None);
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
    async fn each_service_admits_only_its_own_clients() {
        let (notifications, mut receiver) = unbounded_channel();
        let router = phanes_router(notifications.clone(), Some(client_filters(&notifications)));
        // Past the filter, GET /gnosis is 405 and a plain GET /celestia is not 404.
        assert_eq!(
            status(&router, get(GNOSIS_PATH, Some(OSAKA_IP))).await,
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_ne!(
            status(&router, get(CELESTIA_PATH, Some(TOKYO_IP))).await,
            StatusCode::NOT_FOUND
        );
        assert!(receiver.try_recv().is_err());

        assert_eq!(
            status(&router, get(GNOSIS_PATH, Some(TOKYO_IP))).await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(&router, get(CELESTIA_PATH, Some(OSAKA_IP))).await,
            StatusCode::NOT_FOUND
        );
        assert_blocked(
            receiver.try_recv().ok(),
            Service::Gnosis,
            TOKYO_IP,
            GNOSIS_PATH,
        );
        assert_blocked(
            receiver.try_recv().ok(),
            Service::Celestia,
            OSAKA_IP,
            CELESTIA_PATH,
        );
    }

    /// The recorded request itself is covered by the `client_filter` tests.
    fn assert_blocked(
        notification: Option<Notification>,
        expected_service: Service,
        expected_client_ip: IpAddr,
        expected_path: &str,
    ) {
        let Some(Notification::Blocked {
            service,
            client_ip,
            request,
            suppressed,
        }) = notification
        else {
            panic!("expected a blocked notification, got {notification:?}");
        };
        assert_eq!(
            (service, client_ip, suppressed),
            (expected_service, Some(expected_client_ip), 0)
        );
        assert_eq!(request.head.uri.path(), expected_path);
    }

    #[tokio::test]
    async fn unknown_paths_are_not_blamed_on_a_service() {
        /// Arbitrary paths a scanner might try.
        const UNKNOWN_PATHS: [&str; 2] = ["/", "/.env"];
        let (notifications, mut receiver) = unbounded_channel();
        let router = phanes_router(notifications.clone(), Some(client_filters(&notifications)));
        for path in UNKNOWN_PATHS {
            assert_eq!(
                status(&router, get(path, Some(STRANGER_IP))).await,
                StatusCode::NOT_FOUND
            );
        }
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn info_is_open_to_everyone() {
        let (notifications, mut receiver) = unbounded_channel();
        let router = phanes_router(notifications.clone(), Some(client_filters(&notifications)));
        for client_ip in [Some(STRANGER_IP), None] {
            assert_eq!(
                status(&router, get(INFO_PATH, client_ip)).await,
                StatusCode::OK
            );
        }
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn service_names_in_both_languages() {
        assert_eq!(Service::Gnosis.to_string(Language::English), "Gnosis");
        assert_eq!(Service::Gnosis.to_string(Language::Chinese), "神之心请求");
        assert_eq!(Service::Celestia.to_string(Language::English), "Celestia");
        assert_eq!(
            Service::Celestia.to_string(Language::Chinese),
            "天空岛监控连接"
        );
    }

    #[tokio::test]
    async fn info_reports_status_version_and_build() {
        let Json(info) = info().await;
        assert_eq!(info.status, "ok");
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(info.build, "debug");
    }

    #[tokio::test]
    async fn info_reports_utc_time_in_milliseconds() {
        let Json(info) = info().await;
        let time = info
            .server_time
            .strip_suffix(UTC_SUFFIX)
            .expect("server time ends with UTC");
        let (_, milliseconds) = time.rsplit_once('.').expect("server time has a fraction");
        assert_eq!(milliseconds.len(), MILLISECOND_DIGITS);
        let server_time = DateTime::strptime("%Y-%m-%d %H:%M:%S%.f", time)
            .expect("server time parses")
            .to_zoned(TimeZone::UTC)
            .expect("UTC time is representable")
            .timestamp();
        let drift = Timestamp::now().duration_since(server_time).unsigned_abs();
        assert!(
            drift < MAX_SERVER_TIME_DRIFT,
            "server time is {drift:?} off UTC"
        );
    }
}

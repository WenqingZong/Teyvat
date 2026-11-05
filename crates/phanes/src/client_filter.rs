//! IP allow list in front of every service: any address of a `[clients.<name>]` table may call any
//! service, and the service then checks that the address and the handshake's key belong to the
//! same client. Production traffic arrives through cloudflared, so the socket peer is always
//! loopback; the client address comes from `CF-Connecting-IP`, which Cloudflare overwrites (unlike
//! the appendable `X-Forwarded-For`).

use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, warn};

use crate::clients::{ClientName, Clients};
use crate::notify::{Notification, UnauthenticatedThrottle};
use crate::request_details::RequestDetails;

pub const CLIENT_IP_HEADER: &str = "cf-connecting-ip";

/// The client the request's address belongs to, as [`filter_clients`] found it; `None` when the
/// filter is not mounted (debug builds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpClientName(pub Option<ClientName>);

impl<S: Send + Sync> FromRequestParts<S> for IpClientName {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(parts.extensions.get::<ClientName>().cloned()))
    }
}

/// The allow list shared by every service.
pub struct ClientFilter {
    clients: Arc<Clients>,
    notifications: UnboundedSender<Notification>,
    blocked_throttle: Mutex<UnauthenticatedThrottle>,
}

impl ClientFilter {
    pub fn new(clients: Arc<Clients>, notifications: UnboundedSender<Notification>) -> Self {
        Self {
            clients,
            notifications,
            blocked_throttle: Mutex::default(),
        }
    }

    fn notify_blocked(&self, client_ip: Option<IpAddr>, request: RequestDetails) {
        let suppressed = self
            .blocked_throttle
            .lock()
            .expect("blocked throttle mutex is not poisoned")
            .record(Instant::now());
        let Some(suppressed) = suppressed else {
            return;
        };
        let notification = Notification::Blocked {
            client_ip,
            request,
            suppressed,
        };
        if self.notifications.send(notification).is_err() {
            error!("notification dispatcher is gone; push dropped");
        }
    }
}

/// Middleware: lets allowed clients through with their [`ClientName`] (read it with
/// [`IpClientName`]), answers everyone else (including a missing or unparsable header) with an
/// empty 404 after logging the whole request.
pub async fn filter_clients(
    State(filter): State<Arc<ClientFilter>>,
    mut request: Request,
    next: Next,
) -> Response {
    let client_ip = request
        .headers()
        .get(CLIENT_IP_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<IpAddr>().ok());
    let Some((client_ip, name)) = client_ip.and_then(|address| {
        let (name, _) = filter.clients.iter().find(|(_, client)| {
            client
                .addresses()
                .any(|client_address| client_address == address)
        })?;
        Some((address, name.clone()))
    }) else {
        let request = RequestDetails::read_request(request).await;
        warn!("blocked request: {request}");
        filter.notify_blocked(client_ip, request);
        return StatusCode::NOT_FOUND.into_response();
    };
    let path = request.uri().path();
    debug!("allowed client {name} ({client_ip}) to {path}");
    request.extensions_mut().insert(name);
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{HeaderMap, Method, Uri};
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use tower::ServiceExt;

    use super::*;
    use crate::clients::ClientConfig;
    use crate::clients::tests::test_client;
    use crate::request_details::RequestHead;

    const PATH: &str = "/whoami";
    /// Arbitrary documentation addresses (RFC 5737 / RFC 3849) with no business meaning.
    const OSAKA_IPV4: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
    const OSAKA_IPV6: Ipv6Addr = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
    const STRANGER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7));

    /// Answers with the client name the filter found, or nothing when no filter runs.
    async fn whoami(IpClientName(ip_client_name_option): IpClientName) -> String {
        ip_client_name_option.unwrap_or_default()
    }

    fn filtered_router() -> (Router, UnboundedReceiver<Notification>) {
        let (sender, notifications) = unbounded_channel();
        let osaka = ClientConfig {
            ipv6: Some(OSAKA_IPV6),
            ..test_client(OSAKA_IPV4)
        };
        let clients = Clients::from([(ClientName::from("Osaka"), osaka)]);
        let filter = Arc::new(ClientFilter::new(Arc::new(clients), sender));
        let router = Router::new()
            .route(PATH, get(whoami))
            .layer(from_fn_with_state(filter, filter_clients));
        (router, notifications)
    }

    async fn call(router: &Router, client_ip: Option<&str>) -> (StatusCode, String) {
        let mut request = Request::get(PATH);
        if let Some(client_ip) = client_ip {
            request = request.header(CLIENT_IP_HEADER, client_ip);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).expect("http request is valid"))
            .await
            .expect("router is infallible");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body is readable");
        (
            status,
            String::from_utf8(body.to_vec()).expect("body is utf-8"),
        )
    }

    /// What a blocked `GET PATH` without a body is pushed as; `header` is its raw client IP.
    fn blocked(client_ip: Option<IpAddr>, header: Option<&str>) -> Notification {
        let mut headers = HeaderMap::new();
        if let Some(header) = header {
            headers.insert(
                CLIENT_IP_HEADER,
                header.parse().expect("valid header value"),
            );
        }
        let head = RequestHead {
            method: Method::GET,
            uri: Uri::from_static(PATH),
            headers,
        };
        Notification::Blocked {
            client_ip,
            request: RequestDetails::new(head, &[]),
            suppressed: 0,
        }
    }

    #[tokio::test]
    async fn allowed_ipv4_and_ipv6_reach_the_handler_with_their_name() {
        let (router, mut notifications) = filtered_router();
        for address in [IpAddr::V4(OSAKA_IPV4), IpAddr::V6(OSAKA_IPV6)] {
            let response = call(&router, Some(&address.to_string())).await;
            assert_eq!(response, (StatusCode::OK, "Osaka".to_owned()));
        }
        assert!(notifications.try_recv().is_err());
    }

    #[tokio::test]
    async fn stranger_gets_empty_404_and_a_notification() {
        let (router, mut notifications) = filtered_router();
        let header = STRANGER_IP.to_string();
        let response = call(&router, Some(&header)).await;
        assert_eq!(response, (StatusCode::NOT_FOUND, String::new()));
        assert_eq!(
            notifications.try_recv(),
            Ok(blocked(Some(STRANGER_IP), Some(&header)))
        );
    }

    #[tokio::test]
    async fn missing_or_malformed_header_is_blocked_and_throttled() {
        const NOT_AN_ADDRESS: &str = "not an address";
        /// `X-Forwarded-For` style list; `CF-Connecting-IP` always holds exactly one address.
        const ADDRESS_LIST: &str = "192.0.2.1, 10.0.0.1";
        let (router, mut notifications) = filtered_router();
        for client_ip in [None, Some(NOT_AN_ADDRESS), Some(ADDRESS_LIST)] {
            let response = call(&router, client_ip).await;
            assert_eq!(response, (StatusCode::NOT_FOUND, String::new()));
        }
        assert_eq!(notifications.try_recv(), Ok(blocked(None, None)));
        assert!(notifications.try_recv().is_err());
    }

    #[tokio::test]
    async fn unfiltered_handler_sees_no_client_name() {
        let router = Router::new().route(PATH, get(whoami));
        let response = call(&router, None).await;
        assert_eq!(response, (StatusCode::OK, String::new()));
    }
}

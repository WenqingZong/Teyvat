//! Snapshot of a rejected request: every header plus the start of its body (or of the
//! WebSocket frame that failed). Logged in full; pushes carry only [`RequestDetails::summarize`].

use std::convert::Infallible;
use std::fmt::{Display, Formatter, Result as FormatResult};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{FromRequestParts, Request};
use axum::http::request::Parts;
use axum::http::{HeaderMap, Method, Uri};
use futures_util::StreamExt;
use hex::encode as encode_hex;
use tokio::time::{Instant, timeout_at};

/// Same as Gnosis' body limit: enough to recognise any payload, small enough to log.
pub const MAX_RECORDED_PAYLOAD_BYTES: usize = 4096;
/// A blocked client that stalls mid-body only delays its own 404.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Headers quoted in pushes, which cannot carry them all: who sent it and through what.
const SUMMARY_HEADERS: [&str; 5] = [
    "User-Agent",
    "Upgrade",
    "CF-Ray",
    "CF-IPCountry",
    "CF-Worker",
];
/// Keeps a push under Bark's size limit even if a header is huge.
const MAX_SUMMARY_HEADER_CHARACTERS: usize = 128;

/// Everything about a request except its body; extracted by handlers that may reject it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    pub method: Method,
    pub uri: Uri,
    pub headers: HeaderMap,
}

impl RequestHead {
    pub fn from_parts(parts: &Parts) -> Self {
        Self {
            method: parts.method.clone(),
            uri: parts.uri.clone(),
            headers: parts.headers.clone(),
        }
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RequestHead {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self::from_parts(parts))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestDetails {
    pub head: RequestHead,
    /// At most [`MAX_RECORDED_PAYLOAD_BYTES`] of the body or WebSocket frame.
    pub payload: Bytes,
    pub truncated: bool,
}

impl RequestDetails {
    pub fn new(head: RequestHead, payload: &[u8]) -> Self {
        let truncated = payload.len() > MAX_RECORDED_PAYLOAD_BYTES;
        let recorded_length = payload.len().min(MAX_RECORDED_PAYLOAD_BYTES);
        Self {
            head,
            payload: Bytes::copy_from_slice(&payload[..recorded_length]),
            truncated,
        }
    }

    /// Consumes the request, reading just past the recorded limit to know whether it was cut.
    pub async fn read_request(request: Request) -> Self {
        let (parts, body) = request.into_parts();
        let payload = read_body_head(body).await;
        Self::new(RequestHead::from_parts(&parts), &payload)
    }

    /// Method, path, the identifying headers that are present and the payload size.
    pub fn summarize(&self) -> String {
        let RequestHead {
            method,
            uri,
            headers,
        } = &self.head;
        let path = uri.path();
        let mut lines = vec![format!("Request: {method} {path}")];
        for name in SUMMARY_HEADERS {
            if let Some(value) = headers.get(name) {
                let value: String = String::from_utf8_lossy(value.as_bytes())
                    .chars()
                    .take(MAX_SUMMARY_HEADER_CHARACTERS)
                    .collect();
                lines.push(format!("{name}: {value}"));
            }
        }
        let payload_length = self.payload.len();
        let truncation = if self.truncated { " (truncated)" } else { "" };
        lines.push(format!("Payload: {payload_length} bytes{truncation}"));
        lines.join("\n")
    }
}

/// One line: `HeaderMap`'s `Debug` escapes control characters, so a header cannot forge log lines.
impl Display for RequestDetails {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let RequestHead {
            method,
            uri,
            headers,
        } = &self.head;
        let payload_length = self.payload.len();
        let truncation = if self.truncated { ", truncated" } else { "" };
        let payload = encode_hex(&self.payload);
        write!(
            formatter,
            "{method} {uri} headers={headers:?} payload ({payload_length} bytes{truncation})={payload}"
        )
    }
}

/// Stops after `MAX_RECORDED_PAYLOAD_BYTES + 1` bytes, on a body error or at the timeout.
async fn read_body_head(body: Body) -> Vec<u8> {
    let deadline = Instant::now() + BODY_READ_TIMEOUT;
    let mut stream = body.into_data_stream();
    let mut head = Vec::new();
    while head.len() <= MAX_RECORDED_PAYLOAD_BYTES
        && let Ok(Some(Ok(chunk))) = timeout_at(deadline, stream.next()).await
    {
        head.extend_from_slice(&chunk);
    }
    head
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub const PATH: &str = "/celestia";
    /// Arbitrary values with no business meaning.
    const USER_AGENT: &str = "zgrab/0.x";
    const CF_RAY: &str = "8f00000000000000-SEA";
    const OTHER_HEADER: &str = "x-scanner";
    const OTHER_VALUE: &str = "hello";

    pub fn request_head(path: &str) -> RequestHead {
        let mut headers = HeaderMap::new();
        headers.insert(
            "user-agent",
            USER_AGENT.parse().expect("valid header value"),
        );
        headers.insert("cf-ray", CF_RAY.parse().expect("valid header value"));
        headers.insert(
            OTHER_HEADER,
            OTHER_VALUE.parse().expect("valid header value"),
        );
        RequestHead {
            method: Method::GET,
            uri: path.parse().expect("valid uri"),
            headers,
        }
    }

    #[test]
    fn long_payload_is_truncated() {
        let payload = vec![0u8; MAX_RECORDED_PAYLOAD_BYTES + 1];
        let details = RequestDetails::new(request_head(PATH), &payload);
        assert_eq!(details.payload.len(), MAX_RECORDED_PAYLOAD_BYTES);
        assert!(details.truncated);
    }

    #[tokio::test]
    async fn read_request_keeps_headers_and_body() {
        /// Arbitrary body with no business meaning.
        const BODY: &[u8] = b"\x00\x01junk";
        let request = Request::post(PATH)
            .header(OTHER_HEADER, OTHER_VALUE)
            .body(Body::from(BODY))
            .expect("http request is valid");
        let details = RequestDetails::read_request(request).await;
        assert_eq!(details.head.method, Method::POST);
        assert_eq!(details.head.uri.path(), PATH);
        assert_eq!(details.head.headers[OTHER_HEADER], OTHER_VALUE);
        assert_eq!(details.payload.as_ref(), BODY);
        assert!(!details.truncated);
    }

    #[tokio::test]
    async fn read_request_stops_past_the_limit() {
        /// Far beyond the limit, to show the rest is never buffered.
        const BODY_LENGTH: usize = MAX_RECORDED_PAYLOAD_BYTES * 16;
        let request = Request::post(PATH)
            .body(Body::from(vec![0u8; BODY_LENGTH]))
            .expect("http request is valid");
        let details = RequestDetails::read_request(request).await;
        assert_eq!(details.payload.len(), MAX_RECORDED_PAYLOAD_BYTES);
        assert!(details.truncated);
    }

    #[test]
    fn log_line_has_every_header_and_hex_payload() {
        /// Arbitrary bytes with no business meaning.
        const PAYLOAD: &[u8] = &[0xde, 0xad, 0x0a];
        let line = RequestDetails::new(request_head(PATH), PAYLOAD).to_string();
        assert!(line.starts_with(&format!("GET {PATH} ")));
        for value in [USER_AGENT, CF_RAY, OTHER_HEADER, OTHER_VALUE] {
            assert!(line.contains(value), "{line:?} should contain {value:?}");
        }
        let payload_length = PAYLOAD.len();
        assert!(line.ends_with(&format!("payload ({payload_length} bytes)=dead0a")));
        assert!(!line.contains('\n'));
    }

    #[test]
    fn summary_has_only_identifying_headers() {
        let summary = RequestDetails::new(request_head(PATH), &[]).summarize();
        assert!(summary.contains(&format!("Request: GET {PATH}")));
        assert!(summary.contains(&format!("User-Agent: {USER_AGENT}")));
        assert!(summary.contains(&format!("CF-Ray: {CF_RAY}")));
        assert!(!summary.contains(OTHER_HEADER));
        assert!(!summary.contains("CF-Worker"));
        assert!(summary.contains("Payload: 0 bytes"));
    }

    #[test]
    fn summary_shortens_huge_headers() {
        let mut head = request_head(PATH);
        let huge = "a".repeat(MAX_SUMMARY_HEADER_CHARACTERS * 2);
        head.headers
            .insert("user-agent", huge.parse().expect("valid header value"));
        let summary = RequestDetails::new(head, &[]).summarize();
        let shortened = "a".repeat(MAX_SUMMARY_HEADER_CHARACTERS);
        assert!(summary.contains(&format!("User-Agent: {shortened}\n")));
    }
}

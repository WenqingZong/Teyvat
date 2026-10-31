use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::info;

/// An optional request header, such as an API key.
pub(crate) struct Header {
    pub(crate) name: &'static str,
    pub(crate) value: SecretString,
}

/// One websocket at a time, connected and dropped in place, so tests can swap the venue for a mock.
/// Heartbeat and close logic live in `WebSocketConnection`.
pub(crate) trait WebSocketClient {
    /// Replaces any previous connection.
    async fn connect(&mut self, url: &str, header_option: Option<Header>) -> Result<()>;
    /// Writes and flushes.
    async fn send(&mut self, message: Message) -> Result<()>;
    /// Writes what an interrupted `send` left buffered.
    async fn flush(&mut self) -> Result<()>;
    /// `None` once the peer has gone, or while not connected.
    async fn next(&mut self) -> Option<Result<Message>>;
    /// Drops the connection without a closing handshake.
    fn disconnect(&mut self);
}

#[derive(Default)]
pub(crate) struct TungsteniteClient {
    /// Set while connected.
    stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl TungsteniteClient {
    fn stream(&mut self) -> Result<&mut WebSocketStream<MaybeTlsStream<TcpStream>>> {
        self.stream.as_mut().context("not connected")
    }
}

impl WebSocketClient for TungsteniteClient {
    async fn connect(&mut self, url: &str, header_option: Option<Header>) -> Result<()> {
        let mut request = url
            .into_client_request()
            .with_context(|| format!("invalid websocket url {url}"))?;
        if let Some(header) = header_option {
            let mut value = HeaderValue::from_str(header.value.expose_secret())
                .with_context(|| format!("invalid {} header value", header.name))?;
            value.set_sensitive(true);
            request.headers_mut().insert(header.name, value);
        }
        let (stream, response) = connect_async(request)
            .await
            .with_context(|| format!("failed to connect to {url}"))?;
        info!("connected to {url}, status {}", response.status());
        self.stream = Some(stream);
        Ok(())
    }

    async fn send(&mut self, message: Message) -> Result<()> {
        Ok(self.stream()?.send(message).await?)
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(self.stream()?.flush().await?)
    }

    async fn next(&mut self) -> Option<Result<Message>> {
        let frame_option = self.stream.as_mut()?.next().await;
        frame_option.map(|frame_result| frame_result.map_err(Into::into))
    }

    fn disconnect(&mut self) {
        self.stream = None;
    }
}

/// Only the tungstenite plumbing needs a real socket; everything above it is tested against a mock venue.
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::spawn;
    use tokio::sync::oneshot;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::Utf8Bytes;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    const HEADER_NAME: &str = "X-MBX-APIKEY";

    /// Echoes text until Close; reports the header it saw.
    async fn spawn_echo_server() -> (String, oneshot::Receiver<Option<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding a local port is expected to not fail");
        let address = listener
            .local_addr()
            .expect("a bound listener has an address");
        let (header_sender, header) = oneshot::channel();
        spawn(async move {
            let (socket, _) = listener
                .accept()
                .await
                .expect("accepting is expected to not fail");
            #[expect(
                clippy::result_large_err,
                reason = "tungstenite's callback signature fixes the error type"
            )]
            let record_header = |request: &Request, response: Response| {
                let value_option = request
                    .headers()
                    .get(HEADER_NAME)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                let _ = header_sender.send(value_option);
                Ok(response)
            };
            let mut stream = accept_hdr_async(socket, record_header)
                .await
                .expect("the websocket handshake is expected to not fail");
            while let Some(Ok(frame)) = stream.next().await {
                if frame.is_text() {
                    stream
                        .send(frame)
                        .await
                        .expect("echoing is expected to not fail");
                }
            }
        });
        (format!("ws://{address}"), header)
    }

    #[tokio::test]
    async fn sends_header_exchanges_frames_and_closes() {
        let (url, header) = spawn_echo_server().await;
        let api_key = Header {
            name: HEADER_NAME,
            value: SecretString::from("dummy-api-key"),
        };
        let mut client = TungsteniteClient::default();
        client
            .connect(&url, Some(api_key))
            .await
            .expect("connecting is expected to not fail");
        assert_eq!(
            Some("dummy-api-key".to_owned()),
            header.await.expect("the server reports the header")
        );

        let text = Message::Text(Utf8Bytes::from_static("hello"));
        client
            .send(text.clone())
            .await
            .expect("sending is expected to not fail");
        let echoed = client
            .next()
            .await
            .expect("the server is expected to echo")
            .expect("the echo is expected to be readable");
        assert_eq!(text, echoed);

        client
            .send(Message::Close(None))
            .await
            .expect("sending Close is expected to not fail");
        client
            .flush()
            .await
            .expect("flushing is expected to not fail");
        let reply = client
            .next()
            .await
            .expect("the server is expected to answer Close")
            .expect("the answer is expected to be readable");
        assert!(reply.is_close());

        client.disconnect();
        assert!(client.next().await.is_none());
        assert!(client.send(text).await.is_err());
        assert!(client.flush().await.is_err());
    }

    #[tokio::test]
    async fn unreachable_or_invalid_url_is_an_error() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding a local port is expected to not fail");
        let address = listener
            .local_addr()
            .expect("a bound listener has an address");
        drop(listener);
        let mut client = TungsteniteClient::default();
        assert!(
            client
                .connect(&format!("ws://{address}"), None)
                .await
                .is_err()
        );
        assert!(client.connect("not a url", None).await.is_err());
        assert!(client.next().await.is_none());
    }
}

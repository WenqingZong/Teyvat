use super::client::{Header, WebSocketClient};
use anyhow::{Context, Error, Result, anyhow, bail};
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::time::{Instant, Sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Bytes, Message, Utf8Bytes};
use tracing::{debug, info, trace, warn};

/// How an idle connection is kept alive: a websocket control frame, or venue-level text.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Keepalive {
    PingFrame,
    /// For venues that ping in application text, such as OKX; its connector lands next.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "used once the OKX websocket connector lands")
    )]
    Text {
        ping: &'static str,
        pong: &'static str,
    },
}

/// When to ping an idle connection and when to give it up; each venue picks its own.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Heartbeat {
    pub(crate) ping_after_idle: Duration,
    pub(crate) dead_after_silence: Duration,
    /// How long a graceful close waits for the peer's Close.
    pub(crate) close_timeout: Duration,
    /// How long opening a connection may take, so a hung handshake cannot hold the control plane.
    pub(crate) connect_timeout: Duration,
    pub(crate) keepalive: Keepalive,
}

/// One websocket with its heartbeat, connected and closed in place. Replies to Ping and Close are sent
/// explicitly; on tungstenite they replace the replies it queued itself, so nothing is sent twice.
pub(crate) struct WebSocketConnection<Client> {
    client: Client,
    heartbeat: Heartbeat,
    connected: bool,
    /// The last URL connected to.
    url: String,
    /// Absolute, so a `next_message` dropped by `select!` does not restart the countdown.
    last_received: Instant,
    last_ping_sent: Instant,
    /// One timer for both ping and death, armed at the earlier deadline and kept across calls: frames only
    /// move `last_received`, and the timer re-arms itself when it fires early, so reading touches no timer.
    heartbeat_timer: Pin<Box<Sleep>>,
    /// Set while a frame may sit unflushed after `next_message` was cancelled mid-send.
    flush_pending: bool,
}

impl<Client: WebSocketClient> WebSocketConnection<Client> {
    /// Must run inside a tokio runtime, which owns the heartbeat timer.
    pub(crate) fn new(client: Client, heartbeat: Heartbeat) -> Self {
        let now = Instant::now();
        Self {
            client,
            heartbeat,
            connected: false,
            url: String::new(),
            last_received: now,
            last_ping_sent: now,
            heartbeat_timer: Box::pin(sleep_until(now)),
            flush_pending: false,
        }
    }

    pub(crate) async fn connect(&mut self, url: &str, header_option: Option<Header>) -> Result<()> {
        let connect_timeout = self.heartbeat.connect_timeout;
        timeout(connect_timeout, self.client.connect(url, header_option))
            .await
            .with_context(|| {
                format!("connecting to {url} timed out after {connect_timeout:?}")
            })??;
        let now = Instant::now();
        self.connected = true;
        self.url = url.to_owned();
        self.last_received = now;
        self.last_ping_sent = now;
        self.flush_pending = false;
        self.arm_heartbeat_timer();
        Ok(())
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// The next Text or Binary frame. An error means the connection is gone, and it is disconnected by then:
    /// the peer closed it, it failed, or nothing arrived for `dead_after_silence`.
    /// Cancel-safe: reads are, deadlines are absolute, and an interrupted send is flushed on the next call.
    pub(crate) async fn next_message(&mut self) -> Result<Message> {
        let message_result = self.receive().await;
        if message_result.is_err() {
            self.disconnect();
        }
        message_result
    }

    async fn receive(&mut self) -> Result<Message> {
        if !self.connected {
            bail!("not connected");
        }
        if self.flush_pending {
            self.client.flush().await.context("flush failed")?;
            self.flush_pending = false;
        }
        loop {
            select! {
                frame_option = self.client.next() => {
                    self.last_received = Instant::now();
                    let reply = match frame_option {
                        Some(Ok(Message::Text(text))) => {
                            if let Keepalive::Text { ping, pong } = self.heartbeat.keepalive {
                                if text.as_str() == pong {
                                    continue;
                                }
                                if text.as_str() == ping {
                                    self.send(Message::text(pong)).await.context("pong failed")?;
                                    continue;
                                }
                            }
                            return Ok(Message::Text(text));
                        }
                        Some(Ok(message @ Message::Binary(_))) => return Ok(message),
                        Some(Ok(Message::Ping(payload))) => Message::Pong(payload),
                        Some(Ok(Message::Pong(_) | Message::Frame(_))) => continue,
                        Some(Ok(Message::Close(close_frame_option))) => return Err(self.closed_by_peer(close_frame_option).await),
                        Some(Err(error)) => return Err(error.context("read failed")),
                        None => bail!("stream ended"),
                    };
                    self.send(reply).await.context("pong failed")?;
                }
                () = &mut self.heartbeat_timer => {
                    let now = Instant::now();
                    if now >= self.dead_at() {
                        bail!("nothing received for {:?}", self.heartbeat.dead_after_silence);
                    }
                    if now >= self.ping_at() {
                        self.last_ping_sent = now;
                        let ping = match self.heartbeat.keepalive {
                            Keepalive::PingFrame => Message::Ping(Bytes::new()),
                            Keepalive::Text { ping, .. } => Message::text(ping),
                        };
                        self.send(ping).await.context("ping failed")?;
                        trace!("pinged idle {}", self.url);
                    }
                    self.arm_heartbeat_timer();
                }
            }
        }
    }

    fn dead_at(&self) -> Instant {
        self.last_received + self.heartbeat.dead_after_silence
    }

    fn ping_at(&self) -> Instant {
        self.last_received.max(self.last_ping_sent) + self.heartbeat.ping_after_idle
    }

    fn arm_heartbeat_timer(&mut self) {
        let deadline = self.ping_at().min(self.dead_at());
        self.heartbeat_timer.as_mut().reset(deadline);
    }

    /// A failure means the connection is gone, and it is disconnected by then.
    pub(crate) async fn send_text(&mut self, text: String) -> Result<()> {
        if !self.connected {
            bail!("not connected");
        }
        let send_result = self.send(Message::text(text)).await;
        if send_result.is_err() {
            self.disconnect();
        }
        send_result.context("send failed")
    }

    async fn send(&mut self, message: Message) -> Result<()> {
        self.flush_pending = true;
        self.client.send(message).await?;
        self.flush_pending = false;
        Ok(())
    }

    /// Echoes the peer's Close, which completes the handshake.
    async fn closed_by_peer(&mut self, frame_option: Option<CloseFrame>) -> Error {
        let reason = match &frame_option {
            Some(frame) => anyhow!("closed by peer: {} {}", frame.code, frame.reason),
            None => anyhow!("closed by peer"),
        };
        match timeout(
            self.heartbeat.close_timeout,
            self.send(Message::Close(frame_option)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => debug!("failed to answer Close from {}: {error:#}", self.url),
            Err(_) => debug!("timed out answering Close from {}", self.url),
        }
        reason
    }

    /// Sends Close, waits for the peer's Close so the handshake completes, then drops the connection.
    /// Does nothing while not connected.
    pub(crate) async fn close(&mut self) {
        if !self.connected {
            return;
        }
        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: Utf8Bytes::from_static(""),
        };
        match self.send(Message::Close(Some(frame))).await {
            Err(error) => warn!("failed to send Close to {}: {error:#}", self.url),
            Ok(()) => match timeout(self.heartbeat.close_timeout, self.wait_for_close()).await {
                Ok(()) => info!("closed {}", self.url),
                Err(_) => warn!(
                    "{} did not answer Close within {:?}",
                    self.url, self.heartbeat.close_timeout
                ),
            },
        }
        self.disconnect();
    }

    /// Stops at the peer's Close rather than at the end of the stream, since a peer may keep TCP open after it.
    async fn wait_for_close(&mut self) {
        while let Some(Ok(frame)) = self.client.next().await {
            if frame.is_close() {
                return;
            }
            trace!("discarding {frame:?} from closing {}", self.url);
        }
    }

    fn disconnect(&mut self) {
        self.client.disconnect();
        self.connected = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchanges::websocket::tests::{
        MockVenue, MockVenueConnection, MockWebSocketClient, mock_websocket,
    };
    use std::future::pending;

    const URL: &str = "wss://venue.example/stream";
    const HEARTBEAT: Heartbeat = Heartbeat {
        ping_after_idle: Duration::from_secs(10),
        dead_after_silence: Duration::from_secs(30),
        close_timeout: Duration::from_secs(2),
        connect_timeout: Duration::from_secs(2),
        keepalive: Keepalive::PingFrame,
    };
    /// Steps just before or past a deadline, since a deadline equal to the test's timeout may fire either way.
    const MILLISECOND: Duration = Duration::from_millis(1);

    /// A connection to a mock venue; tokio time is paused, so every duration below is exact.
    struct ConnectionFixture {
        venue: MockVenue,
        venue_connection: MockVenueConnection,
        connection: WebSocketConnection<MockWebSocketClient>,
    }

    impl ConnectionFixture {
        async fn new() -> Self {
            let (client, venue) = mock_websocket();
            Self::connect(client, venue).await
        }

        async fn connect(client: MockWebSocketClient, venue: MockVenue) -> Self {
            let mut connection = WebSocketConnection::new(client, HEARTBEAT);
            connection
                .connect(URL, None)
                .await
                .expect("the mock venue is expected to accept");
            let venue_connection = venue.expect_connection();
            Self {
                venue,
                venue_connection,
                connection,
            }
        }

        /// Fails if the connection hands back nothing within `duration`.
        async fn expect_message_within(&mut self, duration: Duration) -> Result<Message> {
            timeout(duration, self.connection.next_message())
                .await
                .expect("the connection is expected to hand back a message in time")
        }

        async fn expect_no_message_for(&mut self, duration: Duration) {
            let message_result = timeout(duration, self.connection.next_message()).await;
            assert!(message_result.is_err(), "unexpected {message_result:?}");
        }

        /// The venue drops its end; returns the connection under test.
        fn hang_up(self) -> WebSocketConnection<MockWebSocketClient> {
            self.connection
        }

        /// The connection is expected to fail at once, with `expected_reason`, and be disconnected.
        async fn expect_lost(&mut self, expected_reason: &str) {
            let error = self
                .expect_message_within(Duration::ZERO)
                .await
                .expect_err("the connection is expected to be lost");
            let reason = format!("{error:#}");
            assert!(reason.contains(expected_reason), "unexpected {reason}");
            assert!(!self.connection.is_connected());
        }
    }

    struct HangingClient;

    impl WebSocketClient for HangingClient {
        async fn connect(&mut self, _url: &str, _header: Option<Header>) -> Result<()> {
            pending().await
        }

        async fn send(&mut self, _message: Message) -> Result<()> {
            pending().await
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn next(&mut self) -> Option<Result<Message>> {
            pending().await
        }

        fn disconnect(&mut self) {}
    }

    fn close_frame(code: CloseCode, reason: &'static str) -> CloseFrame {
        CloseFrame {
            code,
            reason: Utf8Bytes::from_static(reason),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn text_keepalive_is_answered_and_hidden() {
        let heartbeat = Heartbeat {
            keepalive: Keepalive::Text {
                ping: "ping",
                pong: "pong",
            },
            ..HEARTBEAT
        };
        let (client, venue) = mock_websocket();
        let mut connection = WebSocketConnection::new(client, heartbeat);
        connection
            .connect(URL, None)
            .await
            .expect("the mock venue is expected to accept");
        let mut venue_connection = venue.expect_connection();

        venue_connection.send(Message::text("pong"));
        assert!(
            timeout(Duration::from_millis(1), connection.next_message())
                .await
                .is_err()
        );
        venue_connection.send(Message::text("ping"));
        assert!(
            timeout(Duration::from_millis(1), connection.next_message())
                .await
                .is_err()
        );
        assert_eq!(Message::text("pong"), venue_connection.expect_received());

        venue_connection.send(Message::text("data"));
        let message = timeout(Duration::from_millis(1), connection.next_message())
            .await
            .expect("the connection is expected to hand data back")
            .expect("the data frame is expected to be readable");
        assert_eq!(Message::text("data"), message);
    }

    #[tokio::test(start_paused = true)]
    async fn connect_times_out() {
        let mut connection = WebSocketConnection::new(HangingClient, HEARTBEAT);
        let started = Instant::now();
        let error = connection
            .connect(URL, None)
            .await
            .expect_err("a hanging connect is expected to time out");
        assert!(format!("{error:#}").contains("timed out"), "{error:#}");
        assert_eq!(HEARTBEAT.connect_timeout, started.elapsed());
        assert!(!connection.is_connected());
    }

    #[tokio::test(start_paused = true)]
    async fn data_frames_are_handed_back() {
        let mut fixture = ConnectionFixture::new().await;
        assert_eq!(URL, fixture.connection.url());
        assert!(fixture.connection.is_connected());
        let text = Message::Text(Utf8Bytes::from_static("text"));
        let binary = Message::Binary(Bytes::from_static(b"binary"));
        fixture.venue_connection.send(text.clone());
        fixture.venue_connection.send(binary.clone());
        for expected in [text, binary] {
            let message = fixture
                .expect_message_within(Duration::ZERO)
                .await
                .expect("a data frame is expected");
            assert_eq!(expected, message);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn venue_ping_is_answered_with_its_payload() {
        let mut fixture = ConnectionFixture::new().await;
        fixture
            .venue_connection
            .send(Message::Ping(Bytes::from_static(b"venue")));
        fixture.expect_no_message_for(MILLISECOND).await;
        assert_eq!(
            Message::Pong(Bytes::from_static(b"venue")),
            fixture.venue_connection.expect_received()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_connection_is_pinged_and_pongs_keep_it_alive() {
        let mut fixture = ConnectionFixture::new().await;
        fixture
            .expect_no_message_for(HEARTBEAT.ping_after_idle - MILLISECOND)
            .await;
        fixture.venue_connection.expect_nothing_received();
        fixture.expect_no_message_for(2 * MILLISECOND).await;
        assert_eq!(
            Message::Ping(Bytes::new()),
            fixture.venue_connection.expect_received()
        );

        for _ in 0..5 {
            fixture.venue_connection.send(Message::Pong(Bytes::new()));
            fixture
                .expect_no_message_for(HEARTBEAT.ping_after_idle + MILLISECOND)
                .await;
            assert_eq!(
                Message::Ping(Bytes::new()),
                fixture.venue_connection.expect_received()
            );
        }
        assert!(5 * HEARTBEAT.ping_after_idle > HEARTBEAT.dead_after_silence);
    }

    #[tokio::test(start_paused = true)]
    async fn silent_venue_is_declared_dead_exactly_on_time() {
        let mut fixture = ConnectionFixture::new().await;
        let started = Instant::now();
        let error = fixture
            .expect_message_within(HEARTBEAT.dead_after_silence)
            .await
            .expect_err("a silent venue is expected to be declared dead");
        assert!(error.to_string().contains("nothing received"), "{error}");
        assert_eq!(HEARTBEAT.dead_after_silence, started.elapsed());
        assert!(!fixture.connection.is_connected());
        assert_eq!(
            Message::Ping(Bytes::new()),
            fixture.venue_connection.expect_received()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn venue_close_is_echoed() {
        let mut fixture = ConnectionFixture::new().await;
        let frame = close_frame(CloseCode::Away, "restarting");
        fixture
            .venue_connection
            .send(Message::Close(Some(frame.clone())));
        fixture.expect_lost("closed by peer: 1001 restarting").await;
        assert_eq!(
            Message::Close(Some(frame)),
            fixture.venue_connection.expect_received()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn read_failure_loses_the_connection() {
        let mut fixture = ConnectionFixture::new().await;
        fixture.venue_connection.break_read();
        fixture.expect_lost("read failed: connection reset").await;
    }

    #[tokio::test(start_paused = true)]
    async fn text_reaches_the_venue_until_it_hangs_up() {
        let mut fixture = ConnectionFixture::new().await;
        fixture
            .connection
            .send_text("request".to_owned())
            .await
            .expect("sending is expected to not fail");
        assert_eq!(
            Message::text("request"),
            fixture.venue_connection.expect_received()
        );

        let mut connection = fixture.hang_up();
        let error = connection
            .send_text("request".to_owned())
            .await
            .expect_err("a hang-up is expected to fail the send");
        assert!(format!("{error:#}").contains("send failed"), "{error:#}");
        assert!(!connection.is_connected());
        let error = connection
            .send_text("request".to_owned())
            .await
            .expect_err("a lost connection is expected to refuse sends");
        assert_eq!("not connected", error.to_string());
    }

    #[tokio::test(start_paused = true)]
    async fn hang_up_ends_the_stream() {
        let mut connection = ConnectionFixture::new().await.hang_up();
        let error = connection
            .next_message()
            .await
            .expect_err("a hang-up is expected to lose the connection");
        assert_eq!("stream ended", error.to_string());
        assert!(!connection.is_connected());
    }

    #[tokio::test(start_paused = true)]
    async fn hang_up_fails_the_pong() {
        let fixture = ConnectionFixture::new().await;
        fixture.venue_connection.send(Message::Ping(Bytes::new()));
        let mut connection = fixture.hang_up();
        let error = connection
            .next_message()
            .await
            .expect_err("a hang-up is expected to lose the connection");
        assert!(format!("{error:#}").contains("pong failed"), "{error:#}");
        assert!(!connection.is_connected());
    }

    #[tokio::test(start_paused = true)]
    async fn lost_connection_refuses_reads_until_reconnected() {
        let mut fixture = ConnectionFixture::new().await;
        fixture.venue_connection.break_read();
        fixture.expect_lost("read failed").await;
        fixture.expect_lost("not connected").await;

        fixture
            .connection
            .connect(URL, None)
            .await
            .expect("the mock venue is expected to accept again");
        let venue_connection = fixture.venue.expect_connection();
        let text = Message::Text(Utf8Bytes::from_static("again"));
        venue_connection.send(text.clone());
        let message = fixture
            .expect_message_within(Duration::ZERO)
            .await
            .expect("the new connection is expected to deliver");
        assert_eq!(text, message);
    }

    #[tokio::test(start_paused = true)]
    async fn close_ends_on_the_venue_answer() {
        let mut fixture = ConnectionFixture::new().await;
        let started = Instant::now();
        fixture.connection.close().await;
        assert_eq!(Duration::ZERO, started.elapsed());
        assert!(!fixture.connection.is_connected());
        assert_eq!(
            Message::Close(Some(close_frame(CloseCode::Normal, ""))),
            fixture.venue_connection.expect_received()
        );

        fixture.connection.close().await;
        fixture.venue_connection.expect_nothing_received();
    }

    #[tokio::test(start_paused = true)]
    async fn close_gives_up_on_a_venue_that_never_answers() {
        let (client, venue) = mock_websocket();
        venue.ignore_close();
        let mut fixture = ConnectionFixture::connect(client, venue).await;
        fixture
            .venue_connection
            .send(Message::Text(Utf8Bytes::from_static("late data")));
        let started = Instant::now();
        fixture.connection.close().await;
        assert_eq!(HEARTBEAT.close_timeout, started.elapsed());
        assert!(fixture.venue_connection.expect_received().is_close());
        fixture.venue.expect_no_connection();
    }

    #[tokio::test(start_paused = true)]
    async fn close_to_a_vanished_venue_returns_at_once() {
        let mut connection = ConnectionFixture::new().await.hang_up();
        let started = Instant::now();
        connection.close().await;
        assert_eq!(Duration::ZERO, started.elapsed());
        assert!(!connection.is_connected());
    }
}

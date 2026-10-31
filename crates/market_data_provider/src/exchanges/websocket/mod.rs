//! Shared by every exchange connector; nothing here may know about a particular venue.

mod client;
mod connection;
mod endpoint_set;

pub(super) use client::{Header, TungsteniteClient, WebSocketClient};
pub(super) use connection::{Heartbeat, Keepalive};
pub(super) use endpoint_set::{EndpointId, EndpointSet, EndpointState};

/// A mock venue: each connection a client opens is a pair of channels the test drives by hand.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use anyhow::{Result, anyhow, bail};
    use secrecy::ExposeSecret;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, MutexGuard};
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
    use tokio_tungstenite::tungstenite::Message;

    /// The venue's automatic answer to a frame the client sends, if any.
    pub(crate) type Responder = Arc<dyn Fn(&Message) -> Option<Message> + Send + Sync>;

    #[derive(Default)]
    struct VenueState {
        accepted: VecDeque<MockVenueConnection>,
        refuses: bool,
        ignores_close: bool,
        responder: Option<Responder>,
    }

    /// The test's side: every connection its client opens shows up here.
    pub(crate) struct MockVenue {
        state: Arc<Mutex<VenueState>>,
    }

    /// The code under test's side.
    pub(crate) struct MockWebSocketClient {
        state: Arc<Mutex<VenueState>>,
        /// Set while connected.
        connection: Option<MockConnection>,
    }

    struct MockConnection {
        from_venue: UnboundedReceiver<Result<Message>>,
        to_venue: UnboundedSender<Message>,
        answers_close: bool,
        /// Automatic answers, including to our Close, read before anything the test sends.
        answers: VecDeque<Message>,
    }

    pub(crate) fn mock_websocket() -> (MockWebSocketClient, MockVenue) {
        let state = Arc::new(Mutex::new(VenueState::default()));
        let client = MockWebSocketClient {
            state: state.clone(),
            connection: None,
        };
        (client, MockVenue { state })
    }

    fn lock(state: &Mutex<VenueState>) -> MutexGuard<'_, VenueState> {
        state
            .lock()
            .expect("no test panics while holding the mock venue state")
    }

    impl MockVenue {
        pub(crate) fn refuse_connections(&self) {
            lock(&self.state).refuses = true;
        }

        pub(crate) fn accept_connections(&self) {
            lock(&self.state).refuses = false;
        }

        /// From now on, every connection answers each frame the client sends with `responder`'s reply.
        /// The frame still reaches `expect_received`.
        pub(crate) fn respond_with(&self, responder: Responder) {
            lock(&self.state).responder = Some(responder);
        }

        /// From now on, new connections never answer our Close, unlike real venues.
        pub(crate) fn ignore_close(&self) {
            lock(&self.state).ignores_close = true;
        }

        /// The oldest connection the venue accepted and the test has not taken yet.
        pub(crate) fn expect_connection(&self) -> MockVenueConnection {
            lock(&self.state)
                .accepted
                .pop_front()
                .expect("the venue is expected to have accepted a connection")
        }

        pub(crate) fn expect_no_connection(&self) {
            assert!(
                lock(&self.state).accepted.is_empty(),
                "the venue is expected to have accepted no further connection"
            );
        }
    }

    /// One accepted connection. Dropping it hangs up.
    pub(crate) struct MockVenueConnection {
        pub(crate) url: String,
        /// The value of the request header, if one was sent.
        pub(crate) header: Option<String>,
        to_client: UnboundedSender<Result<Message>>,
        from_client: UnboundedReceiver<Message>,
    }

    impl MockVenueConnection {
        pub(crate) fn send(&self, message: Message) {
            self.to_client
                .send(Ok(message))
                .expect("the client side is expected to still exist");
        }

        /// The client's next read fails.
        pub(crate) fn break_read(&self) {
            self.to_client
                .send(Err(anyhow!("connection reset")))
                .expect("the client side is expected to still exist");
        }

        /// The oldest frame the client sent and the test has not taken yet.
        pub(crate) fn expect_received(&mut self) -> Message {
            self.from_client
                .try_recv()
                .expect("the client is expected to have sent a frame")
        }

        pub(crate) fn expect_nothing_received(&mut self) {
            match self.from_client.try_recv() {
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
                Ok(frame) => panic!("the client is expected to have sent nothing, sent {frame:?}"),
            }
        }
    }

    impl WebSocketClient for MockWebSocketClient {
        async fn connect(&mut self, url: &str, header_option: Option<Header>) -> Result<()> {
            let mut state = lock(&self.state);
            if state.refuses {
                bail!("{url} refused the connection");
            }
            let (to_client, from_venue) = unbounded_channel();
            let (to_venue, from_client) = unbounded_channel();
            state.accepted.push_back(MockVenueConnection {
                url: url.to_owned(),
                header: header_option.map(|header| header.value.expose_secret().to_owned()),
                to_client,
                from_client,
            });
            self.connection = Some(MockConnection {
                from_venue,
                to_venue,
                answers_close: !state.ignores_close,
                answers: VecDeque::new(),
            });
            Ok(())
        }

        async fn send(&mut self, message: Message) -> Result<()> {
            let responder_option = lock(&self.state).responder.clone();
            let Some(connection) = &mut self.connection else {
                bail!("not connected");
            };
            if connection.answers_close && message.is_close() {
                connection.answers.push_back(message.clone());
            } else if let Some(answer) = responder_option.and_then(|responder| responder(&message))
            {
                connection.answers.push_back(answer);
            }
            connection
                .to_venue
                .send(message)
                .map_err(|_| anyhow!("the venue hung up"))
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn next(&mut self) -> Option<Result<Message>> {
            let connection = self.connection.as_mut()?;
            if let Some(answer) = connection.answers.pop_front() {
                return Some(Ok(answer));
            }
            connection.from_venue.recv().await
        }

        fn disconnect(&mut self) {
            self.connection = None;
        }
    }
}

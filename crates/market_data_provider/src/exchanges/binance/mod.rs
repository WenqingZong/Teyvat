mod endpoint;
mod message;
mod sbe;
mod venue_stream;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::pending;
use std::num::NonZeroU64;
use std::time::Duration;

use anyhow::{Error, Result, anyhow};
use futures::future::OptionFuture;
use secrecy::SecretString;
use strum::EnumCount;
use tokio::select;
use tokio::time::{Instant, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use super::ExchangeConnector;
use super::websocket::{
    EndpointId, EndpointSet, EndpointState, Header, Heartbeat, Keepalive, TungsteniteClient,
    WebSocketClient,
};
use crate::alert::{SendAlert, alert_and_panic};
use crate::exit_reason::ExitReason;
use crate::phanes_client::{GnosisSource, PhanesClient, fetch_gnosis_or_panic};
use crate::utils::{Clock, RealClock};
use endpoint::{API_KEY_HEADER, BinanceEndpoint};
use layline::bark::Notifier;
use layline::gnosis::binance::BinanceSbeApiKey;
use layline::types::ConnectedApi;
use layline::types::{Bbo, Exchange, LatencyMetric, MarketData, Topic, Trade};
use message::{
    Request, Response, TextFrame, classify_text_frame, event_to_market_data, taker_side,
    to_sequence,
};
use sbe::SbeFrame;
use venue_stream::{
    SBE_BEST_BID_ASK_CHANNEL, STREAM_NAME_CAPACITY, TRADE_CHANNEL, endpoint_for_topic,
    sbe_stream_name, stream_name,
};

/// Binance futures ping only every ~3 minutes, so we ping idle connections ourselves.
const HEARTBEAT: Heartbeat = Heartbeat {
    ping_after_idle: Duration::from_secs(10),
    dead_after_silence: Duration::from_secs(30),
    close_timeout: Duration::from_secs(2),
    connect_timeout: Duration::from_secs(10),
    keepalive: Keepalive::PingFrame,
};
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
/// A lost endpoint is restored at once, then retried after 1 s, doubling up to a minute.
const FIRST_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);
const RESTORE_FAILURES_BEFORE_ALERT: u32 = 5;
const SUBSCRIBE: &str = "SUBSCRIBE";
const UNSUBSCRIBE: &str = "UNSUBSCRIBE";

/// One websocket per endpoint, opened on its first stream and closed with its last; a lost one is
/// reconnected and resubscribed. The type parameters let tests swap the venue, Phanes, the phone and
/// the clock; the defaults are production.
enum FrameOutcome {
    None,
    Response(Response),
    Reconnect,
}

pub(crate) struct BinanceConnector<
    WebSocket = TungsteniteClient,
    KeySource = PhanesClient,
    Alerter = Notifier,
    TimeSource = RealClock,
> {
    demo: bool,
    key_source: KeySource,
    alert: Alerter,
    clock: TimeSource,
    /// Indexed by `BinanceEndpoint::index`, so the per-message path neither hashes nor allocates.
    endpoint_set: BinanceEndpointSet<WebSocket>,
    /// The SBE API key, fetched on first need.
    sbe_api_key: Option<SecretString>,
    /// Parsed but not yet handed out; one SBE frame can carry several trades.
    pending: VecDeque<MarketData>,
    next_request_id: u64,
}

const ENDPOINT_COUNT: usize = BinanceEndpoint::COUNT;

type BinanceEndpointSet<WebSocket> = EndpointSet<ENDPOINT_COUNT, BinanceEndpoint, WebSocket>;

impl EndpointId for BinanceEndpoint {
    fn index(self) -> usize {
        BinanceEndpoint::index(self)
    }

    fn from_index(index: usize) -> Self {
        BinanceEndpoint::from_repr(index).expect("every index below COUNT is an endpoint")
    }
}

impl<WebSocket, KeySource, Alerter, TimeSource> ExchangeConnector
    for BinanceConnector<WebSocket, KeySource, Alerter, TimeSource>
where
    WebSocket: WebSocketClient,
    KeySource: GnosisSource,
    Alerter: SendAlert,
    TimeSource: Clock,
{
    const EXCHANGE: Exchange = Exchange::Binance;
    // Edited by hand: list every venue API this connector calls.
    const CONNECTED_APIS: &'static [ConnectedApi] = endpoint::CONNECTED_APIS;

    /// Fails the whole batch if any endpoint fails, unsubscribing what the batch already subscribed.
    /// An endpoint that cannot be connected to stops the provider instead: see `connect_or_panic`.
    async fn subscribe(&mut self, topics: &HashSet<Topic>) -> Result<()> {
        let streams_by_endpoint = group_streams_by_endpoint(topics);
        info!("subscribing {}", describe_streams(&streams_by_endpoint));
        let mut subscribed = Vec::new();
        for (endpoint, streams) in streams_by_endpoint {
            match self.subscribe_streams(endpoint, streams).await {
                Ok(added_keys) => subscribed.push((endpoint, added_keys)),
                Err(error) => {
                    for (endpoint, added_keys) in subscribed {
                        if let Err(error) = self.unsubscribe_streams(endpoint, &added_keys).await {
                            warn!("failed to roll back {endpoint}: {error:#}");
                        }
                    }
                    return Err(error.context(endpoint.to_string()));
                }
            }
        }
        Ok(())
    }

    async fn unsubscribe(&mut self, topics: &HashSet<Topic>) -> Result<()> {
        let streams_by_endpoint = group_streams_by_endpoint(topics);
        info!("unsubscribing {}", describe_streams(&streams_by_endpoint));
        let mut first_error_option = None;
        for (endpoint, streams) in streams_by_endpoint {
            let keys: Vec<String> = streams.into_keys().collect();
            if let Err(error) = self.unsubscribe_streams(endpoint, &keys).await {
                first_error_option.get_or_insert(error.context(endpoint.to_string()));
            }
        }
        first_error_option.map_or(Ok(()), Err)
    }

    /// Cancel-safe, as the trait requires: reads are, and an interrupted restore is simply retried.
    /// Allocation-free per message: a fixed `select!` over the endpoints, no boxed futures.
    async fn next_event(&mut self) -> MarketData {
        loop {
            if let Some(market_data) = self.pending.pop_front() {
                return market_data;
            }
            let next_restore_at_option = self.endpoint_set.next_restore_at();
            if next_restore_at_option.is_some_and(|scheduled_at| scheduled_at <= Instant::now()) {
                self.restore_due_endpoints().await;
                continue;
            }
            let clock = &self.clock;
            let [spot_sbe, futures_public] = self.endpoint_set.states_mut();
            let spot_sbe_connected = spot_sbe.connection.is_connected();
            let futures_public_connected = futures_public.connection.is_connected();
            let (endpoint, message_result, received_time_nanoseconds) = select! {
                (message_result, received_time_nanoseconds) = read(spot_sbe, clock), if spot_sbe_connected => {
                    (BinanceEndpoint::SpotSbe, message_result, received_time_nanoseconds)
                }
                (message_result, received_time_nanoseconds) = read(futures_public, clock), if futures_public_connected => {
                    (BinanceEndpoint::FuturesPublic, message_result, received_time_nanoseconds)
                }
                Some(()) = OptionFuture::from(next_restore_at_option.map(sleep_until)) => continue,
                else => pending().await,
            };
            match message_result {
                Ok(message) => {
                    match self.handle_frame(endpoint, received_time_nanoseconds, message) {
                        FrameOutcome::Response(response) => log_stray_response(endpoint, &response),
                        FrameOutcome::Reconnect => {
                            self.endpoint_state_mut(endpoint).connection.close().await;
                            self.lost(endpoint, &anyhow!("venue asked to reconnect"));
                        }
                        FrameOutcome::None => {}
                    }
                }
                Err(error) => self.lost(endpoint, &error),
            }
        }
    }

    async fn disconnect(&mut self) {
        self.endpoint_set.disconnect_all().await;
    }
}

impl<WebSocket, KeySource, Alerter, TimeSource>
    BinanceConnector<WebSocket, KeySource, Alerter, TimeSource>
where
    WebSocket: WebSocketClient,
    KeySource: GnosisSource,
    Alerter: SendAlert,
    TimeSource: Clock,
{
    /// Connects nothing: an endpoint connects on its first stream. `create_websocket` makes each endpoint's client.
    pub(super) fn new(
        demo: bool,
        create_websocket: impl FnMut(BinanceEndpoint) -> WebSocket,
        key_source: KeySource,
        alert: Alerter,
        clock: TimeSource,
    ) -> Self {
        info!("BinanceConnector::new is called, demo: {demo}");
        let endpoints = EndpointSet::new(create_websocket, HEARTBEAT);
        Self {
            demo,
            key_source,
            alert,
            clock,
            endpoint_set: endpoints,
            sbe_api_key: None,
            pending: VecDeque::new(),
            next_request_id: 1,
        }
    }

    fn endpoint_state_mut(&mut self, endpoint: BinanceEndpoint) -> &mut EndpointState<WebSocket> {
        self.endpoint_set.state_mut(endpoint)
    }

    #[cfg(test)]
    fn endpoint_state(&self, endpoint: BinanceEndpoint) -> &EndpointState<WebSocket> {
        self.endpoint_set.state(endpoint)
    }

    /// Returns the names it added. A connection that is new or being restored is sent every stream.
    async fn subscribe_streams(
        &mut self,
        endpoint: BinanceEndpoint,
        streams: HashMap<String, Topic>,
    ) -> Result<Vec<String>> {
        let (added_keys, resubscribe_all) = self.endpoint_set.record_topics(endpoint, streams);
        if added_keys.is_empty() {
            return Ok(added_keys);
        }
        let keys = if resubscribe_all {
            self.endpoint_state_mut(endpoint)
                .topics_by_key
                .keys()
                .cloned()
                .collect()
        } else {
            added_keys.clone()
        };
        if !self.endpoint_state_mut(endpoint).connection.is_connected() {
            self.connect_or_panic(endpoint).await;
        }
        match self.request(endpoint, SUBSCRIBE, &keys).await {
            Ok(()) => {
                self.endpoint_set.clear_restore(endpoint);
                Ok(added_keys)
            }
            Err(error) => {
                self.endpoint_set.rollback_topics(endpoint, &added_keys);
                self.fail(endpoint).await;
                Err(error)
            }
        }
    }

    /// The last stream closes the connection; other streams are unsubscribed if the connection is up.
    async fn unsubscribe_streams(
        &mut self,
        endpoint: BinanceEndpoint,
        names: &[String],
    ) -> Result<()> {
        let removed = self.endpoint_set.forget_topics(endpoint, names);
        if removed.is_empty() {
            return Ok(());
        }
        if self.endpoint_state_mut(endpoint).topics_by_key.is_empty() {
            self.endpoint_set.clear_restore(endpoint);
            self.endpoint_state_mut(endpoint).connection.close().await;
            return Ok(());
        }
        if !self.endpoint_state_mut(endpoint).connection.is_connected() {
            return Ok(());
        }
        if let Err(error) = self.request(endpoint, UNSUBSCRIBE, &removed).await {
            self.fail(endpoint).await;
            return Err(error);
        }
        Ok(())
    }

    /// Closes the connection and, if streams remain, schedules their restore with backoff.
    async fn fail(&mut self, endpoint: BinanceEndpoint) {
        self.endpoint_state_mut(endpoint).connection.close().await;
        self.endpoint_set
            .schedule_restore(endpoint, FIRST_RETRY_DELAY, MAX_RETRY_DELAY);
    }

    fn lost(&mut self, endpoint: BinanceEndpoint, error: &Error) {
        let (url, streams) = {
            let endpoint_state = self.endpoint_set.state(endpoint);
            (
                endpoint_state.connection.url().to_owned(),
                endpoint_state.topics_by_key.len(),
            )
        };
        error!("lost {endpoint} connection to {url}: {error:#}; restoring its {streams} streams");
        self.endpoint_set.schedule_immediate_restore(endpoint);
    }

    async fn restore_due_endpoints(&mut self) {
        let due = self.endpoint_set.due_endpoints(Instant::now());
        for endpoint in due {
            let names: Vec<String> = self
                .endpoint_set
                .state(endpoint)
                .topics_by_key
                .keys()
                .cloned()
                .collect();
            if names.is_empty() {
                self.endpoint_set.clear_restore(endpoint);
                continue;
            }
            match self.connect_and_request(endpoint, SUBSCRIBE, &names).await {
                Ok(()) => {
                    let stream_count = names.len();
                    info!("restored {endpoint} with {stream_count} streams");
                    self.endpoint_set.clear_restore(endpoint);
                }
                Err(error) => {
                    self.fail(endpoint).await;
                    let failures = self.endpoint_set.restore_failures(endpoint);
                    let next_retry_delay = retry_delay(failures);
                    error!(
                        "failed to restore {endpoint} ({failures} in a row): {error:#}; retrying in {next_retry_delay:?}"
                    );
                    if failures == RESTORE_FAILURES_BEFORE_ALERT {
                        self.alert
                            .send_alert(
                                &format!("Market Data Provider Cannot Restore Binance {endpoint}"),
                                &format!("{failures} attempts failed, still retrying: {error:#}"),
                            )
                            .await;
                    }
                }
            }
        }
    }

    async fn connect_and_request(
        &mut self,
        endpoint: BinanceEndpoint,
        method: &str,
        names: &[String],
    ) -> Result<()> {
        if !self.endpoint_state_mut(endpoint).connection.is_connected() {
            self.connect(endpoint).await?;
        }
        self.request(endpoint, method, names).await
    }

    async fn connect(&mut self, endpoint: BinanceEndpoint) -> Result<()> {
        let header_option = match endpoint {
            BinanceEndpoint::SpotSbe => Some(Header {
                name: API_KEY_HEADER,
                value: self.sbe_api_key().await,
            }),
            BinanceEndpoint::FuturesPublic => None,
        };
        let url = endpoint.url(self.demo);
        self.endpoint_state_mut(endpoint)
            .connection
            .connect(url, header_option)
            .await
    }

    /// A subscription that cannot connect means a wrong URL, key or network, which no retry fixes:
    /// alerts the phone, then panics with `ExitReason::ExchangeUnreachable`, which the binary's panic
    /// hook turns into the exit code. Restoring a connection that worked before retries instead.
    async fn connect_or_panic(&mut self, endpoint: BinanceEndpoint) {
        if let Err(error) = self.connect(endpoint).await {
            match alert_and_panic(
                &self.alert,
                &format!("Market Data Provider Cannot Connect to Binance {endpoint}"),
                &format!("cannot connect to Binance {endpoint}: {error:#}"),
                ExitReason::ExchangeUnreachable,
            )
            .await {}
        }
    }

    /// Asks Phanes only when an SBE stream really needs the key, once; alerts and panics if that fails.
    async fn sbe_api_key(&mut self) -> SecretString {
        if let Some(api_key) = &self.sbe_api_key {
            return api_key.clone();
        }
        let sbe_api_key: BinanceSbeApiKey =
            fetch_gnosis_or_panic(&self.key_source, &self.alert, self.demo).await;
        self.sbe_api_key = Some(sbe_api_key.api_key());
        sbe_api_key.api_key()
    }

    /// Sends one request and waits for its answer; events arriving meanwhile are queued as usual.
    async fn request(
        &mut self,
        endpoint: BinanceEndpoint,
        method: &str,
        names: &[String],
    ) -> Result<()> {
        let id = self.next_request_id;
        self.next_request_id += 1;
        let text = serde_json::to_string(&Request {
            method,
            params: names,
            id,
        })?;
        debug!("sending {text} to {endpoint}");
        self.endpoint_state_mut(endpoint)
            .connection
            .send_text(text)
            .await?;
        timeout(RESPONSE_TIMEOUT, self.await_response(endpoint, id))
            .await
            .map_err(|_| anyhow!("no answer to {method} within {RESPONSE_TIMEOUT:?}"))?
    }

    async fn await_response(&mut self, endpoint: BinanceEndpoint, id: u64) -> Result<()> {
        loop {
            let message = self
                .endpoint_state_mut(endpoint)
                .connection
                .next_message()
                .await?;
            let received_time_nanoseconds = self.clock.now();
            match self.handle_frame(endpoint, received_time_nanoseconds, message) {
                FrameOutcome::Response(Response {
                    id: Some(response_id),
                    error: error_option,
                }) if response_id == id => {
                    return error_option.map_or(Ok(()), |error| Err(anyhow!("{error}")));
                }
                FrameOutcome::Response(response) => log_stray_response(endpoint, &response),
                FrameOutcome::Reconnect => {
                    self.endpoint_state_mut(endpoint).connection.close().await;
                    self.lost(endpoint, &anyhow!("venue asked to reconnect"));
                    return Err(anyhow!("venue asked to reconnect"));
                }
                FrameOutcome::None => {}
            }
        }
    }

    /// Classifies one frame, queues its market data, and asks for a reconnect when the venue says so.
    fn handle_frame(
        &mut self,
        endpoint: BinanceEndpoint,
        received_time_nanoseconds: NonZeroU64,
        message: Message,
    ) -> FrameOutcome {
        match message {
            Message::Text(text) => match classify_text_frame(&text) {
                Ok(TextFrame::Response(response)) => return FrameOutcome::Response(response),
                Ok(TextFrame::ServerShutdown) => {
                    warn!("{endpoint} asked to reconnect: server shutdown");
                    return FrameOutcome::Reconnect;
                }
                Ok(TextFrame::Event { stream, data }) => {
                    let Some(topic) = self
                        .endpoint_state_mut(endpoint)
                        .topics_by_key
                        .get(stream.as_ref())
                        .copied()
                    else {
                        debug!("dropping {stream} from {endpoint}: not subscribed");
                        return FrameOutcome::None;
                    };
                    match event_to_market_data(topic, &data, received_time_nanoseconds) {
                        Ok(Some(market_data)) => self.pending.push_back(market_data),
                        Ok(None) => debug!("skipping a fill that is not a market trade: {text}"),
                        Err(error) => {
                            warn!("failed to parse {stream} from {endpoint}: {error:#}: {text}")
                        }
                    }
                }
                Err(error) => warn!("unexpected text from {endpoint}: {error:#}: {text}"),
            },
            Message::Binary(frame) => {
                self.handle_sbe_frame(endpoint, received_time_nanoseconds, &frame)
            }
            other => debug!("ignoring {other:?} from {endpoint}"),
        }
        FrameOutcome::None
    }

    fn handle_sbe_frame(
        &mut self,
        endpoint: BinanceEndpoint,
        received_time_nanoseconds: NonZeroU64,
        frame: &[u8],
    ) {
        match sbe::decode(frame) {
            Ok(SbeFrame::BestBidAsk(best_bid_ask)) => {
                let Some(topic) =
                    self.sbe_topic(endpoint, best_bid_ask.symbol, SBE_BEST_BID_ASK_CHANNEL)
                else {
                    return;
                };
                match to_sequence(best_bid_ask.update_id) {
                    Ok(sequence) => self.pending.push_back(MarketData::Bbo(Bbo {
                        instrument: topic.instrument,
                        exchange_time_nanoseconds: Some(best_bid_ask.event_time_nanoseconds),
                        latency_metric: LatencyMetric::received(received_time_nanoseconds),
                        ask_price: best_bid_ask.ask_price,
                        ask_size: best_bid_ask.ask_size,
                        bid_price: best_bid_ask.bid_price,
                        bid_size: best_bid_ask.bid_size,
                        sequence,
                    })),
                    Err(error) => {
                        let symbol = best_bid_ask.symbol;
                        warn!("dropping a {symbol} BBO from {endpoint}: {error:#}");
                    }
                }
            }
            Ok(SbeFrame::Trades(trades)) => {
                let Some(topic) = self.sbe_topic(endpoint, trades.symbol, TRADE_CHANNEL) else {
                    return;
                };
                let exchange_time_nanoseconds_option = Some(trades.transaction_time_nanoseconds);
                for trade_result in trades.trades() {
                    match trade_result.and_then(|trade| Ok((to_sequence(trade.id)?, trade))) {
                        Ok((sequence, trade)) => self.pending.push_back(MarketData::Trade(Trade {
                            instrument: topic.instrument,
                            exchange_time_nanoseconds: exchange_time_nanoseconds_option,
                            latency_metric: LatencyMetric::received(received_time_nanoseconds),
                            price: trade.price,
                            size: trade.size,
                            taker_side: taker_side(trade.buyer_is_maker),
                            sequence,
                        })),
                        Err(error) => {
                            let symbol = trades.symbol;
                            warn!("dropping a {symbol} trade from {endpoint}: {error:#}");
                        }
                    }
                }
            }
            Ok(SbeFrame::Other { template_id }) => {
                debug!("ignoring SBE template {template_id} from {endpoint}");
            }
            Err(error) => {
                let frame_hex = hex::encode(frame);
                warn!("failed to decode SBE from {endpoint}: {error:#}: {frame_hex}");
            }
        }
    }

    /// The subscribed topic of an SBE event, found without allocating.
    fn sbe_topic(
        &mut self,
        endpoint: BinanceEndpoint,
        symbol: &str,
        channel: &str,
    ) -> Option<Topic> {
        let mut buffer = [0; STREAM_NAME_CAPACITY];
        let topic_option = sbe_stream_name(symbol, channel, &mut buffer).and_then(|stream| {
            self.endpoint_state_mut(endpoint)
                .topics_by_key
                .get(stream)
                .copied()
        });
        if topic_option.is_none() {
            debug!("dropping {symbol}@{channel} from {endpoint}: not subscribed");
        }
        topic_option
    }
}

/// The next frame of one endpoint, stamped as soon as it is read.
async fn read<WebSocket: WebSocketClient>(
    endpoint_state: &mut EndpointState<WebSocket>,
    clock: &impl Clock,
) -> (Result<Message>, NonZeroU64) {
    let message_result = endpoint_state.connection.next_message().await;
    (message_result, clock.now())
}

fn log_stray_response(endpoint: BinanceEndpoint, response: &Response) {
    let request_id = &response.id;
    match &response.error {
        Some(error) => warn!("{endpoint} reported {error} for request {request_id:?}"),
        None => debug!("late answer from {endpoint} to request {request_id:?}"),
    }
}

/// Doubles from `FIRST_RETRY_DELAY` with each failure in a row, up to `MAX_RETRY_DELAY`.
fn retry_delay(failures: u32) -> Duration {
    layline::backoff::exponential_backoff(failures, FIRST_RETRY_DELAY, MAX_RETRY_DELAY)
}

fn group_streams_by_endpoint(
    topics: &HashSet<Topic>,
) -> BTreeMap<BinanceEndpoint, HashMap<String, Topic>> {
    let mut groups: BTreeMap<BinanceEndpoint, HashMap<String, Topic>> = BTreeMap::new();
    for topic in topics {
        groups
            .entry(endpoint_for_topic(topic))
            .or_default()
            .insert(stream_name(topic), *topic);
    }
    groups
}

/// `SpotSbe [btcusdt@bestBidAsk], FuturesPublic [btcusdt@bookTicker]`: endpoints tell spot and perpetual apart.
fn describe_streams(
    streams_by_endpoint: &BTreeMap<BinanceEndpoint, HashMap<String, Topic>>,
) -> String {
    streams_by_endpoint
        .iter()
        .map(|(endpoint, streams)| {
            let mut names: Vec<&str> = streams.keys().map(String::as_str).collect();
            names.sort_unstable();
            let joined_names = names.join(", ");
            format!("{endpoint} [{joined_names}]")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rust_decimal::dec;
    use serde_json::{Value, json};
    use strum::IntoEnumIterator;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    use tokio_tungstenite::tungstenite::{Bytes, Utf8Bytes};

    use super::*;
    use crate::alert::tests::{RecordingAlert, expect_alert_and_panic};
    use crate::exchanges::tests::test_connector_context;
    use crate::exchanges::websocket::tests::{
        MockVenue, MockVenueConnection, MockWebSocketClient, Responder, mock_websocket,
    };
    use crate::exit_reason::ExitReason;
    use crate::phanes_client::tests::MockGnosisSource;
    use crate::utils::tests::MockClock;
    use layline::gnosis::{Gnosis, GnosisId};
    use layline::testing::{test_instrument, test_topic};
    use layline::types::{Bbo, Currency, MarketDataType, Side, UnderlyingType};
    use sbe::tests::{EncodedTrade, encode_best_bid_ask, encode_trades};

    const SBE_API_KEY: &str = "dummy-sbe-api-key";
    /// The SBE gnosis as Phanes sends it.
    const SBE_GNOSIS: &str = r#"{"api_key": "dummy-sbe-api-key"}"#;
    /// 2026-10-24 00:00:00 UTC.
    const NOW_NANOSECONDS: NonZeroU64 =
        NonZeroU64::new(1_792_800_000_000_000_000).expect("not zero");
    const MILLISECOND: Duration = Duration::from_millis(1);

    type TestConnector =
        BinanceConnector<MockWebSocketClient, MockGnosisSource, RecordingAlert, MockClock>;

    /// The connector wired to a mock venue per endpoint that acknowledges every request, a mock Phanes
    /// granting the SBE key, a recording phone and a mock clock. Tests run on paused tokio time.
    struct BinanceFixture {
        demo: bool,
        connector: TestConnector,
        venues: HashMap<BinanceEndpoint, MockVenue>,
        alert: RecordingAlert,
    }

    impl BinanceFixture {
        fn new(demo: bool) -> Self {
            let mut venues = HashMap::new();
            let key_source = MockGnosisSource {
                grants: HashMap::from([(sbe_gnosis_id(demo), SBE_GNOSIS.to_owned())]),
                ..MockGnosisSource::default()
            };
            let alert = RecordingAlert::default();
            let connector = BinanceConnector::new(
                demo,
                |endpoint| {
                    let (websocket, venue) = mock_websocket();
                    venue.respond_with(answer(json!(null)));
                    venues.insert(endpoint, venue);
                    websocket
                },
                key_source,
                alert.clone(),
                MockClock::new(NOW_NANOSECONDS),
            );
            Self {
                demo,
                connector,
                venues,
                alert,
            }
        }

        async fn subscribe(&mut self, topics: &[Topic]) {
            self.connector
                .subscribe(&topics.iter().copied().collect())
                .await
                .expect("subscribe is expected to not fail");
        }

        async fn unsubscribe(&mut self, topics: &[Topic]) {
            self.connector
                .unsubscribe(&topics.iter().copied().collect())
                .await
                .expect("unsubscribe is expected to not fail");
        }

        fn venue(&self, endpoint: BinanceEndpoint) -> &MockVenue {
            &self.venues[&endpoint]
        }

        /// The oldest new connection to `endpoint`, checked to be of this environment and to carry `api_key_option`.
        fn expect_connection(
            &self,
            endpoint: BinanceEndpoint,
            api_key_option: Option<&str>,
        ) -> MockVenueConnection {
            let connection = self.venue(endpoint).expect_connection();
            assert_eq!(endpoint.url(self.demo), connection.url);
            assert_eq!(api_key_option, connection.header.as_deref());
            connection
        }

        fn expect_no_new_connection(&self) {
            for venue in self.venues.values() {
                venue.expect_no_connection();
            }
        }

        fn expect_connected_endpoints(&self, endpoints: &[BinanceEndpoint]) {
            let connected: HashSet<BinanceEndpoint> = BinanceEndpoint::iter()
                .filter(|endpoint| {
                    self.connector
                        .endpoint_state(*endpoint)
                        .connection
                        .is_connected()
                })
                .collect();
            assert_eq!(endpoints.iter().copied().collect::<HashSet<_>>(), connected);
        }

        /// The oldest new spot connection, which always carries the SBE key.
        fn expect_spot(&self) -> MockVenueConnection {
            self.expect_connection(BinanceEndpoint::SpotSbe, Some(SBE_API_KEY))
        }

        fn expect_futures(&self) -> MockVenueConnection {
            self.expect_connection(BinanceEndpoint::FuturesPublic, None)
        }

        fn expect_phanes_asked_for(&self, gnosis_ids: &[GnosisId]) {
            let requested = self.connector.key_source.requested_gnosis_ids.borrow();
            assert_eq!(gnosis_ids, requested.as_slice());
        }

        /// Drives the connector for `duration` of tokio time and returns the data it produced.
        async fn run_for(&mut self, duration: Duration) -> Vec<MarketData> {
            let mut produced = Vec::new();
            let _ = timeout(duration, async {
                loop {
                    produced.push(self.connector.next_event().await);
                }
            })
            .await;
            produced
        }
    }

    /// Answers every request carrying an id with `result_value`, as Binance does with `null` on success.
    fn answer(result_value: Value) -> Responder {
        Arc::new(move |message: &Message| {
            let request: Value = serde_json::from_str(message.to_text().ok()?).ok()?;
            let reply = json!({"result": result_value, "id": request.get("id")?});
            Some(Message::text(reply.to_string()))
        })
    }

    fn refuse_requests() -> Responder {
        Arc::new(|message: &Message| {
            let request: Value = serde_json::from_str(message.to_text().ok()?).ok()?;
            let reply =
                json!({"error": {"code": 2, "msg": "Invalid request"}, "id": request.get("id")?});
            Some(Message::text(reply.to_string()))
        })
    }

    /// The next frame the venue received, expected to be `method` with exactly `streams`.
    fn expect_request(connection: &mut MockVenueConnection, method: &str, streams: &[&str]) {
        let frame = connection.expect_received();
        let request: Value =
            serde_json::from_str(frame.to_text().expect("a text request")).expect("a JSON request");
        assert_eq!(method, request["method"], "{request}");
        let mut params: Vec<&str> = request["params"]
            .as_array()
            .expect("params is an array")
            .iter()
            .map(|param| param.as_str().expect("stream names are strings"))
            .collect();
        params.sort_unstable();
        let mut expected = streams.to_vec();
        expected.sort_unstable();
        assert_eq!(expected, params);
    }

    fn sbe_gnosis_id(demo: bool) -> GnosisId {
        BinanceSbeApiKey::gnosis_id(demo)
    }

    fn topic(
        base: Currency,
        underlying_type: UnderlyingType,
        market_data_type: MarketDataType,
    ) -> Topic {
        test_topic(
            test_instrument(Exchange::Binance, base, Currency::Usdt, underlying_type),
            market_data_type,
        )
    }

    fn spot_bbo() -> Topic {
        topic(Currency::Btc, UnderlyingType::Spot, MarketDataType::Bbo)
    }

    fn spot_trade() -> Topic {
        topic(Currency::Btc, UnderlyingType::Spot, MarketDataType::Trade)
    }

    fn perpetual_bbo() -> Topic {
        topic(
            Currency::Btc,
            UnderlyingType::Perpetual,
            MarketDataType::Bbo,
        )
    }

    fn perpetual_trade() -> Topic {
        topic(
            Currency::Btc,
            UnderlyingType::Perpetual,
            MarketDataType::Trade,
        )
    }

    fn ether_perpetual_bbo() -> Topic {
        topic(
            Currency::Eth,
            UnderlyingType::Perpetual,
            MarketDataType::Bbo,
        )
    }

    fn normal_close() -> Message {
        Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: Utf8Bytes::from_static(""),
        }))
    }

    /// A futures `bookTicker` event as Binance sends it.
    fn book_ticker(stream: &str, update_id: u64) -> Message {
        let event = json!({"stream": stream, "data": {"e": "bookTicker", "u": update_id, "s": "BTCUSDT",
            "b": "80570.70", "B": "2.6914", "a": "80570.80", "A": "187.5753", "T": 1_792_881_786_559_u64}});
        Message::text(event.to_string())
    }

    fn topics_of(produced: &[MarketData]) -> Vec<Topic> {
        produced.iter().map(MarketData::topic).collect()
    }

    fn expect_bbo(market_data: &MarketData) -> Bbo {
        match market_data {
            MarketData::Bbo(bbo) => *bbo,
            MarketData::Trade(trade) => panic!("a BBO is expected, got {trade:?}"),
        }
    }

    fn expect_trade(market_data: &MarketData) -> Trade {
        match market_data {
            MarketData::Trade(trade) => *trade,
            MarketData::Bbo(bbo) => panic!("a trade is expected, got {bbo:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn spot_goes_to_sbe_with_the_key_and_perpetuals_to_json_without() {
        for demo in [false, true] {
            let mut fixture = BinanceFixture::new(demo);
            fixture
                .subscribe(&[
                    spot_bbo(),
                    spot_trade(),
                    perpetual_bbo(),
                    ether_perpetual_bbo(),
                ])
                .await;
            let mut spot = fixture.expect_spot();
            expect_request(
                &mut spot,
                SUBSCRIBE,
                &["btcusdt@bestBidAsk", "btcusdt@trade"],
            );
            let mut futures = fixture.expect_futures();
            expect_request(
                &mut futures,
                SUBSCRIBE,
                &["btcusdt@bookTicker", "ethusdt@bookTicker"],
            );
            fixture.expect_no_new_connection();
            fixture.expect_phanes_asked_for(&[sbe_gnosis_id(demo)]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn perpetual_topics_never_ask_phanes() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .subscribe(&[perpetual_bbo(), perpetual_trade()])
            .await;
        fixture.expect_futures();
        fixture.expect_phanes_asked_for(&[]);
    }

    #[tokio::test(start_paused = true)]
    async fn topics_of_one_endpoint_share_a_connection() {
        let mut fixture = BinanceFixture::new(false);
        fixture.subscribe(&[perpetual_bbo()]).await;
        let mut connection = fixture.expect_futures();
        expect_request(&mut connection, SUBSCRIBE, &["btcusdt@bookTicker"]);

        fixture.subscribe(&[perpetual_bbo()]).await;
        connection.expect_nothing_received();
        fixture
            .subscribe(&[perpetual_bbo(), perpetual_trade()])
            .await;
        expect_request(&mut connection, SUBSCRIBE, &["btcusdt@trade"]);
        fixture.expect_no_new_connection();
        fixture.expect_connected_endpoints(&[BinanceEndpoint::FuturesPublic]);
    }

    #[tokio::test(start_paused = true)]
    async fn sbe_key_is_fetched_once_and_reused_on_reconnect() {
        for demo in [false, true] {
            let mut fixture = BinanceFixture::new(demo);
            fixture.subscribe(&[spot_trade()]).await;
            let mut connection = fixture.expect_spot();
            expect_request(&mut connection, SUBSCRIBE, &["btcusdt@trade"]);

            fixture.unsubscribe(&[spot_trade()]).await;
            assert_eq!(normal_close(), connection.expect_received());
            fixture.subscribe(&[spot_bbo()]).await;
            fixture.expect_spot();
            fixture.expect_phanes_asked_for(&[sbe_gnosis_id(demo)]);
            assert!(fixture.alert.alerts().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn phanes_failure_alerts_and_panics() {
        let mut fixture = BinanceFixture::new(false);
        fixture.connector.key_source.offline = true;
        let topics = HashSet::from([spot_bbo()]);
        let alert = expect_alert_and_panic(
            fixture.connector.subscribe(&topics),
            &fixture.alert,
            ExitReason::GnosisUnavailable,
        )
        .await;
        assert!(alert.contains(&sbe_gnosis_id(false).to_string()), "{alert}");
        assert!(alert.contains("Phanes is unreachable"), "{alert}");
        fixture.expect_no_new_connection();
    }

    #[tokio::test(start_paused = true)]
    async fn unsubscribing_keeps_the_connection_until_its_last_stream() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .subscribe(&[perpetual_bbo(), perpetual_trade()])
            .await;
        let mut connection = fixture.expect_futures();
        expect_request(
            &mut connection,
            SUBSCRIBE,
            &["btcusdt@bookTicker", "btcusdt@trade"],
        );

        fixture.unsubscribe(&[perpetual_bbo()]).await;
        expect_request(&mut connection, UNSUBSCRIBE, &["btcusdt@bookTicker"]);
        fixture.unsubscribe(&[perpetual_bbo()]).await;
        connection.expect_nothing_received();
        let started = Instant::now();
        fixture.unsubscribe(&[perpetual_trade()]).await;
        assert_eq!(Duration::ZERO, started.elapsed(), "the venue answers Close");
        assert_eq!(normal_close(), connection.expect_received());
        fixture.expect_connected_endpoints(&[]);
    }

    #[tokio::test(start_paused = true)]
    async fn refused_connection_alerts_and_panics() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .venue(BinanceEndpoint::FuturesPublic)
            .refuse_connections();
        let topics = HashSet::from([spot_bbo(), perpetual_bbo()]);
        let alert = expect_alert_and_panic(
            fixture.connector.subscribe(&topics),
            &fixture.alert,
            ExitReason::ExchangeUnreachable,
        )
        .await;
        assert!(
            alert.starts_with("Market Data Provider Cannot Connect to Binance FuturesPublic"),
            "{alert}"
        );
        assert!(alert.contains("refused"), "{alert}");
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_request_fails_and_rolls_back_the_batch() {
        let mut fixture = BinanceFixture::new(false);
        fixture.subscribe(&[perpetual_bbo()]).await;
        let mut futures = fixture.expect_futures();
        expect_request(&mut futures, SUBSCRIBE, &["btcusdt@bookTicker"]);
        fixture
            .venue(BinanceEndpoint::SpotSbe)
            .respond_with(refuse_requests());

        let error = fixture
            .connector
            .subscribe(&HashSet::from([spot_bbo(), ether_perpetual_bbo()]))
            .await
            .expect_err("a rejected request is expected to fail the batch");
        assert!(
            format!("{error:#}").contains("Invalid request"),
            "{error:#}"
        );
        let mut spot = fixture.expect_spot();
        expect_request(&mut spot, SUBSCRIBE, &["btcusdt@bestBidAsk"]);
        assert_eq!(normal_close(), spot.expect_received());
        fixture.expect_connected_endpoints(&[BinanceEndpoint::FuturesPublic]);
        futures.expect_nothing_received();
        assert!(
            fixture
                .connector
                .endpoint_state(BinanceEndpoint::SpotSbe)
                .restore_plan
                .is_none()
        );

        fixture
            .venue(BinanceEndpoint::SpotSbe)
            .respond_with(answer(json!(null)));
        fixture
            .venue(BinanceEndpoint::FuturesPublic)
            .respond_with(refuse_requests());
        fixture
            .connector
            .subscribe(&HashSet::from([spot_bbo(), ether_perpetual_bbo()]))
            .await
            .expect_err("a rejected request is expected to fail the batch");
        let mut spot = fixture.expect_spot();
        expect_request(&mut spot, SUBSCRIBE, &["btcusdt@bestBidAsk"]);
        expect_request(&mut futures, SUBSCRIBE, &["ethusdt@bookTicker"]);
        assert_eq!(normal_close(), spot.expect_received(), "rolled back");
        assert_eq!(normal_close(), futures.expect_received());
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_request_times_out() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .venue(BinanceEndpoint::FuturesPublic)
            .respond_with(Arc::new(|_: &Message| None));
        let started = Instant::now();
        let error = fixture
            .connector
            .subscribe(&HashSet::from([perpetual_bbo()]))
            .await
            .expect_err("an unanswered request is expected to fail");
        assert!(
            format!("{error:#}").contains("no answer to SUBSCRIBE"),
            "{error:#}"
        );
        assert!(started.elapsed() >= RESPONSE_TIMEOUT);
        fixture.expect_connected_endpoints(&[]);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_unsubscribe_reconnects_with_the_remaining_streams() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .subscribe(&[perpetual_bbo(), ether_perpetual_bbo()])
            .await;
        let mut connection = fixture.expect_futures();
        expect_request(
            &mut connection,
            SUBSCRIBE,
            &["btcusdt@bookTicker", "ethusdt@bookTicker"],
        );
        fixture
            .venue(BinanceEndpoint::FuturesPublic)
            .respond_with(refuse_requests());
        fixture
            .connector
            .unsubscribe(&HashSet::from([ether_perpetual_bbo()]))
            .await
            .expect_err("a rejected unsubscribe is expected to fail");
        expect_request(&mut connection, UNSUBSCRIBE, &["ethusdt@bookTicker"]);
        assert_eq!(normal_close(), connection.expect_received());

        fixture
            .venue(BinanceEndpoint::FuturesPublic)
            .respond_with(answer(json!(null)));
        fixture.run_for(FIRST_RETRY_DELAY + MILLISECOND).await;
        let mut restored = fixture.expect_futures();
        expect_request(&mut restored, SUBSCRIBE, &["btcusdt@bookTicker"]);
    }

    #[tokio::test(start_paused = true)]
    async fn json_events_become_market_data_of_their_topic() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .subscribe(&[ether_perpetual_bbo(), perpetual_trade()])
            .await;
        let futures = fixture.expect_futures();
        futures.send(book_ticker("ethusdt@bookTicker", 7));
        futures.send(book_ticker("bnbusdt@bookTicker", 8));
        futures.send(Message::text(
            "{\"stream\":\"ethusdt@bookTicker\",\"data\":{}}",
        ));
        futures.send(Message::text("not json"));
        futures.send(Message::text("{\"result\":null,\"id\":99}"));
        futures.send(Message::text(
            "{\"error\":{\"code\":3,\"msg\":\"Invalid JSON\"}}",
        ));
        futures.send(Message::text(
            json!({"stream": "btcusdt@trade", "data": {"e": "trade", "T": 1_792_881_786_559_u64,
                "t": 9, "p": "80570.70", "q": "0.0015", "X": "MARKET", "m": false}})
            .to_string(),
        ));
        futures.send(Message::text(
            json!({"stream": "btcusdt@trade", "data": {"e": "trade", "T": 1_792_881_786_559_u64,
                "t": 10, "p": "80570.70", "q": "0.0015", "X": "ADL", "m": false}})
            .to_string(),
        ));

        let produced = fixture.run_for(MILLISECOND).await;
        assert_eq!(
            vec![ether_perpetual_bbo(), perpetual_trade()],
            topics_of(&produced)
        );
        let bbo = expect_bbo(&produced[0]);
        assert_eq!(7, bbo.sequence);
        assert_eq!(
            NOW_NANOSECONDS,
            bbo.latency_metric.provider_receive_time_nanoseconds
        );
        assert_eq!(
            NonZeroU64::new(1_792_881_786_559_000_000),
            bbo.exchange_time_nanoseconds
        );
        let trade = expect_trade(&produced[1]);
        assert_eq!(Side::Buy, trade.taker_side);
        assert_eq!(dec!(80570.70), trade.price);
        assert_eq!(9, trade.sequence);
    }

    #[tokio::test(start_paused = true)]
    async fn sbe_frames_become_bbo_and_one_trade_per_fill() {
        let mut fixture = BinanceFixture::new(true);
        fixture.subscribe(&[spot_bbo(), spot_trade()]).await;
        let connection = fixture.expect_spot();
        let fill = |id| EncodedTrade {
            id,
            price_mantissa: 8_058_630_000_000,
            size_mantissa: 150_000,
            buyer_is_maker: true,
        };
        connection.send(Message::binary(encode_best_bid_ask(
            "BTCUSDT",
            1_792_881_786_559_123,
            5,
        )));
        connection.send(Message::binary(encode_trades(
            "BTCUSDT",
            1_792_881_786_559_123,
            &[fill(1), fill(2)],
        )));
        connection.send(Message::binary(encode_trades("ETHUSDT", 1, &[fill(3)])));
        connection.send(Message::binary(encode_best_bid_ask("ETHUSDT", 1, 6)));
        connection.send(Message::binary(vec![1, 2, 3]));
        let mut other_template = encode_trades("BTCUSDT", 1, &[fill(4)]);
        other_template[2..4].copy_from_slice(&10002_u16.to_le_bytes());
        connection.send(Message::binary(other_template));
        connection.send(Message::binary(encode_trades("BTCUSDT", 1, &[fill(-1)])));

        let produced = fixture.run_for(MILLISECOND).await;
        assert_eq!(
            vec![spot_bbo(), spot_trade(), spot_trade()],
            topics_of(&produced)
        );
        let bbo = expect_bbo(&produced[0]);
        assert_eq!(5, bbo.sequence);
        assert_eq!(dec!(2343.54), bbo.bid_price);
        assert_eq!(dec!(96.2171), bbo.ask_size);
        assert_eq!(
            NonZeroU64::new(1_792_881_786_559_123_000),
            bbo.exchange_time_nanoseconds
        );
        assert_eq!(
            NOW_NANOSECONDS,
            bbo.latency_metric.provider_receive_time_nanoseconds
        );
        for (market_data, sequence) in produced[1..].iter().zip([1, 2]) {
            let trade = expect_trade(market_data);
            assert_eq!(sequence, trade.sequence);
            assert_eq!(Side::Sell, trade.taker_side);
            assert_eq!(dec!(80586.3), trade.price);
            assert_eq!(dec!(0.0015), trade.size);
            assert_eq!(
                NonZeroU64::new(1_792_881_786_559_123_000),
                trade.exchange_time_nanoseconds
            );
            assert_eq!(
                NOW_NANOSECONDS,
                trade.latency_metric.provider_receive_time_nanoseconds
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn events_arriving_before_the_answer_are_kept() {
        let mut fixture = BinanceFixture::new(false);
        fixture.subscribe(&[perpetual_bbo()]).await;
        let futures = fixture.expect_futures();
        futures.send(book_ticker("btcusdt@bookTicker", 1));
        fixture.subscribe(&[ether_perpetual_bbo()]).await;

        let produced = fixture.run_for(MILLISECOND).await;
        assert_eq!(vec![perpetual_bbo()], topics_of(&produced));
    }

    #[tokio::test(start_paused = true)]
    async fn venue_ping_is_answered() {
        let mut fixture = BinanceFixture::new(false);
        fixture.subscribe(&[perpetual_bbo()]).await;
        let mut connection = fixture.expect_futures();
        expect_request(&mut connection, SUBSCRIBE, &["btcusdt@bookTicker"]);
        connection.send(Message::Ping(Bytes::from_static(b"venue")));

        assert!(fixture.run_for(MILLISECOND).await.is_empty());
        assert_eq!(
            Message::Pong(Bytes::from_static(b"venue")),
            connection.expect_received()
        );
        fixture.expect_connected_endpoints(&[BinanceEndpoint::FuturesPublic]);
    }

    #[tokio::test(start_paused = true)]
    async fn lost_connection_is_restored_with_every_stream() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .subscribe(&[perpetual_bbo(), ether_perpetual_bbo(), spot_bbo()])
            .await;
        let mut spot = fixture.expect_spot();
        expect_request(&mut spot, SUBSCRIBE, &["btcusdt@bestBidAsk"]);
        let futures = fixture.expect_futures();
        futures.break_read();

        assert!(fixture.run_for(MILLISECOND).await.is_empty());
        let mut restored = fixture.expect_futures();
        expect_request(
            &mut restored,
            SUBSCRIBE,
            &["btcusdt@bookTicker", "ethusdt@bookTicker"],
        );
        spot.expect_nothing_received();
        restored.send(book_ticker("btcusdt@bookTicker", 2));
        let produced = fixture.run_for(MILLISECOND).await;
        assert_eq!(vec![perpetual_bbo()], topics_of(&produced));
        fixture.expect_phanes_asked_for(&[sbe_gnosis_id(false)]);
    }

    #[tokio::test(start_paused = true)]
    async fn restore_backs_off_and_alerts_after_repeated_failures() {
        let mut fixture = BinanceFixture::new(false);
        fixture.subscribe(&[perpetual_bbo()]).await;
        let futures = fixture.expect_futures();
        fixture
            .venue(BinanceEndpoint::FuturesPublic)
            .refuse_connections();
        futures.break_read();

        let mut until_alert = Duration::ZERO;
        for failures in 1..RESTORE_FAILURES_BEFORE_ALERT {
            until_alert += retry_delay(failures);
        }
        fixture.run_for(until_alert - MILLISECOND).await;
        assert!(fixture.alert.alerts().is_empty());
        fixture.run_for(2 * MILLISECOND).await;
        let alerts = fixture.alert.alerts();
        assert_eq!(1, alerts.len());
        assert!(alerts[0].contains("FuturesPublic"), "{alerts:?}");
        assert!(alerts[0].contains("refused"), "{alerts:?}");

        fixture
            .venue(BinanceEndpoint::FuturesPublic)
            .accept_connections();
        fixture
            .run_for(retry_delay(RESTORE_FAILURES_BEFORE_ALERT) + MILLISECOND)
            .await;
        let mut restored = fixture.expect_futures();
        expect_request(&mut restored, SUBSCRIBE, &["btcusdt@bookTicker"]);
        assert!(
            fixture
                .connector
                .endpoint_state(BinanceEndpoint::FuturesPublic)
                .restore_plan
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn silent_connection_is_pinged_then_restored() {
        let mut fixture = BinanceFixture::new(false);
        let heartbeat = HEARTBEAT;
        fixture.subscribe(&[spot_bbo()]).await;
        let mut connection = fixture.expect_spot();
        expect_request(&mut connection, SUBSCRIBE, &["btcusdt@bestBidAsk"]);

        fixture
            .run_for(heartbeat.ping_after_idle - MILLISECOND)
            .await;
        connection.expect_nothing_received();
        fixture.run_for(2 * MILLISECOND).await;
        assert_eq!(Message::Ping(Bytes::new()), connection.expect_received());

        let until_dead = heartbeat.dead_after_silence - heartbeat.ping_after_idle;
        fixture.run_for(until_dead).await;
        assert_eq!(Message::Ping(Bytes::new()), connection.expect_received());
        let mut restored = fixture.expect_spot();
        expect_request(&mut restored, SUBSCRIBE, &["btcusdt@bestBidAsk"]);
        fixture.expect_connected_endpoints(&[BinanceEndpoint::SpotSbe]);
    }

    #[tokio::test(start_paused = true)]
    async fn venue_close_is_echoed_and_the_connection_restored() {
        let mut fixture = BinanceFixture::new(false);
        fixture.subscribe(&[spot_bbo(), perpetual_bbo()]).await;
        let mut spot = fixture.expect_spot();
        expect_request(&mut spot, SUBSCRIBE, &["btcusdt@bestBidAsk"]);
        let mut futures = fixture.expect_futures();
        expect_request(&mut futures, SUBSCRIBE, &["btcusdt@bookTicker"]);
        let frame_option = Some(CloseFrame {
            code: CloseCode::Away,
            reason: Utf8Bytes::from_static("server shutdown"),
        });
        futures.send(Message::Close(frame_option.clone()));

        fixture.run_for(MILLISECOND).await;
        assert_eq!(Message::Close(frame_option), futures.expect_received());
        spot.expect_nothing_received();
        fixture.expect_futures();
        fixture.expect_connected_endpoints(&[
            BinanceEndpoint::SpotSbe,
            BinanceEndpoint::FuturesPublic,
        ]);
    }

    #[tokio::test(start_paused = true)]
    async fn server_shutdown_forces_reconnect() {
        let mut fixture = BinanceFixture::new(false);
        fixture.subscribe(&[perpetual_bbo()]).await;
        let futures = fixture.expect_futures();
        futures.send(Message::text(r#"{"e":"serverShutdown"}"#));

        fixture.run_for(MILLISECOND).await;
        let mut restored = fixture.expect_futures();
        expect_request(&mut restored, SUBSCRIBE, &["btcusdt@bookTicker"]);
    }

    #[tokio::test(start_paused = true)]
    async fn disconnect_closes_every_connection_for_good() {
        let mut fixture = BinanceFixture::new(false);
        fixture
            .subscribe(&[spot_bbo(), spot_trade(), perpetual_bbo()])
            .await;
        let mut connections = vec![fixture.expect_spot(), fixture.expect_futures()];
        fixture.expect_no_new_connection();

        fixture.connector.disconnect().await;
        fixture.expect_connected_endpoints(&[]);
        for connection in &mut connections {
            connection.expect_received();
            assert_eq!(normal_close(), connection.expect_received());
        }
        assert!(fixture.run_for(Duration::from_secs(60)).await.is_empty());
        fixture.expect_no_new_connection();
    }

    #[test]
    fn stream_descriptions_tell_spot_and_perpetual_apart() {
        let topics = HashSet::from([spot_bbo(), spot_trade(), perpetual_bbo()]);
        assert_eq!(
            "SpotSbe [btcusdt@bestBidAsk, btcusdt@trade], FuturesPublic [btcusdt@bookTicker]",
            describe_streams(&group_streams_by_endpoint(&topics))
        );
    }

    /// Reaches the real venue, so run it by hand: `cargo test live_futures_endpoints -- --ignored`.
    /// Spot needs an SBE key from Phanes, which the long-running local setup covers instead.
    #[tokio::test]
    #[ignore = "connects to Binance"]
    async fn live_futures_endpoints_deliver_bbo_and_trades() {
        for demo in [false, true] {
            let context = test_connector_context();
            let mut connector = BinanceConnector::new(
                demo,
                |_| TungsteniteClient::default(),
                context.phanes_client,
                context.notifier,
                RealClock,
            );
            let topics = HashSet::from([perpetual_bbo(), perpetual_trade()]);
            connector
                .subscribe(&topics)
                .await
                .expect("Binance is expected to accept the subscriptions");
            let mut received = HashSet::new();
            timeout(Duration::from_secs(60), async {
                while received.len() < topics.len() {
                    received.insert(connector.next_event().await.topic());
                }
            })
            .await
            .expect("every topic is expected to deliver within a minute");
            connector.disconnect().await;
        }
    }
}

//! Binance spot order book snapshots: the REST call that starts a local book.

use std::future::Future;

use anyhow::{Context, Result};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::exchanges::binance::endpoint::{SPOT_DEPTH_DEMO, SPOT_DEPTH_LIVE};
use crate::exchanges::binance::venue_stream::venue_symbol;
use crate::order_book_state::OrderBookSnapshot;
use layline::http::REQUEST_TIMEOUT;
use layline::time::get_current_timestamp_nanoseconds;
use layline::types::{Instrument, PriceLevel};

const SNAPSHOT_LIMIT: u16 = 50;

pub(crate) trait DepthSnapshotSource: Clone + Send + 'static {
    fn fetch(
        &self,
        instrument: Instrument,
    ) -> impl Future<Output = Result<OrderBookSnapshot>> + Send;
}

#[derive(Clone)]
pub(crate) struct HttpDepthSnapshots {
    client: Client,
    demo: bool,
}

#[derive(Deserialize)]
struct SnapshotBody {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

impl HttpDepthSnapshots {
    pub(crate) fn new(demo: bool) -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("a reqwest client with a timeout always builds");
        Self { client, demo }
    }
}

impl DepthSnapshotSource for HttpDepthSnapshots {
    fn fetch(
        &self,
        instrument: Instrument,
    ) -> impl Future<Output = Result<OrderBookSnapshot>> + Send {
        let client = self.client.clone();
        let url = if self.demo {
            SPOT_DEPTH_DEMO
        } else {
            SPOT_DEPTH_LIVE
        };
        async move {
            let symbol = venue_symbol(instrument);
            let url = format!("{url}?symbol={symbol}&limit={SNAPSHOT_LIMIT}");
            let body: SnapshotBody = client
                .get(url)
                .send()
                .await
                .context("failed to request a spot depth snapshot")?
                .error_for_status()
                .context("spot depth snapshot returned an error status")?
                .json()
                .await
                .context("failed to parse a spot depth snapshot")?;
            Ok(OrderBookSnapshot {
                sequence: body.last_update_id,
                exchange_time_nanoseconds: None,
                received_time_nanoseconds: get_current_timestamp_nanoseconds(),
                bids: body.bids.into_iter().map(PriceLevel::from).collect(),
                asks: body.asks.into_iter().map(PriceLevel::from).collect(),
            })
        }
    }
}

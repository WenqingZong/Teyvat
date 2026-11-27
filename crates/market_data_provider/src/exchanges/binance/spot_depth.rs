//! Binance spot order books: one REST snapshot plus the SBE diff stream, merged locally.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::num::NonZeroU64;

use anyhow::{Context, Result};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::exchanges::binance::endpoint::{SPOT_DEPTH_DEMO, SPOT_DEPTH_LIVE};
use crate::exchanges::binance::venue_stream::venue_symbol;
use layline::http::REQUEST_TIMEOUT;
use layline::time::get_current_timestamp_nanoseconds;
use layline::types::{Instrument, LatencyMetric, ORDER_BOOK_DEPTH, OrderBook, PriceLevel};

pub(super) const SNAPSHOT_LIMIT: u16 = 20;
/// A snapshot normally takes a few hundred milliseconds; this many updates means something is wrong.
const MAX_BUFFERED_UPDATES: usize = 1024;

/// One spot book: a 20-level REST snapshot maintained by the SBE diff stream.
pub(super) struct SpotOrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_update_id: u64,
    synced: bool,
    buffered: VecDeque<DepthUpdate>,
    snapshot_in_flight: bool,
    failures: u32,
}

/// One diff frame, parsed into owned levels so it can wait for the snapshot.
#[derive(Clone)]
pub(super) struct DepthUpdate {
    pub(super) first_update_id: u64,
    pub(super) last_update_id: u64,
    pub(super) exchange_time_nanoseconds: NonZeroU64,
    pub(super) received_time_nanoseconds: NonZeroU64,
    pub(super) bids: Vec<(Decimal, Decimal)>,
    pub(super) asks: Vec<(Decimal, Decimal)>,
}

/// A REST depth snapshot. It has no exchange time, so the receive time is all the provider can stamp.
#[derive(Clone, Debug)]
pub(crate) struct DepthSnapshot {
    pub(super) last_update_id: u64,
    pub(super) bids: Vec<(Decimal, Decimal)>,
    pub(super) asks: Vec<(Decimal, Decimal)>,
    pub(super) received_time_nanoseconds: NonZeroU64,
}

pub(super) enum UpdateOutcome {
    Published {
        exchange_time_nanoseconds: NonZeroU64,
        received_time_nanoseconds: NonZeroU64,
    },
    Buffered,
    /// The book has a gap or its buffer overflowed; fetch a fresh snapshot.
    NeedsSnapshot,
    /// Older than what the book already applied.
    Ignored,
}

pub(crate) trait DepthSnapshotSource: Clone + Send + 'static {
    fn fetch(&self, instrument: Instrument) -> impl Future<Output = Result<DepthSnapshot>> + Send;
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
    fn fetch(&self, instrument: Instrument) -> impl Future<Output = Result<DepthSnapshot>> + Send {
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
            Ok(DepthSnapshot {
                last_update_id: body.last_update_id,
                bids: body.bids,
                asks: body.asks,
                received_time_nanoseconds: get_current_timestamp_nanoseconds(),
            })
        }
    }
}

impl SpotOrderBook {
    pub(super) fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            synced: false,
            buffered: VecDeque::new(),
            snapshot_in_flight: false,
            failures: 0,
        }
    }

    pub(super) fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_id = 0;
        self.synced = false;
        self.buffered.clear();
    }

    pub(super) fn snapshot_in_flight(&self) -> bool {
        self.snapshot_in_flight
    }

    // AI TODO: 这两个方法修改了self.snapshot_in_flight的值, 命名方法应该写明set_xxx
    pub(super) fn snapshot_started(&mut self) {
        self.snapshot_in_flight = true;
    }

    pub(super) fn snapshot_finished(&mut self) {
        self.snapshot_in_flight = false;
    }

    pub(super) fn reset_failures(&mut self) {
        self.failures = 0;
    }

    pub(super) fn record_failure(&mut self) -> u32 {
        self.failures += 1;
        self.failures
    }

    /// A snapshot is usable only if the buffered updates overlap its `lastUpdateId` and then continue
    /// without a gap. Returns the ready book, or a reason to fetch another snapshot.
    pub(super) fn apply_snapshot(
        &mut self,
        instrument: Instrument,
        snapshot: DepthSnapshot,
    ) -> Result<OrderBook, &'static str> {
        self.bids.clear();
        self.asks.clear();
        insert_levels(&mut self.bids, &snapshot.bids);
        insert_levels(&mut self.asks, &snapshot.asks);
        self.last_update_id = snapshot.last_update_id;
        self.synced = false;

        let mut last_applied_option = None;
        while let Some(update) = self.buffered.front().cloned() {
            if update.last_update_id <= self.last_update_id {
                self.buffered.pop_front();
                continue;
            }
            if update.first_update_id > self.last_update_id + 1 {
                return Err("the snapshot is older than the buffered updates");
            }
            apply_levels(&mut self.bids, &update.bids);
            apply_levels(&mut self.asks, &update.asks);
            self.last_update_id = update.last_update_id;
            last_applied_option = Some(update);
            self.buffered.pop_front();
        }

        self.synced = true;
        self.buffered.clear();
        Ok(match last_applied_option {
            Some(update) => self.order_book(
                instrument,
                Some(update.exchange_time_nanoseconds),
                LatencyMetric::received(update.received_time_nanoseconds),
            ),
            None => self.order_book(
                instrument,
                None,
                LatencyMetric::received(snapshot.received_time_nanoseconds),
            ),
        })
    }

    /// Applies a live diff. While syncing it only buffers; a ready book publishes a new top ten.
    pub(super) fn apply_update(&mut self, update: DepthUpdate) -> UpdateOutcome {
        if !self.synced {
            if self.buffered.len() >= MAX_BUFFERED_UPDATES {
                self.buffered.clear();
                return UpdateOutcome::NeedsSnapshot;
            }
            self.buffered.push_back(update);
            return UpdateOutcome::Buffered;
        }
        if update.last_update_id <= self.last_update_id {
            return UpdateOutcome::Ignored;
        }
        if update.first_update_id > self.last_update_id + 1 {
            self.synced = false;
            self.buffered.clear();
            self.bids.clear();
            self.asks.clear();
            self.buffered.push_back(update);
            return UpdateOutcome::NeedsSnapshot;
        }
        apply_levels(&mut self.bids, &update.bids);
        apply_levels(&mut self.asks, &update.asks);
        self.last_update_id = update.last_update_id;
        UpdateOutcome::Published {
            exchange_time_nanoseconds: update.exchange_time_nanoseconds,
            received_time_nanoseconds: update.received_time_nanoseconds,
        }
    }

    pub(super) fn order_book(
        &self,
        instrument: Instrument,
        exchange_time_nanoseconds: Option<NonZeroU64>,
        latency_metric: LatencyMetric,
    ) -> OrderBook {
        let mut bid_levels = [PriceLevel::EMPTY; ORDER_BOOK_DEPTH];
        let mut ask_levels = [PriceLevel::EMPTY; ORDER_BOOK_DEPTH];
        let bid_count = fill_levels(&mut bid_levels, self.bids.iter().rev());
        let ask_count = fill_levels(&mut ask_levels, self.asks.iter());
        OrderBook {
            instrument,
            exchange_time_nanoseconds,
            latency_metric,
            bid_levels,
            ask_levels,
            bid_count,
            ask_count,
            sequence: self.last_update_id as usize,
        }
    }
}

fn fill_levels<'a>(
    destination: &mut [PriceLevel; ORDER_BOOK_DEPTH],
    levels: impl Iterator<Item = (&'a Decimal, &'a Decimal)>,
) -> u8 {
    let mut count = 0;
    for (slot, (price, size)) in destination.iter_mut().zip(levels) {
        *slot = PriceLevel {
            price: *price,
            size: *size,
        };
        count += 1;
    }
    count
}

fn insert_levels(book: &mut BTreeMap<Decimal, Decimal>, levels: &[(Decimal, Decimal)]) {
    for (price, size) in levels {
        if !size.is_zero() {
            book.insert(*price, *size);
        }
    }
}

fn apply_levels(book: &mut BTreeMap<Decimal, Decimal>, levels: &[(Decimal, Decimal)]) {
    for (price, size) in levels {
        if size.is_zero() {
            book.remove(price);
        } else {
            book.insert(*price, *size);
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;
    use layline::testing::test_instrument;
    use layline::types::{Currency, Exchange, UnderlyingType};

    fn instrument() -> Instrument {
        test_instrument(
            Exchange::Binance,
            Currency::Btc,
            Currency::Usdt,
            UnderlyingType::Spot,
        )
    }

    fn snapshot(
        last_update_id: u64,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
    ) -> DepthSnapshot {
        DepthSnapshot {
            last_update_id,
            bids: bids.to_vec(),
            asks: asks.to_vec(),
            received_time_nanoseconds: NonZeroU64::new(1).expect("not zero"),
        }
    }

    fn update(
        first: u64,
        last: u64,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
    ) -> DepthUpdate {
        DepthUpdate {
            first_update_id: first,
            last_update_id: last,
            exchange_time_nanoseconds: NonZeroU64::new(2).expect("not zero"),
            received_time_nanoseconds: NonZeroU64::new(3).expect("not zero"),
            bids: bids.to_vec(),
            asks: asks.to_vec(),
        }
    }

    #[test]
    fn snapshot_and_overlapping_updates_become_a_book() {
        let mut book = SpotOrderBook::new();
        assert!(matches!(
            book.apply_update(update(2, 3, &[], &[(dec!(101), dec!(1))])),
            UpdateOutcome::Buffered
        ));
        let order_book = book
            .apply_snapshot(
                instrument(),
                snapshot(2, &[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))]),
            )
            .expect("the snapshot overlaps");
        assert_eq!(1, order_book.bids().len());
        assert_eq!(1, order_book.asks().len());
        assert_eq!(3, order_book.sequence);
    }

    #[test]
    fn a_gap_makes_the_book_ask_for_a_new_snapshot() {
        let mut book = SpotOrderBook::new();
        book.apply_snapshot(
            instrument(),
            snapshot(5, &[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))]),
        )
        .expect("the snapshot is ready");
        assert!(matches!(
            book.apply_update(update(7, 8, &[], &[])),
            UpdateOutcome::NeedsSnapshot
        ));
    }

    #[test]
    fn zero_quantity_removes_a_level() {
        let mut book = SpotOrderBook::new();
        book.apply_snapshot(
            instrument(),
            snapshot(1, &[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))]),
        )
        .expect("the snapshot is ready");
        let outcome = book.apply_update(update(2, 2, &[(dec!(100), dec!(0))], &[]));
        let UpdateOutcome::Published {
            exchange_time_nanoseconds,
            received_time_nanoseconds,
        } = outcome
        else {
            panic!("the update is expected to publish");
        };
        let order_book = book.order_book(
            instrument(),
            Some(exchange_time_nanoseconds),
            LatencyMetric::received(received_time_nanoseconds),
        );
        assert!(order_book.bids().is_empty());
    }

    #[test]
    fn the_ten_best_levels_are_published() {
        let mut book = SpotOrderBook::new();
        let bids: Vec<_> = (0..20)
            .map(|index| (dec!(100) - Decimal::from(index), dec!(1)))
            .collect();
        let asks: Vec<_> = (0..20)
            .map(|index| (dec!(101) + Decimal::from(index), dec!(1)))
            .collect();
        let order_book = book
            .apply_snapshot(instrument(), snapshot(1, &bids, &asks))
            .expect("the snapshot is ready");
        assert_eq!(10, order_book.bids().len());
        assert_eq!(10, order_book.asks().len());
        assert_eq!(dec!(100), order_book.bids()[0].price);
        assert_eq!(dec!(91), order_book.bids()[9].price);
        assert_eq!(dec!(101), order_book.asks()[0].price);
        assert_eq!(dec!(110), order_book.asks()[9].price);
    }

    #[test]
    fn a_snapshot_older_than_the_buffered_updates_is_refused() {
        let mut book = SpotOrderBook::new();
        book.apply_update(update(10, 11, &[], &[]));
        assert!(
            book.apply_snapshot(instrument(), snapshot(5, &[], &[]))
                .is_err()
        );
    }
}

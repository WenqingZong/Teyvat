//! One venue's local order book: a snapshot plus the diffs that follow it.
//!
//! Exchange-agnostic: a connector feeds it its venue's snapshots and diffs and gets back the top
//! `ORDER_BOOK_DEPTH` levels to publish. Nothing here knows a venue's wire format.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU64;

use rust_decimal::Decimal;
use thiserror::Error;

use layline::types::{Instrument, LatencyMetric, ORDER_BOOK_DEPTH, OrderBook, PriceLevel};

/// A snapshot normally takes a few hundred milliseconds; this many updates means something is wrong.
const MAX_BUFFERED_UPDATES: usize = 1024;

/// A full book from the venue. Applying it resets the local book to these levels.
#[derive(Clone, Debug)]
pub(crate) struct OrderBookSnapshot {
    /// The venue's sequence at the snapshot; updates up to it are already included.
    pub(crate) sequence: u64,
    /// The venue's time for the snapshot, if it has one; REST snapshots usually do not.
    pub(crate) exchange_time_nanoseconds: Option<NonZeroU64>,
    /// When the connector received the snapshot.
    pub(crate) received_time_nanoseconds: NonZeroU64,
    pub(crate) bids: Vec<PriceLevel>,
    pub(crate) asks: Vec<PriceLevel>,
}

/// A diff that changes the book over `first_sequence` through `last_sequence`, inclusive.
#[derive(Clone, Debug)]
pub(crate) struct OrderBookUpdate {
    pub(crate) first_sequence: u64,
    pub(crate) last_sequence: u64,
    pub(crate) exchange_time_nanoseconds: Option<NonZeroU64>,
    pub(crate) received_time_nanoseconds: NonZeroU64,
    pub(crate) bids: Vec<PriceLevel>,
    pub(crate) asks: Vec<PriceLevel>,
}

/// Why a snapshot cannot be applied; the caller should fetch a newer one.
#[derive(Debug, Eq, Error, PartialEq)]
pub(crate) enum OrderBookError {
    #[error(
        "the snapshot ends at sequence {snapshot_sequence}, before the buffered update starting at {first_sequence}"
    )]
    StaleSnapshot {
        snapshot_sequence: u64,
        first_sequence: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrderBookUpdateOutcome {
    Published {
        exchange_time_nanoseconds: Option<NonZeroU64>,
        received_time_nanoseconds: NonZeroU64,
    },
    Buffered,
    /// The book has a gap or its buffer overflowed; fetch a fresh snapshot.
    NeedsSnapshot,
    /// Older than what the book already applied.
    Ignored,
}

/// The full-depth local book behind a published [`OrderBook`]: a snapshot plus the diffs after it.
#[derive(Debug)]
pub(crate) struct OrderBookState {
    instrument: Instrument,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_sequence: u64,
    synced: bool,
    buffered: VecDeque<OrderBookUpdate>,
}

impl OrderBookState {
    pub(crate) fn new(instrument: Instrument) -> Self {
        Self {
            instrument,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_sequence: 0,
            synced: false,
            buffered: VecDeque::new(),
        }
    }

    /// Empties the book so the next snapshot starts a new sync.
    pub(crate) fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_sequence = 0;
        self.synced = false;
        self.buffered.clear();
    }

    /// Replaces the book with `snapshot`, then applies every buffered update that follows it. A
    /// snapshot is usable only if the updates overlap its sequence and continue without a gap; one
    /// that does not connect leaves the state untouched so the caller can try a newer snapshot.
    pub(crate) fn apply_snapshot(
        &mut self,
        snapshot: OrderBookSnapshot,
    ) -> Result<OrderBook, OrderBookError> {
        let mut expected_sequence = snapshot.sequence;
        for update in &self.buffered {
            if update.last_sequence <= expected_sequence {
                continue;
            }
            if update.first_sequence > expected_sequence + 1 {
                return Err(OrderBookError::StaleSnapshot {
                    snapshot_sequence: snapshot.sequence,
                    first_sequence: update.first_sequence,
                });
            }
            expected_sequence = update.last_sequence;
        }

        self.bids.clear();
        self.asks.clear();
        insert_levels(&mut self.bids, &snapshot.bids);
        insert_levels(&mut self.asks, &snapshot.asks);
        self.last_sequence = snapshot.sequence;
        self.synced = false;

        let mut last_update_option = None;
        while let Some(update) = self.buffered.pop_front() {
            if update.last_sequence <= self.last_sequence {
                continue;
            }
            apply_levels(&mut self.bids, &update.bids);
            apply_levels(&mut self.asks, &update.asks);
            self.last_sequence = update.last_sequence;
            last_update_option = Some(update);
        }

        self.synced = true;
        Ok(match last_update_option {
            Some(update) => self.order_book(
                update.exchange_time_nanoseconds,
                LatencyMetric::received(update.received_time_nanoseconds),
            ),
            None => self.order_book(
                snapshot.exchange_time_nanoseconds,
                LatencyMetric::received(snapshot.received_time_nanoseconds),
            ),
        })
    }

    /// Applies a live diff. While syncing it only buffers; a ready book reports `Published` so
    /// the caller can build a new top ten.
    pub(crate) fn apply_update(&mut self, update: OrderBookUpdate) -> OrderBookUpdateOutcome {
        if !self.synced {
            if self.buffered.len() >= MAX_BUFFERED_UPDATES {
                self.buffered.clear();
                return OrderBookUpdateOutcome::NeedsSnapshot;
            }
            self.buffered.push_back(update);
            return OrderBookUpdateOutcome::Buffered;
        }
        if update.last_sequence <= self.last_sequence {
            return OrderBookUpdateOutcome::Ignored;
        }
        if update.first_sequence > self.last_sequence + 1 {
            self.reset();
            self.buffered.push_back(update);
            return OrderBookUpdateOutcome::NeedsSnapshot;
        }
        apply_levels(&mut self.bids, &update.bids);
        apply_levels(&mut self.asks, &update.asks);
        self.last_sequence = update.last_sequence;
        OrderBookUpdateOutcome::Published {
            exchange_time_nanoseconds: update.exchange_time_nanoseconds,
            received_time_nanoseconds: update.received_time_nanoseconds,
        }
    }

    /// The top `ORDER_BOOK_DEPTH` levels, stamped by the caller with the exchange time and latency.
    pub(crate) fn order_book(
        &self,
        exchange_time_nanoseconds: Option<NonZeroU64>,
        latency_metric: LatencyMetric,
    ) -> OrderBook {
        let mut bid_levels = [PriceLevel::EMPTY; ORDER_BOOK_DEPTH as usize];
        let mut ask_levels = [PriceLevel::EMPTY; ORDER_BOOK_DEPTH as usize];
        let bid_count = fill_levels(&mut bid_levels, self.bids.iter().rev());
        let ask_count = fill_levels(&mut ask_levels, self.asks.iter());
        OrderBook {
            instrument: self.instrument,
            exchange_time_nanoseconds,
            latency_metric,
            bid_levels,
            ask_levels,
            bid_count,
            ask_count,
            sequence: self.last_sequence as usize,
        }
    }
}

fn fill_levels<'a>(
    destination: &mut [PriceLevel; ORDER_BOOK_DEPTH as usize],
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

fn insert_levels(book: &mut BTreeMap<Decimal, Decimal>, levels: &[PriceLevel]) {
    for level in levels {
        if !level.size.is_zero() {
            book.insert(level.price, level.size);
        }
    }
}

fn apply_levels(book: &mut BTreeMap<Decimal, Decimal>, levels: &[PriceLevel]) {
    for level in levels {
        if level.size.is_zero() {
            book.remove(&level.price);
        } else {
            book.insert(level.price, level.size);
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

    fn levels(levels: &[(Decimal, Decimal)]) -> Vec<PriceLevel> {
        levels
            .iter()
            .map(|(price, size)| PriceLevel {
                price: *price,
                size: *size,
            })
            .collect()
    }

    fn snapshot(
        sequence: u64,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
    ) -> OrderBookSnapshot {
        OrderBookSnapshot {
            sequence,
            exchange_time_nanoseconds: None,
            received_time_nanoseconds: NonZeroU64::new(1).expect("not zero"),
            bids: levels(bids),
            asks: levels(asks),
        }
    }

    fn update(
        first_sequence: u64,
        last_sequence: u64,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
    ) -> OrderBookUpdate {
        OrderBookUpdate {
            first_sequence,
            last_sequence,
            exchange_time_nanoseconds: Some(NonZeroU64::new(2).expect("not zero")),
            received_time_nanoseconds: NonZeroU64::new(3).expect("not zero"),
            bids: levels(bids),
            asks: levels(asks),
        }
    }

    #[test]
    fn snapshot_and_overlapping_updates_become_a_book() {
        let mut state = OrderBookState::new(instrument());
        assert_eq!(
            OrderBookUpdateOutcome::Buffered,
            state.apply_update(update(2, 3, &[], &[(dec!(101), dec!(1))]))
        );
        let order_book = state
            .apply_snapshot(snapshot(
                2,
                &[(dec!(100), dec!(1))],
                &[(dec!(101), dec!(1))],
            ))
            .expect("the snapshot overlaps");
        assert_eq!(1, order_book.bids().len());
        assert_eq!(1, order_book.asks().len());
        assert_eq!(3, order_book.sequence);
        assert_eq!(
            Some(NonZeroU64::new(2).expect("not zero")),
            order_book.exchange_time_nanoseconds
        );
    }

    #[test]
    fn a_gap_makes_the_book_ask_for_a_new_snapshot() {
        let mut state = OrderBookState::new(instrument());
        state
            .apply_snapshot(snapshot(
                5,
                &[(dec!(100), dec!(1))],
                &[(dec!(101), dec!(1))],
            ))
            .expect("the snapshot is ready");
        assert_eq!(
            OrderBookUpdateOutcome::NeedsSnapshot,
            state.apply_update(update(7, 8, &[], &[]))
        );
    }

    #[test]
    fn an_update_already_in_the_snapshot_is_ignored() {
        let mut state = OrderBookState::new(instrument());
        state
            .apply_snapshot(snapshot(
                5,
                &[(dec!(100), dec!(1))],
                &[(dec!(101), dec!(1))],
            ))
            .expect("the snapshot is ready");
        assert_eq!(
            OrderBookUpdateOutcome::Ignored,
            state.apply_update(update(4, 5, &[], &[]))
        );
    }

    #[test]
    fn zero_quantity_removes_a_level() {
        let mut state = OrderBookState::new(instrument());
        state
            .apply_snapshot(snapshot(
                1,
                &[(dec!(100), dec!(1))],
                &[(dec!(101), dec!(1))],
            ))
            .expect("the snapshot is ready");
        let outcome = state.apply_update(update(2, 2, &[(dec!(100), dec!(0))], &[]));
        let OrderBookUpdateOutcome::Published {
            exchange_time_nanoseconds,
            received_time_nanoseconds,
        } = outcome
        else {
            panic!("the update is expected to publish");
        };
        let order_book = state.order_book(
            exchange_time_nanoseconds,
            LatencyMetric::received(received_time_nanoseconds),
        );
        assert!(order_book.bids().is_empty());
    }

    #[test]
    fn the_ten_best_levels_are_published() {
        let mut state = OrderBookState::new(instrument());
        let bids: Vec<_> = (0..20)
            .map(|index| (dec!(100) - Decimal::from(index), dec!(1)))
            .collect();
        let asks: Vec<_> = (0..20)
            .map(|index| (dec!(101) + Decimal::from(index), dec!(1)))
            .collect();
        let order_book = state
            .apply_snapshot(snapshot(1, &bids, &asks))
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
        let mut state = OrderBookState::new(instrument());
        state.apply_update(update(10, 11, &[], &[]));
        let error = state
            .apply_snapshot(snapshot(5, &[], &[]))
            .expect_err("the snapshot is older than the buffered updates");
        assert_eq!(
            OrderBookError::StaleSnapshot {
                snapshot_sequence: 5,
                first_sequence: 10,
            },
            error
        );
    }
}

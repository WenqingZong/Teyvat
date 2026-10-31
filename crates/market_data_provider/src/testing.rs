//! Fixtures shared by unit tests, so each module does not spell out the same instrument.
use crate::types::{
    Bbo, Currency, Exchange, Instrument, LatencyMetric, MarketDataType, Subscription, Topic, Trade,
    UnderlyingType,
};
use crate::utils::get_current_timestamp_nanoseconds;
use std::path::Path;

pub(crate) fn instrument(
    exchange: Exchange,
    base: Currency,
    quote: Currency,
    underlying_type: UnderlyingType,
) -> Instrument {
    Instrument {
        exchange,
        base,
        quote,
        underlying_type,
    }
}

pub(crate) fn topic(instrument: Instrument, market_data_type: MarketDataType) -> Topic {
    Topic {
        instrument,
        market_data_type,
    }
}

pub(crate) fn subscription(directory: &Path, topic: Topic) -> Subscription {
    Subscription {
        topic,
        path: topic.file_path(directory),
    }
}

pub(crate) fn bbo(instrument: Instrument, sequence: usize) -> Bbo {
    Bbo {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        ask_price: rust_decimal::dec!(101.5),
        ask_size: rust_decimal::dec!(2),
        bid_price: rust_decimal::dec!(101.4),
        bid_size: rust_decimal::dec!(4),
        sequence,
    }
}

pub(crate) fn trade(instrument: Instrument, sequence: usize) -> Trade {
    Trade {
        instrument,
        exchange_time_nanoseconds: None,
        latency_metric: LatencyMetric::received(get_current_timestamp_nanoseconds()),
        price: rust_decimal::dec!(101.5),
        size: rust_decimal::dec!(0.25),
        taker_side: crate::types::Side::Buy,
        sequence,
    }
}

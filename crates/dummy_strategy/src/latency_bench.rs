use std::collections::HashMap;
use std::mem::take;
use std::num::NonZeroU64;
use std::time::Duration;

use hdrhistogram::Histogram;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::info;

use crate::strategy::Strategy;
use layline::time::get_current_timestamp_nanoseconds;
use layline::types::{Bbo, LatencyMetric, MarketDataType, OrderBook, Topic, Trade};

const PERCENTILES: [f64; 4] = [0.5, 0.9, 0.99, 0.999];
const NANOSECONDS_PER_MICROSECOND: f64 = 1_000.0;
/// Enough significant figures to report microseconds to two decimals.
const HISTOGRAM_SIGNIFICANT_FIGURES: u8 = 5;

/// One window's samples of one topic, in nanoseconds.
struct LatencySamples {
    provider_nanoseconds: Histogram<u64>,
    transport_nanoseconds: Histogram<u64>,
    parse_nanoseconds: Histogram<u64>,
    internal_nanoseconds: Histogram<u64>,
}

impl Default for LatencySamples {
    fn default() -> Self {
        Self {
            provider_nanoseconds: histogram(),
            transport_nanoseconds: histogram(),
            parse_nanoseconds: histogram(),
            internal_nanoseconds: histogram(),
        }
    }
}

fn histogram() -> Histogram<u64> {
    Histogram::new(HISTOGRAM_SIGNIFICANT_FIGURES)
        .expect("five significant figures are a valid histogram")
}

struct Report {
    window: Duration,
    samples: HashMap<Topic, LatencySamples>,
}

/// Records latencies per message and hands each window to the reporter, a task off the hot path that
/// logs percentiles; the reading thread only records, so measuring does not delay the messages
/// measured.
pub struct LatencyBench {
    report_interval: Duration,
    window_start_nanoseconds: NonZeroU64,
    samples: HashMap<Topic, LatencySamples>,
    reports: UnboundedSender<Report>,
}

impl LatencyBench {
    /// Starts the reporter on `runtime`, off the hot path; it stops once the bench is dropped.
    pub fn new(report_interval: Duration, runtime: &Handle) -> Self {
        let (reports, receiver) = unbounded_channel();
        runtime.spawn(log_reports(receiver));
        Self::reporting_to(report_interval, reports)
    }

    fn reporting_to(report_interval: Duration, reports: UnboundedSender<Report>) -> Self {
        Self {
            report_interval,
            window_start_nanoseconds: get_current_timestamp_nanoseconds(),
            samples: HashMap::new(),
            reports,
        }
    }

    fn record(
        &mut self,
        topic: Topic,
        latency_metric: LatencyMetric,
        received_at_nanoseconds: NonZeroU64,
    ) {
        let handled_at_nanoseconds = get_current_timestamp_nanoseconds();
        let provider_nanoseconds = latency_metric
            .provider_write_time_nanoseconds
            .get()
            .saturating_sub(latency_metric.provider_receive_time_nanoseconds.get());
        let transport_nanoseconds = received_at_nanoseconds
            .get()
            .saturating_sub(latency_metric.provider_write_time_nanoseconds.get());
        let parse_nanoseconds = handled_at_nanoseconds
            .get()
            .saturating_sub(received_at_nanoseconds.get());
        let samples = self.samples.entry(topic).or_default();
        samples
            .provider_nanoseconds
            .record(provider_nanoseconds)
            .expect("a histogram records any u64");
        samples
            .transport_nanoseconds
            .record(transport_nanoseconds)
            .expect("a histogram records any u64");
        samples
            .parse_nanoseconds
            .record(parse_nanoseconds)
            .expect("a histogram records any u64");
        samples
            .internal_nanoseconds
            .record(provider_nanoseconds + transport_nanoseconds)
            .expect("a histogram records any u64");
        let window = Duration::from_nanos(
            handled_at_nanoseconds
                .get()
                .saturating_sub(self.window_start_nanoseconds.get()),
        );
        if window >= self.report_interval {
            let report = Report {
                window,
                samples: take(&mut self.samples),
            };
            self.reports
                .send(report)
                .expect("the reporter runs as long as the process");
            self.window_start_nanoseconds = handled_at_nanoseconds;
        }
    }
}

async fn log_reports(mut reports: UnboundedReceiver<Report>) {
    while let Some(report) = reports.recv().await {
        for line in report_lines(report) {
            info!("{line}");
        }
    }
}

/// One header, then per topic (sorted by name) and for all topics together: count and percentiles in µs.
fn report_lines(report: Report) -> Vec<String> {
    let mut topics: Vec<Topic> = report.samples.keys().copied().collect();
    topics.sort_by_key(ToString::to_string);
    let mut all = LatencySamples::default();
    let mut lines = vec![format!(
        "latency over {:.0} s in µs, p50/p90/p99/p99.9/max; internal = provider + transport",
        report.window.as_secs_f64()
    )];
    let mut samples_by_topic = report.samples;
    for topic in topics {
        let samples = samples_by_topic
            .remove(&topic)
            .expect("every sorted topic comes from the map");
        lines.push(summary_line(&topic.to_string(), &samples));
        all.provider_nanoseconds
            .add(&samples.provider_nanoseconds)
            .expect("histograms of one configuration add");
        all.transport_nanoseconds
            .add(&samples.transport_nanoseconds)
            .expect("histograms of one configuration add");
        all.parse_nanoseconds
            .add(&samples.parse_nanoseconds)
            .expect("histograms of one configuration add");
        all.internal_nanoseconds
            .add(&samples.internal_nanoseconds)
            .expect("histograms of one configuration add");
    }
    lines.push(summary_line("all topics", &all));
    lines
}

fn summary_line(name: &str, samples: &LatencySamples) -> String {
    let sample_count = samples.provider_nanoseconds.len();
    let provider_percentiles = percentiles(&samples.provider_nanoseconds);
    let transport_percentiles = percentiles(&samples.transport_nanoseconds);
    let internal_percentiles = percentiles(&samples.internal_nanoseconds);
    let parse_percentiles = percentiles(&samples.parse_nanoseconds);
    format!(
        "{name} n={sample_count} | provider {provider_percentiles} | transport {transport_percentiles} | internal {internal_percentiles} | parse {parse_percentiles}"
    )
}

/// `p50/p90/p99/p99.9/max` in microseconds; `-` when empty.
fn percentiles(samples_nanoseconds: &Histogram<u64>) -> String {
    if samples_nanoseconds.is_empty() {
        return "-".to_owned();
    }
    let mut values: Vec<String> = PERCENTILES
        .iter()
        .map(|fraction| microseconds(samples_nanoseconds.value_at_quantile(*fraction)))
        .collect();
    values.push(microseconds(samples_nanoseconds.max()));
    values.join("/")
}

fn microseconds(nanoseconds: u64) -> String {
    format!("{:.2}", nanoseconds as f64 / NANOSECONDS_PER_MICROSECOND)
}

impl Strategy for LatencyBench {
    fn on_bbo(&mut self, bbo: &Bbo, received_at_nanoseconds: NonZeroU64) {
        let topic = Topic {
            instrument: bbo.instrument,
            market_data_type: MarketDataType::Bbo,
        };
        self.record(topic, bbo.latency_metric, received_at_nanoseconds);
    }

    fn on_trade(&mut self, trade: &Trade, received_at_nanoseconds: NonZeroU64) {
        let topic = Topic {
            instrument: trade.instrument,
            market_data_type: MarketDataType::Trade,
        };
        self.record(topic, trade.latency_metric, received_at_nanoseconds);
    }

    fn on_aggregate_trade(&mut self, trade: &Trade, received_at_nanoseconds: NonZeroU64) {
        let topic = Topic {
            instrument: trade.instrument,
            market_data_type: MarketDataType::AggregateTrade,
        };
        self.record(topic, trade.latency_metric, received_at_nanoseconds);
    }

    fn on_order_book(&mut self, order_book: &OrderBook, received_at_nanoseconds: NonZeroU64) {
        let topic = Topic {
            instrument: order_book.instrument,
            market_data_type: MarketDataType::OrderBook,
        };
        self.record(topic, order_book.latency_metric, received_at_nanoseconds);
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use layline::testing::test_order_book;
    use layline::types::{Currency, Exchange, Instrument, MarketData, Side, UnderlyingType};

    fn instrument() -> Instrument {
        Instrument {
            exchange: Exchange::Binance,
            base: Currency::Btc,
            quote: Currency::Usdt,
            underlying_type: UnderlyingType::Spot,
        }
    }

    /// Written `transport_nanoseconds` before `received_at_nanoseconds`, after `provider_nanoseconds` in the provider.
    fn latency_metric(
        received_at_nanoseconds: u64,
        provider_nanoseconds: u64,
        transport_nanoseconds: u64,
    ) -> LatencyMetric {
        let write_nanoseconds = received_at_nanoseconds - transport_nanoseconds;
        LatencyMetric {
            provider_receive_time_nanoseconds: NonZeroU64::new(
                write_nanoseconds - provider_nanoseconds,
            )
            .expect("not zero"),
            provider_write_time_nanoseconds: NonZeroU64::new(write_nanoseconds).expect("not zero"),
        }
    }

    fn bbo(latency_metric: LatencyMetric) -> MarketData {
        MarketData::Bbo(Bbo {
            instrument: instrument(),
            exchange_time_nanoseconds: None,
            latency_metric,
            ask_price: Decimal::ONE,
            ask_size: Decimal::ONE,
            bid_price: Decimal::ONE,
            bid_size: Decimal::ONE,
            sequence: 1,
        })
    }

    fn trade(latency_metric: LatencyMetric) -> MarketData {
        MarketData::Trade(Trade {
            instrument: instrument(),
            exchange_time_nanoseconds: None,
            latency_metric,
            price: Decimal::ONE,
            size: Decimal::ONE,
            taker_side: Side::Buy,
            sequence: 2,
        })
    }

    fn order_book(latency_metric: LatencyMetric) -> MarketData {
        MarketData::OrderBook(OrderBook {
            latency_metric,
            ..test_order_book(instrument(), 3)
        })
    }

    fn now_nanoseconds() -> NonZeroU64 {
        get_current_timestamp_nanoseconds()
    }

    fn histogram_from(values: impl IntoIterator<Item = u64>) -> Histogram<u64> {
        let mut histogram = histogram();
        for value in values {
            histogram
                .record(value)
                .expect("a histogram records any u64");
        }
        histogram
    }

    #[test]
    fn percentiles_are_hdr_quantiles_in_microseconds() {
        let samples = histogram_from((1..=1000).rev().map(|value| value * 1_000));
        assert_eq!("500.00/900.00/990.00/999.00/1000.00", percentiles(&samples));
        assert_eq!(
            "1.50/1.50/1.50/1.50/1.50",
            percentiles(&histogram_from([1_500]))
        );
        assert_eq!("-", percentiles(&histogram()));
    }

    #[test]
    fn each_window_is_reported_per_topic_and_overall() {
        let (reports, mut receiver) = unbounded_channel();
        let mut bench = LatencyBench::reporting_to(Duration::from_secs(3600), reports);
        let now = now_nanoseconds();
        bench.on_market_data(&bbo(latency_metric(now.get(), 2_000, 5_000)), now);
        bench.on_market_data(&trade(latency_metric(now.get(), 1_000, 3_000)), now);
        bench.on_market_data(&order_book(latency_metric(now.get(), 1_000, 2_000)), now);
        assert!(receiver.try_recv().is_err(), "the window is still open");

        bench.report_interval = Duration::ZERO;
        bench.on_market_data(&trade(latency_metric(now.get(), 1_000, 3_000)), now);
        let report = receiver
            .try_recv()
            .expect("the window is expected to close");
        assert!(bench.samples.is_empty());
        let lines = report_lines(report);
        assert_eq!(5, lines.len(), "{lines:#?}");
        assert!(
            lines[1].starts_with("Binance/BTC-USDT-SPOT/BBO n=1 | provider 2.00/"),
            "{}",
            lines[1]
        );
        assert!(
            lines[1].contains("| transport 5.00/") && lines[1].contains("| internal 7.00/"),
            "{}",
            lines[1]
        );
        assert!(
            lines[2].starts_with("Binance/BTC-USDT-SPOT/ORDER_BOOK n=1 | provider 1.00/"),
            "{}",
            lines[2]
        );
        assert!(
            lines[3].starts_with("Binance/BTC-USDT-SPOT/TRADE n=2 | provider 1.00/"),
            "{}",
            lines[3]
        );
        assert!(lines[4].starts_with("all topics n=4 |"), "{}", lines[4]);
    }

    #[test]
    #[should_panic(expected = "the reporter runs as long as the process")]
    fn a_stopped_reporter_panics() {
        let (reports, receiver) = unbounded_channel();
        drop(receiver);
        let mut bench = LatencyBench::reporting_to(Duration::ZERO, reports);
        let now = now_nanoseconds();
        bench.on_market_data(&bbo(latency_metric(now.get(), 1, 1)), now);
    }

    #[tokio::test]
    async fn reporter_runs_until_the_bench_is_dropped() {
        let bench = LatencyBench::new(Duration::from_secs(60), &Handle::current());
        drop(bench);
    }
}

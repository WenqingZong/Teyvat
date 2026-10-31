use crate::strategy::Strategy;
use layline::time::get_current_timestamp_nanoseconds;
use layline::types::{Bbo, LatencyMetric, MarketDataType, Topic, Trade};
use std::collections::HashMap;
use std::mem::take;
use std::num::NonZeroU64;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::Duration;
use tracing::{info, warn};

const PERCENTILES: [f64; 4] = [0.5, 0.9, 0.99, 0.999];
const NANOSECONDS_PER_MICROSECOND: f64 = 1_000.0;

/// One window's samples of one topic, in nanoseconds.
#[derive(Default)]
struct LatencySamples {
    provider_nanoseconds: Vec<u64>,
    transport_nanoseconds: Vec<u64>,
    parse_nanoseconds: Vec<u64>,
}

struct Report {
    window: Duration,
    samples: HashMap<Topic, LatencySamples>,
}

/// Records latencies per message and hands each window to a reporter thread, which sorts and logs
/// percentiles; the reading thread only appends, so measuring does not delay the messages measured.
pub(crate) struct LatencyBench {
    report_interval: Duration,
    window_start_nanoseconds: NonZeroU64,
    samples: HashMap<Topic, LatencySamples>,
    reports: Sender<Report>,
}

impl LatencyBench {
    pub(crate) fn new(report_interval: Duration) -> Self {
        let (reports, receiver) = channel();
        thread::Builder::new()
            .name("latency-report".to_owned())
            .spawn(move || log_reports(&receiver))
            .expect("the reporter thread is expected to start");
        Self::reporting_to(report_interval, reports)
    }

    fn reporting_to(report_interval: Duration, reports: Sender<Report>) -> Self {
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
        let samples = self.samples.entry(topic).or_default();
        samples.provider_nanoseconds.push(
            latency_metric
                .provider_write_time_nanoseconds
                .get()
                .saturating_sub(latency_metric.provider_receive_time_nanoseconds.get()),
        );
        samples.transport_nanoseconds.push(
            received_at_nanoseconds
                .get()
                .saturating_sub(latency_metric.provider_write_time_nanoseconds.get()),
        );
        samples.parse_nanoseconds.push(
            handled_at_nanoseconds
                .get()
                .saturating_sub(received_at_nanoseconds.get()),
        );
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
            if self.reports.send(report).is_err() {
                warn!("the latency reporter has stopped; dropping this window");
            }
            self.window_start_nanoseconds = handled_at_nanoseconds;
        }
    }
}

fn log_reports(reports: &Receiver<Report>) {
    while let Ok(report) = reports.recv() {
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
        let mut samples = samples_by_topic
            .remove(&topic)
            .expect("every sorted topic comes from the map");
        lines.push(summary_line(&topic.to_string(), &mut samples));
        all.provider_nanoseconds
            .extend(&samples.provider_nanoseconds);
        all.transport_nanoseconds
            .extend(&samples.transport_nanoseconds);
        all.parse_nanoseconds.extend(&samples.parse_nanoseconds);
    }
    lines.push(summary_line("all topics", &mut all));
    lines
}

fn summary_line(name: &str, samples: &mut LatencySamples) -> String {
    let mut internal_nanoseconds: Vec<u64> = samples
        .provider_nanoseconds
        .iter()
        .zip(&samples.transport_nanoseconds)
        .map(|(provider, transport)| provider + transport)
        .collect();
    format!(
        "{name} n={} | provider {} | transport {} | internal {} | parse {}",
        samples.provider_nanoseconds.len(),
        percentiles(&mut samples.provider_nanoseconds),
        percentiles(&mut samples.transport_nanoseconds),
        percentiles(&mut internal_nanoseconds),
        percentiles(&mut samples.parse_nanoseconds),
    )
}

/// `p50/p90/p99/p99.9/max` in microseconds, nearest rank; `-` when empty.
fn percentiles(samples_nanoseconds: &mut [u64]) -> String {
    let Some(&max_nanoseconds) = samples_nanoseconds.iter().max() else {
        return "-".to_owned();
    };
    samples_nanoseconds.sort_unstable();
    let count = samples_nanoseconds.len();
    let mut values: Vec<String> = PERCENTILES
        .iter()
        .map(|fraction| {
            let rank = (fraction * count as f64).ceil() as usize;
            microseconds(samples_nanoseconds[rank.clamp(1, count) - 1])
        })
        .collect();
    values.push(microseconds(max_nanoseconds));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use layline::types::{Currency, Exchange, Instrument, MarketData, Side, UnderlyingType};
    use rust_decimal::Decimal;

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

    fn now_nanoseconds() -> NonZeroU64 {
        get_current_timestamp_nanoseconds()
    }

    #[test]
    fn percentiles_use_nearest_rank_in_microseconds() {
        let mut samples: Vec<u64> = (1..=1000).rev().map(|value| value * 1_000).collect();
        assert_eq!(
            "500.00/900.00/990.00/999.00/1000.00",
            percentiles(&mut samples)
        );
        assert_eq!("1.50/1.50/1.50/1.50/1.50", percentiles(&mut [1_500]));
        assert_eq!("-", percentiles(&mut []));
    }

    #[test]
    fn each_window_is_reported_per_topic_and_overall() {
        let (reports, receiver) = channel();
        let mut bench = LatencyBench::reporting_to(Duration::from_secs(3600), reports);
        let now = now_nanoseconds();
        bench.on_market_data(&bbo(latency_metric(now.get(), 2_000, 5_000)), now);
        bench.on_market_data(&trade(latency_metric(now.get(), 1_000, 3_000)), now);
        assert!(receiver.try_recv().is_err(), "the window is still open");

        bench.report_interval = Duration::ZERO;
        bench.on_market_data(&trade(latency_metric(now.get(), 1_000, 3_000)), now);
        let report = receiver
            .try_recv()
            .expect("the window is expected to close");
        assert!(bench.samples.is_empty());
        let lines = report_lines(report);
        assert_eq!(4, lines.len(), "{lines:#?}");
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
            lines[2].starts_with("Binance/BTC-USDT-SPOT/TRADE n=2 | provider 1.00/"),
            "{}",
            lines[2]
        );
        assert!(lines[3].starts_with("all topics n=3 |"), "{}", lines[3]);
    }

    #[test]
    fn a_stopped_reporter_drops_the_window() {
        let (reports, receiver) = channel();
        drop(receiver);
        let mut bench = LatencyBench::reporting_to(Duration::ZERO, reports);
        let now = now_nanoseconds();
        bench.on_market_data(&bbo(latency_metric(now.get(), 1, 1)), now);
        assert!(bench.samples.is_empty());
    }

    #[test]
    fn reporter_thread_runs_until_the_bench_is_dropped() {
        let bench = LatencyBench::new(Duration::from_secs(60));
        drop(bench);
    }
}

use std::collections::HashSet;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing::error;

use dummy_strategy::client::subscribe;
use dummy_strategy::latency_bench::LatencyBench;
use dummy_strategy::strategy::Strategy;
use layline::logging::init_tracing;
use layline::provider_api::DEFAULT_PROVIDER_ADDRESS;
use layline::shared_memory::MarketDataReader;
use layline::types::{Currency, Exchange, Instrument, MarketDataType, Topic, UnderlyingType};

const STRATEGY_NAME: &str = "dummy_strategy";

#[derive(Parser)]
#[command(
    about = "Development stand-in strategy: reads Binance BTC and ETH data and logs latency percentiles"
)]
struct Arguments {
    /// Read the venues' demo-trading data instead of live.
    #[arg(long)]
    demo: bool,
    /// How often to log latency percentiles over the samples since the last report.
    #[arg(long, default_value_t = 60)]
    report_interval_seconds: u64,
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    let _guard = init_tracing(); // 持有 WorkerGuard，确保退出前 flush
    match run(
        arguments.demo,
        Duration::from_secs(arguments.report_interval_seconds),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!("fatal: {error:?}"); // {:?} 保留完整 context 链
            ExitCode::FAILURE
        }
    }
}

fn run(demo: bool, report_interval: Duration) -> Result<()> {
    let subscriptions = subscribe(
        DEFAULT_PROVIDER_ADDRESS,
        STRATEGY_NAME.to_owned(),
        demo,
        bench_topics(),
    )?;
    let mut strategy = LatencyBench::new(report_interval);
    MarketDataReader::open(&subscriptions)?.run(|data, received_at_nanoseconds| {
        strategy.on_market_data(&data, received_at_nanoseconds)
    })
}

/// BBO and trades of BTC and ETH, spot and perpetual, on Binance.
fn bench_topics() -> HashSet<Topic> {
    let mut topics = HashSet::new();
    for base in [Currency::Btc, Currency::Eth] {
        for underlying_type in [UnderlyingType::Spot, UnderlyingType::Perpetual] {
            let instrument = Instrument {
                exchange: Exchange::Binance,
                base,
                quote: Currency::Usdt,
                underlying_type,
            };
            for market_data_type in [MarketDataType::Bbo, MarketDataType::Trade] {
                topics.insert(Topic {
                    instrument,
                    market_data_type,
                });
            }
        }
    }
    topics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_covers_eight_binance_topics() {
        let topics = bench_topics();
        assert_eq!(8, topics.len());
        assert!(
            topics
                .iter()
                .all(|topic| topic.instrument.exchange == Exchange::Binance)
        );
    }
}

//! Keeps instrument rules fresh while strategies run.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use futures::future::BoxFuture;
use tokio::time::{Instant, interval_at, sleep};
use tracing::{error, warn};

use crate::alert::SendAlert;
use crate::exchanges::fetch_instrument_rules;
use layline::backoff::exponential_backoff;
use layline::bark::Notifier;
use layline::types::{Exchange, Instrument, InstrumentRules};

const REFRESH_INTERVAL: Duration = Duration::from_hours(24);
const REFRESH_RETRY_INITIAL: Duration = Duration::from_mins(1);
const REFRESH_RETRY_MAXIMUM: Duration = Duration::from_hours(1);
const ALERT_TITLE: &str = "instrument rules";

/// Refreshes every 24 hours. A failed fetch keeps the previous map and retries with backoff; the
/// fetch itself panics when an instrument the map already knows is missing or no longer tradable.
pub(super) async fn run(
    instrument_rules: Arc<RwLock<InstrumentRules>>,
    demo: bool,
    notifier: Notifier,
) {
    let mut refresh = interval_at(Instant::now() + REFRESH_INTERVAL, REFRESH_INTERVAL);
    loop {
        refresh.tick().await;
        let instruments: HashSet<Instrument> = {
            let rules = instrument_rules
                .read()
                .expect("no code panics while holding the instrument rules");
            rules.keys().copied().collect()
        };
        if !instruments.is_empty() {
            refresh_until_success(
                &instrument_rules,
                demo,
                &instruments,
                &notifier,
                fetch_from_exchange,
            )
            .await;
        }
    }
}

type FetchInstrumentRules =
    for<'a> fn(Exchange, bool, &'a HashSet<Instrument>) -> BoxFuture<'a, Result<InstrumentRules>>;

fn fetch_from_exchange(
    exchange: Exchange,
    demo: bool,
    instruments: &HashSet<Instrument>,
) -> BoxFuture<'_, Result<InstrumentRules>> {
    Box::pin(fetch_instrument_rules(exchange, demo, instruments))
}

async fn refresh_until_success(
    instrument_rules: &RwLock<InstrumentRules>,
    demo: bool,
    instruments: &HashSet<Instrument>,
    notifier: &Notifier,
    fetch: FetchInstrumentRules,
) {
    let mut instruments_by_exchange: HashMap<Exchange, HashSet<Instrument>> = HashMap::new();
    for instrument in instruments {
        instruments_by_exchange
            .entry(instrument.exchange)
            .or_default()
            .insert(*instrument);
    }
    let mut failures = 0;
    loop {
        let mut fetched = InstrumentRules::new();
        let mut failure = None;
        for (exchange, exchange_instruments) in &instruments_by_exchange {
            match fetch(*exchange, demo, exchange_instruments).await {
                Ok(exchange_rules) => fetched.extend(exchange_rules),
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        match failure {
            None => {
                instrument_rules
                    .write()
                    .expect("no code panics while holding the instrument rules")
                    .extend(fetched);
                return;
            }
            Some(error) => {
                failures += 1;
                let reason = format!("refresh failed, keeping the cached rules: {error:#}");
                error!("{reason}");
                if failures == 1 {
                    notifier.send_alert(ALERT_TITLE, &reason).await;
                }
                let delay =
                    exponential_backoff(failures, REFRESH_RETRY_INITIAL, REFRESH_RETRY_MAXIMUM);
                warn!("retrying the instrument rules refresh in {delay:?}");
                sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::bail;
    use rust_decimal::dec;

    use super::*;
    use layline::types::{Currency, InstrumentRule, UnderlyingType};

    static FETCH_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn fake_fetch(
        _exchange: Exchange,
        _demo: bool,
        instruments: &HashSet<Instrument>,
    ) -> BoxFuture<'_, Result<InstrumentRules>> {
        Box::pin(async move {
            if FETCH_CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
                bail!("temporary failure");
            }
            let instrument = *instruments.iter().next().expect("one instrument");
            Ok(InstrumentRules::from([(
                instrument,
                InstrumentRule {
                    exchange_symbol: "BTCUSDT".to_owned(),
                    price_step: dec!(0.01),
                    quantity_step: dec!(0.001),
                    minimum_quantity: dec!(0.001),
                    minimum_notional_size: dec!(5),
                },
            )]))
        })
    }

    /// Refresh uses tokio timers, so `start_paused` is the clock to mock here; the provider's
    /// `MockClock` only pins wall-clock timestamps.
    #[tokio::test(start_paused = true)]
    async fn a_transient_refresh_failure_is_retried_and_the_rules_are_updated() {
        FETCH_CALLS.store(0, Ordering::SeqCst);
        let instrument = Instrument {
            exchange: Exchange::Binance,
            base: Currency::Btc,
            quote: Currency::Usdt,
            underlying_type: UnderlyingType::Spot,
        };
        let instrument_rules = RwLock::new(InstrumentRules::new());
        let instruments = HashSet::from([instrument]);
        refresh_until_success(
            &instrument_rules,
            false,
            &instruments,
            &Notifier::Log,
            fake_fetch,
        )
        .await;
        assert_eq!(2, FETCH_CALLS.load(Ordering::SeqCst));
        let instrument_rules = instrument_rules
            .read()
            .expect("no code panics while holding the instrument rules");
        assert_eq!(
            Some("BTCUSDT"),
            instrument_rules
                .get(&instrument)
                .map(|rule| rule.exchange_symbol.as_str())
        );
    }
}

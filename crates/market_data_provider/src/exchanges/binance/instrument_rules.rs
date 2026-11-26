//! Fetches Binance's `exchangeInfo` for the instruments a strategy asks for.

use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, Result};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use tracing::info;
use url::Url;

use super::endpoint::{
    FUTURES_EXCHANGE_INFO_DEMO, FUTURES_EXCHANGE_INFO_LIVE, SPOT_EXCHANGE_INFO_DEMO,
    SPOT_EXCHANGE_INFO_LIVE,
};
use super::venue_stream::venue_symbol;
use layline::http::REQUEST_TIMEOUT;
use layline::types::{Instrument, InstrumentRule, InstrumentRules, UnderlyingType};

const TRADING_STATUS: &str = "TRADING";
/// A halt is temporary: subscribing now means the first event arrives when trading resumes.
const TRADING_HALT_STATUS: &str = "TRADING_HALT";
/// Fetches the rules of `instruments`, which must all be Binance instruments. A requested
/// instrument the response does not list, or lists as not trading, is a configuration error and
/// panics; a failed HTTP request is returned to the caller, which decides whether the request
/// fails or a refresh backs off.
pub(crate) async fn fetch_instrument_rules(
    demo: bool,
    instruments: &HashSet<Instrument>,
) -> Result<InstrumentRules> {
    info!("fetching the rules of {instruments} from /exchangeInfo");
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("a reqwest client with a timeout always builds");
    let mut rules = InstrumentRules::new();
    let spot_instruments: HashSet<Instrument> = instruments
        .iter()
        .filter(|instrument| instrument.underlying_type == UnderlyingType::Spot)
        .copied()
        .collect();
    if !spot_instruments.is_empty() {
        let url = with_symbols(
            exchange_info_url(demo, UnderlyingType::Spot),
            &spot_instruments,
        );
        rules.extend(build_rules(
            &spot_instruments,
            &fetch_exchange_info(&client, &url).await?,
        ));
    }
    let perpetual_instruments: HashSet<Instrument> = instruments
        .iter()
        .filter(|instrument| instrument.underlying_type == UnderlyingType::Perpetual)
        .copied()
        .collect();
    if !perpetual_instruments.is_empty() {
        // Futures exchangeInfo ignores `symbols` and returns every symbol (about 905 KiB); the
        // provider fetches only instruments missing from its map and refreshes it every 24 hours,
        // so this is not repeated on every 10-second resubscription.
        let url = exchange_info_url(demo, UnderlyingType::Perpetual);
        rules.extend(build_rules(
            &perpetual_instruments,
            &fetch_exchange_info(&client, &url).await?,
        ));
    }
    Ok(rules)
}

fn exchange_info_url(demo: bool, underlying_type: UnderlyingType) -> Url {
    let url = match (demo, underlying_type) {
        (true, UnderlyingType::Spot) => SPOT_EXCHANGE_INFO_DEMO,
        (false, UnderlyingType::Spot) => SPOT_EXCHANGE_INFO_LIVE,
        (true, UnderlyingType::Perpetual) => FUTURES_EXCHANGE_INFO_DEMO,
        (false, UnderlyingType::Perpetual) => FUTURES_EXCHANGE_INFO_LIVE,
    };
    Url::parse(url).expect("the exchange info URLs are valid")
}

fn with_symbols(mut url: Url, instruments: &HashSet<Instrument>) -> Url {
    let symbols: Vec<String> = instruments
        .iter()
        .map(|instrument| venue_symbol(*instrument))
        .collect();
    let symbols = serde_json::to_string(&symbols).expect("a list of strings is valid JSON");
    url.query_pairs_mut().append_pair("symbols", &symbols);
    url
}

async fn fetch_exchange_info(client: &Client, url: &Url) -> Result<Value> {
    // Unlike the client constructor and the JSON lookups below, a network request can fail for
    // reasons the program cannot fix; the HTTP handler turns this error into 503 so the strategy
    // retries. Replacing it with `expect` would turn a transient network blip into a provider exit.
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("request to {url} failed"))?
        .error_for_status()
        .with_context(|| format!("{url} refused the request"))?;
    response
        .json()
        .await
        .with_context(|| format!("{url} returned unexpected JSON"))
}

fn build_rules(instruments: &HashSet<Instrument>, exchange_info: &Value) -> InstrumentRules {
    let symbols = exchange_info["symbols"]
        .as_array()
        .expect("exchangeInfo has a symbols array");
    let mut rules = InstrumentRules::new();
    for &instrument in instruments {
        let exchange_symbol = venue_symbol(instrument);
        // The strategy explicitly listed this instrument; when the exchange does not return it,
        // the configuration is wrong. Fail fast and name the instrument so the operator can fix it.
        let symbol = symbols
            .iter()
            .find(|symbol| symbol["symbol"].as_str() == Some(exchange_symbol.as_str()))
            .unwrap_or_else(|| panic!("Binance did not return {exchange_symbol} for {instrument}"));
        let status = symbol["status"]
            .as_str()
            .expect("an exchange info symbol has a status");
        if status != TRADING_STATUS && status != TRADING_HALT_STATUS {
            panic!("{exchange_symbol} is not tradable: status {status}");
        }
        let filters = symbol["filters"]
            .as_array()
            .expect("an exchange info symbol has filters");
        let price_filter = filters
            .iter()
            .find(|filter| filter["filterType"] == "PRICE_FILTER")
            .expect("a symbol has a PRICE_FILTER");
        let lot_size_filter = filters
            .iter()
            .find(|filter| filter["filterType"] == "LOT_SIZE")
            .expect("a symbol has a LOT_SIZE");
        let minimum_notional = filters
            .iter()
            .find(|filter| filter["filterType"] == "NOTIONAL")
            .and_then(|filter| filter["minNotional"].as_str())
            .or_else(|| {
                filters
                    .iter()
                    .find(|filter| filter["filterType"] == "MIN_NOTIONAL")
                    .and_then(|filter| filter["notional"].as_str())
            })
            .expect("a symbol has a NOTIONAL or MIN_NOTIONAL filter");
        rules.insert(
            instrument,
            InstrumentRule {
                exchange_symbol: symbol["symbol"]
                    .as_str()
                    .expect("an exchange info symbol has a symbol")
                    .to_owned(),
                price_step: decimal(
                    price_filter["tickSize"]
                        .as_str()
                        .expect("PRICE_FILTER has tickSize"),
                ),
                quantity_step: decimal(
                    lot_size_filter["stepSize"]
                        .as_str()
                        .expect("LOT_SIZE has stepSize"),
                ),
                minimum_quantity: decimal(
                    lot_size_filter["minQty"]
                        .as_str()
                        .expect("LOT_SIZE has minQty"),
                ),
                minimum_notional_size: decimal(minimum_notional),
            },
        );
    }
    rules
}

fn decimal(text: &str) -> Decimal {
    Decimal::from_str(text).unwrap_or_else(|error| panic!("{text:?} is not a decimal: {error}"))
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;
    use serde_json::json;

    use super::*;
    use layline::types::{Currency, Exchange};

    fn instrument(base: Currency, underlying_type: UnderlyingType) -> Instrument {
        Instrument {
            exchange: Exchange::Binance,
            base,
            quote: Currency::Usdt,
            underlying_type,
        }
    }

    fn filters(notional_filter: Value) -> Value {
        json!([
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
            notional_filter,
        ])
    }

    #[test]
    fn rules_are_read_from_the_matching_symbol() {
        let exchange_info = json!({
            "symbols": [{
                "symbol": "AAPLBUSDT",
                "status": "TRADING",
                "filters": filters(json!({"filterType": "NOTIONAL", "minNotional": "5"})),
            }],
        });
        let stock = instrument(Currency::Aapl, UnderlyingType::Spot);
        let rules = build_rules(&HashSet::from([stock]), &exchange_info);
        assert_eq!(
            InstrumentRule {
                exchange_symbol: "AAPLBUSDT".to_owned(),
                price_step: dec!(0.01),
                quantity_step: dec!(0.001),
                minimum_quantity: dec!(0.001),
                minimum_notional_size: dec!(5),
            },
            rules[&stock]
        );
    }

    #[test]
    fn futures_use_min_notional() {
        let exchange_info = json!({
            "symbols": [{
                "symbol": "GOOGLUSDT",
                "status": "TRADING",
                "filters": filters(json!({"filterType": "MIN_NOTIONAL", "notional": "5"})),
            }],
        });
        let stock = instrument(Currency::Googl, UnderlyingType::Perpetual);
        let rules = build_rules(&HashSet::from([stock]), &exchange_info);
        assert_eq!(dec!(5), rules[&stock].minimum_notional_size);
    }

    #[test]
    #[should_panic(expected = "Binance did not return")]
    fn a_missing_symbol_panics() {
        let exchange_info = json!({"symbols": []});
        build_rules(
            &HashSet::from([instrument(Currency::Aapl, UnderlyingType::Spot)]),
            &exchange_info,
        );
    }

    #[test]
    #[should_panic(expected = "is not tradable")]
    fn a_non_trading_symbol_panics() {
        let exchange_info = json!({
            "symbols": [{
                "symbol": "AAPLBUSDT",
                "status": "PENDING_TRADING",
                "filters": filters(json!({"filterType": "NOTIONAL", "minNotional": "5"})),
            }],
        });
        build_rules(
            &HashSet::from([instrument(Currency::Aapl, UnderlyingType::Spot)]),
            &exchange_info,
        );
    }
}

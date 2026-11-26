//! Fetches Binance's `exchangeInfo` for the instruments a strategy asks for.

use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, Result};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tracing::info;
use url::Url;

use super::endpoint::{
    FUTURES_EXCHANGE_INFO_DEMO, FUTURES_EXCHANGE_INFO_LIVE, SPOT_EXCHANGE_INFO_DEMO,
    SPOT_EXCHANGE_INFO_LIVE,
};
use super::venue_stream::venue_symbol;
use crate::utils::to_pretty_json;
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
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("a reqwest client with a timeout always builds");
    let mut rules = InstrumentRules::new();
    for underlying_type in [UnderlyingType::Spot, UnderlyingType::Perpetual] {
        let same_type: HashSet<Instrument> = instruments
            .iter()
            .filter(|instrument| instrument.underlying_type == underlying_type)
            .copied()
            .collect();
        if same_type.is_empty() {
            continue;
        }
        rules.extend(rules_from_exchange_info(&client, demo, underlying_type, &same_type).await?);
    }
    Ok(rules)
}

/// Sends one `exchangeInfo` request and turns the answer into rules for `instruments`. Spot filters
/// the request by symbol; futures has no such filter and lists every symbol (about 905 KiB). So
/// neither answer is logged whole: `build_rules` logs the object of each requested instrument.
async fn rules_from_exchange_info(
    client: &Client,
    demo: bool,
    underlying_type: UnderlyingType,
    instruments: &HashSet<Instrument>,
) -> Result<InstrumentRules> {
    let endpoint = exchange_info_url(demo, underlying_type);
    log_request(&endpoint, instruments);
    let url = match underlying_type {
        UnderlyingType::Spot => with_symbols(endpoint.clone(), instruments),
        UnderlyingType::Perpetual => endpoint.clone(),
    };
    let response = client
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("request to {url} failed"))?;
    let status = response.status();
    info!("{endpoint} answered {status}");
    let response = response
        .error_for_status()
        .with_context(|| format!("{url} refused the request"))?;
    let exchange_info: Value = response
        .json()
        .await
        .with_context(|| format!("{url} returned unexpected JSON"))?;
    let listed = exchange_info["symbols"].as_array().map_or(0, Vec::len);
    info!("{endpoint} listed {listed} symbols");
    Ok(build_rules(instruments, &exchange_info, &endpoint))
}

/// Logs a request the way `jq` prints it. `symbols` is the spot endpoint's query parameter and,
/// since the futures endpoint has no such filter, what the provider looks for in the full answer.
fn log_request(endpoint: &Url, instruments: &HashSet<Instrument>) {
    let request = json!({
        "method": "GET",
        "url": endpoint.as_str(),
        "symbols": symbols(instruments),
    });
    let pretty = to_pretty_json(&request);
    info!("requesting:\n{pretty}");
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

/// The venue symbols of `instruments`, sorted so a log line does not move between runs.
fn symbols(instruments: &HashSet<Instrument>) -> Vec<String> {
    let mut symbols: Vec<String> = instruments
        .iter()
        .map(|instrument| venue_symbol(*instrument))
        .collect();
    symbols.sort();
    symbols
}

fn with_symbols(mut url: Url, instruments: &HashSet<Instrument>) -> Url {
    let symbols =
        serde_json::to_string(&symbols(instruments)).expect("a list of strings is valid JSON");
    url.query_pairs_mut().append_pair("symbols", &symbols);
    url
}

/// Parses one `exchangeInfo` answer into rules, logging the object of every instrument it matches.
fn build_rules(
    instruments: &HashSet<Instrument>,
    exchange_info: &Value,
    endpoint: &Url,
) -> InstrumentRules {
    let symbols = exchange_info["symbols"]
        .as_array()
        .expect("exchangeInfo has a symbols array");
    let mut ordered: Vec<Instrument> = instruments.iter().copied().collect();
    ordered.sort_by_cached_key(|instrument| venue_symbol(*instrument));
    let mut rules = InstrumentRules::new();
    for instrument in ordered {
        let exchange_symbol = venue_symbol(instrument);
        // The strategy explicitly listed this instrument; when the exchange does not return it,
        // the configuration is wrong. Fail fast and name the instrument so the operator can fix it.
        let symbol = symbols
            .iter()
            .find(|symbol| symbol["symbol"].as_str() == Some(exchange_symbol.as_str()))
            .unwrap_or_else(|| panic!("Binance did not return {exchange_symbol} for {instrument}"));
        let pretty = to_pretty_json(symbol);
        info!("{instrument} from {endpoint}:\n{pretty}");
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

    fn endpoint() -> Url {
        Url::parse(SPOT_EXCHANGE_INFO_LIVE).expect("The endpoint is a valid URL")
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
        let rules = build_rules(&HashSet::from([stock]), &exchange_info, &endpoint());
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
        let rules = build_rules(&HashSet::from([stock]), &exchange_info, &endpoint());
        assert_eq!(dec!(5), rules[&stock].minimum_notional_size);
    }

    #[test]
    #[should_panic(expected = "Binance did not return")]
    fn a_missing_symbol_panics() {
        let exchange_info = json!({"symbols": []});
        build_rules(
            &HashSet::from([instrument(Currency::Aapl, UnderlyingType::Spot)]),
            &exchange_info,
            &endpoint(),
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
            &endpoint(),
        );
    }

    /// One field per line, indented by four spaces so a filter stays one line.
    #[test]
    fn symbol_objects_are_printed_with_four_space_indentation() {
        let symbol = json!({
            "symbol": "AAPLBUSDT",
            "status": "TRADING",
            "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}],
        });
        assert_eq!(
            "{\n    \"filters\": [\n        {\n            \"filterType\": \"PRICE_FILTER\",\n            \"tickSize\": \"0.01\"\n        }\n    ],\n    \"status\": \"TRADING\",\n    \"symbol\": \"AAPLBUSDT\"\n}",
            to_pretty_json(&symbol)
        );
    }
}

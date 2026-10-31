use market_data_provider::types::{Bbo, MarketData, Trade};
use std::num::NonZeroU64;

pub(crate) trait Strategy {
    fn on_bbo(&mut self, bbo: &Bbo, received_at_nanoseconds: NonZeroU64);

    fn on_trade(&mut self, trade: &Trade, received_at_nanoseconds: NonZeroU64);

    fn on_market_data(&mut self, data: &MarketData, received_at_nanoseconds: NonZeroU64) {
        match data {
            MarketData::Bbo(bbo) => self.on_bbo(bbo, received_at_nanoseconds),
            MarketData::Trade(trade) => self.on_trade(trade, received_at_nanoseconds),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_data_provider::types::{
        Currency, Exchange, Instrument, LatencyMetric, Side, UnderlyingType,
    };
    use market_data_provider::utils::get_current_timestamp_nanoseconds;
    use rust_decimal::Decimal;

    #[derive(Default)]
    struct Recorder {
        bbo_sequences: Vec<usize>,
        trade_sequences: Vec<usize>,
    }

    impl Strategy for Recorder {
        fn on_bbo(&mut self, bbo: &Bbo, _received_at_nanoseconds: NonZeroU64) {
            self.bbo_sequences.push(bbo.sequence);
        }

        fn on_trade(&mut self, trade: &Trade, _received_at_nanoseconds: NonZeroU64) {
            self.trade_sequences.push(trade.sequence);
        }
    }

    fn instrument() -> Instrument {
        Instrument {
            exchange: Exchange::Okx,
            base: Currency::Eth,
            quote: Currency::Usdt,
            underlying_type: UnderlyingType::Perpetual,
        }
    }

    #[test]
    fn market_data_reaches_the_matching_callback() {
        let now_nanoseconds = get_current_timestamp_nanoseconds();
        let bbo = MarketData::Bbo(Bbo {
            instrument: instrument(),
            exchange_time_nanoseconds: None,
            latency_metric: LatencyMetric::received(now_nanoseconds),
            ask_price: Decimal::ONE,
            ask_size: Decimal::ONE,
            bid_price: Decimal::ONE,
            bid_size: Decimal::ONE,
            sequence: 1,
        });
        let trade = MarketData::Trade(Trade {
            instrument: instrument(),
            exchange_time_nanoseconds: None,
            latency_metric: LatencyMetric::received(now_nanoseconds),
            price: Decimal::ONE,
            size: Decimal::ONE,
            taker_side: Side::Sell,
            sequence: 2,
        });

        let mut recorder = Recorder::default();
        recorder.on_market_data(&bbo, now_nanoseconds);
        recorder.on_market_data(&trade, now_nanoseconds);
        assert_eq!(vec![1], recorder.bbo_sequences);
        assert_eq!(vec![2], recorder.trade_sequences);
    }
}

//! Binance spot SBE stream frames, little-endian per `sbe/schemas/stream_1_0.xml` in binance-spot-api-docs.
//! Block lengths come from the frame, so fields a newer schema appends are skipped. Decoding borrows
//! the frame and allocates nothing, since it runs for every event.

use std::num::NonZeroU64;
use std::str::from_utf8;

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;

use crate::time::microseconds_to_nanoseconds;

const SCHEMA_ID: u16 = 1;
const TRADES_TEMPLATE_ID: u16 = 10000;
const BEST_BID_ASK_TEMPLATE_ID: u16 = 10001;
const HEADER_LENGTH: usize = 8;
/// eventTime, bookUpdateId, priceExponent, qtyExponent, bidPrice, bidQty, askPrice, askQty.
const BEST_BID_ASK_ROOT_LENGTH: usize = 50;
/// eventTime, transactTime, priceExponent, qtyExponent.
const TRADES_ROOT_LENGTH: usize = 18;
/// id, price, qty, isBuyerMaker; isBestMatch is constant and not on the wire.
const TRADE_ENTRY_LENGTH: usize = 25;

pub(super) enum SbeFrame<'a> {
    Trades(SbeTrades<'a>),
    BestBidAsk(SbeBestBidAsk<'a>),
    /// Another template, which nothing subscribes to.
    Other {
        template_id: u16,
    },
}

pub(super) struct SbeTrades<'a> {
    pub(super) symbol: &'a str,
    pub(super) transaction_time_nanoseconds: NonZeroU64,
    price_exponent: i8,
    size_exponent: i8,
    /// Every fill, `entry_length` bytes each.
    entries: &'a [u8],
    entry_length: usize,
}

impl SbeTrades<'_> {
    pub(super) fn trades(&self) -> impl Iterator<Item = Result<SbeTrade>> + '_ {
        self.entries.chunks_exact(self.entry_length).map(|entry| {
            let mut reader = Reader {
                frame: entry,
                offset: 0,
            };
            Ok(SbeTrade {
                id: u64::try_from(reader.i64()?).context("negative trade id")?,
                price: decimal(reader.i64()?, self.price_exponent)?,
                size: decimal(reader.i64()?, self.size_exponent)?,
                buyer_is_maker: reader.u8()? != 0,
            })
        })
    }
}

/// Binance may drop an outdated one under load ("auto-culling"); the next one is always current.
pub(super) struct SbeBestBidAsk<'a> {
    pub(super) symbol: &'a str,
    pub(super) event_time_nanoseconds: NonZeroU64,
    pub(super) update_id: u64,
    pub(super) bid_price: Decimal,
    pub(super) bid_size: Decimal,
    pub(super) ask_price: Decimal,
    pub(super) ask_size: Decimal,
}

pub(super) struct SbeTrade {
    pub(super) id: u64,
    pub(super) price: Decimal,
    pub(super) size: Decimal,
    pub(super) buyer_is_maker: bool,
}

pub(super) fn decode(frame: &[u8]) -> Result<SbeFrame<'_>> {
    let mut reader = Reader { frame, offset: 0 };
    let block_length = usize::from(reader.u16()?);
    let template_id = reader.u16()?;
    let schema_id = reader.u16()?;
    let _version = reader.u16()?;
    if schema_id != SCHEMA_ID {
        bail!("unknown schema {schema_id}");
    }
    match template_id {
        TRADES_TEMPLATE_ID => decode_trades(reader, block_length),
        BEST_BID_ASK_TEMPLATE_ID => decode_best_bid_ask(reader, block_length),
        _ => Ok(SbeFrame::Other { template_id }),
    }
}

fn decode_best_bid_ask(mut reader: Reader<'_>, block_length: usize) -> Result<SbeFrame<'_>> {
    if block_length < BEST_BID_ASK_ROOT_LENGTH {
        bail!("best bid/ask block of {block_length} bytes is too short");
    }
    let event_time_nanoseconds = signed_microseconds_to_nanoseconds(reader.i64()?)?;
    let update_id = u64::try_from(reader.i64()?).context("negative book update id")?;
    let price_exponent = reader.i8()?;
    let size_exponent = reader.i8()?;
    let bid_price = decimal(reader.i64()?, price_exponent)?;
    let bid_size = decimal(reader.i64()?, size_exponent)?;
    let ask_price = decimal(reader.i64()?, price_exponent)?;
    let ask_size = decimal(reader.i64()?, size_exponent)?;
    reader.seek(HEADER_LENGTH + block_length)?;
    let symbol = reader.symbol()?;
    Ok(SbeFrame::BestBidAsk(SbeBestBidAsk {
        symbol,
        event_time_nanoseconds,
        update_id,
        bid_price,
        bid_size,
        ask_price,
        ask_size,
    }))
}

fn decode_trades(mut reader: Reader<'_>, block_length: usize) -> Result<SbeFrame<'_>> {
    if block_length < TRADES_ROOT_LENGTH {
        bail!("trades block of {block_length} bytes is too short");
    }
    let _event_time_microseconds = reader.i64()?;
    let transaction_time_microseconds = reader.i64()?;
    let price_exponent = reader.i8()?;
    let size_exponent = reader.i8()?;
    reader.seek(HEADER_LENGTH + block_length)?;

    let entry_length = usize::from(reader.u16()?);
    let count = usize::try_from(reader.u32()?).context("trade count does not fit usize")?;
    if entry_length < TRADE_ENTRY_LENGTH {
        bail!("trade entry of {entry_length} bytes is too short");
    }
    let entries_length = count
        .checked_mul(entry_length)
        .context("trade group length overflow")?;
    let entries = reader.bytes(entries_length)?;
    let symbol = reader.symbol()?;
    Ok(SbeFrame::Trades(SbeTrades {
        symbol,
        transaction_time_nanoseconds: signed_microseconds_to_nanoseconds(
            transaction_time_microseconds,
        )?,
        price_exponent,
        size_exponent,
        entries,
        entry_length,
    }))
}

/// Binance encodes microseconds as `i64`; a negative one is invalid.
fn signed_microseconds_to_nanoseconds(microseconds: i64) -> Result<NonZeroU64> {
    let microseconds = u64::try_from(microseconds).context("negative timestamp")?;
    Ok(microseconds_to_nanoseconds(microseconds))
}

/// `mantissa × 10^exponent`.
fn decimal(mantissa: i64, exponent: i8) -> Result<Decimal> {
    let decimal = if exponent <= 0 {
        Decimal::try_new(mantissa, u32::from(exponent.unsigned_abs()))?
    } else {
        let scale = 10_i64
            .checked_pow(u32::from(exponent.unsigned_abs()))
            .context("decimal overflow")?;
        Decimal::from(mantissa)
            .checked_mul(Decimal::from(scale))
            .context("decimal overflow")?
    };
    Ok(decimal)
}

struct Reader<'a> {
    frame: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(length).context("offset overflow")?;
        let bytes = self
            .frame
            .get(self.offset..end)
            .with_context(|| format!("frame of {} bytes ends before {end}", self.frame.len()))?;
        self.offset = end;
        Ok(bytes)
    }

    /// The trailing `varString8` every message ends with.
    fn symbol(&mut self) -> Result<&'a str> {
        let length = usize::from(self.u8()?);
        from_utf8(self.bytes(length)?).context("symbol is not UTF-8")
    }

    fn array<const LENGTH: usize>(&mut self) -> Result<[u8; LENGTH]> {
        let mut array = [0; LENGTH];
        array.copy_from_slice(self.bytes(LENGTH)?);
        Ok(array)
    }

    fn seek(&mut self, offset: usize) -> Result<()> {
        if offset > self.frame.len() {
            bail!("frame of {} bytes ends before {offset}", self.frame.len());
        }
        self.offset = offset;
        Ok(())
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    fn i8(&mut self) -> Result<i8> {
        Ok(i8::from_le_bytes(self.array()?))
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }
}

#[cfg(test)]
pub(super) mod tests {
    use rust_decimal::dec;

    use super::*;

    pub(in crate::exchanges::binance) struct EncodedTrade {
        pub(in crate::exchanges::binance) id: i64,
        pub(in crate::exchanges::binance) price_mantissa: i64,
        pub(in crate::exchanges::binance) size_mantissa: i64,
        pub(in crate::exchanges::binance) buyer_is_maker: bool,
    }

    /// A trades frame as Binance encodes it, with `-8` exponents and microsecond times.
    pub(in crate::exchanges::binance) fn encode_trades(
        symbol: &str,
        transaction_time_microseconds: i64,
        trades: &[EncodedTrade],
    ) -> Vec<u8> {
        let mut frame = Vec::new();
        for value in [TRADES_ROOT_LENGTH as u16, TRADES_TEMPLATE_ID, SCHEMA_ID, 0] {
            frame.extend(value.to_le_bytes());
        }
        frame.extend((transaction_time_microseconds + 5).to_le_bytes());
        frame.extend(transaction_time_microseconds.to_le_bytes());
        frame.extend((-8_i8).to_le_bytes());
        frame.extend((-8_i8).to_le_bytes());
        frame.extend((TRADE_ENTRY_LENGTH as u16).to_le_bytes());
        frame.extend(
            u32::try_from(trades.len())
                .expect("few trades")
                .to_le_bytes(),
        );
        for trade in trades {
            frame.extend(trade.id.to_le_bytes());
            frame.extend(trade.price_mantissa.to_le_bytes());
            frame.extend(trade.size_mantissa.to_le_bytes());
            frame.push(u8::from(trade.buyer_is_maker));
        }
        frame.push(u8::try_from(symbol.len()).expect("short symbol"));
        frame.extend(symbol.as_bytes());
        frame
    }

    /// A best bid/ask frame with `-8` exponents: bid 2343.54 × 2.841, ask 2343.55 × 96.2171.
    pub(in crate::exchanges::binance) fn encode_best_bid_ask(
        symbol: &str,
        event_time_microseconds: i64,
        update_id: i64,
    ) -> Vec<u8> {
        let mut frame = Vec::new();
        for value in [
            BEST_BID_ASK_ROOT_LENGTH as u16,
            BEST_BID_ASK_TEMPLATE_ID,
            SCHEMA_ID,
            0,
        ] {
            frame.extend(value.to_le_bytes());
        }
        frame.extend(event_time_microseconds.to_le_bytes());
        frame.extend(update_id.to_le_bytes());
        frame.extend((-8_i8).to_le_bytes());
        frame.extend((-8_i8).to_le_bytes());
        for mantissa in [
            234_354_000_000_i64,
            284_100_000,
            234_355_000_000,
            9_621_710_000,
        ] {
            frame.extend(mantissa.to_le_bytes());
        }
        frame.push(u8::try_from(symbol.len()).expect("short symbol"));
        frame.extend(symbol.as_bytes());
        frame
    }

    fn two_trades() -> Vec<u8> {
        encode_trades(
            "BTCUSDT",
            1_792_881_786_559_123,
            &[
                EncodedTrade {
                    id: 41,
                    price_mantissa: 8_058_630_000_000,
                    size_mantissa: 150_000,
                    buyer_is_maker: true,
                },
                EncodedTrade {
                    id: 42,
                    price_mantissa: 8_058_631_000_000,
                    size_mantissa: 1,
                    buyer_is_maker: false,
                },
            ],
        )
    }

    /// The symbol and every fill.
    fn decode_trades(frame: &[u8]) -> (String, NonZeroU64, Vec<SbeTrade>) {
        match decode(frame).expect("the frame is expected to decode") {
            SbeFrame::Trades(trades) => (
                trades.symbol.to_owned(),
                trades.transaction_time_nanoseconds,
                trades
                    .trades()
                    .collect::<Result<Vec<_>>>()
                    .expect("every fill is expected to decode"),
            ),
            SbeFrame::BestBidAsk(_) => panic!("trades are expected"),
            SbeFrame::Other { template_id } => panic!("unexpected template {template_id}"),
        }
    }

    #[test]
    fn trades_frame_decodes_every_trade() {
        let (symbol, transaction_time_nanoseconds, trades) = decode_trades(&two_trades());
        assert_eq!("BTCUSDT", symbol);
        assert_eq!(
            1_792_881_786_559_123_000,
            transaction_time_nanoseconds.get()
        );
        assert_eq!(2, trades.len());
        let first = &trades[0];
        assert_eq!(41, first.id);
        assert_eq!(dec!(80586.3), first.price);
        assert_eq!(dec!(0.0015), first.size);
        assert!(first.buyer_is_maker);
        let second = &trades[1];
        assert_eq!(42, second.id);
        assert_eq!(dec!(80586.31), second.price);
        assert_eq!(dec!(0.00000001), second.size);
        assert!(!second.buyer_is_maker);
    }

    #[test]
    fn longer_blocks_from_a_newer_schema_are_skipped() {
        let mut frame = Vec::new();
        for value in [
            TRADES_ROOT_LENGTH as u16 + 2,
            TRADES_TEMPLATE_ID,
            SCHEMA_ID,
            1,
        ] {
            frame.extend(value.to_le_bytes());
        }
        let original = two_trades();
        frame.extend(&original[HEADER_LENGTH..HEADER_LENGTH + TRADES_ROOT_LENGTH]);
        frame.extend([0xAA, 0xBB]);
        let group_start = HEADER_LENGTH + TRADES_ROOT_LENGTH;
        frame.extend((TRADE_ENTRY_LENGTH as u16 + 1).to_le_bytes());
        frame.extend(2_u32.to_le_bytes());
        let entries_start = group_start + 6;
        for index in 0..2 {
            let start = entries_start + index * TRADE_ENTRY_LENGTH;
            frame.extend(&original[start..start + TRADE_ENTRY_LENGTH]);
            frame.push(0xCC);
        }
        frame.extend(&original[entries_start + 2 * TRADE_ENTRY_LENGTH..]);

        let (symbol, _, trades) = decode_trades(&frame);
        assert_eq!("BTCUSDT", symbol);
        assert_eq!(
            vec![41, 42],
            trades.iter().map(|trade| trade.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn other_templates_are_reported_not_decoded() {
        let mut frame = two_trades();
        frame[2..4].copy_from_slice(&10002_u16.to_le_bytes());
        assert!(matches!(
            decode(&frame).expect("a known schema"),
            SbeFrame::Other { template_id: 10002 }
        ));
    }

    #[test]
    fn best_bid_ask_frame_decodes() {
        let frame = encode_best_bid_ask("ETHUSDT", 1_792_881_786_559_123, 77);
        let SbeFrame::BestBidAsk(best_bid_ask) = decode(&frame).expect("a valid frame") else {
            panic!("a best bid/ask is expected");
        };
        assert_eq!("ETHUSDT", best_bid_ask.symbol);
        assert_eq!(
            1_792_881_786_559_123_000,
            best_bid_ask.event_time_nanoseconds.get()
        );
        assert_eq!(77, best_bid_ask.update_id);
        assert_eq!(dec!(2343.54), best_bid_ask.bid_price);
        assert_eq!(dec!(2.841), best_bid_ask.bid_size);
        assert_eq!(dec!(2343.55), best_bid_ask.ask_price);
        assert_eq!(dec!(96.2171), best_bid_ask.ask_size);
    }

    #[test]
    fn malformed_best_bid_ask_is_refused() {
        let frame = encode_best_bid_ask("ETHUSDT", 1, 77);
        assert!(decode(&frame[..frame.len() - 1]).is_err());
        let mut short_block = frame.clone();
        short_block[0..2].copy_from_slice(&40_u16.to_le_bytes());
        assert!(decode(&short_block).is_err());
        let mut negative_time = frame.clone();
        negative_time[HEADER_LENGTH..HEADER_LENGTH + 8].copy_from_slice(&(-1_i64).to_le_bytes());
        assert!(decode(&negative_time).is_err());
        let mut negative_id = frame;
        negative_id[HEADER_LENGTH + 8..HEADER_LENGTH + 16].copy_from_slice(&(-1_i64).to_le_bytes());
        assert!(decode(&negative_id).is_err());
    }

    #[test]
    fn malformed_frames_are_refused() {
        let frame = two_trades();
        for length in [0, 7, HEADER_LENGTH + 10, frame.len() - 1] {
            assert!(decode(&frame[..length]).is_err(), "truncated to {length}");
        }
        let mut wrong_schema = frame.clone();
        wrong_schema[4..6].copy_from_slice(&2_u16.to_le_bytes());
        assert!(decode(&wrong_schema).is_err());
        let mut short_block = frame.clone();
        short_block[0..2].copy_from_slice(&10_u16.to_le_bytes());
        assert!(decode(&short_block).is_err());
        let mut short_entry = frame.clone();
        let group_start = HEADER_LENGTH + TRADES_ROOT_LENGTH;
        short_entry[group_start..group_start + 2].copy_from_slice(&3_u16.to_le_bytes());
        assert!(decode(&short_entry).is_err());
        let mut huge_count = frame;
        huge_count[group_start + 2..group_start + 6].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&huge_count).is_err());
    }

    #[test]
    fn exponents_scale_the_mantissa_both_ways() {
        assert_eq!(dec!(1.5), decimal(15, -1).expect("valid"));
        assert_eq!(dec!(1500), decimal(15, 2).expect("valid"));
        assert_eq!(dec!(15), decimal(15, 0).expect("valid"));
        assert!(decimal(i64::MAX, 28).is_err());
        assert!(decimal(1, -29).is_err());
    }
}

//! Timestamp unit conversions, the same for every venue and service.

// TODO: give each unit (milliseconds, microseconds, nanoseconds) its own timestamp type, so the type
// system rejects mixing units; until then every raw timestamp carries its unit in its name.

use std::num::NonZeroU64;

const NANOSECONDS_PER_MICROSECOND: u64 = 1_000;
const NANOSECONDS_PER_MILLISECOND: u64 = 1_000_000;

/// Panics on zero or overflow: a real timestamp is neither, so either means a bug to fix at once.
pub(crate) fn milliseconds_to_nanoseconds(milliseconds: u64) -> NonZeroU64 {
    milliseconds
        .checked_mul(NANOSECONDS_PER_MILLISECOND)
        .and_then(NonZeroU64::new)
        .expect("a millisecond timestamp is positive and fits u64 in nanoseconds")
}

/// Panics on zero or overflow: a real timestamp is neither, so either means a bug to fix at once.
pub(crate) fn microseconds_to_nanoseconds(microseconds: u64) -> NonZeroU64 {
    microseconds
        .checked_mul(NANOSECONDS_PER_MICROSECOND)
        .and_then(NonZeroU64::new)
        .expect("a microsecond timestamp is positive and fits u64 in nanoseconds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions_scale() {
        assert_eq!(
            1_792_881_786_559_000_000,
            milliseconds_to_nanoseconds(1_792_881_786_559).get()
        );
        assert_eq!(
            1_792_881_786_559_123_000,
            microseconds_to_nanoseconds(1_792_881_786_559_123).get()
        );
    }

    #[test]
    #[should_panic(expected = "a millisecond timestamp is positive")]
    fn zero_milliseconds_fail_fast() {
        milliseconds_to_nanoseconds(0);
    }

    #[test]
    #[should_panic(expected = "a millisecond timestamp is positive")]
    fn overflowing_milliseconds_fail_fast() {
        milliseconds_to_nanoseconds(u64::MAX);
    }

    #[test]
    #[should_panic(expected = "a microsecond timestamp is positive")]
    fn zero_microseconds_fail_fast() {
        microseconds_to_nanoseconds(0);
    }

    #[test]
    #[should_panic(expected = "a microsecond timestamp is positive")]
    fn overflowing_microseconds_fail_fast() {
        microseconds_to_nanoseconds(u64::MAX);
    }
}

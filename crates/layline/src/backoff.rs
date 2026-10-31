//! Retry delays shared by every client that reconnects.

use std::time::Duration;

/// Doubles from `initial` with each attempt in a row, up to `maximum`.
pub fn exponential_backoff(attempt: u32, initial: Duration, maximum: Duration) -> Duration {
    let doublings = attempt.saturating_sub(1).min(u32::BITS - 1);
    initial.saturating_mul(1 << doublings).min(maximum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_the_maximum() {
        assert_eq!(
            Duration::from_secs(1),
            exponential_backoff(1, Duration::from_secs(1), Duration::from_secs(60))
        );
        assert_eq!(
            Duration::from_secs(32),
            exponential_backoff(6, Duration::from_secs(1), Duration::from_secs(60))
        );
        assert_eq!(
            Duration::from_secs(60),
            exponential_backoff(7, Duration::from_secs(1), Duration::from_secs(60))
        );
        assert_eq!(
            Duration::from_secs(60),
            exponential_backoff(u32::MAX, Duration::from_secs(1), Duration::from_secs(60))
        );
    }
}

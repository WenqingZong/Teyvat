use std::num::NonZeroU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Release builds push alerts through Bark and refuse to start without it; debug builds log them instead.
pub(crate) const RELEASE_BUILD: bool = !cfg!(debug_assertions);

fn now_nanoseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System clock before UNIX epoch")
        .as_nanos() as u64
}

pub fn get_current_timestamp_nanoseconds() -> NonZeroU64 {
    NonZeroU64::new(now_nanoseconds()).expect("Failed to convert timestamp")
}

/// Doubles from `initial` with each attempt in a row, up to `maximum`.
pub fn exponential_backoff(attempt: u32, initial: Duration, maximum: Duration) -> Duration {
    let doublings = attempt.saturating_sub(1).min(u32::BITS - 1);
    initial.saturating_mul(1 << doublings).min(maximum)
}

/// Wall-clock source, so tests can pin timestamps. Timers use tokio time, which tests pause instead.
pub(crate) trait Clock {
    fn now(&self) -> NonZeroU64;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> NonZeroU64 {
        get_current_timestamp_nanoseconds()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    /// Clones share one time, so a test can move the clock it handed to the code under test.
    #[derive(Clone, Debug)]
    pub(crate) struct MockClock {
        now_nanoseconds: Arc<AtomicU64>,
    }

    impl MockClock {
        pub(crate) fn new(now_nanoseconds: NonZeroU64) -> Self {
            Self {
                now_nanoseconds: Arc::new(AtomicU64::new(now_nanoseconds.get())),
            }
        }

        pub(crate) fn advance(&self, duration: Duration) {
            let nanoseconds =
                u64::try_from(duration.as_nanos()).expect("test durations fit in u64");
            self.now_nanoseconds
                .fetch_add(nanoseconds, Ordering::SeqCst);
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> NonZeroU64 {
            NonZeroU64::new(self.now_nanoseconds.load(Ordering::SeqCst))
                .expect("MockClock starts above zero and only moves forward")
        }
    }

    #[test]
    fn mock_clock_clones_share_time() {
        let start_nanoseconds = NonZeroU64::new(1_000).expect("1000 is not zero");
        let clock = MockClock::new(start_nanoseconds);
        let handle = clock.clone();
        handle.advance(Duration::from_nanos(500));
        assert_eq!(1_500, clock.now().get());
    }

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

    #[test]
    fn real_clock_moves_forward() {
        let first_nanoseconds = RealClock.now();
        assert!(RealClock.now() >= first_nanoseconds);
    }
}

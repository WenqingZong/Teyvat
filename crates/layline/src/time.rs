//! Wall-clock readings shared by every service. Hot paths take `get_current_timestamp_nanoseconds`, a plain
//! integer; nothing on them formats dates.

use std::num::NonZeroU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;

/// UTC, e.g. `2026-10-15 13:30:05.123 UTC`. Only humans read it, through `GET /info`.
const SERVER_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f UTC";

pub fn since_unix_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is set after 1970")
}

pub fn get_current_timestamp_nanoseconds() -> NonZeroU64 {
    NonZeroU64::new(since_unix_epoch().as_nanos() as u64).expect("Failed to convert timestamp")
}

/// The current time for `GET /info`, in `SERVER_TIME_FORMAT`.
pub fn server_time() -> String {
    Utc::now().format(SERVER_TIME_FORMAT).to_string()
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDateTime, TimeDelta};

    use super::*;

    /// Generous bound between formatting the time and checking it.
    const MAX_SERVER_TIME_DRIFT: TimeDelta = TimeDelta::seconds(5);
    const MILLISECOND_DIGITS: usize = 3;
    /// The literal tail of `SERVER_TIME_FORMAT`.
    const UTC_SUFFIX: &str = " UTC";

    #[test]
    fn readings_agree() {
        let before = since_unix_epoch();
        let nanoseconds = get_current_timestamp_nanoseconds().get();
        let after = since_unix_epoch();
        assert!(before.as_nanos() as u64 <= nanoseconds);
        assert!(nanoseconds <= after.as_nanos() as u64);
    }

    #[test]
    fn server_time_is_utc_in_milliseconds() {
        let formatted = server_time();
        let time = formatted
            .strip_suffix(UTC_SUFFIX)
            .expect("server time ends with UTC");
        let (_, milliseconds) = time.rsplit_once('.').expect("server time has a fraction");
        assert_eq!(milliseconds.len(), MILLISECOND_DIGITS);
        let server_time = NaiveDateTime::parse_from_str(&formatted, SERVER_TIME_FORMAT)
            .expect("server time parses")
            .and_utc();
        let drift = (Utc::now() - server_time).abs();
        assert!(
            drift < MAX_SERVER_TIME_DRIFT,
            "server time is {drift:?} off UTC"
        );
    }
}

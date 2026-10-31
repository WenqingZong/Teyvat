use std::time::{Duration, SystemTime, UNIX_EPOCH};

use env_logger::{Builder, Env};

/// Release builds push through Bark and refuse to start without it; debug builds log instead.
pub const RELEASE_BUILD: bool = !cfg!(debug_assertions);

/// Shared by the binary and all examples: microsecond timestamps, plus file and line in debug builds.
pub fn init_logger() {
    Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .format_source_path(!RELEASE_BUILD)
        .init();
}

pub fn since_unix_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is set after 1970")
}

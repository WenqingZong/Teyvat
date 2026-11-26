//! Constants shared by every HTTP client in the system.

use std::time::Duration;

/// The timeout for one HTTP request to a service or an exchange.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

use std::convert::Infallible;
use std::panic::panic_any;

use tracing::error;

use crate::exit_reason::ExitReason;
use layline::bark::{BarkMessage, Notifier};

const ALERT_GROUP: &str = "market_data_provider";
const ALERT_SOUND: &str = "alarm";

/// Best effort: a failed alert is logged, never returned, because callers alert on their way to stopping.
pub(crate) trait SendAlert {
    async fn send_alert(&self, title: &str, body: &str);
}

/// Logs `reason`, alerts the phone, then panics with `exit_reason`, which the binary's panic hook
/// turns into the exit code. Never returns: callers write `match alert_and_panic(...).await {}`.
pub(crate) async fn alert_and_panic(
    alert: &impl SendAlert,
    title: &str,
    reason: &str,
    exit_reason: ExitReason,
) -> Infallible {
    error!("{reason}");
    alert.send_alert(title, reason).await;
    panic_any(exit_reason);
}

impl SendAlert for Notifier {
    async fn send_alert(&self, title: &str, body: &str) {
        let message = BarkMessage {
            title: title.to_owned(),
            body: body.to_owned(),
            sound: ALERT_SOUND.to_owned(),
            group: ALERT_GROUP.to_owned(),
        };
        if let Err(error) = self.send(&message).await {
            error!("failed to alert the phone: {error}; alert: {title}: {body}");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fmt::Debug;
    use std::panic::AssertUnwindSafe;
    use std::sync::{Arc, Mutex};

    use futures::FutureExt;

    use super::*;

    /// Clones share the record, so a test keeps one and hands the other to the code under test.
    #[derive(Clone, Default)]
    pub(crate) struct RecordingAlert {
        alerts: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingAlert {
        /// Each alert as `title: body`.
        pub(crate) fn alerts(&self) -> Vec<String> {
            self.alerts
                .lock()
                .expect("no test panics while holding the record")
                .clone()
        }
    }

    impl SendAlert for RecordingAlert {
        async fn send_alert(&self, title: &str, body: &str) {
            self.alerts
                .lock()
                .expect("no test panics while holding the record")
                .push(format!("{title}: {body}"));
        }
    }

    /// Runs `future`, expects it to panic with `exit_reason`, and returns the one alert `alert` recorded.
    pub(crate) async fn expect_alert_and_panic(
        future: impl Future<Output: Debug>,
        alert: &RecordingAlert,
        exit_reason: ExitReason,
    ) -> String {
        let payload = AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .expect_err("the future is expected to panic");
        assert_eq!(Some(&exit_reason), payload.downcast_ref::<ExitReason>());
        let mut alerts = alert.alerts();
        assert_eq!(1, alerts.len(), "{alerts:?}");
        alerts.remove(0)
    }

    #[tokio::test]
    async fn alert_and_panic_alerts_the_reason_then_panics_with_the_exit_reason() {
        let alert = RecordingAlert::default();
        let recorded = expect_alert_and_panic(
            alert_and_panic(&alert, "title", "reason", ExitReason::FatalError),
            &alert,
            ExitReason::FatalError,
        )
        .await;
        assert_eq!("title: reason", recorded);
    }

    #[tokio::test]
    async fn log_notifier_only_logs() {
        Notifier::Log.send_alert("title", "body").await;
    }
}

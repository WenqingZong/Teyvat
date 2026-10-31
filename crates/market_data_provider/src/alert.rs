use layline::bark::{BarkMessage, Notifier};
use tracing::error;

const ALERT_GROUP: &str = "market_data_provider";
const ALERT_SOUND: &str = "alarm";

/// Best effort: a failed alert is logged, never returned, because callers alert on their way to stopping.
pub(crate) trait SendAlert {
    async fn send_alert(&self, title: &str, body: &str);
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
    use super::*;
    use std::sync::{Arc, Mutex};

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

    #[tokio::test]
    async fn log_notifier_only_logs() {
        Notifier::Log.send_alert("title", "body").await;
    }
}

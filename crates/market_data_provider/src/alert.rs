use crate::bark::{BarkConfig, BarkError, BarkMessage, BarkNotifier};
use std::sync::Arc;
use tracing::{error, info};

const ALERT_GROUP: &str = "market_data_provider";
const ALERT_SOUND: &str = "alarm";

/// Best effort: a failed alert is logged, never returned, because callers alert on their way to stopping.
pub(crate) trait SendAlert {
    async fn send_alert(&self, title: &str, body: &str);
}

/// Where alerts go: Bark pushes in release builds, the log in debug builds. Clones share one Bark client.
#[derive(Clone)]
pub(crate) enum Notifier {
    Bark(Arc<BarkNotifier>),
    /// Local development: no Bark credentials needed, every alert becomes an `info!` line.
    Log,
}

impl Notifier {
    /// Release builds require the `[bark]` section of the config; debug builds always log.
    pub(crate) fn new(
        release_build: bool,
        bark_config_option: Option<BarkConfig>,
    ) -> Result<Self, BarkError> {
        if !release_build {
            return Ok(Self::Log);
        }
        let bark_config = bark_config_option.ok_or(BarkError::MissingConfig)?;
        Ok(Self::Bark(Arc::new(BarkNotifier::new(bark_config)?)))
    }
}

impl SendAlert for Notifier {
    async fn send_alert(&self, title: &str, body: &str) {
        match self {
            Self::Bark(bark) => {
                let message = BarkMessage {
                    title: title.to_owned(),
                    body: body.to_owned(),
                    sound: ALERT_SOUND.to_owned(),
                    group: ALERT_GROUP.to_owned(),
                };
                if let Err(error) = bark.send(&message).await {
                    error!("failed to alert the phone: {error}; alert: {title}: {body}");
                }
            }
            Self::Log => info!("[push] {title}: {body}"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Mutex;

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

    #[test]
    fn debug_build_logs_without_bark_config() {
        let notifier = Notifier::new(false, None).expect("debug builds never need Bark");
        assert!(matches!(notifier, Notifier::Log));
    }

    #[test]
    fn release_build_requires_bark_config() {
        assert!(matches!(
            Notifier::new(true, None),
            Err(BarkError::MissingConfig)
        ));
    }

    #[test]
    fn release_build_pushes_through_bark() {
        let bark_config = toml::from_str(
            "device_key = \"device\"\nencryption_key = \"0123456789abcdefghijklmnopqrstuv\"",
        )
        .expect("the Bark config is expected to parse");
        let notifier = Notifier::new(true, Some(bark_config)).expect("the Bark config is valid");
        assert!(matches!(notifier, Notifier::Bark(_)));
    }

    #[tokio::test]
    async fn log_notifier_only_logs() {
        Notifier::Log.send_alert("title", "body").await;
    }
}

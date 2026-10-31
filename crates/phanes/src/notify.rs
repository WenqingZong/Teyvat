//! Phone notifications for every access decision and for crashes.
//!
//! Request handlers only enqueue a [`Notification`]; a background task delivers them
//! through a [`Notifier`] so a slow push never delays a response.

use log::{error, info};
use std::mem::take;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::UnboundedReceiver;

use crate::bark::{BarkConfig, BarkError, BarkMessage, BarkNotifier};
use crate::request_details::RequestDetails;
use crate::server::{Language, Service};

const GROUP: &str = "phanes";
const ALARM_SOUND: &str = "alarm";
/// Empty means Bark's default sound.
const DEFAULT_SOUND: &str = "";
const UNAUTHENTICATED_NOTIFY_INTERVAL: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notification {
    Started {
        listen_address: String,
    },
    Granted {
        client: String,
        key_id: String,
    },
    /// Gnosis only: an allowed, authenticated client was refused a key, e.g. not in its ACL,
    /// stale timestamp, replayed nonce or missing key file. See `DenialReason`.
    Denied {
        client: String,
        key_id: Option<String>,
        reason: String,
    },
    /// Passed the IP allow list (or none is mounted, in debug builds) but failed the Noise
    /// handshake, or a Celestia frame failed decryption. Throttled; `suppressed` counts unpushed ones.
    Unauthenticated {
        service: Service,
        request: RequestDetails,
        suppressed: u64,
    },
    /// Release builds only: `CF-Connecting-IP` is missing, unparsable (`client_ip` is `None`)
    /// or not in this service's allow list. Throttled like `Unauthenticated`.
    Blocked {
        service: Service,
        client_ip: Option<IpAddr>,
        request: RequestDetails,
        suppressed: u64,
    },
    Crashed {
        reason: String,
    },
}

impl Notification {
    pub fn to_bark_message(&self) -> BarkMessage {
        let (title, body, sound) = match self {
            Self::Started { listen_address } => (
                "天理降临".to_owned(),
                format!("天上永恒的王座到来，世界为之焕然一新。法涅斯开始监听: {listen_address}"),
                DEFAULT_SOUND,
            ),
            Self::Granted { client, key_id } => (
                "神之心已发放".to_owned(),
                format!("魔神: {client}\n神之心: {key_id}"),
                DEFAULT_SOUND,
            ),
            Self::Denied {
                client,
                key_id,
                reason,
            } => {
                let key_id = key_id.as_deref().unwrap_or("(无法解析)");
                (
                    "神之心请求被驳回".to_owned(),
                    format!("魔神: {client}\n神之心: {key_id}\n驳回原因: {reason}"),
                    ALARM_SOUND,
                )
            }
            Self::Unauthenticated {
                service,
                request,
                suppressed,
            } => {
                let service = service.to_string(Language::Chinese);
                let request = request.summarize();
                (
                    "检测到未授权的魔神".to_owned(),
                    format!("未授权的{service}\n{request}\n此前被合并的同类请求: {suppressed}"),
                    ALARM_SOUND,
                )
            }
            Self::Blocked {
                service,
                client_ip,
                request,
                suppressed,
            } => {
                let service = service.to_string(Language::Chinese);
                let client_ip = client_ip.map_or_else(
                    || "(缺失或无法解析)".to_owned(),
                    |address| address.to_string(),
                );
                let request = request.summarize();
                (
                    "检测到深渊入侵!!!".to_owned(),
                    format!(
                        "被拦截的{service}\n来源 IP: {client_ip}\n{request}\n此前被合并的同类请求: {suppressed}"
                    ),
                    ALARM_SOUND,
                )
            }
            Self::Crashed { reason } => (
                "天理陷入沉睡".to_owned(),
                format!("原因: {reason}"),
                ALARM_SOUND,
            ),
        };
        BarkMessage {
            title,
            body,
            sound: sound.to_owned(),
            group: GROUP.to_owned(),
        }
    }
}

/// Pushes at most one `Unauthenticated` per interval and counts the rest.
#[derive(Default)]
pub struct UnauthenticatedThrottle {
    last_notified: Option<Instant>,
    suppressed: u64,
}

impl UnauthenticatedThrottle {
    /// `Some(suppressed)` when a push is due.
    pub fn record(&mut self, now: Instant) -> Option<u64> {
        let due = self
            .last_notified
            .is_none_or(|last| now.duration_since(last) >= UNAUTHENTICATED_NOTIFY_INTERVAL);
        if due {
            self.last_notified = Some(now);
            Some(take(&mut self.suppressed))
        } else {
            self.suppressed += 1;
            None
        }
    }
}

/// Where notifications go: Bark pushes in release builds, the log in debug builds.
pub enum Notifier {
    Bark(BarkNotifier),
    /// Local development: no Bark credentials needed, every push becomes an `info!` line.
    Log,
}

impl Notifier {
    /// Release builds require the `[bark]` section of phanes.toml; debug builds always log.
    pub fn new(release_build: bool, bark_config: Option<BarkConfig>) -> Result<Self, BarkError> {
        if !release_build {
            return Ok(Self::Log);
        }
        let bark_config = bark_config.ok_or(BarkError::MissingConfig)?;
        Ok(Self::Bark(BarkNotifier::new(bark_config)?))
    }

    pub async fn send(&self, notification: &Notification) -> Result<(), BarkError> {
        let message = notification.to_bark_message();
        match self {
            Self::Bark(bark) => bark.send(&message).await,
            Self::Log => {
                let title = &message.title;
                let body = message.body.replace('\n', " | ");
                info!("[push] {title}: {body}");
                Ok(())
            }
        }
    }
}

/// Delivers queued notifications until every sender is dropped.
pub async fn run_dispatcher(
    notifier: Arc<Notifier>,
    mut receiver: UnboundedReceiver<Notification>,
) {
    while let Some(notification) = receiver.recv().await {
        if let Err(error) = notifier.send(&notification).await {
            error!("failed to push notification {notification:?}: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_details::tests::{PATH, request_head};
    use std::net::Ipv4Addr;

    /// Arbitrary documentation address (RFC 5737) with no business meaning.
    const BLOCKED_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7));
    /// Arbitrary count of merged notifications.
    const SUPPRESSED: u64 = 3;

    #[test]
    fn denied_message_contains_all_fields() {
        let message = Notification::Denied {
            client: "osaka".into(),
            key_id: Some("binance_demo_trading".into()),
            reason: "key not allowed".into(),
        }
        .to_bark_message();
        assert_eq!(message.title, "神之心请求被驳回");
        assert_eq!(message.sound, ALARM_SOUND);
        assert!(message.body.contains("osaka"));
        assert!(message.body.contains("binance_demo_trading"));
        assert!(message.body.contains("key not allowed"));
    }

    #[test]
    fn denied_message_without_key_id() {
        let message = Notification::Denied {
            client: "osaka".into(),
            key_id: None,
            reason: "malformed request".into(),
        }
        .to_bark_message();
        assert!(message.body.contains("(无法解析)"));
    }

    #[test]
    fn granted_message_contains_client_and_key() {
        let message = Notification::Granted {
            client: "osaka".into(),
            key_id: "binance_demo_trading".into(),
        }
        .to_bark_message();
        assert_eq!(message.title, "神之心已发放");
        assert_eq!(message.group, GROUP);
        assert!(message.body.contains("osaka"));
        assert!(message.body.contains("binance_demo_trading"));
    }

    #[test]
    fn debug_build_logs_without_bark_config() {
        let notifier = Notifier::new(false, None).expect("debug build never needs Bark");
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
    fn unauthenticated_message_names_service() {
        let request = RequestDetails::new(request_head(PATH), &[]);
        let message = Notification::Unauthenticated {
            service: Service::Celestia,
            request: request.clone(),
            suppressed: SUPPRESSED,
        }
        .to_bark_message();
        assert!(message.body.contains(&request.summarize()));
        assert!(
            message
                .body
                .contains(&Service::Celestia.to_string(Language::Chinese))
        );
        assert!(message.body.contains(&SUPPRESSED.to_string()));
    }

    #[test]
    fn blocked_message_names_service_and_address() {
        let request = RequestDetails::new(request_head(PATH), &[]);
        let message = Notification::Blocked {
            service: Service::Celestia,
            client_ip: Some(BLOCKED_IP),
            request: request.clone(),
            suppressed: SUPPRESSED,
        }
        .to_bark_message();
        assert!(message.body.contains(&request.summarize()));
        assert_eq!(message.sound, ALARM_SOUND);
        assert!(
            message
                .body
                .contains(&Service::Celestia.to_string(Language::Chinese))
        );
        assert!(message.body.contains(&BLOCKED_IP.to_string()));
        assert!(message.body.contains(&SUPPRESSED.to_string()));
    }

    #[test]
    fn blocked_message_without_address() {
        let message = Notification::Blocked {
            service: Service::Gnosis,
            client_ip: None,
            request: RequestDetails::new(request_head(PATH), &[]),
            suppressed: 0,
        }
        .to_bark_message();
        assert!(message.body.contains("(缺失或无法解析)"));
    }

    #[test]
    fn throttle_pushes_once_per_interval_and_counts_the_rest() {
        let mut throttle = UnauthenticatedThrottle::default();
        let start = Instant::now();
        assert_eq!(throttle.record(start), Some(0));
        assert_eq!(throttle.record(start), None);
        assert_eq!(throttle.record(start), None);
        assert_eq!(
            throttle.record(start + UNAUTHENTICATED_NOTIFY_INTERVAL),
            Some(2)
        );
    }

    #[tokio::test]
    async fn log_notifier_always_succeeds() {
        let notification = Notification::Crashed {
            reason: "test".into(),
        };
        assert!(Notifier::Log.send(&notification).await.is_ok());
    }
}

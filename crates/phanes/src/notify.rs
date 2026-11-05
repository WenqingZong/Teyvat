//! Phone notifications for every access decision and for crashes.
//!
//! Request handlers only enqueue a [`Notification`]; a background task delivers them
//! through a [`Notifier`] so a slow push never delays a response.

use std::mem::take;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::error;

use crate::clients::ClientName;
use crate::request_details::RequestDetails;
use crate::service::{Language, Service};
use layline::bark::{BarkMessage, Notifier};
use layline::gnosis::GnosisId;

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
        client: ClientName,
        gnosis_id: GnosisId,
    },
    /// Gnosis only: an authenticated client was refused a gnosis, e.g. not in its
    /// `allowed_gnosis_ids`, stale timestamp or replayed nonce. See `DenialReason`.
    Denied {
        client: ClientName,
        gnosis_id: Option<GnosisId>,
        reason: String,
    },
    /// Passed the IP allow list (or none is mounted, in debug builds) but failed the Noise
    /// handshake, signed with a key of no client or of a client the address does not belong to,
    /// or a Celestia frame failed decryption. Throttled; `suppressed` counts unpushed ones.
    Unauthenticated {
        service: Service,
        request: RequestDetails,
        suppressed: u64,
    },
    /// Release builds only: `CF-Connecting-IP` is missing, unparsable (`client_ip` is `None`)
    /// or belongs to no client. Throttled like `Unauthenticated`.
    Blocked {
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
            Self::Granted { client, gnosis_id } => (
                "神之心已发放".to_owned(),
                format!("魔神: {client}\n神之心: {gnosis_id}"),
                DEFAULT_SOUND,
            ),
            Self::Denied {
                client,
                gnosis_id,
                reason,
            } => {
                let gnosis_id =
                    gnosis_id.map_or_else(|| "(无法解析)".to_owned(), |id| id.to_string());
                (
                    "神之心请求被驳回".to_owned(),
                    format!("魔神: {client}\n神之心: {gnosis_id}\n驳回原因: {reason}"),
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
                client_ip,
                request,
                suppressed,
            } => {
                let client_ip = client_ip.map_or_else(
                    || "(缺失或无法解析)".to_owned(),
                    |address| address.to_string(),
                );
                let request = request.summarize();
                (
                    "检测到深渊入侵!!!".to_owned(),
                    format!("来源 IP: {client_ip}\n{request}\n此前被合并的同类请求: {suppressed}"),
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

/// Queues `notification` for the dispatcher, so a handler never waits on a push.
pub fn enqueue(notification_sender: &UnboundedSender<Notification>, notification: Notification) {
    if notification_sender.send(notification).is_err() {
        error!("notification dispatcher is gone; push dropped");
    }
}

/// Queues at most one notification per interval, through an [`UnauthenticatedThrottle`].
pub struct ThrottledNotificationSender {
    notification_sender: UnboundedSender<Notification>,
    throttle: Mutex<UnauthenticatedThrottle>,
}

impl ThrottledNotificationSender {
    pub fn new(notification_sender: UnboundedSender<Notification>) -> Self {
        Self {
            notification_sender,
            throttle: Mutex::default(),
        }
    }

    /// `build` receives how many notifications were suppressed since the last one queued.
    pub fn enqueue(&self, build: impl FnOnce(u64) -> Notification) {
        let suppressed_option = self
            .throttle
            .lock()
            .expect("throttle mutex is not poisoned")
            .record(Instant::now());
        if let Some(suppressed) = suppressed_option {
            enqueue(&self.notification_sender, build(suppressed));
        }
    }
}

/// Delivers queued notifications until every sender is dropped.
pub async fn run_dispatcher(
    notifier: Arc<Notifier>,
    mut receiver: UnboundedReceiver<Notification>,
) {
    while let Some(notification) = receiver.recv().await {
        if let Err(error) = notifier.send(&notification.to_bark_message()).await {
            error!("failed to push notification {notification:?}: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::request_details::tests::{PATH, request_head};

    /// Arbitrary documentation address (RFC 5737) with no business meaning.
    const BLOCKED_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7));
    /// Arbitrary count of merged notifications.
    const SUPPRESSED: u64 = 3;

    #[test]
    fn denied_message_contains_all_fields() {
        let message = Notification::Denied {
            client: "osaka".into(),
            gnosis_id: Some(GnosisId::BinanceDemo),
            reason: "gnosis not allowed".into(),
        }
        .to_bark_message();
        assert_eq!(message.title, "神之心请求被驳回");
        assert_eq!(message.sound, ALARM_SOUND);
        assert!(message.body.contains("osaka"));
        assert!(message.body.contains("binance_demo"));
        assert!(message.body.contains("gnosis not allowed"));
    }

    #[test]
    fn denied_message_without_gnosis_id() {
        let message = Notification::Denied {
            client: "osaka".into(),
            gnosis_id: None,
            reason: "malformed request".into(),
        }
        .to_bark_message();
        assert!(message.body.contains("(无法解析)"));
    }

    #[test]
    fn granted_message_contains_client_and_gnosis() {
        let message = Notification::Granted {
            client: "osaka".into(),
            gnosis_id: GnosisId::BinanceDemo,
        }
        .to_bark_message();
        assert_eq!(message.title, "神之心已发放");
        assert_eq!(message.group, GROUP);
        assert!(message.body.contains("osaka"));
        assert!(message.body.contains("binance_demo"));
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
    fn blocked_message_names_address_and_request() {
        let request = RequestDetails::new(request_head(PATH), &[]);
        let message = Notification::Blocked {
            client_ip: Some(BLOCKED_IP),
            request: request.clone(),
            suppressed: SUPPRESSED,
        }
        .to_bark_message();
        assert!(message.body.contains(&request.summarize()));
        assert_eq!(message.sound, ALARM_SOUND);
        assert!(message.body.contains(&BLOCKED_IP.to_string()));
        assert!(message.body.contains(&SUPPRESSED.to_string()));
    }

    #[test]
    fn blocked_message_without_address() {
        let message = Notification::Blocked {
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
}

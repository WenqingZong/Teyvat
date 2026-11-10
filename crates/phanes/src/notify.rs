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
        reason: String,
        /// The client the address belongs to; `None` in debug builds, which have no IP filter.
        ip_client_name_option: Option<ClientName>,
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
    /// A reporting process finished the Celestia handshake: `client` is the machine its key
    /// belongs to, `process_name` the process its `Hello` named.
    CelestiaConnected {
        client: ClientName,
        process_name: String,
    },
    /// A reporting process connected again while Phanes still held its old connection, whose
    /// link had died unnoticed; `silence` is how long ago the old connection last heard from it.
    CelestiaReconnected {
        client: ClientName,
        process_name: String,
        silence: Duration,
    },
    /// A reporting process closed its connection on purpose; `reason` is the one it gave, such as
    /// the signal that stopped it.
    CelestiaClosed {
        client: ClientName,
        process_name: String,
        reason: String,
    },
    /// The connection of a reporting process was lost: a dropped link, an idle timeout or an
    /// error, which `reason` names.
    CelestiaDisconnected {
        client: ClientName,
        process_name: String,
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
                reason,
                ip_client_name_option,
                request,
                suppressed,
            } => {
                let service = service.to_string(Language::Chinese);
                let ip_client_name = ip_client_name_option.as_ref().map_or_else(
                    || "(debug 构建无 IP 白名单)".to_owned(),
                    |name| format!("{name} (仅凭 IP，未经密钥验证)"),
                );
                let request = request.summarize();
                (
                    "检测到未授权的魔神".to_owned(),
                    format!(
                        "未授权的{service}\n原因: {reason}\n来源 IP 对应的魔神: {ip_client_name}\n\
                         {request}\n此前被合并的同类请求: {suppressed}"
                    ),
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
            Self::CelestiaConnected {
                client,
                process_name,
            } => (
                "已连接至天空岛".to_owned(),
                format_reporter(client, process_name),
                DEFAULT_SOUND,
            ),
            Self::CelestiaReconnected {
                client,
                process_name,
                silence,
            } => {
                let reporter = format_reporter(client, process_name);
                let silence_seconds = silence.as_secs();
                (
                    "已重新连接至天空岛".to_owned(),
                    format!("{reporter}\n旧连接最后一条消息: {silence_seconds} 秒前"),
                    DEFAULT_SOUND,
                )
            }
            Self::CelestiaClosed {
                client,
                process_name,
                reason,
            } => {
                let reporter = format_reporter(client, process_name);
                (
                    "魔神断绝了与天空岛的连接".to_owned(),
                    format!("{reporter}\n原因: {reason}"),
                    DEFAULT_SOUND,
                )
            }
            Self::CelestiaDisconnected {
                client,
                process_name,
                reason,
            } => {
                let reporter = format_reporter(client, process_name);
                (
                    "天空岛监控丢失".to_owned(),
                    format!("{reporter}\n原因: {reason}"),
                    DEFAULT_SOUND,
                )
            }
        };
        BarkMessage {
            title,
            body,
            sound: sound.to_owned(),
            group: GROUP.to_owned(),
        }
    }
}

/// The body of the Celestia connection pushes: which machine, and which process on it.
fn format_reporter(client: &ClientName, process_name: &str) -> String {
    format!("魔神: {client}\n服务: {process_name}")
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
    fn unauthenticated_message_names_service_reason_and_ip_client() {
        let request = RequestDetails::new(request_head(PATH), &[]);
        let message = Notification::Unauthenticated {
            service: Service::Celestia,
            reason: "noise: decrypt error".into(),
            ip_client_name_option: Some("osaka".into()),
            request: request.clone(),
            suppressed: SUPPRESSED,
        }
        .to_bark_message();
        assert!(message.body.contains(&request.summarize()));
        assert!(message.body.contains("原因: noise: decrypt error"));
        assert!(message.body.contains("osaka (仅凭 IP，未经密钥验证)"));
        assert!(
            message
                .body
                .contains(&Service::Celestia.to_string(Language::Chinese))
        );
        assert!(message.body.contains(&SUPPRESSED.to_string()));
    }

    #[test]
    fn unauthenticated_message_without_ip_filter() {
        let message = Notification::Unauthenticated {
            service: Service::Gnosis,
            reason: "noise: decrypt error".into(),
            ip_client_name_option: None,
            request: RequestDetails::new(request_head(PATH), &[]),
            suppressed: 0,
        }
        .to_bark_message();
        assert!(message.body.contains("(debug 构建无 IP 白名单)"));
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
    fn celestia_connected_message_names_client_and_process() {
        let message = Notification::CelestiaConnected {
            client: "Osaka".into(),
            process_name: "market_data_provider".to_owned(),
        }
        .to_bark_message();
        assert_eq!(message.title, "已连接至天空岛");
        assert_eq!(message.body, "魔神: Osaka\n服务: market_data_provider");
        assert_eq!(message.sound, DEFAULT_SOUND);
        assert_eq!(message.group, GROUP);
    }

    #[test]
    fn celestia_disconnected_message_names_client_process_and_reason() {
        let message = Notification::CelestiaDisconnected {
            client: "Osaka".into(),
            process_name: "market_data_provider".to_owned(),
            reason: "no message within 90s".to_owned(),
        }
        .to_bark_message();
        assert_eq!(message.title, "天空岛监控丢失");
        assert_eq!(
            message.body,
            "魔神: Osaka\n服务: market_data_provider\n原因: no message within 90s"
        );
        assert_eq!(message.sound, DEFAULT_SOUND);
        assert_eq!(message.group, GROUP);
    }

    #[test]
    fn celestia_closed_message_names_client_process_and_reason() {
        let message = Notification::CelestiaClosed {
            client: "Osaka".into(),
            process_name: "market_data_provider".to_owned(),
            reason: "SIGINT".to_owned(),
        }
        .to_bark_message();
        assert_eq!(message.title, "魔神断绝了与天空岛的连接");
        assert_eq!(
            message.body,
            "魔神: Osaka\n服务: market_data_provider\n原因: SIGINT"
        );
    }

    #[test]
    fn celestia_reconnected_message_names_client_process_and_silence() {
        let message = Notification::CelestiaReconnected {
            client: "Osaka".into(),
            process_name: "market_data_provider".to_owned(),
            silence: Duration::from_secs(42),
        }
        .to_bark_message();
        assert_eq!(message.title, "已重新连接至天空岛");
        assert_eq!(
            message.body,
            "魔神: Osaka\n服务: market_data_provider\n旧连接最后一条消息: 42 秒前"
        );
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

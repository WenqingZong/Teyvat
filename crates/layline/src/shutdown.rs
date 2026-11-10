//! The signals that stop a service: SIGINT (Ctrl-C) and SIGTERM (`systemctl stop`).

use std::future::pending;

use strum::Display;
use tokio::select;
use tokio::signal::ctrl_c;
use tokio::signal::unix::{SignalKind, signal};
use tracing::error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum ShutdownSignal {
    #[strum(serialize = "SIGINT")]
    Interrupt,
    #[strum(serialize = "SIGTERM")]
    Terminate,
}

/// Waits for SIGINT or SIGTERM. A signal that cannot be listened for is logged and never arrives;
/// if neither can, this never returns and the service keeps running, since only SIGKILL could stop
/// it then anyway.
pub async fn shutdown_signal() -> ShutdownSignal {
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            error!("cannot listen for SIGTERM: {error}");
            return match ctrl_c().await {
                Ok(()) => ShutdownSignal::Interrupt,
                Err(error) => {
                    error!("cannot listen for ctrl-c: {error}");
                    pending().await
                }
            };
        }
    };
    select! {
        _ = terminate.recv() => ShutdownSignal::Terminate,
        result = ctrl_c() => match result {
            Ok(()) => ShutdownSignal::Interrupt,
            // The SIGTERM handler is already installed and has replaced the default action, so
            // giving up on it here would leave the process deaf to `systemctl stop`.
            Err(error) => {
                error!("cannot listen for ctrl-c: {error}");
                terminate.recv().await;
                ShutdownSignal::Terminate
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::process::{Command, id};
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    /// Long enough for a signal sent to this process to arrive.
    const SIGNAL_WAIT: Duration = Duration::from_secs(5);
    /// Enough for the first poll, which installs the handlers, so the signal never takes its
    /// default action of killing the test process.
    const LISTEN_WAIT: Duration = Duration::from_millis(100);

    /// Both signals in one test, since a signal reaches every listener in the process.
    #[tokio::test]
    async fn each_signal_is_named() {
        for (signal_name, expected) in [
            ("INT", ShutdownSignal::Interrupt),
            ("TERM", ShutdownSignal::Terminate),
        ] {
            let mut shutdown = pin!(shutdown_signal());
            assert!(timeout(LISTEN_WAIT, &mut shutdown).await.is_err());
            let process_id = id().to_string();
            let status = Command::new("kill")
                .args([format!("-{signal_name}"), process_id])
                .status()
                .expect("kill runs");
            assert!(status.success());
            let received = timeout(SIGNAL_WAIT, shutdown)
                .await
                .expect("the signal arrives");
            assert_eq!(received, expected);
            assert_eq!(received.to_string(), format!("SIG{signal_name}"));
        }
    }
}

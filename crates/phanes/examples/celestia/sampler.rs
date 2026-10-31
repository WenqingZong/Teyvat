//! Per-process CPU, memory and network sampling from `/proc` and systemd.
use std::io::Error as IoError;
use std::time::{Duration, Instant};

use tokio::fs::read_to_string;
use tokio::process::Command;

use layline::time::since_unix_epoch;
use phanes::celestia::proto::{NetworkRate, ProcessSample};

/// `/proc` reports CPU time in USER_HZ ticks, which Linux fixes at 100 per second.
const TICKS_PER_SECOND: f64 = 100.0;
const PERCENT: f64 = 100.0;
const BYTES_PER_KIBIBYTE: u64 = 1024;
/// Index of utime among the `/proc/<pid>/stat` fields after the command name; stime follows.
const USER_TIME_FIELD_INDEX: usize = 11;
const RESIDENT_MEMORY_PREFIX: &str = "VmRSS:";
/// systemd reports this when IP accounting is off.
const SYSTEMD_UNSET_COUNTER: u64 = u64::MAX;
/// systemd reports MainPID 0 for a unit that is not running.
const NOT_RUNNING_PROCESS_ID: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    ProcessId(u32),
    /// Network rates need `IPAccounting=yes` on the unit; otherwise they read 0.
    SystemdUnit(String),
}

#[derive(Debug, thiserror::Error)]
pub enum SampleError {
    #[error("cannot read {what}: {source}")]
    Read { what: String, source: IoError },
    #[error("cannot parse {0}")]
    Parse(String),
    #[error("systemd unit {0} is not running")]
    NotRunning(String),
}

/// Zero when unavailable: a bare process id, or a unit without IP accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetworkCounters {
    pub received_bytes: u64,
    pub sent_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitStatus {
    pub main_process_id: u32,
    pub network: NetworkCounters,
}

/// Cumulative counters at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reading {
    pub process_id: u32,
    pub cpu_ticks: u64,
    pub resident_memory_bytes: u64,
    pub network: NetworkCounters,
}

/// Turns cumulative readings into per-second rates; the first reading only primes it.
pub struct Sampler {
    target: Target,
    previous: Option<(Instant, Reading)>,
}

impl Sampler {
    pub fn new(target: Target) -> Self {
        Self {
            target,
            previous: None,
        }
    }

    pub async fn sample(&mut self) -> Result<Option<ProcessSample>, SampleError> {
        let reading = read(&self.target).await?;
        Ok(self.record(Instant::now(), since_unix_epoch(), reading))
    }

    pub fn record(
        &mut self,
        now: Instant,
        timestamp: Duration,
        reading: Reading,
    ) -> Option<ProcessSample> {
        let (previous_instant, previous) = self.previous.replace((now, reading))?;
        let elapsed_seconds = now.duration_since(previous_instant).as_secs_f64();
        // A restarted process starts its counters from zero again.
        if previous.process_id != reading.process_id
            || reading.cpu_ticks < previous.cpu_ticks
            || elapsed_seconds <= 0.0
        {
            return None;
        }
        let cpu_seconds = (reading.cpu_ticks - previous.cpu_ticks) as f64 / TICKS_PER_SECOND;
        let (before, after) = (previous.network, reading.network);
        // Reset counters (unit restarted, accounting toggled) read as no traffic for one sample.
        let network = if after.received_bytes >= before.received_bytes
            && after.sent_bytes >= before.sent_bytes
        {
            NetworkRate {
                received_bytes_per_second: (after.received_bytes - before.received_bytes) as f64
                    / elapsed_seconds,
                sent_bytes_per_second: (after.sent_bytes - before.sent_bytes) as f64
                    / elapsed_seconds,
            }
        } else {
            NetworkRate::default()
        };
        Some(ProcessSample {
            timestamp,
            cpu_percent: cpu_seconds / elapsed_seconds * PERCENT,
            resident_memory_bytes: reading.resident_memory_bytes,
            network,
        })
    }
}

async fn read(target: &Target) -> Result<Reading, SampleError> {
    let (process_id, network) = match target {
        Target::ProcessId(process_id) => (*process_id, NetworkCounters::default()),
        Target::SystemdUnit(unit) => {
            let status = read_unit_status(unit).await?;
            (status.main_process_id, status.network)
        }
    };
    let stat_path = format!("/proc/{process_id}/stat");
    let status_path = format!("/proc/{process_id}/status");
    let stat = read_file(&stat_path).await?;
    let status = read_file(&status_path).await?;
    Ok(Reading {
        process_id,
        cpu_ticks: parse_cpu_ticks(&stat).ok_or(SampleError::Parse(stat_path))?,
        resident_memory_bytes: parse_resident_memory_bytes(&status)
            .ok_or(SampleError::Parse(status_path))?,
        network,
    })
}

async fn read_file(path: &str) -> Result<String, SampleError> {
    read_to_string(path)
        .await
        .map_err(|source| SampleError::Read {
            what: path.to_owned(),
            source,
        })
}

async fn read_unit_status(unit: &str) -> Result<UnitStatus, SampleError> {
    let command = format!("systemctl show {unit}");
    let output = Command::new("systemctl")
        .args([
            "show",
            unit,
            "--property=MainPID,IPIngressBytes,IPEgressBytes",
        ])
        .output()
        .await
        .map_err(|source| SampleError::Read {
            what: command.clone(),
            source,
        })?;
    let status = parse_systemctl_show(&String::from_utf8_lossy(&output.stdout))
        .ok_or(SampleError::Parse(command))?;
    if status.main_process_id == NOT_RUNNING_PROCESS_ID {
        return Err(SampleError::NotRunning(unit.to_owned()));
    }
    Ok(status)
}

/// utime + stime, fields 14 and 15 of `/proc/<pid>/stat`.
pub fn parse_cpu_ticks(stat: &str) -> Option<u64> {
    // The command name (field 2) may contain spaces, so count fields after its closing ')'.
    let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_whitespace();
    let user_ticks: u64 = fields.nth(USER_TIME_FIELD_INDEX)?.parse().ok()?;
    let system_ticks: u64 = fields.next()?.parse().ok()?;
    Some(user_ticks + system_ticks)
}

pub fn parse_resident_memory_bytes(status: &str) -> Option<u64> {
    let line = status
        .lines()
        .find(|line| line.starts_with(RESIDENT_MEMORY_PREFIX))?;
    let kibibytes: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kibibytes * BYTES_PER_KIBIBYTE)
}

pub fn parse_systemctl_show(output: &str) -> Option<UnitStatus> {
    let mut main_process_id = None;
    let mut received_bytes = None;
    let mut sent_bytes = None;
    for line in output.lines() {
        match line.split_once('=') {
            Some(("MainPID", value)) => main_process_id = value.parse().ok(),
            Some(("IPIngressBytes", value)) => received_bytes = value.parse::<u64>().ok(),
            Some(("IPEgressBytes", value)) => sent_bytes = value.parse::<u64>().ok(),
            _ => {}
        }
    }
    let network = match (received_bytes, sent_bytes) {
        (Some(received_bytes), Some(sent_bytes))
            if received_bytes != SYSTEMD_UNSET_COUNTER && sent_bytes != SYSTEMD_UNSET_COUNTER =>
        {
            NetworkCounters {
                received_bytes,
                sent_bytes,
            }
        }
        _ => NetworkCounters::default(),
    };
    Some(UnitStatus {
        main_process_id: main_process_id?,
        network,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::id as current_process_id;
    use tokio::time::sleep;

    const STAT: &str = "1234 (my (weird) app) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 4 0 100 1000000 500 18446744073709551615";

    fn reading(process_id: u32, cpu_ticks: u64, network: Option<(u64, u64)>) -> Reading {
        Reading {
            process_id,
            cpu_ticks,
            resident_memory_bytes: 4096,
            network: network
                .map(|(received_bytes, sent_bytes)| NetworkCounters {
                    received_bytes,
                    sent_bytes,
                })
                .unwrap_or_default(),
        }
    }

    #[test]
    fn parses_cpu_ticks_with_parentheses_in_command_name() {
        assert_eq!(parse_cpu_ticks(STAT), Some(300));
        assert_eq!(parse_cpu_ticks("1234 (app) S 1"), None);
        assert_eq!(parse_cpu_ticks("no parenthesis"), None);
    }

    #[test]
    fn parses_resident_memory() {
        let status = "Name:\tapp\nVmPeak:\t  9000 kB\nVmRSS:\t    4736 kB\n";
        assert_eq!(parse_resident_memory_bytes(status), Some(4736 * 1024));
        assert_eq!(parse_resident_memory_bytes("Name:\tkthread\n"), None);
    }

    #[test]
    fn parses_systemctl_show() {
        let accounted = "MainPID=42\nIPIngressBytes=1000\nIPEgressBytes=2000\n";
        assert_eq!(
            parse_systemctl_show(accounted),
            Some(UnitStatus {
                main_process_id: 42,
                network: NetworkCounters {
                    received_bytes: 1000,
                    sent_bytes: 2000
                },
            })
        );
        let unaccounted =
            "MainPID=42\nIPIngressBytes=[not set]\nIPEgressBytes=18446744073709551615\n";
        assert_eq!(
            parse_systemctl_show(unaccounted),
            Some(UnitStatus {
                main_process_id: 42,
                network: NetworkCounters::default()
            })
        );
        assert_eq!(parse_systemctl_show("IPIngressBytes=1\n"), None);
    }

    #[test]
    fn computes_rates_from_consecutive_readings() {
        let mut sampler = Sampler::new(Target::ProcessId(1));
        let start = Instant::now();
        assert!(
            sampler
                .record(start, Duration::ZERO, reading(1, 100, Some((0, 0))))
                .is_none()
        );
        let sample = sampler
            .record(
                start + Duration::from_secs(2),
                Duration::from_secs(2),
                reading(1, 150, Some((4000, 1000))),
            )
            .expect("second reading yields a sample");
        assert_eq!(sample.timestamp, Duration::from_secs(2));
        assert!((sample.cpu_percent - 25.0).abs() < f64::EPSILON);
        assert_eq!(sample.resident_memory_bytes, 4096);
        assert_eq!(
            sample.network,
            NetworkRate {
                received_bytes_per_second: 2000.0,
                sent_bytes_per_second: 500.0,
            }
        );
    }

    #[test]
    fn restarted_process_skips_one_sample() {
        let mut sampler = Sampler::new(Target::SystemdUnit("app".into()));
        let start = Instant::now();
        sampler.record(start, Duration::ZERO, reading(1, 500, None));
        assert!(
            sampler
                .record(
                    start + Duration::from_secs(1),
                    Duration::ZERO,
                    reading(2, 10, None)
                )
                .is_none()
        );
        let sample = sampler
            .record(
                start + Duration::from_secs(2),
                Duration::ZERO,
                reading(2, 20, None),
            )
            .expect("sampling resumes after a restart");
        assert!((sample.cpu_percent - 10.0).abs() < f64::EPSILON);
        assert_eq!(sample.network, NetworkRate::default());
    }

    #[test]
    fn reset_network_counters_read_as_zero() {
        let mut sampler = Sampler::new(Target::ProcessId(1));
        let start = Instant::now();
        sampler.record(start, Duration::ZERO, reading(1, 0, Some((5000, 5000))));
        let sample = sampler
            .record(
                start + Duration::from_secs(1),
                Duration::ZERO,
                reading(1, 0, Some((10, 10))),
            )
            .expect("cpu and memory are still sampled");
        assert_eq!(sample.network, NetworkRate::default());
    }

    #[tokio::test]
    async fn samples_own_process() {
        let mut sampler = Sampler::new(Target::ProcessId(current_process_id()));
        assert!(
            sampler
                .sample()
                .await
                .expect("first read succeeds")
                .is_none()
        );
        sleep(Duration::from_millis(20)).await;
        let sample = sampler
            .sample()
            .await
            .expect("second read succeeds")
            .expect("second read yields a sample");
        assert!(sample.resident_memory_bytes > 0);
        assert!(sample.cpu_percent >= 0.0);
    }

    #[tokio::test]
    async fn missing_systemd_unit_is_an_error() {
        let mut sampler = Sampler::new(Target::SystemdUnit("phanes-test-missing.service".into()));
        assert!(sampler.sample().await.is_err());
    }

    #[tokio::test]
    async fn missing_process_is_an_error() {
        let mut sampler = Sampler::new(Target::ProcessId(u32::MAX));
        assert!(matches!(
            sampler.sample().await,
            Err(SampleError::Read { .. })
        ));
    }
}

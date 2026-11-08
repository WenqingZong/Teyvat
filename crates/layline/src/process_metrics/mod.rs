//! The process's own CPU, memory, threads and TCP traffic, read from `/proc/self` and netlink
//! `sock_diag` by a task on the process's tokio runtime, logged every interval and optionally handed
//! on to be reported to Phanes Celestia. The kernel answers these reads from memory, without taking
//! locks the other threads hold; they hold up the task's worker for 1 to 3 ms per interval, so the
//! task belongs on a runtime off the hot path.

mod proc_files_parser;
mod tcp_sockets;

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter, Result as FormatResult};
use std::fs::{read_dir, read_link, read_to_string};
use std::io::ErrorKind;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{MissedTickBehavior, interval as tick_every};
use tracing::{info, warn};

use crate::time::{NANOSECONDS_PER_SECOND, get_current_timestamp_nanoseconds};
pub use proc_files_parser::MemoryUsage;
use proc_files_parser::{
    CPU_TICKS_PER_SECOND, ContextSwitches, parse_context_switches, parse_memory_usage,
    parse_socket_inode, parse_stat,
};
use tcp_sockets::{TcpByteCount, read_tcp_byte_counts};

const PROCESS_STAT_PATH: &str = "/proc/self/stat";
const PROCESS_STATUS_PATH: &str = "/proc/self/status";
const THREADS_DIRECTORY: &str = "/proc/self/task";
const FILE_DESCRIPTORS_DIRECTORY: &str = "/proc/self/fd";
/// ESRCH: the thread exited after its directory was listed.
const NO_SUCH_PROCESS_ERROR_CODE: i32 = 3;
const PERCENT: f32 = 100.0;
const KIBIBYTES_PER_MEBIBYTE: f32 = 1024.0;
/// Explains the numbers of each thread in a [`ProcessSample::format_threads`] line.
pub const THREAD_METRICS_LEGEND: &str = "cpu, voluntary/involuntary context switches per second";

/// Logs a [`ProcessSample`] every `interval`, the first one `interval` after the start, and hands
/// each to `sample_sender_option` when there is one. Never returns. It never stops the process: a
/// sample it cannot read is skipped with a warning, and it never waits for the receiver, dropping a
/// sample the receiver has no room for.
pub async fn run_process_monitor(
    interval: Duration,
    sample_sender_option: Option<Sender<ProcessSample>>,
) {
    let mut sampler = ProcessSampler::default();
    // The first tick is immediate; its reading only primes the sampler.
    let mut ticker = tick_every(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let sample = match sampler.sample() {
            Ok(Some(sample)) => sample,
            Ok(None) => continue,
            Err(error) => {
                warn!("cannot sample process metrics: {error:#}");
                continue;
            }
        };
        log_sample(&sample);
        if let Some(sample_sender) = &sample_sender_option {
            match sample_sender.try_send(sample) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(
                        "the reporter has not taken the previous process sample, dropping this one"
                    )
                }
                Err(TrySendError::Closed(_)) => {
                    warn!("the reporter is gone, process sample not reported")
                }
            }
        }
    }
}

fn log_sample(sample: &ProcessSample) {
    info!("process metrics: {sample}");
    let threads = sample.format_threads();
    info!("thread metrics ({THREAD_METRICS_LEGEND}): {threads}");
}

/// Rates over the interval since the previous reading; memory and file descriptors as of now.
/// Rates are `f32`: seven significant digits are plenty for monitoring, at half the size on the wire.
/// Times are whole numbers in the units ClickHouse stores them in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessSample {
    /// When the reading finished, in seconds since the Unix epoch (UTC): a ClickHouse `DateTime`.
    pub timestamp_seconds: NonZeroU32,
    /// 100.0 means one full core.
    pub cpu_percent: f32,
    pub memory: MemoryUsage,
    /// Including the one the monitor holds while listing them.
    pub open_file_descriptors: u32,
    pub voluntary_context_switches_per_second: f32,
    pub involuntary_context_switches_per_second: f32,
    pub tcp_received_bytes_per_second: f32,
    pub tcp_sent_bytes_per_second: f32,
    /// Sorted by thread id, so the main thread comes first.
    pub threads: Vec<ThreadSample>,
    /// How long reading the counters took, i.e. the monitor's own cost per sample.
    pub sampling_duration_microseconds: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadSample {
    pub thread_id: u32,
    pub name: String,
    pub cpu_percent: f32,
    pub voluntary_context_switches_per_second: f32,
    pub involuntary_context_switches_per_second: f32,
}

impl ProcessSample {
    /// Every thread on one line, separated by `; `; [`THREAD_METRICS_LEGEND`] explains the numbers.
    pub fn format_threads(&self) -> String {
        self.threads
            .iter()
            .map(ThreadSample::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    }
}

impl Display for ProcessSample {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let cpu_percent = self.cpu_percent;
        let anonymous_mebibytes = self.memory.anonymous_kibibytes as f32 / KIBIBYTES_PER_MEBIBYTE;
        let file_mebibytes = self.memory.file_kibibytes as f32 / KIBIBYTES_PER_MEBIBYTE;
        let shared_mebibytes = self.memory.shared_memory_kibibytes as f32 / KIBIBYTES_PER_MEBIBYTE;
        let thread_count = self.threads.len();
        let open_file_descriptors = self.open_file_descriptors;
        let voluntary = self.voluntary_context_switches_per_second;
        let involuntary = self.involuntary_context_switches_per_second;
        let received = self.tcp_received_bytes_per_second;
        let sent = self.tcp_sent_bytes_per_second;
        let sampling_duration_microseconds = self.sampling_duration_microseconds;
        write!(
            formatter,
            "cpu {cpu_percent:.1}%, memory anon {anonymous_mebibytes:.1} MiB / file {file_mebibytes:.1} MiB / shmem {shared_mebibytes:.1} MiB, {thread_count} threads, {open_file_descriptors} fds, context switches {voluntary:.1}/s voluntary / {involuntary:.1}/s involuntary, tcp received {received:.0} B/s / sent {sent:.0} B/s, sampled in {sampling_duration_microseconds} µs"
        )
    }
}

impl Display for ThreadSample {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        let Self {
            thread_id,
            name,
            cpu_percent,
            voluntary_context_switches_per_second: voluntary,
            involuntary_context_switches_per_second: involuntary,
        } = self;
        write!(
            formatter,
            "{name}#{thread_id} {cpu_percent:.1}% {voluntary:.1}/{involuntary:.1}"
        )
    }
}

/// Cumulative counters at one moment.
struct ProcessReading {
    taken_at: Instant,
    /// utime + stime of every thread, exited ones included.
    cpu_ticks: u64,
    memory: MemoryUsage,
    open_file_descriptors: u32,
    threads_by_id: HashMap<u32, ThreadReading>,
    tcp_byte_counts_by_inode: HashMap<u32, TcpByteCount>,
}

struct ThreadReading {
    name: String,
    cpu_ticks: u64,
    context_switches: ContextSwitches,
}

struct OpenFileDescriptors {
    count: u32,
    socket_inodes: HashSet<u32>,
}

/// Turns consecutive readings into samples; the first reading only primes it.
#[derive(Default)]
struct ProcessSampler {
    previous_reading: Option<ProcessReading>,
}

impl ProcessSampler {
    fn sample(&mut self) -> Result<Option<ProcessSample>> {
        let reading = read_process()?;
        let sampling_duration = reading.taken_at.elapsed();
        let timestamp_seconds = get_current_timestamp_seconds();
        let sample_option = self.previous_reading.as_ref().map(|previous| {
            compute_sample(previous, &reading, timestamp_seconds, sampling_duration)
        });
        self.previous_reading = Some(reading);
        Ok(sample_option)
    }
}

/// Whole seconds are plenty for a sample every few seconds, and fit ClickHouse's `DateTime`.
fn get_current_timestamp_seconds() -> NonZeroU32 {
    let seconds = get_current_timestamp_nanoseconds().get() / NANOSECONDS_PER_SECOND;
    u32::try_from(seconds)
        .ok()
        .and_then(NonZeroU32::new)
        .expect("the clock reads a time after 1970-01-01 00:00:01 and before 2106")
}

fn compute_sample(
    previous: &ProcessReading,
    current: &ProcessReading,
    timestamp_seconds: NonZeroU32,
    sampling_duration: Duration,
) -> ProcessSample {
    let elapsed_seconds = current
        .taken_at
        .duration_since(previous.taken_at)
        .as_secs_f32();
    // Readings are a whole interval apart, so a monotonic clock that did not advance means the
    // machine is broken, and so is everything timed on it.
    assert!(
        elapsed_seconds > 0.0,
        "the monotonic clock did not advance between two process readings"
    );
    let per_second = |count: u64| count as f32 / elapsed_seconds;
    // Multiplying first keeps whole tick counts exact in f32.
    let cpu_percent = |ticks: u64| ticks as f32 * PERCENT / CPU_TICKS_PER_SECOND / elapsed_seconds;

    let mut threads: Vec<ThreadSample> = current
        .threads_by_id
        .iter()
        .map(|(thread_id, thread)| {
            let previous_thread_option = previous.threads_by_id.get(thread_id);
            let previous_switches_option =
                previous_thread_option.map(|previous_thread| previous_thread.context_switches);
            ThreadSample {
                thread_id: *thread_id,
                name: thread.name.clone(),
                cpu_percent: cpu_percent(increase_since(
                    previous_thread_option.map(|previous_thread| previous_thread.cpu_ticks),
                    thread.cpu_ticks,
                )),
                voluntary_context_switches_per_second: per_second(increase_since(
                    previous_switches_option.map(|switches| switches.voluntary),
                    thread.context_switches.voluntary,
                )),
                involuntary_context_switches_per_second: per_second(increase_since(
                    previous_switches_option.map(|switches| switches.involuntary),
                    thread.context_switches.involuntary,
                )),
            }
        })
        .collect();
    threads.sort_by_key(|thread| thread.thread_id);

    let (mut received_bytes, mut sent_bytes) = (0, 0);
    for (inode, count) in &current.tcp_byte_counts_by_inode {
        let previous_count_option = previous.tcp_byte_counts_by_inode.get(inode);
        received_bytes += increase_since(
            previous_count_option.map(|previous_count| previous_count.received_bytes),
            count.received_bytes,
        );
        sent_bytes += increase_since(
            previous_count_option.map(|previous_count| previous_count.sent_bytes),
            count.sent_bytes,
        );
    }

    ProcessSample {
        timestamp_seconds,
        cpu_percent: cpu_percent(current.cpu_ticks.saturating_sub(previous.cpu_ticks)),
        memory: current.memory,
        open_file_descriptors: current.open_file_descriptors,
        voluntary_context_switches_per_second: threads
            .iter()
            .map(|thread| thread.voluntary_context_switches_per_second)
            .sum(),
        involuntary_context_switches_per_second: threads
            .iter()
            .map(|thread| thread.involuntary_context_switches_per_second)
            .sum(),
        tcp_received_bytes_per_second: per_second(received_bytes),
        tcp_sent_bytes_per_second: per_second(sent_bytes),
        threads,
        // Saturates past 71 minutes, which a sample never takes.
        sampling_duration_microseconds: u32::try_from(sampling_duration.as_micros())
            .unwrap_or(u32::MAX),
    }
}

/// A counter first seen this interval, or one that went back because its thread id or socket
/// inode was reused, counts from zero.
fn increase_since(previous_option: Option<u64>, current: u64) -> u64 {
    match previous_option {
        Some(previous) if previous <= current => current - previous,
        _ => current,
    }
}

fn read_process() -> Result<ProcessReading> {
    let taken_at = Instant::now();
    let stat = read_file(PROCESS_STAT_PATH)?;
    let (_name, cpu_ticks) =
        parse_stat(&stat).with_context(|| format!("cannot parse {PROCESS_STAT_PATH}"))?;
    let status = read_file(PROCESS_STATUS_PATH)?;
    let memory = parse_memory_usage(&status)
        .with_context(|| format!("cannot parse {PROCESS_STATUS_PATH}"))?;
    let open_file_descriptors = read_open_file_descriptors()?;
    Ok(ProcessReading {
        taken_at,
        cpu_ticks,
        memory,
        open_file_descriptors: open_file_descriptors.count,
        threads_by_id: read_threads()?,
        tcp_byte_counts_by_inode: read_tcp_byte_counts(&open_file_descriptors.socket_inodes)?,
    })
}

fn read_open_file_descriptors() -> Result<OpenFileDescriptors> {
    let mut count = 0;
    let mut socket_inodes = HashSet::new();
    for entry_result in read_dir(FILE_DESCRIPTORS_DIRECTORY)
        .with_context(|| format!("cannot list {FILE_DESCRIPTORS_DIRECTORY}"))?
    {
        let path = entry_result?.path();
        // Another thread may close the descriptor between the listing and here.
        let link_target = match read_link(&path) {
            Ok(link_target) => link_target,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                let path = path.display();
                return Err(error).with_context(|| format!("cannot read link {path}"));
            }
        };
        count += 1;
        if let Some(inode) = parse_socket_inode(&link_target.to_string_lossy()) {
            socket_inodes.insert(inode);
        }
    }
    Ok(OpenFileDescriptors {
        count,
        socket_inodes,
    })
}

fn read_threads() -> Result<HashMap<u32, ThreadReading>> {
    let mut threads_by_id = HashMap::new();
    for entry_result in
        read_dir(THREADS_DIRECTORY).with_context(|| format!("cannot list {THREADS_DIRECTORY}"))?
    {
        let file_name = entry_result?.file_name();
        let file_name = file_name.to_string_lossy();
        let thread_id: u32 = file_name
            .parse()
            .with_context(|| format!("{THREADS_DIRECTORY}/{file_name} is not a thread id"))?;
        if let Some(thread) = read_thread(thread_id)? {
            threads_by_id.insert(thread_id, thread);
        }
    }
    Ok(threads_by_id)
}

/// `None` when the thread exited after it was listed.
fn read_thread(thread_id: u32) -> Result<Option<ThreadReading>> {
    let stat_path = format!("{THREADS_DIRECTORY}/{thread_id}/stat");
    let status_path = format!("{THREADS_DIRECTORY}/{thread_id}/status");
    let (Some(stat), Some(status)) = (
        read_file_of_live_thread(&stat_path)?,
        read_file_of_live_thread(&status_path)?,
    ) else {
        return Ok(None);
    };
    let (name, cpu_ticks) =
        parse_stat(&stat).with_context(|| format!("cannot parse {stat_path}"))?;
    let context_switches =
        parse_context_switches(&status).with_context(|| format!("cannot parse {status_path}"))?;
    Ok(Some(ThreadReading {
        name,
        cpu_ticks,
        context_switches,
    }))
}

fn read_file(path: &str) -> Result<String> {
    read_to_string(path).with_context(|| format!("cannot read {path}"))
}

/// `None` when the file's thread has exited.
fn read_file_of_live_thread(path: &str) -> Result<Option<String>> {
    match read_to_string(path) {
        Ok(content) => Ok(Some(content)),
        Err(error)
            if error.kind() == ErrorKind::NotFound
                || error.raw_os_error() == Some(NO_SUCH_PROCESS_ERROR_CODE) =>
        {
            Ok(None)
        }
        Err(error) => Err(error).with_context(|| format!("cannot read {path}")),
    }
}

#[cfg(test)]
mod tests {
    use tokio::spawn;
    use tokio::sync::mpsc::channel;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::time::since_unix_epoch;

    const MEBIBYTE_IN_KIBIBYTES: u32 = 1024;
    const SAMPLE_WAIT: Duration = Duration::from_secs(5);
    const CURRENT_THREAD_LINK: &str = "/proc/thread-self";
    const TIMESTAMP_SECONDS: NonZeroU32 = NonZeroU32::new(1_800_000_000).expect("not zero");
    /// Generous bound between taking a timestamp and checking it.
    const MAX_TIMESTAMP_DRIFT_SECONDS: u64 = 5;
    const MONITOR_INTERVAL: Duration = Duration::from_millis(10);

    fn thread_reading(
        name: &str,
        cpu_ticks: u64,
        voluntary: u64,
        involuntary: u64,
    ) -> ThreadReading {
        ThreadReading {
            name: name.to_owned(),
            cpu_ticks,
            context_switches: ContextSwitches {
                voluntary,
                involuntary,
            },
        }
    }

    fn tcp_count(received_bytes: u64, sent_bytes: u64) -> TcpByteCount {
        TcpByteCount {
            received_bytes,
            sent_bytes,
        }
    }

    fn process_reading(
        taken_at: Instant,
        cpu_ticks: u64,
        threads_by_id: HashMap<u32, ThreadReading>,
        tcp_byte_counts_by_inode: HashMap<u32, TcpByteCount>,
    ) -> ProcessReading {
        ProcessReading {
            taken_at,
            cpu_ticks,
            memory: MemoryUsage {
                anonymous_kibibytes: 3 * MEBIBYTE_IN_KIBIBYTES,
                file_kibibytes: 2 * MEBIBYTE_IN_KIBIBYTES,
                shared_memory_kibibytes: MEBIBYTE_IN_KIBIBYTES,
            },
            open_file_descriptors: 9,
            threads_by_id,
            tcp_byte_counts_by_inode,
        }
    }

    #[test]
    fn consecutive_readings_yield_rates() {
        let start = Instant::now();
        let previous = process_reading(
            start,
            1000,
            HashMap::from([(10, thread_reading("main", 900, 5, 1))]),
            HashMap::from([(100, tcp_count(1000, 500)), (101, tcp_count(70, 70))]),
        );
        let current = process_reading(
            start + Duration::from_secs(2),
            1300,
            HashMap::from([
                (10, thread_reading("main", 1100, 9, 5)),
                // Started during the interval: all of its counts are new.
                (11, thread_reading("worker", 100, 2, 0)),
            ]),
            // Socket 101 closed; socket 102 opened during the interval.
            HashMap::from([(100, tcp_count(5000, 2500)), (102, tcp_count(1000, 1000))]),
        );
        let sample = compute_sample(
            &previous,
            &current,
            TIMESTAMP_SECONDS,
            Duration::from_micros(80),
        );
        assert_eq!(
            ProcessSample {
                timestamp_seconds: TIMESTAMP_SECONDS,
                cpu_percent: 150.0,
                memory: current.memory,
                open_file_descriptors: 9,
                voluntary_context_switches_per_second: 3.0,
                involuntary_context_switches_per_second: 2.0,
                tcp_received_bytes_per_second: 2500.0,
                tcp_sent_bytes_per_second: 1500.0,
                threads: vec![
                    ThreadSample {
                        thread_id: 10,
                        name: "main".to_owned(),
                        cpu_percent: 100.0,
                        voluntary_context_switches_per_second: 2.0,
                        involuntary_context_switches_per_second: 2.0,
                    },
                    ThreadSample {
                        thread_id: 11,
                        name: "worker".to_owned(),
                        cpu_percent: 50.0,
                        voluntary_context_switches_per_second: 1.0,
                        involuntary_context_switches_per_second: 0.0,
                    },
                ],
                sampling_duration_microseconds: 80,
            },
            sample
        );
    }

    #[test]
    fn reused_thread_id_counts_from_zero() {
        let start = Instant::now();
        let previous = process_reading(
            start,
            0,
            HashMap::from([(10, thread_reading("old", 900, 50, 50))]),
            HashMap::new(),
        );
        let current = process_reading(
            start + Duration::from_secs(1),
            0,
            HashMap::from([(10, thread_reading("new", 30, 4, 0))]),
            HashMap::new(),
        );
        let sample = compute_sample(&previous, &current, TIMESTAMP_SECONDS, Duration::ZERO);
        assert_eq!(30.0, sample.threads[0].cpu_percent);
        assert_eq!(4.0, sample.threads[0].voluntary_context_switches_per_second);
    }

    #[test]
    #[should_panic(expected = "the monotonic clock did not advance")]
    fn readings_at_the_same_instant_are_a_broken_clock() {
        let start = Instant::now();
        let previous = process_reading(start, 0, HashMap::new(), HashMap::new());
        let current = process_reading(start, 10, HashMap::new(), HashMap::new());
        compute_sample(&previous, &current, TIMESTAMP_SECONDS, Duration::ZERO);
    }

    #[test]
    fn samples_display_on_one_line_each() {
        let sample = ProcessSample {
            timestamp_seconds: TIMESTAMP_SECONDS,
            cpu_percent: 99.84,
            memory: MemoryUsage {
                anonymous_kibibytes: 6 * MEBIBYTE_IN_KIBIBYTES,
                file_kibibytes: 9 * MEBIBYTE_IN_KIBIBYTES,
                shared_memory_kibibytes: 16 * MEBIBYTE_IN_KIBIBYTES,
            },
            open_file_descriptors: 12,
            voluntary_context_switches_per_second: 2.1,
            involuntary_context_switches_per_second: 0.4,
            tcp_received_bytes_per_second: 912.4,
            tcp_sent_bytes_per_second: 300.0,
            threads: vec![ThreadSample {
                thread_id: 1234,
                name: "dummy_strategy".to_owned(),
                cpu_percent: 99.7,
                voluntary_context_switches_per_second: 0.0,
                involuntary_context_switches_per_second: 0.3,
            }],
            sampling_duration_microseconds: 85,
        };
        assert_eq!(
            "cpu 99.8%, memory anon 6.0 MiB / file 9.0 MiB / shmem 16.0 MiB, 1 threads, 12 fds, context switches 2.1/s voluntary / 0.4/s involuntary, tcp received 912 B/s / sent 300 B/s, sampled in 85 µs",
            sample.to_string()
        );
        assert_eq!("dummy_strategy#1234 99.7% 0.0/0.3", sample.format_threads());
    }

    #[test]
    fn own_process_is_sampled_from_the_second_reading_on() {
        let mut sampler = ProcessSampler::default();
        assert_eq!(None, sampler.sample().expect("reading own counters works"));
        let sample = sampler
            .sample()
            .expect("reading own counters works")
            .expect("the second reading yields a sample");
        assert!(sample.memory.anonymous_kibibytes > 0);
        assert!(sample.memory.file_kibibytes > 0);
        assert!(sample.open_file_descriptors > 0);
        let now_seconds = since_unix_epoch().as_secs();
        assert!(
            now_seconds.abs_diff(u64::from(sample.timestamp_seconds.get()))
                <= MAX_TIMESTAMP_DRIFT_SECONDS
        );
        // `/proc/thread-self` links to `<pid>/task/<tid>` of the calling thread.
        let thread_link_target =
            read_link(CURRENT_THREAD_LINK).expect("the current thread's link exists");
        let current_thread_id: u32 = thread_link_target
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .and_then(|file_name| file_name.parse().ok())
            .expect("the link ends in a thread id");
        assert!(
            sample
                .threads
                .iter()
                .any(|thread| thread.thread_id == current_thread_id),
            "{sample:?}"
        );
    }

    /// Samples the receiver has no room for are dropped, and a gone receiver stops nothing.
    #[tokio::test]
    async fn monitor_hands_on_samples_without_waiting() {
        let (sample_sender, mut sample_receiver) = channel(1);
        let monitor = spawn(run_process_monitor(MONITOR_INTERVAL, Some(sample_sender)));
        // Several samples arrive meanwhile; the channel holds one and the rest are dropped.
        sleep(MONITOR_INTERVAL * 5).await;
        for _ in 0..2 {
            let sample = timeout(SAMPLE_WAIT, sample_receiver.recv())
                .await
                .expect("a sample arrives in time")
                .expect("the monitor keeps its sender");
            assert!(!sample.threads.is_empty());
        }
        drop(sample_receiver);
        sleep(MONITOR_INTERVAL * 3).await;
        assert!(!monitor.is_finished());
    }

    #[tokio::test]
    async fn monitor_without_a_receiver_keeps_running() {
        let monitor = spawn(run_process_monitor(MONITOR_INTERVAL, None));
        sleep(MONITOR_INTERVAL * 3).await;
        assert!(!monitor.is_finished());
    }

    #[test]
    fn missing_file_is_an_error_unless_its_thread_exited() {
        let missing_path = format!("{THREADS_DIRECTORY}/0/stat");
        let error = read_file(&missing_path).expect_err("the file does not exist");
        assert!(error.to_string().contains(&missing_path), "{error}");
        assert!(
            read_file_of_live_thread(&missing_path)
                .expect("a missing file means the thread exited")
                .is_none()
        );
        assert!(read_thread(0).expect("thread 0 never exists").is_none());
        assert!(read_file_of_live_thread("/proc/self").is_err());
    }
}

//! Reads this process's own counters. `/proc` parsing is delegated to `procfs` on Linux; every
//! other platform has no process metrics.

use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::tcp_sockets::TcpByteCount;

/// Resident memory split by kind; their sum is VmRSS. In KiB, the unit `/proc` reports them in,
/// so nothing is rounded; `u32` holds up to 4 TiB.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryUsage {
    /// Private to this process: heap, stacks.
    pub anonymous_kibibytes: u32,
    /// Mapped files, mostly the binary and its libraries.
    pub file_kibibytes: u32,
    /// Shared memory such as the market data rings in `/dev/shm`, which every process mapping
    /// them counts again.
    pub shared_memory_kibibytes: u32,
}

/// Cumulative counts of one thread since it started, from its `/proc/self/task/<tid>/status`;
/// `u64` like the kernel's, since a busy thread passes `u32::MAX` within months. Only the rates
/// derived from them leave the process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ContextSwitches {
    /// The thread gave up the CPU: it blocked or slept.
    pub(super) voluntary: u64,
    /// The scheduler took the CPU away, e.g. to run another thread on the same core.
    pub(super) involuntary: u64,
}

/// Cumulative counters at one moment.
pub(super) struct ProcessReading {
    pub(super) taken_at: Instant,
    /// utime + stime of every thread, exited ones included.
    pub(super) cpu_ticks: u64,
    pub(super) memory: MemoryUsage,
    pub(super) open_file_descriptors: u32,
    pub(super) threads_by_id: HashMap<u32, ThreadReading>,
    pub(super) tcp_byte_counts_by_inode: HashMap<u32, TcpByteCount>,
}

pub(super) struct ThreadReading {
    pub(super) name: String,
    pub(super) cpu_ticks: u64,
    pub(super) context_switches: ContextSwitches,
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::{HashMap, HashSet};
    use std::time::Instant;

    use anyhow::{Context, Result};
    use procfs::ProcError;
    use procfs::process::{FDTarget, Process, Status, Task};

    use super::{ContextSwitches, MemoryUsage, ProcessReading, ThreadReading};
    use crate::process_metrics::tcp_sockets::linux::read_tcp_byte_counts;

    /// The file descriptors this process holds, and the inodes of the sockets among them.
    struct OpenFileDescriptors {
        count: u32,
        socket_inodes: HashSet<u32>,
    }

    pub(crate) fn read() -> Result<ProcessReading> {
        let taken_at = Instant::now();
        let process = Process::myself().context("cannot open /proc/self")?;
        let stat = process.stat().context("cannot read /proc/self/stat")?;
        let status = process.status().context("cannot read /proc/self/status")?;
        let memory = memory_usage(&status)?;
        let open_file_descriptors = read_open_file_descriptors(&process)?;
        let threads_by_id = read_threads(&process)?;
        let tcp_byte_counts_by_inode = read_tcp_byte_counts(&open_file_descriptors.socket_inodes)?;
        Ok(ProcessReading {
            taken_at,
            cpu_ticks: stat.utime.saturating_add(stat.stime),
            memory,
            open_file_descriptors: open_file_descriptors.count,
            threads_by_id,
            tcp_byte_counts_by_inode,
        })
    }

    fn memory_usage(status: &Status) -> Result<MemoryUsage> {
        fn kibibytes(value: Option<u64>, field: &str) -> Result<u32> {
            let value =
                value.with_context(|| format!("{field} is missing from /proc/self/status"))?;
            u32::try_from(value).with_context(|| format!("{field} does not fit u32"))
        }
        Ok(MemoryUsage {
            anonymous_kibibytes: kibibytes(status.rssanon, "RssAnon")?,
            file_kibibytes: kibibytes(status.rssfile, "RssFile")?,
            shared_memory_kibibytes: kibibytes(status.rssshmem, "RssShmem")?,
        })
    }

    fn read_open_file_descriptors(process: &Process) -> Result<OpenFileDescriptors> {
        let mut count = 0u32;
        let mut socket_inodes = HashSet::new();
        for fd_result in process.fd().context("cannot list /proc/self/fd")? {
            let fd = match fd_result {
                Ok(fd) => fd,
                Err(ProcError::NotFound(_)) => continue,
                Err(error) => {
                    return Err(error).context("cannot read a /proc/self/fd entry");
                }
            };
            count = count.saturating_add(1);
            if let FDTarget::Socket(inode) = fd.target {
                socket_inodes
                    .insert(u32::try_from(inode).context("socket inode does not fit in u32")?);
            }
        }
        Ok(OpenFileDescriptors {
            count,
            socket_inodes,
        })
    }

    fn read_threads(process: &Process) -> Result<HashMap<u32, ThreadReading>> {
        let mut threads_by_id = HashMap::new();
        for task_result in process.tasks().context("cannot list /proc/self/task")? {
            let task = match task_result {
                Ok(task) => task,
                Err(ProcError::NotFound(_)) => continue,
                Err(error) => {
                    return Err(error).context("cannot read a /proc/self/task entry");
                }
            };
            let thread_id = u32::try_from(task.tid).context("thread id does not fit in u32")?;
            if let Some(thread) = read_thread(&task)? {
                threads_by_id.insert(thread_id, thread);
            }
        }
        Ok(threads_by_id)
    }

    /// `None` when the thread exited after it was listed.
    fn read_thread(task: &Task) -> Result<Option<ThreadReading>> {
        let stat = match task.stat() {
            Ok(stat) => stat,
            Err(ProcError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error).context("cannot read a thread's stat"),
        };
        let status = match task.status() {
            Ok(status) => status,
            Err(ProcError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error).context("cannot read a thread's status"),
        };
        let context_switches = ContextSwitches {
            voluntary: status
                .voluntary_ctxt_switches
                .context("thread status lacks voluntary context switches")?,
            involuntary: status
                .nonvoluntary_ctxt_switches
                .context("thread status lacks involuntary context switches")?,
        };
        Ok(Some(ThreadReading {
            name: stat.comm,
            cpu_ticks: stat.utime.saturating_add(stat.stime),
            context_switches,
        }))
    }
}

#[cfg(target_os = "linux")]
pub(super) use linux::read;

#[cfg(not(target_os = "linux"))]
pub(super) fn read() -> anyhow::Result<ProcessReading> {
    use anyhow::anyhow;
    Err(anyhow!("process metrics are only supported on Linux"))
}

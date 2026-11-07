//! Parsers for the `/proc/self` text files the process monitor reads.

/// `/proc` reports CPU time in USER_HZ ticks, which Linux fixes at 100 per second.
pub const CPU_TICKS_PER_SECOND: f32 = 100.0;
/// Index of utime among the `/proc/<pid>/stat` fields after the command name; stime follows.
const USER_TIME_FIELD_INDEX: usize = 11;
const ANONYMOUS_MEMORY_PREFIX: &str = "RssAnon:";
const FILE_MEMORY_PREFIX: &str = "RssFile:";
const SHARED_MEMORY_PREFIX: &str = "RssShmem:";
const VOLUNTARY_CONTEXT_SWITCHES_PREFIX: &str = "voluntary_ctxt_switches:";
const INVOLUNTARY_CONTEXT_SWITCHES_PREFIX: &str = "nonvoluntary_ctxt_switches:";
const SOCKET_LINK_PREFIX: &str = "socket:[";
const SOCKET_LINK_SUFFIX: &str = "]";

/// Resident memory split by kind; their sum is VmRSS. In KiB, the unit `/proc` reports them in,
/// so nothing is rounded; `u32` holds up to 4 TiB.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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
pub struct ContextSwitches {
    /// The thread gave up the CPU: it blocked or slept.
    pub voluntary: u64,
    /// The scheduler took the CPU away, e.g. to run another thread on the same core.
    pub involuntary: u64,
}

/// The command name and utime + stime of `/proc/<pid>/stat` or `/proc/<pid>/task/<tid>/stat`.
pub fn parse_stat(stat: &str) -> Option<(String, u64)> {
    // The command name (field 2) is in parentheses and may itself contain spaces or ')'.
    let name_start = stat.find('(')? + 1;
    let name_end = stat.rfind(')')?;
    let name = stat.get(name_start..name_end)?.to_owned();
    let mut fields = stat.get(name_end + 1..)?.split_whitespace();
    let user_ticks: u64 = fields.nth(USER_TIME_FIELD_INDEX)?.parse().ok()?;
    let system_ticks: u64 = fields.next()?.parse().ok()?;
    Some((name, user_ticks + system_ticks))
}

pub fn parse_memory_usage(status: &str) -> Option<MemoryUsage> {
    Some(MemoryUsage {
        anonymous_kibibytes: find_kibibytes(status, ANONYMOUS_MEMORY_PREFIX)?,
        file_kibibytes: find_kibibytes(status, FILE_MEMORY_PREFIX)?,
        shared_memory_kibibytes: find_kibibytes(status, SHARED_MEMORY_PREFIX)?,
    })
}

/// Only meaningful for a thread's status: a process's `/proc/self/status` counts its main
/// thread alone.
pub fn parse_context_switches(status: &str) -> Option<ContextSwitches> {
    Some(ContextSwitches {
        voluntary: find_number(status, VOLUNTARY_CONTEXT_SWITCHES_PREFIX)?,
        involuntary: find_number(status, INVOLUNTARY_CONTEXT_SWITCHES_PREFIX)?,
    })
}

/// The inode of a `/proc/self/fd/<n>` link target such as `socket:[12345]`; `None` for files,
/// pipes and everything else that is not a socket.
pub fn parse_socket_inode(link_target: &str) -> Option<u32> {
    link_target
        .strip_prefix(SOCKET_LINK_PREFIX)?
        .strip_suffix(SOCKET_LINK_SUFFIX)?
        .parse()
        .ok()
}

/// `RssAnon: 4736 kB` style lines, whatever whitespace follows the colon.
fn find_kibibytes(status: &str, prefix: &str) -> Option<u32> {
    let line = status.lines().find(|line| line.starts_with(prefix))?;
    line.get(prefix.len()..)?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// `voluntary_ctxt_switches: 42` style lines, whatever whitespace follows the colon.
fn find_number(status: &str, prefix: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with(prefix))?;
    line.get(prefix.len()..)?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAT: &str = "1234 (my (weird) app) S 1 1234 1234 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 4 0 100 1000000 500 18446744073709551615";
    const STATUS: &str = "Name:\tapp\nVmRSS:\t    7000 kB\nRssAnon:\t    4000 kB\nRssFile:\t    2000 kB\nRssShmem:\t    1000 kB\nThreads:\t3\nvoluntary_ctxt_switches:\t42\nnonvoluntary_ctxt_switches:\t7\n";

    #[test]
    fn stat_yields_name_and_cpu_ticks() {
        assert_eq!(Some(("my (weird) app".to_owned(), 300)), parse_stat(STAT));
    }

    #[test]
    fn truncated_or_malformed_stat_is_refused() {
        assert_eq!(None, parse_stat("1234 (app) S 1"));
        assert_eq!(None, parse_stat("no parenthesis"));
        assert_eq!(None, parse_stat(&STAT.replace(" 250 ", " x ")));
    }

    #[test]
    fn status_yields_memory_by_kind() {
        assert_eq!(
            Some(MemoryUsage {
                anonymous_kibibytes: 4000,
                file_kibibytes: 2000,
                shared_memory_kibibytes: 1000,
            }),
            parse_memory_usage(STATUS)
        );
        assert_eq!(None, parse_memory_usage("Name:\tkthread\n"));
    }

    #[test]
    fn status_yields_context_switches() {
        assert_eq!(
            Some(ContextSwitches {
                voluntary: 42,
                involuntary: 7,
            }),
            parse_context_switches(STATUS)
        );
        assert_eq!(
            None,
            parse_context_switches("voluntary_ctxt_switches:\t42\n")
        );
    }

    #[test]
    fn only_socket_links_yield_an_inode() {
        assert_eq!(Some(12345), parse_socket_inode("socket:[12345]"));
        assert_eq!(None, parse_socket_inode("pipe:[12345]"));
        assert_eq!(None, parse_socket_inode("/dev/null"));
        assert_eq!(None, parse_socket_inode("socket:[not a number]"));
    }
}

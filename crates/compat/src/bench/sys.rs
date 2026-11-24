//! What the kernel says about the machine and about one run: procfs counters, the machine
//! record, and a cold page cache. Linux only; elsewhere every reader answers "unknown" so the
//! harness builds and runs on a laptop as a check of itself (never as a measurement).

use std::fs;
use std::path::Path;
use std::process::Command;

/// Reads a file to a string, empty when it cannot be read (procfs is absent off Linux).
fn read(path: &str) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// The kernel's memory-reclaim counters of one moment, from `/proc/vmstat`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Vm {
    /// Page faults that read from disk, system wide.
    pub major_faults: u64,
    /// Pages the reclaim scanners looked at (kswapd and direct).
    pub scanned: u64,
    /// Pages reclaim took back (kswapd and direct).
    pub stolen: u64,
    /// Evicted file pages that were wanted again soon after (the working set refault count).
    pub refaults: u64,
    /// Times an allocation had to reclaim for itself.
    pub alloc_stalls: u64,
}

impl Vm {
    /// This moment's counters; zeros when `/proc/vmstat` does not exist.
    pub(super) fn now() -> Option<Self> {
        let text = read("/proc/vmstat");
        if text.is_empty() {
            return None;
        }
        let mut vm = Self::default();
        for line in text.lines() {
            let mut parts = line.split_whitespace();
            let (Some(name), Some(value)) = (parts.next(), parts.next()) else {
                continue;
            };
            let Ok(value) = value.parse::<u64>() else {
                continue;
            };
            match name {
                "pgmajfault" => vm.major_faults = value,
                "pgscan_kswapd" | "pgscan_direct" => vm.scanned = vm.scanned.saturating_add(value),
                "pgsteal_kswapd" | "pgsteal_direct" => vm.stolen = vm.stolen.saturating_add(value),
                "workingset_refault_file" | "workingset_refault" => {
                    vm.refaults = vm.refaults.max(value);
                }
                other if other.starts_with("allocstall") => {
                    vm.alloc_stalls = vm.alloc_stalls.saturating_add(value);
                }
                _ => {}
            }
        }
        Some(vm)
    }

    /// What happened between `self` and the later `then`.
    pub(super) const fn since(self, then: Self) -> Self {
        Self {
            major_faults: then.major_faults.saturating_sub(self.major_faults),
            scanned: then.scanned.saturating_sub(self.scanned),
            stolen: then.stolen.saturating_sub(self.stolen),
            refaults: then.refaults.saturating_sub(self.refaults),
            alloc_stalls: then.alloc_stalls.saturating_sub(self.alloc_stalls),
        }
    }
}

/// CPU time the hypervisor took away, as clock ticks: (steal, all) from the first line of
/// `/proc/stat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Steal {
    steal: u64,
    total: u64,
}

impl Steal {
    /// The counters now; `None` when `/proc/stat` does not exist.
    pub(super) fn now() -> Option<Self> {
        let text = read("/proc/stat");
        let line = text.lines().next()?;
        let fields: Vec<u64> = line
            .split_whitespace()
            .skip(1)
            .filter_map(|f| f.parse().ok())
            .collect();
        let total = fields.iter().fold(0_u64, |a, f| a.saturating_add(*f));
        // user nice system idle iowait irq softirq steal
        Some(Self {
            steal: fields.get(7).copied().unwrap_or(0),
            total,
        })
    }

    /// The share of CPU time stolen between `self` and `then`, in percent.
    pub(super) fn percent_until(self, then: Self) -> f64 {
        let all = then.total.saturating_sub(self.total);
        if all == 0 {
            return 0.0;
        }
        super::to_f64(then.steal.saturating_sub(self.steal)) * 100.0 / super::to_f64(all)
    }
}

/// A process's bytes read: (all `read` calls, bytes the storage layer was asked for),
/// from `/proc/<pid>/io`.
pub(super) fn proc_read_bytes(pid: u32) -> Option<(u64, u64)> {
    let text = read(&format!("/proc/{pid}/io"));
    let field = |name: &str| -> Option<u64> {
        text.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.trim().parse().ok())
    };
    Some((field("rchar:")?, field("read_bytes:")?))
}

/// This process's major page faults, from `/proc/self/stat` (field 12, after the command's
/// closing parenthesis).
pub(super) fn self_major_faults() -> Option<u64> {
    let text = read("/proc/self/stat");
    let after = text.rsplit_once(')')?.1;
    // After the command: state ppid pgrp session tty tpgid flags minflt cminflt majflt
    after.split_whitespace().nth(9)?.parse().ok()
}

/// Runs a command and returns its trimmed output, empty when it cannot run.
fn tool(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .replace('\n', " | ")
        })
        .unwrap_or_default()
}

/// Empties the page cache so a run starts cold, and says how: the whole cache when the harness
/// may write `/proc/sys/vm/drop_caches`, else this file's pages alone (GNU `dd` with
/// `iflag=nocache` asks the kernel to drop them), else not at all.
pub(super) fn drop_page_cache(db: &Path) -> &'static str {
    drop(Command::new("sync").status());
    let quiet = || std::process::Stdio::null();
    if fs::write("/proc/sys/vm/drop_caches", "3").is_ok() {
        return "drop_caches=3";
    }
    let dropped = Command::new("dd")
        .arg(format!("if={}", db.display()))
        .args(["iflag=nocache", "count=0", "status=none"])
        .stderr(quiet())
        .status()
        .is_ok_and(|s| s.success());
    if dropped {
        "dd iflag=nocache (the database file only)"
    } else {
        "not dropped (no privilege, no GNU dd)"
    }
}

/// The block device's name under `/sys/block` for the filesystem holding `path`.
fn block_device(path: &Path) -> Option<String> {
    let source = tool(
        "findmnt",
        &["-n", "-o", "SOURCE", "-T", &path.display().to_string()],
    );
    let source = source.split_whitespace().next()?.to_owned();
    let parent = tool("lsblk", &["-no", "pkname", &source]);
    let name = parent.split_whitespace().next().map_or_else(
        || source.trim_start_matches("/dev/").to_owned(),
        str::to_owned,
    );
    Path::new("/sys/block").join(&name).exists().then_some(name)
}

/// One line of the machine record per fact, for the top of every result.
pub(super) fn machine_record(on: &Path) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    let cpuinfo = read("/proc/cpuinfo");
    let model = cpuinfo
        .lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_owned())
        .unwrap_or_else(|| tool("sysctl", &["-n", "machdep.cpu.brand_string"]));
    rows.push(("cpu".to_owned(), model));
    let cores = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);
    rows.push(("logical cpus".to_owned(), cores.to_string()));
    let meminfo = read("/proc/meminfo");
    for key in ["MemTotal", "MemAvailable", "SwapTotal"] {
        let value = meminfo
            .lines()
            .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix(':')))
            .map_or_else(|| "unknown".to_owned(), |v| v.trim().to_owned());
        rows.push((key.to_lowercase(), value));
    }
    rows.push(("kernel".to_owned(), tool("uname", &["-srvm"])));
    rows.push((
        "swappiness".to_owned(),
        read("/proc/sys/vm/swappiness").trim().to_owned(),
    ));
    rows.push((
        "governor".to_owned(),
        read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            .trim()
            .to_owned(),
    ));
    rows.push((
        "transparent hugepages".to_owned(),
        read("/sys/kernel/mm/transparent_hugepage/enabled")
            .trim()
            .to_owned(),
    ));
    let target = on.display().to_string();
    rows.push((
        "filesystem".to_owned(),
        tool(
            "findmnt",
            &["-n", "-o", "SOURCE,FSTYPE,OPTIONS", "-T", &target],
        ),
    ));
    if let Some(dev) = block_device(on) {
        for (label, file) in [
            ("io scheduler", "queue/scheduler"),
            ("rotational", "queue/rotational"),
            ("read_ahead_kb", "queue/read_ahead_kb"),
            ("nr_requests", "queue/nr_requests"),
        ] {
            rows.push((
                format!("{dev} {label}"),
                read(&format!("/sys/block/{dev}/{file}")).trim().to_owned(),
            ));
        }
        rows.push((
            format!("{dev} model"),
            tool("lsblk", &["-dno", "MODEL,SIZE", &format!("/dev/{dev}")]),
        ));
    }
    rows.push(("rustc".to_owned(), tool("rustc", &["--version"])));
    rows.push(("docker".to_owned(), tool("docker", &["--version"])));
    rows
}

/// Sequential read bandwidth of `file` in MB/s, by `dd` with direct I/O over its first `mib` MiB,
/// the number the copy's napkin floor is divided by. `None` when dd or direct I/O is not there.
pub(super) fn disk_read_mb_s(file: &Path, mib: u64) -> Option<f64> {
    let start = super::clock::now();
    let status = Command::new("dd")
        .arg(format!("if={}", file.display()))
        .args(["of=/dev/null", "bs=1M", "iflag=direct", "status=none"])
        .arg(format!("count={mib}"))
        .stderr(std::process::Stdio::null())
        .status()
        .ok()?;
    let took = super::secs(super::clock::now().duration_since(start));
    (status.success() && took > 0.0).then(|| super::to_f64(mib) * 1.048_576 / took)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_differences_and_never_go_negative() {
        let before = Vm {
            major_faults: 10,
            scanned: 5,
            stolen: 4,
            refaults: 3,
            alloc_stalls: 2,
        };
        let after = Vm {
            major_faults: 15,
            scanned: 5,
            stolen: 9,
            refaults: 1,
            alloc_stalls: 2,
        };
        let d = before.since(after);
        assert_eq!((d.major_faults, d.scanned, d.stolen), (5, 0, 5));
        assert_eq!(
            d.refaults, 0,
            "a counter that went down is a restart, not a negative"
        );
    }

    #[test]
    fn steal_is_a_share_of_all_ticks() {
        let a = Steal {
            steal: 10,
            total: 1000,
        };
        let b = Steal {
            steal: 60,
            total: 2000,
        };
        assert!((a.percent_until(b) - 5.0).abs() < 1e-9);
        assert!(a.percent_until(a).abs() < 1e-9);
    }

    #[test]
    fn this_process_reports_its_faults_where_procfs_exists() {
        if cfg!(target_os = "linux") {
            assert!(self_major_faults().is_some());
            assert!(Vm::now().is_some());
            assert!(Steal::now().is_some());
        } else {
            assert!(self_major_faults().is_none());
        }
    }
}

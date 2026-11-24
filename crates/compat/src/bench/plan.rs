//! The frozen experiment: workloads, sizes, rates, seeds and pair counts. Changing any of
//! them after the first timed run is a new experiment.

use std::time::Duration;

/// The seed of every workload: both programs get the same bytes.
pub(super) const SEED: u64 = 0x5eed_0042;
/// The smallest difference worth acting on, in percent.
pub(super) const SESOI: &str = "10";

/// Commits made and waited for before the measured ones start, so the boot is over.
pub(super) const WARMUP_COMMITS: u64 = 3;
/// Steady writer: one commit of one 1000-byte row every 100 ms, 300 commits (30 s).
pub(super) const STEADY_COMMITS: u64 = 300;
/// The steady writer's period.
pub(super) const STEADY_PERIOD: Duration = Duration::from_millis(100);
/// Bursty writer: 6 bursts of 50 back-to-back commits, 5 s apart (300 commits, 30 s).
pub(super) const BURSTS: u64 = 6;
/// Commits in one burst.
pub(super) const BURST_COMMITS: u64 = 50;
/// Time between the starts of two bursts.
pub(super) const BURST_GAP: Duration = Duration::from_secs(5);
/// Interleaved pairs for each latency comparison.
pub(super) const LATENCY_PAIRS: usize = 10;
/// How long a latency run may take before it is called stuck.
pub(super) const LATENCY_RUN_LIMIT: Duration = Duration::from_secs(120);

/// A 64 MiB database (65,536 rows of 1000 bytes), then 2,600 commits of 100 random in-place row
/// updates, one every 15 ms: about 1 GiB of WAL (the run measures the exact figure).
pub(super) const OVERHEAD_PRELOAD_ROWS: u32 = 65_536;
/// Commits of the overhead workload.
pub(super) const OVERHEAD_COMMITS: u32 = 2_600;
/// Rows updated per commit.
pub(super) const OVERHEAD_UPDATES: u32 = 100;
/// The overhead writer's period.
pub(super) const OVERHEAD_PERIOD: Duration = Duration::from_millis(15);
/// Interleaved pairs for the overhead comparison.
pub(super) const OVERHEAD_PAIRS: usize = 10;

/// Restore replica: grown in batches of 4,000 rows (about 4 MiB) every 200 ms until the
/// database holds this many pages (1 GiB of 4 KiB pages),
pub(super) const TARGET_PAGES: u32 = 262_144;
/// rows per growth commit,
pub(super) const GROW_BATCH_ROWS: u32 = 4_000;
/// the pace of growth and churn,
pub(super) const GROW_PERIOD: Duration = Duration::from_millis(200);
/// then 300 commits of 500 random in-place updates.
pub(super) const CHURN_BATCHES: u32 = 300;
/// Rows updated per churn commit.
pub(super) const CHURN_ROWS: u32 = 500;
/// Interleaved pairs for the restore comparison.
pub(super) const RESTORE_PAIRS: usize = 15;
/// How long one restore may take.
pub(super) const RESTORE_LIMIT: Duration = Duration::from_secs(900);

/// How long a boot (up line, boot snapshot, first durable commit) may take.
pub(super) const BOOT_LIMIT: Duration = Duration::from_secs(60);
/// How long the replica may take to catch up once the writer stops.
pub(super) const DRAIN_LIMIT: Duration = Duration::from_secs(300);

/// The napkin estimates, written before the first timed run.
pub(super) const NAPKIN: &str = "napkin (written before timing):
  commit to durable: both programs poll the WAL once a second (monitor-interval 1s), so a
    commit waits uniformly 0 to 1 s for the next poll, then one small PUT (about 10 ms): floor p50 0.5 s, p99 1.0 s;
    a second 1 s clock between staging and upload would raise p99 toward 2 s
  host overhead: 1 GiB of WAL is read once (0.1 s), LZ4-compressed (about 1 GB/s/core: 1 s),
    checksummed (CRC64 about 2 GB/s: 0.5 s), copied into HTTP bodies (0.2 s): floor about 2 CPU-s per GiB;
    peak memory: one second of WAL as one batch (about 25 MiB) held raw plus compressed, plus the runtime:
    floor about 50 MiB
  restore of 1 GiB: LZ4 decode 1 GiB at about 3 GB/s (0.35 s), CRC64 at 2 GB/s (0.5 s), write 1 GiB to the
    page cache at about 2 GB/s (0.5 s), one GET per file at about 1 ms each: floor about 1.5 s;
    peak memory: a streaming decoder needs a few MiB, one buffered file needs its size";

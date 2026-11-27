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

/// The database: 16 GiB, twice the 8 GiB box's memory (4,194,304 pages, 16,777,216 rows).
pub(super) const IMPACT_DB_MIB: u64 = 16 * 1024;
/// The hot set: 1 GiB of leaf pages, one row each (262,144 pages), a seventh of the memory.
pub(super) const IMPACT_HOT_MIB: u64 = 1024;
/// Reads per second over all reader threads.
pub(super) const IMPACT_READS_PER_S: u32 = 4_000;
/// Reader threads (each its own connection, each 1,000 reads a second).
pub(super) const IMPACT_READERS: u32 = 4;
/// Commits per second, each rewriting one hot row and the marker (about 0.1 MB/s of WAL).
pub(super) const IMPACT_WRITES_PER_S: u32 = 10;
/// The application alone before the replicator starts: the run's own baseline window.
pub(super) const IMPACT_PRE: Duration = Duration::from_secs(20);
/// The application after the snapshot has landed and the replica is current.
pub(super) const IMPACT_POST: Duration = Duration::from_secs(10);
/// A copy that has not landed after this long is cut off and counted as timed out.
pub(super) const IMPACT_COPY_LIMIT: Duration = Duration::from_secs(60 * 60);
/// How long the replica may take to hold everything committed before the snapshot landed.
pub(super) const IMPACT_CURRENT_LIMIT: Duration = Duration::from_secs(15 * 60);
/// How long the application may take to warm its hot set.
pub(super) const IMPACT_WARM_LIMIT: Duration = Duration::from_secs(20 * 60);
/// Passes over the database a first-sync boot snapshot makes through the page source: observed
/// on a quiet 65,703-page database as 2 x 65,703 reads, the second pass after the first and
/// both before the snapshot lands. The paced arms' rates are fractions of the page-read rate
/// (this many database sizes over B's calibration copy time). If the box shows another count
/// (the table's copy times against 2, 4 and 8) the rates were off by that factor.
pub(super) const COPY_PASSES: u64 = 2;
/// The smallest change in application p99 worth acting on, in percent.
pub(super) const IMPACT_SESOI: f64 = 25.0;
/// The success bar: a remedy's p99 during the copy, relative to arm A's.
pub(super) const IMPACT_BAR_P99_VS_A: f64 = 2.0;
/// The success bar: a remedy's copy wall time, relative to the unthrottled copy's.
pub(super) const IMPACT_BAR_COPY_VS_B: f64 = 4.0;
/// A/A pairs run before the arms to find the noise floor.
pub(super) const IMPACT_GATE_PAIRS: usize = 6;
/// Fewest and most rounds (one run of every arm each) the A/A noise may ask for.
pub(super) const IMPACT_MIN_ROUNDS: usize = 4;
/// See [`IMPACT_MIN_ROUNDS`].
pub(super) const IMPACT_MAX_ROUNDS: usize = 6;

/// One arm: its label, the patch whose binary it runs ([`None`] is the tree's own [`lsr`]), and
/// the divisor of the calibrated copy rate when it paces reads.
#[derive(Debug, Clone, Copy)]
pub(super) struct ArmDef {
    pub label: &'static str,
    pub patch: Option<&'static str>,
    pub pace_divisor: Option<u64>,
    /// No replicator runs.
    pub idle: bool,
}

/// The arms, in the order of the table. A is the application alone, B the unthrottled copy;
/// the paced arm takes half the rate B's calibration copy read at (a quarter and an eighth were
/// dropped before any counted run: see the frame's last paragraph);
/// [`read-twice`] is the known-bad control and must lose to B clearly.
pub(super) const ARMS: [ArmDef; 5] = [
    ArmDef {
        label: "A",
        patch: None,
        pace_divisor: None,
        idle: true,
    },
    ArmDef {
        label: "B",
        patch: None,
        pace_divisor: None,
        idle: false,
    },
    ArmDef {
        label: "pace-1/2",
        patch: Some("read-pace"),
        pace_divisor: Some(2),
        idle: false,
    },
    ArmDef {
        label: "fadvise",
        patch: Some("fadvise-dontneed"),
        pace_divisor: None,
        idle: false,
    },
    ArmDef {
        label: "read-twice",
        patch: Some("read-twice"),
        pace_divisor: None,
        idle: false,
    },
];

/// The patches the arms run, each [`crates/compat/bench-patches/<name>.patch`].
pub(super) const PATCHES: [&str; 3] = ["read-pace", "fadvise-dontneed", "read-twice"];

/// The environment variable the [`read-pace`] patch reads its rate from, in MiB a second.
pub(super) const PACE_ENV: &str = "LSRBENCH_PACE_MIB_S";

/// The frame of the snapshot-impact set, written before the first timed run and printed at the
/// top of every result.
pub(super) const IMPACT_FRAME: &str = "frame of snapshot-impact (written before any timed run; issue 315):
  question: when lsr copies a whole database (a level-9 snapshot, a scheduled verify) on a machine
    whose database is larger than its memory, how much does it hurt an application reading a hot set
    of pages beside it, and which remedy brings it back? In-process there is no cgroup between the two.
  hypothesis H1: the copy streams the database through the page cache (16 GiB through about 7 GiB of
    free memory is 2.3 turnovers), so hot pages the kernel evicts come back by random 4 KiB reads that
    also queue behind the copy's sequential reads: application p99 read latency rises several times.
  mechanism of the remedies, stated before the numbers (predictions are guesses, not claims):
    read pacing (read-pace, at half of B's calibrated page-read rate) lowers
      the rate of eviction and of competing disk reads but not the volume read: the same 32 GiB (two
      passes) still pass through the cache. It helps if the kernel keeps twice-referenced pages (the hottest tenth
      is re-read every few seconds) and the disk queue was the cost; it does little if volume was.
      It costs a copy twice as long, so a read transaction held that much longer.
    fadvise(DONTNEED) on the pages the copy read (fadvise-dontneed, through SQLite's own descriptor,
      in runs of 1 MiB) attacks the volume: the copy leaves nothing behind. It also drops hot pages the
      copy happens to read (about one page in sixteen is hot), which the application must read again.
    read-twice (control): reads every page twice, one full sweep before the copy. It must lose to B
      clearly: if it does not, the harness cannot see this axis and nothing else is believed.
  primary metric: application p99 read latency during the copy (from lsr's start to the snapshot's
    landing in the bucket), relative to arm A over the same length of time, one number per run,
    compared by pairs of the same round. Also p50 and p99.9, the copy's wall time, the bytes it read
    (/proc/pid/io), WAL growth during it, the time until the replica holds every commit made before
    the snapshot landed, the application's and the system's major faults, reclaim scans and steals.
  SESOI: 25% in p99 (a smaller change is not worth a knob). The size of the problem is the difference
    between A and B; a remedy is judged against B (does it help) and against A (is it back).
  success bar for a remedy, pre-registered: p99 within 2 times arm A's AND copy wall time within 4
    times B's. A remedy below the bar on p99 but above it on copy time trades a longer read
    transaction for latency and is reported as that trade, with the WAL it grew.
  arms: A (no copy), B (lsr takes the boot snapshot of an empty replica), pace-1/2,
    fadvise, read-twice; A is also run against itself (the gate). Rounds: one run of every
    arm per round, the order rotated each round, cold page cache before every run (drop_caches), the
    hot set warmed and referenced twice, MinIO emptied after. A's window is as long as the copy of B's
    calibration run. The rates of the paced arms are fixed from that calibration run and recorded.
  pairs: an A/A run of 6 pairs gives the sd of the per-pair log ratio of p99; rounds = pairs_needed
    (80% power at SESOI), at least 4 and at most 6. The gate fails (the run stops) when the A/A
    interval excludes 0 (drift or an order effect) or when its half width exceeds SESOI.
  napkin (written before timing; 16 GiB = 17.2 GB database, 8 GiB box, hot set 1 GiB):
    copy read floor: the boot snapshot reads every page twice through the page source (observed, see
      COPY_PASSES), 34.4 GB over the sequential read bandwidth (dd, measured on the box): 17 s at 2 GB/s
      NVMe, 140 to 280 s at 125 to 250 MB/s network SSD. CPU floor: LZ4 (about 1 GB/s a core) plus
      CRC and hashing (about 2 GB/s) is about 0.5 GB/s, so 35 s on one core; the 9 to 11 GB the pages
      compress to (1.5 to 2 times) are written to a staging directory beside the database and read
      back for the upload to a local MinIO, 20 GB more through the page cache. Expect B copy 35 to 300 s.
    cache turnover: (34.4 + 20) GB / 7.5 GB of free memory = 7; hot set 1.07 GB = 14% of the cache.
    A: a hit is a pread from cache plus a B-tree walk, about 10 to 30 us; p99 about 50 to 100 us.
    B: if the hot pages are evicted, 4,000 reads a second of which 10% are uniform over the set means
      up to 400 misses a second at 100 us (NVMe) to 1 to 2 ms (network SSD, which also caps near 3,000
      IOPS), queued behind 1 MiB readahead. p99 is then a miss: 3 to 50 times A. The incident's two
      orders of magnitude is the top of that range.
    paced copy: wall time is 34.4 GB / rate, so 2 times B if reading is what B waited on. WAL growth is the application's
      0.1 MB/s times the copy: about 60 MB for a 10 minute copy, small beside the database; the cost of
      throttling at this write rate is time (the read transaction, a stale replica), not disk.
    fadvise: the copy's cache footprint of the database reads falls from 34.4 GB to about 1 MiB (the
      staging file and MinIO's writes are not advised); syscall cost 1 call per 256 pages is noise. Prediction: p99 within 2 times A; the hot pages
      it drops cost a burst of misses at its start.
    read-twice: 3 times the bytes read of B (a full sweep first) and 1.5 to 2 times the copy time;
      p99 not better than B.
  changed before any counted run (2026-11-24): the first calibration on the box (4 dedicated cores,
    8 GiB, one virtual SSD shared with MinIO) copied in 1,257 s, four times the napkin's ceiling of
    300 s, because the same disk also takes the staging file and MinIO's writes. At that length the
    plan as first written (seven arms, 8 gate pairs, 10 to 16 rounds) is about 66 hours, and the
    quarter-rate and eighth-rate arms would pass the 45 minute copy limit. So: the quarter and
    eighth arms are dropped (the eighth fails the copy bar by construction, the quarter sits on it),
    the gate is 6 pairs, rounds are 4 to 6, and the copy limit is 60 minutes. Fewer rounds mean wider
    intervals; the gate still stops the run if the box cannot see the SESOI. Only calibration runs
    (unmeasured) had happened; no arm's counted number was seen before this change.
  confounders accepted and named: MinIO runs on the same disk and in the same page cache (its writes
    of the replica are part of what a copy does here, in every arm that copies); one machine, one disk
    type; a hot set with a 90/10 skew, not the embedder's; uniform random reads within each tier.
";

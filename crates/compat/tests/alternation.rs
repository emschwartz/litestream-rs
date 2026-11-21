//! The alternation test: Go's `litestream replicate` and Rust's `lsr replicate` take turns on
//! one database, meta directory and replica, each turn ended by SIGKILL at a point chosen
//! from a seed the test prints, and every switch must be a plain restart.
//!
//! A turn starts a process, waits until it says it runs, and commits. When there was a
//! previous turn, the new process's first level-0 file must start at the previous tip plus
//! one and hold only the pages the commits wrote (a snapshot holds every page), and no new
//! level-9 file may appear during the turn. The turn then ends at its kill point:
//! - `AfterCommit`: at once after a commit, while the process may be reading the WAL;
//! - `Staged`: once the meta directory holds a level-0 file the replica does not;
//! - `Uploading`: once an upload is in flight (a scratch `.tmp` in the replica) or has just
//!   landed;
//! - `Compacting`: once a compaction is in flight or has just written a level 1 to 8 file.
//!
//! A point the process does not reach within its wait is recorded as missed and the kill
//! happens anyway; the run prints how often each point was reached. Go compacts every second
//! here; `lsr replicate` keeps Go's default 30 s level-1 window, so its compacting turns wait
//! for the next window.
//!
//! The oracle. After every commit the test records the database's page hashes. After every
//! kill, with no process running, every transaction the replica gained is restored with Rust
//! and must equal a recorded state, the states in commit order as the transactions go up
//! (a transaction covers one or more commits, so the match is the next state at or after the
//! previous one). Go restores the replica's newest transaction to the same pages. The last
//! process stops with SIGINT, and a restore at the newest transaction, by both, equals the
//! database. For the recorded states to be the only states, no replicator may commit a
//! transaction of its own: the site creates both replicators' tables before they start, and
//! the run stays below both checkpoint thresholds (1,000 WAL pages, and Rust's one-minute
//! interval, which no turn reaches), so neither checkpoints; the test checks `_litestream_seq`
//! is still empty at the end.
//!
//! Snapshot windows. Both replicators snapshot at once when the replica has none, so the
//! first turn waits for one. After that a day-long window (Go's config here, Rust's default)
//! means a new level-9 file is a forced snapshot, unless the run crosses midnight UTC, which
//! the test reports instead of failing.
//!
//! Seeds: `LSR_ALTERNATION_SEED` (decimal) replays a run; without it the seed is the clock.
//! The Go builds are those under test in the block era (`just compat-live`); a frame-era
//! build cannot read Rust's files, so it has nothing to alternate with.

#![expect(
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test helpers; clippy.toml exempts only #[test] bodies"
)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_compat::{
    CompatError, Era, GoBuild, GoRestore, Kind, Replicator, Shape, Site, SplitMix, Target,
    builds_in, hash_pages, integrity_check, l0_starting_at, list, lsr_binary, real_now_ms,
    restore_replica, restore_with_go, snapshots, tip, uploaded, wait_boot_file, wait_staged_past,
    wait_until, wait_uploaded, write_config_daily_snapshots,
};
use lsr_core::{Level, RestoreTarget};
use lsr_ltx::Txid;

/// Killed turns; each is followed by a start of the other program, so this many switches.
const KILLED_TURNS: u32 = 21;
const UP: Duration = Duration::from_secs(30);
/// How long a turn waits for its kill point before it kills anyway.
const REACH: Duration = Duration::from_secs(10);
/// A compacting turn waits for the next level-1 window, Rust's being 30 s.
const REACH_COMPACTION: Duration = Duration::from_secs(45);
const MS_PER_DAY: i64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillAt {
    AfterCommit,
    Staged,
    Uploading,
    Compacting,
}

const POINTS: [KillAt; 4] = [
    KillAt::AfterCommit,
    KillAt::Staged,
    KillAt::Uploading,
    KillAt::Compacting,
];

fn seed() -> u64 {
    match std::env::var("LSR_ALTERNATION_SEED") {
        Ok(text) => text
            .trim()
            .parse()
            .expect("LSR_ALTERNATION_SEED is a decimal u64"),
        Err(_) => u64::try_from(real_now_ms().expect("clock")).expect("after 1970"),
    }
}

fn pages_of(path: &Path) -> Vec<String> {
    integrity_check(path).expect("integrity_check");
    hash_pages(path).expect("hash").page_hashes
}

/// What one turn did, for the run's summary.
#[derive(Debug)]
struct TurnReport {
    kind: Kind,
    point: KillAt,
    reached: bool,
}

/// One run: the site, both programs and everything the oracle needs.
struct Run {
    name: String,
    seed: u64,
    rng: SplitMix,
    site: Site,
    go: PathBuf,
    lsr: PathBuf,
    config: PathBuf,
    /// The page hashes after every commit, the prepared database first.
    states: Vec<Vec<String>>,
    /// The state the newest checked transaction restored to.
    matched: usize,
    /// The newest transaction restored and checked.
    checked: u64,
    /// The level-9 files once the first turn made one.
    l9: Option<std::collections::BTreeSet<String>>,
    day: i64,
    turns: Vec<TurnReport>,
}

impl Run {
    fn new(build: &GoBuild, lsr: &Path, seed: u64) -> Self {
        let site = Site::create(4096, seed).expect("site");
        site.prepare_for_processes(200).expect("prepare");
        let config = site.dir().join("go.yml");
        write_config_daily_snapshots(&config, &site.db, &site.replica).expect("config");
        let first = site.hash_now().expect("hash");
        Self {
            name: build.name().to_owned(),
            seed,
            rng: SplitMix::new(seed),
            go: build.binary().expect("the build under test is present"),
            lsr: lsr.to_path_buf(),
            config,
            states: vec![first],
            matched: 0,
            checked: 0,
            l9: None,
            day: real_now_ms().expect("clock") / MS_PER_DAY,
            turns: Vec::new(),
            site,
        }
    }

    /// Context for every failure: the seed reproduces the run.
    fn at(&self, turn: u32, kind: Kind) -> String {
        format!(
            "{} seed {} turn {turn} ({kind:?}); rerun with LSR_ALTERNATION_SEED={}",
            self.name, self.seed, self.seed
        )
    }

    fn start(&self, turn: u32, kind: Kind) -> Replicator {
        let log = self.site.dir().join(format!("turn-{turn:02}-{kind:?}.log"));
        match kind {
            Kind::Go => Replicator::start(&self.go, &self.config, &log),
            Kind::Rust => Replicator::start_lsr(&self.lsr, &self.site.db, &self.site.replica, &log),
        }
        .expect("the process starts")
    }

    /// One small commit, or a 64 KiB one (a longer upload) when `big`; records the state.
    fn commit(&mut self, big: bool) {
        let bytes = if big {
            64 * 1024
        } else {
            200 + u32::try_from(self.rng.below(400)).expect("small")
        };
        self.site.commit(Shape::Insert { bytes }).expect("commit");
        self.states.push(self.site.hash_now().expect("hash"));
    }
}

/// Whether the meta directory holds a level-0 file newer than everything uploaded.
fn staged_ahead(site: &Site) -> Result<bool, CompatError> {
    let staged = list(&site.meta)?
        .files
        .iter()
        .filter(|f| f.key.level() == Level::ZERO)
        .map(|f| f.key.range().max())
        .max();
    Ok(staged > uploaded(site)?)
}

/// The replica's files at levels 1 to 8, and whether a scratch file is in flight there.
fn compaction_view(site: &Site) -> Result<(Vec<String>, bool), CompatError> {
    let listing = list(&site.replica)?;
    let higher = listing
        .files
        .iter()
        .filter(|f| f.key.level() != Level::ZERO && f.key.level() != Level::SNAPSHOT)
        .map(|f| f.rel.clone())
        .collect();
    let scratch = listing
        .foreign
        .iter()
        .any(|n| n.ends_with(".tmp") && !n.starts_with("ltx/0/"));
    Ok((higher, scratch))
}

/// Whether an upload is in flight in the replica's level 0.
fn uploading(site: &Site) -> Result<bool, CompatError> {
    Ok(list(&site.replica)?
        .foreign
        .iter()
        .any(|n| n.ends_with(".tmp") && n.starts_with("ltx/0/")))
}

/// A wait that ran out is a missed point; anything else (the process died) fails the run.
fn reached(waited: Result<(), CompatError>, at: &str) -> bool {
    match waited {
        Ok(()) => true,
        Err(CompatError::Timeout(_)) => false,
        Err(other) => panic!("{at}: {other}"),
    }
}

impl Run {
    /// The new process's boot: its first file continues the previous tip, or (first turn)
    /// the replica gets its first snapshot and everything staged.
    fn boot(&mut self, process: &mut Replicator, turn: u32, previous: Option<Txid>) {
        let at = self.at(turn, process.kind());
        self.commit(false);
        let Some(prev) = previous else {
            wait_until("the first snapshot", UP, || {
                process.ensure_running()?;
                Ok(!snapshots(&self.site.replica)?.is_empty())
            })
            .expect("a first snapshot");
            wait_uploaded(process, &self.site).expect("uploaded");
            self.l9 = Some(snapshots(&self.site.replica).expect("l9"));
            // The first sync is a snapshot: the check every later boot passes must see it.
            let dirs = [self.site.meta.as_path(), self.site.replica.as_path()];
            let first = l0_starting_at(&dirs, Txid::MIN)
                .expect("read")
                .expect("a first file");
            assert!(
                first.holds_every_page(),
                "{at}: the first sync is no snapshot: {first:?}"
            );
            return;
        };
        let boot = wait_boot_file(process, &self.site, prev)
            .unwrap_or_else(|e| panic!("{at}: no file continues {prev}: {e}"));
        let fields = boot.header.fields();
        assert!(
            !boot.header.is_snapshot() && !boot.holds_every_page(),
            "{at}: the boot snapshotted after {prev}: its first file {}-{} holds {} of {} pages",
            fields.txids.min(),
            fields.txids.max(),
            boot.pages,
            fields.commit
        );
    }

    /// A few commits, about half of them each waited on until staged.
    fn work(&mut self, process: &mut Replicator) {
        let mut at = tip(&self.site).expect("tip");
        for _ in 0..1 + self.rng.below(4) {
            self.commit(false);
            if self.rng.below(2) == 0 {
                at = Some(wait_staged_past(process, &self.site, at).expect("staged"));
            }
        }
    }

    /// Drives the process to `point`; whether it got there before the wait ran out.
    fn reach(&mut self, process: &mut Replicator, point: KillAt, at: &str) -> bool {
        match point {
            KillAt::AfterCommit => {
                self.commit(false);
                true
            }
            KillAt::Staged => {
                self.commit(true);
                let waited = wait_on(&self.site, process, "a staged file", REACH, &staged_ahead);
                reached(waited, at)
            }
            KillAt::Uploading => {
                let before = uploaded(&self.site).expect("uploaded");
                self.commit(true);
                let landed = |site: &Site| Ok(uploading(site)? || uploaded(site)? > before);
                reached(
                    wait_on(&self.site, process, "an upload", REACH, &landed),
                    at,
                )
            }
            KillAt::Compacting => {
                let (before, _) = compaction_view(&self.site).expect("list");
                let moved = |site: &Site| {
                    let (now, scratch) = compaction_view(site)?;
                    Ok(scratch || now.iter().any(|rel| !before.contains(rel)))
                };
                let waited = wait_on(
                    &self.site,
                    process,
                    "a compaction",
                    REACH_COMPACTION,
                    &moved,
                );
                reached(waited, at)
            }
        }
    }
}

/// Polls `cond` over the site while the process keeps running.
fn wait_on(
    site: &Site,
    process: &mut Replicator,
    what: &str,
    timeout: Duration,
    cond: &dyn Fn(&Site) -> Result<bool, CompatError>,
) -> Result<(), CompatError> {
    wait_until(what, timeout, || {
        process.ensure_running()?;
        cond(site)
    })
}

impl Run {
    fn output(&self, name: &str) -> PathBuf {
        let out = self.site.dir().join(name);
        for suffix in ["", "-wal", "-shm"] {
            drop(std::fs::remove_file(format!("{}{suffix}", out.display())));
        }
        out
    }

    /// With no process running: every transaction the replica gained restores, with Rust, to
    /// a recorded state at or after the previous one's, and Go restores the newest to the
    /// same pages. Returns how many transactions were checked.
    fn check_acknowledged(&mut self, at: &str) -> u64 {
        let Some(newest) = uploaded(&self.site).expect("uploaded") else {
            return 0;
        };
        let from = self.checked;
        let mut last = Vec::new();
        for txid in from + 1..=newest.get() {
            let out = self.output("rs.db");
            let target = RestoreTarget::Txid(Txid::new(txid).expect("txid"));
            let restored = restore_replica(&self.site.replica, target, &out).expect("sink");
            assert!(
                restored.is_ok(),
                "{at}: Rust cannot restore {txid:#x}: {restored:?}"
            );
            last = pages_of(&out);
            let found = self
                .states
                .iter()
                .skip(self.matched)
                .position(|s| *s == last);
            let earlier = self.states.iter().position(|s| *s == last);
            let Some(step) = found else {
                panic!(
                    "{at}: the restore at {txid:#x} is no state at or after commit {}: {}",
                    self.matched,
                    earlier.map_or("no recorded state at all".to_owned(), |k| format!(
                        "it is the state after commit {k}"
                    ))
                );
            };
            self.matched += step;
            self.checked = txid;
        }
        if newest.get() > from {
            let out = self.output("go.db");
            let target = Target::Txid { txid: newest.get() };
            let result = restore_with_go(&self.go, &self.config, &self.site.db, &out, target)
                .expect("go runs");
            assert_eq!(result, GoRestore::Restored, "{at}: Go restores {newest}");
            assert_eq!(pages_of(&out), last, "{at}: Go and Rust at {newest}");
        }
        newest.get() - from
    }

    /// One killed turn: start, boot, work, the kill point, SIGKILL, then the oracle.
    fn killed_turn(&mut self, turn: u32, kind: Kind) {
        let previous = tip(&self.site).expect("tip");
        let mut process = self.start(turn, kind);
        let at = self.at(turn, kind);
        process.wait_up(UP).unwrap_or_else(|e| panic!("{at}: {e}"));
        self.boot(&mut process, turn, previous);
        self.work(&mut process);
        let point = POINTS[usize::try_from(self.rng.below(4)).expect("small")];
        let got_there = self.reach(&mut process, point, &at);
        process.kill().unwrap_or_else(|e| panic!("{at}: {e}"));
        self.check_snapshots(&at);
        let checked = self.check_acknowledged(&at);
        println!(
            "{at}: after {}, killed at {point:?} ({}), {checked} transactions restored",
            previous.map_or_else(|| "nothing".to_owned(), |t| t.to_string()),
            if got_there { "reached" } else { "missed" }
        );
        self.turns.push(TurnReport {
            kind,
            point,
            reached: got_there,
        });
    }

    /// No level-9 file appeared since the first turn's, unless a new UTC day began.
    fn check_snapshots(&self, at: &str) {
        let Some(first) = &self.l9 else { return };
        let now = snapshots(&self.site.replica).expect("l9");
        if now == *first {
            return;
        }
        let today = real_now_ms().expect("clock") / MS_PER_DAY;
        assert_ne!(
            today, self.day,
            "{at}: a snapshot appeared: {first:?} became {now:?}"
        );
        println!("{at}: a scheduled snapshot at midnight UTC: {now:?}");
    }

    /// The last switch, stopped the way an operator stops it: everything committed is on the
    /// replica, and both restore the newest transaction to the database as it is.
    fn final_turn(&mut self, turn: u32, kind: Kind) {
        let previous = tip(&self.site).expect("tip");
        let mut process = self.start(turn, kind);
        let at = self.at(turn, kind);
        process.wait_up(UP).unwrap_or_else(|e| panic!("{at}: {e}"));
        self.boot(&mut process, turn, previous);
        self.work(&mut process);
        wait_uploaded(&mut process, &self.site).expect("uploaded");
        process.stop().unwrap_or_else(|e| panic!("{at}: {e}"));
        self.check_snapshots(&at);
        self.check_acknowledged(&at);
        let want = self.site.hash_now().expect("hash");
        assert_eq!(self.states.last(), Some(&want), "{at}: the recorded end");
        let out = self.output("latest.db");
        let restored = restore_replica(&self.site.replica, RestoreTarget::Latest, &out);
        assert!(
            restored.expect("sink").is_ok(),
            "{at}: Rust restores Latest"
        );
        assert_eq!(pages_of(&out), want, "{at}: Rust at Latest");
        let out = self.output("go-latest.db");
        let result = restore_with_go(&self.go, &self.config, &self.site.db, &out, Target::Latest)
            .expect("go runs");
        assert_eq!(result, GoRestore::Restored, "{at}: Go restores Latest");
        assert_eq!(pages_of(&out), want, "{at}: Go at Latest");
        let bumps: i64 = (self.site.app)
            .query_row("SELECT count(*) FROM _litestream_seq", [], |r| r.get(0))
            .expect("count");
        assert_eq!(
            bumps, 0,
            "{at}: a replicator checkpointed, so the oracle's premise broke"
        );
    }

    fn summary(&self) -> String {
        let mut lines = Vec::new();
        for kind in [Kind::Go, Kind::Rust] {
            for point in POINTS {
                let of: Vec<_> = (self.turns.iter())
                    .filter(|t| t.kind == kind && t.point == point)
                    .collect();
                let hit = of.iter().filter(|t| t.reached).count();
                lines.push(format!("{kind:?} {point:?} {hit}/{}", of.len()));
            }
        }
        lines.join(", ")
    }
}

/// Alternates Go and Rust on one prefix for one build, from `seed`.
fn alternate(build: &GoBuild, lsr: &Path, seed: u64) {
    let mut run = Run::new(build, lsr, seed);
    let go_first = run.rng.below(2) == 0;
    println!(
        "{}: seed {seed} (LSR_ALTERNATION_SEED={seed} replays it), {} first",
        run.name,
        if go_first { "Go" } else { "Rust" }
    );
    let kind_of = |turn: u32| {
        if turn.is_multiple_of(2) == go_first {
            Kind::Go
        } else {
            Kind::Rust
        }
    };
    for turn in 0..KILLED_TURNS {
        run.killed_turn(turn, kind_of(turn));
    }
    run.final_turn(KILLED_TURNS, kind_of(KILLED_TURNS));
    println!(
        "{}: seed {seed}: {KILLED_TURNS} switches, {} transactions restored, {} commits; kill points reached: {}",
        run.name,
        run.checked,
        run.states.len() - 1,
        run.summary()
    );
}

/// Go and Rust take turns on one prefix, each turn ended by SIGKILL at a seeded point, and
/// every switch is a plain restart with no snapshot; every acknowledged transaction restores
/// to a state the database had.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_rust_alternate_on_one_prefix() {
    let builds = builds_in(Era::Blocks);
    if builds.is_empty() {
        println!("no block-era build under test: a frame-era Go cannot read Rust's files");
        return;
    }
    let lsr = lsr_binary().expect("lsr builds");
    let seed = seed();
    for build in &builds {
        alternate(build, &lsr, seed);
    }
}

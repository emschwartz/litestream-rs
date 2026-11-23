//! The alternation run, shared by `alternation.rs` and `sidecar.rs`: Go's `litestream
//! replicate` and Rust's `lsr replicate` take turns on one database, meta directory and
//! replica, each turn ended by SIGKILL at a point chosen from a seed the test prints, and every
//! switch must be a plain restart unless the engines' own rules call for a snapshot there.
//!
//! [`Launch`] says how Rust starts. `Arguments` is Go's one-database form `lsr replicate DB
//! file://REPLICA`, with Go's defaults (a PASSIVE checkpoint at 1,000 WAL pages, one a
//! minute, and never a TRUNCATE, which Rust takes only with the `app-readers: none` claim).
//! `ConfigFile` is the daemon on the very file Go reads, `lsr replicate -config go.yml`, so
//! both sides are config-driven processes with Go's intervals and checkpoint settings (Rust
//! then compacts every second too). Go always reads the file.
//!
//! The file checkpoints often: `checkpoint-interval: 5s`, `min-checkpoint-page-count: 1000`
//! and `truncate-page-n: 1500` (Rust takes the last only with the `litestream-rs.app-readers:
//! none` claim, which the file makes). Commits that rewrite about a hundred 3,000-byte rows
//! ("churn") push the WAL past 1,000 pages within a turn, so every turn may cross a
//! checkpoint, a WAL restart or a TRUNCATE, and the database stays under 10 MB.
//!
//! A turn starts a process, waits until it says it runs, and commits. When there was a
//! previous turn, the new process's first level-0 file must start at the previous position plus one
//! and hold only the pages the commits wrote, and no new level-9 file may appear during the
//! turn. The one exception is a boot after the previous process restarted or truncated the WAL
//! and died before it staged anything in the new generation: then both engines' rules take a
//! snapshot, and the test asserts that this is the situation and that the cause the process
//! logged is the one those rules give (`generation.rs`; Rust's is read from its boot line in
//! `Run::boot_cause`). The turn then ends at its kill point:
//! - `AfterCommit`: at once after a commit, while the process may be reading the WAL;
//! - `Staged`: once the meta directory holds a level-0 file the replica does not. The test
//!   polls without sleeping and stops the process with SIGSTOP the moment it sees one, then
//!   checks that the file is still not uploaded, so the kill lands between staging and upload;
//! - `Uploading`: once an upload is in flight (a scratch `.tmp` in the replica) or has just
//!   landed;
//! - `Compacting`: once a compaction is in flight or has just written a level 1 to 8 file;
//! - `Checkpointed`: churn until the process's checkpoint commits its `_litestream_seq` bump,
//!   then at once: right after a checkpoint;
//! - `Restarted`: churn until the WAL's salts change (a checkpoint restarted it), SIGSTOP at
//!   once, reached when the process has not yet staged a file in the new generation: between a
//!   checkpoint and the next sync;
//! - `Truncated`: a second application connection holds a read transaction while one commit
//!   of about 1,700 pages (a 7 MB row rewritten whole) pushes the WAL past `truncate-page-n`,
//!   so the PASSIVE checkpoint cannot restart it and the process falls back to TRUNCATE (busy
//!   while the read is held); after two such attempts the read ends and the kill follows
//!   SIGSTOP the moment the WAL file shrinks: right after a TRUNCATE. Only for the processes
//!   that run one (Go, and Rust from the config file).
//!
//! Each kind of process takes the points it can reach in a seeded order, cycled, so a run of
//! 21 killed turns gives each kind every point at least once. A point the process does not
//! reach within its wait is recorded as missed and the kill happens anyway; the run prints how
//! often each point was reached, and how many checkpoints, WAL restarts and boot snapshots each
//! kind saw. It fails unless both kinds checkpointed and restarted the WAL.
//!
//! The oracle (`oracle.rs`). After every commit the test records the database's page hashes
//! and its `_litestream_seq` value. After every kill, with no process running, every
//! transaction the replica gained is restored with Rust and must equal a recorded state on
//! every page but `_litestream_seq`'s, with that page pinned by its value, the states in
//! commit order as the transactions go up. Go restores the replica's newest transaction to the
//! same pages. The last process stops with SIGINT, and a restore at the newest transaction, by
//! both, equals the database exactly.
//!
//! One exception, for Go builds without upstream bcd69ae only (`GoBuild::reads_wal_index_end`;
//! #38): such a build reads the WAL file to its last valid frame, so when Rust was stopped
//! mid-commit (frames written, the wal-index not updated) Go's boot transaction can hold that
//! uncommitted tail. Before every Go boot the test reads the wal-index end and the WAL, and
//! records the state SQLite recovers from the whole file when valid transactions follow the
//! end (`uncommitted.rs`). A restore the oracle refuses is accepted only if it is at Go's boot
//! transaction and equals that state on every page; it does not advance the oracle, so every
//! later restore is judged as before. The run prints how often a Go boot found such a tail and
//! how often Go took it. For a build with the fix, the rule holds with no exception.
//!
//! Snapshot windows. Both replicators snapshot at once when the replica has none, so the first
//! turn waits for one. After that a day-long window (Go's config here, Rust's default) means a
//! new level-9 file is a forced snapshot, unless the run crosses midnight UTC, which the test
//! reports instead of failing.
//!
//! A failed run keeps its fixture directory under the system temporary directory, as
//! `lsr-alternation-<seed>-<build><launch>`, and prints where.
//!
//! Seeds: `LSR_ALTERNATION_SEED` (decimal) replays a run; without it the seed is the clock.
//! The Go builds are those under test in the block page encoding (`just compat-live`); a frame-encoding build
//! cannot read Rust's files, so it has nothing to alternate with.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

mod generation;
mod oracle;
pub mod uncommitted;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use generation::{Expect, newest_staged_origin, wal_len, wal_salts};
use lsr_compat::{
    AppFixture, CompatError, GoBuild, GoRestore, Kind, Replicator, Shape, SplitMix, Target,
    l0_starting_at, list, position, real_now_ms, restore_replica, restore_with_go, snapshots,
    uploaded, wait_boot_file, wait_staged_past, wait_until, wait_uploaded,
};
use lsr_core::{Level, RestoreTarget};
use lsr_ltx::Txid;
use oracle::{Held, Oracle, State, capture, live_seq, restored, seq_root};
use rusqlite::Connection;

/// Killed turns; each is followed by a start of the other program, so this many switches.
const KILLED_TURNS: u32 = 21;
const UP: Duration = Duration::from_secs(30);
/// How long a turn waits for its kill point before it kills anyway.
const REACH: Duration = Duration::from_secs(10);
/// A compacting turn waits for the next level-1 window, Rust's being 30 s.
const REACH_COMPACTION: Duration = Duration::from_secs(45);
/// A checkpoint turn waits for the next sync past the threshold, or the TRUNCATE's retry.
const REACH_CHECKPOINT: Duration = Duration::from_secs(30);
const MS_PER_DAY: i64 = 86_400_000;
/// How long the daemon's boot line may lag its first file.
const BOOT_LINE: Duration = Duration::from_secs(5);
/// Rows the fixture prepares; churn rewrites a random hundred of them.
const ROWS: u32 = 200;
/// Churn commits a checkpoint turn writes first: about 1,200 WAL pages.
const CHURN_TO_CHECKPOINT: u32 = 12;
/// The config's `truncate-page-n`.
const TRUNCATE_PAGES: u64 = 1500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum KillAt {
    AfterCommit,
    Staged,
    Uploading,
    Compacting,
    Checkpointed,
    Restarted,
    Truncated,
}

const POINTS: [KillAt; 7] = [
    KillAt::AfterCommit,
    KillAt::Staged,
    KillAt::Uploading,
    KillAt::Compacting,
    KillAt::Checkpointed,
    KillAt::Restarted,
    KillAt::Truncated,
];

/// How Rust's turns start. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Launch {
    /// `lsr replicate DB file://REPLICA`.
    Arguments,
    /// `lsr replicate -config <Go's file>`.
    ConfigFile,
}

/// The run's seed: `LSR_ALTERNATION_SEED`, else the clock.
pub fn seed() -> u64 {
    match std::env::var("LSR_ALTERNATION_SEED") {
        Ok(text) => text
            .trim()
            .parse()
            .expect("LSR_ALTERNATION_SEED is a decimal u64"),
        Err(_) => u64::try_from(real_now_ms().expect("clock")).expect("after 1970"),
    }
}

/// The config both programs read: Go's short compaction levels, a day-long snapshot window,
/// and checkpoints often (see the module doc). Debug logging, so both log why they snapshot.
fn write_config(path: &Path, db: &Path, replica: &Path) {
    let text = format!(
        "logging:
  level: debug
levels:
  - interval: 1s
  - interval: 2s
  - interval: 4s
snapshot:
  interval: 24h
  retention: 24h
litestream-rs:
  app-autocheckpoint: off
  app-readers: none
dbs:
  - path: {db}
    monitor-interval: 100ms
    checkpoint-interval: 5s
    min-checkpoint-page-count: 1000
    truncate-page-n: {TRUNCATE_PAGES}
    replica:
      type: file
      path: {replica}
      sync-interval: 100ms
",
        db = db.display(),
        replica = replica.display(),
    );
    std::fs::write(path, text).expect("write the config");
}

/// What one turn did, for the run's summary.
#[derive(Debug)]
struct TurnReport {
    kind: Kind,
    point: KillAt,
    reached: bool,
    /// `_litestream_seq` bumps the turn committed: its checkpoints.
    checkpoints: i64,
    /// Whether the WAL's salts changed during the turn.
    restarted: bool,
    /// The cause the boot logged for its snapshot, if it took one.
    boot_snapshot: Option<String>,
}

/// One run: the fixture, both programs and everything the oracle needs.
struct Run {
    name: String,
    seed: u64,
    rng: SplitMix,
    fixture: AppFixture,
    go: PathBuf,
    lsr: PathBuf,
    launch: Launch,
    config: PathBuf,
    oracle: Oracle,
    /// The newest transaction restored and checked.
    checked: u64,
    /// The level-9 files once the first turn made one.
    l9: Option<std::collections::BTreeSet<String>>,
    day: i64,
    turns: Vec<TurnReport>,
    /// Each kind's points in a seeded order, and how many it has taken.
    schedule: BTreeMap<&'static str, (Vec<KillAt>, usize)>,
    /// What the next boot must do, read after the last kill.
    expect: Expect,
    /// The first unpublished staged file outside the WAL's generation, read after the last
    /// kill (`generation::unpublished_across_restart`): the next Rust boot uploads it as
    /// staged, so its first file is no snapshot though the boot's snapshot follows.
    crossed: Option<Txid>,
    /// Application commits SQLite refused busy and the run tried again.
    busy: u32,
    /// The database file and wal-index, open for the whole run (`oracle.rs`).
    held: Held,
    /// Whether the Go build reads the committed WAL end from the wal-index (upstream
    /// bcd69ae); without it, the one known mismatch of `uncommitted.rs` is accepted.
    fixed: bool,
    /// The uncommitted WAL tail the running Go process booted on, if any.
    tail: Option<uncommitted::Tail>,
    /// Go boots that found such a tail, and restores at which Go was seen to have taken it.
    tails: u32,
    taken: u32,
}

fn kind_key(kind: Kind) -> &'static str {
    match kind {
        Kind::Go => "go",
        Kind::Rust => "rust",
    }
}

impl Run {
    fn new(build: &GoBuild, lsr: &Path, seed: u64, launch: Launch) -> Self {
        let fixture = AppFixture::create(4096, seed).expect("fixture");
        fixture.prepare_for_processes(ROWS).expect("prepare");
        // A replicator holds the write lock for a snapshot or a checkpoint barrier, which can
        // take seconds here; the application waits, as one would.
        fixture
            .app
            .busy_timeout(Duration::from_secs(30))
            .expect("busy timeout");
        // One row the TRUNCATE turns rewrite whole: a commit of thousands of pages that leaves
        // the database the same size.
        fixture
            .app
            .execute_batch("CREATE TABLE wide(id INTEGER PRIMARY KEY, v BLOB)")
            .expect("wide");
        let config = fixture.dir().join("go.yml");
        write_config(&config, &fixture.db, &fixture.replica);
        let held = Held::open(&fixture);
        let oracle = Oracle::new(capture(&fixture, &held), seq_root(&fixture));
        let mut rng = SplitMix::new(seed);
        let mut schedule = BTreeMap::new();
        for kind in [Kind::Go, Kind::Rust] {
            let mut points: Vec<KillAt> = POINTS
                .into_iter()
                .filter(|p| *p != KillAt::Truncated || truncates(kind, launch))
                .collect();
            for i in (1..points.len()).rev() {
                let j = usize::try_from(rng.below(u64::try_from(i + 1).expect("small")))
                    .expect("small");
                points.swap(i, j);
            }
            schedule.insert(kind_key(kind), (points, 0));
        }
        Self {
            name: format!("{} ({launch:?})", build.name()),
            seed,
            rng,
            go: build.binary().expect("the build under test is present"),
            lsr: lsr.to_path_buf(),
            launch,
            config,
            oracle,
            checked: 0,
            l9: None,
            day: real_now_ms().expect("clock") / MS_PER_DAY,
            turns: Vec::new(),
            schedule,
            expect: Expect::First,
            crossed: None,
            busy: 0,
            held,
            fixed: build.reads_wal_index_end(),
            tail: None,
            tails: 0,
            taken: 0,
            fixture,
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
        let log = self
            .fixture
            .dir()
            .join(format!("turn-{turn:02}-{kind:?}.log"));
        match (kind, self.launch) {
            (Kind::Go, _) => Replicator::start(&self.go, &self.config, &log),
            (Kind::Rust, Launch::Arguments) => {
                Replicator::start_lsr(&self.lsr, &self.fixture.db, &self.fixture.replica, &log)
            }
            (Kind::Rust, Launch::ConfigFile) => {
                Replicator::start_lsr_config(&self.lsr, &self.config, &log)
            }
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
        let floor = live_seq(&self.fixture);
        self.busy_retry("commit", |fixture| fixture.commit(Shape::Insert { bytes }));
        self.oracle
            .record(capture(&self.fixture, &self.held), floor);
    }

    /// Runs one application commit, again while SQLite refuses it busy. A replicator that
    /// retries TRUNCATE every second while a read is held can make SQLite answer `SQLITE_BUSY`
    /// before the busy timeout has run (seen with Go; not root-caused). An application retries
    /// such a commit, and the oracle does not care how often it was tried; the run counts it.
    fn busy_retry(
        &mut self,
        what: &str,
        mut write: impl FnMut(&mut AppFixture) -> Result<(), CompatError>,
    ) {
        let mut busy = 0;
        let fixture = &mut self.fixture;
        wait_until(what, UP, || match write(fixture) {
            Ok(()) => Ok(true),
            Err(CompatError::Sqlite(rusqlite::Error::SqliteFailure(e, _)))
                if e.code == rusqlite::ErrorCode::DatabaseBusy =>
            {
                busy += 1;
                Ok(false)
            }
            Err(other) => Err(other),
        })
        .unwrap_or_else(|e| panic!("{what}: {e}"));
        self.busy += busy;
    }

    /// One commit rewriting about a hundred of the prepared rows: about a hundred WAL pages,
    /// no growth.
    fn churn(&mut self) {
        let floor = live_seq(&self.fixture);
        self.busy_retry("churn", |fixture| {
            fixture.app.execute(
                "UPDATE t SET v = randomblob(3000) WHERE id IN
                   (SELECT id FROM t WHERE id BETWEEN 2 AND ?1 ORDER BY random() LIMIT 100)",
                [ROWS + 1],
            )?;
            Ok(())
        });
        self.oracle
            .record(capture(&self.fixture, &self.held), floor);
    }

    fn next_point(&mut self, kind: Kind) -> KillAt {
        let (points, taken) = self
            .schedule
            .get_mut(kind_key(kind))
            .expect("every kind has a schedule");
        let point = points[*taken % points.len()];
        *taken += 1;
        point
    }
}

impl Drop for Run {
    /// A failed run keeps its fixture (logs, config, database, WAL, meta directory and replica),
    /// which the fixture's temporary directory would take with it: they are the evidence.
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let keep = std::env::temp_dir().join(format!(
            "lsr-alternation-{}-{}",
            self.seed,
            self.name.replace([' ', '(', ')'], "")
        ));
        drop(std::fs::create_dir_all(&keep));
        if let Ok(entries) = std::fs::read_dir(self.fixture.dir()) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if entry.path().is_dir() {
                    drop(lsr_compat::copy_tree(&entry.path(), &keep.join(&name)));
                } else {
                    drop(std::fs::copy(entry.path(), keep.join(&name)));
                }
            }
        }
        println!("{}: the fixture is kept in {}", self.name, keep.display());
    }
}

/// Whether this kind of process runs TRUNCATE checkpoints in this run.
const fn truncates(kind: Kind, launch: Launch) -> bool {
    match (kind, launch) {
        (Kind::Go, _) | (Kind::Rust, Launch::ConfigFile) => true,
        (Kind::Rust, Launch::Arguments) => false,
    }
}

/// Whether the meta directory holds a level-0 file newer than everything uploaded.
fn staged_ahead(fixture: &AppFixture) -> Result<bool, CompatError> {
    let staged = list(&fixture.meta)?
        .files
        .iter()
        .filter(|f| f.key.level() == Level::ZERO)
        .map(|f| f.key.range().max())
        .max();
    Ok(staged > uploaded(fixture)?)
}

/// The replica's files at levels 1 to 8, and whether a scratch file is in flight there.
fn compaction_view(fixture: &AppFixture) -> Result<(Vec<String>, bool), CompatError> {
    let listing = list(&fixture.replica)?;
    let higher = listing
        .files
        .iter()
        .filter(|f| f.key.level() != Level::ZERO && f.key.level() != Level::SNAPSHOT)
        .map(|f| f.rel.clone())
        .collect();
    let scratch = listing
        .unrecognized
        .iter()
        .any(|n| n.ends_with(".tmp") && !n.starts_with("ltx/0/"));
    Ok((higher, scratch))
}

/// Whether an upload is in flight in the replica's level 0.
fn uploading(fixture: &AppFixture) -> Result<bool, CompatError> {
    Ok(list(&fixture.replica)?
        .unrecognized
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

/// Polls `cond` over the fixture while the process keeps running.
fn wait_on(
    fixture: &AppFixture,
    process: &mut Replicator,
    what: &str,
    timeout: Duration,
    cond: &dyn Fn(&AppFixture) -> Result<bool, CompatError>,
) -> Result<(), CompatError> {
    wait_until(what, timeout, || {
        process.ensure_running()?;
        cond(fixture)
    })
    .map_err(|err| process.explain(err))
}

/// [`wait_on`] without the poll's pause between looks, for a point the process passes within
/// milliseconds; the caller stops the process the moment it returns.
#[expect(
    clippy::disallowed_methods,
    reason = "waits on a real process, which keeps real time"
)]
fn spin_on(
    fixture: &AppFixture,
    process: &mut Replicator,
    what: &str,
    timeout: Duration,
    cond: &dyn Fn(&AppFixture) -> Result<bool, CompatError>,
) -> Result<(), CompatError> {
    let deadline = Instant::now().checked_add(timeout);
    loop {
        process.ensure_running()?;
        if cond(fixture)? {
            return Ok(());
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(process.explain(CompatError::Timeout(what.to_owned())));
        }
        std::thread::yield_now();
    }
}

/// SIGSTOP: the process stays where it is until the SIGKILL that follows.
fn pause(process: &Replicator) {
    let pid = rustix::process::Pid::from_raw(i32::try_from(process.pid()).expect("a pid"))
        .expect("a live pid");
    rustix::process::kill_process(pid, rustix::process::Signal::STOP).expect("SIGSTOP");
}

/// Whether the newest staged file is in the WAL generation with these salts.
fn newest_staged_in(fixture: &AppFixture, salts: Option<(u32, u32)>) -> bool {
    let origin = newest_staged_origin(fixture).and_then(|(_, origin)| origin);
    origin.map(lsr_ltx::WalOrigin::salts) == salts
}

impl Run {
    /// The new process's boot: its first file continues the previous position, or (first turn)
    /// the replica gets its first snapshot and everything staged. Returns the cause a boot
    /// snapshot logged.
    fn boot(
        &mut self,
        process: &mut Replicator,
        turn: u32,
        previous: Option<Txid>,
    ) -> Option<String> {
        let at = self.at(turn, process.kind());
        self.commit(false);
        let Some(prev) = previous else {
            self.first_boot(process, &at);
            return None;
        };
        let boot = wait_boot_file(process, &self.fixture, prev)
            .unwrap_or_else(|e| panic!("{at}: no file continues {prev}: {e}"));
        let fields = boot.header.fields();
        let snapshotted = boot.header.is_snapshot() || boot.holds_every_page();
        let cause = self.boot_cause(process, prev, &at);
        let expect = self.expect;
        if !snapshotted {
            assert!(
                cause.is_none() || self.crossed.is_some(),
                "{at}: the boot continued {prev} but logged a snapshot cause {cause:?}"
            );
            if expect.may_snapshot() {
                println!("{at}: continued {prev} although the WAL generation changed ({expect:?})");
            }
            return None;
        }
        assert!(
            expect.may_snapshot(),
            "{at}: the boot snapshotted after {prev} ({cause:?}): its first file {}-{} holds {} \
             of {} pages, and the position was in the WAL's generation",
            fields.txids.min(),
            fields.txids.max(),
            boot.pages,
            fields.commit
        );
        Some(self.expected_cause(process.kind(), cause, &at))
    }

    /// The first turn: the replica gets its first snapshot and everything staged.
    fn first_boot(&mut self, process: &mut Replicator, at: &str) {
        wait_until("the first snapshot", UP, || {
            process.ensure_running()?;
            Ok(!snapshots(&self.fixture.replica)?.is_empty())
        })
        .map_err(|err| process.explain(err))
        .expect("a first snapshot");
        wait_uploaded(process, &self.fixture).expect("uploaded");
        self.l9 = Some(snapshots(&self.fixture.replica).expect("l9"));
        // The first sync is a snapshot: the check every later boot passes must see it.
        let dirs = [self.fixture.meta.as_path(), self.fixture.replica.as_path()];
        let first = l0_starting_at(&dirs, Txid::MIN)
            .expect("read")
            .expect("a first file");
        assert!(
            first.holds_every_page(),
            "{at}: the first sync is no snapshot: {first:?}"
        );
    }

    /// The cause the boot's log names, if it names one: Rust's boot line (either launch is
    /// the daemon), Go's `reason` on the sync that wrote the boot's file.
    fn boot_cause(&self, process: &mut Replicator, prev: Txid, at: &str) -> Option<String> {
        match (process.kind(), self.launch) {
            (Kind::Rust, Launch::Arguments | Launch::ConfigFile) => {
                let line = self.rust_boot_line(process, at);
                // A cause with fields is logged quoted: its variant name is the first word.
                line.split_once("cause=").map(|(_, c)| {
                    let word = c.split_whitespace().next().unwrap_or_default();
                    word.trim_matches('"').to_owned()
                })
            }
            (Kind::Go, _) => {
                let txid = format!("txid={:016x}", prev.get() + 1);
                let log = process.log_text();
                let line = (log.lines()).find(|l| l.contains("msg=sync") && l.contains(&txid))?;
                let (_, rest) = line.split_once("reason=\"")?;
                rest.split_once('"').map(|(reason, _)| reason.to_owned())
            }
        }
    }

    /// The Rust daemon's boot line.
    fn rust_boot_line(&self, process: &mut Replicator, at: &str) -> String {
        // The daemon's log sink is in place before its replicator's thread runs,
        // so every boot logs this line.
        let mut line = String::new();
        wait_until("the boot's log line", BOOT_LINE, || {
            process.ensure_running()?;
            line = (process.log_text().lines())
                .find(|l| l.contains("replica position recovered"))
                .unwrap_or_default()
                .to_owned();
            Ok(!line.is_empty())
        })
        .unwrap_or_else(|e| panic!("{at}: the daemon logged no boot line: {e}"));
        line
    }

    /// Checks a boot snapshot's logged cause against the engines' rules for what the previous
    /// kill left (`generation.rs`); returns it for the report.
    fn expected_cause(&self, kind: Kind, cause: Option<String>, at: &str) -> String {
        let allowed = match kind {
            Kind::Rust => self.expect.rust_causes(),
            Kind::Go => self.expect.go_reasons(),
        };
        match (kind, self.launch, cause) {
            (Kind::Rust, Launch::Arguments | Launch::ConfigFile, None) => {
                format!("{:?} (rule 6 or 4; not logged)", self.expect)
            }
            (_, _, Some(cause)) if allowed.contains(&cause.as_str()) => cause,
            (_, _, cause) => panic!(
                "{at}: a boot snapshot after {:?} gave cause {cause:?}, not one of {allowed:?}",
                self.expect
            ),
        }
    }

    /// A few commits, about half of them each waited on until staged, and in about half the
    /// turns a churn commit or two, so the WAL keeps growing toward a checkpoint.
    fn work(&mut self, process: &mut Replicator) {
        let mut at = position(&self.fixture).expect("position");
        for _ in 0..1 + self.rng.below(4) {
            self.commit(false);
            if self.rng.below(2) == 0 {
                at = Some(wait_staged_past(process, &self.fixture, at).expect("staged"));
            }
        }
        for _ in 0..self.rng.below(3) {
            self.churn();
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
                let waited = spin_on(
                    &self.fixture,
                    process,
                    "a staged file",
                    REACH,
                    &staged_ahead,
                );
                if !reached(waited, at) {
                    return false;
                }
                pause(process);
                staged_ahead(&self.fixture).expect("list")
            }
            KillAt::Uploading => {
                let before = uploaded(&self.fixture).expect("uploaded");
                self.commit(true);
                let landed =
                    |fixture: &AppFixture| Ok(uploading(fixture)? || uploaded(fixture)? > before);
                reached(
                    wait_on(&self.fixture, process, "an upload", REACH, &landed),
                    at,
                )
            }
            KillAt::Compacting => self.reach_compaction(process, at),
            KillAt::Checkpointed => self.reach_checkpoint(process, at),
            KillAt::Restarted => self.reach_restart(process, at),
            KillAt::Truncated => self.reach_truncate(process, at),
        }
    }

    fn reach_compaction(&self, process: &mut Replicator, at: &str) -> bool {
        let (before, _) = compaction_view(&self.fixture).expect("list");
        let moved = |fixture: &AppFixture| {
            let (now, scratch) = compaction_view(fixture)?;
            Ok(scratch || now.iter().any(|rel| !before.contains(rel)))
        };
        let waited = wait_on(
            &self.fixture,
            process,
            "a compaction",
            REACH_COMPACTION,
            &moved,
        );
        reached(waited, at)
    }

    /// Churn until the process commits a checkpoint's `_litestream_seq` bump.
    fn reach_checkpoint(&mut self, process: &mut Replicator, at: &str) -> bool {
        let before = live_seq(&self.fixture);
        for _ in 0..CHURN_TO_CHECKPOINT {
            self.churn();
        }
        let bumped = |fixture: &AppFixture| Ok(live_seq(fixture) > before);
        let waited = wait_on(
            &self.fixture,
            process,
            "a checkpoint",
            REACH_CHECKPOINT,
            &bumped,
        );
        reached(waited, at)
    }

    /// Churn until the WAL restarts, stop the process at once, and see whether it has staged
    /// anything in the new generation yet.
    fn reach_restart(&mut self, process: &mut Replicator, at: &str) -> bool {
        let before = wal_salts(&self.fixture);
        for _ in 0..CHURN_TO_CHECKPOINT {
            self.churn();
        }
        let moved = |fixture: &AppFixture| Ok(wal_salts(fixture) != before);
        let waited = spin_on(
            &self.fixture,
            process,
            "a WAL restart",
            REACH_CHECKPOINT,
            &moved,
        );
        if !reached(waited, at) {
            return false;
        }
        pause(process);
        newest_staged_in(&self.fixture, before)
    }

    /// Hold a read so the PASSIVE cannot restart the WAL past `truncate-page-n`, wait for the
    /// process's attempt, end the read, and stop the process the moment the WAL shrinks.
    fn reach_truncate(&mut self, process: &mut Replicator, at: &str) -> bool {
        self.commit(false);
        let reader = Connection::open(&self.fixture.db).expect("open a reader");
        reader
            .busy_timeout(Duration::from_secs(30))
            .expect("busy timeout");
        reader.execute_batch("BEGIN").expect("begin");
        let _: i64 = reader
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .expect("hold a read");
        // One commit of about 1,700 pages past the read, so the WAL's content passes the
        // threshold wherever it began (its file keeps its length across restarts). One commit,
        // because once the process retries TRUNCATE under the held read, it holds the write
        // lock nearly all the time and further commits starve (`busy_retry`).
        let floor = live_seq(&self.fixture);
        let frame = u64::from(self.fixture.page_size());
        let bytes = i64::try_from((TRUNCATE_PAGES + 200) * frame).expect("small");
        self.busy_retry("a wide commit", |fixture| {
            fixture.app.execute(
                "INSERT OR REPLACE INTO wide(id, v) VALUES (1, randomblob(?1))",
                [bytes],
            )?;
            Ok(())
        });
        self.oracle
            .record(capture(&self.fixture, &self.held), floor);
        // Two bumps past the threshold: the first may come from a sync that measured the WAL
        // just before it (a plain PASSIVE, which leaves no failed attempt behind, so the next
        // sync would PASSIVE again and restart the WAL instead of truncating it).
        let before = live_seq(&self.fixture);
        let attempted = |fixture: &AppFixture| Ok(live_seq(fixture) > before + 1);
        let what = "a failed checkpoint at the truncate threshold";
        let waited = wait_on(&self.fixture, process, what, REACH_CHECKPOINT, &attempted);
        drop(reader);
        if !reached(waited, at) {
            return false;
        }
        let long = wal_len(&self.fixture);
        let shrunk = |fixture: &AppFixture| Ok(wal_len(fixture) < long);
        let waited = spin_on(
            &self.fixture,
            process,
            "a TRUNCATE",
            REACH_CHECKPOINT,
            &shrunk,
        );
        if !reached(waited, at) {
            return false;
        }
        pause(process);
        true
    }
}

impl Run {
    fn output(&self, name: &str) -> PathBuf {
        let out = self.fixture.dir().join(name);
        for suffix in ["", "-wal", "-shm"] {
            drop(std::fs::remove_file(format!("{}{suffix}", out.display())));
        }
        out
    }

    /// With no process running: every transaction the replica gained restores, with Rust, to
    /// a recorded state at or after the previous one's (`oracle.rs`), and Go restores the
    /// newest to the same pages. Returns how many transactions were checked.
    fn check_acknowledged(&mut self, at: &str) -> u64 {
        let Some(newest) = uploaded(&self.fixture).expect("uploaded") else {
            return 0;
        };
        let from = self.checked;
        let live = live_seq(&self.fixture);
        let mut last: Option<State> = None;
        for txid in from + 1..=newest.get() {
            let out = self.output("rs.db");
            let target = RestoreTarget::Txid(Txid::new(txid).expect("txid"));
            let result = restore_replica(&self.fixture.replica, target, &out).expect("sink");
            assert!(
                result.is_ok(),
                "{at}: Rust cannot restore {txid:#x}: {result:?}"
            );
            let got = restored(&out, self.oracle.root());
            if let Err(why) = self.oracle.verdict(&got, live) {
                assert!(
                    self.took_tail(txid, &got),
                    "{at}: at {txid:#x}: {why}{}",
                    self.tail_note(txid)
                );
                println!(
                    "{at}: at {txid:#x}: Go took the uncommitted WAL tail (#38): {why}; \
                     accepted, it is exactly the tail's state ({:?})",
                    self.tail.as_ref().map(|t| (t.mx_frame, t.frames, &t.pages))
                );
            }
            self.checked = txid;
            last = Some(got);
        }
        if let Some(last) = last {
            let out = self.output("go.db");
            let target = Target::Txid { txid: newest.get() };
            let result = restore_with_go(&self.go, &self.config, &self.fixture.db, &out, target)
                .expect("go runs");
            assert_eq!(result, GoRestore::Restored, "{at}: Go restores {newest}");
            let go = restored(&out, self.oracle.root());
            assert_eq!(go.pages, last.pages, "{at}: Go and Rust at {newest}");
        }
        newest.get() - from
    }

    /// Before a Go boot: the uncommitted WAL tail it may take, if the files hold one.
    fn look_for_tail(&mut self, kind: Kind, previous: Option<Txid>, at: &str) {
        self.tail = match (kind, previous) {
            (Kind::Go, Some(prev)) => {
                uncommitted::read(&self.fixture, &self.held, &self.oracle, prev)
            }
            (Kind::Go | Kind::Rust, _) => None,
        };
        if let Some(tail) = &self.tail {
            self.tails += 1;
            println!(
                "{at}: Go boots on {} uncommitted WAL frames past the wal-index's {} \
                 (pages {:?}); Go reads the wal-index end: {}",
                tail.frames, tail.mx_frame, tail.pages, self.fixed
            );
        }
    }

    /// Whether `got`, a restore the oracle refused at `txid`, is the one known Go mismatch:
    /// a build without the fix, at its boot transaction, restoring exactly the state of the
    /// uncommitted tail it booted on (`uncommitted.rs`).
    fn took_tail(&mut self, txid: u64, got: &State) -> bool {
        let took = !self.fixed
            && self
                .tail
                .as_ref()
                .is_some_and(|t| t.boot.get() == txid && t.taken == *got);
        if took {
            self.taken += 1;
        }
        took
    }

    /// Why a refused restore is not the known mismatch, when a tail was found.
    fn tail_note(&self, txid: u64) -> String {
        self.tail.as_ref().map_or_else(String::new, |t| {
            format!(
                " (Go booted on an uncommitted WAL tail, boot transaction {}, fixed build {}, \
                 the tail's state {})",
                t.boot,
                self.fixed,
                if t.boot.get() != txid {
                    "not at this transaction"
                } else {
                    "differs from this restore"
                }
            )
        })
    }

    /// One killed turn: start, boot, work, the kill point, SIGKILL, then the oracle.
    fn killed_turn(&mut self, turn: u32, kind: Kind) {
        let previous = position(&self.fixture).expect("position");
        let (seq, salts) = (live_seq(&self.fixture), wal_salts(&self.fixture));
        self.look_for_tail(kind, previous, &self.at(turn, kind));
        let mut process = self.start(turn, kind);
        let at = self.at(turn, kind);
        process.wait_up(UP).unwrap_or_else(|e| panic!("{at}: {e}"));
        let boot_snapshot = self.boot(&mut process, turn, previous);
        self.work(&mut process);
        let point = self.next_point(kind);
        let got_there = self.reach(&mut process, point, &at);
        process.kill().unwrap_or_else(|e| panic!("{at}: {e}"));
        self.expect = generation::expect(&self.fixture);
        self.crossed = generation::unpublished_across_restart(&self.fixture);
        self.check_snapshots(&at);
        let checked = self.check_acknowledged(&at);
        let report = TurnReport {
            kind,
            point,
            reached: got_there,
            checkpoints: live_seq(&self.fixture) - seq,
            restarted: wal_salts(&self.fixture) != salts,
            boot_snapshot,
        };
        println!(
            "{at}: after {}, killed at {point:?} ({}), {checked} transactions restored, \
             {} checkpoints, WAL {}, next boot {:?}{}",
            previous.map_or_else(|| "nothing".to_owned(), |t| t.to_string()),
            if got_there { "reached" } else { "missed" },
            report.checkpoints,
            if report.restarted {
                "restarted"
            } else {
                "kept"
            },
            self.expect,
            report
                .boot_snapshot
                .as_ref()
                .map_or_else(String::new, |c| format!("; this boot snapshotted: {c}")),
        );
        self.turns.push(report);
    }

    /// No level-9 file appeared since the first turn's, unless a new UTC day began.
    fn check_snapshots(&self, at: &str) {
        let Some(first) = &self.l9 else { return };
        let now = snapshots(&self.fixture.replica).expect("l9");
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
        let previous = position(&self.fixture).expect("position");
        self.look_for_tail(kind, previous, &self.at(turn, kind));
        let mut process = self.start(turn, kind);
        let at = self.at(turn, kind);
        process.wait_up(UP).unwrap_or_else(|e| panic!("{at}: {e}"));
        let boot_snapshot = self.boot(&mut process, turn, previous);
        if let Some(cause) = boot_snapshot {
            println!("{at}: the last boot snapshotted: {cause}");
        }
        self.work(&mut process);
        wait_uploaded(&mut process, &self.fixture).expect("uploaded");
        process.stop().unwrap_or_else(|e| panic!("{at}: {e}"));
        self.check_snapshots(&at);
        self.check_acknowledged(&at);
        let want = capture(&self.fixture, &self.held);
        let recorded = self.oracle.states.last().expect("states");
        assert!(
            self.oracle.same_but_seq(&recorded.pages, &want.pages),
            "{at}: the recorded end"
        );
        let out = self.output("latest.db");
        let result = restore_replica(&self.fixture.replica, RestoreTarget::Latest, &out);
        assert!(result.expect("sink").is_ok(), "{at}: Rust restores Latest");
        assert_eq!(
            restored(&out, self.oracle.root()),
            want,
            "{at}: Rust at Latest"
        );
        let out = self.output("go-latest.db");
        let result = restore_with_go(
            &self.go,
            &self.config,
            &self.fixture.db,
            &out,
            Target::Latest,
        )
        .expect("go runs");
        assert_eq!(result, GoRestore::Restored, "{at}: Go restores Latest");
        assert_eq!(
            restored(&out, self.oracle.root()),
            want,
            "{at}: Go at Latest"
        );
    }

    fn summary(&self) -> String {
        let mut lines = Vec::new();
        for kind in [Kind::Go, Kind::Rust] {
            let mine: Vec<_> = self.turns.iter().filter(|t| t.kind == kind).collect();
            for point in POINTS {
                let of: Vec<_> = mine.iter().filter(|t| t.point == point).collect();
                if of.is_empty() {
                    continue;
                }
                let hit = of.iter().filter(|t| t.reached).count();
                lines.push(format!("{kind:?} {point:?} {hit}/{}", of.len()));
            }
            let checkpoints: i64 = mine.iter().map(|t| t.checkpoints).sum();
            let restarts = mine.iter().filter(|t| t.restarted).count();
            let snaps: Vec<_> = mine
                .iter()
                .filter_map(|t| t.boot_snapshot.clone())
                .collect();
            lines.push(format!(
                "{kind:?}: {checkpoints} checkpoints, {restarts} turns restarted the WAL, \
                 boot snapshots {snaps:?}"
            ));
        }
        lines.join(", ")
    }

    /// Both kinds checkpointed and restarted the WAL at least once: the run crossed what it is
    /// for.
    fn assert_crossed(&self) {
        for kind in [Kind::Go, Kind::Rust] {
            let mine = || self.turns.iter().filter(move |t| t.kind == kind);
            let checkpoints: i64 = mine().map(|t| t.checkpoints).sum();
            assert!(
                checkpoints > 0,
                "{} seed {}: {kind:?} never checkpointed",
                self.name,
                self.seed
            );
            assert!(
                mine().any(|t| t.restarted),
                "{} seed {}: {kind:?} never restarted the WAL",
                self.name,
                self.seed
            );
        }
    }
}

/// Alternates Go and Rust on one prefix for one build, from `seed`, Rust started as `launch`
/// says.
pub fn alternate(build: &GoBuild, lsr: &Path, seed: u64, launch: Launch) {
    let mut run = Run::new(build, lsr, seed, launch);
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
        "{}: seed {seed}: {KILLED_TURNS} switches, {} transactions restored, {} commits \
         ({} retried busy); Go booted on an uncommitted WAL tail {} times and took it {} \
         times (#38; reads the wal-index end: {}); {}",
        run.name,
        run.checked,
        run.oracle.states.len() - 1,
        run.busy,
        run.tails,
        run.taken,
        run.fixed,
        run.summary()
    );
    run.assert_crossed();
}

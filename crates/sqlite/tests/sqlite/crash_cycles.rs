//! Crash cycles under the block crash law, with real SQLite and the real engine, judged by
//! restored content.
//!
//! Each run draws a disk ([`CrashLaw::drawn`]: 512 or 4096-byte blocks) and a workload from its
//! seed. Each cycle runs a segment of application commits, engine ticks and checkpoints, then
//! the power fails: at the `nth` write, sync or truncate of the WAL or the database (inside an
//! application statement or a tick), or between steps. The fault VFS leaves the files as the
//! law draws them; SQLite's own recovery reads them when the application opens the database
//! again; a new engine boots on the same staging directory and replica; the application may
//! commit before the boot; the engine settles. Then the replica's latest state is restored and
//! judged.
//!
//! The judge shares nothing with the engine. The workload keeps its own record of every state
//! it committed (rows it chose, never read back from SQLite), and [`History`] holds a digest of
//! each. A restored database must equal one of those states (a commit SQLite acknowledged, or
//! one whose statement the loss interrupted, which SQLite may have made durable). After a settle
//! with the power on, the restore must equal the database itself. SQLite's recovery must also
//! leave one of those states, and the simulator's oracle judges every transaction on the
//! replica at the end of a run.
//!
//! The law, the weights and the budget below were frozen before the first run against an engine
//! with a known defect (see the commit that added this file). Change them only in a commit of
//! their own, and report seeds to first failure again.
//!
//! [`History`] is where a history checker plugs in: it sees every state with its outcome and
//! answers for each restore.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::{NonZeroI32, NonZeroU32};
use std::path::Path;

use lsr_core::conformance::block_on;
use lsr_core::{Fault as EngineFault, RestoreTarget};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{CrashLaw, Effect, Fault, FileKind, Op, Rig, RigConfig, RigError};
use tempfile::TempDir;

/// Cycles a run makes; each ends with one power loss.
pub const CYCLES: u32 = 12;
/// Steps in a cycle's segment, at most.
const SEGMENT: u64 = 24;
/// Rows the table holds at most; inserts become updates past it.
const MAX_ROWS: usize = 40;

/// The frozen weights of a segment's steps.
const WEIGHTS: [(Step, u64); 8] = [
    (Step::Insert, 30),
    (Step::Update, 25),
    (Step::Delete, 8),
    (Step::MultiRow, 7),
    (Step::Tick, 15),
    (Step::Settle, 8),
    (Step::AppCheckpoint, 4),
    (Step::RestartEngine, 3),
];

/// Where an armed loss fires, and the frozen weights: the WAL's writes most.
const CRASH_AT: [(FileKind, Op, u64); 5] = [
    (FileKind::Wal, Op::Write, 4),
    (FileKind::Wal, Op::Sync, 2),
    (FileKind::Main, Op::Write, 2),
    (FileKind::Main, Op::Sync, 1),
    (FileKind::Wal, Op::Truncate, 1),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Insert,
    Update,
    Delete,
    MultiRow,
    Tick,
    Settle,
    AppCheckpoint,
    RestartEngine,
}

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, upper: u64) -> u64 {
        self.next() % upper.max(1)
    }

    fn pick<T: Copy>(&mut self, table: &[(T, u64)]) -> T {
        let total: u64 = table.iter().map(|(_, w)| w).sum();
        let mut left = self.below(total);
        for (item, weight) in table {
            if left < *weight {
                return *item;
            }
            left -= weight;
        }
        table[0].0
    }
}

type Rows = BTreeMap<i64, Vec<u8>>;

/// A digest of a table's content.
fn digest(rows: &Rows) -> (u64, u64) {
    let mut a = DefaultHasher::new();
    let mut b = DefaultHasher::new();
    0_u8.hash(&mut a);
    1_u8.hash(&mut b);
    rows.hash(&mut a);
    rows.hash(&mut b);
    (a.finish(), b.finish())
}

/// What went wrong, by kind: the report counts each kind on its own.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Anomaly {
    /// The restored database is no state the application committed.
    RestoredUnknown,
    /// After a settle with the power on, the restore is not the database.
    RestoredNotLatest,
    /// The restored database fails `integrity_check`, or does not open.
    RestoredCorrupt(String),
    /// SQLite's recovery left a state the application never committed.
    RecoveredUnknown,
    /// The engine stopped, or did not settle.
    Engine(String),
    /// The simulator's oracle found the replica wrong.
    Oracle(String),
}

/// Every state the workload committed, by outcome. The history checker's seat.
#[derive(Default)]
struct History {
    acked: BTreeSet<(u64, u64)>,
    interrupted: BTreeSet<(u64, u64)>,
}

impl History {
    fn committed(&mut self, rows: &Rows) {
        self.acked.insert(digest(rows));
    }

    fn interrupted(&mut self, rows: &Rows) {
        self.interrupted.insert(digest(rows));
    }

    fn knows(&self, rows: &Rows) -> bool {
        let d = digest(rows);
        self.acked.contains(&d) || self.interrupted.contains(&d)
    }
}

/// One run's state.
struct Run {
    rig: Rig<Sqlite>,
    rng: Rng,
    rows: Rows,
    next_id: i64,
    version: u64,
    history: History,
    root: std::path::PathBuf,
}

/// The value of row `id` at `version`: the workload's own bytes.
fn value(id: i64, version: u64, len: u64) -> Vec<u8> {
    let mut rng = Rng(version.wrapping_mul(0x1000_0001) ^ id.unsigned_abs());
    (0..len).map(|_| rng.next().to_le_bytes()[0]).collect()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        write!(s, "{b:02x}").unwrap();
        s
    })
}

/// The table's content as SQLite reads it through `conn`.
fn read_rows(conn: &rusqlite::Connection) -> rusqlite::Result<Rows> {
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id")?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect()
}

impl Run {
    fn new(seed: u64, root: &Path) -> Self {
        let mut rng = Rng(seed);
        let law = CrashLaw::drawn(rng.next());
        let prng = NonZeroI32::new(i32::try_from(rng.below(1 << 30)).unwrap() + 1).unwrap();
        let cfg = RigConfig {
            power: true,
            synchronous: "NORMAL",
            crash_law: law,
            seed: prng,
            ..RigConfig::default()
        };
        let rig = Rig::<Sqlite>::new(root, cfg).unwrap();
        let mut history = History::default();
        history.committed(&Rows::new());
        Self {
            rig,
            rng,
            rows: Rows::new(),
            next_id: 1,
            version: 0,
            history,
            root: root.to_owned(),
        }
    }

    fn len(&mut self) -> u64 {
        if self.rng.below(3) == 0 {
            50 + self.rng.below(150)
        } else {
            1000 + self.rng.below(2500)
        }
    }

    /// One write of the workload: the SQL and the state it leaves.
    fn write(&mut self, rows: &mut Rows) -> String {
        self.version += 1;
        let len = self.len();
        let existing: Vec<i64> = rows.keys().copied().collect();
        let update = !existing.is_empty() && (rows.len() >= MAX_ROWS || self.rng.below(2) == 0);
        if update {
            let at = self.rng.below(u64::try_from(existing.len()).unwrap());
            let id = existing[usize::try_from(at).unwrap()];
            let v = value(id, self.version, len);
            let sql = format!("UPDATE t SET v = x'{}' WHERE id = {id};", hex(&v));
            rows.insert(id, v);
            sql
        } else {
            let id = self.next_id;
            self.next_id += 1;
            let v = value(id, self.version, len);
            let sql = format!("INSERT INTO t(id, v) VALUES ({id}, x'{}');", hex(&v));
            rows.insert(id, v);
            sql
        }
    }

    fn delete(&mut self, rows: &mut Rows) -> String {
        let at = self.rng.below(u64::try_from(rows.len()).unwrap());
        let Some(id) = rows.keys().nth(usize::try_from(at).unwrap()) else {
            return self.write(rows);
        };
        let id = *id;
        rows.remove(&id);
        format!("DELETE FROM t WHERE id = {id};")
    }

    /// Runs one commit; the state is acknowledged when SQLite says so.
    fn commit(&mut self, step: Step) -> Result<(), RigError> {
        let mut next = self.rows.clone();
        let sql = match step {
            Step::Delete => self.delete(&mut next),
            Step::MultiRow => {
                let n = 2 + self.rng.below(4);
                let body: String = (0..n).map(|_| self.write(&mut next)).collect();
                format!("BEGIN; {body} COMMIT;")
            }
            Step::Insert
            | Step::Update
            | Step::Tick
            | Step::Settle
            | Step::AppCheckpoint
            | Step::RestartEngine => self.write(&mut next),
        };
        match self.rig.exec(&sql) {
            Ok(()) => {
                self.history.committed(&next);
                self.rows = next;
                Ok(())
            }
            Err(err) => {
                self.history.interrupted(&next);
                Err(err)
            }
        }
    }

    /// One step; `Err` only for what is not a power loss's doing.
    fn step(&mut self, step: Step) -> Result<(), Anomaly> {
        let done = match step {
            Step::Insert | Step::Update | Step::Delete | Step::MultiRow => self.commit(step),
            Step::Tick => self.rig.tick().map(drop),
            Step::Settle => self.settle(),
            Step::AppCheckpoint => {
                let mode = if self.rng.below(3) == 0 {
                    "TRUNCATE"
                } else {
                    "PASSIVE"
                };
                self.rig
                    .query(&format!("PRAGMA wal_checkpoint({mode})"))
                    .map(drop)
            }
            Step::RestartEngine => self.rig.restart(),
        };
        match done {
            Err(_) if !self.rig.vfs().powered() => Ok(()),
            Err(err) => Err(Anomaly::Engine(format!("{step:?}: {err}"))),
            Ok(()) => Ok(()),
        }
    }

    /// Settles; a boot that waits for the application's next commit gets one (as in
    /// `power_loss.rs`).
    fn settle(&mut self) -> Result<(), RigError> {
        match self.rig.settle() {
            Err(RigError::Unsettled(_))
                if self.rig.faults().last() == Some(&EngineFault::WalMoved) =>
            {
                self.commit(Step::Insert)?;
                self.rig.settle()
            }
            other => other,
        }
    }

    /// Arms the cycle's loss inside a call, or returns `false` for a loss between steps.
    fn arm_loss(&mut self) -> bool {
        if self.rng.below(2) == 0 {
            return false;
        }
        let table: Vec<((FileKind, Op), u64)> =
            CRASH_AT.iter().map(|(k, o, w)| ((*k, *o), *w)).collect();
        let (kind, op) = self.rng.pick(&table);
        let nth = NonZeroU32::new(1 + u32::try_from(self.rng.below(40)).unwrap()).unwrap();
        self.rig.vfs().arm(Fault {
            kind,
            op,
            nth,
            effect: Effect::PowerLoss,
        });
        true
    }

    /// A segment, then the loss.
    fn segment(&mut self) -> Result<(), Anomaly> {
        self.rig.vfs().reset_calls();
        self.arm_loss();
        self.rig.faults_everywhere(true);
        let steps = 1 + self.rng.below(SEGMENT);
        for _ in 0..steps {
            if !self.rig.vfs().powered() {
                break;
            }
            let step = self.rng.pick(&WEIGHTS);
            self.step(step)?;
        }
        self.rig.faults_everywhere(false);
        self.rig.vfs().disarm();
        if self.rig.vfs().powered() {
            self.rig.vfs().cut_power();
        }
        Ok(())
    }

    /// The reboot: SQLite recovers, the recovered state is judged and becomes the workload's.
    fn reboot(&mut self) -> Result<(), Anomaly> {
        self.rig
            .reboot_after_power_loss()
            .map_err(|err| Anomaly::Engine(format!("reboot: {err}")))?;
        let app = self
            .rig
            .app()
            .map_err(|err| Anomaly::Engine(format!("app after recovery: {err}")))?;
        let recovered =
            read_rows(app).map_err(|err| Anomaly::Engine(format!("read after recovery: {err}")))?;
        if !self.history.knows(&recovered) {
            return Err(Anomaly::RecoveredUnknown);
        }
        self.rows = recovered;
        for _ in 0..self.rng.below(4) {
            self.step(Step::Insert)?;
        }
        Ok(())
    }

    /// Restores the replica's latest state into a file and reads it with plain SQLite.
    fn restore(&self, name: &str) -> Result<Rows, Anomaly> {
        let target = self.root.join(name);
        drop(std::fs::remove_file(&target));
        let sink = lsr_local::FileSink::create(&target)
            .map_err(|err| Anomaly::RestoredCorrupt(format!("sink: {err}")))?;
        block_on(lsr_core::restore(
            &lsr_store_fs::FsStore::new(self.rig.replica_root()),
            RestoreTarget::Latest,
            sink,
        ))
        .map_err(|err| Anomaly::RestoredCorrupt(format!("restore: {err}")))?;
        let conn = rusqlite::Connection::open(&target)
            .map_err(|err| Anomaly::RestoredCorrupt(err.to_string()))?;
        let check: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(|err| Anomaly::RestoredCorrupt(err.to_string()))?;
        if check != "ok" {
            return Err(Anomaly::RestoredCorrupt(check));
        }
        read_rows(&conn).map_err(|err| Anomaly::RestoredCorrupt(err.to_string()))
    }

    /// The cycle's judgment, after the reboot's settle.
    fn judge(&mut self) -> Result<(), Anomaly> {
        if let Err(err) = self.settle() {
            let faults = self.rig.faults();
            let tail = &faults[faults.len().saturating_sub(3)..];
            let tick = self.rig.ticks().last();
            return Err(Anomaly::Engine(format!(
                "settle: {err}; last faults {tail:?}; last tick {tick:?}"
            )));
        }
        let restored = self.restore("restored.db")?;
        if !self.history.knows(&restored) {
            return Err(Anomaly::RestoredUnknown);
        }
        if restored != self.rows {
            return Err(Anomaly::RestoredNotLatest);
        }
        Ok(())
    }

    fn cycle(&mut self) -> Result<(), Anomaly> {
        self.segment()?;
        self.reboot()?;
        self.judge()
    }
}

/// The first anomaly of the run `seed` names, and the cycle it came in (`CYCLES` for the
/// final oracle).
pub fn run(seed: u64, cycles: u32) -> Option<(u32, Anomaly)> {
    let dir = TempDir::new().unwrap();
    let mut run = Run::new(seed, dir.path());
    for cycle in 0..cycles {
        if let Err(anomaly) = run.cycle() {
            return Some((cycle, anomaly));
        }
    }
    run.rig
        .check()
        .err()
        .map(|err| (cycles, Anomaly::Oracle(err.to_string())))
}

/// A few frozen seeds in the gate.
#[test]
fn crash_cycles_under_the_block_law_restore_committed_states() {
    for seed in 0..3 {
        if let Some((cycle, anomaly)) = run(seed, CYCLES) {
            panic!("seed {seed} cycle {cycle}: {anomaly:?}");
        }
    }
}

/// The campaign: `LSR_CRASH_SEEDS` seeds from `LSR_CRASH_BASE`, every anomaly printed with its
/// seed and cycle, the first one failing the test after all seeds ran.
#[test]
#[ignore = "a campaign; run by hand"]
fn crash_cycle_campaign() {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let (base, seeds) = (env("LSR_CRASH_BASE", 0), env("LSR_CRASH_SEEDS", 50));
    let mut found = Vec::new();
    for seed in base..base + seeds {
        let got = run(seed, CYCLES);
        println!("crash-cycle seed {seed}: {got:?}");
        if let Some(got) = got {
            found.push((seed, got));
        }
    }
    assert!(found.is_empty(), "{found:#?}");
}

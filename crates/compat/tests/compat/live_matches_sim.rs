//! The live SQLite source and the simulator's model of it, driven by one op list.
//!
//! A seeded list of ops runs against real SQLite through [`LiveSqlite`] and against
//! [`SimSqlite`], each under its own sync engine (`recover_traced`, `sync_step`, `checkpoint`).
//! After every op the staged LTX files must be byte-equal, and so must the wal-index read, the WAL file's
//! length and the database file's page count.
//!
//! What the model is fed, and what it must work out:
//! - **Fed, because the content is SQLite's:** the pages of every application commit and of every
//!   `bump_seq` frame. After each real write the new transactions are read back from the real WAL
//!   file and committed to the model, so both sides hold the same page bytes. The model never sees
//!   an SQL statement.
//! - **Fed, because SQLite draws it at random:** the salts a fresh WAL header or a restart draws
//!   (`force_random_salts`, set to the salts in the real wal-index after the op). `salt1 + 1` on a
//!   restart is not fed: the model has to produce it, or the LTX bytes differ.
//! - **Not fed, so compared:** when the WAL restarts, what a checkpoint backfills and how far, what
//!   each read mark pins, the WAL offsets and size every file records, the database size, the
//!   checksums, every checkpoint's three numbers and read-lock hold, and the sequence of calls the sync
//!   engine makes of its checkpointer.
//!
//! Known difference, accounted for: the model's `bump_seq` writes page 1, real SQLite writes the
//! page of `_litestream_seq`. The model is given the real transaction, committed as the app
//! connection (the model's own pool connection would only change the header's checkpoint sequence,
//! which no LTX file records).

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use std::collections::VecDeque;
use std::fs;
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use lsr_core::conformance::block_on;
use lsr_core::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, Level, MemStore,
    ObjectStore, PageLimit, PageSource, Position, PositionSource, Probes, ReadLen, SourceError,
    Step, StepEnd, SyncConfig, Synced, SyncedThrough, Verdict, WalSource, WalView, WriteLock,
    checkpoint, recover_traced, sync_step,
};
use lsr_ltx::{PageNo, PageSize, Timestamp};
use lsr_sim::{OtherProcessMode, ReaderId, SimCheckpointer, SimPages, SimSource, SimSqlite};
use lsr_sqlite::Sqlite as LiveSqlite;
use lsr_wal::{Salts, WalReader};
use proptest::prelude::*;
use rusqlite::Connection;
use tempfile::TempDir;

/// A step's end as the engine takes it: the proof when the read-lock hold still
/// holds, else the unpinned position and no proof.
fn confirmed<W: WalSource>(
    reached: lsr_core::ReachedEnd,
    wal: &W,
) -> (Option<Position>, Option<SyncedThrough>) {
    match reached.confirm(wal) {
        Ok(through) => (Some(*through.position()), Some(through)),
        Err(position) => (Some(position), None),
    }
}

const WAL_HEADER: usize = 32;
const FRAME_HEADER: usize = 24;

/// One committed transaction of the real WAL: its pages and the database size it ends at.
#[derive(Clone, Debug)]
struct Tx {
    pages: Vec<(u32, Vec<u8>)>,
    db_size: u32,
}

/// Reads the real WAL file for transactions the model has not been given yet.
#[derive(Debug)]
struct Mirror {
    wal: PathBuf,
    page_size: usize,
    /// The generation last read and how many frames of it are consumed.
    seen: Option<(Salts, usize)>,
}

fn be(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

impl Mirror {
    /// The committed transactions written since the last call. A frame belongs to the
    /// generation only while its salts equal the header's, which is also how SQLite reads a WAL.
    fn drain(&mut self) -> Vec<Tx> {
        let Ok(bytes) = fs::read(&self.wal) else {
            self.seen = None;
            return Vec::new();
        };
        if bytes.len() < WAL_HEADER {
            self.seen = None;
            return Vec::new();
        }
        let salts = Salts {
            salt1: be(&bytes, 16),
            salt2: be(&bytes, 20),
        };
        let frame_len = FRAME_HEADER + self.page_size;
        let mut consumed = match self.seen {
            Some((seen, consumed)) if seen == salts => consumed,
            _ => 0,
        };
        let (mut txs, mut pending) = (Vec::new(), Vec::new());
        let mut at = WAL_HEADER + consumed * frame_len;
        while let Some(frame) = bytes.get(at..at + frame_len) {
            if be(frame, 8) != salts.salt1 || be(frame, 12) != salts.salt2 {
                break;
            }
            pending.push((be(frame, 0), frame[FRAME_HEADER..].to_vec()));
            at += frame_len;
            let commit = be(frame, 4);
            if commit != 0 {
                consumed = (at - WAL_HEADER) / frame_len;
                txs.push(Tx {
                    pages: mem::take(&mut pending),
                    db_size: commit,
                });
            }
        }
        self.seen = Some((salts, consumed));
        txs
    }
}

fn locked<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared.lock().expect("no panic while locked")
}

fn commit_to_model(sim: &SimSqlite, tx: &Tx) -> bool {
    let pages: Vec<(PageNo, Vec<u8>)> = tx
        .pages
        .iter()
        .map(|(pgno, data)| {
            (
                PageNo::new(*pgno, sim.page_size()).expect("a page number SQLite wrote"),
                data.clone(),
            )
        })
        .collect();
    sim.commit(&pages, tx.db_size).is_some()
}

/// What the engine asked of its checkpointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Call {
    Barrier,
    EndBarrier,
    Passive,
    Truncate,
    BumpSeq,
    WriteLock,
    EndWriteLock,
}

/// One call on the real side and what it left behind.
#[derive(Debug)]
struct Event {
    call: Call,
    /// The salts in the real wal-index afterwards.
    salts: Salts,
    /// The transactions a `bump_seq` wrote.
    txs: Vec<Tx>,
    run: Option<CheckpointRun>,
}

type Log = Arc<Mutex<VecDeque<Event>>>;

fn real_salts(live: &LiveSqlite) -> Salts {
    live.clone()
        .committed()
        .expect("wal-index readable between steps")
        .end()
        .salts()
}

/// The real checkpointer, recording each call for the model's side to replay.
struct RealCk {
    live: LiveSqlite,
    mirror: Arc<Mutex<Mirror>>,
    log: Log,
}

impl RealCk {
    fn record(&self, call: Call, run: Option<CheckpointRun>) {
        let txs = if call == Call::BumpSeq {
            locked(&self.mirror).drain()
        } else {
            Vec::new()
        };
        locked(&self.log).push_back(Event {
            call,
            salts: real_salts(&self.live),
            txs,
            run,
        });
    }
}

impl Checkpointer for RealCk {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        let barrier = self.live.begin_barrier()?;
        self.record(Call::Barrier, None);
        Ok(barrier)
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.live.end_barrier(barrier)?;
        self.record(Call::EndBarrier, None);
        Ok(())
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let run = self.live.checkpoint(mode)?;
        let call = match mode {
            CheckpointMode::Passive => Call::Passive,
            CheckpointMode::Truncate => Call::Truncate,
        };
        self.record(call, Some(run));
        Ok(run)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.live.bump_seq()?;
        self.record(Call::BumpSeq, None);
        Ok(())
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        let lock = self.live.begin_write_lock()?;
        self.record(Call::WriteLock, None);
        Ok(lock)
    }

    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError> {
        self.live.end_write_lock(lock)?;
        self.record(Call::EndWriteLock, None);
        Ok(())
    }
}

/// A checkpoint's numbers and read-lock hold on both sides.
fn same_run(call: Call, model: &CheckpointRun, real: Option<CheckpointRun>) {
    assert_eq!(
        format!("{model:?}"),
        format!("{:?}", real.expect("a checkpoint event carries its run")),
        "{call:?}: the checkpoint's numbers or read-lock hold differ"
    );
}

/// The model's checkpointer: each call replays the next real one, in order.
struct ModelCk {
    sim: SimSqlite,
    ck: SimCheckpointer,
    log: Log,
}

impl ModelCk {
    /// The real call that matches this one, with the salts SQLite chose fed to the model.
    fn next(&self, call: Call) -> Event {
        let event = locked(&self.log)
            .pop_front()
            .unwrap_or_else(|| panic!("the model called {call:?}; real SQLite's engine did not"));
        assert_eq!(
            event.call, call,
            "the engines called their checkpointers in different orders"
        );
        self.sim.force_random_salts(Some(event.salts));
        event
    }
}

impl Checkpointer for ModelCk {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.next(Call::Barrier);
        self.ck.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.next(Call::EndBarrier);
        self.ck.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let call = match mode {
            CheckpointMode::Passive => Call::Passive,
            CheckpointMode::Truncate => Call::Truncate,
        };
        let event = self.next(call);
        let run = self.ck.checkpoint(mode)?;
        same_run(call, &run, event.run);
        Ok(run)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        let event = self.next(Call::BumpSeq);
        for tx in &event.txs {
            if !commit_to_model(&self.sim, tx) {
                return Err(SourceError::Busy);
            }
        }
        Ok(())
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        self.next(Call::WriteLock);
        self.ck.begin_write_lock()
    }

    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError> {
        self.next(Call::EndWriteLock);
        self.ck.end_write_lock(lock)
    }
}

/// A sync engine over one side's three doors, with its own staging store.
struct Engine<S: WalSource, P: PageSource, C: Checkpointer> {
    src: S,
    pg: P,
    ck: C,
    staging: MemStore,
    cfg: SyncConfig,
    through: Option<SyncedThrough>,
    position: Option<Position>,
    now: i64,
}

impl<S: WalSource, P: PageSource, C: Checkpointer> Engine<S, P, C> {
    fn new(src: S, pg: P, ck: C) -> Self {
        Self {
            src,
            pg,
            ck,
            staging: MemStore::new(),
            cfg: SyncConfig::default(),
            through: None,
            position: None,
            now: 1_000,
        }
    }

    /// The WAL as one read of its header and one read of the wal-index see it; none while the
    /// file has no header (a TRUNCATE just emptied it).
    fn view(&mut self) -> Option<(WalReader<S::Wal>, WalView, CommittedRead)> {
        let file = self.src.open().expect("open")?;
        let wal = WalReader::open(file).ok()?;
        let committed = self.src.committed().expect("committed");
        let view = WalView {
            header: Some(*wal.header()),
            len: self.src.len().expect("len"),
            committed: Some(committed),
            epoch: committed.epoch(),
        };
        Some((wal, view, committed))
    }

    fn verdict(&self, wal: &mut WalReader<S::Wal>, view: &WalView) -> Verdict {
        match self.position.as_ref() {
            None => recover_traced(None, view, None),
            Some(position) => {
                let probes = Probes::read(wal, position).expect("probes");
                recover_traced(Some(PositionSource::Running(position, probes)), view, None)
            }
        }
    }

    /// Recovers, runs the step the decision calls for, and says what happened.
    fn sync(&mut self) -> String {
        let Some((mut wal, view, committed)) = self.view() else {
            return "no WAL header".to_owned();
        };
        let verdict = self.verdict(&mut wal, &view);
        let seen = format!("{verdict:?} {committed:?}");
        let Ok(step) = Step::try_from(verdict.recovered) else {
            return format!("stop {seen}");
        };
        self.now += 10;
        let synced = block_on(sync_step(
            step,
            &mut wal,
            committed,
            &mut self.pg,
            &mut lsr_core::PageChecksumTable::new(),
            &self.staging,
            &self.cfg,
            Timestamp::from_unix_millis(self.now),
        ))
        .expect("step");
        match synced {
            Synced::Idle(through) => {
                (self.position, self.through) = confirmed(through, &self.src);
                format!("{seen} idle")
            }
            Synced::Wrote {
                file,
                end: StepEnd::Through(through),
                txs,
                ..
            } => {
                (self.position, self.through) = confirmed(through, &self.src);
                format!("{seen} wrote {file:?} over {txs} transactions")
            }
            Synced::Wrote {
                end: StepEnd::Limited(_),
                ..
            } => panic!("a test chain never reaches the WAL cap"),
            Synced::IndexReset
            | Synced::Wrote {
                end: StepEnd::Unproven(_),
                ..
            } => panic!("a test chain never meets a wal-index reset ahead of its header"),
        }
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> String {
        let Some(proof) = self.through.take() else {
            return "no proof".to_owned();
        };
        self.now += 10;
        let done = block_on(checkpoint(
            proof,
            mode,
            &mut self.src,
            &mut self.pg,
            &mut lsr_core::PageChecksumTable::new(),
            &mut self.ck,
            &self.staging,
            &self.cfg,
            Timestamp::from_unix_millis(self.now),
        ))
        .expect("checkpoint");
        let did = format!("{:?} {:?}", done.outcome, done.staged.files());
        self.position = Some(done.staged.position());
        self.through = done.staged.into_proof();
        did
    }

    /// Every staged file, whole, in key order.
    fn staged(&self) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        for level in Level::ALL {
            let mut after = None;
            loop {
                let page = block_on(self.staging.list(level, after, PageLimit::MAX)).expect("list");
                for info in page.files {
                    let bytes = block_on(self.staging.read(
                        &info.key,
                        0,
                        ReadLen::new(ReadLen::MAX_RAW).expect("a read length"),
                    ))
                    .expect("read");
                    out.push((
                        format!(
                            "{:?} {:?}",
                            info.key,
                            info.created_at().expect("an exact created-at time").at()
                        ),
                        bytes,
                    ));
                }
                after = page.next;
                if after.is_none() {
                    break;
                }
            }
        }
        out
    }
}

/// The checkpoint modes the engine runs.
#[derive(Clone, Copy, Debug)]
enum Ours {
    Passive,
    Truncate,
}

#[derive(Clone, Copy, Debug)]
enum Op {
    /// One transaction inserting this many rows of this many bytes.
    Insert { rows: u8, bytes: u16 },
    /// One transaction rewriting every `every`th row with new bytes.
    Overwrite { every: u8, bytes: u16 },
    /// Empties the table, then VACUUMs: a commit whose database size is smaller.
    Shrink,
    /// `recover` and the step it calls for.
    Sync,
    /// Our checkpoint protocol.
    Checkpoint(Ours),
    /// An application's own `PRAGMA wal_checkpoint`.
    Outside(OtherProcessMode),
    /// Another connection starts (true) or ends (false) a read transaction.
    Reader(bool),
}

struct World {
    _dir: TempDir,
    app: Connection,
    path: PathBuf,
    live: LiveSqlite,
    sim: SimSqlite,
    mirror: Arc<Mutex<Mirror>>,
    log: Log,
    real: Engine<LiveSqlite, LiveSqlite, RealCk>,
    model: Engine<SimSource, SimPages, ModelCk>,
    reader: Option<(Connection, ReaderId)>,
}

impl World {
    fn new(page_size: u32) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("app.db");
        let app = Connection::open(&path).expect("open");
        app.busy_timeout(Duration::ZERO).expect("busy timeout");
        app.pragma_update(None, "page_size", page_size)
            .expect("page size");
        app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .expect("wal");
        app.pragma_update(None, "wal_autocheckpoint", 0)
            .expect("autocheckpoint");
        app.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .expect("schema");
        let live = LiveSqlite::open(&path).expect("live");
        let size = PageSize::new(page_size).expect("a page size SQLite allows");
        let db_pages = fs::metadata(&path).expect("stat").len() / u64::from(page_size);
        let sim = SimSqlite::new(size, u32::try_from(db_pages).expect("pages"), 1);
        let mirror = Arc::new(Mutex::new(Mirror {
            wal: live.wal_path(),
            page_size: size.bytes(),
            seen: None,
        }));
        let log = Log::default();
        let real = Engine::new(
            live.clone(),
            live.clone(),
            RealCk {
                live: live.clone(),
                mirror: Arc::clone(&mirror),
                log: Arc::clone(&log),
            },
        );
        let model = Engine::new(
            sim.source(),
            sim.pages(),
            ModelCk {
                sim: sim.clone(),
                ck: sim.checkpointer(),
                log: Arc::clone(&log),
            },
        );
        let world = Self {
            _dir: dir,
            app,
            path,
            live,
            sim,
            mirror,
            log,
            real,
            model,
            reader: None,
        };
        world.mirror_commits();
        assert!(world.sim.our_reader_begin());
        world
    }

    /// Gives the model what the application just wrote.
    fn mirror_commits(&self) {
        self.sim.force_random_salts(Some(real_salts(&self.live)));
        for tx in locked(&self.mirror).drain() {
            assert!(
                commit_to_model(&self.sim, &tx),
                "the model refused a commit SQLite made"
            );
        }
    }

    fn exec(&self, sql: &str, params: impl rusqlite::Params) {
        self.app.execute(sql, params).expect(sql);
        self.mirror_commits();
    }

    fn outside(&self, mode: OtherProcessMode) {
        let pragma = match mode {
            OtherProcessMode::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            OtherProcessMode::Restart => "PRAGMA wal_checkpoint(RESTART)",
            OtherProcessMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        };
        let real: (i64, i64, i64) = self
            .app
            .query_row(pragma, [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect(pragma);
        self.sim.force_random_salts(Some(real_salts(&self.live)));
        let model = self
            .sim
            .other_process_checkpoint(mode)
            .expect("the app is idle");
        assert_eq!(
            real,
            (
                i64::from(model.busy),
                i64::from(model.log),
                i64::from(model.checkpointed)
            ),
            "{pragma}: busy, log and checkpointed"
        );
    }

    fn reader(&mut self, begin: bool) {
        match (begin, self.reader.take()) {
            (true, None) => {
                let conn = Connection::open(&self.path).expect("open");
                conn.busy_timeout(Duration::ZERO).expect("busy timeout");
                conn.execute_batch("BEGIN").expect("begin");
                conn.query_row("SELECT COUNT(1) FROM t", [], |_row| Ok(()))
                    .expect("read mark");
                self.reader = Some((conn, self.sim.other_process_reader_begin()));
            }
            (false, Some((conn, id))) => {
                conn.execute_batch("ROLLBACK").expect("rollback");
                assert!(self.sim.other_process_reader_end(id));
            }
            (_, held) => self.reader = held,
        }
    }

    fn apply(&mut self, op: Op) {
        match op {
            Op::Insert { rows, bytes } => self.exec(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < ?1)
                 INSERT INTO t(v) SELECT randomblob(?2) FROM c",
                (rows, bytes),
            ),
            Op::Overwrite { every, bytes } => self.exec(
                "UPDATE t SET v = randomblob(?2) WHERE id % ?1 = 0",
                (every, bytes),
            ),
            Op::Shrink => {
                self.exec("DELETE FROM t", []);
                self.app.execute_batch("VACUUM").expect("vacuum");
                self.mirror_commits();
            }
            Op::Sync => self.sync_both(),
            Op::Checkpoint(ours) => {
                self.sync_both();
                let mode = match ours {
                    Ours::Passive => CheckpointMode::Passive,
                    Ours::Truncate => CheckpointMode::Truncate,
                };
                let real = self.real.checkpoint(mode);
                let model = self.model.checkpoint(mode);
                assert_eq!(real, model, "the checkpoint's outcome and files");
                assert!(
                    locked(&self.log).is_empty(),
                    "calls the model did not replay"
                );
            }
            Op::Outside(mode) => self.outside(mode),
            Op::Reader(begin) => self.reader(begin),
        }
    }

    fn sync_both(&mut self) {
        let real = self.real.sync();
        let model = self.model.sync();
        assert_eq!(real, model, "the verdict, the committed read and the step");
    }

    /// Everything the two sides must agree on between ops.
    fn assert_same(&mut self, history: &[Op]) {
        let (real, model) = (self.real.src.committed(), self.model.src.committed());
        assert_eq!(
            format!("{real:?}"),
            format!("{model:?}"),
            "wal-index after {history:?}"
        );
        assert_eq!(
            self.real.src.len(),
            self.model.src.len(),
            "WAL file length after {history:?}"
        );
        assert_eq!(
            self.real.pg.page_count(),
            self.model.pg.page_count(),
            "database pages after {history:?}"
        );
        let (real, model) = (self.real.staged(), self.model.staged());
        assert_eq!(
            real.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            model.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            "staged files after {history:?}"
        );
        for ((name, real), (_, model)) in real.iter().zip(&model) {
            let at = real.iter().zip(model).position(|(a, b)| a != b);
            assert!(
                real == model,
                "{name} differs after {history:?}: lengths {} and {}, first differing byte {at:?}",
                real.len(),
                model.len()
            );
        }
    }
}

fn run(page_size: u32, ops: &[Op]) {
    let mut world = World::new(page_size);
    for (done, op) in ops.iter().enumerate() {
        world.apply(*op);
        world.assert_same(&ops[..=done]);
    }
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (1_u8..12, 1_u16..5000).prop_map(|(rows, bytes)| Op::Insert { rows, bytes }),
        2 => (1_u8..4, 1_u16..5000).prop_map(|(every, bytes)| Op::Overwrite { every, bytes }),
        1 => Just(Op::Shrink),
        4 => Just(Op::Sync),
        1 => Just(Op::Checkpoint(Ours::Passive)),
        2 => Just(Op::Checkpoint(Ours::Truncate)),
        1 => Just(Op::Outside(OtherProcessMode::Passive)),
        1 => Just(Op::Outside(OtherProcessMode::Restart)),
        1 => Just(Op::Outside(OtherProcessMode::Truncate)),
        1 => any::<bool>().prop_map(Op::Reader),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn live_matches_sim(page_size in prop_oneof![Just(1024_u32), Just(4096_u32)], ops in prop::collection::vec(op(), 1..20)) {
        run(page_size, &ops);
    }
}

#[test]
fn commits_overwrites_and_growth() {
    run(
        4096,
        &[
            Op::Sync,
            Op::Insert {
                rows: 1,
                bytes: 100,
            },
            Op::Sync,
            Op::Insert {
                rows: 12,
                bytes: 3000,
            },
            Op::Insert {
                rows: 1,
                bytes: 9000,
            },
            Op::Sync,
            Op::Overwrite {
                every: 2,
                bytes: 3500,
            },
            Op::Overwrite {
                every: 1,
                bytes: 50,
            },
            Op::Sync,
            Op::Checkpoint(Ours::Passive),
            Op::Insert {
                rows: 3,
                bytes: 700,
            },
            Op::Sync,
        ],
    );
}

#[test]
fn shrink_commits_a_smaller_database() {
    run(
        1024,
        &[
            Op::Insert {
                rows: 11,
                bytes: 4000,
            },
            Op::Sync,
            Op::Checkpoint(Ours::Passive),
            Op::Shrink,
            Op::Sync,
            Op::Checkpoint(Ours::Passive),
            Op::Sync,
            Op::Insert {
                rows: 2,
                bytes: 100,
            },
            Op::Sync,
            Op::Checkpoint(Ours::Truncate),
            Op::Sync,
        ],
    );
}

#[test]
fn outside_checkpoints_and_restarts() {
    run(
        4096,
        &[
            Op::Insert {
                rows: 4,
                bytes: 2000,
            },
            Op::Sync,
            Op::Outside(OtherProcessMode::Passive),
            Op::Sync,
            Op::Outside(OtherProcessMode::Restart),
            Op::Insert {
                rows: 2,
                bytes: 500,
            },
            Op::Sync,
            Op::Checkpoint(Ours::Truncate),
            Op::Insert {
                rows: 2,
                bytes: 500,
            },
            Op::Sync,
            Op::Outside(OtherProcessMode::Truncate),
            Op::Insert { rows: 1, bytes: 10 },
            Op::Sync,
            Op::Reader(true),
            Op::Insert { rows: 1, bytes: 10 },
            Op::Outside(OtherProcessMode::Restart),
            Op::Reader(false),
            Op::Outside(OtherProcessMode::Truncate),
            Op::Sync,
            Op::Checkpoint(Ours::Passive),
        ],
    );
}

/// SQLite's backfill skips a page whose newest frame is above the lowest read mark, so the
/// database file is neither written nor grown for it (the model once copied the older frame).
#[test]
fn partial_backfill_skips_pages_rewritten_above_the_mark() {
    run(
        1024,
        &[
            Op::Shrink,
            Op::Outside(OtherProcessMode::Passive),
            Op::Sync,
            Op::Checkpoint(Ours::Passive),
            Op::Sync,
        ],
    );
}

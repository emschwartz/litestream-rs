//! The simulator's SQLite against this crate's, through the engine's doors.
//!
//! The simulator runs the engine against `SimSqlite`, a rule model of SQLite, through
//! `SimSource`, `SimPages` and `SimCheckpointer`. In production the engine runs against
//! [`lsr_sqlite::Sqlite`]. `lsr-sim`'s `sqlite_rules` holds the model's WAL file to SQLite's;
//! this holds the model's answers to the engine to this crate's answers, on one drawn history:
//! the application commits, other processes read and checkpoint, and the engine calls every
//! `WalSource`, `PageSource` and `Checkpointer` method in any order. Each answer must match:
//! the committed end (frame count, backfilled frames, how the salts moved), the WAL's length,
//! the page count, a checkpoint's numbers, whether the read-lock hold moved, and every error.
//! Page bytes are not compared (the model's pages are not b-tree pages); whether a page read
//! succeeds is.
//!
//! Three differences are known, each held by a `pin_` test that fails once the model gives
//! SQLite's answer (then the pin and its carve-out go): a second engine lock while one is held,
//! a sequence bump while a lock is held, and the salts of the first frame after the engine's
//! own TRUNCATE. The property leaves them out: one lock at a time, no bump under a lock, and
//! salts not compared. The engine itself keeps the first two rules.
//!
//! The schema puts tables `t0`..`t3` on pages 2..=5 and this crate's `_litestream_seq` and
//! `_litestream_lock` on 6 and 7, so an app commit of tables in `mask` writes one frame per
//! table on both sides.

#[cfg(test)]
mod diff {
    use std::fmt::Debug;
    use std::io::Read;
    use std::time::Duration;

    use lsr_core::{
        Barrier, CheckpointMode, Checkpointer, PageSource, ReadLockHold, WalSource, WriteLock,
    };
    use lsr_ltx::{PageNo, PageSize};
    use lsr_sim::{OtherProcessMode, ReaderId, SimCheckpointer, SimPages, SimSource, SimSqlite};
    use lsr_sqlite::Sqlite;
    use proptest::prelude::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    const PAGE_SIZE: u32 = 512;
    const PAGES: u32 = 7;
    const TABLES: u32 = 4;
    const OTHERS: usize = 2;

    #[derive(Clone, Copy, Debug)]
    enum Op {
        Commit(u8),
        OtherBegin(usize),
        OtherEnd(usize),
        AppCheckpoint(OtherProcessMode),
        Committed,
        Len,
        Open,
        PageCount,
        ReadPage(u32),
        Sync,
        Checkpoint(CheckpointMode),
        BumpSeq,
        BeginBarrier,
        EndBarrier,
        BeginWriteLock,
        EndWriteLock,
    }

    /// How a salt moved since the side's previous committed read.
    #[derive(Debug, PartialEq, Eq)]
    enum Moved {
        Same,
        PlusOne,
        Other,
    }

    /// What one side holds of the engine's two writer locks.
    enum Held {
        Barrier(Barrier),
        Lock(WriteLock),
    }

    /// One side's memory between steps, to say how salts and the hold moved.
    #[derive(Default)]
    struct Last {
        /// Leaves out the three known differences (module doc): only one lock at a time, no
        /// sequence bump under a lock, and salts not compared. The pins turn it off.
        known_out: bool,
        salts: Option<(u32, u32)>,
        epoch: Option<ReadLockHold>,
        held: Vec<Held>,
    }

    impl Last {
        fn moved(&mut self, salts: (u32, u32)) -> (Moved, Moved) {
            let moved = self
                .salts
                .map_or((Moved::Other, Moved::Other), |(one, two)| {
                    let first = match salts.0.wrapping_sub(one) {
                        0 => Moved::Same,
                        1 => Moved::PlusOne,
                        _ => Moved::Other,
                    };
                    let second = if salts.1 == two {
                        Moved::Same
                    } else {
                        Moved::Other
                    };
                    (first, second)
                });
            self.salts = Some(salts);
            moved
        }

        fn epoch_moved(&mut self, now: ReadLockHold) -> bool {
            self.epoch.replace(now).is_some_and(|was| was != now)
        }
    }

    fn show<T: Debug>(value: T) -> String {
        format!("{value:?}")
    }

    fn page_size() -> PageSize {
        PageSize::new(PAGE_SIZE).unwrap()
    }

    fn open(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.busy_timeout(Duration::ZERO).unwrap();
        lsr_sqlite::app_connection(&conn).unwrap();
        conn.execute_batch("PRAGMA synchronous = NORMAL;").unwrap();
        conn
    }

    fn pragma(conn: &Connection, mode: OtherProcessMode) -> String {
        let name = match mode {
            OtherProcessMode::Passive => "PASSIVE",
            OtherProcessMode::Restart => "RESTART",
            OtherProcessMode::Truncate => "TRUNCATE",
        };
        show(
            conn.query_row(&format!("PRAGMA wal_checkpoint({name})"), [], |row| {
                Ok((
                    row.get::<_, i64>(0)? != 0,
                    row.get::<_, u32>(1)?,
                    row.get::<_, u32>(2)?,
                ))
            })
            .unwrap(),
        )
    }

    fn tables(mask: u8) -> impl Iterator<Item = u32> {
        (0..TABLES).filter(move |table| mask & (1 << table) != 0)
    }

    /// The engine's calls, the same on both sides.
    fn engine<S, P, C>(src: &mut S, pages: &mut P, cp: &mut C, last: &mut Last, op: Op) -> String
    where
        S: WalSource,
        P: PageSource,
        C: Checkpointer,
    {
        match op {
            Op::Committed => show(src.committed().map(|read| {
                let salts = read.end().salts();
                let moved = last.moved((salts.salt1, salts.salt2));
                let moved = (!last.known_out).then_some(moved);
                (read.end().frame_count(), read.backfilled(), moved)
            })),
            Op::Len => show(src.len().map(|len| len.map(|l| l.bytes()))),
            Op::Open => show(src.open().map(|wal| {
                wal.map(|mut wal| {
                    let mut bytes = Vec::new();
                    wal.read_to_end(&mut bytes).map(|_| bytes.len())
                })
                .map(|read| read.map_err(|e| e.kind()))
            })),
            Op::PageCount => show(pages.page_count().map(|n| n.get())),
            Op::ReadPage(pgno) => {
                let mut buf = vec![0; 512];
                show(pages.read_page(PageNo::new(pgno, page_size()).unwrap(), &mut buf))
            }
            Op::Sync => show(src.sync()),
            Op::Checkpoint(mode) => show(
                cp.checkpoint(mode)
                    .map(|run| (run.busy, run.log, run.checkpointed)),
            ),
            Op::BumpSeq if last.known_out && !last.held.is_empty() => "released first".to_owned(),
            Op::BumpSeq => show(cp.bump_seq()),
            Op::BeginBarrier | Op::BeginWriteLock if last.known_out && !last.held.is_empty() => {
                "one at a time".to_owned()
            }
            Op::BeginBarrier => {
                let got = cp.begin_barrier();
                let out = show(got.as_ref().map(drop));
                last.held.extend(got.ok().map(Held::Barrier));
                out
            }
            Op::BeginWriteLock => {
                let got = cp.begin_write_lock();
                let out = show(got.as_ref().map(drop));
                last.held.extend(got.ok().map(Held::Lock));
                out
            }
            Op::EndBarrier | Op::EndWriteLock => end_held(cp, last, op),
            Op::Commit(_) | Op::OtherBegin(_) | Op::OtherEnd(_) | Op::AppCheckpoint(_) => {
                String::new()
            }
        }
    }

    /// Ends the newest held lock of the kind `op` names, as the engine would; nothing held of
    /// that kind is a no-op on both sides.
    fn end_held<C: Checkpointer>(cp: &mut C, last: &mut Last, op: Op) -> String {
        let at = last.held.iter().rposition(|held| {
            matches!(
                (held, op),
                (Held::Barrier(_), Op::EndBarrier) | (Held::Lock(_), Op::EndWriteLock)
            )
        });
        match at.map(|at| last.held.remove(at)) {
            None => "nothing held".to_owned(),
            Some(Held::Barrier(barrier)) => show(cp.end_barrier(barrier)),
            Some(Held::Lock(lock)) => show(cp.end_write_lock(lock)),
        }
    }

    /// This crate on real files.
    struct Real {
        _dir: TempDir,
        app: Connection,
        others: Vec<Connection>,
        reading: [bool; OTHERS],
        sqlite: Sqlite,
        last: Last,
    }

    impl Real {
        fn new(known_out: bool) -> Self {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("test.db");
            let app = open(&path);
            app.execute_batch(&format!(
                "PRAGMA page_size = {PAGE_SIZE}; PRAGMA journal_mode = WAL;
                 BEGIN;
                 CREATE TABLE t0 (v BLOB); CREATE TABLE t1 (v BLOB);
                 CREATE TABLE t2 (v BLOB); CREATE TABLE t3 (v BLOB);
                 CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
                 CREATE TABLE _litestream_lock (id INTEGER);
                 INSERT INTO t0 VALUES (randomblob(8)); INSERT INTO t1 VALUES (randomblob(8));
                 INSERT INTO t2 VALUES (randomblob(8)); INSERT INTO t3 VALUES (randomblob(8));
                 COMMIT;"
            ))
            .unwrap();
            let pages: u32 = app
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .unwrap();
            assert_eq!(pages, PAGES);
            pragma(&app, OtherProcessMode::Truncate);
            let sqlite = Sqlite::open(&path).unwrap();
            Self {
                others: (0..OTHERS).map(|_| open(&path)).collect(),
                _dir: dir,
                app,
                reading: [false; OTHERS],
                sqlite,
                last: Last {
                    known_out,
                    ..Last::default()
                },
            }
        }

        fn apply(&mut self, op: Op) -> String {
            match op {
                Op::Commit(mask) => {
                    let mut sql = String::from("BEGIN;");
                    for table in tables(mask) {
                        sql.push_str(&format!("UPDATE t{table} SET v = randomblob(8);"));
                    }
                    sql.push_str("COMMIT;");
                    let done = self.app.execute_batch(&sql).is_ok();
                    if !done {
                        drop(self.app.execute_batch("ROLLBACK"));
                    }
                    show(done)
                }
                Op::OtherBegin(at) if !self.reading[at] => {
                    let conn = &self.others[at];
                    conn.execute_batch("BEGIN").unwrap();
                    let _: i64 = conn
                        .query_row("SELECT count(*) FROM t0", [], |row| row.get(0))
                        .unwrap();
                    self.reading[at] = true;
                    String::new()
                }
                Op::OtherEnd(at) if self.reading[at] => {
                    self.others[at].execute_batch("COMMIT").unwrap();
                    self.reading[at] = false;
                    String::new()
                }
                Op::OtherBegin(_) | Op::OtherEnd(_) => String::new(),
                Op::AppCheckpoint(mode) => pragma(&self.app, mode),
                Op::Committed
                | Op::Len
                | Op::Open
                | Op::PageCount
                | Op::ReadPage(_)
                | Op::Sync
                | Op::Checkpoint(_)
                | Op::BumpSeq
                | Op::BeginBarrier
                | Op::EndBarrier
                | Op::BeginWriteLock
                | Op::EndWriteLock => {
                    let (mut src, mut pages, mut cp) = (
                        self.sqlite.clone(),
                        self.sqlite.clone(),
                        self.sqlite.clone(),
                    );
                    engine(&mut src, &mut pages, &mut cp, &mut self.last, op)
                }
            }
        }

        fn epoch_moved(&mut self) -> bool {
            let now = self.sqlite.epoch();
            self.last.epoch_moved(now)
        }
    }

    /// The simulator's model.
    struct Sim {
        sim: SimSqlite,
        src: SimSource,
        pages: SimPages,
        cp: SimCheckpointer,
        readers: [Option<ReaderId>; OTHERS],
        fill: u8,
        last: Last,
    }

    impl Sim {
        fn new(seed: u64, known_out: bool) -> Self {
            let sim = SimSqlite::new(page_size(), PAGES, seed);
            let all: Vec<_> = (1..=PAGES)
                .map(|n| (PageNo::new(n, page_size()).unwrap(), vec![0; 512]))
                .collect();
            sim.commit(&all, PAGES).unwrap();
            sim.other_process_checkpoint(OtherProcessMode::Truncate)
                .unwrap();
            assert!(sim.our_reader_begin());
            Self {
                src: sim.source(),
                pages: sim.pages(),
                cp: sim.checkpointer(),
                sim,
                readers: [None; OTHERS],
                fill: 0,
                last: Last {
                    known_out,
                    ..Last::default()
                },
            }
        }

        fn apply(&mut self, op: Op) -> String {
            match op {
                Op::Commit(mask) => {
                    self.fill = self.fill.wrapping_add(1);
                    let pages: Vec<_> = tables(mask)
                        .map(|t| {
                            (
                                PageNo::new(t.checked_add(2).unwrap(), page_size()).unwrap(),
                                vec![self.fill; 512],
                            )
                        })
                        .collect();
                    show(self.sim.commit(&pages, self.sim.db_size()).is_some())
                }
                Op::OtherBegin(at) if self.readers[at].is_none() => {
                    self.readers[at] = Some(self.sim.other_process_reader_begin());
                    String::new()
                }
                Op::OtherEnd(at) => {
                    if let Some(id) = self.readers[at].take() {
                        assert!(self.sim.other_process_reader_end(id));
                    }
                    String::new()
                }
                Op::OtherBegin(_) => String::new(),
                Op::AppCheckpoint(mode) => {
                    let n = self.sim.other_process_checkpoint(mode).unwrap();
                    show((n.busy, n.log, n.checkpointed))
                }
                Op::Committed
                | Op::Len
                | Op::Open
                | Op::PageCount
                | Op::ReadPage(_)
                | Op::Sync
                | Op::Checkpoint(_)
                | Op::BumpSeq
                | Op::BeginBarrier
                | Op::EndBarrier
                | Op::BeginWriteLock
                | Op::EndWriteLock => engine(
                    &mut self.src,
                    &mut self.pages,
                    &mut self.cp,
                    &mut self.last,
                    op,
                ),
            }
        }

        fn epoch_moved(&mut self) -> bool {
            let now = self.src.epoch();
            self.last.epoch_moved(now)
        }
    }

    fn op() -> impl Strategy<Value = Op> {
        let mode = prop_oneof![
            Just(CheckpointMode::Passive),
            Just(CheckpointMode::Truncate)
        ];
        let other = prop_oneof![
            Just(OtherProcessMode::Passive),
            Just(OtherProcessMode::Restart),
            Just(OtherProcessMode::Truncate)
        ];
        prop_oneof![
            6 => (1_u8..16).prop_map(Op::Commit),
            2 => (0..OTHERS).prop_map(Op::OtherBegin),
            2 => (0..OTHERS).prop_map(Op::OtherEnd),
            2 => other.prop_map(Op::AppCheckpoint),
            3 => Just(Op::Committed),
            1 => Just(Op::Len),
            1 => Just(Op::Open),
            1 => Just(Op::PageCount),
            1 => (1_u32..10).prop_map(Op::ReadPage),
            1 => Just(Op::Sync),
            3 => mode.prop_map(Op::Checkpoint),
            2 => Just(Op::BumpSeq),
            1 => Just(Op::BeginBarrier),
            1 => Just(Op::EndBarrier),
            1 => Just(Op::BeginWriteLock),
            1 => Just(Op::EndWriteLock),
        ]
    }

    /// Each op's answer from the model and from SQLite, and whether each side's hold moved.
    fn answers(ops: &[Op], seed: u64, known_out: bool) -> Vec<((String, bool), (String, bool))> {
        let mut real = Real::new(known_out);
        let mut sim = Sim::new(seed, known_out);
        ops.iter()
            .map(|op| {
                let (got, want) = (sim.apply(*op), real.apply(*op));
                ((got, sim.epoch_moved()), (want, real.epoch_moved()))
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Every answer the model gives the engine is the answer this crate gives it, but for
        /// the three known differences the pins below hold.
        #[test]
        fn the_model_answers_the_engine_as_sqlite_does(
            seed in any::<u64>(),
            ops in prop::collection::vec(op(), 1..40),
        ) {
            for (step, (got, want)) in answers(&ops, seed, true).into_iter().enumerate() {
                prop_assert_eq!(got, want, "model (left), SQLite (right), step {}: {:?}", step, ops[step]);
            }
        }
    }

    /// The answer at `step` of `ops`, from the model and from SQLite, with nothing left out.
    fn pinned(ops: &[Op], step: usize) -> (String, String) {
        let ((got, _), (want, _)) = answers(ops, 0, false).swap_remove(step);
        (got, want)
    }

    /// Known difference: a second engine lock while one is held. The engine takes one at a
    /// time. Fails once the model refuses as SQLite does; then remove the pin and the carve-out.
    #[test]
    fn pin_a_second_lock_is_busy_in_the_model_and_refused_by_sqlite() {
        let (model, sqlite) = pinned(&[Op::BeginBarrier, Op::BeginWriteLock], 1);
        assert_eq!(
            (model.as_str(), sqlite.as_str()),
            ("Err(Busy)", "Err(Io(InvalidInput))")
        );
    }

    /// Known difference: a sequence bump while a lock is held runs inside the lock's own
    /// transaction here (one writer connection), so the lock's release rolls it back; the
    /// model's pool connection is busy instead. The engine releases first, so only a release
    /// whose ROLLBACK failed leaves the transaction open for a bump to run in.
    #[test]
    fn pin_a_bump_under_a_lock_is_busy_in_the_model_and_runs_inside_it_in_sqlite() {
        let (model, sqlite) = pinned(&[Op::BeginBarrier, Op::BumpSeq], 1);
        assert_eq!((model.as_str(), sqlite.as_str()), ("Err(Busy)", "Ok(())"));
    }

    /// Known difference: after the engine's own TRUNCATE, its sequence bump writes the first
    /// frame. This crate checkpoints on its reader and bumps on its writer, whose checkpoint
    /// count is 0, so SQLite draws both salts at random; the model runs both on one connection
    /// and moves salt1 by one, the restart `recover`'s rule 6 can prove.
    #[test]
    fn pin_salts_after_our_truncate_move_by_one_in_the_model_and_at_random_in_sqlite() {
        let ops = [
            Op::Committed,
            Op::Commit(1),
            Op::Checkpoint(CheckpointMode::Truncate),
            Op::BumpSeq,
            Op::Committed,
        ];
        let (model, sqlite) = pinned(&ops, 4);
        assert_eq!(model, "Ok((1, 0, Some((PlusOne, Other))))");
        assert_eq!(sqlite, "Ok((1, 0, Some((Other, Other))))");
    }
}

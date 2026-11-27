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
//! The schema puts tables `t0`..`t3` on pages 2..=5 and this crate's `_litestream_seq` and
//! `_litestream_lock` on 6 and 7, so an app commit of tables in `mask` writes one frame per
//! table on both sides.

#[cfg(test)]
mod diff {
    use std::fmt::Debug;
    use std::io::Read;
    use std::time::Duration;

    use lsr_core::{Barrier, CheckpointMode, Checkpointer, PageSource, ReadLockHold, WalSource};
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
    }

    /// How a salt moved since the side's previous committed read.
    #[derive(Debug, PartialEq, Eq)]
    enum Moved {
        Same,
        PlusOne,
        Other,
    }

    /// One side's memory between steps, to say how salts and the hold moved.
    #[derive(Default)]
    struct Last {
        salts: Option<(u32, u32)>,
        epoch: Option<ReadLockHold>,
        held: Vec<Barrier>,
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
            OtherProcessMode::Full => "FULL",
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
            Op::BumpSeq => show(cp.bump_seq()),
            Op::BeginBarrier => {
                let got = cp.begin_barrier();
                let out = show(got.as_ref().map(drop));
                last.held.extend(got.ok());
                out
            }
            Op::EndBarrier => end_held(cp, last),
            Op::Commit(_) | Op::OtherBegin(_) | Op::OtherEnd(_) | Op::AppCheckpoint(_) => {
                String::new()
            }
        }
    }

    /// Ends the newest barrier held, as the engine would; nothing held is a no-op on both sides.
    fn end_held<C: Checkpointer>(cp: &mut C, last: &mut Last) -> String {
        match last.held.pop() {
            None => "nothing held".to_owned(),
            Some(barrier) => show(cp.end_barrier(barrier)),
        }
    }

    struct Real {
        _dir: TempDir,
        path: std::path::PathBuf,
        app: Connection,
        others: Vec<Connection>,
        reading: [bool; OTHERS],
        sqlite: Sqlite,
        last: Last,
    }

    impl Real {
        fn new() -> Self {
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
            pragma(&open(&path), OtherProcessMode::Truncate);
            let sqlite = Sqlite::open(&path).unwrap();
            Self {
                others: (0..OTHERS).map(|_| open(&path)).collect(),
                path,
                _dir: dir,
                app,
                reading: [false; OTHERS],
                sqlite,
                last: Last::default(),
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
                Op::AppCheckpoint(mode) => pragma(&open(&self.path), mode),
                Op::Committed
                | Op::Len
                | Op::Open
                | Op::PageCount
                | Op::ReadPage(_)
                | Op::Sync
                | Op::Checkpoint(_)
                | Op::BumpSeq
                | Op::BeginBarrier
                | Op::EndBarrier => {
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
        fn new(seed: u64) -> Self {
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
                last: Last::default(),
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
                | Op::EndBarrier => engine(
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
            Just(CheckpointMode::Truncate),
            Just(CheckpointMode::Full)
        ];
        let other = prop_oneof![
            Just(OtherProcessMode::Passive),
            Just(OtherProcessMode::Restart),
            Just(OtherProcessMode::Truncate),
            Just(OtherProcessMode::Full)
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
        ]
    }

    /// Each op's answer from the model and from SQLite, and whether each side's hold moved.
    fn answers(ops: &[Op], seed: u64) -> Vec<((String, bool), (String, bool))> {
        let mut real = Real::new();
        let mut sim = Sim::new(seed);
        ops.iter()
            .map(|op| {
                let (got, want) = (sim.apply(*op), real.apply(*op));
                ((got, sim.epoch_moved()), (want, real.epoch_moved()))
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Every answer the model gives the engine is the answer this crate gives it.
        #[test]
        fn the_model_answers_the_engine_as_sqlite_does(
            seed in any::<u64>(),
            ops in prop::collection::vec(op(), 1..40),
        ) {
            for (step, (got, want)) in answers(&ops, seed).into_iter().enumerate() {
                prop_assert_eq!(got, want, "model (left), SQLite (right), step {}: {:?}", step, ops[step]);
            }
        }
    }

    /// Runs `ops` on both sides, holds every answer of the model to SQLite's, and returns
    /// SQLite's answers.
    fn agreed(ops: &[Op]) -> Vec<String> {
        answers(ops, 1)
            .into_iter()
            .zip(ops)
            .map(|((got, want), op)| {
                assert_eq!(got, want, "model (left), SQLite (right): {op:?}");
                want.0
            })
            .collect()
    }

    /// The cure after a failed WAL sync (issue #197) counts on this: a FULL checkpoint with no
    /// reader in the way copies every committed frame and says so (`log == checkpointed`), and
    /// leaves the WAL as it was, same length and generation, so the frames it copied can still
    /// be named. The next commit then starts a new generation.
    #[test]
    fn a_full_checkpoint_copies_every_frame_and_leaves_the_wal() {
        use CheckpointMode::Full;
        let answers = agreed(&[
            Op::Commit(0b0011),
            Op::Commit(0b0100),
            Op::Committed,
            Op::Len,
            Op::Checkpoint(Full),
            Op::Len,
            Op::Committed,
            Op::Commit(0b1000),
            Op::Committed,
        ]);
        let [_, _, _, before, run, after, committed, _, next] = answers.as_slice() else {
            panic!("{answers:?}");
        };
        assert_eq!(run, "Ok((false, 3, 3))");
        assert_eq!(before, after, "the WAL keeps its length");
        assert_eq!(
            committed, "Ok((3, 3, (Same, Same)))",
            "same generation, all copied"
        );
        assert_eq!(
            next, "Ok((1, 0, (PlusOne, Other)))",
            "the next commit restarts"
        );
    }

    /// Behind a reader on an older snapshot, FULL copies the frames that snapshot holds and
    /// reports busy with `checkpointed < log`: the cure must not take it for a full copy. Once
    /// the reader ends, FULL copies the rest.
    #[test]
    fn a_full_checkpoint_behind_an_older_snapshot_is_busy() {
        use CheckpointMode::Full;
        let answers = agreed(&[
            Op::Commit(0b0001),
            Op::OtherBegin(0),
            Op::Commit(0b0010),
            Op::Checkpoint(Full),
            Op::OtherEnd(0),
            Op::Checkpoint(Full),
        ]);
        let [_, _, _, held, _, freed] = answers.as_slice() else {
            panic!("{answers:?}");
        };
        assert_eq!(held, "Ok((true, 2, 1))");
        assert_eq!(freed, "Ok((false, 2, 2))");
    }

    /// A reader on the newest snapshot is in no FULL checkpoint's way, but holds a TRUNCATE
    /// back: FULL never restarts the WAL, so it waits only for readers it would overtake.
    #[test]
    fn a_reader_on_the_newest_snapshot_holds_back_truncate_but_not_full() {
        let answers = agreed(&[
            Op::Commit(0b0001),
            Op::OtherBegin(0),
            Op::Checkpoint(CheckpointMode::Full),
            Op::Checkpoint(CheckpointMode::Truncate),
        ]);
        let [_, _, full, truncate] = answers.as_slice() else {
            panic!("{answers:?}");
        };
        assert_eq!(full, "Ok((false, 1, 1))");
        assert_eq!(truncate, "Ok((true, 1, 1))");
    }
}

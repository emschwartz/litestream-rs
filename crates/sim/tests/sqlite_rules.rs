//! `SimSqlite` against SQLite itself.
//!
//! The same history runs on real files in a tempdir, through one rusqlite
//! connection per SQLite connection the model has (the app, our reader, our
//! pool, three strangers; busy timeout 0, no autocheckpoint,
//! `synchronous=NORMAL`), and on the model. After every step the WAL file's
//! length, its header's checkpoint sequence, how each salt moved since the
//! side's previous header, and a checkpoint's three numbers must agree.
//!
//! The schema puts tables `t0`..`t3` on pages 2..=5 and `seq` on page 6, one
//! fixed-size row each, so an app commit of tables in `mask` writes one frame per
//! table, as the model's commit of those pages does.

// `cfg(test)` marks the helpers as test code, so clippy.toml's allow-*-in-tests
// cover them as they cover #[test] fns.
#[cfg(test)]
mod rules {
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    use lsr_core::{CheckpointMode, Checkpointer};
    use lsr_ltx::{PageNo, PageSize};
    use lsr_sim::{ReaderId, SimSqlite, StrangerMode};
    use lsr_wal::{HEADER_LEN, WalHeader};
    use proptest::prelude::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    const PAGE_SIZE: u32 = 512;

    /// A checkpoint's busy flag, log frames and checkpointed frames.
    type Numbers = (bool, u32, u32);
    const TABLES: u32 = 4;
    const STRANGERS: usize = 3;

    #[derive(Clone, Copy, Debug)]
    enum Op {
        Commit(u8),
        StrangerBegin(usize),
        StrangerEnd(usize),
        OursBegin,
        OursEnd,
        AppCheckpoint(StrangerMode),
        OurCheckpoint(CheckpointMode),
        BumpSeq,
    }

    /// How a salt moved since the side's previous header.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Moved {
        Same,
        PlusOne,
        Other,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Header {
        seq: u32,
        salt1: Moved,
        salt2: Moved,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct View {
        len: usize,
        header: Option<Header>,
        numbers: Option<(bool, u32, u32)>,
    }

    /// One side's previous header, to say how the salts moved.
    #[derive(Default)]
    struct Last(Option<(u32, u32)>);

    impl Last {
        fn view(&mut self, wal: &[u8], numbers: Option<(bool, u32, u32)>) -> View {
            let header = wal
                .first_chunk::<HEADER_LEN>()
                .and_then(|head| WalHeader::decode(head).ok())
                .map(|head| {
                    let salts = head.salts();
                    let (salt1, salt2) = match self.0 {
                        None => (Moved::Other, Moved::Other),
                        Some((one, two)) => (
                            match salts.salt1.wrapping_sub(one) {
                                0 => Moved::Same,
                                1 => Moved::PlusOne,
                                _ => Moved::Other,
                            },
                            if salts.salt2 == two {
                                Moved::Same
                            } else {
                                Moved::Other
                            },
                        ),
                    };
                    self.0 = Some((salts.salt1, salts.salt2));
                    Header {
                        seq: head.checkpoint_seq(),
                        salt1,
                        salt2,
                    }
                });
            View {
                len: wal.len(),
                header,
                numbers,
            }
        }
    }

    fn open(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.busy_timeout(Duration::ZERO).unwrap();
        conn.execute_batch("PRAGMA wal_autocheckpoint = 0; PRAGMA synchronous = NORMAL;")
            .unwrap();
        conn
    }

    fn begin_read(conn: &Connection) {
        conn.execute_batch("BEGIN").unwrap();
        let _: i64 = conn
            .query_row("SELECT count(*) FROM t0", [], |row| row.get(0))
            .unwrap();
    }

    fn pragma(conn: &Connection, mode: StrangerMode) -> (bool, u32, u32) {
        let name = match mode {
            StrangerMode::Passive => "PASSIVE",
            StrangerMode::Restart => "RESTART",
            StrangerMode::Truncate => "TRUNCATE",
        };
        conn.query_row(&format!("PRAGMA wal_checkpoint({name})"), [], |row| {
            Ok((
                row.get::<_, i64>(0)? != 0,
                row.get::<_, u32>(1)?,
                row.get::<_, u32>(2)?,
            ))
        })
        .unwrap()
    }

    fn page_size() -> PageSize {
        PageSize::new(PAGE_SIZE).unwrap()
    }

    fn pages(mask: u8, fill: u8) -> Vec<(PageNo, Vec<u8>)> {
        (0..TABLES)
            .filter(|table| mask & (1 << table) != 0)
            .map(|table| {
                let pgno = PageNo::new(table.saturating_add(2), page_size()).unwrap();
                (pgno, vec![fill; 512])
            })
            .collect()
    }

    /// The two sides of one history.
    struct Pair {
        _dir: TempDir,
        wal_path: PathBuf,
        app: Connection,
        ours: Connection,
        pool: Connection,
        strangers: Vec<Connection>,
        real_in: [bool; STRANGERS],
        real_ours: bool,
        real_last: Last,
        sim: SimSqlite,
        sim_in: [Option<ReaderId>; STRANGERS],
        sim_last: Last,
        fill: u8,
    }

    impl Pair {
        fn new(seed: u64) -> Self {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("test.db");
            let app = open(&path);
            app.execute_batch(&format!(
                "PRAGMA page_size = {PAGE_SIZE}; PRAGMA journal_mode = WAL;
             BEGIN;
             CREATE TABLE t0 (v BLOB); CREATE TABLE t1 (v BLOB);
             CREATE TABLE t2 (v BLOB); CREATE TABLE t3 (v BLOB); CREATE TABLE seq (v BLOB);
             INSERT INTO t0 VALUES (randomblob(8)); INSERT INTO t1 VALUES (randomblob(8));
             INSERT INTO t2 VALUES (randomblob(8)); INSERT INTO t3 VALUES (randomblob(8));
             INSERT INTO seq VALUES (randomblob(8));
             COMMIT;"
            ))
            .unwrap();
            let sim = SimSqlite::new(page_size(), 6, seed);
            let all: Vec<_> = (1..=6)
                .map(|n| (PageNo::new(n, page_size()).unwrap(), vec![0; 512]))
                .collect();
            sim.commit(&all, 6).unwrap();
            let wal_path = dir.path().join("test.db-wal");
            let mut pair = Self {
                ours: open(&path),
                pool: open(&path),
                strangers: (0..STRANGERS).map(|_| open(&path)).collect(),
                _dir: dir,
                wal_path,
                app,
                real_in: [false; STRANGERS],
                real_ours: false,
                real_last: Last::default(),
                sim,
                sim_in: [None; STRANGERS],
                sim_last: Last::default(),
                fill: 0,
            };
            pair.compare(None, None);
            pair.apply(Op::AppCheckpoint(StrangerMode::Truncate));
            pair
        }

        fn compare(
            &mut self,
            real: Option<(bool, u32, u32)>,
            sim: Option<(bool, u32, u32)>,
        ) -> View {
            let real_wal = fs::read(&self.wal_path).unwrap_or_default();
            let sim_wal = self.sim.wal_file().unwrap_or_default();
            let real = self.real_last.view(&real_wal, real);
            let sim = self.sim_last.view(&sim_wal, sim);
            assert_eq!(sim, real, "model (left) and SQLite (right) disagree");
            real
        }

        /// Runs `op` on both sides; returns SQLite's view after it.
        fn apply(&mut self, op: Op) -> View {
            let (real, sim) = match op {
                Op::Commit(mask) => {
                    self.commit(mask);
                    (None, None)
                }
                Op::StrangerBegin(at) => {
                    if !self.real_in[at] {
                        begin_read(&self.strangers[at]);
                        self.real_in[at] = true;
                        self.sim_in[at] = Some(self.sim.stranger_reader_begin());
                    }
                    (None, None)
                }
                Op::StrangerEnd(at) => {
                    if self.real_in[at] {
                        self.strangers[at].execute_batch("COMMIT").unwrap();
                        self.real_in[at] = false;
                        assert!(
                            self.sim
                                .stranger_reader_end(self.sim_in[at].take().unwrap())
                        );
                    }
                    (None, None)
                }
                Op::OursBegin => {
                    if !self.real_ours {
                        begin_read(&self.ours);
                        self.real_ours = true;
                        assert!(self.sim.our_reader_begin());
                    }
                    (None, None)
                }
                Op::OursEnd => {
                    if self.real_ours {
                        self.ours.execute_batch("COMMIT").unwrap();
                        self.real_ours = false;
                        assert!(self.sim.our_reader_end());
                    }
                    (None, None)
                }
                Op::AppCheckpoint(mode) => {
                    let sim = self.sim.stranger_checkpoint(mode).unwrap();
                    (
                        Some(pragma(&self.app, mode)),
                        Some((sim.busy, sim.log, sim.checkpointed)),
                    )
                }
                Op::OurCheckpoint(mode) => self.our_checkpoint(mode),
                Op::BumpSeq => {
                    self.pool
                        .execute_batch("UPDATE seq SET v = randomblob(8)")
                        .unwrap();
                    self.sim.checkpointer().bump_seq().unwrap();
                    (None, None)
                }
            };
            self.compare(real, sim)
        }

        fn commit(&mut self, mask: u8) {
            let mut sql = String::from("BEGIN;");
            for table in (0..TABLES).filter(|table| mask & (1 << table) != 0) {
                sql.push_str(&format!("UPDATE t{table} SET v = randomblob(8);"));
            }
            sql.push_str("COMMIT;");
            self.app.execute_batch(&sql).unwrap();
            self.fill = self.fill.wrapping_add(1);
            let size = self.sim.db_size();
            self.sim.commit(&pages(mask, self.fill), size).unwrap();
        }

        fn our_checkpoint(&mut self, mode: CheckpointMode) -> (Option<Numbers>, Option<Numbers>) {
            if self.real_ours {
                self.ours.execute_batch("COMMIT").unwrap();
            }
            let real = pragma(&self.pool, mode.into());
            begin_read(&self.ours);
            self.real_ours = true;
            let run = self.sim.checkpointer().checkpoint(mode).unwrap();
            (Some(real), Some((run.busy, run.log, run.checkpointed)))
        }

        fn commit_moved(&mut self, mask: u8) -> Moved {
            self.apply(Op::Commit(mask)).header.unwrap().salt1
        }

        fn checkpoint(&mut self, mode: StrangerMode) -> (bool, u32, u32) {
            self.apply(Op::AppCheckpoint(mode)).numbers.unwrap()
        }
    }

    fn op() -> impl Strategy<Value = Op> {
        let reader = 0..STRANGERS;
        prop_oneof![
            4 => (1_u8..16).prop_map(Op::Commit),
            2 => reader.clone().prop_map(Op::StrangerBegin),
            2 => reader.prop_map(Op::StrangerEnd),
            1 => Just(Op::OursBegin),
            1 => Just(Op::OursEnd),
            1 => Just(Op::AppCheckpoint(StrangerMode::Passive)),
            1 => Just(Op::AppCheckpoint(StrangerMode::Restart)),
            1 => Just(Op::AppCheckpoint(StrangerMode::Truncate)),
            1 => Just(Op::OurCheckpoint(CheckpointMode::Passive)),
            1 => Just(Op::OurCheckpoint(CheckpointMode::Truncate)),
            1 => Just(Op::BumpSeq),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn rules_match_sqlite(seed in any::<u64>(), ops in prop::collection::vec(op(), 1..48)) {
            let mut pair = Pair::new(seed);
            for op in ops {
                pair.apply(op);
            }
        }
    }

    #[test]
    fn pinned_above_zero_blocks_all_modes() {
        let mut pair = Pair::new(1);
        pair.apply(Op::Commit(0b0001));
        pair.apply(Op::StrangerBegin(0));
        pair.apply(Op::Commit(0b0010));
        let (busy, log, checkpointed) = pair.checkpoint(StrangerMode::Passive);
        assert!(
            !busy && checkpointed == 1 && log == 2,
            "partial progress, not busy"
        );
        assert!(pair.checkpoint(StrangerMode::Restart).0);
        assert!(pair.checkpoint(StrangerMode::Truncate).0);
        assert_eq!(
            pair.commit_moved(0b0100),
            Moved::Same,
            "no restart under the pin"
        );
    }

    #[test]
    fn pinned_on_zero_restarts_once() {
        let mut pair = Pair::new(2);
        pair.apply(Op::Commit(0b0001));
        let (_, log, checkpointed) = pair.checkpoint(StrangerMode::Passive);
        assert_eq!(checkpointed, log);
        pair.apply(Op::StrangerBegin(0));
        assert_eq!(pair.commit_moved(0b0010), Moved::PlusOne, "the one restart");
        assert_eq!(pair.commit_moved(0b0100), Moved::Same);
        let (busy, log, checkpointed) = pair.checkpoint(StrangerMode::Passive);
        assert!(
            !busy && checkpointed == 0 && log == 2,
            "nothing backfilled past the pin"
        );
        assert_eq!(pair.commit_moved(0b1000), Moved::Same, "no second restart");
    }

    #[test]
    fn two_readers_grow_old_generation() {
        let mut pair = Pair::new(3);
        pair.apply(Op::Commit(0b0001));
        pair.apply(Op::StrangerBegin(1));
        let (_, log, checkpointed) = pair.checkpoint(StrangerMode::Passive);
        assert_eq!(checkpointed, log);
        pair.apply(Op::StrangerBegin(0));
        assert_eq!(
            pair.commit_moved(0b0010),
            Moved::Same,
            "the mark above 0 blocks the restart"
        );
        pair.apply(Op::StrangerEnd(1));
        assert_eq!(
            pair.commit_moved(0b0100),
            Moved::Same,
            "the old generation grows"
        );
        let (_, log, checkpointed) = pair.checkpoint(StrangerMode::Passive);
        assert!(
            checkpointed == 1 && log == 3,
            "the mark-0 reader blocks backfill"
        );
        pair.apply(Op::StrangerEnd(0));
        let (_, log, checkpointed) = pair.checkpoint(StrangerMode::Passive);
        assert_eq!(checkpointed, log);
        assert_eq!(
            pair.commit_moved(0b1000),
            Moved::PlusOne,
            "restart once both are gone"
        );
    }
}

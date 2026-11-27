//! `SimSqlite` against SQLite itself.
//!
//! The same history runs on real files in a tempdir, through one rusqlite
//! connection per SQLite connection the model has (the app, our reader, which runs our
//! checkpoints, our writer, which bumps the sequence, three other processes; busy timeout 0, no autocheckpoint,
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
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::Duration;

    use lsr_core::{CheckpointMode, Checkpointer};
    use lsr_ltx::{PageNo, PageSize};
    use lsr_sim::{OtherProcessMode, ReaderId, SimSqlite};
    use lsr_wal::{HEADER_LEN, ScanLimit, TxOutcome, WalHeader, WalReader};
    use proptest::prelude::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    const PAGE_SIZE: u32 = 512;

    type Numbers = (bool, u32, u32);
    const TABLES: u32 = 4;
    const OTHER_PROCESSES: usize = 3;

    #[derive(Clone, Copy, Debug)]
    enum Op {
        Commit(u8),
        OtherProcessBegin(usize),
        OtherProcessEnd(usize),
        OursBegin,
        OursEnd,
        AppCheckpoint(OtherProcessMode),
        OurCheckpoint(CheckpointMode),
        BumpSeq,
    }

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

    fn pragma(conn: &Connection, mode: OtherProcessMode) -> (bool, u32, u32) {
        let name = match mode {
            OtherProcessMode::Passive => "PASSIVE",
            OtherProcessMode::Restart => "RESTART",
            OtherProcessMode::Truncate => "TRUNCATE",
            OtherProcessMode::Full => "FULL",
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

    struct Pair {
        _dir: TempDir,
        wal_path: PathBuf,
        db_path: PathBuf,
        app: Connection,
        ours: Connection,
        writer: Connection,
        other_processes: Vec<Connection>,
        real_in: [bool; OTHER_PROCESSES],
        real_ours: bool,
        real_last: Last,
        sim: SimSqlite,
        sim_in: [Option<ReaderId>; OTHER_PROCESSES],
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
                writer: open(&path),
                other_processes: (0..OTHER_PROCESSES).map(|_| open(&path)).collect(),
                _dir: dir,
                wal_path,
                db_path: path.clone(),
                app,
                real_in: [false; OTHER_PROCESSES],
                real_ours: false,
                real_last: Last::default(),
                sim,
                sim_in: [None; OTHER_PROCESSES],
                sim_last: Last::default(),
                fill: 0,
            };
            pair.compare(None, None);
            pair.apply(Op::AppCheckpoint(OtherProcessMode::Truncate));
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

        fn apply(&mut self, op: Op) -> View {
            let (real, sim) = match op {
                Op::Commit(mask) => {
                    self.commit(mask);
                    (None, None)
                }
                Op::OtherProcessBegin(at) => {
                    if !self.real_in[at] {
                        begin_read(&self.other_processes[at]);
                        self.real_in[at] = true;
                        self.sim_in[at] = Some(self.sim.other_process_reader_begin());
                    }
                    (None, None)
                }
                Op::OtherProcessEnd(at) => {
                    if self.real_in[at] {
                        self.other_processes[at].execute_batch("COMMIT").unwrap();
                        self.real_in[at] = false;
                        assert!(
                            self.sim
                                .other_process_reader_end(self.sim_in[at].take().unwrap())
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
                    let sim = self.sim.other_process_checkpoint(mode).unwrap();
                    (
                        Some(pragma(&open(&self.db_path), mode)),
                        Some((sim.busy, sim.log, sim.checkpointed)),
                    )
                }
                Op::OurCheckpoint(mode) => self.our_checkpoint(mode),
                Op::BumpSeq => {
                    self.writer
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
            if mode == CheckpointMode::Truncate {
                let real = pragma(&self.writer, mode.into());
                let run = self.sim.checkpointer().checkpoint(mode).unwrap();
                return (Some(real), Some((run.busy, run.log, run.checkpointed)));
            }
            if self.real_ours {
                self.ours.execute_batch("COMMIT").unwrap();
            }
            let real = pragma(&self.ours, mode.into());
            begin_read(&self.ours);
            self.real_ours = true;
            let run = self.sim.checkpointer().checkpoint(mode).unwrap();
            (Some(real), Some((run.busy, run.log, run.checkpointed)))
        }

        fn commit_moved(&mut self, mask: u8) -> Moved {
            self.apply(Op::Commit(mask)).header.unwrap().salt1
        }

        fn checkpoint(&mut self, mode: OtherProcessMode) -> (bool, u32, u32) {
            self.apply(Op::AppCheckpoint(mode)).numbers.unwrap()
        }
    }

    fn op() -> impl Strategy<Value = Op> {
        let reader = 0..OTHER_PROCESSES;
        prop_oneof![
            4 => (1_u8..16).prop_map(Op::Commit),
            2 => reader.clone().prop_map(Op::OtherProcessBegin),
            2 => reader.prop_map(Op::OtherProcessEnd),
            1 => Just(Op::OursBegin),
            1 => Just(Op::OursEnd),
            1 => Just(Op::AppCheckpoint(OtherProcessMode::Passive)),
            1 => Just(Op::AppCheckpoint(OtherProcessMode::Restart)),
            1 => Just(Op::AppCheckpoint(OtherProcessMode::Truncate)),
            1 => Just(Op::AppCheckpoint(OtherProcessMode::Full)),
            1 => Just(Op::OurCheckpoint(CheckpointMode::Passive)),
            1 => Just(Op::OurCheckpoint(CheckpointMode::Truncate)),
            1 => Just(Op::OurCheckpoint(CheckpointMode::Full)),
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

    /// A page-size change needs the database out of WAL mode, and SQLite
    /// leaves WAL mode only when no other connection holds it: while the
    /// engine's read transaction is open the change fails, in SQLite and in
    /// the model. With the engine's connection closed both allow it.
    #[test]
    fn our_reader_blocks_leaving_wal_mode() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let app = open(&path);
        app.execute_batch(&format!(
            "PRAGMA page_size = {PAGE_SIZE}; PRAGMA journal_mode = WAL;
             CREATE TABLE t0 (v BLOB); INSERT INTO t0 VALUES (randomblob(8));"
        ))
        .unwrap();
        let ours = open(&path);
        begin_read(&ours);
        let leave = |conn: &Connection| {
            conn.query_row("PRAGMA journal_mode = DELETE", [], |row| {
                row.get::<_, String>(0)
            })
        };
        let held = leave(&app);
        assert!(
            held.as_ref()
                .is_err_and(|err| err.to_string().contains("locked")),
            "{held:?}"
        );
        drop(ours);
        assert_eq!(leave(&app).unwrap(), "delete");

        let bigger = PageSize::new(1024).unwrap();
        let sim = SimSqlite::new(page_size(), 6, 1);
        assert!(sim.our_reader_begin());
        assert!(!sim.rewrite_page_size(bigger));
        assert!(sim.our_reader_end());
        assert!(sim.rewrite_page_size(bigger));
    }

    fn transactions(wal: &[u8]) -> Vec<(Vec<u32>, u32)> {
        let mut reader = WalReader::open(Cursor::new(wal)).unwrap();
        let mut cursor = reader.cursor_at_start();
        let mut out = Vec::new();
        while let TxOutcome::Tx(tx) = reader
            .next_tx(&cursor, ScanLimit::FirstInvalidFrame)
            .unwrap()
        {
            out.push((
                tx.frames().iter().map(|f| f.pgno().get()).collect(),
                tx.commit().get(),
            ));
            cursor = *tx.end();
        }
        out
    }

    fn writes_added(pages: &[u32], old: u32, new: u32) -> bool {
        (old.saturating_add(1)..=new).all(|pgno| pages.contains(&pgno))
    }

    /// What `sqlite::tests::power_crash_keeps_checkpointed_pages` builds its history on. A
    /// transaction that grew the database, spilled and shrank again before it committed leaves
    /// its spilled frames above its commit's size in the WAL, in SQLite and in the model. A
    /// commit that grows the database writes every page it adds, so such an old frame is never
    /// the newest of a page a later growth brings back; a growth that wrote only its last page
    /// would leave the page reading a frame no checkpoint copies.
    #[test]
    fn a_growing_commit_writes_every_page_it_adds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let app = open(&path);
        app.execute_batch(&format!(
            "PRAGMA page_size = {PAGE_SIZE}; PRAGMA auto_vacuum = FULL; PRAGMA journal_mode = WAL;
             CREATE TABLE a (v BLOB); INSERT INTO a VALUES (randomblob(8));"
        ))
        .unwrap();
        pragma(&app, OtherProcessMode::Truncate);
        app.execute_batch(
            "PRAGMA cache_size = 1;
             BEGIN; CREATE TABLE b (v BLOB); INSERT INTO b VALUES (randomblob(2500));
             DROP TABLE b; UPDATE a SET v = randomblob(8); COMMIT;
             PRAGMA cache_size = -2000;
             BEGIN; CREATE TABLE c (v BLOB); INSERT INTO c VALUES (randomblob(1200)); COMMIT;",
        )
        .unwrap();
        let txs = transactions(&fs::read(dir.path().join("test.db-wal")).unwrap());
        let [(shrunk, small), (grown, big)] = txs.as_slice() else {
            panic!("two transactions: {txs:?}");
        };
        assert!(
            shrunk.iter().any(|pgno| pgno > small),
            "spilled frames stay: {txs:?}"
        );
        assert!(big > small && writes_added(grown, *small, *big), "{txs:?}");

        let sim = SimSqlite::new(page_size(), 4, 1);
        let seven = PageNo::new(7, page_size()).unwrap();
        assert!(sim.spill(&[(seven, vec![0xee; 512])]));
        sim.commit(&pages(0b0001, 1), 4).unwrap();
        let txs = transactions(&sim.wal_file().unwrap());
        assert_eq!(
            txs,
            [(vec![7, 2], 4)],
            "the model keeps the spilled frame too"
        );
    }

    /// Pages a growing transaction frees again before it commits are written all the same:
    /// SQLite's hint not to write a freed page does not reach the WAL.
    #[test]
    fn a_growing_commit_writes_the_pages_it_freed() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let app = open(&path);
        app.execute_batch(&format!(
            "PRAGMA page_size = {PAGE_SIZE}; PRAGMA journal_mode = WAL;
             CREATE TABLE a (v BLOB); INSERT INTO a VALUES (randomblob(8));"
        ))
        .unwrap();
        pragma(&app, OtherProcessMode::Truncate);
        let old: u32 = app
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        app.execute_batch(
            "BEGIN; CREATE TABLE b (v BLOB); INSERT INTO b VALUES (randomblob(3000));
             DELETE FROM b; COMMIT;",
        )
        .unwrap();
        let txs = transactions(&fs::read(dir.path().join("test.db-wal")).unwrap());
        let [(grown, new)] = txs.as_slice() else {
            panic!("one transaction: {txs:?}");
        };
        assert!(*new > old && writes_added(grown, old, *new), "{txs:?}");
    }

    #[test]
    fn pinned_above_zero_blocks_all_modes() {
        let mut pair = Pair::new(1);
        pair.apply(Op::Commit(0b0001));
        pair.apply(Op::OtherProcessBegin(0));
        pair.apply(Op::Commit(0b0010));
        let (busy, log, checkpointed) = pair.checkpoint(OtherProcessMode::Passive);
        assert!(
            !busy && checkpointed == 1 && log == 2,
            "partial progress, not busy"
        );
        assert!(pair.checkpoint(OtherProcessMode::Restart).0);
        assert!(pair.checkpoint(OtherProcessMode::Truncate).0);
        assert_eq!(
            pair.commit_moved(0b0100),
            Moved::Same,
            "no restart under the read lock"
        );
    }

    #[test]
    fn pinned_on_zero_restarts_once() {
        let mut pair = Pair::new(2);
        pair.apply(Op::Commit(0b0001));
        let (_, log, checkpointed) = pair.checkpoint(OtherProcessMode::Passive);
        assert_eq!(checkpointed, log);
        pair.apply(Op::OtherProcessBegin(0));
        assert_eq!(pair.commit_moved(0b0010), Moved::PlusOne, "the one restart");
        assert_eq!(pair.commit_moved(0b0100), Moved::Same);
        let (busy, log, checkpointed) = pair.checkpoint(OtherProcessMode::Passive);
        assert!(
            !busy && checkpointed == 0 && log == 2,
            "nothing backfilled past the read lock"
        );
        assert_eq!(pair.commit_moved(0b1000), Moved::Same, "no second restart");
    }

    #[test]
    fn two_readers_grow_old_generation() {
        let mut pair = Pair::new(3);
        pair.apply(Op::Commit(0b0001));
        pair.apply(Op::OtherProcessBegin(1));
        let (_, log, checkpointed) = pair.checkpoint(OtherProcessMode::Passive);
        assert_eq!(checkpointed, log);
        pair.apply(Op::OtherProcessBegin(0));
        assert_eq!(
            pair.commit_moved(0b0010),
            Moved::Same,
            "the mark above 0 blocks the restart"
        );
        pair.apply(Op::OtherProcessEnd(1));
        assert_eq!(
            pair.commit_moved(0b0100),
            Moved::Same,
            "the old generation grows"
        );
        let (_, log, checkpointed) = pair.checkpoint(OtherProcessMode::Passive);
        assert!(
            checkpointed == 1 && log == 3,
            "the read-mark-0 reader blocks backfill"
        );
        pair.apply(Op::OtherProcessEnd(0));
        let (_, log, checkpointed) = pair.checkpoint(OtherProcessMode::Passive);
        assert_eq!(checkpointed, log);
        assert_eq!(
            pair.commit_moved(0b1000),
            Moved::PlusOne,
            "restart once both are gone"
        );
    }
}

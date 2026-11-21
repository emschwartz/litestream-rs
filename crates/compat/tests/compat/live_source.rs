//! The live SQLite source against real SQLite: what the sync engine does when an application
//! checkpoints behind its back, and whether a chain written through [`LiveSqlite`] restores.
//! A later `lsr-sqlite` must pass these unchanged.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_compat::{hash_pages, integrity_check};
use lsr_core::conformance::block_on;
use lsr_core::{
    CheckpointMode, Checkpointer, CommittedRead, FileInfo, MemSink, MemStore, PageSource, Position,
    PositionSource, Probes, RecoverEvent, Recovered, RestoreTarget, SnapshotCause, SourceError,
    Step, StepEnd, SyncConfig, Synced, SyncedThrough, Verdict, WalSource, WalView, checkpoint,
    recover_traced, restore, sync_step,
};
use lsr_ltx::{PageNo, PageSize, Timestamp, Txid};
use lsr_sqlite::Sqlite as LiveSqlite;
use lsr_wal::{Salts, WalReader};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
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

/// A tiny deterministic generator, so a chain replays from its seed.
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33).checked_rem(bound).unwrap_or(0)
    }
}

/// What a sync did, without the proof it carries.
#[derive(Debug)]
enum Did {
    Idle,
    Wrote { file: FileInfo, txs: u32 },
}

/// An application connection and the engine's own, on one database.
struct Rig {
    dir: TempDir,
    db: PathBuf,
    app: Connection,
    live: LiveSqlite,
    staging: MemStore,
    cfg: SyncConfig,
    through: Option<SyncedThrough>,
    position: Option<Position>,
    now: i64,
}

impl Rig {
    fn new(page_size: u32) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("app.db");
        let app = Connection::open(&db).expect("open");
        app.busy_timeout(Duration::ZERO).expect("busy timeout");
        app.pragma_update(None, "page_size", page_size)
            .expect("page size");
        app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .expect("wal");
        app.pragma_update(None, "wal_autocheckpoint", 0)
            .expect("autocheckpoint");
        app.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .expect("schema");
        let live = LiveSqlite::open(&db).expect("live");
        let staging = MemStore::new();
        let rig = Self {
            dir,
            db,
            app,
            live,
            staging,
            cfg: SyncConfig::default(),
            through: None,
            position: None,
            now: 1_000,
        };
        rig.write(100);
        rig
    }

    fn write(&self, bytes: u32) {
        self.app
            .execute("INSERT INTO t(v) VALUES (randomblob(?1))", [bytes])
            .expect("insert");
    }

    fn wal_path(&self) -> PathBuf {
        self.live.wal_path()
    }

    /// The WAL header's 32 bytes: salts, checkpoint sequence and checksum.
    fn wal_header(&self) -> [u8; 32] {
        let bytes = fs::read(self.wal_path()).expect("wal");
        bytes[..32].try_into().expect("32 bytes")
    }

    fn wal_len(&self) -> u64 {
        fs::metadata(self.wal_path()).expect("stat").len()
    }

    fn salts(&self) -> Salts {
        let header = self.wal_header();
        let word =
            |at: usize| u32::from_be_bytes(header[at..at.saturating_add(4)].try_into().expect("4"));
        Salts {
            salt1: word(16),
            salt2: word(20),
        }
    }

    fn txid(&self) -> Txid {
        self.position.expect("synced").txid()
    }

    /// The WAL as one read of its header and one read of the wal-index see it.
    fn view(&mut self) -> (WalReader<fs::File>, WalView, CommittedRead) {
        let file = self.live.open().expect("open").expect("wal exists");
        let wal = WalReader::open(file).expect("wal header");
        let committed = self.live.committed().expect("committed");
        let view = WalView {
            header: Some(*wal.header()),
            len: self.live.len().expect("len"),
            committed: Some(committed),
            epoch: committed.epoch(),
        };
        (wal, view, committed)
    }

    /// What the engine would decide now from the last proven position.
    fn verdict(&mut self) -> Verdict {
        let (mut wal, view, _) = self.view();
        match self.position.as_ref() {
            None => recover_traced(None, &view, None),
            Some(position) => {
                let probes = Probes::read(&mut wal, position).expect("probes");
                recover_traced(Some(PositionSource::Running(position, probes)), &view, None)
            }
        }
    }

    fn run(&mut self, step: Step) -> Did {
        let (mut wal, _, committed) = self.view();
        self.now = self.now.saturating_add(10);
        let mut db = self.live.clone();
        let synced = block_on(sync_step(
            step,
            &mut wal,
            committed,
            &mut db,
            &mut lsr_core::PageTable::new(),
            &self.staging,
            &self.cfg,
            Timestamp::from_unix_millis(self.now),
        ))
        .expect("step");
        match synced {
            Synced::Idle(through) => {
                (self.position, self.through) = confirmed(through, &self.live);
                Did::Idle
            }
            Synced::Wrote {
                file,
                end: StepEnd::Through(through),
                txs,
                ..
            } => {
                (self.position, self.through) = confirmed(through, &self.live);
                Did::Wrote { file, txs }
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

    /// Recovers, then runs whatever step the decision calls for.
    fn sync(&mut self) -> Did {
        let step = Step::try_from(self.verdict().recovered).expect("no halt");
        self.run(step)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> lsr_core::CheckpointOutcome {
        let proof = self.through.take().expect("synced");
        self.now = self.now.saturating_add(10);
        let (mut wal, mut db, mut ck) = (self.live.clone(), self.live.clone(), self.live.clone());
        let done = block_on(checkpoint(
            proof,
            mode,
            &mut wal,
            &mut db,
            &mut lsr_core::PageTable::new(),
            &mut ck,
            &self.staging,
            &self.cfg,
            Timestamp::from_unix_millis(self.now),
        ))
        .expect("checkpoint");
        self.position = Some(done.staged.position());
        self.through = done.staged.into_proof();
        done.outcome
    }

    fn app_checkpoint(&self, pragma: &str) -> (i64, i64, i64) {
        self.app
            .query_row(pragma, [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("checkpoint")
    }
}

fn assert_continue(verdict: &Verdict) {
    assert_eq!(verdict.event, None, "no ReadLockViolated or LocalRewound");
    assert!(
        matches!(verdict.recovered, Recovered::Continue(_)),
        "expected Continue, got {:?}",
        verdict.recovered
    );
}

/// An application checkpoint of a kind that restarts or truncates the WAL, run while our read
/// transaction holds a mark above zero: SQLite cannot restart under it, so nothing changes.
fn outside_checkpoint_against_read_lock(pragma: &str) {
    let mut rig = Rig::new(4096);
    assert!(matches!(rig.sync(), Did::Wrote { .. }), "the snapshot");
    rig.write(500);
    assert!(matches!(rig.sync(), Did::Wrote { .. }));
    let (header, len) = (rig.wal_header(), rig.wal_len());
    let position = rig.txid();

    let (busy, _, _) = rig.app_checkpoint(pragma);
    assert_eq!(busy, 1, "the read lock blocks the restart");
    assert_eq!(rig.wal_header(), header, "salts and sequence unchanged");
    assert_eq!(rig.wal_len(), len, "the file is not truncated");

    rig.write(300);
    assert_eq!(
        rig.wal_header(),
        header,
        "the next commit appends to the same generation"
    );
    assert_continue(&rig.verdict());
    let Did::Wrote { file, txs } = rig.sync() else {
        panic!("the commit after the checkpoint is staged");
    };
    assert_eq!(txs, 1);
    assert_eq!(
        Some(file.key.range().min()),
        position.next(),
        "incremental, no snapshot"
    );
}

#[test]
fn outside_restart_against_read_lock_above_zero() {
    outside_checkpoint_against_read_lock("PRAGMA wal_checkpoint(RESTART)");
}

#[test]
fn outside_truncate_against_read_lock_above_zero() {
    outside_checkpoint_against_read_lock("PRAGMA wal_checkpoint(TRUNCATE)");
}

/// Our read transaction is retaken on read mark 0 (the WAL fully backfilled); one application write
/// then restarts the WAL, which SQLite does once and not again until a checkpoint.
fn restart_under_read_mark_zero(repin: bool) -> Rig {
    let mut rig = Rig::new(4096);
    rig.sync();
    rig.write(500);
    rig.sync();
    let before = rig.salts();
    let run = rig
        .live
        .checkpoint(CheckpointMode::Passive)
        .expect("passive");
    assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
    assert_eq!(run.epoch.count(), 2, "the read transaction was retaken");
    assert!(
        rig.live.committed().expect("committed").fully_backfilled(),
        "read mark 0"
    );
    if repin {
        let Did::Idle = rig.sync() else {
            panic!("nothing new")
        };
    }
    rig.write(200);
    let after = rig.salts();
    assert_eq!(after.salt1, before.salt1.wrapping_add(1), "one restart");
    rig
}

#[test]
fn read_lock_on_zero_one_restart() {
    let mut rig = restart_under_read_mark_zero(false);
    let verdict = rig.verdict();
    assert_eq!(verdict.event, None, "no ReadLockViolated");
    assert!(
        matches!(
            verdict.recovered,
            Recovered::Snapshot {
                cause: SnapshotCause::GenerationChanged,
                ..
            }
        ),
        "the position was pinned to the epoch before the retake: {:?}",
        verdict.recovered
    );
    let Did::Wrote { .. } = rig.sync() else {
        panic!("snapshot staged")
    };
    let salts = rig.salts();
    rig.write(200);
    assert_eq!(rig.salts(), salts, "a second write does not restart again");
    assert_continue(&rig.verdict());
    assert!(matches!(rig.sync(), Did::Wrote { txs: 1, .. }));
}

#[test]
fn read_lock_on_zero_proven_restart_continues_in_the_new_generation() {
    let mut rig = restart_under_read_mark_zero(true);
    let verdict = rig.verdict();
    assert_eq!(verdict.event, None::<RecoverEvent>, "no ReadLockViolated");
    assert!(
        matches!(verdict.recovered, Recovered::WalRestarted(_)),
        "{:?}",
        verdict.recovered
    );
    let Did::Wrote { txs, .. } = rig.sync() else {
        panic!("new generation staged")
    };
    assert_eq!(txs, 1);
    let salts = rig.salts();
    rig.write(200);
    assert_eq!(rig.salts(), salts, "a second write does not restart again");
    assert_continue(&rig.verdict());
    assert!(matches!(rig.sync(), Did::Wrote { txs: 1, .. }));
}

fn hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A copy of the database as of now: the main file and WAL copied together, checkpointed away
/// from the original, hashed.
fn hash_of_copy(rig: &Rig, scratch: &Path) -> Vec<String> {
    for suffix in ["", "-wal", "-shm"] {
        drop(fs::remove_file(scratch.join(format!("copy.db{suffix}"))));
    }
    let copy = scratch.join("copy.db");
    fs::copy(&rig.db, &copy).expect("copy db");
    fs::copy(rig.wal_path(), scratch.join("copy.db-wal")).expect("copy wal");
    let conn = Connection::open(&copy).expect("open copy");
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
        .expect("checkpoint copy");
    drop(conn);
    hash_pages(&copy).expect("hash").page_hashes
}

#[test]
fn live_sync_chain_restores() {
    let mut rig = Rig::new(4096);
    let scratch = tempfile::tempdir().expect("scratch");
    let mut rng = Rng(0x5EED);
    let mut expected: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    rig.sync();
    for commit in 0..200_u32 {
        match rng.below(10) {
            0 => {
                rig.app
                    .execute("DELETE FROM t WHERE id % 3 = 0", [])
                    .expect("delete");
            }
            1 | 2 => {
                rig.app
                    .execute(
                        "UPDATE t SET v = randomblob(?1) WHERE id = (SELECT max(id) FROM t) - ?2",
                        [rng.below(3000).cast_signed(), rng.below(4).cast_signed()],
                    )
                    .expect("update");
            }
            _ => rig.write(u32::try_from(rng.below(3000)).expect("small")),
        }
        if rng.below(3) != 0 {
            rig.sync();
            if commit % 50 == 49 {
                rig.checkpoint(CheckpointMode::Truncate);
                rig.sync();
            } else if commit % 15 == 14 {
                rig.checkpoint(CheckpointMode::Passive);
                rig.sync();
            }
            expected.insert(rig.txid().get(), hash_of_copy(&rig, scratch.path()));
        }
    }
    rig.sync();
    expected.insert(rig.txid().get(), hash_of_copy(&rig, scratch.path()));
    assert!(
        expected.len() > 100,
        "the chain has many restore points: {}",
        expected.len()
    );

    for (txid, hashes) in &expected {
        let target = RestoreTarget::Txid(Txid::new(*txid).expect("txid"));
        let sink = MemSink::new();
        block_on(restore(&rig.staging, target, sink.clone()))
            .unwrap_or_else(|err| panic!("restore at {txid}: {err:?}"));
        let restored = sink.published().expect("published");
        let got: Vec<String> = (1..=restored.page_count())
            .map(|pgno| hex(&restored.page(pgno).expect("page")))
            .collect();
        assert_eq!(&got, hashes, "txid {txid}");
    }
    let sink = MemSink::new();
    block_on(restore(&rig.staging, RestoreTarget::Latest, sink.clone())).expect("restore latest");
    let restored = sink.published().expect("published");
    let latest = rig.dir.path().join("latest.db");
    let bytes: Vec<u8> = (1..=restored.page_count())
        .flat_map(|pgno| restored.page(pgno).expect("page"))
        .collect();
    fs::write(&latest, bytes).expect("write restored");
    integrity_check(&latest).expect("restored database is sound");
    integrity_check(&rig.db).expect("live database is sound");
}

#[test]
fn committed_read_matches_the_wal_file() {
    let mut rig = Rig::new(4096);
    rig.write(2000);
    let read = rig.live.committed().expect("committed");
    assert_eq!(read.end().salts(), rig.salts());
    let frame = 24 + 4096;
    assert_eq!(
        u64::from(read.end().frame_count()),
        (rig.wal_len() - 32) / frame,
        "every frame in the file is committed"
    );
    assert_eq!(read.backfilled(), 0);
}

#[test]
fn page_reads_come_from_the_database_file() {
    let mut rig = Rig::new(4096);
    rig.live
        .checkpoint(CheckpointMode::Passive)
        .expect("passive");
    let pages = rig.live.page_count().expect("count").get();
    assert!(pages >= 2, "{pages}");
    let size = PageSize::new(4096).expect("size");
    let mut buf = vec![0; 4096];
    rig.live
        .read_page(PageNo::new(1, size).expect("page"), &mut buf)
        .expect("page 1");
    assert_eq!(&buf[..16], b"SQLite format 3\0");
    let beyond = PageNo::new(pages + 1, size).expect("page");
    assert_eq!(
        rig.live.read_page(beyond, &mut buf),
        Err(SourceError::Short { pgno: beyond })
    );
}

#[test]
fn the_barrier_waits_for_no_one() {
    let mut rig = Rig::new(4096);
    rig.app
        .execute_batch("BEGIN IMMEDIATE")
        .expect("app writes");
    assert_eq!(rig.live.begin_barrier().err(), Some(SourceError::Busy));
    assert_eq!(rig.live.begin_write_lock().err(), Some(SourceError::Busy));
    rig.app.execute_batch("ROLLBACK").expect("release");
    let barrier = rig.live.begin_barrier().expect("free now");
    assert_eq!(
        rig.live.begin_write_lock().err().map(|_| ()),
        Some(()),
        "one holder at a time"
    );
    rig.live.end_barrier(barrier).expect("end");
    let lock = rig.live.begin_write_lock().expect("free again");
    rig.live.end_write_lock(lock).expect("end");
}

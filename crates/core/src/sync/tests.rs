use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::num::{NonZeroU32, NonZeroU64};

use lsr_ltx::{
    Event, Header, PageNo, PageSize, ReadDecoder, Timestamp, Txid, TxidRange, WalOrigin, lock_page,
};
use lsr_wal::{CommittedEnd, PageMap, Salts, ScanLimit, TxOutcome, WalReader};
use proptest::prelude::*;
use rusqlite::Connection;
use tempfile::TempDir;

use super::*;
use crate::conformance::block_on;
use crate::mem::MemStore;
use crate::source::{FakeDb, FakeWal, SourceError};
use crate::store::{ObjectStore, PageLimit, ReadLen, StoreError, StoreOp};

const PS: PageSize = PageSize::MIN;

fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn ts(n: i64) -> Timestamp {
    Timestamp::from_unix_millis(n)
}

fn reader(bytes: Vec<u8>) -> WalReader<Cursor<Vec<u8>>> {
    WalReader::open(Cursor::new(bytes)).unwrap()
}

fn commit(wal: &FakeWal, pages: &[u32], size: u32) {
    let pages: Vec<(PageNo, Vec<u8>)> = pages
        .iter()
        .map(|n| (pg(*n), vec![u8::try_from(*n).unwrap(); PS.bytes()]))
        .collect();
    wal.append_tx(&pages, nz(size));
}

/// A database page the WAL does not hold: its fill byte says it came from the file.
fn db_fill(n: u32) -> u8 {
    0xD0_u8.saturating_add(u8::try_from(n).unwrap())
}

type Staged = (FileInfo, Header, Vec<(PageNo, Vec<u8>)>);

struct Rig {
    wal: FakeWal,
    db: FakeDb,
    store: MemStore,
}

impl Rig {
    fn new() -> Self {
        Self {
            wal: FakeWal::new(
                PS,
                Salts {
                    salt1: 10,
                    salt2: 20,
                },
            ),
            db: FakeDb::new(PS),
            store: MemStore::new(),
        }
    }

    fn step(&mut self, step: Step, cfg: &SyncConfig, now: i64) -> Result<Synced, StepFailed> {
        let mut wal = reader(self.wal.file_bytes());
        let committed =
            CommittedRead::new(self.wal.committed_end(), self.wal.backfilled()).unwrap();
        block_on(sync_step(
            step,
            &mut wal,
            committed,
            self.wal.epoch(),
            &mut self.db,
            &self.store,
            cfg,
            ts(now),
        ))
    }

    fn snapshot(&mut self, now: i64) -> Synced {
        let step = Step::Snapshot {
            after: None,
            prev_timestamp: None,
            cause: SnapshotCause::FirstSync,
        };
        self.step(step, &SyncConfig::default(), now).unwrap()
    }

    fn files(&self) -> Vec<Staged> {
        staged(&self.store)
            .into_iter()
            .map(|(info, bytes)| {
                let (header, pages) = decode(&bytes);
                (info, header, pages)
            })
            .collect()
    }
}

fn staged(store: &MemStore) -> Vec<(FileInfo, Vec<u8>)> {
    block_on(async {
        let listing = store.list(Level::ZERO, None, PageLimit::MAX).await.unwrap();
        let mut out = Vec::new();
        for info in listing.files {
            let len = ReadLen::new(ReadLen::MAX_RAW).unwrap();
            let bytes = store.read(&info.key, 0, len).await.unwrap();
            out.push((info, bytes));
        }
        out
    })
}

fn decode(bytes: &[u8]) -> (Header, Vec<(PageNo, Vec<u8>)>) {
    let mut decoder = ReadDecoder::new(bytes);
    let mut header = None;
    let mut pages = Vec::new();
    while let Some(event) = decoder.next_event().unwrap() {
        match event {
            Event::Header(h) => header = Some(h),
            Event::Page { pgno, data } => pages.push((pgno, data.to_vec())),
        }
    }
    decoder.finish().unwrap();
    (header.unwrap(), pages)
}

fn position_of(synced: Synced) -> Position {
    match synced {
        Synced::Idle(through) => through.into_position(),
        Synced::Wrote {
            end: StepEnd::Through(through),
            ..
        } => through.into_position(),
        Synced::Wrote {
            end: StepEnd::Limited(position),
            ..
        } => position,
    }
}

fn fills(pages: &[(PageNo, Vec<u8>)]) -> Vec<(u32, u8)> {
    pages
        .iter()
        .map(|(pgno, data)| (pgno.get(), data[0]))
        .collect()
}

// Page sources.
#[test]
fn a_snapshot_takes_the_wal_pages_and_fills_the_rest_from_the_database() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[2], 3);
    rig.db.write_page(pg(1), &[db_fill(1); 512]);
    rig.db.write_page(pg(3), &[db_fill(3); 512]);
    let synced = rig.snapshot(1000);
    assert!(matches!(
        synced,
        Synced::Wrote {
            end: StepEnd::Through(_),
            clamped: None,
            ..
        }
    ));
    let files = rig.files();
    assert_eq!(files.len(), 1);
    let (_, header, pages) = &files[0];
    assert!(header.is_snapshot());
    assert_eq!(header.fields().commit, 3);
    assert_eq!(fills(pages), [(1, db_fill(1)), (2, 2), (3, db_fill(3))]);
}

#[test]
fn walgrowth_pages_come_from_the_database_file() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1, 2], 2);
    let first = position_of(rig.snapshot(1000));
    commit(&rig.wal, &[5], 5);
    rig.db.write_page(pg(3), &[db_fill(3); 512]);
    rig.db.write_page(pg(4), &[db_fill(4); 512]);
    rig.db.write_page(pg(5), &[db_fill(5); 512]);
    rig.step(Step::Incremental(first), &SyncConfig::default(), 2000)
        .unwrap();
    let files = rig.files();
    let (_, header, pages) = &files[1];
    assert_eq!(header.fields().commit, 5);
    assert!(!header.is_snapshot());
    assert_eq!(fills(pages), [(3, db_fill(3)), (4, db_fill(4)), (5, 5)]);
}

#[test]
fn a_snapshot_of_an_empty_wal_takes_its_size_from_the_database() {
    let mut rig = Rig::new();
    rig.db.write_page(pg(1), &[db_fill(1); 512]);
    rig.db.write_page(pg(2), &[db_fill(2); 512]);
    let synced = rig.snapshot(1000);
    let Synced::Wrote { end, .. } = synced else {
        panic!("{synced:?}");
    };
    let StepEnd::Through(through) = end else {
        panic!("{end:?}");
    };
    assert_eq!(through.position().synced().offset().get(), 32);
    let files = rig.files();
    let (_, header, pages) = &files[0];
    assert_eq!(header.fields().commit, 2);
    assert_eq!(fills(pages), [(1, db_fill(1)), (2, db_fill(2))]);
    let origin = header.fields().wal.unwrap();
    assert_eq!((origin.offset(), origin.size()), (32, 0));
}

#[test]
fn an_empty_wal_and_an_empty_database_have_nothing_to_snapshot() {
    let mut rig = Rig::new();
    let step = Step::Snapshot {
        after: None,
        prev_timestamp: None,
        cause: SnapshotCause::FirstSync,
    };
    let failed = rig.step(step, &SyncConfig::default(), 1).unwrap_err();
    assert!(matches!(failed.fault, Fault::NothingToSnapshot));
    assert_eq!(failed.retry, step);
}

#[test]
fn the_lock_page_is_never_planned_even_when_the_database_crosses_it() {
    let lock = lock_page(PS);
    let commit = nz(lock + 5);
    let plan = plan_pages(&PageMap::new(PS), None, commit, PS);
    assert_eq!(plan.len(), usize::try_from(lock + 4).unwrap());
    assert!(plan.iter().all(|page| page.pgno().get() != lock));
    assert!(
        plan.windows(2)
            .all(|w| matches!(w, [a, b] if a.pgno() < b.pgno()))
    );
    assert_eq!(plan.last().unwrap().pgno().get(), lock + 5);
}

#[test]
fn an_idle_step_returns_the_committed_end_pinned() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1], 1);
    let position = position_of(rig.snapshot(1000));
    let synced = rig
        .step(Step::Incremental(position), &SyncConfig::default(), 2000)
        .unwrap();
    let Synced::Idle(through) = synced else {
        panic!("{synced:?}");
    };
    assert_eq!(
        through.position().continuity(),
        Continuity::Pinned(rig.wal.epoch())
    );
    assert_eq!(rig.files().len(), 1);
}

#[test]
fn a_new_generation_step_scans_from_the_start_and_keeps_the_txid() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1, 2], 2);
    let first = position_of(rig.snapshot(1000));
    rig.wal.force_restart();
    commit(&rig.wal, &[2], 2);
    let synced = rig
        .step(Step::NewGeneration(first), &SyncConfig::default(), 2000)
        .unwrap();
    assert!(matches!(synced, Synced::Wrote { .. }));
    let files = rig.files();
    let (info, header, pages) = &files[1];
    assert_eq!(info.key.range(), TxidRange::single(Txid::new(2).unwrap()));
    assert_eq!(header.fields().wal.unwrap().offset(), 32);
    assert_eq!(
        header.fields().wal.unwrap().salts().0,
        rig.wal.salts().salt1
    );
    assert_eq!(fills(pages), [(2, 2)]);
}

// Caps.
fn capped(cap: u64) -> SyncConfig {
    SyncConfig {
        cap: NonZeroU64::new(cap),
        node_id: 0,
    }
}

const FRAME: u64 = 24 + 512;

/// Stages one transaction, then runs capped steps over four more until the end.
fn run_capped(cap: u64, txs: &[(&[u32], u32)]) -> (Rig, Vec<Position>) {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1], 1);
    let mut position = position_of(rig.snapshot(1000));
    for (pages, size) in txs {
        commit(&rig.wal, pages, *size);
    }
    let mut ends = Vec::new();
    for now in 2000.. {
        let synced = rig
            .step(Step::Incremental(position), &capped(cap), now)
            .unwrap();
        match synced {
            Synced::Wrote {
                end: StepEnd::Limited(next),
                ..
            } => {
                ends.push(next);
                position = next;
            }
            Synced::Wrote {
                end: StepEnd::Through(through),
                ..
            } => {
                ends.push(*through.position());
                break;
            }
            Synced::Idle(_) => break,
        }
    }
    (rig, ends)
}

fn sizes(rig: &Rig) -> Vec<u64> {
    rig.files()
        .iter()
        .skip(1)
        .map(|(_, header, _)| header.fields().wal.unwrap().size())
        .collect()
}

#[test]
fn a_cap_stops_on_the_first_commit_that_reaches_it() {
    let txs: [(&[u32], u32); 4] = [(&[2], 2), (&[3, 4, 5], 5), (&[1], 5), (&[2, 3], 5)];
    let (rig, ends) = run_capped(600, &txs);
    assert_eq!(sizes(&rig), [4 * FRAME, 3 * FRAME]);
    let frames: Vec<u32> = ends
        .iter()
        .map(|p| p.cursor().next().frame_index())
        .collect();
    assert_eq!(frames, [5, 8]);
    assert_eq!(ends[0].continuity(), Continuity::Unpinned);
    assert_eq!(ends[1].continuity(), Continuity::Pinned(rig.wal.epoch()));
}

#[test]
fn one_oversized_transaction_makes_one_file() {
    let (rig, ends) = run_capped(1, &[(&[2, 3, 4], 4)]);
    assert_eq!(sizes(&rig), [3 * FRAME]);
    assert_eq!(ends.len(), 1);
}

#[test]
fn a_cap_of_one_byte_writes_a_file_per_transaction() {
    let txs: [(&[u32], u32); 4] = [(&[2], 2), (&[3, 4, 5], 5), (&[1], 5), (&[2, 3], 5)];
    let (rig, ends) = run_capped(1, &txs);
    assert_eq!(sizes(&rig), [FRAME, 3 * FRAME, FRAME, 2 * FRAME]);
    let committed = rig.wal.committed_end().offset().get();
    for end in &ends[..3] {
        assert_eq!(end.continuity(), Continuity::Unpinned);
        assert!(end.cursor().next().get() < committed);
    }
}

// Faults.
#[test]
fn a_page_that_changes_between_scan_and_read_is_a_retry_and_stages_nothing() {
    /// Flips a byte of the first frame's page the second time it is read.
    struct Flip {
        inner: Cursor<Vec<u8>>,
        hits: u32,
    }
    impl Read for Flip {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let start = self.inner.position();
            let n = self.inner.read(buf)?;
            let end = start.saturating_add(u64::try_from(n).map_err(io::Error::other)?);
            if start <= 100 && 100 < end {
                self.hits = self.hits.saturating_add(1);
                let at =
                    usize::try_from(100_u64.saturating_sub(start)).map_err(io::Error::other)?;
                if self.hits >= 2
                    && let Some(byte) = buf.get_mut(at)
                {
                    *byte ^= 0xff;
                }
            }
            Ok(n)
        }
    }
    impl Seek for Flip {
        fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
            self.inner.seek(to)
        }
    }
    let mut rig = Rig::new();
    commit(&rig.wal, &[1], 1);
    let mut wal = WalReader::open(Flip {
        inner: Cursor::new(rig.wal.file_bytes()),
        hits: 0,
    })
    .unwrap();
    let step = Step::Snapshot {
        after: None,
        prev_timestamp: None,
        cause: SnapshotCause::FirstSync,
    };
    let committed = CommittedRead::new(rig.wal.committed_end(), 0).unwrap();
    let failed = block_on(sync_step(
        step,
        &mut wal,
        committed,
        rig.wal.epoch(),
        &mut rig.db,
        &rig.store,
        &SyncConfig::default(),
        ts(1),
    ))
    .unwrap_err();
    assert!(matches!(failed.fault, Fault::WalChangedUnderRead));
    assert_eq!(failed.retry, step);
    assert!(rig.files().is_empty());
}

#[test]
fn a_staging_commit_error_returns_the_same_step_and_the_retry_overwrites_the_file() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1], 1);
    let step = Step::Snapshot {
        after: None,
        prev_timestamp: None,
        cause: SnapshotCause::FirstSync,
    };
    rig.store
        .inject_failure_after_publish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: io::ErrorKind::Other,
        });
    let failed = rig.step(step, &SyncConfig::default(), 1).unwrap_err();
    assert!(matches!(failed.fault, Fault::LocalIo));
    assert_eq!(failed.retry, step);
    rig.step(failed.retry, &SyncConfig::default(), 2).unwrap();
    assert_eq!(rig.files().len(), 1);
}

#[test]
fn a_position_pinned_to_another_epoch_is_read_tx_lost() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1], 1);
    let position = position_of(rig.snapshot(1000));
    rig.wal.set_epoch(rig.wal.epoch().next());
    let failed = rig
        .step(Step::Incremental(position), &SyncConfig::default(), 2000)
        .unwrap_err();
    assert!(matches!(failed.fault, Fault::ReadTxLost));
}

#[test]
fn a_corrupt_committed_frame_halts() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1, 2], 2);
    rig.wal.corrupt_byte(100);
    let step = Step::Snapshot {
        after: None,
        prev_timestamp: None,
        cause: SnapshotCause::FirstSync,
    };
    let failed = rig.step(step, &SyncConfig::default(), 1).unwrap_err();
    assert!(matches!(failed.fault, Fault::WalCorruptBelowCommitted));
    assert!(rig.files().is_empty());
}

#[test]
fn a_clock_stepping_backwards_gets_an_equal_timestamp_and_one_clamped() {
    let mut rig = Rig::new();
    commit(&rig.wal, &[1], 1);
    let position = position_of(rig.snapshot(1000));
    commit(&rig.wal, &[2], 2);
    let synced = rig
        .step(Step::Incremental(position), &SyncConfig::default(), 500)
        .unwrap();
    let Synced::Wrote { clamped, .. } = &synced else {
        panic!("{synced:?}");
    };
    assert_eq!(
        *clamped,
        Some(Clamped {
            prev: ts(1000),
            now: ts(500)
        })
    );
    assert_eq!(rig.files()[1].1.fields().timestamp, ts(1000));
}

// The pure helpers.
fn txs() -> impl Strategy<Value = Vec<(Vec<u32>, u32)>> {
    prop::collection::vec((prop::collection::vec(1_u32..8, 1..4), 1_u32..10), 1..5)
}

fn map_of(txs: &[(Vec<u32>, u32)]) -> PageMap {
    let wal = FakeWal::new(PS, Salts { salt1: 1, salt2: 2 });
    for (pages, size) in txs {
        let top = pages.iter().copied().max().unwrap_or(1);
        commit(&wal, pages, (*size).max(top));
    }
    let mut r = reader(wal.file_bytes());
    let mut at = r.cursor_at_start();
    let mut map = PageMap::new(PS);
    while let TxOutcome::Tx(tx) = r.next_tx(&at, ScanLimit::FirstInvalidFrame).unwrap() {
        map.apply(&tx).unwrap();
        at = *tx.end();
    }
    map
}

proptest! {
    #[test]
    fn plan_pages_lists_what_the_file_must_hold(
        txs in txs(),
        prev in prop::option::of(1_u32..12),
    ) {
        let map = map_of(&txs);
        let commit = nz(map.commit());
        let plan = plan_pages(&map, prev, commit, PS);
        let numbers: Vec<u32> = plan.iter().map(|p| p.pgno().get()).collect();
        prop_assert!(numbers.windows(2).all(|w| matches!(w, [a, b] if a < b)));
        prop_assert!(numbers.iter().all(|n| *n <= commit.get()));
        let in_map: BTreeSet<u32> = map.pages().map(|f| f.pgno().get()).collect();
        let wanted: BTreeSet<u32> = match prev {
            None => (1..=commit.get()).collect(),
            Some(prev) => in_map
                .iter()
                .copied()
                .chain(prev.saturating_add(1)..=commit.get())
                .collect(),
        };
        prop_assert_eq!(numbers.iter().copied().collect::<BTreeSet<_>>(), wanted);
        for page in &plan {
            let held = in_map.contains(&page.pgno().get());
            prop_assert_eq!(matches!(page, PageFrom::Wal(_)), held);
        }
    }

    #[test]
    fn header_fields_build_a_valid_untracked_header(
        txid in 1_u64..u64::MAX,
        commit in 1_u32..1_000_000,
        offset in 1_u64..1 << 40,
        size in 0_u64..1 << 40,
        millis in 0_i64..4_000_000_000_000,
        node in any::<u64>(),
        salts in any::<(u32, u32)>(),
    ) {
        let txid = Txid::new(txid).unwrap();
        let origin = WalOrigin::new(offset, size, salts.0, salts.1).unwrap();
        let fields = header_fields(txid, nz(commit), PS, origin, ts(millis), node);
        prop_assert!(!fields.tracked);
        prop_assert_eq!(fields.pre_apply, None);
        prop_assert_eq!(fields.wal, Some(origin));
        prop_assert_eq!(fields.txids, TxidRange::single(txid));
        let header = Header::new(fields).unwrap();
        prop_assert_eq!(Header::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn clamp_never_goes_back_and_reports_exactly_when_it_clamped(
        prev in prop::option::of(0_i64..1000),
        now in 0_i64..1000,
    ) {
        let (stamped, clamped) = clamp(prev.map(ts), ts(now));
        match prev {
            Some(prev) if prev > now => {
                prop_assert_eq!(stamped, ts(prev));
                prop_assert_eq!(clamped, Some(Clamped { prev: ts(prev), now: ts(now) }));
            }
            Some(_) | None => {
                prop_assert_eq!(stamped, ts(now));
                prop_assert_eq!(clamped, None);
            }
        }
    }
}

// The differential test: SQLite's own checkpoint is the oracle.
struct Db {
    dir: TempDir,
    conn: Connection,
}

struct Copy {
    db: Vec<u8>,
    wal: Vec<u8>,
}

impl Db {
    fn new(ps: u32) -> Self {
        let dir = TempDir::new().unwrap();
        let conn = Connection::open(dir.path().join("test.db")).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA page_size = {ps}; PRAGMA auto_vacuum = INCREMENTAL;
             PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0; PRAGMA cache_size = 1;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);"
        ))
        .unwrap();
        Self { dir, conn }
    }

    fn copy(&self) -> Copy {
        Copy {
            db: fs::read(self.dir.path().join("test.db")).unwrap(),
            wal: fs::read(self.dir.path().join("test.db-wal")).unwrap_or_default(),
        }
    }

    fn insert(&self, rows: u32, size: u32) {
        let mut stmt = self
            .conn
            .prepare_cached("INSERT INTO t (v) VALUES (randomblob(?1))")
            .unwrap();
        for _ in 0..rows {
            stmt.execute([size]).unwrap();
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Insert {
        rows: u32,
        size: u32,
    },
    Update {
        modulo: u32,
    },
    DeleteAll,
    Shrink,
    /// Spills uncommitted frames to the WAL, syncs while they are there, then rolls back.
    SpillWhileOpen {
        rows: u32,
        size: u32,
    },
}

/// The main file as a page source; pages past its end read as zeros, as after a checkpoint.
struct BytesDb {
    bytes: Vec<u8>,
    page_size: PageSize,
}

impl PageSource for BytesDb {
    fn page_count(&mut self) -> Result<u32, SourceError> {
        u32::try_from(self.bytes.len().div_ceil(self.page_size.bytes()))
            .map_err(|_err| SourceError::Io(io::ErrorKind::InvalidData))
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        buf.fill(0);
        let at = usize::try_from(pgno.get().saturating_sub(1))
            .map_err(|_err| SourceError::Io(io::ErrorKind::InvalidData))?
            .saturating_mul(self.page_size.bytes());
        if let Some(src) = self.bytes.get(at..) {
            let n = src.len().min(buf.len());
            buf[..n].copy_from_slice(&src[..n]);
        }
        Ok(())
    }
}

/// SQLite's database after it checkpoints the copy.
fn oracle(copy: &Copy) -> Vec<u8> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    fs::write(&path, &copy.db).unwrap();
    fs::write(dir.path().join("test.db-wal"), &copy.wal).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .unwrap();
    drop(conn);
    fs::read(path).unwrap()
}

struct Chain {
    store: MemStore,
    position: Option<Position>,
    clock: i64,
    spilled: bool,
}

/// Runs steps over a copy until the committed end is staged.
fn sync_copy(chain: &mut Chain, copy: &Copy, ps: PageSize, cap: Option<NonZeroU64>) {
    let Ok(mut wal) = WalReader::open(Cursor::new(copy.wal.clone())) else {
        return;
    };
    let mut at = wal.cursor_at_start();
    while let Ok(TxOutcome::Tx(tx)) = wal.next_tx(&at, ScanLimit::FirstInvalidFrame) {
        at = *tx.end();
    }
    let committed_end = CommittedEnd::new(at.next().frame_index(), wal.header().salts(), ps);
    chain.spilled |= u64::try_from(copy.wal.len()).unwrap() > committed_end.offset().get();
    if committed_end.frame_count() == 0 && chain.position.is_none() {
        return;
    }
    let committed = CommittedRead::new(committed_end, 0).unwrap();
    let mut db = BytesDb {
        bytes: copy.db.clone(),
        page_size: ps,
    };
    let cfg = SyncConfig { cap, node_id: 0 };
    loop {
        let step = chain.position.map_or(
            Step::Snapshot {
                after: None,
                prev_timestamp: None,
                cause: SnapshotCause::FirstSync,
            },
            Step::Incremental,
        );
        chain.clock = chain.clock.saturating_add(1);
        let synced = block_on(sync_step(
            step,
            &mut wal,
            committed,
            ReadEpoch::new(1),
            &mut db,
            &chain.store,
            &cfg,
            ts(chain.clock),
        ))
        .unwrap();
        let done = matches!(
            synced,
            Synced::Idle(_)
                | Synced::Wrote {
                    end: StepEnd::Through(_),
                    ..
                }
        );
        chain.position = Some(position_of(synced));
        if done {
            break;
        }
    }
    check_chain(chain, copy, ps, committed_end);
}

/// The staged chain, applied in txid order and cut at each commit, is SQLite's checkpointed file.
fn check_chain(chain: &Chain, copy: &Copy, ps: PageSize, committed_end: CommittedEnd) {
    let mut pages: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    let mut commit = 0;
    let mut covered = 32;
    for (_, bytes) in staged(&chain.store) {
        let (header, file_pages) = decode(&bytes);
        for (pgno, data) in file_pages {
            pages.insert(pgno.get(), data);
        }
        commit = header.fields().commit;
        pages.retain(|pgno, _| *pgno <= commit);
        let origin = header.fields().wal.unwrap();
        covered = origin.offset().saturating_add(origin.size());
    }
    assert_eq!(
        covered,
        committed_end.offset().get(),
        "a file reaches past the committed end, or the chain stops short of it"
    );
    let mut rebuilt: Vec<u8> = Vec::new();
    for n in 1..=commit {
        rebuilt.extend_from_slice(pages.get(&n).unwrap_or_else(|| panic!("page {n} missing")));
    }
    assert_eq!(
        commit,
        u32::try_from(oracle(copy).len().div_euclid(ps.bytes())).unwrap()
    );
    assert!(
        rebuilt == oracle(copy),
        "the staged chain differs from SQLite's checkpoint"
    );
}

fn run_op(db: &Db, op: &Op, chain: &mut Chain, ps: PageSize, cap: Option<NonZeroU64>) {
    match op {
        Op::Insert { rows, size } => {
            db.conn.execute_batch("BEGIN").unwrap();
            db.insert(*rows, *size);
            db.conn.execute_batch("COMMIT").unwrap();
        }
        Op::Update { modulo } => {
            db.conn
                .execute(
                    "UPDATE t SET v = randomblob(length(v)) WHERE id % ?1 = 0",
                    [modulo],
                )
                .unwrap();
        }
        Op::DeleteAll => {
            db.conn.execute("DELETE FROM t", []).unwrap();
        }
        Op::Shrink => db.conn.execute_batch("PRAGMA incremental_vacuum").unwrap(),
        Op::SpillWhileOpen { rows, size } => {
            db.conn.execute_batch("BEGIN").unwrap();
            db.insert(*rows, *size);
            sync_copy(chain, &db.copy(), ps, cap);
            db.conn.execute_batch("ROLLBACK").unwrap();
        }
    }
}

fn workload(ps: u32, cap: Option<u64>, ops: &[Op]) -> bool {
    let size = PageSize::new(ps).unwrap();
    let cap = cap.and_then(NonZeroU64::new);
    let db = Db::new(ps);
    let mut chain = Chain {
        store: MemStore::new(),
        position: None,
        clock: 0,
        spilled: false,
    };
    sync_copy(&mut chain, &db.copy(), size, cap);
    for op in ops {
        run_op(&db, op, &mut chain, size, cap);
        sync_copy(&mut chain, &db.copy(), size, cap);
    }
    chain.spilled
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1_u32..40, 100_u32..2000).prop_map(|(rows, size)| Op::Insert { rows, size }),
        (2_u32..5).prop_map(|modulo| Op::Update { modulo }),
        Just(Op::DeleteAll),
        Just(Op::Shrink),
        (20_u32..70, 800_u32..2000).prop_map(|(rows, size)| Op::SpillWhileOpen { rows, size }),
    ]
}

#[test]
fn every_kind_of_change_stages_what_sqlite_checkpoints_and_a_spill_is_never_staged() {
    let ops = [
        Op::Insert {
            rows: 30,
            size: 900,
        },
        Op::Update { modulo: 3 },
        Op::SpillWhileOpen {
            rows: 60,
            size: 1500,
        },
        Op::Insert {
            rows: 5,
            size: 3000,
        },
        Op::DeleteAll,
        Op::Shrink,
        Op::Insert {
            rows: 10,
            size: 200,
        },
    ];
    for ps in [512, 4096] {
        for cap in [None, Some(1), Some(5000)] {
            assert!(
                workload(ps, cap, &ops),
                "no spill happened at page size {ps}"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn staged_chain_equals_sqlites_checkpoint_at_any_cap(
        ps in prop_oneof![Just(512_u32), Just(4096)],
        cap in prop::option::of(prop_oneof![Just(1_u64), 600_u64..20_000]),
        ops in prop::collection::vec(op(), 1..6),
    ) {
        workload(ps, cap, &ops);
    }
}

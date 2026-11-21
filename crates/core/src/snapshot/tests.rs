use std::collections::BTreeMap;
use std::io::Cursor;
use std::num::NonZeroU32;

use lsr_ltx::{Event, Header, PageNo, PageSize, ReadDecoder, Timestamp, Txid, TxidRange};
use lsr_wal::Salts;
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::fault::SnapshotCause;
use crate::layout::Level;
use crate::mem::MemStore;
use crate::position::Position;
use crate::source::{FakeDb, FakeWal, SourceError, SourceOp};
use crate::store::{PageLimit, ReadLen, StoreError, StoreOp};
use crate::sync::{Step, StepEnd, SyncConfig, Synced, sync_step};

const PS: PageSize = PageSize::MIN;
const GARBAGE: u8 = 0xEE;

fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn ts(n: i64) -> Timestamp {
    Timestamp::from_unix_millis(n)
}

/// What a database page holds when no WAL frame covers it.
fn db_fill(n: u32) -> u8 {
    0xD0_u8.saturating_add(u8::try_from(n).unwrap())
}

/// One committed transaction of the history: the fill byte of each page it writes.
#[derive(Clone, Debug)]
struct Tx {
    fill: u8,
    pages: Vec<u32>,
    size: u32,
}

struct Rig {
    wal: FakeWal,
    db: FakeDb,
    staging: MemStore,
    remote: MemStore,
    position: Option<Position>,
    through: Option<SyncedThrough>,
    base: u32,
    txs: Vec<Tx>,
}

impl Rig {
    /// A database of `base` pages (at least 1) and an empty WAL.
    fn new(base: u32) -> Self {
        let mut db = FakeDb::new(PS);
        for n in 1..=base {
            db.write_page(pg(n), &vec![db_fill(n); PS.bytes()]);
        }
        Self {
            wal: FakeWal::new(
                PS,
                Salts {
                    salt1: 10,
                    salt2: 20,
                },
            ),
            db,
            staging: MemStore::new(),
            remote: MemStore::new(),
            position: None,
            through: None,
            base,
            txs: Vec::new(),
        }
    }

    fn size(&self) -> u32 {
        self.txs.last().map_or(self.base, |tx| tx.size)
    }

    /// Commits a transaction that writes `pages` and every page the file grows by.
    fn commit(&mut self, pages: &[u32]) {
        let prev = self.size();
        let size = pages.iter().copied().fold(prev, u32::max);
        let mut all: Vec<u32> = pages.to_vec();
        all.extend(prev.saturating_add(1)..=size);
        all.sort_unstable();
        all.dedup();
        let fill = u8::try_from(self.txs.len().saturating_add(1)).unwrap();
        let frames: Vec<(PageNo, Vec<u8>)> = all
            .iter()
            .map(|n| (pg(*n), vec![fill; PS.bytes()]))
            .collect();
        self.wal.append_tx(&frames, nz(size));
        self.txs.push(Tx {
            fill,
            pages: all,
            size,
        });
    }

    /// One sync step; the proof it ends with is kept.
    fn sync(&mut self, now: i64) {
        let step = self.position.map_or(
            Step::Snapshot {
                after: None,
                prev_timestamp: None,
                cause: SnapshotCause::FirstSync,
            },
            Step::Incremental,
        );
        let mut wal = WalReader::open(Cursor::new(self.wal.file_bytes())).unwrap();
        let committed = CommittedRead::new(
            self.wal.committed_end(),
            self.wal.backfilled(),
            self.wal.epoch(),
        )
        .unwrap();
        let synced = block_on(sync_step(
            step,
            &mut wal,
            committed,
            &mut self.db,
            &mut PageTable::new(),
            &self.staging,
            &SyncConfig::default(),
            ts(now),
        ))
        .unwrap();
        let through = match synced {
            Synced::Idle(through)
            | Synced::Wrote {
                end: StepEnd::Through(through),
                ..
            } => through.confirm(&self.wal).unwrap(),
            other @ (Synced::IndexReset | Synced::Wrote { .. }) => panic!("{other:?}"),
        };
        self.position = Some(*through.position());
        self.through = Some(through);
    }

    fn snapshot_with<P: PageSource>(
        &mut self,
        db: &mut P,
        now: i64,
    ) -> Result<FileInfo, SnapshotFailed> {
        let proof = self.through.as_ref().unwrap();
        block_on(write_snapshot(
            proof,
            &mut self.wal,
            db,
            &mut PageTable::new(),
            &self.remote,
            ts(now),
        ))
    }

    fn snapshot(&mut self, now: i64) -> Result<FileInfo, SnapshotFailed> {
        let mut db = std::mem::replace(&mut self.db, FakeDb::new(PS));
        let out = self.snapshot_with(&mut db, now);
        self.db = db;
        out
    }

    /// The image the file must hold: page number to fill byte.
    fn image(&self) -> BTreeMap<u32, u8> {
        let mut image: BTreeMap<u32, u8> = (1..=self.base).map(|n| (n, db_fill(n))).collect();
        for tx in &self.txs {
            for page in &tx.pages {
                image.insert(*page, tx.fill);
            }
        }
        image.retain(|n, _| *n <= self.size());
        image
    }

    /// A stranger copies every frame at or below the mark into the database file.
    fn backfill_to(&mut self, committed_txs: usize) {
        let mut frames = 0_u32;
        for tx in self.txs.iter().take(committed_txs) {
            for page in &tx.pages {
                self.db.write_page(pg(*page), &vec![tx.fill; PS.bytes()]);
            }
            frames = frames.saturating_add(u32::try_from(tx.pages.len()).unwrap());
        }
        self.wal.backfill(frames);
    }

    fn read_back(&self, info: &FileInfo) -> (Header, BTreeMap<u32, u8>) {
        let bytes = block_on(async {
            let len = ReadLen::new(ReadLen::MAX_RAW).unwrap();
            self.remote.read(&info.key, 0, len).await.unwrap()
        });
        let mut decoder = ReadDecoder::new(bytes.as_slice());
        let mut header = None;
        let mut pages = BTreeMap::new();
        while let Some(event) = decoder.next_event().unwrap() {
            match event {
                Event::Header(h) => header = Some(h),
                Event::Page(page) => {
                    let (pgno, data) = (page.pgno(), page.data());
                    assert!(data.iter().all(|b| *b == data[0]), "page {pgno:?}");
                    pages.insert(pgno.get(), data[0]);
                }
            }
        }
        decoder.finish().unwrap();
        (header.unwrap(), pages)
    }

    fn remote_files(&self) -> usize {
        block_on(async {
            let mut total = 0_usize;
            for level in Level::ALL {
                let listing = self.remote.list(level, None, PageLimit::MAX).await.unwrap();
                total = total.saturating_add(listing.files.len());
            }
            total
        })
    }
}

/// A database a stranger backfills into while it is read: before read `at`, page
/// `pgno` is overwritten with garbage, as a torn page mid-copy would be.
struct Racing {
    inner: FakeDb,
    reads: usize,
    tears: Vec<(usize, u32)>,
}

impl PageSource for Racing {
    fn page_count(&mut self) -> Result<u32, SourceError> {
        self.inner.page_count()
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        for (at, torn) in &self.tears {
            if *at == self.reads {
                self.inner.write_page(pg(*torn), &vec![GARBAGE; PS.bytes()]);
            }
        }
        self.reads = self.reads.saturating_add(1);
        self.inner.read_page(pgno, buf)
    }
}

/// A history: the transactions (page lists), the sync points between them, the
/// stranger's backfill point (a count of transactions) and its tear schedule.
#[derive(Clone, Debug)]
struct History {
    base: u32,
    txs: Vec<Vec<u32>>,
    syncs: Vec<bool>,
    backfill: usize,
    tears: Vec<(usize, u32)>,
}

fn histories() -> impl Strategy<Value = History> {
    (
        1_u32..=4,
        prop::collection::vec(prop::collection::vec(1_u32..=9, 1..4), 0..6),
        prop::collection::vec(any::<bool>(), 6),
        0_usize..8,
        prop::collection::vec((0_usize..12, 1_u32..=9), 0..5),
    )
        .prop_map(|(base, txs, syncs, backfill, tears)| History {
            base,
            txs,
            syncs,
            backfill,
            tears,
        })
}

fn build(history: &History) -> Rig {
    let mut rig = Rig::new(history.base);
    let mut now = 1000;
    for (index, pages) in history.txs.iter().enumerate() {
        rig.commit(pages);
        if history.syncs.get(index).copied().unwrap_or(false) {
            now = i64::saturating_add(now, 1000);
            rig.sync(now);
        }
    }
    rig.sync(i64::saturating_add(now, 1000));
    rig
}

proptest! {
    #[test]
    fn snapshot_equals_model(history in histories()) {
        let mut rig = build(&history);
        rig.backfill_to(history.backfill.min(rig.txs.len()));
        let info = rig.snapshot(1_000_000).unwrap();
        let (header, pages) = rig.read_back(&info);
        prop_assert_eq!(pages, rig.image());
        prop_assert_eq!(header.fields().commit, rig.size());
        prop_assert_eq!(info.key.range().max(), rig.position.unwrap().txid());
    }

    #[test]
    fn snapshot_ignores_concurrent_backfill(history in histories()) {
        let mut rig = build(&history);
        rig.backfill_to(history.backfill.min(rig.txs.len()));
        // The stranger only touches pages the WAL holds: those are in the map.
        let written: Vec<u32> = rig.txs.iter().flat_map(|tx| tx.pages.clone()).collect();
        let tears: Vec<(usize, u32)> = history
            .tears
            .iter()
            .filter(|(_, pgno)| written.contains(pgno))
            .copied()
            .collect();
        let mut racing = Racing {
            inner: std::mem::replace(&mut rig.db, FakeDb::new(PS)),
            reads: 0,
            tears,
        };
        let info = rig.snapshot_with(&mut racing, 1_000_000).unwrap();
        let (_, pages) = rig.read_back(&info);
        prop_assert_eq!(pages, rig.image());
    }
}

fn two_commits() -> Rig {
    let mut rig = Rig::new(3);
    rig.commit(&[2]);
    rig.sync(1000);
    rig.commit(&[3]);
    rig.sync(2000);
    rig
}

#[test]
fn snapshot_header_has_no_wal_origin() {
    let mut rig = two_commits();
    let info = rig.snapshot(5000).unwrap();
    let (header, _) = rig.read_back(&info);
    let fields = header.fields();
    assert!(fields.wal.is_none());
    assert!(fields.pre_apply.is_none());
    assert!(!fields.tracked);
    assert_eq!(fields.node_id, 0);
    assert!(header.is_snapshot());
    assert_eq!(fields.commit, 3);
    assert_eq!(fields.timestamp, ts(5000));
}

#[test]
fn snapshot_key_is_level_9_from_1() {
    let mut rig = two_commits();
    let txid = rig.position.unwrap().txid();
    assert!(txid > Txid::MIN);
    let info = rig.snapshot(5000).unwrap();
    assert_eq!(info.key.level(), Level::SNAPSHOT);
    assert_eq!(info.key.range(), TxidRange::new(Txid::MIN, txid).unwrap());
    assert_eq!(rig.remote_files(), 1);
}

#[test]
fn snapshot_timestamp_never_predates_the_proof() {
    let mut rig = two_commits();
    let info = rig.snapshot(1).unwrap();
    let (header, _) = rig.read_back(&info);
    assert_eq!(header.fields().timestamp, ts(2000));
}

#[test]
fn snapshot_borrows_the_proof_so_the_checkpoint_can_use_it() {
    let mut rig = two_commits();
    rig.snapshot(5000).unwrap();
    rig.snapshot(6000).unwrap();
    assert!(rig.through.is_some());
}

#[test]
fn snapshot_mark_above_synced_is_bug() {
    let mut rig = Rig::new(2);
    rig.commit(&[1]);
    rig.sync(1000);
    rig.commit(&[2]);
    // nBackfill passed the synced end: this is not a proof from this read transaction.
    rig.wal.backfill(2);
    let failed = rig.snapshot(2000).unwrap_err();
    assert_eq!(failed.fault, Fault::Bug(BugSite::SnapshotScan));
    assert_eq!(rig.remote_files(), 0);
}

#[test]
fn snapshot_store_error_maps_by_from_remote() {
    let mut rig = two_commits();
    // from_staging would read this as local I/O.
    rig.remote.inject_failure(
        StoreOp::Upload,
        StoreError::Auth {
            op: StoreOp::Upload,
        },
    );
    let failed = rig.snapshot(5000).unwrap_err();
    assert_eq!(failed.fault, Fault::StoreAuth);
    assert_eq!(failed.key.level(), Level::SNAPSHOT);
    assert_eq!(rig.remote_files(), 0);
    // The same call, again, is the retry.
    rig.snapshot(5000).unwrap();
    assert_eq!(rig.remote_files(), 1);
}

#[test]
fn a_database_read_error_aborts_the_upload_and_maps_by_from_source() {
    let mut rig = two_commits();
    rig.db.inject(SourceOp::ReadPage, SourceError::Busy);
    let failed = rig.snapshot(5000).unwrap_err();
    assert_eq!(failed.fault, Fault::SqliteBusy);
    assert_eq!(rig.remote_files(), 0);
}

#[test]
fn a_wal_restarted_since_the_proof_is_wal_moved() {
    let mut rig = two_commits();
    rig.wal.force_restart();
    let failed = rig.snapshot(5000).unwrap_err();
    assert_eq!(failed.fault, Fault::WalMoved);
}

#[test]
fn a_retaken_read_transaction_is_read_tx_lost() {
    let mut rig = two_commits();
    rig.wal.set_epoch(rig.wal.epoch().next());
    let failed = rig.snapshot(5000).unwrap_err();
    assert_eq!(failed.fault, Fault::ReadTxLost);
}

#[test]
fn a_deleted_wal_is_unlinked() {
    let mut rig = two_commits();
    rig.wal.remove_file();
    let failed = rig.snapshot(5000).unwrap_err();
    assert_eq!(failed.fault, Fault::WalUnlinked);
}

#[test]
fn a_corrupt_committed_frame_is_never_snapshotted() {
    let mut rig = two_commits();
    let at = lsr_wal::HEADER_LEN + lsr_wal::FRAME_HEADER_LEN + 7;
    rig.wal.corrupt_byte(at);
    let failed = rig.snapshot(5000).unwrap_err();
    assert_eq!(failed.fault, Fault::WalCorruptBelowCommitted);
    assert_eq!(rig.remote_files(), 0);
}

#[test]
fn an_empty_wal_snapshots_the_database_file() {
    let mut rig = Rig::new(3);
    rig.sync(1000);
    let info = rig.snapshot(2000).unwrap();
    let (header, pages) = rig.read_back(&info);
    assert_eq!(header.fields().commit, 3);
    assert_eq!(pages, rig.image());
}

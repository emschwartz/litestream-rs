//! Verify tests: one replica per finding, asserting exactly that finding.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use lsr_ltx::{
    DbChecksum, Encoder, Header, HeaderFields, PageChecksum, PageNo, Section, Timestamp, TxidRange,
};

use super::*;
use crate::conformance::block_on;
use crate::mem::MemStore;
use crate::store::{Listing, PageLimit, StoreOp, Upload};

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn range(min: u64, max: u64) -> TxidRange {
    TxidRange::new(txid(min), txid(max)).unwrap()
}

fn key(level: u8, min: u64, max: u64) -> FileKey {
    FileKey::new(Level::new(level).unwrap(), range(min, max))
}

fn size(bytes: u32) -> PageSize {
    PageSize::new(bytes).unwrap()
}

fn page_of(ps: PageSize, seed: u8) -> Vec<u8> {
    vec![seed; ps.bytes()]
}

fn checksum(ps: PageSize, state: &BTreeMap<u32, u8>) -> DbChecksum {
    state.iter().fold(DbChecksum::EMPTY, |acc, (pgno, seed)| {
        acc.toggle(PageChecksum::of(
            PageNo::new(*pgno, ps).unwrap(),
            &page_of(ps, *seed),
        ))
    })
}

/// One LTX file to encode: every page is one byte value repeated. The header
/// timestamp is max seconds, which is what [put] stores, so a file agrees
/// with its listing unless a test says otherwise.
#[derive(Clone)]
struct Spec {
    tracked: bool,
    ps: PageSize,
    commit: u32,
    min: u64,
    max: u64,
    pre: Option<DbChecksum>,
    post: Option<DbChecksum>,
    pages: Vec<(u32, u8)>,
}

impl Spec {
    fn plain(ps: u32, min: u64, max: u64, pages: &[(u32, u8)]) -> Self {
        Self {
            tracked: false,
            ps: size(ps),
            commit: 4,
            min,
            max,
            pre: None,
            post: None,
            pages: pages.to_vec(),
        }
    }

    fn encode(&self) -> Vec<u8> {
        let header = Header::new(HeaderFields {
            tracked: self.tracked,
            page_size: self.ps,
            commit: self.commit,
            txids: range(self.min, self.max),
            timestamp: Timestamp::from_unix_millis(i64::try_from(self.max).unwrap() * 1000),
            pre_apply: self.pre,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        for (pgno, seed) in &self.pages {
            let pgno = PageNo::new(*pgno, self.ps).unwrap();
            encoder.write_page(pgno, &page_of(self.ps, *seed)).unwrap();
        }
        encoder.finish(self.post).unwrap().0
    }
}

fn put_at(store: &MemStore, key: FileKey, created_ms: i64, bytes: &[u8]) {
    block_on(async {
        let created = Timestamp::from_unix_millis(created_ms);
        let mut upload = store.begin_upload(&key, created).await.unwrap();
        upload.write(bytes).await.unwrap();
        upload.commit().await.unwrap();
    });
}

fn put(store: &MemStore, level: u8, min: u64, max: u64, bytes: &[u8]) {
    let created = i64::try_from(max).unwrap() * 1000;
    put_at(store, key(level, min, max), created, bytes);
}

/// A snapshot with pages 1 to 4 at transaction max.
fn snapshot(store: &MemStore, max: u64) {
    let pages = [(1, 1), (2, 2), (3, 3), (4, 4)];
    put(store, 9, 1, max, &Spec::plain(512, 1, max, &pages).encode());
}

fn l0(store: &MemStore, txid: u64) {
    put(
        store,
        0,
        txid,
        txid,
        &Spec::plain(512, txid, txid, &[(2, 9)]).encode(),
    );
}

/// The level-0 file for transaction 1, which is a snapshot and holds both pages.
fn first_l0(store: &MemStore) {
    put(
        store,
        0,
        1,
        1,
        &Spec::plain(512, 1, 1, &[(1, 1), (2, 2), (3, 3), (4, 4)]).encode(),
    );
}

/// A snapshot and two transactions: nothing wrong.
fn clean() -> MemStore {
    let store = MemStore::new();
    snapshot(&store, 1);
    l0(&store, 2);
    l0(&store, 3);
    store
}

fn names(store: &impl ObjectStore) -> Report {
    block_on(verify(store, Depth::Names)).unwrap()
}

fn contents(store: &impl ObjectStore) -> Report {
    block_on(verify(store, Depth::Contents)).unwrap()
}

fn only(report: &Report) -> &Finding {
    match report.findings() {
        [one] => one,
        other => panic!("expected exactly one finding, got {other:?}"),
    }
}

/// A store that counts reads, lies about sizes and deletes chosen files just
/// before their first read.
struct Wrapped {
    inner: MemStore,
    vanish: Mutex<BTreeSet<FileKey>>,
    zero_sizes: bool,
    reads: AtomicUsize,
}

impl Wrapped {
    async fn before_read(&self, key: &FileKey) -> Result<(), StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let doomed = self.vanish.lock().unwrap().remove(key);
        if doomed {
            self.inner.delete(&[*key]).await?;
        }
        Ok(())
    }

    fn new(inner: MemStore) -> Self {
        Self {
            inner,
            vanish: Mutex::new(BTreeSet::new()),
            zero_sizes: false,
            reads: AtomicUsize::new(0),
        }
    }
}

impl ObjectStore for Wrapped {
    type Upload = <MemStore as ObjectStore>::Upload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let mut listing = self.inner.list(level, after, limit).await?;
        if self.zero_sizes {
            for file in &mut listing.files {
                file.size = 0;
            }
        }
        Ok(listing)
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.before_read(key).await?;
        self.inner.read(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.before_read(&file.key).await?;
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        self.inner.begin_upload(key, created_at).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

#[test]
fn a_clean_replica_has_no_findings_at_either_depth() {
    let store = clean();
    for depth in [Depth::Names, Depth::Contents] {
        let report = block_on(verify(&store, depth)).unwrap();
        assert!(report.findings().is_empty(), "{:?}", report.findings());
        assert_eq!(report.worst(), None);
        assert!(matches!(report.latest(), PlanOutcome::Plan(chain) if chain.reaches() == txid(3)));
    }
    // The walk reads each file once and the latest restore reads its plan again.
    let report = contents(&store);
    assert_eq!(report.files_read(), 6);
    assert!(report.bytes_read() > 0);
}

#[test]
fn level_summaries_count_files_bytes_and_range() {
    let report = names(&clean());
    let zero = &report.levels()[0];
    assert_eq!(zero.files(), 2);
    assert_eq!(zero.range(), Some(range(2, 3)));
    let listed =
        |min: u64| u64::try_from(Spec::plain(512, min, min, &[(2, 9)]).encode().len()).unwrap();
    assert_eq!(zero.bytes(), listed(2) + listed(3));
    assert_eq!(zero.skipped(), 0);
    assert_eq!(report.levels()[9].range(), Some(range(1, 1)));
    assert_eq!(report.levels()[4].files(), 0);
    assert_eq!(report.levels()[4].range(), None);
}

#[test]
fn level_summaries_count_unrecognized_names() {
    let store = clean();
    store.plant_unrecognized(Level::ZERO, "lock.json");
    store.plant_unrecognized(Level::ZERO, "x.tmp");
    let report = names(&store);
    assert_eq!(report.levels()[0].skipped(), 2);
    assert_eq!(report.levels()[9].skipped(), 0);
}

/// A snapshot several mebibytes long, of pages that do not compress.
fn big_snapshot() -> Vec<u8> {
    let ps = size(65_536);
    let pages = 80_u32;
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps,
        commit: pages,
        txids: range(1, 1),
        timestamp: Timestamp::from_unix_millis(1000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut x = 0x9E37_79B9_7F4A_7C15_u64;
    for pgno in 1..=pages {
        let page: Vec<u8> = (0..ps.bytes())
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_be_bytes()[0]
            })
            .collect();
        encoder
            .write_page(PageNo::new(pgno, ps).unwrap(), &page)
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// A file longer than one read is read whole, four mebibytes at a time: a
/// full chunk is not the end of the object.
#[test]
fn a_file_longer_than_one_chunk_is_read_whole_in_four_mebibyte_reads() {
    let bytes = big_snapshot();
    let chunk = 4 * 1024 * 1024;
    assert!(
        bytes.len() > chunk && bytes.len() < 2 * chunk,
        "{}",
        bytes.len()
    );
    let inner = MemStore::new();
    put(&inner, 9, 1, 1, &bytes);
    let store = Wrapped::new(inner);
    let report = contents(&store);
    assert!(report.findings().is_empty(), "{:?}", report.findings());
    assert_eq!(report.files_read(), 2);
    let read = u64::try_from(bytes.len()).unwrap();
    assert_eq!(report.bytes_read(), 2 * read);
    // Two reads in the walk and two in the dry run.
    assert_eq!(store.reads.load(Ordering::Relaxed), 4);
}

/// A level-0 file from transaction 1 can be a base but is no restore point:
/// the history between it and a newer snapshot is nobody's, so a hole there
/// is not reported.
#[test]
fn a_stray_level_zero_base_beside_a_newer_snapshot_reports_nothing() {
    let store = MemStore::new();
    first_l0(&store);
    snapshot(&store, 5);
    l0(&store, 6);
    let report = names(&store);
    assert!(report.findings().is_empty(), "{:?}", report.findings());
}

/// The page size every file is held to is the latest plan's base, even when
/// the odd file is the first one the walk reads.
#[test]
fn the_page_size_reference_is_the_base_even_when_the_odd_file_is_read_first() {
    let store = MemStore::new();
    snapshot(&store, 1);
    put(
        &store,
        0,
        2,
        2,
        &Spec::plain(1024, 2, 2, &[(2, 9)]).encode(),
    );
    l0(&store, 3);
    let report = contents(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::PageSizeDiffers { key: k, expected, found, .. }
            if *k == key(0, 2, 2) && *expected == size(512) && *found == size(1024)),
        "{finding:?}"
    );
}

/// A database that shrinks and grows again reads as zeros where it did not
/// write: the dry run's sink drops the truncated pages as a file would.
#[test]
fn a_database_that_shrinks_then_grows_verifies_clean() {
    let ps = size(512);
    let zero = PageChecksum::of(PageNo::new(3, ps).unwrap(), &[0; 512]);
    let mut snap = Spec::plain(512, 1, 1, &[(1, 1), (2, 2), (3, 3), (4, 4)]);
    snap.tracked = true;
    let four = checksum(ps, &BTreeMap::from([(1, 1), (2, 2), (3, 3), (4, 4)]));
    snap.post = Some(four);
    let mut shrink = Spec::plain(512, 2, 2, &[(1, 5)]);
    shrink.tracked = true;
    shrink.commit = 2;
    shrink.pre = Some(four);
    let two = checksum(ps, &BTreeMap::from([(1, 5), (2, 2)]));
    shrink.post = Some(two);
    let mut grow = Spec::plain(512, 3, 3, &[(4, 7)]);
    grow.tracked = true;
    grow.pre = Some(two);
    grow.post = Some(checksum(ps, &BTreeMap::from([(1, 5), (2, 2), (4, 7)])).toggle(zero));
    let files: Fixture = vec![
        (9, 1, 1, snap.encode()),
        (0, 2, 2, shrink.encode()),
        (0, 3, 3, grow.encode()),
    ];
    let store = replica(&files);
    restore_outcome(&store).unwrap();
    let report = contents(&store);
    assert!(report.findings().is_empty(), "{:?}", report.findings());
}

#[test]
fn an_empty_replica_has_no_finding_but_is_not_restorable() {
    let report = names(&MemStore::new());
    assert!(report.findings().is_empty());
    assert!(matches!(report.latest(), PlanOutcome::EmptyReplica));
    assert_eq!(report.worst(), Some(Severity::Unrestorable));
}

#[test]
fn a_young_replica_of_level_zero_files_from_one_has_no_findings() {
    let store = MemStore::new();
    first_l0(&store);
    l0(&store, 2);
    assert!(names(&store).findings().is_empty());
}

#[test]
fn a_hole_in_a_young_replica_is_named_against_its_level_zero_base() {
    let store = MemStore::new();
    first_l0(&store);
    l0(&store, 3);
    let report = names(&store);
    assert!(matches!(
        only(&report),
        Finding::Gap { snapshot, at_tip: true, .. } if *snapshot == key(0, 1, 1)
    ));
}

#[test]
fn files_without_a_snapshot_are_no_snapshot() {
    let store = MemStore::new();
    l0(&store, 2);
    let report = names(&store);
    assert!(matches!(only(&report), Finding::NoSnapshot));
    assert_eq!(only(&report).severity(), Severity::Unrestorable);
}

#[test]
fn a_hole_after_the_newest_snapshot_is_a_gap_at_the_tip() {
    let store = MemStore::new();
    snapshot(&store, 1);
    l0(&store, 3);
    let report = names(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::Gap { snapshot, have_through, next_starts, at_tip: true }
            if *snapshot == key(9, 1, 1) && *have_through == txid(1) && *next_starts == txid(3)),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Unrestorable);
}

#[test]
fn a_hole_behind_the_newest_snapshot_is_older_history_and_the_tip_is_fine() {
    let store = MemStore::new();
    snapshot(&store, 1);
    l0(&store, 3);
    snapshot(&store, 3);
    l0(&store, 4);
    let report = names(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::Gap { snapshot, have_through, next_starts, at_tip: false }
            if *snapshot == key(9, 1, 1) && *have_through == txid(1) && *next_starts == txid(3)),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Degraded);
    assert!(matches!(report.latest(), PlanOutcome::Plan(_)));
    assert_eq!(report.worst(), Some(Severity::Degraded));
}

#[test]
fn holes_in_front_of_and_behind_a_snapshot_are_both_reported() {
    let store = MemStore::new();
    snapshot(&store, 1);
    l0(&store, 3);
    snapshot(&store, 3);
    l0(&store, 5);
    let report = names(&store);
    let gaps: Vec<_> = report
        .findings()
        .iter()
        .filter_map(|finding| {
            let Finding::Gap {
                snapshot,
                have_through,
                next_starts,
                at_tip,
            } = finding
            else {
                return None;
            };
            Some((*snapshot, *have_through, *next_starts, *at_tip))
        })
        .collect();
    assert_eq!(gaps.len(), report.findings().len());
    assert_eq!(
        gaps,
        [
            (key(9, 1, 1), txid(1), txid(3), false),
            (key(9, 1, 3), txid(3), txid(5), true)
        ]
    );
    assert_eq!(report.worst(), Some(Severity::Unrestorable));
}

#[test]
fn a_level_nine_file_not_starting_at_one_is_malformed() {
    let store = clean();
    put(&store, 9, 2, 3, b"never read at this depth");
    let report = names(&store);
    assert!(matches!(only(&report), Finding::MalformedSnapshot { key: k } if *k == key(9, 2, 3)));
    assert_eq!(only(&report).severity(), Severity::Degraded);
    assert!(matches!(report.latest(), PlanOutcome::Plan(_)));
}

#[test]
fn unrecognized_names_are_a_note_per_level() {
    let store = clean();
    store.plant_unrecognized(Level::ZERO, "lock.json");
    store.plant_unrecognized(Level::ZERO, "x.tmp");
    let report = names(&store);
    assert!(matches!(
        only(&report),
        Finding::UnrecognizedNames { level, count: 2 } if *level == Level::ZERO
    ));
    assert_eq!(only(&report).severity(), Severity::Note);
    assert_eq!(report.worst(), Some(Severity::Note));
}

#[test]
fn a_truncated_file_is_unreadable_and_names_where() {
    let store = clean();
    let bytes = Spec::plain(512, 2, 2, &[(2, 9)]).encode();
    put(&store, 0, 2, 2, &bytes[..bytes.len() - 5]);
    let report = contents(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::Unreadable { key: k, error: DecodeError::Truncated { section: Section::Trailer }, in_latest_plan: true }
            if *k == key(0, 2, 2)),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Unrestorable);
    assert!(names(&store).findings().is_empty());
}

#[test]
fn a_flipped_byte_in_a_page_is_unreadable() {
    let store = clean();
    let mut bytes = Spec::plain(512, 2, 2, &[(2, 9)]).encode();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x40;
    put(&store, 0, 2, 2, &bytes);
    let report = contents(&store);
    assert!(matches!(only(&report), Finding::Unreadable { key: k, .. } if *k == key(0, 2, 2)));
}

#[test]
fn bytes_after_the_trailer_are_unreadable() {
    let store = clean();
    let mut bytes = Spec::plain(512, 2, 2, &[(2, 9)]).encode();
    bytes.extend_from_slice(b"tail");
    put(&store, 0, 2, 2, &bytes);
    let report = contents(&store);
    assert!(matches!(
        only(&report),
        Finding::Unreadable {
            error: DecodeError::TrailingBytes,
            ..
        }
    ));
}

#[test]
fn a_header_covering_other_transactions_than_its_name_is_a_range_mismatch() {
    let store = MemStore::new();
    snapshot(&store, 1);
    l0(&store, 2);
    // Named 3..3 but the header says 2..3; same timestamp as the name.
    put(&store, 0, 3, 3, &Spec::plain(512, 2, 3, &[(2, 9)]).encode());
    let report = contents(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::RangeMismatch { key: k, header, .. }
            if *k == key(0, 3, 3) && *header == range(2, 3)),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Unrestorable);
}

#[test]
fn a_level_nine_file_with_a_delta_header_is_not_a_snapshot() {
    let store = clean();
    put(&store, 9, 2, 2, &Spec::plain(512, 2, 2, &[(2, 9)]).encode());
    let report = contents(&store);
    assert!(matches!(
        report.findings(),
        [Finding::MalformedSnapshot { .. }, Finding::NotASnapshot { key: k, .. }] if *k == key(9, 2, 2)
    ));
    // The latest plan never reads this file, so it is older history.
    assert_eq!(report.worst(), Some(Severity::Degraded));
}

#[test]
fn a_page_size_that_differs_from_the_snapshots_is_reported_on_the_odd_file() {
    let store = clean();
    put(
        &store,
        0,
        4,
        4,
        &Spec::plain(1024, 4, 4, &[(2, 9)]).encode(),
    );
    let report = contents(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::PageSizeDiffers { key: k, expected, found, .. }
            if *k == key(0, 4, 4) && *expected == size(512) && *found == size(1024)),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Unrestorable);
}

#[test]
fn a_listed_size_that_is_not_the_real_size_is_reported_with_both() {
    let mut store = Wrapped::new(clean());
    store.zero_sizes = true;
    let report = contents(&store);
    let sizes: Vec<_> = report
        .findings()
        .iter()
        .filter_map(|finding| {
            let Finding::SizeDiffers { key, listed, read } = finding else {
                return None;
            };
            Some((*key, *listed, *read))
        })
        .collect();
    assert_eq!(sizes.len(), report.findings().len());
    assert_eq!(sizes.len(), 3);
    assert!(
        sizes
            .iter()
            .all(|(_, listed, read)| *listed == 0 && *read > 0)
    );
    assert_eq!(report.worst(), Some(Severity::Degraded));
}

#[test]
fn a_stored_time_that_differs_from_the_header_is_reported_with_both() {
    let store = clean();
    let bytes = Spec::plain(512, 2, 2, &[(2, 9)]).encode();
    put_at(&store, key(0, 2, 2), 5, &bytes);
    let report = contents(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::TimestampsDiffer { files: 1, first, largest }
            if *first == key(0, 2, 2) && *largest == Duration::from_millis(1995)),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Note);
    assert_eq!(report.worst(), Some(Severity::Note));
}

#[test]
fn a_file_that_does_not_start_where_its_neighbour_ended_breaks_the_checksum_chain() {
    let ps = size(512);
    let first = BTreeMap::from([(1, 1_u8)]);
    let store = MemStore::new();
    let mut snap = Spec::plain(512, 1, 1, &[(1, 1)]);
    snap.commit = 1;
    snap.tracked = true;
    snap.post = Some(checksum(ps, &first));
    put(&store, 9, 1, 1, &snap.encode());
    let second = BTreeMap::from([(1, 3_u8)]);
    let mut next = Spec::plain(512, 2, 2, &[(1, 3)]);
    next.commit = 1;
    next.tracked = true;
    next.pre = Some(checksum(ps, &first));
    next.post = Some(checksum(ps, &second));
    put(&store, 0, 2, 2, &next.encode());
    let wrong = checksum(ps, &BTreeMap::from([(1, 2_u8)]));
    let mut after = Spec::plain(512, 3, 3, &[(1, 4)]);
    after.commit = 1;
    after.tracked = true;
    after.pre = Some(wrong);
    after.post = Some(checksum(ps, &BTreeMap::from([(1, 4_u8)])));
    put(&store, 0, 3, 3, &after.encode());
    let report = contents(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::ChecksumChain { key: k, expected, found, .. }
            if *k == key(0, 3, 3) && *expected == checksum(ps, &second) && *found == wrong),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Unrestorable);
}

#[test]
fn a_file_that_vanishes_mid_walk_is_a_note() {
    let store = Wrapped::new(clean());
    store.vanish.lock().unwrap().insert(key(0, 2, 2));
    let report = contents(&store);
    // The dry run plans over the walk's listing, trips on the missing file,
    // lists afresh and finds a gap, so the latest state does not restore.
    assert!(matches!(
        report.findings(),
        [Finding::Vanished { key: k }, Finding::LatestRestoreFails { key: None, .. }] if *k == key(0, 2, 2)
    ));
    assert_eq!(report.findings()[0].severity(), Severity::Note);
    assert_eq!(report.worst(), Some(Severity::Unrestorable));
    assert_eq!(report.files_read(), 3);
}

#[test]
fn names_depth_reads_no_object() {
    let store = Wrapped::new(clean());
    let report = block_on(verify(&store, Depth::Names)).unwrap();
    assert_eq!(store.reads.load(Ordering::Relaxed), 0);
    assert_eq!((report.files_read(), report.bytes_read()), (0, 0));
    let report = block_on(verify(&store, Depth::Contents)).unwrap();
    assert_eq!(report.files_read(), 6);
    assert!(store.reads.load(Ordering::Relaxed) >= 6);
}

#[test]
fn a_failing_read_is_an_error_not_a_finding() {
    let store = clean();
    let fault = StoreError::Transient {
        op: StoreOp::Read,
        kind: std::io::ErrorKind::TimedOut,
    };
    store.inject_failure(StoreOp::Read, fault.clone());
    let got = block_on(verify(&store, Depth::Contents));
    assert!(matches!(got, Err(VerifyError::Store(err)) if err == fault));
}

#[test]
fn a_failing_listing_is_an_error_at_either_depth() {
    for depth in [Depth::Names, Depth::Contents] {
        let store = clean();
        let fault = StoreError::Transient {
            op: StoreOp::List,
            kind: std::io::ErrorKind::TimedOut,
        };
        store.inject_failure(StoreOp::List, fault.clone());
        let got = block_on(verify(&store, depth));
        assert!(matches!(
            got,
            Err(VerifyError::Inventory(InventoryError::Store(err))) if err == fault
        ));
    }
}

#[test]
fn findings_are_capped_and_the_last_says_how_many_were_dropped() {
    let store = MemStore::new();
    let extra = 5;
    // From 2: a file that begins at transaction 1 would be a base.
    for n in 2..=u64::try_from(MAX_FINDINGS + extra + 1).unwrap() {
        put(&store, 0, n, n, b"x");
    }
    let report = contents(&store);
    // NoSnapshot comes first, then one Unreadable per file.
    assert_eq!(report.findings().len(), MAX_FINDINGS + 1);
    assert!(matches!(
        report.findings().first(),
        Some(Finding::NoSnapshot)
    ));
    assert!(matches!(
        report.findings().last(),
        Some(Finding::Truncated { more }) if *more == extra + 1
    ));
    assert_eq!(report.worst(), Some(Severity::Unrestorable));
}

#[test]
fn a_dropped_finding_still_counts_towards_worst() {
    let mut findings = Findings::default();
    for _ in 0..MAX_FINDINGS {
        findings.push(Finding::Vanished { key: key(0, 1, 1) });
    }
    findings.push(Finding::NoSnapshot);
    let (kept, worst) = findings.finish();
    assert_eq!(kept.len(), MAX_FINDINGS + 1);
    assert!(matches!(kept.last(), Some(Finding::Truncated { more: 1 })));
    assert!(
        !kept
            .iter()
            .any(|finding| matches!(finding, Finding::NoSnapshot))
    );
    assert_eq!(worst, Some(Severity::Unrestorable));
}

// Review (review-core2): failing tests that demonstrate defects. Each pins
// "worst is Unrestorable" to "restore(Latest) fails".

fn restore_outcome(
    store: &MemStore,
) -> Result<crate::restore::Restored, crate::restore::RestoreError> {
    block_on(crate::restore::restore(
        store,
        crate::plan::RestoreTarget::Latest,
        crate::restore::MemSink::new(),
    ))
}

fn tracked_snapshot(store: &MemStore) -> DbChecksum {
    let ps = size(512);
    let state = BTreeMap::from([(1, 1_u8)]);
    let mut snap = Spec::plain(512, 1, 1, &[(1, 1)]);
    snap.commit = 1;
    snap.tracked = true;
    snap.post = Some(checksum(ps, &state));
    put(store, 9, 1, 1, &snap.encode());
    checksum(ps, &state)
}

#[test]
fn a_broken_checksum_link_across_levels_is_unrestorable() {
    // Snapshot at level 9, next file at level 0: the restore compares them,
    // verify only compares neighbours in one level.
    let ps = size(512);
    let store = MemStore::new();
    tracked_snapshot(&store);
    let mut next = Spec::plain(512, 2, 2, &[(1, 3)]);
    next.commit = 1;
    next.tracked = true;
    next.pre = Some(checksum(ps, &BTreeMap::from([(1, 2_u8)])));
    next.post = Some(checksum(ps, &BTreeMap::from([(1, 3_u8)])));
    put(&store, 0, 2, 2, &next.encode());
    let restored = restore_outcome(&store);
    assert!(restored.is_err(), "restore succeeded: {restored:?}");
    let report = contents(&store);
    assert_eq!(
        report.worst(),
        Some(Severity::Unrestorable),
        "restore fails with {restored:?} but verify found {:?}",
        report.findings()
    );
}

#[test]
fn pages_that_do_not_reach_the_post_apply_checksum_are_unrestorable() {
    let ps = size(512);
    let store = MemStore::new();
    let first = tracked_snapshot(&store);
    let mut next = Spec::plain(512, 2, 2, &[(1, 3)]);
    next.commit = 1;
    next.tracked = true;
    next.pre = Some(first);
    next.post = Some(checksum(ps, &BTreeMap::from([(1, 9_u8)])));
    put(&store, 0, 2, 2, &next.encode());
    let restored = restore_outcome(&store);
    assert!(restored.is_err(), "restore succeeded: {restored:?}");
    let report = contents(&store);
    assert_eq!(
        report.worst(),
        Some(Severity::Unrestorable),
        "restore fails with {restored:?} but verify found {:?}",
        report.findings()
    );
}

#[test]
fn a_chain_ending_in_a_commit_zero_file_is_unrestorable() {
    let store = MemStore::new();
    snapshot(&store, 1);
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size(512),
        commit: 0,
        txids: range(2, 2),
        timestamp: Timestamp::from_unix_millis(2000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let bytes = lsr_ltx::testing::encode_commit_zero(header, None).unwrap();
    put(&store, 0, 2, 2, &bytes);
    let restored = restore_outcome(&store);
    assert!(restored.is_err(), "restore succeeded: {restored:?}");
    let report = contents(&store);
    assert_eq!(
        report.worst(),
        Some(Severity::Unrestorable),
        "restore fails with {restored:?} but verify found {:?}",
        report.findings()
    );
}

#[test]
fn a_corrupt_file_the_latest_plan_skips_is_not_unrestorable() {
    // Level 1 covers 2..3, so the latest plan never reads the corrupt level-0
    // file; only Txid(2) does. That is older history: Degraded.
    let store = clean();
    let mut bytes = Spec::plain(512, 2, 2, &[(2, 9)]).encode();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x40;
    put(&store, 0, 2, 2, &bytes);
    put(&store, 1, 2, 3, &Spec::plain(512, 2, 3, &[(2, 9)]).encode());
    let restored = restore_outcome(&store);
    assert!(restored.is_ok(), "restore failed: {restored:?}");
    let report = contents(&store);
    assert_ne!(
        report.worst(),
        Some(Severity::Unrestorable),
        "restore(Latest) succeeds but verify says {:?}",
        report.findings()
    );
}

#[test]
fn a_restore_failure_is_one_finding_naming_its_file() {
    let ps = size(512);
    let store = MemStore::new();
    tracked_snapshot(&store);
    let mut next = Spec::plain(512, 2, 2, &[(1, 3)]);
    next.commit = 1;
    next.tracked = true;
    next.pre = Some(checksum(ps, &BTreeMap::from([(1, 2_u8)])));
    next.post = Some(checksum(ps, &BTreeMap::from([(1, 3_u8)])));
    put(&store, 0, 2, 2, &next.encode());
    let report = contents(&store);
    let [finding] = report.findings() else {
        panic!("expected one finding, got {:?}", report.findings());
    };
    assert!(
        matches!(finding, Finding::LatestRestoreFails { key: Some(_), .. }),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Unrestorable);
}

#[test]
fn names_depth_cannot_vouch_for_a_restore() {
    let store = MemStore::new();
    tracked_snapshot(&store);
    let mut next = Spec::plain(512, 2, 2, &[(1, 3)]);
    next.commit = 1;
    next.tracked = true;
    next.pre = Some(checksum(size(512), &BTreeMap::from([(1, 2_u8)])));
    next.post = Some(checksum(size(512), &BTreeMap::from([(1, 3_u8)])));
    put(&store, 0, 2, 2, &next.encode());
    restore_outcome(&store).unwrap_err();
    assert_eq!(names(&store).worst(), None);
}

#[test]
fn timestamps_that_differ_are_one_note_with_the_count_and_the_largest_gap() {
    let store = clean();
    // Both created long after their headers' times (2000 and 3000).
    put_at(
        &store,
        key(0, 2, 2),
        2500,
        &Spec::plain(512, 2, 2, &[(2, 9)]).encode(),
    );
    put_at(
        &store,
        key(0, 3, 3),
        9000,
        &Spec::plain(512, 3, 3, &[(2, 9)]).encode(),
    );
    let report = contents(&store);
    let finding = only(&report);
    assert!(
        matches!(finding, Finding::TimestampsDiffer { files: 2, first, largest }
            if *first == key(0, 2, 2) && *largest == Duration::from_millis(6000)),
        "{finding:?}"
    );
    assert_eq!(report.worst(), Some(Severity::Note));
}

/// One file of a fixture: level, first and last transaction, bytes.
type Fixture = Vec<(u8, u64, u64, Vec<u8>)>;

fn replica(files: &Fixture) -> MemStore {
    let store = MemStore::new();
    for (level, min, max, bytes) in files {
        put(&store, *level, *min, *max, bytes);
    }
    store
}

/// Verify at contents depth says unrestorable exactly when the latest restore
/// fails.
fn assert_verify_agrees_with_restore(files: &Fixture, what: &str) {
    let store = replica(files);
    let restored = restore_outcome(&store);
    let report = contents(&store);
    let unrestorable = report.worst() == Some(Severity::Unrestorable);
    assert_eq!(
        unrestorable,
        restored.is_err(),
        "{what}: restore {restored:?}, verify {:?}",
        report.findings()
    );
}

fn fixtures() -> Vec<Fixture> {
    let ps = size(512);
    let state = |n: u8| checksum(ps, &BTreeMap::from([(1, n)]));
    let mut snap = Spec::plain(512, 1, 1, &[(1, 1)]);
    snap.commit = 1;
    snap.tracked = true;
    snap.post = Some(state(1));
    let mut next = Spec::plain(512, 2, 2, &[(1, 3)]);
    next.commit = 1;
    next.tracked = true;
    next.pre = Some(state(1));
    next.post = Some(state(3));
    let plain = |level, min, max| {
        (
            level,
            min,
            max,
            Spec::plain(512, min, max, &[(2, 9)]).encode(),
        )
    };
    let snapshot_bytes = Spec::plain(512, 1, 1, &[(1, 1), (2, 2), (3, 3), (4, 4)]).encode();
    vec![
        vec![
            (9, 1, 1, snapshot_bytes.clone()),
            plain(0, 2, 2),
            plain(0, 3, 3),
        ],
        vec![
            (9, 1, 1, snapshot_bytes),
            plain(0, 2, 2),
            plain(0, 3, 3),
            plain(1, 2, 3),
        ],
        vec![(9, 1, 1, snap.encode()), (0, 2, 2, next.encode())],
    ]
}

#[test]
fn verify_is_unrestorable_exactly_when_the_latest_restore_fails() {
    for (n, base) in fixtures().iter().enumerate() {
        assert_verify_agrees_with_restore(base, &format!("fixture {n}"));
        for (at, file) in base.iter().enumerate() {
            let mut gone = base.clone();
            gone.remove(at);
            assert_verify_agrees_with_restore(&gone, &format!("fixture {n} without file {at}"));
            for byte in 0..file.3.len() {
                let mut flipped = base.clone();
                flipped[at].3[byte] ^= 0x40;
                assert_verify_agrees_with_restore(
                    &flipped,
                    &format!("fixture {n} file {at} byte {byte}"),
                );
            }
        }
    }
}

/// The scheduled verify's comparison of a restore with the live database.
mod against {
    use std::num::NonZeroU32;

    use super::*;
    use crate::verify::{Compared, LivePages, verify_against};

    /// The live database as page seeds, at transaction `at`.
    fn live(at: u64, state: &[(u32, u8)]) -> LivePages {
        let ps = size(512);
        let pages = state
            .iter()
            .map(|(pgno, seed)| {
                let page = PageNo::new(*pgno, ps).unwrap();
                (page, PageChecksum::of(page, &page_of(ps, *seed)))
            })
            .collect();
        let commit = NonZeroU32::new(u32::try_from(state.len()).unwrap()).unwrap();
        LivePages::new(txid(at), ps, commit, pages)
    }

    fn compared(store: &MemStore, live: &LivePages) -> Compared {
        let verified = block_on(verify_against(store, live)).unwrap();
        assert!(verified.report().findings().is_empty(), "{verified:?}");
        verified.into_parts().1
    }

    /// The clean replica at 3: the snapshot's pages, page 2 rewritten by 2 and 3.
    const AT_3: [(u32, u8); 4] = [(1, 1), (2, 9), (3, 3), (4, 4)];

    #[test]
    fn a_restore_equal_to_the_live_database_is_the_same() {
        let got = compared(&clean(), &live(3, &AT_3));
        assert!(
            matches!(got, Compared::Same { txid: t, pages: 4 } if t == txid(3)),
            "{got:?}"
        );
    }

    #[test]
    fn an_older_transaction_is_compared_with_its_own_restore() {
        let got = compared(&clean(), &live(1, &[(1, 1), (2, 2), (3, 3), (4, 4)]));
        assert!(matches!(got, Compared::Same { .. }), "{got:?}");
    }

    /// The replica holds a page the live database never had at that
    /// transaction: the page and the transaction are named.
    #[test]
    fn a_stale_page_names_the_page_and_the_transaction() {
        let got = compared(&clean(), &live(3, &[(1, 1), (2, 9), (3, 7), (4, 4)]));
        assert!(
            matches!(
                got,
                Compared::Differs { txid: t, first: 3, pages: 1, live_commit: 4, restored_commit: 4 }
                    if t == txid(3)
            ),
            "{got:?}"
        );
    }

    #[test]
    fn a_different_length_differs_even_when_the_common_pages_agree() {
        let mut longer = AT_3.to_vec();
        longer.push((5, 5));
        let got = compared(&clean(), &live(3, &longer));
        assert!(
            matches!(
                got,
                Compared::Differs {
                    first: 5,
                    pages: 1,
                    live_commit: 5,
                    restored_commit: 4,
                    ..
                }
            ),
            "{got:?}"
        );
    }

    /// Compaction merged transaction 2 into a file that also holds 3, and its
    /// level-0 file is gone: nothing is wrong, the next run reads a newer one.
    #[test]
    fn a_transaction_merged_into_a_larger_file_is_merged_not_a_failure() {
        let store = MemStore::new();
        snapshot(&store, 1);
        put(&store, 1, 2, 3, &Spec::plain(512, 2, 3, &[(2, 9)]).encode());
        let got = compared(&store, &live(2, &AT_3));
        assert!(
            matches!(got, Compared::Merged { txid: t } if t == txid(2)),
            "{got:?}"
        );
    }

    #[test]
    fn a_transaction_the_replica_does_not_hold_fails_to_restore() {
        let got = compared(&clean(), &live(5, &AT_3));
        assert!(
            matches!(got, Compared::RestoreFails { txid: t, error: RestoreError::NoPlan(_) } if t == txid(5)),
            "{got:?}"
        );
    }
}

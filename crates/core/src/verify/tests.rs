//! Verify tests: one replica per finding, asserting exactly that finding.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_ltx::{Checksum, Encoder, Header, HeaderFields, PageNo, Section, Timestamp, TxidRange};

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

fn checksum(ps: PageSize, state: &BTreeMap<u32, u8>) -> Checksum {
    state.iter().fold(Checksum::EMPTY, |acc, (pgno, seed)| {
        acc.toggle(Checksum::of_page(
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
    pre: Option<Checksum>,
    post: Option<Checksum>,
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
        self.reads.fetch_add(1, Ordering::Relaxed);
        let doomed = self.vanish.lock().unwrap().remove(key);
        if doomed {
            self.inner.delete(&[*key]).await?;
        }
        self.inner.read(key, offset, len).await
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
    let report = contents(&store);
    assert_eq!(report.files_read(), 3);
    assert!(report.bytes_read() > 0);
}

#[test]
fn level_summaries_count_files_bytes_and_range() {
    let report = names(&clean());
    let zero = &report.levels()[0];
    assert_eq!(zero.files(), 2);
    assert_eq!(zero.range(), Some(range(2, 3)));
    assert!(zero.bytes() > 0);
    assert_eq!(report.levels()[9].range(), Some(range(1, 1)));
    assert_eq!(report.levels()[4].files(), 0);
    assert_eq!(report.levels()[4].range(), None);
}

#[test]
fn an_empty_replica_has_no_finding_but_is_not_restorable() {
    let report = names(&MemStore::new());
    assert!(report.findings().is_empty());
    assert!(matches!(report.latest(), PlanOutcome::EmptyReplica));
    assert_eq!(report.worst(), Some(Severity::Unrestorable));
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
fn foreign_names_are_a_note_per_level() {
    let store = clean();
    store.plant_foreign(Level::ZERO, "lock.json");
    store.plant_foreign(Level::ZERO, "x.tmp");
    let report = names(&store);
    assert!(matches!(
        only(&report),
        Finding::ForeignNames { level, count: 2 } if *level == Level::ZERO
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
        matches!(finding, Finding::Unreadable { key: k, error: DecodeError::Truncated { section: Section::Trailer } }
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
        matches!(finding, Finding::RangeMismatch { key: k, header }
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
        [Finding::MalformedSnapshot { .. }, Finding::NotASnapshot { key: k }] if *k == key(9, 2, 2)
    ));
    assert_eq!(report.worst(), Some(Severity::Unrestorable));
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
        matches!(finding, Finding::PageSizeDiffers { key: k, expected, found }
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
        matches!(finding, Finding::TimestampDiffers { key: k, header, stored }
            if *k == key(0, 2, 2)
                && header.unix_millis() == 2000
                && stored.unix_millis() == 5),
        "{finding:?}"
    );
    assert_eq!(finding.severity(), Severity::Degraded);
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
        matches!(finding, Finding::ChecksumChain { key: k, expected, found }
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
    assert!(matches!(only(&report), Finding::Vanished { key: k } if *k == key(0, 2, 2)));
    assert_eq!(only(&report).severity(), Severity::Note);
    assert_eq!(report.files_read(), 2);
}

#[test]
fn names_depth_reads_no_object() {
    let store = Wrapped::new(clean());
    let report = block_on(verify(&store, Depth::Names)).unwrap();
    assert_eq!(store.reads.load(Ordering::Relaxed), 0);
    assert_eq!((report.files_read(), report.bytes_read()), (0, 0));
    let report = block_on(verify(&store, Depth::Contents)).unwrap();
    assert_eq!(report.files_read(), 3);
    assert!(store.reads.load(Ordering::Relaxed) >= 3);
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
    for n in 1..=u64::try_from(MAX_FINDINGS + extra).unwrap() {
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

//! Restore tests: hand-built chains for each semantic, then a model oracle.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use lsr_ltx::{
    Checksum, Encoder, Header, HeaderFields, PageNo, PageSize, Section, Timestamp, Txid, TxidRange,
    lock_page,
};
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::layout::Level;
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

/// One LTX file to encode: every page is one byte value repeated.
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
    fn plain(ps: u32, commit: u32, min: u64, max: u64, pages: &[(u32, u8)]) -> Self {
        Self {
            tracked: false,
            ps: size(ps),
            commit,
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
        if self.commit == 0 {
            // The strict encoder refuses commit 0; the decoder accepts it.
            return lsr_ltx::testing::encode_commit_zero(header, self.post).unwrap();
        }
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        for (pgno, seed) in &self.pages {
            let pgno = PageNo::new(*pgno, self.ps).unwrap();
            encoder.write_page(pgno, &page_of(self.ps, *seed)).unwrap();
        }
        encoder.finish(self.post).unwrap().0
    }
}

fn put(store: &MemStore, level: u8, min: u64, max: u64, bytes: &[u8]) {
    block_on(async {
        let key = key(level, min, max);
        let created = Timestamp::from_unix_millis(i64::try_from(max).unwrap() * 1000);
        let mut upload = store.begin_upload(&key, created).await.unwrap();
        upload.write(bytes).await.unwrap();
        upload.commit().await.unwrap();
    });
}

fn put_spec(store: &MemStore, level: u8, spec: &Spec) {
    put(store, level, spec.min, spec.max, &spec.encode());
}

fn run(
    store: &impl ObjectStore,
    target: RestoreTarget,
) -> (Result<Restored, RestoreError>, MemSink) {
    let sink = MemSink::new();
    (block_on(restore(store, target, sink.clone())), sink)
}

/// The first byte of every page: enough, because every page is one repeated byte.
fn seeds(db: &Database) -> Vec<u8> {
    (1..=db.page_count())
        .map(|pgno| db.page(pgno).unwrap().first().copied().unwrap())
        .collect()
}

fn file_fault(result: Result<Restored, RestoreError>) -> (FileKey, FileFault) {
    match result {
        Err(RestoreError::File { key, fault }) => (key, fault),
        other => panic!("expected a file fault, got {other:?}"),
    }
}

/// A snapshot and three incrementals that shrink and regrow the database.
fn four_file_replica() -> MemStore {
    let store = MemStore::new();
    put_spec(
        &store,
        9,
        &Spec::plain(512, 3, 1, 1, &[(1, 1), (2, 2), (3, 3)]),
    );
    put_spec(&store, 0, &Spec::plain(512, 3, 2, 2, &[(2, 9)]));
    put_spec(&store, 0, &Spec::plain(512, 2, 3, 3, &[(1, 5)]));
    put_spec(&store, 0, &Spec::plain(512, 4, 4, 4, &[(4, 7)]));
    store
}

#[test]
fn applies_files_oldest_first_and_newest_page_wins() {
    let store = four_file_replica();
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let restored = result.unwrap();
    let db = sink.published().unwrap();
    // Page 3 was dropped by the shrink and the regrowth never wrote it:
    // zeros, not the snapshot's 3.
    assert_eq!(seeds(&db), [5, 9, 0, 7]);
    assert_eq!(restored.reached(), txid(4));
    assert_eq!(restored.commit(), 4);
    assert_eq!(restored.files(), 4);
    assert_eq!(restored.attempts(), 1);
    assert_eq!(restored.page_size(), size(512));
    assert_eq!(restored.post_apply(), None);
    let listed: u64 = block_on(Inventory::load(&store))
        .unwrap()
        .files()
        .iter()
        .map(|f| f.size)
        .sum();
    assert_eq!(restored.bytes_read(), listed);
}

#[test]
fn pages_above_the_final_commit_are_dropped() {
    let store = four_file_replica();
    let (result, sink) = run(&store, RestoreTarget::Txid(txid(3)));
    result.unwrap();
    let db = sink.published().unwrap();
    assert_eq!(seeds(&db), [5, 9]);
    assert_eq!(db.page(3), None);
}

#[test]
fn tracked_chain_reports_the_final_checksum() {
    let ps = size(512);
    let first = BTreeMap::from([(1, 1_u8), (2, 2)]);
    let second = BTreeMap::from([(1, 1_u8), (2, 8)]);
    let store = MemStore::new();
    let mut snap = Spec::plain(512, 2, 1, 1, &[(1, 1), (2, 2)]);
    snap.tracked = true;
    snap.post = Some(checksum(ps, &first));
    put_spec(&store, 9, &snap);
    let mut next = Spec::plain(512, 2, 2, 2, &[(2, 8)]);
    next.tracked = true;
    next.pre = Some(checksum(ps, &first));
    next.post = Some(checksum(ps, &second));
    put_spec(&store, 0, &next);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    assert_eq!(result.unwrap().post_apply(), Some(checksum(ps, &second)));
    assert_eq!(seeds(&sink.published().unwrap()), [1, 8]);
}

#[test]
fn a_checksum_chain_break_is_reported_and_publishes_nothing() {
    let ps = size(512);
    let first = BTreeMap::from([(1, 1_u8)]);
    let store = MemStore::new();
    let mut snap = Spec::plain(512, 1, 1, 1, &[(1, 1)]);
    snap.tracked = true;
    snap.post = Some(checksum(ps, &first));
    put_spec(&store, 9, &snap);
    let wrong = checksum(ps, &BTreeMap::from([(1, 2_u8)]));
    let mut next = Spec::plain(512, 1, 2, 2, &[(1, 3)]);
    next.tracked = true;
    next.pre = Some(wrong);
    next.post = Some(checksum(ps, &BTreeMap::from([(1, 3_u8)])));
    put_spec(&store, 0, &next);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let (at, fault) = file_fault(result);
    assert_eq!(at, key(0, 2, 2));
    assert!(matches!(
        fault,
        FileFault::ChecksumChain { expected, found }
            if expected == checksum(ps, &first) && found == wrong
    ));
    assert_eq!(sink.published(), None);
}

/// Review: an overlapping file's pre-apply checksum cannot be compared, so only
/// the database's own checksum after the last file can catch a tracked chain
/// whose pages do not add up. Go's restore checks it (the merged snapshot's
/// post-apply, ltx decoder.go Close) and PLAN.md says the rolling checksum is
/// verified before the rename; here the restore publishes a database whose
/// checksum is not the one Restored::post_apply reports.
#[test]
fn an_overlapping_file_whose_pages_miss_its_post_apply_is_rejected() {
    let ps = size(512);
    let first = BTreeMap::from([(1, 1_u8), (2, 2)]);
    let claimed = BTreeMap::from([(1, 5_u8), (2, 9)]);
    let store = MemStore::new();
    let mut snap = Spec::plain(512, 2, 1, 2, &[(1, 1), (2, 2)]);
    snap.tracked = true;
    snap.post = Some(checksum(ps, &first));
    put_spec(&store, 9, &snap);
    // Covers transactions 2 and 3, so it overlaps the snapshot by one and its
    // pre-apply (the state before 2) is never compared.
    let mut merged = Spec::plain(512, 2, 2, 3, &[(1, 5)]);
    merged.tracked = true;
    merged.pre = Some(checksum(ps, &BTreeMap::from([(1, 1_u8)])));
    merged.post = Some(checksum(ps, &claimed));
    put_spec(&store, 1, &merged);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    if let Ok(restored) = &result {
        let db = sink.published().unwrap();
        let actual: BTreeMap<u32, u8> = (1..=db.page_count())
            .map(|p| (p, db.page(p).unwrap()[0]))
            .collect();
        panic!(
            "published {:?} with checksum {:?}, reported post_apply {:?}",
            seeds(&db),
            checksum(ps, &actual),
            restored.post_apply()
        );
    }
    assert!(matches!(
        result,
        Err(RestoreError::DatabaseChecksum { expected, .. }) if expected == checksum(ps, &claimed)
    ));
    assert_eq!(sink.published(), None);
}

#[test]
fn a_sink_failing_its_checksum_read_never_publishes() {
    let ps = size(512);
    let state = BTreeMap::from([(1, 1_u8)]);
    let store = MemStore::new();
    let mut snap = Spec::plain(512, 1, 1, 1, &[(1, 1)]);
    snap.tracked = true;
    snap.post = Some(checksum(ps, &state));
    put_spec(&store, 9, &snap);
    let sink = MemSink::new();
    sink.fail_at(SinkOp::Checksum, 0);
    let result = block_on(restore(&store, RestoreTarget::Latest, sink.clone()));
    assert!(matches!(
        &result,
        Err(RestoreError::Sink {
            op: SinkOp::Checksum,
            ..
        })
    ));
    assert_eq!(sink.published(), None);
}

#[test]
fn an_untracked_chain_is_not_checksummed() {
    let (result, sink) = run(&four_file_replica(), RestoreTarget::Latest);
    result.unwrap();
    assert_eq!(sink.calls(SinkOp::Checksum), 0);
}

#[test]
fn a_tracked_chain_is_checksummed_once_before_publish() {
    let ps = size(512);
    let state = BTreeMap::from([(1, 1_u8)]);
    let store = MemStore::new();
    let mut snap = Spec::plain(512, 1, 1, 1, &[(1, 1)]);
    snap.tracked = true;
    snap.post = Some(checksum(ps, &state));
    put_spec(&store, 9, &snap);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    result.unwrap();
    assert_eq!(sink.calls(SinkOp::Checksum), 1);
}

/// A page nobody wrote counts as zeros, as Go's ChecksumPages does over a
/// database file. (The lock page is skipped because PageNo cannot name it; the
/// restore test over the lock page is too slow to repeat here.)
#[test]
fn the_sink_checksum_counts_unwritten_pages_as_zeros() {
    let ps = size(512);
    let mut sink = MemSink::new();
    sink.write_page(PageNo::new(3, ps).unwrap(), &page_of(ps, 3))
        .unwrap();
    sink.truncate(4, ps).unwrap();
    let state = BTreeMap::from([(1, 0_u8), (2, 0), (3, 3), (4, 0)]);
    assert_eq!(sink.checksum(4, ps).unwrap(), checksum(ps, &state));
}

#[test]
fn nothing_is_published_unless_every_file_verified() {
    let store = four_file_replica();
    let mut bytes = Spec::plain(512, 4, 4, 4, &[(4, 7)]).encode();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    put(&store, 0, 4, 4, &bytes);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let (at, fault) = file_fault(result);
    assert_eq!(at, key(0, 4, 4));
    assert!(matches!(
        fault,
        FileFault::Decode(DecodeError::FileChecksum { .. })
    ));
    assert_eq!(sink.published(), None);
}

#[test]
fn a_file_cut_short_is_truncated_not_a_shorter_database() {
    let store = four_file_replica();
    let bytes = Spec::plain(512, 4, 4, 4, &[(4, 7)]).encode();
    put(&store, 0, 4, 4, &bytes[..bytes.len() - 3]);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let (_, fault) = file_fault(result);
    assert!(matches!(
        fault,
        FileFault::Decode(DecodeError::Truncated {
            section: Section::Trailer
        })
    ));
    assert_eq!(sink.published(), None);
}

#[test]
fn bytes_after_the_trailer_are_rejected() {
    let store = four_file_replica();
    let mut bytes = Spec::plain(512, 4, 4, 4, &[(4, 7)]).encode();
    bytes.push(0);
    put(&store, 0, 4, 4, &bytes);
    let (result, _) = run(&store, RestoreTarget::Latest);
    let (_, fault) = file_fault(result);
    assert!(matches!(
        fault,
        FileFault::Decode(DecodeError::TrailingBytes)
    ));
}

#[test]
fn a_header_range_that_disagrees_with_the_name_is_rejected() {
    let store = MemStore::new();
    put_spec(&store, 9, &Spec::plain(512, 1, 1, 1, &[(1, 1)]));
    put(
        &store,
        0,
        2,
        2,
        &Spec::plain(512, 1, 2, 3, &[(1, 2)]).encode(),
    );
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let (at, fault) = file_fault(result);
    assert_eq!(at, key(0, 2, 2));
    assert!(matches!(fault, FileFault::RangeMismatch { header } if header == range(2, 3)));
    assert_eq!(sink.published(), None);
}

#[test]
fn a_level_nine_file_that_is_not_a_snapshot_is_rejected() {
    let store = MemStore::new();
    put(
        &store,
        9,
        1,
        2,
        &Spec::plain(512, 1, 2, 2, &[(1, 1)]).encode(),
    );
    let (result, _) = run(&store, RestoreTarget::Latest);
    let (at, fault) = file_fault(result);
    assert_eq!(at, key(9, 1, 2));
    assert!(matches!(fault, FileFault::NotASnapshot));
}

#[test]
fn a_page_size_change_mid_chain_is_rejected_before_any_of_its_pages() {
    let store = MemStore::new();
    put_spec(&store, 9, &Spec::plain(512, 1, 1, 1, &[(1, 1)]));
    put_spec(&store, 0, &Spec::plain(1024, 1, 2, 2, &[(1, 2)]));
    let sink = MemSink::new();
    let result = block_on(restore(&store, RestoreTarget::Latest, sink.clone()));
    let (at, fault) = file_fault(result);
    assert_eq!(at, key(0, 2, 2));
    assert!(matches!(
        fault,
        FileFault::PageSizeChanged { expected, found } if expected == size(512) && found == size(1024)
    ));
    // The snapshot's one page was written; none of the second file's.
    assert_eq!(sink.calls(SinkOp::WritePage), 1);
}

#[test]
fn a_chain_that_ends_in_zero_pages_is_rejected() {
    let store = MemStore::new();
    put_spec(&store, 9, &Spec::plain(512, 0, 1, 1, &[]));
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let (at, fault) = file_fault(result);
    assert_eq!(at, key(9, 1, 1));
    assert!(matches!(fault, FileFault::EmptyDatabase));
    assert_eq!(sink.published(), None);
}

#[test]
fn a_replica_with_no_files_has_no_plan() {
    let (result, sink) = run(&MemStore::new(), RestoreTarget::Latest);
    assert!(matches!(
        result,
        Err(RestoreError::NoPlan(ref no)) if *no.outcome() == PlanOutcome::EmptyReplica
    ));
    assert_eq!(sink.published(), None);
}

#[test]
fn a_gap_is_an_error_never_the_pre_gap_state() {
    let store = MemStore::new();
    put_spec(&store, 9, &Spec::plain(512, 1, 1, 1, &[(1, 1)]));
    put_spec(&store, 0, &Spec::plain(512, 1, 3, 3, &[(1, 3)]));
    let (result, sink) = run(&store, RestoreTarget::Latest);
    assert!(matches!(
        result,
        Err(RestoreError::NoPlan(ref no))
            if *no.outcome() == (PlanOutcome::Gap { have_through: txid(1), next_starts: txid(3) })
    ));
    assert_eq!(sink.published(), None);
}

#[test]
fn a_failed_listing_is_an_inventory_error() {
    let store = four_file_replica();
    let fault = StoreError::Transient {
        op: StoreOp::List,
        kind: io::ErrorKind::TimedOut,
    };
    store.inject_failure(StoreOp::List, fault.clone());
    let (result, _) = run(&store, RestoreTarget::Latest);
    assert!(matches!(
        result,
        Err(RestoreError::Inventory(InventoryError::Store(err))) if err == fault
    ));
}

#[test]
fn a_store_error_other_than_not_found_ends_the_restore_without_retry() {
    let store = four_file_replica();
    let fault = StoreError::Throttled {
        op: StoreOp::Read,
        retry_after: Some(Duration::from_secs(1)),
    };
    store.inject_failure(StoreOp::Read, fault.clone());
    let sink = MemSink::new();
    let result = block_on(restore(&store, RestoreTarget::Latest, sink.clone()));
    assert!(matches!(result, Err(RestoreError::Store(err)) if err == fault));
    assert_eq!(sink.calls(SinkOp::Reset), 0);
    assert_eq!(sink.published(), None);
}

/// A store whose files can vanish at the moment they are read, lie about
/// their size, and whose reads are counted.
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

    fn vanishing(inner: MemStore, keys: &[FileKey]) -> Self {
        let wrapped = Self::new(inner);
        wrapped.vanish.lock().unwrap().extend(keys.iter().copied());
        wrapped
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
fn a_file_deleted_between_plan_and_read_is_replanned_once() {
    let inner = four_file_replica();
    // A merged file the first plan prefers; when it vanishes the L0 files serve.
    put_spec(&inner, 1, &Spec::plain(512, 2, 2, 3, &[(1, 5), (2, 9)]));
    let store = Wrapped::vanishing(inner, &[key(1, 2, 3)]);
    let (result, sink) = run(&store, RestoreTarget::Txid(txid(3)));
    let restored = result.unwrap();
    assert_eq!(restored.attempts(), 2);
    assert_eq!(restored.files(), 3);
    assert_eq!(sink.calls(SinkOp::Reset), 1);
    assert_eq!(seeds(&sink.published().unwrap()), [5, 9]);
}

fn three_snapshots() -> MemStore {
    let store = MemStore::new();
    for max in 1..=3 {
        put_spec(&store, 9, &Spec::plain(512, 1, 1, max, &[(1, 1)]));
    }
    store
}

#[test]
fn files_vanishing_on_every_attempt_are_unstable() {
    let store = Wrapped::vanishing(
        three_snapshots(),
        &[key(9, 1, 1), key(9, 1, 2), key(9, 1, 3)],
    );
    let (result, sink) = run(&store, RestoreTarget::Latest);
    assert!(matches!(
        result,
        Err(RestoreError::Unstable { attempts: 3, missing }) if missing == key(9, 1, 1)
    ));
    assert_eq!(sink.published(), None);
    assert_eq!(sink.calls(SinkOp::Reset), 2);
}

#[test]
fn a_listed_size_is_never_trusted() {
    let mut store = Wrapped::new(four_file_replica());
    store.zero_sizes = true;
    let (result, sink) = run(&store, RestoreTarget::Latest);
    result.unwrap();
    assert_eq!(seeds(&sink.published().unwrap()), [5, 9, 0, 7]);
}

fn xorshift(state: &mut u64) -> u8 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    u8::try_from(*state >> 56).unwrap()
}

#[test]
fn a_file_larger_than_one_chunk_is_read_in_several_reads() {
    let ps = size(65536);
    let store = MemStore::new();
    let mut rng = 0x9e37_79b9_7f4a_7c15_u64;
    let mut encoder = {
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size: ps,
            commit: 70,
            txids: range(1, 1),
            timestamp: Timestamp::from_unix_millis(0),
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        Encoder::new(Vec::new(), header).unwrap()
    };
    let mut want = Vec::new();
    for pgno in 1..=70 {
        let page: Vec<u8> = (0..ps.bytes()).map(|_| xorshift(&mut rng)).collect();
        encoder
            .write_page(PageNo::new(pgno, ps).unwrap(), &page)
            .unwrap();
        want.push(page);
    }
    let bytes = encoder.finish(None).unwrap().0;
    assert!(
        bytes.len() > 4 * 1024 * 1024,
        "incompressible pages exceed one chunk"
    );
    put(&store, 9, 1, 1, &bytes);
    let store = Wrapped::new(store);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    let restored = result.unwrap();
    // Two full chunks, then the read that returns nothing.
    assert_eq!(store.reads.load(Ordering::Relaxed), 3);
    assert_eq!(restored.bytes_read(), u64::try_from(bytes.len()).unwrap());
    let db = sink.published().unwrap();
    for (index, page) in want.iter().enumerate() {
        let pgno = u32::try_from(index + 1).unwrap();
        assert_eq!(db.page(pgno).as_ref(), Some(page));
    }
}

#[test]
fn the_read_chunk_is_a_valid_read_len() {
    assert_eq!(chunk_len().get(), u64::from(CHUNK_BYTES));
}

#[test]
fn a_sink_failing_at_each_operation_never_publishes() {
    let cases = [
        (SinkOp::WritePage, 0),
        (SinkOp::WritePage, 5),
        (SinkOp::Truncate, 0),
        (SinkOp::Truncate, 3),
        (SinkOp::Publish, 0),
    ];
    for (op, nth) in cases {
        let store = four_file_replica();
        let sink = MemSink::new();
        sink.fail_at(op, nth);
        let result = block_on(restore(&store, RestoreTarget::Latest, sink.clone()));
        assert!(
            matches!(&result, Err(RestoreError::Sink { op: got, .. }) if *got == op),
            "{op:?} {nth}: {result:?}"
        );
        assert_eq!(sink.published(), None, "{op:?} {nth}");
    }
}

#[test]
fn a_sink_that_cannot_reset_ends_the_restore() {
    let inner = four_file_replica();
    put_spec(&inner, 1, &Spec::plain(512, 2, 2, 3, &[(1, 5), (2, 9)]));
    let store = Wrapped::vanishing(inner, &[key(1, 2, 3)]);
    let sink = MemSink::new();
    sink.fail_at(SinkOp::Reset, 0);
    let result = block_on(restore(&store, RestoreTarget::Txid(txid(3)), sink.clone()));
    assert!(matches!(
        result,
        Err(RestoreError::Sink {
            op: SinkOp::Reset,
            ..
        })
    ));
    assert_eq!(sink.published(), None);
}

/// The one slow test (seconds): a snapshot must hold every page, so crossing
/// the lock page means encoding and decoding a gigabyte of 512-byte pages.
/// Page size 512 puts the lock page at 2,097,153: a database of 2,097,154
/// pages crosses it, and the snapshot holds every page but that one.
#[test]
fn the_lock_page_is_never_written_and_reads_as_zeros() {
    let ps = size(512);
    let lock = lock_page(ps);
    assert_eq!(lock, 2_097_153);
    let commit = lock + 1;
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps,
        commit,
        txids: range(1, 1),
        timestamp: Timestamp::from_unix_millis(0),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let (zeros, sevens) = (page_of(ps, 0), page_of(ps, 7));
    for pgno in (1..=commit).filter(|p| *p != lock) {
        let page = if pgno + 1 >= lock { &sevens } else { &zeros };
        encoder
            .write_page(PageNo::new(pgno, ps).unwrap(), page)
            .unwrap();
    }
    let store = MemStore::new();
    put(&store, 9, 1, 1, &encoder.finish(None).unwrap().0);
    let (result, sink) = run(&store, RestoreTarget::Latest);
    assert_eq!(result.unwrap().commit(), commit);
    let db = sink.published().unwrap();
    assert_eq!(db.page(lock), Some(vec![0; 512]));
    assert_eq!(db.page(lock - 1), Some(vec![7; 512]));
    assert_eq!(db.page(commit), Some(vec![7; 512]));
    assert_eq!(db.page(1), Some(vec![0; 512]));
}

// ---- the model oracle ----

/// A history of transactions and the database after each.
struct Model {
    ps: PageSize,
    /// The database after transaction i + 1.
    states: Vec<BTreeMap<u32, u8>>,
    commits: Vec<u32>,
    /// The pages transaction i + 1 wrote.
    written: Vec<BTreeSet<u32>>,
}

const MAX_PAGES: u32 = 24;

type TxDraft = (i8, Vec<(u8, u8)>);

fn model_from(ps: PageSize, drafts: &[TxDraft]) -> Model {
    let mut model = Model {
        ps,
        states: Vec::new(),
        commits: Vec::new(),
        written: Vec::new(),
    };
    let mut state = BTreeMap::new();
    let mut commit = 0_u32;
    for (delta, writes) in drafts {
        let next =
            u32::try_from((i64::from(commit) + i64::from(*delta)).clamp(1, i64::from(MAX_PAGES)))
                .unwrap();
        let mut wrote: BTreeSet<u32> = (commit + 1..=next).collect();
        for (sel, seed) in writes {
            let pgno = 1 + u32::from(*sel) % next;
            wrote.insert(pgno);
            state.insert(pgno, *seed);
        }
        for pgno in commit + 1..=next {
            state.entry(pgno).or_insert(0);
        }
        state.retain(|pgno, _| *pgno <= next);
        commit = next;
        model.states.push(state.clone());
        model.commits.push(commit);
        model.written.push(wrote);
    }
    model
}

impl Model {
    /// The state after transaction n (1-based).
    fn at(&self, n: usize) -> &BTreeMap<u32, u8> {
        &self.states[n - 1]
    }

    fn commit(&self, n: usize) -> u32 {
        self.commits[n - 1]
    }

    /// Writes the compacted file for transactions min..=max the way Go does:
    /// the newest version of every page they touched, or every page when the
    /// file starts at transaction 1.
    fn spec(&self, min: usize, max: usize, tracked: bool) -> Spec {
        let state = self.at(max);
        let commit = self.commit(max);
        let pages: Vec<(u32, u8)> = if min == 1 {
            state.iter().map(|(p, s)| (*p, *s)).collect()
        } else {
            let touched: BTreeSet<u32> = (min..=max)
                .flat_map(|tx| self.written[tx - 1].iter().copied())
                .filter(|p| *p <= commit)
                .collect();
            touched.iter().map(|p| (*p, state[p])).collect()
        };
        Spec {
            tracked,
            ps: self.ps,
            commit,
            min: u64::try_from(min).unwrap(),
            max: u64::try_from(max).unwrap(),
            pre: (tracked && min > 1).then(|| checksum(self.ps, self.at(min - 1))),
            post: tracked.then(|| checksum(self.ps, state)),
            pages,
        }
    }
}

/// Lays the history out as Go does: L0 per transaction, merged ranges at
/// levels 1 to 8, snapshots at level 9.
fn compact(
    model: &Model,
    tracked: bool,
    merges: &[(usize, usize, u8)],
    snaps: &[usize],
) -> MemStore {
    let n = model.states.len();
    let store = MemStore::new();
    for tx in 1..=n {
        put_spec(&store, 0, &model.spec(tx, tx, tracked));
    }
    let mut merged = BTreeSet::new();
    for (x, y, level) in merges {
        let (a, b) = ((x % n) + 1, (y % n) + 1);
        let (a, b) = (a.min(b), a.max(b));
        if merged.insert((*level, a, b)) {
            put_spec(&store, *level, &model.spec(a, b, tracked));
        }
    }
    for tx in std::iter::once(&0).chain(snaps) {
        let b = (tx % n) + 1;
        put_spec(&store, 9, &model.spec(1, b, tracked));
    }
    store
}

fn assert_matches_model(model: &Model, n: usize, db: &Database) {
    assert_eq!(db.page_count(), model.commit(n), "length at {n}");
    assert_eq!(db.page_size(), model.ps);
    for pgno in 1..=model.commit(n) {
        let want = page_of(model.ps, model.at(n).get(&pgno).copied().unwrap_or(0));
        assert_eq!(db.page(pgno), Some(want), "page {pgno} at {n}");
    }
}

fn drafts() -> impl Strategy<Value = Vec<TxDraft>> {
    prop::collection::vec(
        (
            -8_i8..=8,
            prop::collection::vec((any::<u8>(), any::<u8>()), 0..5),
        ),
        1..10,
    )
}

/// The store of [compact], with one page of one file rewritten to other bytes
/// while the file keeps its own valid file checksum and its claimed pre and
/// post-apply checksums.
fn corrupt_one_page(
    model: &Model,
    merges: &[(usize, usize, u8)],
    snaps: &[usize],
    pick: usize,
) -> Option<MemStore> {
    let n = model.states.len();
    let mut files: Vec<(u8, Spec)> = (1..=n).map(|tx| (0, model.spec(tx, tx, true))).collect();
    let mut seen = BTreeSet::new();
    for (x, y, level) in merges {
        let (a, b) = ((x % n) + 1, (y % n) + 1);
        let (a, b) = (a.min(b), a.max(b));
        if seen.insert((*level, a, b)) {
            files.push((*level, model.spec(a, b, true)));
        }
    }
    for tx in std::iter::once(&0).chain(snaps) {
        files.push((9, model.spec(1, (tx % n) + 1, true)));
    }
    // A file that starts at transaction 1 is a snapshot, and the encoder
    // itself refuses a snapshot whose post-apply is not its own pages'.
    let victims: Vec<usize> = (0..files.len())
        .filter(|i| files[*i].1.pre.is_some() && !files[*i].1.pages.is_empty())
        .collect();
    let victim = *victims.get(pick % victims.len().max(1))?;
    let store = MemStore::new();
    for (index, (level, spec)) in files.iter_mut().enumerate() {
        if index == victim {
            let slot = pick % spec.pages.len();
            let seed = &mut spec.pages[slot].1;
            *seed = seed.wrapping_add(1);
        }
        put_spec(&store, *level, spec);
    }
    Some(store)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn a_swapped_page_in_any_tracked_file_never_publishes_a_wrong_database(
        drafts in drafts(),
        ps_pick in 0_usize..3,
        merges in prop::collection::vec((0_usize..10, 0_usize..10, 1_u8..=8), 0..6),
        snaps in prop::collection::vec(0_usize..10, 0..3),
        pick in 0_usize..64,
    ) {
        let ps = size([512, 1024, 4096][ps_pick]);
        let model = model_from(ps, &drafts);
        let Some(store) = corrupt_one_page(&model, &merges, &snaps, pick) else {
            return Err(TestCaseError::reject("every file is a snapshot"));
        };
        let (result, sink) = run(&store, RestoreTarget::Latest);
        match result {
            Ok(_) => assert_matches_model(&model, model.states.len(), &sink.published().unwrap()),
            Err(_) => prop_assert_eq!(sink.published(), None),
        }
    }

    #[test]
    fn restore_equals_the_model_at_every_transaction(
        drafts in drafts(),
        ps_pick in 0_usize..3,
        tracked in any::<bool>(),
        merges in prop::collection::vec((0_usize..10, 0_usize..10, 1_u8..=8), 0..6),
        snaps in prop::collection::vec(0_usize..10, 0..3),
    ) {
        let ps = size([512, 1024, 4096][ps_pick]);
        let model = model_from(ps, &drafts);
        let store = compact(&model, tracked, &merges, &snaps);
        for n in 1..=model.states.len() {
            let (result, sink) = run(&store, RestoreTarget::Txid(txid(u64::try_from(n).unwrap())));
            let restored = result.unwrap();
            prop_assert_eq!(restored.reached(), txid(u64::try_from(n).unwrap()));
            assert_matches_model(&model, n, &sink.published().unwrap());
        }
        let (result, sink) = run(&store, RestoreTarget::Latest);
        result.unwrap();
        assert_matches_model(&model, model.states.len(), &sink.published().unwrap());
    }
}

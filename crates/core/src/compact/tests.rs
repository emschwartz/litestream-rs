//! Compactor tests: hand-built chains for each semantic and each fault, then
//! properties over chains shaped the way SQLite writes them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_ltx::{
    DbChecksum, Encoder, HeaderFields, PageChecksum, PageNo, Section, Timestamp, Txid, TxidRange,
    WalOrigin, lock_page,
};
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::layout::CreatedAt;
use crate::layout::ListedTime;
use crate::mem::{MemStore, MemUpload};
use crate::plan::RestoreTarget;
use crate::restore::{Database, MemSink, restore};
use crate::store::{Listing, PageLimit, StoreOp};

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn range(min: u64, max: u64) -> TxidRange {
    TxidRange::new(txid(min), txid(max)).unwrap()
}

fn level(n: u8) -> Level {
    Level::new(n).unwrap()
}

fn key(l: u8, min: u64, max: u64) -> FileKey {
    FileKey::new(level(l), range(min, max))
}

fn size(bytes: u32) -> PageSize {
    PageSize::new(bytes).unwrap()
}

fn solid(ps: PageSize, seed: u8) -> Vec<u8> {
    vec![seed; ps.bytes()]
}

/// Incompressible bytes, so a file is as big as its pages.
fn noisy(ps: PageSize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..ps.bytes())
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_be_bytes()[0]
        })
        .collect()
}

fn checksum(ps: PageSize, state: &BTreeMap<u32, u8>) -> DbChecksum {
    state.iter().fold(DbChecksum::EMPTY, |acc, (pgno, seed)| {
        acc.toggle(PageChecksum::of(
            PageNo::new(*pgno, ps).unwrap(),
            &solid(ps, *seed),
        ))
    })
}

/// One LTX file to encode.
#[derive(Clone)]
struct Spec {
    tracked: bool,
    ps: PageSize,
    commit: u32,
    min: u64,
    max: u64,
    pre: Option<DbChecksum>,
    post: Option<DbChecksum>,
    node_id: u64,
    wal: Option<WalOrigin>,
    pages: Vec<(u32, Vec<u8>)>,
}

impl Spec {
    fn plain(ps: u32, commit: u32, min: u64, max: u64, pages: &[(u32, u8)]) -> Self {
        let ps = size(ps);
        Self {
            tracked: false,
            ps,
            commit,
            min,
            max,
            pre: None,
            post: None,
            node_id: 0,
            wal: None,
            pages: pages.iter().map(|(p, s)| (*p, solid(ps, *s))).collect(),
        }
    }

    fn timestamp(&self) -> Timestamp {
        Timestamp::from_unix_millis(i64::try_from(self.max).unwrap() * 1000)
    }

    fn header(&self) -> Header {
        Header::new(HeaderFields {
            tracked: self.tracked,
            page_size: self.ps,
            commit: self.commit,
            txids: range(self.min, self.max),
            timestamp: self.timestamp(),
            pre_apply: self.pre,
            wal: self.wal,
            node_id: self.node_id,
        })
        .unwrap()
    }

    fn encode(&self) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new(), self.header()).unwrap();
        for (pgno, data) in &self.pages {
            encoder
                .write_page(PageNo::new(*pgno, self.ps).unwrap(), data)
                .unwrap();
        }
        encoder.finish(self.post).unwrap().0
    }
}

fn put_bytes(store: &impl ObjectStore, at: FileKey, created: Timestamp, bytes: &[u8]) -> FileInfo {
    block_on(async {
        let mut upload = store
            .begin_upload(&at, created, WriteMode::Replace)
            .await
            .unwrap();
        upload.write(bytes).await.unwrap();
        upload.commit().await.unwrap()
    })
}

/// Stores a spec under the key its own range names.
fn put(store: &impl ObjectStore, l: u8, spec: &Spec) -> FileInfo {
    put_bytes(
        store,
        key(l, spec.min, spec.max),
        spec.timestamp(),
        &spec.encode(),
    )
}

fn put_all(store: &impl ObjectStore, l: u8, specs: &[Spec]) -> Vec<FileInfo> {
    specs.iter().map(|spec| put(store, l, spec)).collect()
}

fn inputs(infos: &[FileInfo]) -> CompactionInputs {
    CompactionInputs::new(infos.to_vec()).unwrap()
}

fn compact_infos<S: ObjectStore, D: ObjectStore>(
    source: &S,
    dest: &D,
    infos: &[FileInfo],
    l: u8,
) -> Result<Compacted, CompactError> {
    block_on(compact(source, dest, &inputs(infos), level(l)))
}

fn read_all(store: &impl ObjectStore, at: &FileKey) -> Vec<u8> {
    let len = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let mut out = Vec::new();
    loop {
        let chunk = block_on(store.read(at, u64::try_from(out.len()).unwrap(), len)).unwrap();
        if chunk.is_empty() {
            return out;
        }
        out.extend_from_slice(&chunk);
    }
}

type Pages = Vec<(u32, Vec<u8>)>;

fn decode_all(bytes: &[u8]) -> Result<(Verified, Pages), DecodeError> {
    let mut decoder = lsr_ltx::Decoder::new();
    let mut pages = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (used, event) = decoder.feed(rest)?;
        if let Some(Event::Page(page)) = event {
            let (pgno, data) = (page.pgno(), page.data());
            pages.push((pgno.get(), data.to_vec()));
        }
        rest = &rest[used..];
    }
    Ok((decoder.finish()?, pages))
}

fn list(store: &impl ObjectStore, l: u8) -> Vec<FileInfo> {
    block_on(store.list(level(l), None, PageLimit::MAX))
        .unwrap()
        .files
}

fn nothing_published(store: &impl ObjectStore) -> bool {
    (0..=9).all(|l| list(store, l).is_empty())
}

fn error_of(result: Result<Compacted, CompactError>) -> CompactError {
    result.expect_err("the compaction should have failed")
}

fn input_fault(err: CompactError) -> (FileKey, InputFault) {
    match err {
        CompactError::Input { key, fault } => (key, fault),
        CompactError::InputVanished { .. }
        | CompactError::Source(_)
        | CompactError::Dest(_)
        | CompactError::Encode(_)
        | CompactError::SnapshotLevel { .. }
        | CompactError::Inputs(_) => panic!("expected an input fault, got {err:?}"),
    }
}

// A store that counts calls and fails the ones a test names; its uploads are
// counted so a test can prove none is left open.

#[derive(Default)]
struct Script {
    reads: usize,
    fail_read: Option<(usize, StoreError)>,
    begins: usize,
    fail_begin: Option<StoreError>,
    writes: usize,
    fail_write: Option<(usize, StoreError)>,
    fail_commit: Option<StoreError>,
    abort_error: Option<StoreError>,
    commits: usize,
    aborts: usize,
}

#[derive(Clone, Default)]
struct Spy {
    inner: MemStore,
    script: Arc<Mutex<Script>>,
}

impl Spy {
    fn script(&self) -> MutexGuard<'_, Script> {
        self.script.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Uploads begun and neither committed nor aborted.
    fn open_uploads(&self) -> usize {
        let s = self.script();
        s.begins - s.commits - s.aborts
    }
}

struct SpyUpload {
    inner: MemUpload,
    script: Arc<Mutex<Script>>,
}

impl SpyUpload {
    fn script(&self) -> MutexGuard<'_, Script> {
        self.script.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl ObjectStore for Spy {
    type Upload = SpyUpload;

    fn list(
        &self,
        l: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
        self.inner.list(l, after, limit)
    }

    fn read(
        &self,
        at: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        let mut script = self.script();
        let nth = script.reads;
        script.reads += 1;
        let result = match script.fail_read.take() {
            Some((n, err)) if n == nth => Err(err),
            other => {
                script.fail_read = other;
                block_on(self.inner.read(at, offset, len))
            }
        };
        std::future::ready(result)
    }

    fn created_at(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<CreatedAt, StoreError>> + Send {
        self.inner.created_at(file)
    }

    fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, CreatedAt), StoreError>> + Send {
        self.inner.read_with_created_at(file, len)
    }

    fn begin_upload(
        &self,
        at: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> impl Future<Output = Result<SpyUpload, StoreError>> + Send {
        let mut script = self.script();
        let result = match script.fail_begin.take() {
            Some(err) => Err(err),
            None => block_on(self.inner.begin_upload(at, created_at, mode)).map(|inner| {
                script.begins += 1;
                SpyUpload {
                    inner,
                    script: Arc::clone(&self.script),
                }
            }),
        };
        std::future::ready(result)
    }

    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.inner.delete(keys)
    }
}

impl Upload for SpyUpload {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send {
        let mut script = self.script();
        let nth = script.writes;
        script.writes += 1;
        let injected = match script.fail_write.take() {
            Some((n, err)) if n == nth => Some(err),
            other => {
                script.fail_write = other;
                None
            }
        };
        drop(script);
        let result = match injected {
            Some(err) => Err(err),
            None => block_on(self.inner.write(chunk)),
        };
        std::future::ready(result)
    }

    fn commit(self) -> impl Future<Output = Result<FileInfo, StoreError>> + Send {
        let mut script = self.script();
        script.commits += 1;
        let injected = script.fail_commit.take();
        drop(script);
        let result = match injected {
            Some(err) => Err(err),
            None => block_on(self.inner.commit()),
        };
        std::future::ready(result)
    }

    fn abort(self) -> impl Future<Output = Result<(), StoreError>> + Send {
        let mut script = self.script();
        script.aborts += 1;
        let injected = script.abort_error.take();
        drop(script);
        let result = match injected {
            Some(err) => Err(err),
            None => block_on(self.inner.abort()),
        };
        std::future::ready(result)
    }
}

fn transient(op: StoreOp) -> StoreError {
    StoreError::Transient {
        op,
        kind: std::io::ErrorKind::TimedOut,
    }
}

// Inputs.

fn info(l: u8, min: u64, max: u64) -> FileInfo {
    FileInfo {
        key: key(l, min, max),
        size: 1,
        listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(0))),
    }
}

#[test]
fn inputs_must_not_be_empty() {
    assert_eq!(CompactionInputs::new(Vec::new()), Err(InputsError::Empty));
}

#[test]
fn inputs_are_capped_at_the_fan_in() {
    let files = |n: u64| (1..=n).map(|i| info(0, i, i)).collect::<Vec<_>>();
    let ok = CompactionInputs::new(files(32)).unwrap();
    assert_eq!(ok.files().len(), MAX_INPUTS);
    assert_eq!(ok.range(), range(1, 32));
    assert_eq!(
        CompactionInputs::new(files(33)),
        Err(InputsError::TooMany { found: 33 })
    );
}

#[test]
fn inputs_must_start_right_after_the_previous_one_ends() {
    let gap = CompactionInputs::new(vec![info(0, 1, 1), info(0, 3, 3)]);
    assert_eq!(
        gap,
        Err(InputsError::NotContiguous {
            after: key(0, 1, 1),
            next: key(0, 3, 3)
        })
    );
    // Go's IsContiguous lets inputs overlap; this does not.
    let overlap = CompactionInputs::new(vec![info(0, 1, 2), info(0, 2, 3)]);
    assert!(matches!(overlap, Err(InputsError::NotContiguous { .. })));
    let descending = CompactionInputs::new(vec![info(0, 2, 2), info(0, 1, 1)]);
    assert!(matches!(descending, Err(InputsError::NotContiguous { .. })));
    let mixed = CompactionInputs::new(vec![info(1, 1, 4), info(0, 5, 5)]).unwrap();
    assert_eq!(mixed.range(), range(1, 5));
}

// Semantics.

#[test]
fn one_input_keeps_its_pages_and_header_fields_minus_node_id_and_wal_origin() {
    let mut spec = Spec::plain(512, 3, 5, 5, &[(2, 9), (3, 4)]);
    spec.node_id = 7;
    spec.wal = WalOrigin::new(32, 4120, 1, 2);
    let source = MemStore::new();
    let infos = put_all(&source, 0, std::slice::from_ref(&spec));
    let dest = MemStore::new();
    let done = compact_infos(&source, &dest, &infos, 1).unwrap();

    let mut want = spec.header().fields();
    want.node_id = 0;
    want.wal = None;
    assert_eq!(done.header().fields(), want);
    assert_eq!(done.info().key, key(1, 5, 5));
    assert_eq!(done.pages(), 2);
    assert_eq!(done.info().created_at().unwrap().at(), spec.timestamp());
    let (verified, pages) = decode_all(&read_all(&dest, &done.info().key)).unwrap();
    assert_eq!(verified.header(), done.header());
    assert_eq!(verified.trailer(), done.trailer());
    assert_eq!(pages, spec.pages);
    assert_eq!(done.bytes_read(), infos[0].size);
}

#[test]
fn the_output_header_takes_the_first_and_last_inputs_as_go_does() {
    let source = MemStore::new();
    let specs = [
        Spec::plain(512, 3, 1, 1, &[(1, 1), (2, 2), (3, 3)]),
        Spec::plain(512, 4, 2, 2, &[(2, 5), (4, 4)]),
        Spec::plain(512, 2, 3, 4, &[(1, 6)]),
    ];
    let infos = put_all(&source, 0, &specs);
    let dest = MemStore::new();
    let done = compact_infos(&source, &dest, &infos, 1).unwrap();
    let h = done.header().fields();
    assert_eq!(
        (h.tracked, h.page_size, h.commit, h.txids, h.node_id, h.wal),
        (false, size(512), 2, range(1, 4), 0, None)
    );
    assert_eq!(h.timestamp, specs[2].timestamp());
    assert_eq!(h.pre_apply, None);
    // Pages 3 and 4 are above the final commit: dropped, not refused. Page 2's
    // newest copy is in the second input, which no later input shrank below.
    let (_, pages) = decode_all(&read_all(&dest, &done.info().key)).unwrap();
    assert_eq!(pages, [(1, solid(size(512), 6)), (2, solid(size(512), 5))]);
}

#[test]
fn a_snapshot_start_makes_a_snapshot_that_holds_every_page() {
    let source = MemStore::new();
    let specs = [
        Spec::plain(512, 2, 1, 1, &[(1, 1), (2, 2)]),
        Spec::plain(512, 3, 2, 2, &[(3, 3)]),
    ];
    let infos = put_all(&source, 0, &specs);
    let dest = MemStore::new();
    let done = compact_infos(&source, &dest, &infos, 9).unwrap();
    assert!(done.header().is_snapshot());
    assert_eq!(done.pages(), 3);
    assert_eq!(done.info().key, key(9, 1, 2));
}

#[test]
fn a_growth_that_skips_a_page_is_refused_and_never_published() {
    let source = MemStore::new();
    let specs = [
        Spec::plain(512, 3, 1, 1, &[(1, 1), (2, 2), (3, 3)]),
        // Growth to 5 that wrote only page 5: SQLite writes page 4 too.
        Spec::plain(512, 5, 2, 2, &[(5, 5)]),
    ];
    let infos = put_all(&source, 0, &specs);
    let dest = Spy::default();
    let (_, fault) = input_fault(error_of(compact_infos(&source, &dest, &infos, 1)));
    assert!(matches!(fault, InputFault::PageNotRewritten { pgno } if pgno.get() == 4));
    assert!(nothing_published(&dest));
    assert_eq!(dest.open_uploads(), 0);
}

#[test]
fn the_snapshot_level_takes_only_files_that_start_at_transaction_one() {
    let source = MemStore::new();
    let infos = put_all(&source, 0, &[Spec::plain(512, 2, 2, 2, &[(1, 1)])]);
    let dest = Spy::default();
    let err = error_of(compact_infos(&source, &dest, &infos, 9));
    assert!(matches!(err, CompactError::SnapshotLevel { first } if first == key(0, 2, 2)));
    assert!(nothing_published(&dest));
    assert_eq!(dest.script().begins, 0);
}

#[test]
fn the_lock_page_never_appears_and_growth_across_it_is_one_page() {
    let ps = size(512);
    let lock = lock_page(ps);
    let source = MemStore::new();
    let specs = [
        Spec::plain(512, lock - 1, 2, 2, &[(5, 1), (lock - 1, 2)]),
        // Grows across the lock page: SQLite writes the page after it only.
        Spec::plain(512, lock + 1, 3, 3, &[(lock + 1, 3)]),
    ];
    let infos = put_all(&source, 0, &specs);
    let dest = MemStore::new();
    let done = compact_infos(&source, &dest, &infos, 1).unwrap();
    assert_eq!(done.header().commit(), lock + 1);
    let (_, pages) = decode_all(&read_all(&dest, &done.info().key)).unwrap();
    let numbers: Vec<u32> = pages.iter().map(|(p, _)| *p).collect();
    assert_eq!(numbers, [5, lock - 1, lock + 1]);
}

// Chain semantics: shrinks.

fn shrink_chain(rewrite: bool) -> [Spec; 3] {
    let regrow: &[(u32, u8)] = if rewrite {
        &[(3, 7), (4, 8)]
    } else {
        &[(1, 6)]
    };
    [
        Spec::plain(512, 4, 1, 1, &[(1, 1), (2, 2), (3, 3), (4, 4)]),
        Spec::plain(512, 2, 2, 2, &[(1, 5)]),
        Spec::plain(512, 4, 3, 3, regrow),
    ]
}

#[test]
fn a_page_that_survives_a_shrink_below_it_is_refused_and_the_inputs_are_left_alone() {
    let source = MemStore::new();
    let infos = put_all(&source, 0, &shrink_chain(false));
    let dest = Spy::default();
    let err = error_of(compact_infos(&source, &dest, &infos, 1));
    let (at, fault) = input_fault(err);
    assert_eq!(at, key(0, 1, 1));
    assert!(matches!(fault, InputFault::PageSurvivesShrink { pgno } if pgno.get() == 3));
    assert!(nothing_published(&dest));
    assert_eq!(dest.open_uploads(), 0);
    assert_eq!(list(&source, 0).len(), 3);
}

#[test]
fn a_shrink_followed_by_the_growth_sqlite_writes_is_merged() {
    let source = MemStore::new();
    let infos = put_all(&source, 0, &shrink_chain(true));
    let dest = MemStore::new();
    let done = compact_infos(&source, &dest, &infos, 9).unwrap();
    let (_, pages) = decode_all(&read_all(&dest, &done.info().key)).unwrap();
    let seeds: Vec<u8> = pages.iter().map(|(_, d)| d[0]).collect();
    assert_eq!(seeds, [5, 2, 7, 8]);
}

// Header faults.

#[test]
fn an_input_whose_header_names_other_transactions_is_a_range_mismatch() {
    let source = MemStore::new();
    let spec = Spec::plain(512, 1, 3, 3, &[(1, 1)]);
    let at = key(0, 2, 2);
    let infos = vec![put_bytes(&source, at, spec.timestamp(), &spec.encode())];
    let dest = Spy::default();
    let (found, fault) = input_fault(error_of(compact_infos(&source, &dest, &infos, 1)));
    assert_eq!(found, at);
    assert!(matches!(fault, InputFault::RangeMismatch { header } if header == range(3, 3)));
    assert!(nothing_published(&dest));
    assert_eq!(dest.script().begins, 0);
}

#[test]
fn inputs_of_two_page_sizes_are_refused_before_anything_is_uploaded() {
    let source = MemStore::new();
    let specs = [
        Spec::plain(512, 1, 1, 1, &[(1, 1)]),
        Spec::plain(1024, 1, 2, 2, &[(1, 2)]),
    ];
    let infos = put_all(&source, 0, &specs);
    let dest = Spy::default();
    let (found, fault) = input_fault(error_of(compact_infos(&source, &dest, &infos, 1)));
    assert_eq!(found, key(0, 2, 2));
    assert!(matches!(
        fault,
        InputFault::PageSizeChanged { expected, found }
            if expected == size(512) && found == size(1024)
    ));
    assert_eq!(dest.script().begins, 0);
}

// Tracked chains.

fn tracked_chain(ps: u32) -> ([Spec; 3], Vec<BTreeMap<u32, u8>>) {
    let size = size(ps);
    let s1 = BTreeMap::from([(1, 1_u8), (2, 2)]);
    let s2 = BTreeMap::from([(1, 1_u8), (2, 8)]);
    let s3 = BTreeMap::from([(1, 9_u8), (2, 8), (3, 3)]);
    let mut a = Spec::plain(ps, 2, 1, 1, &[(1, 1), (2, 2)]);
    a.tracked = true;
    a.post = Some(checksum(size, &s1));
    let mut b = Spec::plain(ps, 2, 2, 2, &[(2, 8)]);
    b.tracked = true;
    b.pre = a.post;
    b.post = Some(checksum(size, &s2));
    let mut c = Spec::plain(ps, 3, 3, 3, &[(1, 9), (3, 3)]);
    c.tracked = true;
    c.pre = b.post;
    c.post = Some(checksum(size, &s3));
    ([a, b, c], vec![s1, s2, s3])
}

#[test]
fn a_tracked_chain_carries_its_checksums_and_restores_through_its_chain_check() {
    let source = MemStore::new();
    let (specs, states) = tracked_chain(512);
    let infos = put_all(&source, 0, &specs[1..]);
    let dest = MemStore::new();
    // Incremental output: pre-apply from the first input, post-apply from the last.
    let done = compact_infos(&source, &dest, &infos, 1).unwrap();
    assert!(done.header().tracked());
    assert_eq!(done.header().fields().pre_apply, specs[1].pre);
    assert_eq!(done.trailer().post_apply(), specs[2].post);

    // Snapshot output: the encoder recomputes the pages' checksum and agrees.
    let all = put_all(&source, 0, &specs[..1]);
    let infos: Vec<FileInfo> = all.into_iter().chain(infos).collect();
    let snap = compact_infos(&source, &dest, &infos, 9).unwrap();
    assert_eq!(snap.trailer().post_apply(), specs[2].post);
    assert_eq!(snap.header().fields().pre_apply, None);

    let store = MemStore::new();
    put(&store, 9, &specs[0]);
    let tail = put_bytes(
        &store,
        done.info().key,
        done.info().created_at().unwrap().at(),
        &read_all(&dest, &done.info().key),
    );
    assert_eq!(tail.key, key(1, 2, 3));
    let sink = MemSink::new();
    let restored = block_on(restore(&store, RestoreTarget::Latest, sink.clone())).unwrap();
    assert_eq!(restored.post_apply(), specs[2].post);
    assert_db(&sink.published().unwrap(), &states[2], 3, size(512));
}

#[test]
fn a_mixed_tracked_and_untracked_set_yields_an_untracked_output_with_no_checksums() {
    let (specs, _) = tracked_chain(512);
    for set in [
        [specs[0].clone(), Spec::plain(512, 2, 2, 2, &[(2, 8)])],
        [
            Spec::plain(512, 2, 1, 1, &[(1, 1), (2, 2)]),
            specs[1].clone(),
        ],
    ] {
        let source = MemStore::new();
        let infos = put_all(&source, 0, &set);
        let dest = MemStore::new();
        let done = compact_infos(&source, &dest, &infos, 1).unwrap();
        assert!(!done.header().tracked());
        assert_eq!(done.header().fields().pre_apply, None);
        assert_eq!(done.trailer().post_apply(), None);
        decode_all(&read_all(&dest, &done.info().key)).unwrap();
    }
}

#[test]
fn a_broken_checksum_link_is_refused_and_nothing_is_published() {
    let source = MemStore::new();
    let (mut specs, _) = tracked_chain(512);
    let wrong = checksum(size(512), &BTreeMap::from([(1, 7_u8)]));
    specs[1].pre = Some(wrong);
    let infos = put_all(&source, 0, &specs);
    let dest = Spy::default();
    let (at, fault) = input_fault(error_of(compact_infos(&source, &dest, &infos, 9)));
    assert_eq!(at, key(0, 2, 2));
    assert!(matches!(
        fault,
        InputFault::ChecksumChain { expected, found }
            if expected == specs[0].post.unwrap() && found == wrong
    ));
    assert!(nothing_published(&dest));
    assert_eq!(dest.open_uploads(), 0);
    assert_eq!(dest.script().aborts, 1);
}

// Faults, each at each point.

/// A four-file chain and the store holding it.
fn four(source: &MemStore) -> Vec<FileInfo> {
    let specs = [
        Spec::plain(512, 3, 1, 1, &[(1, 1), (2, 2), (3, 3)]),
        Spec::plain(512, 3, 2, 2, &[(2, 9)]),
        Spec::plain(512, 3, 3, 3, &[(1, 5), (3, 6)]),
        Spec::plain(512, 3, 4, 4, &[(2, 7)]),
    ];
    put_all(source, 0, &specs)
}

fn with_bytes(source: &MemStore, infos: &[FileInfo], at: usize, edit: impl Fn(&mut Vec<u8>)) {
    let target = infos[at];
    let mut bytes = read_all(source, &target.key);
    edit(&mut bytes);
    put_bytes(
        source,
        target.key,
        target.created_at().unwrap().at(),
        &bytes,
    );
}

fn assert_clean_failure(dest: &Spy) {
    assert!(nothing_published(&dest.inner));
    assert_eq!(dest.open_uploads(), 0);
}

#[test]
fn a_flipped_byte_in_any_input_names_that_input_and_publishes_nothing() {
    for at in 0..4 {
        for position in [Position::Middle, Position::Last] {
            let source = MemStore::new();
            let infos = four(&source);
            with_bytes(&source, &infos, at, |bytes| {
                let i = match position {
                    Position::Middle => bytes.len() / 2,
                    Position::Last => bytes.len() - 1,
                };
                bytes[i] ^= 0x40;
            });
            let dest = Spy::default();
            let (found, fault) = input_fault(error_of(compact_infos(&source, &dest, &infos, 1)));
            assert_eq!(found, infos[at].key);
            assert!(matches!(fault, InputFault::Decode(_)), "{fault:?}");
            assert_clean_failure(&dest);
        }
    }
}

#[derive(Clone, Copy)]
enum Position {
    Middle,
    Last,
}

#[test]
fn a_truncated_input_is_a_decode_error_never_fewer_pages() {
    for at in 0..4 {
        let source = MemStore::new();
        let infos = four(&source);
        with_bytes(&source, &infos, at, |bytes| bytes.truncate(bytes.len() - 3));
        let dest = Spy::default();
        let (found, fault) = input_fault(error_of(compact_infos(&source, &dest, &infos, 1)));
        assert_eq!(found, infos[at].key);
        assert!(matches!(
            fault,
            InputFault::Decode(DecodeError::Truncated {
                section: Section::Trailer
            })
        ));
        assert_clean_failure(&dest);
    }
}

#[test]
fn bytes_after_an_inputs_trailer_are_a_decode_error() {
    let source = MemStore::new();
    let infos = four(&source);
    with_bytes(&source, &infos, 2, |bytes| bytes.push(0));
    let dest = Spy::default();
    let (found, fault) = input_fault(error_of(compact_infos(&source, &dest, &infos, 1)));
    assert_eq!(found, infos[2].key);
    assert!(matches!(
        fault,
        InputFault::Decode(DecodeError::TrailingBytes)
    ));
    assert_clean_failure(&dest);
}

fn reads_of_a_clean_run() -> usize {
    let source = Spy::default();
    let infos = four(&source.inner);
    compact_infos(&source, &MemStore::new(), &infos, 1).unwrap();
    std::mem::take(&mut source.script().reads)
}

#[test]
fn an_input_that_vanishes_at_any_read_is_input_vanished() {
    let reads = reads_of_a_clean_run();
    assert!(reads >= 4, "each input is read at least once");
    for nth in 0..reads {
        let source = Spy::default();
        let infos = four(&source.inner);
        let gone = infos[nth % 4].key;
        source.script().fail_read = Some((nth, StoreError::NotFound { key: gone }));
        let dest = Spy::default();
        let err = error_of(compact_infos(&source, &dest, &infos, 1));
        assert!(
            matches!(err, CompactError::InputVanished { key } if key == gone),
            "read {nth}: {err:?}"
        );
        assert_clean_failure(&dest);
    }
}

#[test]
fn a_read_error_at_any_read_is_a_source_error_and_is_not_retried() {
    for nth in 0..reads_of_a_clean_run() {
        let source = Spy::default();
        let infos = four(&source.inner);
        source.script().fail_read = Some((nth, transient(StoreOp::Read)));
        let dest = Spy::default();
        let err = error_of(compact_infos(&source, &dest, &infos, 1));
        assert!(
            matches!(err, CompactError::Source(StoreError::Transient { .. })),
            "read {nth}: {err:?}"
        );
        assert_eq!(source.script().reads, nth + 1, "no read after the failure");
        assert_clean_failure(&dest);
    }
}

#[test]
fn a_failed_begin_is_a_destination_error_with_nothing_to_abort() {
    let source = MemStore::new();
    let infos = four(&source);
    let dest = Spy::default();
    dest.script().fail_begin = Some(transient(StoreOp::Upload));
    let err = error_of(compact_infos(&source, &dest, &infos, 1));
    assert!(matches!(
        err,
        CompactError::Dest(StoreError::Transient { .. })
    ));
    assert_eq!(dest.script().aborts, 0);
    assert_clean_failure(&dest);
}

#[test]
fn a_failed_upload_write_aborts_and_an_abort_failure_does_not_replace_the_error() {
    let source = MemStore::new();
    let infos = four(&source);
    let dest = Spy::default();
    dest.script().fail_write = Some((0, transient(StoreOp::Upload)));
    dest.script().abort_error = Some(StoreError::Denied {
        op: StoreOp::Upload,
        why: crate::store::Denied::Credentials,
    });
    let err = error_of(compact_infos(&source, &dest, &infos, 1));
    assert!(matches!(
        err,
        CompactError::Dest(StoreError::Transient {
            op: StoreOp::Upload,
            ..
        })
    ));
    assert_eq!(dest.script().aborts, 1);
    assert_clean_failure(&dest);
}

#[test]
fn a_commit_that_fails_before_publishing_leaves_the_destination_empty() {
    let source = MemStore::new();
    let infos = four(&source);
    let dest = Spy::default();
    dest.script().fail_commit = Some(transient(StoreOp::Upload));
    let err = error_of(compact_infos(&source, &dest, &infos, 1));
    assert!(matches!(err, CompactError::Dest(_)));
    assert!(nothing_published(&dest.inner));
}

#[test]
fn a_commit_that_fails_after_publishing_reports_the_error_and_the_file_is_whole() {
    let source = MemStore::new();
    let infos = four(&source);
    let dest = MemStore::new();
    dest.inject_failure_after_publish(transient(StoreOp::Upload));
    let err = error_of(compact_infos(&source, &dest, &infos, 1));
    assert!(matches!(err, CompactError::Dest(_)));
    let published = list(&dest, 1);
    assert_eq!(published.len(), 1);
    let (verified, _) = decode_all(&read_all(&dest, &published[0].key)).unwrap();
    assert_eq!(verified.header().txids(), range(1, 4));
}

#[test]
fn a_successful_compaction_aborts_nothing() {
    let source = MemStore::new();
    let infos = four(&source);
    let dest = Spy::default();
    compact_infos(&source, &dest, &infos, 1).unwrap();
    let s = dest.script();
    assert_eq!((s.begins, s.commits, s.aborts), (1, 1, 0));
}

// Memory.

#[test]
#[cfg_attr(miri, ignore = "compacts 64000 pages; native runs measure the bound")]
fn thirty_two_inputs_of_two_thousand_pages_stay_under_the_stated_bound() {
    let ps = size(512);
    let source = MemStore::new();
    let infos: Vec<FileInfo> = (1..=32_u64)
        .map(|n| {
            let mut spec = Spec::plain(512, 2000, n, n, &[]);
            spec.pages = (1..=2000_u32)
                .map(|p| (p, noisy(ps, n * 10_000 + u64::from(p))))
                .collect();
            put(&source, 0, &spec)
        })
        .collect();
    let total: u64 = infos.iter().map(|i| i.size).sum();
    assert!(total > 30_000_000, "inputs total {total} bytes");
    let page = ps.bytes();

    // The public entry point: 1 MiB chunks, 1 MiB output buffer.
    let dest = MemStore::new();
    let done = compact_infos(&source, &dest, &infos, 9).unwrap();
    let chunk = usize::try_from(CHUNK_BYTES).unwrap();
    let bound = MAX_INPUTS * (chunk + page) + chunk + 2 * page;
    assert!(done.peak_buffered() <= bound, "{}", done.peak_buffered());
    assert_eq!(done.pages(), 2000);
    assert_eq!(done.bytes_read(), total);

    // Small chunks make the bound bite: it is a few hundred KiB for 32 MB of input.
    let small = Budget {
        chunk: ReadLen::new(16 * 1024).unwrap(),
        flush: 16 * 1024,
    };
    let dest = MemStore::new();
    let done = block_on(compact_with(
        &source,
        &dest,
        &inputs(&infos),
        level(9),
        small,
    ))
    .unwrap();
    let bound = MAX_INPUTS * (16 * 1024 + page) + 16 * 1024 + 2 * page;
    assert!(done.peak_buffered() <= bound, "{}", done.peak_buffered());
    // The peak is measured, not assumed: every input holds a chunk at once,
    // so it is far above what the output buffer alone could reach.
    assert!(
        done.peak_buffered() >= MAX_INPUTS * 16 * 1024 / 2,
        "{}",
        done.peak_buffered()
    );
    assert!(u64::try_from(bound).unwrap() * 50 < total);
    let (verified, pages) = decode_all(&read_all(&dest, &done.info().key)).unwrap();
    assert_eq!(verified.page_count(), 2000);
    assert_eq!(pages[0].1, noisy(ps, 32 * 10_000 + 1));
}

// Properties over chains shaped the way SQLite writes them.

/// What one transaction does: its new size, what a grown page is filled with,
/// and overwrites as (page choice, fill).
type Draft = (u32, u8, Vec<(u32, u8)>);

fn drafts() -> impl Strategy<Value = Vec<Draft>> {
    prop::collection::vec(
        (
            1_u32..=24,
            1_u8..=255,
            prop::collection::vec((0_u32..64, 1_u8..=255), 0..6),
        ),
        1..=32,
    )
}

/// The files SQLite would write for the drafts (growth always writes the new
/// pages), and the database state after each.
fn build(ps: PageSize, tracked: bool, drafts: &[Draft]) -> (Vec<Spec>, Vec<BTreeMap<u32, u8>>) {
    let mut state: BTreeMap<u32, u8> = BTreeMap::new();
    let mut commit = 0;
    let (mut specs, mut states) = (Vec::new(), Vec::new());
    let mut previous: Option<DbChecksum> = None;
    for (i, (new_commit, fill, writes)) in drafts.iter().enumerate() {
        let mut written: BTreeMap<u32, u8> = BTreeMap::new();
        for pgno in (commit + 1)..=*new_commit {
            written.insert(pgno, *fill);
        }
        for (choice, seed) in writes {
            written.insert(1 + choice % new_commit, *seed);
        }
        state.retain(|pgno, _| pgno <= new_commit);
        state.extend(&written);
        let txid = u64::try_from(i).unwrap() + 1;
        let mut spec = Spec::plain(ps.get(), *new_commit, txid, txid, &[]);
        spec.pages = written.iter().map(|(p, s)| (*p, solid(ps, *s))).collect();
        if tracked {
            spec.tracked = true;
            spec.pre = previous;
            spec.post = Some(checksum(ps, &state));
            previous = spec.post;
        }
        commit = *new_commit;
        specs.push(spec);
        states.push(state.clone());
    }
    (specs, states)
}

fn assert_db(db: &Database, state: &BTreeMap<u32, u8>, commit: u32, ps: PageSize) {
    assert_eq!(db.page_count(), commit);
    for pgno in 1..=commit {
        let want = state
            .get(&pgno)
            .map_or_else(|| vec![0; ps.bytes()], |s| solid(ps, *s));
        assert_eq!(db.page(pgno), Some(want), "page {pgno}");
    }
}

fn restore_latest(store: &MemStore) -> Database {
    let sink = MemSink::new();
    block_on(restore(store, RestoreTarget::Latest, sink.clone())).unwrap();
    sink.published().unwrap()
}

/// Pages a merge of specs[from..] should hold: newest wins, the final commit caps.
fn expected_pages(ps: PageSize, specs: &[Spec]) -> Pages {
    let mut merged: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    for spec in specs {
        merged.extend(spec.pages.iter().cloned());
    }
    let commit = specs.last().unwrap().commit;
    let _ = ps;
    merged.into_iter().filter(|(p, _)| *p <= commit).collect()
}

fn pick(index: usize) -> PageSize {
    size([512, 4096, 65536][index])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    #[cfg_attr(miri, ignore = "32 random cases of whole compactions; native runs cover them")]
    fn restoring_the_snapshot_and_the_chain_equals_restoring_the_compacted_file(
        drafts in drafts(),
        ps_pick in 0_usize..3,
        tracked in any::<bool>(),
    ) {
        let ps = pick(ps_pick);
        let (specs, states) = build(ps, tracked, &drafts);
        let n = specs.len();
        let last_commit = specs[n - 1].commit;
        let source = MemStore::new();
        let infos = put_all(&source, 0, &specs);

        // The whole chain, as the new snapshot.
        let dest = MemStore::new();
        let done = compact_infos(&source, &dest, &infos, 9).unwrap();
        let h = done.header().fields();
        prop_assert_eq!(
            (h.commit, h.txids, h.node_id, h.wal, h.tracked, h.pre_apply),
            (last_commit, range(1, u64::try_from(n).unwrap()), 0, None, tracked, None)
        );
        prop_assert_eq!(h.timestamp, specs[n - 1].timestamp());
        prop_assert_eq!(done.trailer().post_apply(), specs[n - 1].post);
        let (verified, pages) = decode_all(&read_all(&dest, &done.info().key)).unwrap();
        prop_assert_eq!(verified.header(), done.header());
        prop_assert_eq!(pages, expected_pages(ps, &specs));
        assert_db(&restore_latest(&dest), &states[n - 1], last_commit, ps);

        // Snapshot plus the compacted rest.
        if n >= 2 {
            let rest = compact_infos(&source, &source, &infos[1..], 1).unwrap();
            let store = MemStore::new();
            put(&store, 9, &specs[0]);
            put_bytes(&store, rest.info().key, rest.info().created_at().unwrap().at(),
                &read_all(&source, &rest.info().key));
            let h = rest.header().fields();
            prop_assert_eq!(h.pre_apply, specs[1].pre);
            prop_assert_eq!(h.txids, range(2, u64::try_from(n).unwrap()));
            assert_db(&restore_latest(&store), &states[n - 1], last_commit, ps);
            let (_, pages) = decode_all(&read_all(&store, &rest.info().key)).unwrap();
            prop_assert_eq!(pages, expected_pages(ps, &specs[1..]));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "32 random cases of whole compactions; native runs cover them")]
    fn compacting_two_compactions_equals_compacting_everything_at_once(
        drafts in drafts(),
        ps_pick in 0_usize..3,
        tracked in any::<bool>(),
        split in 1_usize..32,
    ) {
        let ps = pick(ps_pick);
        let (specs, _) = build(ps, tracked, &drafts);
        prop_assume!(specs.len() >= 2);
        let split = 1 + split % (specs.len() - 1);
        let source = MemStore::new();
        let infos = put_all(&source, 0, &specs);
        let mid = MemStore::new();
        let x = compact_infos(&source, &mid, &infos[..split], 1).unwrap();
        let y = compact_infos(&source, &mid, &infos[split..], 1).unwrap();
        let two = compact_infos(&mid, &mid, &[x.info(), y.info()], 2).unwrap();
        let all = compact_infos(&source, &source, &infos, 2).unwrap();
        prop_assert_eq!(two.header(), all.header());
        prop_assert_eq!(two.trailer().post_apply(), all.trailer().post_apply());
        let a = decode_all(&read_all(&mid, &two.info().key)).unwrap().1;
        let b = decode_all(&read_all(&source, &all.info().key)).unwrap().1;
        prop_assert_eq!(a, b);
    }
}

#[test]
fn a_level_not_above_every_input_is_refused_before_any_store_call() {
    // Source and destination are one replica for levels 1 and up: an output
    // at an input's level would take the input's own key.
    let store = MemStore::new();
    let info = put(&store, 1, &Spec::plain(512, 2, 2, 3, &[(1, 5), (2, 6)]));
    let refused = |source: &MemStore, dest: &MemStore, l: u8| {
        matches!(
            compact_infos(source, dest, &[info], l),
            Err(CompactError::Inputs(InputsError::LevelNotAbove { .. }))
        )
    };
    assert!(refused(&store, &store, 1), "equal level");
    assert!(refused(&store, &store, 0), "lower level");
    let other = MemStore::new();
    assert!(refused(&store, &other, 1), "source differs from dest");
    assert!(nothing_published(&other), "a refusal stores nothing");
    let ok = compact_infos(&store, &other, &[info], 2).unwrap();
    assert_ne!(ok.info().key, info.key);
}

#[test]
fn a_regrow_that_skips_a_page_above_the_shrink_is_refused() {
    // The base holds page 4; transaction 2 shrinks to 2 pages; transaction 3
    // regrows to 4 but writes only page 3. In-order restore leaves page 4
    // zero; the compacted file hides the shrink, so the base's page 4 comes
    // back. The input-held version of this chain is PageSurvivesShrink; this
    // one is published.
    let base = Spec::plain(512, 4, 1, 1, &[(1, 1), (2, 2), (3, 3), (4, 4)]);
    let chain = [
        Spec::plain(512, 2, 2, 2, &[(1, 5)]),
        Spec::plain(512, 4, 3, 3, &[(3, 7)]),
    ];
    let original = MemStore::new();
    put(&original, 9, &base);
    let infos = put_all(&original, 0, &chain);
    let compacted = MemStore::new();
    put(&compacted, 9, &base);
    let result = compact_infos(&original, &compacted, &infos, 1);
    let Err(CompactError::Input { fault, .. }) = result else {
        panic!("a regrow that leaves page 4 unwritten compacted");
    };
    assert!(
        matches!(fault, InputFault::PageNotRewritten { pgno } if pgno.get() == 4),
        "{fault:?}"
    );
}

#[test]
fn a_regrow_that_rewrites_every_page_above_the_shrink_compacts_and_restores_equal() {
    let base = Spec::plain(512, 4, 1, 1, &[(1, 1), (2, 2), (3, 3), (4, 4)]);
    let chain = [
        Spec::plain(512, 2, 2, 2, &[(1, 5)]),
        Spec::plain(512, 4, 3, 3, &[(3, 7), (4, 8)]),
    ];
    let original = MemStore::new();
    put(&original, 9, &base);
    let infos = put_all(&original, 0, &chain);
    let compacted = MemStore::new();
    put(&compacted, 9, &base);
    compact_infos(&original, &compacted, &infos, 1).unwrap();
    let (before, after) = (restore_latest(&original), restore_latest(&compacted));
    for pgno in 1..=4 {
        assert_eq!(before.page(pgno), after.page(pgno), "page {pgno}");
    }
}

#[test]
fn the_first_unrewritten_page_skips_the_lock_page_and_stops_at_the_bound() {
    let size = PageSize::MIN;
    let first =
        |lo, hi, shortest| first_unrewritten(lo, hi, DbPages::new(shortest), size).map(PageNo::get);
    assert_eq!(first(1, 9, 3), Some(4));
    assert_eq!(first(6, 9, 3), Some(6));
    assert_eq!(first(1, 4, 3), None);
    let lock = lsr_ltx::lock_page(size);
    assert_eq!(first(1, lock + 2, lock - 1), Some(lock + 1));
    assert_eq!(first(1, lock + 1, lock - 1), None);
    assert_eq!(first(lock, lock + 2, 3), Some(lock + 1));
}

#[test]
fn the_chunk_is_one_mebibyte() {
    assert_eq!(CHUNK_BYTES, 1 << 20);
}

/// Output is handed to the upload when the buffer reaches the flush size, not
/// page by page: a small file is one write.
#[test]
fn a_small_output_is_written_to_the_upload_once() {
    let source = MemStore::new();
    let infos = four(&source);
    let dest = Spy::default();
    let done = compact_infos(&source, &dest, &infos, 1).unwrap();
    assert!(done.pages() > 1, "{}", done.pages());
    assert_eq!(dest.script().writes, 1);
}

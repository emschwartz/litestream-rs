//! Upload sequencing tests: the pure plan, each ordering and verification rule
//! against the in-memory store, and a property over random fault scripts.

use std::collections::VecDeque;
use std::future::Future;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, WalOrigin};
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::fault::Next;
use crate::fault::role::{Replica, Role, Staging};
use crate::fault::{Backoff, PersistentCause, Policy, policy};
use crate::layout::CreatedAt;
use crate::mem::{MemStore, MemUpload};
use crate::store::Upload;
use crate::store::{CorruptCause, Listing, StoreError, StoreOp};

// The whole-tick loop the actor runs (`Actor::upload_unit`), as these tests
// drive it: the uploader itself only steps.
/// A whole tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Ticked {
    /// The state to plan from next.
    state: UploadState,
    /// How many files this tick committed.
    put: u32,
    /// `Now` when the tick stopped at its file budget with more staged,
    /// otherwise `OnWalChange`.
    next: Next,
}

/// Steps until the plan is idle or `config` says the tick is full.
///
/// # Errors
/// The first failed step; its state already includes what this tick confirmed.
async fn upload_tick<L: ObjectStore, S: ObjectStore>(
    mut state: UploadState,
    config: UploadConfig,
    staging: &L,
    remote: &S,
    newest_staged: Option<Txid>,
) -> Result<Ticked, UploadFailed> {
    let mut put = 0_u32;
    loop {
        let full = config.files_per_tick.is_some_and(|cap| put >= cap.get());
        if full && plan_upload(&state, newest_staged) != UploadPlan::Idle {
            return Ok(Ticked {
                state,
                put,
                next: Next::Now,
            });
        }
        let witnesses = Witnesses::trust_all(staging);
        let step = upload_next(state, staging, remote, newest_staged, &witnesses).await?;
        state = step.state;
        match step.did {
            Did::Nothing => {
                return Ok(Ticked {
                    state,
                    put,
                    next: Next::OnWalChange,
                });
            }
            Did::Listed { .. } | Did::Diverged { .. } => {}
            Did::Put(_) | Did::AlreadyPut(_) => put = put.saturating_add(1),
        }
    }
}

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn key(n: u64) -> FileKey {
    staged_key(txid(n))
}

fn ps() -> PageSize {
    PageSize::new(4096).unwrap()
}

fn created_at(n: u64) -> Timestamp {
    Timestamp::from_unix_millis(i64::try_from(n).unwrap().saturating_mul(1000))
}

/// A level-0 file for one transaction. Bytes differ per transaction.
fn encode(n: u64) -> Vec<u8> {
    encode_pages(n, u8::try_from(n % 251).unwrap())
}

/// A level-0 file for transaction `n` whose pages are filled with `seed`.
fn encode_pages(n: u64, seed: u8) -> Vec<u8> {
    encode_file(n, seed, ps(), None)
}

/// A level-0 file for transaction `n` of `page_size` pages filled with
/// `seed`, read from `wal` when it names a WAL range.
fn encode_file(n: u64, seed: u8, page_size: PageSize, wal: Option<WalOrigin>) -> Vec<u8> {
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit: 2,
        txids: TxidRange::single(txid(n)),
        timestamp: created_at(n),
        pre_apply: None,
        wal,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for pgno in 1..=2_u32 {
        encoder
            .write_page(
                PageNo::new(pgno, page_size).unwrap(),
                &vec![seed; page_size.bytes()],
            )
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

fn put_bytes(store: &MemStore, at: FileKey, bytes: &[u8]) -> FileInfo {
    block_on(async {
        let mut up = store
            .begin_upload(&at, created_at(1), WriteMode::Replace)
            .await
            .unwrap();
        up.write(bytes).await.unwrap();
        up.commit().await.unwrap()
    })
}

fn stage(staging: &MemStore, n: u64) -> Vec<u8> {
    let bytes = encode(n);
    put_bytes(staging, key(n), &bytes);
    bytes
}

fn read_all(store: &MemStore, at: &FileKey) -> Vec<u8> {
    block_on(store.read(at, 0, ReadLen::new(ReadLen::MAX_RAW).unwrap())).unwrap()
}

fn files(store: &MemStore) -> Vec<FileInfo> {
    (0..=9)
        .flat_map(|l| {
            block_on(store.list(Level::new(l).unwrap(), None, PageLimit::MAX))
                .unwrap()
                .files
        })
        .collect()
}

fn transient() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Upload,
        kind: ErrorKind::TimedOut,
    }
}

/// What a test wants the wrapped replica to do, and what it saw.
#[derive(Default)]
struct Log {
    lists: u32,
    reads: u32,
    deletes: Vec<FileKey>,
    fail_delete: bool,
    begins: Vec<FileKey>,
    aborts: u32,
    /// Bytes every write was handed, summed.
    written: u64,
    commit_before: VecDeque<StoreError>,
    commit_after: VecDeque<StoreError>,
    write_fail: VecDeque<StoreError>,
    lie_about_size: bool,
    /// The modes uploads were begun with.
    modes: Vec<WriteMode>,
    /// A key whose whole reads from the start are let through this many
    /// times, and then one comes back with a byte flipped.
    flip_read: Option<(FileKey, u32)>,
    /// The whole reads from the start of any key, counted from one, that come back with a
    /// byte flipped.
    flip_whole_reads: Vec<u32>,
    /// Whole reads from the start of a key seen so far.
    whole_reads: u32,
}

/// A [MemStore] that counts calls and can fail a commit before it publishes
/// or answer with a wrong size.
#[derive(Clone, Default)]
struct Probe {
    inner: MemStore,
    log: Arc<Mutex<Log>>,
}

impl Probe {
    fn log(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct ProbeUpload {
    inner: MemUpload,
    log: Arc<Mutex<Log>>,
}

impl ObjectStore for Probe {
    type Upload = ProbeUpload;

    /// Forwards to the store it wraps.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
        {
            let mut log = self.log();
            log.lists = log.lists.saturating_add(1);
        }
        self.inner.list(level, after, limit)
    }

    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        let flip = {
            let mut log = self.log();
            log.reads = log.reads.saturating_add(1);
            if offset == 0 {
                log.whole_reads = log.whole_reads.saturating_add(1);
            }
            let nth = log.whole_reads;
            let counted = offset == 0 && log.flip_whole_reads.contains(&nth);
            match log.flip_read {
                _ if counted => true,
                Some((at, 0)) if at == *key && offset == 0 => {
                    log.flip_read = None;
                    true
                }
                Some((at, skip)) if at == *key && offset == 0 => {
                    log.flip_read = Some((at, skip.saturating_sub(1)));
                    false
                }
                Some(_) | None => false,
            }
        };
        let read = self.inner.read(key, offset, len);
        async move {
            let mut bytes = read.await?;
            let middle = bytes.len() / 2;
            if flip && let Some(byte) = bytes.get_mut(middle) {
                *byte ^= 0x40;
            }
            Ok(bytes)
        }
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
        {
            let mut log = self.log();
            log.reads = log.reads.saturating_add(1);
        }
        self.inner.read_with_created_at(file, len)
    }

    fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> impl Future<Output = Result<ProbeUpload, StoreError>> + Send {
        self.log().begins.push(*key);
        self.log().modes.push(mode);
        let log = Arc::clone(&self.log);
        let begun = self.inner.begin_upload(key, created_at, mode);
        async move {
            Ok(ProbeUpload {
                inner: begun.await?,
                log,
            })
        }
    }

    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
        let refused = {
            let mut log = self.log();
            log.deletes.extend_from_slice(keys);
            log.fail_delete
        };
        let done = self.inner.delete(keys);
        async move {
            if refused {
                return Err(transient());
            }
            done.await
        }
    }
}

impl Upload for ProbeUpload {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send {
        let early = {
            let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
            log.written += u64::try_from(chunk.len()).unwrap();
            log.write_fail.pop_front()
        };
        let wrote = early.map_or_else(|| Ok(self.inner.write(chunk)), Err);
        async move {
            match wrote {
                Ok(inner) => inner.await,
                Err(err) => Err(err),
            }
        }
    }

    fn commit(self) -> impl Future<Output = Result<FileInfo, StoreError>> + Send {
        let (early, late, lie) = {
            let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
            (
                log.commit_before.pop_front(),
                log.commit_after.pop_front(),
                log.lie_about_size,
            )
        };
        async move {
            if let Some(err) = early {
                self.inner.abort().await?;
                return Err(err);
            }
            let mut info = self.inner.commit().await?;
            if let Some(err) = late {
                return Err(err);
            }
            if lie {
                info.size = info.size.saturating_add(1);
            }
            Ok(info)
        }
    }

    fn abort(self) -> impl Future<Output = Result<(), StoreError>> + Send {
        {
            let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
            log.aborts = log.aborts.saturating_add(1);
        }
        self.inner.abort()
    }
}

/// One step, with a witness for every file staged now.
fn next<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    position: u64,
) -> Result<Uploaded, UploadFailed> {
    let witnesses = Witnesses::trust_all(staging);
    next_with(state, staging, remote, position, &witnesses)
}

/// One step with the given witnesses, taken before a fault was armed or a
/// probe that counts reads was put in front of staging.
fn next_with<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    position: u64,
    witnesses: &Witnesses,
) -> Result<Uploaded, UploadFailed> {
    block_on(upload_next(
        state,
        staging,
        remote,
        Txid::new(position),
        witnesses,
    ))
}

fn failure(result: Result<Uploaded, UploadFailed>) -> UploadFailed {
    result.expect_err("the step should fail")
}

#[test]
fn plan_lists_before_anything_when_the_replica_is_not_known() {
    let position = Some(txid(5));
    assert_eq!(
        plan_upload(&UploadState::Unknown, position),
        UploadPlan::List
    );
    let after_error = UploadState::ListFirst {
        confirmed: Some(txid(2)),
    };
    assert_eq!(plan_upload(&after_error, position), UploadPlan::List);
}

#[test]
fn plan_uploads_the_next_transaction_until_the_position() {
    let at = |c| UploadState::At(c);
    assert_eq!(
        plan_upload(&at(None), Some(txid(3))),
        UploadPlan::Upload(key(1))
    );
    assert_eq!(
        plan_upload(&at(Some(txid(2))), Some(txid(3))),
        UploadPlan::Upload(key(3))
    );
    assert_eq!(
        plan_upload(&at(Some(txid(3))), Some(txid(3))),
        UploadPlan::Idle
    );
    assert_eq!(
        plan_upload(&at(Some(txid(4))), Some(txid(3))),
        UploadPlan::Idle
    );
    assert_eq!(plan_upload(&at(None), None), UploadPlan::Idle);
    assert_eq!(
        plan_upload(&at(Some(Txid::MAX)), Some(Txid::MAX)),
        UploadPlan::Idle
    );
}

#[test]
fn a_listing_of_an_empty_replica_then_uploads_from_one() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let bytes = stage(&staging, 1);
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(None));
    assert_eq!(listed.did, Did::Listed { newest: None });
    let put = next(listed.state, &staging, &remote, 1).unwrap();
    assert_eq!(put.state, UploadState::At(Some(txid(1))));
    let Did::Put(info) = put.did else {
        panic!("expected a put")
    };
    assert_eq!(info.key, key(1));
    assert_eq!(info.size, u64::try_from(bytes.len()).unwrap());
    assert_eq!(read_all(&remote, &key(1)), bytes);
    assert_eq!(info.created_at().unwrap().at(), created_at(1));
}

#[test]
fn steady_state_makes_no_list_call_in_a_thousand_ticks() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let mut state = next(UploadState::Unknown, &staging, &remote, 0)
        .unwrap()
        .state;
    assert_eq!(
        usize::try_from(remote.log().lists).unwrap(),
        Level::ALL.len()
    );
    let booted = remote.log().lists;
    for n in 1..=1000 {
        stage(&staging, n);
        let ticked = block_on(upload_tick(
            state,
            UploadConfig::default(),
            &staging,
            &remote,
            Txid::new(n),
        ))
        .unwrap();
        assert_eq!((ticked.put, ticked.next), (1, Next::OnWalChange));
        state = ticked.state;
    }
    assert_eq!(remote.log().lists, booted);
    assert_eq!(state, UploadState::At(Some(txid(1000))));
    assert_eq!(files(&remote.inner).len(), 1000);
}

#[test]
fn a_commit_that_failed_after_publishing_is_found_by_one_listing_and_not_uploaded_twice() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    remote.inner.inject_failure_after_publish(transient());
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::StoreTransient);
    assert_eq!(failed.state, UploadState::ListFirst { confirmed: None });
    let listed = next(failed.state, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
    assert_eq!(remote.log().begins.len(), 1);
    let idle = next(listed.state, &staging, &remote, 1).unwrap();
    assert_eq!(idle.did, Did::Nothing);
    assert_eq!(remote.log().begins.len(), 1);
}

#[test]
fn a_commit_that_failed_before_publishing_is_listed_once_then_uploaded_again() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    remote.log().commit_before.push_back(transient());
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.state, UploadState::ListFirst { confirmed: None });
    assert!(files(&remote.inner).is_empty());
    let listed = next(failed.state, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(None));
    let put = next(listed.state, &staging, &remote, 1).unwrap();
    assert_eq!(put.state, UploadState::At(Some(txid(1))));
    assert_eq!(remote.log().begins.len(), 2);
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
}

#[test]
fn a_listed_key_of_another_size_is_uploaded_again_over_the_same_key() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let bytes = stage(&staging, 1);
    for removed in [false, true] {
        block_on(remote.delete(&[key(1)])).unwrap();
        if !removed {
            put_bytes(&remote, key(1), &bytes[..bytes.len() - 1]);
        }
        let mismatch = StoreError::Corrupt {
            key: Some(key(1)),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: u64::try_from(bytes.len()).unwrap(),
                stored: u64::try_from(bytes.len() - 1).unwrap(),
                removed,
            },
        };
        let listing_first = UploadState::ListFirst { confirmed: None };
        assert_eq!(
            policy(&Replica::<MemStore>::fault(&mismatch)),
            Policy::Retry(crate::fault::Backoff::Doubling)
        );
        let listed = next(listing_first, &staging, &remote, 1).unwrap();
        assert_eq!(listed.state, UploadState::At(None), "removed: {removed}");
        let put = next(listed.state, &staging, &remote, 1).unwrap();
        assert_eq!(put.state, UploadState::At(Some(txid(1))));
        assert_eq!(read_all(&remote, &key(1)), bytes);
    }
}

#[test]
fn a_checksum_mismatch_writes_the_key_again_without_trusting_a_listing() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    // The object is visible at the right size, which a listing would confirm.
    remote.log().commit_after.push_back(StoreError::Corrupt {
        key: Some(key(1)),
        cause: CorruptCause::ChecksumMismatch { part: None },
    });
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::UploadChecksumMismatch);
    assert_eq!(failed.state, UploadState::At(None));
    assert_eq!(
        plan_upload(&failed.state, Txid::new(1)),
        UploadPlan::Upload(key(1))
    );
    let put = next(failed.state, &staging, &remote, 1).unwrap();
    assert_eq!(put.state, UploadState::At(Some(txid(1))));
    assert_eq!(remote.log().lists, 0);
    assert_eq!(remote.log().begins, vec![key(1), key(1)]);
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
}

#[test]
fn a_boot_listing_does_not_confirm_a_newest_file_of_the_wrong_size() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let bytes = stage(&staging, 1);
    stage(&staging, 2);
    put_bytes(&remote, key(1), &bytes);
    put_bytes(&remote, key(2), b"short");
    let listed = next(UploadState::Unknown, &staging, &remote, 2).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
    let put = next(listed.state, &staging, &remote, 2).unwrap();
    assert_eq!(put.state, UploadState::At(Some(txid(2))));
    assert_eq!(read_all(&remote, &key(2)), encode(2));
}

#[test]
fn a_listing_never_moves_confirmed_backwards_through_a_size_check() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    stage(&staging, 1);
    put_bytes(&remote, key(1), b"not the staged bytes");
    let state = UploadState::ListFirst {
        confirmed: Some(txid(1)),
    };
    let listed = next(state, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
}

/// A tail that equals what is already confirmed was confirmed by its own
/// commit, so the listing does not read it back.
#[test]
fn a_listing_does_not_read_back_a_tail_already_confirmed() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    put_bytes(&remote.inner, key(1), b"never read back");
    let state = UploadState::ListFirst {
        confirmed: Some(txid(1)),
    };
    let listed = next(state, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
    assert_eq!(remote.log().reads, 0);
}

#[test]
fn a_commit_that_answers_with_a_wrong_size_is_ambiguous_not_confirmed() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    remote.log().lie_about_size = true;
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::UploadAmbiguous);
    assert_eq!(failed.state, UploadState::ListFirst { confirmed: None });
}

/// The object such a commit left does not hold what was sent: it is deleted before the
/// fault is reported, so no restore meets it while the retry waits.
#[test]
fn a_commit_that_answers_with_a_wrong_size_leaves_no_object() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    remote.log().lie_about_size = true;
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::UploadAmbiguous);
    let listed = block_on(remote.inner.list(Level::ZERO, None, PageLimit::MAX)).unwrap();
    assert!(listed.files.is_empty(), "{:?}", listed.files);
}

#[test]
fn errors_before_the_commit_leave_the_state_and_cost_no_listing() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    let state = UploadState::At(None);
    remote.inner.inject_failure(StoreOp::Upload, transient());
    let failed = failure(next(state, &staging, &remote, 1));
    assert_eq!((failed.state, failed.fault), (state, Fault::StoreTransient));
    remote.log().write_fail.push_back(transient());
    let failed = failure(next(state, &staging, &remote, 1));
    assert_eq!(failed.state, state);
    let witnesses = Witnesses::trust_all(&staging);
    staging.inject_failure(StoreOp::Read, transient());
    let failed = failure(next_with(state, &staging, &remote, 1, &witnesses));
    assert_eq!((failed.state, failed.fault), (state, Fault::LocalIo));
    assert_eq!(remote.log().lists, 0);
    assert!(files(&remote.inner).is_empty());
    next(state, &staging, &remote, 1).unwrap();
}

#[test]
fn a_write_error_aborts_the_upload() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    remote.log().write_fail.push_back(transient());
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.state, UploadState::At(None));
    assert_eq!(remote.log().aborts, 1);
    assert!(files(&remote.inner).is_empty());
}

#[test]
fn every_single_byte_flip_of_a_staged_file_is_refused_and_leaves_nothing_visible() {
    let good = encode(1);
    for at in 0..good.len() {
        let mut bad = good.clone();
        bad[at] ^= 0xFF;
        let (staging, remote) = (MemStore::new(), Probe::default());
        put_bytes(&staging, key(1), &bad);
        let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
        assert_eq!(failed.fault, Fault::StagedLost, "flip at byte {at}");
        assert_eq!(failed.state, UploadState::At(None));
        assert!(files(&remote.inner).is_empty(), "flip at byte {at}");
    }
}

#[test]
fn a_truncated_or_overlong_staged_file_is_refused() {
    let good = encode(1);
    let mut long = good.clone();
    long.push(0);
    for bad in [&good[..good.len() - 1], &good[..0], &good[..40], &long[..]] {
        let (staging, remote) = (MemStore::new(), Probe::default());
        put_bytes(&staging, key(1), bad);
        let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
        assert_eq!(failed.fault, Fault::StagedLost, "{} bytes", bad.len());
        assert!(files(&remote.inner).is_empty());
    }
}

#[test]
fn a_staged_file_that_names_another_transaction_is_refused() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    put_bytes(&staging, key(1), &encode(2));
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::StagedLost);
    assert!(remote.log().begins.is_empty());
    assert!(files(&remote.inner).is_empty());
}

/// The level-0 file for transaction 1, of incompressible pages, so it spans
/// several read chunks.
fn big_file() -> Vec<u8> {
    big_file_for(1, 0x9E37_79B9_7F4A_7C15)
}

/// [`big_file`] for transaction `n`, its pages drawn from `seed`.
fn big_file_for(n: u64, seed: u64) -> Vec<u8> {
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps(),
        commit: 200,
        txids: TxidRange::single(txid(n)),
        timestamp: created_at(1),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut x = seed;
    for pgno in 1..=200_u32 {
        let page: Vec<u8> = (0..ps().bytes())
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_be_bytes()[0]
            })
            .collect();
        encoder
            .write_page(PageNo::new(pgno, ps()).unwrap(), &page)
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

#[test]
fn a_staged_file_larger_than_one_chunk_is_streamed_whole() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let bytes = big_file();
    let quarter_mib = 256 * 1024;
    assert!(bytes.len() > quarter_mib * 2);
    put_bytes(&staging, key(1), &bytes);
    let witnesses = Witnesses::trust_all(&staging);
    let staging = Probe {
        inner: staging,
        log: Arc::default(),
    };
    let put = next_with(UploadState::At(None), &staging, &remote, 1, &witnesses).unwrap();
    assert!(matches!(put.did, Did::Put(_)));
    assert_eq!(read_all(&remote, &key(1)), bytes);
    // Read a quarter mebibyte at a time: few round trips, bounded memory.
    let reads = staging.log().reads;
    assert_eq!(
        usize::try_from(reads).unwrap(),
        bytes.len().div_ceil(quarter_mib),
        "{reads} reads"
    );
}

/// A listed copy that differs from the staged file only past the first read
/// chunk is not the staged file: the comparison reads both to the end.
#[test]
fn a_listed_copy_that_differs_past_the_first_chunk_is_uploaded_again() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let bytes = big_file();
    put_bytes(&staging, key(1), &bytes);
    let mut other = bytes.clone();
    let last = other.len() - 1;
    other[last] ^= 0x40;
    put_bytes(&remote, key(1), &other);
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(None));
}

/// A replica tail with no staged copy is decoded to its end, however many
/// chunks that takes, before the listing trusts it.
#[test]
fn a_large_replica_tail_without_a_staged_copy_verifies_whole() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    put_bytes(&remote, key(1), &big_file());
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
}

#[test]
fn a_missing_staged_file_is_lost_not_skipped() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 2);
    let failed = failure(next(UploadState::At(None), &staging, &remote, 2));
    assert_eq!(failed.fault, Fault::StagedLost);
    assert_eq!(policy(&failed.fault), Policy::Retry(Backoff::Doubling));
    assert!(remote.log().begins.is_empty());
    // The gap is never filled by the file after it.
    assert!(files(&remote.inner).is_empty());
}

#[test]
fn a_remote_ahead_of_local_is_a_second_writer() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    put_bytes(&remote, key(3), &encode(3));
    let failed = failure(next(UploadState::Unknown, &staging, &remote, 2));
    assert_eq!(failed.fault, Fault::RemoteAheadOfLocal);
    assert_eq!(
        policy(&failed.fault),
        Policy::Persistent(PersistentCause::OtherWriter)
    );
    assert_eq!(failed.state, UploadState::Unknown);
    let failed = failure(next(UploadState::Unknown, &staging, &remote, 0));
    assert_eq!(failed.fault, Fault::RemoteAheadOfLocal);
}

#[test]
fn a_remote_that_went_backwards_is_a_persistent_fault() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    put_bytes(&remote, key(1), &encode(1));
    let state = UploadState::ListFirst {
        confirmed: Some(txid(2)),
    };
    let failed = failure(next(state, &staging, &remote, 3));
    assert_eq!(failed.fault, Fault::RemoteRegressed);
    assert_eq!(
        policy(&failed.fault),
        Policy::Persistent(PersistentCause::RemoteRegressed)
    );
    assert_eq!(failed.state, state);
    let empty = MemStore::new();
    let failed = failure(next(state, &staging, &empty, 3));
    assert_eq!(failed.fault, Fault::RemoteRegressed);
}

#[test]
fn a_failed_listing_changes_nothing() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    remote.inject_failure(StoreOp::List, transient());
    let failed = failure(next(UploadState::Unknown, &staging, &remote, 1));
    assert_eq!(
        (failed.fault, failed.state),
        (Fault::StoreTransient, UploadState::Unknown)
    );
}

#[test]
fn files_per_tick_is_honoured_and_asks_to_step_again() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    for n in 1..=5 {
        stage(&staging, n);
    }
    let config = UploadConfig {
        files_per_tick: NonZeroU32::new(2),
        ..UploadConfig::default()
    };
    let mut state = UploadState::Unknown;
    let mut seen = Vec::new();
    loop {
        let ticked = block_on(upload_tick(state, config, &staging, &remote, Txid::new(5))).unwrap();
        state = ticked.state;
        seen.push((ticked.put, ticked.next));
        if ticked.next != Next::Now {
            break;
        }
    }
    assert_eq!(
        seen,
        vec![(2, Next::Now), (2, Next::Now), (1, Next::OnWalChange)]
    );
    assert_eq!(files(&remote).len(), 5);
}

#[test]
fn a_tick_that_fails_keeps_what_it_confirmed() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    stage(&staging, 1);
    let failed = failure_of_tick(UploadState::At(None), &staging, &remote, 2);
    assert_eq!(failed.fault, Fault::StagedLost);
    assert_eq!(failed.state, UploadState::At(Some(txid(1))));
}

fn failure_of_tick(
    state: UploadState,
    staging: &MemStore,
    remote: &MemStore,
    position: u64,
) -> UploadFailed {
    block_on(upload_tick(
        state,
        UploadConfig::default(),
        staging,
        remote,
        Txid::new(position),
    ))
    .expect_err("the tick should fail")
}

#[derive(Clone, Copy, Debug)]
enum Hiccup {
    None,
    BeginOrWrite,
    CommitBefore,
    CommitAfter,
    List,
}

fn hiccup() -> impl Strategy<Value = Hiccup> {
    prop_oneof![
        4 => Just(Hiccup::None),
        1 => Just(Hiccup::BeginOrWrite),
        1 => Just(Hiccup::CommitBefore),
        1 => Just(Hiccup::CommitAfter),
        1 => Just(Hiccup::List),
    ]
}

proptest! {
    #[test]
    fn random_fault_scripts_keep_uploads_ordered_bounded_and_monotonic(
        script in proptest::collection::vec((0_u64..3, hiccup()), 1..60),
    ) {
        let (staging, remote) = (MemStore::new(), Probe::default());
        let mut state = UploadState::Unknown;
        let (mut position, mut staged) = (0_u64, 0_u64);
        let mut confirmed_before = None;
        let spent = script.len();
        for (grow, hiccup) in script {
            position = position.saturating_add(grow);
            while staged < position {
                staged = staged.saturating_add(1);
                stage(&staging, staged);
            }
            let must_list = matches!(state, UploadState::Unknown | UploadState::ListFirst { .. });
            match hiccup {
                Hiccup::None => {}
                Hiccup::BeginOrWrite => remote.inner.inject_failure(StoreOp::Upload, transient()),
                Hiccup::CommitBefore => remote.log().commit_before.push_back(transient()),
                Hiccup::CommitAfter => remote.inner.inject_failure_after_publish(transient()),
                // Only when the step will list, so the fault is spent inside it.
                Hiccup::List if must_list => {
                    remote.inner.inject_failure(StoreOp::List, transient());
                }
                Hiccup::List => {}
            }
            let begins_before = remote.log().begins.len();
            let result = next(state, &staging, &remote, position);
            let after = match &result {
                Ok(done) => done.state,
                Err(failed) => failed.state,
            };
            let begun: Vec<FileKey> = remote.log().begins[begins_before..].to_vec();
            // Nothing is written while the replica is unknown.
            prop_assert!(!must_list || begun.is_empty());
            for at in &begun {
                // Always the next transaction after what is confirmed, never above the position.
                prop_assert_eq!(Some(at.range().max()), state.confirmed().map_or(Some(Txid::MIN), Txid::next));
                prop_assert!(at.range().max().get() <= position);
            }
            // The position never decreases.
            prop_assert!(after.confirmed() >= confirmed_before);
            confirmed_before = after.confirmed();
            // Whatever is visible is a contiguous prefix of the staged files, byte for byte.
            for (index, info) in files(&remote.inner).iter().enumerate() {
                let want = u64::try_from(index).unwrap().saturating_add(1);
                prop_assert_eq!(info.key, key(want));
                prop_assert_eq!(read_all(&remote.inner, &info.key), encode(want));
            }
            state = after;
        }
        // With the faults gone, everything staged arrives.
        remote.inner.inject_failure(StoreOp::Upload, transient());
        drop(next(state, &staging, &remote, position));
        // Faults still queued each cost one more step.
        for _ in 0..position.saturating_add(u64::try_from(spent).unwrap().saturating_mul(3)).saturating_add(4) {
            match next(state, &staging, &remote, position) {
                Ok(done) => state = done.state,
                Err(failed) => state = failed.state,
            }
        }
        prop_assert_eq!(u64::try_from(files(&remote.inner).len()).unwrap(), position);
    }
}

// Review: failing tests (blind adversarial review of upload sequencing).

/// What a store holds after it answers a commit with a checksum mismatch: the
/// key is published, at the staged size, with other bytes.
fn review_rejected_commit(staging: &MemStore, remote: &Probe) -> (Vec<u8>, UploadState) {
    let bytes = stage(staging, 1);
    remote.log().commit_after.push_back(StoreError::Corrupt {
        key: Some(key(1)),
        cause: CorruptCause::ChecksumMismatch { part: None },
    });
    let failed = failure(next(UploadState::At(None), staging, remote, 1));
    assert_eq!(failed.fault, Fault::UploadChecksumMismatch);
    let mut wrong = bytes.clone();
    if let Some(b) = wrong.last_mut() {
        *b ^= 0xFF;
    }
    put_bytes(&remote.inner, key(1), &wrong);
    (bytes, failed.state)
}

/// A restart after a rejected commit lists, finds the key at the staged size
/// and confirms it, though the store said those bytes were wrong.
#[test]
fn review_a_restart_after_a_checksum_mismatch_does_not_confirm_the_rejected_bytes() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let (bytes, _) = review_rejected_commit(&staging, &remote);
    let mut state = UploadState::Unknown;
    for _ in 0..3 {
        state = next(state, &staging, &remote, 1).unwrap().state;
    }
    assert_eq!(state, UploadState::At(Some(txid(1))));
    assert_eq!(
        read_all(&remote.inner, &key(1)),
        bytes,
        "transaction 1 is confirmed over the bytes the store rejected"
    );
}

/// The retry after a rejected commit fails before publishing, a transient
/// error that sends the uploader to a listing. The listing finds the
/// rejected object at the staged size and confirms it.
#[test]
fn review_a_failed_retry_after_a_checksum_mismatch_does_not_confirm_the_rejected_bytes() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let (bytes, state) = review_rejected_commit(&staging, &remote);
    remote.log().commit_before.push_back(transient());
    let retry = failure(next(state, &staging, &remote, 1));
    let mut state = retry.state;
    for _ in 0..3 {
        state = next(state, &staging, &remote, 1).unwrap().state;
    }
    assert_eq!(state, UploadState::At(Some(txid(1))));
    assert_eq!(
        read_all(&remote.inner, &key(1)),
        bytes,
        "transaction 1 is confirmed over the bytes the store rejected"
    );
}

#[test]
fn a_listing_confirms_by_content_not_size() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let bytes = stage(&staging, 1);
    let mut wrong = bytes.clone();
    if let Some(b) = wrong.last_mut() {
        *b ^= 0xFF;
    }
    put_bytes(&remote, key(1), &wrong);
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(None));
    next(listed.state, &staging, &remote, 1).unwrap();
    assert_eq!(read_all(&remote, &key(1)), bytes);
}

/// The replica's newest object verifies and holds another transaction than
/// the staged file under the same key (a run before a power loss uploaded it,
/// or a staging backup was put back). It may already be compacted into level
/// 1 and above, so writing the staged file over it would give one id two
/// contents: the object stands and the listing reports the divergence.
#[test]
fn a_verifying_listed_key_unlike_the_staged_file_is_never_written_over() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 2);
    put_bytes(&remote.inner, key(1), &encode(1));
    let theirs = encode_pages(2, 200);
    put_bytes(&remote.inner, key(2), &theirs);
    let listed = next(UploadState::Unknown, &staging, &remote, 2).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(2))));
    assert_eq!(
        listed.did,
        Did::Diverged {
            confirmed: txid(2),
            from: txid(2)
        }
    );
    next(listed.state, &staging, &remote, 2).unwrap();
    assert!(remote.log().begins.is_empty());
    assert_eq!(read_all(&remote.inner, &key(2)), theirs);
}

/// A file reaches the replica only with its witness: with none, it is
/// refused unread, as a lost file is, and nothing begins on the replica.
#[test]
fn a_staged_file_without_a_witness_is_refused_unread() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    let state = UploadState::At(None);
    let none = Witnesses::new();
    let failed = failure(block_on(upload_next(
        state,
        &staging,
        &remote,
        Some(txid(1)),
        &none,
    )));
    assert_eq!(failed.fault, Fault::StagedLost);
    assert_eq!(failed.state, state);
    assert!(remote.log().begins.is_empty());
}

/// The witness binds the bytes, not the name: a staged file replaced after
/// it was witnessed by another whole, valid file under the same key (a
/// staging backup put back under a running engine) is refused on the
/// upload's own read, and the upload it began is aborted unpublished.
#[test]
fn a_staged_file_replaced_after_its_witness_is_refused_and_leaves_nothing_visible() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    let witnesses = Witnesses::trust_all(&staging);
    put_bytes(&staging, key(1), &encode_pages(1, 77));
    let state = UploadState::At(None);
    let failed = failure(block_on(upload_next(
        state,
        &staging,
        &remote,
        Some(txid(1)),
        &witnesses,
    )));
    assert_eq!(failed.fault, Fault::StagedLost);
    assert_eq!(failed.state, state);
    assert!(files(&remote.inner).is_empty());
    assert_eq!(remote.log().aborts, 1);
    // The bytes it was witnessed for go up.
    put_bytes(&staging, key(1), &encode(1));
    let put = block_on(upload_next(
        state,
        &staging,
        &remote,
        Some(txid(1)),
        &witnesses,
    ));
    assert!(matches!(put.map(|up| up.did), Ok(Did::Put(_))));
}

#[test]
fn a_listing_confirms_a_listed_key_equal_to_the_staged_file() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let bytes = stage(&staging, 1);
    put_bytes(&remote, key(1), &bytes);
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
}

#[test]
fn a_listed_key_with_no_staged_file_must_decode() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    put_bytes(&remote, key(1), &encode(1));
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
    // Garbage, and a valid file naming another transaction, are both corrupt.
    for bad in [b"not ltx".to_vec(), encode(2)] {
        let (staging, remote) = (MemStore::new(), MemStore::new());
        put_bytes(&remote, key(1), &bad);
        let failed = failure(next(UploadState::Unknown, &staging, &remote, 1));
        assert_eq!(failed.fault, Fault::RemoteCorrupt);
        assert_eq!(failed.state, UploadState::Unknown);
    }
}

#[test]
fn steady_state_makes_no_read_call() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let mut state = next(UploadState::Unknown, &staging, &remote, 0)
        .unwrap()
        .state;
    for n in 1..=20 {
        stage(&staging, n);
        state = block_on(upload_tick(
            state,
            UploadConfig::default(),
            &staging,
            &remote,
            Txid::new(n),
        ))
        .unwrap()
        .state;
    }
    assert_eq!(remote.log().reads, 0);
}

#[test]
fn a_rejected_commit_deletes_the_key() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    remote.log().commit_after.push_back(StoreError::Corrupt {
        key: Some(key(1)),
        cause: CorruptCause::ChecksumMismatch { part: None },
    });
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::UploadChecksumMismatch);
    assert_eq!(remote.log().deletes, vec![key(1)]);
    assert!(files(&remote.inner).is_empty());
    // A failing delete changes nothing else: the same key is still due.
    let remote = Probe::default();
    remote.log().fail_delete = true;
    remote.log().commit_after.push_back(StoreError::Corrupt {
        key: Some(key(1)),
        cause: CorruptCause::ChecksumMismatch { part: None },
    });
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.state, UploadState::At(None));
}

#[test]
fn the_upload_reads_in_quarter_mebibyte_chunks() {
    assert_eq!(CHUNK.get(), 256 * 1024);
}

#[test]
fn a_staging_listing_whose_cursor_never_advances_is_a_fault_not_a_loop() {
    let staging = crate::stuck::StuckListing::new(MemStore::new());
    let got = block_on(async {
        let mut none = InFlight::default();
        drop_staged_from(&Staging(&staging), txid(1), &none.stop().await).await
    });
    assert_eq!(got, Err(Fault::LocalIo));
}

/// The file checksum covers an LTX file's decompressed content, not its LZ4
/// framing, and an LZ4 block is one of many encodings of its page: a changed
/// match offset can copy equal bytes from elsewhere and decode to the same
/// page, so the decoder cannot see it (lsr_ltx's frame docs call it aliasing).
/// A zero page with one other byte aliases: its second run of zeros may copy
/// from many offsets in the first. A staged file read with such a change (a
/// bad read, or bytes replaced) is not the file this engine staged: the upload
/// refuses it rather than publish other bytes.
#[test]
fn a_staged_file_whose_bytes_differ_but_still_verify_is_refused() {
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps(),
        commit: 1,
        txids: TxidRange::single(txid(1)),
        timestamp: created_at(1),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut page = vec![0; ps().bytes()];
    page[100] = 0x55;
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    encoder
        .write_page(PageNo::new(1, ps()).unwrap(), &page)
        .unwrap();
    let bytes = encoder.finish(None).unwrap().0;
    let silent = (0..bytes.len()).find_map(|at| {
        let mut changed = bytes.clone();
        changed[at] ^= 1;
        Witnessed::of_bytes(key(1), &changed).map(|_| changed)
    });
    let changed = silent.expect("a byte change the decoder does not see");
    let (staging, remote) = (MemStore::new(), Probe::default());
    put_bytes(&staging, key(1), &bytes);
    let witnesses = Witnesses::trust_all(&staging);
    put_bytes(&staging, key(1), &changed);
    let state = UploadState::At(None);
    let failed = failure(next_with(state, &staging, &remote, 1, &witnesses));
    assert_eq!(failed.fault, Fault::StagedLost);
    assert!(files(&remote.inner).is_empty());
}

// ---------------------------------------------------------------- the bucket refuses

/// Every upload the uploader makes is of a key it takes to be free, so it asks
/// the bucket to refuse a taken one.
#[test]
fn every_upload_asks_the_bucket_to_refuse_a_taken_key() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    stage(&staging, 2);
    let mut state = UploadState::At(None);
    for _ in 0..2 {
        state = next(state, &staging, &remote, 2).unwrap().state;
    }
    assert_eq!(state, UploadState::At(Some(txid(2))));
    assert_eq!(remote.log().modes, [WriteMode::New, WriteMode::New]);
}

/// The key already holds the staged bytes: an earlier commit of this engine
/// whose answer was lost, or a listing too stale to show it. Read back, it is
/// confirmed, and nothing is written over it.
#[test]
fn a_refused_upload_of_the_same_bytes_is_our_own_earlier_commit() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    put_bytes(&remote.inner, key(1), &bytes);
    let step = next(UploadState::At(None), &staging, &remote, 1).unwrap();
    assert_eq!(step.state, UploadState::At(Some(txid(1))));
    assert_eq!(step.did, Did::AlreadyPut(key(1)));
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    assert_eq!(
        remote.log().lists,
        0,
        "a matching read-back needs no listing"
    );
}

/// The key holds another verifying object (an earlier run's under the same
/// id, or another writer's) and no listing shows the replica ahead: the
/// bucket's refusal makes it the listing's divergence, so the object stands
/// and the staged files from that id on must go.
#[test]
fn a_refused_upload_of_other_verifying_bytes_is_never_written_over() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    stage(&staging, 2);
    let theirs = encode_pages(2, 200);
    put_bytes(&remote.inner, key(2), &theirs);
    let state = next(UploadState::At(None), &staging, &remote, 2)
        .unwrap()
        .state;
    let step = next(state, &staging, &remote, 2).unwrap();
    assert_eq!(
        step.did,
        Did::Diverged {
            confirmed: txid(2),
            from: txid(2)
        }
    );
    assert_eq!(step.state, UploadState::At(Some(txid(2))));
    assert_eq!(read_all(&remote.inner, &key(2)), theirs);
}

/// The other object stands past what this engine has staged: another writer.
#[test]
fn a_refused_upload_with_the_replica_ahead_is_another_writer() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    put_bytes(&remote.inner, key(1), &encode_pages(1, 200));
    put_bytes(&remote.inner, key(2), &encode_pages(2, 200));
    let failed = next(UploadState::At(None), &staging, &remote, 1).unwrap_err();
    assert_eq!(failed.fault, Fault::RemoteAheadOfLocal);
    assert_eq!(read_all(&remote.inner, &key(1)), encode_pages(1, 200));
}

/// The key holds something that is not a whole LTX file naming it (a torn
/// object a directory store left): nothing can have compacted it, so the
/// staged file replaces it, as a listing's read-back would.
#[test]
fn a_refused_upload_over_an_object_that_does_not_verify_replaces_it() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    put_bytes(&remote.inner, key(1), b"torn");
    let step = next(UploadState::At(None), &staging, &remote, 1).unwrap();
    assert!(matches!(step.did, Did::Put(_)), "{:?}", step.did);
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    assert_eq!(remote.log().modes, [WriteMode::New, WriteMode::Replace]);
}

/// A level-0 tail the reads judge torn is replaced by its staged copy, which lowers the end
/// the listing found by one. When a level 1 to 9 file already holds that transaction, the
/// end would fall below an object the listing shows and the replacement would give the
/// transaction a second content, so the boot stops instead: an end it cannot prove is not
/// taken.
#[test]
fn a_torn_tail_a_higher_level_file_already_holds_is_not_replaced() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    stage(&staging, 2);
    put_bytes(&remote.inner, key(1), &encode(1));
    put_bytes(&remote.inner, key(2), b"torn");
    let level_one = FileKey::new(
        Level::new(1).unwrap(),
        TxidRange::new(txid(1), txid(2)).unwrap(),
    );
    put_bytes(&remote.inner, level_one, &encode(2));
    let failed = next(UploadState::Unknown, &staging, &remote, 2).unwrap_err();
    assert_eq!(failed.fault, Fault::RemoteCorrupt);
    assert_eq!(read_all(&remote.inner, &key(2)), b"torn");
}

/// The replica's object verifies and differs from the staged file, and the
/// first whole read of the staged copy comes back torn. One read never
/// condemns a file: a second read decides, and the replica's object is not
/// confirmed as this engine's (the listing reports the divergence).
#[test]
fn one_torn_read_of_the_staged_copy_confirms_no_differing_replica_object() {
    let (staging, remote) = (Probe::default(), MemStore::new());
    put_bytes(&staging.inner, key(1), &encode(1));
    put_bytes(&remote, key(1), &encode_pages(1, 200));
    let witnesses = Witnesses::trust_all(&staging);
    // The size check passes and the first compare reads the staged copy
    // whole; the read after it (its own verification) is the torn one.
    staging.log().flip_read = Some((key(1), 1));
    let listed = next_with(UploadState::Unknown, &staging, &remote, 1, &witnesses).unwrap();
    assert_eq!(
        listed.did,
        Did::Diverged {
            confirmed: txid(1),
            from: txid(1)
        }
    );
}

/// The same with the replica's copy: its first verifying read comes back
/// torn. A verifying object with other bytes is never written over, so one
/// failed read does not make it a torn object to replace: a second read
/// decides, and the listing reports the divergence.
#[test]
fn one_torn_read_of_the_replica_copy_does_not_write_over_it() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    let theirs = encode_pages(1, 200);
    put_bytes(&remote.inner, key(1), &theirs);
    // The first compare reads the replica's copy whole; the read after it
    // (its own verification) is the torn one.
    remote.log().flip_read = Some((key(1), 1));
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(
        listed.did,
        Did::Diverged {
            confirmed: txid(1),
            from: txid(1)
        }
    );
    assert_eq!(read_all(&remote.inner, &key(1)), theirs);
}

/// The replica's object is the staged file's own bytes, and the compares of the two copies
/// read it wrong (the first and the second, with its one clean verification between). A
/// compare that differs proves nothing about the object: only a read of it that verifies
/// whole can be another writer's file, so the listing confirms the object as this engine's.
#[test]
fn compares_that_read_the_replica_copy_wrong_do_not_make_it_another_writers() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    put_bytes(&remote.inner, key(1), &bytes);
    // Whole reads of the replica's copy: the first compare, one verification, the second
    // compare, and the read that verifies and names the file's checksum.
    remote.log().flip_whole_reads = vec![1, 3];
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
    assert_eq!(
        listed.did,
        Did::Listed {
            newest: Some(txid(1))
        }
    );
}

/// The same, with the read that would name the object's checksum wrong too: nothing verified
/// the replica's copy against the staged file, so the step is a store fault to retry, never a
/// divergence.
#[test]
fn a_replica_copy_that_never_verifies_on_the_deciding_read_is_a_store_fault() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    put_bytes(&remote.inner, key(1), &bytes);
    remote.log().flip_whole_reads = vec![1, 3, 4];
    let failed = next(UploadState::Unknown, &staging, &remote, 1).unwrap_err();
    assert_eq!(failed.fault, Fault::StoreTransient);
}

/// A listed tail whose size matches the staged file is confirmed by one
/// compare of the two copies: the verify-each-copy path is for copies that
/// differ, and a matching tail never pays for it (one read of the replica's
/// copy).
#[test]
fn a_listed_tail_equal_to_the_staged_file_is_confirmed_by_one_compare() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    put_bytes(&remote.inner, key(1), &bytes);
    let listed = next(UploadState::Unknown, &staging, &remote, 1).unwrap();
    assert_eq!(listed.state, UploadState::At(Some(txid(1))));
    assert_eq!(remote.log().reads, 1);
}

/// A refusal is the bucket saying this key is taken only when it names this
/// key. One that names another key is an answer that cannot be believed: the
/// commit is ambiguous and a listing decides, rather than a read-back of a key
/// nothing says is taken.
#[test]
fn a_refusal_naming_another_key_is_an_ambiguous_commit() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    remote
        .log()
        .commit_before
        .push_back(StoreError::AlreadyExists { key: key(2) });
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::UploadAmbiguous);
    assert_eq!(failed.state, UploadState::ListFirst { confirmed: None });
}

/// A store may answer a commit with an error while the write is still in
/// flight, and a read after it may say the key is absent before the write
/// lands (Cloudflare R2 documents both). The relist after the failed commit
/// finds nothing, the write then lands, and the next put of the same key asks
/// the bucket to refuse a taken key: the refusal and a read-back confirm the
/// late write as this engine's own, and nothing is written over it.
#[test]
fn a_put_that_lands_after_the_relist_found_nothing_is_confirmed_not_written_twice() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    let bytes = stage(&staging, 1);
    remote.log().commit_before.push_back(transient());
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.state, UploadState::ListFirst { confirmed: None });
    let listed = next(failed.state, &staging, &remote, 1).unwrap();
    assert_eq!(
        listed.state,
        UploadState::At(None),
        "the write has not landed"
    );
    // The write that was in flight lands now.
    put_bytes(&remote.inner, key(1), &bytes);
    let step = next(listed.state, &staging, &remote, 1).unwrap();
    assert_eq!(step.did, Did::AlreadyPut(key(1)));
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    assert_eq!(
        remote.log().modes,
        [WriteMode::New, WriteMode::New],
        "every put asks the bucket to refuse a taken key"
    );
}

mod endless;
mod slices;

/// A rejected write deletes only an object it created. A create-only put
/// creates its key or is refused, so its rejected object is its own; a put that
/// replaces an object (here a torn one the replica held) deletes nothing when
/// the store rejects it, and the retry writes the key again.
#[test]
fn a_rejected_write_over_an_object_that_was_there_deletes_nothing() {
    let (staging, remote) = (MemStore::new(), Probe::default());
    stage(&staging, 1);
    put_bytes(&remote.inner, key(1), b"torn");
    // The create-only put is refused (its queued answer goes unused, since the
    // refusal comes first); the replace over the torn object is taken and then
    // rejected.
    remote.log().commit_after.push_back(transient());
    remote.log().commit_after.push_back(StoreError::Corrupt {
        key: Some(key(1)),
        cause: CorruptCause::ChecksumMismatch { part: None },
    });
    let failed = failure(next(UploadState::At(None), &staging, &remote, 1));
    assert_eq!(failed.fault, Fault::UploadChecksumMismatch);
    assert_eq!(remote.log().modes, [WriteMode::New, WriteMode::Replace]);
    assert!(
        remote.log().deletes.is_empty(),
        "{:?}",
        remote.log().deletes
    );
}

/// The same after a refused upload: the bucket refuses the create-only commit, the key holds
/// another verifying object, and the staged copy's own verification read (its third read: the
/// upload and the compare come first) comes back torn. One read never condemns a file, and it
/// never confirms the replica's object either: the step fails on the staged copy
/// (`StagedLost`) and the other object is not taken for this engine's.
#[test]
fn one_torn_read_after_a_refusal_confirms_no_other_object() {
    let (staging, remote) = (Probe::default(), MemStore::new());
    put_bytes(&staging.inner, key(1), &encode(1));
    let theirs = encode_pages(1, 200);
    put_bytes(&remote, key(1), &theirs);
    let witnesses = Witnesses::trust_all(&staging);
    staging.log().flip_read = Some((key(1), 2));
    let failed = next_with(UploadState::At(None), &staging, &remote, 1, &witnesses)
        .expect_err("a torn staged read confirmed the replica's object");
    assert_eq!(failed.fault, Fault::StagedLost);
    assert_eq!(read_all(&remote, &key(1)), theirs);
}

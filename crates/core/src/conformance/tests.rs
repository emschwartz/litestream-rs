//! Self-tests of the suite: it must fail a store that breaks a promise it
//! checks, and its helpers must do what the cases rely on. Every store in the
//! repository is correct, so without these a weakened check would go unnoticed.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use lsr_ltx::{Timestamp, TxidRange};

use super::*;
use crate::MemStore;
use crate::layout::CreatedAt;
use crate::store::Listing;

/// The one promise a [`Flawed`] store breaks.
#[derive(Clone, Copy, Debug)]
enum Flaw {
    /// Lists what it holds but reads back no bytes, as a store reading from a
    /// copy that lags its listing would.
    ReadsEmpty,
    /// Lists every file as exact with a created-at time one millisecond off the
    /// committed one.
    ListsAnotherCreatedAt,
    /// Takes every upload as `Replace`, as an S3 provider that ignores
    /// `If-None-Match` would.
    IgnoresWriteMode,
}

/// A [`MemStore`] with one [`Flaw`]; everything else is the model's.
#[derive(Debug)]
struct Flawed {
    inner: MemStore,
    flaw: Flaw,
}

impl Flawed {
    fn shown(&self, bytes: Vec<u8>) -> Vec<u8> {
        match self.flaw {
            Flaw::ReadsEmpty => Vec::new(),
            Flaw::ListsAnotherCreatedAt | Flaw::IgnoresWriteMode => bytes,
        }
    }

    fn relist(&self, listing: Listing) -> Listing {
        match self.flaw {
            Flaw::ReadsEmpty | Flaw::IgnoresWriteMode => listing,
            Flaw::ListsAnotherCreatedAt => Listing {
                files: listing
                    .files
                    .into_iter()
                    .map(|file| FileInfo {
                        listed: ListedTime::Created(CreatedAt::mtime(at(CREATED_AT + 1))),
                        ..file
                    })
                    .collect(),
                ..listing
            },
        }
    }
}

impl ObjectStore for Flawed {
    type Upload = crate::MemUpload;

    /// Forwards to the store it wraps.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        Ok(self.relist(self.inner.list(level, after, limit).await?))
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        Ok(self.shown(self.inner.read(key, offset, len).await?))
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        let (bytes, created_at) = self.inner.read_with_created_at(file, len).await?;
        Ok((self.shown(bytes), created_at))
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<crate::MemUpload, StoreError> {
        let mode = match self.flaw {
            Flaw::IgnoresWriteMode => WriteMode::Replace,
            Flaw::ReadsEmpty | Flaw::ListsAnotherCreatedAt => mode,
        };
        self.inner.begin_upload(key, created_at, mode).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

/// A [`MemHarness`] whose store has a flaw. Injection goes through the
/// `MemHarness` it wraps, so the cases that need a late commit failure get one.
struct FlawedHarness {
    mem: MemHarness,
    store: Flawed,
}

impl FlawedHarness {
    fn new(flaw: Flaw) -> Self {
        let mem = MemHarness::default();
        let store = Flawed {
            inner: mem.0.clone(),
            flaw,
        };
        Self { mem, store }
    }
}

impl Harness for FlawedHarness {
    type Store = Flawed;

    fn store(&self) -> &Flawed {
        &self.store
    }

    fn plant_unrecognized(&self, level: Level, name: &str) -> Check {
        self.mem.plant_unrecognized(level, name)
    }

    fn fail_next_commit_after_publish(&self) -> bool {
        self.mem.fail_next_commit_after_publish()
    }
}

/// A correct store whose harness cannot inject a late commit failure, so it
/// takes the trait's default.
#[derive(Default)]
struct NoInjection(MemHarness);

impl Harness for NoInjection {
    type Store = MemStore;

    fn store(&self) -> &MemStore {
        self.0.store()
    }

    fn plant_unrecognized(&self, level: Level, name: &str) -> Check {
        self.0.plant_unrecognized(level, name)
    }
}

/// The failure's message, which must name the check that broke.
fn failed(result: Check) -> String {
    match result {
        Ok(()) => panic!("the suite passed a store that breaks its promise"),
        Err(failure) => failure.to_string(),
    }
}

#[test]
fn a_store_that_reads_back_nothing_after_a_late_failed_commit_fails_the_suite() {
    let harness = FlawedHarness::new(Flaw::ReadsEmpty);
    let message = failed(block_on(commit_error_may_leave_the_object_visible(
        &harness,
    )));
    assert!(
        message.starts_with("read agrees with the listing"),
        "{message}"
    );
}

#[test]
fn a_store_that_lists_another_exact_created_at_fails_the_suite() {
    let harness = FlawedHarness::new(Flaw::ListsAnotherCreatedAt);
    let message = failed(block_on(listed_created_at_is_exact_or_marked(&harness)));
    assert!(message.contains("listed with Created"), "{message}");
}

#[test]
fn a_store_that_replaces_on_a_new_upload_fails_the_suite() {
    let harness = FlawedHarness::new(Flaw::IgnoresWriteMode);
    let message = failed(block_on(a_new_upload_never_replaces(&harness)));
    assert!(
        message.starts_with("a New commit into a taken key"),
        "{message}"
    );
}

#[test]
fn a_harness_that_cannot_inject_skips_the_late_commit_case() {
    let harness = NoInjection::default();
    assert!(!harness.fail_next_commit_after_publish());
    block_on(commit_error_may_leave_the_object_visible(&harness)).unwrap();
}

#[test]
fn the_mem_harness_injects_a_late_commit_failure() {
    let harness = MemHarness::default();
    assert!(harness.fail_next_commit_after_publish());
    block_on(async {
        let mut upload = harness
            .store()
            .begin_upload(&key(0, 1, 1)?, at(CREATED_AT), WriteMode::Replace)
            .await?;
        upload.write(b"x").await?;
        upload.commit().await.unwrap_err();
        Ok::<(), Failure>(())
    })
    .unwrap();
}

#[test]
fn the_last_modified_harness_lists_last_modified() {
    let harness = MemHarness::listing_last_modified();
    let listed = block_on(async {
        put(harness.store(), key(0, 1, 1)?, b"x").await?;
        listed_one(harness.store(), key(0, 1, 1)?).await
    })
    .unwrap();
    assert!(
        matches!(listed.listed, ListedTime::LastModified(_)),
        "{:?}",
        listed.listed
    );
}

/// Pending once, after waking its own waker, as a future woken from elsewhere
/// before the executor parks would be.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A waker that does not unpark the thread leaves `block_on` parked for good,
/// so a broken one shows as this test hanging.
#[test]
fn block_on_resumes_a_future_that_was_woken() {
    block_on(YieldOnce(false));
}

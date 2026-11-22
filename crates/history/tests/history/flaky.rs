//! A file replica whose next upload commits fail, as a reset connection fails them: the
//! engine's retries back off on the fake clock, and the drivers must move that clock until it
//! has caught up instead of moving it once and watching (`network::resets_mid_body_...` stalled
//! that way on CI).

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use lsr_core::{
    CreatedAt, FileInfo, FileKey, Level, Listing, ObjectStore, PageLimit, ReadLen, StoreError,
    StoreOp, Upload, WriteMode,
};
use lsr_ltx::{Timestamp, TxidRange};
use tempfile::TempDir;

use crate::replicator::Run;

/// `inner`, but each of the next `failing` commits fails after writing nothing visible.
pub struct Flaky<S> {
    inner: S,
    failing: Arc<AtomicU32>,
}

/// An upload of [`Flaky`]: its commit fails while failures are owed.
pub struct FlakyUpload<U> {
    inner: U,
    failing: Arc<AtomicU32>,
}

impl<U: Upload> Upload for FlakyUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.inner.write(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let owed = self
            .failing
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if owed {
            self.inner.abort().await?;
            return Err(StoreError::Transient {
                op: StoreOp::Upload,
                kind: io::ErrorKind::ConnectionReset,
            });
        }
        self.inner.commit().await
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

impl<S: ObjectStore> ObjectStore for Flaky<S> {
    type Upload = FlakyUpload<S::Upload>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
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
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        let inner = self.inner.begin_upload(key, created_at, mode).await?;
        Ok(FlakyUpload {
            inner,
            failing: Arc::clone(&self.failing),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }
}

/// Three failed commits in a row make the engine wait 1, 2 and then 4 s of its clock before
/// the next try. A driver that moves the clock one poll and watches waits forever; one that
/// moves it while the engine sits parked on it sees everything replicated.
#[test]
fn a_backoff_after_failed_commits_is_waited_out_on_the_fake_clock() {
    let dir = TempDir::new().unwrap();
    let failing = Arc::new(AtomicU32::new(0));
    let store = Arc::new(Flaky {
        inner: lsr_store_fs::FsStore::new(dir.path().join("replica")),
        failing: Arc::clone(&failing),
    });
    let db = dir.path().join("app.db");
    let mut run = Run::new(&store, store.as_ref(), dir.path(), &db, 21);
    run.start();
    run.commit();
    run.poll();
    failing.store(3, Ordering::SeqCst);
    for _ in 0..3 {
        run.commit();
        run.poll();
    }
    assert_eq!(failing.load(Ordering::SeqCst), 0, "every failure was spent");
    run.commit();
    run.stop();
}

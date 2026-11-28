//! A staging store whose listing cursor never moves, for the tests of every
//! paged loop. The store gives up loudly after [`CAP`] pages, so a loop with
//! no progress check fails its test with a message instead of hanging CI.

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};

use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::mem::{MemStore, MemUpload};
use crate::store::WriteMode;
use crate::store::{Listing, ObjectStore, PageLimit, ReadLen, StoreError};

/// How many listing calls a test tolerates before it calls the loop unbounded.
const CAP: u32 = 50;

/// A [`MemStore`] whose every listing page says more follows, always from the
/// same cursor.
#[derive(Debug, Default)]
pub(crate) struct StuckListing {
    inner: MemStore,
    calls: AtomicU32,
}

impl StuckListing {
    /// Wraps `inner`, which holds whatever files the test staged.
    pub(crate) fn new(inner: MemStore) -> Self {
        Self {
            inner,
            calls: AtomicU32::new(0),
        }
    }
}

impl ObjectStore for StuckListing {
    type Upload = MemUpload;

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
        let calls = self.calls.fetch_add(1, Ordering::Relaxed);
        assert!(
            calls < CAP,
            "listing was called {CAP} times and its cursor never advanced: the paging loop has no progress check"
        );
        let page = self.inner.list(level, after, limit);
        async move {
            let mut page = page.await?;
            page.next = Some(TxidRange::single(Txid::MIN));
            Ok(page)
        }
    }

    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        self.inner.read(key, offset, len)
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

    fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> impl Future<Output = Result<MemUpload, StoreError>> + Send {
        self.inner.begin_sized_upload(key, created_at, mode, len)
    }

    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.inner.delete(keys)
    }
}

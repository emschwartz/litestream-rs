//! The wrapped test store shared by the restore and verify tests.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_ltx::{Timestamp, TxidRange};

use super::MemStore;
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::store::{Listing, ObjectStore, PageLimit, ReadLen, StoreError};

/// A store whose files can vanish at the moment they are read, lie about
/// their size, and whose reads are counted.
pub(crate) struct Wrapped {
    inner: MemStore,
    /// Files deleted just before their first read.
    pub(crate) vanish: Mutex<BTreeSet<FileKey>>,
    /// Report every listed file as empty.
    pub(crate) zero_sizes: bool,
    /// Reads begun, counted before any vanishing.
    pub(crate) reads: AtomicUsize,
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

    pub(crate) fn new(inner: MemStore) -> Self {
        Self {
            inner,
            vanish: Mutex::new(BTreeSet::new()),
            zero_sizes: false,
            reads: AtomicUsize::new(0),
        }
    }

    pub(crate) fn vanishing(inner: MemStore, keys: &[FileKey]) -> Self {
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
        self.before_read(key).await?;
        self.inner.read(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        let doomed = self.vanish.lock().unwrap().remove(&file.key);
        if doomed {
            self.inner.delete(&[file.key]).await?;
        }
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

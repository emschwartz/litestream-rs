//! The wrapped test store shared by the restore and verify tests.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_ltx::{Timestamp, TxidRange};

use super::MemStore;
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::store::WriteMode;
use crate::store::{Listing, ObjectStore, PageLimit, ReadLen, StoreError, Upload};

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
    /// Files the next listing of their level leaves out, as a directory read that races
    /// a writer's rename can (POSIX leaves a file added during a `readdir` unspecified).
    pub(crate) hide_once: Mutex<BTreeSet<FileKey>>,
    /// Files a concurrent writer puts, one right after each listing of level 0 returns: the
    /// next listing shows it, and a read made in between already finds it.
    pub(crate) put_after_list: Mutex<VecDeque<(FileKey, Vec<u8>)>>,
    /// A put in flight: it commits right after a read of its key has found nothing, as R2
    /// allows (a 404 is not final with respect to an earlier put whose outcome is unknown).
    pub(crate) put_after_miss: Mutex<Option<(FileKey, Vec<u8>)>>,
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
            hide_once: Mutex::new(BTreeSet::new()),
            put_after_list: Mutex::new(VecDeque::new()),
            put_after_miss: Mutex::new(None),
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
        let mut listing = self.inner.list(level, after, limit).await?;
        {
            let mut hidden = self.hide_once.lock().unwrap();
            listing.files.retain(|file| !hidden.remove(&file.key));
        }
        if self.zero_sizes {
            for file in &mut listing.files {
                file.size = 0;
            }
        }
        let next = if level == Level::ZERO {
            self.put_after_list.lock().unwrap().pop_front()
        } else {
            None
        };
        if let Some((key, bytes)) = next {
            let mut upload = self
                .inner
                .begin_upload(&key, Timestamp::from_unix_millis(0), WriteMode::Replace)
                .await?;
            upload.write(&bytes).await?;
            upload.commit().await?;
        }
        Ok(listing)
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.before_read(key).await?;
        let read = self.inner.read(key, offset, len).await;
        if matches!(read, Err(StoreError::NotFound { .. })) {
            let landing = {
                let mut slot = self.put_after_miss.lock().unwrap();
                slot.take_if(|(pending, _)| pending == key)
            };
            if let Some((pending, bytes)) = landing {
                let mut upload = self
                    .inner
                    .begin_upload(&pending, Timestamp::from_unix_millis(0), WriteMode::Replace)
                    .await?;
                upload.write(&bytes).await?;
                upload.commit().await?;
            }
        }
        read
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
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        self.inner.begin_upload(key, created_at, mode).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

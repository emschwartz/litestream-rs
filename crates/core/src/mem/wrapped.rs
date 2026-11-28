//! The wrapped test store shared by the restore and verify tests.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_ltx::{Timestamp, TxidRange};

use super::MemStore;
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::store::WriteMode;
use crate::store::{
    BatchKind, ListAt, Listing, ObjectStore, PageLimit, Ranged, ReadLen, ReadStream, StoreError,
    Upload,
};
use std::num::NonZeroUsize;

/// A writer's compaction or retention that runs as a read of `trigger` at offset `from` or later
/// begins: each file of `put` lands, then each key of `delete` goes.
pub(crate) struct Compaction {
    pub(crate) trigger: FileKey,
    pub(crate) from: u64,
    pub(crate) put: Vec<(FileKey, Vec<u8>)>,
    pub(crate) delete: Vec<FileKey>,
}

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
    /// Each read begun, by key and offset, in order.
    pub(crate) read_log: Mutex<Vec<(FileKey, u64)>>,
    /// Compactions waiting for their trigger; each runs once.
    pub(crate) compactions: Mutex<Vec<Compaction>>,
    /// Files the next listing of their level leaves out, as a directory read that races
    /// a writer's rename can (POSIX leaves a file added during a `readdir` unspecified).
    pub(crate) hide_once: Mutex<BTreeSet<FileKey>>,
    /// Files a concurrent writer puts, one right after each listing of level 0 returns: the
    /// next listing shows it, and a read made in between already finds it.
    pub(crate) put_after_list: Mutex<VecDeque<(FileKey, Vec<u8>)>>,
    /// A put in flight: it commits right after a read of its key has found nothing, as R2
    /// allows (a 404 is not final with respect to an earlier put whose outcome is unknown).
    pub(crate) put_after_miss: Mutex<Option<(FileKey, Vec<u8>)>>,
    /// How many pages of a batch the store runs at once ([`ObjectStore::batch_width`]).
    pub(crate) width: usize,
    /// The size of each `list_many` batch asked of it, in order.
    pub(crate) batches: Mutex<Vec<usize>>,
    /// Each span opened ([`ObjectStore::read_span`]): its key, start and end, and how many reads
    /// had begun before it.
    pub(crate) spans: Mutex<Vec<(FileKey, u64, u64, usize)>>,
    /// The size of each `created_at_many` batch asked of it, in order.
    pub(crate) created_batches: Mutex<Vec<usize>>,
    /// Each file whose created-at time was asked, by a batch or alone, in order.
    pub(crate) created_asked: Mutex<Vec<FileKey>>,
}

impl Wrapped {
    async fn before_read(&self, key: &FileKey, offset: u64) -> Result<(), StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_log.lock().unwrap().push((*key, offset));
        let doomed = self.vanish.lock().unwrap().remove(key);
        if doomed {
            self.inner.delete(&[*key]).await?;
        }
        let due = {
            let mut waiting = self.compactions.lock().unwrap();
            let at = waiting
                .iter()
                .position(|c| c.trigger == *key && offset >= c.from);
            at.map(|at| waiting.remove(at))
        };
        if let Some(compaction) = due {
            for (put, bytes) in &compaction.put {
                let mut upload = self
                    .inner
                    .begin_upload(put, Timestamp::from_unix_millis(0), WriteMode::Replace)
                    .await?;
                upload.write(bytes).await?;
                upload.commit().await?;
            }
            self.inner.delete(&compaction.delete).await?;
        }
        Ok(())
    }

    /// Reads begun of `key`, at any offset.
    pub(crate) fn reads_of(&self, key: FileKey) -> usize {
        self.read_log
            .lock()
            .unwrap()
            .iter()
            .filter(|(read, _)| *read == key)
            .count()
    }

    pub(crate) fn new(inner: MemStore) -> Self {
        Self {
            inner,
            vanish: Mutex::new(BTreeSet::new()),
            zero_sizes: false,
            reads: AtomicUsize::new(0),
            read_log: Mutex::new(Vec::new()),
            compactions: Mutex::new(Vec::new()),
            hide_once: Mutex::new(BTreeSet::new()),
            put_after_list: Mutex::new(VecDeque::new()),
            put_after_miss: Mutex::new(None),
            width: 1,
            batches: Mutex::new(Vec::new()),
            spans: Mutex::new(Vec::new()),
            created_batches: Mutex::new(Vec::new()),
            created_asked: Mutex::new(Vec::new()),
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

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    fn batch_width(&self, _kind: BatchKind) -> NonZeroUsize {
        NonZeroUsize::new(self.width).unwrap_or(NonZeroUsize::MIN)
    }

    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        assert!(pages.len() <= self.width, "a batch wider than the store");
        self.batches.lock().unwrap().push(pages.len());
        let mut out = Vec::with_capacity(pages.len());
        for at in pages {
            out.push(self.list(at.level, at.after, limit).await);
        }
        out
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
        self.before_read(key, offset).await?;
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

    async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
        assert!(files.len() <= self.width, "a batch wider than the store");
        self.created_batches.lock().unwrap().push(files.len());
        let mut out = Vec::with_capacity(files.len());
        for file in files {
            out.push(self.created_at(file).await);
        }
        out
    }

    fn read_span(&self, key: FileKey, start: u64, end: u64) -> Box<dyn ReadStream + '_> {
        let begun = self.reads.load(Ordering::Relaxed);
        self.spans.lock().unwrap().push((key, start, end, begun));
        Box::new(Ranged {
            store: self,
            key,
            offset: start,
            end: Some(end),
        })
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.created_asked.lock().unwrap().push(file.key);
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
        self.before_read(&file.key, 0).await?;
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        self.inner
            .begin_sized_upload(key, created_at, mode, len)
            .await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

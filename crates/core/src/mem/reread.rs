//! A test store with parts in miniature, and the source a test hands it.
//!
//! An upload of [`Rereads`] written from a source ([`Reread`]) keeps a tally of each chunk
//! in place of its bytes and reads them back, when it commits or (as a store whose batch of
//! parts filled does) inside the write, and publishes what the source gave it. Its plain
//! writes are a [`MemStore`]'s. Like the S3 store it refuses an upload written one way and
//! then the other.

use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use lsr_ltx::{Timestamp, TxidRange};

use super::{MemStore, MemUpload};
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::store::{
    BoxFuture, Listing, ObjectStore, PageLimit, ReadLen, Reread, StoreError, StoreOp, Upload,
    WriteMode,
};
use crate::witness::ByteTally;

/// When an upload of [`Rereads`] reads back what it was handed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum When {
    /// At the commit: an object below one part.
    #[default]
    Commit,
    /// Inside the write that handed the bytes over: a batch of parts that filled.
    Write,
}

/// What a test asks of the store.
#[derive(Debug, Default)]
pub(crate) struct Asked {
    pub(crate) when: When,
    /// The store's own failure of the next commit, reported in place of a second read's:
    /// an earlier part's request failed first.
    pub(crate) own: Option<StoreError>,
}

/// The store.
#[derive(Clone, Debug, Default)]
pub(crate) struct Rereads {
    pub(crate) inner: MemStore,
    pub(crate) asked: Arc<Mutex<Asked>>,
}

impl Rereads {
    pub(crate) fn reading_at(when: When) -> Self {
        let store = Self::default();
        store.asked().when = when;
        store
    }

    pub(crate) fn asked(&self) -> std::sync::MutexGuard<'_, Asked> {
        self.asked.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What an upload refuses with when it is written one way and then used the other.
pub(crate) const fn mixed() -> StoreError {
    StoreError::Refused {
        op: StoreOp::Upload,
        status: 0,
    }
}

pub(crate) struct RereadsUpload {
    inner: MemUpload,
    asked: Arc<Mutex<Asked>>,
    /// Where each chunk let go of starts, and its tally.
    kept: Vec<(u64, ByteTally)>,
    end: u64,
    sourced: bool,
}

impl RereadsUpload {
    /// Reads back and writes through every chunk let go of.
    async fn read_back(&mut self, again: &dyn Reread) -> Result<(), StoreError> {
        for (offset, was) in std::mem::take(&mut self.kept) {
            let bytes = again.read(offset, &was).await?;
            self.inner.write(&bytes).await?;
        }
        Ok(())
    }
}

impl Upload for RereadsUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        if self.sourced {
            return Err(mixed());
        }
        self.end = self.end.saturating_add(u64::try_from(chunk.len()).unwrap());
        self.inner.write(chunk).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        if !self.sourced && self.end > 0 {
            return Err(mixed());
        }
        self.sourced = true;
        // One read's worth to a tally, as the source gives back.
        for cell in chunk.chunks(256 * 1024) {
            let mut was = ByteTally::new();
            was.add(cell);
            self.kept.push((self.end, was));
            self.end = self.end.saturating_add(u64::try_from(cell.len()).unwrap());
        }
        let when = self.asked.lock().unwrap().when;
        match when {
            When::Write => self.read_back(again).await,
            When::Commit => Ok(()),
        }
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        if self.sourced {
            self.inner.abort().await?;
            return Err(mixed());
        }
        self.inner.commit().await
    }

    async fn commit_from(mut self, again: &dyn Reread) -> Result<FileInfo, StoreError> {
        let read = self.read_back(again).await;
        let own = self.asked.lock().unwrap().own.take();
        if let Some(err) = own.or_else(|| read.err()) {
            self.inner.abort().await?;
            return Err(err);
        }
        self.inner.commit().await
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

impl ObjectStore for Rereads {
    type Upload = RereadsUpload;

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

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<RereadsUpload, StoreError> {
        Ok(RereadsUpload {
            inner: self
                .inner
                .begin_sized_upload(key, created_at, mode, len)
                .await?,
            asked: Arc::clone(&self.asked),
            kept: Vec::new(),
            end: 0,
            sourced: false,
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

/// The bytes a test handed an upload, as the source it reads them back from, counting the
/// reads.
#[derive(Debug, Default)]
pub(crate) struct Handed {
    pub(crate) bytes: Vec<u8>,
    pub(crate) reads: AtomicU32,
}

impl Handed {
    pub(crate) fn of(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
            reads: AtomicU32::new(0),
        }
    }

    pub(crate) fn reads(&self) -> u32 {
        self.reads.load(Ordering::Relaxed)
    }
}

/// What [`Handed`] answers when it does not hold the bytes asked for.
pub(crate) const fn lost() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Read,
        kind: io::ErrorKind::InvalidData,
    }
}

impl Reread for Handed {
    fn read<'a>(
        &'a self,
        offset: u64,
        was: &'a ByteTally,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let start = usize::try_from(offset).unwrap();
        let end = start.saturating_add(usize::try_from(was.size()).unwrap());
        let bytes = self.bytes.get(start..end).map(<[u8]>::to_vec);
        Box::pin(async move {
            let bytes = bytes.ok_or_else(lost)?;
            let mut now = ByteTally::new();
            now.add(&bytes);
            if now == *was { Ok(bytes) } else { Err(lost()) }
        })
    }
}

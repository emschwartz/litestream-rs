//! A test store with parts in miniature, and the source a test hands it.
//!
//! An upload of [`Rereads`] written from a source ([`Reread`]) keeps a tally of each chunk
//! in place of its bytes and reads them back, when it commits or (as a store whose batch of
//! parts filled does) inside the write, and publishes what the source gave it. Its plain
//! writes are a [`MemStore`]'s. Like the S3 store it refuses an upload written one way and
//! then the other.

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use lsr_ltx::{Timestamp, TxidRange};

use super::{MemStore, MemUpload};
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::store::{
    Again, BoxFuture, Flight, Listing, ObjectStore, PageLimit, ReadLen, Reread, Rest, Seen,
    StoreError, StoreOp, Upload, WriteMode,
};

/// When an upload of [`Rereads`] reads back what it was handed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum When {
    /// At the commit: an object below one part.
    #[default]
    Commit,
    /// Inside the write that handed the bytes over: a batch of parts that filled.
    Write,
    /// When it is next asked about its parts: a part sent while the caller did other work.
    Flight,
}

/// What a test asks of the store.
#[derive(Debug, Default)]
pub(crate) struct Asked {
    pub(crate) when: When,
    /// The store's own failure of the next commit, reported in place of a second read's:
    /// an earlier part's request failed first.
    pub(crate) own: Option<StoreError>,
    /// What the next questions about an upload's parts are answered, in order; with none
    /// left, that no part is in flight.
    pub(crate) flights: VecDeque<Result<Flight, StoreError>>,
    /// Every question asked so far.
    pub(crate) rests: Vec<Rest>,
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
    /// Where each chunk let go of starts, and what was seen of it.
    kept: Vec<(u64, Seen)>,
    end: u64,
    /// The source's reader, taken at the first write from a source.
    reader: Option<Box<dyn Again>>,
}

impl RereadsUpload {
    /// Reads back and writes through every chunk let go of.
    async fn read_back(&mut self) -> Result<(), StoreError> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(());
        };
        for (offset, was) in std::mem::take(&mut self.kept) {
            let bytes = reader.read(offset, &was).await?;
            self.inner.write(&bytes).await?;
        }
        Ok(())
    }
}

impl Upload for RereadsUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        if self.reader.is_some() {
            return Err(mixed());
        }
        self.end = self.end.saturating_add(u64::try_from(chunk.len()).unwrap());
        self.inner.write(chunk).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        if self.reader.is_none() {
            if self.end > 0 {
                return Err(mixed());
            }
            self.reader = Some(again.open().await?);
        }
        // One read's worth at a time, as the source gives back.
        for cell in chunk.chunks(256 * 1024) {
            self.kept.push((self.end, Seen::of(cell)));
            self.end = self.end.saturating_add(u64::try_from(cell.len()).unwrap());
        }
        let when = self.asked.lock().unwrap().when;
        match when {
            When::Write => self.read_back().await,
            When::Commit | When::Flight => Ok(()),
        }
    }

    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        let (when, scripted) = {
            let mut asked = self.asked.lock().unwrap();
            asked.rests.push(rest);
            (asked.when, asked.flights.pop_front())
        };
        if when == When::Flight {
            self.read_back().await?;
        }
        scripted.unwrap_or(Ok(Flight::Idle))
    }

    async fn commit(mut self) -> Result<FileInfo, StoreError> {
        let read = self.read_back().await;
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
            reader: None,
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

/// The bytes a test handed an upload, as the source it reads them back from, counting the
/// reads and the readers opened.
#[derive(Clone, Debug, Default)]
pub(crate) struct Handed {
    pub(crate) bytes: Arc<Vec<u8>>,
    pub(crate) reads: Arc<AtomicU32>,
    pub(crate) opens: Arc<AtomicU32>,
}

impl Handed {
    pub(crate) fn of(bytes: &[u8]) -> Self {
        Self {
            bytes: Arc::new(bytes.to_vec()),
            ..Self::default()
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
    fn open(&self) -> BoxFuture<'_, Result<Box<dyn Again>, StoreError>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        let reader: Box<dyn Again> = Box::new(self.clone());
        Box::pin(async move { Ok(reader) })
    }
}

impl Again for Handed {
    fn read<'a>(
        &'a mut self,
        offset: u64,
        was: &'a Seen,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let start = usize::try_from(offset).unwrap();
        let end = start.saturating_add(usize::try_from(was.len()).unwrap());
        let bytes = self.bytes.get(start..end).map(<[u8]>::to_vec);
        Box::pin(async move {
            let bytes = bytes.ok_or_else(lost)?;
            if Seen::of(&bytes) == *was {
                Ok(bytes)
            } else {
                Err(lost())
            }
        })
    }
}

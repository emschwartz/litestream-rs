//! A test store whose commits acknowledge one byte fewer than was written: the
//! short write a store took as whole, which every engine commit must catch.

use lsr_ltx::{Timestamp, TxidRange};

use super::MemStore;
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::store::{Listing, ObjectStore, PageLimit, ReadLen, StoreError, Upload, WriteMode};

/// A store whose commits report a size other than the one written.
#[derive(Debug)]
pub(crate) struct SizeLiar(pub(crate) MemStore);

pub(crate) struct LyingUpload(<MemStore as ObjectStore>::Upload);

impl ObjectStore for SizeLiar {
    type Upload = LyingUpload;

    /// Leaves no scratch.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        Ok(0)
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.0.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.0.read(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.0.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.0.read_with_created_at(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<LyingUpload, StoreError> {
        Ok(LyingUpload(self.0.begin_upload(key, created_at, mode).await?))
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.0.delete(keys).await
    }
}

impl Upload for LyingUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.0.write(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let mut info = self.0.commit().await?;
        info.size = info.size.saturating_sub(1);
        Ok(info)
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.0.abort().await
    }
}

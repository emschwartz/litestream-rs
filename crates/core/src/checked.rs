//! The one commit every engine write goes through.
//!
//! A store's commit answers with the [`FileInfo`] it says it now holds. A store
//! that took a short write as whole answers with fewer bytes than were sent,
//! and the object under the key does not decode. [`Checked`] counts what was
//! written and compares the answer with it: on a mismatch it deletes the object
//! before it reports [`CorruptCause::CommitSizeMismatch`], so the engine never
//! leaves a listable object it knows is short, and never takes one as
//! published. `removed: false` (the delete failed too) tells the caller not to
//! trust a listing of that key.
//!
//! The uploader, the compactor and the staging writer open their uploads only
//! through [`Checked::begin`], so none can skip the check; [`crate::Retrying`]
//! settles its own commits with the same [`settle`].

use std::future::Future;

use lsr_ltx::Timestamp;

use crate::layout::{FileInfo, FileKey};
use crate::store::{CorruptCause, ObjectStore, StoreError, Upload, WriteMode};

/// An upload into `store` whose commit is checked against the bytes written.
#[derive(Debug)]
pub(crate) struct Checked<'s, S: ObjectStore> {
    store: &'s S,
    key: FileKey,
    upload: S::Upload,
    written: u64,
}

impl<'s, S: ObjectStore> Checked<'s, S> {
    /// Opens an upload of `key` in `store`, which `mode` says may or may not
    /// replace an object already under the key.
    ///
    /// # Errors
    /// What the store's `begin_upload` returned.
    pub(crate) async fn begin(
        store: &'s S,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self, StoreError> {
        let upload = store.begin_upload(key, created_at, mode).await?;
        Ok(Self {
            store,
            key: *key,
            upload,
            written: 0,
        })
    }
}

impl<S: ObjectStore> Upload for Checked<'_, S> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.upload.write(chunk).await?;
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.written = self.written.saturating_add(len);
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let info = self.upload.commit().await?;
        let (store, keys) = (self.store, [self.key]);
        settle(info, self.key, self.written, || store.delete(&keys)).await
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.upload.abort().await
    }
}

/// `info` when the store holds the `written` bytes it was sent under `key`;
/// otherwise the object is deleted through `delete` and the mismatch reported,
/// saying whether the delete took.
///
/// # Errors
/// [`StoreError::Corrupt`] with [`CorruptCause::CommitSizeMismatch`].
pub(crate) async fn settle<D, F>(
    info: FileInfo,
    key: FileKey,
    written: u64,
    delete: D,
) -> Result<FileInfo, StoreError>
where
    D: FnOnce() -> F,
    F: Future<Output = Result<(), StoreError>>,
{
    if info.size == written {
        return Ok(info);
    }
    // Safe to delete: the caller still holds what it wrote until a later
    // verified listing, so the cost of a wrong delete is a rewrite.
    let removed = delete().await.is_ok();
    Err(StoreError::Corrupt {
        key: Some(key),
        cause: CorruptCause::CommitSizeMismatch {
            wrote: written,
            stored: info.size,
            removed,
        },
    })
}

#[cfg(test)]
mod tests;

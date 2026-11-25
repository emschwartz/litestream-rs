//! The one place the engine writes to and deletes from a store.
//!
//! **Writes.** A store's commit answers with the [`FileInfo`] it says it now
//! holds. A store that took a short write as whole answers with fewer bytes
//! than were sent, and the object under the key does not decode. [`Writer`]
//! counts what was written and compares the answer with it, and never takes a
//! short object as published: it reports [`CorruptCause::CommitSizeMismatch`].
//! After a create-only write it deletes the object first, since the key held
//! nothing before. After a replace it deletes nothing (`removed: false`): the
//! key may have held an acknowledged object, and a delete would end the
//! replica a transaction early with no error, where the short object makes a
//! restore stop with a typed one until the caller writes it again or removes
//! it under a [`Removal`] reason. `removed: false` tells the caller not to
//! trust a listing of that key. A commit that holds is a
//! [`Published`], which says whether the write created the object or replaced
//! one; [`ours`] makes the third kind, a key a read showed already holds our
//! bytes.
//!
//! **Deletes after a failed write.** A store that rejects the bytes it took
//! (a digest that does not match) leaves them listed. Only a create-only
//! write ([`WriteMode::New`]) can be sure the rejected object is its own: the
//! bucket refuses a taken key before it takes any bytes. So only such a
//! commit hands back a [`Rejected`], and [`remove_rejected`] deletes nothing
//! else. A rejected replace deletes nothing; its retry writes the key again.
//!
//! **Every other delete** names its reason ([`Removal`]): retention, a rotten
//! object a later file covers, a rejected snapshot or compaction output.
//! Staged files go through [`remove_staged`].
//!
//! No other module of the engine calls a store's `begin_upload`, `commit` or
//! `delete` (`publish::guard`, run by `just check`, counts them); store
//! implementations and the wrappers that pass calls through are the
//! exceptions it lists. [`crate::Retrying`] settles its own commits with the
//! same [`settle`].

use std::future::Future;

use lsr_ltx::Timestamp;

use crate::layout::{FileInfo, FileKey};
use crate::store::{CorruptCause, DynUpload, ObjectStore, Reread, StoreError, Upload, WriteMode};
use crate::upload::Unsent;

/// An upload whose commit is checked against the bytes written. It borrows no store, so one
/// can be kept between ticks; the commit is handed the store it was begun in.
pub(crate) struct Writer<U> {
    key: FileKey,
    upload: U,
    written: u64,
    mode: WriteMode,
}

/// An upload begun and not committed, of any store: what the engine keeps of a file part-way
/// sent. Dropping one does no I/O and leaves scratch the store's `remove_scratch` clears at
/// the next boot, as a crash does.
pub(crate) type Open = Writer<Box<dyn DynUpload>>;

impl<U> std::fmt::Debug for Writer<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("key", &self.key)
            .field("written", &self.written)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// How a key came to hold the engine's bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HowPublished {
    /// A create-only write the store accepted: the key was free.
    Created,
    /// A write that may have replaced an object under the key.
    Replaced,
}

/// A key that holds the engine's bytes, as a commit checked against the bytes
/// written showed. Only this module makes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Published {
    info: FileInfo,
    how: HowPublished,
}

impl Published {
    /// What the store says it holds.
    pub(crate) const fn info(&self) -> FileInfo {
        self.info
    }

    /// The key.
    pub(crate) const fn key(&self) -> FileKey {
        self.info.key
    }
}

/// The object a rejected create-only commit left: the only object a delete
/// after a failed write may take. Only [`Writer::commit`] makes one.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a rejected object stays listed until it is removed"]
pub(crate) struct Rejected(FileKey);

/// Why a commit did not publish.
#[derive(Debug)]
pub(crate) enum Unpublished {
    /// A create-only write found the key taken; nothing was written.
    Taken(StoreError),
    /// The store took the bytes and says they are not the bytes sent. A
    /// create-only write's object is its own, so it can go ([`remove_rejected`]).
    Rejected(StoreError, Option<Rejected>),
    /// Any other failure, including a short write already removed.
    Failed(StoreError),
}

impl Unpublished {
    /// The store's error.
    pub(crate) const fn error(&self) -> &StoreError {
        match self {
            Self::Taken(err) | Self::Rejected(err, _) | Self::Failed(err) => err,
        }
    }

    /// The store's error, the rejected object dropped.
    pub(crate) fn into_error(self) -> StoreError {
        match self {
            Self::Taken(err) | Self::Rejected(err, _) | Self::Failed(err) => err,
        }
    }
}

impl<U: Upload> Writer<U> {
    /// Opens an upload of `key` in `store`, which `mode` says may or may not
    /// replace an object already under the key.
    ///
    /// # Errors
    /// What the store's `begin_upload` returned.
    pub(crate) async fn begin<S: ObjectStore<Upload = U>>(
        store: &S,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self, StoreError> {
        let upload = store.begin_sized_upload(key, created_at, mode, len).await?;
        Ok(Self {
            key: *key,
            upload,
            written: 0,
            mode,
        })
    }

    /// The same upload with its store's type erased.
    pub(crate) fn boxed(self) -> Open
    where
        U: 'static,
    {
        let upload: Box<dyn DynUpload> = Box::new(self.upload);
        Writer {
            key: self.key,
            upload,
            written: self.written,
            mode: self.mode,
        }
    }

    /// Appends a chunk.
    ///
    /// # Errors
    /// What the store's write returned.
    pub(crate) async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.upload.write(chunk).await?;
        self.wrote(chunk);
        Ok(())
    }

    /// Appends a chunk the store may let go of and read back through `again`
    /// ([`Upload::write_from`]).
    ///
    /// # Errors
    /// What the store's write returned.
    pub(crate) async fn write_from(
        &mut self,
        chunk: &[u8],
        again: &dyn Reread,
    ) -> Result<(), StoreError> {
        self.upload.write_from(chunk, again).await?;
        self.wrote(chunk);
        Ok(())
    }

    fn wrote(&mut self, chunk: &[u8]) {
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.written = self.written.saturating_add(len);
    }

    /// Discards the written bytes.
    ///
    /// # Errors
    /// What the store's abort returned.
    pub(crate) async fn abort(self) -> Result<(), StoreError> {
        self.upload.abort().await
    }

    /// Publishes the object into `store`, the one it was begun in, checked against the bytes
    /// written.
    ///
    /// # Errors
    /// [`Unpublished`], saying what the store left under the key.
    pub(crate) async fn commit<S: ObjectStore>(self, store: &S) -> Result<Published, Unpublished> {
        self.commit_with(store, None).await
    }

    /// [`Self::commit`] of an upload written with [`Self::write_from`].
    ///
    /// # Errors
    /// As [`Self::commit`].
    pub(crate) async fn commit_from<S: ObjectStore>(
        self,
        store: &S,
        again: &dyn Reread,
    ) -> Result<Published, Unpublished> {
        self.commit_with(store, Some(again)).await
    }

    async fn commit_with<S: ObjectStore>(
        self,
        store: &S,
        again: Option<&dyn Reread>,
    ) -> Result<Published, Unpublished> {
        let Self {
            key,
            upload,
            written,
            mode,
        } = self;
        let committed = match again {
            Some(again) => upload.commit_from(again).await,
            None => upload.commit().await,
        };
        let info = match committed {
            Ok(info) => info,
            Err(err) => return Err(unpublished(err, key, mode)),
        };
        let keys = [key];
        let info = settle(info, key, written, mode, || store.delete(&keys))
            .await
            .map_err(Unpublished::Failed)?;
        let how = match mode {
            WriteMode::New => HowPublished::Created,
            WriteMode::Replace => HowPublished::Replaced,
        };
        Ok(Published { info, how })
    }
}

/// What a failed commit of `key`, written under `mode`, left.
fn unpublished(err: StoreError, key: FileKey, mode: WriteMode) -> Unpublished {
    let rejected = matches!(
        err,
        StoreError::Corrupt {
            cause: CorruptCause::ChecksumMismatch { .. },
            ..
        }
    );
    match (&err, mode) {
        (StoreError::AlreadyExists { key: taken }, WriteMode::New) if *taken == key => {
            Unpublished::Taken(err)
        }
        _ if rejected => {
            // A part that came back wrong ended the upload before its commit asked the store
            // for the key, so whatever the key holds is not this upload's to remove.
            let whole = matches!(
                err,
                StoreError::Corrupt {
                    cause: CorruptCause::ChecksumMismatch { part: None },
                    ..
                }
            );
            let own = (mode == WriteMode::New && whole).then_some(Rejected(key));
            Unpublished::Rejected(err, own)
        }
        _ => Unpublished::Failed(err),
    }
}

/// Deletes the object a rejected create-only commit left.
///
/// # Errors
/// What the store's delete returned; the object may still be listed.
pub(crate) async fn remove_rejected<S: ObjectStore>(
    store: &S,
    rejected: Rejected,
) -> Result<(), StoreError> {
    store.delete(&[rejected.0]).await
}

/// Why replica objects are deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Why {
    /// Retention: later files cover them and their window has passed.
    Retention,
    /// A read-back found the object rotten, twice, and a later file covers it.
    RottenCovered,
    /// A snapshot the store took and then rejected.
    RejectedSnapshot,
    /// A compaction output the store took and then rejected.
    RejectedOutput,
}

/// Replica objects to delete, with the reason. Item 4 of issue #171 replaces
/// the reasons that rest on a listing with a proof built from a read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Removal<'k> {
    keys: &'k [FileKey],
    why: Why,
}

impl<'k> Removal<'k> {
    /// `keys` deleted for `why`.
    pub(crate) const fn new(keys: &'k [FileKey], why: Why) -> Self {
        Self { keys, why }
    }
}

/// Deletes the replica objects `removal` names.
///
/// # Errors
/// What the store's delete returned.
pub(crate) async fn remove<S: ObjectStore>(
    store: &S,
    removal: &Removal<'_>,
) -> Result<(), StoreError> {
    store.delete(removal.keys).await
}

/// Deletes the one replica object `key`, for `why`.
///
/// # Errors
/// What the store's delete returned.
pub(crate) async fn remove_one<S: ObjectStore>(
    store: &S,
    key: FileKey,
    why: Why,
) -> Result<(), StoreError> {
    remove(store, &Removal::new(&[key], why)).await
}

/// Deletes staged files: the staging directory's own, which nothing else
/// writes. `_unsent` is the upload task's word that it is not part-way through sending one
/// of them ([`Unsent`]).
///
/// # Errors
/// What the store's delete returned.
pub(crate) async fn remove_staged<L: ObjectStore>(
    staging: &L,
    keys: &[FileKey],
    _unsent: &Unsent<'_>,
) -> Result<(), StoreError> {
    staging.delete(keys).await
}

/// `info` when the store holds the `written` bytes it was sent under `key`;
/// otherwise the mismatch is reported, saying whether the object is gone. Only
/// a create-only write (`mode`) deletes it, through `delete`: a replace never
/// takes a key that may have held something before it.
///
/// # Errors
/// [`StoreError::Corrupt`] with [`CorruptCause::CommitSizeMismatch`].
pub(crate) async fn settle<D, F>(
    info: FileInfo,
    key: FileKey,
    written: u64,
    mode: WriteMode,
    delete: D,
) -> Result<FileInfo, StoreError>
where
    D: FnOnce() -> F,
    F: Future<Output = Result<(), StoreError>>,
{
    if info.size == written {
        return Ok(info);
    }
    let removed = match mode {
        // A create-only write found the key free: the object is ours alone, and the caller still
        // holds what it wrote until a later verified listing, so a wrong delete costs a rewrite.
        WriteMode::New => delete().await.is_ok(),
        // A replace may land on a key that held an acknowledged object. Deleting what the short
        // commit left would end the replica a transaction early with no error (seed 0x1dc); the
        // object stays, a restore that meets it stops with a typed error, and the caller writes
        // it again or removes it under a reason of its own (`Removal`).
        WriteMode::Replace => false,
    };
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
mod guard;
#[cfg(test)]
mod tests;

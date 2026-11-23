//! The one place the engine writes to and deletes from a store.
//!
//! **Writes.** A store's commit answers with the [`FileInfo`] it says it now
//! holds. A store that took a short write as whole answers with fewer bytes
//! than were sent, and the object under the key does not decode. [`Writer`]
//! counts what was written and compares the answer with it: on a mismatch it
//! deletes the object before it reports [`CorruptCause::CommitSizeMismatch`],
//! so the engine never leaves a listable object it knows is short, and never
//! takes one as published. `removed: false` (the delete failed too) tells the
//! caller not to trust a listing of that key. A commit that holds is a
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
use crate::store::{CorruptCause, ObjectStore, StoreError, Upload, WriteMode};

/// An upload into `store` whose commit is checked against the bytes written.
#[derive(Debug)]
pub(crate) struct Writer<'s, S: ObjectStore> {
    store: &'s S,
    key: FileKey,
    upload: S::Upload,
    written: u64,
    mode: WriteMode,
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

impl<'s, S: ObjectStore> Writer<'s, S> {
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
            mode,
        })
    }

    /// Appends a chunk.
    ///
    /// # Errors
    /// What the store's write returned.
    pub(crate) async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.upload.write(chunk).await?;
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.written = self.written.saturating_add(len);
        Ok(())
    }

    /// Discards the written bytes.
    ///
    /// # Errors
    /// What the store's abort returned.
    pub(crate) async fn abort(self) -> Result<(), StoreError> {
        self.upload.abort().await
    }

    /// Publishes the object, checked against the bytes written.
    ///
    /// # Errors
    /// [`Unpublished`], saying what the store left under the key.
    pub(crate) async fn commit(self) -> Result<Published, Unpublished> {
        let Self {
            store,
            key,
            upload,
            written,
            mode,
        } = self;
        let info = match upload.commit().await {
            Ok(info) => info,
            Err(err) => return Err(unpublished(err, key, mode)),
        };
        let keys = [key];
        let info = settle(info, key, written, || store.delete(&keys))
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
            let own = (mode == WriteMode::New).then_some(Rejected(key));
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
/// writes.
///
/// # Errors
/// What the store's delete returned.
pub(crate) async fn remove_staged<L: ObjectStore>(
    staging: &L,
    keys: &[FileKey],
) -> Result<(), StoreError> {
    staging.delete(keys).await
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
mod guard;
#[cfg(test)]
mod tests;

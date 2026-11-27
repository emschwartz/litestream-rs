//! The one place the engine writes to and deletes from a store.
//!
//! **Writes.** A store's commit answers with the [`FileInfo`] it says it now
//! holds. A store that took a short write as whole answers with fewer bytes
//! than were sent, and the object under the key does not decode. [`Writer`]
//! counts what was written and compares the answer with it, and never takes a
//! short object as published: it reports [`CorruptCause::CommitSizeMismatch`].
//! After a create-only write it deletes the object, since the key held nothing before.
//! After a replace it deletes nothing (`removed: false`): the key may have held an
//! acknowledged object, and a delete would end the replica a transaction early with no error,
//! where the short object makes a restore stop with a typed one until the caller writes it
//! again or removes it under a [`Removal`] reason. `removed: false` tells the caller not to
//! trust a listing of that key. A commit that holds is a [`Published`], which says whether
//! the write created the object or replaced one; [`ours`] makes the third kind, a key a read
//! showed already holds our bytes.
//!
//! **No delete after a rejected write.** A store that says the bytes are not the bytes sent
//! ([`CorruptCause::ChecksumMismatch`]) is never answered with a delete of the key, in either
//! mode, as Go deletes nothing after a failed write. S3 refuses a body against its signed hash
//! before it looks at a create-only condition, so the refusal says nothing about the key: it may
//! hold an earlier attempt's object that an ambiguous commit published, and a delete would take
//! that transaction off the replica. The caller writes the key again; a create-only retry that
//! finds it taken reads it back (`docs/decisions/D280.md`).
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
use std::num::NonZeroU64;

use lsr_ltx::Timestamp;

use crate::layout::{FileInfo, FileKey};
use crate::store::{
    CorruptCause, DynUpload, Flight, ObjectStore, Reread, Rest, StoreError, Upload, WriteMode,
};
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

/// The send half of a sliced upload, shared by the snapshot's and the compaction's slice
/// loops: what was handed over, and whether the last slice stopped for the upload's parts
/// (`docs/decisions/D233.md`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Handoff {
    /// How many bytes were handed to the replica.
    pub(crate) sent: u64,
    /// The last slice stopped for the upload's parts.
    pub(crate) waits: bool,
}

impl Handoff {
    /// Whether a slice may go on handing over bytes (`rest`: [`Upload::flight`]). `patient`
    /// waits for the parts inside the write instead, so it never asks. When the parts are in
    /// the way, `waits` says the slice stopped for them.
    ///
    /// # Errors
    /// What the store's answer was.
    pub(crate) async fn clear(
        &mut self,
        open: &mut Open,
        rest: Rest,
        patient: bool,
    ) -> Result<bool, StoreError> {
        if patient {
            return Ok(true);
        }
        let flight = open.flight(rest).await?;
        let clear = match rest {
            Rest::More => flight != Flight::Full,
            Rest::None => flight == Flight::Idle,
        };
        self.waits = !clear;
        Ok(clear)
    }

    /// Hands over the first `room` bytes of `out` at most, and drains what went. The room
    /// left, or `None` once it is spent.
    ///
    /// # Errors
    /// What the store's write returned.
    pub(crate) async fn send(
        &mut self,
        open: &mut Open,
        out: &mut Vec<u8>,
        room: NonZeroU64,
    ) -> Result<Option<NonZeroU64>, StoreError> {
        let n = out
            .len()
            .min(usize::try_from(room.get()).unwrap_or(usize::MAX));
        open.write(out.get(..n).unwrap_or_default()).await?;
        out.drain(..n);
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        self.sent = self.sent.saturating_add(n);
        Ok(NonZeroU64::new(room.get().saturating_sub(n)))
    }
}

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

/// Why a commit did not publish.
#[derive(Debug)]
pub(crate) enum Unpublished {
    /// A create-only write found the key taken; nothing was written.
    Taken(StoreError),
    /// The store says the bytes are not the bytes sent. It may have published nothing (S3
    /// refuses the body), so the key holds whatever it held before, and nothing is deleted.
    Rejected(StoreError),
    /// Any other failure, including a short write already removed.
    Failed(StoreError),
}

impl Unpublished {
    /// The store's error.
    pub(crate) const fn error(&self) -> &StoreError {
        match self {
            Self::Taken(err) | Self::Rejected(err) | Self::Failed(err) => err,
        }
    }

    /// The store's error, by value.
    pub(crate) fn into_error(self) -> StoreError {
        match self {
            Self::Taken(err) | Self::Rejected(err) | Self::Failed(err) => err,
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

    /// Where the upload's parts are ([`Upload::flight`]).
    ///
    /// # Errors
    /// What the store's answer was.
    pub(crate) async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        self.upload.flight(rest).await
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
        let Self {
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
        _ if rejected => Unpublished::Rejected(err),
        _ => Unpublished::Failed(err),
    }
}

/// Why replica objects are deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Why {
    /// Retention: later files cover them and their window has passed.
    Retention,
    /// A read-back found the object rotten, twice, and a later file covers it.
    RottenCovered,
    /// A snapshot the store took at a size other than the one sent.
    RejectedSnapshot,
    /// A compaction output the store took at a size other than the one sent.
    RejectedOutput,
}

/// Replica objects to delete, with the reason.
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
        // A replace may land on a key that held an acknowledged object. Deleting would end the
        // replica a transaction early with no error; the object stays and a restore that meets it
        // stops with a typed error (see the module doc).
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

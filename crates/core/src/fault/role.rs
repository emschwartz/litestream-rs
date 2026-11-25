//! Which store an error came from, carried by the store's type.
//!
//! The same [`StoreError`] means one thing from the staging directory, a local
//! directory nothing else writes, and another from the replica: a missing
//! staged file is lost, a missing replica object means list again; a refused
//! local read is the directory's permissions, a refused replica read is a
//! credential. Engine code reaches a store only as [`Staging`] or
//! [`Replica`], and turns its errors into a [`Fault`] only through
//! [`Role::fault`], so an error cannot be classified as the other store's: the
//! two classifiers are private to `fault` (the source guard in
//! `publish::guard` counts any other classification).
//!
//! Both wrappers are stores themselves, passing every call through, so code
//! that only moves bytes takes either.

use std::num::NonZeroUsize;

use lsr_ltx::{Timestamp, TxidRange};

use super::Fault;
use crate::compact::CompactError;
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::store::{
    BatchKind, ListAt, Listing, ObjectStore, Opened, PageLimit, ReadAt, ReadLen, StoreError,
    WriteMode,
};

/// A store whose errors mean what they mean on one side of the engine.
pub(crate) trait Role: ObjectStore {
    /// The fault an error from this store is.
    fn fault(err: &StoreError) -> Fault;

    /// Compaction's error for a failed read of an input from this store.
    fn compact_input(err: StoreError) -> CompactError;

    /// The most bytes a compaction asks of this store per read of an input. A merge holds
    /// one such read for every input at once, so this times the fan-in is its memory
    /// unless [`Self::COMPACT_BUDGET`] cuts it. The staging directory answers a read from
    /// the page cache for the price of a system call, so a small window costs it nothing;
    /// a replica answers each read with a request.
    const COMPACT_WINDOW: ReadLen;

    /// Bytes the reads of all inputs may hold at once. Each input's window is its share of
    /// this, at most [`Self::COMPACT_WINDOW`] and at least [`STAGING_WINDOW`]: a merge of
    /// a few inputs reads in big windows (few requests), a merge of 32 in small ones
    /// (`docs/decisions/D212.md`).
    const COMPACT_BUDGET: u64;
}

/// What a compaction reads of each staged file at a time: twice the largest page.
pub(crate) const STAGING_WINDOW: ReadLen = ReadLen::or_chunk(128 * 1024);

/// The staging directory.
#[derive(Debug)]
pub(crate) struct Staging<'a, S>(pub(crate) &'a S);

/// The replica.
#[derive(Debug)]
pub(crate) struct Replica<'a, S>(pub(crate) &'a S);

impl<S> Clone for Staging<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Staging<'_, S> {}

impl<S> Clone for Replica<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Replica<'_, S> {}

impl<S: ObjectStore> Role for Staging<'_, S> {
    fn fault(err: &StoreError) -> Fault {
        Fault::from_staging(err)
    }

    fn compact_input(err: StoreError) -> CompactError {
        CompactError::Local(err)
    }

    const COMPACT_WINDOW: ReadLen = STAGING_WINDOW;
    const COMPACT_BUDGET: u64 = 4 * 1024 * 1024;
}

impl<S: ObjectStore> Role for Replica<'_, S> {
    fn fault(err: &StoreError) -> Fault {
        Fault::from_remote(err)
    }

    fn compact_input(err: StoreError) -> CompactError {
        CompactError::Source(err)
    }

    const COMPACT_WINDOW: ReadLen = ReadLen::CHUNK;
    const COMPACT_BUDGET: u64 = 16 * 1024 * 1024;
}

/// Passes every call of a store through to the wrapped one.
macro_rules! pass_through {
    ($wrapper:ident) => {
        impl<S: ObjectStore> ObjectStore for $wrapper<'_, S> {
            type Upload = S::Upload;

            async fn list(
                &self,
                level: Level,
                after: Option<TxidRange>,
                limit: PageLimit,
            ) -> Result<Listing, StoreError> {
                self.0.list(level, after, limit).await
            }

            async fn read(
                &self,
                key: &FileKey,
                offset: u64,
                len: ReadLen,
            ) -> Result<Vec<u8>, StoreError> {
                self.0.read(key, offset, len).await
            }

            async fn open(&self, key: &FileKey) -> Result<Option<Box<dyn Opened>>, StoreError> {
                self.0.open(key).await
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

            async fn begin_sized_upload(
                &self,
                key: &FileKey,
                created_at: Timestamp,
                mode: WriteMode,
                len: Option<u64>,
            ) -> Result<S::Upload, StoreError> {
                self.0.begin_sized_upload(key, created_at, mode, len).await
            }

            async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
                self.0.delete(keys).await
            }

            async fn remove_scratch(&self) -> Result<u64, StoreError> {
                self.0.remove_scratch().await
            }

            fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
                self.0.batch_width(kind)
            }

            async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
                self.0.read_many(reads).await
            }

            async fn created_at_many(
                &self,
                files: &[FileInfo],
            ) -> Vec<Result<CreatedAt, StoreError>> {
                self.0.created_at_many(files).await
            }

            async fn list_many(
                &self,
                pages: &[ListAt],
                limit: PageLimit,
            ) -> Vec<Result<Listing, StoreError>> {
                self.0.list_many(pages, limit).await
            }
        }
    };
}

pass_through!(Staging);
pass_through!(Replica);

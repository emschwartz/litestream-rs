//! The object-store contract: what a replica backend must do, and how it may fail.
//!
//! Every backend (directory, S3, the in-memory model) implements
//! [`ObjectStore`]; the restore and sync code sees only this trait. The shared
//! suite in `conformance` pins the behaviour all of them must have.
//!
//! The rules a caller can lean on:
//! - A listing is ascending by key and paged. A page that cannot be produced
//!   is an error, never a shorter listing.
//! - A read returns at most the bytes asked for and is short only at the end of
//!   the object.
//! - An upload is invisible until `commit` is called and whole afterwards.
//!   Dropping an [`Upload`] does no I/O and publishes nothing; the store may be
//!   left holding scratch data that no listing or read can see.
//! - When `commit` returns an error the object may or may not be visible: a
//!   response can be lost after the write landed, on S3 as on a disk whose
//!   directory sync failed after the rename. The caller's only safe move is to
//!   list again and trust the listing.
//! - Retrying is the caller's decision, made from the [`StoreError`] variant.
//! - Every method returns within a deadline the implementation enforces, failing as
//!   `Transient`; core holds no timer.

use std::fmt;
use std::future::Future;
use std::io;
use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use lsr_ltx::{Timestamp, TxidRange};
use thiserror::Error;

use crate::layout::{FileInfo, FileKey, Level, cmp_range, file_name, is_scratch_name};

/// How many entries one listing page may hold: 1 to 1000, S3's own cap, so
/// every backend can return exactly the page asked for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PageLimit(NonZeroU16);

impl PageLimit {
    /// The largest page.
    pub const MAX_RAW: u16 = 1000;
    /// The largest page as a limit; equals [`Self::MAX_RAW`] (pinned by a test).
    pub const MAX: Self = Self(NonZeroU16::MIN.saturating_add(999));

    /// Returns `None` for zero or above [`Self::MAX_RAW`].
    #[must_use]
    pub fn new(raw: u16) -> Option<Self> {
        NonZeroU16::new(raw)
            .filter(|n| n.get() <= Self::MAX_RAW)
            .map(Self)
    }

    /// The limit as a count.
    #[must_use]
    pub fn get(self) -> usize {
        usize::from(self.0.get())
    }
}

/// How many bytes one read may ask for: 1 to 16 MiB. The bound is what keeps
/// a read's memory independent of the object's size.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadLen(NonZeroU32);

impl ReadLen {
    /// The largest read.
    pub const MAX_RAW: u32 = 16_777_216;

    /// Returns `None` for zero or above [`Self::MAX_RAW`].
    #[must_use]
    pub fn new(raw: u32) -> Option<Self> {
        NonZeroU32::new(raw)
            .filter(|n| n.get() <= Self::MAX_RAW)
            .map(Self)
    }

    /// The length as a byte count.
    #[must_use]
    pub fn get(self) -> u64 {
        u64::from(self.0.get())
    }
}

/// One page of a listing.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Listing {
    /// The page's files, ascending by key, at most the limit.
    pub files: Vec<FileInfo>,
    /// The cursor for the next page: `Some` exactly when at least one more key
    /// follows, so a caller never fetches an empty trailing page.
    pub next: Option<TxidRange>,
    /// Names in the level that are not LTX keys and sort within this page's
    /// span (after the cursor, up to the page's last name; to the end on the
    /// final page). Summed over a full paging, every foreign name counts once.
    pub skipped: usize,
}

impl Listing {
    /// A listing of an empty or absent level.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            files: Vec::new(),
            next: None,
            skipped: 0,
        }
    }
}

/// Cuts one page out of everything a level holds.
///
/// Stores that can enumerate a whole level (a directory, a map) call this so
/// the paging and foreign-name rules live in one place. Foreign names are
/// ordered against key names byte-wise, which is key order because the
/// transaction ids are fixed-width. A scratch name ([`is_scratch_name`]) in
/// `foreign` is dropped: it is not foreign.
#[must_use]
pub fn build_listing(
    mut files: Vec<FileInfo>,
    foreign: &[String],
    after: Option<TxidRange>,
    limit: PageLimit,
) -> Listing {
    files.sort_by_key(|info| info.key);
    if let Some(cursor) = after {
        files.retain(|info| cmp_range(info.key.range(), cursor).is_gt());
    }
    let more = files.len() > limit.get();
    files.truncate(limit.get());
    let next = more
        .then(|| files.last().map(|info| info.key.range()))
        .flatten();
    let lower = after.map(file_name);
    let upper = next.map(file_name);
    let skipped = foreign
        .iter()
        .filter(|name| !is_scratch_name(name))
        .filter(|name| {
            let above = lower
                .as_ref()
                .is_none_or(|low| name.as_str() > low.as_str());
            let within = upper
                .as_ref()
                .is_none_or(|high| name.as_str() <= high.as_str());
            above && within
        })
        .count();
    Listing {
        files,
        next,
        skipped,
    }
}

/// The operation a [`StoreError`] interrupted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreOp {
    /// Listing a level.
    List,
    /// Reading a range of an object.
    Read,
    /// Beginning, writing, committing or aborting an upload.
    Upload,
    /// Deleting objects.
    Delete,
}

impl fmt::Display for StoreOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::List => "list",
            Self::Read => "read",
            Self::Upload => "upload",
            Self::Delete => "delete",
        })
    }
}

/// Why an object cannot be used as stored.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum CorruptCause {
    /// The name exists but is not a regular file.
    #[error("not a regular file")]
    NotARegularFile,
    /// The object's timestamp is outside what a header can hold.
    #[error("timestamp out of range")]
    TimestampOutOfRange,
    /// The store acknowledged a commit of a different size than was written.
    #[error("committed {stored} bytes after {wrote} were written ({})", if *removed { "object removed" } else { "object NOT removed" })]
    CommitSizeMismatch {
        /// Bytes the caller wrote.
        wrote: u64,
        /// Bytes the store reports holding.
        stored: u64,
        /// Whether the wrapper deleted the object it had just committed.
        /// `false` only when that delete itself failed; the caller must then
        /// not trust a listing of this key.
        removed: bool,
    },
}

/// Everything a store can do wrong, in the classes that need different
/// handling. The store never retries; the caller picks the policy.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum StoreError {
    /// Might work if tried again: a dropped connection, a busy disk, a 5xx.
    #[error("{op} failed, may succeed on retry ({kind})")]
    Transient {
        /// What was being done.
        op: StoreOp,
        /// The underlying I/O class.
        kind: io::ErrorKind,
    },
    /// The backend asked us to slow down.
    #[error("{op} throttled")]
    Throttled {
        /// What was being done.
        op: StoreOp,
        /// How long the backend asked us to wait, when it said.
        retry_after: Option<Duration>,
    },
    /// Credentials or permissions refuse the operation; retrying cannot help.
    #[error("{op} refused: not authorized")]
    Auth {
        /// What was being done.
        op: StoreOp,
    },
    /// The object does not exist.
    #[error("object {key} not found")]
    NotFound {
        /// The key that was asked for.
        key: FileKey,
    },
    /// Every scratch name for this key is taken, so an upload cannot start.
    /// Leftovers of crashed uploads fill them; nothing removes them
    /// automatically, so an operator clears the key's scratch files.
    #[error("no free scratch name for object {key}")]
    ScratchExhausted {
        /// The key being uploaded.
        key: FileKey,
    },
    /// The object exists but its stored form is unusable.
    #[error("object is corrupt: {cause}")]
    Corrupt {
        /// The key, when the fault belongs to one object.
        key: Option<FileKey>,
        /// What is wrong.
        cause: CorruptCause,
    },
}

impl StoreError {
    /// Classifies a local I/O error. A missing file is `NotFound` only when
    /// the caller names the key it was reading.
    #[must_use]
    pub fn from_io(op: StoreOp, key: Option<FileKey>, err: &io::Error) -> Self {
        match (err.kind(), key) {
            (io::ErrorKind::NotFound, Some(key)) => Self::NotFound { key },
            (io::ErrorKind::PermissionDenied, _) => Self::Auth { op },
            (kind, _) => Self::Transient { op, kind },
        }
    }
}

/// A backend holding a replica's LTX objects, keyed by [`FileKey`].
///
/// Futures are `Send` and name no runtime; a backend may complete them
/// without ever yielding.
pub trait ObjectStore: Send + Sync {
    /// The in-progress write this store hands out.
    type Upload: Upload;

    /// One page of a level, ascending, strictly after `after` when given.
    fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send;

    /// Up to `len` bytes from `offset`: fewer only when the object ends first,
    /// none when `offset` is at or past the end.
    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send;

    /// Starts writing `key`, stamped `created_at`. Nothing is visible yet.
    fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> impl Future<Output = Result<Self::Upload, StoreError>> + Send;

    /// Removes the keys. A key that is already gone is not an error, so a
    /// retried delete converges.
    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// One object being written. See the module docs for visibility.
pub trait Upload: Send {
    /// Appends a chunk.
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Publishes the object, replacing any object under the key. The object is
    /// whole and durable once this returns `Ok`. After an `Err` the object may
    /// or may not be visible (see the module docs): list again.
    fn commit(self) -> impl Future<Output = Result<FileInfo, StoreError>> + Send;

    /// Discards the written bytes; a prior object under the key is untouched.
    fn abort(self) -> impl Future<Output = Result<(), StoreError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsr_ltx::Txid;
    use proptest::prelude::*;

    #[test]
    fn page_limit_max_matches_its_raw_form() {
        assert_eq!(PageLimit::new(PageLimit::MAX_RAW), Some(PageLimit::MAX));
    }

    fn range(min: u64, max: u64) -> TxidRange {
        TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap()
    }

    fn info(min: u64, max: u64) -> FileInfo {
        FileInfo {
            key: FileKey::new(Level::ZERO, range(min, max)),
            size: 1,
            created_at: Timestamp::from_unix_millis(0),
        }
    }

    #[test]
    fn bounded_types_reject_zero_and_overflow() {
        assert_eq!(PageLimit::new(0), None);
        assert_eq!(PageLimit::new(1001), None);
        assert_eq!(PageLimit::new(1000).map(PageLimit::get), Some(1000));
        assert_eq!(ReadLen::new(0), None);
        assert_eq!(ReadLen::new(16_777_217), None);
        assert_eq!(ReadLen::new(16_777_216).map(ReadLen::get), Some(16_777_216));
    }

    #[test]
    fn io_errors_classify_without_inventing_not_found() {
        let key = FileKey::new(Level::ZERO, range(1, 1));
        let missing = io::Error::from(io::ErrorKind::NotFound);
        assert_eq!(
            StoreError::from_io(StoreOp::Read, Some(key), &missing),
            StoreError::NotFound { key }
        );
        assert_eq!(
            StoreError::from_io(StoreOp::Delete, None, &missing),
            StoreError::Transient {
                op: StoreOp::Delete,
                kind: io::ErrorKind::NotFound
            }
        );
        assert_eq!(
            StoreError::from_io(
                StoreOp::List,
                None,
                &io::Error::from(io::ErrorKind::PermissionDenied)
            ),
            StoreError::Auth { op: StoreOp::List }
        );
        assert_eq!(
            StoreError::from_io(
                StoreOp::Upload,
                None,
                &io::Error::from(io::ErrorKind::StorageFull)
            ),
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: io::ErrorKind::StorageFull
            }
        );
    }

    #[test]
    fn scratch_names_are_not_counted_as_foreign() {
        let foreign: Vec<String> = [
            "0000000000000003-0000000000000003.ltx.tmp",
            "0000000000000003-0000000000000003.ltx.1-0.tmp",
        ]
        .map(str::to_owned)
        .to_vec();
        let listing = build_listing(vec![info(1, 1)], &foreign, None, PageLimit::MAX);
        assert_eq!(listing.skipped, 0);
    }

    #[test]
    fn foreign_names_are_counted_in_the_span_they_sort_into() {
        let foreign: Vec<String> = ["0", "0000000000000003-0000000000000003.ltx.bak", "zz"]
            .map(str::to_owned)
            .to_vec();
        let files = vec![info(1, 1), info(5, 5)];
        let limit = PageLimit::new(1).unwrap();
        let first = build_listing(files.clone(), &foreign, None, limit);
        assert_eq!((first.files.len(), first.skipped), (1, 1));
        let second = build_listing(files, &foreign, first.next, limit);
        assert_eq!(
            (second.files.len(), second.skipped, second.next),
            (1, 2, None)
        );
    }

    proptest! {
        #[test]
        fn paging_visits_every_file_once_in_order(
            pairs in proptest::collection::btree_set((1u64..40, 0u64..5), 0..30),
            limit in 1u16..8,
        ) {
            let files: Vec<FileInfo> =
                pairs.iter().map(|&(min, len)| info(min, min.saturating_add(len))).collect();
            let limit = PageLimit::new(limit).unwrap();
            let mut seen = Vec::new();
            let mut after = None;
            for _ in 0..40 {
                let page = build_listing(files.clone(), &[], after, limit);
                prop_assert!(page.files.len() <= limit.get());
                seen.extend(page.files.iter().map(|f| f.key));
                prop_assert_eq!(page.next.is_some(), page.files.len() == limit.get() && seen.len() < files.len());
                match page.next { Some(next) => after = Some(next), None => break }
            }
            let mut want: Vec<FileKey> = files.iter().map(|f| f.key).collect();
            want.sort();
            prop_assert_eq!(seen, want);
        }
    }
}

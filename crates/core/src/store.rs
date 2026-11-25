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
//! - An upload begun with [`WriteMode::New`] publishes only into a free key:
//!   a commit that finds an object there fails with
//!   [`StoreError::AlreadyExists`] and leaves that object as it was. The
//!   bucket, not a listing, then says the key is taken, so a stale listing
//!   cannot hide it. A store that cannot make the check (an S3 provider
//!   without conditional writes) fails the commit with
//!   [`StoreError::CreateOnlyUnsupported`] and publishes nothing; it never
//!   replaces in its place (`docs/decisions.md` D79).
//! - When `commit` returns an error the object may or may not be visible: a
//!   response can be lost after the write landed, on S3 as on a disk whose
//!   directory sync failed after the rename. The caller's only safe move is to
//!   list again and trust the listing.
//! - Retrying is the caller's decision, made from the [`StoreError`] variant.
//! - Every method returns within a deadline the implementation enforces, failing as
//!   `Transient`; core holds no timer.
//! - The batch methods (`read_many`, `list_many`, `created_at_many`) answer
//!   each item exactly as the single method would, one result per item in
//!   order. Their defaults call the single method item by item, so a store
//!   that is not concurrent (the simulator's, a directory) needs nothing.
//!   [`ObjectStore::batch_width`] says how many items of a [`BatchKind`] a
//!   store runs at once (a store may allow more HEADs than ranged reads);
//!   callers size their batches by it, so at width one nothing is fetched
//!   ahead of need and the call sequence is the one sequential code makes.

use std::fmt;
use std::future::Future;
use std::io;
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::time::Duration;

use lsr_ltx::{Timestamp, TxidRange};
use thiserror::Error;

use crate::layout::{CreatedAt, FileInfo, FileKey, Level, cmp_range, file_name, is_scratch_name};

/// How many entries one listing page may hold: 1 to 1000, S3's own cap, so
/// every backend can return exactly the page asked for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PageLimit(NonZeroU16);

impl PageLimit {
    /// The largest page.
    pub const MAX_RAW: u16 = 1000;
    /// The largest page as a limit; equals [`Self::MAX_RAW`] (pinned by a test).
    pub const MAX: Self = Self(NonZeroU16::MIN.saturating_add(999));

    /// A page of one entry.
    pub const ONE: Self = Self(NonZeroU16::MIN);

    /// Returns `None` for zero or above [`Self::MAX_RAW`].
    #[must_use]
    pub const fn new(raw: u16) -> Option<Self> {
        match NonZeroU16::new(raw) {
            Some(n) if n.get() <= Self::MAX_RAW => Some(Self(n)),
            Some(_) | None => None,
        }
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

    /// One mebibyte: the chunk a file is read in when nothing calls for
    /// another size (pinned by a test).
    pub const CHUNK: Self = Self(NonZeroU32::MIN.saturating_add(1_048_575));

    /// Returns `None` for zero or above [`Self::MAX_RAW`].
    #[must_use]
    pub const fn new(raw: u32) -> Option<Self> {
        match NonZeroU32::new(raw) {
            Some(n) if n.get() <= Self::MAX_RAW => Some(Self(n)),
            Some(_) | None => None,
        }
    }

    /// `raw` as a length, or [`Self::CHUNK`] when it is not one. For a
    /// module's own constant, so that a read size is a value fixed when the
    /// crate is built and no call site has a failure to handle; a test pins
    /// each such constant to the size it names.
    #[must_use]
    pub const fn or_chunk(raw: u32) -> Self {
        match Self::new(raw) {
            Some(len) => len,
            None => Self::CHUNK,
        }
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
    /// final page). Summed over a full paging, every unrecognized name counts once.
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
/// the paging and unrecognized-name rules live in one place. Unrecognized names are
/// ordered against key names byte-wise, which is key order because the
/// transaction ids are fixed-width. A scratch name ([`is_scratch_name`]) in
/// `unrecognized` is dropped: it is not unrecognized.
#[must_use]
pub fn build_listing(
    mut files: Vec<FileInfo>,
    unrecognized: &[String],
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
    let skipped = unrecognized
        .iter()
        .filter(|name| !is_scratch_name(name))
        .filter(|name| on_page(name, lower.as_deref(), upper.as_deref()))
        .count();
    Listing {
        files,
        next,
        skipped,
    }
}

/// Whether an unrecognized name falls on the page after the cursor `lower` up to
/// and including its last key `upper`. Such a name never equals a key name, so
/// the bound at `lower` could as well be inclusive.
fn on_page(name: &str, lower: Option<&str>, upper: Option<&str>) -> bool {
    lower.is_none_or(|low| name > low) && upper.is_none_or(|high| name <= high)
}

/// Whether an upload may replace an object already under its key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WriteMode {
    /// The key must be free when the commit lands; otherwise the commit fails
    /// with [`StoreError::AlreadyExists`] and changes nothing (S3's
    /// `If-None-Match: *`).
    New,
    /// The commit replaces whatever the key holds, as Go's writes do.
    Replace,
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
        /// Whether the object just committed was deleted. An engine commit
        /// (`publish.rs`, as [`crate::Retrying`]) deletes it after a
        /// create-only write only; after a replace it is left, since the key
        /// may have held an acknowledged object, and `false` is the answer
        /// then, as when the delete itself failed. The caller must not trust
        /// a listing of this key on `false`.
        removed: bool,
    },
    /// The store acknowledged bytes whose digest is not the one sent. `None`
    /// is a single PUT.
    #[error("{} stored with a different checksum than was sent", match part { Some(n) => format!("part {}", n.get()), None => "object".to_owned() })]
    ChecksumMismatch {
        /// The part, for a multipart upload.
        part: Option<PartNumber>,
    },
    /// The `litestream-timestamp` metadata is present but does not parse.
    #[error("unparseable timestamp metadata")]
    BadCreatedAt,
}

/// A multipart part number: 1 to 10000, S3's part cap.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PartNumber(NonZeroU16);

impl PartNumber {
    /// The largest part number.
    pub const MAX_RAW: u16 = 10_000;

    /// Returns `None` for zero or above [`Self::MAX_RAW`].
    #[must_use]
    pub fn new(raw: u16) -> Option<Self> {
        NonZeroU16::new(raw)
            .filter(|n| n.get() <= Self::MAX_RAW)
            .map(Self)
    }

    /// The part number as sent on the wire.
    #[must_use]
    pub fn get(self) -> u16 {
        self.0.get()
    }
}

// S3's own caps, which every backend must be able to honour: a page of at most
// 1000 keys, a read of at most 16 MiB (what keeps a read's memory flat), at most
// 10000 parts. Raising one past the provider's cap fails at build time.
const _: () = assert!(PageLimit::MAX_RAW <= 1000);
const _: () = assert!(ReadLen::MAX_RAW <= 16 * 1024 * 1024);
const _: () = assert!(PartNumber::MAX_RAW <= 10_000);

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
    /// The store refuses access to the replica, or could not be reached
    /// securely: waiting alone does not fix it, a person does, and `why` says
    /// which person and what to fix.
    #[error("{op} refused: {why}")]
    Denied {
        /// What was being done.
        op: StoreOp,
        /// Which kind of refusal.
        why: Denied,
    },
    /// The object does not exist.
    #[error("object {key} not found")]
    NotFound {
        /// The key that was asked for.
        key: FileKey,
    },
    /// A [`WriteMode::New`] commit found an object under its key, so it
    /// published nothing. Only the bucket's answer; whose object it is, and
    /// whether it holds the bytes this upload sent, is the caller's to read.
    #[error("object {key} already exists")]
    AlreadyExists {
        /// The key that was taken.
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
    /// A [`WriteMode::New`] commit the store cannot make create-only (an S3
    /// provider that answers the condition with `NotImplemented`): nothing was
    /// published, and the key was not checked. The caller decides whether to
    /// write it as a replace (D79).
    #[error("{op} of {key}: the store cannot make a create-only write")]
    CreateOnlyUnsupported {
        /// What was being done.
        op: StoreOp,
        /// The key being written.
        key: FileKey,
    },
    /// The backend refused a request that retrying cannot fix and that is not
    /// an authorization failure: a 4xx outside the mapped codes, 501, or an
    /// object too large for the store's mode.
    #[error("{op} refused by the backend (status {status})")]
    Refused {
        /// What was being done.
        op: StoreOp,
        /// The HTTP status, or the nearest equivalent for a non-HTTP store.
        status: u16,
    },
}

/// Why a store refuses access. Each kind asks a different person to fix a
/// different thing, so they are told apart all the way to the health report:
/// the classes were one (`credentials`) until clock skew, a missing bucket and
/// a failed certificate check each sent operators to rotate keys that were fine.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum Denied {
    /// The store does not accept the credentials: an unknown access key, a
    /// signature that does not match, an expired or malformed session token.
    #[error("credentials not accepted")]
    Credentials,
    /// The credentials are accepted and a policy refuses this request: a
    /// bucket or IAM policy, a KMS key policy or a KMS key that is disabled,
    /// missing or of the wrong kind, object lock refusing a delete, a local
    /// directory's permissions. Other requests may still succeed.
    #[error("permission denied")]
    Permission,
    /// The request's signing time is too far from the store's clock
    /// (`RequestTimeTooSkewed`): this host's clock is wrong, not its keys.
    #[error("host clock too far from the store's")]
    ClockSkew,
    /// The bucket does not exist (`NoSuchBucket`): a wrong name or region in
    /// the configuration, or a deleted bucket.
    #[error("bucket does not exist")]
    BucketMissing,
    /// The TLS handshake or certificate check failed, so no request reached
    /// the store: an expired or untrusted certificate, a missing trust store,
    /// or an endpoint that is not the store.
    #[error("tls handshake or certificate check failed")]
    Tls,
}

impl StoreError {
    /// Classifies a local I/O error. A missing file is `NotFound` only when
    /// the caller names the key it was reading.
    #[must_use]
    pub fn from_io(op: StoreOp, key: Option<FileKey>, err: &io::Error) -> Self {
        match (err.kind(), key) {
            (io::ErrorKind::NotFound, Some(key)) => Self::NotFound { key },
            (io::ErrorKind::PermissionDenied, _) => Self::Denied {
                op,
                why: Denied::Permission,
            },
            (kind, _) => Self::Transient { op, kind },
        }
    }
}

/// One ranged read of a batch: up to `len` bytes of `key` from `offset`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadAt {
    /// The object.
    pub key: FileKey,
    /// The first byte.
    pub offset: u64,
    /// How many bytes, at most.
    pub len: ReadLen,
}

/// One listing of a batch: the page of `level` strictly after `after`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ListAt {
    /// The level.
    pub level: Level,
    /// The cursor, as for [`ObjectStore::list`].
    pub after: Option<TxidRange>,
}

/// A listing returned a cursor at or before the one it was asked from, so
/// following it would page forever. A store that does this is broken, not busy.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
#[error("listing of level {level} returned cursor {cursor:?} that does not advance")]
pub struct ListingStalled {
    /// The level being listed.
    pub level: Level,
    /// The cursor that did not advance.
    pub cursor: TxidRange,
}

impl ListAt {
    /// The first page of `level`.
    #[must_use]
    pub const fn first(level: Level) -> Self {
        Self { level, after: None }
    }

    /// Where to ask next, given the `next` cursor of the page this item
    /// returned: `None` when the listing is complete. Every paged loop steps
    /// through this, so none can follow a cursor that fails to move forward.
    ///
    /// # Errors
    /// [`ListingStalled`] when `next` is not strictly after `self.after`.
    pub fn following(self, next: Option<TxidRange>) -> Result<Option<Self>, ListingStalled> {
        let Some(cursor) = next else { return Ok(None) };
        if self
            .after
            .is_some_and(|prev| cmp_range(cursor, prev).is_le())
        {
            return Err(ListingStalled {
                level: self.level,
                cursor,
            });
        }
        Ok(Some(Self {
            level: self.level,
            after: Some(cursor),
        }))
    }
}

/// Which batch method a width is asked for. A store limits each kind apart
/// because a backend's limits differ per kind of request.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BatchKind {
    /// [`ObjectStore::read_many`].
    Reads,
    /// [`ObjectStore::list_many`].
    Lists,
    /// [`ObjectStore::created_at_many`].
    ResolvedCreatedAt,
}

/// A backend holding a replica's LTX objects, keyed by [`FileKey`].
///
/// Futures are `Send` and name no runtime; a backend may complete them
/// without ever yielding.
pub trait ObjectStore: Send + Sync {
    /// The in-progress write this store hands out. It borrows nothing, so the engine can keep
    /// one between ticks ([`DynUpload`]).
    type Upload: Upload + 'static;

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

    /// The stored created-at time of a listed file. `NotFound` when it is gone;
    /// `Transient { InvalidData }` when its size is no longer `file.size`
    /// (another writer replaced it). With no stored created-at time the answer is
    /// [`CreatedAt::fallback`] of the listed last-modified time.
    fn created_at(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<CreatedAt, StoreError>> + Send;

    /// `read(key, 0, len)` plus the stored created-at time, in one request where the
    /// backend allows. Both come from the same object version.
    fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, CreatedAt), StoreError>> + Send;

    /// Starts writing `key` with `created_at` as its created-at time; `mode`
    /// says whether the commit may replace an object already there. Nothing is
    /// visible yet.
    fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> impl Future<Output = Result<Self::Upload, StoreError>> + Send;

    /// Removes the keys. A key that is already gone is not an error, so a
    /// retried delete converges.
    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Removes the scratch files of uploads a killed process left in this store, and says
    /// how many it removed. The engine calls it when it boots, before it reads anything:
    /// a crash loop otherwise leaves one per boot, each as large as the file it was writing.
    /// A store removes only names of its own scratch form and bounds the work of one call; one
    /// that leaves nothing behind (memory) answers 0. A wrapper forwards it. There is no
    /// default, so a new store or wrapper cannot drop it by saying nothing.
    fn remove_scratch(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// How many items of a batch of `kind` this store runs at once. A caller
    /// asks for at most this many at a time, so its memory is this many
    /// responses. One, the default, means the batch methods are sequential.
    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        let _ = kind;
        NonZeroUsize::MIN
    }

    /// [`Self::read`] for each item, one result per item, in order.
    fn read_many(
        &self,
        reads: &[ReadAt],
    ) -> impl Future<Output = Vec<Result<Vec<u8>, StoreError>>> + Send {
        async move {
            let mut out = Vec::with_capacity(reads.len());
            for at in reads {
                out.push(self.read(&at.key, at.offset, at.len).await);
            }
            out
        }
    }

    /// [`Self::created_at`] for each file, one result per file, in order.
    fn created_at_many(
        &self,
        files: &[FileInfo],
    ) -> impl Future<Output = Vec<Result<CreatedAt, StoreError>>> + Send {
        async move {
            let mut out = Vec::with_capacity(files.len());
            for file in files {
                out.push(self.created_at(file).await);
            }
            out
        }
    }

    /// [`Self::list`] for each item with the same `limit`, one result per
    /// item, in order.
    fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> impl Future<Output = Vec<Result<Listing, StoreError>>> + Send {
        async move {
            let mut out = Vec::with_capacity(pages.len());
            for at in pages {
                out.push(self.list(at.level, at.after, limit).await);
            }
            out
        }
    }
}

/// One object being written. See the module docs for visibility.
pub trait Upload: Send {
    /// Appends a chunk.
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Publishes the object: under [`WriteMode::Replace`] over any object
    /// under the key, under [`WriteMode::New`] only into a free key. The object
    /// is whole and durable once this returns `Ok`. After an `Err` the object may
    /// or may not be visible (see the module docs): list again.
    fn commit(self) -> impl Future<Output = Result<FileInfo, StoreError>> + Send;

    /// Discards the written bytes; a prior object under the key is untouched.
    fn abort(self) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// A boxed future that may move between threads.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// [`Upload`] with boxed futures, so an upload of any store can be held as one type: the
/// engine keeps a file part-way sent between ticks without naming its store. Every `Upload`
/// is one, and `Box<dyn DynUpload>` is an `Upload` again.
pub trait DynUpload: Send {
    /// [`Upload::write`].
    fn write_boxed<'a>(&'a mut self, chunk: &'a [u8]) -> BoxFuture<'a, Result<(), StoreError>>;
    /// [`Upload::commit`].
    fn commit_boxed(self: Box<Self>) -> BoxFuture<'static, Result<FileInfo, StoreError>>;
    /// [`Upload::abort`].
    fn abort_boxed(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>>;
}

impl<U: Upload + 'static> DynUpload for U {
    fn write_boxed<'a>(&'a mut self, chunk: &'a [u8]) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.write(chunk))
    }

    fn commit_boxed(self: Box<Self>) -> BoxFuture<'static, Result<FileInfo, StoreError>> {
        Box::pin((*self).commit())
    }

    fn abort_boxed(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        Box::pin((*self).abort())
    }
}

/// A type-erased upload as an [`Upload`] again. Each call names the trait object's method:
/// `Box<dyn DynUpload>` is itself a `DynUpload` through the blanket impl, and a method call
/// on the box would find that one and recurse.
impl Upload for Box<dyn DynUpload> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        let upload: &mut dyn DynUpload = &mut **self;
        upload.write_boxed(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let upload: Box<dyn DynUpload> = self;
        DynUpload::commit_boxed(upload).await
    }

    async fn abort(self) -> Result<(), StoreError> {
        let upload: Box<dyn DynUpload> = self;
        DynUpload::abort_boxed(upload).await
    }
}

/// Whether the store holds an object under `key`, by a one-byte read. Used where one key is
/// named without a listing: a compaction's output key, a torn tail's key.
///
/// # Errors
/// The store's error for a read that fails other than as not found.
pub(crate) async fn exists<S: ObjectStore>(store: &S, key: &FileKey) -> Result<bool, StoreError> {
    match store.read(key, 0, ReadLen::or_chunk(1)).await {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound { .. }) => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::ListedTime;
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
            listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(0))),
        }
    }

    #[test]
    fn part_number_bounds() {
        assert_eq!(PartNumber::new(0), None);
        assert_eq!(PartNumber::new(10_001), None);
        assert_eq!(PartNumber::new(1).map(PartNumber::get), Some(1));
        assert_eq!(PartNumber::new(10_000).map(PartNumber::get), Some(10_000));
    }

    #[test]
    fn new_error_displays_carry_no_header_values() {
        let part = PartNumber::new(7);
        assert_eq!(
            CorruptCause::ChecksumMismatch { part }.to_string(),
            "part 7 stored with a different checksum than was sent"
        );
        assert_eq!(
            CorruptCause::ChecksumMismatch { part: None }.to_string(),
            "object stored with a different checksum than was sent"
        );
        assert_eq!(
            CorruptCause::BadCreatedAt.to_string(),
            "unparseable timestamp metadata"
        );
        let refused = StoreError::Refused {
            op: StoreOp::Upload,
            status: 501,
        };
        assert_eq!(
            refused.to_string(),
            "upload refused by the backend (status 501)"
        );
    }

    #[test]
    fn bounded_types_reject_zero_and_overflow() {
        assert_eq!(PageLimit::new(0), None);
        assert_eq!(PageLimit::new(1001), None);
        assert_eq!(PageLimit::new(1000).map(PageLimit::get), Some(1000));
        assert_eq!(ReadLen::CHUNK.get(), 1 << 20);
        assert_eq!(ReadLen::or_chunk(7).get(), 7);
        assert_eq!(ReadLen::or_chunk(0), ReadLen::CHUNK);
        assert_eq!(ReadLen::or_chunk(u32::MAX), ReadLen::CHUNK);
        assert_eq!(PageLimit::ONE.get(), 1);
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
            StoreError::Denied {
                op: StoreOp::List,
                why: Denied::Permission
            }
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
    fn scratch_names_are_not_counted_as_unrecognized() {
        let unrecognized: Vec<String> = [
            "0000000000000003-0000000000000003.ltx.tmp",
            "0000000000000003-0000000000000003.ltx.1-0.tmp",
        ]
        .map(str::to_owned)
        .to_vec();
        let listing = build_listing(vec![info(1, 1)], &unrecognized, None, PageLimit::MAX);
        assert_eq!(listing.skipped, 0);
    }

    #[test]
    fn unrecognized_names_are_counted_in_the_span_they_sort_into() {
        let unrecognized: Vec<String> = ["0", "0000000000000003-0000000000000003.ltx.bak", "zz"]
            .map(str::to_owned)
            .to_vec();
        let files = vec![info(1, 1), info(5, 5)];
        let limit = PageLimit::new(1).unwrap();
        let first = build_listing(files.clone(), &unrecognized, None, limit);
        assert_eq!((first.files.len(), first.skipped), (1, 1));
        let second = build_listing(files, &unrecognized, first.next, limit);
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

//! One suite every [`ObjectStore`] must pass.
//!
//! A backend crate implements [`Harness`] for its store and invokes
//! [`conformance_suite!`] inside a test module; each case becomes its own test.
//! Cases return a [`Failure`] instead of asserting so this module, which ships
//! in a feature rather than under `cfg(test)`, stays free of panics.
//! `block_on` is a few lines of std because the stores complete without a
//! reactor, and the suite should not pick the backends' runtime for them.

use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::layout::{FileInfo, FileKey, Level, ListedStamp, StampOrigin};
use crate::store::{ObjectStore, PageLimit, ReadLen, StoreError, StoreOp, Upload};

/// Why a conformance case failed.
#[derive(Debug)]
pub struct Failure(pub String);

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Failure {}

impl From<StoreError> for Failure {
    fn from(err: StoreError) -> Self {
        Self(format!("store error: {err}"))
    }
}

type Check = Result<(), Failure>;

macro_rules! ensure {
    ($cond:expr, $($msg:tt)+) => {
        if !$cond {
            return Err(Failure(format!($($msg)+)));
        }
    };
}

macro_rules! ensure_eq {
    ($left:expr, $right:expr, $what:expr) => {{
        let (left, right) = (&$left, &$right);
        ensure!(left == right, "{}: got {:?}, want {:?}", $what, left, right);
    }};
}

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs a future to completion on the calling thread.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => thread::park(),
        }
    }
}

/// A fresh, empty store plus the one thing the trait cannot express: a stray
/// file that is not an LTX key.
pub trait Harness {
    /// The store under test.
    type Store: ObjectStore;

    /// The store.
    fn store(&self) -> &Self::Store;

    /// Puts a non-key name into the level, the way a stray file lands in a
    /// directory.
    ///
    /// # Errors
    /// Whatever stops the backend from planting it.
    fn plant_foreign(&self, level: Level, name: &str) -> Check;

    /// Arranges for the next commit to publish its object and then fail with a
    /// transient error, as a lost acknowledgement does. Returns `false` when
    /// the backend cannot be made to; the case that needs it then passes
    /// without checking anything.
    fn fail_next_commit_after_publish(&self) -> bool {
        false
    }
}

/// The harness for [`MemStore`](crate::MemStore).
#[derive(Debug, Default)]
pub struct MemHarness(crate::MemStore);

impl MemHarness {
    /// A harness whose store lists `LastModified`, as S3 does after 8g4.
    #[must_use]
    pub fn listing_last_modified() -> Self {
        Self(crate::MemStore::with_listed_last_modified())
    }
}

impl Harness for MemHarness {
    type Store = crate::MemStore;

    fn store(&self) -> &Self::Store {
        &self.0
    }

    fn plant_foreign(&self, level: Level, name: &str) -> Check {
        self.0.plant_foreign(level, name);
        Ok(())
    }

    fn fail_next_commit_after_publish(&self) -> bool {
        self.0.inject_failure_after_publish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: std::io::ErrorKind::TimedOut,
        });
        true
    }
}

fn txid(raw: u64) -> Result<Txid, Failure> {
    Txid::new(raw).ok_or_else(|| Failure("zero transaction id in a case".to_owned()))
}

fn range(min: u64, max: u64) -> Result<TxidRange, Failure> {
    TxidRange::new(txid(min)?, txid(max)?)
        .ok_or_else(|| Failure("reversed range in a case".to_owned()))
}

fn key(level: u8, min: u64, max: u64) -> Result<FileKey, Failure> {
    let level = Level::new(level).ok_or_else(|| Failure("level above 9 in a case".to_owned()))?;
    Ok(FileKey::new(level, range(min, max)?))
}

fn limit(raw: u16) -> Result<PageLimit, Failure> {
    PageLimit::new(raw).ok_or_else(|| Failure("bad page limit in a case".to_owned()))
}

fn read_len(raw: u32) -> Result<ReadLen, Failure> {
    ReadLen::new(raw).ok_or_else(|| Failure("bad read length in a case".to_owned()))
}

fn at(millis: i64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

const STAMP: i64 = 1_700_000_000_123;

async fn put<S: ObjectStore>(store: &S, key: FileKey, bytes: &[u8]) -> Result<FileInfo, Failure> {
    let mut upload = store.begin_upload(&key, at(STAMP)).await?;
    upload.write(bytes).await?;
    Ok(upload.commit().await?)
}

async fn read_all<S: ObjectStore>(store: &S, key: &FileKey) -> Result<Vec<u8>, Failure> {
    Ok(store.read(key, 0, read_len(1024)?).await?)
}

struct Paged {
    keys: Vec<FileKey>,
    infos: Vec<FileInfo>,
    skipped: usize,
    pages: usize,
}

/// Pages a level to its end at one limit, checking the paging rules as it goes.
async fn page_through<S: ObjectStore>(
    store: &S,
    level: Level,
    page: u16,
    after: Option<TxidRange>,
) -> Result<Paged, Failure> {
    let want = limit(page)?;
    let mut out = Paged {
        keys: Vec::new(),
        infos: Vec::new(),
        skipped: 0,
        pages: 0,
    };
    let mut cursor = after;
    for _ in 0..2000 {
        let listing = store.list(level, cursor, want).await?;
        out.pages = out.pages.saturating_add(1);
        out.skipped = out.skipped.saturating_add(listing.skipped);
        ensure!(
            listing.files.len() <= want.get(),
            "page of {} over limit {page}",
            listing.files.len()
        );
        out.keys.extend(listing.files.iter().map(|f| f.key));
        out.infos.extend(listing.files.iter().copied());
        match listing.next {
            Some(next) => {
                ensure_eq!(
                    listing.files.len(),
                    want.get(),
                    "a page with a cursor is full"
                );
                ensure!(
                    listing.files.last().map(|f| f.key.range()) == Some(next),
                    "cursor is the page's last range"
                );
                cursor = Some(next);
            }
            None => return Ok(out),
        }
    }
    Err(Failure("paging did not terminate".to_owned()))
}

/// An empty level lists as nothing, with no cursor.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn list_empty_level<H: Harness>(h: &H) -> Check {
    for raw in [0, 5, 9] {
        let level = Level::new(raw).ok_or_else(|| Failure("level".to_owned()))?;
        let listing = h.store().list(level, None, limit(1000)?).await?;
        ensure_eq!(listing, crate::Listing::empty(), "empty level");
    }
    Ok(())
}

/// Paging at every limit from 1 reproduces the full listing, ascending,
/// whatever order the files were uploaded in.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn paging_at_every_limit<H: Harness>(h: &H) -> Check {
    let ranges = [(7, 9), (1, 1), (3, 5), (2, 2), (6, 6)];
    for (min, max) in ranges {
        put(h.store(), key(1, min, max)?, b"x").await?;
    }
    let want = [
        key(1, 1, 1)?,
        key(1, 2, 2)?,
        key(1, 3, 5)?,
        key(1, 6, 6)?,
        key(1, 7, 9)?,
    ];
    let level = Level::new(1).ok_or_else(|| Failure("level".to_owned()))?;
    for page in 1..=7u16 {
        let got = page_through(h.store(), level, page, None).await?;
        ensure_eq!(got.keys, want, format!("keys at limit {page}"));
        ensure_eq!(
            got.pages,
            want.len().div_ceil(usize::from(page)),
            format!("page count at limit {page}")
        );
    }
    Ok(())
}

/// The cursor is exclusive and need not name a stored key.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn cursor_is_exclusive<H: Harness>(h: &H) -> Check {
    for (min, max) in [(1, 1), (2, 2), (3, 5), (6, 6)] {
        put(h.store(), key(2, min, max)?, b"x").await?;
    }
    let level = Level::new(2).ok_or_else(|| Failure("level".to_owned()))?;
    let from = |after: TxidRange| page_through(h.store(), level, 10, Some(after));
    ensure_eq!(
        from(range(2, 2)?).await?.keys,
        [key(2, 3, 5)?, key(2, 6, 6)?],
        "after a stored key"
    );
    ensure_eq!(
        from(range(3, 3)?).await?.keys,
        [key(2, 3, 5)?, key(2, 6, 6)?],
        "after a key between"
    );
    ensure_eq!(
        from(range(3, 5)?).await?.keys,
        [key(2, 6, 6)?],
        "after the exact key"
    );
    ensure_eq!(
        from(range(6, 6)?).await?.keys,
        Vec::<FileKey>::new(),
        "after the last key"
    );
    Ok(())
}

/// A level lists only its own files.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn levels_are_separate<H: Harness>(h: &H) -> Check {
    put(h.store(), key(0, 1, 1)?, b"a").await?;
    put(h.store(), key(9, 1, 1)?, b"bb").await?;
    for (raw, size) in [(0u8, 1u64), (9, 2)] {
        let level = Level::new(raw).ok_or_else(|| Failure("level".to_owned()))?;
        let got = page_through(h.store(), level, 10, None).await?;
        ensure_eq!(
            got.infos.iter().map(|i| i.size).collect::<Vec<_>>(),
            [size],
            "size at a level"
        );
    }
    Ok(())
}

/// Names that are not LTX keys are skipped and counted once across a paging.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn foreign_names_are_skipped_and_counted<H: Harness>(h: &H) -> Check {
    let level = Level::new(3).ok_or_else(|| Failure("level".to_owned()))?;
    let foreign = [
        "0",
        "README",
        "0000000000000002-0000000000000001.ltx",
        "000000000000000A-000000000000000A.ltx",
        "0000000000000003-0000000000000003.ltx.bak",
        "zzzz",
    ];
    for name in foreign {
        h.plant_foreign(level, name)?;
    }
    for (min, max) in [(1, 1), (4, 4), (5, 8)] {
        put(h.store(), key(3, min, max)?, b"x").await?;
    }
    for page in [1u16, 2, 3, 1000] {
        let got = page_through(h.store(), level, page, None).await?;
        ensure_eq!(got.keys.len(), 3, format!("files at limit {page}"));
        ensure_eq!(
            got.skipped,
            foreign.len(),
            format!("skipped at limit {page}")
        );
    }
    Ok(())
}

/// A store's scratch files (Go's `<name>.ltx.tmp` and any
/// `<name>.ltx.<unique>.tmp`) are not foreign names, whether they are planted
/// or left by a dropped upload.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn scratch_names_are_not_foreign<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(0, 1, 1)?;
    let name = format!("{}.tmp", target.file_name());
    h.plant_foreign(target.level(), &name)?;
    let mut upload = store.begin_upload(&target, at(STAMP)).await?;
    upload.write(b"half").await?;
    drop(upload);
    let got = page_through(store, target.level(), 10, None).await?;
    ensure_eq!(got.keys.len(), 0, "files listed beside scratch");
    ensure_eq!(got.skipped, 0, "scratch counted as foreign");
    Ok(())
}

/// After a commit error the listing is the truth: an object that was published
/// before the error is listed and readable, and the two agree.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn commit_error_may_leave_the_object_visible<H: Harness>(h: &H) -> Check {
    if !h.fail_next_commit_after_publish() {
        return Ok(());
    }
    let store = h.store();
    let target = key(0, 1, 1)?;
    let mut upload = store.begin_upload(&target, at(STAMP)).await?;
    upload.write(b"landed").await?;
    ensure!(
        upload.commit().await.is_err(),
        "the injected commit failure"
    );
    let listed = store.list(target.level(), None, limit(10)?).await?.files;
    ensure_eq!(
        listed.iter().map(|f| f.key).collect::<Vec<_>>(),
        [target],
        "listing after a commit that failed late"
    );
    ensure_eq!(
        read_all(store, &target).await?,
        b"landed".to_vec(),
        "read agrees with the listing"
    );
    Ok(())
}

/// An aborted upload leaves nothing, and leaves an older object untouched.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn abort_publishes_nothing<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let fresh = key(1, 1, 1)?;
    let mut upload = store.begin_upload(&fresh, at(STAMP)).await?;
    upload.write(b"never seen").await?;
    upload.abort().await?;
    ensure!(
        store
            .list(fresh.level(), None, limit(10)?)
            .await?
            .files
            .is_empty(),
        "abort listed a file"
    );
    ensure!(
        matches!(read_all(store, &fresh).await, Err(Failure(_))),
        "abort left something readable"
    );

    put(store, fresh, b"old").await?;
    let mut upload = store.begin_upload(&fresh, at(STAMP)).await?;
    upload.write(b"newer").await?;
    upload.abort().await?;
    ensure_eq!(
        read_all(store, &fresh).await?,
        b"old".to_vec(),
        "object after an aborted overwrite"
    );
    Ok(())
}

/// A pending upload is invisible, and dropping one publishes nothing.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn pending_and_dropped_uploads_are_invisible<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(0, 1, 1)?;
    let mut upload = store.begin_upload(&target, at(STAMP)).await?;
    upload.write(b"half").await?;
    ensure!(
        store
            .list(target.level(), None, limit(10)?)
            .await?
            .files
            .is_empty(),
        "pending upload listed"
    );
    drop(upload);
    ensure!(
        store
            .list(target.level(), None, limit(10)?)
            .await?
            .files
            .is_empty(),
        "dropped upload listed"
    );
    let missing = store.read(&target, 0, read_len(4)?).await;
    ensure_eq!(
        missing,
        Err(StoreError::NotFound { key: target }),
        "read of a dropped upload"
    );

    put(store, target, b"kept").await?;
    let mut upload = store.begin_upload(&target, at(STAMP)).await?;
    upload.write(b"lost").await?;
    drop(upload);
    ensure_eq!(
        read_all(store, &target).await?,
        b"kept".to_vec(),
        "object after a dropped overwrite"
    );
    Ok(())
}

/// Reads return what was asked for, short only at the end of the object.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn read_spans_the_end<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(0, 1, 1)?;
    put(store, target, b"0123456789").await?;
    let cases: [(u64, u32, &[u8]); 9] = [
        (0, 10, b"0123456789"),
        (0, 11, b"0123456789"),
        (3, 4, b"3456"),
        (8, 5, b"89"),
        (9, 1, b"9"),
        (10, 5, b""),
        (1000, 5, b""),
        (1 << 63, 5, b""),
        (u64::MAX, 5, b""),
    ];
    for (offset, len, want) in cases {
        let got = store.read(&target, offset, read_len(len)?).await?;
        ensure_eq!(got, want.to_vec(), format!("read at {offset} for {len}"));
    }
    Ok(())
}

/// Reading a key that does not exist is [`StoreError::NotFound`].
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn read_of_missing_key_is_not_found<H: Harness>(h: &H) -> Check {
    let absent = key(4, 1, 2)?;
    let got = h.store().read(&absent, 0, read_len(8)?).await;
    ensure_eq!(
        got,
        Err(StoreError::NotFound { key: absent }),
        "read of a missing key"
    );
    Ok(())
}

/// Deleting is idempotent, takes missing keys in stride and removes only what
/// it names.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn delete_is_idempotent<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let (a, b, never) = (key(0, 1, 1)?, key(0, 2, 2)?, key(0, 9, 9)?);
    put(store, a, b"a").await?;
    put(store, b, b"b").await?;
    store.delete(&[]).await?;
    store.delete(&[a, never]).await?;
    store.delete(&[a, never]).await?;
    let level = a.level();
    ensure_eq!(
        page_through(store, level, 10, None).await?.keys,
        [b],
        "keys after deleting a"
    );
    Ok(())
}

/// Two uploads to one key never interleave: the last commit wins whole.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn two_uploads_to_one_key<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(2, 1, 4)?;
    let mut first = store.begin_upload(&target, at(1_000)).await?;
    let mut second = store.begin_upload(&target, at(2_000)).await?;
    first.write(b"aaaa").await?;
    second.write(b"bbbbbbbb").await?;
    first.write(b"aaaa").await?;
    ensure!(
        store
            .list(target.level(), None, limit(10)?)
            .await?
            .files
            .is_empty(),
        "uploads listed before commit"
    );
    second.commit().await?;
    ensure_eq!(
        read_all(store, &target).await?,
        b"bbbbbbbb".to_vec(),
        "after the second commit"
    );
    first.commit().await?;
    ensure_eq!(
        read_all(store, &target).await?,
        b"aaaaaaaa".to_vec(),
        "after the first commit"
    );
    let listing = store.list(target.level(), None, limit(10)?).await?;
    ensure_eq!(listing.files.len(), 1, "one object under one key");
    ensure_eq!(
        listing.files.first().map(|f| f.size),
        Some(8),
        "the surviving object"
    );
    let survivor = listing
        .files
        .first()
        .ok_or_else(|| Failure("no surviving object listed".to_owned()))?;
    ensure_eq!(
        store.stamp(survivor).await?.at(),
        at(1_000),
        "the surviving object's stamp"
    );
    Ok(())
}

/// Chunks concatenate (empty ones included) and the commit and the listing
/// report the size and the stamp that were given.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn chunks_concatenate_and_stamp_survives<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(5, 1, 1)?;
    let mut upload = store.begin_upload(&target, at(STAMP)).await?;
    for chunk in [&b"ab"[..], b"", b"cde"] {
        upload.write(chunk).await?;
    }
    let info = upload.commit().await?;
    ensure_eq!((info.key, info.size), (target, 5), "commit info");
    ensure!(
        exact_or_marked(&info, at(STAMP)),
        "the commit reports a stamp other than the one given"
    );
    let listing = store.list(target.level(), None, limit(10)?).await?;
    ensure_eq!(listing.files, vec![info], "listed info");
    ensure_eq!(read_all(store, &target).await?, b"abcde".to_vec(), "bytes");
    Ok(())
}

/// A listed file is either exact and equal to `expected`, or marked as only a
/// last-modified time.
fn exact_or_marked(info: &FileInfo, expected: Timestamp) -> bool {
    match info.listed {
        ListedStamp::Stored(stamp) => stamp.at() == expected,
        ListedStamp::LastModified(_) => true,
    }
}

fn instant(text: &str) -> Result<Timestamp, Failure> {
    Timestamp::parse_rfc3339(text).map_err(|_bad| Failure(format!("bad instant {text} in a case")))
}

async fn put_stamped<S: ObjectStore>(
    store: &S,
    key: FileKey,
    bytes: &[u8],
    stamp: Timestamp,
) -> Result<FileInfo, Failure> {
    let mut upload = store.begin_upload(&key, stamp).await?;
    upload.write(bytes).await?;
    Ok(upload.commit().await?)
}

async fn listed_one<S: ObjectStore>(store: &S, key: FileKey) -> Result<FileInfo, Failure> {
    let listing = store.list(key.level(), None, limit(10)?).await?;
    listing
        .files
        .into_iter()
        .find(|f| f.key == key)
        .ok_or_else(|| Failure(format!("{key} missing from its listing")))
}

/// `stamp` returns the stamp the writer gave, never a stand-in, across the
/// edge instants.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn stamp_returns_the_committed_stamp<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let stamps = [
        at(0),
        at(1),
        instant("2024-02-29T12:34:56.789Z")?,
        instant("2200-01-01T00:00:00.001Z")?,
    ];
    for (n, stamp) in (1_u64..).zip(stamps) {
        let target = key(1, n, n)?;
        put_stamped(store, target, b"abc", stamp).await?;
        let file = listed_one(store, target).await?;
        let got = store.stamp(&file).await?;
        ensure_eq!(got.at(), stamp, format!("stamp of {target}"));
        ensure!(
            got.origin() != StampOrigin::Fallback,
            "stamp of {target} came from the fallback"
        );
    }
    Ok(())
}

/// A listed file is `Stored` and equal to the committed stamp, or it is marked
/// `LastModified`; nothing in between.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn listed_stamp_is_exact_or_marked<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(2, 1, 1)?;
    put_stamped(store, target, b"abc", at(STAMP)).await?;
    let file = listed_one(store, target).await?;
    ensure!(
        exact_or_marked(&file, at(STAMP)),
        "{target} listed with {:?}",
        file.listed
    );
    Ok(())
}

/// A file that was deleted after it was listed has no stamp: `NotFound`.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn stamp_of_a_vanished_file_is_not_found<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(3, 1, 1)?;
    put_stamped(store, target, b"abc", at(STAMP)).await?;
    let file = listed_one(store, target).await?;
    store.delete(&[target]).await?;
    ensure_eq!(
        store.stamp(&file).await,
        Err(StoreError::NotFound { key: target }),
        "stamp of a deleted file"
    );
    Ok(())
}

/// A file re-committed with another size after it was listed is refused, not
/// stamped as if it were the listed one.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn stamp_of_an_overwritten_file_is_refused<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(4, 1, 1)?;
    put_stamped(store, target, b"abc", at(STAMP)).await?;
    let file = listed_one(store, target).await?;
    put_stamped(store, target, b"abcdef", at(STAMP)).await?;
    ensure_eq!(
        store.stamp(&file).await,
        Err(StoreError::Transient {
            op: StoreOp::Read,
            kind: std::io::ErrorKind::InvalidData
        }),
        "stamp after an overwrite with another size"
    );
    Ok(())
}

/// `read_with_stamp` returns what `read` and `stamp` return separately.
///
/// # Errors
/// A [`Failure`] naming the rule that broke.
pub async fn read_with_stamp_agrees_with_read_and_stamp<H: Harness>(h: &H) -> Check {
    let store = h.store();
    let target = key(5, 2, 2)?;
    put_stamped(store, target, b"abcde", at(STAMP)).await?;
    let file = listed_one(store, target).await?;
    let (bytes, stamp) = store.read_with_stamp(&file, read_len(3)?).await?;
    ensure_eq!(
        bytes,
        store.read(&target, 0, read_len(3)?).await?,
        "bytes of read_with_stamp"
    );
    ensure_eq!(
        Ok(stamp),
        store.stamp(&file).await,
        "stamp of read_with_stamp"
    );
    ensure_eq!(stamp.at(), at(STAMP), "the committed stamp");
    Ok(())
}

/// Hands the suite to a backend: one test per case, each on a fresh harness.
///
/// `make` is an expression that builds the harness, called once per test.
#[macro_export]
macro_rules! conformance_suite {
    ($make:expr) => {
        $crate::conformance_suite!(@each $make;
            list_empty_level,
            paging_at_every_limit,
            cursor_is_exclusive,
            levels_are_separate,
            foreign_names_are_skipped_and_counted,
            scratch_names_are_not_foreign,
            commit_error_may_leave_the_object_visible,
            abort_publishes_nothing,
            pending_and_dropped_uploads_are_invisible,
            read_spans_the_end,
            read_of_missing_key_is_not_found,
            delete_is_idempotent,
            two_uploads_to_one_key,
            chunks_concatenate_and_stamp_survives,
            stamp_returns_the_committed_stamp,
            listed_stamp_is_exact_or_marked,
            stamp_of_a_vanished_file_is_not_found,
            stamp_of_an_overwritten_file_is_refused,
            read_with_stamp_agrees_with_read_and_stamp,
        );
    };
    (@each $make:expr; $($case:ident,)+) => {
        mod conformance_cases {
            use super::*;
            $(
                #[test]
                fn $case() {
                    let harness = ($make)();
                    $crate::conformance::block_on($crate::conformance::$case(&harness)).unwrap();
                }
            )+
        }
    };
}

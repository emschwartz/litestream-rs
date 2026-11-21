//! Stamps resolved one at a time, for decisions that need few of them.
//!
//! A listing may carry only a last-modified time, which no reader decides by
//! (see [`ListedStamp`]). A decision is written as a [`Step`]: it is advanced
//! with the stamps known so far and either finishes or names the one file whose
//! exact stamp it needs next ([`Need`]), optionally followed by the files it
//! expects to need after it. [`resolve`] reads the needed stamp, plus as many
//! of those expected ones as the store runs at once
//! ([`ObjectStore::batch_width`]) in one [`ObjectStore::stamp_many`], and drops
//! the collected [`Stamps`] with the decision: nothing is kept across
//! listings, attempts or ticks, so this is a ledger and not a cache. At width
//! one only the needed stamp is read, one call per request, as before. A read
//! ahead that fails is dropped: if the step does need that file, it asks, and
//! the single read reports the failure then.
//!
//! A step that asks for the same file twice is a bug in the step, and
//! [`resolve`] fails closed with [`ResolveError::Repeated`] instead of looping.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use thiserror::Error;

use crate::layout::{FileInfo, FileKey, ListedStamp, StoredStamp};
use crate::store::{ObjectStore, StoreError};

/// The exact stamps resolved during one decision.
#[derive(Clone, Debug, Default)]
pub struct Stamps {
    resolved: BTreeMap<FileKey, StoredStamp>,
}

impl Stamps {
    /// The file's exact stamp: the listing's own when it is exact, else the
    /// one resolved for it, else none yet.
    #[must_use]
    pub fn exact(&self, info: &FileInfo) -> Option<StoredStamp> {
        match info.listed {
            ListedStamp::Stored(stamp) => Some(stamp),
            ListedStamp::LastModified(_) => self.resolved.get(&info.key).copied(),
        }
    }

    /// How many stamps were resolved through the store.
    #[must_use]
    pub fn len(&self) -> usize {
        self.resolved.len()
    }

    /// Whether none were.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.resolved.is_empty()
    }
}

/// The most files a [`Need`] names ahead; a batch is never wider anyway.
pub const MAX_AHEAD: usize = 64;

/// The stamp a step needs now, and those it expects to need next.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Need {
    first: FileInfo,
    ahead: Vec<FileInfo>,
}

impl Need {
    /// Only `first`.
    #[must_use]
    pub fn one(first: FileInfo) -> Self {
        Self {
            first,
            ahead: Vec::new(),
        }
    }

    /// `first`, then the files the step expects to ask for after it, in the
    /// order it would ask; at most [`MAX_AHEAD`] are kept.
    #[must_use]
    pub fn with_ahead(first: FileInfo, ahead: impl IntoIterator<Item = FileInfo>) -> Self {
        Self {
            first,
            ahead: ahead
                .into_iter()
                .filter(|info| info.key != first.key)
                .take(MAX_AHEAD)
                .collect(),
        }
    }

    /// The file whose stamp the step needs now.
    #[must_use]
    pub const fn first(&self) -> &FileInfo {
        &self.first
    }

    /// The files the step expects to need next.
    #[must_use]
    pub fn ahead(&self) -> &[FileInfo] {
        &self.ahead
    }
}

impl From<FileInfo> for Need {
    fn from(first: FileInfo) -> Self {
        Self::one(first)
    }
}

/// One advance of a [`Step`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Resolve<T> {
    /// The decision is made.
    Done(T),
    /// The decision needs this stamp first.
    Need(Need),
}

/// A decision that asks for stamps as it goes.
pub trait Step {
    /// What the decision produces.
    type Output;

    /// Decides with the stamps known so far, or names the next one needed. It
    /// is called again after each answer, so it must be restartable.
    fn advance(&mut self, stamps: &Stamps) -> Resolve<Self::Output>;
}

impl<T, F: FnMut(&Stamps) -> Resolve<T>> Step for F {
    type Output = T;

    fn advance(&mut self, stamps: &Stamps) -> Resolve<T> {
        self(stamps)
    }
}

/// A finished decision and what it cost.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Resolved<T> {
    /// The decision.
    pub value: T,
    /// How many stamps it read from the store, ahead reads included.
    pub stamps_read: u64,
}

/// Why a decision could not be resolved.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum ResolveError {
    /// The store failed to answer a stamp request.
    #[error("reading a stamp failed: {0}")]
    Store(#[from] StoreError),
    /// The step asked for a file it was already given.
    #[error("the step asked twice for the stamp of {key}")]
    Repeated {
        /// The file asked for twice.
        key: FileKey,
    },
}

/// Drives `step` to completion, answering each request with the needed stamp
/// and up to `batch_width - 1` of the ones it names ahead (see the module docs).
///
/// # Errors
/// The store's error from the first failed read of a needed stamp, or
/// [`ResolveError::Repeated`] when the step asks for the same file twice.
pub async fn resolve<S: ObjectStore, P: Step>(
    store: &S,
    step: &mut P,
) -> Result<Resolved<P::Output>, ResolveError> {
    let mut stamps_read = 0_u64;
    let value = resolve_counted(store, step, &mut stamps_read).await?;
    Ok(Resolved { value, stamps_read })
}

/// [`resolve`], adding every stamp read to `stamps_read` as it goes, so a
/// caller that retries after an error still counts the reads that failed.
///
/// # Errors
/// As [`resolve`].
pub async fn resolve_counted<S: ObjectStore, P: Step>(
    store: &S,
    step: &mut P,
    stamps_read: &mut u64,
) -> Result<P::Output, ResolveError> {
    let mut stamps = Stamps::default();
    let width = store.batch_width().get();
    loop {
        let need = match step.advance(&stamps) {
            Resolve::Done(value) => return Ok(value),
            Resolve::Need(need) => need,
        };
        let first = need.first;
        if stamps.resolved.contains_key(&first.key) {
            return Err(ResolveError::Repeated { key: first.key });
        }
        let mut batch = vec![first];
        for info in need.ahead {
            if batch.len() >= width {
                break;
            }
            if stamps.exact(&info).is_none() && batch.iter().all(|held| held.key != info.key) {
                batch.push(info);
            }
        }
        read_batch(store, &batch, &mut stamps, stamps_read).await?;
    }
}

/// Reads the stamps of `batch`, whose first file is needed and the rest
/// ahead, adding each read to `read`. Only the needed one's failure counts.
async fn read_batch<S: ObjectStore>(
    store: &S,
    batch: &[FileInfo],
    stamps: &mut Stamps,
    read: &mut u64,
) -> Result<(), ResolveError> {
    if batch.len() > 1 {
        let answers = store.stamp_many(batch).await;
        *read = read.saturating_add(u64::try_from(answers.len()).unwrap_or(u64::MAX));
        for (at, (info, result)) in batch.iter().zip(answers).enumerate() {
            match result {
                Ok(stamp) => {
                    stamps.resolved.insert(info.key, stamp);
                }
                Err(err) if at == 0 => return Err(err.into()),
                Err(_) => {}
            }
        }
    }
    if let Some(first) = batch.first()
        && let Entry::Vacant(slot) = stamps.resolved.entry(first.key)
    {
        // Width one, or a store that answered fewer stamps than asked.
        *read = read.saturating_add(1);
        slot.insert(store.stamp(first).await?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::block_on;
    use crate::layout::Level;
    use crate::mem::MemStore;
    use crate::store::{PageLimit, Upload};
    use lsr_ltx::{Timestamp, Txid, TxidRange};

    fn key(n: u64) -> FileKey {
        let id = Txid::new(n).unwrap();
        FileKey::new(Level::ZERO, TxidRange::single(id))
    }

    /// Commits files 1..=count stamped 100 * n and lists them back.
    fn filled(store: &MemStore, count: u64) -> Vec<FileInfo> {
        block_on(async {
            for n in 1..=count {
                let at = Timestamp::from_unix_millis(i64::try_from(n).unwrap().saturating_mul(100));
                let mut up = store.begin_upload(&key(n), at).await.unwrap();
                up.write(b"x").await.unwrap();
                up.commit().await.unwrap();
            }
            let limit = PageLimit::new(10).unwrap();
            store.list(Level::ZERO, None, limit).await.unwrap().files
        })
    }

    #[test]
    fn resolve_asks_each_key_once() {
        let store = MemStore::with_listed_last_modified();
        let files = filled(&store, 1);
        let file = files[0];
        let mut step = |_: &Stamps| Resolve::<()>::Need(file.into());
        let got = block_on(resolve(&store, &mut step));
        assert_eq!(got, Err(ResolveError::Repeated { key: file.key }));
        assert_eq!(store.stamp_calls(), 1);
    }

    #[test]
    fn resolve_counts_stamps_read() {
        let store = MemStore::with_listed_last_modified();
        let files = filled(&store, 3);
        let mut step =
            |stamps: &Stamps| match files.iter().take(2).find(|f| stamps.exact(f).is_none()) {
                Some(next) => Resolve::Need((*next).into()),
                None => Resolve::Done(
                    files
                        .iter()
                        .take(2)
                        .filter_map(|f| stamps.exact(f))
                        .map(|s| s.at().unix_millis())
                        .collect::<Vec<_>>(),
                ),
            };
        let got = block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!(got.value, vec![100, 200]);
        assert_eq!(got.stamps_read, 2);
        assert_eq!(store.stamp_calls(), 2);
    }

    #[test]
    fn the_ledger_counts_each_stamp_resolved_through_the_store() {
        let store = MemStore::with_listed_last_modified();
        let files = filled(&store, 2);
        let mut seen = Vec::new();
        let mut step = |stamps: &Stamps| {
            seen.push((stamps.len(), stamps.is_empty()));
            match files.iter().find(|f| stamps.exact(f).is_none()) {
                Some(next) => Resolve::Need((*next).into()),
                None => Resolve::Done(()),
            }
        };
        block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!(seen, vec![(0, true), (1, false), (2, false)]);
    }

    #[test]
    fn stored_listing_resolves_with_no_calls() {
        let store = MemStore::new();
        let files = filled(&store, 2);
        let mut step = |stamps: &Stamps| match files
            .iter()
            .map(|f| stamps.exact(f))
            .collect::<Option<Vec<_>>>()
        {
            Some(all) => Resolve::Done(all.len()),
            None => Resolve::Need(files[0].into()),
        };
        let got = block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!((got.value, got.stamps_read), (2, 0));
        assert_eq!(store.stamp_calls(), 0);
    }

    /// MemStore run `width` wide, recording each stamp batch, failing the
    /// stamp of `fails`.
    struct Wide {
        inner: MemStore,
        width: usize,
        fails: Option<FileKey>,
        batches: std::sync::Mutex<Vec<usize>>,
    }

    impl ObjectStore for Wide {
        type Upload = <MemStore as ObjectStore>::Upload;

        async fn list(
            &self,
            level: Level,
            after: Option<TxidRange>,
            limit: PageLimit,
        ) -> Result<crate::store::Listing, StoreError> {
            self.inner.list(level, after, limit).await
        }

        async fn read(
            &self,
            key: &FileKey,
            offset: u64,
            len: crate::store::ReadLen,
        ) -> Result<Vec<u8>, StoreError> {
            self.inner.read(key, offset, len).await
        }

        async fn stamp(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
            if self.fails == Some(file.key) {
                return Err(StoreError::Transient {
                    op: crate::store::StoreOp::Read,
                    kind: std::io::ErrorKind::ConnectionReset,
                });
            }
            self.inner.stamp(file).await
        }

        async fn read_with_stamp(
            &self,
            file: &FileInfo,
            len: crate::store::ReadLen,
        ) -> Result<(Vec<u8>, StoredStamp), StoreError> {
            self.inner.read_with_stamp(file, len).await
        }

        async fn begin_upload(
            &self,
            key: &FileKey,
            created_at: Timestamp,
        ) -> Result<Self::Upload, StoreError> {
            self.inner.begin_upload(key, created_at).await
        }

        async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
            self.inner.delete(keys).await
        }

        fn batch_width(&self) -> std::num::NonZeroUsize {
            std::num::NonZeroUsize::new(self.width).unwrap()
        }

        async fn stamp_many(&self, files: &[FileInfo]) -> Vec<Result<StoredStamp, StoreError>> {
            assert!(files.len() <= self.width, "a batch wider than the store");
            self.batches.lock().unwrap().push(files.len());
            let mut out = Vec::new();
            for file in files {
                out.push(self.stamp(file).await);
            }
            out
        }
    }

    fn wide(count: u64, width: usize, fails: Option<u64>) -> (Wide, Vec<FileInfo>) {
        let inner = MemStore::with_listed_last_modified();
        let files = filled(&inner, count);
        let store = Wide {
            inner,
            width,
            fails: fails.map(key),
            batches: std::sync::Mutex::new(Vec::new()),
        };
        (store, files)
    }

    /// Asks for the first unstamped file, naming every later one ahead.
    fn all_of(files: &[FileInfo]) -> impl FnMut(&Stamps) -> Resolve<usize> + '_ {
        move |stamps: &Stamps| {
            let mut missing = files.iter().filter(|f| stamps.exact(f).is_none());
            match missing.next() {
                Some(first) => Resolve::Need(Need::with_ahead(*first, missing.copied())),
                None => Resolve::Done(files.len()),
            }
        }
    }

    #[test]
    fn needs_named_ahead_are_read_a_store_width_at_a_time() {
        let (store, files) = wide(10, 4, None);
        let got = block_on(resolve(&store, &mut all_of(&files))).unwrap();
        assert_eq!((got.value, got.stamps_read), (10, 10));
        assert_eq!(*store.batches.lock().unwrap(), vec![4, 4, 2]);
        assert_eq!(store.inner.stamp_calls(), 10);
    }

    #[test]
    fn a_failed_read_ahead_is_dropped_and_asked_again_alone() {
        let (store, files) = wide(4, 4, Some(3));
        let got = block_on(resolve(&store, &mut all_of(&files)));
        assert!(matches!(
            got,
            Err(ResolveError::Store(StoreError::Transient { .. }))
        ));
        assert_eq!(*store.batches.lock().unwrap(), vec![4], "then file 3 alone");
    }

    #[test]
    fn at_width_one_only_the_needed_stamp_is_read() {
        let (store, files) = wide(3, 1, None);
        let got = block_on(resolve(&store, &mut all_of(&files))).unwrap();
        assert_eq!(got.stamps_read, 3);
        assert!(store.batches.lock().unwrap().is_empty());
    }
}

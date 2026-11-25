//! Created-at times resolved one at a time, for decisions that need few of them.
//!
//! A listing may carry only a last-modified time, which no reader decides by
//! (see [`ListedTime`]). A decision is written as a [`Step`]: it is advanced
//! with the created-at times known so far and either finishes or names the one file whose
//! exact created-at time it needs next ([`Need`]), optionally followed by the files it
//! expects to need after it. [`resolve`] reads the needed created-at time, plus as many
//! of those expected ones as the store runs at once
//! ([`ObjectStore::batch_width`] of [`BatchKind::ResolvedCreatedAt`]) in one [`ObjectStore::created_at_many`], and drops
//! the collected [`ResolvedCreatedAt`] with the decision: nothing is kept across
//! listings, attempts or ticks, so this is a ledger and not a cache. At width
//! one only the needed created-at time is read, one call per request, as before. A read
//! ahead that fails is dropped: if the step does need that file, it asks, and
//! the single read reports the failure then.
//!
//! A step that asks for the same file twice is a bug in the step, and
//! [`resolve`] fails closed with [`ResolveError::Repeated`] instead of looping.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use thiserror::Error;

use crate::layout::{CreatedAt, FileInfo, FileKey, ListedTime};
use crate::store::{BatchKind, ObjectStore, StoreError};

/// The exact created-at times resolved during one decision.
#[derive(Clone, Debug, Default)]
pub struct ResolvedCreatedAt {
    resolved: BTreeMap<FileKey, CreatedAt>,
}

impl ResolvedCreatedAt {
    /// The file's exact created-at time: the listing's own when it is exact, else the
    /// one resolved for it, else none yet.
    #[must_use]
    pub fn exact(&self, info: &FileInfo) -> Option<CreatedAt> {
        match info.listed {
            ListedTime::Created(created_at) => Some(created_at),
            ListedTime::LastModified(_) => self.resolved.get(&info.key).copied(),
        }
    }

    /// How many created-at times were resolved through the store.
    #[must_use]
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.resolved.len()
    }

    /// Whether none were.
    #[must_use]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.resolved.is_empty()
    }
}

/// The most files a [`Need`] names ahead; a batch is never wider anyway.
pub const MAX_AHEAD: usize = 64;

/// The created-at time a step needs now, and those it expects to need next.
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

    /// The file whose created-at time the step needs now.
    #[must_use]
    pub const fn first(&self) -> &FileInfo {
        &self.first
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
    /// The decision needs this created-at time first.
    Need(Need),
}

/// A decision that asks for created-at times as it goes.
pub trait Step {
    /// What the decision produces.
    type Output;

    /// Decides with the created-at times known so far, or names the next one needed. It
    /// is called again after each answer, so it must be restartable.
    fn advance(&mut self, resolved: &ResolvedCreatedAt) -> Resolve<Self::Output>;
}

impl<T, F: FnMut(&ResolvedCreatedAt) -> Resolve<T>> Step for F {
    type Output = T;

    fn advance(&mut self, resolved: &ResolvedCreatedAt) -> Resolve<T> {
        self(resolved)
    }
}

/// A finished decision and what it cost.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Resolved<T> {
    /// The decision.
    pub value: T,
    /// How many created-at times it read from the store, ahead reads included.
    pub created_at_read: u64,
}

/// Why a decision could not be resolved.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum ResolveError {
    /// The store failed to answer a created-at time request.
    #[error("reading a file's created-at time failed: {0}")]
    Store(#[from] StoreError),
    /// The step asked for a file it was already given.
    #[error("the step asked twice for the created-at time of {key}")]
    Repeated {
        /// The file asked for twice.
        key: FileKey,
    },
}

/// Drives `step` to completion, answering each request with the needed created-at time
/// and up to the created-at times width minus one of the ones it names ahead (see the module docs).
///
/// # Errors
/// The store's error from the first failed read of a needed created-at time, or
/// [`ResolveError::Repeated`] when the step asks for the same file twice.
pub async fn resolve<S: ObjectStore, P: Step>(
    store: &S,
    step: &mut P,
) -> Result<Resolved<P::Output>, ResolveError> {
    let mut created_at_read = 0_u64;
    let value = resolve_counted(store, step, &mut created_at_read).await?;
    Ok(Resolved {
        value,
        created_at_read,
    })
}

/// [`resolve`], adding every created-at time read to `created_at_read` as it goes, so a
/// caller that retries after an error still counts the reads that failed.
///
/// # Errors
/// As [`resolve`].
pub async fn resolve_counted<S: ObjectStore, P: Step>(
    store: &S,
    step: &mut P,
    created_at_read: &mut u64,
) -> Result<P::Output, ResolveError> {
    let mut resolved = ResolvedCreatedAt::default();
    let width = store.batch_width(BatchKind::ResolvedCreatedAt).get();
    loop {
        let need = match step.advance(&resolved) {
            Resolve::Done(value) => return Ok(value),
            Resolve::Need(need) => need,
        };
        let first = need.first;
        if resolved.resolved.contains_key(&first.key) {
            return Err(ResolveError::Repeated { key: first.key });
        }
        let mut batch = vec![first];
        for info in need.ahead {
            if batch.len() >= width {
                break;
            }
            if resolved.exact(&info).is_none() && batch.iter().all(|held| held.key != info.key) {
                batch.push(info);
            }
        }
        read_batch(store, &batch, &mut resolved, created_at_read).await?;
    }
}

/// Reads the created-at times of `batch`, whose first file is needed and the rest
/// ahead, adding each read to `read`. Only the needed one's failure counts.
async fn read_batch<S: ObjectStore>(
    store: &S,
    batch: &[FileInfo],
    resolved: &mut ResolvedCreatedAt,
    read: &mut u64,
) -> Result<(), ResolveError> {
    if batch.len() > 1 {
        let answers = store.created_at_many(batch).await;
        *read = read.saturating_add(u64::try_from(answers.len()).unwrap_or(u64::MAX));
        for (at, (info, result)) in batch.iter().zip(answers).enumerate() {
            match result {
                Ok(created_at) => {
                    resolved.resolved.insert(info.key, created_at);
                }
                Err(err) if at == 0 => return Err(err.into()),
                Err(_) => {}
            }
        }
    }
    if let Some(first) = batch.first()
        && let Entry::Vacant(slot) = resolved.resolved.entry(first.key)
    {
        // Width one, or a store that answered fewer created-at times than asked.
        *read = read.saturating_add(1);
        slot.insert(store.created_at(first).await?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::block_on;
    use crate::layout::Level;
    use crate::mem::MemStore;
    use crate::store::WriteMode;
    use crate::store::{PageLimit, Upload};
    use lsr_ltx::{Timestamp, Txid, TxidRange};

    fn key(n: u64) -> FileKey {
        let id = Txid::new(n).unwrap();
        FileKey::new(Level::ZERO, TxidRange::single(id))
    }

    /// Commits files 1..=count recorded as created 100 * n and lists them back.
    fn filled(store: &MemStore, count: u64) -> Vec<FileInfo> {
        block_on(async {
            for n in 1..=count {
                let at = Timestamp::from_unix_millis(i64::try_from(n).unwrap().saturating_mul(100));
                let mut up = store
                    .begin_upload(&key(n), at, WriteMode::Replace)
                    .await
                    .unwrap();
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
        let mut step = |_: &ResolvedCreatedAt| Resolve::<()>::Need(file.into());
        let got = block_on(resolve(&store, &mut step));
        assert_eq!(got, Err(ResolveError::Repeated { key: file.key }));
        assert_eq!(store.created_at_calls(), 1);
    }

    #[test]
    fn resolve_counts_created_at_read() {
        let store = MemStore::with_listed_last_modified();
        let files = filled(&store, 3);
        let mut step = |resolved: &ResolvedCreatedAt| match files
            .iter()
            .take(2)
            .find(|f| resolved.exact(f).is_none())
        {
            Some(next) => Resolve::Need((*next).into()),
            None => Resolve::Done(
                files
                    .iter()
                    .take(2)
                    .filter_map(|f| resolved.exact(f))
                    .map(|s| s.at().unix_millis())
                    .collect::<Vec<_>>(),
            ),
        };
        let got = block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!(got.value, vec![100, 200]);
        assert_eq!(got.created_at_read, 2);
        assert_eq!(store.created_at_calls(), 2);
    }

    #[test]
    fn the_ledger_counts_each_created_at_resolved_through_the_store() {
        let store = MemStore::with_listed_last_modified();
        let files = filled(&store, 2);
        let mut seen = Vec::new();
        let mut step = |resolved: &ResolvedCreatedAt| {
            seen.push((resolved.len(), resolved.is_empty()));
            match files.iter().find(|f| resolved.exact(f).is_none()) {
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
        let mut step = |resolved: &ResolvedCreatedAt| match files
            .iter()
            .map(|f| resolved.exact(f))
            .collect::<Option<Vec<_>>>()
        {
            Some(all) => Resolve::Done(all.len()),
            None => Resolve::Need(files[0].into()),
        };
        let got = block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!((got.value, got.created_at_read), (2, 0));
        assert_eq!(store.created_at_calls(), 0);
    }

    /// MemStore run `width` wide, recording each created-at time batch, failing the
    /// created-at time of `fails`.
    struct Wide {
        inner: MemStore,
        width: usize,
        fails: Option<FileKey>,
        batches: std::sync::Mutex<Vec<usize>>,
        asked: std::sync::Mutex<Vec<u64>>,
    }

    impl ObjectStore for Wide {
        type Upload = <MemStore as ObjectStore>::Upload;

        /// Forwards to the store it wraps.
        async fn remove_scratch(&self) -> Result<u64, StoreError> {
            self.inner.remove_scratch().await
        }

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

        async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
            self.asked
                .lock()
                .unwrap()
                .push(file.key.range().max().get());
            if self.fails == Some(file.key) {
                return Err(StoreError::Transient {
                    op: crate::store::StoreOp::Read,
                    kind: std::io::ErrorKind::ConnectionReset,
                });
            }
            self.inner.created_at(file).await
        }

        async fn read_with_created_at(
            &self,
            file: &FileInfo,
            len: crate::store::ReadLen,
        ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
            self.inner.read_with_created_at(file, len).await
        }

        async fn begin_sized_upload(
            &self,
            key: &FileKey,
            created_at: Timestamp,
            mode: WriteMode,
            len: Option<u64>,
        ) -> Result<Self::Upload, StoreError> {
            self.inner
                .begin_sized_upload(key, created_at, mode, len)
                .await
        }

        async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
            self.inner.delete(keys).await
        }

        fn batch_width(&self, _: BatchKind) -> std::num::NonZeroUsize {
            std::num::NonZeroUsize::new(self.width).unwrap()
        }

        async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
            assert!(files.len() <= self.width, "a batch wider than the store");
            self.batches.lock().unwrap().push(files.len());
            let mut out = Vec::new();
            for file in files {
                out.push(self.created_at(file).await);
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
            asked: std::sync::Mutex::new(Vec::new()),
        };
        (store, files)
    }

    /// Asks for the first without a created-at time file, naming every later one ahead.
    fn all_of(files: &[FileInfo]) -> impl FnMut(&ResolvedCreatedAt) -> Resolve<usize> + '_ {
        move |resolved: &ResolvedCreatedAt| {
            let mut missing = files.iter().filter(|f| resolved.exact(f).is_none());
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
        assert_eq!((got.value, got.created_at_read), (10, 10));
        assert_eq!(*store.batches.lock().unwrap(), vec![4, 4, 2]);
        assert_eq!(store.inner.created_at_calls(), 10);
    }

    /// A failed read ahead is dropped and asked again alone once needed; the
    /// needed file's failure ends the resolve at once.
    #[test]
    fn only_the_needed_files_failure_counts() {
        for (fails, asked) in [(3, vec![1, 2, 3, 4, 3]), (1, vec![1, 2, 3, 4])] {
            let (store, files) = wide(4, 4, Some(fails));
            let got = block_on(resolve(&store, &mut all_of(&files)));
            assert!(matches!(
                got,
                Err(ResolveError::Store(StoreError::Transient { .. }))
            ));
            assert_eq!(*store.batches.lock().unwrap(), vec![4], "{fails}");
            assert_eq!(*store.asked.lock().unwrap(), asked, "{fails}");
        }
    }

    /// A file named twice ahead, or already resolved, takes no slot in a batch.
    #[test]
    fn a_batch_holds_each_unresolved_file_once() {
        let (store, files) = wide(4, 4, None);
        let [one, two, three, four] = files[..] else {
            panic!("{files:?}")
        };
        let mut step = |resolved: &ResolvedCreatedAt| match resolved.len() {
            0 => Resolve::Need(Need::with_ahead(one, [two, two])),
            2 => Resolve::Need(Need::with_ahead(three, [one, two, four])),
            _ => Resolve::Done(()),
        };
        block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!(*store.batches.lock().unwrap(), vec![2, 2]);
    }

    #[test]
    fn at_width_one_only_the_needed_created_at_is_read() {
        let (store, files) = wide(3, 1, None);
        let got = block_on(resolve(&store, &mut all_of(&files))).unwrap();
        assert_eq!(got.created_at_read, 3);
        assert!(store.batches.lock().unwrap().is_empty());
    }
}

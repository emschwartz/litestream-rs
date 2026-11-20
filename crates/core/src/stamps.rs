//! Stamps resolved one at a time, for decisions that need few of them.
//!
//! A listing may carry only a last-modified time, which no reader decides by
//! (see [`ListedStamp`]). A decision is written as a [`Step`]: it is advanced
//! with the stamps known so far and either finishes or names the one file whose
//! exact stamp it needs next. [`resolve`] answers each such request with one
//! [`ObjectStore::stamp`] call, strictly one at a time, and drops the
//! collected [`Stamps`] with the decision: nothing is kept across listings,
//! attempts or ticks, so this is a ledger and not a cache.
//!
//! A step that asks for the same file twice is a bug in the step, and
//! [`resolve`] fails closed with [`ResolveError::Repeated`] instead of looping.

use std::collections::BTreeMap;

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

/// One advance of a [`Step`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Resolve<T> {
    /// The decision is made.
    Done(T),
    /// The decision needs this file's exact stamp first.
    Need(FileInfo),
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
    /// How many [`ObjectStore::stamp`] calls it took.
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

/// Drives `step` to completion, answering each request with one
/// [`ObjectStore::stamp`] call.
///
/// # Errors
/// The store's error from the first failed `stamp`, or [`ResolveError::Repeated`]
/// when the step asks for the same file twice.
pub async fn resolve<S: ObjectStore, P: Step>(
    store: &S,
    step: &mut P,
) -> Result<Resolved<P::Output>, ResolveError> {
    let mut stamps = Stamps::default();
    let mut stamps_read = 0_u64;
    loop {
        match step.advance(&stamps) {
            Resolve::Done(value) => {
                return Ok(Resolved { value, stamps_read });
            }
            Resolve::Need(file) => {
                if stamps.resolved.contains_key(&file.key) {
                    return Err(ResolveError::Repeated { key: file.key });
                }
                let stamp = store.stamp(&file).await?;
                stamps_read = stamps_read.saturating_add(1);
                stamps.resolved.insert(file.key, stamp);
            }
        }
    }
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
        let mut step = |_: &Stamps| Resolve::<()>::Need(file);
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
                Some(next) => Resolve::Need(*next),
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
                Some(next) => Resolve::Need(*next),
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
            None => Resolve::Need(files[0]),
        };
        let got = block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!((got.value, got.stamps_read), (2, 0));
        assert_eq!(store.stamp_calls(), 0);
    }
}

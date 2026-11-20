//! The in-memory reference model of [`ObjectStore`].
//!
//! Every other backend is judged against this one by the shared conformance
//! suite. Clones share one state, so a test can hold a handle while the code
//! under test holds another. Futures complete on first poll. A fault queue lets
//! a test make the next call of an operation fail with any [`StoreError`].

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_ltx::{Timestamp, TxidRange};

use crate::layout::{FileInfo, FileKey, Level, ListedStamp, StoredStamp};
use crate::store::{
    Listing, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp, Upload, build_listing,
};

#[derive(Debug)]
struct Object {
    bytes: Vec<u8>,
    created_at: Timestamp,
    /// What a lazy-mode listing reports: the store's clock at commit unless a
    /// test moved it.
    last_modified: Timestamp,
}

impl Object {
    fn listed(&self, lazy: bool) -> ListedStamp {
        if lazy {
            ListedStamp::LastModified(self.last_modified)
        } else {
            ListedStamp::Stored(StoredStamp::mtime(self.created_at))
        }
    }
}

#[derive(Debug, Default)]
struct State {
    /// List `LastModified` instead of `Stored`.
    lazy: bool,
    /// Logical milliseconds, advanced by one at each commit.
    clock: i64,
    stamp_calls: u64,
    objects: BTreeMap<FileKey, Object>,
    foreign: BTreeMap<Level, BTreeSet<String>>,
    faults: VecDeque<(StoreOp, StoreError)>,
    /// Errors for commits that publish first and then fail.
    late_commit_faults: VecDeque<StoreError>,
}

impl State {
    fn take_fault(&mut self, op: StoreOp) -> Result<(), StoreError> {
        match self.faults.iter().position(|(queued, _)| *queued == op) {
            Some(at) => match self.faults.remove(at) {
                Some((_, err)) => Err(err),
                None => Ok(()),
            },
            None => Ok(()),
        }
    }
}

/// An object store that lives in memory.
#[derive(Clone, Debug, Default)]
pub struct MemStore {
    state: Arc<Mutex<State>>,
}

impl MemStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing here panics while holding the lock, so poison carries no
        // half-written state worth refusing.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A store whose listings carry `LastModified`, as S3's do, so a reader
    /// must ask for the exact stamp. The last-modified time is the store's
    /// clock (one logical millisecond per commit), which is not the stamp.
    #[must_use]
    pub fn with_listed_last_modified() -> Self {
        let store = Self::default();
        store.lock().lazy = true;
        store
    }

    /// Moves what a lazy-mode listing reports for `key`. Does nothing for a
    /// key that is not stored.
    pub fn set_last_modified(&self, key: &FileKey, at: Timestamp) {
        if let Some(object) = self.lock().objects.get_mut(key) {
            object.last_modified = at;
        }
    }

    /// How many times [`ObjectStore::stamp`] was called.
    #[must_use]
    pub fn stamp_calls(&self) -> u64 {
        self.lock().stamp_calls
    }

    /// Puts a name that is not an LTX key into a level, as a stray file would
    /// be in a directory.
    pub fn plant_foreign(&self, level: Level, name: &str) {
        self.lock()
            .foreign
            .entry(level)
            .or_default()
            .insert(name.to_owned());
    }

    /// Makes the next call of `op` fail with `err`.
    pub fn inject_failure(&self, op: StoreOp, err: StoreError) {
        self.lock().faults.push_back((op, err));
    }

    /// Makes the next commit publish its object and then return `err`, as a
    /// backend does when the acknowledgement is lost after the write landed.
    pub fn inject_failure_after_publish(&self, err: StoreError) {
        self.lock().late_commit_faults.push_back(err);
    }

    fn list_now(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let mut state = self.lock();
        state.take_fault(StoreOp::List)?;
        let lazy = state.lazy;
        let files = state
            .objects
            .iter()
            .filter(|(key, _)| key.level() == level)
            .map(|(key, object)| FileInfo {
                key: *key,
                size: u64::try_from(object.bytes.len()).unwrap_or(u64::MAX),
                listed: object.listed(lazy),
            })
            .collect();
        let foreign: Vec<String> = state
            .foreign
            .get(&level)
            .map(|names| names.iter().cloned().collect())
            .unwrap_or_default();
        Ok(build_listing(files, &foreign, after, limit))
    }

    fn read_now(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let mut state = self.lock();
        state.take_fault(StoreOp::Read)?;
        let object = state
            .objects
            .get(key)
            .ok_or(StoreError::NotFound { key: *key })?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let want = usize::try_from(len.get()).unwrap_or(usize::MAX);
        Ok(object
            .bytes
            .get(start..)
            .unwrap_or_default()
            .iter()
            .take(want)
            .copied()
            .collect())
    }
}

impl MemStore {
    fn stamp_now(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
        let mut state = self.lock();
        state.stamp_calls = state.stamp_calls.saturating_add(1);
        state.take_fault(StoreOp::Read)?;
        let object = state
            .objects
            .get(&file.key)
            .ok_or(StoreError::NotFound { key: file.key })?;
        if u64::try_from(object.bytes.len()).ok() != Some(file.size) {
            return Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: std::io::ErrorKind::InvalidData,
            });
        }
        Ok(StoredStamp::mtime(object.created_at))
    }

    fn read_with_stamp_now(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, StoredStamp), StoreError> {
        let bytes = self.read_now(&file.key, 0, len)?;
        let stamp = self
            .lock()
            .objects
            .get(&file.key)
            .map(|object| StoredStamp::mtime(object.created_at))
            .ok_or(StoreError::NotFound { key: file.key })?;
        Ok((bytes, stamp))
    }
}

impl ObjectStore for MemStore {
    type Upload = MemUpload;

    fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
        std::future::ready(self.list_now(level, after, limit))
    }

    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        std::future::ready(self.read_now(key, offset, len))
    }

    fn stamp(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<StoredStamp, StoreError>> + Send {
        std::future::ready(self.stamp_now(file))
    }

    fn read_with_stamp(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, StoredStamp), StoreError>> + Send {
        std::future::ready(self.read_with_stamp_now(file, len))
    }

    fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> impl Future<Output = Result<MemUpload, StoreError>> + Send {
        let result = self.lock().take_fault(StoreOp::Upload).map(|()| MemUpload {
            store: self.clone(),
            key: *key,
            created_at,
            buf: Vec::new(),
        });
        std::future::ready(result)
    }

    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
        let mut state = self.lock();
        let result = state.take_fault(StoreOp::Delete).map(|()| {
            for key in keys {
                state.objects.remove(key);
            }
        });
        std::future::ready(result)
    }
}

/// A [`MemStore`] upload: bytes held aside until commit.
#[derive(Debug)]
pub struct MemUpload {
    store: MemStore,
    key: FileKey,
    created_at: Timestamp,
    buf: Vec<u8>,
}

impl Upload for MemUpload {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send {
        let result = self.store.lock().take_fault(StoreOp::Upload).map(|()| {
            self.buf.extend_from_slice(chunk);
        });
        std::future::ready(result)
    }

    fn commit(self) -> impl Future<Output = Result<FileInfo, StoreError>> + Send {
        let mut state = self.store.lock();
        let published = state.take_fault(StoreOp::Upload).map(|()| {
            let size = u64::try_from(self.buf.len()).unwrap_or(u64::MAX);
            state.clock = state.clock.saturating_add(1);
            let object = Object {
                bytes: self.buf,
                created_at: self.created_at,
                last_modified: Timestamp::from_unix_millis(state.clock),
            };
            let listed = object.listed(state.lazy);
            state.objects.insert(self.key, object);
            FileInfo {
                key: self.key,
                size,
                listed,
            }
        });
        let result = match (published, state.late_commit_faults.pop_front()) {
            (Ok(_), Some(err)) => Err(err),
            (outcome, _) => outcome,
        };
        std::future::ready(result)
    }

    fn abort(self) -> impl Future<Output = Result<(), StoreError>> + Send {
        std::future::ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance;
    use lsr_ltx::Txid;
    use std::time::Duration;

    crate::conformance_suite!(conformance::MemHarness::default);

    mod lazy_listing {
        use super::*;

        crate::conformance_suite!(conformance::MemHarness::listing_last_modified);
    }

    #[test]
    fn lazy_mode_lists_last_modified() {
        let store = MemStore::with_listed_last_modified();
        let limit = PageLimit::new(10).unwrap();
        let committed = Timestamp::from_unix_millis(5_000);
        conformance::block_on(async {
            let mut up = store.begin_upload(&key(), committed).await.unwrap();
            up.write(b"x").await.unwrap();
            let info = up.commit().await.unwrap();
            let listed = store.list(Level::ZERO, None, limit).await.unwrap().files;
            assert_eq!(listed, vec![info]);
            let ListedStamp::LastModified(clock) = info.listed else {
                panic!("lazy mode listed {:?}", info.listed);
            };
            assert_ne!(clock, committed);
            assert_eq!(info.exact_stamp(), None);
            let moved = Timestamp::from_unix_millis(77);
            store.set_last_modified(&key(), moved);
            let listed = store.list(Level::ZERO, None, limit).await.unwrap().files;
            assert_eq!(
                listed.first().map(|f| f.listed),
                Some(ListedStamp::LastModified(moved))
            );
            let stamp = store.stamp(&info).await.unwrap();
            assert_eq!(stamp.at(), committed);
        });
        assert_eq!(store.stamp_calls(), 1);
    }

    fn key() -> FileKey {
        let id = Txid::new(1).unwrap();
        FileKey::new(Level::ZERO, TxidRange::single(id))
    }

    #[test]
    fn injected_failures_surface_once_per_operation() {
        let store = MemStore::new();
        let limit = PageLimit::new(10).unwrap();
        let throttled = StoreError::Throttled {
            op: StoreOp::List,
            retry_after: Some(Duration::from_secs(1)),
        };
        store.inject_failure(StoreOp::List, throttled.clone());
        store.inject_failure(StoreOp::Read, StoreError::Auth { op: StoreOp::Read });
        let first = conformance::block_on(store.list(Level::ZERO, None, limit));
        assert_eq!(first, Err(throttled));
        let second = conformance::block_on(store.list(Level::ZERO, None, limit));
        assert_eq!(second.map(|l| l.files.len()), Ok(0));
        let read = conformance::block_on(store.read(&key(), 0, ReadLen::new(1).unwrap()));
        assert_eq!(read, Err(StoreError::Auth { op: StoreOp::Read }));
    }

    #[test]
    fn a_commit_that_fails_late_has_already_published() {
        let store = MemStore::new();
        store.inject_failure_after_publish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: std::io::ErrorKind::TimedOut,
        });
        conformance::block_on(async {
            let mut up = store
                .begin_upload(&key(), Timestamp::from_unix_millis(1))
                .await
                .unwrap();
            up.write(b"x").await.unwrap();
            up.commit().await.unwrap_err();
            let page = store
                .list(Level::ZERO, None, PageLimit::new(5).unwrap())
                .await
                .unwrap();
            assert_eq!(page.files.len(), 1);
        });
    }

    #[test]
    fn a_failed_commit_publishes_nothing() {
        let store = MemStore::new();
        conformance::block_on(async {
            let mut up = store
                .begin_upload(&key(), Timestamp::from_unix_millis(1))
                .await
                .unwrap();
            up.write(b"x").await.unwrap();
            store.inject_failure(
                StoreOp::Upload,
                StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: std::io::ErrorKind::TimedOut,
                },
            );
            up.commit().await.unwrap_err();
            let page = store
                .list(Level::ZERO, None, PageLimit::new(5).unwrap())
                .await
                .unwrap();
            assert!(page.files.is_empty());
        });
    }
}

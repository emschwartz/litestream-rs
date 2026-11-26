//! The in-memory reference model of [`ObjectStore`].
//!
//! Every other backend is judged against this one by the shared conformance
//! suite. Clones share one state, so a test can hold a handle while the code
//! under test holds another. Futures complete on first poll. A fault queue lets
//! a test make the next call of an operation fail with any [`StoreError`].

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_ltx::{Timestamp, TxidRange};

use crate::layout::{CreatedAt, FileInfo, FileKey, Level, ListedTime};
use crate::store::{
    BoxFuture, Listing, ObjectStore, Opened, PageLimit, ReadLen, StoreError, StoreOp, Upload,
    WriteMode, build_listing,
};

#[derive(Debug)]
struct Object {
    bytes: Vec<u8>,
    created_at: Timestamp,
    /// What a lazy-mode listing reports: the store's clock at commit unless a
    /// test moved it.
    last_modified: Timestamp,
    /// The stored created-at time was lost ([`MemStore::drop_created_at`]), so
    /// reads fall back to `last_modified`, as S3 does without metadata.
    dropped: bool,
}

impl Object {
    fn listed(&self, lazy: bool) -> ListedTime {
        if lazy {
            ListedTime::LastModified(self.last_modified)
        } else {
            ListedTime::Created(self.created_at())
        }
    }

    fn created_at(&self) -> CreatedAt {
        if self.dropped {
            CreatedAt::fallback(self.last_modified)
        } else {
            CreatedAt::mtime(self.created_at)
        }
    }
}

#[derive(Debug, Default)]
struct State {
    /// List `LastModified` instead of `Stored`.
    lazy: bool,
    /// Logical milliseconds, advanced by one at each commit.
    clock: i64,
    created_at_calls: u64,
    /// Scratch files a killed writer left, as `remove_scratch` counts them: planted, or one
    /// for each upload dropped with neither a commit nor an abort.
    scratch: u64,
    objects: BTreeMap<FileKey, Object>,
    unrecognized: BTreeMap<Level, BTreeSet<String>>,
    faults: VecDeque<(StoreOp, StoreError)>,
    /// Errors for commits that publish first and then fail.
    late_commit_faults: VecDeque<StoreError>,
    /// The store cannot make create-only writes, as an S3 provider without conditional writes:
    /// every [`WriteMode::New`] commit fails and publishes nothing ([`MemStore::refuse_create_only`]).
    no_create_only: bool,
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

    /// Every stored object as a listing would show it, read straight from the store's map:
    /// no paging, no merge across pages and no injected fault, so a checker can hold what an
    /// engine listed to what the store holds without sharing the engine's listing code.
    #[must_use]
    pub fn stored(&self) -> Vec<FileInfo> {
        let state = self.lock();
        state
            .objects
            .iter()
            .map(|(key, object)| FileInfo {
                key: *key,
                size: u64::try_from(object.bytes.len()).unwrap_or(u64::MAX),
                listed: object.listed(state.lazy),
            })
            .collect()
    }

    /// A store whose listings carry `LastModified`, as S3's do, so a reader
    /// must ask for the exact created-at time. The last-modified time is the store's
    /// clock (one logical millisecond per commit), which is not the created-at time.
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

    /// Forgets the created-at time stored with `key`, as a writer before Go
    /// v0.5.1 or a bucket copy that drops metadata leaves it: from now on the
    /// store reports [`CreatedAt::fallback`] of the object's last-modified
    /// time. Does nothing for a key that is not stored.
    pub fn drop_created_at(&self, key: &FileKey) {
        if let Some(object) = self.lock().objects.get_mut(key) {
            object.dropped = true;
        }
    }

    /// Leaves `count` scratch files, as uploads a killed process began would: the next
    /// [`ObjectStore::remove_scratch`] removes and counts them. Lets a test see that a
    /// wrapper forwards it.
    pub fn plant_scratch(&self, count: u64) {
        let mut state = self.lock();
        state.scratch = state.scratch.saturating_add(count);
    }

    /// Scratch files planted and not yet removed.
    #[must_use]
    pub fn scratch(&self) -> u64 {
        self.lock().scratch
    }

    /// How many times [`ObjectStore::created_at`] was called.
    #[must_use]
    pub fn created_at_calls(&self) -> u64 {
        self.lock().created_at_calls
    }

    /// Puts a name that is not an LTX key into a level, as a stray file would
    /// be in a directory.
    pub fn plant_unrecognized(&self, level: Level, name: &str) {
        self.lock()
            .unrecognized
            .entry(level)
            .or_default()
            .insert(name.to_owned());
    }

    /// Makes the next call of `op` fail with `err`.
    pub fn inject_failure(&self, op: StoreOp, err: StoreError) {
        self.lock().faults.push_back((op, err));
    }

    /// From now on every [`WriteMode::New`] commit fails with
    /// [`StoreError::CreateOnlyUnsupported`] and publishes nothing, as on an S3 provider that
    /// answers the condition with `NotImplemented` (`docs/decisions.md` D79). Replacing writes
    /// go on as before.
    pub fn refuse_create_only(&self) {
        self.lock().no_create_only = true;
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
        let unrecognized: Vec<String> = state
            .unrecognized
            .get(&level)
            .map(|names| names.iter().cloned().collect())
            .unwrap_or_default();
        Ok(build_listing(files, &unrecognized, after, limit))
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
        let tail = object.bytes.get(start..).unwrap_or_default();
        Ok(tail.get(..want).unwrap_or(tail).to_vec())
    }
}

impl MemStore {
    fn created_at_now(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        let mut state = self.lock();
        state.created_at_calls = state.created_at_calls.saturating_add(1);
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
        Ok(object.created_at())
    }

    fn read_with_created_at_now(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        let bytes = self.read_now(&file.key, 0, len)?;
        let created_at = self
            .lock()
            .objects
            .get(&file.key)
            .map(Object::created_at)
            .ok_or(StoreError::NotFound { key: file.key })?;
        Ok((bytes, created_at))
    }
}

/// An object of a [`MemStore`] opened ([`ObjectStore::open`]): the store and the key.
#[derive(Debug)]
struct MemOpened {
    store: MemStore,
    key: FileKey,
}

impl Opened for MemOpened {
    fn read(&mut self, offset: u64, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(std::future::ready(
            self.store.read_now(&self.key, offset, len),
        ))
    }
}

impl ObjectStore for MemStore {
    type Upload = MemUpload;

    /// Removes what [`MemStore::plant_scratch`] left.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        Ok(std::mem::take(&mut self.lock().scratch))
    }

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

    /// Nothing is kept open here: what is opened reads by the key each time, as
    /// [`Self::read`] does, and owns its handle on the store, so a caller can keep it
    /// between ticks and read it on another thread.
    fn open(
        &self,
        key: &FileKey,
    ) -> impl Future<Output = Result<Option<Box<dyn Opened>>, StoreError>> + Send {
        let held = if self.lock().objects.contains_key(key) {
            let held: Box<dyn Opened> = Box::new(MemOpened {
                store: self.clone(),
                key: *key,
            });
            Ok(Some(held))
        } else {
            Err(StoreError::NotFound { key: *key })
        };
        std::future::ready(held)
    }

    fn created_at(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<CreatedAt, StoreError>> + Send {
        std::future::ready(self.created_at_now(file))
    }

    fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, CreatedAt), StoreError>> + Send {
        std::future::ready(self.read_with_created_at_now(file, len))
    }

    fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        _len: Option<u64>,
    ) -> impl Future<Output = Result<MemUpload, StoreError>> + Send {
        let result = self.lock().take_fault(StoreOp::Upload).map(|()| MemUpload {
            store: self.clone(),
            key: *key,
            created_at,
            mode,
            buf: Vec::new(),
            ended: false,
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

/// A [`MemStore`] upload: bytes held aside until commit. One dropped with neither a commit
/// nor an abort leaves a scratch file, as a store on disk or an open multipart upload does.
#[derive(Debug)]
pub struct MemUpload {
    store: MemStore,
    key: FileKey,
    created_at: Timestamp,
    mode: WriteMode,
    buf: Vec<u8>,
    /// Committed or aborted, whatever the outcome: nothing is left behind.
    ended: bool,
}

impl Drop for MemUpload {
    fn drop(&mut self) {
        if !self.ended {
            let mut state = self.store.lock();
            state.scratch = state.scratch.saturating_add(1);
        }
    }
}

impl Upload for MemUpload {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send {
        let result = self.store.lock().take_fault(StoreOp::Upload).map(|()| {
            self.buf.extend_from_slice(chunk);
        });
        std::future::ready(result)
    }

    fn commit(mut self) -> impl Future<Output = Result<FileInfo, StoreError>> + Send {
        self.ended = true;
        let buf = std::mem::take(&mut self.buf);
        let mut state = self.store.lock();
        let taken = self.mode == WriteMode::New && state.objects.contains_key(&self.key);
        let unchecked = self.mode == WriteMode::New && state.no_create_only;
        let published = state.take_fault(StoreOp::Upload).and_then(|()| {
            if unchecked {
                return Err(StoreError::CreateOnlyUnsupported {
                    op: StoreOp::Upload,
                    key: self.key,
                });
            }
            if taken {
                return Err(StoreError::AlreadyExists { key: self.key });
            }
            let size = u64::try_from(buf.len()).unwrap_or(u64::MAX);
            state.clock = state.clock.saturating_add(1);
            let object = Object {
                bytes: buf,
                created_at: self.created_at,
                last_modified: Timestamp::from_unix_millis(state.clock),
                dropped: false,
            };
            let listed = object.listed(state.lazy);
            state.objects.insert(self.key, object);
            Ok(FileInfo {
                key: self.key,
                size,
                listed,
            })
        });
        let result = match (published, state.late_commit_faults.pop_front()) {
            (Ok(_), Some(err)) => Err(err),
            (outcome, _) => outcome,
        };
        std::future::ready(result)
    }

    fn abort(mut self) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.ended = true;
        std::future::ready(Ok(()))
    }
}

#[cfg(test)]
pub(crate) mod reread;
#[cfg(test)]
pub(crate) mod short;
#[cfg(test)]
pub(crate) mod wrapped;

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

    /// `stored` holds every object with its size and listed time, as a listing of each level
    /// would, and spends no fault queued for the engine: the simulator's judges read the
    /// replica through it.
    #[test]
    fn stored_holds_every_object_and_spends_no_fault() {
        let store = MemStore::new();
        let at = Timestamp::from_unix_millis(5_000);
        let range = |n| {
            let txid = Txid::new(n).unwrap();
            TxidRange::new(txid, txid).unwrap()
        };
        let keys = [
            FileKey::new(Level::ZERO, range(1)),
            FileKey::new(Level::new(1).unwrap(), range(2)),
        ];
        conformance::block_on(async {
            for (key, body) in keys.iter().zip([&b"one"[..], &b"three"[..]]) {
                let mut up = store
                    .begin_upload(key, at, WriteMode::Replace)
                    .await
                    .unwrap();
                up.write(body).await.unwrap();
                up.commit().await.unwrap();
            }
        });
        store.inject_failure(
            StoreOp::List,
            StoreError::Transient {
                op: StoreOp::List,
                kind: std::io::ErrorKind::ConnectionReset,
            },
        );
        let stored = store.stored();
        let seen: Vec<(FileKey, u64)> = stored.iter().map(|info| (info.key, info.size)).collect();
        assert_eq!(seen, vec![(keys[0], 3), (keys[1], 5)]);
        assert!(
            stored
                .iter()
                .all(|info| info.listed == ListedTime::Created(CreatedAt::mtime(at)))
        );
        let listed = conformance::block_on(store.list(Level::ZERO, None, PageLimit::MAX));
        assert!(listed.is_err(), "stored spent the engine's fault");
    }

    #[test]
    fn lazy_mode_lists_last_modified() {
        let store = MemStore::with_listed_last_modified();
        let limit = PageLimit::new(10).unwrap();
        let committed = Timestamp::from_unix_millis(5_000);
        conformance::block_on(async {
            let mut up = store
                .begin_upload(&key(), committed, WriteMode::Replace)
                .await
                .unwrap();
            up.write(b"x").await.unwrap();
            let info = up.commit().await.unwrap();
            let listed = store.list(Level::ZERO, None, limit).await.unwrap().files;
            assert_eq!(listed, vec![info]);
            let ListedTime::LastModified(clock) = info.listed else {
                panic!("lazy mode listed {:?}", info.listed);
            };
            assert_ne!(clock, committed);
            assert_eq!(info.created_at(), None);
            let moved = Timestamp::from_unix_millis(77);
            store.set_last_modified(&key(), moved);
            let listed = store.list(Level::ZERO, None, limit).await.unwrap().files;
            assert_eq!(
                listed.first().map(|f| f.listed),
                Some(ListedTime::LastModified(moved))
            );
            let created_at = store.created_at(&info).await.unwrap();
            assert_eq!(created_at.at(), committed);
        });
        assert_eq!(store.created_at_calls(), 1);
    }

    fn key() -> FileKey {
        let id = Txid::new(1).unwrap();
        FileKey::staged(id)
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
        store.inject_failure(
            StoreOp::Read,
            StoreError::Denied {
                op: StoreOp::Read,
                why: crate::store::Denied::Credentials,
            },
        );
        let first = conformance::block_on(store.list(Level::ZERO, None, limit));
        assert_eq!(first, Err(throttled));
        let second = conformance::block_on(store.list(Level::ZERO, None, limit));
        assert_eq!(second.map(|l| l.files.len()), Ok(0));
        let read = conformance::block_on(store.read(&key(), 0, ReadLen::new(1).unwrap()));
        assert_eq!(
            read,
            Err(StoreError::Denied {
                op: StoreOp::Read,
                why: crate::store::Denied::Credentials
            })
        );
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
                .begin_upload(&key(), Timestamp::from_unix_millis(1), WriteMode::Replace)
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
                .begin_upload(&key(), Timestamp::from_unix_millis(1), WriteMode::Replace)
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

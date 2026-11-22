//! The application's store, type-erased, and the counters every call through it feeds.
//!
//! `ObjectStore` returns `impl Future`, so it cannot be a trait object. [`DynStore`] is the
//! same contract with each call's future boxed (one allocation per call, next to an HTTP
//! request or a file operation), so the engine is compiled once inside this crate rather than
//! once per application store type. Every `ObjectStore` is a `DynStore`, and `dyn DynStore` is an
//! `ObjectStore` again, so the same `Arc` serves the replicator, `restore` and `verify`.
//!
//! [`Metered`] wraps the replica's store inside the thread and counts each operation's calls,
//! errors and time on the injected clock, and the bytes uploaded (Go's store metrics).

use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lsr_core::{
    BatchKind, Clock, CreatedAt, FileInfo, FileKey, Level, ListAt, Listing, ObjectStore, PageLimit,
    ReadAt, ReadLen, StoreError, Upload,
};
use lsr_ltx::{Timestamp, TxidRange};

use crate::health::{OpCounters, StoreCounters};
use crate::runtime::millis_until;

/// A boxed future that may move between threads.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// [`ObjectStore`] with boxed futures, so a store can sit behind `Arc<dyn DynStore>`.
/// Implemented for every `ObjectStore`; an application never implements it itself.
pub trait DynStore: Send + Sync {
    /// [`ObjectStore::list`].
    fn list_boxed(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> BoxFuture<'_, Result<Listing, StoreError>>;

    /// [`ObjectStore::read`].
    fn read_boxed<'a>(
        &'a self,
        key: &'a FileKey,
        offset: u64,
        len: ReadLen,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>>;

    /// [`ObjectStore::created_at`].
    fn created_at_boxed<'a>(
        &'a self,
        file: &'a FileInfo,
    ) -> BoxFuture<'a, Result<CreatedAt, StoreError>>;

    /// [`ObjectStore::read_with_created_at`].
    fn read_with_created_at_boxed<'a>(
        &'a self,
        file: &'a FileInfo,
        len: ReadLen,
    ) -> BoxFuture<'a, Result<(Vec<u8>, CreatedAt), StoreError>>;

    /// [`ObjectStore::begin_upload`].
    fn begin_upload_boxed<'a>(
        &'a self,
        key: &'a FileKey,
        created_at: Timestamp,
    ) -> BoxFuture<'a, Result<Box<dyn DynUpload>, StoreError>>;

    /// [`ObjectStore::delete`].
    fn delete_boxed<'a>(&'a self, keys: &'a [FileKey]) -> BoxFuture<'a, Result<(), StoreError>>;

    /// [`ObjectStore::batch_width`].
    fn batch_width_dyn(&self, kind: BatchKind) -> NonZeroUsize;

    /// [`ObjectStore::read_many`].
    fn read_many_boxed<'a>(
        &'a self,
        reads: &'a [ReadAt],
    ) -> BoxFuture<'a, Vec<Result<Vec<u8>, StoreError>>>;

    /// [`ObjectStore::created_at_many`].
    fn created_at_many_boxed<'a>(
        &'a self,
        files: &'a [FileInfo],
    ) -> BoxFuture<'a, Vec<Result<CreatedAt, StoreError>>>;

    /// [`ObjectStore::list_many`].
    fn list_many_boxed<'a>(
        &'a self,
        pages: &'a [ListAt],
        limit: PageLimit,
    ) -> BoxFuture<'a, Vec<Result<Listing, StoreError>>>;
}

/// [`Upload`] with boxed futures.
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

impl<S> DynStore for S
where
    S: ObjectStore,
    S::Upload: 'static,
{
    fn list_boxed(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> BoxFuture<'_, Result<Listing, StoreError>> {
        Box::pin(self.list(level, after, limit))
    }

    fn read_boxed<'a>(
        &'a self,
        key: &'a FileKey,
        offset: u64,
        len: ReadLen,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(self.read(key, offset, len))
    }

    fn created_at_boxed<'a>(
        &'a self,
        file: &'a FileInfo,
    ) -> BoxFuture<'a, Result<CreatedAt, StoreError>> {
        Box::pin(self.created_at(file))
    }

    fn read_with_created_at_boxed<'a>(
        &'a self,
        file: &'a FileInfo,
        len: ReadLen,
    ) -> BoxFuture<'a, Result<(Vec<u8>, CreatedAt), StoreError>> {
        Box::pin(self.read_with_created_at(file, len))
    }

    fn begin_upload_boxed<'a>(
        &'a self,
        key: &'a FileKey,
        created_at: Timestamp,
    ) -> BoxFuture<'a, Result<Box<dyn DynUpload>, StoreError>> {
        let begun = self.begin_upload(key, created_at);
        Box::pin(async move {
            let upload: Box<dyn DynUpload> = Box::new(begun.await?);
            Ok(upload)
        })
    }

    fn delete_boxed<'a>(&'a self, keys: &'a [FileKey]) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.delete(keys))
    }

    fn batch_width_dyn(&self, kind: BatchKind) -> NonZeroUsize {
        self.batch_width(kind)
    }

    fn read_many_boxed<'a>(
        &'a self,
        reads: &'a [ReadAt],
    ) -> BoxFuture<'a, Vec<Result<Vec<u8>, StoreError>>> {
        Box::pin(self.read_many(reads))
    }

    fn list_many_boxed<'a>(
        &'a self,
        pages: &'a [ListAt],
        limit: PageLimit,
    ) -> BoxFuture<'a, Vec<Result<Listing, StoreError>>> {
        Box::pin(self.list_many(pages, limit))
    }

    fn created_at_many_boxed<'a>(
        &'a self,
        files: &'a [FileInfo],
    ) -> BoxFuture<'a, Vec<Result<CreatedAt, StoreError>>> {
        Box::pin(self.created_at_many(files))
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

/// A `dyn DynStore` behind a pointer (a borrow, or an `Arc` in tests) as a sized
/// [`ObjectStore`], for the engine's functions, which take their store by type.
#[derive(Clone, Copy)]
pub(crate) struct Erased<P>(pub(crate) P);

impl<'a, P: Deref<Target = dyn DynStore + 'a> + Send + Sync> ObjectStore for Erased<P> {
    type Upload = Box<dyn DynUpload>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.0.list_boxed(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.0.read_boxed(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.0.created_at_boxed(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.0.read_with_created_at_boxed(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        self.0.begin_upload_boxed(key, created_at).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.0.delete_boxed(keys).await
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        self.0.batch_width_dyn(kind)
    }

    async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        self.0.read_many_boxed(reads).await
    }

    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        self.0.list_many_boxed(pages, limit).await
    }

    async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
        self.0.created_at_many_boxed(files).await
    }
}

/// One operation's running totals.
#[derive(Debug, Default)]
pub(crate) struct OpMeter {
    calls: AtomicU64,
    errors: AtomicU64,
    millis: AtomicU64,
}

impl OpMeter {
    fn note<T>(&self, result: &Result<T, StoreError>, began: Timestamp, ended: Timestamp) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if result.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.millis
            .fetch_add(millis_until(began, ended), Ordering::Relaxed);
    }

    fn read(&self) -> OpCounters {
        OpCounters {
            calls: self.calls.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            millis: self.millis.load(Ordering::Relaxed),
        }
    }
}

/// Every operation's totals, shared between the thread and the health snapshot.
#[derive(Debug, Default)]
pub(crate) struct Meters {
    list: OpMeter,
    read: OpMeter,
    created_at: OpMeter,
    begin_upload: OpMeter,
    write: OpMeter,
    commit: OpMeter,
    abort: OpMeter,
    delete: OpMeter,
    bytes_uploaded: AtomicU64,
}

impl Meters {
    pub(crate) fn read(&self) -> StoreCounters {
        StoreCounters {
            list: self.list.read(),
            read: self.read.read(),
            created_at: self.created_at.read(),
            begin_upload: self.begin_upload.read(),
            write: self.write.read(),
            commit: self.commit.read(),
            abort: self.abort.read(),
            delete: self.delete.read(),
            bytes_uploaded: self.bytes_uploaded.load(Ordering::Relaxed),
        }
    }
}

/// The replica's store as the thread uses it: every call counted.
pub(crate) struct Metered {
    store: Arc<dyn DynStore>,
    meters: Arc<Meters>,
    clock: Arc<dyn Clock>,
}

impl Metered {
    pub(crate) const fn new(
        store: Arc<dyn DynStore>,
        meters: Arc<Meters>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            store,
            meters,
            clock,
        }
    }
}

/// Runs `call`, counting it on `meter`.
async fn metered<T>(
    clock: &dyn Clock,
    meter: &OpMeter,
    call: impl Future<Output = Result<T, StoreError>>,
) -> Result<T, StoreError> {
    let began = clock.now();
    let result = call.await;
    meter.note(&result, began, clock.now());
    result
}

/// An upload whose writes and commit are counted; the bytes count once committed.
pub(crate) struct MeteredUpload {
    inner: Box<dyn DynUpload>,
    meters: Arc<Meters>,
    clock: Arc<dyn Clock>,
    written: u64,
}

impl Upload for MeteredUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        let clock = Arc::clone(&self.clock);
        let meters = Arc::clone(&self.meters);
        let result = metered(
            &*clock,
            &meters.write,
            DynUpload::write_boxed(&mut *self.inner, chunk),
        )
        .await;
        if result.is_ok() {
            let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            self.written = self.written.saturating_add(len);
        }
        result
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let result = metered(
            &*self.clock,
            &self.meters.commit,
            DynUpload::commit_boxed(self.inner),
        )
        .await;
        if result.is_ok() {
            self.meters
                .bytes_uploaded
                .fetch_add(self.written, Ordering::Relaxed);
        }
        result
    }

    async fn abort(self) -> Result<(), StoreError> {
        metered(
            &*self.clock,
            &self.meters.abort,
            DynUpload::abort_boxed(self.inner),
        )
        .await
    }
}

impl ObjectStore for Metered {
    type Upload = MeteredUpload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let call = self.store.list_boxed(level, after, limit);
        metered(&*self.clock, &self.meters.list, call).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let call = self.store.read_boxed(key, offset, len);
        metered(&*self.clock, &self.meters.read, call).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        let call = self.store.created_at_boxed(file);
        metered(&*self.clock, &self.meters.created_at, call).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        let call = self.store.read_with_created_at_boxed(file, len);
        metered(&*self.clock, &self.meters.read, call).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        let call = self.store.begin_upload_boxed(key, created_at);
        let inner = metered(&*self.clock, &self.meters.begin_upload, call).await?;
        Ok(MeteredUpload {
            inner,
            meters: Arc::clone(&self.meters),
            clock: Arc::clone(&self.clock),
            written: 0,
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        let call = self.store.delete_boxed(keys);
        metered(&*self.clock, &self.meters.delete, call).await
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        self.store.batch_width_dyn(kind)
    }

    /// Each read of the batch counts as one call, timed as the whole batch:
    /// the reads ran side by side, so each took about that long.
    async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        let began = self.clock.now();
        let results = self.store.read_many_boxed(reads).await;
        let ended = self.clock.now();
        for result in &results {
            self.meters.read.note(result, began, ended);
        }
        results
    }

    /// Each listing of the batch counts as one call, timed as the whole batch.
    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        let began = self.clock.now();
        let results = self.store.list_many_boxed(pages, limit).await;
        let ended = self.clock.now();
        for result in &results {
            self.meters.list.note(result, began, ended);
        }
        results
    }

    /// Each created-at time of the batch counts as one call, timed as the whole batch.
    async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
        let began = self.clock.now();
        let results = self.store.created_at_many_boxed(files).await;
        let ended = self.clock.now();
        for result in &results {
            self.meters.created_at.note(result, began, ended);
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use lsr_core::conformance::{Failure, Harness, block_on};
    use lsr_core::{FakeClock, MemStore};
    use lsr_ltx::Txid;

    use super::*;

    /// The shared store conformance suite over `dyn DynStore`, so erasing a store changes
    /// nothing a caller can see.
    struct ErasedHarness {
        mem: Arc<MemStore>,
        erased: Erased<Arc<dyn DynStore>>,
    }

    impl ErasedHarness {
        fn new() -> Self {
            let mem = Arc::new(MemStore::new());
            // Owned by an `Arc`, not leaked: AddressSanitizer's leak check (`just asan`) runs
            // over these tests and a leaked store per test fails it.
            let erased: Arc<dyn DynStore> = mem.clone();
            Self {
                mem,
                erased: Erased(erased),
            }
        }
    }

    impl Harness for ErasedHarness {
        type Store = Erased<Arc<dyn DynStore>>;

        fn store(&self) -> &Self::Store {
            &self.erased
        }

        fn plant_unrecognized(&self, level: Level, name: &str) -> Result<(), Failure> {
            self.mem.plant_unrecognized(level, name);
            Ok(())
        }
    }

    lsr_core::conformance_suite!(ErasedHarness::new);

    #[test]
    fn metered_calls_and_bytes_are_counted() {
        let store: Arc<dyn DynStore> = Arc::new(MemStore::new());
        let meters = Arc::new(Meters::default());
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Timestamp::from_unix_millis(0)));
        let metered = Metered::new(store, Arc::clone(&meters), clock);
        let key = FileKey::new(Level::ZERO, TxidRange::new(Txid::MIN, Txid::MIN).unwrap());
        block_on(async {
            let mut upload = metered
                .begin_upload(&key, Timestamp::from_unix_millis(0))
                .await
                .unwrap();
            upload.write(b"abc").await.unwrap();
            upload.commit().await.unwrap();
            metered
                .list(Level::ZERO, None, PageLimit::MAX)
                .await
                .unwrap();
        });
        let counted = meters.read();
        assert_eq!(counted.bytes_uploaded, 3);
        assert_eq!(counted.commit.calls, 1);
        assert_eq!(counted.list.calls, 1);
        assert_eq!(counted.list.errors, 0);
    }
}

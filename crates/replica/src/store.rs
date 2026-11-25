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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lsr_core::{
    BatchKind, Clock, CreatedAt, FileInfo, FileKey, Flight, Level, ListAt, Listing, ObjectStore,
    PageLimit, ReadAt, ReadLen, Reread, Rest, StoreError, Upload,
};
pub use lsr_core::{BoxFuture, DynUpload};
use lsr_ltx::{Timestamp, TxidRange};

use crate::health::{OpCounters, StoreCounters};
use crate::runtime::millis_until;
use lsr_core::WriteMode;

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

    /// [`ObjectStore::begin_sized_upload`].
    fn begin_upload_boxed<'a>(
        &'a self,
        key: &'a FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> BoxFuture<'a, Result<Box<dyn DynUpload>, StoreError>>;

    /// [`ObjectStore::delete`].
    fn delete_boxed<'a>(&'a self, keys: &'a [FileKey]) -> BoxFuture<'a, Result<(), StoreError>>;

    /// [`ObjectStore::remove_scratch`].
    fn remove_scratch_boxed(&self) -> BoxFuture<'_, Result<u64, StoreError>>;

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
        mode: WriteMode,
        len: Option<u64>,
    ) -> BoxFuture<'a, Result<Box<dyn DynUpload>, StoreError>> {
        let begun = self.begin_sized_upload(key, created_at, mode, len);
        Box::pin(async move {
            let upload: Box<dyn DynUpload> = Box::new(begun.await?);
            Ok(upload)
        })
    }

    fn delete_boxed<'a>(&'a self, keys: &'a [FileKey]) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(self.delete(keys))
    }

    fn remove_scratch_boxed(&self) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(self.remove_scratch())
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

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        self.0.begin_upload_boxed(key, created_at, mode, len).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.0.delete_boxed(keys).await
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.0.remove_scratch_boxed().await
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

impl MeteredUpload {
    /// A write, with the source the engine gave when it gave one.
    async fn write_with(
        &mut self,
        chunk: &[u8],
        again: Option<&dyn Reread>,
    ) -> Result<(), StoreError> {
        let clock = Arc::clone(&self.clock);
        let meters = Arc::clone(&self.meters);
        let write = match again {
            Some(again) => DynUpload::write_from_boxed(&mut *self.inner, chunk, again),
            None => DynUpload::write_boxed(&mut *self.inner, chunk),
        };
        let result = metered(&*clock, &meters.write, write).await;
        if result.is_ok() {
            let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            self.written = self.written.saturating_add(len);
        }
        result
    }
}

impl Upload for MeteredUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.write_with(chunk, None).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        self.write_with(chunk, Some(again)).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let commit = DynUpload::commit_boxed(self.inner);
        let result = metered(&*self.clock, &self.meters.commit, commit).await;
        if result.is_ok() {
            self.meters
                .bytes_uploaded
                .fetch_add(self.written, Ordering::Relaxed);
        }
        result
    }

    /// Not a call to count: the question sends nothing. A layer that took the default would
    /// answer that nothing is in flight, and the engine would commit beside its own parts'
    /// requests, waiting inside its tick.
    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        DynUpload::flight_boxed(&mut *self.inner, rest).await
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

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        let call = self.store.begin_upload_boxed(key, created_at, mode, len);
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

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.store.remove_scratch_boxed().await
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

    /// The same suite over the meters, which count each call and must change nothing else.
    mod metered_conformance {
        use super::*;

        struct MeteredHarness {
            mem: Arc<MemStore>,
            metered: Metered,
        }

        impl MeteredHarness {
            fn new() -> Self {
                let mem = Arc::new(MemStore::new());
                let store: Arc<dyn DynStore> = mem.clone();
                let clock: Arc<dyn Clock> =
                    Arc::new(FakeClock::new(Timestamp::from_unix_millis(0)));
                Self {
                    mem,
                    metered: Metered::new(store, Arc::new(Meters::default()), clock),
                }
            }
        }

        impl Harness for MeteredHarness {
            type Store = Metered;

            fn store(&self) -> &Metered {
                &self.metered
            }

            fn plant_unrecognized(&self, level: Level, name: &str) -> Result<(), Failure> {
                self.mem.plant_unrecognized(level, name);
                Ok(())
            }
        }

        lsr_core::conformance_suite!(MeteredHarness::new);
    }

    /// The boot's scratch removal reaches the application's store through each layer the
    /// engine sees it by: the type erasure, its dyn view and the meters.
    #[test]
    fn remove_scratch_is_forwarded_by_every_layer() {
        let mem = MemStore::new();
        let store: Arc<dyn DynStore> = Arc::new(mem.clone());
        mem.plant_scratch(7);
        assert_eq!(block_on(store.remove_scratch_boxed()), Ok(7));
        mem.plant_scratch(5);
        assert_eq!(block_on(Erased(Arc::clone(&store)).remove_scratch()), Ok(5));
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Timestamp::from_unix_millis(0)));
        let metered = Metered::new(store, Arc::new(Meters::default()), clock);
        mem.plant_scratch(3);
        assert_eq!(block_on(metered.remove_scratch()), Ok(3));
        assert_eq!(mem.scratch(), 0);
    }

    /// An upload that notes which way it was written and what it was asked.
    struct Noting {
        inner: <MemStore as ObjectStore>::Upload,
        calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    impl Noting {
        fn note(&self, call: &'static str) {
            self.calls.lock().unwrap().push(call);
        }
    }

    impl Upload for Noting {
        async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
            self.note("write");
            self.inner.write(chunk).await
        }

        async fn write_from(
            &mut self,
            chunk: &[u8],
            _again: &dyn Reread,
        ) -> Result<(), StoreError> {
            self.note("write_from");
            self.inner.write(chunk).await
        }

        async fn commit(self) -> Result<FileInfo, StoreError> {
            self.note("commit");
            self.inner.commit().await
        }

        async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
            self.note(match rest {
                Rest::More => "flight, more to come",
                Rest::None => "flight, nothing more",
            });
            Ok(Flight::Full)
        }

        async fn abort(self) -> Result<(), StoreError> {
            self.inner.abort().await
        }
    }

    /// A source no test reads.
    struct Unread;

    impl Reread for Unread {
        fn open(&self) -> lsr_core::BoxFuture<'_, Result<Box<dyn lsr_core::Again>, StoreError>> {
            Box::pin(async {
                Err(StoreError::Refused {
                    op: lsr_core::StoreOp::Read,
                    status: 0,
                })
            })
        }
    }

    /// The meters sit between the engine and the S3 store in the sidecar. A layer that took
    /// the defaults would turn every write from a staged file into a plain one, and the store
    /// would hold its parts again with nothing failing; and it would answer the engine that
    /// no part is in flight when the store says every lane is taken.
    #[test]
    fn the_meters_hand_the_source_of_a_write_and_the_stores_answers_on() {
        let mem = MemStore::new();
        let meters = Arc::new(Meters::default());
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let key = FileKey::new(Level::ZERO, TxidRange::new(Txid::MIN, Txid::MIN).unwrap());
        block_on(async {
            let inner = mem
                .begin_upload(&key, Timestamp::from_unix_millis(0), WriteMode::Replace)
                .await
                .unwrap();
            let noting = Noting {
                inner,
                calls: Arc::clone(&calls),
            };
            let mut upload = MeteredUpload {
                inner: Box::new(noting),
                meters: Arc::clone(&meters),
                clock: Arc::new(FakeClock::new(Timestamp::from_unix_millis(0))),
                written: 0,
            };
            upload.write_from(b"abc", &Unread).await.unwrap();
            upload.write(b"de").await.unwrap();
            assert_eq!(upload.flight(Rest::More).await, Ok(Flight::Full));
            assert_eq!(upload.flight(Rest::None).await, Ok(Flight::Full));
            upload.commit().await.unwrap();
        });
        assert_eq!(
            *calls.lock().unwrap(),
            [
                "write_from",
                "write",
                "flight, more to come",
                "flight, nothing more",
                "commit"
            ]
        );
        let counted = meters.read();
        assert_eq!(counted.bytes_uploaded, 5);
        assert_eq!(counted.write.calls, 2);
        assert_eq!(counted.commit.calls, 1);
    }

    #[test]
    fn metered_calls_and_bytes_are_counted() {
        let store: Arc<dyn DynStore> = Arc::new(MemStore::new());
        let meters = Arc::new(Meters::default());
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Timestamp::from_unix_millis(0)));
        let metered = Metered::new(store, Arc::clone(&meters), clock);
        let key = FileKey::new(Level::ZERO, TxidRange::new(Txid::MIN, Txid::MIN).unwrap());
        block_on(async {
            let mut upload = metered
                .begin_upload(&key, Timestamp::from_unix_millis(0), WriteMode::Replace)
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

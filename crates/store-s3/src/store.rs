//! `S3Store`: the `ObjectStore` over any `Transport`.
//!
//! Every method is an `async` block around blocking sends and never yields, as
//! `FsStore` does; the trait allows it. A request is built sans-IO by the
//! `RequestBuilder`, sent, then judged by `classify`, so what a status or code
//! means is decided in one place. The store never retries.
//!
//! Batches: a call that needs several independent requests (the parts of an
//! upload, reads ahead, the levels of a listing, a set of stamps) builds them
//! all, hands them to [`Transport::send_many`] with the limit of their kind,
//! and judges each response alone, exactly as a single request. The kinds are
//! limited apart, as Go does: multipart parts by the `concurrency` setting
//! (provider defaults in [`S3Config::concurrency`]), HEADs by
//! `CREATION_TIME_CONCURRENCY`, ranged reads and listing pages by
//! `READ_CONCURRENCY`.
//!
//! Deadlines: the transport bounds one request. A call that sends several (a
//! listing, a large delete) also checks the `Clock` between them against four
//! times the configured deadline and fails as `Transient` `TimedOut` past it.

use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lsr_core::{
    BatchKind, Clock, FileInfo, FileKey, Level, ListAt, ListedStamp, Listing, ObjectStore,
    PageLimit, ReadAt, ReadLen, StampOrigin, StoreError, StoreOp, StoredStamp,
};
use lsr_ltx::{Timestamp, TxidRange};

use crate::classify::{classify, classify_transport, is_range_past_end};
use crate::config::{
    CREATION_TIME_CONCURRENCY, ConfigError, PartSize, Prefix, READ_CONCURRENCY, S3Config,
};
use crate::credentials::S3Credentials;
use crate::orphans::Orphans;
use crate::request::{HttpRequest, MAX_DELETE_KEYS, RequestBuilder, RequestError};
use crate::response::{Head, parse_delete_failures, parse_head, parse_stamp_header};
use crate::transport::{HttpResponse, Transport};
use crate::upload::S3Upload;
#[cfg(feature = "ureq")]
use crate::ureq_transport::UreqTransport;

/// What the stamps read so far found, for the event layer (this crate emits no
/// events). A listing reads none; each [`ObjectStore::stamp`] and
/// [`ObjectStore::read_with_stamp`] that settles adds one.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ListStats {
    /// Stamps that came from the object's metadata.
    pub stamped: u64,
    /// Objects with no stamp metadata, stamped with their `LastModified`.
    pub fell_back: u64,
    /// Files listed and gone by the time they were stamped or read.
    pub vanished: u64,
}

/// A replica on an S3-compatible bucket.
#[cfg(feature = "ureq")]
pub struct S3Store<T: Transport = UreqTransport> {
    inner: Arc<Inner<T>>,
}

/// A replica on an S3-compatible bucket.
#[cfg(not(feature = "ureq"))]
pub struct S3Store<T: Transport> {
    inner: Arc<Inner<T>>,
}

impl<T: Transport> std::fmt::Debug for S3Store<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store").finish_non_exhaustive()
    }
}

/// What a store and its uploads share.
pub(crate) struct Inner<T> {
    pub(crate) builder: RequestBuilder,
    transport: T,
    clock: Arc<dyn Clock>,
    pub(crate) part_size: PartSize,
    pub(crate) concurrency: NonZeroUsize,
    prefix: Prefix,
    deadline: Duration,
    stats: Mutex<ListStats>,
    pub(crate) orphans: Orphans,
}

/// The wall clock, for a store that talks to a real bucket (signatures expire
/// against the server's clock). [`S3Store::new`] and [`S3Store::over`] use it;
/// [`S3Store::with_transport`] takes it, or a fake, explicitly.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "this is the real clock every other component takes as a `Clock`"
    )]
    fn now(&self) -> Timestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
            });
        Timestamp::from_unix_millis(millis)
    }
}

#[cfg(feature = "ureq")]
impl S3Store<UreqTransport> {
    /// A store that talks to the configured endpoint over HTTP(S), signing
    /// with the wall clock.
    ///
    /// # Errors
    /// [`ConfigError`] when the endpoint cannot address the bucket.
    pub fn new(config: S3Config, credentials: S3Credentials) -> Result<Self, ConfigError> {
        let transport = UreqTransport::new(config.deadline());
        Self::over(config, credentials, transport)
    }
}

impl<T: Transport> S3Store<T> {
    /// A store over `transport` (for example a
    /// [`ReqwestTransport`](crate::ReqwestTransport) under the `reqwest`
    /// feature), signing with the wall clock.
    ///
    /// # Errors
    /// [`ConfigError`] when the endpoint cannot address the bucket.
    pub fn over(
        config: S3Config,
        credentials: S3Credentials,
        transport: T,
    ) -> Result<Self, ConfigError> {
        Self::with_transport(config, credentials, transport, SystemClock)
    }

    /// A store over any transport and clock.
    ///
    /// # Errors
    /// [`ConfigError`] when the endpoint cannot address the bucket.
    pub fn with_transport(
        config: S3Config,
        credentials: S3Credentials,
        transport: T,
        clock: impl Clock + 'static,
    ) -> Result<Self, ConfigError> {
        let builder = RequestBuilder::new(&config, &credentials)?;
        Ok(Self {
            inner: Arc::new(Inner {
                builder,
                transport,
                clock: Arc::new(clock),
                part_size: config.part_size(),
                concurrency: config.concurrency(),
                prefix: config.prefix().clone(),
                deadline: config.deadline(),
                stats: Mutex::new(ListStats::default()),
                orphans: Orphans::default(),
            }),
        })
    }

    /// Aborts the multipart uploads that were dropped or failed, oldest first
    /// (ruling F3), and returns how many are settled. The first failure stops
    /// the pass and keeps that upload and the rest listed.
    ///
    /// # Errors
    /// The error of the first abort that failed.
    pub fn abort_orphans(&self) -> Result<usize, StoreError> {
        self.inner.abort_orphans()
    }

    /// How many abandoned multipart uploads await an abort (at most 64).
    #[must_use]
    pub fn pending_orphans(&self) -> usize {
        self.inner.orphans.len()
    }

    /// The stamp counts since the store was made (a listing reads none).
    #[must_use]
    pub fn stamp_stats(&self) -> ListStats {
        *self
            .inner
            .stats
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: Transport> ObjectStore for S3Store<T> {
    type Upload = S3Upload<T>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.inner.list(level, after, limit)
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.inner.read(key, offset, len)
    }

    async fn stamp(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
        self.inner.stamp(file)
    }

    async fn read_with_stamp(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, StoredStamp), StoreError> {
        self.inner.read_with_stamp(file, len)
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<S3Upload<T>, StoreError> {
        // Best effort: a failed abort keeps its id listed for the next try.
        match self.inner.abort_orphans() {
            Ok(_) | Err(_) => {}
        }
        Ok(S3Upload::new(Arc::clone(&self.inner), *key, created_at))
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys)
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        match kind {
            BatchKind::Reads | BatchKind::Lists => READ_CONCURRENCY,
            BatchKind::Stamps => CREATION_TIME_CONCURRENCY,
        }
    }

    async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        self.inner.read_many(reads)
    }

    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        self.inner.list_many(pages, limit)
    }

    async fn stamp_many(&self, files: &[FileInfo]) -> Vec<Result<StoredStamp, StoreError>> {
        self.inner.stamp_many(files)
    }
}

/// The stamp a HEAD settles: the object's metadata when it has one, else the
/// listed `LastModified` marked as the fallback. The size must be the one the
/// caller saw, or the object was overwritten since and a retry reads the new one.
fn stamp_from_head(
    op: StoreOp,
    head: &Head,
    size: u64,
    last_modified: Timestamp,
) -> Result<StoredStamp, StoreError> {
    if head.size != size {
        return Err(StoreError::Transient {
            op,
            kind: io::ErrorKind::InvalidData,
        });
    }
    Ok(match head.stamp {
        Some(stamp) => StoredStamp::metadata(stamp),
        None => StoredStamp::fallback(last_modified),
    })
}

/// A request that cannot be built is the caller's fault and cannot succeed on
/// retry, so it is a refusal; status 0 says no HTTP exchange happened.
fn request_error(op: StoreOp, err: RequestError) -> StoreError {
    match err {
        RequestError::ClockOutOfRange | RequestError::BadBatch { .. } => {
            StoreError::Refused { op, status: 0 }
        }
    }
}

/// The value of a 206's `Content-Range` start: `bytes 5-9/10` gives 5.
fn content_range_start(value: &str) -> Option<u64> {
    value
        .strip_prefix("bytes ")?
        .split('-')
        .next()?
        .trim()
        .parse()
        .ok()
}

impl<T: Transport> Inner<T> {
    pub(crate) fn prefix(&self) -> &Prefix {
        &self.prefix
    }

    pub(crate) fn now(&self) -> Timestamp {
        self.clock.now()
    }

    /// Builds a request stamped with the current time.
    pub(crate) fn request<R>(
        &self,
        op: StoreOp,
        build: impl FnOnce(&RequestBuilder, Timestamp) -> Result<R, RequestError>,
    ) -> Result<R, StoreError> {
        build(&self.builder, self.now()).map_err(|err| request_error(op, err))
    }

    /// Sends and returns whatever came back, whatever the status.
    pub(crate) fn send(
        &self,
        op: StoreOp,
        request: HttpRequest,
    ) -> Result<HttpResponse, StoreError> {
        self.transport
            .send(request)
            .map_err(|err| classify_transport(op, &err))
    }

    /// Sends a batch with at most `limit` requests in flight; one result per
    /// request, in order, whatever each status.
    pub(crate) fn send_many(
        &self,
        op: StoreOp,
        limit: NonZeroUsize,
        requests: Vec<HttpRequest>,
    ) -> Vec<Result<HttpResponse, StoreError>> {
        self.transport
            .send_many(requests, limit)
            .into_iter()
            .map(|sent| sent.map_err(|err| classify_transport(op, &err)))
            .collect()
    }

    /// Sends, and turns a failure status into its [`StoreError`].
    pub(crate) fn exec(
        &self,
        op: StoreOp,
        key: Option<&FileKey>,
        request: HttpRequest,
    ) -> Result<HttpResponse, StoreError> {
        let response = self.send(op, request)?;
        classify(op, key, None, &response)?;
        Ok(response)
    }

    /// Fails as a timeout once more than four deadlines have passed since `start`.
    pub(crate) fn within_budget(&self, op: StoreOp, start: Timestamp) -> Result<(), StoreError> {
        let limit = i64::try_from(self.deadline.saturating_mul(4).as_millis()).unwrap_or(i64::MAX);
        let spent = self.now().unix_millis().saturating_sub(start.unix_millis());
        if spent > limit {
            return Err(StoreError::Transient {
                op,
                kind: io::ErrorKind::TimedOut,
            });
        }
        Ok(())
    }

    fn tally(&self, add: impl FnOnce(&mut ListStats)) {
        add(&mut self.stats.lock().unwrap_or_else(PoisonError::into_inner));
    }

    /// Counts how one stamp read ended: found by metadata or by fallback, or the file was gone.
    fn tally_stamp(&self, result: &Result<StoredStamp, StoreError>) {
        self.tally(|stats| match result {
            Ok(stamp) => match stamp.origin() {
                StampOrigin::Fallback => stats.fell_back = stats.fell_back.saturating_add(1),
                StampOrigin::Metadata | StampOrigin::Mtime => {
                    stats.stamped = stats.stamped.saturating_add(1);
                }
            },
            Err(StoreError::NotFound { .. }) => {
                stats.vanished = stats.vanished.saturating_add(1);
            }
            Err(_) => {}
        });
    }

    /// Builds every request, sends the batch, and judges each response
    /// as [`Self::read`] does. A request that cannot be built fails its slot.
    fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        let op = StoreOp::Read;
        self.batch(
            op,
            READ_CONCURRENCY,
            reads.iter().copied(),
            |at| {
                let request =
                    self.request(op, |b, now| b.get_range(now, &at.key, at.offset, at.len))?;
                Ok((request, at))
            },
            |at, response| Self::range_body(&at.key, at.offset, at.len, response),
        )
    }

    /// One request per item, sent as one batch. `build` makes an item's
    /// request and what judging its answer needs; `judge` turns each response
    /// into the item's result. An item whose request could not be built keeps
    /// that error and is not sent.
    pub(crate) fn batch<I, J, R>(
        &self,
        op: StoreOp,
        limit: NonZeroUsize,
        items: impl IntoIterator<Item = I>,
        mut build: impl FnMut(I) -> Result<(HttpRequest, J), StoreError>,
        judge: impl Fn(J, &HttpResponse) -> Result<R, StoreError>,
    ) -> Vec<Result<R, StoreError>> {
        let mut requests = Vec::new();
        let mut contexts = Vec::new();
        for item in items {
            match build(item) {
                Ok((request, context)) => {
                    requests.push(request);
                    contexts.push(Ok(context));
                }
                Err(err) => contexts.push(Err(err)),
            }
        }
        let mut sent = self.send_many(op, limit, requests).into_iter();
        contexts
            .into_iter()
            .map(|context| {
                let context = context?;
                let response = sent.next().unwrap_or(Err(StoreError::Transient {
                    op,
                    kind: io::ErrorKind::Other,
                }))?;
                judge(context, &response)
            })
            .collect()
    }

    fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let op = StoreOp::Read;
        let request = self.request(op, |b, now| b.get_range(now, key, offset, len))?;
        let response = self.send(op, request)?;
        Self::range_body(key, offset, len, &response)
    }

    /// One HEAD: the stamp the object carries, else the `LastModified` the
    /// listing gave for it (F9), checked against the size the caller saw. An
    /// unparseable stamp is `Corrupt { BadStamp }` here, not at listing time (G6).
    fn stamp(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
        let result = self.head_stamp(file);
        self.tally_stamp(&result);
        result
    }

    fn head_stamp(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
        let op = StoreOp::Read;
        let key = file.key;
        let request = self.request(op, |b, now| b.head(now, &key))?;
        let response = self.send(op, request)?;
        Self::judge_head(file, &response)
    }

    /// What one HEAD's answer says about `file`'s stamp.
    fn judge_head(file: &FileInfo, response: &HttpResponse) -> Result<StoredStamp, StoreError> {
        let op = StoreOp::Read;
        let key = file.key;
        classify(op, Some(&key), None, response)?;
        let head = parse_head(response).map_err(|err| err.into_store(op, Some(key)))?;
        let listed = match file.listed {
            ListedStamp::Stored(stamp) => stamp.at(),
            ListedStamp::LastModified(at) => at,
        };
        stamp_from_head(op, &head, file.size, listed)
    }

    /// One HEAD per file as one batch, each judged and counted as
    /// [`Self::stamp`] does.
    fn stamp_many(&self, files: &[FileInfo]) -> Vec<Result<StoredStamp, StoreError>> {
        let op = StoreOp::Read;
        let results = self.batch(
            op,
            CREATION_TIME_CONCURRENCY,
            files.iter(),
            |file| Ok((self.request(op, |b, now| b.head(now, &file.key))?, file)),
            |file, response| Self::judge_head(file, response),
        );
        for result in &results {
            self.tally_stamp(result);
        }
        results
    }

    /// The first bytes and the stamp from one ranged GET, which carries the
    /// object's metadata; a response without it costs one HEAD more.
    fn read_with_stamp(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, StoredStamp), StoreError> {
        let op = StoreOp::Read;
        let key = file.key;
        let request = self.request(op, |b, now| b.get_range(now, &key, 0, len))?;
        let response = self.send(op, request)?;
        let bytes = Self::range_body(&key, 0, len, &response).inspect_err(|err| {
            if let StoreError::NotFound { .. } = err {
                self.tally_stamp(&Err(err.clone()));
            }
        })?;
        let carried = match response.status() {
            200 | 206 => {
                parse_stamp_header(&response).map_err(|err| err.into_store(op, Some(key)))?
            }
            _ => None,
        };
        match carried {
            Some(at) => {
                let stamp = StoredStamp::metadata(at);
                self.tally_stamp(&Ok(stamp));
                Ok((bytes, stamp))
            }
            None => Ok((bytes, self.stamp(file)?)),
        }
    }

    fn range_body(
        key: &FileKey,
        offset: u64,
        len: ReadLen,
        response: &HttpResponse,
    ) -> Result<Vec<u8>, StoreError> {
        let op = StoreOp::Read;
        if is_range_past_end(response) {
            return Ok(Vec::new());
        }
        classify(op, Some(key), None, response)?;
        let body = response.body();
        let body = if response.status() == 206 {
            // A 206 for another range would hand back the wrong bytes under the right key.
            let starts = response
                .header("content-range")
                .and_then(content_range_start);
            if starts != Some(offset) {
                return Err(StoreError::Transient {
                    op,
                    kind: io::ErrorKind::InvalidData,
                });
            }
            body
        } else {
            // A server that ignored the Range header sent the whole object.
            let from = usize::try_from(offset).unwrap_or(usize::MAX);
            body.get(from..).unwrap_or_default()
        };
        let want = usize::try_from(len.get()).unwrap_or(usize::MAX);
        Ok(body.get(..want).unwrap_or(body).to_vec())
    }

    /// Deletes in batches of `MAX_DELETE_KEYS`, in key order, naming each key once.
    pub(crate) fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        let op = StoreOp::Delete;
        let mut sorted = keys.to_vec();
        sorted.sort();
        sorted.dedup();
        let start = self.now();
        for batch in sorted.chunks(MAX_DELETE_KEYS) {
            self.within_budget(op, start)?;
            let request = self.request(op, |b, now| b.delete_objects(now, batch))?;
            let response = self.exec(op, None, request)?;
            let failed =
                parse_delete_failures(&response).map_err(|err| err.into_store(op, None))?;
            if failed > 0 {
                // A key that is already gone is not a failure; anything else is.
                return Err(StoreError::Transient {
                    op,
                    kind: io::ErrorKind::Other,
                });
            }
        }
        Ok(())
    }
}

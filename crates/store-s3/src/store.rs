//! `S3Store`: the `ObjectStore` over any `Transport`.
//!
//! Every method is an `async` block around blocking sends and never yields, as
//! `FsStore` does; the trait allows it. A request is built sans-IO by the
//! `RequestBuilder`, sent, then judged by `classify`, so what a status or code
//! means is decided in one place. The store never retries.
//!
//! Deadlines: the transport bounds one request. A call that sends several (a
//! listing, a large delete) also checks the `Clock` between them against four
//! times the configured deadline and fails as `Transient` `TimedOut` past it.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lsr_core::{
    Clock, FileKey, Level, Listing, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp,
};
use lsr_ltx::{Timestamp, TxidRange};

use crate::classify::{classify, classify_transport, is_range_past_end};
use crate::config::{ConfigError, PartSize, Prefix, S3Config};
use crate::credentials::S3Credentials;
use crate::request::{HttpRequest, MAX_DELETE_KEYS, RequestBuilder, RequestError};
use crate::response::parse_delete_failures;
use crate::transport::{HttpResponse, Transport};
use crate::upload::S3Upload;
use crate::ureq_transport::UreqTransport;

/// What the last listing cost and found, for the event layer (this crate emits
/// no events).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ListStats {
    /// Files whose stamp came from the object's metadata.
    pub stamped: u64,
    /// Files with no stamp metadata, listed with their `LastModified`.
    pub fell_back: u64,
    /// Files listed and gone by the time they were stamped.
    pub vanished: u64,
}

/// A replica on an S3-compatible bucket.
pub struct S3Store<T: Transport = UreqTransport> {
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
    prefix: Prefix,
    deadline: Duration,
    stats: Mutex<ListStats>,
}

/// The wall clock, for the store that talks to a real bucket (signatures
/// expire against the server's clock).
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
            });
        Timestamp::from_unix_millis(millis)
    }
}

impl S3Store<UreqTransport> {
    /// A store that talks to the configured endpoint over HTTP(S), signing
    /// with the wall clock.
    ///
    /// # Errors
    /// [`ConfigError`] when the endpoint cannot address the bucket.
    pub fn new(config: S3Config, credentials: S3Credentials) -> Result<Self, ConfigError> {
        let transport = UreqTransport::new(config.deadline());
        Self::with_transport(config, credentials, transport, SystemClock)
    }
}

impl<T: Transport> S3Store<T> {
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
                prefix: config.prefix().clone(),
                deadline: config.deadline(),
                stats: Mutex::new(ListStats::default()),
            }),
        })
    }

    /// The counts from the most recent `list` call, finished or failed.
    #[must_use]
    pub fn last_list_stats(&self) -> ListStats {
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<S3Upload<T>, StoreError> {
        Ok(S3Upload::new(Arc::clone(&self.inner), *key, created_at))
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys)
    }
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
    pub(crate) fn request(
        &self,
        op: StoreOp,
        build: impl FnOnce(&RequestBuilder, Timestamp) -> Result<HttpRequest, RequestError>,
    ) -> Result<HttpRequest, StoreError> {
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

    pub(crate) fn record(&self, stats: ListStats) {
        *self.stats.lock().unwrap_or_else(PoisonError::into_inner) = stats;
    }

    fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let op = StoreOp::Read;
        let request = self.request(op, |b, now| b.get_range(now, key, offset, len))?;
        let response = self.send(op, request)?;
        if is_range_past_end(&response) {
            return Ok(Vec::new());
        }
        classify(op, Some(key), None, &response)?;
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

//! The retrying store: the one place core waits and tries again.
//!
//! [`Retrying`] wraps an [`ObjectStore`] and retries `list`, `read`,
//! `delete` and `begin_upload` on the errors [`classify`] calls retryable.
//! Waits are full jitter (uniform up to an exponentially growing, capped
//! bound), so many replicas that fail together do not retry together.
//!
//! Boundaries, each deliberate:
//! - `Upload::write` and `commit` are never retried. The chunk stream cannot
//!   be replayed, and a failed commit may have published, so the right move
//!   is the caller's: list, then decide.
//! - A backend-named wait longer than the policy's cap is returned, not slept:
//!   long waits belong to the caller's schedule, not to a caller holding a
//!   connection and a lock.
//! - Every retry has a limit: after `attempts` calls the last error is returned
//!   as it came.
//! - The upload checks the bytes it wrote against the size the commit reports.
//!   A store that acknowledges a different size than was sent has stored
//!   something else, and a restore would read it as a short file. The wrapper
//!   deletes that object (through the retry policy) before it reports the
//!   error, so a listing never shows a file the wrapper called corrupt.

use std::fmt;
use std::future::Future;
use std::num::{NonZeroU8, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use lsr_ltx::{Timestamp, TxidRange};

use crate::env::{Clock, Entropy, Sleeper};
use crate::layout::CreatedAt;
use crate::layout::{FileInfo, FileKey, Level};
use crate::store::{
    BatchKind, BoxFuture, ListAt, Listing, ObjectStore, PageLimit, ReadLen, ReadStream, Reread,
    StoreError, Upload, WriteMode,
};

/// How a [`StoreError`] should be handled.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Class {
    /// Try again after a jittered wait.
    Retry,
    /// Try again after exactly this long, the backend having named it.
    RetryAfter(Duration),
    /// The object is not there; retrying cannot create it.
    Missing,
    /// Retrying cannot help.
    Permanent,
}

/// Sorts a store error into what to do about it. Exhaustive: a new
/// [`StoreError`] variant breaks this build.
#[must_use]
pub fn classify(err: &StoreError) -> Class {
    match err {
        StoreError::Transient { .. } => Class::Retry,
        StoreError::Throttled { retry_after, .. } => match retry_after {
            Some(wait) if !wait.is_zero() => Class::RetryAfter(*wait),
            Some(_) | None => Class::Retry,
        },
        StoreError::NotFound { .. } => Class::Missing,
        StoreError::Denied { .. }
        | StoreError::AlreadyExists { .. }
        | StoreError::CreateOnlyUnsupported { .. }
        | StoreError::ScratchExhausted { .. }
        | StoreError::ObjectTooLarge { .. }
        | StoreError::Corrupt { .. }
        | StoreError::Refused { .. } => Class::Permanent,
    }
}

/// How many times to try and how long to wait between tries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RetryPolicy {
    /// Calls per operation, the first included.
    pub attempts: NonZeroU8,
    /// The wait bound after the first failure; it doubles per failure.
    pub base: Duration,
    /// The largest wait bound, and the longest backend-named wait honored.
    pub cap: Duration,
}

impl RetryPolicy {
    /// Five attempts, 100 ms doubling to at most 5 s.
    pub const DEFAULT: Self = Self {
        attempts: NonZeroU8::MIN.saturating_add(4),
        base: Duration::from_millis(100),
        cap: Duration::from_secs(5),
    };

    /// The upper bound of the jittered wait after the `failures`-th failure
    /// (1-based): `min(cap, base * 2^(failures - 1))`.
    #[must_use]
    pub fn bound(&self, failures: u32) -> Duration {
        let factor = 1_u32
            .checked_shl(failures.saturating_sub(1))
            .unwrap_or(u32::MAX);
        self.base.saturating_mul(factor).min(self.cap)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What a [`Retrying`] store has done, for resource properties.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct RetryStats {
    /// Calls made to the inner store, retries and uploads' writes and commits
    /// included.
    pub calls: u64,
    /// Calls that were repeats of a failed one.
    pub retries: u64,
    /// Operations that returned a retryable error because the attempts or the
    /// cap were used up.
    pub gave_up: u64,
    /// The sum of every wait asked of the sleeper.
    pub waited: Duration,
}

#[derive(Debug, Default)]
struct Counters {
    calls: AtomicU64,
    retries: AtomicU64,
    gave_up: AtomicU64,
    waited_nanos: AtomicU64,
}

impl Counters {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn add_wait(&self, wait: Duration) {
        let nanos = u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX);
        self.waited_nanos
            .update(Ordering::Relaxed, Ordering::Relaxed, |sum| {
                sum.saturating_add(nanos)
            });
    }
}

/// The store, its policy and its counters, shared by a [`Retrying`] and the
/// uploads it hands out (a mismatched commit deletes through it).
#[derive(Debug)]
struct Core<S, P, E> {
    inner: S,
    sleeper: P,
    entropy: E,
    policy: RetryPolicy,
    counters: Counters,
    listing: Option<ListingBudget>,
}

/// A time budget for all the listing a store does, for a caller that waits on it (a restore run
/// by a person): once it is spent, a failed listing is returned as it came instead of retried.
/// It bounds the retries, not a request in flight: the longest a listing phase can take is the
/// budget plus the transport's deadline for one request.
struct ListingBudget {
    clock: Arc<dyn Clock>,
    budget: Duration,
    /// Unix milliseconds of the first listing call, or [`Self::NOT_STARTED`].
    started: AtomicI64,
    calls: AtomicU64,
    expired: Mutex<Option<ListingExpired>>,
}

impl fmt::Debug for ListingBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListingBudget")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl ListingBudget {
    const NOT_STARTED: i64 = i64::MIN;

    /// Counts `n` listing calls, starting the clock at the first.
    fn calls(&self, n: u64) {
        let now = self.clock.now().unix_millis();
        // The first call wins; a later one finds the start already set.
        self.started
            .compare_exchange(Self::NOT_STARTED, now, Ordering::Relaxed, Ordering::Relaxed)
            .unwrap_or_else(|already| already);
        self.calls.fetch_add(n, Ordering::Relaxed);
    }

    /// Whether a retry begun after `wait` would begin at or past the budget's end.
    fn spent_after(&self, wait: Duration) -> bool {
        let started = self.started.load(Ordering::Relaxed);
        let elapsed = self.clock.now().unix_millis().saturating_sub(started);
        let elapsed = Duration::from_millis(u64::try_from(elapsed).unwrap_or(0));
        elapsed.saturating_add(wait) >= self.budget
    }

    /// Records why the listing stopped, keeping the first record.
    fn expire(&self, last: &StoreError) {
        let mut slot = self.expired.lock().unwrap_or_else(PoisonError::into_inner);
        slot.get_or_insert_with(|| ListingExpired {
            budget: self.budget,
            listings: self.calls.load(Ordering::Relaxed),
            last: last.clone(),
        });
    }
}

/// A listing that stopped retrying because its [`Retrying::with_listing_budget`] ran out.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ListingExpired {
    /// The budget that was spent.
    pub budget: Duration,
    /// Listing calls made, the retries included.
    pub listings: u64,
    /// The error the last call failed with, which is also what the caller was given.
    pub last: StoreError,
}

/// An [`ObjectStore`] that retries the inner store's transient failures.
#[derive(Debug)]
pub struct Retrying<S, P, E> {
    core: Arc<Core<S, P, E>>,
}

impl<S, P, E> Retrying<S, P, E> {
    /// Wraps `inner`.
    #[must_use]
    pub fn new(inner: S, sleeper: P, entropy: E, policy: RetryPolicy) -> Self {
        Self::build(inner, sleeper, entropy, policy, None)
    }

    /// [`Self::new`], with every listing together held to `budget` from the first one: once it
    /// is spent a failed listing is returned, not retried, and [`Self::listing_expired`] says
    /// so. Retries of other operations keep the policy alone. A restore uses it so an endpoint
    /// that never answers fails in about the budget, not in the policy's attempts for every level.
    #[must_use]
    pub fn with_listing_budget(
        inner: S,
        sleeper: P,
        entropy: E,
        policy: RetryPolicy,
        clock: Arc<dyn Clock>,
        budget: Duration,
    ) -> Self {
        let listing = ListingBudget {
            clock,
            budget,
            started: AtomicI64::new(ListingBudget::NOT_STARTED),
            calls: AtomicU64::new(0),
            expired: Mutex::new(None),
        };
        Self::build(inner, sleeper, entropy, policy, Some(listing))
    }

    fn build(
        inner: S,
        sleeper: P,
        entropy: E,
        policy: RetryPolicy,
        listing: Option<ListingBudget>,
    ) -> Self {
        Self {
            core: Arc::new(Core {
                inner,
                sleeper,
                entropy,
                policy,
                counters: Counters::default(),
                listing,
            }),
        }
    }

    /// Why a listing stopped retrying, when the listing budget ran out.
    #[must_use]
    pub fn listing_expired(&self) -> Option<ListingExpired> {
        let budget = self.core.listing.as_ref()?;
        budget
            .expired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The wrapped store.
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.core.inner
    }

    /// What this store has done so far.
    #[must_use]
    pub fn stats(&self) -> RetryStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let counters = &self.core.counters;
        RetryStats {
            calls: load(&counters.calls),
            retries: load(&counters.retries),
            gave_up: load(&counters.gave_up),
            waited: Duration::from_nanos(load(&counters.waited_nanos)),
        }
    }
}

impl<S, P: Sleeper, E: Entropy> Core<S, P, E> {
    /// Full jitter: uniform in `1..=bound` nanoseconds, never zero.
    fn jitter(&self, failures: u32) -> Duration {
        let bound = u64::try_from(self.policy.bound(failures).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        // Clamped so a misbehaving Entropy cannot exceed the bound.
        let draw = self.entropy.below(bound).min(bound.saturating_sub(1));
        Duration::from_nanos(draw.saturating_add(1))
    }

    /// The wait before the next attempt, or `None` when the error is final.
    fn wait_after(&self, failures: u32, err: &StoreError) -> Option<Duration> {
        let wait = match classify(err) {
            Class::Missing | Class::Permanent => return None,
            Class::Retry => None,
            Class::RetryAfter(named) => Some(named),
        };
        let over_cap = wait.is_some_and(|named| named > self.policy.cap);
        if over_cap || failures >= u32::from(self.policy.attempts.get()) {
            Counters::bump(&self.counters.gave_up);
            return None;
        }
        Some(wait.unwrap_or_else(|| self.jitter(failures)))
    }

    async fn run<T, F, Fut>(&self, mut op: F) -> Result<T, StoreError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, StoreError>> + Send,
    {
        Counters::bump(&self.counters.calls);
        let first = op().await;
        self.settle(first, op, false).await
    }

    /// [`Self::run`] for a listing, which the listing budget bounds.
    async fn run_listing<T, F, Fut>(&self, mut op: F) -> Result<T, StoreError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, StoreError>> + Send,
    {
        Counters::bump(&self.counters.calls);
        if let Some(budget) = &self.listing {
            budget.calls(1);
        }
        let first = op().await;
        self.settle(first, op, true).await
    }

    /// Carries on an operation whose first attempt (already counted as a
    /// call) returned `first`, under the same policy as [`Self::run`]: an
    /// item of a batch is retried exactly as a single call would be.
    async fn settle<T, F, Fut>(
        &self,
        first: Result<T, StoreError>,
        mut op: F,
        listing: bool,
    ) -> Result<T, StoreError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, StoreError>> + Send,
    {
        let budget = self.listing.as_ref().filter(|_| listing);
        let mut failures = 0_u32;
        let mut outcome = first;
        loop {
            let err = match outcome {
                Ok(done) => return Ok(done),
                Err(err) => err,
            };
            failures = failures.saturating_add(1);
            let Some(wait) = self.wait_after(failures, &err) else {
                return Err(err);
            };
            if let Some(budget) = budget {
                if budget.spent_after(wait) {
                    budget.expire(&err);
                    return Err(err);
                }
                budget.calls(1);
            }
            self.counters.add_wait(wait);
            Counters::bump(&self.counters.retries);
            self.sleeper.sleep(wait).await;
            Counters::bump(&self.counters.calls);
            outcome = op().await;
        }
    }
}

impl<S: ObjectStore + 'static, P: Sleeper + 'static, E: Entropy + 'static> ObjectStore
    for Retrying<S, P, E>
{
    type Upload = RetryingUpload<S, P, E>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let core = &*self.core;
        core.run_listing(|| core.inner.list(level, after, limit))
            .await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let core = &*self.core;
        core.run(|| core.inner.read(key, offset, len)).await
    }

    fn read_stream(&self, key: FileKey, offset: u64) -> Box<dyn ReadStream + '_> {
        Box::new(Resuming {
            core: &self.core,
            key,
            offset,
            open: None,
        })
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        let core = &*self.core;
        core.run(|| core.inner.created_at(file)).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        let core = &*self.core;
        core.run(|| core.inner.read_with_created_at(file, len))
            .await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        let core = &*self.core;
        let inner = core
            .run(|| core.inner.begin_sized_upload(key, created_at, mode, len))
            .await?;
        Ok(RetryingUpload {
            inner,
            key: *key,
            written: 0,
            mode,
            core: Arc::clone(&self.core),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        let core = &*self.core;
        core.run(|| core.inner.delete(keys)).await
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        let core = &*self.core;
        core.run(|| core.inner.remove_scratch()).await
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        self.core.inner.batch_width(kind)
    }

    /// One batch through the inner store, then each failed created-at time read retried
    /// alone under the policy, in order.
    async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
        let core = &*self.core;
        let firsts = core.inner.created_at_many(files).await;
        let mut out = Vec::with_capacity(firsts.len());
        for (file, first) in files.iter().zip(firsts) {
            Counters::bump(&core.counters.calls);
            out.push(
                core.settle(first, || core.inner.created_at(file), false)
                    .await,
            );
        }
        out
    }

    /// One batch through the inner store, then each failed listing retried
    /// alone under the policy, in order.
    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        let core = &*self.core;
        if let Some(budget) = &core.listing {
            budget.calls(u64::try_from(pages.len()).unwrap_or(u64::MAX));
        }
        let firsts = core.inner.list_many(pages, limit).await;
        let mut out = Vec::with_capacity(firsts.len());
        for (at, first) in pages.iter().zip(firsts) {
            Counters::bump(&core.counters.calls);
            let settled = core
                .settle(first, || core.inner.list(at.level, at.after, limit), true)
                .await;
            out.push(settled);
        }
        out
    }
}

/// A stream through a [`Retrying`] store. A piece that fails is asked again of a new stream
/// from the offset the pieces before it reached, under the policy as a read would be, so a
/// connection lost a gigabyte into a file costs one request more, not the file.
struct Resuming<'a, S, P, E> {
    core: &'a Core<S, P, E>,
    key: FileKey,
    /// Where the next piece starts: the bytes handed out so far.
    offset: u64,
    /// The inner stream, once one answered; `None` before the first piece and after a failure.
    open: Option<Box<dyn ReadStream + 'a>>,
}

impl<S: ObjectStore, P: Sleeper, E: Entropy> ReadStream for Resuming<'_, S, P, E> {
    fn next(&mut self, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            let (core, key, offset) = (self.core, self.key, self.offset);
            let reopen = move || async move {
                let mut fresh = core.inner.read_stream(key, offset);
                let bytes = fresh.next(len).await?;
                Ok((fresh, bytes))
            };
            // A stream that failed is dropped here: the retry opens another.
            let first = match self.open.take() {
                Some(mut open) => open.next(len).await.map(|bytes| (open, bytes)),
                None => {
                    Counters::bump(&core.counters.calls);
                    reopen().await
                }
            };
            let (open, bytes) = core.settle(first, reopen, false).await?;
            self.open = Some(open);
            let read = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            self.offset = offset.saturating_add(read);
            Ok(bytes)
        })
    }
}

/// An upload through a [`Retrying`] store: passes writes and the commit
/// through once, and checks the committed size.
///
/// A commit whose stored size differs is reported, never taken as whole. After
/// a create-only write the object is deleted first; after a replace it is left,
/// since the key may have held an acknowledged object (`crate::publish::settle`,
/// the rule every engine commit follows); see
/// [`crate::store::CorruptCause::CommitSizeMismatch`] for what the caller may conclude.
#[derive(Debug)]
pub struct RetryingUpload<S: ObjectStore, P, E> {
    inner: S::Upload,
    key: FileKey,
    written: u64,
    mode: WriteMode,
    core: Arc<Core<S, P, E>>,
}

impl<S: ObjectStore, P: Sleeper, E: Entropy> RetryingUpload<S, P, E> {
    /// Commits the inner upload, through `again` when the upload was written from a source,
    /// and checks the answer against the bytes written.
    async fn commit_with(self, again: Option<&dyn Reread>) -> Result<FileInfo, StoreError> {
        Counters::bump(&self.core.counters.calls);
        let info = match again {
            Some(again) => self.inner.commit_from(again).await?,
            None => self.inner.commit().await?,
        };
        let core = &*self.core;
        let keys = [self.key];
        crate::publish::settle(info, self.key, self.written, self.mode, || {
            core.run(|| core.inner.delete(&keys))
        })
        .await
    }
}

impl<S: ObjectStore, P: Sleeper, E: Entropy> Upload for RetryingUpload<S, P, E> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        Counters::bump(&self.core.counters.calls);
        self.inner.write(chunk).await?;
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.written = self.written.saturating_add(len);
        Ok(())
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        Counters::bump(&self.core.counters.calls);
        self.inner.write_from(chunk, again).await?;
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.written = self.written.saturating_add(len);
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        self.commit_with(None).await
    }

    async fn commit_from(self, again: &dyn Reread) -> Result<FileInfo, StoreError> {
        self.commit_with(Some(again)).await
    }

    async fn abort(self) -> Result<(), StoreError> {
        Counters::bump(&self.core.counters.calls);
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests;

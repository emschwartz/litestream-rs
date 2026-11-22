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

use std::future::Future;
use std::num::{NonZeroU8, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lsr_ltx::{Timestamp, TxidRange};

use crate::env::{Entropy, Sleeper};
use crate::layout::CreatedAt;
use crate::layout::{FileInfo, FileKey, Level};
use crate::store::{
    BatchKind, ListAt, Listing, ObjectStore, PageLimit, ReadAt, ReadLen, StoreError, Upload,
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
        | StoreError::ScratchExhausted { .. }
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
        Self {
            core: Arc::new(Core {
                inner,
                sleeper,
                entropy,
                policy,
                counters: Counters::default(),
            }),
        }
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
        self.settle(first, op).await
    }

    /// Carries on an operation whose first attempt (already counted as a
    /// call) returned `first`, under the same policy as [`Self::run`]: an
    /// item of a batch is retried exactly as a single call would be.
    async fn settle<T, F, Fut>(
        &self,
        first: Result<T, StoreError>,
        mut op: F,
    ) -> Result<T, StoreError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<T, StoreError>> + Send,
    {
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
            self.counters.add_wait(wait);
            Counters::bump(&self.counters.retries);
            self.sleeper.sleep(wait).await;
            Counters::bump(&self.counters.calls);
            outcome = op().await;
        }
    }
}

impl<S: ObjectStore, P: Sleeper, E: Entropy> ObjectStore for Retrying<S, P, E> {
    type Upload = RetryingUpload<S, P, E>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let core = &*self.core;
        core.run(|| core.inner.list(level, after, limit)).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let core = &*self.core;
        core.run(|| core.inner.read(key, offset, len)).await
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        let core = &*self.core;
        let inner = core
            .run(|| core.inner.begin_upload(key, created_at))
            .await?;
        Ok(RetryingUpload {
            inner,
            key: *key,
            written: 0,
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
            out.push(core.settle(first, || core.inner.created_at(file)).await);
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
        let firsts = core.inner.list_many(pages, limit).await;
        let mut out = Vec::with_capacity(firsts.len());
        for (at, first) in pages.iter().zip(firsts) {
            Counters::bump(&core.counters.calls);
            let settled = core
                .settle(first, || core.inner.list(at.level, at.after, limit))
                .await;
            out.push(settled);
        }
        out
    }

    /// One batch through the inner store, then each failed read retried
    /// alone under the policy, in order.
    async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        let core = &*self.core;
        let firsts = core.inner.read_many(reads).await;
        let mut out = Vec::with_capacity(firsts.len());
        for (at, first) in reads.iter().zip(firsts) {
            Counters::bump(&core.counters.calls);
            let settled = core
                .settle(first, || core.inner.read(&at.key, at.offset, at.len))
                .await;
            out.push(settled);
        }
        out
    }
}

/// An upload through a [`Retrying`] store: passes writes and the commit
/// through once, and checks the committed size.
///
/// A commit whose stored size differs deletes the object before it reports
/// the error, so a mismatch never leaves a listable file; see
/// [`crate::store::CorruptCause::CommitSizeMismatch`] for what the caller may conclude.
#[derive(Debug)]
pub struct RetryingUpload<S: ObjectStore, P, E> {
    inner: S::Upload,
    key: FileKey,
    written: u64,
    core: Arc<Core<S, P, E>>,
}

impl<S: ObjectStore, P: Sleeper, E: Entropy> Upload for RetryingUpload<S, P, E> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        Counters::bump(&self.core.counters.calls);
        self.inner.write(chunk).await?;
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.written = self.written.saturating_add(len);
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        Counters::bump(&self.core.counters.calls);
        let info = self.inner.commit().await?;
        let core = &*self.core;
        let keys = [self.key];
        crate::checked::settle(info, self.key, self.written, || {
            core.run(|| core.inner.delete(&keys))
        })
        .await
    }

    async fn abort(self) -> Result<(), StoreError> {
        Counters::bump(&self.core.counters.calls);
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests;

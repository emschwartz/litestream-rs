//! Store wrappers: faults, crash points, ground truth and power loss.
//!
//! Each wrapper is an [`ObjectStore`] over another. The simulator stacks them
//! as `Faults<Yield<Tap<Stage<MemStore>>>>` for the staging directory and
//! `Faults<Yield<Tap<MemStore>>>` for the replica, and keeps a plain `MemStore`
//! handle to the same state: the oracle's restores read through that handle, so
//! no fault or yield ever touches a check.
//!
//! - [`Faults`] fails calls on a schedule. Faults live here so the raw store
//!   stays clean.
//! - [`Yield`] returns `Pending` once before every call, which is what makes
//!   each store call a crash point for the executor.
//! - [`Tap`] records what the store really did, below the faults, so a commit
//!   that a fault turned into an error still shows as published. The code under
//!   test never reads it.
//! - [`Stage`] tracks which staged files are not yet durable, for a power loss.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use lsr_core::{
    Entropy, FileInfo, FileKey, Level, Listing, ObjectStore, PageLimit, ReadLen, SeededEntropy,
    StoreError, StoreOp, Upload,
};
use lsr_ltx::{HEADER_LEN, Header, Timestamp, TxidRange, WalOrigin};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // No guarded section panics, so poison carries no half-written state.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn bump(count: &mut u64) {
    *count = count.saturating_add(1);
}

// ---------------------------------------------------------------- Faults

/// What a scheduled fault does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultShape {
    /// The call fails before it has any effect: a commit that fails this way
    /// publishes nothing.
    Error(StoreError),
    /// The commit publishes the object and then fails: the response was lost,
    /// or the directory sync failed after the rename. Counted over commits, and
    /// meaningful for `StoreOp::Upload` only.
    ErrorAfterPublish(StoreError),
    /// The commit publishes the object and reports a size one byte too large.
    /// Counted over commits, `StoreOp::Upload` only.
    SizeMismatch,
    /// The read returns its bytes with the lowest bit of the byte at object
    /// offset `at` flipped when the range covers it; the stored bytes are
    /// untouched. `StoreOp::Read` only.
    FlipByteOnRead {
        /// The object offset of the byte.
        at: u64,
    },
    /// From the scheduled call on, every call of the operation fails with
    /// `err` with probability `per_mille` in a thousand, drawn from the seed.
    Rate {
        /// The chance per call, out of 1000.
        per_mille: u16,
        /// The error.
        err: StoreError,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Call {
    List,
    Read,
    Begin,
    Write,
    Commit,
    Delete,
}

impl Call {
    const fn op(self) -> StoreOp {
        match self {
            Self::List => StoreOp::List,
            Self::Read => StoreOp::Read,
            Self::Begin | Self::Write | Self::Commit => StoreOp::Upload,
            Self::Delete => StoreOp::Delete,
        }
    }
}

enum Verdict {
    Pass,
    Fail(StoreError),
    AfterPublish(StoreError),
    SizeMismatch,
    Flip(u64),
}

#[derive(Debug)]
struct Entry {
    op: StoreOp,
    nth: u32,
    shape: FaultShape,
    fired: bool,
}

#[derive(Debug)]
struct FaultState {
    lists: u32,
    reads: u32,
    uploads: u32,
    deletes: u32,
    commits: u32,
    schedule: Vec<Entry>,
    rng: SeededEntropy,
}

impl FaultState {
    fn count(&mut self, op: StoreOp) -> &mut u32 {
        match op {
            StoreOp::List => &mut self.lists,
            StoreOp::Read => &mut self.reads,
            StoreOp::Upload => &mut self.uploads,
            StoreOp::Delete => &mut self.deletes,
        }
    }

    /// Counts the call and returns the first scheduled fault it triggers.
    fn decide(&mut self, call: Call) -> Verdict {
        let op = call.op();
        let seen = {
            let slot = self.count(op);
            let seen = *slot;
            *slot = seen.saturating_add(1);
            seen
        };
        let commit_seen = (call == Call::Commit).then(|| {
            let seen = self.commits;
            self.commits = seen.saturating_add(1);
            seen
        });
        let Self { schedule, rng, .. } = self;
        for entry in schedule.iter_mut().filter(|e| !e.fired && e.op == op) {
            let hit = match &entry.shape {
                FaultShape::Error(err) => (seen == entry.nth).then(|| Verdict::Fail(err.clone())),
                FaultShape::ErrorAfterPublish(err) => {
                    (commit_seen == Some(entry.nth)).then(|| Verdict::AfterPublish(err.clone()))
                }
                FaultShape::SizeMismatch => {
                    (commit_seen == Some(entry.nth)).then_some(Verdict::SizeMismatch)
                }
                FaultShape::FlipByteOnRead { at } => {
                    (call == Call::Read && seen == entry.nth).then_some(Verdict::Flip(*at))
                }
                FaultShape::Rate { per_mille, err } => (seen >= entry.nth
                    && rng.below(1000) < u64::from(*per_mille))
                .then(|| Verdict::Fail(err.clone())),
            };
            if let Some(verdict) = hit {
                entry.fired = !matches!(entry.shape, FaultShape::Rate { .. });
                return verdict;
            }
        }
        Verdict::Pass
    }
}

/// Schedules faults after the wrapper is built, and reads its call counts.
#[derive(Clone, Debug)]
pub struct FaultHandle {
    state: Arc<Mutex<FaultState>>,
}

impl FaultHandle {
    /// Schedules `shape` for the `nth` call of `op` (counted from the start,
    /// zero-based; see [`FaultShape`] for what `nth` counts per shape).
    pub fn push(&self, op: StoreOp, nth: u32, shape: FaultShape) {
        lock(&self.state).schedule.push(Entry {
            op,
            nth,
            shape,
            fired: false,
        });
    }

    /// Schedules `shape` `ahead` calls from now (0: the next call).
    pub fn push_in(&self, op: StoreOp, ahead: u32, shape: FaultShape) {
        let mut state = lock(&self.state);
        let base = match shape {
            FaultShape::ErrorAfterPublish(_) | FaultShape::SizeMismatch => state.commits,
            FaultShape::Error(_) | FaultShape::FlipByteOnRead { .. } | FaultShape::Rate { .. } => {
                *state.count(op)
            }
        };
        state.schedule.push(Entry {
            op,
            nth: base.saturating_add(ahead),
            shape,
            fired: false,
        });
    }

    /// How many calls of `op` have been made.
    #[must_use]
    pub fn calls(&self, op: StoreOp) -> u32 {
        *lock(&self.state).count(op)
    }

    /// One-shot faults that have not fired yet.
    #[must_use]
    pub fn pending(&self) -> usize {
        lock(&self.state)
            .schedule
            .iter()
            .filter(|e| !e.fired && !matches!(e.shape, FaultShape::Rate { .. }))
            .count()
    }
}

/// A store that fails on a seeded schedule.
#[derive(Clone, Debug)]
pub struct Faults<S> {
    inner: S,
    state: Arc<Mutex<FaultState>>,
    stage: Option<StageHandle>,
}

impl<S> Faults<S> {
    /// Wraps `inner` with `schedule`; `seed` drives [`FaultShape::Rate`].
    #[must_use]
    pub fn new(inner: S, schedule: Vec<(StoreOp, u32, FaultShape)>, seed: u64) -> Self {
        let schedule = schedule
            .into_iter()
            .map(|(op, nth, shape)| Entry {
                op,
                nth,
                shape,
                fired: false,
            })
            .collect();
        let state = FaultState {
            lists: 0,
            reads: 0,
            uploads: 0,
            deletes: 0,
            commits: 0,
            schedule,
            rng: SeededEntropy::new(seed),
        };
        Self {
            inner,
            state: Arc::new(Mutex::new(state)),
            stage: None,
        }
    }

    /// Marks a key volatile in `stage` whenever a commit publishes and then
    /// fails: the staging store's "rename done, sync failed".
    #[must_use]
    pub fn volatile_on_late_error(mut self, stage: StageHandle) -> Self {
        self.stage = Some(stage);
        self
    }

    /// The handle for scheduling more faults.
    #[must_use]
    pub fn handle(&self) -> FaultHandle {
        FaultHandle {
            state: Arc::clone(&self.state),
        }
    }

    fn verdict(&self, call: Call) -> Verdict {
        lock(&self.state).decide(call)
    }
}

fn flip(bytes: &mut [u8], offset: u64, at: u64) {
    let index = at.checked_sub(offset).and_then(|i| usize::try_from(i).ok());
    if let Some(byte) = index.and_then(|i| bytes.get_mut(i)) {
        *byte ^= 1;
    }
}

impl<S: ObjectStore> ObjectStore for Faults<S> {
    type Upload = FaultsUpload<S::Upload>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::List) {
            return Err(err);
        }
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let verdict = self.verdict(Call::Read);
        if let Verdict::Fail(err) = &verdict {
            return Err(err.clone());
        }
        let mut bytes = self.inner.read(key, offset, len).await?;
        if let Verdict::Flip(at) = verdict {
            flip(&mut bytes, offset, at);
        }
        Ok(bytes)
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::Begin) {
            return Err(err);
        }
        let inner = self.inner.begin_upload(key, created_at).await?;
        Ok(FaultsUpload {
            inner,
            key: *key,
            wrote: 0,
            state: Arc::clone(&self.state),
            stage: self.stage.clone(),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::Delete) {
            return Err(err);
        }
        self.inner.delete(keys).await
    }
}

/// An upload through [`Faults`].
#[derive(Debug)]
pub struct FaultsUpload<U> {
    inner: U,
    key: FileKey,
    wrote: u64,
    state: Arc<Mutex<FaultState>>,
    stage: Option<StageHandle>,
}

impl<U: Upload> Upload for FaultsUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        if let Verdict::Fail(err) = lock(&self.state).decide(Call::Write) {
            return Err(err);
        }
        self.inner.write(chunk).await?;
        self.wrote = self
            .wrote
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let verdict = lock(&self.state).decide(Call::Commit);
        match verdict {
            Verdict::Fail(err) => {
                drop(self.inner);
                Err(err)
            }
            Verdict::AfterPublish(err) => {
                self.inner.commit().await?;
                if let Some(stage) = &self.stage {
                    stage.mark_volatile(self.key);
                }
                Err(err)
            }
            Verdict::SizeMismatch => {
                let mut info = self.inner.commit().await?;
                info.size = info.size.saturating_add(1);
                Ok(info)
            }
            Verdict::Pass | Verdict::Flip(_) => self.inner.commit().await,
        }
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

// ----------------------------------------------------------------- Yield

/// Returns `Pending` once, waking itself, then completes.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A store whose every call is one await point.
#[derive(Clone, Debug)]
pub struct Yield<S> {
    inner: S,
}

impl<S> Yield<S> {
    /// Wraps `inner`.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: ObjectStore> ObjectStore for Yield<S> {
    type Upload = YieldUpload<S::Upload>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        YieldNow(false).await;
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        YieldNow(false).await;
        self.inner.read(key, offset, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        YieldNow(false).await;
        let inner = self.inner.begin_upload(key, created_at).await?;
        Ok(YieldUpload { inner })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.delete(keys).await
    }
}

/// An upload through [`Yield`].
#[derive(Debug)]
pub struct YieldUpload<U> {
    inner: U,
}

impl<U: Upload> Upload for YieldUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.write(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        YieldNow(false).await;
        self.inner.commit().await
    }

    async fn abort(self) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.abort().await
    }
}

// ------------------------------------------------------------------- Tap

/// One thing the store really did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TapEvent {
    /// A commit.
    Put {
        /// The object.
        key: FileKey,
        /// Its header, `None` when the first bytes written are not a valid one.
        header: Option<Header>,
        /// The WAL range its pages came from, from the header.
        origin: Option<WalOrigin>,
    },
    /// A delete the store accepted.
    Delete {
        /// The keys it was asked to remove.
        keys: Vec<FileKey>,
    },
}

/// Call counts a [`Tap`] keeps for the resource properties.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TapCounts {
    /// Listing calls.
    pub lists: u64,
    /// Read calls.
    pub reads: u64,
    /// Uploads begun.
    pub begins: u64,
    /// Chunks written.
    pub writes: u64,
    /// Commits attempted.
    pub commits: u64,
    /// Delete calls.
    pub deletes: u64,
    /// The most bytes one upload held before it ended.
    pub max_upload_bytes: u64,
    /// The largest single chunk handed to `write`: what a streaming writer
    /// holds at once, whatever the object's size.
    pub max_write_bytes: u64,
    /// Every byte handed to `write`.
    pub written_bytes: u64,
}

#[derive(Debug, Default)]
struct TapState {
    events: Vec<TapEvent>,
    counts: TapCounts,
}

/// The reading side of a [`Tap`].
#[derive(Clone, Debug, Default)]
pub struct TapLog {
    state: Arc<Mutex<TapState>>,
}

impl TapLog {
    /// Every event so far, oldest first.
    #[must_use]
    pub fn events(&self) -> Vec<TapEvent> {
        lock(&self.state).events.clone()
    }

    /// How many events there are: the stamp the model records per commit.
    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.state).events.len()
    }

    /// Whether nothing has happened yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The call counts.
    #[must_use]
    pub fn counts(&self) -> TapCounts {
        lock(&self.state).counts
    }

    fn with(&self, f: impl FnOnce(&mut TapState)) {
        f(&mut lock(&self.state));
    }
}

/// A store that records what it did.
#[derive(Clone, Debug)]
pub struct Tap<S> {
    inner: S,
    log: TapLog,
}

impl<S> Tap<S> {
    /// Wraps `inner` with an empty log.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            log: TapLog::default(),
        }
    }

    /// The log; clones share it.
    #[must_use]
    pub fn log(&self) -> TapLog {
        self.log.clone()
    }
}

impl<S: ObjectStore> ObjectStore for Tap<S> {
    type Upload = TapUpload<S>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.log.with(|s| bump(&mut s.counts.lists));
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.log.with(|s| bump(&mut s.counts.reads));
        self.inner.read(key, offset, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        self.log.with(|s| bump(&mut s.counts.begins));

        let inner = self.inner.begin_upload(key, created_at).await?;
        Ok(TapUpload {
            inner,
            key: *key,
            head: Vec::new(),
            wrote: 0,
            log: self.log.clone(),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.log.with(|s| bump(&mut s.counts.deletes));

        self.inner.delete(keys).await?;
        self.log.with(|s| {
            s.events.push(TapEvent::Delete {
                keys: keys.to_vec(),
            });
        });
        Ok(())
    }
}

/// An upload through [`Tap`].
#[derive(Debug)]
pub struct TapUpload<S: ObjectStore> {
    inner: S::Upload,
    key: FileKey,
    head: Vec<u8>,
    wrote: u64,
    log: TapLog,
}

fn parse_header(head: &[u8]) -> Option<Header> {
    let bytes: &[u8; HEADER_LEN] = head.try_into().ok()?;
    Header::decode(bytes).ok()
}

impl<S: ObjectStore> Upload for TapUpload<S> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.inner.write(chunk).await?;
        let room = HEADER_LEN.saturating_sub(self.head.len());
        self.head
            .extend_from_slice(chunk.get(..room.min(chunk.len())).unwrap_or_default());
        self.wrote = self
            .wrote
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        let wrote = self.wrote;
        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.log.with(|s| {
            bump(&mut s.counts.writes);
            s.counts.max_upload_bytes = s.counts.max_upload_bytes.max(wrote);
            s.counts.max_write_bytes = s.counts.max_write_bytes.max(chunk_len);
            s.counts.written_bytes = s.counts.written_bytes.saturating_add(chunk_len);
        });
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let Self {
            inner,
            key,
            head,
            log,
            ..
        } = self;
        let header = parse_header(&head);
        let outcome = inner.commit().await;
        log.with(|s| {
            bump(&mut s.counts.commits);
            // Only an object the store accepted is recorded: the tap sits below
            // `Faults`, so a commit it sees fail never published.
            if outcome.is_ok() {
                s.events.push(TapEvent::Put {
                    key,
                    header,
                    origin: header.and_then(|h| h.fields().wal),
                });
            }
        });
        outcome
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

// ----------------------------------------------------------------- Stage

#[derive(Debug, Default)]
struct StageState {
    volatile: BTreeSet<FileKey>,
    stamps: BTreeMap<FileKey, Timestamp>,
}

/// What a power loss did to one volatile staged file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lost {
    /// The file is gone.
    Dropped,
    /// The file is cut to its first `kept` bytes.
    Torn {
        /// The bytes that survive, fewer than the file had.
        kept: u64,
    },
}

/// The side of a [`Stage`] the simulator keeps: which keys are volatile, and the
/// power loss itself.
#[derive(Clone, Debug, Default)]
pub struct StageHandle {
    state: Arc<Mutex<StageState>>,
}

/// How much a power loss reads of a file at once.
const LOSS_READ: u32 = 1 << 20;

impl StageHandle {
    /// A key that is visible but not yet durable.
    pub fn mark_volatile(&self, key: FileKey) {
        lock(&self.state).volatile.insert(key);
    }

    /// A directory sync: every earlier rename is now durable.
    pub fn fsync_all(&self) {
        lock(&self.state).volatile.clear();
    }

    /// The keys a power loss would hit now.
    #[must_use]
    pub fn volatile(&self) -> Vec<FileKey> {
        lock(&self.state).volatile.iter().copied().collect()
    }

    /// Drops each volatile key or cuts it to a seeded prefix of its bytes, in
    /// `raw` (a handle to the store under the wrappers), then forgets the volatile
    /// set. A key that is already gone is skipped.
    ///
    /// # Errors
    /// The store's error when it cannot read, delete or rewrite a file.
    pub async fn power_loss<S: ObjectStore>(
        &self,
        raw: &S,
        entropy: &dyn Entropy,
    ) -> Result<Vec<(FileKey, Lost)>, StoreError> {
        let keys = self.volatile();
        let mut lost = Vec::new();
        for key in keys {
            let bytes = match read_all(raw, &key).await {
                Ok(bytes) => bytes,
                Err(StoreError::NotFound { .. }) => continue,
                Err(other) => return Err(other),
            };
            let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let torn = entropy.below(2) == 1;
            if torn {
                let kept = entropy.below(len);
                let prefix = bytes
                    .get(..usize::try_from(kept).unwrap_or(0))
                    .unwrap_or_default();
                let stamp = lock(&self.state).stamps.get(&key).copied();
                let mut up = raw
                    .begin_upload(&key, stamp.unwrap_or(Timestamp::from_unix_millis(0)))
                    .await?;
                up.write(prefix).await?;
                up.commit().await?;
                lost.push((key, Lost::Torn { kept }));
            } else {
                raw.delete(&[key]).await?;
                lost.push((key, Lost::Dropped));
            }
        }
        self.fsync_all();
        Ok(lost)
    }
}

async fn read_all<S: ObjectStore>(store: &S, key: &FileKey) -> Result<Vec<u8>, StoreError> {
    let Some(chunk) = ReadLen::new(LOSS_READ) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    loop {
        let offset = u64::try_from(out.len()).unwrap_or(u64::MAX);
        let part = store.read(key, offset, chunk).await?;
        let done = u64::try_from(part.len()).unwrap_or(0) < chunk.get();
        out.extend_from_slice(&part);
        if done {
            return Ok(out);
        }
    }
}

/// Power-loss bookkeeping for the staging store.
///
/// A commit that returns an error after publishing models "rename done, sync
/// failed": the key is visible and volatile. [`Faults`] reports such a commit
/// through [`Faults::volatile_on_late_error`], because the wrapper that fails a
/// commit sits above this one and this one saw it succeed. A later successful
/// commit is a directory sync and makes every earlier rename durable.
#[derive(Clone, Debug)]
pub struct Stage<S> {
    inner: S,
    handle: StageHandle,
}

impl<S> Stage<S> {
    /// Wraps `inner` with nothing volatile.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            handle: StageHandle::default(),
        }
    }

    /// The handle; clones share state.
    #[must_use]
    pub fn handle(&self) -> StageHandle {
        self.handle.clone()
    }
}

impl<S: ObjectStore> ObjectStore for Stage<S> {
    type Upload = StageUpload<S::Upload>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.inner.read(key, offset, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        let inner = self.inner.begin_upload(key, created_at).await?;
        lock(&self.handle.state).stamps.insert(*key, created_at);
        Ok(StageUpload {
            inner,
            handle: self.handle.clone(),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await?;
        let mut state = lock(&self.handle.state);
        for key in keys {
            state.volatile.remove(key);
            state.stamps.remove(key);
        }
        Ok(())
    }
}

/// An upload through [`Stage`].
#[derive(Debug)]
pub struct StageUpload<U> {
    inner: U,
    handle: StageHandle,
}

impl<U: Upload> Upload for StageUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.inner.write(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let info = self.inner.commit().await?;
        self.handle.fsync_all();
        Ok(info)
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests;

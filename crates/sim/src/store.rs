//! Store wrappers: faults, crash points, ground truth and power loss.
//!
//! Each wrapper is an [`ObjectStore`] over another. The simulator stacks them
//! as `Faults<YieldEachCall<Recorder<Volatile<MemStore>>>>` for the staging directory and
//! `Faults<YieldEachCall<Recorder<MemStore>>>` for the replica, and keeps a plain `MemStore`
//! handle to the same state: the oracle's restores read through that handle, so
//! no fault or yield ever touches a check.
//!
//! - [`Faults`] fails calls on a schedule. Faults live here so the raw store
//!   stays clean. It also says how wide a batch may be, so callers take
//!   their batched paths, and runs each batch one item at a time, last item
//!   first, results in order: out of order as a concurrent store would be,
//!   and the same order every run.
//! - [`YieldEachCall`] returns `Pending` once before every call, which is what makes
//!   each store call a crash point for the executor.
//! - [`Recorder`] records what the store really did, below the faults, so a commit
//!   that a fault turned into an error still shows as published. The code under
//!   test never reads it.
//! - [`Volatile`] tracks which staged files are not yet durable, for a power loss.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use lsr_core::WriteMode;
use lsr_core::{
    Again, BatchKind, BoxFuture, CorruptCause, Entropy, FileInfo, FileKey, Flight, Level, ListAt,
    Listing, ObjectStore, Opened, PageLimit, ReadLen, Reread, Rest, SeededEntropy, Seen,
    StoreError, StoreOp, Upload,
};
use lsr_core::{Clock, CreatedAt, FakeClock};
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
    /// The commit publishes the object one byte short, its last byte lost,
    /// and acknowledges it with the size it holds: a short write the store
    /// took as whole. Without [`Faults::corrupting_through`] the object stays
    /// whole and only the reported size is short. Counted over commits,
    /// `StoreOp::Upload` only.
    SizeMismatch,
    /// The commit publishes the object and then reports that the store holds
    /// other bytes than were sent (an ETag that does not match). The object
    /// stays as it is. Counted over commits, `StoreOp::Upload` only.
    ChecksumMismatch,
    /// As [`FaultShape::ChecksumMismatch`], and the published object has one
    /// byte changed, so its size is right and its content is wrong: a
    /// single-request upload a store acknowledged and damaged. Needs
    /// [`Faults::corrupting_through`]. Counted over commits, `StoreOp::Upload`
    /// only.
    CorruptPublish,
    /// The read returns its bytes with the lowest bit of the byte at object
    /// offset `at` flipped when the range covers it; the stored bytes are
    /// untouched. `StoreOp::Read` only.
    FlipByteOnRead {
        /// The object offset of the byte.
        at: u64,
    },
    /// Every `remove_scratch` (the boot's sweep of leftover uploads) fails with the error,
    /// until healed. Draws nothing from the seed and counts as no call, so no schedule that
    /// does not use it moves; the scheduled operation is ignored.
    Scratch(StoreError),
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
    Checksum,
    Corrupt,
    Flip(u64),
}

#[derive(Debug)]
struct Entry {
    op: StoreOp,
    nth: u32,
    shape: FaultShape,
    fired: bool,
    /// Calls it failed or changed: once for a one-shot fault, any number for
    /// a rate.
    hits: u32,
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
    damaged: Vec<FileKey>,
    rejected: Vec<FileKey>,
    // Objects a size mismatch left one byte short, while they are.
    short: Vec<FileKey>,
    // Short objects whose delete has begun and not returned: a crash that drops the call
    // leaves them in the store, as a failed delete would.
    deleting: Vec<FileKey>,
    // Objects a flipped byte reached on a read, once per such read.
    flipped: Vec<FileKey>,
    // Reads, listings and created-at times one batch may hold.
    widths: [NonZeroUsize; 2],
    // Batch calls that held more than one item.
    batched: u32,
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
                FaultShape::ChecksumMismatch => {
                    (commit_seen == Some(entry.nth)).then_some(Verdict::Checksum)
                }
                FaultShape::CorruptPublish => {
                    (commit_seen == Some(entry.nth)).then_some(Verdict::Corrupt)
                }
                FaultShape::FlipByteOnRead { at } => {
                    (call == Call::Read && seen == entry.nth).then_some(Verdict::Flip(*at))
                }
                FaultShape::Scratch(_) => None,
                FaultShape::Rate { per_mille, err } => (seen >= entry.nth
                    && rng.below(1000) < u64::from(*per_mille))
                .then(|| Verdict::Fail(err.clone())),
            };
            if let Some(verdict) = hit {
                entry.fired = !matches!(entry.shape, FaultShape::Rate { .. });
                entry.hits = entry.hits.saturating_add(1);
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
            hits: 0,
        });
    }

    /// Schedules `shape` `ahead` calls from now (0: the next call).
    pub fn push_in(&self, op: StoreOp, ahead: u32, shape: FaultShape) {
        let mut state = lock(&self.state);
        let base = match shape {
            FaultShape::ErrorAfterPublish(_)
            | FaultShape::SizeMismatch
            | FaultShape::ChecksumMismatch
            | FaultShape::CorruptPublish => state.commits,
            FaultShape::Error(_)
            | FaultShape::FlipByteOnRead { .. }
            | FaultShape::Rate { .. }
            | FaultShape::Scratch(_) => *state.count(op),
        };
        state.schedule.push(Entry {
            op,
            nth: base.saturating_add(ahead),
            shape,
            fired: false,
            hits: 0,
        });
    }

    /// How many calls of `op` have been made.
    #[must_use]
    pub fn calls(&self, op: StoreOp) -> u32 {
        *lock(&self.state).count(op)
    }

    /// The objects a [`FaultShape::CorruptPublish`] changed, and those a
    /// [`FaultShape::SizeMismatch`] left short that the engine keeps: after a
    /// level-0 replace, which it must not delete, or after any other write whose
    /// delete failed or was cut off by a crash before it returned. In order: where the damage really landed, whatever the
    /// scenario meant to hit.
    #[must_use]
    pub fn damaged(&self) -> Vec<FileKey> {
        let state = lock(&self.state);
        state
            .damaged
            .iter()
            .chain(&state.deleting)
            .copied()
            .collect()
    }

    /// Every scheduled fault that failed or changed at least one call, with
    /// how many it did.
    #[must_use]
    pub fn hits(&self) -> Vec<(StoreOp, FaultShape, u32)> {
        lock(&self.state)
            .schedule
            .iter()
            .filter(|e| e.hits > 0)
            .map(|e| (e.op, e.shape.clone(), e.hits))
            .collect()
    }

    /// The objects a commit published and then reported as failed or wrong,
    /// in order: the engine may distrust and delete each.
    #[must_use]
    pub fn rejected(&self) -> Vec<FileKey> {
        lock(&self.state).rejected.clone()
    }

    /// The objects a [`FaultShape::FlipByteOnRead`] reached, once per read it
    /// changed: a read that returned other bytes than the store holds.
    #[must_use]
    pub fn flipped(&self) -> Vec<FileKey> {
        lock(&self.state).flipped.clone()
    }

    /// Lets a batch of `kind` hold `width` items. Callers then take their
    /// batched paths; the batch still runs one item at a time, last item
    /// first (see [`Faults`]), so the run stays deterministic.
    pub fn set_batch_width(&self, kind: BatchKind, width: NonZeroUsize) {
        if let Some(slot) = lock(&self.state).widths.get_mut(width_slot(kind)) {
            *slot = width;
        }
    }

    /// Batch calls that held more than one item.
    #[must_use]
    pub fn batched(&self) -> u32 {
        lock(&self.state).batched
    }

    /// Makes every call of `op` from the next one on fail with `err`, until
    /// [`Self::release`] is given the returned id: a fault that lasts.
    pub fn hold(&self, op: StoreOp, err: StoreError) -> HeldFault {
        self.held(
            op,
            0,
            FaultShape::Rate {
                per_mille: 1000,
                err,
            },
        )
    }

    /// Schedules `shape` `ahead` calls of `op` from now, as [`Self::push_in`] does, and returns
    /// an id [`Self::release`] takes it back with if it has not fired by then.
    pub fn held(&self, op: StoreOp, ahead: u32, shape: FaultShape) -> HeldFault {
        self.push_in(op, ahead, shape);
        HeldFault(lock(&self.state).schedule.len().saturating_sub(1))
    }

    /// Ends a fault [`Self::hold`] began.
    pub fn release(&self, held: HeldFault) {
        if let Some(entry) = lock(&self.state).schedule.get_mut(held.0) {
            entry.fired = true;
        }
    }

    /// Stops every fault that fails at a rate, and every failing sweep.
    pub fn heal(&self) {
        for entry in &mut lock(&self.state).schedule {
            if matches!(
                entry.shape,
                FaultShape::Rate { .. } | FaultShape::Scratch(_)
            ) {
                entry.fired = true;
            }
        }
    }

    /// One-shot faults that have not fired yet.
    #[must_use]
    pub fn pending(&self) -> usize {
        lock(&self.state)
            .schedule
            .iter()
            .filter(|e| {
                !e.fired && !matches!(e.shape, FaultShape::Rate { .. } | FaultShape::Scratch(_))
            })
            .count()
    }
}

/// A fault [`FaultHandle::hold`] began, for [`FaultHandle::release`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldFault(usize);

/// A store that fails on a seeded schedule.
#[derive(Clone, Debug)]
pub struct Faults<S> {
    inner: S,
    state: Arc<Mutex<FaultState>>,
    volatile: Option<VolatileHandle>,
    raw: Option<crate::remote::Bucket>,
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
                hits: 0,
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
            damaged: Vec::new(),
            short: Vec::new(),
            deleting: Vec::new(),
            flipped: Vec::new(),
            rejected: Vec::new(),
            widths: [NonZeroUsize::MIN; 2],
            batched: 0,
        };
        Self {
            inner,
            state: Arc::new(Mutex::new(state)),
            volatile: None,
            raw: None,
        }
    }

    /// Lets [`FaultShape::CorruptPublish`] change the bytes of an object it
    /// published, through a handle to the store under the wrappers.
    #[must_use]
    pub fn corrupting_through(mut self, raw: crate::remote::Bucket) -> Self {
        self.raw = Some(raw);
        self
    }

    /// Marks a key volatile in `volatile` whenever a commit publishes and then
    /// fails: the staging store's "rename done, sync failed".
    #[must_use]
    pub fn volatile_on_late_error(mut self, volatile: VolatileHandle) -> Self {
        self.volatile = Some(volatile);
        self
    }

    /// The handle for scheduling more faults.
    #[must_use]
    pub fn handle(&self) -> FaultHandle {
        FaultHandle {
            state: Arc::clone(&self.state),
        }
    }

    fn note_batch(&self, items: usize) {
        if items > 1 {
            let mut state = lock(&self.state);
            state.batched = state.batched.saturating_add(1);
        }
    }

    fn verdict(&self, call: Call) -> Verdict {
        lock(&self.state).decide(call)
    }

    /// The error a scheduled [`FaultShape::Scratch`] gives the sweep, if one stands.
    fn scratch_error(&self) -> Option<StoreError> {
        let mut state = lock(&self.state);
        let entry = state
            .schedule
            .iter_mut()
            .find(|e| !e.fired && matches!(e.shape, FaultShape::Scratch(_)))?;
        entry.hits = entry.hits.saturating_add(1);
        match &entry.shape {
            FaultShape::Scratch(err) => Some(err.clone()),
            FaultShape::Error(_)
            | FaultShape::ErrorAfterPublish(_)
            | FaultShape::SizeMismatch
            | FaultShape::ChecksumMismatch
            | FaultShape::CorruptPublish
            | FaultShape::FlipByteOnRead { .. }
            | FaultShape::Rate { .. } => None,
        }
    }
}

const fn width_slot(kind: BatchKind) -> usize {
    match kind {
        BatchKind::Lists => 0,
        BatchKind::ResolvedCreatedAt => 1,
    }
}

/// Flips the byte at object offset `at` when `bytes`, read from `offset`,
/// holds it; whether it did.
/// An object held open behind [`Faults`]: each read is judged as a read by range is.
#[derive(Debug)]
struct FaultsOpened {
    inner: Box<dyn Opened>,
    state: Arc<Mutex<FaultState>>,
    key: FileKey,
}

impl Opened for FaultsOpened {
    fn read(&mut self, offset: u64, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            let verdict = lock(&self.state).decide(Call::Read);
            if let Verdict::Fail(err) = &verdict {
                return Err(err.clone());
            }
            let mut bytes = self.inner.read(offset, len).await?;
            if let Verdict::Flip(at) = verdict
                && flip(&mut bytes, offset, at)
            {
                lock(&self.state).flipped.push(self.key);
            }
            Ok(bytes)
        })
    }
}

fn flip(bytes: &mut [u8], offset: u64, at: u64) -> bool {
    let index = at.checked_sub(offset).and_then(|i| usize::try_from(i).ok());
    match index.and_then(|i| bytes.get_mut(i)) {
        Some(byte) => {
            *byte ^= 1;
            true
        }
        None => false,
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
        let listing = self.inner.list(level, after, limit).await?;
        Ok(listing)
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let verdict = self.verdict(Call::Read);
        if let Verdict::Fail(err) = &verdict {
            return Err(err.clone());
        }
        let mut bytes = self.inner.read(key, offset, len).await?;
        if let Verdict::Flip(at) = verdict
            && flip(&mut bytes, offset, at)
        {
            lock(&self.state).flipped.push(*key);
        }
        Ok(bytes)
    }

    /// The open itself is no call of the schedule's: each read through what it gives is one,
    /// so a fault lands on the same read whether the store holds the file open or not.
    async fn open(&self, key: &FileKey) -> Result<Option<Box<dyn Opened>>, StoreError> {
        let held = self.inner.open(key).await?.map(|inner| {
            let held: Box<dyn Opened> = Box::new(FaultsOpened {
                inner,
                state: Arc::clone(&self.state),
                key: *key,
            });
            held
        });
        Ok(held)
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::Read) {
            return Err(err);
        }
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        let verdict = self.verdict(Call::Read);
        if let Verdict::Fail(err) = &verdict {
            return Err(err.clone());
        }
        let (mut bytes, created_at) = self.inner.read_with_created_at(file, len).await?;
        if let Verdict::Flip(at) = verdict
            && flip(&mut bytes, 0, at)
        {
            lock(&self.state).flipped.push(file.key);
        }
        Ok((bytes, created_at))
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::Begin) {
            return Err(err);
        }
        let inner = self
            .inner
            .begin_sized_upload(key, created_at, mode, len)
            .await?;
        Ok(FaultsUpload {
            inner,
            key: *key,
            mode,
            wrote: 0,
            state: Arc::clone(&self.state),
            volatile: self.volatile.clone(),
            raw: self.raw.clone(),
        })
    }

    /// A failed delete of an object a size mismatch left short makes it
    /// damage the store keeps: the engine tried to remove it and could not,
    /// whether this layer failed the delete or the store under it did.
    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::Delete) {
            let mut state = lock(&self.state);
            let kept: Vec<FileKey> = keys
                .iter()
                .filter(|key| state.short.contains(key))
                .copied()
                .collect();
            state.damaged.extend(kept);
            return Err(err);
        }
        {
            let mut state = lock(&self.state);
            let begun: Vec<FileKey> = keys
                .iter()
                .filter(|key| state.short.contains(key))
                .copied()
                .collect();
            state.deleting.extend(begun);
        }
        let deleted = self.inner.delete(keys).await;
        lock(&self.state).deleting.retain(|key| !keys.contains(key));
        if let Err(err) = deleted {
            // The store under the fault layer failed it (an S3 store's request met a provider
            // fault): a short object it was asked to remove is damage it may keep.
            let mut state = lock(&self.state);
            let kept: Vec<FileKey> = keys
                .iter()
                .filter(|key| state.short.contains(key))
                .copied()
                .collect();
            state.damaged.extend(kept);
            return Err(err);
        }
        let mut state = lock(&self.state);
        state.short.retain(|key| !keys.contains(key));
        Ok(())
    }

    /// Forwarded without a verdict or a yield: the simulator's stores leave no scratch, and a
    /// draw here would move every frozen schedule. Only a scheduled [`FaultShape::Scratch`]
    /// fails it.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        match self.scratch_error() {
            Some(err) => Err(err),
            None => self.inner.remove_scratch().await,
        }
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        lock(&self.state)
            .widths
            .get(width_slot(kind))
            .copied()
            .unwrap_or(NonZeroUsize::MIN)
    }

    // The batch methods run last item first and return results in order: a
    // real store sends a batch side by side and it finishes in some other
    // order; this one is fixed, so a run replays.

    async fn created_at_many(&self, files: &[FileInfo]) -> Vec<Result<CreatedAt, StoreError>> {
        self.note_batch(files.len());
        let mut out = Vec::with_capacity(files.len());
        for file in files.iter().rev() {
            out.push(self.created_at(file).await);
        }
        out.reverse();
        out
    }

    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        self.note_batch(pages.len());
        let mut out = Vec::with_capacity(pages.len());
        for at in pages.iter().rev() {
            out.push(self.list(at.level, at.after, limit).await);
        }
        out.reverse();
        out
    }
}

/// An upload through [`Faults`].
#[derive(Debug)]
pub struct FaultsUpload<U> {
    inner: U,
    key: FileKey,
    mode: WriteMode,
    wrote: u64,
    state: Arc<Mutex<FaultState>>,
    volatile: Option<VolatileHandle>,
    raw: Option<crate::remote::Bucket>,
}

fn checksum_mismatch(key: FileKey) -> StoreError {
    StoreError::Corrupt {
        key: Some(key),
        cause: CorruptCause::ChecksumMismatch { part: None },
    }
}

/// Changes one byte in the middle of the published object, keeping its size
/// and its created-at time.
async fn damage(raw: &crate::remote::Bucket, info: &FileInfo) -> Result<(), StoreError> {
    rewrite(raw, info, |bytes| {
        let middle = bytes.len() / 2;
        if let Some(byte) = bytes.get_mut(middle) {
            *byte ^= 0xff;
        }
    })
    .await
}

/// Drops the published object's last byte, keeping its created-at time.
async fn truncate(raw: &crate::remote::Bucket, info: &FileInfo) -> Result<(), StoreError> {
    rewrite(raw, info, |bytes| {
        bytes.pop();
    })
    .await
}

/// Puts the published object back through `edit`, keeping its created-at
/// time.
async fn rewrite(
    raw: &crate::remote::Bucket,
    info: &FileInfo,
    edit: impl FnOnce(&mut Vec<u8>),
) -> Result<(), StoreError> {
    let mut bytes = read_all(raw, &info.key).await?;
    edit(&mut bytes);
    let at = info
        .created_at()
        .map_or(Timestamp::from_unix_millis(0), |created_at| created_at.at());
    let mut up = raw.begin_upload(&info.key, at, WriteMode::Replace).await?;
    up.write(&bytes).await?;
    up.commit().await?;
    Ok(())
}

impl<U: Upload> Upload for FaultsUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.write_inner(chunk, None).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        self.write_inner(chunk, Some(again)).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        self.commit_inner().await
    }

    /// Not a call to fault: it sends nothing of its own, and a part's failure comes through
    /// it as the store reports it.
    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        self.inner.flight(rest).await
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

/// Writes `chunk` to `inner`, with the source the engine gave when it gave one.
async fn write_of<U: Upload>(
    inner: &mut U,
    chunk: &[u8],
    again: Option<&dyn Reread>,
) -> Result<(), StoreError> {
    match again {
        Some(again) => inner.write_from(chunk, again).await,
        None => inner.write(chunk).await,
    }
}

impl<U: Upload> FaultsUpload<U> {
    async fn write_inner(
        &mut self,
        chunk: &[u8],
        again: Option<&dyn Reread>,
    ) -> Result<(), StoreError> {
        if let Verdict::Fail(err) = lock(&self.state).decide(Call::Write) {
            return Err(err);
        }
        write_of(&mut self.inner, chunk, again).await?;
        self.wrote = self
            .wrote
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    async fn commit_inner(self) -> Result<FileInfo, StoreError> {
        let verdict = lock(&self.state).decide(Call::Commit);
        if matches!(
            verdict,
            Verdict::AfterPublish(_) | Verdict::SizeMismatch | Verdict::Checksum | Verdict::Corrupt
        ) {
            lock(&self.state).rejected.push(self.key);
        }
        match verdict {
            Verdict::Fail(err) => {
                drop(self.inner);
                Err(err)
            }
            Verdict::AfterPublish(err) => {
                self.inner.commit().await?;
                if let Some(volatile) = &self.volatile {
                    volatile.mark_volatile(self.key);
                }
                Err(err)
            }
            Verdict::SizeMismatch => {
                let mut info = self.inner.commit().await?;
                // Not damage the store hid: it said the size, so the engine is
                // told. A level-0 replace must not delete it (the key may have
                // held an acknowledged object, D78), so the short object is
                // damage a restore may stop on until the uploader writes the key
                // again. Any other write must remove it (a create-only write
                // deletes it in its commit; a compaction output or a snapshot is
                // removed under the engine's own reason, its inputs or the chain
                // below still covering it), so a restore that meets it stays
                // judged unless that delete failed or a crash cut it off.
                if let Some(raw) = &self.raw {
                    truncate(raw, &info).await?;
                    let mut state = lock(&self.state);
                    let kept = self.mode == WriteMode::Replace && self.key.level() == Level::ZERO;
                    if kept {
                        state.damaged.push(info.key);
                    } else {
                        state.short.push(info.key);
                    }
                }
                info.size = info.size.saturating_sub(1);
                Ok(info)
            }
            Verdict::Checksum => {
                self.inner.commit().await?;
                Err(checksum_mismatch(self.key))
            }
            Verdict::Corrupt => {
                let info = self.inner.commit().await?;
                if let Some(raw) = &self.raw {
                    damage(raw, &info).await?;
                    lock(&self.state).damaged.push(info.key);
                }
                Err(checksum_mismatch(self.key))
            }
            Verdict::Pass | Verdict::Flip(_) => self.inner.commit().await,
        }
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
pub struct YieldEachCall<S> {
    inner: S,
}

impl<S> YieldEachCall<S> {
    /// Wraps `inner`.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }
}

/// An object held open behind [`YieldEachCall`].
#[derive(Debug)]
struct YieldOpened(Box<dyn Opened>);

impl Opened for YieldOpened {
    fn read(&mut self, offset: u64, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            YieldNow(false).await;
            self.0.read(offset, len).await
        })
    }
}

impl<S: ObjectStore> ObjectStore for YieldEachCall<S> {
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

    /// No yield for the open: each read through what it gives yields, as a read by range does.
    async fn open(&self, key: &FileKey) -> Result<Option<Box<dyn Opened>>, StoreError> {
        let held = self.inner.open(key).await?.map(|inner| {
            let held: Box<dyn Opened> = Box::new(YieldOpened(inner));
            held
        });
        Ok(held)
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        YieldNow(false).await;
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        YieldNow(false).await;
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        YieldNow(false).await;
        let inner = self
            .inner
            .begin_sized_upload(key, created_at, mode, len)
            .await?;
        Ok(YieldUpload { inner })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.delete(keys).await
    }

    /// Forwarded without a yield, so the await points a schedule counts are unchanged.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }
}

/// An upload through [`YieldEachCall`].
#[derive(Debug)]
pub struct YieldUpload<U> {
    inner: U,
}

impl<U: Upload> Upload for YieldUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.write(chunk).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.write_from(chunk, again).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        YieldNow(false).await;
        self.inner.commit().await
    }

    /// No crash point of its own: the question sends nothing.
    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        self.inner.flight(rest).await
    }

    async fn abort(self) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.abort().await
    }
}

// ------------------------------------------------------------------- Recorder

/// One thing the store really did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecorderEvent {
    /// A commit.
    Put {
        /// The object.
        key: FileKey,
        /// Whether the code under test asked for a create-only write or a replace.
        mode: WriteMode,
        /// Its header, `None` when the first bytes written are not a valid one.
        header: Option<Header>,
        /// The WAL range its pages came from, from the header.
        origin: Option<WalOrigin>,
        /// The created-at time the code under test gave the upload.
        created_at: Timestamp,
        /// The bytes written.
        size: u64,
        /// [`content_digest`] of the bytes written: what tells a file the
        /// engine staged from one put in its place with the same name.
        digest: u64,
        /// The simulator's clock when the upload began: a snapshot is stamped then and sent
        /// over many ticks, so its created-at time may be older than the put.
        begun: Timestamp,
        /// The simulator's clock when the store took it, read below the code
        /// under test: what the oracle judges created-at times against.
        at: Timestamp,
        /// The latest time the code under test had been told by then: an
        /// engine that clamps its clock against an earlier reading never
        /// stamps past it.
        high: Timestamp,
    },
    /// A delete the store accepted.
    Delete {
        /// The keys it was asked to remove.
        keys: Vec<FileKey>,
        /// The simulator's clock when the store took it.
        at: Timestamp,
        /// The latest time the code under test had been told by then.
        high: Timestamp,
    },
}

/// Call counts a [`Recorder`] keeps for the resource properties.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecorderCounts {
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
    /// Second reads (a store reading back, through the source the engine gave it, bytes it
    /// was handed and let go of) that gave the bytes the engine first handed over.
    pub rereads_given: u64,
    /// Second reads that gave other bytes than the engine first handed over, or another
    /// count than was asked for: bytes no first read verified, on their way to the store.
    pub rereads_differing: u64,
    /// Second reads the source refused.
    pub rereads_refused: u64,
}

/// The latest time the code under test has been told, shared between the
/// run loop, which notes each tick's time, and the recorders.
#[derive(Clone, Debug, Default)]
pub struct HighWater(Arc<Mutex<Option<Timestamp>>>);

impl HighWater {
    /// Notes that the code under test was told `at`.
    pub fn note(&self, at: Timestamp) {
        let mut high = lock(&self.0);
        *high = Some(high.map_or(at, |high| high.max(at)));
    }

    /// The latest time noted, at least `now`.
    #[must_use]
    pub fn at_least(&self, now: Timestamp) -> Timestamp {
        lock(&self.0).map_or(now, |high| high.max(now))
    }
}

/// What the bucket under a store holds at a key now: the [`content_digest`] of the object,
/// `None` when there is none. Read beneath the store, never through it.
pub type Landed = Arc<dyn Fn(&FileKey) -> Option<u64> + Send + Sync>;

#[derive(Default)]
struct TapState {
    events: Vec<RecorderEvent>,
    counts: RecorderCounts,
    clock: Option<(Arc<FakeClock>, HighWater)>,
    landed: Option<Landed>,
    // Objects a commit reported failed that the bucket took all the same.
    rejected: Vec<FileKey>,
}

impl std::fmt::Debug for TapState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TapState")
            .field("events", &self.events.len())
            .field("counts", &self.counts)
            .field("landed", &self.landed.is_some())
            .finish_non_exhaustive()
    }
}

impl TapState {
    /// The clock now and the latest the code under test has been told, both
    /// at 0 without a clock.
    fn times(&self) -> (Timestamp, Timestamp) {
        self.clock.as_ref().map_or(
            (
                Timestamp::from_unix_millis(0),
                Timestamp::from_unix_millis(0),
            ),
            |(clock, high)| {
                let now = clock.now();
                (now, high.at_least(now))
            },
        )
    }
}

/// The reading side of a [`Recorder`].
#[derive(Clone, Debug, Default)]
pub struct RecorderLog {
    state: Arc<Mutex<TapState>>,
}

impl RecorderLog {
    /// With a bucket to ask ([`Recorder::landed_through`]), the objects whose commit the store
    /// reported failed, in order: published or not, the engine may distrust and delete each.
    #[must_use]
    pub fn rejected(&self) -> Vec<FileKey> {
        lock(&self.state).rejected.clone()
    }

    /// Every event so far, oldest first.
    #[must_use]
    pub fn events(&self) -> Vec<RecorderEvent> {
        lock(&self.state).events.clone()
    }

    /// How many events there are: the created-at time the model records per commit.
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
    pub fn counts(&self) -> RecorderCounts {
        lock(&self.state).counts
    }

    fn with(&self, f: impl FnOnce(&mut TapState)) {
        f(&mut lock(&self.state));
    }
}

/// A store that records what it did.
#[derive(Clone, Debug)]
pub struct Recorder<S> {
    inner: S,
    log: RecorderLog,
}

impl<S> Recorder<S> {
    /// Wraps `inner` with an empty log.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            log: RecorderLog::default(),
        }
    }

    /// Reads `clock` and `high` at every commit, for the times a
    /// [`RecorderEvent::Put`] carries.
    #[must_use]
    pub fn with_clock(self, clock: Arc<FakeClock>, high: HighWater) -> Self {
        self.log.with(|s| s.clock = Some((clock, high)));
        self
    }

    /// Asks `landed` about a commit or a delete that failed: a store whose failure can come
    /// after the bucket took the request (an S3 store losing the reply) may have published the
    /// object or removed the keys all the same, and what landed is recorded as if it had
    /// succeeded, as a fault above the recorder that fails a commit after publishing is.
    #[must_use]
    pub fn landed_through(self, landed: Landed) -> Self {
        self.log.with(|s| s.landed = Some(landed));
        self
    }

    /// The log; clones share it.
    #[must_use]
    pub fn log(&self) -> RecorderLog {
        self.log.clone()
    }
}

/// An object held open behind [`Recorder`]: each read counts as a read by range does.
#[derive(Debug)]
struct RecorderOpened {
    inner: Box<dyn Opened>,
    log: RecorderLog,
}

impl Opened for RecorderOpened {
    fn read(&mut self, offset: u64, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        self.log.with(|s| bump(&mut s.counts.reads));
        self.inner.read(offset, len)
    }
}

impl<S: ObjectStore + 'static> ObjectStore for Recorder<S> {
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

    async fn open(&self, key: &FileKey) -> Result<Option<Box<dyn Opened>>, StoreError> {
        let held = self.inner.open(key).await?.map(|inner| {
            let held: Box<dyn Opened> = Box::new(RecorderOpened {
                inner,
                log: self.log.clone(),
            });
            held
        });
        Ok(held)
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.log.with(|s| bump(&mut s.counts.reads));
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        let mut begun = Timestamp::from_unix_millis(0);
        self.log.with(|s| {
            bump(&mut s.counts.begins);
            begun = s.times().0;
        });

        let inner = self
            .inner
            .begin_sized_upload(key, created_at, mode, len)
            .await?;
        Ok(TapUpload {
            inner,
            key: *key,
            mode,
            created_at,
            begun,
            head: Vec::new(),
            wrote: 0,
            digest: DIGEST_SEED,
            first: Arc::default(),
            log: self.log.clone(),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.log.with(|s| bump(&mut s.counts.deletes));

        let outcome = self.inner.delete(keys).await;
        self.log.with(|s| {
            // A failed delete removed the keys the bucket no longer holds, when it says so.
            let gone: Vec<FileKey> = match (&outcome, &s.landed) {
                (Ok(()), _) => keys.to_vec(),
                (Err(_), Some(probe)) => keys
                    .iter()
                    .filter(|key| probe(key).is_none())
                    .copied()
                    .collect(),
                (Err(_), None) => Vec::new(),
            };
            if !gone.is_empty() {
                let (at, high) = s.times();
                s.events.push(RecorderEvent::Delete {
                    keys: gone,
                    at,
                    high,
                });
            }
        });
        outcome
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }
}

/// An upload through [`Recorder`].
#[derive(Debug)]
pub struct TapUpload<S: ObjectStore> {
    inner: S::Upload,
    key: FileKey,
    mode: WriteMode,
    created_at: Timestamp,
    /// The simulator's clock when the upload began.
    begun: Timestamp,
    head: Vec<u8>,
    wrote: u64,
    digest: u64,
    /// The bytes handed over with a source to read them back from: the first read, which
    /// every later read is compared with. Shared with the readers the store was given, which
    /// a part in flight may read after the write that handed its bytes over.
    first: Arc<Mutex<Vec<u8>>>,
    log: RecorderLog,
}

/// The source an engine gave with its bytes, watched: each later read through a reader it
/// opened is counted as what it was against the first read the recorder kept
/// ([`RecorderCounts::rereads_given`] and the two beside it). It changes no answer.
struct Watched<'u> {
    inner: &'u dyn Reread,
    first: &'u Arc<Mutex<Vec<u8>>>,
    log: &'u RecorderLog,
}

impl Reread for Watched<'_> {
    fn open(&self) -> BoxFuture<'_, Result<Box<dyn Again>, StoreError>> {
        Box::pin(async move {
            let reader: Box<dyn Again> = Box::new(WatchedReader {
                inner: self.inner.open().await?,
                first: Arc::clone(self.first),
                log: self.log.clone(),
            });
            Ok(reader)
        })
    }
}

/// A reader [`Watched`] opened.
struct WatchedReader {
    inner: Box<dyn Again>,
    first: Arc<Mutex<Vec<u8>>>,
    log: RecorderLog,
}

impl Again for WatchedReader {
    fn read<'a>(
        &'a mut self,
        offset: u64,
        was: &'a Seen,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            let out = self.inner.read(offset, was).await;
            let same = out.as_ref().ok().map(|bytes| {
                let first = lock(&self.first);
                let held = usize::try_from(offset)
                    .ok()
                    .and_then(|start| first.get(start..))
                    .and_then(|rest| rest.get(..bytes.len()));
                let whole = u64::try_from(bytes.len()).is_ok_and(|len| len == was.len());
                whole && held == Some(bytes.as_slice())
            });
            self.log.with(|s| match same {
                Some(true) => bump(&mut s.counts.rereads_given),
                Some(false) => bump(&mut s.counts.rereads_differing),
                None => bump(&mut s.counts.rereads_refused),
            });
            out
        })
    }
}

const DIGEST_SEED: u64 = 0xcbf2_9ce4_8422_2325;

fn digest_more(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// FNV-1a over an object's bytes, as [`RecorderEvent::Put`] records it.
#[must_use]
pub fn content_digest(bytes: &[u8]) -> u64 {
    digest_more(DIGEST_SEED, bytes)
}

/// The WAL generation and end offset an LTX file's header names, when it
/// names a range with frames: what the oracle maps a file to a commit by.
pub(crate) fn wal_end(bytes: &[u8]) -> Option<crate::model::WalEnd> {
    let origin = parse_header(bytes.get(..HEADER_LEN)?)?.fields().wal?;
    if origin.size() == 0 {
        return None;
    }
    let (salt1, salt2) = origin.salts();
    Some((
        lsr_wal::Salts { salt1, salt2 },
        origin.offset().saturating_add(origin.size()),
    ))
}

fn parse_header(head: &[u8]) -> Option<Header> {
    let bytes: &[u8; HEADER_LEN] = head.try_into().ok()?;
    Header::decode(bytes).ok()
}

impl<S: ObjectStore> Upload for TapUpload<S> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.write_inner(chunk, None).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        self.write_inner(chunk, Some(again)).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        self.commit_inner().await
    }

    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        self.inner.flight(rest).await
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

impl<S: ObjectStore> TapUpload<S> {
    async fn write_inner(
        &mut self,
        chunk: &[u8],
        again: Option<&dyn Reread>,
    ) -> Result<(), StoreError> {
        match again {
            Some(again) => {
                // The store may read back inside this write, so the first read is kept first.
                lock(&self.first).extend_from_slice(chunk);
                let watched = Watched {
                    inner: again,
                    first: &self.first,
                    log: &self.log,
                };
                self.inner.write_from(chunk, &watched).await?;
            }
            None => self.inner.write(chunk).await?,
        }
        self.digest = digest_more(self.digest, chunk);
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

    async fn commit_inner(self) -> Result<FileInfo, StoreError> {
        let Self {
            inner,
            key,
            mode,
            created_at,
            begun,
            head,
            wrote,
            digest,
            first,
            log,
        } = self;
        let header = parse_header(&head);
        drop(first);
        let outcome = inner.commit().await;
        log.with(|s| {
            bump(&mut s.counts.commits);
            // An object the store accepted is recorded. The recorder sits below `Faults`, so a
            // commit it sees fail published only if the store failed after the bucket took it,
            // which the bucket itself says (`Recorder::landed_through`).
            // With a bucket to ask, a failed commit may have published and been removed again
            // (an S3 store deletes an object whose ETag came back wrong before it reports the
            // error): the engine may delete the key either way, so it is listed as rejected.
            let landed = match (&outcome, &s.landed) {
                (Ok(_), _) => true,
                (Err(_), Some(probe)) => {
                    s.rejected.push(key);
                    probe(&key) == Some(digest)
                }
                (Err(_), None) => false,
            };
            if landed {
                let (at, high) = s.times();
                s.events.push(RecorderEvent::Put {
                    key,
                    mode,
                    header,
                    origin: header.and_then(|h| h.fields().wal),
                    created_at,
                    size: wrote,
                    digest,
                    begun,
                    at,
                    high,
                });
            }
        });
        outcome
    }
}

// ----------------------------------------------------------------- Stage

#[derive(Debug, Default)]
struct StageState {
    volatile: BTreeSet<FileKey>,
    resolved: BTreeMap<FileKey, Timestamp>,
    // Unlinks since the directory's last sync, with what each entry durably held then.
    unlinked: BTreeMap<FileKey, Durable>,
    // Whether a power loss may put an unsynced entry back: the block law's directory rule.
    entries_revert: bool,
}

/// What a directory entry durably held: a file's bytes and created-at time, or no file.
type Durable = Option<(Vec<u8>, Timestamp)>;

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
    /// The file was unlinked and the directory never synced after: the unlink is undone and
    /// the file is back as it durably was.
    Restored,
}

/// The side of a [`Volatile`] the simulator keeps: which keys are volatile, and the
/// power loss itself.
#[derive(Clone, Debug, Default)]
pub struct VolatileHandle {
    state: Arc<Mutex<StageState>>,
}

/// How much a power loss reads of a file at once.
const LOSS_READ: u32 = 1 << 20;

impl VolatileHandle {
    /// A key that is visible but not yet durable.
    pub fn mark_volatile(&self, key: FileKey) {
        lock(&self.state).volatile.insert(key);
    }

    /// A directory sync: every earlier rename and unlink is now durable.
    pub fn fsync_all(&self) {
        let mut state = lock(&self.state);
        state.volatile.clear();
        state.unlinked.clear();
    }

    /// From now on a power loss undoes each unlink the directory has not synced since, or
    /// keeps it, on a coin: the directory-entry rule of [`crate::crash_law::CrashLaw::Blocks`],
    /// as the real-SQLite fault VFS applies it. Without this, unlinks stay done, as under
    /// [`crate::crash_law::CrashLaw::AllOrNothing`].
    pub fn revert_unsynced_entries(&self) {
        lock(&self.state).entries_revert = true;
    }

    /// Unlinks since the directory's last sync.
    #[must_use]
    pub fn unsynced_unlinks(&self) -> Vec<FileKey> {
        lock(&self.state).unlinked.keys().copied().collect()
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
                let created_at = lock(&self.state).resolved.get(&key).copied();
                let mut up = raw
                    .begin_upload(
                        &key,
                        created_at.unwrap_or(Timestamp::from_unix_millis(0)),
                        WriteMode::Replace,
                    )
                    .await?;
                up.write(prefix).await?;
                up.commit().await?;
                lost.push((key, Lost::Torn { kept }));
            } else {
                raw.delete(&[key]).await?;
                lost.push((key, Lost::Dropped));
            }
        }
        lost.extend(self.undo_unlinks(raw, entropy).await?);
        self.fsync_all();
        Ok(lost)
    }

    /// Under the block law, each unsynced unlink is undone or kept on a coin; drawn after
    /// every volatile key, so a run without unlinks draws as before.
    async fn undo_unlinks<S: ObjectStore>(
        &self,
        raw: &S,
        entropy: &dyn Entropy,
    ) -> Result<Vec<(FileKey, Lost)>, StoreError> {
        let (revert, unlinked) = {
            let mut state = lock(&self.state);
            (state.entries_revert, std::mem::take(&mut state.unlinked))
        };
        let mut lost = Vec::new();
        for (key, durable) in unlinked.into_iter().filter(|_| revert) {
            if entropy.below(2) == 0 {
                continue;
            }
            let Some((bytes, created_at)) = durable else {
                continue;
            };
            let mut up = raw
                .begin_upload(&key, created_at, WriteMode::Replace)
                .await?;
            up.write(&bytes).await?;
            up.commit().await?;
            lost.push((key, Lost::Restored));
        }
        Ok(lost)
    }
}

/// What `key`'s directory entry holds in `store` now, read before an unlink: its bytes and
/// created-at time, or nothing when there is no such file.
async fn entry_of<S: ObjectStore>(store: &S, key: &FileKey) -> Result<Durable, StoreError> {
    let listing = store.list(key.level(), None, PageLimit::MAX).await?;
    let Some(info) = listing.files.iter().find(|f| f.key == *key) else {
        return Ok(None);
    };
    let created_at = store.created_at(info).await?.at();
    Ok(Some((read_all(store, key).await?, created_at)))
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
/// commit is a directory sync and makes every earlier rename durable. So is a
/// successful delete, as the directory store syncs each directory it deleted
/// from; [`Volatile::unsynced_deletes`] models a store that does not, whose
/// unlinks a power loss may undo.
#[derive(Clone, Debug)]
pub struct Volatile<S> {
    inner: S,
    handle: VolatileHandle,
    sync_after_delete: bool,
}

impl<S> Volatile<S> {
    /// Wraps `inner` with nothing volatile.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            handle: VolatileHandle::default(),
            sync_after_delete: true,
        }
    }

    /// The same store, deleting without syncing the directory after: an unlink
    /// stays undoable until a later commit syncs it.
    #[must_use]
    pub const fn unsynced_deletes(mut self) -> Self {
        self.sync_after_delete = false;
        self
    }

    /// The handle; clones share state.
    #[must_use]
    pub fn handle(&self) -> VolatileHandle {
        self.handle.clone()
    }
}

impl<S: ObjectStore> ObjectStore for Volatile<S> {
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

    async fn open(&self, key: &FileKey) -> Result<Option<Box<dyn Opened>>, StoreError> {
        self.inner.open(key).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        let inner = self
            .inner
            .begin_sized_upload(key, created_at, mode, len)
            .await?;
        lock(&self.handle.state).resolved.insert(*key, created_at);
        Ok(StageUpload {
            inner,
            handle: self.handle.clone(),
        })
    }

    /// An unlink, then a directory sync, as the directory store does; with
    /// [`Volatile::unsynced_deletes`], no sync: until the next commit syncs the directory, a
    /// power loss may undo the unlink ([`VolatileHandle::revert_unsynced_entries`]). A key
    /// whose own rename was never synced durably held nothing.
    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        let mut before = Vec::new();
        for key in keys.iter().filter(|_| !self.sync_after_delete) {
            let (seen, volatile) = {
                let state = lock(&self.handle.state);
                (
                    state.unlinked.contains_key(key),
                    state.volatile.contains(key),
                )
            };
            if !seen {
                let durable = if volatile {
                    None
                } else {
                    entry_of(&self.inner, key).await?
                };
                before.push((*key, durable));
            }
        }
        self.inner.delete(keys).await?;
        {
            let mut state = lock(&self.handle.state);
            state.unlinked.extend(before);
            for key in keys {
                state.volatile.remove(key);
                state.resolved.remove(key);
            }
        }
        if self.sync_after_delete {
            self.handle.fsync_all();
        }
        Ok(())
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }
}

/// An upload through [`Volatile`].
#[derive(Debug)]
pub struct StageUpload<U> {
    inner: U,
    handle: VolatileHandle,
}

impl<U: Upload> Upload for StageUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.inner.write(chunk).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        self.inner.write_from(chunk, again).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let info = self.inner.commit().await?;
        self.handle.fsync_all();
        Ok(info)
    }

    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        self.inner.flight(rest).await
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests;

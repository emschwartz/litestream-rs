//! Store wrappers: faults, crash points, ground truth and power loss.
//!
//! Each wrapper is an [`ObjectStore`] over another. The simulator stacks them
//! as `Faults<YieldEachCall<Recorder<Volatile<MemStore>>>>` for the staging directory and
//! `Faults<YieldEachCall<Recorder<MemStore>>>` for the replica, and keeps a plain `MemStore`
//! handle to the same state: the oracle's restores read through that handle, so
//! no fault or yield ever touches a check.
//!
//! - [`Faults`] fails calls on a schedule. Faults live here so the raw store
//!   stays clean. It can also make listings lag behind the bucket
//!   ([`FaultHandle::lag_listings`]): a write stays out of them, a deleted key
//!   stays in them, for a bounded number of list calls, as an eventually
//!   consistent listing (or a cache in front of one) does. Reads stay exact. It also says how wide a batch may be, so callers take
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
    BatchKind, CorruptCause, Entropy, FileInfo, FileKey, Level, ListAt, Listing, MemStore,
    ObjectStore, PageLimit, ReadAt, ReadLen, SeededEntropy, StoreError, StoreOp, Upload,
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
    /// The commit publishes the object and reports a size one byte too large.
    /// Counted over commits, `StoreOp::Upload` only.
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
    // Objects a flipped byte reached on a read, once per such read.
    flipped: Vec<FileKey>,
    // Reads, listings and created-at times one batch may hold.
    widths: [NonZeroUsize; 3],
    // Batch calls that held more than one item.
    batched: u32,
    lag: Lag,
}

/// Listings that lag behind the bucket.
#[derive(Debug, Default)]
struct Lag {
    /// Writes and deletes still to be hidden from listings.
    writes_left: u32,
    /// The most list calls one stays hidden.
    window: u32,
    /// What listings show instead of the truth, key by key.
    stale: Vec<Stale>,
    /// Listings it changed.
    changed: u32,
}

/// One key a listing shows as it was, not as it is.
#[derive(Debug)]
struct Stale {
    key: FileKey,
    /// What a listing shows for it: the object before the write or the
    /// delete, `None` when there was none.
    shown: Option<FileInfo>,
    /// How many more list calls see it so.
    lists_left: u32,
}

impl FaultState {
    /// Hides a write or a delete of `key` from listings for a while, when
    /// listings lag and writes are left to hide. `before` is what the key held.
    fn lag_write(&mut self, key: FileKey, before: Option<FileInfo>) {
        let Self { lag, rng, .. } = self;
        if lag.writes_left == 0 {
            return;
        }
        lag.writes_left = lag.writes_left.saturating_sub(1);
        let lists = u32::try_from(rng.below(u64::from(lag.window.max(1))))
            .unwrap_or(0)
            .saturating_add(1);
        match lag.stale.iter_mut().find(|stale| stale.key == key) {
            // Still showing an older state: keep it, for the longer time.
            Some(stale) => stale.lists_left = stale.lists_left.max(lists),
            None => lag.stale.push(Stale {
                key,
                shown: before,
                lists_left: lists,
            }),
        }
    }

    /// The stale keys of `level` for this list call, and one call counted
    /// against every stale key.
    fn stale_for(&mut self, level: Level) -> Vec<(FileKey, Option<FileInfo>)> {
        let here = self
            .lag
            .stale
            .iter()
            .filter(|stale| stale.key.level() == level)
            .map(|stale| (stale.key, stale.shown))
            .collect();
        for stale in &mut self.lag.stale {
            stale.lists_left = stale.lists_left.saturating_sub(1);
        }
        self.lag.stale.retain(|stale| stale.lists_left > 0);
        here
    }

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
            FaultShape::Error(_) | FaultShape::FlipByteOnRead { .. } | FaultShape::Rate { .. } => {
                *state.count(op)
            }
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

    /// The objects a [`FaultShape::CorruptPublish`] changed, in order: where
    /// the damage really landed, whatever the scenario meant to hit.
    #[must_use]
    pub fn damaged(&self) -> Vec<FileKey> {
        lock(&self.state).damaged.clone()
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

    /// Makes listings lag: each of the next `writes` commits and deletes
    /// stays out of listings (a deleted key stays in, as it was) for 1 to
    /// `window` list calls, drawn from the seed. Needs
    /// [`Faults::corrupting_through`], whose handle on the store under the
    /// wrappers is what a stale listing is built from.
    pub fn lag_listings(&self, writes: u32, window: u32) {
        let mut state = lock(&self.state);
        state.lag.writes_left = state.lag.writes_left.saturating_add(writes);
        state.lag.window = window.max(1);
    }

    /// Listings a lag changed.
    #[must_use]
    pub fn stale_listings(&self) -> u32 {
        lock(&self.state).lag.changed
    }

    /// Ends the lag: writes from now on show at once, and stale keys show as
    /// they are.
    pub fn end_lag(&self) {
        let mut state = lock(&self.state);
        state.lag.writes_left = 0;
        state.lag.stale.clear();
    }

    /// Stops every fault that fails at a rate.
    pub fn heal(&self) {
        for entry in &mut lock(&self.state).schedule {
            if matches!(entry.shape, FaultShape::Rate { .. }) {
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
            .filter(|e| !e.fired && !matches!(e.shape, FaultShape::Rate { .. }))
            .count()
    }
}

/// A store that fails on a seeded schedule.
#[derive(Clone, Debug)]
pub struct Faults<S> {
    inner: S,
    state: Arc<Mutex<FaultState>>,
    volatile: Option<VolatileHandle>,
    raw: Option<MemStore>,
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
            flipped: Vec::new(),
            rejected: Vec::new(),
            widths: [NonZeroUsize::MIN; 3],
            batched: 0,
            lag: Lag::default(),
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
    pub fn corrupting_through(mut self, raw: MemStore) -> Self {
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
}

const fn width_slot(kind: BatchKind) -> usize {
    match kind {
        BatchKind::Reads => 0,
        BatchKind::Lists => 1,
        BatchKind::ResolvedCreatedAt => 2,
    }
}

/// Flips the byte at object offset `at` when `bytes`, read from `offset`,
/// holds it; whether it did.
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
        let stale = lock(&self.state).stale_for(level);
        match (&self.raw, stale.is_empty()) {
            (Some(raw), false) => {
                let shown =
                    stale_listing(raw, level, &stale, after, limit, listing.skipped).await?;
                if shown != listing {
                    let mut state = lock(&self.state);
                    state.lag.changed = state.lag.changed.saturating_add(1);
                }
                Ok(shown)
            }
            (Some(_) | None, _) => Ok(listing),
        }
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::Begin) {
            return Err(err);
        }
        let inner = self.inner.begin_upload(key, created_at, mode).await?;
        Ok(FaultsUpload {
            inner,
            key: *key,
            wrote: 0,
            state: Arc::clone(&self.state),
            volatile: self.volatile.clone(),
            raw: self.raw.clone(),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        if let Verdict::Fail(err) = self.verdict(Call::Delete) {
            return Err(err);
        }
        let mut before = Vec::new();
        if let Some(raw) = &self.raw {
            for key in keys {
                before.push((*key, info_of(raw, key).await?));
            }
        }
        self.inner.delete(keys).await?;
        let mut state = lock(&self.state);
        for (key, info) in before {
            if info.is_some() {
                state.lag_write(key, info);
            }
        }
        Ok(())
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

    async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        self.note_batch(reads.len());
        let mut out = Vec::with_capacity(reads.len());
        for at in reads.iter().rev() {
            out.push(self.read(&at.key, at.offset, at.len).await);
        }
        out.reverse();
        out
    }

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
    wrote: u64,
    state: Arc<Mutex<FaultState>>,
    volatile: Option<VolatileHandle>,
    raw: Option<MemStore>,
}

fn checksum_mismatch(key: FileKey) -> StoreError {
    StoreError::Corrupt {
        key: Some(key),
        cause: CorruptCause::ChecksumMismatch { part: None },
    }
}

/// What `raw` holds under `key`, as a listing shows it.
async fn info_of(raw: &MemStore, key: &FileKey) -> Result<Option<FileInfo>, StoreError> {
    Ok(level_files(raw, key.level())
        .await?
        .into_iter()
        .find(|info| info.key == *key))
}

/// Every file `raw` holds in `level`.
async fn level_files(raw: &MemStore, level: Level) -> Result<Vec<FileInfo>, StoreError> {
    let mut files = Vec::new();
    let mut at = ListAt::first(level);
    loop {
        let page = raw.list(at.level, at.after, PageLimit::MAX).await?;
        files.extend(page.files);
        match at.following(page.next) {
            Ok(Some(next)) => at = next,
            Ok(None) | Err(_) => return Ok(files),
        }
    }
}

/// The page a lagging listing shows: the level as `raw` holds it, with each
/// stale key as it was instead.
async fn stale_listing(
    raw: &MemStore,
    level: Level,
    stale: &[(FileKey, Option<FileInfo>)],
    after: Option<TxidRange>,
    limit: PageLimit,
    skipped: usize,
) -> Result<Listing, StoreError> {
    let mut files = level_files(raw, level).await?;
    files.retain(|info| stale.iter().all(|(key, _)| *key != info.key));
    files.extend(stale.iter().filter_map(|(_, shown)| *shown));
    Ok(Listing {
        skipped,
        ..lsr_core::build_listing(files, &[], after, limit)
    })
}

/// Changes one byte in the middle of the published object, keeping its size
/// and its created-at time.
async fn damage(raw: &MemStore, info: &FileInfo) -> Result<(), StoreError> {
    let mut bytes = read_all(raw, &info.key).await?;
    let middle = bytes.len() / 2;
    if let Some(byte) = bytes.get_mut(middle) {
        *byte ^= 0xff;
    }
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
        let before = match &self.raw {
            Some(raw) => Some(info_of(raw, &self.key).await?),
            None => None,
        };
        let state = Arc::clone(&self.state);
        let (key, raw) = (self.key, self.raw.clone());
        let outcome = self.commit_inner().await;
        // Whatever the answer, a write that landed is what a lagging listing hides.
        if let (Some(before), Some(raw)) = (before, raw)
            && info_of(&raw, &key).await? != before
        {
            lock(&state).lag_write(key, before);
        }
        outcome
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.inner.abort().await
    }
}

impl<U: Upload> FaultsUpload<U> {
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
                info.size = info.size.saturating_add(1);
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        YieldNow(false).await;
        let inner = self.inner.begin_upload(key, created_at, mode).await?;
        Ok(YieldUpload { inner })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        YieldNow(false).await;
        self.inner.delete(keys).await
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

    async fn commit(self) -> Result<FileInfo, StoreError> {
        YieldNow(false).await;
        self.inner.commit().await
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

#[derive(Debug, Default)]
struct TapState {
    events: Vec<RecorderEvent>,
    counts: RecorderCounts,
    clock: Option<(Arc<FakeClock>, HighWater)>,
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

    /// The log; clones share it.
    #[must_use]
    pub fn log(&self) -> RecorderLog {
        self.log.clone()
    }
}

impl<S: ObjectStore> ObjectStore for Recorder<S> {
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        self.log.with(|s| bump(&mut s.counts.begins));

        let inner = self.inner.begin_upload(key, created_at, mode).await?;
        Ok(TapUpload {
            inner,
            key: *key,
            created_at,
            head: Vec::new(),
            wrote: 0,
            digest: DIGEST_SEED,
            log: self.log.clone(),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.log.with(|s| bump(&mut s.counts.deletes));

        self.inner.delete(keys).await?;
        self.log.with(|s| {
            let (at, high) = s.times();
            s.events.push(RecorderEvent::Delete {
                keys: keys.to_vec(),
                at,
                high,
            });
        });
        Ok(())
    }
}

/// An upload through [`Recorder`].
#[derive(Debug)]
pub struct TapUpload<S: ObjectStore> {
    inner: S::Upload,
    key: FileKey,
    created_at: Timestamp,
    head: Vec<u8>,
    wrote: u64,
    digest: u64,
    log: RecorderLog,
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
        self.inner.write(chunk).await?;
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

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let Self {
            inner,
            key,
            created_at,
            head,
            wrote,
            digest,
            log,
        } = self;
        let header = parse_header(&head);
        let outcome = inner.commit().await;
        log.with(|s| {
            bump(&mut s.counts.commits);
            // Only an object the store accepted is recorded: the recorder sits below
            // `Faults`, so a commit it sees fail never published.
            if outcome.is_ok() {
                let (at, high) = s.times();
                s.events.push(RecorderEvent::Put {
                    key,
                    header,
                    origin: header.and_then(|h| h.fields().wal),
                    created_at,
                    size: wrote,
                    digest,
                    at,
                    high,
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
    resolved: BTreeMap<FileKey, Timestamp>,
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
pub struct Volatile<S> {
    inner: S,
    handle: VolatileHandle,
}

impl<S> Volatile<S> {
    /// Wraps `inner` with nothing volatile.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            handle: VolatileHandle::default(),
        }
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        let inner = self.inner.begin_upload(key, created_at, mode).await?;
        lock(&self.handle.state).resolved.insert(*key, created_at);
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
            state.resolved.remove(key);
        }
        Ok(())
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

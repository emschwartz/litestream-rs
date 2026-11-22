//! The staging store under the wrappers: the simulator's `MemStore`, or the real directory store
//! (`lsr-store-fs`) on the in-memory filesystem ([`crate::memfs`]).
//!
//! With `MemStore` a power loss is [`VolatileHandle::power_loss`]: the harness decides which
//! staged files were not yet durable (those a commit published and then failed) and drops or cuts
//! them. That is a model of the directory store, kinder than the store can be: it assumes every
//! successful commit fsynced its file, renamed it and fsynced the directory, and every delete
//! synced its directory.
//!
//! With [`StagingRaw::Fs`] none of that is assumed. The store's own calls reach the filesystem, and
//! a power loss is the filesystem's ([`crate::memfs::MemFs::power_loss`]): whatever the store did
//! not make durable can go, under the run's crash law. What the staging namespace held before is
//! compared with what it holds after:
//!
//! - A file whose commit the fault layer turned into an error after publishing (the keys
//!   [`VolatileHandle`] holds) may go or be cut to a prefix: the engine was told it might not be
//!   there. It is reported to the model as lost, as with `MemStore`. (The store did sync it, so
//!   only a store that skips a sync loses one; any other change to it is a contract break.)
//! - Any other difference (a file whose commit returned success gone or changed, a file whose
//!   delete returned success back) breaks the store's contract, and is kept for the runner to
//!   raise as [`crate::Violation::StagedReverted`]. That judge is the one place the harness
//!   checks the store rather than the engine; the engine's judges see the same run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::conformance::block_on;
use lsr_core::{
    Entropy, FileInfo, FileKey, Level, Listing, MemStore, ObjectStore, PageLimit, ReadLen,
    StoreError, Upload,
};
use lsr_ltx::{Timestamp, TxidRange};
use lsr_store_fs::{FsStore, FsUpload};

use crate::crash_law::CrashLaw;
use crate::memfs::{MemFs, PowerLoss};
use crate::store::{Faults, Lost, Recorder, Volatile, VolatileHandle, YieldEachCall};
use crate::world::World;

/// Where the directory store keeps the staging directory on the in-memory filesystem.
const ROOT: &str = "/meta/staging";

/// How a staged file broke the store's contract across a power loss.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reverted {
    /// A file whose commit returned success is gone.
    Lost(FileKey),
    /// A file whose commit returned success came back with other bytes or another created-at.
    Changed(FileKey),
    /// A file whose delete returned success is back.
    Returned(FileKey),
    /// The namespace could not be read back after the power loss.
    Unreadable(String),
}

impl Reverted {
    /// The file, as text, and what happened to it.
    #[must_use]
    pub fn describe(&self) -> (String, &'static str) {
        match self {
            Self::Lost(key) => (key.to_string(), "committed, then gone"),
            Self::Changed(key) => (key.to_string(), "committed, then changed"),
            Self::Returned(key) => (key.to_string(), "deleted, then back"),
            Self::Unreadable(err) => (err.clone(), "unreadable after the power loss"),
        }
    }
}

#[derive(Debug, Default)]
struct FsLog {
    reverted: Vec<Reverted>,
    losses: Vec<PowerLoss>,
}

/// The real directory store on the in-memory filesystem, and what its power losses did.
#[derive(Clone, Debug)]
pub struct FsStaging {
    store: Arc<FsStore<MemFs>>,
    log: Arc<Mutex<FsLog>>,
}

/// The staging store with nothing in front of it.
#[derive(Clone, Debug)]
pub enum StagingRaw {
    /// The simulator's in-memory store.
    Mem(MemStore),
    /// The directory store on the in-memory filesystem.
    Fs(FsStaging),
}

/// An upload through [`StagingRaw`].
#[derive(Debug)]
pub enum StagingUpload {
    /// Through `MemStore`.
    Mem(<MemStore as ObjectStore>::Upload),
    /// Through the directory store.
    Fs(FsUpload<MemFs>),
}

/// A file's listing and bytes, as the store returns them.
type Namespace = BTreeMap<FileKey, (FileInfo, Vec<u8>)>;

impl StagingRaw {
    /// The directory store on a fresh in-memory filesystem whose power losses follow `law`.
    #[must_use]
    pub fn fs(law: CrashLaw) -> Self {
        Self::Fs(FsStaging {
            store: Arc::new(FsStore::with_fs(ROOT, MemFs::new(law))),
            log: Arc::default(),
        })
    }

    /// Loses power over staging. With `MemStore`, `volatile` decides what goes; with the
    /// directory store, the filesystem does, and `volatile` names what may (module doc).
    ///
    /// # Errors
    /// The store's error when it cannot read or rewrite a file.
    pub async fn power_loss(
        &self,
        volatile: &VolatileHandle,
        entropy: &dyn Entropy,
    ) -> Result<Vec<(FileKey, Lost)>, StoreError> {
        match self {
            Self::Mem(store) => volatile.power_loss(store, entropy).await,
            Self::Fs(fs) => Ok(fs.power_loss(volatile)),
        }
    }

    /// The contract breaks and power losses since the last call, emptied; nothing with
    /// `MemStore`.
    #[must_use]
    pub fn take_log(&self) -> (Vec<Reverted>, Vec<PowerLoss>) {
        match self {
            Self::Mem(_) => (Vec::new(), Vec::new()),
            Self::Fs(fs) => {
                let mut log = fs.log.lock().unwrap_or_else(PoisonError::into_inner);
                (
                    std::mem::take(&mut log.reverted),
                    std::mem::take(&mut log.losses),
                )
            }
        }
    }
}

impl FsStaging {
    fn power_loss(&self, volatile: &VolatileHandle) -> Vec<(FileKey, Lost)> {
        let may_go: BTreeSet<FileKey> = volatile.volatile().into_iter().collect();
        volatile.fsync_all();
        let before = namespace(self.store.as_ref());
        let loss = self.store.fs().power_loss();
        let after = namespace(self.store.as_ref());
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        log.losses.push(loss);
        let (before, after) = match (before, after) {
            (Ok(before), Ok(after)) => (before, after),
            (Err(err), _) | (_, Err(err)) => {
                log.reverted.push(Reverted::Unreadable(err.to_string()));
                return Vec::new();
            }
        };
        let (lost, reverted) = judge(&before, &after, &may_go);
        log.reverted.extend(reverted);
        lost
    }
}

/// What a power loss did to each file: the losses the engine was warned of, and the contract
/// breaks.
fn judge(
    before: &Namespace,
    after: &Namespace,
    may_go: &BTreeSet<FileKey>,
) -> (Vec<(FileKey, Lost)>, Vec<Reverted>) {
    let mut lost = Vec::new();
    let mut reverted = Vec::new();
    let keys: BTreeSet<FileKey> = before.keys().chain(after.keys()).copied().collect();
    for key in keys {
        let warned = may_go.contains(&key);
        match (before.get(&key), after.get(&key)) {
            (Some(was), Some(now)) if was == now => {}
            (Some(_), None) if warned => lost.push((key, Lost::Dropped)),
            (Some(_), None) => reverted.push(Reverted::Lost(key)),
            (Some((_, was)), Some((_, now))) if warned && was.starts_with(now) => {
                let kept = u64::try_from(now.len()).unwrap_or(u64::MAX);
                lost.push((key, Lost::Torn { kept }));
            }
            (Some(_), Some(_)) => reverted.push(Reverted::Changed(key)),
            (None, Some(_)) => reverted.push(Reverted::Returned(key)),
            (None, None) => {}
        }
    }
    (lost, reverted)
}

/// Every staged file, with its listing and bytes.
fn namespace(store: &FsStore<MemFs>) -> Result<Namespace, StoreError> {
    let mut out = BTreeMap::new();
    let chunk = ReadLen::new(1 << 20).ok_or(StoreError::Transient {
        op: lsr_core::StoreOp::Read,
        kind: std::io::ErrorKind::InvalidInput,
    })?;
    for level in Level::ALL {
        for info in block_on(store.list(level, None, PageLimit::MAX))?.files {
            let mut bytes = Vec::new();
            loop {
                let offset = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let part = block_on(store.read(&info.key, offset, chunk))?;
                let done = u64::try_from(part.len()).unwrap_or(0) < chunk.get();
                bytes.extend_from_slice(&part);
                if done {
                    break;
                }
            }
            out.insert(info.key, (info, bytes));
        }
    }
    Ok(out)
}

impl ObjectStore for StagingRaw {
    type Upload = StagingUpload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        match self {
            Self::Mem(s) => s.list(level, after, limit).await,
            Self::Fs(s) => s.store.list(level, after, limit).await,
        }
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        match self {
            Self::Mem(s) => s.read(key, offset, len).await,
            Self::Fs(s) => s.store.read(key, offset, len).await,
        }
    }

    async fn created_at(&self, file: &FileInfo) -> Result<lsr_core::CreatedAt, StoreError> {
        match self {
            Self::Mem(s) => s.created_at(file).await,
            Self::Fs(s) => s.store.created_at(file).await,
        }
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, lsr_core::CreatedAt), StoreError> {
        match self {
            Self::Mem(s) => s.read_with_created_at(file, len).await,
            Self::Fs(s) => s.store.read_with_created_at(file, len).await,
        }
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<StagingUpload, StoreError> {
        match self {
            Self::Mem(s) => s
                .begin_upload(key, created_at)
                .await
                .map(StagingUpload::Mem),
            Self::Fs(s) => s
                .store
                .begin_upload(key, created_at)
                .await
                .map(StagingUpload::Fs),
        }
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        match self {
            Self::Mem(s) => s.delete(keys).await,
            Self::Fs(s) => s.store.delete(keys).await,
        }
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        match self {
            Self::Mem(s) => s.remove_scratch().await,
            Self::Fs(s) => s.store.remove_scratch().await,
        }
    }
}

impl Upload for StagingUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        match self {
            Self::Mem(u) => u.write(chunk).await,
            Self::Fs(u) => u.write(chunk).await,
        }
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        match self {
            Self::Mem(u) => u.commit().await,
            Self::Fs(u) => u.commit().await,
        }
    }

    async fn abort(self) -> Result<(), StoreError> {
        match self {
            Self::Mem(u) => u.abort().await,
            Self::Fs(u) => u.abort().await,
        }
    }
}

impl World {
    /// Puts staging on the directory store over a fresh in-memory filesystem whose power losses
    /// follow `law`, with the same wrappers in front of it as before. Call before the first
    /// boot: staging starts empty, and the fault schedule and batch widths start over.
    pub fn use_fs_staging(&mut self, law: CrashLaw) {
        let raw = StagingRaw::fs(law);
        let volatile = Volatile::new(raw.clone());
        let handle = volatile.handle();
        let recorder =
            Recorder::new(volatile).with_clock(Arc::clone(&self.clock), self.high.clone());
        self.staging_recorder = recorder.log();
        self.staging = Faults::new(
            YieldEachCall::new(recorder),
            Vec::new(),
            self.seed.stream(2),
        )
        .volatile_on_late_error(handle.clone());
        self.staging_faults = self.staging.handle();
        self.staging_raw = raw;
        self.volatile = handle;
    }
}

#[cfg(test)]
mod tests;

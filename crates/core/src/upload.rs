//! Upload sequencing: staged L0 files go to the replica one at a time, in
//! ascending transaction order, each one verified on the way out.
//!
//! The state is what this engine knows about the replica's newest transaction.
//! [`UploadState::Unknown`] (boot) and [`UploadState::ListFirst`] (after a commit
//! error) both plan a listing before anything is written, because a commit that
//! failed may still have published the object. Steady state makes no LIST call:
//! the next upload is always the confirmed transaction plus one.
//!
//! A file is confirmed in exactly two ways: its commit returned the key with
//! the size that was written, or a listing found the key at the size of the
//! staged file. A listed size that differs (a store that acknowledged a short
//! object, a leftover of a crashed run) confirms nothing; the key is uploaded
//! again over the same name, which replaces it. A listing is also never trusted
//! to move the position backwards or past the local tip: either is a halt, not
//! a guess.
//!
//! Each staged file is streamed through the LTX decoder while it is copied. The
//! bytes are written to an upload that no listing can see, and the upload is
//! committed only after the decoder verified the whole file and its header names
//! exactly the key. Otherwise the upload is aborted and the staged file is
//! reported as corrupt: a bad staged file never becomes visible.
//!
//! A failure before the commit leaves the state alone (nothing could have been
//! published). A failure of the commit itself gives [`UploadState::ListFirst`],
//! except a checksum mismatch: the store holds wrong bytes that a listing would
//! confirm by size, so the state stays and the same key is written again.
//! Core runs no timer: [`upload_tick`] tells the actor whether to step again.

use std::num::NonZeroU32;

use lsr_ltx::{Decoder, Header, Txid, TxidRange};

use crate::fault::{Fault, Next};
use crate::inventory::{Inventory, InventoryError};
use crate::layout::{FileInfo, FileKey, Level};
use crate::store::{ObjectStore, PageLimit, ReadLen, Upload};

/// How much of a staged file is read at once.
const CHUNK: u32 = 256 * 1024;

/// What this engine knows about the replica's newest transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadState {
    /// Nothing yet (boot): list before writing.
    Unknown,
    /// The replica holds everything through this transaction (`None`: it is empty).
    At(Option<Txid>),
    /// A commit failed after the object may have become visible: list before
    /// writing. `confirmed` is what was known before that upload.
    ListFirst {
        /// The newest transaction confirmed before the failed commit.
        confirmed: Option<Txid>,
    },
}

impl UploadState {
    /// The newest confirmed transaction, if any is known.
    #[must_use]
    pub const fn confirmed(self) -> Option<Txid> {
        match self {
            Self::Unknown => None,
            Self::At(confirmed) | Self::ListFirst { confirmed } => confirmed,
        }
    }
}

/// What the next step does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadPlan {
    /// Everything staged is on the replica.
    Idle,
    /// List the replica to learn its newest transaction.
    List,
    /// Upload this staged file.
    Upload(FileKey),
}

/// Upload limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct UploadConfig {
    /// Files uploaded per tick before yielding (Go `MaxSyncLTXFiles`,
    /// `replica.go:208-218`). `None`: all that are staged.
    pub files_per_tick: Option<NonZeroU32>,
}

/// What one step did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Did {
    /// There was nothing to do.
    Nothing,
    /// The replica was listed; `newest` is what is now confirmed.
    Listed {
        /// The confirmed newest transaction.
        newest: Option<Txid>,
    },
    /// One file was committed.
    Put(FileInfo),
}

/// A step that went through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Uploaded {
    /// The state to plan from next.
    pub state: UploadState,
    /// What the step did.
    pub did: Did,
}

/// A step that failed. `state` is the state to plan from next, and what the
/// fault's policy then decides is the actor's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadFailed {
    /// The state to plan from next.
    pub state: UploadState,
    /// What went wrong.
    pub fault: Fault,
}

/// A whole tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticked {
    /// The state to plan from next.
    pub state: UploadState,
    /// How many files this tick committed.
    pub put: u32,
    /// `Now` when the tick stopped at its file budget with more staged,
    /// otherwise `OnWalChange`.
    pub next: Next,
}

fn staged_key(txid: Txid) -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(txid))
}

/// The next step from a state, given the newest staged transaction. Pure.
#[must_use]
pub fn plan_upload(state: &UploadState, local_tip: Option<Txid>) -> UploadPlan {
    match state {
        UploadState::Unknown | UploadState::ListFirst { .. } => UploadPlan::List,
        UploadState::At(confirmed) => {
            let next = confirmed.map_or(Some(Txid::MIN), Txid::next);
            match (next, local_tip) {
                (Some(next), Some(tip)) if next <= tip => UploadPlan::Upload(staged_key(next)),
                _ => UploadPlan::Idle,
            }
        }
    }
}

/// Runs one step of [`plan_upload`]: a listing, one upload, or nothing.
///
/// `staging` holds the staged L0 files; `remote` is the replica.
///
/// # Errors
/// [`UploadFailed`] with the state to continue from and the fault. A listing
/// that shows the replica ahead of `local_tip` or behind what was confirmed, a
/// staged file that is missing or fails verification: all halts by policy.
pub async fn upload_next<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    local_tip: Option<Txid>,
) -> Result<Uploaded, UploadFailed> {
    match plan_upload(&state, local_tip) {
        UploadPlan::Idle => Ok(Uploaded {
            state,
            did: Did::Nothing,
        }),
        UploadPlan::List => list_step(state, staging, remote, local_tip).await,
        UploadPlan::Upload(key) => put_step(state, staging, remote, key).await,
    }
}

/// Steps until the plan is idle or `config` says the tick is full.
///
/// # Errors
/// The first failed step; its state already includes what this tick confirmed.
pub async fn upload_tick<L: ObjectStore, S: ObjectStore>(
    mut state: UploadState,
    config: UploadConfig,
    staging: &L,
    remote: &S,
    local_tip: Option<Txid>,
) -> Result<Ticked, UploadFailed> {
    let mut put = 0_u32;
    loop {
        let full = config.files_per_tick.is_some_and(|cap| put >= cap.get());
        if full && plan_upload(&state, local_tip) != UploadPlan::Idle {
            return Ok(Ticked {
                state,
                put,
                next: Next::Now,
            });
        }
        let step = upload_next(state, staging, remote, local_tip).await?;
        state = step.state;
        match step.did {
            Did::Nothing => {
                return Ok(Ticked {
                    state,
                    put,
                    next: Next::OnWalChange,
                });
            }
            Did::Listed { .. } => {}
            Did::Put(_) => put = put.saturating_add(1),
        }
    }
}

fn fail(state: UploadState, fault: Fault) -> UploadFailed {
    UploadFailed { state, fault }
}

fn inventory_fault(err: &InventoryError) -> Fault {
    match err {
        InventoryError::Store(err) => Fault::from_remote(err),
        InventoryError::ConflictingDuplicate { .. } | InventoryError::ListingStalled { .. } => {
            Fault::RemoteCorrupt
        }
    }
}

async fn list_step<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    local_tip: Option<Txid>,
) -> Result<Uploaded, UploadFailed> {
    let inventory = Inventory::load(remote)
        .await
        .map_err(|err| fail(state, inventory_fault(&err)))?;
    let newest = inventory.newest();
    let confirmed = state.confirmed();
    if newest > local_tip {
        return Err(fail(state, Fault::RemoteAheadOfLocal));
    }
    if newest < confirmed {
        return Err(fail(state, Fault::RemoteRegressed));
    }
    let known = match newest {
        Some(tail) if newest > confirmed => {
            let sound = tail_matches_staged(&inventory, staging, tail)
                .await
                .map_err(|fault| fail(state, fault))?;
            if sound {
                newest
            } else {
                tail.prev().max(confirmed)
            }
        }
        _ => newest,
    };
    Ok(Uploaded {
        state: UploadState::At(known),
        did: Did::Listed { newest: known },
    })
}

/// Whether the replica's newest level-0 file has the size of the staged file
/// under the same key. A key with no staged counterpart cannot be compared and
/// is trusted: the listing is all there is.
async fn tail_matches_staged<L: ObjectStore>(
    inventory: &Inventory,
    staging: &L,
    tail: Txid,
) -> Result<bool, Fault> {
    let key = staged_key(tail);
    let Some(listed) = inventory
        .level(Level::ZERO)
        .iter()
        .find(|info| info.key == key)
    else {
        return Ok(true);
    };
    let after = tail.prev().map(TxidRange::single);
    let limit = PageLimit::new(1).ok_or(Fault::Bug)?;
    let page = staging
        .list(Level::ZERO, after, limit)
        .await
        .map_err(|err| Fault::from_staging(&err))?;
    Ok(page
        .files
        .first()
        .is_none_or(|staged| staged.key != key || staged.size == listed.size))
}

/// Why a copy did not commit, and so what the state becomes.
enum PutError {
    /// Nothing can have been published.
    Before(Fault),
    /// The commit failed or its answer cannot be believed.
    Commit(Fault),
    /// The store says the bytes it took are not the bytes sent. A listing
    /// cannot tell a wrong object of the right size from a right one, so the
    /// key is written again without asking.
    Rejected(Fault),
}

async fn put_step<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    key: FileKey,
) -> Result<Uploaded, UploadFailed> {
    match put_staged(staging, remote, &key).await {
        Ok(info) => Ok(Uploaded {
            state: UploadState::At(Some(key.range().max())),
            did: Did::Put(info),
        }),
        Err(PutError::Before(fault) | PutError::Rejected(fault)) => Err(fail(state, fault)),
        Err(PutError::Commit(fault)) => Err(fail(
            UploadState::ListFirst {
                confirmed: state.confirmed(),
            },
            fault,
        )),
    }
}

async fn put_staged<L: ObjectStore, S: ObjectStore>(
    staging: &L,
    remote: &S,
    key: &FileKey,
) -> Result<FileInfo, PutError> {
    let mut upload = None;
    let wrote = match copy_verified(staging, remote, key, &mut upload).await {
        Ok(wrote) => wrote,
        Err(err) => {
            if let Some(open) = upload {
                // Abort only releases scratch; a failure leaves nothing visible either way.
                drop(open.abort().await);
            }
            return Err(err);
        }
    };
    let Some(open) = upload else {
        return Err(PutError::Before(Fault::Bug));
    };
    match open.commit().await {
        Ok(info) if info.key == *key && info.size == wrote => Ok(info),
        Ok(_) => Err(PutError::Commit(Fault::UploadAmbiguous)),
        Err(err) => Err(commit_failed(Fault::from_remote(&err))),
    }
}

fn commit_failed(fault: Fault) -> PutError {
    if fault == Fault::UploadChecksumMismatch {
        PutError::Rejected(fault)
    } else {
        PutError::Commit(fault)
    }
}

fn feed_all(decoder: &mut Decoder, chunk: &[u8]) -> Result<(), PutError> {
    let mut rest = chunk;
    while !rest.is_empty() {
        let (used, _) = decoder
            .feed(rest)
            .map_err(|_undecodable| PutError::Before(Fault::StagedCorrupt))?;
        if used == 0 {
            return Err(PutError::Before(Fault::Bug));
        }
        rest = rest.get(used..).unwrap_or_default();
    }
    Ok(())
}

fn names_key(header: &Header, key: &FileKey) -> Result<(), PutError> {
    if header.txids() == key.range() {
        Ok(())
    } else {
        Err(PutError::Before(Fault::StagedCorrupt))
    }
}

/// Reads the staged file in chunks, decodes each, and forwards it to an upload
/// begun as soon as the header is known. Returns the bytes written once the
/// decoder has verified the whole file.
async fn copy_verified<L: ObjectStore, S: ObjectStore>(
    staging: &L,
    remote: &S,
    key: &FileKey,
    upload: &mut Option<S::Upload>,
) -> Result<u64, PutError> {
    let len = ReadLen::new(CHUNK).ok_or(PutError::Before(Fault::Bug))?;
    let mut decoder = Decoder::new();
    let mut held = Vec::new();
    let mut offset = 0_u64;
    loop {
        let chunk = staging
            .read(key, offset, len)
            .await
            .map_err(|err| PutError::Before(Fault::from_staging(&err)))?;
        feed_all(&mut decoder, &chunk)?;
        let read = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        offset = offset.saturating_add(read);
        let header = decoder.header().copied();
        match (upload.as_mut(), header) {
            (Some(open), _) => write_chunk(open, &chunk).await?,
            (None, Some(header)) => {
                names_key(&header, key)?;
                // Stored before the first write so a failed write still aborts it.
                let open = upload.insert(begin(remote, key, &header).await?);
                held.extend_from_slice(&chunk);
                write_chunk(open, &held).await?;
                held = Vec::new();
            }
            (None, None) => held.extend_from_slice(&chunk),
        }
        if read < len.get() {
            break;
        }
    }
    let verified = decoder
        .finish()
        .map_err(|_undecodable| PutError::Before(Fault::StagedCorrupt))?;
    names_key(verified.header(), key)?;
    Ok(offset)
}

async fn begin<S: ObjectStore>(
    remote: &S,
    key: &FileKey,
    header: &Header,
) -> Result<S::Upload, PutError> {
    remote
        .begin_upload(key, header.fields().timestamp)
        .await
        .map_err(|err| PutError::Before(Fault::from_remote(&err)))
}

async fn write_chunk<U: Upload>(upload: &mut U, chunk: &[u8]) -> Result<(), PutError> {
    upload
        .write(chunk)
        .await
        .map_err(|err| PutError::Before(Fault::from_remote(&err)))
}

#[cfg(test)]
mod tests;

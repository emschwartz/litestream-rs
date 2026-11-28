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
//! the size that was written, or a listing found the key and the listed object,
//! read back, is the staged file byte for byte. A size alone confirms nothing:
//! a store that acknowledged wrong bytes of the right size would pass it. A
//! listed object that is not a whole LTX file naming its key (a short object,
//! a torn write) confirms nothing and the key is uploaded again over the same
//! name, which replaces it. A listed object that verifies but differs from the
//! staged file (a run before a power loss uploaded another commit under the
//! id, or a staging backup was put back) is never written over: it may
//! already be compacted into higher levels, so it stands, and the listing
//! reports [`Did::Diverged`] for the actor to drop the staged files from it on
//! and snapshot above it. A key with no staged counterpart (staging is dropped once a
//! file is confirmed) must decode and verify as LTX naming that key, else the
//! replica is corrupt. The read-back runs at boot and after an ambiguous commit
//! only. A listing is also
//! never trusted to move the position backwards or past the local position: either
//! is a persistent fault for the actor's configured response, not a guess.
//!
//! The replica's store is required to list its own writes (docs/decisions/D173.md), so a
//! listing is taken as it stands: a file it shows past the local position is another
//! writer's (`Fault::RemoteAheadOfLocal`), and an end below what was confirmed is a
//! regression (`Fault::RemoteRegressed`). A write whose answer was lost is the one case a
//! listing is not needed for: the next put is create-only and the bucket refuses it.
//!
//! Each staged file is streamed through the LTX decoder while it is copied. The
//! bytes are written to an upload that no listing can see, and the upload is
//! committed only after the decoder verified the whole file, its header names
//! exactly the key, and its length and trailer checksum are the ones its
//! [`Witnessed`] token was minted for (`crate::witness`). Otherwise the upload
//! is aborted and the staged file is reported lost (`Fault::StagedLost`): a
//! bad staged file, one with no token, or one replaced after it got its token
//! never becomes visible. One failed read is evidence about the read, not the
//! file, so the actor retries; when a read on a later tick fails too, it
//! deletes the file and everything staged after it ([`drop_staged_from`]) and
//! snapshots in its place. At boot, the one pass over the staged files
//! (`crate::witness::scan`) applies the same rule before anything is uploaded.
//!
//! Every upload here is of a key the engine takes to be free (the confirmed
//! transaction plus one), so it is a [`WriteMode::New`] write: the bucket, not
//! a listing, refuses a key that is taken, which a stale listing or another
//! writer cannot hide. A refused commit reads the object back:
//! - the same bytes are this engine's own earlier commit whose answer was
//!   lost: confirmed, as [`Did::AlreadyPut`];
//! - a staged copy that does not verify on a read of its own is
//!   `Fault::StagedLost`, and nothing is judged from it (one read never
//!   condemns a file; the next tick puts it again);
//! - an object that is not a whole LTX file naming the key is replaced, as a
//!   listing's read-back replaces one;
//! - a verifying object with other bytes, on a second compare too, and a read of each copy
//!   that verifies whole with a different file checksum (a compare alone may be a read that
//!   lied), is another writer's or an earlier run's: the replica is listed at once and judged by
//!   the listing's rules above ([`Did::Diverged`], or
//!   `Fault::RemoteAheadOfLocal` when the replica is ahead). A listing that still ends below
//!   the refused key hides a file the bucket holds: `Fault::RemoteRegressed`, never a second
//!   put of the key in the same step.
//!
//! A failure before the commit leaves the state alone (nothing could have been
//! published). A failure of the commit itself gives [`UploadState::ListFirst`],
//! except a checksum mismatch: the state stays and the same key is written again, with
//! nothing deleted. The refusal says nothing about the key (S3 refuses a body before it looks
//! at the create-only condition), which may hold an earlier attempt's object; a retry that
//! finds the key taken reads it back as any refusal does (`docs/decisions/D280.md`).
//!
//! **A slice per step.** A step hands the replica at most its budget of file bytes
//! ([`SLICE`] a tick), so a file as long as the database does not hold the engine for its
//! length (`docs/decisions/D217.md`). What is left of a file is a [`Sending`]: the decoder
//! part-way through it, the bytes tallied so far and the upload begun for it, kept by the
//! caller between steps in an [`InFlight`]. The next step reads on from where the last
//! stopped, so every byte sent is still verified on the read that produced it, and the
//! upload is committed only when the whole file verified against its token. Nothing of a
//! send is on disk: a fault, a restart or a rebuild starts the file again from its first
//! byte, and the upload it had begun is aborted, or left as scratch the next boot clears.
//! Whatever deletes staged files asks the [`InFlight`] first ([`Unsent`]), so a file is
//! never deleted under a send of it. [`upload_next`] is the same steps run until a file is
//! whole. Core runs no timer: the actor steps [`upload_step`] up to its per-tick limits.
//!
//! **One open file a send.** A staging store that has a file to hold open
//! ([`ObjectStore::open`]: the directory store, which otherwise opens the file again for
//! every chunk) is asked for it at the send's first byte, and every chunk of every slice is
//! read through what it gave, kept in the [`Sending`] and dropped with it. What is held is
//! the file that was opened. A file renamed over the staged one under a send is therefore
//! not read: the file the send began with goes up, verified chunk by chunk and whole against
//! its token, where a store read by range reads the new file and refuses the send
//! (`docs/decisions/D234.md`). The second read a store with parts asks for ([`Staged`]) is
//! a read by name either way, so it still refuses a file that is no longer the one first
//! read.

use std::io;
use std::marker::PhantomData;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex, PoisonError};

use lsr_ltx::{Decoder, Txid, TxidRange, Verified};

use crate::fault::role::{Replica, Role, Staging};
use crate::fault::{BugSite, Fault};
use crate::inventory::{EmptyLevels, Inventory, InventoryError};
use crate::layout::{FileInfo, FileKey, Level};
use crate::ltx_read::verified;
use crate::publish::{self, Open, Unpublished, Writer};
use crate::store::{
    Again, BoxFuture, Flight, ListAt, ObjectStore, Opened, PageLimit, ReadLen, Reread, Rest,
    Seeing, Seen, StoreError, StoreOp, WriteMode,
};
use crate::witness::{Witnessed, Witnesses};

/// How much of a staged file is read at once.
const CHUNK: ReadLen = ReadLen::or_chunk(256 * 1024);

/// The most bytes of staged files one tick hands the replica unless the configuration says
/// otherwise: 8 MiB, 32 reads of [`CHUNK`]. A tick with a slice makes at most that many
/// reads and writes, each within its store's own deadline (`docs/decisions/D217.md`).
const SLICE: NonZeroU64 = NonZeroU64::MIN.saturating_add(8 * 1024 * 1024 - 1);

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadConfig {
    /// Files uploaded per tick before yielding (Go `MaxSyncLTXFiles`,
    /// `replica.go:208-218`). `None`: all that are staged.
    pub files_per_tick: Option<NonZeroU32>,
    /// Bytes of staged files one tick reads and hands the replica before yielding: 8 MiB
    /// unless a test or the simulator says otherwise. A file they run out in stays in flight,
    /// and the next upload step sends on from there (`docs/decisions/D217.md`).
    pub bytes_per_tick: NonZeroU64,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            files_per_tick: None,
            bytes_per_tick: SLICE,
        }
    }
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
    /// The bucket refused the upload of this key because it already held the
    /// same bytes, read back: this engine's earlier commit, whose answer was
    /// lost or whose listing was stale, is confirmed.
    AlreadyPut(FileKey),
    /// The replica was listed and the staged files from `confirmed` on cannot
    /// continue it, so they must go and new content takes a transaction above
    /// it: the replica's newest level-0 object verifies and differs from the
    /// staged file under the same key (`confirmed` is that key's
    /// transaction). It stands and is confirmed, since it may already be
    /// compacted into higher levels and writing over it would give one
    /// transaction two contents. Whether the staged files after it continue
    /// the replica is not a listing's to judge: each needs its witness
    /// (`crate::witness`), which a boot takes from the live WAL.
    Diverged {
        /// The newest transaction the replica holds, now confirmed: also the first staged
        /// transaction that cannot follow it.
        confirmed: Txid,
    },
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

/// The next step from a state, given the newest staged transaction. Pure.
#[must_use]
pub(crate) fn plan_upload(state: &UploadState, newest_staged: Option<Txid>) -> UploadPlan {
    match state {
        UploadState::Unknown | UploadState::ListFirst { .. } => UploadPlan::List,
        UploadState::At(confirmed) => {
            let next = confirmed.map_or(Some(Txid::MIN), Txid::next);
            match (next, newest_staged) {
                (Some(next), Some(position)) if next <= position => {
                    UploadPlan::Upload(FileKey::staged(next))
                }
                _ => UploadPlan::Idle,
            }
        }
    }
}

/// Runs one step of [`plan_upload`]: a listing, one upload, or nothing.
///
/// `staging` holds the staged L0 files; `remote` is the replica. A file is
/// uploaded only with its token in `witnesses`, and only when the bytes read
/// for the upload are the bytes the token was minted for (`crate::witness`).
///
/// # Errors
/// [`UploadFailed`] with the state to continue from and the fault. A listing
/// that shows the replica ahead of `newest_staged` or behind what was confirmed, a
/// staged file that is missing or fails verification: persistent faults
/// (another writer, a replica that went backwards) and `StagedLost`.
pub async fn upload_next<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    newest_staged: Option<Txid>,
    witnesses: &Witnesses,
) -> Result<Uploaded, UploadFailed> {
    let (staging, remote) = (&Staging(staging), &Replica(remote));
    let with = (witnesses, WriteMode::New);
    let mut in_flight = InFlight::default();
    loop {
        let mut budget = SLICE.get();
        let step = upload_step(
            state,
            (staging, remote),
            newest_staged,
            with,
            (&mut in_flight, &mut budget),
        );
        if let Some(uploaded) = step.await? {
            return Ok(uploaded);
        }
    }
}

/// One step of [`plan_upload`]: a listing, nothing, or the planned file sent until it is
/// whole or `budget`, bytes of staged files this step may still read, runs out. `None` is
/// the budget running out part-way through a file, which `in_flight` then keeps for the
/// next step. A send in flight that is not of the planned file is ended first.
///
/// # Errors
/// As [`upload_next`]. A failed step leaves nothing in flight.
pub(crate) async fn upload_step<L: Role, S: Role>(
    state: UploadState,
    (staging, remote): (&L, &S),
    newest_staged: Option<Txid>,
    (witnesses, mode): (&Witnesses, WriteMode),
    (in_flight, budget): (&mut InFlight, &mut u64),
) -> Result<Option<Uploaded>, UploadFailed> {
    let plan = plan_upload(&state, newest_staged);
    let token = match plan {
        UploadPlan::Upload(key) => witnesses.get(&key),
        UploadPlan::Idle | UploadPlan::List => None,
    };
    let sending = in_flight.take_for(token, mode).await;
    match (plan, sending) {
        (UploadPlan::Idle, _) => Ok(Some(Uploaded {
            state,
            did: Did::Nothing,
        })),
        (UploadPlan::List, _) => list_step(state, staging, remote, newest_staged)
            .await
            .map(|(listed, _)| Some(listed)),
        (UploadPlan::Upload(_), Some(sending)) => {
            let stores = (staging, remote);
            put_step(state, stores, sending, newest_staged, (in_flight, budget)).await
        }
        // No witness: the file is refused unread, as a lost one is.
        (UploadPlan::Upload(_), None) => Err(fail(state, Fault::StagedLost)),
    }
}

fn fail(state: UploadState, fault: Fault) -> UploadFailed {
    UploadFailed { state, fault }
}

/// A listing failure as the fault the uploader and the actor route.
pub(crate) fn inventory_fault<R: Role>(err: &InventoryError) -> Fault {
    match err {
        InventoryError::Store(err) => R::fault(err),
        InventoryError::ConflictingDuplicate { .. } | InventoryError::ListingStalled(_) => {
            Fault::RemoteCorrupt
        }
    }
}

/// The uploader's listing of every level, and what it showed of which levels 1 to 8 are
/// empty (the boot keeps that for maintenance, `EmptyLevels`).
pub(crate) async fn list_step<L: Role, S: Role>(
    state: UploadState,
    staging: &L,
    remote: &S,
    newest_staged: Option<Txid>,
) -> Result<(Uploaded, EmptyLevels), UploadFailed> {
    let starts = Level::ALL.map(ListAt::first);
    let inventory = Inventory::load_from(remote, &starts)
        .await
        .map_err(|err| fail(state, inventory_fault::<S>(&err)))?;
    let mut empty = EmptyLevels::default();
    empty.learn(&inventory, &starts);
    let confirmed = state.confirmed();
    let newest = inventory.newest();
    if newest > newest_staged {
        return Err(fail(state, Fault::RemoteAheadOfLocal));
    }
    if newest < confirmed {
        return Err(fail(state, Fault::RemoteRegressed));
    }
    let known = match newest {
        Some(tail) if newest > confirmed => {
            let verdict = match listed_size(&inventory, tail) {
                Some(size) => judge_tail(staging, remote, tail, size)
                    .await
                    .map_err(|fault| fail(state, fault))?,
                // The newest transaction is in a higher level only: nothing
                // at level 0 to read back.
                None => TailVerdict::Sound,
            };
            match verdict {
                TailVerdict::Sound => newest,
                // Replacing the tail would end the replica below a file the listing shows that
                // already holds it: an end the boot cannot prove, so it stops.
                TailVerdict::Replace if held_above(&inventory, tail) => {
                    return Err(fail(state, Fault::RemoteCorrupt));
                }
                TailVerdict::Replace => tail.prev().max(confirmed),
                TailVerdict::Diverged => {
                    return Ok((diverged(tail), empty));
                }
            }
        }
        _ => newest,
    };
    Ok((
        Uploaded {
            state: UploadState::At(known),
            did: Did::Listed { newest: known },
        },
        empty,
    ))
}

/// Whether a level 1 to 9 file in `inventory` holds `txid`: it covers it, so the transaction's
/// level-0 file cannot be written again without giving it a second content.
fn held_above(inventory: &Inventory, txid: Txid) -> bool {
    inventory
        .files()
        .iter()
        .any(|info| info.key.level() != Level::ZERO && info.key.range().contains(txid))
}

/// The size a listing shows for the level-0 file of `txid`, when it shows one.
fn listed_size(inventory: &Inventory, txid: Txid) -> Option<u64> {
    let key = FileKey::staged(txid);
    inventory
        .level(Level::ZERO)
        .iter()
        .find(|info| info.key == key)
        .map(|info| info.size)
}

fn diverged(confirmed: Txid) -> Uploaded {
    Uploaded {
        state: UploadState::At(Some(confirmed)),
        did: Did::Diverged { confirmed },
    }
}

/// What a listing's read-back found at the replica's newest level-0 key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TailVerdict {
    /// Equal to the staged file, or with no usable staged file, a verified
    /// LTX file naming the key: confirmed.
    Sound,
    /// Not a whole LTX file naming the key, beside a staged file that is: the
    /// staged file is uploaded over it (nothing could have compacted it).
    Replace,
    /// Both copies verify and differ: the replica's object stands.
    Diverged,
}

/// Judges the replica's newest level-0 file against the staged file under
/// the same key; `size` is the replica object's listed size. Only the newest key is read
/// back: an older one was confirmed by its own commit, and reading every key at boot would
/// be a whole replica scan.
async fn judge_tail<L: Role, S: Role>(
    staging: &L,
    remote: &S,
    tail: Txid,
    size: u64,
) -> Result<TailVerdict, Fault> {
    let key = FileKey::staged(tail);
    let after = tail.prev().map(TxidRange::single);
    let limit = PageLimit::ONE;
    let page = staging
        .list(Level::ZERO, after, limit)
        .await
        .map_err(|err| L::fault(&err))?;
    let Some(staged) = page.files.first().filter(|staged| staged.key == key) else {
        return remote_decodes(remote, &key)
            .await
            .map(|()| TailVerdict::Sound);
    };
    if size == staged.size && same_bytes(staging, remote, &key).await? {
        return Ok(TailVerdict::Sound);
    }
    // One read never condemns a file: a staged copy that fails to verify is
    // read again before it is taken as torn.
    if staged_verified(staging, &key).await?.is_none()
        && staged_verified(staging, &key).await?.is_none()
    {
        // Torn on two reads: it says nothing about the replica's object.
        return remote_decodes(remote, &key)
            .await
            .map(|()| TailVerdict::Sound);
    }
    if remote_torn(remote, &key).await? {
        return Ok(TailVerdict::Replace);
    }
    // Both copies verify on their own reads, so the first compare read
    // something wrong or they differ: a second compare decides, as one read
    // never condemns a file.
    if same_bytes(staging, remote, &key).await? {
        return Ok(TailVerdict::Sound);
    }
    // A compare that differs may be a read that lied, so it proves nothing about the replica's
    // object: only a read of each that verifies whole can show another writer's file. The
    // trailer's checksum covers the whole file. A copy that does not verify on this read is
    // one more bad read, and the step is retried.
    let ours = staged_verified(staging, &key)
        .await?
        .ok_or(Fault::StagedLost)?;
    let theirs = verified(remote, &key, CHUNK, S::fault)
        .await?
        .ok_or(Fault::StoreTransient)?;
    Ok(
        if ours.trailer().file_checksum() == theirs.trailer().file_checksum() {
            TailVerdict::Sound
        } else {
            TailVerdict::Diverged
        },
    )
}

/// Streams both copies and compares them chunk by chunk.
pub(crate) async fn same_bytes<L: Role, S: Role>(
    staging: &L,
    remote: &S,
    key: &FileKey,
) -> Result<bool, Fault> {
    let len = CHUNK;
    let mut offset = 0_u64;
    loop {
        let ours = staging
            .read(key, offset, len)
            .await
            .map_err(|err| L::fault(&err))?;
        let theirs = remote
            .read(key, offset, len)
            .await
            .map_err(|err| S::fault(&err))?;
        if ours != theirs {
            return Ok(false);
        }
        let read = u64::try_from(ours.len()).unwrap_or(u64::MAX);
        offset = offset.saturating_add(read);
        if read < len.get() {
            return Ok(true);
        }
    }
}

/// Reads the replica's object through the LTX decoder. It must verify whole and
/// name exactly `key`; anything else is a corrupt replica.
async fn remote_decodes<S: Role>(remote: &S, key: &FileKey) -> Result<(), Fault> {
    if remote_verifies(remote, key).await? {
        Ok(())
    } else {
        Err(Fault::RemoteCorrupt)
    }
}

/// Whether the replica's object `key` verifies whole and names exactly `key`.
///
/// # Errors
/// The replica's fault when a read fails: that says nothing about the bytes.
pub(crate) async fn remote_verifies<S: Role>(remote: &S, key: &FileKey) -> Result<bool, Fault> {
    verified(remote, key, CHUNK, S::fault)
        .await
        .map(|summary| summary.is_some())
}

/// Whether the replica's object `key` fails verification on two reads in a
/// row. Writing over it is decided by this, never by one read: a read that
/// came back wrong says nothing about the stored bytes, and a verifying
/// object with other bytes is never written over.
async fn remote_torn<S: Role>(remote: &S, key: &FileKey) -> Result<bool, Fault> {
    Ok(!remote_verifies(remote, key).await? && !remote_verifies(remote, key).await?)
}

/// What verified of the staged file `key`: its summary when it verifies whole and names
/// exactly `key`. A missing or unusable one does not; any other store error is the fault.
async fn staged_verified<L: Role>(staging: &L, key: &FileKey) -> Result<Option<Verified>, Fault> {
    match verified(staging, key, CHUNK, L::fault).await {
        Err(Fault::StagedLost) => Ok(None),
        other => other,
    }
}

/// The staged level-0 files whose last transaction is at least `from` (every one when
/// `None`), oldest first and a listing page at a time. A staging directory can hold any
/// number of files (Go's has no cap, and a long outage grows it), so no pass holds more of
/// it than one page.
pub(crate) struct StagedPages {
    at: Option<ListAt>,
    from: Option<Txid>,
}

impl StagedPages {
    /// From the first page.
    pub(crate) const fn from(from: Option<Txid>) -> Self {
        Self {
            at: Some(ListAt::first(Level::ZERO)),
            from,
        }
    }

    /// The next page's files at or after `from` (possibly none), or `None` past the last page.
    ///
    /// # Errors
    /// The staging store's fault when it cannot list, or lists with a cursor that stalls.
    pub(crate) async fn next<L: Role>(
        &mut self,
        staging: &L,
    ) -> Result<Option<Vec<FileInfo>>, Fault> {
        let Some(at) = self.at else {
            return Ok(None);
        };
        let page = staging
            .list(at.level, at.after, PageLimit::MAX)
            .await
            .map_err(|err| L::fault(&err))?;
        self.at = at
            .following(page.next)
            .map_err(Fault::from_staging_listing)?;
        let from = self.from;
        Ok(Some(
            page.files
                .into_iter()
                .filter(|info| from.is_none_or(|from| info.key.range().max() >= from))
                .collect(),
        ))
    }
}

/// The first `most` staged level-0 files whose last transaction is at least `from`, oldest
/// first; the listing is read no further than they need.
///
/// # Errors
/// As [`StagedPages::next`].
#[cfg(test)]
pub(crate) async fn staged_from<L: Role>(
    staging: &L,
    from: Option<Txid>,
    most: usize,
) -> Result<Vec<FileInfo>, Fault> {
    let mut files = Vec::new();
    let mut pages = StagedPages::from(from);
    while files.len() < most
        && let Some(page) = pages.next(staging).await?
    {
        let room = most.saturating_sub(files.len());
        files.extend(page.into_iter().take(room));
    }
    Ok(files)
}

/// Deletes every staged level-0 file whose last transaction is at least
/// `from`, newest first and one at a time, so a crash part way leaves the
/// oldest of them in place and the next boot finds it again. It holds one
/// listing page at a time: each round lists to the last page and deletes it
/// from its end, so a directory of n files costs about n squared over the
/// page size in listing calls, and never n keys in memory.
///
/// # Errors
/// The staging store's fault when it cannot list or delete.
pub(crate) async fn drop_staged_from<L: Role>(
    staging: &L,
    from: Txid,
    unsent: &Unsent<'_>,
) -> Result<(), Fault> {
    loop {
        let mut last = Vec::new();
        let mut pages = StagedPages::from(Some(from));
        while let Some(page) = pages.next(staging).await? {
            if !page.is_empty() {
                last = page;
            }
        }
        if last.is_empty() {
            return Ok(());
        }
        for info in last.iter().rev() {
            publish::remove_staged(staging, &[info.key], unsent)
                .await
                .map_err(|err| L::fault(&err))?;
        }
    }
}

/// Why a copy did not commit, and so what the state becomes.
enum PutError {
    /// The bucket refused a [`WriteMode::New`] commit: the key holds an
    /// object.
    Taken,
    /// Nothing can have been published.
    Before(Fault),
    /// The commit failed or its answer cannot be believed.
    Commit(Fault),
    /// The store says the bytes are not the bytes sent: the key is written again without
    /// asking, and nothing is deleted.
    Rejected(Fault),
}

/// One staged file part-way to the replica: what the verified copy of it holds between two
/// slices. It is in memory only.
#[derive(Debug)]
pub(crate) struct Sending {
    /// The file, and the bytes a whole read of it must turn out to be.
    token: Witnessed,
    mode: WriteMode,
    /// Fed every byte read so far, in order.
    decoder: Decoder,
    /// How many bytes were read: where the next read starts.
    offset: u64,
    /// The staged file held open since its first read, by a store that has something to hold
    /// (a file store, which would otherwise open the file again for every chunk).
    source: Option<Box<dyn Opened>>,
    tally: Seeing,
    /// What was read before the header was whole; empty once the upload is begun.
    held: Vec<u8>,
    /// Begun as soon as the header is known.
    open: Option<Open>,
    /// Second reads that gave nothing, noted by the readers the upload's parts hold.
    notes: Notes,
    /// The file's last byte was read and the whole verified: only the upload's parts and
    /// its commit are left.
    ended: bool,
    /// The last step stopped for the upload's parts: nothing to do on this send until one
    /// of them ends.
    waits: bool,
}

/// The upload task's file part-way sent, if any: at most one, kept between steps.
#[derive(Debug, Default)]
pub(crate) struct InFlight(Option<Box<Sending>>);

/// The upload task's word that no send in flight is reading the staged files a caller is
/// about to delete. Only an [`InFlight`] gives it, and no send starts while it is held.
#[derive(Debug)]
pub(crate) struct Unsent<'a>(PhantomData<&'a mut InFlight>);

impl InFlight {
    /// How many bytes of the file in flight were read and handed to the replica.
    pub(crate) fn sent(&self) -> Option<u64> {
        self.0.as_ref().map(|sending| sending.offset)
    }

    /// Whether the send in flight stopped for parts its upload has started: a tick has
    /// nothing to do on it until one ends (the upload wakes the task that asked).
    pub(crate) fn waits(&self) -> bool {
        self.0.as_ref().is_some_and(|sending| sending.waits)
    }

    /// Ends the send in flight and aborts the upload it had begun, so that any staged file
    /// may be deleted.
    pub(crate) async fn stop(&mut self) -> Unsent<'_> {
        if let Some(sending) = self.0.take() {
            abort(sending.open).await;
        }
        Unsent(PhantomData)
    }

    /// For a delete of staged files through `through` and no later: `None` while the file in
    /// flight is one of them.
    pub(crate) fn unsent_through(&mut self, through: Txid) -> Option<Unsent<'_>> {
        self.0
            .as_ref()
            .is_none_or(|sending| sending.token.key().range().min() > through)
            .then_some(Unsent(PhantomData))
    }

    /// The send in flight when it is of `token`'s file, as witnessed; any other is ended, and
    /// a new send of `token`'s file, when there is one, starts at its first byte.
    async fn take_for(
        &mut self,
        token: Option<&Witnessed>,
        mode: WriteMode,
    ) -> Option<Box<Sending>> {
        match (self.0.take(), token) {
            (Some(sending), Some(token)) if sending.token == *token => Some(sending),
            (stale, token) => {
                if let Some(stale) = stale {
                    abort(stale.open).await;
                }
                token.map(|token| Sending::start(*token, mode))
            }
        }
    }
}

/// Ends an upload begun and not committed. Abort only releases scratch; a failure leaves
/// nothing visible either way.
async fn abort(open: Option<Open>) {
    if let Some(open) = open {
        drop(open.abort().await);
    }
}

/// Sends `sending` until its file is whole or the budget runs out, and settles the outcome.
async fn put_step<L: Role, S: Role>(
    state: UploadState,
    (staging, remote): (&L, &S),
    mut sending: Box<Sending>,
    newest_staged: Option<Txid>,
    (in_flight, budget): (&mut InFlight, &mut u64),
) -> Result<Option<Uploaded>, UploadFailed> {
    let token = sending.token;
    let outcome = match sending.advance(staging, remote, budget).await {
        Ok(stopped @ (Reached::Budget | Reached::Full)) => {
            sending.waits = matches!(stopped, Reached::Full);
            in_flight.0 = Some(sending);
            return Ok(None);
        }
        Ok(Reached::End) => sending.commit::<L, S>(remote).await,
        Err(err) => {
            abort(sending.open.take()).await;
            Err(err)
        }
    };
    match outcome {
        Err(PutError::Taken) => {
            let stores = (staging, remote);
            taken(state, stores, &token, newest_staged, in_flight).await
        }
        outcome => settle_put(state, token.key(), outcome).map(Some),
    }
}

/// The state and step a put's outcome gives (a refusal is [`taken`]'s).
fn settle_put(
    state: UploadState,
    key: FileKey,
    outcome: Result<FileInfo, PutError>,
) -> Result<Uploaded, UploadFailed> {
    match outcome {
        Ok(info) => Ok(put_done(key, info)),
        // A rejected write deletes nothing: the refusal says nothing about what the key holds
        // (module doc), and the retry writes it again either way.
        Err(PutError::Before(fault) | PutError::Rejected(fault)) => Err(fail(state, fault)),
        Err(PutError::Commit(fault)) => Err(fail(list_first(state), fault)),
        // Only a `New` write is refused; a refused `Replace` is a store that
        // broke its contract, and a listing finds out what it holds.
        Err(PutError::Taken) => Err(fail(list_first(state), Fault::UploadAmbiguous)),
    }
}

fn put_done(key: FileKey, info: FileInfo) -> Uploaded {
    Uploaded {
        state: UploadState::At(Some(key.range().max())),
        did: Did::Put(info),
    }
}

fn list_first(state: UploadState) -> UploadState {
    UploadState::ListFirst {
        confirmed: state.confirmed(),
    }
}

/// The bucket refused the upload of `key`: an object is already there. See
/// the module docs for what each read-back finds.
async fn taken<L: Role, S: Role>(
    state: UploadState,
    (staging, remote): (&L, &S),
    token: &Witnessed,
    newest_staged: Option<Txid>,
    in_flight: &mut InFlight,
) -> Result<Option<Uploaded>, UploadFailed> {
    let key = token.key();
    let relist = |fault| fail(list_first(state), fault);
    let already = Uploaded {
        state: UploadState::At(Some(key.range().max())),
        did: Did::AlreadyPut(key),
    };
    if same_bytes(staging, remote, &key).await.map_err(relist)? {
        return Ok(Some(already));
    }
    // One read never condemns a file: the staged copy must verify on a read
    // of its own before the replica's object is judged against it.
    if staged_verified(staging, &key)
        .await
        .map_err(relist)?
        .is_none()
    {
        return Err(fail(state, Fault::StagedLost));
    }
    if remote_torn(remote, &key).await.map_err(relist)? {
        // The staged file replaces it, sent a slice a step from its first byte as any file is.
        in_flight.0 = Some(Sending::start(*token, WriteMode::Replace));
        return Ok(None);
    }
    // Both verify: a second compare decides, as the listing's read-back does.
    if same_bytes(staging, remote, &key).await.map_err(relist)? {
        return Ok(Some(already));
    }
    // The relist judges the refused key from a fresh listing and a byte compare.
    let (listed, _) = list_step(list_first(state), staging, remote, newest_staged).await?;
    // The bucket just refused the key as taken, yet the listing ends below it: a listing that
    // hides the newest file. Taken as it stands, it would plan the same key again, and the
    // refusal, the read-back and the relist would repeat with no wait inside one tick (issue
    // #540). The listing ends below a file the replica holds, which is a regression's
    // signal and wait.
    if listed.state.confirmed() < Some(key.range().max())
        && !matches!(listed.did, Did::Diverged { .. })
    {
        return Err(fail(list_first(state), Fault::RemoteRegressed));
    }
    Ok(Some(listed))
}

/// Why a second read of a staged file gave an upload no bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Unread {
    /// The read failed.
    Failed(StoreError),
    /// The bytes read are fewer or other than the ones first handed over.
    Changed,
}

impl Unread {
    /// The error the store that asked is answered with, and fails its upload with.
    fn error(&self) -> StoreError {
        match self {
            Self::Failed(err) => err.clone(),
            // What a store answers with when a response cannot be believed.
            Self::Changed => StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::InvalidData,
            },
        }
    }
}

/// The most second reads that gave nothing one send remembers: a store fails its upload at
/// the first it collects, and only the parts it has in flight read at once, so more than
/// this is a store that went on asking.
const UNREAD_KEPT: usize = 64;

/// The second reads of the file being sent that gave nothing, kept for the whole send. A
/// part is read again on the thread that sends it, ticks after the slice that handed it
/// over, so when the store fails the upload with one of these errors, whichever call
/// reports it, the step says a staged file that changed, or a staging read that failed, and
/// not the replica's fault.
#[derive(Clone, Debug, Default)]
struct Notes(Arc<Mutex<Vec<Unread>>>);

impl Notes {
    /// Remembers `unread`; the error the store that asked is answered with.
    fn note(&self, unread: Unread) -> StoreError {
        let err = unread.error();
        let mut kept = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if kept.len() < UNREAD_KEPT {
            kept.push(unread);
        }
        err
    }

    /// The fault of a write, a part or a commit that failed with `err`: the staged file's
    /// when `err` is what a second read of it answered the store with, else the replica's.
    fn fault<L: Role, S: Role>(&self, err: &StoreError) -> Fault {
        let unread = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match unread.iter().find(|unread| unread.error() == *err) {
            Some(Unread::Changed) => Fault::StagedLost,
            Some(Unread::Failed(read)) => L::fault(read),
            None => S::fault(err),
        }
    }

    /// Whether `err` is what a second read answered the store with.
    fn gave(&self, err: &StoreError) -> bool {
        let unread = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        unread.iter().any(|unread| unread.error() == *err)
    }
}

/// The staged file being sent, for the replica's store to open readers of ([`Reread`]). It
/// is made for one step; what it opens is the staging store's own held-open file
/// ([`ObjectStore::open`]), which borrows nothing and goes with the part it is for.
struct Staged<'a, L> {
    staging: &'a L,
    key: FileKey,
    notes: &'a Notes,
}

impl<L: Role> Reread for Staged<'_, L> {
    fn open(&self) -> BoxFuture<'_, Result<Box<dyn Again>, StoreError>> {
        Box::pin(async move {
            let unread = match self.staging.open(&self.key).await {
                Ok(Some(source)) => {
                    let reader: Box<dyn Again> = Box::new(Checked {
                        source,
                        notes: self.notes.clone(),
                    });
                    return Ok(reader);
                }
                // The send is written from a source only when the store held the file open
                // for its first read: a store with nothing to hold now is not that store.
                Ok(None) => Unread::Failed(StoreError::Refused {
                    op: StoreOp::Read,
                    status: 0,
                }),
                Err(err) => Unread::Failed(err),
            };
            Err(self.notes.note(unread))
        })
    }
}

/// One reader of the staged file being sent: the held-open file, answered only when the
/// bytes are the ones the send first read and verified ([`Seen`]).
struct Checked {
    source: Box<dyn Opened>,
    notes: Notes,
}

impl Again for Checked {
    fn read<'a>(
        &'a mut self,
        offset: u64,
        was: &'a Seen,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            // More than one read's bytes (or none) is not what this source can check in a
            // bounded read: the store asked for what the contract does not give.
            let unread = match u32::try_from(was.len()).ok().and_then(ReadLen::new) {
                None => Unread::Failed(StoreError::Refused {
                    op: StoreOp::Read,
                    status: 0,
                }),
                Some(len) => match self.source.read(offset, len).await {
                    Ok(bytes) if Seen::of(&bytes) == *was => return Ok(bytes),
                    Ok(_) => Unread::Changed,
                    Err(err) => Unread::Failed(err),
                },
            };
            Err(self.notes.note(unread))
        })
    }
}

/// Feeds a chunk to the decoder; `false` when it refuses the bytes.
fn feed_all(decoder: &mut Decoder, chunk: &[u8]) -> Result<bool, Fault> {
    let mut rest = chunk;
    while !rest.is_empty() {
        let Ok((used, _)) = decoder.feed(rest) else {
            return Ok(false);
        };
        crate::invariant!(used != 0, BugSite::DecoderStalled);
        rest = rest.get(used..).unwrap_or_default();
    }
    Ok(true)
}

/// Where a slice stopped.
enum Reached {
    /// The budget ran out before the staged file ended.
    Budget,
    /// The upload has a whole part waiting for one in flight to end: reading on would wait
    /// with it.
    Full,
    /// The staged file ended, verified whole, and every part of its upload was answered.
    End,
}

impl Sending {
    /// A send of `token`'s file from its first byte, written as `mode` says.
    fn start(token: Witnessed, mode: WriteMode) -> Box<Self> {
        Box::new(Self {
            token,
            mode,
            decoder: Decoder::new(),
            offset: 0,
            source: None,
            tally: Seeing::new(),
            held: Vec::new(),
            open: None,
            notes: Notes::default(),
            ended: false,
            waits: false,
        })
    }

    /// Reads the staged file on from where the last slice stopped, in chunks, decodes each
    /// and forwards it to an upload begun as soon as the header is known, until the file
    /// ends, `budget` bytes were read, or the upload's parts are in the way
    /// ([`Flight::Full`]: the step never waits for one). The upload counts the bytes it
    /// writes; its commit checks the store holds them ([`Writer`]).
    ///
    /// The file's end is verified here, on the read that reached it: the decoder's verdict
    /// on the whole file, and its length and trailer checksum against the token. A file
    /// replaced after it was witnessed is refused then and its upload aborted: one replaced
    /// before the first slice always, one replaced between two slices when the store reads
    /// it by range (a file held open is still the file that was opened, and goes up if that
    /// one is what was witnessed).
    async fn advance<L: Role, S: Role>(
        &mut self,
        staging: &L,
        remote: &S,
        budget: &mut u64,
    ) -> Result<Reached, PutError> {
        let key = &self.token.key();
        let again = Staged {
            staging,
            key: *key,
            notes: &self.notes,
        };
        while *budget > 0 && !self.ended {
            if let Some(open) = self.open.as_mut() {
                let flight = open
                    .flight(Rest::More)
                    .await
                    .map_err(|err| PutError::Before(self.notes.fault::<L, S>(&err)))?;
                if flight == Flight::Full {
                    return Ok(Reached::Full);
                }
            }
            // Never past the budget: a tick hands the replica no more than it may.
            let len = u32::try_from((*budget).min(CHUNK.get()))
                .ok()
                .and_then(ReadLen::new)
                .unwrap_or(CHUNK);
            // Opened once, at the file's first byte: a send that begins again is a new send.
            if self.offset == 0 {
                self.source = staging
                    .open(key)
                    .await
                    .map_err(|err| PutError::Before(L::fault(&err)))?;
            }
            let read = match self.source.as_mut() {
                Some(held) => held.read(self.offset, len).await,
                None => staging.read(key, self.offset, len).await,
            };
            let chunk = read.map_err(|err| PutError::Before(L::fault(&err)))?;
            self.tally.add(&chunk);
            if !feed_all(&mut self.decoder, &chunk).map_err(PutError::Before)? {
                return Err(PutError::Before(Fault::StagedLost));
            }
            let read = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            self.offset = self.offset.saturating_add(read);
            *budget = budget.saturating_sub(read);
            let header = self.decoder.header().copied();
            // A store that held the file open can open it again for each part; any other
            // is handed the bytes to keep.
            let again = self.source.is_some().then_some(&again);
            match (self.open.as_mut(), header) {
                (Some(open), _) => write_chunk::<L, S>(open, &chunk, again, &self.notes).await?,
                (None, Some(header)) => {
                    if header.txids() != key.range() {
                        return Err(PutError::Before(Fault::StagedLost));
                    }
                    // Stored before the first write so a failed write still aborts it.
                    let len = Some(self.token.len());
                    let begun =
                        Writer::begin(remote, key, header.fields().timestamp, self.mode, len)
                            .await
                            .map_err(|err| PutError::Before(S::fault(&err)))?;
                    let open = self.open.insert(begun.boxed());
                    self.held.extend_from_slice(&chunk);
                    let held = std::mem::take(&mut self.held);
                    write_chunk::<L, S>(open, &held, again, &self.notes).await?;
                }
                (None, None) => self.held.extend_from_slice(&chunk),
            }
            if read < len.get() {
                let decoder = std::mem::replace(&mut self.decoder, Decoder::new());
                let sound = decoder.finish().is_ok_and(|verified| {
                    verified.header().txids() == key.range()
                        && self
                            .token
                            .matches(self.tally, verified.trailer().file_checksum())
                });
                if !sound {
                    return Err(PutError::Before(Fault::StagedLost));
                }
                self.ended = true;
            }
        }
        if self.ended {
            return self.landed::<L, S>().await;
        }
        Ok(Reached::Budget)
    }

    /// The staged file ended and verified: tells the upload no byte follows, so it starts
    /// its last part, and says whether every part it started has been answered
    /// ([`Reached::End`]) or not ([`Reached::Full`]). The commit waits for that across
    /// ticks, never inside one.
    async fn landed<L: Role, S: Role>(&mut self) -> Result<Reached, PutError> {
        let Some(open) = self.open.as_mut() else {
            return Err(PutError::Before(BugSite::UploadNeverOpened.hit()));
        };
        let flight = open
            .flight(Rest::None)
            .await
            .map_err(|err| PutError::Before(self.notes.fault::<L, S>(&err)))?;
        Ok(if flight == Flight::Idle {
            Reached::End
        } else {
            Reached::Full
        })
    }

    /// Commits the upload of a file that ended, verified, with every part answered.
    ///
    /// A second read that gave nothing fails the commit before anything is published, so
    /// the state stands and nothing is deleted, whatever the store's error would mean
    /// coming from the replica.
    async fn commit<L: Role, S: Role>(self: Box<Self>, remote: &S) -> Result<FileInfo, PutError> {
        let Self {
            token, open, notes, ..
        } = *self;
        let key = token.key();
        let Some(open) = open else {
            return Err(PutError::Before(BugSite::UploadNeverOpened.hit()));
        };
        match open.commit(remote).await {
            Ok(published) if published.key() == key => Ok(published.info()),
            Ok(_) => Err(PutError::Commit(Fault::UploadAmbiguous)),
            Err(unpublished) if notes.gave(unpublished.error()) => {
                Err(PutError::Before(notes.fault::<L, S>(unpublished.error())))
            }
            Err(Unpublished::Taken(_)) => Err(PutError::Taken),
            Err(Unpublished::Rejected(err)) => Err(PutError::Rejected(S::fault(&err))),
            Err(Unpublished::Failed(err)) => Err(PutError::Commit(S::fault(&err))),
        }
    }
}

/// Hands the upload a chunk: one it may let go of and read back from the staged file when
/// `again` is given, else one it keeps.
async fn write_chunk<L: Role, S: Role>(
    upload: &mut Open,
    chunk: &[u8],
    again: Option<&Staged<'_, L>>,
    notes: &Notes,
) -> Result<(), PutError> {
    let wrote = match again {
        Some(again) => upload.write_from(chunk, again).await,
        None => upload.write(chunk).await,
    };
    wrote.map_err(|err| PutError::Before(notes.fault::<L, S>(&err)))
}

#[cfg(test)]
mod tests;

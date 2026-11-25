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
//! only, so steady state makes no list and no read call. A listing is also
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
//!   `Fault::RemoteAheadOfLocal` when the replica is ahead).
//!
//! A failure before the commit leaves the state alone (nothing could have been
//! published). A failure of the commit itself gives [`UploadState::ListFirst`],
//! except a checksum mismatch: the store may hold wrong bytes under the key, so
//! the state stays, the key is deleted (best effort, so readers do not see the
//! bad position while the retry is pending) and the same key is written again.
//! Core runs no timer: the actor steps [`upload_next`] up to its per-tick limit.

use std::num::NonZeroU32;

use lsr_ltx::{Decoder, Header, Txid, TxidRange, Verified};

use crate::fault::role::{Replica, Role, Staging};
use crate::fault::{BugSite, Fault};
use crate::inventory::{Inventory, InventoryError};
use crate::layout::{FileInfo, FileKey, Level};
use crate::ltx_read::{verified, verifies};
use crate::publish::{self, Unpublished, Writer};
use crate::store::{ListAt, ObjectStore, PageLimit, ReadLen, WriteMode};
use crate::witness::{ByteTally, Witnessed, Witnesses};

/// How much of a staged file is read at once.
const CHUNK: ReadLen = ReadLen::or_chunk(256 * 1024);

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

/// Whether a level-0 upload asks the bucket to refuse a key that is taken (D79). A store that
/// cannot make that check fails the write with `CreateOnlyUnsupported`; the actor then raises
/// the cause and uploads [`Guard::Unguarded`] for the rest of the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum Guard {
    /// A create-only write: the bucket refuses a taken key ([`WriteMode::New`]).
    #[default]
    BucketRefuses,
    /// A replacing write, as Go's are ([`WriteMode::Replace`]): nothing refuses another
    /// writer's file under the key, which the standing cause reports.
    Unguarded,
}

impl Guard {
    /// The write mode a level-0 upload takes.
    const fn mode(self) -> WriteMode {
        match self {
            Self::BucketRefuses => WriteMode::New,
            Self::Unguarded => WriteMode::Replace,
        }
    }
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
    /// The bucket refused the upload of this key because it already held the
    /// same bytes, read back: this engine's earlier commit, whose answer was
    /// lost or whose listing was stale, is confirmed.
    AlreadyPut(FileKey),
    /// The replica was listed and the staged files from `from` on cannot
    /// continue it, so they must go and new content takes a transaction above
    /// `confirmed`: the replica's newest level-0 object verifies and differs
    /// from the staged file under the same key (`from` is that key's
    /// transaction). It stands and is confirmed, since it may already be
    /// compacted into higher levels and writing over it would give one
    /// transaction two contents. Whether the staged files after it continue
    /// the replica is not a listing's to judge: each needs its witness
    /// (`crate::witness`), which a boot takes from the live WAL.
    Diverged {
        /// The newest transaction the replica holds, now confirmed.
        confirmed: Txid,
        /// The first staged transaction that cannot follow it.
        from: Txid,
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

fn staged_key(txid: Txid) -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(txid))
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
                    UploadPlan::Upload(staged_key(next))
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
    upload_next_in(
        state,
        &Staging(staging),
        &Replica(remote),
        newest_staged,
        (witnesses, Guard::BucketRefuses),
    )
    .await
}

/// [`upload_next`] with each store as its role, and level-0 writes as `guard` says.
pub(crate) async fn upload_next_in<L: Role, S: Role>(
    state: UploadState,
    staging: &L,
    remote: &S,
    newest_staged: Option<Txid>,
    (witnesses, guard): (&Witnesses, Guard),
) -> Result<Uploaded, UploadFailed> {
    match plan_upload(&state, newest_staged) {
        UploadPlan::Idle => Ok(Uploaded {
            state,
            did: Did::Nothing,
        }),
        UploadPlan::List => list_step(state, staging, remote, newest_staged).await,
        UploadPlan::Upload(key) => match witnesses.get(&key) {
            Some(token) => put_step(state, (staging, remote), token, newest_staged, guard).await,
            // No witness: the file is refused unread, as a lost one is.
            None => Err(fail(state, Fault::StagedLost)),
        },
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

async fn list_step<L: Role, S: Role>(
    state: UploadState,
    staging: &L,
    remote: &S,
    newest_staged: Option<Txid>,
) -> Result<Uploaded, UploadFailed> {
    let inventory = Inventory::load(remote)
        .await
        .map_err(|err| fail(state, inventory_fault::<S>(&err)))?;
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
                    return Ok(diverged(tail, tail));
                }
            }
        }
        _ => newest,
    };
    Ok(Uploaded {
        state: UploadState::At(known),
        did: Did::Listed { newest: known },
    })
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
    let key = staged_key(txid);
    inventory
        .level(Level::ZERO)
        .iter()
        .find(|info| info.key == key)
        .map(|info| info.size)
}

fn diverged(confirmed: Txid, from: Txid) -> Uploaded {
    Uploaded {
        state: UploadState::At(Some(confirmed)),
        did: Did::Diverged { confirmed, from },
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
/// the same key; `size` is the replica object's listed size. Only the newest key is read back: an older one was confirmed
/// by its own commit in an earlier run, and reading every key at boot would be
/// a whole replica scan.
async fn judge_tail<L: Role, S: Role>(
    staging: &L,
    remote: &S,
    tail: Txid,
    size: u64,
) -> Result<TailVerdict, Fault> {
    let key = staged_key(tail);
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
    if !staged_verifies(staging, &key).await? && !staged_verifies(staging, &key).await? {
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
async fn same_bytes<L: Role, S: Role>(
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
    verifies(remote, key, CHUNK, S::fault).await
}

/// Whether the replica's object `key` fails verification on two reads in a
/// row. Writing over it is decided by this, never by one read: a read that
/// came back wrong says nothing about the stored bytes, and a verifying
/// object with other bytes is never written over.
async fn remote_torn<S: Role>(remote: &S, key: &FileKey) -> Result<bool, Fault> {
    Ok(!remote_verifies(remote, key).await? && !remote_verifies(remote, key).await?)
}

/// Whether the staged file `key` verifies whole and names exactly `key`.
/// A missing or unusable one does not; any other store error is the fault.
async fn staged_verifies<L: Role>(staging: &L, key: &FileKey) -> Result<bool, Fault> {
    staged_verified(staging, key)
        .await
        .map(|summary| summary.is_some())
}

/// What verified of the staged file `key`, as [`staged_verifies`] reads it.
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
pub(crate) async fn drop_staged_from<L: Role>(staging: &L, from: Txid) -> Result<(), Fault> {
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
            publish::remove_staged(staging, &[info.key])
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
    /// The store says the bytes it took are not the bytes sent. A
    /// create-only write's object is its own and goes ([`publish::Rejected`]);
    /// either way the key is written again without asking.
    Rejected(Fault, Option<publish::Rejected>),
}

async fn put_step<L: Role, S: Role>(
    state: UploadState,
    (staging, remote): (&L, &S),
    token: &Witnessed,
    newest_staged: Option<Txid>,
    guard: Guard,
) -> Result<Uploaded, UploadFailed> {
    let key = token.key();
    match put_staged(staging, remote, token, guard.mode()).await {
        Err(PutError::Taken) => taken(state, staging, remote, token, newest_staged).await,
        outcome => settle_put(state, remote, key, outcome).await,
    }
}

/// The state and step a put's outcome gives (a refusal is [`taken`]'s).
async fn settle_put<S: ObjectStore>(
    state: UploadState,
    remote: &S,
    key: FileKey,
    outcome: Result<FileInfo, PutError>,
) -> Result<Uploaded, UploadFailed> {
    match outcome {
        Ok(info) => Ok(put_done(key, info)),
        Err(PutError::Before(fault)) => Err(fail(state, fault)),
        Err(PutError::Rejected(fault, own)) => {
            // Only a rejected create-only write hands back its object (the
            // bucket refuses a taken key before it takes any bytes), so only
            // that object goes. Best effort: the retry replaces the key either
            // way, and a boot listing reads it back, so a failed delete costs
            // only visibility. A write that may have replaced an object
            // deletes nothing.
            if let Some(own) = own {
                drop(publish::remove_rejected(remote, own).await);
            }
            Err(fail(state, fault))
        }
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
    staging: &L,
    remote: &S,
    token: &Witnessed,
    newest_staged: Option<Txid>,
) -> Result<Uploaded, UploadFailed> {
    let key = token.key();
    let relist = |fault| fail(list_first(state), fault);
    let already = Uploaded {
        state: UploadState::At(Some(key.range().max())),
        did: Did::AlreadyPut(key),
    };
    if same_bytes(staging, remote, &key).await.map_err(relist)? {
        return Ok(already);
    }
    // One read never condemns a file: the staged copy must verify on a read
    // of its own before the replica's object is judged against it.
    if !staged_verifies(staging, &key).await.map_err(relist)? {
        return Err(fail(state, Fault::StagedLost));
    }
    if remote_torn(remote, &key).await.map_err(relist)? {
        let outcome = put_staged(staging, remote, token, WriteMode::Replace).await;
        return settle_put(state, remote, key, outcome).await;
    }
    // Both verify: a second compare decides, as the listing's read-back does.
    if same_bytes(staging, remote, &key).await.map_err(relist)? {
        return Ok(already);
    }
    // The relist judges the refused key from a fresh listing and a byte compare.
    list_step(list_first(state), staging, remote, newest_staged).await
}

async fn put_staged<L: Role, S: Role>(
    staging: &L,
    remote: &S,
    token: &Witnessed,
    mode: WriteMode,
) -> Result<FileInfo, PutError> {
    let key = &token.key();
    let mut upload = None;
    if let Err(err) = copy_verified(staging, remote, token, mode, &mut upload).await {
        if let Some(open) = upload {
            // Abort only releases scratch; a failure leaves nothing visible either way.
            drop(open.abort().await);
        }
        return Err(err);
    }
    let Some(open) = upload else {
        return Err(PutError::Before(BugSite::UploadNeverOpened.hit()));
    };
    match open.commit().await {
        Ok(published) if published.key() == *key => Ok(published.info()),
        Ok(_) => Err(PutError::Commit(Fault::UploadAmbiguous)),
        Err(Unpublished::Taken(_)) => Err(PutError::Taken),
        Err(Unpublished::Rejected(err, own)) => Err(PutError::Rejected(S::fault(&err), own)),
        Err(Unpublished::Failed(err)) => Err(PutError::Commit(S::fault(&err))),
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

fn names_key(header: &Header, key: &FileKey) -> Result<(), PutError> {
    if header.txids() == key.range() {
        Ok(())
    } else {
        Err(PutError::Before(Fault::StagedLost))
    }
}

/// Reads the staged file in chunks, decodes each, and forwards it to an upload
/// begun as soon as the header is known. Returns once the decoder has
/// verified the whole file and its length and trailer checksum, on this read,
/// are the ones `token` was minted for: a file replaced after it was
/// witnessed is refused before the commit. The upload counts the bytes it
/// writes; its commit checks the store holds them ([`Writer`]).
async fn copy_verified<'s, L: Role, S: Role>(
    staging: &L,
    remote: &'s S,
    token: &Witnessed,
    mode: WriteMode,
    upload: &mut Option<Writer<'s, S>>,
) -> Result<(), PutError> {
    let key = &token.key();
    let len = CHUNK;
    let mut decoder = Decoder::new();
    let mut held = Vec::new();
    let mut offset = 0_u64;
    let mut bytes = ByteTally::new();
    loop {
        let chunk = staging
            .read(key, offset, len)
            .await
            .map_err(|err| PutError::Before(L::fault(&err)))?;
        bytes.add(&chunk);
        if !feed_all(&mut decoder, &chunk).map_err(PutError::Before)? {
            return Err(PutError::Before(Fault::StagedLost));
        }
        let read = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        offset = offset.saturating_add(read);
        let header = decoder.header().copied();
        match (upload.as_mut(), header) {
            (Some(open), _) => write_chunk(open, &chunk).await?,
            (None, Some(header)) => {
                names_key(&header, key)?;
                // Stored before the first write so a failed write still aborts it.
                let open = upload.insert(begin(remote, key, &header, mode).await?);
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
        .map_err(|_undecodable| PutError::Before(Fault::StagedLost))?;
    names_key(verified.header(), key)?;
    if !token.matches(bytes, verified.trailer().file_checksum()) {
        return Err(PutError::Before(Fault::StagedLost));
    }
    Ok(())
}

async fn begin<'s, S: Role>(
    remote: &'s S,
    key: &FileKey,
    header: &Header,
    mode: WriteMode,
) -> Result<Writer<'s, S>, PutError> {
    Writer::begin(remote, key, header.fields().timestamp, mode)
        .await
        .map_err(|err| PutError::Before(S::fault(&err)))
}

async fn write_chunk<S: Role>(upload: &mut Writer<'_, S>, chunk: &[u8]) -> Result<(), PutError> {
    upload
        .write(chunk)
        .await
        .map_err(|err| PutError::Before(S::fault(&err)))
}

#[cfg(test)]
mod tests;

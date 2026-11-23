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
//! A listing's end is not trusted on its own either. A store may list behind
//! its writes (a cache in front of a listing, an eventually consistent
//! index) while its reads are exact, so a key a listing leaves out may still
//! be there. Every listing here is followed by reads of the keys past its
//! newest that a reader can name without a listing (`crate::probe`, the same
//! reads a restore of the newest state makes), in order until [`PROBE_GAP`]
//! transactions in a row are missing ([`probe_past`], at most [`PROBE_CAP`]
//! transactions, [`fresh_probe_reads`] on a fresh listing; through the newest
//! staged transaction at least): what they find
//! is the replica's newest. When a confirmed transaction is past the listing,
//! the reads start at its key, so a regression is reported only when a read,
//! not a listing, finds the confirmed key gone. A file a listing shows past
//! the local position is another writer's only when a read finds it, since a
//! listing that lags a delete shows a file that is gone. The reads find the
//! tail because nothing this engine runs deletes the replica's newest level-0 file
//! (retention keeps each level's last file, see `maintain::retain`). What
//! they cannot find is a level 1 to 8 file whose level-0 inputs were all
//! deleted by someone else (an operator removing another writer's newest
//! file after maintenance merged it): its start is not knowable without a
//! listing, so a lagging listing that hides it is taken as the replica's end.
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
//! snapshots in its place. At boot, [`sweep_staged`] applies the same rule
//! before anything is uploaded.
//!
//! Every upload here is of a key the engine takes to be free (the confirmed
//! transaction plus one), so it is a [`WriteMode::New`] write: the bucket, not
//! a listing, refuses a key that is taken, which a stale listing or another
//! writer cannot hide. A refused commit reads the object back:
//! - the same bytes are this engine's own earlier commit whose answer was
//!   lost, or that a stale listing hid: confirmed, as [`Did::AlreadyPut`];
//! - a staged copy that does not verify on a read of its own is
//!   `Fault::StagedLost`, and nothing is judged from it (one read never
//!   condemns a file; the next tick puts it again);
//! - an object that is not a whole LTX file naming the key is replaced, as a
//!   listing's read-back replaces one;
//! - a verifying object with other bytes, on a second compare too, is another
//!   writer's or an earlier run's: the replica is listed at once and judged by
//!   the listing's rules above ([`Did::Diverged`], or
//!   `Fault::RemoteAheadOfLocal` when the replica is ahead); the relist's
//!   probe reads the refused key, so a listing too stale to show the object
//!   cannot hide it.
//!
//! A failure before the commit leaves the state alone (nothing could have been
//! published). A failure of the commit itself gives [`UploadState::ListFirst`],
//! except a checksum mismatch: the store may hold wrong bytes under the key, so
//! the state stays, the key is deleted (best effort, so readers do not see the
//! bad position while the retry is pending) and the same key is written again.
//! Core runs no timer: the actor steps [`upload_next`] up to its per-tick limit.

use std::num::NonZeroU32;

use lsr_ltx::{Decoder, Header, Txid, TxidRange};

use crate::fault::{BugSite, Fault};
use crate::inventory::{Inventory, InventoryError};
use crate::layout::{FileInfo, FileKey, Level};
use crate::ltx_read::verifies;
use crate::probe;
use crate::publish::{self, Unpublished, Writer};
use crate::store::{ListAt, ObjectStore, PageLimit, ReadLen, WriteMode};
use crate::witness::{ByteTally, Witnessed, Witnesses};

/// How much of a staged file is read at once.
const CHUNK: ReadLen = ReadLen::or_chunk(256 * 1024);

/// The most transactions past a listing's newest that one probe looks for
/// ([`probe_past`]). A listing that hides more uploads than this is far
/// outside what an S3-compatible store does; past it, the bucket's refusal of
/// a taken key ([`taken`]) still stops a second upload under the same key.
pub const PROBE_CAP: u32 = 64;

/// The most missing transactions in a row a probe reads past before it
/// stops. Keys past a replica's newest are never taken, but one below it can
/// be free: another writer's file an operator removed leaves a gap under the
/// files uploaded after it.
pub const PROBE_GAP: u32 = 3;

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
    match plan_upload(&state, newest_staged) {
        UploadPlan::Idle => Ok(Uploaded {
            state,
            did: Did::Nothing,
        }),
        UploadPlan::List => list_step(state, staging, remote, newest_staged).await,
        UploadPlan::Upload(key) => match witnesses.get(&key) {
            Some(token) => put_step(state, staging, remote, token, newest_staged).await,
            // No witness: the file is refused unread, as a lost one is.
            None => Err(fail(state, Fault::StagedLost)),
        },
    }
}

fn fail(state: UploadState, fault: Fault) -> UploadFailed {
    UploadFailed { state, fault }
}

/// A listing failure as the fault the uploader and the actor route.
pub(crate) fn inventory_fault(err: &InventoryError) -> Fault {
    match err {
        InventoryError::Store(err) => Fault::from_remote(err),
        InventoryError::ConflictingDuplicate { .. } | InventoryError::ListingStalled(_) => {
            Fault::RemoteCorrupt
        }
    }
}

async fn list_step<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    newest_staged: Option<Txid>,
) -> Result<Uploaded, UploadFailed> {
    let inventory = Inventory::load(remote)
        .await
        .map_err(|err| fail(state, inventory_fault(&err)))?;
    let confirmed = state.confirmed();
    let probed = look_past(&inventory, staging, remote, confirmed, newest_staged)
        .await
        .map_err(|fault| fail(state, fault))?;
    let seen = Seen {
        inventory: &inventory,
        probed,
    };
    let newest = seen.newest();
    if newest > newest_staged {
        return Err(fail(state, Fault::RemoteAheadOfLocal));
    }
    if newest < confirmed {
        return Err(fail(state, Fault::RemoteRegressed));
    }
    let known = match newest {
        Some(tail) if newest > confirmed => {
            let verdict = match seen.level_zero(tail) {
                Some(size) => judge_tail(staging, remote, tail, size)
                    .await
                    .map_err(|fault| fail(state, fault))?,
                // The newest transaction is in a higher level only: nothing
                // at level 0 to read back.
                None => TailVerdict::Sound,
            };
            match verdict {
                TailVerdict::Sound => newest,
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

/// What a listing showed, with the keys reads found past it.
struct Seen<'a> {
    inventory: &'a Inventory,
    /// The keys found by reads, ascending by transaction.
    probed: Vec<FileKey>,
}

impl Seen<'_> {
    fn newest(&self) -> Option<Txid> {
        let probed = self.probed.last().map(|key| key.range().max());
        self.inventory.newest().max(probed)
    }

    /// Whether the level-0 key of `txid` holds an object, and its listed
    /// size when a listing showed it (a probe learns no size).
    fn level_zero(&self, txid: Txid) -> Option<Option<u64>> {
        let key = staged_key(txid);
        if self.probed.contains(&key) {
            return Some(None);
        }
        self.inventory
            .level(Level::ZERO)
            .iter()
            .find(|info| info.key == key)
            .map(|info| Some(info.size))
    }
}

/// Reads what a listing cannot prove. A file it shows past the local
/// position is another writer's only when a read finds it (a listing that
/// lags a delete shows a file that is gone: list again); otherwise the
/// keys past it are probed ([`probe_past`]), through at least the newest
/// staged transaction, which this engine may have uploaded already.
async fn look_past<L: ObjectStore, S: ObjectStore>(
    inventory: &Inventory,
    staging: &L,
    remote: &S,
    confirmed: Option<Txid>,
    newest_staged: Option<Txid>,
) -> Result<Vec<FileKey>, Fault> {
    if inventory.newest() > newest_staged {
        return Err(if past_known(remote, inventory, newest_staged).await? {
            Fault::RemoteAheadOfLocal
        } else {
            Fault::RemoteObjectMissing
        });
    }
    // The probe looks at no more than PROBE_CAP transactions, so no more staged files than
    // that bound it.
    let cap = usize::try_from(PROBE_CAP).unwrap_or(usize::MAX);
    let staged = staged_from(staging, inventory.newest(), cap).await?;
    let through = staged.last().map(|info| info.key.range().max());
    probe_past(remote, inventory.newest(), confirmed, through).await
}

/// Whether a read finds any of the listed files that reach past `known`,
/// newest first, at most [`PROBE_CAP`] of them. A listing that lags a delete
/// shows a file that is gone; only a file that is there proves another
/// writer.
///
/// # Errors
/// The replica's fault when a read fails other than by a missing key.
pub(crate) async fn past_known<S: ObjectStore>(
    remote: &S,
    inventory: &Inventory,
    known: Option<Txid>,
) -> Result<bool, Fault> {
    let mut past: Vec<&FileInfo> = inventory
        .files()
        .iter()
        .filter(|info| Some(info.key.range().max()) > known)
        .collect();
    past.sort_by_key(|info| std::cmp::Reverse(info.key.range().max()));
    let cap = usize::try_from(PROBE_CAP).unwrap_or(usize::MAX);
    for info in past.into_iter().take(cap) {
        if exists(remote, &info.key).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The keys past a listing that reads find, ascending. S3 and its peers
/// read their own writes at once while a listing may lag behind them, so a
/// listing's absence of a key is not evidence; a read's is.
///
/// Each transaction from the one after `listed` (from `confirmed`, when that
/// is later, so that a relist proves what was confirmed is still there
/// before a regression is reported) is looked for under the keys a reader
/// can name without a listing (`crate::probe`): its level-0 file, a level-0
/// file from the first transaction, and the snapshot that ends at it. A
/// higher-level file has a start no read can guess. The reads go on through
/// `through` (the newest staged transaction: a gap below it does not end
/// them, since this engine may have uploaded anything it staged), then stop
/// once [`PROBE_GAP`] transactions in a row past the last one found are
/// missing, and never look at more than [`PROBE_CAP`] transactions. A fresh
/// listing with nothing staged past it costs [`fresh_probe_reads`] reads, of
/// keys that are not there (three per transaction).
///
/// # Errors
/// The replica's fault when a read fails other than by a missing key.
pub(crate) async fn probe_past<S: ObjectStore>(
    remote: &S,
    listed: Option<Txid>,
    confirmed: Option<Txid>,
    through: Option<Txid>,
) -> Result<Vec<FileKey>, Fault> {
    let first = match (listed, confirmed) {
        (listed, Some(confirmed)) if listed < Some(confirmed) => Some(confirmed),
        (listed, _) => listed.map_or(Some(Txid::MIN), Txid::next),
    };
    let mut found = Vec::new();
    let (mut at, mut missed) = (first, 0_u32);
    for _ in 0..PROBE_CAP {
        let Some(txid) = at.filter(|txid| missed <= PROBE_GAP || Some(*txid) <= through) else {
            break;
        };
        match holder(remote, txid).await? {
            Some(key) => {
                found.push(key);
                missed = 0;
            }
            None => missed = missed.saturating_add(1),
        }
        at = txid.next();
    }
    Ok(found)
}

/// The reads a probe past a listing that ends at `listed` makes when it finds nothing: the
/// keys named for each of the [`PROBE_GAP`] + 1 transactions after it (`crate::probe`).
#[must_use]
pub fn fresh_probe_reads(listed: Option<Txid>) -> u64 {
    let mut reads = 0_u64;
    let mut at = listed.map_or(Some(Txid::MIN), Txid::next);
    for _ in 0..=PROBE_GAP {
        let Some(txid) = at else { break };
        reads = reads.saturating_add(probe::reads_per_transaction(txid));
        at = txid.next();
    }
    reads
}

/// The first key named for `txid` that a read finds (`crate::probe`).
async fn holder<S: ObjectStore>(remote: &S, txid: Txid) -> Result<Option<FileKey>, Fault> {
    probe::holder(remote, txid)
        .await
        .map_err(|err| Fault::from_remote(&err))
}

/// Whether the replica holds an object under `key`, by a read: a listing may
/// show a key that is gone or leave out one that is there, a read does not.
///
/// # Errors
/// The replica's fault when the read fails other than by a missing key.
pub(crate) async fn exists<S: ObjectStore>(remote: &S, key: &FileKey) -> Result<bool, Fault> {
    probe::exists(remote, key)
        .await
        .map_err(|err| Fault::from_remote(&err))
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
/// the same key; `size` is the replica object's listed size, when a listing
/// showed it. Only the newest key is read back: an older one was confirmed
/// by its own commit in an earlier run, and reading every key at boot would be
/// a whole replica scan.
async fn judge_tail<L: ObjectStore, S: ObjectStore>(
    staging: &L,
    remote: &S,
    tail: Txid,
    size: Option<u64>,
) -> Result<TailVerdict, Fault> {
    let key = staged_key(tail);
    let after = tail.prev().map(TxidRange::single);
    let limit = PageLimit::ONE;
    let page = staging
        .list(Level::ZERO, after, limit)
        .await
        .map_err(|err| Fault::from_staging(&err))?;
    let Some(staged) = page.files.first().filter(|staged| staged.key == key) else {
        return remote_decodes(remote, &key)
            .await
            .map(|()| TailVerdict::Sound);
    };
    if size.is_none_or(|size| size == staged.size) && same_bytes(staging, remote, &key).await? {
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
    Ok(if same_bytes(staging, remote, &key).await? {
        TailVerdict::Sound
    } else {
        TailVerdict::Diverged
    })
}

/// Streams both copies and compares them chunk by chunk.
async fn same_bytes<L: ObjectStore, S: ObjectStore>(
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
            .map_err(|err| Fault::from_staging(&err))?;
        let theirs = remote
            .read(key, offset, len)
            .await
            .map_err(|err| Fault::from_remote(&err))?;
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
async fn remote_decodes<S: ObjectStore>(remote: &S, key: &FileKey) -> Result<(), Fault> {
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
pub(crate) async fn remote_verifies<S: ObjectStore>(
    remote: &S,
    key: &FileKey,
) -> Result<bool, Fault> {
    verifies(remote, key, CHUNK, Fault::from_remote).await
}

/// Whether the replica's object `key` fails verification on two reads in a
/// row. Writing over it is decided by this, never by one read: a read that
/// came back wrong says nothing about the stored bytes, and a verifying
/// object with other bytes is never written over.
async fn remote_torn<S: ObjectStore>(remote: &S, key: &FileKey) -> Result<bool, Fault> {
    Ok(!remote_verifies(remote, key).await? && !remote_verifies(remote, key).await?)
}

/// Whether the staged file `key` verifies whole and names exactly `key`.
/// A missing or unusable one does not; any other store error is the fault.
async fn staged_verifies<L: ObjectStore>(staging: &L, key: &FileKey) -> Result<bool, Fault> {
    match verifies(staging, key, CHUNK, Fault::from_staging).await {
        Ok(verifies) => Ok(verifies),
        Err(Fault::StagedLost) => Ok(false),
        Err(fault) => Err(fault),
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
    pub(crate) async fn next<L: ObjectStore>(
        &mut self,
        staging: &L,
    ) -> Result<Option<Vec<FileInfo>>, Fault> {
        let Some(at) = self.at else {
            return Ok(None);
        };
        let page = staging
            .list(at.level, at.after, PageLimit::MAX)
            .await
            .map_err(|err| Fault::from_staging(&err))?;
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
pub(crate) async fn staged_from<L: ObjectStore>(
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
pub(crate) async fn drop_staged_from<L: ObjectStore>(staging: &L, from: Txid) -> Result<(), Fault> {
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
                .map_err(|err| Fault::from_staging(&err))?;
        }
    }
}

/// What a sweep of the staged files found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Swept {
    /// Every staged file the replica may still need verifies.
    Clean,
    /// This file failed verification on its first failed read: nothing was
    /// deleted, since one read says nothing about the stored bytes.
    Suspect(FileKey),
    /// The suspect failed again: it and every staged file after it must go,
    /// from this transaction on ([`drop_staged_from`]). The caller deletes
    /// them, once what they vouched for is recorded.
    Condemned(Txid),
}

/// Verifies every staged level-0 file the replica may still need: from
/// `confirmed`, the replica's newest transaction, on (everything when the
/// replica is empty). The first that does not verify is condemned, with every
/// one after it, only when it is `suspect`, the file an earlier tick's read
/// already failed on; otherwise it is reported as the new suspect. The sweep
/// deletes nothing itself.
///
/// # Errors
/// The staging store's fault when it cannot list, read or delete.
pub(crate) async fn sweep_staged<L: ObjectStore>(
    staging: &L,
    confirmed: Option<Txid>,
    suspect: Option<FileKey>,
) -> Result<Swept, Fault> {
    let mut pages = StagedPages::from(confirmed);
    while let Some(page) = pages.next(staging).await? {
        for info in page {
            if !staged_verifies(staging, &info.key).await? {
                if suspect != Some(info.key) {
                    return Ok(Swept::Suspect(info.key));
                }
                return Ok(Swept::Condemned(info.key.range().min()));
            }
        }
    }
    Ok(Swept::Clean)
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

async fn put_step<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    token: &Witnessed,
    newest_staged: Option<Txid>,
) -> Result<Uploaded, UploadFailed> {
    let key = token.key();
    match put_staged(staging, remote, token, WriteMode::New).await {
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
async fn taken<L: ObjectStore, S: ObjectStore>(
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
    // The relist reads the refused key whatever its listing shows (the
    // probe starts at the key after what was confirmed, which is this one).
    list_step(list_first(state), staging, remote, newest_staged).await
}

async fn put_staged<L: ObjectStore, S: ObjectStore>(
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
        Err(Unpublished::Rejected(err, own)) => {
            Err(PutError::Rejected(Fault::from_remote(&err), own))
        }
        Err(Unpublished::Failed(err)) => Err(PutError::Commit(Fault::from_remote(&err))),
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
async fn copy_verified<'s, L: ObjectStore, S: ObjectStore>(
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
            .map_err(|err| PutError::Before(Fault::from_staging(&err)))?;
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

async fn begin<'s, S: ObjectStore>(
    remote: &'s S,
    key: &FileKey,
    header: &Header,
    mode: WriteMode,
) -> Result<Writer<'s, S>, PutError> {
    Writer::begin(remote, key, header.fields().timestamp, mode)
        .await
        .map_err(|err| PutError::Before(Fault::from_remote(&err)))
}

async fn write_chunk<S: ObjectStore>(
    upload: &mut Writer<'_, S>,
    chunk: &[u8],
) -> Result<(), PutError> {
    upload
        .write(chunk)
        .await
        .map_err(|err| PutError::Before(Fault::from_remote(&err)))
}

#[cfg(test)]
mod tests;

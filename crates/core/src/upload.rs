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
use crate::store::{ListAt, ObjectStore, PageLimit, ReadLen, Upload};
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
            Some(token) => put_step(state, staging, remote, token).await,
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
            mark!(Boot17);
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
    let newest = inventory.newest();
    let confirmed = state.confirmed();
    if newest > newest_staged {
        mark!(Up10);
        return Err(fail(state, Fault::RemoteAheadOfLocal));
    }
    if newest < confirmed {
        mark!(Up11);
        return Err(fail(state, Fault::RemoteRegressed));
    }
    let known = match newest {
        Some(tail) if newest > confirmed => {
            let verdict = judge_tail(&inventory, staging, remote, tail)
                .await
                .map_err(|fault| fail(state, fault))?;
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
/// the same key. Only the newest key is read back: an older one was confirmed
/// by its own commit in an earlier run, and reading every key at boot would be
/// a whole replica scan.
async fn judge_tail<L: ObjectStore, S: ObjectStore>(
    inventory: &Inventory,
    staging: &L,
    remote: &S,
    tail: Txid,
) -> Result<TailVerdict, Fault> {
    let key = staged_key(tail);
    let Some(listed) = inventory
        .level(Level::ZERO)
        .iter()
        .find(|info| info.key == key)
    else {
        return Ok(TailVerdict::Sound);
    };
    let after = tail.prev().map(TxidRange::single);
    let limit = PageLimit::ONE;
    let page = staging
        .list(Level::ZERO, after, limit)
        .await
        .map_err(|err| Fault::from_staging(&err))?;
    match page.files.first() {
        Some(staged) if staged.key == key => {
            if staged.size == listed.size && same_bytes(staging, remote, &key).await? {
                return Ok(TailVerdict::Sound);
            }
            if staged_verifies(staging, &key).await? {
                if !remote_verifies(remote, &key).await? {
                    mark!(Boot16);
                    return Ok(TailVerdict::Replace);
                }
                // Both copies verify on their own reads, so the first compare
                // read something wrong or they differ: a second compare
                // decides, as one read never condemns a file.
                return Ok(if same_bytes(staging, remote, &key).await? {
                    TailVerdict::Sound
                } else {
                    TailVerdict::Diverged
                });
            }
            // A torn staged copy says nothing about the replica's object.
            remote_decodes(remote, &key)
                .await
                .map(|()| TailVerdict::Sound)
        }
        _ => remote_decodes(remote, &key)
            .await
            .map(|()| TailVerdict::Sound),
    }
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
        mark!(Boot14);
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
            staging
                .delete(&[info.key])
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
    /// The suspect failed again: it and every staged file after it were
    /// deleted, from this transaction on.
    Dropped(Txid),
}

/// Verifies every staged level-0 file the replica may still need: from
/// `confirmed`, the replica's newest transaction, on (everything when the
/// replica is empty). The first that does not verify is deleted, with every
/// one after it ([`drop_staged_from`]), only when it is `suspect`, the file an
/// earlier tick's read already failed on; otherwise it is reported as the new
/// suspect and nothing is deleted.
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
                    mark!(Boot5);
                    return Ok(Swept::Suspect(info.key));
                }
                mark!(Boot5);
                let from = info.key.range().min();
                drop_staged_from(staging, from).await?;
                return Ok(Swept::Dropped(from));
            }
        }
    }
    Ok(Swept::Clean)
}

/// Why a copy did not commit, and so what the state becomes.
enum PutError {
    /// Nothing can have been published.
    Before(Fault),
    /// The commit failed or its answer cannot be believed.
    Commit(Fault),
    /// The store says the bytes it took are not the bytes sent: the key is
    /// deleted and written again without asking.
    Rejected(Fault),
}

async fn put_step<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    staging: &L,
    remote: &S,
    token: &Witnessed,
) -> Result<Uploaded, UploadFailed> {
    let key = token.key();
    match put_staged(staging, remote, token).await {
        Ok(info) => Ok(Uploaded {
            state: UploadState::At(Some(key.range().max())),
            did: Did::Put(info),
        }),
        Err(PutError::Before(fault)) => Err(fail(state, fault)),
        Err(PutError::Rejected(fault)) => {
            // Best effort: the retry replaces the key either way, and a boot
            // listing reads it back, so a failed delete costs only visibility.
            mark!(Up8);
            drop(remote.delete(&[key]).await);
            Err(fail(state, fault))
        }
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
    token: &Witnessed,
) -> Result<FileInfo, PutError> {
    let key = &token.key();
    let mut upload = None;
    let wrote = match copy_verified(staging, remote, token, &mut upload).await {
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
        return Err(PutError::Before(BugSite::UploadNeverOpened.hit()));
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
        mark!(Up6);
        PutError::Commit(fault)
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
/// begun as soon as the header is known. Returns the bytes written once the
/// decoder has verified the whole file and its length and trailer checksum,
/// on this read, are the ones `token` was minted for: a file replaced after
/// it was witnessed is refused before the commit.
async fn copy_verified<L: ObjectStore, S: ObjectStore>(
    staging: &L,
    remote: &S,
    token: &Witnessed,
    upload: &mut Option<S::Upload>,
) -> Result<u64, PutError> {
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
        .map_err(|_undecodable| PutError::Before(Fault::StagedLost))?;
    names_key(verified.header(), key)?;
    if !token.matches(bytes, verified.trailer().file_checksum()) {
        return Err(PutError::Before(Fault::StagedLost));
    }
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

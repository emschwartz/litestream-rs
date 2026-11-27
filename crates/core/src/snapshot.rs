//! The level-9 snapshot: every page of the database as of the synced transaction.
//!
//! Go writes it through `SnapshotReader` while holding `chkMu` (`db.go:2877-2960`,
//! `3102-3117`): pages from the database file, overlaid with the WAL frames up to
//! the last synced offset, so no checkpoint runs under it. Here the proof is
//! the lock: a [`SyncedThrough`] is borrowed for the whole write, and the actor
//! that holds it is the only thing that can start a checkpoint.
//!
//! The page sources are the sync step's snapshot step ([`scan_whole`],
//! [`plan_pages`], the shared `stage`): frames of the current generation up to
//! the synced end win, the database file supplies the rest, and the page count is
//! the proof's last commit. The read transaction pins the WAL at our mark, so
//! another process may copy frames at or below the mark into the database file
//! while the file is read. Every such frame is at or below the synced end, so it is in the
//! page map and the map wins: a page torn by a concurrent backfill is never read.
//! That argument needs the synced end to be at or above the wal-index's
//! `nBackfill`; a proof below it cannot be a proof from this read transaction and
//! is [`Fault::Bug`].
//!
//! Level 9 is written to the replica only (no staged copy), as in Go, through the
//! caller's store so its retry wrapper applies. The header carries no WAL origin,
//! no pre-apply checksum and `min_txid = 1`, as Go's does.

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroU64};

use lsr_ltx::{Header, HeaderFields, PageChecksum, PageSize, Timestamp};
use lsr_wal::{CommittedEnd, OpenError, PageMap, Salts, WalReader};
use thiserror::Error;

use crate::fault::role::Role;
use crate::fault::{BugSite, Fault};
use crate::layout::{FileInfo, FileKey};
use crate::pages::PageChecksumTable;
use crate::position::{Continuity, ReadLockHold, SyncedThrough};
use crate::publish::{Handoff, Open, Writer};
use crate::source::{CommittedRead, PageSource, WalSource};
use crate::store::{Rest, WriteMode};
use crate::sync::{Filling, PageFrom, Plan, clamp, scan_whole};
use crate::verify::LivePages;

/// A snapshot that was not written. Nothing of this attempt is visible under `key`, unless
/// the store took the commit at another size (`UploadAmbiguous`): the caller deletes the key
/// then. After a checksum refusal (`UploadChecksumMismatch`) the key may hold this attempt's
/// bytes or an earlier one's, so nothing is deleted and the caller owes a read-back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("snapshot {key} failed: {fault:?}")]
pub struct SnapshotFailed {
    /// What went wrong; `policy` says what to do.
    pub fault: Fault,
    /// The file that was being written.
    pub key: FileKey,
}

/// Writes the level-9 file for `proof`'s transaction: `1..=txid`.
///
/// `proof` is borrowed: the checkpoint that follows needs it. `now` is clamped
/// to the proof's timestamp so a snapshot never predates the file it covers.
/// Every page taken from the database file is checked against `pages` first.
///
/// # Errors
/// [`SnapshotFailed`]. Retrying the same call is safe: the key is the same and
/// a commit replaces whatever an earlier, rejected one left under it.
pub(crate) async fn write_snapshot<W: WalSource, P: PageSource, S: Role>(
    proof: &SyncedThrough,
    wal: &mut W,
    db: &mut P,
    pages: &mut PageChecksumTable,
    remote: &S,
    now: Timestamp,
) -> Result<FileInfo, SnapshotFailed> {
    let mut snapshot = Snapshotting::begin(proof, wal, remote, now, false).await?;
    let key = snapshot.key();
    match snapshot
        .slice((wal, db, pages), remote, u64::MAX, true)
        .await
    {
        Ok(Some(info)) => Ok(info),
        Ok(None) => {
            snapshot.abort().await;
            Err(SnapshotFailed {
                fault: BugSite::Encode.hit(),
                key,
            })
        }
        Err(fault) => {
            snapshot.abort().await;
            Err(SnapshotFailed { fault, key })
        }
    }
}

/// Everything a whole-database read at a proof's transaction reads: the WAL's salts, and
/// where each page comes from.
struct Whole {
    salts: Salts,
    map: PageMap,
    commit: NonZeroU32,
    page_size: PageSize,
}

/// The checks a whole-database read makes before it reads a page, and its plan (see the module
/// doc for which source wins).
fn whole<W: WalSource>(proof: &SyncedThrough, wal: &mut W) -> Result<Whole, Fault> {
    let position = proof.position();
    let mut reader = open_wal(wal)?;
    let committed = wal.committed().map_err(Fault::from_source)?;
    let salts = reader.header().salts();
    let page_size = position.page_size();
    if position.continuity() != Continuity::Held(committed.epoch()) {
        return Err(Fault::ReadTxLost);
    }
    if reader.header().page_size() != page_size {
        return Err(Fault::PageSizeChanged {
            was: page_size,
            now: reader.header().page_size(),
        });
    }
    if committed.end().salts() != salts || position.cursor().salts() != salts {
        return Err(Fault::WalMoved);
    }
    let synced = CommittedEnd::new(position.synced().offset().frame_index(), salts, page_size);
    let upto = CommittedRead::new(synced, committed.backfilled(), committed.epoch())
        .ok_or_else(|| BugSite::SnapshotScan.hit())?;
    let map = scan_whole(&mut reader, upto)?;
    let commit = position.commit();
    crate::invariant!(
        map.commit().is_none_or(|scanned| scanned == commit),
        BugSite::SnapshotScan
    );
    Ok(Whole {
        salts,
        map,
        commit,
        page_size,
    })
}

/// A level-9 snapshot part-way to the replica: what it holds between two slices, in memory
/// only. A failure, a stop or a crash starts it again from its first page; nothing of it is
/// visible until its commit.
///
/// Between two slices the engine syncs and uploads, and runs no checkpoint of its own. So the
/// read transaction the proof was taken under is held throughout, and SQLite copies no frame
/// past its read mark into the database file: the mark is at or below the synced end, whose
/// frames the page map holds and which win over the file. Each slice first reads the
/// wal-index again: another hold of the read transaction, another WAL generation or a
/// backfill past the synced end ends the snapshot before it reads a page, and so does the
/// table's pin let go (a snapshot step emptied it). A database-file page is checked against
/// the table as it stood when the snapshot began ([`PageChecksumTable::pin`]).
#[derive(Debug)]
pub(crate) struct Snapshotting {
    key: FileKey,
    epoch: ReadLockHold,
    salts: Salts,
    /// The synced end's frame count: the frames the snapshot holds, and the most SQLite may
    /// have copied into the database file under it.
    frames: u32,
    map: PageMap,
    commit: NonZeroU32,
    page_size: PageSize,
    filling: Filling,
    open: Option<Open>,
    pub(crate) handoff: Handoff,
    pinned: bool,
}

impl Snapshotting {
    /// Begins the level-9 file for `proof`'s transaction: reads the WAL's page map up to the
    /// synced end and opens the upload. `pinned`: the caller pinned `pages` and syncs between
    /// the slices.
    ///
    /// # Errors
    /// [`SnapshotFailed`]; nothing was written.
    pub(crate) async fn begin<W: WalSource, S: Role>(
        proof: &SyncedThrough,
        wal: &mut W,
        remote: &S,
        now: Timestamp,
        pinned: bool,
    ) -> Result<Self, SnapshotFailed> {
        let key = FileKey::snapshot(proof.position().txid());
        let failed = |fault| SnapshotFailed { fault, key };
        let epoch = wal.epoch();
        let Whole {
            salts,
            map,
            commit,
            page_size,
        } = whole(proof, wal).map_err(failed)?;
        let (timestamp, _clamped) = clamp(Some(proof.position().timestamp()), now);
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size,
            commit: commit.get(),
            txids: key.range(),
            timestamp,
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .map_err(|_err| failed(BugSite::Encode.hit()))?;
        let filling =
            Filling::new(header, &Plan::new(&map, None, commit, page_size)).map_err(failed)?;
        let open = Writer::begin(remote, &key, timestamp, WriteMode::Replace, None)
            .await
            .map_err(|err| failed(S::fault(&err)))?
            .boxed();
        Ok(Self {
            key,
            epoch,
            salts,
            frames: proof.position().synced().offset().frame_index(),
            map,
            commit,
            page_size,
            filling,
            open: Some(open),
            handoff: Handoff::default(),
            pinned,
        })
    }

    /// The file being written.
    pub(crate) const fn key(&self) -> FileKey {
        self.key
    }

    /// Hands the replica the next `budget` bytes of the file, never more, and commits it
    /// once every byte is handed over and the upload's parts are answered. `Ok(None)`: it
    /// goes on in a later slice. `patient` waits for the upload's parts inside the call, as a
    /// whole-file write does; otherwise a slice that finds them in the way stops, and
    /// `waits` says so.
    ///
    /// # Errors
    /// The fault; the caller aborts the upload ([`Self::abort`]).
    pub(crate) async fn slice<W: WalSource, P: PageSource, S: Role>(
        &mut self,
        (wal, db, pages): (&mut W, &mut P, &mut PageChecksumTable),
        remote: &S,
        budget: u64,
        patient: bool,
    ) -> Result<Option<FileInfo>, Fault> {
        self.handoff.waits = false;
        self.holds(wal, pages)?;
        let mut reader = self.reader(wal)?;
        let plan = Plan::new(&self.map, None, self.commit, self.page_size);
        // Zero once the budget is spent: a loop on a nonzero count cannot be left spinning on a
        // wrong comparison, and every pass must hand over a byte (an encoded but unsent byte
        // waits whenever the file is not whole), so a pass that leaves the room as it was is a bug.
        let mut left = NonZeroU64::new(budget);
        let open = self
            .open
            .as_mut()
            .ok_or_else(|| BugSite::UploadNeverOpened.hit())?;
        while let Some(room) = left
            && !self.filling.whole()
        {
            let clear = self.handoff.clear(open, Rest::More, patient);
            if !clear.await.map_err(|err| S::fault(&err))? {
                break;
            }
            self.filling
                .encode(&plan, &mut reader, db, pages, self.pinned)?;
            let sent = self.handoff.send(open, self.filling.out(), room);
            left = sent.await.map_err(|err| S::fault(&err))?;
            let drained = left.is_none() || self.filling.out().is_empty();
            crate::invariant!(left != Some(room) && drained, BugSite::Encode);
        }
        pages.pin_past(self.filling.next());
        if !self.filling.whole() {
            return Ok(None);
        }
        let clear = self.handoff.clear(open, Rest::None, patient);
        if !clear.await.map_err(|err| S::fault(&err))? {
            return Ok(None);
        }
        self.holds(wal, pages)?;
        let open = self
            .open
            .take()
            .ok_or_else(|| BugSite::UploadNeverOpened.hit())?;
        let published = open
            .commit(remote)
            .await
            .map_err(|err| S::fault(err.error()))?;
        Ok(Some(published.info()))
    }

    /// Ends the snapshot: its upload is aborted, so nothing of it stays as scratch.
    pub(crate) async fn abort(self) {
        if let Some(open) = self.open {
            // Nothing was published; a failed abort leaves scratch the next start clears.
            drop(open.abort().await);
        }
    }

    /// Whether the read the snapshot began under still holds: the same hold of the read
    /// transaction, the same WAL generation, nothing copied into the database file past the
    /// synced end, and, for a snapshot the engine syncs beside, the table's pin.
    fn holds<W: WalSource>(&self, wal: &mut W, pages: &PageChecksumTable) -> Result<(), Fault> {
        let committed = wal.committed().map_err(Fault::from_source)?;
        if committed.epoch() != self.epoch {
            return Err(Fault::ReadTxLost);
        }
        let moved = committed.end().salts() != self.salts || committed.backfilled() > self.frames;
        if moved || (self.pinned && !pages.pinned()) {
            return Err(Fault::WalMoved);
        }
        Ok(())
    }

    /// The WAL, opened again for this slice: the same generation, or the snapshot ends.
    fn reader<W: WalSource>(&self, wal: &mut W) -> Result<WalReader<W::Wal>, Fault> {
        let reader = open_wal(wal)?;
        if reader.header().salts() == self.salts {
            Ok(reader)
        } else {
            Err(Fault::WalMoved)
        }
    }
}

/// The live database at `proof`'s transaction, one checksum a page: what a restore of that
/// transaction must produce (the scheduled verify, `crate::verify`).
///
/// It reads what [`write_snapshot`] reads, under the same proof and the same argument, and
/// opens nothing: a WAL page's checksum is the one its frame was verified with on the scan,
/// so only the pages the database file supplies are read, each checked against `pages`
/// first, as a snapshot checks them.
///
/// # Errors
/// The [`Fault`] a snapshot would meet on the same read.
pub(crate) fn read_live<W: WalSource, P: PageSource>(
    proof: &SyncedThrough,
    wal: &mut W,
    db: &mut P,
    pages: &mut PageChecksumTable,
) -> Result<LivePages, Fault> {
    let Whole {
        map,
        commit,
        page_size,
        ..
    } = whole(proof, wal)?;
    let plan = Plan::new(&map, None, commit, page_size);
    let mut buf = vec![0_u8; page_size.bytes()];
    let mut digests = BTreeMap::new();
    for page in plan.iter() {
        let digest = match page {
            PageFrom::Wal(frame) => frame.digest(),
            PageFrom::Db(pgno) => {
                db.read_page(pgno, &mut buf).map_err(Fault::from_source)?;
                pages
                    .check(pgno, &buf)
                    .map_err(|mismatch| Fault::DatabasePageMismatch {
                        pgno: mismatch.pgno,
                    })?;
                PageChecksum::of(pgno, &buf)
            }
        };
        digests.insert(page.pgno(), digest);
    }
    Ok(LivePages::new(
        proof.position().txid(),
        page_size,
        commit,
        digests,
    ))
}

fn open_wal<W: WalSource>(wal: &mut W) -> Result<WalReader<W::Wal>, Fault> {
    let file = wal
        .open()
        .map_err(Fault::from_source)?
        .ok_or(Fault::WalUnlinked)?;
    WalReader::open(file).map_err(open_fault)
}

/// A WAL whose header cannot be read: a torn header is a restart in progress, so
/// the next `recover` classifies it.
fn open_fault(err: OpenError) -> Fault {
    match err {
        OpenError::Io(_) => Fault::LocalIo,
        OpenError::Truncated { .. } | OpenError::Header(_) => Fault::WalMoved,
    }
}

#[cfg(test)]
mod tests;

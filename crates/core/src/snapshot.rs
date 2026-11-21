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
//! the proof's last commit. The read transaction pins the WAL at our mark, so a
//! other process may copy frames at or below the mark into the database file while the
//! file is read. Every such frame is at or below the synced end, so it is in the
//! page map and the map wins: a page torn by a concurrent backfill is never read.
//! That argument needs the synced end to be at or above the wal-index's
//! `nBackfill`; a proof below it cannot be a proof from this read transaction and
//! is [`Fault::Bug`].
//!
//! Level 9 is written to the replica only (no staged copy), as in Go, through the
//! caller's store so its retry wrapper applies. The header carries no WAL origin,
//! no pre-apply checksum and `min_txid = 1`, as Go's does.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use lsr_ltx::{Header, HeaderFields, PageChecksum, PageSize, Timestamp};
use lsr_wal::{CommittedEnd, OpenError, WalReader};
use thiserror::Error;

use crate::fault::{BugSite, Fault};
use crate::layout::{FileInfo, FileKey};
use crate::pages::PageChecksumTable;
use crate::position::{Continuity, SyncedThrough};
use crate::source::{CommittedRead, PageSource, WalSource};
use crate::store::ObjectStore;
use crate::sync::{PageFrom, clamp, plan_pages, scan_whole, stage};
use crate::verify::LivePages;

/// A snapshot that was not written. Nothing is visible under `key`, unless
/// the fault says the store rejected its own commit (`UploadAmbiguous`,
/// `UploadChecksumMismatch`): the caller deletes the key then.
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
pub(crate) async fn write_snapshot<W: WalSource, P: PageSource, S: ObjectStore>(
    proof: &SyncedThrough,
    wal: &mut W,
    db: &mut P,
    pages: &mut PageChecksumTable,
    remote: &S,
    now: Timestamp,
) -> Result<FileInfo, SnapshotFailed> {
    let key = FileKey::snapshot(proof.position().txid());
    run(proof, &key, wal, db, pages, remote, now)
        .await
        .map_err(|fault| SnapshotFailed { fault, key })
}

/// Everything a whole-database read at a proof's transaction reads: the WAL reader, and
/// where each page comes from.
struct Whole<R> {
    reader: WalReader<R>,
    plan: Vec<PageFrom>,
    commit: NonZeroU32,
    page_size: PageSize,
}

/// The checks a whole-database read makes before it reads a page, and its plan: the WAL
/// frames of the current generation up to the synced end win, the database file supplies
/// the rest, and the page count is the proof's last commit.
fn whole<W: WalSource>(proof: &SyncedThrough, wal: &mut W) -> Result<Whole<W::Wal>, Fault> {
    let position = proof.position();
    let file = wal
        .open()
        .map_err(Fault::from_source)?
        .ok_or(Fault::WalUnlinked)?;
    let mut reader = WalReader::open(file).map_err(open_fault)?;
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
    let plan = plan_pages(&map, None, commit, page_size);
    Ok(Whole {
        reader,
        plan,
        commit,
        page_size,
    })
}

async fn run<W: WalSource, P: PageSource, S: ObjectStore>(
    proof: &SyncedThrough,
    key: &FileKey,
    wal: &mut W,
    db: &mut P,
    pages: &mut PageChecksumTable,
    remote: &S,
    now: Timestamp,
) -> Result<FileInfo, Fault> {
    let position = proof.position();
    let Whole {
        mut reader,
        plan,
        commit,
        page_size,
    } = whole(proof, wal)?;
    let (timestamp, _clamped) = clamp(Some(position.timestamp()), now);
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
    .map_err(|_err| BugSite::Encode.hit())?;
    let (file, _last) = stage(
        remote,
        key,
        header,
        &plan,
        &mut reader,
        db,
        pages,
        Fault::from_remote,
    )
    .await?;
    Ok(file)
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
        plan,
        commit,
        page_size,
        ..
    } = whole(proof, wal)?;
    let mut buf = vec![0_u8; page_size.bytes()];
    let mut digests = BTreeMap::new();
    for page in &plan {
        let digest = match page {
            PageFrom::Wal(frame) => frame.digest(),
            PageFrom::Db(pgno) => {
                db.read_page(*pgno, &mut buf).map_err(Fault::from_source)?;
                pages
                    .check(*pgno, &buf)
                    .map_err(|mismatch| Fault::DatabasePageMismatch {
                        pgno: mismatch.pgno,
                    })?;
                PageChecksum::of(*pgno, &buf)
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

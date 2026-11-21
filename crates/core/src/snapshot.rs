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
//! stranger may copy frames at or below the mark into the database file while the
//! file is read. Every such frame is at or below the synced end, so it is in the
//! page map and the map wins: a page torn by a concurrent backfill is never read.
//! That argument needs the synced end to be at or above the wal-index's
//! `nBackfill`; a proof below it cannot be a proof from this read transaction and
//! is [`Fault::Bug`].
//!
//! Level 9 is written to the replica only (no staged copy), as in Go, through the
//! caller's store so its retry wrapper applies. The header carries no WAL origin,
//! no pre-apply checksum and `min_txid = 1`, as Go's does.

use lsr_ltx::{Header, HeaderFields, Timestamp, Txid, TxidRange};
use lsr_wal::{CommittedEnd, OpenError, WalReader};
use thiserror::Error;

use crate::fault::Fault;
use crate::layout::{FileInfo, FileKey, Level};
use crate::pages::PageTable;
use crate::position::{Continuity, SyncedThrough};
use crate::source::{CommittedRead, PageSource, WalSource};
use crate::store::ObjectStore;
use crate::sync::{clamp, plan_pages, scan_whole, stage};

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
pub async fn write_snapshot<W: WalSource, P: PageSource, S: ObjectStore>(
    proof: &SyncedThrough,
    wal: &mut W,
    db: &mut P,
    pages: &mut PageTable,
    remote: &S,
    now: Timestamp,
) -> Result<FileInfo, SnapshotFailed> {
    let txid = proof.position().txid();
    let Some(range) = TxidRange::new(Txid::MIN, txid) else {
        let key = FileKey::new(Level::SNAPSHOT, TxidRange::single(txid));
        return Err(SnapshotFailed {
            fault: Fault::Bug,
            key,
        });
    };
    let key = FileKey::new(Level::SNAPSHOT, range);
    run(proof, &key, wal, db, pages, remote, now)
        .await
        .map_err(|fault| SnapshotFailed { fault, key })
}

async fn run<W: WalSource, P: PageSource, S: ObjectStore>(
    proof: &SyncedThrough,
    key: &FileKey,
    wal: &mut W,
    db: &mut P,
    pages: &mut PageTable,
    remote: &S,
    now: Timestamp,
) -> Result<FileInfo, Fault> {
    let position = proof.position();
    let file = wal
        .open()
        .map_err(Fault::from_source)?
        .ok_or(Fault::WalUnlinked)?;
    let mut reader = WalReader::open(file).map_err(open_fault)?;
    let committed = wal.committed().map_err(Fault::from_source)?;
    let salts = reader.header().salts();
    let page_size = position.page_size();
    if position.continuity() != Continuity::Pinned(wal.epoch()) {
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
    let upto = CommittedRead::new(synced, committed.backfilled()).ok_or(Fault::Bug)?;
    let map = scan_whole(&mut reader, upto)?;
    let commit = position.commit();
    if map.commit() != 0 && map.commit() != commit.get() {
        return Err(Fault::Bug);
    }
    let plan = plan_pages(&map, None, commit, page_size);
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
    .map_err(|_err| Fault::Bug)?;
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

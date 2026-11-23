//! The check a boot owes when the engine journal holds a WAL-sync debt: is the database file
//! ahead of its WAL? (`docs/decisions.md` D103.)
//!
//! After a failed sync of the WAL, a checkpoint (the application's, another process's, or the
//! TRUNCATE that pays the debt) can copy frames the failed sync covered into the database file
//! and sync it, and a power loss or an eviction can then lose those frames from the WAL. SQLite
//! recovers the shorter WAL over the newer file: a database no commit made, every page valid.
//!
//! The check needs the WAL to still be the generation of the newest staged file (recovery's
//! `Continue`): every legitimate change since that file's transaction T is then a frame of this
//! generation, and a checkpoint is the only writer of the database file. So every page no frame
//! of the WAL covers must hold its image at T. The images at T are the replica's state at its
//! newest confirmed transaction, restored into one checksum a page, overlaid with the page table,
//! which holds the pages the staged files after that transaction wrote (`crate::pages`). A page
//! above the size at T has no image and is not checked.
//!
//! The scan and the reads run under one committed read of the wal-index: a checkpoint that
//! copies a frame below its end copies a page the scan saw covered, so it is skipped. The
//! wal-index is read again after the reads, and a read transaction lost or a WAL restarted under
//! them is `WalMoved`, retried as any sync step is.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use lsr_ltx::{PageChecksum, PageNo, Txid};
use lsr_wal::{OpenError, WalReader};

use crate::fault::Fault;
use crate::fault::role::Role;
use crate::pages::PageChecksumTable;
use crate::plan::RestoreTarget;
use crate::position::Position;
use crate::restore::{RestoreError, RestoreOptions, restore_from};
use crate::source::{PageSource, WalSource};
use crate::sync::scan_whole;
use crate::verify::DigestSink;

/// Database-file pages that are not their images at the newest staged transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Found {
    /// How many.
    pub pages: NonZeroU32,
    /// The lowest.
    pub first: NonZeroU32,
}

/// Checks the database file against the images at `position`'s transaction, the newest staged
/// one; `confirmed` is the replica's newest transaction, `table` the page table seeded at boot.
///
/// # Errors
/// The [`Fault`] a read met: the WAL moved, a page could not be read, or the replica could not
/// give its state at `confirmed`.
pub(crate) async fn check<W: WalSource, P: PageSource, R: Role>(
    position: &Position,
    wal: &mut W,
    db: &mut P,
    table: &PageChecksumTable,
    remote: &R,
    confirmed: Option<Txid>,
) -> Result<Option<Found>, Fault> {
    let page_size = position.page_size();
    let file = wal
        .open()
        .map_err(Fault::from_source)?
        .ok_or(Fault::WalUnlinked)?;
    let mut reader = WalReader::open(file).map_err(open_fault)?;
    let committed = wal.committed().map_err(Fault::from_source)?;
    let salts = reader.header().salts();
    if reader.header().page_size() != page_size {
        return Err(Fault::PageSizeChanged {
            was: page_size,
            now: reader.header().page_size(),
        });
    }
    if committed.end().salts() != salts || position.cursor().salts() != salts {
        return Err(Fault::WalMoved);
    }
    let covered = scan_whole(&mut reader, committed)?;
    let size = match covered.commit() {
        Some(size) => size.get(),
        None => db.page_count().map_err(Fault::from_source)?.get(),
    };
    let mut image = BTreeMap::new();
    if let Some(confirmed) = confirmed {
        digests_at(remote, confirmed, &mut image).await?;
    }
    let zero = vec![0_u8; page_size.bytes()];
    let mut buf = vec![0_u8; page_size.bytes()];
    let mut differ: Option<Found> = None;
    for n in 1..=size.min(position.commit().get()) {
        // The lock page is the one number PageNo refuses.
        let Some(pgno) = PageNo::new(n, page_size) else {
            continue;
        };
        if covered.get(pgno).is_some() {
            continue;
        }
        db.read_page(pgno, &mut buf).map_err(Fault::from_source)?;
        let want = table
            .get(pgno)
            .or_else(|| image.get(&n).copied())
            .unwrap_or_else(|| PageChecksum::of(pgno, &zero));
        if PageChecksum::of(pgno, &buf) != want {
            differ = Some(tally(differ, pgno));
        }
    }
    // A lost read transaction may have let a checkpoint copy frames the scan never saw; a WAL
    // restarted in place (another connection's TRUNCATE over a read mark of 0) names new salts
    // in the wal-index while the hold stands.
    let after = wal.committed().map_err(Fault::from_source)?;
    if after.epoch() != committed.epoch() || after.end().salts() != salts {
        return Err(Fault::WalMoved);
    }
    Ok(differ)
}

/// `found` with one more page; the pages are visited in ascending order, so the first one
/// stays the lowest.
fn tally(found: Option<Found>, pgno: PageNo) -> Found {
    match found {
        None => Found {
            pages: NonZeroU32::MIN,
            first: NonZeroU32::new(pgno.get()).unwrap_or(NonZeroU32::MIN),
        },
        Some(found) => Found {
            pages: found.pages.saturating_add(1),
            ..found
        },
    }
}

fn open_fault(err: OpenError) -> Fault {
    match err {
        OpenError::Io(_) => Fault::LocalIo,
        // A header torn mid-restart: the next recover classifies it.
        OpenError::Truncated { .. } | OpenError::Header(_) => Fault::WalMoved,
    }
}

/// The replica's state at `txid`, one checksum a page, into `pages`.
async fn digests_at<R: Role>(
    remote: &R,
    txid: Txid,
    pages: &mut BTreeMap<u32, PageChecksum>,
) -> Result<(), Fault> {
    let sink = DigestSink { pages };
    match restore_from(
        remote,
        None,
        RestoreTarget::Txid(txid),
        RestoreOptions::default(),
        sink,
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(err) => Err(restore_fault::<R>(&err)),
    }
}

/// What a restore of the replica's newest confirmed state failing means for the check: a store
/// error is the store's fault, retried as such; a file that does not verify or a history that
/// does not plan is the replica's damage.
fn restore_fault<R: Role>(err: &RestoreError) -> Fault {
    match err {
        RestoreError::Store(err) => R::fault(err),
        RestoreError::Unstable { .. } | RestoreError::ListingBehind { .. } => Fault::StoreTransient,
        RestoreError::Inventory(_)
        | RestoreError::NoPlan(_)
        | RestoreError::File { .. }
        | RestoreError::DatabaseChecksum { .. }
        | RestoreError::CreatedAtRepeated { .. } => Fault::RemoteCorrupt,
        RestoreError::Sink { .. } | RestoreError::Publish { .. } => Fault::LocalIo,
    }
}

#[cfg(test)]
mod tests;

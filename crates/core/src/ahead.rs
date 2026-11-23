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
//!
//! A replica that cannot give its state at its newest confirmed transaction is told apart from
//! one that cannot be reached ([`CheckError`]): a failed call heals by retrying, damage (a gap a
//! bucket rollback left, a file that does not verify) does not, and the actor gives the check up
//! when it recurs (`docs/decisions.md` D106).

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use lsr_ltx::{PageChecksum, PageNo, Txid};
use lsr_wal::{OpenError, WalReader};

use crate::fault::Fault;
use crate::fault::role::Role;
use crate::inventory::InventoryError;
use crate::pages::PageChecksumTable;
use crate::plan::RestoreTarget;
use crate::position::Position;
use crate::restore::{RestoreError, RestoreOptions, restore_one_plan};
use crate::source::{PageSource, WalSource};
use crate::store::StoreError;
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

/// Why the check did not finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckError {
    /// A read failed or the WAL moved: retried as any sync step is.
    Fault(Fault),
    /// The replica could not give its state at its newest confirmed transaction because it is
    /// damaged, not because a call failed: retrying does not heal it.
    Unrestorable,
}

impl From<Fault> for CheckError {
    fn from(fault: Fault) -> Self {
        Self::Fault(fault)
    }
}

/// Checks the database file against the images at `position`'s transaction, the newest staged
/// one; `confirmed` is the replica's newest transaction, `table` the page table seeded at boot.
///
/// # Errors
/// [`CheckError::Fault`] for what a read met (the WAL moved, a page could not be read, a store
/// call failed); [`CheckError::Unrestorable`] when the replica's state at `confirmed` is damaged.
pub(crate) async fn check<W: WalSource, P: PageSource, R: Role>(
    position: &Position,
    wal: &mut W,
    db: &mut P,
    table: &PageChecksumTable,
    remote: &R,
    confirmed: Option<Txid>,
) -> Result<Option<Found>, CheckError> {
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
        }
        .into());
    }
    if committed.end().salts() != salts || position.cursor().salts() != salts {
        return Err(Fault::WalMoved.into());
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
        return Err(Fault::WalMoved.into());
    }
    Ok(differ)
}

/// `found` with one more page; the pages are visited in ascending order, so the first one
/// stays the lowest.
pub(crate) fn tally(found: Option<Found>, pgno: PageNo) -> Found {
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

pub(crate) fn open_fault(err: OpenError) -> Fault {
    match err {
        OpenError::Io(_) => Fault::LocalIo,
        // A header torn mid-restart: the next recover classifies it.
        OpenError::Truncated { .. } | OpenError::Header(_) => Fault::WalMoved,
    }
}

/// The replica's state at `txid`, one checksum a page, into `pages`, from one plan.
async fn digests_at<R: Role>(
    remote: &R,
    txid: Txid,
    pages: &mut BTreeMap<u32, PageChecksum>,
) -> Result<(), CheckError> {
    let sink = DigestSink { pages };
    // One plan a tick: the actor tries again on a later tick, and gives up on damage that
    // recurs there (`super::actor::ahead`).
    match restore_one_plan(
        remote,
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

/// What a restore of the replica's newest confirmed state failing means for the check: a failed
/// call (a read, a listing page) or a replica changing under the restore is the store's fault,
/// retried as such; a history that does not plan, a file or database that does not verify, or a
/// listing no real store returns is the replica's damage.
pub(crate) fn restore_fault<R: Role>(err: &RestoreError) -> CheckError {
    match err {
        RestoreError::Store(StoreError::Corrupt { .. })
        | RestoreError::Inventory(
            InventoryError::ConflictingDuplicate { .. } | InventoryError::ListingStalled(_),
        )
        | RestoreError::NoPlan(_)
        | RestoreError::File { .. }
        | RestoreError::DatabaseChecksum { .. }
        | RestoreError::CreatedAtRepeated { .. } => CheckError::Unrestorable,
        RestoreError::Store(err) | RestoreError::Inventory(InventoryError::Store(err)) => {
            R::fault(err).into()
        }
        RestoreError::Unstable { .. } => Fault::StoreTransient.into(),
        RestoreError::Sink { .. } | RestoreError::Publish { .. } => Fault::LocalIo.into(),
    }
}

#[cfg(test)]
mod tests;

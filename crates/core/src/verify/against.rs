//! The scheduled verify: the replica checked at
//! [Depth::Names](super::Depth::Names) (Go's validation, and more), then the
//! real restore of the live database's transaction compared with the live
//! database page for page.
//!
//! Go's validation monitor (`store.go:933-968` at 041a8c3) lists each compaction level
//! and checks the files are sorted and contiguous; it reads no file and compares nothing
//! with the database. With `NoChecksum` files nothing in the replica can show a stale page,
//! so the comparison is the part Go lacks (`docs/PLAN.md` section 7). The listing half is
//! [verify](super::verify()) at [Depth::Names](super::Depth::Names), whose
//! holes and malformed snapshots cover Go's gap and order checks; the comparison is the dry run's restore, aimed at [LivePages::txid] instead of
//! the latest state, into the same one-checksum-a-page sink.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use lsr_ltx::{PageChecksum, PageNo, PageSize, Txid};

use super::dry_run::digest_restore;
use super::{Findings, Report, VerifyError, name_findings, report};
use crate::inventory::Inventory;
use crate::plan::{PlanOutcome, RestoreTarget};
use crate::restore::RestoreError;
use crate::store::ObjectStore;

/// The live database at one transaction, one checksum a page: what a restore of that
/// transaction must produce. The engine reads it under its read transaction
/// (`crate::Actor::read_live_pages`); nothing else builds one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LivePages {
    txid: Txid,
    page_size: PageSize,
    commit: NonZeroU32,
    pages: BTreeMap<PageNo, PageChecksum>,
}

impl LivePages {
    pub(crate) const fn new(
        txid: Txid,
        page_size: PageSize,
        commit: NonZeroU32,
        pages: BTreeMap<PageNo, PageChecksum>,
    ) -> Self {
        Self {
            txid,
            page_size,
            commit,
            pages,
        }
    }

    /// The transaction the pages are the state after.
    #[must_use]
    pub const fn txid(&self) -> Txid {
        self.txid
    }

    /// The page size.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// The database's length in pages.
    #[must_use]
    pub const fn commit(&self) -> NonZeroU32 {
        self.commit
    }
}

/// How the restore of the live transaction compares with the live database.
#[derive(Debug)]
pub enum Compared {
    /// Page for page the same.
    Same {
        /// The transaction compared.
        txid: Txid,
        /// Pages compared.
        pages: u32,
    },
    /// The restore holds other bytes than the live database at the same transaction: the
    /// replica would restore a database that never existed.
    Differs {
        /// The transaction compared.
        txid: Txid,
        /// The lowest page that differs (a page one side lacks counts).
        first: u32,
        /// How many pages differ.
        pages: u32,
        /// The live database's length in pages.
        live_commit: u32,
        /// The restored database's.
        restored_commit: u32,
    },
    /// The replica has no exact plan for the transaction any more: a compaction merged it
    /// into a file that also holds later ones and its level-0 files are gone. Nothing is
    /// wrong; the next run reads a newer transaction.
    Merged {
        /// The transaction asked for.
        txid: Txid,
    },
    /// The restore of the transaction fails.
    RestoreFails {
        /// The transaction asked for.
        txid: Txid,
        /// What restore said.
        error: RestoreError,
    },
}

/// What a scheduled verify found.
#[derive(Debug)]
pub struct Verified {
    report: Report,
    compared: Compared,
}

impl Verified {
    /// The replica checker's report at [Depth::Names](super::Depth::Names): list calls only.
    #[must_use]
    pub const fn report(&self) -> &Report {
        &self.report
    }

    /// The comparison with the live database.
    #[must_use]
    pub const fn compared(&self) -> &Compared {
        &self.compared
    }

    /// The report and the comparison, owned.
    #[must_use]
    pub fn into_parts(self) -> (Report, Compared) {
        (self.report, self.compared)
    }
}

/// Checks the replica `store` holds at [Depth::Names](super::Depth::Names), then restores [LivePages::txid]
/// and compares it with `live`. Costs the listing and one restore's reads (the files of
/// that transaction's plan); memory is 8 bytes a page on each side.
///
/// # Errors
/// [VerifyError] when a listing or a read fails: that says nothing about the replica.
pub async fn verify_against<S: ObjectStore>(
    store: &S,
    live: &LivePages,
) -> Result<Verified, VerifyError> {
    let inventory = Inventory::load(store)
        .await
        .map_err(VerifyError::Inventory)?;
    let mut findings = Findings::default();
    let latest = name_findings(&inventory, &mut findings);
    let mut restored = BTreeMap::new();
    let target = RestoreTarget::Txid(live.txid);
    let (result, files_read, bytes_read) =
        digest_restore(store, &inventory, target, &mut restored).await;
    let compared = match result {
        Ok(done) => compare(live, &restored, done.commit().get()),
        Err(RestoreError::Inventory(err)) => return Err(VerifyError::Inventory(err)),
        Err(RestoreError::Store(err)) => return Err(VerifyError::Store(err)),
        Err(RestoreError::NoPlan(no)) if inside_file(no.outcome()) => {
            mark!(Verify4);
            Compared::Merged { txid: live.txid }
        }
        Err(error) => Compared::RestoreFails {
            txid: live.txid,
            error,
        },
    };
    let report = report(&inventory, findings, latest, (files_read, bytes_read));
    Ok(Verified { report, compared })
}

/// Whether the transaction sits inside a compacted file and cannot be
/// restored exactly.
const fn inside_file(outcome: &PlanOutcome) -> bool {
    match outcome {
        PlanOutcome::InsideFile { .. } => true,
        PlanOutcome::Plan(_)
        | PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::Gap { .. }
        | PlanOutcome::MalformedSnapshot { .. } => false,
    }
}

fn compare(live: &LivePages, restored: &BTreeMap<u32, PageChecksum>, commit: u32) -> Compared {
    let live_page = |n: u32| PageNo::new(n, live.page_size).and_then(|pgno| live.pages.get(&pgno));
    let differ = |n: &u32| live_page(*n) != restored.get(n);
    let mut keys: Vec<u32> = live
        .pages
        .keys()
        .map(|pgno| pgno.get())
        .chain(restored.keys().copied())
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let differing: Vec<u32> = keys.into_iter().filter(differ).collect();
    let pages = u32::try_from(differing.len()).unwrap_or(u32::MAX);
    match differing.first() {
        None if commit == live.commit.get() => Compared::Same {
            txid: live.txid,
            pages: u32::try_from(live.pages.len()).unwrap_or(u32::MAX),
        },
        first => Compared::Differs {
            txid: live.txid,
            first: first.copied().unwrap_or(commit.min(live.commit.get())),
            pages,
            live_commit: live.commit.get(),
            restored_commit: commit,
        },
    }
}

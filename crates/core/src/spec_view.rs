//! The planner's values as the restore-plan spec's (`lsr-spec`), so the planner's
//! own tests and the fuzz targets judge it by an oracle that cannot call it. The
//! simulator judges restores by its own restatement of the contract instead
//! (`crates/sim/src/contract.rs`).
//!
//! Conversion only: no rule of the contract lives here, and nothing here
//! decides. The spec depends on nothing, so the direction of this adapter is
//! the only one the build allows.

use std::num::NonZeroU64;

use lsr_spec::restore_plan as spec;

use crate::inventory::Inventory;
use crate::layout::{CreatedAt, CreatedAtSource, FileInfo, FileKey};
use crate::plan::{ContiguousChain, PlanOutcome, RestoreTarget};

/// A key as the spec names it.
#[must_use]
pub fn key(key: FileKey) -> spec::Key {
    spec::Key {
        level: key.level().get(),
        min: key.range().min().get(),
        max: key.range().max().get(),
    }
}

/// A created-at time as the spec reads it: the instant and its origin, the
/// two facts its provenance table needs.
#[must_use]
pub const fn created_at(created_at: CreatedAt) -> spec::CreatedAt {
    spec::CreatedAt {
        at_ms: created_at.at().unix_millis(),
        origin: match created_at.origin() {
            CreatedAtSource::Metadata => spec::Origin::Metadata,
            CreatedAtSource::Mtime => spec::Origin::Mtime,
            CreatedAtSource::Fallback => spec::Origin::Fallback,
        },
    }
}

/// A file with the created-at time its listing carries, if exact.
#[must_use]
pub fn file(info: &FileInfo) -> spec::File {
    spec::File {
        key: key(info.key),
        created_at: info.created_at().map(created_at),
    }
}

/// A file with a created-at time known some other way (resolved from the
/// store, or set by the test that stored it).
#[must_use]
pub fn file_at(info: &FileInfo, at: CreatedAt) -> spec::File {
    spec::File {
        key: key(info.key),
        created_at: Some(created_at(at)),
    }
}

/// The inventory as a listing, with each file's listed created-at time.
/// `None` only if the spec refuses a key the inventory holds, which would
/// mean the two disagree on what a valid key is.
#[must_use]
pub fn listing(inventory: &Inventory) -> Option<spec::Listing> {
    spec::Listing::new(inventory.files().iter().map(file))
}

/// The target as the spec names it; `None` never, as a transaction id is not zero.
#[must_use]
pub fn target(target: RestoreTarget) -> Option<spec::Target> {
    match target {
        RestoreTarget::Latest => Some(spec::Target::Latest),
        RestoreTarget::Txid(txid) => NonZeroU64::new(txid.get()).map(spec::Target::Txid),
        RestoreTarget::AsOf(instant) => Some(spec::Target::AsOf(instant.unix_millis())),
    }
}

fn chain(chain: &ContiguousChain) -> Vec<spec::Key> {
    chain.files().iter().map(|info| key(info.key)).collect()
}

/// The outcome as the spec shapes it.
#[must_use]
pub fn outcome(outcome: &PlanOutcome) -> spec::Outcome {
    match outcome {
        PlanOutcome::Plan(planned) => spec::Outcome::Plan {
            chain: chain(planned),
        },
        PlanOutcome::EmptyReplica => spec::Outcome::EmptyReplica,
        PlanOutcome::NoSnapshot {
            oldest_snapshot,
            malformed_snapshots,
        } => spec::Outcome::NoSnapshot {
            oldest_snapshot: oldest_snapshot.map(|txid| txid.get()),
            malformed_snapshots: malformed_snapshots.iter().copied().map(key).collect(),
        },
        PlanOutcome::BeyondNewest { requested, newest } => spec::Outcome::BeyondNewest {
            requested: requested.get(),
            newest: newest.get(),
        },
        PlanOutcome::AsOfInsideFile {
            chain: planned,
            file,
        } => spec::Outcome::AsOfInsideFile {
            chain: chain(planned),
            file: key(*file),
        },
        PlanOutcome::Gap {
            have_through,
            next_starts,
        } => spec::Outcome::Gap {
            have_through: have_through.get(),
            next_starts: next_starts.get(),
        },
        PlanOutcome::InsideFile { requested, file } => spec::Outcome::InsideFile {
            requested: requested.get(),
            file: key(*file),
        },
        PlanOutcome::MalformedSnapshot { have_through, file } => spec::Outcome::MalformedSnapshot {
            have_through: have_through.get(),
            file: key(*file),
        },
    }
}

/// The spec's judgement of `target` over `inventory`'s listed created-at
/// times; `None` as for [`listing`].
#[must_use]
pub fn judge(inventory: &Inventory, target: RestoreTarget) -> Option<spec::Judgement> {
    Some(spec::judge(&listing(inventory)?, self::target(target)?))
}

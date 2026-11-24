//! Short listings: what a maintenance pass reads between whole ones.
//!
//! A whole listing pages through every file of all ten levels, so its cost grows with the
//! replica's retention: a week of level-1 files is about 20,000 entries and 21 requests, and
//! maintenance used to run one whole listing at least every 15 seconds while the database was
//! written (issue 324, and upstream's report of the same shape). The engine now remembers, in
//! memory only, the newest file it knows at each level 1 to 8 ([`Remembered`]), and between
//! whole listings a pass asks the store only for level 0, level 9 and the files at each level
//! after what it knows ([`Tail`]). Nothing is written to disk and nothing survives a fault,
//! a stop or a restart: [`Remembered::forget`] drops it all and the next pass is whole.
//!
//! # When a pass is whole
//!
//! [`Remembered::short_ok`] is false, and the pass lists everything and decides with [`super::due`]
//! exactly as before, at boot and after any fault (the actor calls `forget`), after a refused or
//! listed upload, while a restore point is pinned, a rejected output is distrusted or the boot
//! read-back is owed, after a retention delete or a snapshot, when the last whole listing did not
//! pass the retention gate ([`Gate::Held`]), when snapshot retention next has
//! something to delete ([`Remembered::retention_wake`]), and at the latest [`FULL_LISTING_INTERVAL`]
//! after the last whole listing.
//!
//! # What each decision reads
//!
//! | Decision | Listing it reads | Why that is enough |
//! |---|---|---|
//! | Compaction into level `d` | level `d` after what is known, level `d-1` after the level's last transaction | The planner reads the destination's last file and the sources above its last transaction, nothing older. Level 0 and level 9 are listed whole. A remembered newest file can only be a file that was there: retention never deletes a level's last file, and a file another process removed is caught at the next whole listing. |
//! | Snapshot | level 9 whole, the newest position over what was listed or known | The planner reads the newest snapshot start and the newest transaction. |
//! | Level-0 retention | level 0 whole, level 9 whole, level 1 to 8 files after the cursor | [`super::retain::short_level_zero`]: its deletes are a subset of the whole rule's. A covering file must be listed in this pass, never remembered. |
//! | Snapshot retention (rule 1) | whole listing only | It deletes by age against every snapshot and the floor moves by it. |
//! | Level 1 to 8 prefix deletes (rule 2) | whole listing only | They are decided against the floor and the whole level. |
//! | Delete of a rejected output, read-back repair | whole listing only | They are decided against what the whole level holds. |
//!
//! The level-0 rule skips the whole gate's walk of history (no hole after the newest snapshot,
//! the newest state plans) and the self-check, which need every file. It runs only while the
//! last whole listing passed the gate ([`Gate::Clear`]). What can change that between two whole
//! listings is this engine's own work, which keeps the gate (compactions never delete and
//! retention's self-check passed), or a change to the replica by someone else, which the next whole
//! listing finds: the longest a deletion made on that stale clear gate can go unchecked is
//! [`FULL_LISTING_INTERVAL`]. What such a deletion removes is only level-0 files wholly covered by
//! level 1 to 8 files that were listed in the same pass.
//!
//! The cover cursor ([`advance_cover`]) is the key of the last level-1 file known to end before
//! the first level-0 file the engine still holds. Every level-1 file that could cover a level-0 file
//! from there on sorts after it, so a tail listing from it returns all of them, about the last five
//! minutes of compaction. A cursor that is too late only leaves a file for the whole listing.
//!
//! # The second-writer check
//!
//! - Caught at once, as before: a create-only upload another writer's file already holds is refused
//!   (that is the upload's own check, untouched).
//! - Caught on the pass that lists it: a file past this engine's position at level 0 (listed whole
//!   every pass) or at any level 1 to 8 after the remembered newest file. A live second writer
//!   uploads at level 0, so this is its first file's next pass, as before.
//! - Caught at the next whole listing: a file past this engine's position that sorts before the
//!   remembered newest file of its level (a foreign compaction of an older range), and a replica
//!   that lost files below the tail. The longest that takes is [`FULL_LISTING_INTERVAL`] of
//!   maintenance passes (a replica nothing is written to has no passes, as before).
//! - The regression check (the replica ended below what was confirmed) reads only listed files,
//!   never remembered ones, so a rolled-back bucket is still seen by the pass that lists level 0.

use std::collections::BTreeMap;
use std::time::Duration;

use lsr_ltx::{Timestamp, Txid, TxidRange};

use super::retain::short_level_zero;
use super::{
    Decided, DeleteJob, Due, Job, MaintainState, Maintenance, RetentionMode, Step, absorb,
    delete_first, plan_level, plan_snapshot, soonest,
};
use crate::created_at::{Need, Resolve, ResolvedCreatedAt};
use crate::inventory::{Inventory, InventoryError};
use crate::layout::{FileInfo, Level, cmp_range};
use crate::store::{ListAt, ObjectStore};

/// How long short listings may run without a whole one: the backstop for another writer's
/// files that a tail cannot see (see "The second-writer check"), and for the retention gate's
/// walk of history. Chosen against the cost of a whole listing, about 30 requests at 20,000
/// level-1 files, which at 15 minutes is 120 requests an hour against the thousands the tail
/// passes save (docs/decisions/D193.md).
pub(crate) const FULL_LISTING_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// What the last whole listing said about the retention gate.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) enum Gate {
    /// The listing read as a history (or retention is off).
    Clear,
    /// It did not: no short level-0 deletes.
    #[default]
    Held,
}

/// The newest file at each level 1 to 8 the engine knows, and when its whole listing is next due.
/// In memory only; [`Self::forget`] drops all of it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Remembered {
    newest: BTreeMap<Level, FileInfo>,
    cover: Option<TxidRange>,
    known: bool,
    full_by: Option<Timestamp>,
    retention_wake: Option<Timestamp>,
    gate: Gate,
    full_owed: bool,
}

impl Remembered {
    /// Drops everything: the next pass lists whole.
    pub(crate) fn forget(&mut self) {
        *self = Self::default();
    }

    /// The next pass lists whole (a refused or listed upload, a restore point released).
    pub(crate) const fn owe_full(&mut self) {
        self.full_owed = true;
    }

    /// Whether a short listing may stand in for a whole one at `now`.
    pub(crate) fn short_ok(&self, now: Timestamp) -> bool {
        self.known
            && !self.full_owed
            && self.gate == Gate::Clear
            && self.full_by.is_some_and(|by| now < by)
            && self.retention_wake.is_none_or(|wake| now < wake)
    }

    /// The last transaction of the newest known file at compaction level `level`.
    pub(crate) fn newest_txid(&self, level: u8) -> Option<Txid> {
        let level = Level::new(level)?;
        self.newest.get(&level).map(|info| info.key.range().max())
    }

    /// When snapshot retention next has something to delete, per the last whole decision.
    pub(crate) const fn retention_wake(&self) -> Option<Timestamp> {
        self.retention_wake
    }

    /// Learns from a whole listing and the decision made on it.
    pub(crate) fn learn_full(
        &mut self,
        inventory: &Inventory,
        now: Timestamp,
        config: &Maintenance,
        decided: &Decided,
    ) {
        self.newest.clear();
        for level in compaction_levels(config) {
            if let Some(last) = inventory.level(level).last() {
                self.newest.insert(level, *last);
            }
        }
        let first_zero = inventory
            .level(Level::ZERO)
            .first()
            .map(|info| info.key.range().min());
        self.cover = advance_cover(
            None,
            inventory.level(ONE),
            first_zero,
            self.newest.get(&ONE),
        );
        self.known = true;
        self.full_by = Some(now.saturating_add(FULL_LISTING_INTERVAL));
        self.retention_wake = decided.retention_wake;
        self.gate = match config.retention() {
            RetentionMode::Off => Gate::Clear,
            RetentionMode::Enforce => decided.gate,
        };
        // A delete above level 0 or a snapshot moves the floor: the next pass is whole too.
        self.full_owed = match &decided.due {
            Due::Run(Job::Snapshot(_)) => true,
            Due::Run(Job::Delete(job)) => job.keys().iter().any(|key| key.level() != Level::ZERO),
            Due::Run(Job::Compact(_)) | Due::Wait { .. } => false,
        };
    }

    /// Learns from a short listing; `settled_min` is [`super::retain::ShortZero::settled_min`].
    pub(crate) fn learn_short(
        &mut self,
        tail: &Tail,
        config: &Maintenance,
        settled_min: Option<Txid>,
    ) {
        for level in compaction_levels(config) {
            if let Some(last) = tail.listed.level(level).last() {
                self.note(*last);
            }
        }
        let first_zero = tail
            .listed
            .level(Level::ZERO)
            .first()
            .map(|info| info.key.range().min());
        self.cover = advance_cover(
            self.cover,
            tail.listed.level(ONE),
            settled_min.or(first_zero),
            self.newest.get(&ONE),
        );
    }

    /// A compaction output this engine confirmed.
    pub(crate) fn note(&mut self, info: FileInfo) {
        let level = info.key.level();
        if self
            .newest
            .get(&level)
            .is_none_or(|known| known.key < info.key)
        {
            self.newest.insert(level, info);
        }
    }

    /// The first pages a short pass asks for: level 0 and level 9 whole, and each compaction
    /// level after the lowest key its decisions need (below).
    pub(crate) fn starts(&self, config: &Maintenance) -> Vec<ListAt> {
        let zero_retention = matches!(
            (config.retention(), config.l0_retention()),
            (RetentionMode::Enforce, super::L0Retention::After(_))
        );
        let top = config.top().get();
        let mut starts = vec![ListAt::first(Level::ZERO)];
        for n in 1..=top {
            let Some(level) = Level::new(n) else { continue };
            // What is newer than the newest file known (a compaction destination's last
            // file and anything another writer put after it).
            let mut needs = vec![self.newest.get(&level).map(|info| info.key.range())];
            if n == 1 && zero_retention {
                needs.push(self.cover);
            }
            // The sources above the level above's last transaction.
            if n < top
                && let Some(above) = Level::new(n.saturating_add(1))
            {
                needs.push(
                    self.newest
                        .get(&above)
                        .map(|info| TxidRange::single(info.key.range().max())),
                );
            }
            starts.push(ListAt {
                level,
                after: lowest(&needs),
            });
        }
        starts.push(ListAt::first(Level::SNAPSHOT));
        starts
    }
}

const ONE: Level = match Level::new(1) {
    Some(level) => level,
    None => Level::ZERO,
};

/// Levels 1 to `top`, the compaction destinations.
fn compaction_levels(config: &Maintenance) -> impl Iterator<Item = Level> + use<> {
    (1..=config.top().get()).filter_map(Level::new)
}

/// The lowest of the cursors, or `None` (from the first file) when any is.
fn lowest(cursors: &[Option<TxidRange>]) -> Option<TxidRange> {
    let mut lowest: Option<TxidRange> = None;
    for (at, cursor) in cursors.iter().enumerate() {
        let cursor = (*cursor)?;
        lowest = Some(match lowest {
            Some(low) if at > 0 && cmp_range(low, cursor).is_le() => low,
            _ => cursor,
        });
    }
    lowest
}

/// The cover cursor after a listing of level 1: the key of the last file, in the maximal prefix
/// of `listed` (ascending, all after `previous`), whose last transaction is below `first_zero`,
/// the first transaction of the first level-0 file that still needs a cover. Every level-1 file
/// with a last transaction at or past it sorts after the result, so a listing after it returns
/// every file that could cover a level-0 file from `first_zero` on. With no level-0 file left
/// to cover, the newest level-1 file known.
fn advance_cover(
    previous: Option<TxidRange>,
    listed: &[FileInfo],
    first_zero: Option<Txid>,
    newest: Option<&FileInfo>,
) -> Option<TxidRange> {
    let Some(first_zero) = first_zero else {
        return newest.map(|info| info.key.range()).or(previous);
    };
    let mut cursor = previous;
    for info in listed {
        if info.key.range().max() >= first_zero {
            break;
        }
        cursor = Some(info.key.range());
    }
    cursor
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and time arithmetic on small bounded values"
)]
mod tests;

/// What a short pass listed: level 0 and 9 whole, and the files after the remembered cursors.
#[derive(Debug)]
pub(crate) struct Tail {
    /// Only what the store returned.
    listed: Inventory,
    /// The listed files and the remembered newest file of each level when the listing did not
    /// return it again: what compaction and snapshot planning read. Never used as a cover.
    planning: Inventory,
}

impl Tail {
    /// Lists what `remembered` says a short pass needs.
    ///
    /// # Errors
    /// As [`Inventory::load`].
    pub(crate) async fn load<S: ObjectStore>(
        store: &S,
        remembered: &Remembered,
        config: &Maintenance,
    ) -> Result<Self, InventoryError> {
        let listed = Inventory::load_from(store, &remembered.starts(config)).await?;
        let known = remembered.newest.values().filter(|info| {
            !listed
                .level(info.key.level())
                .iter()
                .any(|seen| seen.key == info.key)
        });
        let planning = Inventory::from_files(listed.files().iter().copied().chain(known.copied()))?;
        Ok(Self { listed, planning })
    }

    /// The newest transaction among the listed files only: what the second-writer and the
    /// regression checks compare, never a remembered record.
    pub(crate) fn newest_listed(&self) -> Option<Txid> {
        self.listed.newest()
    }
}

/// A short pass's decision.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct ShortDecided {
    /// What to do.
    pub(crate) due: Due,
    /// Where the level-0 scan stopped (see [`super::retain::ShortZero::settled_min`]).
    pub(crate) settled_min: Option<Txid>,
    /// When to ask again if the pass runs nothing, or after a level-0 delete.
    pub(crate) until: Option<Timestamp>,
}

/// The next job on a short listing, or when to ask again. Compaction and the snapshot as
/// [`super::due`] plans them (the files they read are all in `tail`), then the level-0 rule of the
/// short view. Never a delete above level 0 and never a hold: those need a whole listing.
pub(crate) fn due_short(
    tail: &Tail,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Resolve<ShortDecided> {
    match decide(tail, now, config, published, state, resolved) {
        Ok(decided) => Resolve::Done(decided),
        Err(file) => Resolve::Need(file),
    }
}

fn decide(
    tail: &Tail,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Result<ShortDecided, Need> {
    let run = |job| ShortDecided {
        due: Due::Run(job),
        settled_min: None,
        until: None,
    };
    let mut until: Option<Timestamp> = None;
    for level in compaction_levels(config) {
        let step = plan_level(&tail.planning, now, config, level, state, resolved)?;
        if let Some(job) = absorb(step, &mut until) {
            return Ok(run(job));
        }
    }
    let waits = state.backing_off_until(Level::SNAPSHOT, now);
    let step = match (waits, state.distrusted_at(Level::SNAPSHOT)) {
        (Some(retry), _) => Step::Wait(retry),
        (None, Some(bad)) => delete_first(bad),
        (None, None) => plan_snapshot(&tail.planning, now, config, published, resolved)?,
    };
    if let Some(job) = absorb(step, &mut until) {
        return Ok(run(job));
    }
    let wait = |until| ShortDecided {
        due: Due::Wait { until, held: None },
        settled_min: None,
        until,
    };
    if config.retention() == RetentionMode::Off {
        return Ok(wait(until));
    }
    let zero = match short_level_zero(&tail.listed, now, config, published, state, resolved) {
        Resolve::Done(zero) => zero,
        Resolve::Need(file) => return Err(file),
    };
    let settled_min = zero.settled_min;
    Ok(match DeleteJob::new(zero.keys) {
        Ok(job) => ShortDecided {
            due: Due::Run(Job::Delete(job)),
            settled_min,
            until: soonest(until, zero.wake),
        },
        Err(_) => ShortDecided {
            settled_min,
            ..wait(soonest(until, zero.wake))
        },
    })
}

//! Short listings: what a maintenance pass reads between whole ones.
//!
//! A whole listing pages through every file of all ten levels, so its cost grows with the
//! replica's retention: a week of level-1 files is about 20,000 entries and 21 requests, and
//! maintenance used to run one whole listing at least every 15 seconds while the database was
//! written (issue 324, and upstream's report of the same shape). The engine now remembers, in
//! memory only, the newest file it knows at each level 1 to 8 ([`Remembered`]), and between
//! whole listings a pass asks the store only for what its decision reads ([`Plan`], [`Tail`]): level 0,
//! the tail of a compaction level whose window is open and the level below it, level 1 after the
//! cover cursor and level 9 when level-0 retention is due. A pass with nothing due lists nothing, and the
//! passes after this engine's own jobs reuse a listing at most five seconds old ([`Carry`], which keeps the time it was really made, gains every output this engine confirms meanwhile, and is never used by a pass that may delete).
//! A level whose own output closed its window is quiet until the next window; level-0 retention is
//! checked every [`Maintenance::retention_check_every`]. Nothing is written to disk and nothing survives a fault,
//! a stop or a restart: [`Remembered::forget`] drops it all and the next pass is whole.
//!
//! # When a pass is whole
//!
//! [`Remembered::short_ok`] is false, and the pass lists everything and decides with [`super::due`]
//! exactly as before, at boot and after any fault (the actor calls `forget`), after a refused or
//! listed upload, a step back of the clock, a compaction that found an input gone, a level whose sources no longer follow its newest file or a compaction that closed such a hole (see "Holes" in [`super`]), while a restore point is pinned, a rejected output is distrusted or the boot
//! read-back is owed, after a retention delete or a snapshot, when the last whole listing did not
//! pass the retention gate ([`Gate::Held`]), when snapshot retention next has
//! something to delete ([`Remembered::retention_wake`]), and at the latest [`FULL_LISTING_INTERVAL`]
//! after the last whole listing.
//!
//! # What each decision reads
//!
//! | Decision | Listing it reads | Why that is enough |
//! |---|---|---|
//! | Compaction into level `d` | level `d` after what is known, level `d-1` after the level's last transaction | The planner reads the destination's last file and the sources above its last transaction, nothing older. Level 0 and level 9 are listed whole. A remembered newest file can only be a file that was there: retention never deletes a level's last file, and a file another process removed is caught at the next whole listing. A tail whose first source does not start right after the destination's newest file is not planned from: the next pass is whole. |
//! | Snapshot | level 9 whole, the newest position over what was listed or known | The planner reads the newest snapshot start and the newest transaction. |
//! | Level-0 retention | level 0 whole, level 9 whole, level 1 to 8 files after the cursor | [`super::retain::short_level_zero`]: its deletes are a subset of the whole rule's, because its anchor reads the newest known file of every level 1 to 8 as the whole anchor does (a remembered one for its age only). A covering file must be listed in this pass, never remembered. |
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
//! - With a record above the configured top, level-0 retention is decided on whole listings only
//!   (see [`Remembered::plan`]): the record may name a file another process removed, which would
//!   hold the anchor too high.
//! - Never seen by a short pass: a foreign snapshot until the snapshot window or retention lists
//!   level 9, and any file at a level above the configured top, which no short pass lists (the
//!   anchor reads the remembered newest file of each such level, learned at the whole listing). The
//!   gap is accepted in docs/decisions/D193.md.
//! - The regression check (the replica ended below what was confirmed) reads only listed files,
//!   never remembered ones, so a rolled-back bucket is still seen by the pass that lists level 0.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use lsr_ltx::{Timestamp, Txid, TxidRange};

use super::retain::short_level_zero;
use super::{
    Decided, DeleteJob, Due, Job, MaintainState, Maintenance, RetentionMode, Step, View, absorb,
    delete_first, plan_level, plan_snapshot, soonest, window_closed_by,
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

/// How old a listing may be and still stand in for the one a pass directly after this engine's
/// own job would make: the job changed nothing it listed but the output it noted.
const CARRY_MAX_AGE_MILLIS: i64 = 5_000;

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
    /// The first level-0 transaction the last listing showed: while it stays the same (a file the
    /// rules never delete), a whole listing keeps the cursor a short pass advanced past it.
    zero_min: Option<Txid>,
    known: bool,
    full_by: Option<Timestamp>,
    retention_wake: Option<Timestamp>,
    gate: Gate,
    full_owed: bool,
    /// Levels whose compaction window this engine has seen closed (the destination's newest file
    /// was created inside it), until the next one opens: no listing is needed for them before.
    quiet: BTreeMap<Level, Timestamp>,
    /// The same for the snapshot window.
    snapshot_quiet: Option<Timestamp>,
    /// When a pass last decided level-0 retention.
    checked_at: Option<Timestamp>,
    /// Levels whose newest file is an output this engine confirmed since the last pass: that
    /// pass need not list them again to learn it.
    fresh: BTreeSet<Level>,
    /// The listing the last pass made, for the pass that directly follows this engine's own job.
    carry: Option<Carry>,
    /// Levels whose newest file is one this engine's own job confirmed and no listing has
    /// replaced: the destination's last file is known without listing the level after it.
    own: BTreeSet<Level>,
    /// The last whole listing held no snapshot and no level-1 file: the first snapshot is
    /// planned before any level (D224), and only a whole listing plans it, so a short pass
    /// that came first would compact level 1 while the snapshot waited.
    first_snapshot_owed: bool,
}

/// A listing kept for the pass right after this engine's own job (see
/// [`Remembered::starts`]), and what it covers. It stands in for a listing made now, so every
/// output confirmed after it was made is added to it ([`Self::with`]): a pass that knew only
/// the newest output of a level would plan the level above from that file alone.
#[derive(Clone, Debug)]
struct Carry {
    listed: Inventory,
    covers: Vec<ListAt>,
    confirmed: Option<Txid>,
    at: Timestamp,
}

impl Carry {
    /// The listing with a file this engine has confirmed since it was made. `None` when the
    /// listing shows another object under that key (the output replaced it): it no longer
    /// stands in for the store, and the next pass lists.
    fn with(mut self, info: FileInfo) -> Option<Self> {
        let output = Inventory::from_files([info]).ok()?;
        self.listed = self.listed.replacing_levels(&[], &output).ok()?;
        Some(self)
    }
}

/// What a short pass needs to decide, given what is known: the compaction levels whose window is
/// open, whether the snapshot's is, and whether level-0 retention is due.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Compaction levels to plan.
    pub(crate) active: Vec<Level>,
    /// Plan the snapshot.
    pub(crate) snapshot: bool,
    /// Decide level-0 retention.
    pub(crate) retention: bool,
}

impl Plan {
    /// Nothing is due: no request is needed.
    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty() && !self.snapshot && !self.retention
    }
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
            && !self.first_snapshot_owed
            && self.gate == Gate::Clear
            && self.full_by.is_some_and(|by| now < by)
            && self.retention_wake.is_none_or(|wake| now < wake)
    }

    /// The last transaction of the newest known file at compaction level `level`.
    pub(crate) fn newest_txid(&self, level: u8) -> Option<Txid> {
        let level = Level::new(level)?;
        self.newest.get(&level).map(|info| info.key.range().max())
    }

    /// Whether a file covering `range` at `level` starts at the transaction after the newest
    /// file known there, or none is known. Every compaction a short pass plans does.
    pub(crate) fn follows_newest(&self, level: Level, range: TxidRange) -> bool {
        self.newest
            .get(&level)
            .is_none_or(|known| known.key.range().max().next() == Some(range.min()))
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
        // Every level 1 to 8, not only the configured ones: the retention anchor reads the
        // newest file of each, and a short pass cannot list the levels above the top.
        for level in (1..=8).filter_map(Level::new) {
            if let Some(last) = inventory.level(level).last() {
                self.newest.insert(level, *last);
            }
        }
        let first_zero = inventory
            .level(Level::ZERO)
            .first()
            .map(|info| info.key.range().min());
        let cover = advance_cover(
            None,
            inventory.level(ONE),
            first_zero,
            self.newest.get(&ONE),
        );
        // A level-0 file no rule deletes holds the first-listed position back at every whole
        // listing, and the next retention check would list level 1 from far behind it: keep the
        // further cursor a short pass reached while that file is still the first.
        self.cover = match (self.cover, cover) {
            (Some(kept), Some(new))
                if self.zero_min == first_zero && cmp_range(new, kept).is_lt() =>
            {
                Some(kept)
            }
            (_, new) => new,
        };
        self.zero_min = first_zero;
        self.known = true;
        self.first_snapshot_owed =
            super::snapshot_end(inventory).is_none() && inventory.level(ONE).is_empty();
        self.quiet.clear();
        self.snapshot_quiet = None;
        self.fresh.clear();
        self.carry = None;
        self.own.clear();
        self.checked_at = matches!(decided.due, Due::Wait { .. }).then_some(now);
        self.full_by = Some(now.saturating_add(FULL_LISTING_INTERVAL));
        // A compaction that comes first leaves retention unplanned on this listing, which says
        // nothing new about when it is next due: keep the wake an earlier whole listing found.
        self.retention_wake = match &decided.due {
            Due::Run(Job::Compact(_)) => decided
                .retention_wake
                .or(self.retention_wake.filter(|wake| *wake > now)),
            Due::Run(_) | Due::Wait { .. } => decided.retention_wake,
        };
        self.gate = match config.retention() {
            RetentionMode::Off => Gate::Clear,
            RetentionMode::Enforce => decided.gate,
        };
        // A delete above level 0 or a snapshot moves the floor: the next pass is whole too.
        // So is the one after a compaction that closes a hole (its output is not the newest
        // file of its level): only a whole listing shows the next hole and what can close it.
        self.full_owed = match &decided.due {
            Due::Run(Job::Snapshot(_)) => true,
            Due::Run(Job::Delete(job)) => job.keys().iter().any(|key| key.level() != Level::ZERO),
            Due::Run(Job::Compact(job)) => self
                .newest_txid(job.level().get())
                .is_some_and(|newest| job.inputs().range().max() < newest),
            Due::Wait { .. } => false,
        };
    }

    /// Learns from a short pass: what it listed, what it found quiet, and whether it decided
    /// retention. `settled_min` is [`super::retain::ShortZero::settled_min`].
    pub(crate) fn learn_short(
        &mut self,
        tail: &Tail,
        config: &Maintenance,
        decided: &ShortDecided,
        now: Timestamp,
        confirmed: Option<Txid>,
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
            decided.settled_min.or(first_zero),
            self.newest.get(&ONE),
        );
        self.zero_min = first_zero;
        for (level, until) in &decided.quiet {
            self.quiet.insert(*level, *until);
        }
        if let Some(until) = decided.snapshot_quiet {
            self.snapshot_quiet = Some(until);
        }
        if decided.retention_decided {
            self.checked_at = Some(now);
        }
        self.fresh.clear();
        self.carry = Some(Carry {
            listed: tail.listed.clone(),
            covers: tail.covers.clone(),
            confirmed,
            at: tail.made_at,
        });
    }

    /// A pass that asked for nothing consumed what the last job taught it.
    pub(crate) fn settle(&mut self) {
        self.fresh.clear();
    }

    /// A compaction output this engine confirmed: the newest file of its level, and when it
    /// closes the level's window (its time, that of its last input, is inside it), the level is
    /// quiet until the next one.
    pub(crate) fn note_output(&mut self, info: FileInfo, config: &Maintenance, now: Timestamp) {
        let level = info.key.level();
        self.note(info);
        // The carried listing stands in for one made now, which would show this file: a pass
        // that plans from it must see every output since, not only the newest of each level.
        self.carry = self.carry.take().and_then(|carry| carry.with(info));
        self.own.insert(level);
        self.fresh.insert(level);
        if let Some(next) = config
            .interval(level)
            .and_then(|interval| window_closed_by(&info, now, interval))
        {
            self.quiet.insert(level, next);
        }
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
            self.own.remove(&level);
        }
    }

    /// Whether level `level`'s compaction needs a look at `now`: its window is not known closed
    /// and it has sources above its newest file as far as is known.
    fn active(&self, level: Level, now: Timestamp, confirmed: Option<Txid>) -> bool {
        if self.quiet.get(&level).is_some_and(|until| now < *until) {
            return false;
        }
        // Sources above the destination's last file: what this engine uploaded (level 0) or
        // compacted (the levels above) past it. Another writer's files wait for a listing.
        let frontier = match Level::new(level.get().saturating_sub(1)) {
            Some(below) if below == Level::ZERO => confirmed,
            Some(below) => self.newest.get(&below).map(|info| info.key.range().max()),
            None => None,
        };
        let dest = self.newest.get(&level).map(|info| info.key.range().max());
        !matches!((frontier, dest), (Some(source), Some(dest)) if source <= dest)
    }

    /// Whether the last whole listing found a file at a level above the configured top. A short
    /// pass never lists those levels, so a record there may name a file another process has
    /// since removed, which would hold the retention anchor too high: with one remembered, level-0
    /// retention is decided on whole listings only.
    fn above_top(&self, config: &Maintenance) -> bool {
        self.newest.keys().any(|level| *level > config.top())
    }

    /// What a pass at `now` has to decide.
    pub(crate) fn plan(
        &self,
        config: &Maintenance,
        now: Timestamp,
        confirmed: Option<Txid>,
    ) -> Plan {
        let active: Vec<Level> = compaction_levels(config)
            .filter(|level| self.active(*level, now, confirmed))
            .collect();
        // A compaction that comes first leaves retention undecided and the next pass lists it
        // all again: at the default interval retention waits for a pass with no compaction to
        // plan, unless it is a whole check interval overdue. A set interval is kept as set.
        let due = |after: i64| {
            self.checked_at.is_none_or(|at| {
                now.unix_millis()
                    >= at
                        .unix_millis()
                        .saturating_add(self.check_millis(config, after))
            })
        };
        Plan {
            snapshot: self.snapshot_quiet.is_none_or(|until| now >= until),
            retention: matches!(
                (config.retention(), config.l0_retention()),
                (RetentionMode::Enforce, super::L0Retention::After(_))
            ) && !self.above_top(config)
                && if active.is_empty() {
                    due(1)
                } else {
                    due(config.retention_check_behind_a_job())
                },
            active,
        }
    }

    /// `times` retention check intervals, in milliseconds.
    fn check_millis(&self, config: &Maintenance, times: i64) -> i64 {
        i64::try_from(config.retention_check_every().millis())
            .unwrap_or(i64::MAX)
            .saturating_mul(times)
    }

    /// The earliest time a quiet pass has something to look at: a window opening, the next
    /// retention check, a retention wake or the whole listing's backstop.
    pub(crate) fn next_wake(&self, config: &Maintenance, now: Timestamp) -> Option<Timestamp> {
        let check = |times: i64| {
            self.checked_at.map(|at| {
                Timestamp::from_unix_millis(
                    at.unix_millis()
                        .saturating_add(self.check_millis(config, times)),
                )
            })
        };
        self.quiet
            .values()
            .copied()
            .chain(self.snapshot_quiet)
            .chain(check(1))
            .chain(check(2))
            .chain(self.retention_wake)
            .chain(self.full_by)
            .filter(|at| *at > now)
            .min()
    }

    /// The first pages a short pass asks for. Level 0 always, whole. A compaction level is listed
    /// after its newest file when its window is open (to see what follows the destination's last
    /// file) unless this engine confirmed that file a moment ago, after the level above's last
    /// transaction when that level's window is open (its sources), and, for level 1 when
    /// retention is due, after the cover cursor. Level 9 when the snapshot's window is open or
    /// retention is due.
    pub(crate) fn starts(&self, config: &Maintenance, plan: &Plan) -> Vec<ListAt> {
        let top = config.top().get();
        let mut starts = vec![ListAt::first(Level::ZERO)];
        for n in 1..=top {
            let Some(level) = Level::new(n) else { continue };
            let above = Level::new(n.saturating_add(1)).filter(|_| n < top);
            let own = plan.active.contains(&level)
                && !self.fresh.contains(&level)
                && !self.own.contains(&level);
            let source = above.is_some_and(|above| plan.active.contains(&above));
            let cover = n == 1 && plan.retention;
            if !(own || source || cover) {
                continue;
            }
            // What is newer than the newest file known (a compaction destination's last file
            // and anything another writer put after it).
            let mut needs = vec![self.newest.get(&level).map(|info| info.key.range())];
            if cover {
                needs.push(self.cover);
            }
            // The sources above the level above's last transaction.
            if source && let Some(above) = above {
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
        if plan.snapshot || plan.retention {
            starts.push(ListAt::first(Level::SNAPSHOT));
        }
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

/// What a short pass listed: level 0 whole, and the files after the remembered cursors of the
/// levels its plan reads.
#[derive(Debug)]
pub(crate) struct Tail {
    /// Only what the store returned, or the pass right before returned and this engine has
    /// confirmed since (see [`Carry`]).
    listed: Inventory,
    /// The listed files and the remembered newest file of each level when the listing did not
    /// return it again: what compaction and snapshot planning read. Never used as a cover.
    planning: Inventory,
    /// What the pass decides.
    pub(crate) plan: Plan,
    /// The pages `listed` covers.
    covers: Vec<ListAt>,
    /// What this engine had confirmed when level 0 in `listed` was listed: the replica may not
    /// be judged to have lost files below what was confirmed since.
    baseline: Option<Txid>,
    /// When the oldest part of `listed` was really listed.
    made_at: Timestamp,
}

/// Whether the pages listed from `have` include everything a listing from `want` returns.
fn covered(have: &[ListAt], want: &ListAt) -> bool {
    have.iter().any(|at| {
        at.level == want.level
            && match (at.after, want.after) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(has), Some(wants)) => cmp_range(has, wants).is_le(),
            }
    })
}

impl Tail {
    /// Lists what `remembered` says the plan needs. Right after this engine's own job the
    /// listing the last pass made stands in for the levels it covered (the job changed nothing
    /// it listed but the output the engine noted); level 0 is listed again when an upload was
    /// confirmed since.
    ///
    /// # Errors
    /// As [`Inventory::load`].
    pub(crate) async fn load<S: ObjectStore>(
        store: &S,
        remembered: &Remembered,
        config: &Maintenance,
        plan: Plan,
        now: Timestamp,
        confirmed: Option<Txid>,
    ) -> Result<Self, InventoryError> {
        let wanted = remembered.starts(config, &plan);
        // Never for a pass that may delete: a delete is decided from a listing made in the same
        // pass. A carried listing keeps the time it was really made (a pass that stands on it
        // saves it again with that time), so a run of this engine's own jobs cannot stretch
        // its age past [`CARRY_MAX_AGE_MILLIS`].
        let carried = remembered.carry.as_ref().filter(|carry| {
            !remembered.fresh.is_empty()
                && !plan.retention
                && now >= carry.at
                && now.unix_millis().saturating_sub(carry.at.unix_millis()) <= CARRY_MAX_AGE_MILLIS
        });
        let mut baseline = confirmed;
        let carried_at = carried.map(|carry| carry.at);
        let (listed, covers) = match carried {
            Some(carry) => {
                let missing: Vec<ListAt> = wanted
                    .iter()
                    .filter(|want| {
                        !covered(&carry.covers, want)
                            || (want.level == Level::ZERO && carry.confirmed != confirmed)
                    })
                    .copied()
                    .collect();
                let fetched = Inventory::load_from(store, &missing).await?;
                let replaced: Vec<Level> = missing.iter().map(|at| at.level).collect();
                if !replaced.contains(&Level::ZERO) {
                    baseline = carry.confirmed;
                }
                let listed = carry.listed.replacing_levels(&replaced, &fetched)?;
                let mut covers: Vec<ListAt> = carry
                    .covers
                    .iter()
                    .filter(|at| !replaced.contains(&at.level))
                    .copied()
                    .collect();
                covers.extend(missing);
                (listed, covers)
            }
            None => (Inventory::load_from(store, &wanted).await?, wanted),
        };
        let known = remembered.newest.values().filter(|info| {
            !listed
                .level(info.key.level())
                .iter()
                .any(|seen| seen.key == info.key)
        });
        let planning = Inventory::from_files(listed.files().iter().copied().chain(known.copied()))?;
        Ok(Self {
            listed,
            planning,
            plan,
            covers,
            baseline,
            made_at: carried_at.unwrap_or(now),
        })
    }

    /// The newest file at each level 1 to 8 this engine knows, listed or remembered: what the
    /// retention anchor reads (as ages only, never as covers).
    pub(crate) fn newest_known(&self) -> Vec<FileInfo> {
        (1..=8)
            .filter_map(Level::new)
            .filter_map(|level| self.planning.level(level).last().copied())
            .collect()
    }

    /// What the engine had confirmed when level 0 was listed.
    pub(crate) const fn baseline(&self) -> Option<Txid> {
        self.baseline
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
    /// Levels whose destination was created inside the current window: nothing to plan before
    /// the instant.
    pub(crate) quiet: Vec<(Level, Timestamp)>,
    /// The same for the snapshot.
    pub(crate) snapshot_quiet: Option<Timestamp>,
    /// Level-0 retention was decided on this listing (no compaction or snapshot came first).
    pub(crate) retention_decided: bool,
    /// Nothing was decided: a level's sources do not follow its newest file as far as this
    /// listing shows, and the pass is made again on a whole one.
    pub(crate) whole: bool,
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
    let mut quiet = Vec::new();
    let mut snapshot_quiet = None;
    let run = |job, quiet| ShortDecided {
        due: Due::Run(job),
        settled_min: None,
        until: None,
        quiet,
        snapshot_quiet: None,
        retention_decided: false,
        whole: false,
    };
    let mut until: Option<Timestamp> = None;
    for level in compaction_levels(config).filter(|level| tail.plan.active.contains(level)) {
        let step = plan_level(
            &tail.planning,
            now,
            config,
            level,
            state,
            resolved,
            View::Tail,
        )?;
        match step {
            Step::Wait(at) => quiet.push((level, at)),
            Step::Whole => {
                return Ok(ShortDecided {
                    due: Due::Wait {
                        until: Some(now),
                        held: None,
                    },
                    settled_min: None,
                    until: Some(now),
                    quiet,
                    snapshot_quiet: None,
                    retention_decided: false,
                    whole: true,
                });
            }
            Step::Run(_) | Step::Idle => {}
        }
        if let Some(job) = absorb(step, &mut until) {
            return Ok(run(job, quiet));
        }
    }
    if tail.plan.snapshot {
        let waits = state.backing_off_until(Level::SNAPSHOT, now);
        let step = match (waits, state.distrusted_at(Level::SNAPSHOT)) {
            (Some(retry), _) => Step::Wait(retry),
            (None, Some(bad)) => delete_first(bad),
            (None, None) => plan_snapshot(&tail.planning, now, config, published, state, resolved)?,
        };
        if let Step::Wait(at) = step {
            snapshot_quiet = Some(at);
        }
        if let Some(job) = absorb(step, &mut until) {
            return Ok(run(job, quiet));
        }
    }
    let wait = |until, quiet, snapshot_quiet| ShortDecided {
        due: Due::Wait { until, held: None },
        settled_min: None,
        until,
        quiet,
        snapshot_quiet,
        retention_decided: false,
        whole: false,
    };
    if config.retention() == RetentionMode::Off || !tail.plan.retention {
        return Ok(wait(until, quiet, snapshot_quiet));
    }
    let zero = match short_level_zero(
        &tail.listed,
        &tail.newest_known(),
        now,
        config,
        published,
        state,
        resolved,
    ) {
        Resolve::Done(zero) => zero,
        Resolve::Need(file) => return Err(file),
    };
    let settled_min = zero.settled_min;
    Ok(match DeleteJob::new(zero.keys) {
        Ok(job) => ShortDecided {
            due: Due::Run(Job::Delete(job)),
            settled_min,
            until: soonest(until, zero.wake),
            quiet,
            snapshot_quiet,
            retention_decided: true,
            whole: false,
        },
        Err(_) => ShortDecided {
            settled_min,
            retention_decided: true,
            ..wait(soonest(until, zero.wake), quiet, snapshot_quiet)
        },
    })
}

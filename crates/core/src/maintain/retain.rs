//! Retention: which files may be deleted, and when.
//!
//! [`retention`] is a pure function of a listing, an instant and a schedule,
//! like [`super::due`], which calls it only when no compaction or snapshot
//! is ready. A wrong delete is unrecoverable, so every rule is fail-closed:
//! when a rule can be read two ways, the reading that deletes less is the one
//! implemented.
//!
//! Trust gate. Before any rule runs the listing must be trustworthy; the first
//! failure is a [`Hold`] and nothing is deleted. A name this code cannot
//! parse, a malformed snapshot, a listing missing a file this process knows it
//! wrote, a hole after the newest snapshot, a history that does not plan, a
//! snapshot or level-0 file without a real stamp, and snapshots whose stamps
//! run backwards all mean the listing cannot be read as a history. A hole
//! further back than the newest snapshot does not hold: it is history already
//! lost, the rules never widen it, and holding on it would pin every delete
//! after one Go-era loss.
//!
//! Time base. Ages come from `FileInfo::created_at`, the header stamp, and are
//! measured from the anchor, `min(now, newest stamp in the listing)`. A clock
//! that jumps ahead deletes nothing the replica's own newest stamp would not,
//! and a clock behind only makes files look young. Go on S3 uses the upload
//! time (`LastModified`), never earlier than the header stamp, so Go keeps
//! these files at least as long as this code does.
//!
//! Rules, in the order they are computed:
//! 1. Snapshots (ascending): the first retained one is the first not created
//!    before `anchor - snapshot_retention`, or the last one when all are older.
//!    Earlier snapshots are deleted. The floor is the max TXID of the snapshot
//!    just before the first retained one (Go: `db.go:3122-3185`, fix 2b34013),
//!    so a restore that planned on that snapshot still finds its files.
//! 2. Levels 1 to 8: a prefix of each level, the files whose max is below the
//!    floor, never the level's last file.
//! 3. Level 0, only with `L0Retention::After`, and only when a snapshot
//!    exists: a prefix of the level, the files created at or before
//!    `anchor - l0_retention` whose whole range lies inside the union of the
//!    level 1 to 8 files rule 2 keeps, or wholly below the floor. Never the
//!    last file. Go compares only L1's max; a file that L1 does not cover
//!    stays here. The floor clause is the one place this deletes more than
//!    coverage alone: rule 2 removes the level 1 file that covered such a
//!    file, so without it the file would stay forever and, as the prefix stops
//!    at it, block every level-0 delete above it. Nothing under the floor is
//!    needed by a retained snapshot or by a restore planned on the floor's.
//!
//! The candidates are sorted by key and cut to [`MAX_DELETE`]. Key order puts
//! levels 0 to 8 before the snapshots, so the floor's snapshot is still listed
//! while the files under it go, and the next call computes the same floor.
//! While a long run of jobs is in flight an older, doomed snapshot can have
//! lost files its chain needed; it is deleted by the last job. A restore of
//! the retained history never loses a file.
//!
//! Self-check: the job's keys are removed from the listing and `plan` must
//! still reach the same newest transaction. It is unreachable if the rules are
//! right; it exists so a bug in them costs a hold, not data.

use std::time::Duration;

use lsr_ltx::{Timestamp, Txid};

use super::{DeleteJob, Hold, L0Retention, MAX_DELETE, Maintenance};
use crate::inventory::{Inventory, is_snapshot_start};
use crate::layout::{FileInfo, FileKey, Level};
use crate::plan::{PlanOutcome, RestoreTarget, plan};

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and time arithmetic on small bounded values"
)]
mod tests;

#[cfg(test)]
thread_local! {
    /// Test hook: level 0 deletes ignore coverage, so the self-check has
    /// something to catch.
    static BREAK_L0_COVERAGE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// What retention wants.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) enum Retention {
    /// Delete these keys.
    Delete(DeleteJob),
    /// Nothing to delete now; `until` is when that may change without a
    /// new write.
    Wait { until: Option<Timestamp> },
    /// Deletions are refused.
    Held(Hold),
}

/// The next delete job, or why there is none. See the module docs.
pub(super) fn retention(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
) -> Retention {
    if let Some(hold) = gate(inventory, published) {
        return Retention::Held(hold);
    }
    let Some(newest_stamp) = inventory.files().iter().map(|f| f.created_at).max() else {
        return Retention::Wait { until: None };
    };
    let anchor = now.unix_millis().min(newest_stamp.unix_millis());
    let found = candidates(inventory, anchor, newest_stamp.unix_millis(), config);
    let until = found.wake.map(Timestamp::from_unix_millis);
    if found.keys.is_empty() {
        return Retention::Wait { until };
    }
    let mut keys = found.keys;
    keys.sort();
    keys.truncate(MAX_DELETE);
    let Ok(job) = DeleteJob::new(keys) else {
        // Sorted, distinct, 1 to MAX_DELETE keys: unreachable, and a hold of
        // nothing is the fail-closed answer if it ever is not.
        return Retention::Wait { until };
    };
    match self_check(inventory, &job) {
        Some(hold) => Retention::Held(hold),
        None => Retention::Delete(job),
    }
}

fn gate(inventory: &Inventory, published: Option<Txid>) -> Option<Hold> {
    foreign_names(inventory)
        .or_else(|| {
            inventory
                .malformed_snapshots()
                .first()
                .map(|key| Hold::MalformedSnapshot { key: *key })
        })
        .or_else(|| listing_behind(inventory, published))
        .or_else(|| hole(inventory))
        .or_else(|| latest_unplannable(inventory))
        .or_else(|| unstamped(inventory))
        .or_else(|| stamps_backwards(inventory))
}

fn foreign_names(inventory: &Inventory) -> Option<Hold> {
    Level::ALL.iter().find_map(|level| {
        let count = inventory.skipped(*level);
        (count > 0).then_some(Hold::ForeignNames {
            level: *level,
            count,
        })
    })
}

fn listing_behind(inventory: &Inventory, published: Option<Txid>) -> Option<Hold> {
    let published = published?;
    let listed = inventory.newest();
    (listed < Some(published)).then_some(Hold::ListingBehind { listed, published })
}

/// The level-9 files a restore can start from, ascending.
fn snapshots(inventory: &Inventory) -> Vec<&FileInfo> {
    inventory
        .level(Level::SNAPSHOT)
        .iter()
        .filter(|info| is_snapshot_start(info))
        .collect()
}

/// A transaction after the newest snapshot, up to the newest one anywhere,
/// that no file of levels 0 to 8 covers. Checked before the plan so the hold
/// names the cause: a hole there also makes `Latest` unplannable.
fn hole(inventory: &Inventory) -> Option<Hold> {
    let newest = inventory.newest()?;
    let snapshot = snapshots(inventory)
        .last()
        .map(|info| info.key.range().max())?;
    let mut files: Vec<&FileInfo> = inventory
        .files()
        .iter()
        .filter(|info| !info.key.level().is_snapshot() && info.key.range().max() > snapshot)
        .collect();
    files.sort_by_key(|info| info.key.range().min());
    let mut reach = snapshot;
    for info in files {
        let range = info.key.range();
        if reach.next().is_some_and(|one_past| range.min() > one_past) {
            return Some(Hold::Hole {
                after: reach,
                next: Some(range.min()),
            });
        }
        reach = reach.max(range.max());
    }
    (reach < newest).then_some(Hold::Hole {
        after: reach,
        next: None,
    })
}

fn latest_unplannable(inventory: &Inventory) -> Option<Hold> {
    let newest = inventory.newest()?;
    match plan(inventory, RestoreTarget::Latest) {
        PlanOutcome::Plan(chain) if chain.reaches() == newest => None,
        // A plan that stops short is a missing tail; the planner never
        // returns one, and the hold must not claim a plan.
        PlanOutcome::Plan(chain) => Some(Hold::Hole {
            after: chain.reaches(),
            next: None,
        }),
        outcome @ (PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::Gap { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. }) => Some(Hold::LatestUnplannable { outcome }),
    }
}

fn unstamped(inventory: &Inventory) -> Option<Hold> {
    inventory
        .files()
        .iter()
        .find(|info| {
            let aged = info.key.level() == Level::ZERO || info.key.level().is_snapshot();
            aged && info.created_at.unix_millis() <= 0
        })
        .map(|info| Hold::Unstamped { key: info.key })
}

fn stamps_backwards(inventory: &Inventory) -> Option<Hold> {
    snapshots(inventory).windows(2).find_map(|pair| match pair {
        [earlier, later] if earlier.created_at > later.created_at => {
            Some(Hold::SnapshotStampsBackwards {
                earlier: earlier.key,
                later: later.key,
            })
        }
        _ => None,
    })
}

/// What the rules chose, before the cut and the self-check.
#[derive(Default)]
struct Found {
    keys: Vec<FileKey>,
    /// The earliest instant a rule would delete more, in anchor time.
    wake: Option<i64>,
}

impl Found {
    fn wake_at(&mut self, at: i64, newest_stamp: i64) {
        // The anchor never passes the newest stamp, so a later instant is
        // never reached without a new write.
        if at <= newest_stamp {
            self.wake = Some(self.wake.map_or(at, |soonest| soonest.min(at)));
        }
    }
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn candidates(
    inventory: &Inventory,
    anchor: i64,
    newest_stamp: i64,
    config: &Maintenance,
) -> Found {
    let mut found = Found::default();
    let snaps = snapshots(inventory);
    let Some(last) = snaps.len().checked_sub(1) else {
        return found;
    };
    let retention = millis(config.snapshot_retention());
    let cutoff = anchor.saturating_sub(retention);
    let first = snaps
        .iter()
        .position(|info| info.created_at.unix_millis() >= cutoff)
        .unwrap_or(last);
    if let Some(retained) = snaps.get(first).filter(|_| first < last) {
        let aged_out = retained
            .created_at
            .unix_millis()
            .saturating_add(retention)
            .saturating_add(1);
        found.wake_at(aged_out, newest_stamp);
    }
    let floor = first
        .checked_sub(1)
        .and_then(|before| snaps.get(before))
        .map(|info| info.key.range().max());
    let kept = levels_one_to_eight(inventory, floor, &mut found.keys);
    if let L0Retention::After(age) = config.l0_retention() {
        level_zero(
            inventory,
            floor,
            (anchor, newest_stamp),
            age,
            &kept,
            &mut found,
        );
    }
    found
        .keys
        .extend(snaps.iter().take(first).map(|info| info.key));
    found
}

/// Queues each level's deletable prefix; returns the files that stay.
fn levels_one_to_eight(
    inventory: &Inventory,
    floor: Option<Txid>,
    out: &mut Vec<FileKey>,
) -> Vec<FileInfo> {
    let mut kept = Vec::new();
    for level in Level::ALL.iter().skip(1).take(8) {
        let files = inventory.level(*level);
        let deletable = floor.map_or(0, |floor| {
            files
                .iter()
                .take(files.len().saturating_sub(1))
                .take_while(|info| info.key.range().max() < floor)
                .count()
        });
        out.extend(files.iter().take(deletable).map(|info| info.key));
        kept.extend(files.iter().skip(deletable).copied());
    }
    kept
}

/// Level 0: a prefix, covered and aged, never the last file.
fn level_zero(
    inventory: &Inventory,
    floor: Option<Txid>,
    (anchor, newest_stamp): (i64, i64),
    age: Duration,
    kept: &[FileInfo],
    found: &mut Found,
) {
    let cover = merged_ranges(kept);
    let cutoff = anchor.saturating_sub(millis(age));
    let files = inventory.level(Level::ZERO);
    for info in files.iter().take(files.len().saturating_sub(1)) {
        let below = floor.is_some_and(|floor| info.key.range().max() < floor);
        if !below && !covered(&cover, info) {
            return;
        }
        let created = info.created_at.unix_millis();
        if created > cutoff {
            found.wake_at(created.saturating_add(millis(age)), newest_stamp);
            return;
        }
        found.keys.push(info.key);
    }
}

/// The union of the files' ranges as sorted, disjoint, non-adjacent spans.
fn merged_ranges(files: &[FileInfo]) -> Vec<(Txid, Txid)> {
    let mut spans: Vec<(Txid, Txid)> = files
        .iter()
        .map(|info| (info.key.range().min(), info.key.range().max()))
        .collect();
    spans.sort();
    let mut merged: Vec<(Txid, Txid)> = Vec::new();
    for (min, max) in spans {
        match merged.last_mut() {
            Some((_, end)) if end.next().is_none_or(|one_past| min <= one_past) => {
                *end = (*end).max(max);
            }
            _ => merged.push((min, max)),
        }
    }
    merged
}

fn covered(cover: &[(Txid, Txid)], info: &FileInfo) -> bool {
    #[cfg(test)]
    if BREAK_L0_COVERAGE.get() {
        return true;
    }
    let range = info.key.range();
    let after = cover.partition_point(|(_, end)| *end < range.max());
    cover
        .get(after)
        .is_some_and(|(start, end)| *start <= range.min() && range.max() <= *end)
}

fn self_check(inventory: &Inventory, job: &DeleteJob) -> Option<Hold> {
    let rest = Inventory::from_files(
        inventory
            .files()
            .iter()
            .filter(|info| job.keys().binary_search(&info.key).is_err())
            .copied(),
    );
    let Ok(rest) = rest else {
        // A subset of a valid listing cannot conflict; hold if it somehow does.
        return Some(Hold::SelfCheck {
            outcome: PlanOutcome::EmptyReplica,
        });
    };
    match plan(&rest, RestoreTarget::Latest) {
        PlanOutcome::Plan(chain) if Some(chain.reaches()) == inventory.newest() => None,
        // A plan that stops short of the newest transaction.
        PlanOutcome::Plan(chain) => Some(Hold::SelfCheck {
            outcome: PlanOutcome::BeyondNewest {
                requested: inventory.newest().unwrap_or(chain.reaches()),
                newest: chain.reaches(),
            },
        }),
        outcome @ (PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::Gap { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. }) => Some(Hold::SelfCheck { outcome }),
    }
}

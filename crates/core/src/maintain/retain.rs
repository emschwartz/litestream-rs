//! Retention: which files may be deleted, and when.
//!
//! [`retention`] is a pure function of a listing, an instant and a schedule,
//! like [`super::due`], which calls it only when no compaction or snapshot
//! is ready. A wrong delete is unrecoverable, so every rule is fail-closed:
//! when a rule can be read two ways, the reading that deletes less is the one
//! implemented.
//!
//! Trust gate. Before any rule runs the listing must be trustworthy; the first
//! failure is a [`RetentionHold`] and nothing is deleted. A name this code cannot
//! parse, a malformed snapshot, a listing missing a file this process knows it
//! wrote, a hole after the newest snapshot, a history that does not plan, a
//! snapshot or level-0 file without a non-zero created-at time, and snapshots whose created-at times
//! run backwards all mean the listing cannot be read as a history. The first
//! group is judged on names alone; the created-at time holds look only at the files whose
//! created-at times were read (see "Time base"). A hole
//! further back than the newest snapshot does not hold: it is history already
//! lost, the rules never widen it, and holding on it would pin every delete
//! after one Go-era loss.
//!
//! Time base. Ages come from exact created-at times (the header created-at time, never a listed
//! last-modified time, which can precede it by any writer clock lead), and are
//! measured from the anchor, computed afresh on every call. The anchor is
//! `min(now, newest created-at time among the last file of each non-empty level and
//! every snapshot start)`: a listing that carries no exact created-at times costs a
//! request per file asked for, so only those files are read, plus the level 0
//! files a rule would delete, in key order up to and including the first young
//! one (at most [`MAX_DELETE`], past which the job is full anyway). On a store
//! that runs batches the anchors are read side by side, and the level 0 scan
//! reads ahead of its position, never more files than it has already found
//! old, so past the first young one it reads at most that many for nothing. On a
//! writer's listing a file's created-at time rises with its range, so the anchor equals
//! the newest created-at time anywhere; on any listing it is never above it, and a lower
//! anchor only keeps more. A clock that jumps ahead deletes nothing the
//! replica's own newest created-at time would not, and a clock behind only makes files
//! look young. The anchor is deliberately not remembered: a remembered high one
//! would keep deleting on the strength of one bad clock reading. What holds
//! instead:
//! - Every job is computed against the listing it was given and is
//!   restore-safe for the range its anchor promises: from the first retained
//!   snapshot up.
//! - Any subset of a job is as safe as the whole job, so a partial
//!   `DeleteObjects` or a crash between deletes loses nothing retained.
//! - The promised range only shrinks as the anchor rises, so a clock that
//!   steps back promises a wider range than the last job did and never
//!   weakens what an earlier job kept.
//! - The rules are not monotone in the anchor: a lower anchor can delete a
//!   file a higher one keeps. A level-0 file straddling the high anchor's
//!   floor stays (not under it, not covered), but at a lower anchor the level
//!   1 file below that floor is kept, covers it, and the level-0 file goes.
//!   The test `a_lower_anchor_can_delete_a_level_zero_file_a_higher_one_keeps`
//!   pins the example.
//!
//! The cost of a clock that steps back is a snapshot kept after an earlier job
//! cut the level-0 chain behind it; a restore goes through the newer snapshot
//! and loses nothing. Go on S3 uses the upload time (`LastModified`), never earlier than the header created-at time, so Go keeps
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
//!    exists: the files created at or before `anchor - l0_retention` whose
//!    whole range lies inside the union of the level 1 to 8 files rule 2
//!    keeps, or wholly below the floor. Never the last file. Go compares only
//!    L1's max; a file that L1 does not cover stays here, and the files above
//!    it that are covered still go (each is judged alone). The floor clause is
//!    the one place this deletes more than coverage alone: rule 2 removes the
//!    level 1 file that covered such a file, so without it the file would
//!    stay forever. Nothing under the floor is needed by a retained snapshot
//!    or by a restore planned on the floor's. The age test still stops the
//!    scan: created-at times follow transactions, so the files after a young one are
//!    young.
//!
//! The kept restore point. While a task of the actor is degraded, [`MaintainState::kept_restore_point`]
//! names the newest transaction the replica held when it began. Before the
//! cut, every file of that transaction's restore chain (`plan` at it) leaves
//! the candidates, and the self-check also asks that the kept restore point still plans
//! without the job. Everything else is retained by the rules above, so the
//! history after the kept restore point keeps rolling: it keeps one restore point and
//! never freezes every delete. A kept restore point that does not plan on the listing is a
//! hold.
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

use super::{DeleteJob, L0Retention, MAX_DELETE, MaintainState, Maintenance, RetentionHold};
use crate::created_at::{Need, Resolve, ResolvedCreatedAt};
use crate::inventory::{Inventory, is_snapshot_start};
use crate::layout::{CreatedAt, FileInfo, FileKey, Level, NonZeroCreatedAt};
use crate::plan::{KeyTarget, PlanOutcome, plan, plan_without};

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
    /// Test hook: every file anchors the clock, as main's exact created-at time over
    /// the whole listing did, so R' can be compared with it.
    static ANCHOR_FROM_EVERY_FILE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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
    Held(RetentionHold),
}

/// A decision step that may need one more created-at time: `Err` names the file, and
/// the files it expects to need next.
type Needs<T> = Result<T, Need>;

/// The next delete job, or why there is none, or the one created-at time needed before
/// either is known. See the module docs.
pub(super) fn retention(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Resolve<Retention> {
    decide(inventory, now, config, published, state, resolved).into()
}

fn decide(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Needs<Retention> {
    if let Some(hold) = gate(inventory, published, state) {
        return Ok(Retention::Held(hold));
    }
    let anchors = anchor_files(inventory);
    if anchors.is_empty() {
        return Ok(Retention::Wait { until: None });
    }
    // Every anchor's created-at time is needed below, so ask for all that are missing at once.
    let mut missing = anchors.iter().filter(|info| resolved.exact(info).is_none());
    if let Some(first) = missing.next() {
        return Err(Need::with_ahead(*first, missing.copied()));
    }
    // A file with no non-zero created-at time sets no anchor: it has no age to measure.
    let mut newest_created_at: Option<Timestamp> = None;
    for info in &anchors {
        if let Some(created_at) = created_at_of(resolved, info)? {
            let at = created_at.at();
            newest_created_at = Some(newest_created_at.map_or(at, |newest| newest.max(at)));
        }
    }
    if let Some(hold) =
        without_created_at(anchors.iter(), resolved)?.or(created_at_backwards(inventory, resolved)?)
    {
        return Ok(Retention::Held(hold));
    }
    // Every anchor without a created-at time and none held: no snapshot, so nothing to age.
    let Some(newest_created_at) = newest_created_at else {
        return Ok(Retention::Wait { until: None });
    };
    let anchor = now.min(newest_created_at);
    let found = match candidates(inventory, anchor, newest_created_at, config, resolved)? {
        Ok(found) => found,
        Err(hold) => return Ok(Retention::Held(hold)),
    };
    let until = found.wake;
    if found.keys.is_empty() {
        return Ok(Retention::Wait { until });
    }
    let mut keys = found.keys;
    if let Some(restore_point) = state.kept_restore_point() {
        match pinned_chain(inventory, &[], restore_point) {
            Ok(chain) => keys.retain(|key| !chain.contains(key)),
            Err(hold) => return Ok(Retention::Held(hold)),
        }
        if keys.is_empty() {
            return Ok(Retention::Wait { until });
        }
    }
    keys.sort();
    keys.truncate(MAX_DELETE);
    let Ok(job) = DeleteJob::new(keys) else {
        // Sorted, distinct, 1 to MAX_DELETE keys: unreachable, and a hold of
        // nothing is the fail-closed answer if it ever is not.
        return Ok(Retention::Wait { until });
    };
    Ok(
        match self_check(inventory, &job, state.kept_restore_point()) {
            Some(hold) => Retention::Held(hold),
            None => Retention::Delete(job),
        },
    )
}

/// The files whose created-at times set the anchor and the snapshot rule: the last file
/// of each non-empty level and every snapshot start, in key order.
fn anchor_files(inventory: &Inventory) -> Vec<FileInfo> {
    #[cfg(test)]
    if ANCHOR_FROM_EVERY_FILE.get() {
        return inventory.files().to_vec();
    }
    let mut files: Vec<FileInfo> = Level::ALL
        .iter()
        .filter_map(|level| inventory.level(*level).last().copied())
        .collect();
    files.extend(snapshots(inventory).into_iter().copied());
    files.sort_by_key(|info| info.key);
    files.dedup_by_key(|info| info.key);
    files
}

fn gate(
    inventory: &Inventory,
    published: Option<Txid>,
    state: &MaintainState,
) -> Option<RetentionHold> {
    distrusted(inventory, state)
        .or_else(|| unrecognized_names(inventory))
        .or_else(|| {
            inventory
                .malformed_snapshots()
                .first()
                .map(|key| RetentionHold::MalformedSnapshot { key: *key })
        })
        .or_else(|| listing_behind(inventory, published))
        .or_else(|| hole(inventory))
        .or_else(|| latest_unplannable(inventory))
}

/// Whether the retention gate passes on this listing (see the module docs): what the short
/// view's level-0 rule needs from the last whole listing.
pub(super) fn gate_is_clear(
    inventory: &Inventory,
    published: Option<Txid>,
    state: &MaintainState,
) -> bool {
    gate(inventory, published, state).is_none()
}

/// A compaction output or a snapshot the store rejected that is still listed.
fn distrusted(inventory: &Inventory, state: &MaintainState) -> Option<RetentionHold> {
    Level::ALL.iter().skip(1).find_map(|level| {
        let key = state.distrusted_at(*level)?;
        inventory
            .level(*level)
            .iter()
            .any(|info| info.key == key)
            .then_some(RetentionHold::OutputDistrusted { key })
    })
}

fn unrecognized_names(inventory: &Inventory) -> Option<RetentionHold> {
    Level::ALL.iter().find_map(|level| {
        let count = inventory.skipped(*level);
        (count > 0).then_some(RetentionHold::UnrecognizedNames {
            level: *level,
            count,
        })
    })
}

fn listing_behind(inventory: &Inventory, published: Option<Txid>) -> Option<RetentionHold> {
    let published = published?;
    let listed = inventory.newest();
    (listed < Some(published)).then_some(RetentionHold::ListingBehind { listed, published })
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
fn hole(inventory: &Inventory) -> Option<RetentionHold> {
    let newest = inventory.newest()?;
    let snapshot = snapshots(inventory)
        .last()
        .map(|info| info.key.range().max())?;
    let mut files: Vec<&FileInfo> = inventory
        .files()
        .iter()
        .filter(|info| past_snapshot(info, snapshot))
        .collect();
    files.sort_by_key(|info| info.key.range().min());
    let mut reach = snapshot;
    for info in files {
        let range = info.key.range();
        if reach.next().is_some_and(|one_past| range.min() > one_past) {
            return Some(RetentionHold::Hole {
                after: reach,
                next: Some(range.min()),
            });
        }
        reach = reach.max(range.max());
    }
    // The newest file is a snapshot start (the snapshot reaches it) or a level 0-8
    // file past the snapshot (the walk reaches it): a level-9 file that does not
    // start at 1 is held as malformed before this runs.
    debug_assert!(reach >= newest, "{reach:?} < {newest:?}");
    None
}

/// Whether a level 0-8 file ends past the newest snapshot's end. Only those can
/// open a hole or move the reach; one ending at the snapshot could be taken too
/// and would change neither, so the bound could as well be inclusive. It keeps
/// the walk to the few files past the snapshot.
fn past_snapshot(info: &FileInfo, snapshot: Txid) -> bool {
    !info.key.level().is_snapshot() && info.key.range().max() > snapshot
}

fn latest_unplannable(inventory: &Inventory) -> Option<RetentionHold> {
    let newest = inventory.newest()?;
    match plan(inventory, KeyTarget::Latest).into_chain() {
        Ok(chain) if chain.reaches() == newest => None,
        // A plan that stops short is a missing tail; the planner never
        // returns one, and the hold must not claim a plan.
        Ok(chain) => Some(RetentionHold::Hole {
            after: chain.reaches(),
            next: None,
        }),
        Err(outcome) => Some(RetentionHold::LatestUnplannable { outcome }),
    }
}

/// The file's exact created-at time when it is a real one (`None` for a writer that
/// recorded no created-at time), or the request for it.
fn created_at_of(resolved: &ResolvedCreatedAt, info: &FileInfo) -> Needs<Option<NonZeroCreatedAt>> {
    resolved
        .exact(info)
        .map(CreatedAt::non_zero)
        .ok_or_else(|| Need::one(*info))
}

/// Whether a created-at time at or before the epoch is no created-at time at all for this file:
/// it ages a level 0 file or a snapshot.
fn aged(info: &FileInfo) -> bool {
    info.key.level() == Level::ZERO || info.key.level().is_snapshot()
}

fn without_created_at<'a>(
    files: impl Iterator<Item = &'a FileInfo>,
    resolved: &ResolvedCreatedAt,
) -> Needs<Option<RetentionHold>> {
    for info in files.filter(|info| aged(info)) {
        if created_at_of(resolved, info)?.is_none() {
            return Ok(Some(RetentionHold::NoCreatedAt { key: info.key }));
        }
    }
    Ok(None)
}

fn created_at_backwards(
    inventory: &Inventory,
    resolved: &ResolvedCreatedAt,
) -> Needs<Option<RetentionHold>> {
    for pair in snapshots(inventory).windows(2) {
        if let [earlier, later] = pair
            && created_at_of(resolved, earlier)? > created_at_of(resolved, later)?
        {
            return Ok(Some(RetentionHold::SnapshotCreatedAtBackwards {
                earlier: earlier.key,
                later: later.key,
            }));
        }
    }
    Ok(None)
}

/// What the rules chose, before the cut and the self-check.
#[derive(Default)]
struct Found {
    keys: Vec<FileKey>,
    /// The earliest instant a rule would delete more, in anchor time.
    wake: Option<Timestamp>,
}

impl Found {
    fn wake_at(&mut self, at: Timestamp, newest_created_at: Timestamp) {
        // The anchor never passes the newest created-at time, so a later instant is
        // never reached without a new write.
        if at <= newest_created_at {
            self.wake = Some(self.wake.map_or(at, |soonest| soonest.min(at)));
        }
    }
}

/// `created_at` plus `age`: the instant it ages out. Past the last instant a
/// timestamp holds it is that last instant, which the anchor never passes.
fn aged_at(created_at: NonZeroCreatedAt, age: Duration) -> Timestamp {
    created_at.at().checked_add(age).unwrap_or(Timestamp::MAX)
}

/// Whether `created_at` is at or after `anchor - age`. Every created-at time is when that
/// instant is before the first one a timestamp holds.
fn not_before(created_at: NonZeroCreatedAt, anchor: Timestamp, age: Duration) -> bool {
    anchor
        .checked_sub(age)
        .is_none_or(|cutoff| created_at.at() >= cutoff)
}

/// Whether `created_at` is after `anchor - age`: too young to age out.
fn after(created_at: NonZeroCreatedAt, anchor: Timestamp, age: Duration) -> bool {
    anchor
        .checked_sub(age)
        .is_none_or(|cutoff| created_at.at() > cutoff)
}

/// What the rules chose, or the hold a level 0 file's created-at time calls for.
fn candidates(
    inventory: &Inventory,
    anchor: Timestamp,
    newest_created_at: Timestamp,
    config: &Maintenance,
    resolved: &ResolvedCreatedAt,
) -> Needs<Result<Found, RetentionHold>> {
    let mut found = Found::default();
    let snaps = snapshots(inventory);
    let rule = match snapshot_rule(
        &snaps,
        anchor,
        newest_created_at,
        config,
        resolved,
        &mut found,
    )? {
        Ok(Some(rule)) => rule,
        Ok(None) => return Ok(Ok(found)),
        Err(hold) => return Ok(Err(hold)),
    };
    let SnapshotRule { floor, first } = rule;
    let kept = levels_one_to_eight(inventory, floor, &mut found.keys);
    if let L0Retention::After(age) = config.l0_retention() {
        let scan = LevelZero {
            floor,
            anchor,
            newest_created_at,
            age,
        };
        if let Some(hold) = level_zero(inventory, &scan, &kept, resolved, &mut found)? {
            return Ok(Err(hold));
        }
    }
    found
        .keys
        .extend(snaps.iter().take(first).map(|info| info.key));
    Ok(Ok(found))
}

/// What the snapshot rule decided: the floor below which history is not needed by a retained
/// snapshot, and the index of the first retained snapshot.
struct SnapshotRule {
    floor: Option<Txid>,
    first: usize,
}

/// Rule 1: which snapshot is the first retained, and so the floor. `None` without a snapshot.
/// Shared by the full rules and the level-0 rule of the short view, so both read one floor from
/// one list of snapshots (level 9 is always listed whole).
fn snapshot_rule(
    snaps: &[&FileInfo],
    anchor: Timestamp,
    newest_created_at: Timestamp,
    config: &Maintenance,
    resolved: &ResolvedCreatedAt,
    found: &mut Found,
) -> Needs<Result<Option<SnapshotRule>, RetentionHold>> {
    let Some(last) = snaps.len().checked_sub(1) else {
        return Ok(Ok(None));
    };
    let retention = config.snapshot_retention();
    let mut first = last;
    for (position, info) in snaps.iter().enumerate() {
        // The gate held on any without a created-at time snapshot; this is its fail-closed twin.
        let Some(created_at) = created_at_of(resolved, info)? else {
            return Ok(Err(RetentionHold::NoCreatedAt { key: info.key }));
        };
        if not_before(created_at, anchor, retention) {
            first = position;
            break;
        }
    }
    if let Some(retained) = snaps.get(first).filter(|_| first < last) {
        let Some(created_at) = created_at_of(resolved, retained)? else {
            return Ok(Err(RetentionHold::NoCreatedAt { key: retained.key }));
        };
        let aged_out = aged_at(created_at, retention)
            .checked_add(Duration::from_millis(1))
            .unwrap_or(Timestamp::MAX);
        found.wake_at(aged_out, newest_created_at);
    }
    let floor = first
        .checked_sub(1)
        .and_then(|before| snaps.get(before))
        .map(|info| info.key.range().max());
    Ok(Ok(Some(SnapshotRule { floor, first })))
}

/// Queues each level's deletable prefix; returns, as slices of the inventory,
/// the files that stay.
fn levels_one_to_eight<'a>(
    inventory: &'a Inventory,
    floor: Option<Txid>,
    out: &mut Vec<FileKey>,
) -> Vec<&'a [FileInfo]> {
    let mut kept = Vec::new();
    for level in &Level::COMPACTION {
        let files = inventory.level(*level);
        let deletable = floor.map_or(0, |floor| {
            files
                .iter()
                .take(files.len().saturating_sub(1))
                .take_while(|info| info.key.range().max() < floor)
                .count()
        });
        out.extend(files.iter().take(deletable).map(|info| info.key));
        kept.extend(files.get(deletable..));
    }
    kept
}

/// What the level 0 rule measures against.
struct LevelZero {
    floor: Option<Txid>,
    anchor: Timestamp,
    newest_created_at: Timestamp,
    age: Duration,
}

/// Level 0: a prefix, covered and aged, never the last file. Reads the created-at time
/// of each file only once keys alone would delete it, and stops at the first
/// young one or when the job is full. A file with no usable created-at time is a hold.
fn level_zero(
    inventory: &Inventory,
    scan: &LevelZero,
    kept: &[&[FileInfo]],
    resolved: &ResolvedCreatedAt,
    found: &mut Found,
) -> Needs<Option<RetentionHold>> {
    let cover = merged_ranges(kept);
    let files = inventory.level(Level::ZERO);
    let mut chosen = 0_usize;
    for (at, info) in files.iter().enumerate().take(files.len().saturating_sub(1)) {
        if !by_keys(scan, &cover, info) {
            continue;
        }
        if chosen == MAX_DELETE {
            return Ok(None);
        }
        if resolved.exact(info).is_none() {
            let later = files.get(at.saturating_add(1)..).unwrap_or_default();
            return Err(level_zero_need(later, scan, &cover, resolved, info, chosen));
        }
        let Some(created) = created_at_of(resolved, info)? else {
            return Ok(Some(RetentionHold::NoCreatedAt { key: info.key }));
        };
        if after(created, scan.anchor, scan.age) {
            found.wake_at(aged_at(created, scan.age), scan.newest_created_at);
            return Ok(None);
        }
        found.keys.push(info.key);
        chosen = chosen.saturating_add(1);
    }
    Ok(None)
}

/// Whether keys alone would delete the level 0 file: it ends below the floor,
/// or a file of levels 1 to 8 that stays covers it.
fn by_keys(scan: &LevelZero, cover: &[(Txid, Txid)], info: &FileInfo) -> bool {
    scan.floor
        .is_some_and(|floor| info.key.range().max() < floor)
        || covered(cover, info)
}

/// The level 0 file whose created-at time the scan needs, and after it the files the
/// scan would read next (from `later`, the level's files after `need`) if each turns out old enough to delete. The scan
/// stops at the first young one, so these are a guess: no more are named than
/// files this scan already found old (`chosen`), which keeps an idle tick,
/// whose first candidate is young, at one read, and wastes at most as many
/// reads as were needed.
fn level_zero_need(
    later: &[FileInfo],
    scan: &LevelZero,
    cover: &[(Txid, Txid)],
    resolved: &ResolvedCreatedAt,
    need: &FileInfo,
    chosen: usize,
) -> Need {
    let ahead = later
        .iter()
        .take(later.len().saturating_sub(1))
        .filter(|info| by_keys(scan, cover, info))
        .take(chosen.min(MAX_DELETE.saturating_sub(chosen).saturating_sub(1)))
        .filter(|info| resolved.exact(info).is_none())
        .copied();
    Need::with_ahead(*need, ahead)
}

/// What the short view's level-0 rule chose.
pub(super) struct ShortZero {
    /// Level-0 keys to delete, ascending; at most [`MAX_DELETE`].
    pub(super) keys: Vec<FileKey>,
    /// When the first young covered level-0 file ages out, in anchor time.
    pub(super) wake: Option<Timestamp>,
    /// The first transaction of the first level-0 file the scan leaves in place: the one
    /// whose young age stopped it, or the next past a full job. Every level-0 file before it
    /// is deleted by this job or is old and not covered by the short view, which the next
    /// full listing judges. The short view's level-1 tail starts at the covers of the files
    /// from here on.
    pub(super) settled_min: Option<Txid>,
}

/// The level-0 rule on the files a short listing returned, which are not a whole
/// history. Its deletes are a subset of what [`retention`] on the whole listing would delete,
/// when that listing's gate is clear. Every difference from the full rule only keeps more:
///
/// - The anchor is the newest created-at time among the last level-0 file, the snapshot
///   starts and `known`, the newest file this engine knows at each level 1 to 8 (a listed
///   one or a remembered one): the files the full anchor reads. A remembered file is read for
///   its age only, never as a cover. Without it the anchor could sit below the full one, and
///   the rules are not monotone in the anchor (see the module doc).
/// - The floor comes from every snapshot, as in the full rule (level 9 is listed whole).
/// - A level-0 file counts as covered only by a *listed* level 1 to 8 file that stays
///   (its last transaction is at or above the floor). The full rule's covers are all files
///   that stay, listed or not, so these are a subset. A remembered record is never a cover:
///   it may name a file that is gone.
/// - The scan stops at the first young level-0 file, covered or not, where the full rule
///   skips an uncovered one without reading its age. Stopping earlier only deletes less.
/// - Nothing is deleted while a kept restore point is pinned, an output is distrusted, a
///   name did not parse, a snapshot is malformed or the listing is behind `published`.
///
/// What it does not repeat is the full gate's walk of the whole history (no hole after the
/// newest snapshot, the newest state plans) and the self-check: they need every file. The
/// caller runs this only while the last full listing passed that gate, and the module doc of
/// `short` argues why that is enough.
pub(super) fn short_level_zero(
    listed: &Inventory,
    known: &[FileInfo],
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Resolve<ShortZero> {
    short_decide(listed, known, now, config, published, state, resolved).into()
}

fn short_decide(
    listed: &Inventory,
    known: &[FileInfo],
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Needs<ShortZero> {
    let nothing = ShortZero {
        keys: Vec::new(),
        wake: None,
        settled_min: None,
    };
    let files = listed.level(Level::ZERO);
    let (L0Retention::After(age), Some(last_zero)) = (config.l0_retention(), files.last()) else {
        return Ok(nothing);
    };
    let distrusted = Level::ALL
        .iter()
        .any(|level| state.distrusted_at(*level).is_some());
    if distrusted || state.kept_restore_point().is_some() || short_gate(listed, published).is_some()
    {
        return Ok(nothing);
    }
    let snaps = snapshots(listed);
    let mut anchors: Vec<FileInfo> = vec![*last_zero];
    anchors.extend(snaps.iter().map(|info| **info));
    anchors.extend(known.iter().copied());
    anchors.sort_by_key(|info| info.key);
    anchors.dedup_by_key(|info| info.key);
    let mut missing = anchors.iter().filter(|info| resolved.exact(info).is_none());
    if let Some(first) = missing.next() {
        return Err(Need::with_ahead(*first, missing.copied()));
    }
    let mut newest: Option<Timestamp> = None;
    for info in &anchors {
        if let Some(created_at) = created_at_of(resolved, info)? {
            let at = created_at.at();
            newest = Some(newest.map_or(at, |newest| newest.max(at)));
        }
    }
    if without_created_at(anchors.iter(), resolved)?
        .or(created_at_backwards(listed, resolved)?)
        .is_some()
    {
        return Ok(nothing);
    }
    let Some(newest_created_at) = newest else {
        return Ok(nothing);
    };
    let anchor = now.min(newest_created_at);
    let mut found = Found::default();
    let rule = match snapshot_rule(
        &snaps,
        anchor,
        newest_created_at,
        config,
        resolved,
        &mut found,
    )? {
        Ok(Some(rule)) => rule,
        Ok(None) | Err(_) => return Ok(nothing),
    };
    let floor = rule.floor;
    let kept: Vec<FileInfo> = Level::COMPACTION
        .iter()
        .flat_map(|level| listed.level(*level))
        .filter(|info| floor.is_none_or(|floor| info.key.range().max() >= floor))
        .copied()
        .collect();
    let cover = merged_ranges(&[kept.as_slice()]);
    let scan = LevelZero {
        floor,
        anchor,
        newest_created_at,
        age,
    };
    let mut keys: Vec<FileKey> = Vec::new();
    let mut wake = None;
    let mut settled_min = last_zero.key.range().min();
    for (at, info) in files.iter().enumerate().take(files.len().saturating_sub(1)) {
        if keys.len() == MAX_DELETE {
            settled_min = info.key.range().min();
            break;
        }
        if resolved.exact(info).is_none() {
            let later = files.get(at.saturating_add(1)..).unwrap_or_default();
            let ahead = later
                .iter()
                .take(later.len().saturating_sub(1))
                .take(
                    keys.len()
                        .min(MAX_DELETE.saturating_sub(keys.len()).saturating_sub(1)),
                )
                .filter(|info| resolved.exact(info).is_none())
                .copied();
            return Err(Need::with_ahead(*info, ahead));
        }
        let Some(created) = created_at_of(resolved, info)? else {
            return Ok(nothing);
        };
        let by_keys = by_keys(&scan, &cover, info);
        if after(created, anchor, age) {
            if by_keys {
                wake = Some(aged_at(created, age)).filter(|at| *at <= newest_created_at);
            }
            settled_min = info.key.range().min();
            break;
        }
        if by_keys {
            keys.push(info.key);
        }
    }
    Ok(ShortZero {
        keys,
        wake,
        settled_min: Some(settled_min),
    })
}

/// The gate checks that read only the files a short listing returned.
fn short_gate(listed: &Inventory, published: Option<Txid>) -> Option<RetentionHold> {
    unrecognized_names(listed)
        .or_else(|| {
            listed
                .malformed_snapshots()
                .first()
                .map(|key| RetentionHold::MalformedSnapshot { key: *key })
        })
        .or_else(|| listing_behind(listed, published))
}

/// The union of the files' ranges as sorted, disjoint, non-adjacent spans.
///
/// Each run is one level's files, ascending by first transaction, so it folds
/// into spans in one pass without a list of every file; the few spans each run
/// leaves are then folded together.
fn merged_ranges(runs: &[&[FileInfo]]) -> Vec<(Txid, Txid)> {
    let mut spans: Vec<(Txid, Txid)> = Vec::new();
    for run in runs {
        let mut folded = Vec::new();
        for info in *run {
            add_span(&mut folded, info.key.range().min(), info.key.range().max());
        }
        spans.extend(folded);
    }
    spans.sort();
    let mut merged = Vec::new();
    for (min, max) in spans {
        add_span(&mut merged, min, max);
    }
    merged
}

/// Folds `min..=max` into the last span when they touch or overlap; `min` must not
/// precede the last span's start.
fn add_span(spans: &mut Vec<(Txid, Txid)>, min: Txid, max: Txid) {
    match spans.last_mut() {
        Some((_, end)) if end.next().is_none_or(|one_past| min <= one_past) => {
            *end = (*end).max(max);
        }
        _ => spans.push((min, max)),
    }
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

/// The keys of the pinned restore point's chain over the inventory without
/// `gone` (ascending), or the hold when it does not plan.
fn pinned_chain(
    inventory: &Inventory,
    gone: &[FileKey],
    restore_point: Txid,
) -> Result<Vec<FileKey>, RetentionHold> {
    match plan_without(inventory, gone, KeyTarget::Txid(restore_point)).into_chain() {
        Ok(chain) if chain.reaches() == restore_point => {
            Ok(chain.files().iter().map(|info| info.key).collect())
        }
        Ok(chain) => Err(RetentionHold::RestorePointUnplannable {
            restore_point,
            outcome: PlanOutcome::BeyondNewest {
                requested: restore_point,
                newest: chain.reaches(),
            },
        }),
        Err(outcome) => Err(RetentionHold::RestorePointUnplannable {
            restore_point,
            outcome,
        }),
    }
}

fn self_check(
    inventory: &Inventory,
    job: &DeleteJob,
    restore_point: Option<Txid>,
) -> Option<RetentionHold> {
    // The inventory minus the job's keys, read in place: a copy of it here
    // would double the peak of every retention pass.
    if let Some(restore_point) = restore_point
        && let Err(hold) = pinned_chain(inventory, job.keys(), restore_point)
    {
        return Some(hold);
    }
    match plan_without(inventory, job.keys(), KeyTarget::Latest).into_chain() {
        Ok(chain) if Some(chain.reaches()) == inventory.newest() => None,
        // A plan that stops short of the newest transaction.
        Ok(chain) => Some(RetentionHold::SelfCheck {
            outcome: PlanOutcome::BeyondNewest {
                requested: inventory.newest().unwrap_or(chain.reaches()),
                newest: chain.reaches(),
            },
        }),
        Err(outcome) => Some(RetentionHold::SelfCheck { outcome }),
    }
}

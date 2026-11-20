//! The restore planner: which files, in what order, rebuild a requested state.
//!
//! Pure: a function of an [`Inventory`] and a [`RestoreTarget`], with no I/O and
//! no clock. The answer is either a [`ContiguousChain`] that ends exactly where
//! the target says, or a typed reason there is no exact answer. It is never a
//! shorter history: upstream's timestamp restore stops at a hole and exits 0,
//! and this planner reports [`PlanOutcome::Gap`] instead.
//!
//! How a plan is chosen:
//! - Eligibility: [`RestoreTarget::Latest`] admits every file; [`Txid`] admits
//!   files with a last transaction at or below the target; [`AsOf`] admits files
//!   created strictly before the instant. Go skips a file when
//!   [`!info.CreatedAt.Before(timestamp)`] in both the snapshot loop and
//!   [`restoreLevelCursor.refresh`] (replica.go), the same comparison.
//! - The base is the newest eligible snapshot (level 9, first transaction 1).
//!   A newer snapshot can reach everything an older one can: any file that
//!   bridges from the older base also touches the newer one's end.
//! - Each step takes the eligible file, from levels 0 to 8, that touches the
//!   chain's end (first transaction at most one past it) and reaches furthest.
//!   Ties go to the higher level, then the lower first transaction: fewer,
//!   larger files. An overlapping file is valid because a file holds the
//!   newest version of every page it touches.
//! - A plan must end exactly at the target: at the requested transaction, or
//!   for the other targets at the largest last transaction of any eligible
//!   file. Anything short of that is explained, not returned.

use std::cmp::Reverse;

use lsr_ltx::{Timestamp, Txid};

use crate::inventory::{Inventory, is_snapshot_start};
use crate::layout::{FileInfo, FileKey, Level};

/// The state of the database to rebuild.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RestoreTarget {
    /// The newest state the replica holds.
    Latest,
    /// The state right after this transaction.
    Txid(Txid),
    /// The newest state whose files were all created strictly before this
    /// instant. An instant later than every file is [`Latest`].
    AsOf(Timestamp),
}

/// Files that rebuild one state, in the order to apply them.
///
/// Only the planner builds one. The first file is a snapshot starting at
/// transaction 1; every later file starts no later than one past the previous
/// end and ends beyond it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ContiguousChain {
    snapshot: FileInfo,
    files: Vec<FileInfo>,
    reaches: Txid,
}

impl ContiguousChain {
    fn start(snapshot: FileInfo) -> Option<Self> {
        is_snapshot_start(&snapshot).then(|| Self {
            snapshot,
            files: vec![snapshot],
            reaches: snapshot.key.range().max(),
        })
    }

    /// Adds a file if it keeps the invariants; reports whether it did.
    fn push(&mut self, next: FileInfo) -> bool {
        let range = next.key.range();
        let fits = !next.key.level().is_snapshot()
            && touches(range.min(), self.reaches)
            && range.max() > self.reaches;
        if fits {
            self.files.push(next);
            self.reaches = range.max();
        }
        fits
    }

    /// The files to apply, oldest state first: the snapshot, then each
    /// extension.
    #[must_use]
    pub fn files(&self) -> &[FileInfo] {
        &self.files
    }

    /// The snapshot the chain starts from.
    #[must_use]
    pub const fn snapshot(&self) -> &FileInfo {
        &self.snapshot
    }

    /// The last transaction the chain covers.
    #[must_use]
    pub const fn reaches(&self) -> Txid {
        self.reaches
    }
}

/// What the planner found. Exactly one variant is a plan; each other variant
/// is a different reason there is no exact answer.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PlanOutcome {
    /// Files that rebuild the target exactly.
    Plan(ContiguousChain),
    /// The replica holds no LTX file.
    EmptyReplica,
    /// No snapshot is eligible for the target: none exists, none is at or
    /// below a requested transaction, none is old enough for a requested
    /// instant, or the only level-9 files do not start at transaction 1.
    NoSnapshot {
        /// The oldest valid snapshot's last transaction, whatever the target.
        oldest_snapshot: Option<Txid>,
        /// Level-9 files that do not start at transaction 1 and were refused.
        malformed_snapshots: Vec<FileKey>,
    },
    /// A requested transaction is later than anything the replica holds.
    BeyondNewest {
        /// What was asked for.
        requested: Txid,
        /// The last transaction any file covers.
        newest: Txid,
    },
    /// The chain stops short and the next eligible file starts after a hole.
    Gap {
        /// The last transaction the chain reaches.
        have_through: Txid,
        /// The first transaction of the nearest file beyond the hole.
        next_starts: Txid,
    },
    /// A requested transaction falls strictly inside a file, and the finer
    /// files that would stop at it are gone.
    InsideFile {
        /// What was asked for.
        requested: Txid,
        /// The file that covers it.
        file: FileKey,
    },
    /// The only history beyond the chain is in level-9 files that do not
    /// start at transaction 1.
    MalformedSnapshot {
        /// The last transaction the chain reaches.
        have_through: Txid,
        /// A refused file that holds history beyond it.
        file: FileKey,
    },
}

/// Whether a file starting at [`min`] can follow a chain that ends at [`reach`].
fn touches(min: Txid, reach: Txid) -> bool {
    reach.next().is_none_or(|one_past| min <= one_past)
}

fn eligible(info: &FileInfo, target: RestoreTarget) -> bool {
    match target {
        RestoreTarget::Latest => true,
        RestoreTarget::Txid(wanted) => info.key.range().max() <= wanted,
        RestoreTarget::AsOf(instant) => info.created_at < instant,
    }
}

/// Plans a restore of [`target`] from [`inventory`].
#[must_use]
pub fn plan(inventory: &Inventory, target: RestoreTarget) -> PlanOutcome {
    let Some(newest) = inventory.newest() else {
        return PlanOutcome::EmptyReplica;
    };
    if let RestoreTarget::Txid(requested) = target
        && requested > newest
    {
        return PlanOutcome::BeyondNewest { requested, newest };
    }
    let pool: Vec<FileInfo> = inventory
        .files()
        .iter()
        .filter(|info| eligible(info, target))
        .copied()
        .collect();
    let base = pool
        .iter()
        .filter(|info| is_snapshot_start(info))
        .max_by_key(|info| info.key.range().max())
        .and_then(|info| ContiguousChain::start(*info));
    let Some(mut chain) = base else {
        return no_snapshot(inventory);
    };
    extend(&mut chain, &pool);
    let want = match target {
        RestoreTarget::Txid(requested) => Some(requested),
        RestoreTarget::Latest | RestoreTarget::AsOf(_) => {
            pool.iter().map(|info| info.key.range().max()).max()
        }
    };
    if want == Some(chain.reaches()) {
        return PlanOutcome::Plan(chain);
    }
    explain_shortfall(inventory, &pool, target, chain.reaches())
}

fn no_snapshot(inventory: &Inventory) -> PlanOutcome {
    PlanOutcome::NoSnapshot {
        oldest_snapshot: inventory
            .files()
            .iter()
            .filter(|info| is_snapshot_start(info))
            .map(|info| info.key.range().max())
            .min(),
        malformed_snapshots: inventory.malformed_snapshots(),
    }
}

/// Greedy interval cover over the eligible files of levels 0 to 8, one sweep
/// in order of first transaction.
fn extend(chain: &mut ContiguousChain, pool: &[FileInfo]) {
    let mut sweep: Vec<FileInfo> = pool
        .iter()
        .filter(|info| !info.key.level().is_snapshot())
        .copied()
        .collect();
    sweep.sort_by_key(|info| (info.key.range().min(), info.key));
    let mut sweep = sweep.into_iter().peekable();
    loop {
        let mut best: Option<FileInfo> = None;
        while let Some(info) = sweep.next_if(|i| touches(i.key.range().min(), chain.reaches())) {
            let beats = best.is_none_or(|held| preference(&info) > preference(&held));
            if info.key.range().max() > chain.reaches() && beats {
                best = Some(info);
            }
        }
        match best {
            Some(info) if chain.push(info) => {}
            Some(_) | None => break,
        }
    }
}

/// Larger is better: furthest reach, then higher level, then earlier start.
fn preference(info: &FileInfo) -> (Txid, Level, Reverse<Txid>) {
    let range = info.key.range();
    (range.max(), info.key.level(), Reverse(range.min()))
}

/// The chain ended before the target. Names why, for an operator.
///
/// A [`Txid`] target looks at every file, not just eligible ones: the file
/// that covers it is by definition ineligible.
fn explain_shortfall(
    inventory: &Inventory,
    pool: &[FileInfo],
    target: RestoreTarget,
    reach: Txid,
) -> PlanOutcome {
    let candidates = match target {
        RestoreTarget::Txid(_) => inventory.files(),
        RestoreTarget::Latest | RestoreTarget::AsOf(_) => pool,
    };
    let ahead = || candidates.iter().filter(|i| i.key.range().max() > reach);
    if let RestoreTarget::Txid(requested) = target
        && let Some(file) = ahead()
            .filter(|i| is_usable_beyond(i) && touches(i.key.range().min(), reach))
            .filter(|i| i.key.range().max() > requested)
            .min_by_key(|i| (i.key.range().max(), Reverse(i.key.level()), i.key))
    {
        return PlanOutcome::InsideFile {
            requested,
            file: file.key,
        };
    }
    let next = ahead()
        .filter(|i| is_usable_beyond(i) && !touches(i.key.range().min(), reach))
        .map(|i| i.key.range().min())
        .min();
    if let Some(next_starts) = next {
        return PlanOutcome::Gap {
            have_through: reach,
            next_starts,
        };
    }
    let refused = ahead()
        .map(|i| i.key)
        .filter(|k| k.level().is_snapshot())
        .min();
    match refused {
        Some(file) => PlanOutcome::MalformedSnapshot {
            have_through: reach,
            file,
        },
        // Unreachable by construction (a usable file that touches the end and
        // passes it would have extended the chain); an error, never a plan.
        None => PlanOutcome::Gap {
            have_through: reach,
            next_starts: reach.next().unwrap_or(reach),
        },
    }
}

/// A file that can hold history: anything but a level-9 file refused as a
/// snapshot.
fn is_usable_beyond(info: &FileInfo) -> bool {
    !info.key.level().is_snapshot() || is_snapshot_start(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsr_ltx::TxidRange;
    use proptest::prelude::*;
    use std::collections::HashSet;

    fn txid(raw: u64) -> Txid {
        Txid::new(raw).unwrap()
    }

    fn key(level: u8, min: u64, max: u64) -> FileKey {
        let range = TxidRange::new(txid(min), txid(max)).unwrap();
        FileKey::new(Level::new(level).unwrap(), range)
    }

    /// A file created at [`created`] milliseconds.
    fn file(level: u8, min: u64, max: u64, created: i64) -> FileInfo {
        FileInfo {
            key: key(level, min, max),
            size: 1,
            created_at: Timestamp::from_unix_millis(created),
        }
    }

    /// A file created at 100 ms per transaction, the usual shape.
    fn at(level: u8, min: u64, max: u64) -> FileInfo {
        file(level, min, max, ms(max))
    }

    /// 100 ms per transaction.
    fn ms(txn: u64) -> i64 {
        i64::try_from(txn).unwrap().saturating_mul(100)
    }

    fn inv(files: &[FileInfo]) -> Inventory {
        Inventory::from_files(files.iter().copied())
    }

    fn keys(outcome: &PlanOutcome) -> Vec<FileKey> {
        let PlanOutcome::Plan(chain) = outcome else {
            panic!("expected a plan, got {outcome:?}");
        };
        chain.files().iter().map(|f| f.key).collect()
    }

    const ALL_TARGETS: [RestoreTarget; 3] = [
        RestoreTarget::Latest,
        RestoreTarget::AsOf(Timestamp::from_unix_millis(1_000_000)),
        RestoreTarget::Txid(Txid::MIN),
    ];

    fn targets_through(txn: u64) -> [RestoreTarget; 3] {
        let [latest, as_of, _] = ALL_TARGETS;
        [latest, as_of, RestoreTarget::Txid(txid(txn))]
    }

    fn gap(have_through: u64, next_starts: u64) -> PlanOutcome {
        PlanOutcome::Gap {
            have_through: txid(have_through),
            next_starts: txid(next_starts),
        }
    }

    #[test]
    fn a_gap_at_level_zero_is_a_gap_for_every_target() {
        // Transaction 6 is gone from L0 and nothing covers it.
        let files = [
            at(9, 1, 3),
            at(0, 4, 4),
            at(0, 5, 5),
            at(0, 7, 7),
            at(0, 8, 8),
        ];
        for target in targets_through(8) {
            assert_eq!(plan(&inv(&files), target), gap(5, 7), "{target:?}");
        }
    }

    #[test]
    fn a_gap_at_a_higher_level_is_a_gap_for_every_target() {
        // L1 4-6 is gone; L0 4 to 6 were compacted away long ago.
        let files = [at(9, 1, 3), at(1, 7, 9), at(0, 10, 10)];
        for target in targets_through(10) {
            assert_eq!(plan(&inv(&files), target), gap(3, 7), "{target:?}");
        }
    }

    #[test]
    fn a_target_before_the_hole_still_plans() {
        let files = [at(9, 1, 3), at(0, 4, 4), at(0, 5, 5), at(0, 7, 7)];
        let outcome = plan(&inv(&files), RestoreTarget::Txid(txid(5)));
        assert_eq!(
            keys(&outcome),
            vec![key(9, 1, 3), key(0, 4, 4), key(0, 5, 5)]
        );
        let as_of = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(600)),
        );
        assert_eq!(keys(&as_of).last(), Some(&key(0, 5, 5)));
    }

    #[test]
    fn a_timestamp_restore_that_would_stop_at_a_hole_is_a_gap() {
        // The upstream bug: files 1-3, 4, then a hole, then 6 and 7 created
        // before the instant. Go restores 1-4 and exits 0.
        let files = [at(9, 1, 3), at(0, 4, 4), at(0, 6, 6), at(0, 7, 7)];
        let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(750));
        assert_eq!(plan(&inv(&files), target), gap(4, 6));
    }

    #[test]
    fn a_higher_level_file_spanning_the_hole_heals_it() {
        let files = [at(9, 1, 3), at(0, 4, 4), at(0, 6, 6), at(1, 5, 7)];
        for target in targets_through(7) {
            let outcome = plan(&inv(&files), target);
            assert_eq!(
                keys(&outcome),
                vec![key(9, 1, 3), key(0, 4, 4), key(1, 5, 7)],
                "{target:?}"
            );
        }
    }

    #[test]
    fn an_overlapping_higher_file_replaces_the_small_ones() {
        let files = [at(9, 1, 3), at(0, 4, 4), at(0, 5, 5), at(2, 3, 8)];
        let outcome = plan(&inv(&files), RestoreTarget::Latest);
        assert_eq!(keys(&outcome), vec![key(9, 1, 3), key(2, 3, 8)]);
    }

    #[test]
    fn equal_reach_prefers_the_higher_level_then_the_earlier_start() {
        let files = [
            at(9, 1, 3),
            at(0, 4, 6),
            at(2, 4, 6),
            at(2, 3, 6),
            at(1, 4, 6),
        ];
        let outcome = plan(&inv(&files), RestoreTarget::Latest);
        assert_eq!(keys(&outcome), vec![key(9, 1, 3), key(2, 3, 6)]);
    }

    #[test]
    fn a_target_inside_a_compacted_file_names_the_file() {
        let files = [at(9, 1, 3), at(1, 4, 9)];
        assert_eq!(
            plan(&inv(&files), RestoreTarget::Txid(txid(6))),
            PlanOutcome::InsideFile {
                requested: txid(6),
                file: key(1, 4, 9),
            }
        );
        // At the file's end it is a plan.
        let at_end = plan(&inv(&files), RestoreTarget::Txid(txid(9)));
        assert_eq!(keys(&at_end), vec![key(9, 1, 3), key(1, 4, 9)]);
    }

    #[test]
    fn a_target_inside_a_snapshot_names_it() {
        let files = [at(9, 1, 3), at(9, 1, 9), at(0, 10, 10)];
        assert_eq!(
            plan(&inv(&files), RestoreTarget::Txid(txid(6))),
            PlanOutcome::InsideFile {
                requested: txid(6),
                file: key(9, 1, 9),
            }
        );
    }

    #[test]
    fn a_target_with_the_finer_files_present_plans_to_it() {
        let files = [
            at(9, 1, 3),
            at(1, 4, 9),
            at(0, 4, 4),
            at(0, 5, 5),
            at(0, 6, 6),
        ];
        let outcome = plan(&inv(&files), RestoreTarget::Txid(txid(6)));
        assert_eq!(
            keys(&outcome),
            vec![key(9, 1, 3), key(0, 4, 4), key(0, 5, 5), key(0, 6, 6)]
        );
        let PlanOutcome::Plan(chain) = outcome else {
            panic!("plan expected");
        };
        assert_eq!(chain.reaches(), txid(6));
        assert_eq!(chain.snapshot().key, key(9, 1, 3));
    }

    #[test]
    fn the_newer_snapshot_is_used_when_eligible_and_the_older_for_earlier_targets() {
        let files = [
            at(9, 1, 3),
            at(9, 1, 6),
            at(0, 4, 4),
            at(0, 5, 5),
            at(0, 6, 6),
            at(0, 7, 7),
        ];
        let inventory = inv(&files);
        let latest = plan(&inventory, RestoreTarget::Latest);
        assert_eq!(keys(&latest), vec![key(9, 1, 6), key(0, 7, 7)]);
        let earlier = plan(&inventory, RestoreTarget::Txid(txid(5)));
        assert_eq!(
            keys(&earlier),
            vec![key(9, 1, 3), key(0, 4, 4), key(0, 5, 5)]
        );
        let instant = RestoreTarget::AsOf(Timestamp::from_unix_millis(450));
        assert_eq!(
            keys(&plan(&inventory, instant)),
            vec![key(9, 1, 3), key(0, 4, 4)]
        );
    }

    #[test]
    fn as_of_is_strictly_before_the_file_creation_time() {
        let files = [at(9, 1, 3), at(0, 4, 4)];
        let exactly = RestoreTarget::AsOf(Timestamp::from_unix_millis(400));
        assert_eq!(keys(&plan(&inv(&files), exactly)), vec![key(9, 1, 3)]);
        let after = RestoreTarget::AsOf(Timestamp::from_unix_millis(401));
        assert_eq!(keys(&plan(&inv(&files), after)).len(), 2);
    }

    #[test]
    fn as_of_later_than_every_file_is_latest() {
        let files = [at(9, 1, 3), at(0, 4, 4), at(0, 5, 5)];
        let late = RestoreTarget::AsOf(Timestamp::from_unix_millis(i64::MAX));
        assert_eq!(
            plan(&inv(&files), late),
            plan(&inv(&files), RestoreTarget::Latest)
        );
    }

    #[test]
    fn an_empty_replica_is_its_own_outcome() {
        for target in ALL_TARGETS {
            assert_eq!(
                plan(&Inventory::default(), target),
                PlanOutcome::EmptyReplica
            );
        }
    }

    #[test]
    fn without_an_eligible_snapshot_the_outcome_says_so() {
        let only_l0 = inv(&[at(0, 1, 1), at(0, 2, 2)]);
        let none = PlanOutcome::NoSnapshot {
            oldest_snapshot: None,
            malformed_snapshots: Vec::new(),
        };
        assert_eq!(plan(&only_l0, RestoreTarget::Latest), none);

        let files = inv(&[at(9, 1, 5), at(0, 6, 6)]);
        let older = PlanOutcome::NoSnapshot {
            oldest_snapshot: Some(txid(5)),
            malformed_snapshots: Vec::new(),
        };
        assert_eq!(plan(&files, RestoreTarget::Txid(txid(4))), older);
        let before_all = RestoreTarget::AsOf(Timestamp::from_unix_millis(10));
        assert_eq!(plan(&files, before_all), older);
    }

    #[test]
    fn a_level_nine_file_not_starting_at_one_is_never_a_snapshot() {
        let files = inv(&[at(9, 2, 5), at(0, 1, 1), at(0, 2, 2)]);
        assert_eq!(
            plan(&files, RestoreTarget::Latest),
            PlanOutcome::NoSnapshot {
                oldest_snapshot: None,
                malformed_snapshots: vec![key(9, 2, 5)],
            }
        );
    }

    #[test]
    fn history_only_in_a_refused_snapshot_is_reported_not_dropped() {
        let files = inv(&[at(9, 1, 3), at(0, 4, 4), at(9, 2, 9)]);
        assert_eq!(
            plan(&files, RestoreTarget::Latest),
            PlanOutcome::MalformedSnapshot {
                have_through: txid(4),
                file: key(9, 2, 9),
            }
        );
    }

    #[test]
    fn a_transaction_beyond_the_newest_is_its_own_outcome() {
        let files = inv(&[at(9, 1, 3), at(0, 4, 4)]);
        assert_eq!(
            plan(&files, RestoreTarget::Txid(txid(5))),
            PlanOutcome::BeyondNewest {
                requested: txid(5),
                newest: txid(4),
            }
        );
    }

    #[test]
    fn a_transaction_past_the_hole_is_a_gap_not_beyond() {
        let files = inv(&[at(9, 1, 3), at(0, 20, 30)]);
        assert_eq!(plan(&files, RestoreTarget::Txid(txid(25))), gap(3, 20));
    }

    // The oracle. It shares no code with the planner: its own file model, its
    // own eligibility, and a search over every chain instead of a greedy sweep.

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    struct Spec {
        level: u8,
        min: u64,
        max: u64,
        created: i64,
    }

    impl Spec {
        fn info(self) -> FileInfo {
            file(self.level, self.min, self.max, self.created)
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Goal {
        Latest,
        Txid(u64),
        AsOf(i64),
    }

    impl Goal {
        fn target(self) -> RestoreTarget {
            match self {
                Self::Latest => RestoreTarget::Latest,
                Self::Txid(n) => RestoreTarget::Txid(txid(n)),
                Self::AsOf(t) => RestoreTarget::AsOf(Timestamp::from_unix_millis(t)),
            }
        }

        fn admits(self, spec: &Spec) -> bool {
            match self {
                Self::Latest => true,
                Self::Txid(n) => spec.max <= n,
                Self::AsOf(t) => spec.created < t,
            }
        }
    }

    /// The transaction a correct restore must end at, if any file qualifies.
    fn model_end(files: &[Spec], goal: Goal) -> Option<u64> {
        match goal {
            Goal::Txid(n) => Some(n),
            Goal::Latest | Goal::AsOf(_) => {
                files.iter().filter(|s| goal.admits(s)).map(|s| s.max).max()
            }
        }
    }

    /// Every chain, depth first, without memoising: whether any ends at [`end`].
    fn some_chain_ends_at(files: &[&Spec], reach: u64, end: u64) -> bool {
        if reach == end {
            return true;
        }
        files
            .iter()
            .filter(|s| s.level < 9 && s.min <= reach.saturating_add(1) && s.max > reach)
            .any(|s| some_chain_ends_at(files, s.max, end))
    }

    fn model_has_plan(files: &[Spec], goal: Goal) -> bool {
        let Some(end) = model_end(files, goal) else {
            return false;
        };
        let admitted: Vec<&Spec> = files.iter().filter(|s| goal.admits(s)).collect();
        admitted
            .iter()
            .filter(|s| s.level == 9 && s.min == 1)
            .any(|s| some_chain_ends_at(&admitted, s.max, end))
    }

    /// One generated replica: [`n`] transactions, compactions, snapshots, then a
    /// random subset of files deleted.
    #[derive(Clone, Debug)]
    struct History {
        files: Vec<Spec>,
    }

    fn history() -> impl Strategy<Value = History> {
        let compaction = (1u8..=8, 1u64..=10, 1u64..=10, 0i64..40);
        let snapshot = (1u64..=10, prop::bool::weighted(0.15), 1u64..=10, 0i64..40);
        (
            1u64..=10,
            prop::collection::vec(compaction, 0..8),
            prop::collection::vec(snapshot, 0..4),
            prop::collection::vec(prop::bool::weighted(0.3), 40),
        )
            .prop_map(|(n, compactions, snapshots, deletions)| {
                let mut files = Vec::new();
                for txn in 1..=n {
                    files.push(Spec {
                        level: 0,
                        min: txn,
                        max: txn,
                        created: ms(txn),
                    });
                }
                for (level, a, b, jitter) in compactions {
                    let (min, max) = (a.min(b).min(n), a.max(b).min(n));
                    files.push(Spec {
                        level,
                        min,
                        max,
                        created: ms(max).saturating_add(jitter),
                    });
                }
                for (end, malformed, start, jitter) in snapshots {
                    let max = end.min(n);
                    let min = if malformed { start.min(max) } else { 1 };
                    files.push(Spec {
                        level: 9,
                        min,
                        max,
                        created: ms(max).saturating_add(jitter),
                    });
                }
                let mut seen = HashSet::new();
                files.retain(|s| seen.insert((s.level, s.min, s.max)));
                let mut flags = deletions.into_iter();
                files.retain(|_| !flags.next().unwrap_or(false));
                History { files }
            })
    }

    fn goal() -> impl Strategy<Value = Goal> {
        prop_oneof![
            Just(Goal::Latest),
            (1u64..=12).prop_map(Goal::Txid),
            (-50i64..=1200).prop_map(Goal::AsOf),
        ]
    }

    fn assert_chain_is_valid(chain: &ContiguousChain, files: &[Spec], goal: Goal) {
        let listed: Vec<(u8, u64, u64)> = files.iter().map(|s| (s.level, s.min, s.max)).collect();
        let infos = chain.files();
        let mut reach = 0u64;
        for (i, info) in infos.iter().enumerate() {
            let (level, min, max) = (
                info.key.level().get(),
                info.key.range().min().get(),
                info.key.range().max().get(),
            );
            assert!(listed.contains(&(level, min, max)), "not in the inventory");
            if i == 0 {
                assert!(
                    level == 9 && min == 1,
                    "first file must be a snapshot from 1"
                );
            } else {
                assert!(
                    level < 9 && min <= reach.saturating_add(1) && max > reach,
                    "chain invariant"
                );
            }
            let spec = files
                .iter()
                .find(|s| (s.level, s.min, s.max) == (level, min, max))
                .unwrap();
            assert!(goal.admits(spec), "ineligible file in the chain");
            reach = max;
        }
        assert_eq!(Some(reach), model_end(files, goal));
        assert_eq!(chain.reaches().get(), reach);
    }

    proptest! {
        #[test]
        fn plan_agrees_with_the_exhaustive_model(
            history in history(),
            goal in goal(),
            shuffle in any::<prop::sample::Index>(),
        ) {
            let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
            let outcome = plan(&Inventory::from_files(infos.clone()), goal.target());
            let model = model_has_plan(&history.files, goal);
            prop_assert_eq!(matches!(outcome, PlanOutcome::Plan(_)), model, "{:?}", outcome);
            if let PlanOutcome::Plan(chain) = &outcome {
                assert_chain_is_valid(chain, &history.files, goal);
            }
            if let PlanOutcome::Gap { have_through, next_starts } = &outcome {
                prop_assert!(next_starts.get() > have_through.get().saturating_add(1));
            }
            // Input order never matters: rotate and reverse.
            let mut shuffled = infos;
            let len = shuffled.len().max(1);
            shuffled.rotate_left(shuffle.index(len));
            shuffled.reverse();
            let again = plan(&Inventory::from_files(shuffled), goal.target());
            prop_assert_eq!(outcome, again);
        }
    }
}

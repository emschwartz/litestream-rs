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
//! - The base is the eligible file that begins at transaction 1 (any level;
//!   LTX calls such a file a snapshot) and reaches furthest. Ties go to the
//!   higher level, then the lower key. A base that reaches further can reach
//!   everything a nearer one can: any file that bridges from the nearer base
//!   also touches the further one's end. A level-9 file that begins later is
//!   never a base.
//! - Each step takes the eligible file, from levels 0 to 8, that touches the
//!   chain's end (first transaction at most one past it) and reaches furthest.
//!   Ties go to the higher level, then the lower first transaction: fewer,
//!   larger files. An overlapping file is valid because a file holds the
//!   newest version of every page it touches.
//! - A plan must end exactly at the target: at the requested transaction, or
//!   for [`RestoreTarget::Latest`] at the largest last transaction of any
//!   file. Anything short of that is explained, not returned.
//! - [`AsOf`] ends where the eligible chain ends, not at the largest eligible
//!   transaction (ruling A1). An eligible file past the end that does not
//!   connect is a stray left by level-0 retention, never a reason to refuse.
//!   What lies past the end is judged by keys over the whole inventory,
//!   stamps not consulted (`bridges_nothing_past`): a usable file that touches
//!   the end and passes it means the replica continues; none, while files
//!   exist past it, is a hole. The missing transactions may have committed
//!   before the instant, so the answer is the [`PlanOutcome::Gap`] (or
//!   malformed-snapshot refusal) that [`RestoreTarget::Latest`] reports for
//!   the same listing. A stray is otherwise ignored, full stop: the outcome is
//!   the one the listing gives without it. The plan never applies a file
//!   stamped at or after the instant.
//! - An instant can fall inside a file. Go stamps a compacted file with its
//!   newest input's time, so once the finer files are gone, the transactions
//!   after the chain's end up to the instant cannot be told from the rest of
//!   the file. That is [`PlanOutcome::AsOfInsideFile`], never a plain
//!   [`PlanOutcome::Plan`]: the chain is the newest state restorable at or
//!   before the instant, and the caller must choose to accept it. A file that
//!   ends right after the chain does not count: its newest transaction is
//!   stamped at or after the instant, so that transaction is placed, and so is
//!   everything after it.
//!
//! An `AsOf` restore learns eligibility lazily. A listing may carry only a
//! last-modified time, so [`AsOfPlanner`] is a [`Step`] that asks for one exact
//! stamp at a time, and only for the files a decision turns on: the
//! transaction-1 files in base order until the first eligible one, then at
//! each step the files that touch and pass the chain's end, in preference
//! order, until the first eligible one. What lies past the end is judged by
//! keys, and the straddle rule looks only at files the last step already
//! read. Each phase makes the decision [`plan`] makes with every stamp known,
//! so the order of the reads never changes the answer. [`plan`] stays the
//! planner for `Latest` and `Txid`, which read no stamp, and the exact
//! reference for `AsOf` over a listing whose stamps are all exact.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use lsr_ltx::{Timestamp, Txid};

use crate::inventory::{Inventory, is_snapshot_start};
use crate::layout::{FileInfo, FileKey, Level};
use crate::stamps::{Resolve, Stamps, Step};
/// The state of the database to rebuild.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RestoreTarget {
    /// The newest state the replica holds.
    Latest,
    /// The state right after this transaction.
    Txid(Txid),
    /// The newest state whose files were all created strictly before this
    /// instant. An instant later than every file is [`Latest`]. An instant
    /// inside a compacted file is [`PlanOutcome::AsOfInsideFile`].
    AsOf(Timestamp),
}

/// Files that rebuild one state, in the order to apply them.
///
/// Only the planner builds one. The first file starts at transaction 1, at any
/// level; every later file starts no later than one past the previous
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

    /// The files to apply, oldest state first: the base, then each extension.
    #[must_use]
    pub fn files(&self) -> &[FileInfo] {
        &self.files
    }

    /// The file the chain starts from: it begins at transaction 1.
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
    /// No eligible file begins at transaction 1: none exists, none is at or
    /// below a requested transaction, none is old enough for a requested
    /// instant, or the only level-9 files do not start at transaction 1.
    NoSnapshot {
        /// The smallest last transaction among all files that begin at
        /// transaction 1, whatever the target.
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
    /// An instant falls inside a file that is later than it. `chain` is the
    /// newest state restorable at or before the instant; `file` links to the
    /// chain and holds at least two transactions past it, but was created at or
    /// after the instant, so the transactions in it up to the instant cannot be
    /// separated from the rest. Only `AsOf` produces it, and only when
    /// the chain otherwise ends where the eligible history that connects to it
    /// ends: a [`Self::Gap`] wins. Accepting it can leave out transactions that
    /// did commit before the instant.
    AsOfInsideFile {
        /// The newest state restorable at or before the instant.
        chain: ContiguousChain,
        /// The nearest-ending file that straddles the instant.
        file: FileKey,
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
        RestoreTarget::AsOf(instant) => {
            info.exact_stamp().is_some_and(|stamp| stamp.at() < instant)
        }
    }
}

/// Plans a restore of [`target`] from [`inventory`].
///
/// For [`RestoreTarget::AsOf`] this reads each file's listed stamp, so it is
/// exact only over a listing whose stamps are all exact: a file listed with
/// only a last-modified time counts as ineligible. A restore plans `AsOf`
/// through [`AsOfPlanner`], which asks for the stamps it needs and returns
/// what this function returns with every stamp known.
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
        .max_by_key(|info| base_order(info))
        .and_then(|info| ContiguousChain::start(*info));
    let Some(mut chain) = base else {
        return no_snapshot(inventory);
    };
    extend(&mut chain, &pool);
    if let RestoreTarget::AsOf(_) = target {
        return as_of_outcome(inventory, chain, target);
    }
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

/// The outcome for an `AsOf` chain, which ends where the eligible files take
/// it. Eligible files past the end that do not connect are strays, never a
/// reason to refuse (ruling A1). What lies past the end is judged by keys over
/// the whole inventory ([`bridges_nothing_past`]); a chain that passes is
/// [`PlanOutcome::AsOfInsideFile`] when a later file straddles, else a plan. A
/// stray never changes which.
fn as_of_outcome(
    inventory: &Inventory,
    chain: ContiguousChain,
    target: RestoreTarget,
) -> PlanOutcome {
    let reach = chain.reaches();
    if bridges_nothing_past(inventory, reach) {
        return explain_shortfall(inventory, inventory.files(), RestoreTarget::Latest, reach);
    }
    match straddling_file(inventory, target, reach) {
        Some(file) => PlanOutcome::AsOfInsideFile { chain, file },
        None => PlanOutcome::Plan(chain),
    }
}

/// Step 3 of an `AsOf` plan, by keys alone (stamps are not consulted): the
/// listing holds files past the chain's end, but no usable file, whatever its
/// stamp, touches the end and passes it. The files the instant cut away are
/// all that could have said what the missing transactions were stamped, so
/// the instant cannot be certified: the refusal is what [`RestoreTarget::Latest`]
/// reports for the same listing. A refused level-9 file cannot link: it cannot
/// be applied. A usable file that does link means the replica continues, even
/// when it is stamped at or after the instant.
fn bridges_nothing_past(inventory: &Inventory, reach: Txid) -> bool {
    let files = inventory.files();
    let links = |info: &FileInfo| {
        is_usable_beyond(info)
            && touches(info.key.range().min(), reach)
            && info.key.range().max() > reach
    };
    files.iter().any(|info| info.key.range().max() > reach) && !files.iter().any(links)
}

/// The file that makes an `AsOf` chain ambiguous, if any: usable, created at
/// or after the instant, linking to the chain and holding at least two
/// transactions past it. When some file ends exactly one past the chain, that
/// transaction is stamped at or after the instant (otherwise the chain would
/// have taken the file), so nothing is ambiguous.
fn straddling_file(inventory: &Inventory, target: RestoreTarget, reach: Txid) -> Option<FileKey> {
    let RestoreTarget::AsOf(_) = target else {
        return None;
    };
    let one_past = reach.next()?;
    let files = inventory.files();
    if files.iter().any(|info| info.key.range().max() == one_past) {
        return None;
    }
    files
        .iter()
        .filter(|info| !eligible(info, target) && straddles(info, one_past))
        .min_by_key(|info| straddler_order(info))
        .map(|info| info.key)
}

/// Whether a usable file links to a chain whose next transaction is
/// [`one_past`] and holds at least two transactions past the chain.
fn straddles(info: &FileInfo, one_past: Txid) -> bool {
    let range = info.key.range();
    is_usable_beyond(info) && range.min() <= one_past && range.max() > one_past
}

/// Smaller is named first: the nearest end, then the higher level, then the key.
fn straddler_order(info: &FileInfo) -> (Txid, Reverse<Level>, FileKey) {
    (info.key.range().max(), Reverse(info.key.level()), info.key)
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
    sweep.sort_by_key(sweep_order);
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

/// The preference of a level 0-8 file at an extension step; larger is better.
type Preference = (Txid, Level, Reverse<Txid>);

/// Where an [`AsOfPlanner`] is.
#[derive(Clone, Debug)]
enum Phase {
    /// Walking the transaction-1 files for the first eligible one.
    Base,
    /// Extending the chain one eligible file at a time.
    Extend,
    /// Judging what lies past the reach.
    Past,
    /// Decided.
    Done(PlanOutcome),
}

/// An [`RestoreTarget::AsOf`] plan that learns eligibility one stamp at a time.
///
/// Driven by [`crate::resolve`], it returns exactly what [`plan`] returns for
/// the same instant with every stamp known, and asks only for the stamps of
/// transaction-1 files and of files that touch and pass the chain's end at
/// some step. Each key is asked for at most once. It sorts the inventory once
/// and resumes from its cursor on every advance.
#[derive(Clone, Debug)]
pub struct AsOfPlanner<'a> {
    inventory: &'a Inventory,
    instant: Timestamp,
    /// Transaction-1 files, most preferred base first.
    starts: Vec<FileInfo>,
    next_start: usize,
    /// Level 0-8 files by first transaction, then key.
    sweep: Vec<FileInfo>,
    /// How many of `sweep` have touched the chain's end so far.
    entered: usize,
    /// Indexes into `sweep` that touched the end and were not yet settled.
    touching: BinaryHeap<(Preference, usize)>,
    chain: Option<ContiguousChain>,
    phase: Phase,
    comparisons: u64,
}

impl<'a> AsOfPlanner<'a> {
    /// A planner for the newest state whose files were all created strictly
    /// before [`instant`].
    #[must_use]
    pub fn new(inventory: &'a Inventory, instant: Timestamp) -> Self {
        let mut comparisons = 0_u64;
        let mut counted = |order: Ordering| {
            comparisons = comparisons.saturating_add(1);
            order
        };
        let mut starts: Vec<FileInfo> = inventory
            .files()
            .iter()
            .filter(|info| is_snapshot_start(info))
            .copied()
            .collect();
        starts.sort_by(|a, b| counted(base_order(b).cmp(&base_order(a))));
        let mut sweep: Vec<FileInfo> = inventory
            .files()
            .iter()
            .filter(|info| !info.key.level().is_snapshot())
            .copied()
            .collect();
        sweep.sort_by(|a, b| counted(sweep_order(a).cmp(&sweep_order(b))));
        let phase = if inventory.is_empty() {
            Phase::Done(PlanOutcome::EmptyReplica)
        } else {
            Phase::Base
        };
        Self {
            inventory,
            instant,
            starts,
            next_start: 0,
            sweep,
            entered: 0,
            touching: BinaryHeap::new(),
            chain: None,
            phase,
            comparisons,
        }
    }

    /// Key comparisons made so far: the two sorts exactly, each heap operation
    /// counted at the heap's depth (twice for a pop). Reported so a test can
    /// hold the planner to O(N log N).
    #[must_use]
    pub const fn comparisons(&self) -> u64 {
        self.comparisons
    }

    /// Phase 1: the first eligible transaction-1 file in base order is the
    /// base, which is the one [`plan`] picks.
    fn find_base(&mut self, stamps: &Stamps) -> Resolve<()> {
        while let Some(info) = self.starts.get(self.next_start).copied() {
            match eligible_at(&info, self.instant, stamps) {
                None => return Resolve::Need(info),
                Some(true) => {
                    self.chain = ContiguousChain::start(info);
                    self.phase = match self.chain {
                        Some(_) => Phase::Extend,
                        None => Phase::Done(no_snapshot(self.inventory)),
                    };
                    return Resolve::Done(());
                }
                Some(false) => self.next_start = self.next_start.saturating_add(1),
            }
        }
        self.phase = Phase::Done(no_snapshot(self.inventory));
        Resolve::Done(())
    }

    /// Phase 2: at each step the most preferred eligible file that touches
    /// and passes the end, which is the file [`plan`]'s sweep pushes (a file
    /// it passes over ends at or before the one chosen, so it never passes a
    /// later end). A settled file leaves the heap for good: an ineligible one
    /// stays ineligible, and a file at or below the end stays there.
    fn extend(&mut self, stamps: &Stamps) -> Resolve<()> {
        let Some(chain) = self.chain.as_mut() else {
            self.phase = Phase::Done(no_snapshot(self.inventory));
            return Resolve::Done(());
        };
        loop {
            let reach = chain.reaches();
            while let Some(info) = self
                .sweep
                .get(self.entered)
                .filter(|info| touches(info.key.range().min(), reach))
            {
                self.touching.push((preference(info), self.entered));
                self.comparisons = self.comparisons.saturating_add(depth(self.touching.len()));
                self.entered = self.entered.saturating_add(1);
            }
            // The heap's top reaches furthest, so a top at or below the end
            // means nothing left passes it.
            let Some(info) = self
                .touching
                .peek()
                .and_then(|(_, at)| self.sweep.get(*at))
                .copied()
            else {
                break;
            };
            if info.key.range().max() <= reach {
                break;
            }
            let Some(admitted) = eligible_at(&info, self.instant, stamps) else {
                return Resolve::Need(info);
            };
            let pop = depth(self.touching.len()).saturating_mul(2);
            self.comparisons = self.comparisons.saturating_add(pop);
            self.touching.pop();
            // The top touches and passes the end, so the push cannot fail;
            // stopping mirrors [`extend`] if it ever did.
            if admitted && !chain.push(info) {
                break;
            }
        }
        self.phase = Phase::Past;
        Resolve::Done(())
    }

    /// Phase 3: what lies past the reach, by keys ([`bridges_nothing_past`]),
    /// then the straddle rule, whose candidates all touch and pass the reach
    /// and so were read by the last step.
    fn judge_past(&mut self, stamps: &Stamps) -> Resolve<()> {
        let Some(chain) = self.chain.as_ref() else {
            self.phase = Phase::Done(no_snapshot(self.inventory));
            return Resolve::Done(());
        };
        let reach = chain.reaches();
        let outcome = if bridges_nothing_past(self.inventory, reach) {
            explain_shortfall(
                self.inventory,
                self.inventory.files(),
                RestoreTarget::Latest,
                reach,
            )
        } else {
            match lazy_straddler(self.inventory, self.instant, reach, stamps) {
                Resolve::Need(info) => return Resolve::Need(info),
                Resolve::Done(Some(file)) => PlanOutcome::AsOfInsideFile {
                    chain: chain.clone(),
                    file,
                },
                Resolve::Done(None) => PlanOutcome::Plan(chain.clone()),
            }
        };
        self.phase = Phase::Done(outcome);
        Resolve::Done(())
    }
}

impl Step for AsOfPlanner<'_> {
    type Output = PlanOutcome;

    fn advance(&mut self, stamps: &Stamps) -> Resolve<PlanOutcome> {
        loop {
            let step = match &self.phase {
                Phase::Done(outcome) => return Resolve::Done(outcome.clone()),
                Phase::Base => self.find_base(stamps),
                Phase::Extend => self.extend(stamps),
                Phase::Past => self.judge_past(stamps),
            };
            if let Resolve::Need(info) = step {
                return Resolve::Need(info);
            }
        }
    }
}

/// Whether a file is created strictly before [`instant`], once its exact
/// stamp is known.
fn eligible_at(info: &FileInfo, instant: Timestamp, stamps: &Stamps) -> Option<bool> {
    stamps.exact(info).map(|stamp| stamp.at() < instant)
}

/// [`straddling_file`] with stamps asked for as needed.
fn lazy_straddler(
    inventory: &Inventory,
    instant: Timestamp,
    reach: Txid,
    stamps: &Stamps,
) -> Resolve<Option<FileKey>> {
    let Some(one_past) = reach.next() else {
        return Resolve::Done(None);
    };
    let files = inventory.files();
    if files.iter().any(|info| info.key.range().max() == one_past) {
        return Resolve::Done(None);
    }
    let mut best: Option<&FileInfo> = None;
    for info in files.iter().filter(|info| straddles(info, one_past)) {
        match eligible_at(info, instant, stamps) {
            None => return Resolve::Need(*info),
            Some(true) => {}
            Some(false) => {
                if best.is_none_or(|held| straddler_order(info) < straddler_order(held)) {
                    best = Some(info);
                }
            }
        }
    }
    Resolve::Done(best.map(|info| info.key))
}

/// Larger is the better base: furthest reach, then higher level, then the
/// lower key.
fn base_order(info: &FileInfo) -> (Txid, Level, Reverse<FileKey>) {
    (info.key.range().max(), info.key.level(), Reverse(info.key))
}

fn sweep_order(info: &FileInfo) -> (Txid, FileKey) {
    (info.key.range().min(), info.key)
}

/// The comparisons one sift through a heap of [`len`] entries can take.
fn depth(len: usize) -> u64 {
    u64::from(usize::BITS.saturating_sub(len.leading_zeros()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{ListedStamp, StoredStamp};
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
            listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(created))),
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
        Inventory::from_files(files.iter().copied()).unwrap()
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

    /// A healthy writer-shaped replica after level-0 retention: L0 0x125 to
    /// 0x12b are gone, the level 1 file that covers them is stamped after the
    /// instant, and the stray L0 files 0x12c and 0x12d are stamped before it.
    /// Go restores 0x124. The planner reaches it too, but says it lies inside
    /// L1 0x125-0x12f; Latest still reaches 0x140.
    fn retention_replica() -> Vec<FileInfo> {
        let mut files = vec![
            file(9, 1, 0x124, 360_086),
            file(1, 0x11b, 0x124, 360_086),
            file(1, 0x125, 0x12f, 361_086),
            file(1, 0x130, 0x13a, 362_086),
        ];
        for (step, t) in (0x12c..=0x140u64).enumerate() {
            let stamp =
                360_786_i64.saturating_add(100_i64.saturating_mul(i64::try_from(step).unwrap()));
            files.push(file(0, t, t, stamp));
        }
        files
    }

    #[test]
    fn a_stray_file_before_the_instant_is_not_a_gap_and_the_straddler_still_shows() {
        let files = retention_replica();
        let as_of = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(360_900)),
        );
        let PlanOutcome::AsOfInsideFile { chain, file } = as_of else {
            panic!("expected AsOfInsideFile, got {as_of:?}");
        };
        assert_eq!(chain.reaches(), txid(0x124));
        assert_eq!(file, key(1, 0x125, 0x12f));
        let latest = plan(&inv(&files), RestoreTarget::Latest);
        let PlanOutcome::Plan(chain) = latest else {
            panic!("expected a plan, got {latest:?}");
        };
        assert_eq!(chain.reaches(), txid(0x140));
    }

    #[test]
    fn as_of_before_a_hole_reports_the_gap() {
        let files = [file(0, 1, 2, 100), file(0, 5, 6, 200)];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
        );
        assert_eq!(
            outcome,
            PlanOutcome::Gap {
                have_through: txid(2),
                next_starts: txid(5)
            }
        );
        assert_eq!(
            outcome,
            plan(&inv(&files), RestoreTarget::Latest),
            "the Latest planner reports the same hole"
        );
    }

    #[test]
    fn as_of_before_a_hole_that_a_later_snapshot_covers_is_not_a_gap() {
        let files = [file(0, 1, 2, 100), file(0, 5, 6, 200), file(9, 1, 6, 300)];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
        );
        assert!(
            matches!(outcome, PlanOutcome::AsOfInsideFile { ref file, .. } if file.level().get() == 9),
            "{outcome:?}"
        );
    }

    #[test]
    fn as_of_with_a_linking_file_stamped_after_the_instant_keeps_its_outcome() {
        let files = [file(0, 1, 2, 100), file(0, 3, 6, 200)];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
        );
        assert!(
            matches!(outcome, PlanOutcome::AsOfInsideFile { .. }),
            "{outcome:?}"
        );
    }

    #[test]
    fn as_of_with_nothing_past_the_reach_plans() {
        let files = [file(0, 1, 2, 100)];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
        );
        assert_eq!(keys(&outcome), vec![key(0, 1, 2)]);
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
        // Transaction 6 is absent and may have committed before the instant.
        assert_eq!(as_of, gap(5, 7));
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
        let starts_late = inv(&[at(0, 2, 2), at(0, 3, 3)]);
        let none = PlanOutcome::NoSnapshot {
            oldest_snapshot: None,
            malformed_snapshots: Vec::new(),
        };
        assert_eq!(plan(&starts_late, RestoreTarget::Latest), none);

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
    fn a_replica_with_only_level_zero_files_from_transaction_one_plans_latest() {
        let files = inv(&[at(0, 1, 1), at(0, 2, 2), at(0, 3, 3)]);
        let outcome = plan(&files, RestoreTarget::Latest);
        assert_eq!(
            keys(&outcome),
            vec![key(0, 1, 1), key(0, 2, 2), key(0, 3, 3)]
        );
    }

    #[test]
    fn a_level_one_file_from_transaction_one_is_a_base() {
        let files = inv(&[at(1, 1, 30), at(0, 31, 31), at(0, 32, 32)]);
        let outcome = plan(&files, RestoreTarget::Latest);
        assert_eq!(
            keys(&outcome),
            vec![key(1, 1, 30), key(0, 31, 31), key(0, 32, 32)]
        );
        let PlanOutcome::Plan(chain) = outcome else {
            panic!("expected a plan");
        };
        assert_eq!(chain.snapshot().key, key(1, 1, 30));
    }

    #[test]
    fn an_instant_older_than_the_first_level_nine_file_plans_from_level_zero() {
        let files = inv(&[
            at(0, 1, 1),
            at(0, 2, 2),
            at(0, 3, 3),
            at(0, 4, 4),
            at(9, 1, 4),
        ]);
        let instant = RestoreTarget::AsOf(Timestamp::from_unix_millis(ms(1) + 1));
        assert_eq!(keys(&plan(&files, instant)), vec![key(0, 1, 1)]);
    }

    #[test]
    fn the_further_reaching_base_wins_and_a_tie_goes_to_the_higher_level() {
        let files = inv(&[at(9, 1, 5), at(2, 1, 8), at(0, 9, 9)]);
        let outcome = plan(&files, RestoreTarget::Latest);
        assert_eq!(keys(&outcome), vec![key(2, 1, 8), key(0, 9, 9)]);

        let tied = inv(&[at(9, 1, 5), at(2, 1, 5)]);
        assert_eq!(
            keys(&plan(&tied, RestoreTarget::Latest)),
            vec![key(9, 1, 5)]
        );
    }

    #[test]
    fn a_replica_whose_lowest_file_begins_at_two_has_no_snapshot() {
        let files = inv(&[at(0, 2, 2), at(1, 2, 4)]);
        assert_eq!(
            plan(&files, RestoreTarget::Latest),
            PlanOutcome::NoSnapshot {
                oldest_snapshot: None,
                malformed_snapshots: Vec::new(),
            }
        );
    }

    #[test]
    fn the_oldest_snapshot_counts_files_of_every_level_that_begin_at_one() {
        let files = inv(&[at(0, 1, 1), at(9, 1, 5), at(0, 2, 2)]);
        let before_all = RestoreTarget::AsOf(Timestamp::from_unix_millis(10));
        assert_eq!(
            plan(&files, before_all),
            PlanOutcome::NoSnapshot {
                oldest_snapshot: Some(txid(1)),
                malformed_snapshots: Vec::new(),
            }
        );
    }

    #[test]
    fn a_level_nine_file_not_starting_at_one_is_never_a_snapshot() {
        let files = inv(&[at(9, 2, 5), at(0, 2, 2)]);
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

    #[test]
    fn an_instant_inside_a_compacted_file_is_its_own_outcome() {
        // L1 4-10 is stamped with its newest input's time; the L0 files are gone.
        let files = [file(9, 1, 3, 300), file(1, 4, 10, 1000)];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
        );
        let PlanOutcome::AsOfInsideFile { chain, file } = outcome else {
            panic!("expected the inside-file outcome, got {outcome:?}");
        };
        assert_eq!(chain.reaches(), txid(3));
        assert_eq!(file, key(1, 4, 10));
        // At or after the file's stamp the file is eligible and it is a plan.
        let after = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(1001)),
        );
        assert_eq!(keys(&after), vec![key(9, 1, 3), key(1, 4, 10)]);
    }

    #[test]
    fn a_snapshot_can_straddle_the_instant_too() {
        let files = [file(9, 1, 3, 300), file(9, 1, 10, 1000)];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
        );
        let PlanOutcome::AsOfInsideFile { file, .. } = outcome else {
            panic!("expected the inside-file outcome, got {outcome:?}");
        };
        assert_eq!(file, key(9, 1, 10));
    }

    #[test]
    fn a_refused_snapshot_is_not_a_straddler() {
        let files = [file(9, 1, 3, 300), file(9, 2, 10, 1000)];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
        );
        // Not AsOfInsideFile; the history past the chain is unusable, which is
        // what Latest reports for the same listing.
        assert_eq!(
            outcome,
            PlanOutcome::MalformedSnapshot {
                have_through: txid(3),
                file: key(9, 2, 10)
            }
        );
    }

    #[test]
    fn a_file_that_pins_the_next_transaction_removes_the_doubt() {
        // L0 5 is stamped 500, after the instant, so transaction 5 happened
        // after it and so did everything the big L1 file adds.
        let files = [
            file(9, 1, 3, 300),
            file(0, 4, 4, 400),
            file(0, 5, 5, 500),
            file(1, 4, 9, 900),
        ];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(450)),
        );
        assert_eq!(keys(&outcome), vec![key(9, 1, 3), key(0, 4, 4)]);
    }

    #[test]
    fn a_stray_past_a_straddling_file_is_still_inside_file() {
        // L0 20 is stamped before the instant but does not connect (ruling A1:
        // ignored, full stop), and L1 4-9 links, so the replica continues. The
        // outcome is the one without the stray: the state at 3, inside L1 4-9.
        let files = [
            file(9, 1, 3, 300),
            file(1, 4, 9, 1000),
            file(0, 20, 20, 500),
        ];
        let outcome = plan(
            &inv(&files),
            RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
        );
        let PlanOutcome::AsOfInsideFile { chain, file } = outcome else {
            panic!("expected AsOfInsideFile, got {outcome:?}");
        };
        assert_eq!(chain.reaches(), txid(3));
        assert_eq!(file, key(1, 4, 9));
    }

    #[test]
    fn the_other_targets_never_say_inside_file_for_a_timestamp() {
        let files = [file(9, 1, 3, 300), file(1, 4, 10, 1000)];
        for target in [RestoreTarget::Latest, RestoreTarget::Txid(txid(10))] {
            assert_eq!(
                keys(&plan(&inv(&files), target)),
                vec![key(9, 1, 3), key(1, 4, 10)]
            );
        }
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
    /// An instant ends at the furthest state some chain of admitted files
    /// reaches, whatever else is admitted (ruling A1).
    fn model_end(files: &[Spec], goal: Goal) -> Option<u64> {
        match goal {
            Goal::Txid(n) => Some(n),
            Goal::Latest => files.iter().map(|s| s.max).max(),
            Goal::AsOf(_) => {
                let admitted: Vec<&Spec> = files.iter().filter(|s| goal.admits(s)).collect();
                admitted
                    .iter()
                    .map(|s| s.max)
                    .filter(|end| {
                        admitted
                            .iter()
                            .filter(|b| b.min == 1)
                            .any(|b| some_chain_ends_at(&admitted, b.max, *end))
                    })
                    .max()
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
            .filter(|s| s.min == 1)
            .any(|s| some_chain_ends_at(&admitted, s.max, end))
            && !model_hole(files, goal, end)
    }

    /// An instant before a hole: some file, of any stamp, lies past the end,
    /// and none of them starts at or before one past it.
    fn model_hole(files: &[Spec], goal: Goal, end: u64) -> bool {
        let Goal::AsOf(_) = goal else {
            return false;
        };
        files.iter().any(|s| s.max > end)
            && !files.iter().any(|s| {
                (s.level < 9 || s.min == 1) && s.min <= end.saturating_add(1) && s.max > end
            })
    }

    /// Whether an instant cuts through a file: some usable file later than the
    /// instant links to the end and holds two or more transactions past it, no
    /// file ends right after the end (which would date that transaction).
    fn model_straddlers(files: &[Spec], goal: Goal, end: u64) -> Vec<Spec> {
        let Goal::AsOf(_) = goal else {
            return Vec::new();
        };
        let Some(one_past) = end.checked_add(1) else {
            return Vec::new();
        };
        if files.iter().any(|s| s.max == one_past) {
            return Vec::new();
        }
        files
            .iter()
            .filter(|s| {
                !goal.admits(s)
                    && (s.level < 9 || s.min == 1)
                    && s.min <= one_past
                    && s.max > one_past
            })
            .copied()
            .collect()
    }

    /// One generated replica: [`n`] transactions, compactions, snapshots, then a
    /// random subset of files deleted. Ids start at [`base`] plus one, so some
    /// histories sit against `Txid::MAX`.
    #[derive(Clone, Debug)]
    struct History {
        files: Vec<Spec>,
        base: u64,
    }

    fn history() -> impl Strategy<Value = History> {
        // A compacted file is stamped long after its range, so an instant can
        // land inside it: the jitter reaches past the next transactions.
        let compaction = (1u8..=8, 1u64..=10, 1u64..=10, 0i64..1500);
        let snapshot = (1u64..=10, prop::bool::weighted(0.15), 1u64..=10, 0i64..1500);
        (
            prop_oneof![Just(0u64), Just(u64::MAX - 10)],
            1u64..=10,
            prop::collection::vec(compaction, 0..8),
            prop::collection::vec(snapshot, 0..4),
            prop::collection::vec(prop::bool::weighted(0.3), 40),
        )
            .prop_map(|(base, n, compactions, snapshots, deletions)| {
                let id = |idx: u64| base.saturating_add(idx);
                let mut files = Vec::new();
                for txn in 1..=n {
                    files.push(Spec {
                        level: 0,
                        min: id(txn),
                        max: id(txn),
                        created: ms(txn),
                    });
                }
                for (level, a, b, jitter) in compactions {
                    let (min, max) = (a.min(b).min(n), a.max(b).min(n));
                    files.push(Spec {
                        level,
                        min: id(min),
                        max: id(max),
                        created: ms(max).saturating_add(jitter),
                    });
                }
                for (end, malformed, start, jitter) in snapshots {
                    let max = end.min(n);
                    let min = if malformed { id(start.min(max)) } else { 1 };
                    files.push(Spec {
                        level: 9,
                        min,
                        max: id(max),
                        created: ms(max).saturating_add(jitter),
                    });
                }
                let mut seen = HashSet::new();
                files.retain(|s| seen.insert((s.level, s.min, s.max)));
                let mut flags = deletions.into_iter();
                files.retain(|_| !flags.next().unwrap_or(false));
                History { files, base }
            })
    }

    fn goal(base: u64) -> impl Strategy<Value = Goal> {
        prop_oneof![
            Just(Goal::Latest),
            (1u64..=10).prop_map(move |d| Goal::Txid(base.saturating_add(d))),
            (-50i64..=2600).prop_map(Goal::AsOf),
        ]
    }

    fn case() -> impl Strategy<Value = (History, Goal)> {
        history().prop_flat_map(|history| {
            let base = history.base;
            (Just(history), goal(base))
        })
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
                assert!(min == 1, "first file must begin at the first transaction");
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
        #![proptest_config(ProptestConfig::with_cases(2000))]
        #[test]
        fn as_of_never_plans_across_a_hole_latest_reports(history in history(), t in -50i64..=2600) {
            let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
            let inventory = Inventory::from_files(infos).unwrap();
            let reach = match plan(&inventory, RestoreTarget::AsOf(Timestamp::from_unix_millis(t))) {
                PlanOutcome::Plan(chain) | PlanOutcome::AsOfInsideFile { chain, .. } => chain.reaches(),
                PlanOutcome::EmptyReplica
                | PlanOutcome::NoSnapshot { .. }
                | PlanOutcome::BeyondNewest { .. }
                | PlanOutcome::Gap { .. }
                | PlanOutcome::InsideFile { .. }
                | PlanOutcome::MalformedSnapshot { .. } => return Ok(()),
            };
            match plan(&inventory, RestoreTarget::Latest) {
                PlanOutcome::Gap { have_through, .. }
                | PlanOutcome::MalformedSnapshot { have_through, .. } => {
                    prop_assert!(have_through > reach, "planned at {reach}, Latest stops at {have_through}");
                }
                PlanOutcome::Plan(_)
                | PlanOutcome::EmptyReplica
                | PlanOutcome::NoSnapshot { .. }
                | PlanOutcome::BeyondNewest { .. }
                | PlanOutcome::AsOfInsideFile { .. }
                | PlanOutcome::InsideFile { .. } => {}
            }
        }

        #[test]
        fn as_of_never_reaches_further_than_latest_and_applies_only_older_files(
            history in history(),
            t in -50i64..=2600,
        ) {
            let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
            let inventory = Inventory::from_files(infos).unwrap();
            let (PlanOutcome::Plan(chain) | PlanOutcome::AsOfInsideFile { chain, .. }) =
                plan(&inventory, RestoreTarget::AsOf(Timestamp::from_unix_millis(t)))
            else {
                return Ok(());
            };
            for info in chain.files() {
                prop_assert!(info.exact_stamp().expect("an exact stamp").at() < Timestamp::from_unix_millis(t), "{info:?} is not older than {t}");
            }
            if let PlanOutcome::Plan(latest) = plan(&inventory, RestoreTarget::Latest) {
                prop_assert!(chain.reaches() <= latest.reaches());
            }
        }

        /// Strays are unobservable: when a usable file links past the reach, the
        /// outcome is the same with every eligible file that does not connect to
        /// the chain removed. (Without a linking file, the files past the reach
        /// are the hole rule's evidence, so removing them would change the answer.)
        #[test]
        fn removing_every_stray_leaves_a_continuing_outcome_unchanged(
            history in history(),
            t in -50i64..=2600,
        ) {
            let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
            let inventory = Inventory::from_files(infos.clone()).unwrap();
            let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(t));
            let outcome = plan(&inventory, target);
            let (PlanOutcome::Plan(chain) | PlanOutcome::AsOfInsideFile { chain, .. }) = &outcome else {
                return Ok(());
            };
            let reach = chain.reaches();
            let continues = infos.iter().any(|i| {
                is_usable_beyond(i) && touches(i.key.range().min(), reach) && i.key.range().max() > reach
            });
            if !continues {
                return Ok(());
            }
            let instant = Timestamp::from_unix_millis(t);
            let kept: Vec<FileInfo> = infos
                .into_iter()
                .filter(|i| {
                    let stray = i.exact_stamp().expect("an exact stamp").at() < instant
                        && !touches(i.key.range().min(), reach)
                        && i.key.range().max() > reach;
                    !stray
                })
                .collect();
            let without = plan(&Inventory::from_files(kept).unwrap(), target);
            prop_assert_eq!(without, outcome);
        }

        #[test]
        fn plan_agrees_with_the_exhaustive_model(
            (history, goal) in case(),
            order in prop::collection::vec(any::<u32>(), 40),
        ) {
            let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
            let inventory = Inventory::from_files(infos.clone()).unwrap();
            let outcome = plan(&inventory, goal.target());
            let has_plan = model_has_plan(&history.files, goal);
            let straddlers = match model_end(&history.files, goal) {
                Some(end) if has_plan => model_straddlers(&history.files, goal, end),
                Some(_) | None => Vec::new(),
            };
            let inside = !straddlers.is_empty();
            prop_assert_eq!(
                matches!(outcome, PlanOutcome::Plan(_)),
                has_plan && !inside,
                "{:?}",
                outcome
            );
            prop_assert_eq!(
                matches!(outcome, PlanOutcome::AsOfInsideFile { .. }),
                inside,
                "{:?}",
                outcome
            );
            match &outcome {
                PlanOutcome::Plan(chain) => assert_chain_is_valid(chain, &history.files, goal),
                PlanOutcome::AsOfInsideFile { chain, file } => {
                    assert_chain_is_valid(chain, &history.files, goal);
                    let named = straddlers.iter().find(|s| {
                        (s.level, s.min, s.max)
                            == (file.level().get(), file.range().min().get(), file.range().max().get())
                    });
                    prop_assert!(named.is_some(), "{:?} is not a straddler", file);
                }
                PlanOutcome::Gap { have_through, next_starts } => {
                    prop_assert!(next_starts.get() > have_through.get().saturating_add(1));
                }
                PlanOutcome::BeyondNewest { requested, newest } => {
                    let top = history.files.iter().map(|s| s.max).max();
                    prop_assert_eq!(Some(newest.get()), top);
                    prop_assert!(requested > newest);
                }
                PlanOutcome::InsideFile { requested, file } => {
                    prop_assert!(file.range().max() > *requested);
                    let listed = history.files.iter().any(|s| {
                        (s.level, s.min, s.max)
                            == (file.level().get(), file.range().min().get(), file.range().max().get())
                    });
                    prop_assert!(listed);
                }
                PlanOutcome::EmptyReplica => prop_assert!(history.files.is_empty()),
                PlanOutcome::NoSnapshot { .. } | PlanOutcome::MalformedSnapshot { .. } => {}
            }
            // A real permutation of the input (keys are random sort keys): the
            // inventory is the same value and so is the plan.
            let mut shuffled: Vec<(u32, FileInfo)> = order
                .iter()
                .copied()
                .cycle()
                .zip(infos)
                .collect();
            shuffled.sort_by_key(|(rank, _)| *rank);
            let shuffled = Inventory::from_files(shuffled.into_iter().map(|(_, info)| info)).unwrap();
            prop_assert_eq!(&shuffled, &inventory);
            prop_assert_eq!(plan(&shuffled, goal.target()), outcome);
        }
    }
}

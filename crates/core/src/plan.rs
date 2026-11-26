//! The restore planner: which files, in what order, rebuild a requested state.
//!
//! Pure: a function of an [`Inventory`] and a [`RestoreTarget`], with no I/O and
//! no clock. The answer is either a [`ContiguousChain`] that ends exactly where
//! the target says, or a typed reason there is no exact answer. It is never a
//! shorter history: upstream's timestamp restore refuses a hole whose next file was
//! created by the instant, but stops at one whose next file was created after it and
//! exits 0, and this planner reports [`PlanOutcome::Gap`] for both.
//!
//! # Contract
//!
//! A chain for a target is a run of eligible files: the first begins at
//! transaction 1 (any level), each later one is from levels 0 to 8, starts at
//! most one past the previous end and ends beyond it. Its reach is its last
//! transaction. Eligibility: [`RestoreTarget::Latest`] admits every file;
//! [`Txid`] admits files whose last transaction is at or below the target;
//! [`AsOf`] admits files created strictly before the instant (a created-at time is when the
//! engine first staged the transaction, at or after its commit: [`crate::Dating::Commit`], so
//! `AsOf` never applies a commit made after the instant, and may leave out commits made before
//! it that the engine had not yet seen, such as those made while it was down). Go skips a file
//! when [`!info.CreatedAt.Before(timestamp)`] in both the snapshot loop and
//! [`restoreLevelCursor.refresh`] (replica.go), the same comparison.
//!
//! For `Latest` and `Txid` the outcome is [`PlanOutcome::Plan`] if and only if
//! an eligible chain from a transaction-1 base reaches the target exactly, and
//! the plan is such a chain. The target is the requested transaction, or for
//! `Latest` the largest last transaction of any file, refused ones included:
//! history held only in a refused file makes `Latest` refuse, not plan to the
//! usable end.
//!
//! Otherwise the refusal is the first of these that applies: request validity,
//! then a starting point, then the first obstacle at the chain's end, where the
//! reach `r` is the furthest any eligible chain gets.
//! 1. [`PlanOutcome::EmptyReplica`]: the listing holds no file.
//! 2. [`PlanOutcome::BeyondNewest`]: a `Txid` past every file. A wrong request
//!    is not reported as a broken replica.
//! 3. [`PlanOutcome::NoSnapshot`]: for `Latest` and `Txid`, no file at all
//!    begins at transaction 1; for `AsOf`, none created before the instant.
//! 4. [`PlanOutcome::InsideFile`] (`Txid` only): a usable file links to the
//!    chain (starts at most one past `r`, with `r` = 0 when no eligible file
//!    begins at transaction 1) and ends past the target. Only finer files are
//!    missing, so any hole lies beyond the target and does not matter.
//! 5. [`PlanOutcome::Gap`]: a usable file lies past `r` and does not link.
//! 6. [`PlanOutcome::MalformedSnapshot`]: only refused level-9 files lie past
//!    `r`. Gap wins over it because a hole is the claim an operator can act on.
//!
//! Payloads are public fields, so callers match on them. Requirements, true of
//! every outcome: `have_through` is `r`; `requested` and `newest` are as
//! named; `oldest_snapshot` is the smallest last transaction of all files that
//! begin at transaction 1, whatever the target; `malformed_snapshots` is every
//! refused level-9 key, in ascending key order (level, first
//! transaction, last transaction); the file `InsideFile` names
//! is usable, links and passes the target; the file `MalformedSnapshot` names
//! is a refused file past `r`. Design choices among files that all qualify,
//! pinned by tests but open to change: `InsideFile` names the nearest-ending
//! file, then the higher level, then the lower key; `Gap::next_starts` is the
//! smallest first transaction over every usable file past `r` that does not
//! link, whatever the target and eligibility (where history resumes is a fact
//! about the replica, not the request); `MalformedSnapshot` names the lowest
//! key; `AsOfInsideFile` names, among the straddlers below, the
//! nearest-ending file, then the higher level, then the lower key.
//!
//! How a plan is chosen within the contract:
//! - The base is the eligible file that begins at transaction 1 (any level;
//!   LTX calls such a file a snapshot) and reaches furthest. Ties go to the
//!   higher level. A base that reaches further can reach everything a nearer
//!   one can: any file that bridges from the nearer base also touches the
//!   further one's end. A level-9 file that begins later is never a base.
//! - Each step takes the eligible file, from levels 0 to 8, that touches the
//!   chain's end (first transaction at most one past it) and reaches furthest.
//!   Ties go to the higher level, then the lower first transaction: fewer,
//!   larger files. An overlapping file is valid because a file holds the
//!   newest version of every page it touches.
//! - [`AsOf`] ends where the eligible chain ends, not at the largest eligible
//!   transaction. An eligible file past the end that does not
//!   connect is a stray left by level-0 retention, never a reason to refuse.
//!   What lies past the end is judged by keys over the whole inventory,
//!   created-at times not consulted (`bridges_nothing_past`): a usable file that touches
//!   the end and passes it means the replica continues; none, while files
//!   exist past it, is a hole. The missing transactions may have committed
//!   before the instant, so the answer is the [`PlanOutcome::Gap`] (or
//!   malformed-snapshot refusal) that [`RestoreTarget::Latest`] reports for
//!   the same listing. A stray is otherwise ignored, full stop: the outcome is
//!   the one the listing gives without it. The plan never applies a file
//!   recorded as created at or after the instant.
//! - An instant can fall inside a file. Go records a compacted file's created-at time as its
//!   newest input's time, so once the finer files are gone, the transactions
//!   after the chain's end up to the instant cannot be told from the rest of
//!   the file. That is [`PlanOutcome::AsOfInsideFile`], never a plain
//!   [`PlanOutcome::Plan`]: the chain is the newest state restorable at or
//!   before the instant, and [`crate::AsOfInside::TakeEarlier`] on the restore
//!   (the CLI's `--allow-earlier`) accepts it. A file that ends right after
//!   the chain removes the doubt (`dates_next`) only when its time dates
//!   that transaction's commit ([`crate::Dating::Commit`]): a level 0-8 file
//!   with its stored created-at time. It is ineligible (else the chain would
//!   have taken it), and its created-at time is the created-at time of the
//!   level-0 file that first carried its newest transaction, because a
//!   level-0 file is that carrier and compaction gives its output its newest
//!   input's created-at time (here and in Go). So that transaction was
//!   created at or after the instant, and so was everything after it. A
//!   snapshot proves nothing of the kind: it is recorded as created when it
//!   is written, which can be long after its newest transaction committed.
//!   Nor does a time the store fell back to for want of stored metadata (a
//!   writer before Go v0.5.1, a bucket copy that dropped it): that is the
//!   object's upload. Such a file that ends right after the chain is a
//!   straddler itself, since its one transaction may have committed before
//!   the instant. Times are read only through [`crate::Stamp`], which carries
//!   this table.
//!
//! An `AsOf` restore learns eligibility lazily. A listing may carry only a
//! last-modified time, so [`AsOfPlanner`] is a [`Step`] that asks for one exact
//! created-at time at a time, and only for the files a decision turns on: the
//! transaction-1 files in base order until the first eligible one, then at
//! each step the files that touch and pass the chain's end, in preference
//! order, until the first eligible one. What lies past the end is judged by
//! keys, and the straddle rule looks only at files the last step already
//! read. Each pass makes the decision [`plan`] makes with every created-at time known,
//! so the order of the reads never changes the answer. When it asks for a
//! base's created-at time it names the later bases ahead, and when it judges the
//! straddlers it names all of them, so a store that runs batches reads those
//! side by side; the bases ahead are a guess, and a few may be read for
//! nothing once an earlier one is eligible. [`plan`] is the planner
//! for [`KeyTarget`] (`Latest` and `Txid`), which read no created-at time; `plan_reference`
//! is the exact `AsOf` planner over a listing whose created-at times are all exact, kept
//! as the eager twin the lazy one is checked against. The contract above is the oracle:
//! `crates/spec` restates it, clause by clause, in a crate that cannot reach this one.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use lsr_ltx::{Timestamp, Txid};

use crate::created_at::{Need, Resolve, ResolvedCreatedAt, Step};
use crate::inventory::{Inventory, Listed, is_snapshot_start};
use crate::layout::{CreatedAt, FileInfo, FileKey, Level, Stamp};
/// The state of the database to rebuild.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum RestoreTarget {
    /// The newest state the replica holds.
    #[default]
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
        let fits = touches(range.min(), self.reaches)
            && range.max() > self.reaches
            && !next.key.level().is_snapshot();
        if fits {
            self.files.push(next);
            self.reaches = range.max();
        }
        fits
    }

    /// Recomputes the chain's invariants from its files alone and checks every
    /// file is in the listing: the greedy cover in `extend` is the only thing
    /// that built it, so nothing else would notice a gap. Expensive, so only
    /// under [`crate::VERIFY`].
    fn is_contiguous_in(&self, listed: Listed<'_>) -> bool {
        let mut reaches = None;
        for info in &self.files {
            let range = info.key.range();
            let fits = match reaches {
                None => is_snapshot_start(info),
                Some(end) => {
                    !info.key.level().is_snapshot()
                        && touches(range.min(), end)
                        && range.max() > end
                }
            };
            if !fits || !listed.files().any(|file| file == info) {
                return false;
            }
            reaches = Some(range.max());
        }
        reaches == Some(self.reaches) && self.files.first() == Some(&self.snapshot)
    }

    /// [`Self::is_contiguous_in`] over a whole inventory.
    #[cfg(test)]
    fn is_contiguous(&self, inventory: &Inventory) -> bool {
        self.is_contiguous_in(Listed::all(inventory))
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
    /// No eligible file begins at transaction 1: none exists, none is old
    /// enough for a requested instant, or the only level-9 files do not start
    /// at transaction 1. For `Latest` and `Txid` it means no file at all begins
    /// there; a transaction-1 file that covers a requested transaction is
    /// [`Self::InsideFile`].
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
    /// chain and passes it, but was created at or after the instant, so the
    /// transactions in it up to the instant cannot be separated from the
    /// rest. It holds at least two transactions past the chain, or one whose
    /// time does not date its commit (a snapshot's write, an upload). Only
    /// `AsOf` produces it, and only when the chain otherwise ends where the
    /// eligible history that connects to it ends: a [`Self::Gap`] wins. Accepting it can leave out transactions that
    /// did commit before the instant.
    AsOfInsideFile {
        /// The newest state restorable at or before the instant.
        chain: ContiguousChain,
        /// The nearest-ending file that straddles the instant.
        file: FileKey,
    },
    /// The chain stops short and the nearest usable file beyond it starts after
    /// a hole.
    Gap {
        /// The last transaction the chain reaches.
        have_through: Txid,
        /// The smallest first transaction among every usable file past the hole,
        /// eligible or not.
        next_starts: Txid,
    },
    /// A requested transaction falls strictly inside a file that links to the
    /// chain, and the finer files that would stop at it are gone.
    InsideFile {
        /// What was asked for.
        requested: Txid,
        /// The nearest-ending file that covers it.
        file: FileKey,
    },
    /// Every file past the chain's end is a level-9 file that does not start at
    /// transaction 1, whatever the target.
    MalformedSnapshot {
        /// The last transaction the chain reaches.
        have_through: Txid,
        /// The lowest-keyed refused file that holds history beyond it.
        file: FileKey,
    },
}

impl PlanOutcome {
    /// The chain when this is the plan, else the outcome itself, so a caller names only the
    /// variants it answers differently.
    ///
    /// # Errors
    /// Every outcome that is not [`Self::Plan`], returned unchanged.
    pub fn into_chain(self) -> Result<ContiguousChain, Self> {
        match self {
            Self::Plan(chain) => Ok(chain),
            other @ (Self::EmptyReplica
            | Self::NoSnapshot { .. }
            | Self::BeyondNewest { .. }
            | Self::AsOfInsideFile { .. }
            | Self::Gap { .. }
            | Self::InsideFile { .. }
            | Self::MalformedSnapshot { .. }) => Err(other),
        }
    }

    /// Where the chain of a plan, or of an instant inside a file, ends; `None` for every
    /// other outcome.
    #[must_use]
    pub const fn reaches(&self) -> Option<Txid> {
        match self {
            Self::Plan(chain) | Self::AsOfInsideFile { chain, .. } => Some(chain.reaches),
            Self::EmptyReplica
            | Self::NoSnapshot { .. }
            | Self::BeyondNewest { .. }
            | Self::Gap { .. }
            | Self::InsideFile { .. }
            | Self::MalformedSnapshot { .. } => None,
        }
    }

    /// The plan's chain, `None` for every other outcome.
    #[must_use]
    pub const fn chain(&self) -> Option<&ContiguousChain> {
        match self {
            Self::Plan(chain) => Some(chain),
            Self::EmptyReplica
            | Self::NoSnapshot { .. }
            | Self::BeyondNewest { .. }
            | Self::AsOfInsideFile { .. }
            | Self::Gap { .. }
            | Self::InsideFile { .. }
            | Self::MalformedSnapshot { .. } => None,
        }
    }
}

/// Whether a file starting at [`min`] can follow a chain that ends at [`reach`].
pub(crate) fn touches(min: Txid, reach: Txid) -> bool {
    reach.next().is_none_or(|one_past| min <= one_past)
}

fn eligible(info: &FileInfo, target: RestoreTarget) -> bool {
    match target {
        RestoreTarget::Latest => true,
        RestoreTarget::Txid(wanted) => info.key.range().max() <= wanted,
        RestoreTarget::AsOf(instant) => {
            stamp_of(info, info.created_at()).is_some_and(|stamp| stamp.before(instant))
        }
    }
}

/// A file's created-at time, when known, with what it dates for its level.
fn stamp_of(info: &FileInfo, created_at: Option<CreatedAt>) -> Option<Stamp> {
    created_at.map(|created_at| Stamp::of(info.key.level(), created_at))
}

/// The targets that need no created-at time: the only ones [`plan`] accepts.
///
/// An `AsOf` plan on a listing whose created-at times are not all exact would quietly
/// answer with a shorter history, so the type keeps it off [`plan`]; it goes
/// through [`AsOfPlanner`], which asks for the created-at times it needs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KeyTarget {
    /// The newest state the replica holds.
    Latest,
    /// The state right after this transaction.
    Txid(Txid),
}

impl From<KeyTarget> for RestoreTarget {
    fn from(target: KeyTarget) -> Self {
        match target {
            KeyTarget::Latest => Self::Latest,
            KeyTarget::Txid(txid) => Self::Txid(txid),
        }
    }
}

/// Plans a restore of [`target`] from [`inventory`], by keys alone.
#[must_use]
pub fn plan(inventory: &Inventory, target: KeyTarget) -> PlanOutcome {
    plan_exact(Listed::all(inventory), target.into())
}

/// [`plan`] over the inventory without the keys in `gone` (ascending): what a
/// restore would do if those files were deleted, answered without copying the
/// inventory.
pub(crate) fn plan_without(
    inventory: &Inventory,
    gone: &[FileKey],
    target: KeyTarget,
) -> PlanOutcome {
    plan_exact(Listed::without(inventory, gone), target.into())
}

/// The exact planner for any [`RestoreTarget`], the oracle for [`AsOfPlanner`]
/// in tests and the fuzz checker.
///
/// For `AsOf` it reads each file's listed created-at time, so it is exact only over a
/// listing whose created-at times are all exact: a file listed with only a last-modified
/// time counts as ineligible. That is why it exists only under the `testing`
/// feature and production code cannot call it (a source guard checks the
/// callers too).
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
#[must_use]
pub fn plan_reference(inventory: &Inventory, target: RestoreTarget) -> PlanOutcome {
    plan_exact(Listed::all(inventory), target)
}

fn plan_exact(listed: Listed<'_>, target: RestoreTarget) -> PlanOutcome {
    let outcome = plan_unchecked(listed, target);
    if crate::VERIFY
        && let PlanOutcome::Plan(chain) | PlanOutcome::AsOfInsideFile { chain, .. } = &outcome
    {
        debug_assert!(
            chain.is_contiguous_in(listed),
            "the planner returned a chain that is not contiguous: {chain:?}"
        );
    }
    outcome
}

fn plan_unchecked(listed: Listed<'_>, target: RestoreTarget) -> PlanOutcome {
    let Some(newest) = listed.newest() else {
        return PlanOutcome::EmptyReplica;
    };
    if let RestoreTarget::Txid(requested) = target
        && requested > newest
    {
        return PlanOutcome::BeyondNewest { requested, newest };
    }
    // The eligible files are read in place, never copied: a million-file
    // inventory is planned in the memory it already holds.
    let pool = || listed.files().filter(|info| eligible(info, target));
    let base = pool()
        .filter(|info| is_snapshot_start(info))
        .max_by_key(|info| base_order(info))
        .and_then(|info| ContiguousChain::start(*info));
    let Some(mut chain) = base else {
        return inside_the_base(listed, target).unwrap_or_else(|| no_snapshot(listed));
    };
    extend(&mut chain, Sweep::new(listed, target));
    if let RestoreTarget::AsOf(_) = target {
        return as_of_outcome(listed, chain, target);
    }
    let want = match target {
        RestoreTarget::Txid(requested) => Some(requested),
        RestoreTarget::Latest | RestoreTarget::AsOf(_) => {
            pool().map(|info| info.key.range().max()).max()
        }
    };
    if want == Some(chain.reaches()) {
        return PlanOutcome::Plan(chain);
    }
    explain_shortfall(listed, target, chain.reaches())
}

/// The outcome for an `AsOf` chain, which ends where the eligible files take
/// it. Eligible files past the end that do not connect are strays, never a
/// reason to refuse. What lies past the end is judged by keys over
/// the whole inventory ([`bridges_nothing_past`]); a chain that passes is
/// [`PlanOutcome::AsOfInsideFile`] when a later file straddles, else a plan. A
/// stray never changes which.
fn as_of_outcome(listed: Listed<'_>, chain: ContiguousChain, target: RestoreTarget) -> PlanOutcome {
    let reach = chain.reaches();
    if bridges_nothing_past(listed, reach) {
        return explain_shortfall(listed, RestoreTarget::Latest, reach);
    }
    match straddling_file(listed, target, reach) {
        Some(file) => PlanOutcome::AsOfInsideFile { chain, file },
        None => PlanOutcome::Plan(chain),
    }
}

/// Step 3 of an `AsOf` plan, by keys alone (created-at times are not consulted): the
/// listing holds files past the chain's end, but no usable file, whatever its
/// created-at time, touches the end and passes it. The files the instant cut away are
/// all that could have said when the missing transactions were created, so
/// the instant cannot be certified: the refusal is what [`RestoreTarget::Latest`]
/// reports for the same listing. A refused level-9 file cannot link: it cannot
/// be applied. A usable file that does link means the replica continues, even
/// when it is recorded as created at or after the instant.
fn bridges_nothing_past(listed: Listed<'_>, reach: Txid) -> bool {
    listed.files().any(|info| info.key.range().max() > reach)
        && !listed.files().any(|info| passes(info, reach))
}

/// Whether a usable file links to a chain that ends at [`reach`] and passes it.
fn passes(info: &FileInfo, reach: Txid) -> bool {
    is_usable_beyond(info)
        && touches(info.key.range().min(), reach)
        && info.key.range().max() > reach
}

/// The file that makes an `AsOf` chain ambiguous, if any ([`straddler`]),
/// over the listed created-at times.
fn straddling_file(listed: Listed<'_>, target: RestoreTarget, reach: Txid) -> Option<FileKey> {
    let RestoreTarget::AsOf(instant) = target else {
        return None;
    };
    straddler(listed.files(), reach, instant, |info| {
        stamp_of(info, info.created_at())
    })
}

/// The straddle rule, given each file's [`Stamp`] (`None`: not known). Every
/// file that passes the chain's end and is not recorded as created before
/// the instant leaves the transactions past the end undated, unless one of
/// them dates the next transaction ([`dates_next`]); then nothing is
/// ambiguous. Otherwise the nearest-ending one is named. A file holding one
/// transaction past the end straddles too when its time is a snapshot's
/// write or an upload: that transaction may have committed before the
/// instant.
fn straddler<'a>(
    files: impl Iterator<Item = &'a FileInfo> + Clone,
    reach: Txid,
    instant: Timestamp,
    stamp: impl Fn(&FileInfo) -> Option<Stamp>,
) -> Option<FileKey> {
    let one_past = reach.next()?;
    let passing = || files.clone().filter(|info| passes(info, reach));
    if passing().any(|info| dates_next(info, one_past, instant, stamp(info))) {
        return None;
    }
    passing()
        .filter(|info| !stamp(info).is_some_and(|known| known.before(instant)))
        .min_by_key(|info| straddler_order(info))
        .map(|info| info.key)
}

/// Whether a file proves that transaction [`one_past`] committed at or after
/// an `AsOf` instant: it ends at [`one_past`] and its time dates that commit
/// ([`Stamp::commit_at_or_after`]). Only a level 0-8 file with its stored
/// created-at time can: a level-0 file is stamped at the sync that first
/// carried the transaction, and a compacted file takes its newest input's
/// time, which recurses down to it. A snapshot is stamped when it is written
/// (`snapshot.rs`), and a time a store fell back to is the object's upload,
/// both any time after the commit, so neither dates anything.
fn dates_next(info: &FileInfo, one_past: Txid, instant: Timestamp, stamp: Option<Stamp>) -> bool {
    info.key.range().max() == one_past
        && stamp.and_then(|known| known.commit_at_or_after(instant)) == Some(true)
}

/// Smaller is named first: the nearest end, then the higher level, then the key.
fn straddler_order(info: &FileInfo) -> (Txid, Reverse<Level>, FileKey) {
    (info.key.range().max(), Reverse(info.key.level()), info.key)
}

/// A `Txid` target with no eligible base: the chain is empty, so its reach is
/// 0 and a file links when it begins at transaction 1. Such a file ends past
/// the target (else it would be the base), so the transaction exists only
/// inside it: the same cause as with an older base, and the same answer.
fn inside_the_base(listed: Listed<'_>, target: RestoreTarget) -> Option<PlanOutcome> {
    let RestoreTarget::Txid(requested) = target else {
        return None;
    };
    inside_file(
        listed.files().filter(|i| i.key.range().min().get() == 1),
        requested,
    )
}

/// The nearest-ending usable file among [`linking`] ones: the file the target
/// falls inside. The caller supplies the files that link to the chain, because
/// the file must link: otherwise a hole lies between, and finer files would
/// not help. Every usable linking file ends past [`requested`]: one ending at or
/// before it would be eligible, so it would be the base (the eligible start
/// that ends last) or the sweep would have extended the chain with it.
fn inside_file<'a>(
    linking: impl Iterator<Item = &'a FileInfo>,
    requested: Txid,
) -> Option<PlanOutcome> {
    linking
        .filter(|i| is_usable_beyond(i))
        .min_by_key(|i| (i.key.range().max(), Reverse(i.key.level()), i.key))
        .map(|file| {
            debug_assert!(file.key.range().max() > requested, "{file:?}");
            PlanOutcome::InsideFile {
                requested,
                file: file.key,
            }
        })
}

fn no_snapshot(listed: Listed<'_>) -> PlanOutcome {
    PlanOutcome::NoSnapshot {
        oldest_snapshot: listed
            .files()
            .filter(|info| is_snapshot_start(info))
            .map(|info| info.key.range().max())
            .min(),
        malformed_snapshots: listed.malformed_snapshots(),
    }
}

/// Greedy interval cover over the eligible files of levels 0 to 8, one sweep
/// in order of first transaction. The touching file that ends last wins (the
/// preference leads with the end); when even it does not pass the reach,
/// `push` refuses it and the cover is done.
fn extend<'a>(chain: &mut ContiguousChain, sweep: impl Iterator<Item = &'a FileInfo>) {
    let mut sweep = sweep.peekable();
    loop {
        let mut best: Option<FileInfo> = None;
        while let Some(info) = sweep.next_if(|i| touches(i.key.range().min(), chain.reaches())) {
            if best.is_none_or(|held| preference(info) > preference(&held)) {
                best = Some(*info);
            }
        }
        match best {
            Some(info) if chain.push(info) => {}
            Some(_) | None => break,
        }
    }
}

/// The eligible files of levels 0 to 8 in [`sweep_order`], merged from the
/// inventory's own per-level runs (each already ascending in that order, since
/// a key sorts by first transaction within a level), so nothing is copied or
/// sorted and the sweep holds nine slices however many files there are.
struct Sweep<'a> {
    listed: Listed<'a>,
    target: RestoreTarget,
    /// What is left of each level; the snapshot level stays empty.
    runs: [&'a [FileInfo]; Level::ALL.len()],
}

impl<'a> Sweep<'a> {
    fn new(listed: Listed<'a>, target: RestoreTarget) -> Self {
        let runs = Level::ALL.map(|level| {
            if level.is_snapshot() {
                &[]
            } else {
                listed.level_slice(level)
            }
        });
        Self {
            listed,
            target,
            runs,
        }
    }
}

impl<'a> Iterator for Sweep<'a> {
    type Item = &'a FileInfo;

    fn next(&mut self) -> Option<&'a FileInfo> {
        let mut best: Option<&mut &'a [FileInfo]> = None;
        for run in &mut self.runs {
            while let Some((head, rest)) = run.split_first() {
                if self.listed.keeps(head) && eligible(head, self.target) {
                    break;
                }
                *run = rest;
            }
            let Some(head) = run.first() else { continue };
            let earlier = best
                .as_ref()
                .and_then(|held| held.first())
                .is_none_or(|held| sweep_order(head) < sweep_order(held));
            if earlier {
                best = Some(run);
            }
        }
        let run = best?;
        let (head, rest) = run.split_first()?;
        *run = rest;
        Some(head)
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
fn explain_shortfall(listed: Listed<'_>, target: RestoreTarget, reach: Txid) -> PlanOutcome {
    let candidates = || {
        listed.files().filter(move |info| match target {
            RestoreTarget::Txid(_) => true,
            RestoreTarget::Latest | RestoreTarget::AsOf(_) => eligible(info, target),
        })
    };
    let ahead = || candidates().filter(|i| i.key.range().max() > reach);
    if let RestoreTarget::Txid(requested) = target
        && let Some(outcome) = inside_file(
            ahead().filter(|i| touches(i.key.range().min(), reach)),
            requested,
        )
    {
        return outcome;
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
enum Pass {
    /// Walking the transaction-1 files for the first eligible one.
    Base,
    /// Extending the chain one eligible file at a time.
    Extend,
    /// Judging what lies past the reach.
    Past,
    /// Decided.
    Done(PlanOutcome),
}

/// An [`RestoreTarget::AsOf`] plan that learns eligibility one created-at time at a time.
///
/// Driven by [`crate::resolve`], it returns exactly what [`plan`] returns for
/// the same instant with every created-at time known, and asks only for the created-at times of
/// transaction-1 files and of files that touch and pass the chain's end at
/// some step. Each key is asked for at most once. It sorts the inventory once
/// and resumes from its cursor on every advance.
#[derive(Clone, Debug)]
pub struct AsOfPlanner<'a> {
    inventory: &'a Inventory,
    instant: Timestamp,
    /// Transaction-1 files, most preferred base first. Borrowed from the
    /// inventory: a pointer a file, not a second copy of it.
    starts: Vec<&'a FileInfo>,
    next_start: usize,
    /// Level 0-8 files by first transaction, then key; pointers, as `starts`.
    sweep: Vec<&'a FileInfo>,
    /// How many of `sweep` have touched the chain's end so far.
    entered: usize,
    /// Indexes into `sweep` that touched the end and were not yet settled.
    touching: BinaryHeap<(Preference, usize)>,
    chain: Option<ContiguousChain>,
    pass: Pass,
    #[cfg(any(test, feature = "testing"))]
    comparisons: u64,
}

impl<'a> AsOfPlanner<'a> {
    /// A planner for the newest state whose files were all created strictly
    /// before [`instant`].
    #[must_use]
    pub fn new(inventory: &'a Inventory, instant: Timestamp) -> Self {
        #[cfg(any(test, feature = "testing"))]
        let mut comparisons = 0_u64;
        #[cfg(any(test, feature = "testing"))]
        let mut counted = |order: Ordering| {
            comparisons = comparisons.saturating_add(1);
            order
        };
        #[cfg(not(any(test, feature = "testing")))]
        let counted = |order: Ordering| order;
        let mut starts: Vec<&FileInfo> = inventory
            .files()
            .iter()
            .filter(|info| is_snapshot_start(info))
            .collect();
        starts.sort_unstable_by(|a, b| counted(base_order(b).cmp(&base_order(a))));
        let mut sweep: Vec<&FileInfo> = inventory
            .files()
            .iter()
            .filter(|info| !info.key.level().is_snapshot())
            .collect();
        sweep.sort_unstable_by(|a, b| counted(sweep_order(a).cmp(&sweep_order(b))));
        let pass = if inventory.is_empty() {
            Pass::Done(PlanOutcome::EmptyReplica)
        } else {
            Pass::Base
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
            pass,
            #[cfg(any(test, feature = "testing"))]
            comparisons,
        }
    }

    /// Key comparisons made so far: the two sorts exactly, each heap operation
    /// counted at the heap's depth (twice for a pop). Reported so a test can
    /// hold the planner to O(N log N).
    #[must_use]
    #[cfg(any(test, feature = "testing"))]
    pub const fn comparisons(&self) -> u64 {
        self.comparisons
    }

    /// Pass 1: the first eligible transaction-1 file in base order is the
    /// base, which is the one [`plan`] picks.
    fn find_base(&mut self, resolved: &ResolvedCreatedAt) -> Resolve<()> {
        while let Some(info) = self.starts.get(self.next_start).map(|info| **info) {
            match eligible_at(&info, self.instant, resolved) {
                None => {
                    // The bases after it, in the order they would be tried:
                    // read ahead, a few may go unused once one is eligible.
                    let rest = self.starts.iter().skip(self.next_start.saturating_add(1));
                    let ahead = rest
                        .filter(|later| resolved.exact(later).is_none())
                        .map(|later| **later);
                    return Resolve::Need(Need::with_ahead(info, ahead));
                }
                Some(true) => {
                    self.chain = ContiguousChain::start(info);
                    self.pass = match self.chain {
                        Some(_) => Pass::Extend,
                        None => Pass::Done(no_snapshot(Listed::all(self.inventory))),
                    };
                    return Resolve::Done(());
                }
                Some(false) => self.next_start = self.next_start.saturating_add(1),
            }
        }
        self.pass = Pass::Done(no_snapshot(Listed::all(self.inventory)));
        Resolve::Done(())
    }

    /// Pass 2: at each step the most preferred eligible file that touches
    /// and passes the end, which is the file [`plan`]'s sweep pushes (a file
    /// it passes over ends at or before the one chosen, so it never passes a
    /// later end). A settled file leaves the heap for good: an ineligible one
    /// stays ineligible, and a file at or below the end stays there.
    fn extend(&mut self, resolved: &ResolvedCreatedAt) -> Resolve<()> {
        let Some(chain) = self.chain.as_mut() else {
            self.pass = Pass::Done(no_snapshot(Listed::all(self.inventory)));
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
                #[cfg(any(test, feature = "testing"))]
                {
                    self.comparisons = self.comparisons.saturating_add(depth(self.touching.len()));
                }
                self.entered = self.entered.saturating_add(1);
            }
            // The heap's top reaches furthest, so a top at or below the end
            // means nothing left passes it.
            let Some(info) = self
                .touching
                .peek()
                .and_then(|(_, at)| self.sweep.get(*at))
                .map(|info| **info)
            else {
                break;
            };
            if info.key.range().max() <= reach {
                break;
            }
            let Some(admitted) = eligible_at(&info, self.instant, resolved) else {
                return Resolve::Need(Need::one(info));
            };
            #[cfg(any(test, feature = "testing"))]
            {
                let pop = depth(self.touching.len()).saturating_mul(2);
                self.comparisons = self.comparisons.saturating_add(pop);
            }
            self.touching.pop();
            // The top touches and passes the end, so the push cannot fail;
            // stopping mirrors [`extend`] if it ever did.
            if admitted && !chain.push(info) {
                break;
            }
        }
        self.pass = Pass::Past;
        Resolve::Done(())
    }

    /// Pass 3: what lies past the reach, by keys ([`bridges_nothing_past`]),
    /// then the straddle rule, whose candidates all touch and pass the reach
    /// and so were read by the last step.
    fn judge_past(&mut self, resolved: &ResolvedCreatedAt) -> Resolve<()> {
        let Some(chain) = self.chain.as_ref() else {
            self.pass = Pass::Done(no_snapshot(Listed::all(self.inventory)));
            return Resolve::Done(());
        };
        let reach = chain.reaches();
        let listed = Listed::all(self.inventory);
        let outcome = if bridges_nothing_past(listed, reach) {
            explain_shortfall(listed, RestoreTarget::Latest, reach)
        } else {
            match lazy_straddler(self.inventory, self.instant, reach, resolved) {
                Resolve::Need(info) => return Resolve::Need(info),
                Resolve::Done(Some(file)) => PlanOutcome::AsOfInsideFile {
                    chain: chain.clone(),
                    file,
                },
                Resolve::Done(None) => PlanOutcome::Plan(chain.clone()),
            }
        };
        self.pass = Pass::Done(outcome);
        Resolve::Done(())
    }
}

impl Step for AsOfPlanner<'_> {
    type Output = PlanOutcome;

    fn advance(&mut self, resolved: &ResolvedCreatedAt) -> Resolve<PlanOutcome> {
        loop {
            let step = match &self.pass {
                Pass::Done(outcome) => return Resolve::Done(outcome.clone()),
                Pass::Base => self.find_base(resolved),
                Pass::Extend => self.extend(resolved),
                Pass::Past => self.judge_past(resolved),
            };
            if let Resolve::Need(info) = step {
                return Resolve::Need(info);
            }
        }
    }
}

/// Whether a file is created strictly before [`instant`], once its exact
/// created-at time is known.
fn eligible_at(info: &FileInfo, instant: Timestamp, resolved: &ResolvedCreatedAt) -> Option<bool> {
    stamp_of(info, resolved.exact(info)).map(|stamp| stamp.before(instant))
}

/// [`straddling_file`] with created-at times asked for as needed: every file
/// that passes the reach is judged, so every missing time among them is
/// needed. The last extension step already read them all, so in practice
/// this asks for nothing.
fn lazy_straddler(
    inventory: &Inventory,
    instant: Timestamp,
    reach: Txid,
    resolved: &ResolvedCreatedAt,
) -> Resolve<Option<FileKey>> {
    let files = inventory.files().iter();
    let mut missing = files
        .clone()
        .filter(|info| passes(info, reach) && resolved.exact(info).is_none());
    if let Some(first) = missing.next() {
        return Resolve::Need(Need::with_ahead(*first, missing.copied()));
    }
    Resolve::Done(straddler(files, reach, instant, |info| {
        stamp_of(info, resolved.exact(info))
    }))
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
#[cfg(any(test, feature = "testing"))]
fn depth(len: usize) -> u64 {
    u64::from(usize::BITS.saturating_sub(len.leading_zeros()))
}

#[cfg(test)]
mod tests;

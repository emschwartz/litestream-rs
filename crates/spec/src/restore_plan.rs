//! The restore planner's contract, from the "Contract" section of the module
//! doc of `crates/core/src/plan.rs` and the provenance table of
//! `crates/core/src/layout.rs` (`Dating`, `Stamp::of`).
//!
//! Written from that text, not from the planner: the reach is a fixpoint over
//! every chain, each refusal a scan over the whole listing in the order the
//! text gives, the chain the text's own choice rule applied file by file. The
//! planner sorts once and sweeps; nothing here does. Each rule's comment names
//! the [`Clause`] it implements, quoting the text where the wording matters.
//!
//! Terms, as the contract uses them: a file "begins at transaction 1" when its
//! first transaction is 1, at any level. A level-9 file that does not is
//! "refused" (never a base, never applied); every other file is "usable". A file
//! "links" to an end `r` when its first transaction is at most `r + 1`, and
//! "passes" `r` when its last transaction is beyond it.

use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::num::NonZeroU64;

/// The snapshot level: a file there that does not begin at transaction 1 is refused.
const SNAPSHOT_LEVEL: u8 = 9;

/// Where a recorded created-at time was read from.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Origin {
    /// The object's stored metadata.
    Metadata,
    /// A file store's modification time, which is how such a store keeps it.
    Mtime,
    /// The object's last-modified time, standing in for metadata that is absent.
    Fallback,
}

/// A file's recorded created-at time, in Unix milliseconds, and where it came from.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CreatedAt {
    /// The instant as recorded.
    pub at_ms: i64,
    /// Where it was read from.
    pub origin: Origin,
}

/// What a created-at time says about when the file's newest transaction
/// committed (layout.rs, `Dating`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Dating {
    /// Equal to the commit.
    Commit,
    /// At or after the commit, by an unknown margin.
    NotBeforeCommit,
    /// The writer recorded no time.
    Unset,
}

/// A file's name: level, first and last transaction. The derived order is the
/// contract's key order, "(level, first transaction, last transaction)".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Key {
    /// 0 to 9.
    pub level: u8,
    /// The first transaction.
    pub min: u64,
    /// The last transaction.
    pub max: u64,
}

impl Key {
    /// "Anything but a level-9 file refused as a snapshot."
    #[must_use]
    pub const fn usable(self) -> bool {
        self.level != SNAPSHOT_LEVEL || self.min == 1
    }

    /// Whether the file "starts at most one past" a chain ending at `end`.
    /// Past the largest id every file starts in time (and none passes).
    #[must_use]
    pub const fn links(self, end: u64) -> bool {
        match end.checked_add(1) {
            Some(one_past) => self.min <= one_past,
            None => true,
        }
    }

    /// Whether a usable file links to a chain ending at `end` and passes it:
    /// "the replica continues".
    #[must_use]
    pub const fn passes(self, end: u64) -> bool {
        self.usable() && self.links(end) && self.max > end
    }
}

/// One listed file.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct File {
    /// Its name.
    pub key: Key,
    /// Its exact created-at time, or `None` when the listing did not carry one.
    /// Only an `AsOf` judgement reads it, and it needs every file's.
    pub created_at: Option<CreatedAt>,
}

/// A replica's files: valid keys, each once, in key order.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Listing {
    files: Vec<File>,
}

impl Listing {
    /// The listing, or `None` when a key is not one a replica can hold (a
    /// level above 9, a first transaction of 0 or after the last) or appears
    /// twice: a store lists a key once.
    #[must_use]
    pub fn new(files: impl IntoIterator<Item = File>) -> Option<Self> {
        let mut files: Vec<File> = files.into_iter().collect();
        files.sort_by_key(|file| file.key);
        let valid = files.iter().all(|file| {
            let key = file.key;
            key.level <= SNAPSHOT_LEVEL && key.min >= 1 && key.min <= key.max
        });
        let unique = files
            .iter()
            .zip(files.iter().skip(1))
            .all(|(a, b)| a.key != b.key);
        (valid && unique).then_some(Self { files })
    }

    /// The files, in key order.
    #[must_use]
    pub fn files(&self) -> &[File] {
        &self.files
    }
}

/// The state to rebuild.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Target {
    /// The newest state the replica holds.
    Latest,
    /// The state right after this transaction.
    Txid(NonZeroU64),
    /// The newest state whose files were all created strictly before this
    /// instant, in Unix milliseconds.
    AsOf(i64),
}

/// The outcome, shaped as the planner's `PlanOutcome`, with each file named by
/// its key and each chain by its keys in the order to apply them.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Outcome {
    /// Files that rebuild the target exactly.
    Plan {
        /// The chain, base first.
        chain: Vec<Key>,
    },
    /// The listing holds no file.
    EmptyReplica,
    /// No eligible file begins at transaction 1.
    NoSnapshot {
        /// The smallest last transaction of all files that begin at transaction 1.
        oldest_snapshot: Option<u64>,
        /// Every refused level-9 key, ascending.
        malformed_snapshots: Vec<Key>,
    },
    /// A requested transaction past every file.
    BeyondNewest {
        /// What was asked for.
        requested: u64,
        /// The last transaction any file covers.
        newest: u64,
    },
    /// An instant falls inside a file later than it.
    AsOfInsideFile {
        /// The newest state restorable at or before the instant.
        chain: Vec<Key>,
        /// The straddling file.
        file: Key,
    },
    /// The chain stops short of a hole.
    Gap {
        /// The reach.
        have_through: u64,
        /// Where history resumes.
        next_starts: u64,
    },
    /// A requested transaction lies strictly inside a linking file.
    InsideFile {
        /// What was asked for.
        requested: u64,
        /// The file it lies inside.
        file: Key,
    },
    /// Only refused level-9 files lie past the reach.
    MalformedSnapshot {
        /// The reach.
        have_through: u64,
        /// The refused file named.
        file: Key,
    },
}

/// A clause of the contract: the sentence of plan.rs's "Contract" section (or
/// of layout.rs's provenance table) that decides an outcome.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Clause {
    /// Refusal 1: "the listing holds no file".
    EmptyReplica,
    /// Refusal 2: "a `Txid` past every file".
    BeyondNewest,
    /// "For `Latest` and `Txid` the outcome is Plan if and only if an eligible
    /// chain from a transaction-1 base reaches the target exactly." The target
    /// of `Latest` is "the largest last transaction of any file, refused ones included".
    ReachesTarget,
    /// Refusal 3: "for `Latest` and `Txid`, no file at all begins at transaction
    /// 1; for `AsOf`, none created before the instant".
    NoSnapshot,
    /// Refusal 4 (`Txid` only): "a usable file links to the chain ... and ends
    /// past the target"; it names "the nearest-ending file, then the higher
    /// level, then the lower key".
    InsideFile,
    /// Refusal 5: "a usable file lies past `r` and does not link";
    /// `next_starts` is "the smallest first transaction over every usable file
    /// past `r` that does not link, whatever the target and eligibility".
    Gap,
    /// Refusal 6: "only refused level-9 files lie past `r`"; it names "the lowest key".
    MalformedSnapshot,
    /// "`AsOf` ends where the eligible chain ends, not at the largest eligible
    /// transaction", and no file past it straddles the instant.
    AsOfChainEnd,
    /// "A usable file that touches the end and passes it means the replica
    /// continues; none, while files exist past it, is a hole ... the answer is
    /// the Gap (or malformed-snapshot refusal) that `Latest` reports for the
    /// same listing."
    AsOfHole,
    /// "An instant can fall inside a file ... That is `AsOfInsideFile`":
    /// a usable file links and passes the end and is not recorded as created
    /// before the instant. Named: "the nearest-ending file that straddles the
    /// instant" (the text does not break ties among equal ends).
    AsOfStraddle,
    /// "A file that ends right after the chain removes the doubt (`dates_next`)
    /// only when its time dates that transaction's commit": a level 0-8 file
    /// with its stored created-at time (layout.rs, `Dating::Commit`).
    AsOfDatesNext,
    /// The contract decides nothing for this input: no outcome is allowed.
    Silent(Silence),
}

/// Why the contract decides nothing: each is a hole in the text, or an input
/// outside it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Silence {
    /// No refusal in the list applies and no plan is due.
    NoRefusalApplies,
    /// An `AsOf` judgement over a file whose created-at time is not known.
    UnknownCreatedAt,
    /// The text's choice of base and steps does not end where some chain ends.
    ChoiceMissesReach,
    /// An `AsOf` hole for which `Latest` does not report a gap or a
    /// malformed snapshot at the same reach.
    HoleWithoutLatestRefusal,
}

/// What the contract allows for one listing and target, and the clause that says so.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Judgement {
    /// Every outcome the contract allows; usually one.
    pub allowed: Vec<Outcome>,
    /// The clause that decided.
    pub by: Clause,
}

impl Judgement {
    fn one(outcome: Outcome, by: Clause) -> Self {
        Self {
            allowed: vec![outcome],
            by,
        }
    }

    fn silent(why: Silence) -> Self {
        Self {
            allowed: Vec::new(),
            by: Clause::Silent(why),
        }
    }

    /// Whether the contract allows this outcome.
    #[must_use]
    pub fn admits(&self, outcome: &Outcome) -> bool {
        self.allowed.contains(outcome)
    }
}

/// The provenance table (layout.rs, `Dating` and `Stamp::of`): a time at or
/// before the epoch is unset ("the writer recorded no time"); a snapshot's time
/// is its write ("a snapshot proves nothing of the kind"); a time the store fell
/// back to is the upload; any other level-0-to-8 time is the commit.
#[must_use]
pub const fn dating(level: u8, created_at: CreatedAt) -> Dating {
    if created_at.at_ms <= 0 {
        Dating::Unset
    } else if level == SNAPSHOT_LEVEL {
        Dating::NotBeforeCommit
    } else {
        match created_at.origin {
            Origin::Metadata | Origin::Mtime => Dating::Commit,
            Origin::Fallback => Dating::NotBeforeCommit,
        }
    }
}

/// What the contract allows for `target` over `listing`.
#[must_use]
pub fn judge(listing: &Listing, target: Target) -> Judgement {
    let files = listing.files();
    // Refusal 1, first in precedence.
    let Some(newest) = files.iter().map(|file| file.key.max).max() else {
        return Judgement::one(Outcome::EmptyReplica, Clause::EmptyReplica);
    };
    match target {
        Target::Latest => judge_keys(files, newest, None),
        // Refusal 2: "A wrong request is not reported as a broken replica."
        Target::Txid(requested) if requested.get() > newest => Judgement::one(
            Outcome::BeyondNewest {
                requested: requested.get(),
                newest,
            },
            Clause::BeyondNewest,
        ),
        Target::Txid(requested) => judge_keys(files, requested.get(), Some(requested.get())),
        Target::AsOf(instant) => judge_as_of(files, instant),
    }
}

fn begins_at_one(key: Key) -> bool {
    key.min == 1
}

/// Every transaction some chain over `eligible` can end at (chain definition:
/// "the first begins at transaction 1 (any level), each later one is from
/// levels 0 to 8, starts at most one past the previous end and ends beyond
/// it"), by fixpoint over all chains.
fn chain_ends(eligible: &[Key]) -> BTreeSet<u64> {
    let mut ends: BTreeSet<u64> = eligible
        .iter()
        .filter(|key| begins_at_one(**key))
        .map(|key| key.max)
        .collect();
    loop {
        let grown: Vec<u64> = eligible
            .iter()
            .filter(|key| key.level < SNAPSHOT_LEVEL && !ends.contains(&key.max))
            .filter(|key| ends.iter().any(|end| key.links(*end) && key.max > *end))
            .map(|key| key.max)
            .collect();
        if grown.is_empty() {
            return ends;
        }
        ends.extend(grown);
    }
}

/// The chain the text's choice rule builds ("How a plan is chosen within the
/// contract"): the base is "the eligible file that begins at transaction 1 ...
/// and reaches furthest. Ties go to the higher level"; each step "takes the
/// eligible file, from levels 0 to 8, that touches the chain's end ... and
/// reaches furthest. Ties go to the higher level, then the lower first
/// transaction". `None` without a base.
fn chosen_chain(eligible: &[Key]) -> Option<(Vec<Key>, u64)> {
    let base = eligible
        .iter()
        .filter(|key| begins_at_one(**key))
        .max_by_key(|key| (key.max, key.level))?;
    let mut chain = vec![*base];
    let mut end = base.max;
    // Each step ends further than the last, so at most one step per file.
    for _ in eligible {
        let Some(step) = eligible
            .iter()
            .filter(|key| key.level < SNAPSHOT_LEVEL && key.links(end) && key.max > end)
            .max_by_key(|key| (key.max, key.level, Reverse(key.min)))
        else {
            break;
        };
        chain.push(*step);
        end = step.max;
    }
    Some((chain, end))
}

/// "`oldest_snapshot` is the smallest last transaction of all files that begin
/// at transaction 1, whatever the target; `malformed_snapshots` is every refused
/// level-9 key, in ascending key order".
fn no_snapshot(files: &[File]) -> Judgement {
    let oldest_snapshot = files
        .iter()
        .filter(|file| begins_at_one(file.key))
        .map(|file| file.key.max)
        .min();
    let mut malformed_snapshots: Vec<Key> = files
        .iter()
        .map(|file| file.key)
        .filter(|key| !key.usable())
        .collect();
    malformed_snapshots.sort();
    Judgement::one(
        Outcome::NoSnapshot {
            oldest_snapshot,
            malformed_snapshots,
        },
        Clause::NoSnapshot,
    )
}

/// `Latest` (`requested` `None`, `goal` the newest transaction) or `Txid`.
fn judge_keys(files: &[File], goal: u64, requested: Option<u64>) -> Judgement {
    // Eligibility: "`Latest` admits every file; `Txid` admits files whose last
    // transaction is at or below the target".
    let eligible: Vec<Key> = files
        .iter()
        .map(|file| file.key)
        .filter(|key| requested.is_none_or(|wanted| key.max <= wanted))
        .collect();
    let ends = chain_ends(&eligible);
    if ends.contains(&goal) {
        return match chosen_chain(&eligible) {
            Some((chain, end)) if end == goal => {
                Judgement::one(Outcome::Plan { chain }, Clause::ReachesTarget)
            }
            Some(_) | None => Judgement::silent(Silence::ChoiceMissesReach),
        };
    }
    // "`have_through` is `r`": the furthest any eligible chain gets, 0 when none starts.
    let reach = ends.last().copied().unwrap_or(0);
    if !files.iter().any(|file| begins_at_one(file.key)) {
        return no_snapshot(files);
    }
    if let Some(wanted) = requested
        && let Some(file) = files
            .iter()
            .map(|file| file.key)
            .filter(|key| key.usable() && key.links(reach) && key.max > wanted)
            .min_by_key(|key| (key.max, Reverse(key.level), *key))
    {
        return Judgement::one(
            Outcome::InsideFile {
                requested: wanted,
                file,
            },
            Clause::InsideFile,
        );
    }
    past_the_reach(files, reach)
}

/// Refusals 5 and 6, judged over every file whatever the target.
fn past_the_reach(files: &[File], reach: u64) -> Judgement {
    let past = || {
        files
            .iter()
            .map(|file| file.key)
            .filter(|key| key.max > reach)
    };
    if let Some(next_starts) = past()
        .filter(|key| key.usable() && !key.links(reach))
        .map(|key| key.min)
        .min()
    {
        return Judgement::one(
            Outcome::Gap {
                have_through: reach,
                next_starts,
            },
            Clause::Gap,
        );
    }
    match past().filter(|key| !key.usable()).min() {
        Some(file) => Judgement::one(
            Outcome::MalformedSnapshot {
                have_through: reach,
                file,
            },
            Clause::MalformedSnapshot,
        ),
        None => Judgement::silent(Silence::NoRefusalApplies),
    }
}

/// `AsOf`: eligibility is "created strictly before the instant", which holds
/// for every dating ("no time precedes the commit it bounds; an unset time
/// counts as before every later instant").
fn judge_as_of(files: &[File], instant: i64) -> Judgement {
    let mut timed: Vec<(Key, CreatedAt)> = Vec::new();
    for file in files {
        let Some(created_at) = file.created_at else {
            return Judgement::silent(Silence::UnknownCreatedAt);
        };
        timed.push((file.key, created_at));
    }
    let eligible: Vec<Key> = timed
        .iter()
        .filter(|(_, created_at)| created_at.at_ms < instant)
        .map(|(key, _)| *key)
        .collect();
    // Refusal 3 for `AsOf`: no file created before the instant begins at 1.
    let Some((chain, reach)) = chosen_chain(&eligible) else {
        return no_snapshot(files);
    };
    if chain_ends(&eligible).last() != Some(&reach) {
        return Judgement::silent(Silence::ChoiceMissesReach);
    }
    // The hole rule is "judged by keys over the whole inventory, created-at
    // times not consulted".
    let continues = files.iter().any(|file| file.key.passes(reach));
    if files.iter().any(|file| file.key.max > reach) && !continues {
        return as_of_hole(files, reach);
    }
    straddle(&timed, chain, reach, instant)
}

/// "The answer is the Gap (or malformed-snapshot refusal) that `Latest`
/// reports for the same listing", which must also meet "`have_through` is `r`".
fn as_of_hole(files: &[File], reach: u64) -> Judgement {
    let newest = files.iter().map(|file| file.key.max).max().unwrap_or(0);
    let latest = judge_keys(files, newest, None);
    let allowed: Vec<Outcome> = latest
        .allowed
        .into_iter()
        .filter(|outcome| match outcome {
            Outcome::Gap { have_through, .. } | Outcome::MalformedSnapshot { have_through, .. } => {
                *have_through == reach
            }
            Outcome::Plan { .. }
            | Outcome::EmptyReplica
            | Outcome::NoSnapshot { .. }
            | Outcome::BeyondNewest { .. }
            | Outcome::AsOfInsideFile { .. }
            | Outcome::InsideFile { .. } => false,
        })
        .collect();
    if allowed.is_empty() {
        return Judgement::silent(Silence::HoleWithoutLatestRefusal);
    }
    Judgement {
        allowed,
        by: Clause::AsOfHole,
    }
}

/// The straddle rule, over every file that passes the reach.
fn straddle(timed: &[(Key, CreatedAt)], chain: Vec<Key>, reach: u64, instant: i64) -> Judgement {
    let passing: Vec<(Key, CreatedAt)> = timed
        .iter()
        .filter(|(key, _)| key.passes(reach))
        .copied()
        .collect();
    // `dates_next`: a passing file that ends right after the chain and whose
    // time dates that commit at or after the instant.
    let dated = reach.checked_add(1).is_some_and(|one_past| {
        passing.iter().any(|(key, created_at)| {
            key.max == one_past
                && dating(key.level, *created_at) == Dating::Commit
                && created_at.at_ms >= instant
        })
    });
    if dated {
        return Judgement::one(Outcome::Plan { chain }, Clause::AsOfDatesNext);
    }
    let straddlers: Vec<Key> = passing
        .iter()
        .filter(|(_, created_at)| created_at.at_ms >= instant)
        .map(|(key, _)| *key)
        .collect();
    let Some(nearest) = straddlers.iter().map(|key| key.max).min() else {
        return Judgement::one(Outcome::Plan { chain }, Clause::AsOfChainEnd);
    };
    let allowed = straddlers
        .into_iter()
        .filter(|key| key.max == nearest)
        .map(|file| Outcome::AsOfInsideFile {
            chain: chain.clone(),
            file,
        })
        .collect();
    Judgement {
        allowed,
        by: Clause::AsOfStraddle,
    }
}

//! What the restore planner must answer, restated for the oracle from its
//! documented contract (the "Contract" section of `crates/core/src/plan.rs`),
//! never from its code: two oracles that copied the planner's own rules have
//! already agreed with it when it was wrong.
//!
//! A chain is a run of eligible files: the first begins at transaction 1 (any
//! level), each later one is from levels 0 to 8, starts at most one past the
//! previous end and ends beyond it. A level-9 file that does not begin at
//! transaction 1 is refused; every other file is usable. A chain reaching `e`
//! holds only files that end at or before `e`, so the transactions a `Txid`
//! target can plan to are the ends reachable over the whole listing, cut at
//! the target ([`Reach`]).
//!
//! `Latest` and `Txid` plan exactly when a chain reaches the target, else
//! refuse with the first of: no file at all begins at transaction 1; a usable
//! file starts at most one past the furthest reach `r` (0 with no eligible
//! base) and ends past the target (inside a file); a usable file lies past `r`
//! (a gap); only refused files lie past `r`.
//!
//! `AsOf(t)` admits files created strictly before `t` and ends where the
//! eligible chain ends, `r`. Nothing usable touching `r` and passing it means
//! a gap when usable files lie past `r` (refused ones only: a malformed
//! snapshot), else a plan. Something usable passing `r` means the replica
//! continues, so `t` falls inside it, unless a level 0 to 8 file ends at
//! `r + 1` with its stored created-at time: that time is the transaction's,
//! so the transaction came at or after `t` and `r` is exact. A snapshot's
//! time is its write and a time the store fell back to (no stored metadata)
//! is the object's upload, both any time after the commit, so neither dates
//! anything: then any file passing `r` straddles, even one holding a single
//! transaction past it.

use std::collections::BTreeSet;
use std::fmt;

use lsr_core::{CreatedAtSource, FileInfo, FileKey, PlanOutcome};
use lsr_ltx::{Timestamp, Txid};

/// What the planner must answer for one target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expected {
    /// A chain that reaches exactly this transaction.
    Plan(Txid),
    /// `AsOf` only: the chain reaching this transaction, with a file that
    /// straddles the instant.
    AsOfInside {
        /// Where the chain ends.
        reach: Txid,
    },
    /// No file at all begins at transaction 1 (or, for `AsOf`, none
    /// eligible does).
    NoSnapshot,
    /// `Txid` only: the target lies inside a usable file that links to the
    /// furthest reach (0 when nothing reaches) and passes the target.
    InsideFile {
        /// The furthest reach below the target, if any.
        reach: Option<Txid>,
        /// The target.
        target: Txid,
    },
    /// A usable file lies past the furthest reach and does not link.
    Gap,
    /// Only refused level-9 files lie past the furthest reach.
    MalformedSnapshot,
}

impl fmt::Display for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plan(txid) => write!(f, "a plan to {}", txid.get()),
            Self::AsOfInside { reach, .. } => {
                write!(f, "an instant inside a file past {}", reach.get())
            }
            Self::NoSnapshot => f.write_str("no snapshot"),
            Self::InsideFile { reach, target } => write!(
                f,
                "{} inside a file past {}",
                target.get(),
                reach.map_or(0, lsr_ltx::Txid::get)
            ),
            Self::Gap => f.write_str("a gap"),
            Self::MalformedSnapshot => f.write_str("a malformed snapshot"),
        }
    }
}

/// Whether a file begins at transaction 1, restated from the contract: such a
/// file can start a chain at any level.
#[must_use]
pub fn begins_at_one(file: &FileInfo) -> bool {
    file.key.range().min() == Txid::MIN
}

/// Whether the listing keeps a file at all: a level-9 file that does not
/// begin at transaction 1 is refused.
#[must_use]
pub fn usable(file: &FileInfo) -> bool {
    !file.key.level().is_snapshot() || begins_at_one(file)
}

/// Whether a file starting at `min` can follow a chain that ends at `end`
/// (`None`: nothing yet, so only a start at transaction 1).
fn links(min: Txid, end: Option<Txid>) -> bool {
    let one_past = end.map_or(Some(Txid::MIN), Txid::next);
    one_past.is_none_or(|one_past| min <= one_past)
}

/// Every transaction some chain of eligible files reaches exactly.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reach(BTreeSet<Txid>);

impl Reach {
    /// The ends reachable over the files `eligible` admits.
    #[must_use]
    pub fn over(files: &[FileInfo], eligible: impl Fn(&FileInfo) -> bool) -> Self {
        let admitted: Vec<&FileInfo> = files.iter().filter(|f| usable(f) && eligible(f)).collect();
        let mut ends: BTreeSet<Txid> = admitted
            .iter()
            .filter(|f| begins_at_one(f))
            .map(|f| f.key.range().max())
            .collect();
        let mut frontier: Vec<Txid> = ends.iter().copied().collect();
        while let Some(end) = frontier.pop() {
            for file in admitted.iter().filter(|f| !f.key.level().is_snapshot()) {
                let range = file.key.range();
                if links(range.min(), Some(end)) && range.max() > end && ends.insert(range.max()) {
                    frontier.push(range.max());
                }
            }
        }
        Self(ends)
    }

    /// Whether a chain reaches exactly `txid`.
    #[must_use]
    pub fn contains(&self, txid: Txid) -> bool {
        self.0.contains(&txid)
    }

    /// The furthest end at or below `upto`.
    #[must_use]
    pub fn furthest_upto(&self, upto: Txid) -> Option<Txid> {
        self.0.range(..=upto).next_back().copied()
    }

    /// The furthest end.
    #[must_use]
    pub fn furthest(&self) -> Option<Txid> {
        self.0.last().copied()
    }
}

/// What lies past a chain's end when nothing usable links to it and passes
/// it: a gap, a malformed snapshot, or nothing (then the chain is the answer).
fn past(files: &[FileInfo], reach: Option<Txid>) -> Option<Expected> {
    let beyond = |f: &&FileInfo| reach.is_none_or(|r| f.key.range().max() > r);
    if files.iter().filter(beyond).any(usable) {
        Some(Expected::Gap)
    } else if files.iter().any(|f| beyond(&f)) {
        Some(Expected::MalformedSnapshot)
    } else {
        None
    }
}

/// What `Txid(target)` must answer over `files`, a listing that is not
/// empty and reaches `target`; `reach` is [`Reach::over`] every file.
#[must_use]
pub fn txid(files: &[FileInfo], reach: &Reach, target: Txid) -> Expected {
    if reach.contains(target) {
        return Expected::Plan(target);
    }
    if !files.iter().any(begins_at_one) {
        return Expected::NoSnapshot;
    }
    let r = reach.furthest_upto(target);
    let inside = files
        .iter()
        .any(|f| usable(f) && links(f.key.range().min(), r) && f.key.range().max() > target);
    if inside {
        return Expected::InsideFile { reach: r, target };
    }
    past(files, r).unwrap_or(Expected::Gap)
}

fn created_before(file: &FileInfo, instant: Timestamp) -> bool {
    file.created_at().is_some_and(|c| c.at() < instant)
}

/// What `AsOf(instant)` must answer over `files`, a non-empty listing whose
/// created-at times are all exact.
#[must_use]
pub fn as_of(files: &[FileInfo], instant: Timestamp) -> Expected {
    let reach = Reach::over(files, |f| created_before(f, instant));
    let Some(r) = reach.furthest() else {
        return Expected::NoSnapshot;
    };
    let passing: Vec<&FileInfo> = files
        .iter()
        .filter(|f| usable(f) && links(f.key.range().min(), Some(r)) && f.key.range().max() > r)
        .collect();
    if passing.is_empty() {
        return past(files, Some(r)).unwrap_or(Expected::Plan(r));
    }
    let next = r.next();
    let dates_next = passing.iter().any(|f| {
        Some(f.key.range().max()) == next && dates_its_commit(f) && !created_before(f, instant)
    });
    if dates_next {
        return Expected::Plan(r);
    }
    Expected::AsOfInside { reach: r }
}

/// Whether a file's created-at time is its newest transaction's: levels 0 to
/// 8, stored by the writer (not a store's fallback), and set.
fn dates_its_commit(file: &FileInfo) -> bool {
    !file.key.level().is_snapshot()
        && file
            .created_at()
            .is_some_and(|c| c.origin() != CreatedAtSource::Fallback && c.at().unix_millis() > 0)
}

/// Whether `file` can be the file an instant falls inside, past a chain
/// that ends at `reach`: usable, linking, and passing it.
fn is_straddler(file: &FileInfo, reach: Txid) -> bool {
    usable(file) && links(file.key.range().min(), Some(reach)) && file.key.range().max() > reach
}

/// Whether the planner's answer is one the contract allows.
#[must_use]
pub fn allows(expected: &Expected, files: &[FileInfo], got: &Answer) -> bool {
    match (expected, got) {
        // The planner chose a file; whether it reads is the store's business.
        (_, Answer::Unreadable(_)) => true,
        (Expected::Plan(want), Answer::Reached { txid, inside: None }) => want == txid,
        (
            Expected::AsOfInside { reach },
            Answer::Reached {
                txid,
                inside: Some(key),
            },
        ) => {
            reach == txid
                && files
                    .iter()
                    .any(|f| f.key == *key && is_straddler(f, *reach))
        }
        (
            Expected::InsideFile { reach, target },
            Answer::Refused(PlanOutcome::InsideFile { file, .. }),
        ) => files.iter().any(|f| {
            f.key == *file
                && usable(f)
                && links(f.key.range().min(), *reach)
                && f.key.range().max() > *target
        }),
        (Expected::NoSnapshot, Answer::Refused(PlanOutcome::NoSnapshot { .. }))
        | (Expected::Gap, Answer::Refused(PlanOutcome::Gap { .. }))
        | (Expected::MalformedSnapshot, Answer::Refused(PlanOutcome::MalformedSnapshot { .. })) => {
            true
        }
        (
            Expected::Plan(_)
            | Expected::AsOfInside { .. }
            | Expected::NoSnapshot
            | Expected::InsideFile { .. }
            | Expected::Gap
            | Expected::MalformedSnapshot,
            _,
        ) => false,
    }
}

/// What a restore answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// It built a database at this transaction; `inside` names the file an
    /// `AsOf` instant fell inside, when the restore took the earlier state.
    Reached {
        /// The transaction reached.
        txid: Txid,
        /// The straddling file, if any.
        inside: Option<FileKey>,
    },
    /// The planner refused.
    Refused(PlanOutcome),
    /// The plan named a file the store damaged as it published it, and the
    /// restore stopped on it with a typed error: loud, not a wrong answer.
    Unreadable(FileKey),
}

impl fmt::Display for Answer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reached { txid, inside: None } => write!(f, "a plan to {}", txid.get()),
            Self::Reached {
                txid,
                inside: Some(key),
            } => write!(f, "{} inside {key:?}", txid.get()),
            Self::Refused(outcome) => write!(f, "{outcome:?}"),
            Self::Unreadable(key) => write!(f, "a damaged {key:?}"),
        }
    }
}

#[cfg(test)]
mod tests;

//! `AsOf` restores and created-at times, judged.
//!
//! Two kinds of truth meet here. The planner's contract
//! ([`crate::contract::as_of`]) says what an instant must answer over the
//! listing's created-at times. The recorders, which read the simulator's
//! clock below the code under test, say what those times must be. The engine
//! only ever clamps its clock upward to an earlier reading, so a stamp lies
//! between the clock when it was taken and the latest the clock had read. A
//! level-0 file is stamped when it is staged and a compacted file carries its
//! newest input's time, so every file of levels 0 to 8 holds the stamp of a
//! staged file that ended at the same transaction. A level-9 snapshot read
//! from the database is stamped when it is written, so it holds a time from its own put;
//! one built from level 0 (the first, `docs/decisions/D203.md`) is a compaction and holds
//! the stamp of a staged file that ended at its last transaction ([`stamps`]).
//! Together they pin the answer without copying the planner.
//!
//! One property needs only the recorder: an `AsOf(t)` restore that reaches
//! `r` used a file created before `t` whose newest transaction is `r`, and
//! that file's time is no earlier than the first staging of a file ending at
//! `r`. So that staging came before `t`: an `AsOf` restore is never newer than
//! the instant ([`Violation::AsOfTooNew`]).

use std::collections::{BTreeMap, BTreeSet};

use lsr_core::conformance::block_on;
use lsr_core::{
    AsOfInside, CreatedAtSource, FileInfo, FileKey, MemSink, PlanOutcome, RestoreError,
    RestoreOptions, RestoreTarget, restore_with,
};
use lsr_ltx::{Timestamp, Txid};

use super::{Checked, Violation, bug_fault, first_diff};
use crate::contract::{self, Answer};
use crate::model::{CommitId, Model};
use crate::store::{RecorderEvent, RecorderLog};

/// Instants asked per check, at most: each is a whole restore.
const INSTANTS: usize = 16;

/// For each transaction, the stamps (`at` to `high`) of every staged file
/// that ended at it.
type Stagings = BTreeMap<Txid, Vec<(Timestamp, Timestamp)>>;

fn stagings(staging: &RecorderLog) -> Stagings {
    let mut out = Stagings::new();
    for event in staging.events() {
        if let RecorderEvent::Put { key, at, high, .. } = event {
            out.entry(key.range().max()).or_default().push((at, high));
        }
    }
    out
}

/// Judges the created-at time of every file the replica took: each lies
/// between the clock and its latest reading at some staging of a file that
/// ends at the same transaction, or for a snapshot at its own put. A file
/// ending at another writer's transaction is skipped. Returns how many were
/// judged.
///
/// # Errors
/// [`Violation::WrongCreatedAt`] for the first that does not.
pub fn stamps(
    remote: &RecorderLog,
    staging: &RecorderLog,
    foreign: &[(Txid, Txid)],
) -> Result<u32, Violation> {
    let staged = stagings(staging);
    let mut judged = 0_u32;
    for event in remote.events() {
        let RecorderEvent::Put {
            key,
            created_at,
            at,
            high,
            ..
        } = event
        else {
            continue;
        };
        // Another writer stamped its own file; the engine only copies it.
        if foreign
            .iter()
            .any(|(theirs, _)| *theirs == key.range().max())
        {
            continue;
        }
        let within = |(at, high): &(Timestamp, Timestamp)| *at <= created_at && created_at <= *high;
        // A snapshot built from level 0 carries its newest input's time, as a compaction does.
        let from_staged = staged
            .get(&key.range().max())
            .is_some_and(|times| times.iter().any(within));
        let fits = if key.level().is_snapshot() {
            within(&(at, high)) || from_staged
        } else {
            from_staged
        };
        if !fits {
            return Err(Violation::WrongCreatedAt { key, created_at });
        }
        judged = judged.saturating_add(1);
    }
    Ok(judged)
}

/// Everything an `AsOf` judgment reads.
pub struct Judge<'a> {
    /// The raw replica.
    pub remote: &'a crate::remote::Bucket,
    /// Its listing.
    pub files: &'a [FileInfo],
    /// The commits.
    pub model: &'a Model,
    /// Which commit each transaction covers.
    pub cover: &'a BTreeMap<Txid, CommitId>,
    /// The staging recorder, when the run kept one with a clock.
    pub staging: Option<&'a RecorderLog>,
    /// Transactions a bucket rollback took for good.
    pub lost: &'a BTreeSet<Txid>,
    /// Objects the store damaged as it published them.
    pub damaged: &'a [FileKey],
    /// Transactions another writer put a file for.
    pub foreign: &'a [(Txid, Txid)],
}

/// The instants worth asking about: just before, at and just after each
/// created-at time, and before and after all of them; spread evenly when
/// there are more than [`INSTANTS`].
fn instants(files: &[FileInfo]) -> Vec<Timestamp> {
    let mut times: Vec<i64> = files
        .iter()
        .filter_map(|f| f.created_at())
        .map(|c| c.at().unix_millis())
        .flat_map(|t| [t.saturating_sub(1), t, t.saturating_add(1)])
        .collect();
    times.sort_unstable();
    times.dedup();
    let len = times.len();
    if len > INSTANTS {
        let last = len.saturating_sub(1);
        let spans = INSTANTS.saturating_sub(1).max(1);
        let step = |i: usize| i.saturating_mul(last).checked_div(spans).unwrap_or(0);
        times = (0..INSTANTS)
            .filter_map(|i| times.get(step(i)).copied())
            .collect();
        times.dedup();
    }
    times.into_iter().map(Timestamp::from_unix_millis).collect()
}

impl Judge<'_> {
    /// Restores and judges every instant [`instants`] picks, counting the
    /// outcomes into `out`. A listing with a file whose created-at time is
    /// not exact is skipped: the contract is stated over exact times.
    ///
    /// # Errors
    /// The first [`Violation`].
    pub fn all(&self, latest: Txid, out: &mut Checked) -> Result<(), Violation> {
        if self.files.is_empty() || self.files.iter().any(|f| f.created_at().is_none()) {
            return Ok(());
        }
        self.after_every_file(latest)?;
        let fell_back = self
            .files
            .iter()
            .filter(|f| {
                f.created_at()
                    .is_some_and(|c| c.origin() == CreatedAtSource::Fallback)
            })
            .count();
        out.as_of_fallback = out
            .as_of_fallback
            .saturating_add(u32::try_from(fell_back).unwrap_or(u32::MAX));
        for instant in instants(self.files) {
            match self.one(instant)? {
                Answer::Reached { inside: None, .. } => {
                    out.as_of_planned = out.as_of_planned.saturating_add(1);
                }
                Answer::Reached {
                    inside: Some(_), ..
                } => {
                    out.as_of_inside = out.as_of_inside.saturating_add(1);
                }
                Answer::Refused(_) => out.as_of_refused = out.as_of_refused.saturating_add(1),
                Answer::Unreadable(_) => out.damaged = out.damaged.saturating_add(1),
            }
        }
        Ok(())
    }

    /// An instant later than every created-at time is `Latest` (the
    /// `RestoreTarget::AsOf` doc): the same transaction, nothing straddled.
    fn after_every_file(&self, latest: Txid) -> Result<(), Violation> {
        let Some(last) = self
            .files
            .iter()
            .filter_map(|f| f.created_at())
            .map(|c| c.at().unix_millis())
            .max()
        else {
            return Ok(());
        };
        let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(last.saturating_add(1)));
        match self.restore(target)? {
            Answer::Reached { txid, inside: None } if txid == latest => Ok(()),
            Answer::Unreadable(_) => Ok(()),
            got @ (Answer::Reached { .. } | Answer::Refused(_)) => Err(Violation::NotLatest {
                target,
                got: Box::new(got),
                latest: latest.get(),
            }),
        }
    }

    fn one(&self, instant: Timestamp) -> Result<Answer, Violation> {
        let target = RestoreTarget::AsOf(instant);
        let expected = contract::as_of(self.files, instant);
        let got = self.restore(target)?;
        if !contract::allows(&expected, self.files, &got) {
            return Err(Violation::WrongAnswer {
                target,
                expected,
                got: Box::new(got),
            });
        }
        if let Answer::Reached { txid, .. } = got {
            self.not_newer(instant, txid)?;
        }
        Ok(got)
    }

    /// The first staging of a file that ends at `reached` came before
    /// `instant`.
    fn not_newer(&self, instant: Timestamp, reached: Txid) -> Result<(), Violation> {
        let Some(staging) = self.staging else {
            return Ok(());
        };
        // Another writer's file carries its own time, not a staging's.
        if self.foreign.iter().any(|(theirs, _)| *theirs == reached) {
            return Ok(());
        }
        let first = stagings(staging)
            .get(&reached)
            .and_then(|times| times.iter().map(|(at, _)| *at).min());
        match first {
            Some(staged) if staged >= instant => Err(Violation::AsOfTooNew {
                instant,
                reached: reached.get(),
                staged,
            }),
            Some(_) | None => Ok(()),
        }
    }

    /// Restores `Txid(txid)` and judges the answer by the contract over
    /// `reach`, every chain end of the listing. A refusal from the oldest
    /// snapshot's end on is lost history unless the transaction lies inside
    /// a file.
    pub fn txid(
        &self,
        txid: Txid,
        reach: &contract::Reach,
        oldest: Option<Txid>,
        out: &mut Checked,
    ) -> Result<(), Violation> {
        let target = RestoreTarget::Txid(txid);
        let expected = contract::txid(self.files, reach, txid);
        let got = self.restore(target)?;
        if !contract::allows(&expected, self.files, &got) {
            return Err(Violation::WrongAnswer {
                target,
                expected,
                got: Box::new(got),
            });
        }
        let below = oldest.is_none_or(|oldest| txid < oldest);
        match got {
            Answer::Reached { .. } => out.restored = out.restored.saturating_add(1),
            Answer::Unreadable(_) => out.damaged = out.damaged.saturating_add(1),
            Answer::Refused(_) if below => out.below_base = out.below_base.saturating_add(1),
            Answer::Refused(PlanOutcome::InsideFile { .. }) => {
                out.inside_file = out.inside_file.saturating_add(1);
            }
            Answer::Refused(outcome) if self.over_lost(&outcome) => {
                out.lost = out.lost.saturating_add(1);
            }
            Answer::Refused(outcome) => return Err(Violation::RestoreRefused { target, outcome }),
        }
        Ok(())
    }

    /// Restores `Latest`, which is `Txid(newest)`: the same contract, the
    /// same answer. `None` when the answer is a refusal a rollback explains
    /// or a stop on a damaged object (counted into `out`).
    ///
    /// # Errors
    /// [`Violation::WrongAnswer`] or [`Violation::RestoreRefused`].
    pub fn latest(
        &self,
        reach: &contract::Reach,
        newest: Option<Txid>,
        out: &mut Checked,
    ) -> Result<Option<Txid>, Violation> {
        let target = RestoreTarget::Latest;
        match self.restore(target)? {
            Answer::Reached { txid, inside } => {
                let expected = contract::txid(self.files, reach, newest.unwrap_or(txid));
                let got = Answer::Reached { txid, inside };
                if !contract::allows(&expected, self.files, &got) {
                    return Err(Violation::WrongAnswer {
                        target,
                        expected,
                        got: Box::new(got),
                    });
                }
                Ok(Some(txid))
            }
            Answer::Refused(outcome) if self.over_lost(&outcome) => {
                out.lost = out.lost.saturating_add(1);
                Ok(None)
            }
            Answer::Unreadable(_) => {
                out.damaged = out.damaged.saturating_add(1);
                Ok(None)
            }
            Answer::Refused(outcome) => Err(Violation::RestoreRefused { target, outcome }),
        }
    }

    /// Whether `outcome` is explained by what a bucket rollback took: a gap
    /// over a lost transaction, or no snapshot once transaction 1 was lost.
    pub fn over_lost(&self, outcome: &PlanOutcome) -> bool {
        match outcome {
            PlanOutcome::Gap {
                have_through,
                next_starts,
            } => self
                .lost
                .range(..*next_starts)
                .next_back()
                .is_some_and(|lost| lost > have_through),
            PlanOutcome::NoSnapshot { .. } => self.lost.contains(&Txid::MIN),
            PlanOutcome::Plan(_)
            | PlanOutcome::EmptyReplica
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::AsOfInsideFile { .. }
            | PlanOutcome::InsideFile { .. }
            | PlanOutcome::MalformedSnapshot { .. } => false,
        }
    }

    /// Restores `target`, taking the earlier state when an instant falls
    /// inside a file, and compares a database it builds with the model.
    pub fn restore(&self, target: RestoreTarget) -> Result<Answer, Violation> {
        let sink = MemSink::new();
        let options = RestoreOptions {
            as_of_inside: AsOfInside::TakeEarlier,
        };
        let done = match block_on(restore_with(self.remote, target, options, sink.clone())) {
            Ok(done) => done,
            Err(RestoreError::NoPlan(no)) => return Ok(Answer::Refused(no.outcome().clone())),
            Err(RestoreError::File { key, .. }) if self.damaged.contains(&key) => {
                return Ok(Answer::Unreadable(key));
            }
            Err(other) => return Err(bug_fault(format!("restore {target:?}: {other}"))),
        };
        let reached = done.reached();
        let commit = self
            .cover
            .get(&reached)
            .and_then(|id| self.model.rec(*id))
            .ok_or_else(|| {
                bug_fault(format!("no commit known for transaction {}", reached.get()))
            })?;
        let db = sink
            .published()
            .ok_or_else(|| bug_fault("restore returned without publishing"))?;
        if let Some(page) = first_diff(&commit.image, &db) {
            return Err(Violation::RestoreMismatch {
                target,
                commit: commit.id,
                first_diff: page,
            });
        }
        Ok(Answer::Reached {
            txid: reached,
            inside: done.inside_file(),
        })
    }
}

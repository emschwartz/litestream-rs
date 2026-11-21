//! The scheduled verify: Go's validation monitor (`store.go:933-968` at 041a8c3), plus a
//! restore compared with the live database (`docs/PLAN.md` section 7).
//!
//! Go ticks every `validation.interval`, lists each compaction level and checks its files
//! are sorted and contiguous, then logs a warning per problem and counts the result in
//! `litestream_validation_checks_total`; it never stops replication and reads no file. Here a
//! run has three steps, in two places:
//!
//! 1. Due (one interval after the start, then one interval after the previous run ended, on
//!    the injected clock: Go's ticker counts from the start and drops a tick while a run is
//!    still going, so the two differ by at most one run's length), the replicator's thread asks the
//!    engine for the live pages. The engine reads them in its next sync tick once the replica
//!    holds everything staged, under that tick's proof, as a level-9 snapshot reads the
//!    database (one read of each page the database file supplies; WAL pages cost nothing,
//!    their checksums are the scan's). No descriptor is opened: the reads go through the
//!    replicator's own connection, as every other read does.
//! 2. The pages go to a thread of its own (`lsr-verify`, started on the first run), which runs
//!    `lsr_core::verify_against`: the listing checks of `verify` at `Depth::Names`, then the
//!    real restore of that transaction into a sink that keeps one checksum a page. Every store
//!    call, however slow, happens there, so the sync task never waits on one. One run at a
//!    time: the hand-off is a slot of one.
//! 3. The result comes back to the replicator's thread (the verify thread unparks it), which
//!    updates `Health::verification`, emits `Event::Verified`, and pins retention.
//!
//! A failing run is a degraded state, never a stop: the event and the health say what
//! differed (the lowest page, how many, the transaction), and retention keeps the restore
//! point of the last run that passed (or the failing transaction, if none did). The next run
//! that passes clears both. A transaction compaction merged before the run read it is neither
//! (nothing to compare), and a failed listing or read says nothing about the replica.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use lsr_core::{Actor, Compared, LivePages, Severity, Verified, VerifyError, verify_against};
use lsr_ltx::{Timestamp, Txid};

use crate::events::Event;
use crate::exec::block_on;
use crate::health::{Mismatch, Verification, VerifyFailure, VerifyOutcome};
use crate::runtime::{Park, later};
use crate::store::Metered;

/// The verify thread's name.
pub(crate) const THREAD: &str = "lsr-verify";

/// Where a run is.
#[derive(Clone, Copy, Debug)]
enum Stage {
    /// The next run starts at this time.
    Due(Timestamp),
    /// The engine was asked for the live pages.
    Reading,
    /// The verify thread has them.
    Running,
}

/// A run's result as the verify thread hands it back; `None` when it panicked.
type Done = (Txid, Option<Result<Verified, VerifyError>>);

#[derive(Default)]
struct Slot {
    live: Option<LivePages>,
    done: Option<Done>,
    closed: bool,
}

/// The hand-off between the two threads: one run's pages in, one result out.
#[derive(Default)]
struct Job {
    slot: Mutex<Slot>,
    ready: Condvar,
}

impl Job {
    fn lock(&self) -> MutexGuard<'_, Slot> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The next pages to verify; `None` once closed.
    fn next(&self) -> Option<LivePages> {
        let mut slot = self.lock();
        loop {
            if slot.closed {
                return None;
            }
            if let Some(live) = slot.live.take() {
                return Some(live);
            }
            slot = self
                .ready
                .wait(slot)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// The verify thread's body: one run at a time until the replicator goes.
fn serve(job: &Job, store: &Metered, park: &dyn Park) {
    while let Some(live) = job.next() {
        let result = catch_unwind(AssertUnwindSafe(|| block_on(verify_against(store, &live)))).ok();
        job.lock().done = Some((live.txid(), result));
        park.unpark();
    }
}

/// The schedule, the hand-off and what the runs found.
pub(crate) struct Scheduled {
    interval: Duration,
    stage: Stage,
    job: Arc<Job>,
    /// The store and the park, until the thread starts and takes them.
    start: Option<(Metered, Arc<dyn Park>)>,
    health: Verification,
}

impl Scheduled {
    pub(crate) fn new(
        interval: Duration,
        store: Metered,
        park: Arc<dyn Park>,
        now: Timestamp,
    ) -> Self {
        Self {
            interval,
            stage: Stage::Due(later(now, interval)),
            job: Arc::new(Job::default()),
            start: Some((store, park)),
            health: Verification::default(),
        }
    }

    /// When the replicator's thread must wake for the next run.
    pub(crate) const fn wake(&self) -> Option<Timestamp> {
        match self.stage {
            Stage::Due(at) => Some(at),
            Stage::Reading | Stage::Running => None,
        }
    }

    pub(crate) const fn health(&self) -> Verification {
        self.health
    }

    /// Before a tick: a run that is due asks the engine for the live pages.
    pub(crate) fn before_tick(&mut self, actor: &mut Actor, now: Timestamp) {
        if let Stage::Due(at) = self.stage
            && now >= at
        {
            actor.read_live_pages();
            self.stage = Stage::Reading;
        }
    }

    /// After a tick: hands the pages over, and takes a finished run's result.
    pub(crate) fn after_tick(&mut self, actor: &mut Actor, now: Timestamp) -> Option<Event> {
        if let Some(live) = actor.take_live_pages() {
            self.hand_over(live, now);
        }
        let (txid, result) = self.job.lock().done.take()?;
        self.stage = Stage::Due(later(now, self.interval));
        let outcome = self.record(txid, result, now);
        Some(Event::Verified { txid, outcome })
    }

    /// The restore point a failing run keeps from retention. The replicator's thread hands
    /// it to the engine, with any other pin it holds, after every tick.
    pub(crate) const fn pinned(&self) -> Option<Txid> {
        self.health.pinned
    }

    fn hand_over(&mut self, live: LivePages, now: Timestamp) {
        if let Some((store, park)) = self.start.take() {
            let job = Arc::clone(&self.job);
            let spawned = crate::stack::spawn(THREAD, crate::stack::ENGINE_STACK, move || {
                serve(&job, &store, &*park)
            });
            if spawned.is_err() {
                // No thread, so no run: counted, and tried again at the next interval.
                self.health.runs.incomplete = self.health.runs.incomplete.saturating_add(1);
                self.stage = Stage::Due(later(now, self.interval));
                return;
            }
        }
        let mut slot = self.job.lock();
        slot.live = Some(live);
        drop(slot);
        self.job.ready.notify_all();
        self.stage = Stage::Running;
    }

    fn record(
        &mut self,
        txid: Txid,
        result: Option<Result<Verified, VerifyError>>,
        now: Timestamp,
    ) -> VerifyOutcome {
        let (outcome, worst) = classify(result);
        let health = &mut self.health;
        let runs = &mut health.runs;
        let bump = |n: &mut u64| *n = n.saturating_add(1);
        match outcome {
            VerifyOutcome::Passed { .. } => {
                bump(&mut runs.passed);
                health.last_good = Some((txid, now));
                health.failing = None;
                health.pinned = None;
            }
            VerifyOutcome::Failed(found) => {
                bump(&mut runs.failed);
                let since = health.failing.map_or(now, |was| was.since);
                health.failing = Some(VerifyFailure { since, txid, found });
                if health.pinned.is_none() {
                    health.pinned = Some(health.last_good.map_or(txid, |(good, _)| good));
                }
            }
            VerifyOutcome::Merged => bump(&mut runs.merged),
            VerifyOutcome::Incomplete => bump(&mut runs.incomplete),
        }
        match outcome {
            VerifyOutcome::Passed { .. } | VerifyOutcome::Failed(_) | VerifyOutcome::Merged => {
                health.worst_finding = worst;
            }
            VerifyOutcome::Incomplete => {}
        }
        health.last_run = Some(now);
        outcome
    }
}

impl Drop for Scheduled {
    /// The verify thread ends after the run it is in, if any; nobody waits for it.
    fn drop(&mut self) {
        self.job.lock().closed = true;
        self.job.ready.notify_all();
    }
}

/// How a run ended, and the worst finding of its listing check.
fn classify(result: Option<Result<Verified, VerifyError>>) -> (VerifyOutcome, Option<Severity>) {
    let Some(Ok(verified)) = result else {
        return (VerifyOutcome::Incomplete, None);
    };
    let worst = verified.report().worst();
    let unrestorable = worst == Some(Severity::Unrestorable);
    let (_report, compared) = verified.into_parts();
    let outcome = match compared {
        Compared::Differs {
            first,
            pages,
            live_commit,
            restored_commit,
            ..
        } => VerifyOutcome::Failed(Mismatch::Pages {
            first,
            pages,
            live_commit,
            restored_commit,
        }),
        Compared::RestoreFails { .. } => VerifyOutcome::Failed(Mismatch::Unrestorable),
        Compared::Same { .. } | Compared::Merged { .. } if unrestorable => {
            VerifyOutcome::Failed(Mismatch::Unrestorable)
        }
        Compared::Same { pages, .. } => VerifyOutcome::Passed { pages },
        Compared::Merged { .. } => VerifyOutcome::Merged,
    };
    (outcome, worst)
}

#[cfg(test)]
mod tests {
    use lsr_core::{StoreError, StoreOp};

    use super::*;

    /// A run that panicked, or whose listing failed, says nothing about the replica: it is
    /// incomplete, never a failure, and has no finding.
    #[test]
    fn a_run_that_could_not_finish_is_incomplete_not_failed() {
        assert_eq!(classify(None), (VerifyOutcome::Incomplete, None));
        let failed = VerifyError::Store(StoreError::Auth { op: StoreOp::List });
        assert_eq!(
            classify(Some(Err(failed))),
            (VerifyOutcome::Incomplete, None)
        );
    }
}

//! The fail-stop judge's place in the run loop (`crate::failstop`): after every tick it learns
//! what fired, whether the replica now holds everything, and judges the engine's trip.

use lsr_core::{Did, Event, RestoreTarget, SnapshotCause, Tick};
use lsr_ltx::Timestamp;

use super::Runner;
use crate::failstop::{Excused, Fired, TickSeen, data_path, fired};
use crate::generate::Op;
use crate::history::{image_digest, restored_digest};
use crate::oracle::Violation;
use crate::world::TickEnd;

impl Runner {
    /// Judges the engine's trip after the tick that began at `began`; `fired0` is what the
    /// stores' faults had fired before it.
    pub(super) fn judge_fail_stop(
        &mut self,
        began: Timestamp,
        fired0: (Fired, Fired),
        ended: &TickEnd<Tick>,
    ) -> Result<(), Violation> {
        self.fail_stop_commits();
        let (remote, staging) = (self.remote_fired(), fired(&self.w.staging_faults));
        let reported_failure = match ended {
            TickEnd::Done(Tick::Ran { event, .. }) => match event {
                // A step that waited on held writes did not move its commits either.
                Event::Retrying { .. } | Event::WritesHeld { .. } => true,
                Event::Degraded { task, .. } => data_path(*task),
                Event::Booted { .. }
                | Event::Synced { .. }
                | Event::Checkpointed { .. }
                | Event::Uploaded(_)
                | Event::Maintained { .. }
                | Event::Idle
                | Event::Resnapshot(_)
                | Event::Clamped(_)
                | Event::TakeoverWanted(_) => false,
            },
            TickEnd::Done(_) | TickEnd::Crashed { .. } => false,
        };
        let tick = TickSeen {
            at: began,
            true_at: began.unix_millis().saturating_sub(self.jumped),
            fault_fired: remote.all > fired0.0.all || staging.all > fired0.1.all,
            reported_failure,
            refusal_fired: remote.refused > fired0.0.refused,
            read_wal: matches!(
                ended,
                TickEnd::Done(Tick::Ran {
                    event: Event::Booted { .. }
                        | Event::Synced { .. }
                        | Event::Resnapshot(_)
                        | Event::Checkpointed { .. },
                    ..
                })
            ),
            reads_lying: self.reads_lying,
            unvouched_boot: matches!(
                ended,
                TickEnd::Done(Tick::Ran {
                    event: Event::Booted { cause: Some(cause) },
                    ..
                }) if breaks_continuity(*cause)
            ),
        };
        self.fail_stop.saw(tick);
        let believed = matches!(
            ended,
            TickEnd::Done(Tick::Ran {
                event: Event::Uploaded(Did::Put(_)),
                ..
            })
        );
        self.fail_stop
            .put(remote.doubtful > fired0.0.doubtful, believed);
        self.fail_stop_caught_up()?;
        let trip = self.actor.trip();
        if self.report.trips.last().map(|(_, was)| *was) != Some(trip) {
            self.report.trips.push((began, trip));
        }
        let bound = self.actor_bound();
        let booted = self.booted;
        let excused = self.fail_stop.excused();
        let unstaged_only = matches!(trip, Some(lsr_core::Trip::UnreplicatedTooLong { .. }))
            && self.w.sqlite.mx_frame() > 0
            && self.nothing_staged_waits();
        let judged = self
            .fail_stop
            .judge(trip, tick, bound, booted, unstaged_only);
        self.count_excused(excused);
        judged
    }

    /// Counts each exemption the judge used since it had excused `was`, under its own key.
    fn count_excused(&mut self, was: Excused) {
        let now = self.fail_stop.excused();
        if now.unread_wal > was.unread_wal {
            self.report.reach.bump("failstop.excused.unread_wal");
        }
        if now.lying_reads > was.lying_reads {
            self.report.reach.bump("failstop.excused.lying_reads");
        }
        if now.unvouched_snapshot > was.unvouched_snapshot {
            self.report
                .reach
                .bump("failstop.excused.unvouched_snapshot");
        }
    }

    /// When the replica changed while the judge thinks something is off it, restores
    /// `Latest` and compares it with the model's newest image. A restore that fails says the
    /// replica may not hold everything: the judge stays behind, which only makes it lenient.
    fn fail_stop_caught_up(&mut self) -> Result<(), Violation> {
        let len = self.w.remote_recorder.len();
        if !self.fail_stop.behind() || len == self.fail_stop_seen {
            return Ok(());
        }
        self.fail_stop_seen = len;
        let want = image_digest(&self.w.model.newest_image());
        if let Ok((got, _)) = restored_digest(&self.w.remote_raw, RestoreTarget::Latest)?
            && got == want
        {
            self.fail_stop.caught_up(want);
        }
        Ok(())
    }

    /// Whether every level-0 file in staging ends at or before the replica's newest
    /// transaction: nothing staged waits to be uploaded (a staging put back from an older
    /// backup is behind it). A store that cannot be listed says nothing waits not.
    fn nothing_staged_waits(&self) -> bool {
        let staged = match self.w.newest_staged() {
            Ok(staged) => staged.map(|info| info.key.range().max().get()),
            Err(_) => return false,
        };
        staged.is_none_or(|staged| {
            self.w
                .remote_newest()
                .is_some_and(|remote| staged <= remote)
        })
    }

    /// Something outside the engine is about to change the replica: the judge cannot tell
    /// which commits it took, so it is lenient until it next sees the replica whole.
    pub(super) fn fail_stop_outside(&mut self, op: &Op) {
        if matches!(op, Op::Remote(_) | Op::SecondWriter { .. }) {
            self.fail_stop.replica_changed_outside();
            self.fail_stop_seen = usize::MAX;
        }
    }

    /// Tells the judge of every commit the model gained since it last looked (the app's, a
    /// vacuum's, one inside a tick), dated at the start of the operation that made it: never
    /// later than the commit, so the age the judge measures is never short.
    pub(super) fn fail_stop_commits(&mut self) {
        let commits = self.w.model.commits().len();
        if commits == self.fail_stop_commits {
            return;
        }
        self.fail_stop_commits = commits;
        let at = self.op_began;
        let after = image_digest(&self.w.model.newest_image());
        let before = std::mem::replace(&mut self.fail_stop_image, after);
        self.fail_stop.committed(
            at,
            at.unix_millis().saturating_sub(self.jumped),
            before,
            after,
        );
    }

    /// The bound the engines of this run are built with.
    fn actor_bound(&self) -> std::time::Duration {
        lsr_core::MaxUnreplicatedAge::DEFAULT.get()
    }
}

/// Whether a boot's logged snapshot cause says continuity broke, so the snapshot may hold
/// commits that were never staged and the engine has nothing to compare it with.
const fn breaks_continuity(cause: SnapshotCause) -> bool {
    match cause {
        SnapshotCause::StagedUnverified { .. } | SnapshotCause::LocalRewound { .. } => true,
        // No replica yet, or one ahead of us (D81 keeps it out); a checkpoint is a mid-run
        // cause; the rest the engine never counts as lag.
        SnapshotCause::FirstSync
        | SnapshotCause::BehindRemote { .. }
        | SnapshotCause::CheckpointBoundary
        | SnapshotCause::OperatorRequested
        | SnapshotCause::GenerationChanged
        | SnapshotCause::WalMissing
        | SnapshotCause::PrevFrameMismatch
        | SnapshotCause::LastPageMismatch
        | SnapshotCause::NewestStagedFileUnreadable => false,
    }
}

//! The fail-stop judge's place in the run loop (`crate::failstop`): after every tick it learns
//! what fired, whether the replica now holds everything, and judges the engine's trip.

use lsr_core::{Event, RestoreTarget, Tick};
use lsr_ltx::Timestamp;

use super::Runner;
use crate::failstop::{Fired, TickSeen, data_path, fired};
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
        committed_in_tick: bool,
    ) -> Result<(), Violation> {
        if committed_in_tick {
            self.fail_stop
                .committed(began, began.unix_millis().saturating_sub(self.jumped));
        }
        let (remote, staging) = (fired(&self.w.remote_faults), fired(&self.w.staging_faults));
        let reported_failure = match ended {
            TickEnd::Done(Tick::Ran { event, .. }) => match event {
                Event::Retrying { .. } => true,
                Event::Degraded { task, .. } => data_path(*task),
                Event::Booted { .. }
                | Event::Synced { .. }
                | Event::Checkpointed { .. }
                | Event::Uploaded(_)
                | Event::Maintained { .. }
                | Event::Idle
                | Event::Resnapshot(_)
                | Event::Clamped(_) => false,
            },
            TickEnd::Done(_) | TickEnd::Crashed { .. } => false,
        };
        let tick = TickSeen {
            at: began,
            true_at: began.unix_millis().saturating_sub(self.jumped),
            fault_fired: remote.all > fired0.0.all || staging.all > fired0.1.all,
            reported_failure,
            refusal_fired: remote.refused > fired0.0.refused,
        };
        self.fail_stop.saw(tick);
        if self.w.remote_recorder.len() != self.fail_stop_seen {
            self.fail_stop.put(remote.doubtful > fired0.0.doubtful);
        }
        self.fail_stop_caught_up()?;
        let trip = self.actor.trip();
        if self.report.trips.last().map(|(_, was)| *was) != Some(trip) {
            self.report.trips.push((began, trip));
        }
        let bound = self.actor_bound();
        let booted = self.life.boot_salts.is_some();
        self.fail_stop.judge(trip, tick, bound, booted)
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
            self.fail_stop.caught_up();
        }
        Ok(())
    }

    /// Something outside the engine is about to change the replica: the judge cannot tell
    /// which commits it took, so it is lenient until it next sees the replica whole.
    pub(super) fn fail_stop_outside(&mut self, op: &Op) {
        if matches!(op, Op::Remote(_) | Op::SecondWriter { .. }) {
            self.fail_stop.replica_changed_outside();
            self.fail_stop_seen = usize::MAX;
        }
    }

    /// The bound the engines of this run are built with.
    fn actor_bound(&self) -> std::time::Duration {
        lsr_core::MaxUnreplicatedAge::DEFAULT.get()
    }
}

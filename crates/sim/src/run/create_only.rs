//! The runner's side of a replica store without conditional writes (`docs/decisions.md` D79):
//! the world fact, and the judge that no level-0 file is replaced on such a store before the
//! engine reports it.
//!
//! On a store that refuses create-only writes, the engine's uploads replace, as Go's do, and
//! nothing refuses another writer's file under the same key. D79 makes that loud: the upload task
//! stands degraded at `ConditionalWritesUnsupported` (or the engine stops). The judge holds the
//! engine to it: every level-0 replace the replica takes in such a world must find the upload task
//! degraded, or the engine stopped. A task holds one degraded cause, so while another fault holds
//! it (revoked credentials, say) health names that one, and the store's cause shows again at the
//! task's next success; what may never happen is a replace while health calls uploads healthy.

use lsr_core::{DegradedCause, Level, StopCause, Task, WriteMode};

use super::Runner;
use crate::oracle::Violation;
use crate::store::RecorderEvent;

impl Runner {
    /// From now on the replica's store refuses every create-only commit; the engine's stop on it
    /// is legitimate.
    pub(super) fn refuse_create_only(&mut self) {
        self.w.remote_raw.refuse_create_only();
        self.w.no_create_only = true;
        self.expect(StopCause::ConditionalWritesUnsupported);
        self.report.reach.bump("remote.no_create_only");
    }

    /// Every level-0 replace the replica took since the last call, on a store without
    /// conditional writes, came with the upload task degraded.
    pub(super) fn judge_unguarded(&mut self) -> Result<(), Violation> {
        let events = self.w.remote_recorder.events();
        let from = self.unguarded_seen.min(events.len());
        self.unguarded_seen = events.len();
        if !self.w.no_create_only {
            return Ok(());
        }
        let reported = self.actor.degraded(Task::Upload).is_some();
        for event in events.iter().skip(from) {
            let RecorderEvent::Put { key, mode, .. } = event else {
                continue;
            };
            if *mode != WriteMode::Replace || key.level() != Level::ZERO {
                continue;
            }
            if !reported {
                return Err(Violation::UnguardedReplace {
                    key: key.to_string(),
                });
            }
            self.report.reach.bump("remote.replaced_unguarded");
            if self
                .actor
                .degraded(Task::Upload)
                .map(|degraded| degraded.cause)
                == Some(DegradedCause::ConditionalWritesUnsupported)
            {
                self.report.reach.bump("remote.replaced_unguarded.named");
            }
        }
        Ok(())
    }
}

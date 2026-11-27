//! The snapshot a drain ends with on request: Go's `replicate -once -force-snapshot`
//! (`cmd/litestream/replicate.go:510-516`, `db.go:3102`), docs/decisions.md D167.
//!
//! Go syncs, uploads, then writes the whole database as one level-9 file `1..position`, outside
//! the snapshot interval and whatever the newest snapshot holds. Here a drain asked for it
//! ([`Actor::request_final_snapshot`]) runs it once everything committed is on the replica: one
//! sync step for the proof, then the same whole-database write the snapshot job makes, which
//! replaces a file already at that position as Go's write does. The planner is not asked: it
//! would call the snapshot idle at a position the newest snapshot already holds, where Go writes.
//!
//! It runs once and is never retried, as Go returns the first error: a fault, or a step that
//! cannot prove the position (a reset wal-index proves nothing until the next commit, which an
//! idle database never makes), ends it as [`FinalSnapshot::Failed`] and the drain finishes
//! without it, so the caller can say by name that the snapshot was not written. A step that
//! staged new commits goes back to the drain, which uploads them and comes back. It is not cut
//! by the drain's deadline once everything is uploaded: Go's `runOnce` has none.

use lsr_ltx::Txid;

use super::{Actor, Env, Event, MaintainedEvent, Stepped, Task, Tick};
use crate::fault::Fault;
use crate::fault::role::Role;
use crate::layout::FileInfo;
use crate::publish::Why;
use crate::snapshot::write_snapshot;
use crate::source::{Checkpointer, PageSource, WalSource};
use lsr_ltx::Timestamp;

/// Steps that end without a proof before the snapshot is given up: a checkpoint by another
/// connection can move the read-lock hold under one step, not under three in a row.
const PROOF_TRIES: u8 = 3;

/// Where the snapshot a drain ends with stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinalSnapshot {
    /// Not asked for.
    NotAsked,
    /// Asked for, not yet written; `tries` steps have ended without a proof.
    Asked {
        /// Steps that ended without a proof so far.
        tries: u8,
    },
    /// Written: the level-9 file.
    Written(FileInfo),
    /// Not written, and why.
    Failed(FinalSnapshotFailure),
}

/// Why the snapshot a drain ends with was not written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FinalSnapshotFailure {
    /// The sync step or the write failed.
    #[error("{0:?}")]
    Fault(Fault),
    /// No step proved the position: the wal-index was reset and nothing has been committed
    /// since, or the read-lock hold kept moving.
    #[error("no sync step proved the database's position{}", .reset.map_or(String::new(), |txid| format!(" (the wal-index was reset after transaction {txid})")))]
    NoProof {
        /// The newest staged transaction when the wal-index was found reset, if it was.
        reset: Option<Txid>,
    },
}

impl Actor {
    /// Makes the next drain end with a level-9 snapshot of the whole database, once everything
    /// committed is uploaded. Asking again changes nothing.
    pub fn request_final_snapshot(&mut self) {
        if self.kept.final_snapshot == FinalSnapshot::NotAsked {
            self.kept.final_snapshot = FinalSnapshot::Asked { tries: 0 };
        }
    }

    /// Where the snapshot stands.
    #[must_use]
    pub const fn final_snapshot(&self) -> FinalSnapshot {
        self.kept.final_snapshot
    }

    /// One attempt at the snapshot, for a drain that has uploaded everything; `None` when none
    /// is owed.
    pub(super) async fn final_snapshot_unit<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick>
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: Role,
        R: Role,
    {
        let FinalSnapshot::Asked { tries } = self.kept.final_snapshot else {
            return None;
        };
        let stepped = self.step(env, now).await;
        let proof = match stepped {
            Ok(Stepped::Synced {
                files: 0,
                proof: Some(proof),
            }) => proof,
            Ok(Stepped::Synced {
                files: 0,
                proof: None,
            }) => return Some(self.unproven_step(tries, now)),
            Err(fault) => {
                self.kept.final_snapshot =
                    FinalSnapshot::Failed(FinalSnapshotFailure::Fault(fault));
                // The fault still goes through the sync task's policy: its wait, its stall
                // count, and a failed WAL sync's debt.
                return Some(match self.stepped(Err(fault), now) {
                    Ok(_) => self.drained(),
                    Err(tick) => tick,
                });
            }
            other @ Ok(_) => {
                return Some(match self.stepped(other, now) {
                    Ok((files, _)) => self.ran(Event::Synced { files }, now),
                    Err(tick) => tick,
                });
            }
        };
        self.kept.tasks.get_mut(Task::Sync).succeed();
        let stamp = self
            .kept
            .clock
            .map_or(now, super::clock::EngineClock::stamp_floor);
        let written = write_snapshot(
            &proof,
            &mut *env.wal,
            &mut *env.db,
            &mut self.engine.pages,
            env.remote,
            stamp,
        )
        .await;
        Some(match written {
            Ok(info) => {
                self.kept.final_snapshot = FinalSnapshot::Written(info);
                self.snapshot_written(info, now, 0)
            }
            Err(failed) => {
                if failed.fault.leaves_bad_snapshot()
                    && crate::publish::remove_one(env.remote, failed.key, Why::RejectedSnapshot)
                        .await
                        .is_err()
                {
                    self.engine.maintain.state.distrust_snapshot(failed.key);
                }
                self.owe_read_back(failed.fault);
                self.kept.final_snapshot =
                    FinalSnapshot::Failed(FinalSnapshotFailure::Fault(failed.fault));
                self.drained()
            }
        })
    }

    /// A step that ended without a proof: try again, up to [`PROOF_TRIES`], except on a reset
    /// wal-index, which proves nothing until a commit comes.
    fn unproven_step(&mut self, tries: u8, now: Timestamp) -> Tick {
        let reset = self.reset().and_then(|_| self.staged());
        let tries = tries.saturating_add(1);
        if self.reset().is_some() || tries >= PROOF_TRIES {
            self.kept.final_snapshot =
                FinalSnapshot::Failed(FinalSnapshotFailure::NoProof { reset });
            return self.drained();
        }
        self.kept.final_snapshot = FinalSnapshot::Asked { tries };
        self.ran_maintained(MaintainedEvent::Waiting { held: None }, 0, now)
    }
}

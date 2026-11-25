//! The retention pass a drain ends with on request: Go's `replicate -once -enforce-retention`
//! (`cmd/litestream/replicate.go:519-526`, `store.go:863-883`), docs/decisions/D167.md.
//!
//! Go deletes level-9 files created before `now - snapshot-retention`, keeping the last, then the
//! level 1 to 8 files below the floor that leaves, and nothing at level 0. Here a drain asked
//! for it ([`Actor::request_final_retention`]) runs the daemon's own retention planner once
//! everything committed is on the replica, with level-0 deletion off and no compaction or
//! snapshot planned, one delete job per tick, until it has nothing more to delete. What that
//! keeps that Go deletes, never the reverse:
//! - a listing the planner's trust gates refuse is a hold: nothing is deleted and the pass ends
//!   as [`FinalRetention::Held`], so the caller exits 1 naming it;
//! - ages are measured from the replica's newest created-at time when that is behind the clock
//!   (the planner's anchor), so on an idle replica snapshots Go would delete are kept;
//! - each level's last file is kept, as Go keeps it.
//!
//! With retention switched off nothing is deleted ([`FinalRetention::Off`]), as Go deletes
//! nothing remote then. A failed listing, created-at read or delete ends the pass as
//! [`FinalRetention::Failed`], not retried, as Go returns its first error. Like the drain's
//! uploads it is not cut by the drain's deadline once everything is uploaded: Go's one-off run
//! has no deadline.

use lsr_ltx::Timestamp;

use super::{Actor, Env, Event, MaintainedEvent, Tick};
use crate::created_at::{ResolvedCreatedAt, resolve_knowing};
use crate::fault::Fault;
use crate::fault::role::Role;
use crate::inventory::Inventory;
use crate::layout::FileKey;
use crate::maintain::{OnceRetention, Outcome, RetentionHold, once_retention, run_delete};
use crate::source::{Checkpointer, PageSource, WalSource};

/// Delete jobs in a row that begin with the same key before the pass stops as stuck.
const REPEATS: u8 = 3;

/// Where the retention pass a drain ends with stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalRetention {
    /// Not asked for.
    NotAsked,
    /// Asked for and running.
    Asked {
        /// Keys deleted so far.
        deleted: u64,
        /// The first key of the last delete job, and how many jobs in a row began with it.
        repeated: Option<(FileKey, u8)>,
    },
    /// Nothing more to delete.
    Done {
        /// Keys deleted.
        deleted: u64,
    },
    /// Retention is switched off: nothing was deleted.
    Off,
    /// The snapshot asked to come first was not written, so the pass did not run, as Go
    /// returns before retention when its snapshot fails.
    Skipped,
    /// The planner refused to delete from the listing.
    Held {
        /// Keys deleted before the hold.
        deleted: u64,
        /// Why.
        hold: RetentionHold,
    },
    /// The listing kept showing a key after it was deleted, [`REPEATS`] times in a row: the
    /// pass stops rather than delete it again for ever.
    Stuck {
        /// Keys deleted before it stopped.
        deleted: u64,
        /// The key the listing kept showing.
        key: FileKey,
    },
    /// A listing, created-at read or delete failed.
    Failed {
        /// Keys deleted before the failure.
        deleted: u64,
        /// What failed.
        fault: Fault,
    },
}

impl Actor {
    /// Makes the next drain end with a retention pass, once everything committed is uploaded.
    /// Asking again changes nothing.
    pub fn request_final_retention(&mut self) {
        if self.kept.final_retention == FinalRetention::NotAsked {
            self.kept.final_retention = FinalRetention::Asked {
                deleted: 0,
                repeated: None,
            };
        }
    }

    /// A pass still owed does not run: the snapshot asked to come first was not written.
    pub(super) fn skip_final_retention(&mut self) {
        if matches!(self.kept.final_retention, FinalRetention::Asked { .. }) {
            self.kept.final_retention = FinalRetention::Skipped;
        }
    }

    /// Where the retention pass a drain ends with stands.
    #[must_use]
    pub fn final_retention(&self) -> FinalRetention {
        self.kept.final_retention.clone()
    }

    /// One step of the pass, for a drain that has uploaded everything; `None` when none is
    /// owed.
    pub(super) async fn final_retention_unit<W, P, C, L, R>(
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
        let FinalRetention::Asked { deleted, repeated } = self.kept.final_retention.clone() else {
            return None;
        };
        let inventory = match Inventory::load(env.remote).await {
            Ok(inventory) => inventory,
            Err(err) => {
                let fault = super::inventory_fault::<R>(&err);
                return Some(self.retention_ended(FinalRetention::Failed { deleted, fault }));
            }
        };
        let published = self.engine.upload.confirmed();
        self.engine
            .maintain
            .state
            .set_kept_restore_point(self.kept.kept_restore_point());
        let decided = {
            let (cfg, state) = (&self.kept.cfg.maintenance, &self.engine.maintain.state);
            let mut step = |resolved: &ResolvedCreatedAt| {
                once_retention(&inventory, now, cfg, published, state, resolved)
            };
            resolve_knowing(env.remote, &mut self.engine.known, &mut step).await
        };
        let decided = match decided {
            Ok(decided) => decided.value,
            Err(err) => {
                let fault = Fault::from_created_at(&err);
                return Some(self.retention_ended(FinalRetention::Failed { deleted, fault }));
            }
        };
        Some(match decided {
            None => self.retention_ended(FinalRetention::Off),
            Some(OnceRetention::Done) => self.retention_ended(FinalRetention::Done { deleted }),
            Some(OnceRetention::Held(hold)) => {
                self.retention_ended(FinalRetention::Held { deleted, hold })
            }
            Some(OnceRetention::Delete(job)) => {
                let first = job.keys().first().copied();
                let repeated = match (first, repeated) {
                    (Some(key), Some((was, n))) if key == was => Some((key, n.saturating_add(1))),
                    (Some(key), _) => Some((key, 1)),
                    (None, _) => None,
                };
                if let Some((key, n)) = repeated
                    && n > REPEATS
                {
                    return Some(self.retention_ended(FinalRetention::Stuck { deleted, key }));
                }
                self.delete_step(env.remote, &job, deleted, repeated, now)
                    .await
            }
        })
    }

    /// Runs one delete job of the pass.
    async fn delete_step<R: Role>(
        &mut self,
        remote: &R,
        job: &crate::maintain::DeleteJob,
        deleted: u64,
        repeated: Option<(FileKey, u8)>,
        now: Timestamp,
    ) -> Tick {
        match run_delete(job, remote).await {
            Outcome::Deleted { count } => {
                let count64 = u64::try_from(count).unwrap_or(u64::MAX);
                self.kept.final_retention = FinalRetention::Asked {
                    deleted: deleted.saturating_add(count64),
                    repeated,
                };
                self.ran(
                    Event::Maintained {
                        event: MaintainedEvent::Deleted(count),
                        created_at_read: 0,
                    },
                    now,
                )
            }
            Outcome::Failed(err) => {
                let fault = Fault::from_maintain(&err);
                self.retention_ended(FinalRetention::Failed { deleted, fault })
            }
            Outcome::Compacted { .. } | Outcome::Replan { .. } => self.retention_waiting(None, now),
        }
    }

    /// The pass has ended: the drain finishes.
    fn retention_ended(&mut self, ended: FinalRetention) -> Tick {
        self.kept.final_retention = ended;
        self.drained()
    }

    fn retention_waiting(&mut self, held: Option<RetentionHold>, now: Timestamp) -> Tick {
        self.ran(
            Event::Maintained {
                event: MaintainedEvent::Waiting { held },
                created_at_read: 0,
            },
            now,
        )
    }
}

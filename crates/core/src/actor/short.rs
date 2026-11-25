//! The maintenance pass that lists only what a check reads, and what every pass does with its
//! decision. The rules, per delete kind, are in `crate::maintain`'s `short` module.

use super::*;
use crate::maintain::{ShortDecided, Tail, due_short};

/// What a short pass decided: the job or wait, and what to do after a level-0 delete.
pub(super) struct ShortPass {
    pub(super) due: Due,
    pub(super) after_delete: Option<Option<Timestamp>>,
    pub(super) created_at_read: u64,
}

impl Actor {
    /// Whether the next maintenance pass may list a tail instead of the whole replica: the
    /// remembered newest files are valid and recent enough, and nothing needs a whole view
    /// (the boot read-back, a pinned restore point, a distrusted output).
    pub(super) fn short_pass_ok(&self, now: Timestamp) -> bool {
        let maintain = &self.engine.maintain;
        maintain.remembered.short_ok(now)
            && !maintain.read_back_owed
            && !maintain.state.has_distrusted()
            && self.kept.kept_restore_point().is_none()
    }

    /// One maintenance pass's decision over a short listing, or the tick of the fault it found.
    /// The checks the whole pass makes on its listing (a file this engine does not know to be
    /// its own, a replica below what it confirmed) are made on the listed files only.
    ///
    /// The caller acts on the decision: this future's frames are gone before a job runs, which
    /// keeps the replicator thread's stack under the headroom gate (crates/replica/src/stack.rs).
    pub(super) async fn maintain_short<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: Role,
        R: Role,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<ShortPass, Box<Tick>> {
        let confirmed = self.engine.upload.confirmed();
        let (plan, wake) = {
            let (remembered, cfg) = (&self.engine.maintain.remembered, &self.kept.cfg.maintenance);
            (
                remembered.plan(cfg, now, confirmed),
                remembered.next_wake(cfg, now),
            )
        };
        if plan.is_empty() {
            // Nothing is due, or nothing a listing could change: a pass that asks for no request.
            let maintain = &mut self.engine.maintain;
            maintain.remembered.settle();
            maintain.stale = false;
            return Ok(ShortPass {
                due: Due::Wait {
                    until: wake,
                    held: None,
                },
                after_delete: Some(wake),
                created_at_read: 0,
            });
        }
        let loaded = Tail::load(
            env.remote,
            &self.engine.maintain.remembered,
            &self.kept.cfg.maintenance,
            plan,
            now,
            confirmed,
        )
        .await;
        let tail = match loaded.map_err(|err| inventory_fault::<R>(&err)) {
            Ok(tail) => tail,
            Err(fault) => return Err(Box::new(self.fault(Task::Maintenance, fault, now))),
        };
        let listed = tail.newest_listed();
        if let Some(tick) = self.listing_untrusted(listed, now) {
            return Err(Box::new(tick));
        }
        if listed < tail.baseline() {
            return Err(Box::new(self.fault(
                Task::Upload,
                Fault::RemoteRegressed,
                now,
            )));
        }
        let decided = {
            let (cfg, state) = (&self.kept.cfg.maintenance, &self.engine.maintain.state);
            let mut step = |resolved: &ResolvedCreatedAt| {
                due_short(&tail, now, cfg, confirmed, state, resolved)
            };
            resolve(env.remote, &mut step).await
        };
        let decided = match decided {
            Ok(decided) => decided,
            Err(err) => {
                return Err(Box::new(self.fault(
                    Task::Maintenance,
                    Fault::from_created_at(&err),
                    now,
                )));
            }
        };
        let maintain = &mut self.engine.maintain;
        if decided.value.whole {
            // Nothing was planned from this listing and nothing is learned from it: the
            // next tick lists the whole replica and decides there.
            maintain.relist();
            return Ok(ShortPass {
                due: decided.value.due,
                after_delete: None,
                created_at_read: decided.created_at_read,
            });
        }
        maintain.remembered.learn_short(
            &tail,
            &self.kept.cfg.maintenance,
            &decided.value,
            now,
            confirmed,
        );
        let wake = maintain
            .remembered
            .next_wake(&self.kept.cfg.maintenance, now);
        let ShortDecided { due, until, .. } = decided.value;
        let until = [until, wake].into_iter().flatten().min();
        let due = match due {
            Due::Wait { until: wait, held } => Due::Wait {
                until: [wait, wake].into_iter().flatten().min(),
                held,
            },
            run @ Due::Run(_) => run,
        };
        maintain.stale = false;
        maintain.passed_over = 0;
        maintain.snapshot_waits = false;
        maintain.level_one = maintain.remembered.newest_txid(Level::ONE);
        Ok(ShortPass {
            due,
            after_delete: Some(until),
            created_at_read: decided.created_at_read,
        })
    }

    /// What a pass does with its decision. `after_delete` is `Some` for a short pass: the
    /// level-0 delete it plans cannot make anything else due, so unless the job was full the
    /// next pass waits for `after_delete` instead of listing again.
    pub(super) async fn act_on<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        due: Due,
        after_delete: Option<Option<Timestamp>>,
        created_at_read: u64,
        now: Timestamp,
    ) -> Tick {
        match due {
            Due::Wait { until, held } => {
                self.maintained(now);
                let wake = self.engine.maintain.remembered.retention_wake();
                self.engine.maintain.until = [until, wake].into_iter().flatten().min();
                let event = Event::Maintained {
                    event: MaintainedEvent::Waiting { held },
                    created_at_read,
                };
                self.ran(event, now)
            }
            // A snapshot's last transaction must be at most what was published: with
            // uploads busy it waits, and the next listing with them.
            Due::Run(Job::Snapshot(_)) if !self.uploads_idle() => {
                self.engine.maintain.stale = true;
                self.engine.maintain.snapshot_waits = true;
                let event = Event::Maintained {
                    event: MaintainedEvent::Waiting { held: None },
                    created_at_read,
                };
                self.ran(event, now)
            }
            Due::Run(job) => {
                // Whatever the job does, the next listing is what to plan from.
                self.engine.maintain.stale = true;
                if matches!(job, Job::Snapshot(_)) {
                    self.engine.maintain.remembered.owe_full();
                }
                if after_delete.is_some()
                    && let Err(bug) = self.follows_its_level(&job)
                {
                    return self.fault(Task::Maintenance, bug, now);
                }
                let tick = self.run_job(env, &job, now, created_at_read).await;
                self.after_job(&job, &tick, after_delete, now);
                tick
            }
        }
    }

    /// Checked before a short pass's compaction writes anything: its output starts at the
    /// transaction after the newest file the engine knows at that level. A short pass that
    /// sees otherwise asks for a whole listing and plans nothing, so a job that fails this
    /// was planned from a listing that lost a file, which is how levels 2 and 3 gained holes
    /// that Go's compaction then refused (docs/decisions/D228.md).
    pub(super) fn follows_its_level(&self, job: &Job) -> Result<(), Fault> {
        if let Job::Compact(compact) = job {
            let (level, range) = (compact.level(), compact.inputs().range());
            crate::invariant!(
                self.engine.maintain.remembered.follows_newest(level, range),
                crate::BugSite::CompactionLeavesAHole
            );
        }
        Ok(())
    }

    /// What a finished job teaches the remembered files: a compaction's output is the newest
    /// file of its level, and a short pass's level-0 delete, when it was not cut at the
    /// delete limit, leaves nothing more to plan until the next file ages.
    pub(super) fn after_job(
        &mut self,
        job: &Job,
        tick: &Tick,
        after_delete: Option<Option<Timestamp>>,
        now: Timestamp,
    ) {
        let Tick::Ran {
            event: Event::Maintained { event, .. },
            ..
        } = tick
        else {
            return;
        };
        match (event, job) {
            (MaintainedEvent::Compacted(info), _) => {
                let cfg = &self.kept.cfg.maintenance;
                self.engine.maintain.remembered.note_output(*info, cfg, now);
            }
            // An input is gone, or the output's key already holds an object the listing did not
            // show: someone else is changing the replica, and the next listing is a whole one.
            (MaintainedEvent::Replan, _) => self.engine.maintain.remembered.owe_full(),
            (MaintainedEvent::Deleted(count), Job::Delete(_)) if *count < MAX_DELETE => {
                if let Some(until) = after_delete {
                    self.engine.maintain.stale = false;
                    self.engine.maintain.until = until;
                }
            }
            _ => {}
        }
    }
}

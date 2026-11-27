//! The replicator's write gate in the run loop (`docs/decisions/D192.md`): with
//! [`super::Opts::write_gate`] set, the runner holds the application's writes as
//! `lsr-replica`'s thread does, by the same rule (`lsr_core::blocks_writes` over the
//! `AppGate`), through the model's held-write-lock door, at every start and after every tick.
//! The boot is judged once it has listed the replica and decided how to start, before its
//! snapshot is staged (`docs/decisions/D201.md`), so nothing the engine writes meets a hold
//! for the boot.
//!
//! The judge here keeps its own record of what it held and checks two things the model must
//! never let happen: a hold it took went away without a crash or a release of its own (checked
//! after each tick that did not crash), and an application commit landed while it held.

use lsr_core::{AppGate, IfReplicationStops, WhyBlocked, blocks_writes};

use super::Runner;
use crate::oracle::Violation;

/// The gate's state in a run with one.
#[derive(Debug, Default)]
pub(super) struct WriteGate {
    /// The fail-stop setting; `None` runs no gate.
    mode: Option<IfReplicationStops>,
    /// What this process's gate has released, as the replicator's thread keeps it.
    app_gate: AppGate,
    /// The runner took the write lock and has not let it go.
    held: bool,
}

impl WriteGate {
    pub(super) fn new(mode: Option<IfReplicationStops>) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }
}

impl Runner {
    /// A new process: nothing released, no lock held (the old one died with its process), and
    /// the gate taken at once if the boot is pending.
    pub(super) fn gate_restarted(&mut self) {
        self.write_gate = WriteGate::new(self.write_gate.mode);
        self.keep_writes();
    }

    /// Holds or releases the application's writes as the engine's state now asks.
    pub(super) fn keep_writes(&mut self) {
        let Some(mode) = self.write_gate.mode else {
            return;
        };
        let trip = self.actor.trip();
        let judged = self.actor.boot_judged() || trip.is_some();
        let gate = self.write_gate.app_gate.judge(mode, trip, judged);
        let why = blocks_writes(mode, gate);
        self.write_gate.held = match why {
            Some(why) => {
                self.report.reach.bump(format!("writes.blocked.{why:?}"));
                // A hold an application transaction keeps out is tried again after the next
                // tick, as the thread tries it at its next turn.
                self.w.sqlite.hold_writes()
            }
            None => {
                if self.write_gate.held && self.actor.pending_snapshot().is_some() {
                    self.report.reach.bump("writes.allowed.snapshot_owed");
                }
                self.w.sqlite.release_writes();
                false
            }
        };
    }

    /// The tick told of the identity check as it started (`checking`): writes held for the boot
    /// were held for the check, as the replicator's thread names it while the tick runs.
    pub(super) fn gate_saw_identity_check(&mut self, checking: bool) {
        if checking && self.write_gate.held {
            self.report
                .reach
                .bump(format!("writes.blocked.{:?}", WhyBlocked::CheckingIdentity));
        }
    }

    pub(super) const fn holding_writes(&self) -> bool {
        self.write_gate.held
    }

    /// After a tick that began with writes held (`held`) and did not crash: the lock is still
    /// held (nothing between ticks or inside this one took it away), and no commit of the
    /// application's landed inside the tick (`committed`).
    pub(super) fn judge_gate_kept(&self, held: bool, committed: bool) -> Result<(), Violation> {
        if !held {
            return Ok(());
        }
        if !self.w.sqlite.writes_held() {
            return Err(Violation::WritesLeaked {
                detail: "the write lock went away while the runner held it".to_owned(),
            });
        }
        if committed {
            return Err(Violation::WritesLeaked {
                detail: "an application commit landed inside a tick".to_owned(),
            });
        }
        Ok(())
    }

    /// An application commit outside a tick landed (`landed`): never while the runner holds.
    pub(super) fn judge_commit_landed(&mut self, landed: bool) {
        if landed {
            self.turns.committed();
        }
        if !landed && self.holding_writes() {
            self.report.reach.bump("writes.commit_refused");
        }
        if landed && self.holding_writes() && self.leaked.is_none() {
            self.leaked = Some(Violation::WritesLeaked {
                detail: "an application commit landed".to_owned(),
            });
        }
    }
}

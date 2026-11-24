//! One scenario per state of the replicator's write gate (`docs/decisions/D192.md`): writes
//! held while the boot is pending, let through once it is judged, and held again by a stop
//! under `stop-app`, but never under `report`. The run's write-gate judge
//! (`crate::run`) fails any of them in which the held lock goes away or a commit lands under
//! it; each scenario's check says the state it is about was reached.

use lsr_core::{CheckpointMode, IfReplicationStops, StopCause};

use super::{EventMatch, Expect, SETTLE, Scenario, build, commit, second_writer_mid_run, ticks};
use crate::generate::{Op, RemoteOp};
use crate::run::RunReport;
use crate::sqlite::OtherProcessMode;

/// The reach key of a commit the gate refused.
const REFUSED: &str = "writes.commit_refused";

fn gated(mut scenario: Scenario, mode: IfReplicationStops) -> Scenario {
    scenario.write_gate = Some(mode);
    scenario
}

fn reached(report: &RunReport, key: &str) -> Result<(), String> {
    if report.reach.get(key) == 0 {
        return Err(format!("{key} never happened"));
    }
    Ok(())
}

fn never(report: &RunReport, prefix: &str) -> Result<(), String> {
    match report.reach.iter().find(|(key, _)| key.starts_with(prefix)) {
        Some((key, _)) => Err(format!("{key} happened")),
        None => Ok(()),
    }
}

/// Under `stop-app` a commit made before the first boot is judged is refused, and the same
/// pages commit once the boot lets writes through.
#[must_use]
pub fn writes_held_while_the_boot_is_pending() -> Scenario {
    let mut s = gated(
        build(
            "writes_held_while_the_boot_is_pending",
            vec![
                vec![commit(3, false)],
                ticks(4),
                vec![commit(2, false)],
                ticks(SETTLE),
                vec![Op::Restore],
            ],
            vec![Expect::Oracle],
        ),
        IfReplicationStops::StopApp,
    );
    s.check = Some(|report| {
        reached(report, "writes.blocked.Booting")?;
        reached(report, REFUSED)
    });
    s
}

/// Once the boot is judged, commits land and the engine's checkpoints run.
#[must_use]
pub fn writes_allowed_after_the_boot() -> Scenario {
    let mut s = gated(
        build(
            "writes_allowed_after_the_boot",
            vec![
                ticks(4),
                vec![commit(9, true), commit(2, false)],
                ticks(SETTLE),
                vec![Op::Restore],
            ],
            vec![
                Expect::Event(EventMatch::Checkpointed(CheckpointMode::Passive)),
                Expect::Oracle,
            ],
        ),
        IfReplicationStops::StopApp,
    );
    s.check = Some(|report| never(report, REFUSED));
    s
}

/// [`second_writer_mid_run`], with commits and another process's RESTART checkpoint after its
/// stop: under `stop-app` the stop holds writes, so the commits are refused and the checkpoint
/// does not take the lock away.
#[must_use]
pub fn writes_held_after_a_stop() -> Scenario {
    let mut s = gated(
        stopped_then_commits("writes_held_after_a_stop"),
        IfReplicationStops::StopApp,
    );
    s.check = Some(|report| {
        reached(report, "writes.blocked.Tripped(Stopped(OtherWriter))")?;
        reached(report, REFUSED)
    });
    s
}

/// The same under `report`: nothing is ever held, not even while the boot is pending.
#[must_use]
pub fn writes_never_held_under_report() -> Scenario {
    let mut s = gated(
        stopped_then_commits("writes_never_held_under_report"),
        IfReplicationStops::Report,
    );
    s.check = Some(|report| {
        never(report, "writes.blocked")?;
        never(report, REFUSED)
    });
    s
}

fn stopped_then_commits(name: &'static str) -> Scenario {
    let mut s = second_writer_mid_run();
    s.name = name;
    let restore = s
        .ops
        .iter()
        .rposition(|op| matches!(op, Op::Remote(RemoteOp::RemoveUnrecognized)));
    let at = restore.unwrap_or(s.ops.len());
    // Another process's RESTART checkpoint needs the write lock too: it must not break the
    // hold, and leaves the WAL as it is.
    s.ops.splice(
        at..at,
        [
            commit(2, false),
            Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
            Op::Tick,
            commit(3, false),
        ],
    );
    s.expect = vec![Expect::Stopped(StopCause::OtherWriter), Expect::Oracle];
    s
}

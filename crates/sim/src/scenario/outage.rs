//! The store is down across a restart, and the application commits throughout
//! (`docs/decisions/D220.md`): once boot attempts have failed for the bound, the engine stages
//! and checkpoints from local state, so the WAL stays short, and the boot that at last lists
//! the replica judges it as any boot does. One scenario for the store coming back as it was,
//! and one each for it coming back with another writer's file, rolled back, and refusing.

use lsr_core::{CheckpointMode, Denied, Event, SnapshotCause, StopCause, StoreError, StoreOp};

use super::{EventMatch, Expect, SETTLE, Scenario, build, commit, ticks};
use crate::generate::{Hold, MetaOp, Op, RemoteOp, Target};
use crate::run::{RunReport, Seen};
use crate::store::FaultShape;

/// Rounds of one commit and one tick the application runs while the store is down.
const ROUNDS: u32 = 40;

/// The most frames the WAL may ever hold in [`store_down_across_a_restart`]: the engine's
/// TRUNCATE threshold in these runs (`crate::run`), which the PASSIVE checkpoints of a local
/// course keep the WAL under. The application writes about `3 * ROUNDS` frames meanwhile.
pub const OUTAGE_WAL_FRAMES: u32 = 32;

/// The reach key of a tick that ended with the engine staging before it booted.
const LOCAL_COURSE: &str = "boot.local_course";

const OPS: [StoreOp; 4] = [
    StoreOp::List,
    StoreOp::Read,
    StoreOp::Upload,
    StoreOp::Delete,
];

fn failing(op: StoreOp, err: StoreError) -> Op {
    Op::StoreFault {
        target: Target::Remote,
        op,
        ahead: 0,
        shape: FaultShape::Rate {
            per_mille: 1000,
            err,
        },
    }
}

/// Every call on the replica fails as a timeout until [`Op::Heal`].
fn store_down() -> Vec<Op> {
    OPS.into_iter()
        .map(|op| {
            let err = StoreError::Transient {
                op,
                kind: std::io::ErrorKind::TimedOut,
            };
            failing(op, err)
        })
        .collect()
}

/// A snapshot on the replica, then the store goes down, two commits are staged and cannot
/// be sent, the process is killed, and the new one runs `ROUNDS` rounds of a commit and a
/// tick with the store still down.
fn restarted_while_down() -> Vec<Op> {
    let mut ops = vec![commit(3, false)];
    ops.extend(ticks(4));
    ops.extend(store_down());
    ops.extend([commit(2, false), Op::Tick, commit(2, false), Op::Tick]);
    ops.push(Op::Restart);
    for _ in 0..ROUNDS {
        ops.extend([commit(2, false), Op::Tick]);
    }
    ops
}

/// The store is down across a restart and through `ROUNDS` commits after it, then answers.
/// The engine stages every commit and checkpoints while it is down, so the WAL never holds
/// more than [`OUTAGE_WAL_FRAMES`] frames; the boot that lists the replica continues with no
/// snapshot, and the replica restores every commit.
#[must_use]
pub fn store_down_across_a_restart() -> Scenario {
    let mut s = build(
        "store_down_across_a_restart",
        vec![
            restarted_while_down(),
            vec![Op::Heal],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Checkpointed(CheckpointMode::Passive)),
            Expect::Event(EventMatch::Booted(None)),
            Expect::Oracle,
        ],
    );
    s.check = Some(short_wal_and_no_snapshot);
    s
}

/// The run's events from the first failed call on the replica to the first boot after it:
/// what the engine did while the store was down.
fn while_down(report: &RunReport) -> Result<&[Seen], String> {
    let seen = &report.seen;
    let down = seen
        .iter()
        .position(|seen| matches!(seen, Seen::Event(Event::Retrying { .. })))
        .ok_or("no call on the replica ever failed")?;
    let after = seen.get(down..).unwrap_or_default();
    let back = after
        .iter()
        .position(|seen| matches!(seen, Seen::Event(Event::Booted { .. })) || stop(seen))
        .unwrap_or(after.len());
    Ok(after.get(..back).unwrap_or_default())
}

const fn stop(seen: &Seen) -> bool {
    matches!(seen, Seen::Stop(_))
}

/// While the store was down the engine ran on a local course, staged files and checkpointed,
/// and sent nothing.
fn ran_from_local_state(report: &RunReport) -> Result<(), String> {
    if report.reach.get(LOCAL_COURSE) == 0 {
        return Err("the engine never staged before it booted".to_owned());
    }
    let down = while_down(report)?;
    let staged: u32 = down
        .iter()
        .map(|seen| match seen {
            Seen::Event(Event::Synced { files }) => *files,
            Seen::Event(_)
            | Seen::Stop(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => 0,
        })
        .sum();
    if staged < ROUNDS / 2 {
        return Err(format!("{staged} files staged while the store was down"));
    }
    let checkpoints = down
        .iter()
        .filter(|seen| matches!(seen, Seen::Event(Event::Checkpointed { .. })))
        .count();
    if checkpoints < 2 {
        return Err(format!(
            "{checkpoints} checkpoints while the store was down"
        ));
    }
    let sent = down.iter().any(|seen| {
        matches!(
            seen,
            Seen::Event(Event::Uploaded(_) | Event::Maintained { .. }) | Seen::Snapshot(_)
        )
    });
    if sent {
        return Err(
            "the engine wrote to the replica, or took a snapshot, before a boot".to_owned(),
        );
    }
    Ok(())
}

fn short_wal_and_no_snapshot(report: &RunReport) -> Result<(), String> {
    if report.max_wal_frames > OUTAGE_WAL_FRAMES {
        return Err(format!(
            "the WAL grew to {} frames, past {OUTAGE_WAL_FRAMES}",
            report.max_wal_frames
        ));
    }
    ran_from_local_state(report)
}

/// The store comes back with another writer's file where this engine's first staged file
/// would go: the boot refuses it as a boot from nothing does, with nothing staged during
/// the outage sent; once the operator removes the file and restarts, the engine replicates.
#[must_use]
pub fn store_returns_with_another_writers_file() -> Scenario {
    let mut s = build(
        "store_returns_with_another_writers_file",
        vec![
            restarted_while_down(),
            vec![Op::Remote(RemoteOp::PutUnrecognized), Op::Heal],
            ticks(SETTLE),
            vec![
                Op::Remote(RemoteOp::RemoveUnrecognized),
                Op::Restart,
                commit(1, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Stopped(StopCause::OtherWriter), Expect::Oracle],
    );
    s.check = Some(|report| {
        ran_from_local_state(report)?;
        super::refused_until_removed(report)
    });
    s
}

/// A second file on the replica before the store goes down, then [`restarted_while_down`]'s
/// outage and restart.
fn restarted_while_down_past_two_files() -> Vec<Op> {
    let mut ops = vec![commit(3, false)];
    ops.extend(ticks(4));
    ops.extend([commit(2, false), Op::Tick, Op::Tick]);
    ops.extend(restarted_while_down().into_iter().skip(5));
    ops
}

/// The bucket rolls back by one transaction while the store is down. Staging still holds
/// the file after the replica's new newest, so the boot continues and sends the staged
/// files again: no snapshot, and the replica restores every commit.
#[must_use]
pub fn store_returns_rolled_back() -> Scenario {
    let mut s = build(
        "store_returns_rolled_back",
        vec![
            restarted_while_down_past_two_files(),
            vec![Op::Remote(RemoteOp::RollBack), Op::Heal],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Booted(None)), Expect::Oracle],
    );
    s.check = Some(ran_from_local_state);
    s
}

/// The store comes back refusing the credentials, and the application goes on committing.
/// The engine keeps staging and checkpointing from local state and reports the refusal;
/// when the refusal ends, the boot continues with no snapshot.
#[must_use]
pub fn store_returns_refusing() -> Scenario {
    let refusing = OPS.into_iter().map(|op| {
        let err = StoreError::Denied {
            op,
            why: Denied::Credentials,
        };
        failing(op, err)
    });
    let mut ops = restarted_while_down();
    ops.push(Op::Heal);
    ops.extend(refusing);
    for _ in 0..ROUNDS {
        ops.extend([commit(2, false), Op::Tick]);
    }
    let mut s = build(
        "store_returns_refusing",
        vec![ops, vec![Op::Heal], ticks(SETTLE), vec![Op::Restore]],
        vec![
            Expect::Stopped(StopCause::StoreDenied(Denied::Credentials)),
            Expect::Event(EventMatch::Booted(None)),
            Expect::Oracle,
        ],
    );
    s.check = Some(short_wal_and_no_snapshot);
    s
}

/// Staging fell behind the replica (its newest file deleted) before a restart with the store
/// down, so the course staged files at transactions the replica already held; the process
/// was killed again; and when the store answered, reads of the replica lied for a while.
/// An upload that found one of the course's files under the unreadable newest replica file's
/// key would write it over that file: one transaction with two contents. The course's files
/// are kept apart from level 0, so no upload sees them, and the boot that lists the replica
/// deletes them for numbering transactions the replica holds.
#[must_use]
pub fn store_returns_unreadable_after_staging_fell_behind() -> Scenario {
    let mut ops = vec![commit(3, false)];
    ops.extend(ticks(4));
    ops.extend([commit(2, false), Op::Tick, Op::Tick]);
    ops.push(Op::StagingDamage(MetaOp::DeleteNewest));
    ops.extend(store_down());
    ops.push(Op::Restart);
    for _ in 0..ROUNDS {
        ops.extend([commit(2, false), Op::Tick]);
    }
    ops.push(Op::Restart);
    for _ in 0..ROUNDS {
        ops.extend([commit(2, false), Op::Tick]);
    }
    ops.push(Op::Heal);
    ops.push(Op::Hold {
        hold: Hold::LyingReads,
        ticks: 12,
    });
    let mut s = build(
        "store_returns_unreadable_after_staging_fell_behind",
        vec![ops, ticks(SETTLE), vec![Op::Restore]],
        vec![Expect::Oracle],
    );
    s.check = Some(|report| {
        if report.reach.get(LOCAL_COURSE) == 0 {
            return Err("the engine never staged from local state".to_owned());
        }
        // The boot found level 0 as the damage left it, behind the replica: the course's
        // files were not there to hide it.
        let behind = |seen: &Seen| {
            matches!(
                seen,
                Seen::Event(Event::Booted {
                    cause: Some(SnapshotCause::BehindRemote { .. })
                })
            )
        };
        if !report.seen.iter().any(behind) {
            return Err("no boot found staging behind the replica".to_owned());
        }
        Ok(())
    });
    s
}

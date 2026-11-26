//! The engine's own checkpoint against a write lock another connection holds: a wait or a
//! finished checkpoint, never a failure, and never a snapshot for the WAL that writer restarts
//! (`docs/decisions/D225.md`).

use super::*;
use crate::SnapshotCauseKind;
use crate::source::{CheckpointEffect, CheckpointScript};

/// An actor that checkpoints PASSIVE at one synced frame, settled on a rig.
fn eager() -> (Rig, Actor) {
    let mut config = cfg();
    config.checkpoint.min_pages = Some(nz(1));
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    (rig, actor)
}

/// Ticks at `now` until a tick ends in a checkpoint or a deferred one.
fn checkpoint_tick(rig: &mut Rig, actor: &mut Actor, now: i64) -> Tick {
    for _ in 0..8 {
        let tick = rig.tick(actor, now);
        if matches!(
            event(&tick),
            Some(Event::Checkpointed { .. } | Event::CheckpointDeferred { .. })
        ) {
            return tick;
        }
        assert!(
            !matches!(
                event(&tick),
                Some(Event::Retrying { .. } | Event::Degraded { .. })
            ),
            "nothing fails: {tick:?}"
        );
    }
    panic!("no checkpoint came due at {now}");
}

fn deferred_until(tick: &Tick) -> i64 {
    match event(tick) {
        Some(Event::CheckpointDeferred { until }) => until.unix_millis(),
        other => panic!("not deferred: {other:?}"),
    }
}

/// A write lock that is busy at every barrier defers the checkpoint for a wait that doubles
/// from ten milliseconds to the retry interval. More refusals in a row than stall a failing
/// task count no failure; the checkpoint runs when the lock is free, and the next refusal
/// starts from the first wait again.
#[test]
fn a_busy_write_lock_defers_the_checkpoint_on_a_doubling_wait_and_never_degrades() {
    let (mut rig, mut actor) = eager();
    rig.commit(2);
    let mut now = T0 + 1;
    let mut waits = Vec::new();
    for _ in 0..crate::fault::STALLED_AFTER.get() + 2 {
        rig.ck.inject(SourceOp::BeginBarrier, SourceError::Busy);
        let tick = checkpoint_tick(&mut rig, &mut actor, now);
        let until = deferred_until(&tick);
        waits.push(until - now);
        let early = rig.tick(&mut actor, until - 1);
        assert!(
            !matches!(
                event(&early),
                Some(Event::Checkpointed { .. } | Event::CheckpointDeferred { .. })
            ),
            "the gate holds until the wait ends: {early:?}"
        );
        now = until;
    }
    assert_eq!(waits, [10, 20, 40, 80, 160, 320, 640, 1000, 1000, 1000]);
    assert_eq!(
        actor.degraded(Task::Checkpoint),
        None,
        "a wait is not a failure"
    );
    let ran = checkpoint_tick(&mut rig, &mut actor, now);
    assert!(
        matches!(
            event(&ran),
            Some(Event::Checkpointed {
                mode: CheckpointMode::Passive,
                busy: false
            })
        ),
        "{ran:?}"
    );
    rig.settle(&mut actor, now);
    rig.commit(3);
    rig.ck.inject(SourceOp::BeginBarrier, SourceError::Busy);
    let again = checkpoint_tick(&mut rig, &mut actor, now + 5);
    assert_eq!(
        deferred_until(&again),
        now + 5 + 10,
        "the count starts over"
    );
}

/// A checkpoint that failed between two refusals also ends the run of refusals.
#[test]
fn a_failed_checkpoint_between_two_refusals_starts_the_wait_over() {
    let (mut rig, mut actor) = eager();
    rig.commit(2);
    rig.ck.inject(SourceOp::BeginBarrier, SourceError::Busy);
    let first = checkpoint_tick(&mut rig, &mut actor, T0 + 1);
    let until = deferred_until(&first);
    rig.ck
        .inject(SourceOp::BeginBarrier, SourceError::Io(ErrorKind::Other));
    let failed = rig.tick(&mut actor, until);
    let retry = match event(&failed) {
        Some(Event::Retrying { until, .. }) => until.unix_millis(),
        other => panic!("the barrier's I/O error is a fault: {other:?}"),
    };
    rig.ck.inject(SourceOp::BeginBarrier, SourceError::Busy);
    let again = checkpoint_tick(&mut rig, &mut actor, retry);
    assert_eq!(deferred_until(&again), retry + 10);
}

/// A writer overtakes the PASSIVE's bump and its commit restarts the WAL the checkpoint
/// copied whole. The checkpoint counts as run, and the next sync follows the restart: no
/// retry, no snapshot.
#[test]
fn a_restart_after_an_overtaken_bump_is_followed_without_a_snapshot() {
    let (mut rig, mut actor) = eager();
    let salts = rig.wal.salts();
    rig.commit(2);
    rig.ck.inject(SourceOp::BumpSeq, SourceError::Busy);
    let ran = checkpoint_tick(&mut rig, &mut actor, T0 + 1);
    assert_eq!(
        event(&ran),
        Some(&Event::Checkpointed {
            mode: CheckpointMode::Passive,
            busy: false
        }),
        "{ran:?}"
    );
    assert_eq!(rig.wal.salts(), salts, "nothing restarted the WAL yet");
    rig.commit(3);
    assert_ne!(rig.wal.salts(), salts, "the writer's commit restarted it");
    let seen = rig.settle(&mut actor, T0 + 2);
    assert!(
        !seen.iter().any(|tick| matches!(
            event(tick),
            Some(Event::Resnapshot(_) | Event::Retrying { .. } | Event::Degraded { .. })
        )),
        "{seen:?}"
    );
    assert_eq!(
        actor.snapshots().get(SnapshotCauseKind::GenerationChanged),
        0
    );
    assert_eq!(actor.staged(), actor.replicated());
}

/// What an overtaken checkpoint reports as busy is SQLite's own word: it did not look at the
/// WAL again.
#[test]
fn an_overtaken_checkpoint_reports_what_sqlite_said_of_busy() {
    let (mut rig, mut actor) = eager();
    rig.commit(2);
    rig.commit(3);
    rig.ck.script(CheckpointScript {
        busy: true,
        effect: CheckpointEffect::Backfill(1),
    });
    rig.ck.inject(SourceOp::BumpSeq, SourceError::Busy);
    let ran = checkpoint_tick(&mut rig, &mut actor, T0 + 1);
    assert_eq!(
        event(&ran),
        Some(&Event::Checkpointed {
            mode: CheckpointMode::Passive,
            busy: true
        }),
        "{ran:?}"
    );
}

/// A TRUNCATE that emptied the WAL and was overtaken at its bump leaves the position pinned:
/// the writer that held the lock starts the new generation, and the next sync follows it with
/// no snapshot.
#[test]
fn a_truncate_overtaken_at_its_bump_follows_the_next_generation() {
    let mut config = cfg();
    config.checkpoint = CheckpointConfig {
        interval: None,
        min_pages: None,
        truncate_pages: Some(nz(1)),
    };
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    rig.commit(2);
    // A reader holds the PASSIVE back, so the TRUNCATE comes due.
    rig.ck.script(CheckpointScript {
        busy: false,
        effect: CheckpointEffect::Backfill(1),
    });
    let passive = checkpoint_tick(&mut rig, &mut actor, T0 + 1);
    assert!(
        matches!(
            event(&passive),
            Some(Event::Checkpointed {
                mode: CheckpointMode::Passive,
                ..
            })
        ),
        "{passive:?}"
    );
    rig.ck.inject(SourceOp::BumpSeq, SourceError::Busy);
    let truncate = checkpoint_tick(&mut rig, &mut actor, T0 + 2);
    assert_eq!(
        event(&truncate),
        Some(&Event::Checkpointed {
            mode: CheckpointMode::Truncate,
            busy: false
        }),
        "{truncate:?}"
    );
    assert_eq!(actor.pending_snapshot(), None);
    let before = actor.snapshots().total();
    // The writer that held the lock commits the new generation's first frames.
    rig.commit(3);
    let seen = rig.settle(&mut actor, T0 + 3);
    assert_eq!(actor.snapshots().total(), before, "{seen:?}");
    assert_eq!(actor.pending_snapshot(), None);
    assert!(
        !seen.iter().any(|tick| matches!(
            event(tick),
            Some(Event::Resnapshot(_) | Event::Retrying { .. })
        )),
        "{seen:?}"
    );
}

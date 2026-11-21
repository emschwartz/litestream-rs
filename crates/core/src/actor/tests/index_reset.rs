//! A wal-index reset ahead of the WAL header (#26): SQLite restarted the log in
//! the index, and the header write that would follow (a truncate, a header
//! write, a recovery that read none) has not happened. The actor waits for the
//! next commit, drains, and runs no checkpoint on a proof the reset never gave.

use super::*;

/// The index after a restart in the index only: the next generation, no frame.
fn reset(wal: &FakeWal) -> CommittedRead {
    let salts = wal.salts();
    let next = Salts {
        salt1: salts.salt1.wrapping_add(1),
        salt2: 77,
    };
    CommittedRead::new(lsr_wal::CommittedEnd::new(0, next, PS), 0).unwrap()
}

/// Checkpoints that a reset would make due were it to give a proof.
fn eager() -> ActorConfig {
    ActorConfig {
        checkpoint: CheckpointConfig {
            interval: Some(Duration::from_secs(1)),
            min_pages: NonZeroU32::new(1),
            truncate_pages: None,
        },
        ..cfg()
    }
}

fn caught_up_eager() -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(eager());
    rig.follow(&mut actor, T0, 50);
    (rig, actor)
}

fn retried_wal_moved(seen: &[Tick]) -> bool {
    seen.iter().any(|tick| {
        matches!(
            event(tick),
            Some(Event::Retrying {
                fault: Fault::WalMoved,
                ..
            })
        )
    })
}

#[test]
fn an_idle_database_under_a_reset_index_settles_with_no_checkpoint() {
    let (mut rig, mut actor) = caught_up_eager();
    rig.wal.script_committed(Some(reset(&rig.wal)));
    let checkpoints = rig.ck.calls().len();
    let seen = rig.follow(&mut actor, T0 + 60_000, 50);
    assert!(!retried_wal_moved(&seen), "{seen:?}");
    assert_eq!(rig.ck.calls().len(), checkpoints, "{seen:?}");
    let last = seen.last().unwrap();
    assert_eq!(next_of(last), Some(Next::OnWalChange), "{seen:?}");
    // Polled again with nothing new, the actor does nothing.
    let (staging, remote, _) = rig.io();
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 70_000)),
        Some(&Event::Idle)
    );
    assert_eq!(rig.io().0, staging);
    assert_eq!(rig.io().1, remote);
}

#[test]
fn a_drain_under_a_reset_index_finishes() {
    let (mut rig, mut actor) = caught_up();
    rig.wal.script_committed(Some(reset(&rig.wal)));
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = rig.follow(&mut actor, T0 + 10, 20);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
}

#[test]
fn a_commit_after_the_position_under_a_reset_index_is_a_snapshot() {
    let (mut rig, mut actor) = caught_up();
    // Committed and backfilled before the reset: only the database file holds it now.
    rig.commit(2);
    rig.wal.script_committed(Some(reset(&rig.wal)));
    let seen = rig.settle(&mut actor, T0 + 10);
    assert!(
        seen.iter()
            .any(|tick| event(tick) == Some(&Event::Resnapshot(SnapshotCause::GenerationChanged))),
        "{seen:?}"
    );
}

/// The reset gives no pin: a position the checkpoint protocol left unpinned
/// stays unpinned, so the header the next commit writes is an unproven restart.
#[test]
fn the_next_commit_after_a_reset_with_the_read_transaction_retaken_is_a_snapshot() {
    let (mut rig, mut actor) = caught_up();
    rig.wal.set_epoch(rig.wal.epoch().next());
    rig.wal.script_committed(Some(reset(&rig.wal)));
    rig.follow(&mut actor, T0 + 10, 20);
    rig.wal.script_committed(None);
    rig.wal.force_restart();
    rig.commit(2);
    let seen = rig.settle(&mut actor, T0 + 20);
    assert!(
        seen.iter()
            .any(|tick| event(tick) == Some(&Event::Resnapshot(SnapshotCause::GenerationChanged))),
        "{seen:?}"
    );
}

/// Pinned through the reset by the same read transaction, the position proves the
/// restart the next commit makes, as it did before the reset.
#[test]
fn the_next_commit_after_a_reset_under_the_same_read_transaction_continues() {
    let (mut rig, mut actor) = caught_up();
    rig.wal.script_committed(Some(reset(&rig.wal)));
    rig.follow(&mut actor, T0 + 10, 20);
    rig.wal.script_committed(None);
    rig.wal.force_restart();
    rig.commit(2);
    let seen = rig.settle(&mut actor, T0 + 20);
    assert!(
        !seen
            .iter()
            .any(|tick| matches!(event(tick), Some(Event::Resnapshot(_)))),
        "{seen:?}"
    );
    assert!(
        seen.iter()
            .any(|tick| event(tick) == Some(&Event::Synced { files: 1 })),
        "{seen:?}"
    );
}

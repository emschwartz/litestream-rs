//! The snapshot a drain ends with on request (`actor::once`, Go's `-once -force-snapshot`).

use super::*;
use crate::actor::{FinalSnapshot, FinalSnapshotFailure};

fn drain_with_snapshot(rig: &mut Rig, actor: &mut Actor, at: i64) -> Vec<Tick> {
    actor.request_final_snapshot();
    actor.begin_shutdown(actor.shutdown_deadline(ts(at)));
    rig.follow(actor, at, 40)
}

fn snapshots(rig: &Rig) -> Vec<FileInfo> {
    Rig::files(&rig.remote, Level::SNAPSHOT)
}

/// The snapshot is written after the last upload: it covers the commit the drain began with,
/// and the drain then ends.
#[test]
fn the_snapshot_follows_the_last_upload_and_ends_the_drain() {
    let (mut rig, mut actor) = caught_up();
    let before = snapshots(&rig);
    rig.commit(2);
    let seen = drain_with_snapshot(&mut rig, &mut actor, T0 + 10);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { txid: Some(_) })),
        "{seen:?}"
    );
    let newest = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max())
        .max()
        .unwrap();
    let written: Vec<FileInfo> = snapshots(&rig)
        .into_iter()
        .filter(|f| !before.iter().any(|b| b.key == f.key))
        .collect();
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!(
        written[0].key.range(),
        TxidRange::new(Txid::MIN, newest).unwrap()
    );
    assert_eq!(actor.final_snapshot(), FinalSnapshot::Written(written[0]));
    let upload = seen
        .iter()
        .position(|t| matches!(event(t), Some(Event::Uploaded(Did::Put(info))) if info.key.range().max() == newest));
    let snapshot = seen.iter().position(|t| {
        matches!(
            event(t),
            Some(Event::Maintained {
                event: MaintainedEvent::Snapshot(_),
                ..
            })
        )
    });
    assert!(
        matches!((upload, snapshot), (Some(u), Some(s)) if u < s),
        "the snapshot must follow the last upload: {seen:?}"
    );
}

/// A drain not asked for one writes none.
#[test]
fn a_plain_drain_writes_no_snapshot() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let before = snapshots(&rig);
    rig.follow(&mut actor, T0 + 10, 40);
    assert_eq!(snapshots(&rig), before);
    assert_eq!(actor.final_snapshot(), FinalSnapshot::NotAsked);
}

/// A second run at a position the newest snapshot already holds writes it again, as Go's
/// write does, where the planner would call the snapshot idle.
#[test]
fn a_second_run_at_the_same_position_writes_the_snapshot_again() {
    let (mut rig, mut actor) = caught_up();
    drain_with_snapshot(&mut rig, &mut actor, T0 + 10);
    let first = snapshots(&rig);
    let mut again = Actor::new(cfg());
    rig.settle(&mut again, T0 + 20);
    let seen = drain_with_snapshot(&mut rig, &mut again, T0 + 20);
    assert!(
        matches!(again.final_snapshot(), FinalSnapshot::Written(info) if first.iter().any(|f| f.key == info.key)),
        "{seen:?}"
    );
    assert_eq!(snapshots(&rig).len(), first.len(), "replaced, not added");
}

/// The drain's deadline does not cut the snapshot once everything is uploaded.
#[test]
fn a_passed_deadline_does_not_cut_the_snapshot() {
    let (mut rig, mut actor) = caught_up();
    actor.request_final_snapshot();
    actor.begin_shutdown(ts(T0 + 10));
    rig.follow(&mut actor, T0 + 60_000, 40);
    assert!(matches!(actor.final_snapshot(), FinalSnapshot::Written(_)));
}

/// A write that fails is not retried: the drain ends and says why.
#[test]
fn a_failed_write_ends_the_drain_without_a_snapshot() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    rig.settle(&mut actor, T0 + 5);
    let before = snapshots(&rig);
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let seen = drain_with_snapshot(&mut rig, &mut actor, T0 + 10);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert!(
        matches!(
            actor.final_snapshot(),
            FinalSnapshot::Failed(FinalSnapshotFailure::Fault(_))
        ),
        "{:?}",
        actor.final_snapshot()
    );
    assert_eq!(snapshots(&rig), before);
}

/// An idle database whose wal-index was reset proves nothing until a commit that never comes:
/// the snapshot fails by name instead of waiting.
#[test]
fn a_reset_wal_index_fails_the_snapshot_by_name() {
    let (mut rig, mut actor) = caught_up();
    let salts = rig.wal.salts();
    let next = Salts {
        salt1: salts.salt1.wrapping_add(1),
        salt2: 77,
    };
    let reset =
        CommittedRead::new(lsr_wal::CommittedEnd::new(0, next, PS), 0, rig.wal.epoch()).unwrap();
    rig.wal.script_committed(Some(reset));
    rig.follow(&mut actor, T0 + 60_000, 20);
    let seen = drain_with_snapshot(&mut rig, &mut actor, T0 + 60_010);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert!(
        matches!(
            actor.final_snapshot(),
            FinalSnapshot::Failed(FinalSnapshotFailure::NoProof { reset: Some(_) })
        ),
        "{:?}",
        actor.final_snapshot()
    );
}

/// The request outlives a rebuild of the engine mid-drain.
#[test]
fn the_request_survives_a_rebuild() {
    let (mut rig, mut actor) = caught_up();
    actor.request_final_snapshot();
    let mut rebuilt = actor.rebuilt();
    assert_eq!(rebuilt.final_snapshot(), FinalSnapshot::Asked { tries: 0 });
    rig.settle(&mut rebuilt, T0 + 10);
    rebuilt.begin_shutdown(rebuilt.shutdown_deadline(ts(T0 + 10)));
    rig.follow(&mut rebuilt, T0 + 10, 40);
    assert!(matches!(
        rebuilt.final_snapshot(),
        FinalSnapshot::Written(_)
    ));
}

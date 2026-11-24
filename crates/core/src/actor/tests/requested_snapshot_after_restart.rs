//! An operator's snapshot request does not outlive the WAL it was made against.

use super::*;

/// A request keeps the page table, which describes the database only while the WAL still
/// follows the engine's position. When the WAL restarted before the request ran, recovery
/// decides first and its verdict replaces the request with a cause that clears the table: a
/// user who asks for a snapshot across a restart must get one cut from the database as it is
/// now, not frames from the old generation. Run as the request alone, the step would stage
/// from the old position and never name the restart.
#[test]
fn a_requested_snapshot_over_a_restarted_wal_is_replaced_by_the_restart() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    actor.request_snapshot();
    rig.wal.force_restart();
    rig.wal.set_epoch(rig.wal.epoch().next());
    rig.commit(3);
    let tick = rig.tick(&mut actor, T0 + 2);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Resnapshot(SnapshotCause::GenerationChanged))
        ),
        "{tick:?}"
    );
}

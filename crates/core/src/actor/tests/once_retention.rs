//! The retention pass a drain ends with on request (`actor::once_retention`, Go's `-once
//! -enforce-retention`).

use super::*;
use crate::actor::FinalRetention;
use crate::maintain::{RetentionHold, RetentionMode};

const HOUR: i64 = 60 * 60 * 1000;

/// Snapshots every two hours, kept three: with snapshots at `T0`, `+2h`, `+4h` and `+6h`, a
/// pass measured from the newest keeps the last two by an hour's margin either way.
fn schedule(retention: RetentionMode) -> Maintenance {
    Maintenance::new(
        vec![
            Duration::from_secs(30),
            Duration::from_secs(5 * 60),
            Duration::from_secs(60 * 60),
        ],
        Duration::from_secs(2 * 60 * 60),
        Duration::from_secs(3 * 60 * 60),
        crate::maintain::L0Retention::Keep,
    )
    .unwrap()
    .with_retention(retention)
}

fn config(retention: RetentionMode) -> ActorConfig {
    ActorConfig {
        maintenance: schedule(retention),
        ..cfg()
    }
}

/// A replica holding four snapshots two hours apart, the last at `T0 + 6h`, written with
/// retention off, and a fresh engine with `retention` that has not booted yet.
fn four_snapshots(retention: RetentionMode) -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut writer = Actor::new(config(RetentionMode::Off));
    rig.settle(&mut writer, T0);
    for (n, at) in [(2, T0 + 2 * HOUR), (3, T0 + 4 * HOUR), (4, T0 + 6 * HOUR)] {
        rig.commit(n);
        rig.follow(&mut writer, at, 200);
    }
    (rig, Actor::new(config(retention)))
}

fn snapshot_ends(rig: &Rig) -> Vec<u64> {
    Rig::files(&rig.remote, Level::SNAPSHOT)
        .iter()
        .map(|f| f.key.range().max().get())
        .collect()
}

fn drain_with_retention(rig: &mut Rig, actor: &mut Actor, at: i64) -> Vec<Tick> {
    actor.request_final_retention();
    actor.begin_shutdown(actor.shutdown_deadline(ts(at)));
    rig.follow(actor, at, 200)
}

#[test]
fn the_fixture_holds_four_snapshots() {
    let (rig, _) = four_snapshots(RetentionMode::Enforce);
    assert_eq!(snapshot_ends(&rig).len(), 4, "{:?}", snapshot_ends(&rig));
}

/// Snapshots older than the retention are deleted, the last kept, and the drain ends.
#[test]
fn the_pass_deletes_aged_snapshots_and_ends_the_drain() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let before = snapshot_ends(&rig);
    let seen = drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    let after = snapshot_ends(&rig);
    assert_eq!(after, before[2..].to_vec(), "{before:?} -> {after:?}");
    assert!(
        matches!(actor.final_retention(), FinalRetention::Done { deleted } if deleted >= 1),
        "{:?}",
        actor.final_retention()
    );
}

/// The departure from Go: on an idle replica ages run from its newest created-at time, not
/// the clock, so a day on the pass still keeps the snapshot two hours older than the newest,
/// which Go, measuring from now, deletes.
#[test]
fn an_idle_replica_keeps_snapshots_go_would_delete() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let before = snapshot_ends(&rig);
    drain_with_retention(&mut rig, &mut actor, T0 + 30 * HOUR);
    assert_eq!(snapshot_ends(&rig), before[2..].to_vec());
    assert!(matches!(
        actor.final_retention(),
        FinalRetention::Done { .. }
    ));
}

/// A plain drain deletes nothing.
#[test]
fn a_plain_drain_deletes_nothing() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let before = snapshot_ends(&rig);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 6 * HOUR + 60_000)));
    rig.follow(&mut actor, T0 + 6 * HOUR + 60_000, 200);
    assert_eq!(snapshot_ends(&rig), before);
    assert_eq!(actor.final_retention(), FinalRetention::NotAsked);
}

/// With retention switched off nothing is deleted, as Go deletes nothing remote then.
#[test]
fn retention_switched_off_deletes_nothing() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Off);
    let before = snapshot_ends(&rig);
    let newest = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    let reads = rig.remote.reads_of(&newest.key);
    drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert_eq!(snapshot_ends(&rig), before);
    assert_eq!(
        rig.remote.reads_of(&newest.key),
        reads,
        "nothing is read back for no delete"
    );
    assert_eq!(actor.final_retention(), FinalRetention::Off);
}

/// A listing the trust gates refuse is a hold: nothing is deleted, and the pass says why.
#[test]
fn a_hold_deletes_nothing_and_names_itself() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    rig.remote
        .inner
        .plant_unrecognized(Level::SNAPSHOT, "not-an-ltx-name");
    let before = snapshot_ends(&rig);
    drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert_eq!(snapshot_ends(&rig), before);
    assert!(
        matches!(
            actor.final_retention(),
            FinalRetention::Held {
                deleted: 0,
                hold: RetentionHold::UnrecognizedNames { .. }
            }
        ),
        "{:?}",
        actor.final_retention()
    );
}

/// A delete that fails is not retried: the pass ends and says why.
#[test]
fn a_failed_delete_ends_the_pass() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    rig.remote
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    let seen = drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert!(
        matches!(
            actor.final_retention(),
            FinalRetention::Failed { deleted: 0, .. }
        ),
        "{:?}",
        actor.final_retention()
    );
}

/// The pass is not cut by the drain's deadline once everything is uploaded: the engine boots
/// inside the grace, and the pass runs after it.
#[test]
fn a_passed_deadline_does_not_cut_the_pass() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let before = snapshot_ends(&rig);
    let at = T0 + 6 * HOUR + 60_000;
    actor.request_final_retention();
    actor.begin_shutdown(actor.shutdown_deadline(ts(at)));
    let boot = rig.tick(&mut actor, at);
    assert!(
        matches!(event(&boot), Some(Event::Booted { .. })),
        "{boot:?}"
    );
    rig.follow(&mut actor, at + 60_000, 200);
    assert!(
        matches!(actor.final_retention(), FinalRetention::Done { .. }),
        "{:?}",
        actor.final_retention()
    );
    assert_eq!(snapshot_ends(&rig), before[2..].to_vec());
}

/// The request outlives a rebuild of the engine.
#[test]
fn the_request_survives_a_rebuild() {
    let (_rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    actor.request_final_retention();
    assert!(matches!(
        actor.rebuilt().final_retention(),
        FinalRetention::Asked { deleted: 0, .. }
    ));
}

/// A listing that keeps showing a key after its delete would plan the same delete for ever:
/// the pass deletes it three times in a row and then stops, named. The count of deleted keys
/// pins the number of tries: one try fewer deletes one key fewer.
#[test]
fn a_listing_that_keeps_a_deleted_key_stops_the_pass() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let oldest = Rig::files(&rig.remote, Level::SNAPSHOT)[0];
    rig.remote.hidden.lock().unwrap().push(oldest.key);
    rig.remote.ghosts.lock().unwrap().push(oldest);
    let seen = drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert!(
        matches!(actor.final_retention(), FinalRetention::Stuck { key, deleted: 4 } if key == oldest.key),
        "{:?}",
        actor.final_retention()
    );
}

/// With both asked, a snapshot that is not written skips the retention pass, as Go returns
/// before retention when its snapshot fails: nothing is deleted.
#[test]
fn a_snapshot_not_written_skips_the_pass() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let before = snapshot_ends(&rig);
    let at = T0 + 6 * HOUR + 60_000;
    actor.request_final_snapshot();
    actor.request_final_retention();
    actor.begin_shutdown(actor.shutdown_deadline(ts(at)));
    let boot = rig.tick(&mut actor, at);
    assert!(
        matches!(event(&boot), Some(Event::Booted { .. })),
        "{boot:?}"
    );
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let seen = rig.follow(&mut actor, at, 200);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert!(
        matches!(
            actor.final_snapshot(),
            crate::actor::FinalSnapshot::Failed(_)
        ),
        "{:?}",
        actor.final_snapshot()
    );
    assert_eq!(actor.final_retention(), FinalRetention::Skipped);
    assert_eq!(snapshot_ends(&rig), before);
}

/// With both asked and the snapshot written, the pass runs after it.
#[test]
fn a_written_snapshot_is_followed_by_the_pass() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    actor.request_final_snapshot();
    actor.request_final_retention();
    drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert!(matches!(
        actor.final_snapshot(),
        crate::actor::FinalSnapshot::Written(_)
    ));
    assert!(
        matches!(actor.final_retention(), FinalRetention::Done { deleted } if deleted >= 1),
        "{:?}",
        actor.final_retention()
    );
}

/// A drain runs no maintenance, so the read-back a boot owes (`maintain::audit`) is still owed
/// when the pass starts, and the pass deletes the older snapshots on the strength of the
/// newest one. It reads the newest files back first: one that does not verify holds the pass,
/// and nothing is deleted.
#[test]
fn a_rotten_newest_snapshot_holds_the_pass_before_it_deletes_anything() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let before = snapshot_ends(&rig);
    let newest = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    super::audit::rot_at(&rig, &newest, ts(T0 + 6 * HOUR));
    let seen = drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert_eq!(snapshot_ends(&rig), before);
    assert!(
        matches!(
            actor.final_retention(),
            FinalRetention::Failed {
                deleted: 0,
                fault: Fault::RemoteCorrupt
            }
        ),
        "{:?}",
        actor.final_retention()
    );
}

/// The read-back runs once for the pass, not once per delete job, and a sound replica is
/// pruned as before.
#[test]
fn the_pass_reads_the_newest_files_back_once() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let newest = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    let before = rig.remote.reads_of(&newest.key);
    drain_with_retention(&mut rig, &mut actor, T0 + 6 * HOUR + 60_000);
    assert!(matches!(
        actor.final_retention(),
        FinalRetention::Done { deleted } if deleted >= 2
    ));
    assert_eq!(rig.remote.reads_of(&newest.key), before + 1);
    assert!(!actor.engine.maintain.read_back_owed);
}

/// A read-back that fails to read ends the pass as a failure and deletes nothing: the newest
/// file is not confirmed.
#[test]
fn a_failed_read_back_ends_the_pass_and_deletes_nothing() {
    let (mut rig, mut actor) = four_snapshots(RetentionMode::Enforce);
    let before = snapshot_ends(&rig);
    let at = T0 + 6 * HOUR + 60_000;
    actor.request_final_retention();
    actor.begin_shutdown(actor.shutdown_deadline(ts(at)));
    let boot = rig.tick(&mut actor, at);
    assert!(
        matches!(event(&boot), Some(Event::Booted { .. })),
        "{boot:?}"
    );
    rig.remote
        .inner
        .inject_failure(StoreOp::Read, transient(StoreOp::Read));
    rig.follow(&mut actor, at, 200);
    assert_eq!(snapshot_ends(&rig), before);
    assert!(
        matches!(
            actor.final_retention(),
            FinalRetention::Failed {
                deleted: 0,
                fault: Fault::StoreTransient
            }
        ),
        "{:?}",
        actor.final_retention()
    );
}

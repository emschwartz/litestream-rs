//! The boot gate (`crate::takeover`): replica files above where staging
//! stood must be vouched for by the live WAL, or covered by a takeover
//! record, before a boot builds on them; otherwise it stops.

use super::*;
use crate::TakeoverRecord;

fn go_like_actor() -> Actor {
    Actor::new(go_like())
}

/// A replica caught up through transaction 3 by an engine whose staging
/// still holds 3; returns the rig.
fn three() -> Rig {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    rig
}

fn tx(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn booted(tick: &Tick) -> Option<Option<SnapshotCause>> {
    match event(tick) {
        Some(Event::Booted { cause }) => Some(*cause),
        Some(_) | None => None,
    }
}

/// Wiped staging, the same WAL generation: the WAL holds the replica's
/// newest file, so the boot continues from it with no takeover and no
/// snapshot.
#[test]
fn a_wiped_staging_in_the_same_wal_generation_continues() {
    let mut rig = three();
    rig.staging = Counting::default();
    let mut actor = go_like_actor();
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(booted(&boot), Some(None), "{boot:?}");
    assert_eq!(actor.refused_through(), None);
}

/// Staging lost its newest files but the WAL still holds the replica's
/// newest: vouched, so the boot continues from it, where it used to take a
/// `BehindRemote` snapshot.
#[test]
fn staging_behind_a_replica_the_wal_vouches_for_continues() {
    let mut rig = three();
    let keys: Vec<FileKey> = Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .map(|info| info.key)
        .filter(|key| key.range().max() > Txid::MIN)
        .collect();
    assert!(!keys.is_empty());
    block_on(rig.staging.inner.delete(&keys)).unwrap();
    if Rig::files(&rig.staging, Level::ZERO).is_empty() {
        // Prune had left only the newest; keep one older file staged.
        let one = FileKey::new(Level::ZERO, TxidRange::single(Txid::MIN));
        let bytes = block_on(rig.remote.inner.read(&one, 0, ReadLen::CHUNK));
        if let Ok(bytes) = bytes {
            block_on(async {
                let mut up = rig.staging.inner.begin_upload(&one, ts(T0)).await.unwrap();
                up.write(&bytes).await.unwrap();
                up.commit().await.unwrap()
            });
        }
    }
    assert_eq!(staged(&rig), vec![1], "staging is behind, not empty");
    let mut actor = go_like_actor();
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(booted(&boot), Some(None), "{boot:?}");
}

/// A foreign writer put a file above everything while the engine was down
/// (it never saw it, so it has no stop record): nothing vouches for it, so
/// the boot stops and names the replica's newest transaction, and writes
/// nothing.
#[test]
fn a_foreign_file_put_while_the_engine_was_down_stops_the_boot() {
    let mut rig = three();
    super::second_writer::put_foreign(&rig, 4);
    let before = Rig::files(&rig.remote, Level::ZERO).len();
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert_eq!(actor.refused_through(), Some(tx(4)));
    assert_eq!(Rig::files(&rig.remote, Level::ZERO).len(), before);
    assert_eq!(
        actor.stop_record(),
        None,
        "a boot's refusal records nothing"
    );
}

/// A VM restored to an older image: database, WAL and staging all put back,
/// so the replica holds this database's own later files, which the restored
/// WAL no longer holds. Nothing local tells them from a foreign writer's:
/// a stop. A takeover through the replica's newest then lets the boot take
/// the snapshot above them it used to take.
#[test]
fn a_vm_restore_stops_until_a_takeover_covers_the_replica() {
    let mut rig = three();
    let old_staging = Counting::default();
    let one = FileKey::new(Level::ZERO, TxidRange::single(Txid::MIN));
    let bytes = block_on(rig.remote.inner.read(&one, 0, ReadLen::CHUNK)).unwrap();
    block_on(async {
        let mut up = old_staging.inner.begin_upload(&one, ts(T0)).await.unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap()
    });
    rig.staging = old_staging;
    rig.wal.force_restart();
    rig.commit(7);
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    let tip = actor.refused_through().unwrap();
    let mut actor = go_like_actor();
    actor.remember_takeover(TakeoverRecord::through(tip));
    let boot = rig.tick(&mut actor, T0 + 3);
    assert!(
        matches!(
            booted(&boot),
            Some(Some(SnapshotCause::BehindRemote { .. }))
        ),
        "{boot:?}"
    );
}

/// A takeover covers only what the replica held when it was taken: another
/// writer's file above it stops the boot again.
#[test]
fn a_takeover_does_not_cover_a_foreign_file_above_it() {
    let mut rig = three();
    rig.staging = Counting::default();
    rig.wal.force_restart();
    rig.commit(7);
    super::second_writer::put_foreign(&rig, 4);
    let mut actor = go_like_actor();
    actor.remember_takeover(TakeoverRecord::through(tx(3)));
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert_eq!(actor.refused_through(), Some(tx(4)));
}

/// The Go-compatible opt-out checks nothing: the boot snapshots above the
/// foreign file, as it always did.
#[test]
fn continuing_past_another_writer_skips_the_gate() {
    let mut rig = three();
    super::second_writer::put_foreign(&rig, 4);
    let mut config = go_like();
    config.responses.other_writer = crate::OtherWriter::ContinueAndInterleaveHistories;
    let mut actor = Actor::new(config);
    let boot = rig.tick(&mut actor, T0 + 2);
    assert!(
        matches!(
            booted(&boot),
            Some(Some(SnapshotCause::BehindRemote { .. }))
        ),
        "{boot:?}"
    );
}

/// A takeover through the replica's newest overrides a stop record: the
/// operator chose to replicate over those files, and the record goes.
#[test]
fn a_takeover_covering_the_replica_clears_a_stop_record() {
    let mut rig = three();
    super::second_writer::put_foreign(&rig, 4);
    let mut actor = go_like_actor();
    actor.remember_stop(crate::StopRecord::other_writer(Some(tx(3))));
    actor.remember_takeover(TakeoverRecord::through(tx(4)));
    let boot = rig.tick(&mut actor, T0 + 2);
    assert!(
        matches!(
            booted(&boot),
            Some(Some(SnapshotCause::BehindRemote { .. }))
        ),
        "{boot:?}"
    );
    assert_eq!(actor.stop_record(), None);
}

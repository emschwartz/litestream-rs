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
                let mut up = rig
                    .staging
                    .inner
                    .begin_upload(&one, ts(T0), WriteMode::Replace)
                    .await
                    .unwrap();
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
/// (it never saw it): nothing vouches for it, so
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
        let mut up = old_staging
            .inner
            .begin_upload(&one, ts(T0), WriteMode::Replace)
            .await
            .unwrap();
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

/// The engine drops its own staged files the replica holds (the newest
/// turned unreadable), and the process dies before the snapshot that
/// replaces them is staged; meanwhile the WAL restarts, so it no longer
/// holds the replica's newest range. Before deleting anything the engine had
/// the driver keep a takeover through what staging vouched for, so the next
/// boot takes the snapshot instead of refusing its own files.
#[test]
fn a_crash_between_the_engines_own_drop_and_its_snapshot_is_not_a_refusal() {
    let mut rig = three();
    super::tear(&rig, 3, super::flipped);
    let mut actor = go_like_actor();
    // Tick until the boot that drops them, and no further: the process dies
    // there, before the snapshot that replaces them is staged.
    let mut now = T0 + 2;
    let mut seen = Vec::new();
    for _ in 0..20 {
        let tick = rig.tick(&mut actor, now);
        if let Some(Event::Retrying { until, .. }) = event(&tick) {
            now = now.max(until.unix_millis());
        }
        let dropped = matches!(
            booted(&tick),
            Some(Some(SnapshotCause::NewestStagedFileUnreadable))
        );
        seen.push(tick);
        if dropped {
            break;
        }
    }
    assert!(
        matches!(
            seen.last().and_then(booted),
            Some(Some(SnapshotCause::NewestStagedFileUnreadable))
        ),
        "{seen:?}"
    );
    // The rig, as the driver, kept the takeover the engine asked for before
    // the drop.
    assert_eq!(rig.takeover.map(TakeoverRecord::txid), Some(tx(3)));
    assert!(staged(&rig).iter().all(|n| *n < 3), "{:?}", staged(&rig));
    rig.wal.force_restart();
    rig.commit(7);
    let mut next = go_like_actor();
    next.remember_takeover(rig.takeover.unwrap());
    let seen = rig.follow(&mut next, T0 + 3, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(next.refused_through(), None);
}

/// A meta directory that refuses the record: the engine deletes nothing, and
/// the task waits its backoff between asks instead of spinning.
#[test]
fn a_takeover_the_driver_cannot_write_keeps_the_staged_files_and_waits() {
    let mut rig = three();
    super::tear(&rig, 3, super::flipped);
    let before = staged(&rig);
    let mut actor = go_like_actor();
    let mut now = T0 + 2;
    let mut asked = 0;
    let mut waited = false;
    for _ in 0..20 {
        let env = Env {
            wal: &mut rig.wal,
            db: &mut rig.db,
            ck: &mut rig.ck,
            staging: &rig.staging,
            remote: &rig.remote,
            clock: &FakeClock::new(ts(now)),
            journal: None,
        };
        // No driver: nothing is handed back.
        let tick = block_on(actor.tick(env, ts(now)));
        match event(&tick) {
            Some(Event::TakeoverWanted(_)) => asked += 1,
            Some(Event::Retrying {
                fault: Fault::LocalIo,
                until,
            }) => {
                waited = true;
                now = now.max(until.unix_millis());
            }
            Some(Event::Retrying { until, .. }) => now = now.max(until.unix_millis()),
            Some(_) | None => {}
        }
    }
    assert!(asked >= 2, "it asks again after the wait");
    assert!(waited, "an unanswered ask waits its backoff");
    assert_eq!(staged(&rig), before, "nothing is deleted unrecorded");
}

/// Transaction 4's put lands on the replica but comes back with the wrong
/// checksum, and the delete that would clear it fails: the replica holds 4,
/// unconfirmed. Its staged copy then reads back damaged twice in a row
/// (`flipped`), so the running engine keeps a takeover record through 4 and
/// drops it, and a snapshot replaces it. Returns the rig, the actor, and when
/// the snapshot is due.
fn own_upload_unconfirmed_then_dropped() -> (Rig, Actor, i64) {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    rig.commit(4);
    rig.remote
        .inner
        .inject_failure_after_publish(StoreError::Corrupt {
            key: None,
            cause: crate::store::CorruptCause::ChecksumMismatch { part: None },
        });
    rig.remote.inner.inject_failure(
        StoreOp::Delete,
        StoreError::Transient {
            op: StoreOp::Delete,
            kind: ErrorKind::ConnectionReset,
        },
    );
    let mut now = T0 + 2;
    let mut mismatched = false;
    for _ in 0..20 {
        let tick = rig.tick(&mut actor, now);
        if let Some(Event::Retrying { fault, until }) = event(&tick) {
            now = now.max(until.unix_millis());
            if *fault == Fault::UploadChecksumMismatch {
                mismatched = true;
                break;
            }
        }
    }
    assert!(mismatched, "the put of 4 reported another checksum");
    let remote = Rig::files(&rig.remote, Level::ZERO);
    assert!(
        remote.iter().any(|f| f.key.range().max() == tx(4)),
        "{remote:?}"
    );
    super::tear(&rig, 4, super::flipped);
    (rig, actor, now)
}

/// The staging write of the snapshot that replaces the dropped file fails
/// once, so a listing runs before the snapshot is staged. It finds 4 above
/// where staging now stands. That is the engine's own upload, which its
/// record vouched for: the listing confirms it and the snapshot goes above
/// it. The listing used to take it for another writer's file and stop.
#[test]
fn a_listing_after_a_running_drop_takes_the_vouched_upload_as_its_own() {
    let (mut rig, mut actor, now) = own_upload_unconfirmed_then_dropped();
    rig.staging.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        },
    );
    let seen = rig.follow(&mut actor, now, 60);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(rig.takeover.map(TakeoverRecord::txid), Some(tx(4)));
    assert_eq!(super::restored(&rig), 4);
}

/// The listing after the drop fails once, so the snapshot is staged first
/// and takes id 4, which the replica already holds with the engine's own
/// unconfirmed upload. The next listing finds the two differ. Within the ids
/// its own drop vouched for, the replica's copy stands and the staged
/// snapshot goes, as for a staged file that changed. It used to take the
/// replica's copy for another writer's and stop.
#[test]
fn a_snapshot_on_an_id_a_running_drop_vouched_for_gives_way_to_the_replicas_copy() {
    let (mut rig, mut actor, now) = own_upload_unconfirmed_then_dropped();
    rig.remote.inner.inject_failure(
        StoreOp::List,
        StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        },
    );
    let seen = rig.follow(&mut actor, now, 60);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(super::restored(&rig), 4);
}

/// The drop leaves nothing to upload, and the staging write of the
/// snapshot that replaces it fails once, at a time maintenance is due to
/// list the replica. Maintenance runs only while uploads are idle, and it
/// takes a file past everything the engine staged or confirmed for another
/// writer's. The drop therefore marks the uploads to list first: the
/// listing confirms 4 as the engine's own before maintenance looks. Without
/// that, maintenance found 4 and stopped for another writer.
#[test]
fn maintenance_after_a_running_drop_waits_for_the_listing_that_confirms_its_upload() {
    let (mut rig, mut actor, now) = own_upload_unconfirmed_then_dropped();
    rig.staging.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        },
    );
    let seen = rig.follow(&mut actor, super::listing_due(now), 60);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(rig.takeover.map(TakeoverRecord::txid), Some(tx(4)));
    assert_eq!(super::restored(&rig), 4);
}

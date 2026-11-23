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

/// A staging directory put back from a backup taken when it held only
/// transaction 1, with the replica's own copy of 1.
fn staging_holding_only_one(rig: &Rig) -> Counting {
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
    old_staging
}

/// The bucket rolls back: every replica file ending at `newest` is gone.
fn roll_back_to_before(rig: &Rig, newest: Txid) {
    let all = block_on(crate::Inventory::load(&rig.remote.inner)).unwrap();
    let gone: Vec<FileKey> = all
        .files()
        .iter()
        .map(|info| info.key)
        .filter(|key| key.range().max() == newest)
        .collect();
    block_on(rig.remote.inner.delete(&gone)).unwrap();
}

/// The replica holds transactions 1 to 3, but staging is an old backup put
/// back holding only 1, and a takeover record covers the replica, so the
/// boot takes the replica as its base and owes a snapshot above 3. That
/// snapshot cannot be staged yet (staging refuses writes). After
/// maintenance's first listing the bucket rolls back and loses 3. The
/// listing after the regression finds 2: above what staging holds, but a
/// file this process confirmed as its base, so it is the replica's, not
/// another writer's. It used to stop the engine for another writer.
#[test]
fn a_listing_after_a_rollback_takes_the_base_its_boot_confirmed_as_the_replicas() {
    let mut rig = three();
    rig.staging = staging_holding_only_one(&rig);
    rig.wal.force_restart();
    rig.commit(7);
    let mut actor = go_like_actor();
    actor.remember_takeover(TakeoverRecord::through(tx(3)));
    for _ in 0..200 {
        rig.staging.inner.inject_failure(
            StoreOp::Upload,
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: std::io::ErrorKind::ConnectionReset,
            },
        );
    }
    let boot = rig.tick(&mut actor, T0 + 3);
    assert!(
        matches!(
            booted(&boot),
            Some(Some(SnapshotCause::BehindRemote { .. }))
        ),
        "{boot:?}"
    );
    let mut now = T0 + 3;
    let mut listed = false;
    for _ in 0..40 {
        let tick = rig.tick(&mut actor, now);
        if matches!(event(&tick), Some(Event::Maintained { .. })) {
            listed = true;
            break;
        }
        if let Some(Next::At(at)) = next_of(&tick) {
            now = now.max(at.unix_millis());
        }
    }
    assert!(listed, "maintenance listed once before the rollback");
    roll_back_to_before(&rig, tx(3));
    let seen = rig.follow(&mut actor, super::listing_due(now), 80);
    let regressed = seen.iter().any(|tick| {
        matches!(
            event(tick),
            Some(Event::Degraded {
                cause: PersistentCause::RemoteRegressed,
                ..
            })
        )
    });
    assert!(regressed, "{seen:?}");
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
}

/// A redeploy that keeps the database but not the meta directory: staging is empty, there is
/// no journal and no takeover record, and the application's last close checkpointed the WAL
/// into the database file and restarted it, so the WAL cannot vouch for the replica's newest
/// transaction. The replica holds 1 to 3, and the database file holds exactly its state at 3
/// (page 1 written by transaction 3, pages 2 and 3 from the start). Returns that rig.
pub(super) fn redeployed_with_a_new_meta_directory() -> Rig {
    let mut rig = three();
    rig.staging = Counting::default();
    rig.wal.force_restart();
    rig.db.write_page(pg(1), &vec![3; PS.bytes()]);
    rig
}

/// Every page of the local database equals the replica's state at its newest transaction, so
/// the replica holds nothing this database did not write: the boot snapshots above it (the WAL
/// restarted, `GenerationChanged`), as a takeover record through that transaction would, and
/// nothing stops.
#[test]
fn a_new_meta_directory_over_the_replicas_own_state_snapshots_above_it() {
    let mut rig = redeployed_with_a_new_meta_directory();
    let mut actor = go_like_actor();
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(
        booted(&boot),
        Some(Some(SnapshotCause::GenerationChanged)),
        "{boot:?}"
    );
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(super::restored(&rig), 3);
}

/// One page of the local database differs from the replica's state (a database that is not
/// the one the replica holds, or one the application changed before the boot): the boot
/// stops for another writer, as with no check.
#[test]
fn a_new_meta_directory_over_a_different_database_stops() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.db.write_page(pg(2), &vec![9; PS.bytes()]);
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert_eq!(actor.refused_through(), Some(tx(3)));
}

/// The same pages but one more of them: the database grew past the replica's state, so it is
/// not that state, and the boot stops.
#[test]
fn a_new_meta_directory_over_a_longer_database_stops() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.db.set_page_count(4);
    rig.db.write_page(pg(4), &vec![4; PS.bytes()]);
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}

/// The local state is read through the WAL: after the restart the database file still holds
/// page 1 as transaction 1 wrote it, and a frame of the new generation holds page 1 as
/// transaction 3 wrote it. The committed state equals the replica's, so the boot snapshots.
#[test]
fn a_new_meta_directory_reads_the_local_state_through_the_wal() {
    let mut rig = three();
    rig.staging = Counting::default();
    rig.wal.force_restart();
    rig.commit(3);
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(super::restored(&rig), 3);
}

/// Staging holds a backup with transaction 1 only, put back while the database stayed: the meta
/// directory is not new, so the boot does not ask whether the database is the replica's state,
/// and the gate's own rule stops it even though every page is equal.
#[test]
fn an_old_staging_backup_over_the_replicas_own_state_still_stops() {
    let mut rig = redeployed_with_a_new_meta_directory();
    let one = FileKey::new(Level::ZERO, TxidRange::single(Txid::MIN));
    let bytes = block_on(rig.remote.inner.read(&one, 0, ReadLen::CHUNK)).unwrap();
    put_level_zero(&rig.staging.inner, one, &bytes);
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}

/// A takeover record through 2 is held, so the meta directory is not new: its record judges,
/// and transaction 3 above it stops the boot though every page is equal.
#[test]
fn a_held_takeover_short_of_the_replica_still_stops_over_its_own_state() {
    let mut rig = redeployed_with_a_new_meta_directory();
    let mut actor = go_like_actor();
    actor.remember_takeover(TakeoverRecord::through(tx(2)));
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}

/// The application vacuumed to another page size: the database is not the replica's state,
/// and the boot stops for the page size, as it did before the check.
#[test]
fn a_new_meta_directory_over_a_resized_database_stops_for_the_page_size() {
    let mut rig = redeployed_with_a_new_meta_directory();
    super::resize(&mut rig, super::big());
    let mut actor = Actor::new(cfg());
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(seen.last(), Some(&super::page_size_changed()), "{seen:?}");
}

/// A checkpoint by another connection lands while the boot reads the replica: the local reads
/// may mix two states, so the check proves nothing and the boot runs again (`WalMoved`); the
/// next boot finds the database equal and snapshots.
#[test]
fn a_wal_that_moves_under_the_check_retries_the_boot() {
    let mut rig = redeployed_with_a_new_meta_directory();
    *rig.remote.moves_epoch_on_read.lock().unwrap() = Some(rig.wal.clone());
    let mut actor = go_like_actor();
    let boot = rig.tick(&mut actor, T0 + 2);
    let retried = matches!(
        event(&boot),
        Some(Event::Retrying {
            fault: Fault::WalMoved,
            ..
        })
    );
    assert!(retried, "{boot:?}");
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(super::restored(&rig), 3);
}

fn put_level_zero(store: &MemStore, key: FileKey, bytes: &[u8]) {
    block_on(async {
        let mut up = store
            .begin_upload(&key, ts(T0), WriteMode::Replace)
            .await
            .unwrap();
        up.write(bytes).await.unwrap();
        up.commit().await.unwrap()
    });
}

/// The application's last close deleted the WAL: the local state is the database file alone,
/// its length the page count. Equal to the replica's state, the boot snapshots above it.
#[test]
fn a_new_meta_directory_with_no_wal_reads_the_database_file_alone() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.wal.remove_file();
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(super::restored(&rig), 3);
}

/// With no WAL, a database file one page longer than the replica's state is not that state.
#[test]
fn a_new_meta_directory_with_no_wal_over_a_longer_database_stops() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.wal.remove_file();
    rig.db.set_page_count(4);
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}

/// With no WAL, one page of the database file differs: the boot stops.
#[test]
fn a_new_meta_directory_with_no_wal_over_a_different_database_stops() {
    let mut rig = redeployed_with_a_new_meta_directory();
    rig.wal.remove_file();
    rig.db.write_page(pg(3), &vec![7; PS.bytes()]);
    let mut actor = go_like_actor();
    let seen = rig.follow(&mut actor, T0 + 2, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}

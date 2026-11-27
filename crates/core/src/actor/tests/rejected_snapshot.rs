//! A level-9 snapshot whose commit the store rejected after publishing it.

use super::*;
use crate::store::CorruptCause;

fn rejected(key: Option<FileKey>) -> StoreError {
    StoreError::Corrupt {
        key,
        cause: CorruptCause::ChecksumMismatch { part: None },
    }
}

/// Ticks until the top compaction level is published: the next upload to the
/// replica is then the first snapshot.
pub(super) fn before_first_snapshot() -> (Rig, Actor) {
    before_first_snapshot_with(cfg())
}

/// [`before_first_snapshot`] under `config`.
pub(super) fn before_first_snapshot_with(config: ActorConfig) -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    let top = Level::new(cfg().maintenance.top().get()).unwrap();
    // The first snapshot goes before any level, so this state (every level compacted, no
    // snapshot) is the one a first snapshot that failed leaves: refuse the first attempt, which
    // backs the snapshot off while the levels run.
    let mut refused = false;
    for _ in 0..20 {
        if !refused && !Rig::files(&rig.remote, Level::ZERO).is_empty() {
            rig.remote
                .inner
                .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
            refused = true;
        }
        let tick = rig.tick(&mut actor, T0);
        if let Some(Event::Maintained {
            event: MaintainedEvent::Compacted(info),
            ..
        }) = event(&tick)
            && info.key.level() == top
        {
            assert!(Rig::files(&rig.remote, Level::SNAPSHOT).is_empty());
            // The test clock stands still: the backoff would never end.
            actor
                .engine
                .maintain
                .state
                .record_snapshot(Timestamp::from_unix_millis(T0), None);
            return (rig, actor);
        }
    }
    panic!("the top level never compacted");
}

fn is_rejection(tick: &Tick) -> bool {
    matches!(
        event(tick),
        Some(Event::Retrying {
            fault: Fault::UploadChecksumMismatch,
            ..
        })
    )
}

/// A snapshot the store took and then refused against its checksum is not deleted: S3 checks
/// the body before the key, so the key may hold an earlier attempt's sound snapshot
/// (`docs/decisions/D280.md`). The read-back the refusal owes finds this one sound.
#[test]
fn a_refused_snapshot_is_kept_and_read_back() {
    let (mut rig, mut actor) = before_first_snapshot();
    rig.remote
        .inner
        .inject_failure_after_publish(rejected(None));
    let tick = rig.tick(&mut actor, T0);
    assert!(is_rejection(&tick), "{tick:?}");
    let [kept] = Rig::files(&rig.remote, Level::SNAPSHOT)[..] else {
        panic!("the refused snapshot is still listed");
    };
    assert!(
        actor.engine.maintain.read_back_owed,
        "the refusal owes a read-back"
    );
    let reads = rig.remote.reads_of(&kept.key);
    let seen = rig.follow(&mut actor, T0, 200);
    no_stop(&seen);
    assert!(rig.remote.reads_of(&kept.key) > reads, "it was read back");
    assert!(!rig.remote.deleted().contains(&kept.key));
    assert!(!actor.engine.maintain.read_back_owed);
    assert_eq!(restored(&rig), 1);
}

/// The same, for a snapshot read from the database (the first one is built from level 0).
#[test]
fn a_refused_database_snapshot_is_kept_and_owes_a_read_back() {
    let (mut rig, mut actor) = before_first_snapshot();
    actor.engine.maintain.state.refuse_seed();
    rig.remote
        .inner
        .inject_failure_after_publish(rejected(None));
    let reads = rig.db.reads();
    let tick = rig.tick(&mut actor, T0);
    assert!(is_rejection(&tick), "{tick:?}");
    assert!(rig.db.reads() > reads, "it read the database");
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT).len(), 1);
    assert!(actor.engine.maintain.read_back_owed);
}

/// A snapshot the store took damaged and then refused: the read-back finds it rotten,
/// deletes it (the levels below still cover it) and the snapshot is taken again.
#[test]
fn a_damaged_snapshot_the_store_refused_is_read_back_and_taken_again() {
    let (mut rig, mut actor) = before_first_snapshot();
    rig.remote
        .inner
        .inject_failure_after_publish(rejected(None));
    let tick = rig.tick(&mut actor, T0);
    assert!(is_rejection(&tick), "{tick:?}");
    let [bad] = Rig::files(&rig.remote, Level::SNAPSHOT)[..] else {
        panic!("the refused snapshot is still listed");
    };
    super::audit::rot(&rig, &bad);
    let seen = rig.follow(&mut actor, T0, 200);
    no_stop(&seen);
    let repaired = seen.iter().any(|tick| {
        matches!(
            event(tick),
            Some(Event::Maintained {
                event: MaintainedEvent::Repaired(key),
                ..
            }) if *key == bad.key
        )
    });
    assert!(repaired, "{seen:?}");
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT).len(), 1);
    assert_eq!(restored(&rig), 1);
}

/// A snapshot that fails before its commit published nothing, so nothing is
/// deleted: the key may hold an earlier good object.
#[test]
fn a_snapshot_that_fails_before_its_commit_deletes_nothing() {
    let (mut rig, mut actor) = before_first_snapshot();
    let before = rig.remote.deleted();
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let tick = rig.tick(&mut actor, T0);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Retrying {
                fault: Fault::StoreTransient,
                ..
            })
        ),
        "{tick:?}"
    );
    assert_eq!(rig.remote.deleted(), before);
}

/// A snapshot the store took at another size than the one sent is this attempt's own, so it
/// is deleted; when that delete fails, it goes first and holds retention.
#[test]
fn a_short_snapshot_that_cannot_be_deleted_goes_first_and_holds_retention() {
    let (mut rig, mut actor) = before_first_snapshot();
    rig.remote
        .inner
        .inject_failure_after_publish(StoreError::Corrupt {
            key: None,
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 1,
                stored: 0,
                removed: false,
            },
        });
    rig.remote
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    let tick = rig.tick(&mut actor, T0);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Retrying {
                fault: Fault::UploadAmbiguous,
                ..
            })
        ),
        "{tick:?}"
    );
    let listed = Rig::files(&rig.remote, Level::SNAPSHOT);
    let [bad] = listed.as_slice() else {
        panic!("the rejected snapshot is still listed: {listed:?}");
    };
    // A later transaction gives the next snapshot another key, so only a
    // delete removes the rejected one.
    rig.commit(2);
    let seen = rig.follow(&mut actor, T0 + 1, 200);
    no_stop(&seen);
    let held = seen.iter().any(|tick| {
        matches!(
            event(tick),
            Some(Event::Maintained {
                event: MaintainedEvent::Waiting {
                    held: Some(RetentionHold::OutputDistrusted { key }),
                },
                ..
            }) if *key == bad.key
        )
    });
    assert!(held, "retention holds while it is listed: {seen:?}");
    let deleted = seen
        .iter()
        .position(|tick| {
            matches!(
                event(tick),
                Some(Event::Maintained {
                    event: MaintainedEvent::Deleted(1),
                    ..
                })
            )
        })
        .unwrap_or_else(|| panic!("no delete: {seen:?}"));
    let snapshot = seen
        .iter()
        .position(|tick| {
            matches!(
                event(tick),
                Some(Event::Maintained {
                    event: MaintainedEvent::Snapshot(_),
                    ..
                })
            )
        })
        .unwrap_or_else(|| panic!("no snapshot: {seen:?}"));
    assert!(deleted < snapshot, "{seen:?}");
    let after = Rig::files(&rig.remote, Level::SNAPSHOT);
    assert!(!after.contains(bad), "{after:?}");
    assert_eq!(after.len(), 1);
    assert_eq!(restored(&rig), 2);
}

fn created(info: &FileInfo) -> i64 {
    info.created_at().unwrap().at().unix_millis()
}

/// A clock that steps back across a restart, with staging intact: the new engine's clamp
/// starts from the newest staged file's header, so it stamps its next file no earlier than
/// the files the engine before it wrote. Its snapshots do not go backwards either: the next
/// one waits until the clock's snapshot window starts after the newest snapshot's created-at
/// time, and is stamped then, so retention meets no snapshot pair whose created-at times run
/// backwards and holds nothing.
#[test]
fn a_clock_stepped_back_across_a_restart_stamps_nothing_earlier() {
    let day: i64 = 86_400_000;
    let (mut rig, mut actor) = before_first_snapshot();
    rig.follow(&mut actor, T0, 50);
    let [first] = Rig::files(&rig.remote, Level::SNAPSHOT)[..] else {
        panic!("one snapshot");
    };
    let mut actor = Actor::new(cfg());
    let back = T0 - 3_600_000;
    rig.commit(2);
    let seen = rig.follow(&mut actor, back, 50);
    no_stop(&seen);
    let newest_l0 = Rig::files(&rig.remote, Level::ZERO)
        .into_iter()
        .max_by_key(|info| info.key.range().max())
        .unwrap();
    assert_eq!(newest_l0.key.range().max().get(), 2);
    assert!(
        created(&newest_l0) >= created(&first),
        "the new engine stamped transaction 2 before the snapshot of transaction 1"
    );
    assert_eq!(
        Rig::files(&rig.remote, Level::SNAPSHOT),
        vec![first],
        "no snapshot while the clock is behind the newest one"
    );
    let seen = rig.follow(&mut actor, T0 + day + 1, 300);
    no_stop(&seen);
    let held_backwards = seen.iter().any(|tick| {
        matches!(
            event(tick),
            Some(Event::Maintained {
                event: MaintainedEvent::Waiting {
                    held: Some(RetentionHold::SnapshotCreatedAtBackwards { .. }),
                },
                ..
            })
        )
    });
    assert!(!held_backwards, "{seen:?}");
    let [second] = Rig::files(&rig.remote, Level::SNAPSHOT)[..] else {
        panic!("one snapshot after retention");
    };
    assert_eq!(second.key.range().max().get(), 2);
    assert!(created(&second) > created(&first));
}

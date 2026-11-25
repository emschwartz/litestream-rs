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
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    let top = Level::new(cfg().maintenance.top().get()).unwrap();
    for _ in 0..20 {
        let tick = rig.tick(&mut actor, T0);
        if let Some(Event::Maintained {
            event: MaintainedEvent::Compacted(info),
            ..
        }) = event(&tick)
            && info.key.level() == top
        {
            assert!(Rig::files(&rig.remote, Level::SNAPSHOT).is_empty());
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

#[test]
fn a_rejected_snapshot_is_deleted_at_once() {
    let (mut rig, mut actor) = before_first_snapshot();
    rig.remote
        .inner
        .inject_failure_after_publish(rejected(None));
    let tick = rig.tick(&mut actor, T0);
    assert!(is_rejection(&tick), "{tick:?}");
    assert_eq!(
        Rig::files(&rig.remote, Level::SNAPSHOT),
        Vec::new(),
        "the object the store rejected is not left listed"
    );
}

/// The same, for a snapshot read from the database (the first one is built from level 0).
#[test]
fn a_rejected_database_snapshot_is_deleted_at_once() {
    let (mut rig, mut actor) = before_first_snapshot();
    actor.engine.maintain.state.refuse_seed();
    rig.remote
        .inner
        .inject_failure_after_publish(rejected(None));
    let reads = rig.db.reads();
    let tick = rig.tick(&mut actor, T0);
    assert!(is_rejection(&tick), "{tick:?}");
    assert!(rig.db.reads() > reads, "it read the database");
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT), Vec::new());
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

#[test]
fn a_rejected_snapshot_that_cannot_be_deleted_goes_first_and_holds_retention() {
    let (mut rig, mut actor) = before_first_snapshot();
    rig.remote
        .inner
        .inject_failure_after_publish(rejected(None));
    rig.remote
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    let tick = rig.tick(&mut actor, T0);
    assert!(is_rejection(&tick), "{tick:?}");
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

/// The stored created-at time of `info`.
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

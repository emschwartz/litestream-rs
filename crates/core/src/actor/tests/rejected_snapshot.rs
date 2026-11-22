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

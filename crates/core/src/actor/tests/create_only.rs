//! A replica store that cannot make create-only writes (`docs/decisions.md` D79): the first
//! level-0 upload fails with nothing published, the upload task is degraded for the rest of the
//! process, and uploads go on as replacing writes; or the engine stops, under that response.

use super::*;

fn newest_on_replica(rig: &Rig) -> Option<u64> {
    Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .max()
}

fn cause(actor: &Actor) -> Option<PersistentCause> {
    actor.degraded(Task::Upload).map(|degraded| degraded.cause)
}

/// Under the default responses the refusal degrades the upload task, the retry replaces, and
/// the cause stands after that and later successes: every upload from then on is unguarded.
#[test]
fn a_store_without_create_only_writes_degrades_and_uploads_by_replacing() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    assert_eq!(newest_on_replica(&rig), Some(1));
    rig.remote.inner.refuse_create_only();
    rig.commit(2);
    let seen = rig.settle(&mut actor, T0 + 1);
    let raised = seen.iter().position(|tick| {
        matches!(
            event(tick),
            Some(Event::Degraded {
                task: Task::Upload,
                cause: PersistentCause::ConditionalWritesUnsupported,
                ..
            })
        )
    });
    assert!(raised.is_some(), "{seen:?}");
    rig.settle(&mut actor, T0 + 60_000);
    assert_eq!(newest_on_replica(&rig), Some(2), "the retry replaced");
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 120_000);
    assert_eq!(newest_on_replica(&rig), Some(3));
    assert_eq!(
        cause(&actor),
        Some(PersistentCause::ConditionalWritesUnsupported),
        "the cause outlasts the successes"
    );
}

/// Nothing replaces a key before the cause is raised: the refused write published nothing.
#[test]
fn the_refused_create_only_write_publishes_nothing() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    rig.remote.inner.refuse_create_only();
    rig.commit(2);
    let mut seen = Vec::new();
    for _ in 0..10 {
        let tick = rig.tick(&mut actor, T0 + 1);
        let degraded = cause(&actor).is_some();
        seen.push(tick);
        if degraded {
            break;
        }
        assert_eq!(newest_on_replica(&rig), Some(1), "{seen:?}");
    }
    assert_eq!(
        cause(&actor),
        Some(PersistentCause::ConditionalWritesUnsupported),
        "{seen:?}"
    );
    assert_eq!(newest_on_replica(&rig), Some(1));
}

/// Under `STRICT` the engine stops on the first refusal, and nothing more is uploaded.
#[test]
fn a_store_without_create_only_writes_stops_under_strict_responses() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    rig.remote.inner.refuse_create_only();
    rig.commit(2);
    let seen = rig.settle(&mut actor, T0 + 1);
    assert!(
        seen.contains(&Tick::Stopped(StopCause::ConditionalWritesUnsupported)),
        "{seen:?}"
    );
    assert_eq!(newest_on_replica(&rig), Some(1));
}

/// The switch lasts one process: a new one asks the store again, and is told again.
#[test]
fn a_restart_asks_for_create_only_writes_again() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    rig.remote.inner.refuse_create_only();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    rig.settle(&mut actor, T0 + 60_000);
    assert_eq!(newest_on_replica(&rig), Some(2));
    let mut restarted = Actor::new(go_like());
    rig.settle(&mut restarted, T0 + 120_000);
    assert_eq!(cause(&restarted), None, "nothing to upload yet");
    commit_and_settle(&mut rig, &mut restarted, 3, T0 + 120_001);
    assert_eq!(
        cause(&restarted),
        Some(PersistentCause::ConditionalWritesUnsupported)
    );
}

/// Another upload fault takes the task's degraded state for a while; once an upload succeeds
/// again the store's missing conditional writes are reported again, as long as uploads replace
/// (simulator seed 0xf: a denial took the slot and the success cleared it).
#[test]
fn the_cause_comes_back_after_another_fault_on_the_upload_task() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    rig.remote.inner.refuse_create_only();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    rig.settle(&mut actor, T0 + 60_000);
    assert_eq!(newest_on_replica(&rig), Some(2));
    rig.remote.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Denied {
            op: StoreOp::Upload,
            why: crate::store::Denied::Permission,
        },
    );
    rig.commit(3);
    rig.settle(&mut actor, T0 + 60_001);
    assert!(
        matches!(
            cause(&actor),
            Some(PersistentCause::StoreDenied(_) | PersistentCause::ConditionalWritesUnsupported)
        ),
        "{:?}",
        cause(&actor)
    );
    rig.settle(&mut actor, T0 + 180_000);
    assert_eq!(newest_on_replica(&rig), Some(3));
    assert_eq!(
        cause(&actor),
        Some(PersistentCause::ConditionalWritesUnsupported)
    );
}
/// A compaction can be the first create-only write a process makes (a restart with a level
/// due and nothing new to upload). The refusal publishes nothing and is reported; the output
/// is then written as a replace, and so is every upload after it, which the upload task says.
#[test]
fn a_refused_compaction_output_is_written_again_by_replacing() {
    let one = Level::new(1).unwrap();
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    assert_eq!(newest_on_replica(&rig), Some(2));
    let before = Rig::files(&rig.remote, one).len();
    rig.remote.inner.refuse_create_only();
    let mut restarted = Actor::new(go_like());
    let seen = rig.settle(&mut restarted, T0 + 60_000);
    assert!(
        seen.iter().any(|tick| matches!(
            event(tick),
            Some(Event::Degraded {
                task: Task::Maintenance,
                cause: PersistentCause::ConditionalWritesUnsupported,
                ..
            })
        )),
        "{seen:?}"
    );
    assert_eq!(Rig::files(&rig.remote, one).len(), before, "{seen:?}");
    // From the refusal on every level-0 upload replaces, so the upload task says so before
    // the first one does (the simulator's judge of D79).
    assert_eq!(
        cause(&restarted),
        Some(PersistentCause::ConditionalWritesUnsupported),
        "{seen:?}"
    );
    let seen = rig.follow(&mut restarted, T0 + 60_000, 50);
    assert_eq!(Rig::files(&rig.remote, one).len(), before + 1, "{seen:?}");
    commit_and_settle(&mut rig, &mut restarted, 3, T0 + 180_000);
    assert_eq!(newest_on_replica(&rig), Some(3));
    assert_eq!(
        cause(&restarted),
        Some(PersistentCause::ConditionalWritesUnsupported)
    );
}

/// Under the stop response the engine stops on a compaction's refusal too.
#[test]
fn a_refused_compaction_output_stops_under_strict_responses() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    rig.remote.inner.refuse_create_only();
    let mut restarted = Actor::new(cfg());
    let seen = rig.settle(&mut restarted, T0 + 60_000);
    assert!(
        seen.contains(&Tick::Stopped(StopCause::ConditionalWritesUnsupported)),
        "{seen:?}"
    );
}

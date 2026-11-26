//! A whole-database copy is staged beside the database and takes up to the database's size.
//! Go never looks at the free space and keeps going until the disk is full, so the engine must not
//! stop or refuse either: it reports, through a notice and the sync task's health, that the
//! staging disk has less free space than the copy can take, and stages the copy.

use super::*;
use crate::actor::apart::Apart;

/// What a copy of the rig's three pages can take.
fn copy() -> u64 {
    u64::try_from(PS.bytes()).unwrap().checked_mul(3).unwrap()
}

/// One byte less than the copy can take.
fn just_short() -> u64 {
    copy().checked_sub(1).unwrap()
}

/// Ticks at `now` until nothing is ready, saying whether the sync task was ever reported degraded.
fn settle_watching(rig: &mut Rig, actor: &mut Actor, now: i64) -> bool {
    let mut seen = false;
    for _ in 0..100 {
        let tick = rig.tick(actor, now);
        seen |= actor.degraded(Task::Sync).is_some();
        if !matches!(
            tick,
            Tick::Ran {
                next: Next::Now,
                ..
            }
        ) {
            return seen;
        }
    }
    panic!("never settled");
}

/// Ticks at `now` until a sync step has staged a file, before any upload has run.
fn until_staged(rig: &mut Rig, actor: &mut Actor, now: i64) {
    for _ in 0..20 {
        let tick = rig.tick(actor, now);
        if matches!(event(&tick), Some(Event::Synced { .. })) {
            return;
        }
    }
    panic!("nothing was staged");
}

/// A first start that finds less room than the copy can take says so once, and is degraded while
/// the copy is in staging, yet the copy is staged and uploaded as if there were room.
#[test]
fn a_copy_that_may_not_fit_is_reported_and_still_runs() {
    let mut rig = Rig::new();
    rig.staging.inner.set_free_bytes(Some(just_short()));
    let mut actor = Actor::new(go_like());
    let degraded = settle_watching(&mut rig, &mut actor, T0);
    assert_eq!(rig.short, vec![(copy(), just_short())]);
    assert!(degraded, "the sync task was never reported degraded");
    assert_eq!(
        Rig::files(&rig.remote, Level::ZERO)
            .iter()
            .map(|f| f.key.range().max().get())
            .max(),
        Some(1),
        "the copy was uploaded"
    );
    assert_eq!(
        actor.degraded(Task::Sync),
        None,
        "everything is on the replica"
    );
}

/// The report says why: the staging disk is the thing that is too small, and the task is dated
/// from the first copy that did not fit.
#[test]
fn the_report_is_the_local_disk_full_stall_dated_from_the_copy() {
    let mut rig = Rig::new();
    rig.staging.inner.set_free_bytes(Some(0));
    let mut actor = Actor::new(go_like());
    until_staged(&mut rig, &mut actor, T0);
    let Some(degraded) = actor.degraded(Task::Sync) else {
        panic!("not degraded: {:?}", rig.short);
    };
    assert_eq!(
        degraded.cause,
        PersistentCause::Stalled(Stall::LocalDiskFull)
    );
    assert_eq!(degraded.since, ts(T0));
    assert_eq!(degraded.retries, 1);
    assert_eq!(actor.degraded(Task::Upload), None);
}

/// A copy that exactly fits is not reported.
#[test]
fn a_copy_that_fits_exactly_is_not_reported() {
    let mut rig = Rig::new();
    rig.staging.inner.set_free_bytes(Some(copy()));
    let mut actor = Actor::new(go_like());
    let degraded = settle_watching(&mut rig, &mut actor, T0);
    assert!(rig.short.is_empty(), "{:?}", rig.short);
    assert!(!degraded);
}

/// A staging store that cannot say how much room it has is never reported on.
#[test]
fn a_staging_store_that_cannot_say_reports_nothing() {
    let mut rig = Rig::new();
    rig.staging.inner.set_free_bytes(None);
    let mut actor = Actor::new(go_like());
    let degraded = settle_watching(&mut rig, &mut actor, T0);
    assert!(rig.short.is_empty());
    assert!(!degraded);
}

/// Only a whole-database copy is checked: the commits that follow stage a few pages each.
#[test]
fn a_step_that_stages_only_commits_is_not_checked() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    settle_watching(&mut rig, &mut actor, T0);
    rig.staging.inner.set_free_bytes(Some(0));
    rig.commit(2);
    let degraded = settle_watching(&mut rig, &mut actor, T0 + 1);
    assert!(rig.short.is_empty(), "{:?}", rig.short);
    assert!(!degraded);
}

/// A later copy that fits ends the report, even while the earlier one is still in staging.
#[test]
fn a_later_copy_that_fits_ends_the_report() {
    let mut rig = Rig::new();
    rig.staging.inner.set_free_bytes(Some(0));
    let mut actor = Actor::new(go_like());
    until_staged(&mut rig, &mut actor, T0);
    assert!(actor.degraded(Task::Sync).is_some());
    rig.staging.inner.set_free_bytes(Some(copy()));
    actor.request_snapshot();
    rig.commit(2);
    until_staged(&mut rig, &mut actor, T0 + 1);
    assert_eq!(actor.degraded(Task::Sync), None);
}
/// A course's view of the staging store answers for the store under it, whatever it says.
#[test]
fn a_local_course_view_forwards_the_free_space() {
    let store = MemStore::new();
    assert_eq!(Apart::over(&store, None).free_bytes(), None);
    store.set_free_bytes(Some(7));
    assert_eq!(Apart::over(&store, None).free_bytes(), Some(7));
}

//! A retry waits from when the failed call returned, not from when its tick began.

use super::*;

/// The S3 client's deadline: how long a call that never answers holds the tick.
const DEADLINE: Duration = Duration::from_secs(60);

/// One tick on `clock`, which the stores may move on while the tick runs.
fn tick_on(rig: &mut Rig, actor: &mut Actor, clock: &FakeClock) -> Tick {
    let env = Env {
        wal: &mut rig.wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &rig.staging,
        remote: &rig.remote,
        clock,
        notice: &mut |_| {},
    };
    block_on(actor.tick(env, clock.now()))
}

/// An upload hangs until the client's deadline and then fails: the retry waits the task's
/// interval after the failure, as Go's monitors sleep their backoff after the failed call
/// returns (`replica.go` `monitor`). Timed from the tick's start, the wait had passed by the
/// time the call returned, and the next tick sent the same request at once.
#[test]
fn a_retry_waits_from_when_the_failed_call_returned() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    let clock = Arc::new(FakeClock::new(ts(T0 + 1_000)));
    assert_eq!(
        event(&tick_on(&mut rig, &mut actor, &clock)),
        Some(&Event::Synced { files: 1 })
    );
    rig.remote.refuse_uploads.store(true, Ordering::SeqCst);
    *rig.remote.hangs_uploads.lock().unwrap() = Some((Arc::clone(&clock), DEADLINE));
    let failed = tick_on(&mut rig, &mut actor, &clock);
    let returned = clock.now();
    let due = returned.saturating_add(cfg().retry_interval);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { until, .. }) if *until >= due),
        "{failed:?}, returned at {returned:?}"
    );
    *rig.remote.hangs_uploads.lock().unwrap() = None;
    let requests = rig.remote.requests_made();
    let at_once = tick_on(&mut rig, &mut actor, &clock);
    assert_eq!(
        rig.remote.requests_made(),
        requests,
        "retried the moment the call returned: {at_once:?}"
    );
}

/// A wait set by an earlier tick stays where it was when a later tick is slow: only the waits a
/// tick sets itself are timed from its end. Here the upload's retry is due a second after its
/// failure, and the sync that stages the next commit meanwhile takes half a second; the retry
/// still goes at its second.
#[test]
fn a_slow_tick_moves_no_wait_an_earlier_tick_set() {
    let (mut rig, mut actor) = caught_up();
    let clock = Arc::new(FakeClock::new(ts(T0 + 1_000)));
    rig.remote.refuse_uploads.store(true, Ordering::SeqCst);
    *rig.remote.hangs_uploads.lock().unwrap() = Some((Arc::clone(&clock), DEADLINE));
    rig.commit(2);
    tick_on(&mut rig, &mut actor, &clock);
    let failed = tick_on(&mut rig, &mut actor, &clock);
    let Some(&Event::Retrying { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    *rig.remote.hangs_uploads.lock().unwrap() = None;
    rig.remote.refuse_uploads.store(false, Ordering::SeqCst);
    rig.commit(3);
    *rig.staging.hangs_uploads.lock().unwrap() =
        Some((Arc::clone(&clock), Duration::from_millis(500)));
    let synced = tick_on(&mut rig, &mut actor, &clock);
    assert_eq!(
        event(&synced),
        Some(&Event::Synced { files: 1 }),
        "{synced:?}"
    );
    assert!(
        clock.now() < until,
        "the sync ended before the retry was due"
    );
    *rig.staging.hangs_uploads.lock().unwrap() = None;
    clock.set(until);
    let retried = tick_on(&mut rig, &mut actor, &clock);
    assert!(
        matches!(event(&retried), Some(Event::Uploaded(_))),
        "the retry did not go at {until:?}: {retried:?}"
    );
}

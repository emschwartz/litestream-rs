//! A step the host holds the application's writes for waits without failing
//! (`Fault::WritesHeld`, `docs/decisions/D192.md`).

use super::*;

/// Nothing writes before the boot decides (`docs/decisions/D201.md`), so a hold at boot lasts:
/// the step waits one retry interval, and the hold does not judge the boot.
#[test]
fn a_boot_held_by_the_hosts_writes_waits_and_stays_unjudged() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.wal.inject(SourceOp::Len, SourceError::WritesHeld);
    let held = rig.tick(&mut actor, T0);
    let until = ts(T0 + 1_000);
    assert_eq!(event(&held), Some(&Event::WritesHeld { until }), "{held:?}");
    assert_eq!(next_of(&held), Some(Next::At(until)), "{held:?}");
    assert!(!actor.boot_judged(), "a hold judges no boot");
    assert_eq!(actor.trip(), None);
}

/// Once the boot is judged, a hold waits one retry interval, and the held step does not run
/// again before then.
#[test]
fn a_hold_after_the_boot_waits_one_retry_interval() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    assert!(actor.boot_judged());
    rig.commit(2);
    rig.wal.inject(SourceOp::Len, SourceError::WritesHeld);
    let held = rig.tick(&mut actor, T0 + 10);
    let until = ts(T0 + 10 + 1_000);
    assert_eq!(event(&held), Some(&Event::WritesHeld { until }), "{held:?}");
    assert_eq!(next_of(&held), Some(Next::At(until)), "{held:?}");
    let early = rig.tick(&mut actor, T0 + 500);
    assert!(
        !matches!(event(&early), Some(Event::Synced { .. })),
        "{early:?}"
    );
    let released = rig.tick(&mut actor, T0 + 10 + 1_000);
    assert_eq!(
        event(&released),
        Some(&Event::Synced { files: 1 }),
        "{released:?}"
    );
}

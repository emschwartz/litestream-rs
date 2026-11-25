//! A `replicate -once` engine runs none of the maintenance schedule (`Maintenance::one_off`):
//! Go switches its monitors off before it opens the store under `-once`, so its one-off run
//! never compacts on its own, and an engine that did would stamp a fresh file on an idle
//! replica that the retention pass then measures the replica's age from.

use super::*;
use crate::actor::FinalRetention;
use crate::maintain::RetentionMode;

const DAY: i64 = 24 * 60 * 60 * 1000;

/// An idle replica: level-0 files, nothing at level 1.
fn idle(maintenance: Maintenance) -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(ActorConfig {
        maintenance,
        ..cfg()
    });
    rig.settle(&mut actor, T0);
    rig.commit(2);
    rig.settle(&mut actor, T0 + 1);
    (rig, actor)
}

fn maintained(ticks: &[Tick]) -> Vec<&MaintainedEvent> {
    ticks
        .iter()
        .filter_map(|tick| match event(tick) {
            Some(Event::Maintained { event, .. }) => Some(event),
            _ => None,
        })
        .collect()
}

/// The control: the daemon's engine compacts the idle replica's level 0 into an empty level 1
/// at once, which is what made the file the one-off retention pass measured from.
#[test]
fn the_daemon_s_engine_compacts_an_idle_replica() {
    let (mut rig, mut actor) = idle(Maintenance::go_default());
    let seen = rig.follow(&mut actor, T0 + 60_000, 40);
    assert!(
        maintained(&seen)
            .iter()
            .any(|event| matches!(event, MaintainedEvent::Compacted(_))),
        "{seen:?}"
    );
    assert!(!Rig::files(&rig.remote, Level::new(1).unwrap()).is_empty());
}

/// Ticks past every window plan no compaction, no snapshot and no delete, and the engine
/// goes back to waiting for the log rather than for a maintenance wake.
#[test]
fn a_one_off_engine_plans_nothing_however_late_it_ticks() {
    let (mut rig, mut actor) = idle(Maintenance::go_default().one_off());
    let level_zero = Rig::files(&rig.remote, Level::ZERO);
    for at in [T0 + 60_000, T0 + DAY, T0 + 3 * DAY] {
        let seen = rig.follow(&mut actor, at, 40);
        assert_eq!(
            maintained(&seen),
            Vec::<&MaintainedEvent>::new(),
            "{seen:?}"
        );
        assert!(
            matches!(
                seen.last(),
                Some(Tick::Ran {
                    next: Next::OnWalChange,
                    ..
                })
            ),
            "{seen:?}"
        );
    }
    for level in [1, 2, 3] {
        assert!(Rig::files(&rig.remote, Level::new(level).unwrap()).is_empty());
    }
    assert!(Rig::files(&rig.remote, Level::SNAPSHOT).len() <= 1);
    assert_eq!(Rig::files(&rig.remote, Level::ZERO), level_zero);
}

/// What the drain is asked to end with still runs: the retention pass deletes by the same
/// rules, and the engine plans nothing else around it.
#[test]
fn a_one_off_drain_still_ends_with_the_retention_pass() {
    let (mut rig, mut actor) = idle(Maintenance::go_default().one_off());
    let before = Rig::files(&rig.remote, Level::SNAPSHOT);
    rig.follow(&mut actor, T0 + 3 * DAY, 40);
    actor.request_final_retention();
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 3 * DAY)));
    let seen = rig.follow(&mut actor, T0 + 3 * DAY, 40);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert!(
        maintained(&seen)
            .iter()
            .all(|event| matches!(event, MaintainedEvent::Deleted(_))),
        "{seen:?}"
    );
    assert!(
        matches!(actor.final_retention(), FinalRetention::Done { .. }),
        "{:?}",
        actor.final_retention()
    );
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT), before);
    assert_eq!(actor.retention(), RetentionMode::Enforce);
}

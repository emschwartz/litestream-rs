//! The app's behaviours after a restart reach what they are for: a redo that replays more
//! commits than a power loss took, a retry that restamps some of them, and a busy app whose WAL climbs past its pre-crash end. The
//! swarm draws each of them.

use lsr_sim::{AfterRestart, Seed, Weights, run};

/// The first run from seed 1 on whose reach has `key`, under the block law and `behaviour`.
fn first_reaching(behaviour: AfterRestart, key: &str) -> Option<u64> {
    (1..=40).find(|n| {
        let seed = Seed::new(*n);
        let weights = Weights {
            block_crash_law: true,
            after_restart: behaviour,
            ..Weights::default()
        };
        run(seed, 300, &weights).is_ok_and(|report| report.reach.get(key) > 0)
    })
}

#[test]
fn a_redo_replays_past_the_commits_a_power_loss_took() {
    assert!(first_reaching(AfterRestart::Redo, "app.redo_past_lost_end").is_some());
}

#[test]
fn a_retry_restamps_some_of_what_it_replays() {
    assert!(first_reaching(AfterRestart::Retry, "app.retry_restamped").is_some());
}

#[test]
fn a_busy_app_commits_past_its_pre_crash_end() {
    assert!(first_reaching(AfterRestart::Busy, "app.busy_past_end").is_some());
}

#[test]
fn the_swarm_draws_every_behaviour_after_a_restart() {
    let drawn: Vec<_> = (0..300)
        .map(|n| Weights::swarm(Seed::new(n)).after_restart)
        .collect();
    for behaviour in [
        AfterRestart::Nothing,
        AfterRestart::Redo,
        AfterRestart::Retry,
        AfterRestart::Busy,
    ] {
        assert!(drawn.contains(&behaviour), "{behaviour:?}");
    }
    assert_eq!(Weights::default().after_restart, AfterRestart::Nothing);
}

#[test]
fn the_swarm_draws_lying_and_honest_disks() {
    let drawn: Vec<_> = (0..300)
        .map(|n| Weights::swarm(Seed::new(n)).lying_wal_sync)
        .collect();
    assert!(drawn.contains(&true) && drawn.contains(&false));
    assert!(!Weights::default().lying_wal_sync);
}

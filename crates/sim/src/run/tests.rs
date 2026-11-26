use super::*;

fn ops(trace: &[String]) -> Vec<&str> {
    trace
        .iter()
        .map(String::as_str)
        .filter(|l| {
            l.split(':')
                .next()
                .is_some_and(|n| n.parse::<u32>().is_ok())
        })
        .collect()
}

#[test]
fn a_short_run_passes() {
    let report = run(Seed::new(1), 40, &Weights::default()).unwrap_or_else(|f| panic!("{f}"));
    assert_eq!(report.steps, 40);
    assert!(report.ticks > 0);
}

/// A seed replays: two runs give the same report down to every trace line,
/// event, recorder entry (with the clock it read) and reach count. A
/// `HashMap` on a sim path, a clock or a thread would show here first.
#[test]
fn the_same_seed_gives_the_same_full_report() {
    let opts = Opts {
        trace: true,
        record: true,
        ..Opts::default()
    };
    for n in 1..=8 {
        let seed = Seed::new(n);
        let weights = Weights::mixed(seed);
        let first = run_with(seed, 300, &weights, &opts);
        let second = run_with(seed, 300, &weights, &opts);
        match (first, second) {
            (Ok(a), Ok(b)) => {
                assert!(!a.trace.is_empty() && !a.seen.is_empty() && !a.remote_events.is_empty());
                assert_eq!(a, b, "seed {n}");
            }
            (Err(a), Err(b)) => assert_eq!(a, b, "seed {n}"),
            (a, b) => panic!("seed {n} passed once and failed once: {a:?} {b:?}"),
        }
    }
}

#[test]
fn history_is_prefix_stable() {
    let opts = Opts {
        plant: None,
        trace: true,
        ..Opts::default()
    };
    let short =
        run_with(Seed::new(5), 60, &Weights::default(), &opts).unwrap_or_else(|f| panic!("{f}"));
    let long =
        run_with(Seed::new(5), 120, &Weights::default(), &opts).unwrap_or_else(|f| panic!("{f}"));
    let (short, long) = (ops(&short.trace), ops(&long.trace));
    assert_eq!(short.len(), 60);
    assert_eq!(short.as_slice(), long.get(..60).unwrap_or_default());
}

fn planted(step: u32) -> Opts {
    Opts {
        plant: Some((step, bug_fault("planted"))),
        trace: false,
        ..Opts::default()
    }
}

#[test]
fn failure_prints_replay_line() {
    let failure = run_with(Seed::new(2), 200, &Weights::default(), &planted(117)).unwrap_err();
    assert_eq!(failure.step, 117);
    let text = failure.message(118);
    let mut lines = text.lines();
    assert_eq!(
        lines.next(),
        Some("seed 0x0000000000000002 failed at step 117: bug fault: planted")
    );
    assert_eq!(
        lines.next(),
        Some("replay: just sim-replay 0x0000000000000002 118")
    );
}

#[test]
fn idle_resource_property() {
    let report = run(Seed::new(1), 120, &Weights::default()).unwrap_or_else(|f| panic!("{f}"));
    assert!(report.checks > 0);
}

/// A first sync over a replica that already holds files is the engine starting history over
/// with nothing to prove continuity, so it reports a loss; over an empty replica it does not.
#[test]
fn a_first_sync_over_files_reports_a_loss() {
    use lsr_core::SnapshotCause;
    assert!(snapshot_reports_loss(&SnapshotCause::FirstSync, true));
    assert!(!snapshot_reports_loss(&SnapshotCause::FirstSync, false));
    assert!(!snapshot_reports_loss(
        &SnapshotCause::OperatorRequested,
        true
    ));
}

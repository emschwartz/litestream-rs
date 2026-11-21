//! The retention switch over whole simulated histories: with retention off
//! the engine still compacts and snapshots, deletes nothing retention would,
//! and the oracle passes every check; switched on mid-run, deletes resume.

use lsr_core::{Event, FileKey, MaintainedEvent, RetentionMode};
use lsr_sim::run::Seen;
use lsr_sim::{Opts, RunReport, Seed, TapEvent, Weights, run_with};

const STEPS: u32 = 300;
const SEEDS: std::ops::RangeInclusive<u64> = 1..=8;

fn run(seed: u64, opts: &Opts) -> RunReport {
    let opts = Opts {
        record: true,
        ..opts.clone()
    };
    match run_with(Seed::new(seed), STEPS, &Weights::default(), &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    }
}

/// What one run did, summed over seeds.
#[derive(Default, Debug)]
struct Tally {
    compacted: usize,
    snapshots: usize,
    /// Every key the replica was asked to delete. A file the store damaged
    /// as it was published would be removed even with retention off (a
    /// repair, not retention); these seeds damage none.
    deleted: Vec<FileKey>,
}

fn add(sum: &mut Tally, report: &RunReport) {
    for seen in &report.seen {
        if let Seen::Event(Event::Maintained {
            event: MaintainedEvent::Compacted(_),
            ..
        }) = seen
        {
            sum.compacted = sum.compacted.saturating_add(1);
        }
        if let Seen::Snapshot(_) = seen {
            sum.snapshots = sum.snapshots.saturating_add(1);
        }
    }
    for event in &report.remote_events {
        if let TapEvent::Delete { keys } = event {
            sum.deleted.extend(keys);
        }
    }
}

fn over_seeds(opts: &Opts) -> Tally {
    let mut sum = Tally::default();
    for seed in SEEDS {
        add(&mut sum, &run(seed, opts));
    }
    sum
}

/// Eight histories with crashes, faults and clock jumps of a day: off,
/// nothing is deleted, compaction and snapshots still happen, and every
/// oracle check passes. On, the same histories delete, so the check bites.
#[test]
fn with_retention_off_a_long_run_deletes_nothing_and_still_compacts_and_snapshots() {
    let off = over_seeds(&Opts {
        retention: RetentionMode::Off,
        ..Opts::default()
    });
    assert_eq!(off.deleted, Vec::<FileKey>::new(), "{off:?}");
    assert!(off.compacted > 0 && off.snapshots > 0, "{off:?}");
    let on = over_seeds(&Opts::default());
    assert!(!on.deleted.is_empty(), "{on:?}");
}

/// Off for the first half of each history, then switched on in the running
/// engine: deletes resume, and every oracle check still passes.
#[test]
fn switching_retention_on_in_a_running_engine_resumes_deletes() {
    let switched = over_seeds(&Opts {
        retention: RetentionMode::Off,
        switch_retention: Some((STEPS / 2, RetentionMode::Enforce)),
        ..Opts::default()
    });
    assert!(!switched.deleted.is_empty(), "{switched:?}");
    let off = over_seeds(&Opts {
        retention: RetentionMode::Off,
        ..Opts::default()
    });
    assert!(switched.compacted > 0 && off.compacted > 0);
}

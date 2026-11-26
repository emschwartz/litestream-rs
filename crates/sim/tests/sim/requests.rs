//! Request budgets fixed by Go's code (docs/decisions/D241.md): in the same scenario lsr makes
//! no more replica requests than Go does.

use std::collections::BTreeSet;

use lsr_core::Level;
use lsr_sim::run::run_ops;
use lsr_sim::store::RecorderEvent;
use lsr_sim::{Op, Opts, Seed};

/// Commits of one page, two ticks apart.
fn commits(n: usize) -> Vec<Op> {
    (0..n)
        .flat_map(|_| {
            [
                Op::Commit {
                    pages: 1,
                    grow: false,
                },
                Op::Tick,
                Op::Tick,
            ]
        })
        .collect()
}

/// Steady replication with the replica caught up and nothing else due: Go's `Replica.sync`
/// knows its position, so each level-0 file is one PUT (`uploadLTXFile`, one
/// `WriteLTXFile`) and nothing is listed, read or deleted (`replica.go` `syncOnce`; the
/// position is listed once, by `calcPos`, only while it is unknown). The window runs from
/// after the boot and its first uploads to before the first maintenance pass.
#[test]
fn a_steady_sync_puts_each_level_zero_file_once_and_asks_nothing_else() {
    let ops = commits(9);
    let opts = Opts {
        record: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    // Operations 6 to 26: seven commits and their ticks, one checkpoint among them.
    let (from, to) = (5, 26);
    let counts = &report.remote_counts;
    let spent = lsr_sim::resources::delta(counts[from], counts[to]);
    assert_eq!(
        (spent.lists, spent.reads, spent.deletes),
        (0, 0, 0),
        "{spent:?}"
    );
    let puts: Vec<_> = report
        .remote_events
        .iter()
        .filter_map(|event| match event {
            RecorderEvent::Put { key, .. } => Some(*key),
            RecorderEvent::Delete { .. } => None,
        })
        .collect();
    let before = usize::try_from(counts[from].commits).unwrap();
    let window = &puts[before..before + usize::try_from(spent.commits).unwrap()];
    assert!(window.len() >= 7, "{window:?}");
    assert!(
        window.iter().all(|key| key.level() == Level::ZERO),
        "{window:?}"
    );
    let distinct: BTreeSet<_> = window.iter().collect();
    assert_eq!(distinct.len(), window.len(), "a file put twice: {window:?}");
}

//! Two level-1 files in one window, then the level above compacts in the same breath.
//!
//! A level-1 file made at the instant its window opens leaves the window open, so commits
//! that land right after it become more level-1 files in the same window. When that window
//! is also where level 2's opens, the level-2 compaction runs straight after them, from a
//! listing made before any of them existed. An engine that then knew only the newest file of
//! level 1 compacted that one alone into level 2 and left the others behind a hole, which
//! Go's compaction of level 3 refuses to read across (`docs/decisions/D228.md`). The level
//! judge (`crates/sim/src/oracle/levels.rs`) fails the run at that put.

use lsr_sim::run::run_ops;
use lsr_sim::{Op, Opts, RecorderEvent, Seed};

fn commit() -> Op {
    Op::Commit {
        pages: 1,
        grow: false,
    }
}

fn ticks(n: usize) -> impl Iterator<Item = Op> {
    std::iter::repeat_n(Op::Tick, n)
}

#[test]
fn level_one_files_that_share_a_window_all_reach_level_two() {
    let mut ops = vec![commit()];
    // The first sync, the first files of every level, and the wait for the next commit.
    ops.extend(ticks(12));
    // The run starts 100 seconds before a five-minute boundary (Go's default level 2) and
    // the idle engine has reached second 60: the next commit's ticks end at second 100,
    // where the windows of levels 1 and 2 both open.
    ops.push(Op::ClockJump(30_000));
    ops.push(commit());
    ops.extend(ticks(3));
    // Three commits at the boundary, a level-1 compaction between each.
    ops.push(commit());
    ops.push(Op::Tick);
    ops.push(commit());
    ops.extend(ticks(3));
    ops.push(commit());
    ops.extend(ticks(6));
    let opts = Opts {
        record: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{}", failure.violation),
    };
    let put_at = |level: u8| -> Vec<(u64, u64)> {
        report
            .remote_events
            .iter()
            .filter_map(|event| match event {
                RecorderEvent::Put { key, .. } if key.level().get() == level => {
                    Some((key.range().min().get(), key.range().max().get()))
                }
                RecorderEvent::Put { .. } | RecorderEvent::Delete { .. } => None,
            })
            .collect()
    };
    // The scenario is the one described: three level-1 files at the boundary, then one
    // level-2 file over them (what follows is the run's end, a later window).
    let (one, two) = (put_at(1), put_at(2));
    assert!(
        one.starts_with(&[(1, 1), (2, 3), (4, 5), (6, 6)]),
        "{one:?}"
    );
    assert!(two.starts_with(&[(1, 1), (2, 6)]), "{two:?}");
    assert!(report.reach.get("levels.Joins") > 0, "{:?}", report.reach);
}

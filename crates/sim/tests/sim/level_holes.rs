//! Several level-1 files in one window, then the level above compacts in the same breath.
//!
//! A level is compacted once per window, unless more files wait than one compaction takes:
//! a full job leaves the window open, and the jobs that drain the backlog follow at once,
//! each a level-1 file in the same window. When that window is also where level 2's opens,
//! the level-2 compaction runs straight after them, from a listing made before any of them
//! existed. An engine that then knew only the newest file of level 1 left the others out of
//! level 2: behind a hole, which Go's compaction of level 3 refuses to read across
//! (`docs/decisions/D228.md`) and the level judge (`crates/sim/src/oracle/levels.rs`) fails
//! the run for, or, with an older level-1 file still waiting below them, for another window.

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
    // The first sync, the first files of every level, and then an idle five minutes (60 ticks of
    // five seconds): the first window of level 2 opens and closes, so what follows is the
    // engine's steady state, whose maintenance passes list only what each one decides from
    // and plan from a listing carried over from the pass before.
    ops.extend(ticks(72));
    // The run starts 100 seconds before a five-minute boundary (Go's default level 2) and
    // the idle engine has reached second 360: the next commit's ticks end at second 400,
    // where the windows of levels 1 and 2 both open.
    ops.push(Op::ClockJump(30_000));
    ops.push(commit());
    ops.extend(ticks(3));
    // Commits at the boundary: level 1 and level 2 take them, and the engine goes idle.
    ops.push(commit());
    ops.extend(ticks(4));
    ops.push(commit());
    ops.extend(ticks(6));
    // Second 375, 25 seconds before level 2's next window: level 1 is compacted in its own
    // window, and then a writer that never lets the engine idle leaves 73 level-0 files
    // behind it, which one compaction takes.
    ops.push(Op::ClockJump(255_000));
    ops.push(commit());
    ops.extend(ticks(4));
    ops.push(commit());
    ops.extend(ticks(4));
    for _ in 0..70 {
        ops.push(commit());
        ops.push(Op::Tick);
    }
    // The idle engine wakes at second 400, where the windows of levels 1 and 2 both open.
    ops.extend(ticks(14));
    let opts = Opts {
        record: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{}", failure.violation),
    };
    let puts: Vec<(u8, u64, u64)> = report
        .remote_events
        .iter()
        .filter_map(|event| match event {
            RecorderEvent::Put { key, .. } if matches!(key.level().get(), 1 | 2) => Some((
                key.level().get(),
                key.range().min().get(),
                key.range().max().get(),
            )),
            RecorderEvent::Put { .. } | RecorderEvent::Delete { .. } => None,
        })
        .collect();
    // The scenario is the one described. The first files of both levels (they start above the
    // first snapshot, at 2), those of the boundary after the idle five minutes, and the level-1
    // file of second 375; then, at second 400, one level-1 file of every file behind it (73,
    // fewer than one compaction takes, `MAX_INPUTS`) and one more, a level-1 file written after
    // the last compaction, and one level-2 file over everything level 1 holds above it.
    let expected = [
        (1, 2, 2),
        (2, 2, 2),
        (1, 3, 5),
        (2, 3, 5),
        (1, 6, 8),
        (2, 6, 8),
        (1, 9, 81),
        (1, 82, 82),
        (2, 9, 82),
    ];
    assert!(puts.starts_with(&expected), "{puts:?}");
    assert!(report.reach.get("levels.Joins") > 0, "{:?}", report.reach);
}

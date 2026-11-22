//! A fault the blind-plant pilot (issue #127) showed no layer drew: a staged file given one page
//! more than its range wrote (`MetaOp::GrowNewest`). Each test is the smallest history that
//! reaches it, while the engine is down and while it runs, and kills a plant
//! (`docs/planted-bugs.toml`: `witness-extra-page-sim`, `witness-trailer-compare-sim`).

use lsr_sim::generate::MetaOp;
use lsr_sim::run::run_ops;
use lsr_sim::{Op, Opts, Seed};

/// Runs `ops` as a scenario, both judges on: a step with nothing to act on fails the run.
fn run(ops: &[Op]) {
    if let Err(failure) = run_ops(Seed::new(1), ops, &Opts::default()) {
        panic!("{failure}");
    }
}

/// A commit staged from the live WAL and not yet uploaded, `edit` applied to it, then enough
/// ticks to boot, upload and restore. The WAL still holds the commit's frames at the edit, so
/// the boot's witness can prove the file but for the page `edit` adds.
fn staged_then(edit: Op) -> Vec<Op> {
    let mut ops = vec![
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Tick,
        edit,
    ];
    ops.extend(std::iter::repeat_n(Op::Tick, 8));
    ops.push(Op::Restore);
    ops
}

/// The boot's witness counts the pages of a pending staged file: one holding a page its range
/// never wrote, put there while the engine was down, is refused, never uploaded as the
/// transaction.
#[test]
fn a_staged_file_given_a_page_more_while_down_is_never_uploaded() {
    run(&staged_then(Op::StagingDamage(MetaOp::GrowNewest)));
}

/// The same page added under a running engine.
#[test]
fn a_staged_file_given_a_page_more_while_running_is_never_uploaded() {
    run(&staged_then(Op::Meta(MetaOp::GrowNewest)));
}

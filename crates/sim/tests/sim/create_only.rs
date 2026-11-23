//! A replica store without conditional writes (`docs/decisions.md` D79), under the responses
//! that keep trying: uploads go on as replacing writes, and the runner's judge
//! (`crates/sim/src/run/create_only.rs`) requires the standing cause on every one of them.

use lsr_core::Responses;
use lsr_sim::generate::{Op, RemoteOp};
use lsr_sim::run::run_ops;
use lsr_sim::{Opts, Seed};

fn ops() -> Vec<Op> {
    let mut ops = vec![
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Remote(RemoteOp::RefuseCreateOnly),
    ];
    for _ in 0..3 {
        ops.extend([
            Op::Commit {
                pages: 2,
                grow: false,
            },
            Op::Tick,
            Op::Tick,
            Op::ClockJump(5_000),
            Op::Tick,
            Op::Tick,
        ]);
    }
    ops.extend([
        Op::Shutdown,
        Op::Commit {
            pages: 1,
            grow: false,
        },
    ]);
    ops.extend(std::iter::repeat_n(Op::Tick, 6));
    ops
}

/// The refused upload raises the cause, the uploads after it replace with the cause standing,
/// and a restart asks again: every replace the replica took is reported. Some seeds must reach
/// a replace, or the judge judged nothing.
#[test]
fn every_replace_on_a_store_without_create_only_writes_is_reported() {
    let ops = ops();
    let mut replaced = 0;
    for seed in 1..=8 {
        let opts = Opts {
            responses: Responses::GO_LIKE,
            ..Opts::default()
        };
        let report =
            run_ops(Seed::new(seed), &ops, &opts).unwrap_or_else(|failure| panic!("{failure}"));
        replaced += report.reach.get("remote.replaced_unguarded");
    }
    assert!(replaced > 0, "no replace reached the judge");
}

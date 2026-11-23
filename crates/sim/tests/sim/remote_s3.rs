//! The replica on the real S3 store (`lsr_sim::remote`, `docs/decisions/D124.md`): swarm seeds
//! run with the replica on `S3Store` over its in-memory bucket and provider faults drawn, and
//! each must pass wherever the same history passes on `MemStore` (HTTP faults are skipped there,
//! so the two runs draw the same operations). A seed that fails on both is an engine defect
//! judged where `MemStore` runs, and is left out; most seeds must still run.

use lsr_sim::generate::{DenialCode, HttpFault, HttpShape, Op};
use lsr_sim::run::run_ops;
use lsr_sim::{Opts, Seed, Weights, replay_line, run_with};

const STEPS: u32 = 120;
const HTTP_FAULT: u32 = 40;

fn weights(seed: Seed, s3: bool) -> Weights {
    let mut weights = Weights::swarm(seed);
    weights.remote_s3 = s3;
    weights.http_fault = HTTP_FAULT;
    weights
}

#[test]
fn swarm_seeds_pass_on_the_s3_replica_as_on_memstore() {
    let (mut ran, mut s3_runs) = (0, 0_u64);
    for n in 1..=12 {
        let seed = Seed::new(n);
        if run_with(seed, STEPS, &weights(seed, false), &Opts::default()).is_err() {
            continue;
        }
        ran += 1;
        let report = run_with(seed, STEPS, &weights(seed, true), &Opts::default())
            .unwrap_or_else(|failure| panic!("{failure}\n{}", replay_line(seed, None)));
        s3_runs += report.reach.get("remote_s3.runs");
    }
    assert!(ran >= 9, "MemStore passed only {ran} of 12 seeds");
    assert_eq!(s3_runs, ran, "every S3 run put the replica on S3");
}

/// Each refusal the provider answers an upload with, and what the engine must report it as:
/// the operator's fix, never a transient fault to retry.
#[test]
fn a_refused_upload_is_reported_as_what_the_operator_must_fix() {
    for code in [
        DenialCode::KmsDisabled,
        DenialCode::ExpiredToken,
        DenialCode::RequestTimeTooSkewed,
        DenialCode::NoSuchBucket,
    ] {
        let ops = [
            Op::Tick,
            Op::HttpFault(HttpFault::Denied(HttpShape::Put, code)),
            Op::Commit {
                pages: 1,
                grow: false,
            },
            Op::Tick,
            Op::Tick,
            Op::Heal,
            Op::Tick,
            Op::Tick,
        ];
        let opts = Opts {
            remote_s3: true,
            ..Opts::default()
        };
        let report = run_ops(Seed::new(1), &ops, &opts)
            .unwrap_or_else(|failure| panic!("{code:?}: {failure}"));
        assert_eq!(
            report.reach.get("http_fault.denial_reported"),
            1,
            "{code:?}: the refusal was met and judged"
        );
    }
}

//! The replica on the real S3 store (`lsr_sim::remote`, `docs/decisions/D124.md`): swarm seeds
//! run with the replica on `S3Store` over its in-memory bucket and provider faults drawn, and
//! each must pass wherever the same history passes on `MemStore` (HTTP faults are skipped there,
//! so the two runs draw the same operations). A seed that fails on both is an engine defect
//! judged where `MemStore` runs, and is left out; most seeds must still run.

use lsr_sim::generate::{DenialCode, Hold, HttpFault, HttpShape, Op, WalOp};
use lsr_sim::run::run_ops;
use lsr_sim::{ClockLaw, CrashKind};
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
    let (mut ran, mut s3_runs, mut read_back) = (0, 0_u64, 0_u64);
    for n in 1..=12 {
        let seed = Seed::new(n);
        if run_with(seed, STEPS, &weights(seed, false), &Opts::default()).is_err() {
            continue;
        }
        ran += 1;
        let report = run_with(seed, STEPS, &weights(seed, true), &Opts::default())
            .unwrap_or_else(|failure| panic!("{failure}\n{}", replay_line(seed, None)));
        s3_runs += report.reach.get("remote_s3.runs");
        read_back += u64::from(report.reach.get("reread.given") > 0);
    }
    assert!(ran >= 9, "MemStore passed only {ran} of 12 seeds");
    assert_eq!(s3_runs, ran, "every S3 run put the replica on S3");
    // The S3 store reads each staged file back when it sends it, and the judge of second
    // reads (`lsr_sim::oracle`) saw them: most runs upload at least one.
    assert!(
        read_back >= 6,
        "only {read_back} of {ran} runs read a file back"
    );
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

/// A refused HEAD that only a batched created-at read-ahead met, which the engine drops by
/// design, while a task stands degraded on a damaged WAL: the engine is loud about the WAL, and
/// owes no report of a request nothing waited on. The judge must not take the standing
/// degradation for the failure of that request.
#[test]
fn a_dropped_read_ahead_refusal_is_not_owed_a_report_beside_a_standing_wal_fault() {
    let ops = [
        Op::Hold {
            hold: Hold::Wal(WalOp::RegrowSameSalts(11_888_806_653_224_795_474)),
            ticks: 102,
        },
        Op::Crash(CrashKind::Process),
        Op::HttpFault(HttpFault::Denied(
            HttpShape::Head,
            DenialCode::AllAccessDisabled,
        )),
        Op::WalDamage {
            op: WalOp::FlipLastFrameBit,
            ticks: 3,
        },
    ];
    let opts = Opts {
        remote_s3: true,
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: true,
        clock_law: ClockLaw::EachRead,
        overlap: true,
        truncate_pages: std::num::NonZeroU32::new(2),
        ..Opts::default()
    };
    let report = run_ops(Seed::new(2), &ops, &opts).unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(
        report.reach.get("http_fault.denial_read_ahead"),
        1,
        "the refused HEAD was met and judged as a dropped read-ahead"
    );
}

/// The boot's sweep of open uploads is best effort (`docs/decisions/D206.md`): a permission
/// refusal of it is skipped, noted once, and no refusal is reported. The judge expects exactly
/// that, counted as `http_fault.sweep_denied`.
#[test]
fn a_permission_refusal_of_the_boot_sweep_is_skipped_and_reports_no_refusal() {
    let ops = [
        Op::HttpFault(HttpFault::Denied(
            HttpShape::ListUploads,
            DenialCode::AccessDenied,
        )),
        Op::Tick,
        Op::Tick,
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Tick,
        Op::Tick,
    ];
    let opts = Opts {
        remote_s3: true,
        ..Opts::default()
    };
    let report = run_ops(Seed::new(1), &ops, &opts).unwrap_or_else(|failure| panic!("{failure}"));
    assert_eq!(
        report.reach.get("http_fault.sweep_denied"),
        1,
        "the sweep was refused and judged"
    );
    assert_eq!(
        report.reach.get("http_fault.denial_reported"),
        0,
        "no refusal was owed"
    );
}

/// The exemption is by operation and by kind of refusal: the same sweep refused for another
/// reason (the credentials) fails the boot and must be reported, as must a refusal of the
/// replica's own listing, which is not the sweep.
#[test]
fn only_a_permission_refusal_of_the_sweep_is_exempt() {
    for (shape, code) in [
        (HttpShape::ListUploads, DenialCode::InvalidAccessKeyId),
        (HttpShape::ListUploads, DenialCode::NoSuchBucket),
        (HttpShape::List, DenialCode::AccessDenied),
    ] {
        let ops = [
            Op::HttpFault(HttpFault::Denied(shape, code)),
            Op::Tick,
            Op::Tick,
            Op::Heal,
            Op::Tick,
        ];
        let opts = Opts {
            remote_s3: true,
            allow_stop: true,
            ..Opts::default()
        };
        let report = run_ops(Seed::new(1), &ops, &opts)
            .unwrap_or_else(|failure| panic!("{shape:?} {code:?}: {failure}"));
        assert_eq!(
            report.reach.get("http_fault.sweep_denied"),
            0,
            "{shape:?} {code:?}"
        );
        assert_eq!(
            report.reach.get("http_fault.denial_reported"),
            1,
            "{shape:?} {code:?}"
        );
    }
}

/// The simulator's cells are small so that its few-hundred-byte files are read again at all,
/// but not so small that a put costs it many staging requests: over a run, the replica is
/// read again no more than once for each file it takes. With cells of 32 bytes a put made
/// several reads, the staging side of each tick cost several times what it did, and a run
/// could end its drain with files still staged (a nightly seed did).
#[test]
fn a_small_put_on_the_s3_replica_is_read_again_no_more_than_once_a_file() {
    let mut ops = Vec::new();
    for _ in 0..6 {
        ops.push(Op::Commit {
            pages: 3,
            grow: true,
        });
        ops.extend(std::iter::repeat_n(Op::Tick, 4));
    }
    let opts = Opts {
        remote_s3: true,
        record: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    let puts = report
        .remote_events
        .iter()
        .filter(|event| matches!(event, lsr_sim::store::RecorderEvent::Put { .. }))
        .count();
    let reads = report
        .remote_counts
        .last()
        .map_or(0, |counts| counts.rereads_given);
    assert!(puts >= 3, "{puts} puts");
    assert!(reads > 0, "no second read was made");
    assert!(
        reads <= u64::try_from(puts).unwrap(),
        "{reads} second reads for {puts} puts"
    );
}

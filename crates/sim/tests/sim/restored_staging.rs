//! A staging directory put back from an older copy, an operator's act (`docs/decisions/D173.md`):
//! what the boot refuses and what the harness answers as `lsr trust-local` does.

use lsr_sim::generate::{MetaOp, RemoteOp, WalOp};
use lsr_sim::run::run_ops;
use lsr_sim::sqlite::CrashKind;
use lsr_sim::{Op, Opts, Seed};

/// Runs `ops` as a drawn history of `seed` would run them, both judges on.
fn run(seed: u64, block_crash_law: bool, ops: &[Op]) -> lsr_sim::RunReport {
    let opts = Opts {
        drawn: true,
        block_crash_law,
        trace: true,
        ..Opts::default()
    };
    match run_ops(Seed::new(seed), ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    }
}

/// Seed 0xb6b of the unaided campaign, found once the oracle stopped switching its id check off
/// after a power loss: transaction 18 is uploaded and drained (164 bytes), then, with the store's
/// reads lying (`Hold::LyingReads`) and an old staging backup put back, the engine snapshots the
/// database as transaction 18 again and puts the same level-0 key with other bytes (266) holding
/// another commit. The replica's object is overwritten: the class of BOOT-18 and BOOT-19
/// (docs/BEHAVIOR.md). The conditional level-0 write closes it: the bucket refuses the second
/// put into the held key and the read-back reports the divergence. Its history is recorded and
/// the settings its mix drew are written out.
#[test]
fn an_id_the_replica_holds_is_not_taken_again_after_a_lying_read_seed_0xb6b() {
    let opts = lsr_sim::Opts {
        drawn: true,
        after_restart: lsr_sim::AfterRestart::Busy,
        ..lsr_sim::Opts::default()
    };
    let seed = lsr_sim::Seed::new(0xb6b);
    if let Err(failure) = lsr_sim::run::run_ops(seed, crate::histories::seed_0xb6b::OPS, &opts) {
        panic!("{failure}");
    }
}

/// The engine replicates transactions 1 to 3 while a copy of staging is taken after 1. The bucket
/// rolls back and loses 3, and the copy is put back, so staging holds only 1. The engine notices
/// the rollback and lists the replica again, confirming nothing until that listing answers. A
/// commit by another connection, a torn WAL header (so the WAL can no longer vouch for the
/// replica's files) and a process crash follow. The boot finds the replica's 2 above what staging
/// holds and refuses it: staging was put back, so the refusal is by design and the operator runs
/// `lsr trust-local`. The checker used to call staging intact here: it took a staged 1 as at least the nothing
/// the engine had confirmed while its listing was pending, and forgot that staging was put back.
#[test]
fn a_boot_refusal_after_staging_was_put_back_is_answered_by_trust_local_while_a_relist_is_pending()
{
    let commit = |pages| Op::Commit { pages, grow: false };
    let ticks = |n| vec![Op::Tick; n];
    let ops: Vec<Op> = [
        vec![commit(3)],
        ticks(4),
        vec![Op::Meta(MetaOp::Save), commit(2)],
        ticks(4),
        vec![commit(2)],
        ticks(4),
        vec![
            Op::Remote(RemoteOp::RollBack),
            Op::Meta(MetaOp::Restore),
            Op::OtherConnectionCommit { pages: 2 },
            Op::WalDamage {
                op: WalOp::TearHeader,
                ticks: 6,
            },
            Op::Crash(CrashKind::Process),
        ],
        ticks(4),
    ]
    .concat();
    let report = run(0, false, &ops);
    assert!(
        report.reach.get("refusal.trust_local") > 0,
        "the boot never refused"
    );
}

/// Staging emptied by an operation after the engine had already staged a snapshot of its own:
/// the refusal of the replica's files through the engine's own first file has a cause, though
/// staging holds a newer file than the replica's newest by the time of the refusal (seed
/// 0x6b065161, reduced). The harness answers as `lsr trust-local` does.
#[test]
fn staging_emptied_by_an_operation_is_a_cause_for_a_refusal_after_a_newer_snapshot() {
    use lsr_core::{SourceOp, StoreError, StoreOp};
    use lsr_sim::generate::{AppAt, AppOp, Target};
    use lsr_sim::{FaultShape, OtherProcessMode};
    let ops = [
        Op::Shrink { pages: 3 },
        Op::AppInTick {
            at: AppAt::Before(SourceOp::PageCount),
            op: AppOp::Commit {
                pages: 2,
                grow: false,
            },
        },
        Op::ClockJump(72986),
        Op::AppInTick {
            at: AppAt::Before(SourceOp::EndBarrier),
            op: AppOp::Commit {
                pages: 2,
                grow: true,
            },
        },
        Op::Meta(MetaOp::FlipNewestByte),
        Op::AppInTick {
            at: AppAt::Call(17),
            op: AppOp::Commit {
                pages: 2,
                grow: false,
            },
        },
        Op::AppInTick {
            at: AppAt::Call(1),
            op: AppOp::Checkpoint(OtherProcessMode::Truncate),
        },
        Op::StoreFault {
            target: Target::Staging,
            op: StoreOp::List,
            ahead: 2,
            shape: FaultShape::Error(StoreError::Transient {
                op: StoreOp::List,
                kind: std::io::ErrorKind::ConnectionReset,
            }),
        },
        Op::AppInTick {
            at: AppAt::Call(10),
            op: AppOp::Checkpoint(OtherProcessMode::Passive),
        },
        Op::AppInTick {
            at: AppAt::Call(1),
            op: AppOp::ReaderBegin,
        },
        Op::Shrink { pages: 2 },
        Op::Commit {
            pages: 3,
            grow: true,
        },
        Op::Meta(MetaOp::DeleteAll),
        Op::SecondWriter { ticks: 4 },
    ];
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: true,
        after_restart: lsr_sim::AfterRestart::Retry,
        overlap: true,
        truncate_pages: std::num::NonZeroU32::new(6),
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x6b06_5161), &ops, &opts) {
        panic!("{failure}");
    }
}

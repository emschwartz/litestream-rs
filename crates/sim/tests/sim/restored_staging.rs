//! A staging directory put back from an older copy after the bucket rolled back.
//!
//! Three swarm seeds (0x484, 0x15c, 0x385), reported as "latest went back" on the rebased
//! bucket-authority tree, shrunk by removing operations while the history judge still failed.
//! None needs a lagging listing: each needs `Remote(RollBack)` and a staging copy saved before it
//! and put back, or deleted, after it. Before the witness (b812e0f, "upload a staged file only
//! with a witness bound to its bytes"), the boot took the restored staged file's bytes for the
//! newest transaction and uploaded them as a new one, so `Latest` restored an older database:
//! all three fail on 6f5940a and pass from b812e0f.

use lsr_sim::generate::{MetaOp, RemoteOp, WalOp};
use lsr_sim::run::run_ops;
use lsr_sim::sqlite::CrashKind;
use lsr_sim::{Op, Opts, OtherProcessMode, Seed};

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

fn seed_0x484() -> Vec<Op> {
    use MetaOp::{Restore, Save, TruncateNewest};
    vec![
        Op::Shutdown,
        Op::StagingDamage(TruncateNewest),
        Op::Shutdown,
        Op::Tick,
        Op::Remote(RemoteOp::RollBack),
        Op::Meta(Save),
        Op::Shutdown,
        Op::Remote(RemoteOp::RollBack),
        Op::Spill,
        Op::Commit {
            pages: 4,
            grow: true,
        },
        Op::Tick,
        Op::Tick,
        Op::Shutdown,
        Op::StagingDamage(Restore),
    ]
}

/// Seed 0x484 shrunk from 300 operations: was "latest went back from node 2 to node 0".
#[test]
fn a_staging_copy_put_back_after_two_rollbacks_never_restores_an_older_latest_seed_0x484() {
    run(0x484, false, &seed_0x484());
}

/// Seed 0x15c shrunk from 110 operations: another writer's files, a rollback and a vacuum, then
/// staging put back from before them; was "latest went back from node 2 to node 0".
#[test]
fn a_staging_copy_from_before_a_vacuum_never_restores_an_older_latest_seed_0x15c() {
    use MetaOp::{Restore, Save, StripNewestOrigin};
    run(
        0x15c,
        true,
        &[
            Op::Shutdown,
            Op::SecondWriter { ticks: 4 },
            Op::Meta(Save),
            Op::Remote(RemoteOp::RollBack),
            Op::Vacuum { page_size: None },
            Op::Shutdown,
            Op::StagingDamage(Restore),
            Op::StagingDamage(StripNewestOrigin),
        ],
    );
}

/// Seed 0x385 shrunk from 300 operations: staging deleted, two rollbacks around WAL damage and
/// a truncating checkpoint, then the old copy put back; was "latest went back from node 7 to
/// node 0".
#[test]
fn a_staging_copy_put_back_after_a_truncated_wal_never_restores_an_older_latest_seed_0x385() {
    use MetaOp::{DeleteAll, Restore, Save};
    let flip = |ticks| Op::WalDamage {
        op: WalOp::FlipLastFrameBit,
        ticks,
    };
    run(
        0x385,
        true,
        &[
            Op::CommitInTick { pages: 1 },
            Op::Tick,
            Op::RequestSnapshot,
            Op::Tick,
            flip(4),
            Op::Meta(Save),
            Op::Remote(RemoteOp::RollBack),
            Op::Meta(DeleteAll),
            Op::Crash(CrashKind::Process),
            Op::Remote(RemoteOp::RollBack),
            flip(4),
            Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
            flip(3),
            Op::StagingDamage(Restore),
        ],
    );
}

/// Seed 0x484 on main, with the witness: the history judge passes, but transaction 2 is put
/// twice with different contents (4370 bytes, then 164 after the second rollback and a
/// restart). Within one process the in-memory confirmed id makes a rolled-back bucket
/// `RemoteRegressed`; across a restart nothing remembers it, so a rolled-back bucket is just a
/// short one and the freed id is taken again.
#[test]
#[ignore = "needs a confirmed high-water mark that outlives the process: pending the engine-journal \
            decision (beyond-go.md section 1)"]
fn a_rolled_back_bucket_never_gets_a_transaction_id_reused_across_a_restart_seed_0x484() {
    let report = run(0x484, false, &seed_0x484());
    let mut put: Vec<(String, String)> = Vec::new();
    for line in &report.trace {
        let Some(rest) = line.split("Uploaded(Put(FileInfo { key: FileKey(").nth(1) else {
            continue;
        };
        let key = rest.split(')').next().unwrap_or_default().to_owned();
        let size = rest
            .split("size: ")
            .nth(1)
            .and_then(|s| s.split(',').next())
            .unwrap_or_default()
            .to_owned();
        if let Some((_, was)) = put.iter().find(|(k, _)| *k == key) {
            assert_eq!(*was, size, "{key} put twice with different contents");
        }
        put.push((key, size));
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
/// holds and refuses it: staging was put back, so the refusal is by design and the operator takes
/// over. The checker used to call staging intact here: it took a staged 1 as at least the nothing
/// the engine had confirmed while its listing was pending, and forgot that staging was put back.
#[test]
fn a_boot_refusal_after_staging_was_put_back_is_taken_over_while_a_relist_is_pending() {
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
        report.reach.get("refusal.take_over") > 0,
        "the boot never refused"
    );
}

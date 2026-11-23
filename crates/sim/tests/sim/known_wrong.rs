//! The open engine defects the error domain reaches (`lsr_sim::domain::KnownWrong`), each pinned
//! to a seed's recorded history (`tests/sim/histories/`, drawn under the seed's mix with the
//! known-wrong cells drawn) that must still fail with the defect's signature. The per-push fixed
//! seeds leave those cells out; the nightly draws them and counts their failures as known.
//!
//! When a defect's fix lands, its test here fails, on purpose: flip the defect's cells in
//! `crates/sim/src/domain.rs` from `Cell::KnownWrong` to `Cell::Drawn`, remove the variant, and
//! move the seed to a test that requires the run to pass. A defect that stays known-wrong after
//! its fix would hide its regressions from the per-push seeds.

use lsr_sim::domain::{KnownWrong, known_in};
use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, ClockLaw, Op, Opts, Seed, Weights};

/// Runs `seed`'s recorded `ops` under `opts` and requires them to fail with `defect`'s
/// signature.
fn still_fails(seed: u64, ops: &[Op], opts: &Opts, defect: KnownWrong) {
    let seed = Seed::new(seed);
    // What `known_in` reads of a mix, from the settings the mix drew.
    let weights = Weights {
        overlap: opts.overlap,
        known_wrong: opts.known_wrong,
        ..Weights::default()
    };
    assert!(
        known_in(ops, &weights).contains(&defect),
        "{seed} draws no cell of {defect:?}"
    );
    match run_ops(seed, ops, opts) {
        Err(failure) if defect.matches(&failure.violation) => {}
        Err(failure) => panic!("{seed} fails another way: {failure}"),
        Ok(_) => panic!(
            "{seed} passes: {defect:?} looks fixed ({}); flip its cells to Drawn",
            defect.tracking()
        ),
    }
}

/// A failed WAL sync, another process's checkpoint of the frames it covered, then a power loss:
/// SQLite recovers a database file no commit made (D103). The engine's boot check now finds it
/// and raises its verdict; the simulator does not yet adopt the mixed file, so the run still fails.
/// Written by hand without a bucket rollback: seed 0x585's history, which first reached it, drifted
/// off it once a rollback could spend a takeover record.
#[test]
fn a_checkpoint_of_unproven_frames_then_a_power_loss_leaves_a_database_ahead_of_its_wal() {
    let opts = Opts {
        drawn: false,
        known_wrong: true,
        ..Opts::default()
    };
    still_fails(
        0x585,
        crate::histories::mix_after_failed_sync::OPS,
        &opts,
        KnownWrong::DatabaseAheadOfWal,
    );
}

/// Fixed: a failed WAL sync was remembered in memory only, so a restart before the TRUNCATE
/// staged frames the failed sync covered, until the engine journal recorded the debt. Seed 0x21
/// of the mixed set with the known-wrong cells drawn (its first 130 operations) reaches it on
/// the tree that stops at another writer, where seed 0xf, whose history began with second
/// writers, no longer did; it must pass now.
#[test]
fn a_restart_after_a_failed_wal_sync_owes_the_truncate_seed_0x21() {
    let opts = Opts {
        drawn: true,
        block_crash_law: false,
        lying_wal_sync: false,
        after_restart: AfterRestart::Retry,
        clock_law: ClockLaw::PerTick,
        error_domain: true,
        known_wrong: true,
        staging_fs: false,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x21), crate::histories::seed_0x21::OPS, &opts) {
        panic!("{failure}");
    }
}

/// Fixed: a staging read refused for permission during level-1 compaction degraded maintenance
/// as the replica's `StoreDenied(Permission)` until #145 routed compaction's local reads through
/// `Fault::from_staging`. Seed 0x68f of the fixed set's mix (its first 39 operations) reaches it
/// on the tree that stops at another writer, where seed 0xb9e no longer did; it must pass now.
#[test]
fn compaction_takes_a_staging_denial_as_staging_seed_0x68f() {
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: false,
        after_restart: AfterRestart::Redo,
        clock_law: ClockLaw::EachRead,
        error_domain: true,
        staging_fs: false,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x68f), crate::histories::seed_0x68f::OPS, &opts) {
        panic!("{failure}");
    }
}

/// A halt that outlasts restarts inside an overlapping hold is never reported degraded: each
/// boot starts its retry count again (D9).
#[test]
fn a_halt_across_restarts_still_goes_unreported_seed_0xe9() {
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: false,
        after_restart: AfterRestart::Nothing,
        clock_law: ClockLaw::PerTick,
        overlap: true,
        error_domain: false,
        known_wrong: true,
        staging_fs: false,
        ..Opts::default()
    };
    still_fails(
        0xe9,
        crate::histories::seed_0xe9::OPS,
        &opts,
        KnownWrong::HaltForgottenAtRestart,
    );
}

/// Fixed: bucket rollbacks leave a hole below the engine's newest upload, staging is wiped, and
/// lagging listings hide what changed. The boot's read past its listing used to stop in the hole
/// (at most three missing transactions), stage transactions the replica already held above it,
/// and leave a latest state that went back to an older one with no stop (BOOT-18). With nothing
/// staged the walk now runs to its cap, so this must pass.
///
/// Shrunk by hand from the survival campaign's seed 0x95a (83 operations) to these 22, each
/// needed to reach the defect: the replica takes transactions 1, 4 and 5 under lagging listings;
/// five rollbacks take it back to 1; the engine, not noticing (D17), uploads 6; staging is wiped
/// and a boot under a lagging listing stages 4 and 5 again.
#[test]
fn a_hole_wider_than_the_probe_below_a_hidden_upload_is_walked_past() {
    use lsr_core::{SourceError, SourceOp};
    use lsr_sim::EnvFault;
    use lsr_sim::generate::{Hold, RemoteOp};
    let commit = |pages| Op::Commit { pages, grow: false };
    let rollback = || Op::Remote(RemoteOp::RollBack);
    let ops = [
        Op::LaggingListings {
            writes: 2,
            window: 40,
        },
        commit(2),
        Op::EnvFault(EnvFault::Source(SourceOp::Checkpoint, SourceError::Busy)),
        Op::Shutdown,
        Op::CommitInTick { pages: 4 },
        Op::CommitInTick { pages: 2 },
        commit(4),
        Op::CommitInTick { pages: 2 },
        Op::Shutdown,
        Op::LaggingListings {
            writes: 5,
            window: 24,
        },
        Op::Shutdown,
        rollback(),
        Op::Tick,
        rollback(),
        Op::OtherProcessReader { begin: true },
        rollback(),
        rollback(),
        rollback(),
        Op::Shutdown,
        Op::EnvFault(EnvFault::WipeStaging),
        Op::CommitInTick { pages: 1 },
        Op::Hold {
            hold: Hold::LaggingListing,
            ticks: 8,
        },
    ];
    let opts = Opts {
        drawn: true,
        after_restart: AfterRestart::Retry,
        clock_law: ClockLaw::PerTick,
        error_domain: true,
        known_wrong: true,
        staging_fs: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x95a), &ops, &opts) {
        panic!("{failure}");
    }
}

//! The fixed seed set: 64 histories of 300 operations, the whole thing inside
//! ten seconds. A failure prints its replay line (`just sim-replay`).

#[cfg(test)]
mod fixed {
    use std::time::{Duration, Instant};

    use lsr_sim::run::run_ops;
    use lsr_sim::{Generator, Op, Opts, Seed, Weights, frozen_history, run};

    const STEPS: u32 = 300;
    const BUDGET: Duration = Duration::from_secs(10);

    #[expect(
        clippy::disallowed_methods,
        reason = "reports how long a batch of seeds takes; the runs themselves use simulated time"
    )]
    fn batch(first: u64) {
        let started = Instant::now();
        for n in (first..).take(8) {
            let outcome = run(Seed::new(n), STEPS, &Weights::default());
            if let Err(failure) = outcome {
                panic!("{failure}");
            }
        }
        assert!(started.elapsed() < BUDGET, "simulator budget exceeded");
    }

    #[test]
    fn seeds_1_to_8() {
        batch(1);
    }

    #[test]
    fn seeds_9_to_16() {
        batch(9);
    }

    #[test]
    fn seeds_17_to_24() {
        batch(17);
    }

    #[test]
    fn seeds_25_to_32() {
        batch(25);
    }

    #[test]
    fn seeds_33_to_40() {
        batch(33);
    }

    #[test]
    fn seeds_41_to_48() {
        batch(41);
    }

    #[test]
    fn seeds_49_to_56() {
        batch(49);
    }

    #[test]
    fn seeds_57_to_64() {
        batch(57);
    }

    /// A regression: the history `seed` drew in `steps` operations under
    /// the frozen generator it was found with, so a new draw in the live
    /// generator never reshapes it.
    fn replay(seed: u64, steps: u32) {
        let seed = Seed::new(seed);
        let ops = frozen_history(seed, steps, Generator::V1);
        if let Err(failure) = run_ops(seed, &ops, &Opts::default()) {
            panic!("{failure}");
        }
    }

    /// Every regression above, with a digest of its operations.
    const REGRESSIONS: [(u64, u32, u64); 6] = [
        (0x25, 44, 0xcc32_7036_14fd_e9c0),
        (0x2d, 256, 0x09b3_c7db_4d3c_63f9),
        (0x46, 252, 0x7d0a_3cb2_c84c_6298),
        (0x0000_126d_68ef_1c07, 52, 0x44cb_38c0_6961_9272),
        (0x0000_126d_68ed_250e, 215, 0xf04f_3f7b_7f9b_6425),
        (0x0000_126d_68ef_545f, 176, 0xb236_1b18_218a_c8df),
    ];

    /// FNV-1a over the operations' debug text: stable across builds, and any
    /// change to an operation or its fields changes it.
    fn digest(ops: &[Op]) -> u64 {
        format!("{ops:?}")
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
            })
    }

    /// A frozen generator that drew something else would let every
    /// regression above pass without reaching its bug.
    #[test]
    fn regression_histories_never_change() {
        for (seed, steps, want) in REGRESSIONS {
            let ops = frozen_history(Seed::new(seed), steps, Generator::V1);
            assert_eq!(digest(&ops), want, "seed {seed:#x} at {steps} steps");
        }
    }

    // Finding 2: a power loss tore a staged level-0 file whose commit never
    // returned. Boot snapshotted past it and left it in staging, and the upload
    // task stopped on it. The torn file, and all staged after it, is now lost.

    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x25() {
        replay(0x25, 44);
    }

    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x2d() {
        replay(0x2d, 256);
    }

    /// Reached through `LocalRewound`: the torn file was an upload whose commit
    /// errored after publishing, torn by an earlier power loss.
    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x46() {
        replay(0x46, 252);
    }

    // Issue #39: a snapshot staged for some id never reached the replica (a
    // power loss tore it, or the staging directory was wiped), and the engine
    // took the same id again from a later commit against an empty WAL (after a
    // VACUUM, or a rewind to the database file). The replica held only the
    // second snapshot, but the oracle paired the id with the first note's
    // commit. The restore was right; the oracle now takes the newest note.

    #[test]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ef1c07() {
        replay(0x0000_126d_68ef_1c07, 52);
    }

    #[test]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ed250e() {
        replay(0x0000_126d_68ed_250e, 215);
    }

    #[test]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ef545f() {
        replay(0x0000_126d_68ef_545f, 176);
    }

    /// The smallest history behind the three seeds above, shrunk from
    /// 0x126d68ed250e: the first boot stages a snapshot as transaction 1, the
    /// database is vacuumed into another page size, the staging directory is
    /// wiped, and the next boot (into a still empty replica) snapshots the
    /// vacuumed database as transaction 1 again. Only that file is uploaded.
    #[test]
    fn a_snapshot_id_taken_again_after_a_lost_staged_snapshot() {
        use lsr_ltx::PageSize;
        use lsr_sim::{CrashKind, EnvFault};
        let ops = [
            Op::Crash(CrashKind::Power),
            Op::Tick,
            Op::Tick,
            Op::Vacuum {
                page_size: PageSize::new(2048),
            },
            Op::EnvFault(EnvFault::WipeStaging),
            Op::Crash(CrashKind::Power),
        ];
        let seed = Seed::new(0x0000_126d_68ed_250e);
        if let Err(failure) = run_ops(seed, &ops, &Opts::default()) {
            panic!("{}", failure.violation);
        }
    }
}

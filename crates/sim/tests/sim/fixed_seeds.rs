//! The fixed seed set: 64 histories of 300 operations, the whole thing inside
//! ten seconds, half under the default mix and half under a swarm mix of
//! their own (`Weights::mixed`). A failure prints its replay line (`just
//! sim-replay`).

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
            let seed = Seed::new(n);
            let outcome = run(seed, STEPS, &Weights::mixed(seed));
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

#[cfg(test)]
mod lapse {
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, OtherProcessMode, Seed};

    /// The engine's read transaction lapses; another process truncates the
    /// WAL and the app commits, so the next sync snapshots. Then the app
    /// commits again and another process restarts the WAL before the engine
    /// reads that commit. The simulated source answered committed reads
    /// under the old hold without retaking the read transaction, so the
    /// engine took the restart as harmless and the replica lost the commit.
    #[test]
    fn a_restart_after_a_lapsed_read_lock_loses_no_commit() {
        let ops = [
            Op::Tick,
            Op::Tick,
            Op::Tick,
            Op::Tick,
            Op::OurReader { begin: false },
            Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
            Op::Commit {
                pages: 1,
                grow: false,
            },
            Op::Tick,
            Op::Tick,
            Op::Commit {
                pages: 1,
                grow: false,
            },
            Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
            Op::Commit {
                pages: 2,
                grow: true,
            },
        ];
        if let Err(failure) = run_ops(Seed::new(0x3a), &ops, &Opts::default()) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod page_size {
    use lsr_ltx::PageSize;
    use lsr_sim::generate::MetaOp;
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, Seed};

    /// A snapshot is staged at 512-byte pages and never uploaded; the
    /// database is vacuumed into 2048-byte pages, and while the engine is
    /// down the staged file loses its WAL origin. The boot found the newest
    /// staged file unusable as a position and, with an empty replica, had no
    /// page size to compare: it staged a 2048-byte snapshot beside the
    /// 512-byte file and uploaded both, so the replica held two page sizes.
    #[test]
    fn an_unusable_staged_file_still_names_its_page_size() {
        let ops = [
            Op::CommitInTick { pages: 4 },
            Op::Tick,
            Op::Vacuum {
                page_size: PageSize::new(2048),
            },
            Op::Meta(MetaOp::StripNewestOrigin),
        ];
        if let Err(failure) = run_ops(Seed::new(0x453), &ops, &Opts::default()) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod requested {
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, OtherProcessMode, Seed};

    /// The engine's read transaction lapses; another process truncates the
    /// WAL, the app commits, and another truncate copies that commit into
    /// the database and resets the WAL again before the engine reads it.
    /// Then the operator asks for a snapshot. The requested snapshot trusted
    /// the engine's table of page checksums, which never saw the commit, so
    /// it read the database page as changed from outside and degraded the
    /// sync task on DatabasePageMismatch for ever.
    #[test]
    fn a_requested_snapshot_after_the_wal_moved_trusts_no_stale_page() {
        let mut ops = vec![
            Op::Tick,
            Op::Tick,
            Op::Tick,
            Op::Tick,
            Op::Tick,
            Op::OurReader { begin: false },
            Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
            Op::Commit {
                pages: 1,
                grow: false,
            },
            Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
            Op::RequestSnapshot,
        ];
        ops.extend(std::iter::repeat_n(Op::Tick, 20));
        if let Err(failure) = run_ops(Seed::new(0x6ae), &ops, &Opts::default()) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod asked_before_boot {
    use lsr_core::{StoreError, StoreOp};
    use lsr_sim::generate::Target;
    use lsr_sim::run::run_ops;
    use lsr_sim::{FaultShape, Op, Opts, OtherProcessMode, Seed};

    /// The operator asks for a snapshot before the first boot, and the boot's
    /// own snapshot fails to stage. The request had replaced the boot's
    /// cause, so the retried snapshot kept the table of page checksums; the
    /// app then rewrote every page, the read lock lapsed and another
    /// process truncated the WAL into the database, and the snapshot read a
    /// page unlike the table and degraded the sync task on
    /// DatabasePageMismatch for ever.
    #[test]
    fn a_request_before_boot_keeps_the_boots_own_snapshot_cause() {
        let transient = StoreError::Transient {
            op: StoreOp::Upload,
            kind: std::io::ErrorKind::ConnectionReset,
        };
        let mut ops = vec![
            Op::StoreFault {
                target: Target::Staging,
                op: StoreOp::Upload,
                ahead: 2,
                shape: FaultShape::Error(transient),
            },
            Op::RequestSnapshot,
            Op::Tick,
            Op::Tick,
            Op::Vacuum { page_size: None },
            Op::OurReader { begin: false },
            Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
            Op::Commit {
                pages: 1,
                grow: false,
            },
        ];
        ops.extend(std::iter::repeat_n(Op::Tick, 30));
        if let Err(failure) = run_ops(Seed::new(0x24), &ops, &Opts::default()) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod diverged {
    use lsr_sim::generate::{MetaOp, RemoteOp, WalOp};
    use lsr_sim::run::run_ops;
    use lsr_sim::{CrashKind, EnvFault, Op, Opts, OtherProcessMode, Seed};

    fn drawn(seed: u64, ops: &[Op]) {
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(seed), ops, &opts) {
            panic!("{}", failure.violation);
        }
    }

    /// A power loss rewinds the WAL after transaction 1 was uploaded and
    /// compacted, and the staged file for 1 loses its WAL origin, so it differs
    /// from the replica's object. The boot listing wrote the staged file over
    /// the replica's level-0 key; the staging directory was then wiped and the
    /// replacement snapshot took transaction 1 again from a later commit, while
    /// level 1 still held the first.
    #[test]
    fn a_verifying_replica_object_is_not_written_over_seed_0xc6b2() {
        let ops = [
            Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
            Op::Tick,
            Op::CommitInTick { pages: 1 },
            Op::Crash(CrashKind::Power),
            Op::Tick,
            Op::Tick,
            Op::Tick,
            Op::StagingDamage(MetaOp::StripNewestOrigin),
            Op::CommitInTick { pages: 1 },
            Op::Meta(MetaOp::DeleteAll),
            Op::Commit {
                pages: 4,
                grow: true,
            },
        ];
        drawn(0xc6b2, &ops);
    }

    /// The bucket rolls back to empty and staging is wiped; the engine takes
    /// transaction 1 again from a later commit and uploads it. Then an old
    /// staging backup is put back, holding the first transaction 1: the boot
    /// listing wrote it over the replica's level-0 key, under a level-1 file
    /// built from the other.
    #[test]
    fn a_restored_staging_backup_is_not_written_over_the_replica_seed_0xd1b() {
        let ops = [
            Op::Shutdown,
            Op::Commit {
                pages: 1,
                grow: false,
            },
            Op::Meta(MetaOp::Save),
            Op::Remote(RemoteOp::RollBack),
            Op::EnvFault(EnvFault::WipeStaging),
            Op::Tick,
            Op::WalDamage {
                op: WalOp::TruncateAtFrame(4),
                ticks: 3,
            },
            Op::StagingDamage(MetaOp::Restore),
        ];
        drawn(0xd1b, &ops);
    }
}

#[cfg(test)]
mod restored_backup {
    use lsr_sim::generate::MetaOp;
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, Seed};

    /// A staging backup is taken holding a snapshot as transaction 1; that
    /// file loses its WAL origin and is deleted, so the engine stages a later
    /// commit as 1; then the backup is put back and uploaded. The replica held
    /// the backup's file, a real state of the database, but the oracle mapped a
    /// file with no WAL origin to the newest note for its id. The engine was
    /// right: a restored backup now notes its files again (seed 0x66c).
    #[test]
    fn a_restored_backup_holds_the_commits_it_copied_seed_0x66c() {
        let ops = [
            Op::Tick,
            Op::Tick,
            Op::StagingDamage(MetaOp::StripNewestOrigin),
            Op::Meta(MetaOp::Save),
            Op::Commit {
                pages: 3,
                grow: false,
            },
            Op::StagingDamage(MetaOp::DeleteNewest),
            Op::Tick,
            Op::CommitInTick { pages: 1 },
            Op::StagingDamage(MetaOp::Restore),
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0x66c), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }

    /// The same mistake, shrunk from seed 5's history on another branch
    /// (restoring transaction 4 differed from commit 6 there).
    #[test]
    fn a_restored_backup_holds_the_commits_it_copied_seed_0x5() {
        let ops = [
            Op::Tick,
            Op::Tick,
            Op::StagingDamage(MetaOp::StripNewestOrigin),
            Op::Commit {
                pages: 4,
                grow: true,
            },
            Op::Meta(MetaOp::Save),
            Op::StagingDamage(MetaOp::DeleteNewest),
            Op::Tick,
            Op::Tick,
            Op::StagingDamage(MetaOp::Restore),
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0x5), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod unlinked {
    use lsr_ltx::PageSize;
    use lsr_sim::generate::{MetaOp, RemoteOp, WalOp};
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, Seed};

    /// A staging backup holding 1024-byte files is put back after the
    /// database was vacuumed to 2048-byte pages and its new history uploaded,
    /// and the bucket rolled back to that history's first file. The boot
    /// listing confirmed it and uploaded the backup's next file after it, so
    /// the replica chained two histories of two page sizes (seed 0x72a,
    /// shrunk).
    #[test]
    fn a_staged_file_from_another_history_is_not_chained_seed_0x72a() {
        let commit = Op::Commit {
            pages: 3,
            grow: false,
        };
        let ops = [
            Op::Vacuum {
                page_size: PageSize::new(1024),
            },
            Op::Shutdown,
            Op::WalDamage {
                op: WalOp::FlipLastFrameBit,
                ticks: 6,
            },
            Op::Remote(RemoteOp::RollBack),
            Op::ClockJump(17060),
            Op::WalDamage {
                op: WalOp::TruncateAtFrame(32),
                ticks: 3,
            },
            Op::Remote(RemoteOp::RollBack),
            Op::Meta(MetaOp::Save),
            Op::Vacuum {
                page_size: PageSize::new(2048),
            },
            Op::WalDamage {
                op: WalOp::TearHeader,
                ticks: 4,
            },
            Op::Shutdown,
            Op::Shutdown,
            commit,
            Op::Shutdown,
            Op::Remote(RemoteOp::RollBack),
            Op::StagingDamage(MetaOp::Restore),
            Op::Vacuum {
                page_size: PageSize::new(1024),
            },
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0x72a), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }

    /// A staging backup holds a file read from the first frame of the WAL.
    /// The WAL is then rewritten under the same salts, and the engine
    /// snapshots it as transaction 1 and uploads it. The backup is put back
    /// and its file for transaction 2, a shorter range of the same WAL, was
    /// uploaded after 1: transaction 2 held an earlier commit than 1 (seed
    /// 0x1919f, shrunk).
    #[test]
    fn a_staged_file_from_an_earlier_state_of_the_wal_is_not_chained_seed_0x1919f() {
        use lsr_sim::generate::WalOp;
        let ops = [
            Op::WalDamage {
                op: WalOp::TruncateAtFrame(4),
                ticks: 2,
            },
            Op::Tick,
            Op::Meta(MetaOp::DeleteAll),
            Op::CommitInTick { pages: 1 },
            Op::Meta(MetaOp::Save),
            Op::Tick,
            Op::WalDamage {
                op: WalOp::RegrowSameSalts(9_383_215_839_333_341_487),
                ticks: 6,
            },
            Op::StagingDamage(MetaOp::Restore),
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0x1919f), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod staged_input {
    use lsr_core::{StoreError, StoreOp};
    use lsr_sim::generate::{MetaOp, WalOp};
    use lsr_sim::run::run_ops;
    use lsr_sim::{FaultShape, Op, Opts, Seed, Target};

    /// A staging backup holding the first snapshot as transaction 1 is put
    /// back after the engine took 1 again from a later commit and uploaded
    /// it. Level 1 was built from the staged copy of level 0 while restores
    /// read the replica's, so transaction 1 had two contents (seed 0xcf8,
    /// shrunk).
    #[test]
    fn level_one_is_not_built_from_a_staged_file_unlike_the_replica_seed_0xcf8() {
        let ops = [
            Op::WalDamage {
                op: WalOp::TearHeader,
                ticks: 1,
            },
            Op::Tick,
            Op::Meta(MetaOp::Save),
            Op::Commit {
                pages: 4,
                grow: false,
            },
            Op::Meta(MetaOp::DeleteAll),
            Op::StoreFault {
                target: Target::Remote,
                op: StoreOp::Upload,
                ahead: 1,
                shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: std::io::ErrorKind::ConnectionReset,
                }),
            },
            Op::Shutdown,
            Op::SecondWriter { ticks: 5 },
            Op::StagingDamage(MetaOp::Restore),
            Op::WalDamage {
                op: WalOp::RegrowWithoutCommitMarks(2_324_847_808_936_637_740),
                ticks: 6,
            },
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0xcf8), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod prefix_page_size {
    use lsr_ltx::PageSize;
    use lsr_sim::generate::{MetaOp, RemoteOp, WalOp};
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, Seed};

    /// A staging backup holds a file at 1024-byte pages. The database moves
    /// to 2048, the bucket rolls back and the replica goes on at 2048 up to
    /// transaction 3. The backup is put back and the database moves to 1024
    /// again: the boot judged the page size by the staged file alone and
    /// snapshotted 1024-byte pages onto the 2048 prefix, so restores stopped
    /// on the change (seed 0xdc8, shrunk).
    #[test]
    fn a_boot_behind_the_replica_keeps_its_page_size_seed_0xdc8() {
        let ops = [
            Op::Vacuum {
                page_size: PageSize::new(1024),
            },
            Op::WalDamage {
                op: WalOp::RegrowWithoutCommitMarks(587_138_737_148_633_869),
                ticks: 6,
            },
            Op::Meta(MetaOp::Save),
            Op::Vacuum {
                page_size: PageSize::new(2048),
            },
            Op::Meta(MetaOp::DeleteAll),
            Op::Remote(RemoteOp::RollBack),
            Op::Shutdown,
            Op::SecondWriter { ticks: 5 },
            Op::StagingDamage(MetaOp::Restore),
            Op::Vacuum {
                page_size: PageSize::new(1024),
            },
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0xdc8), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod budget {
    use lsr_sim::generate::{RemoteOp, WalOp};
    use lsr_sim::run::run_ops;
    use lsr_sim::{EnvFault, Op, Opts, Seed};

    const fn commit(pages: u8, grow: bool) -> Op {
        Op::Commit { pages, grow }
    }

    const fn wal(op: WalOp, ticks: u8) -> Op {
        Op::WalDamage { op, ticks }
    }

    /// Level 1 merges level-0 files 2 to 5, and a later bucket rollback
    /// takes 2 to 4. The budget left the taken files out of level 0 but kept
    /// the level-1 file built from them, so it charged level 1 for bytes it
    /// had read (seed 0x54e, shrunk).
    #[test]
    fn a_level_built_from_files_a_rollback_took_is_within_budget_seed_0x54e() {
        let back = Op::Remote(RemoteOp::RollBack);
        let ops = [
            commit(2, true),
            Op::CommitInTick { pages: 3 },
            commit(4, true),
            commit(4, true),
            wal(
                WalOp::RegrowWithoutCommitMarks(7_897_884_903_207_927_620),
                4,
            ),
            wal(
                WalOp::RegrowWithoutCommitMarks(12_632_097_625_952_158_396),
                4,
            ),
            wal(WalOp::RegrowSameSalts(5_163_590_175_676_367_914), 5),
            back.clone(),
            Op::CommitInTick { pages: 4 },
            back.clone(),
            back.clone(),
            Op::EnvFault(EnvFault::WipeStaging),
            back.clone(),
            Op::CommitInTick { pages: 2 },
            Op::CommitInTick { pages: 1 },
            wal(WalOp::TearHeader, 5),
            Op::CommitInTick { pages: 3 },
            commit(3, false),
            Op::CommitInTick { pages: 4 },
            commit(1, true),
            wal(
                WalOp::RegrowWithoutCommitMarks(2_609_243_433_445_728_187),
                7,
            ),
            wal(WalOp::TruncateAtFrame(8), 3),
            back,
            commit(3, true),
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0x54e), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod failed_drop {
    use lsr_core::{StoreError, StoreOp};
    use lsr_sim::generate::WalOp;
    use lsr_sim::run::run_ops;
    use lsr_sim::{FaultShape, Op, Opts, Seed, Target};

    fn reset(op: StoreOp) -> FaultShape {
        FaultShape::Error(StoreError::Transient {
            op,
            kind: std::io::ErrorKind::ConnectionReset,
        })
    }

    /// The other writer's file stands over the engine's staged transaction 7,
    /// and the listing that finds them unlike drops the staged files from 7
    /// on; that delete fails. The uploader had already taken 7 as confirmed
    /// and uploaded the staged 8, built on the staged 7, onto the other
    /// writer's file: level 1 could then not be merged from the replica and
    /// maintenance degraded on `RemoteCorrupt` (seed 0xd79, shrunk).
    #[test]
    fn a_failed_drop_after_a_divergence_uploads_nothing_built_on_it_seed_0xd79() {
        let ops = [
            Op::Tick,
            Op::Tick,
            Op::StoreFault {
                target: Target::Staging,
                op: StoreOp::Delete,
                ahead: 1,
                shape: reset(StoreOp::Delete),
            },
            Op::WalDamage {
                op: WalOp::TearHeader,
                ticks: 4,
            },
            Op::SecondWriter { ticks: 4 },
            Op::Tick,
            Op::RequestSnapshot,
            Op::Tick,
            Op::ClockJump(98_967),
            Op::Commit {
                pages: 1,
                grow: false,
            },
            Op::Tick,
            Op::Commit {
                pages: 1,
                grow: true,
            },
            Op::StoreFault {
                target: Target::Remote,
                op: StoreOp::Upload,
                ahead: 3,
                shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: std::io::ErrorKind::ConnectionReset,
                }),
            },
            Op::WalDamage {
                op: WalOp::TruncateAtFrame(7),
                ticks: 3,
            },
            Op::Commit {
                pages: 4,
                grow: true,
            },
            Op::ClockJump(59_427),
            Op::SecondWriter { ticks: 7 },
        ];
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0xd79), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod one_bad_read {
    use lsr_core::StoreOp;
    use lsr_sim::generate::Target;
    use lsr_sim::run::run_ops;
    use lsr_sim::{FaultShape, Op, Opts, Seed};

    /// One read of the boot's read-back of a sound level-9 snapshot returns
    /// a flipped byte, and the engine deleted the snapshot as rotten (seed
    /// 0x487's path, written out).
    #[test]
    fn one_flipped_read_deletes_no_sound_snapshot() {
        let mut ops = vec![Op::Commit {
            pages: 3,
            grow: false,
        }];
        ops.extend(std::iter::repeat_n(Op::Tick, 8));
        ops.push(Op::Commit {
            pages: 2,
            grow: false,
        });
        ops.extend(std::iter::repeat_n(Op::Tick, 4));
        ops.push(Op::Shutdown);
        ops.push(Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Read,
            ahead: 4,
            shape: FaultShape::FlipByteOnRead { at: 600 },
        });
        ops.extend(std::iter::repeat_n(Op::Tick, 10));
        ops.push(Op::Restore);
        if let Err(failure) = run_ops(Seed::new(0x487), &ops, &Opts::default()) {
            panic!("{}", failure.violation);
        }
    }
}

#[cfg(test)]
mod second_writer {
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, Seed};

    /// Another writer puts a level-0 file one transaction past the engine's
    /// newest while it runs. Maintenance listed the replica every window and
    /// compacted the other writer's file into level 1 instead of stopping;
    /// the engine's own upload of that transaction then replaced the
    /// level-0 key, so the replica held two histories for one transaction
    /// (seed 0x50c, shrunk).
    #[test]
    fn maintenance_stops_on_another_writer_seed_0x50c() {
        let mut ops = vec![
            Op::CommitInTick { pages: 2 },
            Op::Commit {
                pages: 1,
                grow: false,
            },
        ];
        ops.extend(std::iter::repeat_n(Op::Tick, 6));
        ops.push(Op::SecondWriter { ticks: 5 });
        ops.push(Op::Restore);
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0x50c), &ops, &opts) {
            panic!("{}", failure.violation);
        }
    }

    fn drawn(seed: u64, ops: &[Op]) {
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(seed), ops, &opts) {
            panic!("{}", failure.violation);
        }
    }

    /// The other writer's file appears while the engine is down; the boot
    /// takes it as history and snapshots after it, then the operator removes
    /// it. The removal takes that transaction for good, which the oracle
    /// counted as a gap the engine left (seed 0xf85, shrunk).
    #[test]
    fn removing_a_file_a_boot_built_on_loses_its_transaction_seed_0xf85() {
        use lsr_core::{SourceError, SourceOp};
        use lsr_sim::generate::WalOp;
        use lsr_sim::{CrashKind, EnvFault};
        let ops = [
            Op::WalDamage {
                op: WalOp::TruncateAtFrame(15),
                ticks: 3,
            },
            Op::Crash(CrashKind::Power),
            Op::EnvFault(EnvFault::Source(SourceOp::Len, SourceError::Busy)),
            Op::SecondWriter { ticks: 5 },
        ];
        drawn(0xf85, &ops);
    }

    /// The engine's own upload of the same transaction replaces the other
    /// writer's file before any listing, and level 1 is built from it: the
    /// oracle mapped that level-1 file to the other writer's pages (seed
    /// 0x433, shrunk).
    #[test]
    fn a_level_built_after_the_engine_replaced_the_file_holds_its_pages_seed_0x433() {
        let ops = [
            Op::Tick,
            Op::Tick,
            Op::CommitInTick { pages: 1 },
            Op::Shrink { pages: 3 },
            Op::SecondWriter { ticks: 5 },
        ];
        drawn(0x433, &ops);
    }

    /// The other writer puts transaction 3 while the engine runs, the engine
    /// stops, and the operator removes it; staging is wiped and the other
    /// writer puts 3 again, with no replica call between the removal and the
    /// second put. The boot takes the second file as history and compacts it
    /// into levels 1 to 9. The oracle ordered the first removal after the
    /// second put (both at one recorder length) and mapped the compacted file
    /// to the engine's own staged note for 3 instead of the other writer's
    /// pages (seed 0x18efc, shrunk).
    #[test]
    fn a_removal_before_a_second_put_of_the_same_id_seed_0x18efc() {
        use lsr_core::{StoreError, StoreOp};
        use lsr_sim::{EnvFault, FaultShape, Target};
        let ops = [
            Op::Shutdown,
            Op::StoreFault {
                target: Target::Remote,
                op: StoreOp::Upload,
                ahead: 1,
                shape: FaultShape::Error(StoreError::Throttled {
                    op: StoreOp::Upload,
                    retry_after: None,
                }),
            },
            Op::SecondWriter { ticks: 4 },
            Op::Commit {
                pages: 2,
                grow: false,
            },
            Op::Shutdown,
            Op::Tick,
            Op::SecondWriter { ticks: 3 },
            Op::EnvFault(EnvFault::WipeStaging),
            Op::SecondWriter { ticks: 7 },
        ];
        drawn(0x18efc, &ops);
    }
}

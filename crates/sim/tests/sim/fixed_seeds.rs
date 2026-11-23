//! The fixed seed set: 64 histories of 300 operations, each batch of eight inside a budget of
//! engine ticks (a count, never the clock: a loaded machine changes how long a seed takes, not how
//! many ticks it needs), half under the default mix and half under a swarm mix of
//! their own (`Weights::mixed`). A failure prints its replay line (`just
//! sim-replay`).

#[cfg(test)]
mod fixed {
    use crate::histories as h;
    use lsr_sim::run::run_ops;
    use lsr_sim::{Op, Opts, Seed, Weights, run};

    const STEPS: u32 = 300;
    /// Engine ticks a batch of eight seeds may run. The batches of seeds 1 to 64 take 1,500 to
    /// 2,000 (no seed alone takes more than 410), so a seed that starts looping, a retry that
    /// never backs off or a drain that spins, trips it long before nextest's hang limit.
    const BATCH_TICKS: u32 = 4_000;

    /// The ticks a run took: `RunReport::ticks`, which counts every tick the actor ran.
    fn spent(runs: impl IntoIterator<Item = u32>) -> u32 {
        runs.into_iter().fold(0, u32::saturating_add)
    }

    fn within_budget(ticks: u32) {
        assert!(
            ticks <= BATCH_TICKS,
            "simulator tick budget exceeded: {ticks} ticks, budget {BATCH_TICKS}"
        );
    }

    fn batch(first: u64) {
        let mut ticks = Vec::new();
        for n in (first..).take(8) {
            let seed = Seed::new(n);
            match run(seed, STEPS, &Weights::mixed(seed)) {
                Ok(report) => ticks.push(report.ticks),
                Err(failure) => panic!("{failure}"),
            }
        }
        within_budget(spent(ticks));
    }

    /// The budget fails a history that ticks past it: 5,000 idle ticks, the shape of a seed
    /// whose engine loops, run through the same measure and the same assertion.
    #[test]
    #[should_panic(expected = "simulator tick budget exceeded")]
    fn a_history_that_loops_past_the_tick_budget_fails_it() {
        let ops = vec![Op::Tick; 5_000];
        let report = run_ops(Seed::new(1), &ops, &Opts::default()).expect("idle ticks pass");
        assert!(
            report.ticks > BATCH_TICKS,
            "the plant must exceed the budget"
        );
        within_budget(spent([report.ticks]));
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

    /// A regression found under the first generator: its recorded history,
    /// run with no drawn settings, as that generator's runs were.
    fn replay(seed: u64, ops: &[Op]) {
        if let Err(failure) = run_ops(Seed::new(seed), ops, &Opts::default()) {
            panic!("{failure}");
        }
    }

    // A staging backup from older WAL generations, put back while the
    // engine was down, was uploaded after a replica file that held a later
    // state: a listing accepted a staged file starting a new generation's
    // first frame as following the replica, a hop the boot rules refuse to
    // prove. Staged files now go up only with a witness bound to their
    // bytes (`crates/core/src/witness.rs`).

    #[test]
    fn a_restored_staging_backup_is_not_uploaded_seed_0x1919f() {
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(0x1919f), h::seed_0x1919f::OPS, &opts) {
            panic!("{failure}");
        }
    }

    /// A regression the soak found: its recorded history, run as `run_with`
    /// ran it that night: drawn, under the crash law the seed's mix drew then.
    fn replay_drawn(seed: u64, ops: &[Op], block_crash_law: bool) {
        let opts = Opts {
            drawn: true,
            block_crash_law,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(Seed::new(seed), ops, &opts) {
            panic!("{failure}");
        }
    }

    // The rotating soak of 2026-11-22 ran on a tree before the witness fix
    // above and failed these five with "latest went back from node N to
    // node M, and no rewind cut node N off": a staging directory put back
    // from a backup, or a staged file another valid one replaced, was
    // uploaded by name after a later state. Each history is the operations
    // that night drew, recorded.

    #[test]
    fn a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fac4b6() {
        replay_drawn(0x0000_126d_68fa_c4b6, h::seed_0x126d68fac4b6::OPS, false);
    }

    #[test]
    fn a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fac789() {
        replay_drawn(0x0000_126d_68fa_c789, h::seed_0x126d68fac789::OPS, false);
    }

    #[test]
    fn a_put_back_staged_file_never_sends_latest_back_seed_0x126d68facf21() {
        replay_drawn(0x0000_126d_68fa_cf21, h::seed_0x126d68facf21::OPS, false);
    }

    #[test]
    fn a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fad061() {
        replay_drawn(0x0000_126d_68fa_d061, h::seed_0x126d68fad061::OPS, true);
    }

    #[test]
    fn a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fae16b() {
        replay_drawn(0x0000_126d_68fa_e16b, h::seed_0x126d68fae16b::OPS, false);
    }

    // The same night: the level-bytes budget failed with "12050 is over the
    // limit 10917" because it counted only this engine's own files, not
    // another writer's that compaction merged. The budget now counts them
    // (crates/sim/src/run.rs).

    #[test]
    fn another_writer_s_files_fit_the_level_bytes_budget_seed_0x126d68facf74() {
        replay_drawn(0x0000_126d_68fa_cf74, h::seed_0x126d68facf74::OPS, false);
    }

    // Finding 2: a power loss tore a staged level-0 file whose commit never
    // returned. Boot snapshotted past it and left it in staging, and the upload
    // task stopped on it. The torn file, and all staged after it, is now lost.

    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x25() {
        replay(0x25, h::seed_0x25::OPS);
    }

    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x2d() {
        replay(0x2d, h::seed_0x2d::OPS);
    }

    /// Reached through `LocalRewound`: the torn file was an upload whose commit
    /// errored after publishing, torn by an earlier power loss. Since the
    /// store's size-mismatch fault publishes a short object, it also holds a
    /// level-2 compaction output acknowledged one byte short: an engine that
    /// took that acknowledgement left a file `restore Latest` cannot decode
    /// (step 89).
    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x46() {
        replay(0x46, h::seed_0x46::OPS);
    }

    // Issue #39: a snapshot staged for some id never reached the replica (a
    // power loss tore it, or the staging directory was wiped), and the engine
    // took the same id again from a later commit against an empty WAL (after a
    // VACUUM, or a rewind to the database file). The replica held only the
    // second snapshot, but the oracle paired the id with the first note's
    // commit. The restore was right; the oracle now takes the newest note.

    #[test]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ef1c07() {
        replay(0x0000_126d_68ef_1c07, h::seed_0x126d68ef1c07::OPS);
    }

    #[test]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ed250e() {
        replay(0x0000_126d_68ed_250e, h::seed_0x126d68ed250e::OPS);
    }

    #[test]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ef545f() {
        replay(0x0000_126d_68ef_545f, h::seed_0x126d68ef545f::OPS);
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

#[cfg(test)]
mod replaced_staging {
    use lsr_core::{StoreError, StoreOp};
    use lsr_sim::generate::{MetaOp, Target};
    use lsr_sim::run::run_ops;
    use lsr_sim::store::FaultShape;
    use lsr_sim::{Op, Opts, Seed};

    fn failed_upload() -> Op {
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Upload,
            ahead: 0,
            shape: FaultShape::Error(StoreError::Transient {
                op: StoreOp::Upload,
                kind: std::io::ErrorKind::ConnectionReset,
            }),
        }
    }

    fn commit() -> Op {
        Op::Commit {
            pages: 2,
            grow: false,
        }
    }

    /// `replace` happens to the newest staged file while its upload waits
    /// on a failed attempt; the oracle judges every level-0 put against
    /// what the engine staged and the WAL it booted on.
    fn pending_then(replace: Op) {
        let mut ops = vec![commit(), Op::Tick, Op::Tick, Op::Tick];
        ops.extend([failed_upload(), commit(), Op::Tick, replace]);
        ops.extend(std::iter::repeat_n(Op::Tick, 12));
        ops.push(Op::Restore);
        if let Err(failure) = run_ops(Seed::new(0x5717), &ops, &Opts::default()) {
            panic!("{}", failure.violation);
        }
    }

    /// Replaced under the running engine by another whole, valid file: the
    /// upload's read does not match the file's witness, so it is refused.
    #[test]
    fn a_staged_file_forged_under_a_running_engine_is_refused() {
        pending_then(Op::Meta(MetaOp::ForgeNewest));
    }

    /// Replaced while the engine is down: the boot's WAL pass refuses it,
    /// and the WAL stages its transaction again.
    #[test]
    fn a_staged_file_forged_while_down_is_refused_at_boot() {
        pending_then(Op::StagingDamage(MetaOp::ForgeNewest));
    }
}

#[cfg(test)]
mod lagging_listings {
    use lsr_core::{SourceError, SourceOp, StopCause, StoreError, StoreOp};
    use lsr_sim::generate::{MetaOp, WalOp};
    use lsr_sim::run::run_ops;
    use lsr_sim::{EnvFault, FaultShape, Op, Opts, Seed, Target, Weights};

    /// Bucket rollbacks take transactions 1 to 4 and leave 5, staging is wiped, and the boot
    /// lists behind a lag that shows nothing. Its walk stopped after 3 missing transactions,
    /// short of 5, so it took the replica as empty, wrote a first sync and uploaded a new 4
    /// below the replica's 5: the latest state went back with no signal (seed 0x95a under its
    /// mixed mix, 22 recorded operations). With nothing staged to bound it, the walk now goes to
    /// its cap and finds 5.
    #[test]
    fn a_boot_with_staging_wiped_walks_past_a_wide_hole_seed_0x95a() {
        let seed = Seed::new(0x95a);
        let weights = Weights::mixed(seed);
        let opts = Opts {
            drawn: true,
            block_crash_law: weights.block_crash_law,
            lying_wal_sync: weights.lying_wal_sync,
            after_restart: weights.after_restart,
            clock_law: weights.clock_law,
            overlap: weights.overlap,
            error_domain: weights.error_domain,
            known_wrong: weights.known_wrong,
            staging_fs: weights.staging_fs,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(seed, crate::histories::seed_0x95a::OPS, &opts) {
            panic!("{failure}");
        }
    }

    /// Lagging listings, bucket rollbacks and a second writer: with the probe past a listing
    /// naming no level 1 to 8 key, a restore of a transaction differs from its commit (seed
    /// 0x4f4 under its swarm mix, 72 recorded operations). It stands in for the seeds 0x517 and
    /// 0x4dc histories, recorded on an older tree, that no longer reach the defect.
    #[test]
    fn a_merged_file_a_lagging_listing_hides_is_found_by_its_level_key_seed_0x4f4() {
        let seed = Seed::new(0x4f4);
        let weights = Weights::swarm(seed);
        let opts = Opts {
            drawn: true,
            block_crash_law: weights.block_crash_law,
            lying_wal_sync: weights.lying_wal_sync,
            after_restart: weights.after_restart,
            clock_law: weights.clock_law,
            overlap: weights.overlap,
            error_domain: weights.error_domain,
            known_wrong: weights.known_wrong,
            staging_fs: weights.staging_fs,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(seed, crate::histories::seed_0x4f4::OPS, &opts) {
            panic!("{failure}");
        }
    }

    /// The replica's listings lag; the engine uploads transactions 1 and 2,
    /// its staging directory is wiped and it boots again with the WAL intact.
    /// The boot listing does not show 2 yet, so the engine took the replica as
    /// ending at 1, then found 2 past everything it knew and stopped as if
    /// another writer had put it, though its own previous process did. The
    /// boot now reads the keys past its listing and finds 2, which the WAL
    /// vouches for. Written by hand: the swarm history first pinned here (seed
    /// 0x39) also rolled the bucket back and regrew the WAL, and since boot
    /// refuses replica files nothing local vouches for (docs/decisions.md D12)
    /// that history stops on those grounds instead.
    #[test]
    fn a_stale_boot_listing_does_not_stop_the_engine_on_its_own_files() {
        let commit = || Op::Commit {
            pages: 1,
            grow: false,
        };
        let ops = [
            Op::LaggingListings {
                writes: 2,
                window: 16,
            },
            commit(),
            Op::Tick,
            Op::Tick,
            commit(),
            Op::Tick,
            Op::Tick,
            Op::EnvFault(EnvFault::WipeStaging),
            Op::Shutdown,
        ];
        let opts = Opts {
            drawn: true,
            trace: true,
            ..Opts::default()
        };
        let report = match run_ops(Seed::new(1), &ops, &opts) {
            Ok(report) => report,
            Err(failure) => panic!("{failure}"),
        };
        let stop = format!("Stopped({:?})", StopCause::OtherWriter);
        assert!(
            report.trace.iter().all(|line| !line.contains(&stop)),
            "the engine stopped on its own files"
        );
    }

    /// The replica's listings lag and a commit fails after it published, so
    /// the engine lists before its next upload. The listing does not show the
    /// transaction the engine had confirmed, and the engine reported the
    /// replica as gone backwards, a degraded task until a person looks (seed
    /// 0x440 under its swarm mix, shrunk from 170 operations). The relist now
    /// reads the confirmed key before it reports a regression.
    #[test]
    fn a_lagging_relist_does_not_report_the_replica_gone_backwards_seed_0x440() {
        let seed = Seed::new(0x440);
        let ops = [
            Op::WalDamage {
                op: WalOp::FlipLastFrameBit,
                ticks: 7,
            },
            Op::LaggingListings {
                writes: 6,
                window: 25,
            },
            Op::StoreFault {
                target: Target::Remote,
                op: StoreOp::Upload,
                ahead: 1,
                shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: std::io::ErrorKind::ConnectionReset,
                }),
            },
            Op::Commit {
                pages: 3,
                grow: false,
            },
            Op::Commit {
                pages: 3,
                grow: false,
            },
            Op::WalDamage {
                op: WalOp::RegrowWithoutCommitMarks(5_374_048_325_351_388_651),
                ticks: 4,
            },
        ];
        swarm(seed, &ops);
    }

    /// The replica's listings lag. Another writer's file at transaction 2 is
    /// merged into a level-1 file, the operator removes it, and the engine
    /// boots behind a listing that shows neither that level-1 file nor the
    /// engine's own transaction 3. It took the replica as ending at 1 and
    /// uploaded its next transaction into the freed key 2, which the level-1
    /// file still holds with other pages: one transaction, two contents
    /// (seed 0x4c5 under its swarm mix). The boot now reads past the free key
    /// and finds 3. Since boot stops on another writer's files, the engine
    /// stops before it merges that file, so this history no longer frees a key
    /// a level-1 file holds; it stays as a second writer under lagging
    /// listings, and the operator case is open (#99).
    #[test]
    fn a_lagging_boot_listing_does_not_send_an_upload_into_a_freed_key_seed_0x4c5() {
        let seed = Seed::new(0x4c5);
        let ops = [
            Op::LaggingListings {
                writes: 5,
                window: 35,
            },
            Op::Shutdown,
            Op::Vacuum { page_size: None },
            Op::Tick,
            Op::SecondWriter { ticks: 5 },
            Op::Shutdown,
        ];
        swarm(seed, &ops);
    }

    /// The replica's listings lag. Another writer puts the transaction after the engine's,
    /// the engine's maintenance merges it into a level-1 file, and the operator removes the
    /// other writer's level-0 file and restarts the engine. The boot listing hides the
    /// level-1 file, and the read past the listing named only level-0 keys and snapshots,
    /// so the engine took the replica as ending below it and uploaded its own transaction
    /// into the freed key: a restore of the newest state gave pages no commit made (seed
    /// 0x517 under its swarm mix, shrunk from 47 operations). The read past the listing now
    /// names the level-1 key from where the listed level-1 files end.
    #[test]
    fn a_lagging_listing_that_hides_a_merged_file_sends_no_upload_into_its_key_seed_0x517() {
        let seed = Seed::new(0x517);
        let ops = [
            Op::CommitInTick { pages: 3 },
            Op::CommitInTick { pages: 4 },
            Op::EnvFault(EnvFault::Source(SourceOp::ReadPage, SourceError::Busy)),
            Op::CommitInTick { pages: 1 },
            Op::CommitInTick { pages: 1 },
            Op::Shutdown,
            Op::LaggingListings {
                writes: 6,
                window: 22,
            },
            Op::Shutdown,
            Op::SecondWriter { ticks: 4 },
            Op::Shutdown,
        ];
        swarm(seed, &ops);
    }

    /// The same defect reached through a staging directory saved and put back: the other
    /// writer's file is merged into level 1 and removed, the lagging listing hides the
    /// level-1 file, and the engine's upload reuses the freed transaction, so a restore of
    /// that transaction differs from what it committed (seed 0x4dc under its swarm mix,
    /// shrunk to 11 operations).
    #[test]
    fn a_merged_file_hidden_from_a_lagging_listing_is_found_by_its_level_key_seed_0x4dc() {
        let seed = Seed::new(0x4dc);
        let ops = [
            Op::LaggingListings {
                writes: 3,
                window: 31,
            },
            Op::Tick,
            Op::Tick,
            Op::WalDamage {
                op: WalOp::TruncateAtFrame(19),
                ticks: 1,
            },
            Op::Meta(MetaOp::Save),
            Op::SecondWriter { ticks: 2 },
            Op::EnvFault(EnvFault::Source(SourceOp::Open, SourceError::Busy)),
            Op::Meta(MetaOp::Restore),
            Op::EnvFault(EnvFault::Source(SourceOp::ReadPage, SourceError::Busy)),
            Op::SecondWriter { ticks: 6 },
            Op::Tick,
        ];
        swarm(seed, &ops);
    }

    /// Runs `ops` under `seed`'s swarm mix; any violation fails the test.
    fn swarm(seed: Seed, ops: &[Op]) {
        let weights = Weights::swarm(seed);
        let opts = Opts {
            drawn: true,
            block_crash_law: weights.block_crash_law,
            lying_wal_sync: weights.lying_wal_sync,
            after_restart: weights.after_restart,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(seed, ops, &opts) {
            panic!("{failure}");
        }
    }
}

#[cfg(test)]
mod refused_upload {
    use lsr_core::StoreOp;
    use lsr_sim::run::run_ops;
    use lsr_sim::{FaultShape, Op, Opts, Seed, Target, Weights};

    /// Another writer's file stands at the transaction the engine uploads
    /// next, and the bucket refuses the engine's upload. One read of the
    /// staged copy came back with a flipped byte, so the read-back judged the
    /// staged copy torn and confirmed the other writer's object as the
    /// engine's own transaction: a restore of it returned the other writer's
    /// pages (seed 0x4ef under its swarm mix, shrunk from 23 operations).
    #[test]
    fn one_bad_staged_read_after_a_refusal_confirms_no_other_object_seed_0x4ef() {
        let seed = Seed::new(0x4ef);
        let ops = [
            Op::CommitInTick { pages: 1 },
            Op::CommitInTick { pages: 1 },
            Op::Tick,
            Op::StoreFault {
                target: Target::Staging,
                op: StoreOp::Read,
                ahead: 2,
                shape: FaultShape::FlipByteOnRead { at: 1140 },
            },
            Op::Shrink { pages: 1 },
            Op::RequestSnapshot,
            Op::SecondWriter { ticks: 7 },
            Op::Restore,
        ];
        let opts = Opts {
            drawn: true,
            block_crash_law: Weights::swarm(seed).block_crash_law,
            ..Opts::default()
        };
        if let Err(failure) = run_ops(seed, &ops, &opts) {
            panic!("{failure}");
        }
    }
}

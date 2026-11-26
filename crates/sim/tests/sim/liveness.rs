//! The large profile, with each tick's I/O charged to the clock (`lsr_sim::cost`): a database
//! of tens of thousands of pages, so a snapshot or a compaction input is hundreds of slices,
//! and the judges that only time can feed: every tick's cost against its bound, and the healthy
//! tail's commits reaching the replica in time (`run::tail`).

use std::num::{NonZeroU32, NonZeroU64};

use lsr_sim::cost::Profile;
use lsr_sim::run::run_ops;
use lsr_sim::{EnvFault, Op, Opts, RunReport, Seed};

/// The large profile: 20,000 pages of 512 bytes (10 MiB), sent 64 KiB a tick.
fn large() -> Opts {
    Opts {
        first_db_pages: NonZeroU32::new(20_000),
        slice_bytes: NonZeroU64::new(64 << 10),
        cost: Some(Profile::DEFAULT),
        ..Opts::default()
    }
}

fn run(ops: &[Op]) -> RunReport {
    match run_ops(Seed::new(1), ops, &large()) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    }
}

/// The replica on S3 sending 64-byte parts one at a time, 256 bytes a tick: the tail's
/// snapshot often finds its part in the way, and goes on from where it stood once the part
/// lands; a job that started again each time would waste a slice a time with nothing failing.
#[test]
fn a_job_behind_its_parts_goes_on_from_where_it_stood() {
    let opts = Opts {
        remote_s3: true,
        remote_parts: lsr_sim::remote::Parts::new(64, 1),
        slice_bytes: NonZeroU64::new(256),
        cost: Some(Profile::DEFAULT),
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &commits(20, 4), &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        report.reach.get("liveness.judged") > 0,
        "{:?}",
        report.reach
    );
}

/// Commits of `pages` pages each, `n` of them, two ticks apart.
fn commits(n: usize, pages: u8) -> Vec<Op> {
    (0..n)
        .flat_map(|_| [Op::Commit { pages, grow: false }, Op::Tick, Op::Tick])
        .collect()
}

/// The level-9 snapshot of the whole database goes up a slice a tick, and the read-back that
/// checks the newest file of every level after the boot goes a slice a tick too: the run passes
/// and judges the tail's lag. Read in one maintenance tick, the read-back held the replica for
/// ten seconds here and for the whole database at any size.
#[test]
fn the_read_back_of_a_large_snapshot_goes_a_slice_a_tick() {
    let report = run(&commits(4, 2));
    assert!(
        report.reach.get("liveness.judged") > 0,
        "{:?}",
        report.reach
    );
}

/// Sixty commits of 200 pages each stage about 6 MiB of level-0 files, which the level-1
/// compaction in the tail merges: a hundred slices of input, each tick at most one, and every
/// commit made meanwhile reaches the replica in time.
#[test]
fn commits_made_during_a_large_compaction_reach_the_replica_in_time() {
    let report = run(&commits(60, 200));
    assert!(
        report.reach.get("liveness.judged") > 0,
        "{:?}",
        report.reach
    );
    assert!(report.reach.get("put.level.1") > 0, "{:?}", report.reach);
}

/// A boot that finds nothing staged and another writer's files on the replica restores the
/// replica to compare it with the database, a slice a tick: no tick of it goes past the
/// per-tick bound, which allows a boot nothing past it on the replica.
#[test]
fn a_boot_over_another_writer_reads_the_replica_a_slice_a_tick() {
    let mut ops = commits(200, 1);
    ops.extend([
        Op::EnvFault(EnvFault::WipeStaging),
        Op::SecondWriter { ticks: 2 },
        Op::Tick,
        Op::Tick,
    ]);
    let report = match run_ops(Seed::new(1), &ops, &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        report.reach.get("tick_cost.judged") > 0,
        "{:?}",
        report.reach
    );
}

/// A defect allowance's test (`lsr_sim::cost::s3_rereads`, <https://github.com/emschwartz/litestream-rs/issues/478>): the S3 replica
/// reads each staged body it puts whole again from staging, to hash it and to send it. It
/// fails once the store hashes and sends from one read; the allowance goes with it.
#[test]
fn an_s3_put_reads_its_staged_body_again() {
    let opts = Opts {
        remote_s3: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &commits(5, 1), &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        report.reach.get("tick_cost.s3_rereads_allowed") > 0,
        "{:?}",
        report.reach
    );
}

/// Commits two ticks apart, then the healthy tail: once each tail commit reaches the replica,
/// the WAL holds no more than Go's PASSIVE minimum, the commit's frames and the engine's bump
/// frame (`lsr_sim::run::tail::wal_bound`).
#[test]
fn the_wal_stays_near_the_passive_minimum_through_the_tail() {
    let report = match run_ops(Seed::new(1), &commits(6, 2), &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        report.reach.get("wal_bound.judged") >= 30,
        "{:?}",
        report.reach
    );
}

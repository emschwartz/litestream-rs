//! Fail-stop in named runs: the trips that must come
//! (P2, a missed trip; P3, clearing; P5, a crash loop) where each run's truth is exact, and one
//! run for each class of fault that must never trip. The judge of every run
//! (`lsr_sim::failstop`, P1) watches all of them too.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{Denied, StoreError, StoreOp, Trip};
use lsr_ltx::Timestamp;
use lsr_sim::generate::MetaOp;
use lsr_sim::run::run_ops;
use lsr_sim::{ClockLaw, FaultShape, Op, Opts, RunReport, Seed, Target};

/// The first instant of every run (`lsr_sim::world::EPOCH_MILLIS`).
const EPOCH: i64 = lsr_sim::world::EPOCH_MILLIS;
/// The default bound.
const BOUND: i64 = 600_000;
/// One probe and one tick past the bound: the slack P2 allows.
const SLACK: i64 = 60_000;

fn down(op: StoreOp, err: StoreError) -> Op {
    Op::StoreFault {
        target: Target::Remote,
        op,
        ahead: 0,
        shape: FaultShape::Rate {
            per_mille: 1000,
            err,
        },
    }
}

const fn timeout(op: StoreOp) -> StoreError {
    StoreError::Transient {
        op,
        kind: ErrorKind::TimedOut,
    }
}

fn commit() -> Op {
    Op::Commit {
        pages: 2,
        grow: false,
    }
}

fn ticks(n: usize) -> impl Iterator<Item = Op> {
    std::iter::repeat_n(Op::Tick, n)
}

fn run(ops: &[Op]) -> RunReport {
    run_with(ops, &Opts::default())
}

/// A run whose store refusal is expected: a task it degrades is not a bug.
fn run_refused(ops: &[Op]) -> RunReport {
    run_with(
        ops,
        &Opts {
            allow_stop: true,
            ..Opts::default()
        },
    )
}

fn run_with(ops: &[Op], opts: &Opts) -> RunReport {
    if std::env::var_os("LSR_FAILSTOP_TRACE").is_some() {
        let traced = Opts {
            trace: true,
            ..opts.clone()
        };
        if let Ok(report) = run_ops(Seed::new(0x115), ops, &traced) {
            eprintln!("{}", report.trace.join("\n"));
        }
    }
    match run_ops(Seed::new(0x115), ops, opts) {
        Ok(report) => report,
        Err(failure) => panic!("{}\n{}", failure.violation, failure.message(failure.steps)),
    }
}

/// The first trip of a run, with its time from the run's start.
fn first_trip(report: &RunReport) -> Option<(i64, Trip)> {
    report
        .trips
        .iter()
        .find_map(|(at, trip)| trip.map(|trip| (at.unix_millis().saturating_sub(EPOCH), trip)))
}

fn age_trips(report: &RunReport) -> usize {
    report
        .trips
        .iter()
        .filter(|(_, trip)| matches!(trip, Some(Trip::UnreplicatedTooLong { .. })))
        .count()
}

/// The store is down from the start for twice the bound: the trip comes at the bound plus at
/// most the slack (P2), and clears once the store heals and the replica catches up (P3).
#[test]
fn a_store_down_past_the_bound_trips_in_time_and_clears_after() {
    let mut ops = vec![down(StoreOp::Upload, timeout(StoreOp::Upload)), commit()];
    ops.extend(ticks(60));
    ops.push(Op::Heal);
    ops.extend(ticks(60));
    let report = run(&ops);
    let (at, trip) = first_trip(&report).expect("trips");
    assert!(
        matches!(trip, Trip::UnreplicatedTooLong { .. }),
        "{:?}",
        report.trips
    );
    assert!(
        (BOUND..=BOUND + SLACK).contains(&at),
        "tripped {at} ms in: {:?}",
        report.trips
    );
    let in_history = report
        .trips
        .get(..report.trips_in_history)
        .unwrap_or_default();
    assert_eq!(
        in_history.last().map(|(_, trip)| *trip),
        Some(None),
        "cleared before the final drain: {:?}",
        report.trips
    );
}

/// A crash loop with lives shorter than the bound still trips (P5): the clock comes from the
/// oldest pending staged file, and a plain restart starts from it.
///
/// Here the second boot cannot vouch for the first life's staged files (a checkpoint moved
/// the WAL on), deletes them and stages a snapshot of their end state, stamped at that boot.
/// The clock keeps the deleted files' stamps for the life of that process; the next process
/// can only read the snapshot's. So the clock's start moves once, to that boot, and never at
/// a restart that deletes nothing (`docs/decisions.md` D80: a durable mark would close it).
#[test]
fn a_crash_loop_trips_for_age() {
    let mut ops = vec![down(StoreOp::Upload, timeout(StoreOp::Upload)), commit()];
    // Each life retries at 1, 3, 7 ... 255 s or more: minutes, under the bound.
    for _ in 0..4 {
        ops.extend(ticks(12));
        ops.push(Op::Restart);
    }
    ops.extend(ticks(8));
    ops.push(Op::Heal);
    ops.extend(ticks(40));
    let report = run(&ops);
    assert!(report.crashes >= 4, "lives: {}", report.crashes);
    let starts: Vec<Timestamp> = report
        .trips
        .iter()
        .filter_map(|(_, trip)| match trip {
            Some(Trip::UnreplicatedTooLong { since }) => Some(*since),
            Some(_) | None => None,
        })
        .collect();
    let (at, _) = first_trip(&report).expect("the crash loop trips");
    let since = starts.first().copied().expect("an age trip");
    assert!(
        starts.iter().all(|start| *start == since),
        "the clock moved at a restart that deleted nothing: {:?}",
        report.trips
    );
    let since = since.unix_millis() - EPOCH;
    assert!(
        at <= since + BOUND + SLACK,
        "tripped {at} ms in, clock from {since}: {:?}",
        report.trips
    );
    assert!(
        since < BOUND,
        "the clock starts before the bound would let a life pass: {since}"
    );
}

/// A blip, a few minutes of timeouts, never trips.
#[test]
fn a_short_outage_never_trips() {
    let mut ops = vec![down(StoreOp::Upload, timeout(StoreOp::Upload)), commit()];
    ops.extend(ticks(8));
    ops.push(Op::Heal);
    ops.extend(ticks(40));
    let report = run(&ops);
    assert_eq!(first_trip(&report), None, "{:?}", report.trips);
}

/// Throttling with a long `retry_after`, healed inside the bound, never trips.
#[test]
fn throttling_inside_the_bound_never_trips() {
    let throttled = StoreError::Throttled {
        op: StoreOp::Upload,
        retry_after: Some(Duration::from_secs(120)),
    };
    let mut ops = vec![down(StoreOp::Upload, throttled), commit()];
    ops.extend(ticks(3));
    ops.push(Op::Heal);
    ops.extend(ticks(40));
    let report = run(&ops);
    assert_eq!(first_trip(&report), None, "{:?}", report.trips);
}

/// Timeouts at boot with nothing unreplicated yet start: no trip.
#[test]
fn a_store_down_at_boot_does_not_trip() {
    let mut ops = vec![down(StoreOp::List, timeout(StoreOp::List))];
    ops.extend(ticks(6));
    ops.push(Op::Heal);
    ops.extend(ticks(20));
    let report = run(&ops);
    assert_eq!(first_trip(&report), None, "{:?}", report.trips);
}

/// A refusal before the first listing trips, and the first listing releases it by itself.
#[test]
fn a_refusal_at_boot_holds_until_it_heals() {
    let denied = StoreError::Denied {
        op: StoreOp::List,
        why: Denied::ClockSkew,
    };
    let mut ops = vec![down(StoreOp::List, denied)];
    ops.extend(ticks(4));
    ops.push(Op::Heal);
    ops.extend(ticks(20));
    let report = run_refused(&ops);
    assert_eq!(
        first_trip(&report).map(|(_, trip)| trip),
        Some(Trip::RefusedAtStart(Denied::ClockSkew)),
        "{:?}",
        report.trips
    );
    assert_eq!(report.trips.last().map(|(_, trip)| *trip), Some(None));
}

/// The boot's sweep of open uploads is best effort: a replica whose policy refuses it (Go's
/// published S3 policy does) still lists, so the start is judged, nothing trips, and the
/// replica holds every commit.
#[test]
fn a_refused_scratch_sweep_never_holds_the_boot() {
    let denied = StoreError::Denied {
        op: StoreOp::Upload,
        why: Denied::Permission,
    };
    let mut ops = vec![Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::Scratch(denied),
    }];
    ops.extend(ticks(4));
    ops.push(commit());
    ops.extend(ticks(20));
    let report = run(&ops);
    assert_eq!(first_trip(&report), None, "{:?}", report.trips);
}

/// A refusal after the first listing, healed inside the bound, never trips.
#[test]
fn a_refusal_after_the_first_listing_inside_the_bound_never_trips() {
    let denied = StoreError::Denied {
        op: StoreOp::Upload,
        why: Denied::Credentials,
    };
    let mut ops: Vec<Op> = ticks(4).collect();
    ops.extend([down(StoreOp::Upload, denied), commit()]);
    ops.extend(ticks(7));
    ops.push(Op::Heal);
    ops.extend(ticks(30));
    let report = run_refused(&ops);
    assert_eq!(first_trip(&report), None, "{:?}", report.trips);
}

/// Clock steps either way with a healthy store never trip: a step forward is found by the
/// probe, which succeeds.
#[test]
fn clock_steps_never_trip() {
    let mut ops: Vec<Op> = ticks(4).collect();
    ops.extend([commit(), Op::ClockJump(3_600_000), Op::Tick, commit()]);
    ops.extend(ticks(10));
    ops.extend([commit(), Op::ClockJump(-3_600_000), Op::Tick, commit()]);
    ops.extend(ticks(10));
    let report = run(&ops);
    assert_eq!(age_trips(&report), 0, "{:?}", report.trips);
}

/// Restarts, crashes and power losses with an honest disk and a healthy store never trip.
#[test]
fn restarts_never_trip() {
    let mut ops: Vec<Op> = ticks(4).collect();
    for _ in 0..4 {
        ops.extend([commit(), Op::Tick, Op::Restart, commit(), Op::PowerLoss]);
        ops.extend(ticks(6));
    }
    let report = run(&ops);
    assert_eq!(first_trip(&report), None, "{:?}", report.trips);
}

/// The two clocks a trip's time can be read on: what a test asserts is the engine's.
#[test]
fn trip_times_are_engine_times() {
    let report = run(&[Op::Tick]);
    assert!(
        report
            .trips
            .iter()
            .all(|(at, _)| *at >= Timestamp::from_unix_millis(EPOCH))
    );
}

/// A checkpoint's file that changes no page is not a commit, even after a day-long clock step: the
/// replica holds every commit, so the engine must not trip for age (D81).
#[test]
fn a_checkpoint_file_that_changes_no_page_does_not_trip_after_a_day_long_step() {
    let ops = [
        Op::Tick,
        Op::Commit {
            pages: 4,
            grow: false,
        },
        Op::CommitInTick { pages: 3 },
        Op::Meta(MetaOp::FlipNewestByte),
        Op::ClockJump(93_600_000),
    ];
    let opts = Opts {
        clock_law: ClockLaw::EachRead,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x8ef), &ops, &opts) {
        panic!("step {}: {}", failure.step, failure.violation);
    }
}

/// Every read of the replica lies, long after the engine's last upload and a day's clock step:
/// the engine cannot confirm that upload landed and dates it by its stamp, so an age trip is
/// right while the lie lasts, and the judge excuses it (and counts it) though the replica is whole.
#[test]
fn an_age_trip_while_every_read_of_the_replica_lies_is_excused() {
    let ops = [
        Op::CommitInTick { pages: 3 },
        Op::CommitInTick { pages: 3 },
        Op::ClockJump(93_600_000),
        Op::Hold {
            hold: lsr_sim::Hold::LyingReads,
            ticks: 37,
        },
    ];
    let opts = Opts {
        drawn: true,
        overlap: true,
        error_domain: true,
        staging_fs: true,
        kill_after_fault: true,
        ..Opts::default()
    };
    let report = run_ops(Seed::new(5), &ops, &opts)
        .unwrap_or_else(|failure| panic!("step {}: {}", failure.step, failure.violation));
    assert!(
        report.reach.get("failstop.excused.lying_reads") > 0,
        "no age trip was judged under the lie"
    );
}

/// A trip the judge accepted holds until the commits it tripped on reach the replica, not until
/// the age falls under the bound (`lsr_core::failstop`, Clearing). A clock stepped back after
/// the trip makes the age on the clock the engine reads shorter than the bound, and the engine's
/// trip stands all the same: that is the module's rule, not a trip that came early.
#[test]
fn a_trip_stands_when_the_clock_steps_back_under_the_bound() {
    let mut ops = vec![
        down(StoreOp::Upload, timeout(StoreOp::Upload)),
        commit(),
        Op::Tick,
    ];
    ops.push(Op::ClockJump(BOUND + 10_000));
    ops.extend(ticks(3));
    ops.push(Op::ClockJump(-100_000));
    ops.extend(ticks(3));
    let report = run(&ops);
    assert!(age_trips(&report) > 0, "{:?}", report.trips);
}

/// The store refuses conditional writes, so nothing uploads, and the application writes
/// nothing after its first transaction. A boot with its staging directory lost finds the
/// database equal to the replica and stages a snapshot that holds no commit. A day-long clock
/// step later, fail-stop must not trip: no commit of the application is off the replica (#345).
fn no_conditional_writes() -> Opts {
    Opts {
        no_create_only: true,
        allow_stop: true,
        ..Opts::default()
    }
}

/// The eight operations of #345: two snapshots with no commit in them, a second writer, and a
/// restart before the day-long step.
#[test]
fn two_no_commit_snapshots_and_a_second_writer_never_trip_the_age_bound() {
    let ops = [
        Op::Shutdown,
        Op::EnvFault(lsr_sim::EnvFault::WipeStaging),
        Op::SecondWriter { ticks: 3 },
        Op::Tick,
        Op::RequestSnapshot,
        Op::Tick,
        Op::SecondWriter { ticks: 6 },
        Op::ClockJump(93_600_000),
    ];
    run_with(&ops, &no_conditional_writes());
}

/// A power loss inside a tick drops a staged file whose upload had landed. The staging check
/// looks between ticks and never saw the file, so the boot's refusal of the replica's files
/// is not a refusal without cause: the operator trusts the local database and starts again.
#[test]
fn a_power_loss_inside_a_tick_that_drops_an_uploaded_staged_file_is_a_cause_for_a_refusal() {
    use lsr_core::{SourceOp, StoreError, StoreOp};
    use lsr_sim::generate::{AppAt, AppOp, WalOp};
    use lsr_sim::{CrashKind, OtherProcessMode};
    let ops = [
        Op::Crash(CrashKind::Process),
        Op::CommitInTick { pages: 2 },
        Op::StoreFault {
            target: Target::Staging,
            op: StoreOp::List,
            ahead: 0,
            shape: FaultShape::Error(StoreError::Transient {
                op: StoreOp::List,
                kind: ErrorKind::Other,
            }),
        },
        Op::Crash(CrashKind::Power),
        Op::Tick,
        Op::StoreFault {
            target: Target::Staging,
            op: StoreOp::Upload,
            ahead: 1,
            shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
                op: StoreOp::Upload,
                kind: ErrorKind::ConnectionReset,
            }),
        },
        Op::AppInTick {
            at: AppAt::Call(20),
            op: AppOp::Commit {
                pages: 3,
                grow: false,
            },
        },
        Op::Crash(CrashKind::Process),
        Op::AppInTick {
            at: AppAt::Call(2),
            op: AppOp::Checkpoint(OtherProcessMode::Truncate),
        },
        Op::AppInTick {
            at: AppAt::Before(SourceOp::PageCount),
            op: AppOp::Checkpoint(OtherProcessMode::Truncate),
        },
        Op::WalDamage {
            op: WalOp::TearHeader,
            ticks: 7,
        },
        Op::Crash(CrashKind::Process),
        Op::Crash(CrashKind::Power),
    ];
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        after_restart: lsr_sim::AfterRestart::Nothing,
        overlap: true,
        kill_after_fault: true,
        remote_s3: true,
        no_create_only: true,
        truncate_pages: std::num::NonZeroU32::new(3),
        ..Opts::default()
    };
    match run_ops(Seed::new(0x6b06_248f), &ops, &opts) {
        Ok(_) => {}
        Err(failure) => panic!("{}\n{}", failure.violation, failure.message(failure.steps)),
    }
}

/// The engine's trip over WAL frames it could not read stands after the replica holds every
/// commit, while its own clock still dates the frames: the judge keeps the trip it accepted
/// until the engine lets go (a day-long clock step under a source that fails to open, a
/// checkpoint and a deleted WAL).
#[test]
fn an_accepted_trip_over_unread_frames_stands_until_the_engine_lets_go() {
    use lsr_core::{SourceError, SourceOp};
    use lsr_sim::OtherProcessMode;
    use lsr_sim::generate::{AppAt, AppOp, EnvFault, Hold, WalOp};
    let ops = [
        Op::WalDamage {
            op: WalOp::RegrowSameSalts(16571050836624415656),
            ticks: 3,
        },
        Op::CommitInTick { pages: 2 },
        Op::StoreFault {
            target: Target::Staging,
            op: StoreOp::Upload,
            ahead: 3,
            shape: FaultShape::Rate {
                per_mille: 56,
                err: StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: ErrorKind::ConnectionReset,
                },
            },
        },
        Op::CommitInTick { pages: 2 },
        Op::WalDamage {
            op: WalOp::TearHeader,
            ticks: 1,
        },
        Op::SecondWriter { ticks: 2 },
        Op::ClockJump(114138),
        Op::StoreFault {
            target: Target::Staging,
            op: StoreOp::List,
            ahead: 2,
            shape: FaultShape::Error(StoreError::Transient {
                op: StoreOp::List,
                kind: ErrorKind::ConnectionReset,
            }),
        },
        Op::CommitInTick { pages: 1 },
        Op::CommitInTick { pages: 2 },
        Op::CommitInTick { pages: 3 },
        Op::AppInTick {
            at: AppAt::Call(1),
            op: AppOp::Checkpoint(OtherProcessMode::Restart),
        },
        Op::Hold {
            hold: Hold::CredentialsRevoked,
            ticks: 71,
        },
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Hold {
            hold: Hold::LyingReads,
            ticks: 56,
        },
        Op::Hold {
            hold: Hold::LyingReads,
            ticks: 105,
        },
        Op::CommitInTick { pages: 4 },
        Op::WalDamage {
            op: WalOp::FlipLastFrameBit,
            ticks: 2,
        },
        Op::Tick,
        Op::CommitInTick { pages: 2 },
        Op::StoreFault {
            target: Target::Staging,
            op: StoreOp::Upload,
            ahead: 3,
            shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
                op: StoreOp::Upload,
                kind: ErrorKind::ConnectionReset,
            }),
        },
        Op::Tick,
        Op::EnvFault(EnvFault::WipeStaging),
        Op::CommitInTick { pages: 1 },
        Op::CommitInTick { pages: 1 },
        Op::OtherProcessReader { begin: true },
        Op::ClockJump(65406),
        Op::CommitInTick { pages: 1 },
        Op::CommitInTick { pages: 3 },
        Op::EnvFault(EnvFault::Source(
            SourceOp::Open,
            SourceError::Io(ErrorKind::Other),
        )),
        Op::ClockJump(93600000),
        Op::AppInTick {
            at: AppAt::Call(18),
            op: AppOp::Checkpoint(OtherProcessMode::Passive),
        },
        Op::CommitInTick { pages: 1 },
        Op::AppInTick {
            at: AppAt::Before(SourceOp::EndBarrier),
            op: AppOp::Checkpoint(OtherProcessMode::Truncate),
        },
        Op::CommitInTick { pages: 1 },
        Op::CommitInTick { pages: 4 },
        Op::AppInTick {
            at: AppAt::Before(SourceOp::Sync),
            op: AppOp::DeleteWal,
        },
    ];
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: true,
        after_restart: lsr_sim::AfterRestart::Busy,
        overlap: true,
        remote_s3: true,
        no_create_only: true,
        truncate_pages: std::num::NonZeroU32::new(4),
        ..Opts::default()
    };
    match run_ops(Seed::new(0x6b06_17e5), &ops, &opts) {
        Ok(_) => {}
        Err(failure) => panic!("{}\n{}", failure.violation, failure.message(failure.steps)),
    }
}

/// A rollback of the replica leaves staged files it lacks. The boot reads them whole and uploads
/// them as staged (`docs/decisions/D175.md`), instead of deleting them (the live WAL not vouching
/// for them) and staging a snapshot of their end state (`SnapshotCause::StagedUnverified`),
/// whose uploads were refused past the bound and tripped the engine. Nothing is left to replace.
#[test]
fn seed_0x734_uploads_the_files_a_rollback_left_and_never_trips() {
    let opts = Opts {
        drawn: true,
        after_restart: lsr_sim::AfterRestart::Nothing,
        clock_law: lsr_sim::ClockLaw::PerTick,
        overlap: true,
        error_domain: true,
        staging_fs: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(0x734), crate::histories::seed_0x734::OPS, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("step {}: {}", failure.step, failure.violation),
    };
    assert_eq!(age_trips(&report), 0, "{:?}", report.trips);
}

/// A restart while every read of the replica lies leaves the engine dating a pending staged file
/// from its stamp (the listing hid the replica's copy). The engine trips for it on the first tick
/// after the lie ends, before an honest read could show it the replica holds everything: that is
/// the lie's doing, not a trip the judge may call false.
#[test]
fn a_trip_dated_by_lying_reads_stands_on_the_first_honest_tick() {
    use lsr_sim::generate::Hold;
    let ops = [
        Op::Shutdown,
        Op::ClockJump(93_600_000),
        Op::Hold {
            hold: Hold::LyingReads,
            ticks: 2,
        },
    ];
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: true,
        after_restart: lsr_sim::AfterRestart::Redo,
        clock_law: lsr_sim::ClockLaw::EachRead,
        truncate_pages: std::num::NonZeroU32::new(6),
        no_create_only: true,
        ..Opts::default()
    };
    if let Err(failure) = lsr_sim::run::run_ops(lsr_sim::Seed::new(0x6b06_5d24), &ops, &opts) {
        panic!("{failure}");
    }
}

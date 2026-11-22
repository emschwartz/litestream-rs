//! Fail-stop in named runs (`docs/proposals/fail-stop.md` section 2): the trips that must come
//! (P2, a missed trip; P3, clearing; P5, a crash loop) where each run's truth is exact, and one
//! run for each class of fault that must never trip. The judge of every run
//! (`lsr_sim::failstop`, P1) watches all of them too.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{Denied, StoreError, StoreOp, Trip};
use lsr_ltx::Timestamp;
use lsr_sim::run::run_ops;
use lsr_sim::{FaultShape, Op, Opts, RunReport, Seed, Target};

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
    assert_eq!(
        report.trips.last().map(|(_, trip)| *trip),
        Some(None),
        "cleared: {:?}",
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

/// Seeds whose mixed histories tripped for age with every commit on the replica, each now
/// run to the end clean: a run of lag dated from its start though its commits landed (0x330,
/// 0x9f9, 0x56a), a checkpoint's file that changes no page counted as a commit (0x8ef), a
/// trip that held after its commits landed because a newer frame kept the age above zero
/// (0xa4d), and WAL frames the replica held counted while a doubt snapshot was owed (0x14b).
/// The generator may move on and stop reaching these states; the named tests in
/// `lsr-core`'s `actor::tests::lag` pin each mechanism without it.
#[test]
fn the_reported_false_trip_seeds_run_clean() {
    for (seed, known_wrong) in [
        (0x330, true),
        (0x9f9, false),
        (0x9f9, true),
        (0x56a, true),
        (0xa4d, true),
        (0x8ef, true),
        (0x14b, false),
    ] {
        let seed = Seed::new(seed);
        let mut weights = lsr_sim::Weights::mixed(seed);
        weights.known_wrong = known_wrong;
        if let Err(failure) = lsr_sim::run_with(seed, 300, &weights, &Opts::default()) {
            panic!("{}", failure.message(failure.steps));
        }
    }
}

mod seed_0xf4342;

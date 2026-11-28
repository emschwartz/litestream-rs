//! Gray weather (`docs/decisions/D276.md`): a store that is slow or silent rather than failing,
//! and a process that stops inside a tick.
//!
//! The stores answer at once and every fault is an error or a wrong answer, so before this the
//! simulator never saw the engine with a replica call that hung until the client's timeout,
//! with an endpoint that hung every call, with a slow staging disk, or with a clock that jumped
//! between two reads of one tick. Each test below is a short written scenario: the weather, then
//! the run's own judges (every restore, the per-tick bounds, the healthy tail).

use lsr_core::StoreOp;
use lsr_sim::generate::{HttpFault, HttpShape};
use lsr_sim::run::run_ops;
use lsr_sim::{FaultShape, Hold, Op, Opts, RunReport, Seed, Target};

/// Commits and ticks enough for the engine to boot and put a few files.
fn warm() -> Vec<Op> {
    let mut ops = Vec::new();
    for _ in 0..4 {
        ops.push(Op::Commit {
            pages: 2,
            grow: true,
        });
        ops.push(Op::Tick);
        ops.push(Op::Tick);
    }
    ops
}

/// `warm`, then `weather`, then commits and ticks under it, and a restore.
fn through(weather: Op) -> Vec<Op> {
    let mut ops = warm();
    // Work for the weather's tick, so it reads the clock and calls the stores.
    ops.push(Op::Commit {
        pages: 2,
        grow: false,
    });
    ops.push(weather);
    for _ in 0..4 {
        ops.push(Op::Commit {
            pages: 1,
            grow: false,
        });
        ops.push(Op::Tick);
        ops.push(Op::Tick);
    }
    ops.push(Op::Restore);
    ops
}

fn passes(ops: &[Op], opts: &Opts) -> RunReport {
    match run_ops(Seed::new(11), ops, opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    }
}

/// The client's timeout is the S3 store's own default deadline, the one lsr gives every S3
/// replica it is not told otherwise about.
#[test]
fn a_hang_costs_the_s3_clients_default_deadline() {
    assert_eq!(
        lsr_sim::remote::client_timeout(),
        std::time::Duration::from_secs(60)
    );
    assert_eq!(
        lsr_sim::store::timed_out(StoreOp::Upload),
        lsr_core::StoreError::Transient {
            op: StoreOp::Upload,
            kind: std::io::ErrorKind::TimedOut,
        }
    );
}

/// A replica call of each operation hangs until the client gives up: the tick takes the
/// timeout, the call fails as timed out, and every judge holds.
#[test]
fn a_replica_call_that_hangs_fails_as_timed_out_a_minute_later() {
    for op in [
        StoreOp::List,
        StoreOp::Read,
        StoreOp::Upload,
        StoreOp::Delete,
    ] {
        let ops = through(Op::StoreFault {
            target: Target::Remote,
            op,
            ahead: 0,
            shape: FaultShape::Hang,
        });
        let report = passes(&ops, &Opts::default());
        if report.reach.get("gray.hang.calls") > 0 {
            assert!(report.reach.get("gray.tick.over_a_minute") > 0, "{op:?}");
        }
    }
    let ops = through(Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::Hang,
    });
    let report = passes(&ops, &Opts::default());
    assert_eq!(report.reach.get("gray.hang.calls"), 1);
    assert!(report.reach.get("gray.tick.over_a_minute") > 0);
}

/// A commit published, and its answer never came before the timeout: the engine must take the
/// timeout as a write that may have landed, and the replica stays restorable.
#[test]
fn a_commit_that_hangs_after_it_published_leaves_the_replica_restorable() {
    let ops = through(Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::HangAfterPublish,
    });
    let report = passes(&ops, &Opts::default());
    assert_eq!(report.reach.get("gray.hang.calls"), 1);
}

/// The HTTP request itself times out on the S3 replica: the store's own client fails it, and
/// the tick spends the timeout.
#[test]
fn an_s3_request_that_times_out_costs_the_timeout() {
    let opts = Opts {
        remote_s3: true,
        ..Opts::default()
    };
    let ops = through(Op::HttpFault(HttpFault::Hang(HttpShape::Put)));
    let report = passes(&ops, &opts);
    assert!(report.reach.get("gray.tick.over_a_minute") > 0);
}

/// An endpoint that takes every upload and never answers: replication halts, and health must
/// say so with a degraded task, never a silent run of retries each a minute long.
#[test]
fn a_store_that_hangs_every_upload_is_reported_degraded() {
    let mut ops = warm();
    ops.push(Op::Hold {
        hold: Hold::Hang(StoreOp::Upload),
        ticks: 40,
    });
    ops.push(Op::Restore);
    let report = passes(&ops, &Opts::default());
    assert_eq!(report.reach.get("hold.hang.Upload.judged"), 2);
    assert!(
        report.reach.get("hold.hang.Upload.halted") > 0,
        "the halt was judged: {:?}",
        report.reach.get("gray.hang.calls")
    );
}

/// A staging disk a thousand times slower than its price: every tick's staging calls take that
/// much longer, nothing fails, and the run's judges hold through it and after it.
#[test]
fn a_slow_staging_disk_slows_the_clock_and_breaks_nothing() {
    let end = |factor: u16| {
        let mut ops = warm();
        ops.push(Op::Hold {
            hold: Hold::SlowStaging(factor),
            ticks: 8,
        });
        ops.push(Op::Restore);
        let opts = Opts {
            record: true,
            ..Opts::default()
        };
        let report = passes(&ops, &opts);
        assert_eq!(report.reach.get("hold.SlowStaging.judged"), 2);
        assert_eq!(report.reach.get("gray.slow_staging.ticks"), 8);
        report
            .seen
            .iter()
            .filter_map(|seen| {
                if let lsr_sim::run::Seen::Tick { at, .. } = seen {
                    Some(*at)
                } else {
                    None
                }
            })
            .max()
    };
    assert!(end(1000) > end(10), "a slower disk takes longer");
}

/// The process stops for more than a day between two clock reads of one tick, past the
/// snapshot interval and the retention window: every restore, the retention judges and the
/// healthy tail must hold, at every read the pause can fall before.
#[test]
fn a_pause_past_a_day_inside_a_tick_keeps_every_restore_exact() {
    for read in 1..=6 {
        let ops = through(Op::Pause {
            read,
            millis: 25 * 3_600_000,
        });
        let opts = Opts {
            drawn: true,
            ..Opts::default()
        };
        let report = passes(&ops, &opts);
        if report.reach.get("gray.pause.fired") > 0 {
            assert!(report.reach.get("gray.pause.past_a_day") > 0, "{read}");
        }
    }
    let report = passes(
        &through(Op::Pause {
            read: 1,
            millis: 25 * 3_600_000,
        }),
        &Opts::default(),
    );
    assert_eq!(report.reach.get("gray.pause.fired"), 1);
}

/// A run with the latency tail is charged, and some of its requests take ten times their
/// price or more.
#[test]
fn a_latency_tail_charges_the_run_and_reaches_its_tail() {
    let mut ops = warm();
    ops.extend(warm());
    ops.extend(warm());
    let opts = Opts {
        latency_tail: true,
        ..Opts::default()
    };
    let report = passes(&ops, &opts);
    assert!(report.reach.get("gray.latency.tenfold") > 0);
}

/// A boot lists the replica while every list hangs: each attempt costs the client's timeout
/// inside one tick, and the engine's age trip comes in a tick whose hung calls carried its
/// clock past the ten-minute bound. The failure that justifies the trip came after the tick
/// began, so the fail-stop judge dates the tick where its reads ended; dated at its start, it
/// called the trip one with no failure after the bound.
#[test]
fn an_age_trip_inside_a_hung_tick_is_dated_where_the_tick_ended() {
    let ops = [
        Op::Shutdown,
        Op::Shrink { pages: 1 },
        Op::CommitInTick { pages: 1 },
        Op::AppInTick {
            at: lsr_sim::AppAt::Before(lsr_core::SourceOp::Sync),
            op: lsr_sim::AppOp::ReaderEnd,
        },
        Op::Tick,
        Op::Hold {
            hold: Hold::Hang(StoreOp::List),
            ticks: 62,
        },
    ];
    let opts = Opts {
        drawn: true,
        overlap: true,
        no_create_only: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(0xf4401), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        report
            .trips
            .iter()
            .any(|(_, trip)| matches!(trip, Some(lsr_core::Trip::UnreplicatedTooLong { .. }))),
        "the hang tripped for age: {:?}",
        report.trips
    );
}

/// A drain outlives its grace because calls hung before it, while the app holds a spilled
/// transaction. The operator runs the engine until it is idle and asks again, but with the
/// write lock held the engine's checkpoint is deferred every second and it is never idle:
/// the harness waited for an idle tick that could not come. A deferred checkpoint is as idle as the engine gets until the app lets go.
#[test]
fn a_drain_past_its_grace_under_a_spill_is_asked_again() {
    use lsr_core::{SourceError, SourceOp};
    let ops = [
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Delete,
            ahead: 0,
            shape: FaultShape::Hang,
        },
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Upload,
            ahead: 0,
            shape: FaultShape::Hang,
        },
        Op::Shutdown,
        Op::Hold {
            hold: Hold::Hang(StoreOp::Upload),
            ticks: 33,
        },
        Op::Shutdown,
        Op::Spill,
        Op::EnvFault(lsr_sim::EnvFault::Source(
            SourceOp::Committed,
            SourceError::Busy,
        )),
        Op::Shutdown,
    ];
    let opts = Opts {
        after_restart: lsr_sim::AfterRestart::Busy,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x126d_6928_8c4d), &ops, &opts) {
        panic!("{failure}");
    }
}

//! A staged file sent a slice a tick, in short explicit scenarios: what the history does
//! between two slices (a commit, a crash, a power loss, a store fault, a drain, damage to the
//! staged file) and what the engine makes of the file in flight. Every run ends with the drain
//! and the judges of any other run, so the replica restores every transaction afterwards.

use std::num::NonZeroU64;

use lsr_core::{Did, Event, Fault, SnapshotCause, StoreError, StoreOp};
use lsr_sim::generate::MetaOp;
use lsr_sim::run::{Seen, run_ops};
use lsr_sim::sqlite::CrashKind;
use lsr_sim::{FaultShape, Op, Opts, RunReport, Seed, Target};

/// A tick's bytes. The first file (the snapshot of the eight-page database) is 2,940 bytes:
/// 22 slices and the tick that ends it.
const SLICE: u64 = 128;

fn run(ops: &[Op]) -> RunReport {
    let opts = Opts {
        slice_bytes: NonZeroU64::new(SLICE),
        record: true,
        ..Opts::default()
    };
    run_ops(Seed::new(1), ops, &opts)
        .map_err(|failure| failure.violation.to_string())
        .unwrap()
}

/// The events of the run's ticks, in order.
fn events(report: &RunReport) -> Vec<&Event> {
    report
        .seen
        .iter()
        .filter_map(|seen| {
            if let Seen::Event(event) = seen {
                Some(event)
            } else {
                None
            }
        })
        .collect()
}

/// The bytes each slice tick reported, in order; a file published in between is a zero.
fn sent(report: &RunReport) -> Vec<u64> {
    events(report)
        .into_iter()
        .filter_map(|event| {
            if let Event::Sending { bytes } = event {
                Some(*bytes)
            } else {
                matches!(event, Event::Uploaded(Did::Put(_))).then_some(0)
            }
        })
        .collect()
}

/// A commit, the boot, the sync that stages the first file, and its first three slices.
fn in_flight() -> Vec<Op> {
    let mut ops = vec![Op::Commit {
        pages: 3,
        grow: true,
    }];
    ops.extend(ticks(5));
    ops
}

fn ticks(n: usize) -> impl Iterator<Item = Op> {
    std::iter::repeat_n(Op::Tick, n)
}

/// Every slice of the first file, to the tick that publishes it.
fn whole_file() -> Vec<u64> {
    (1..=22_u64)
        .map(|n| n.saturating_mul(SLICE))
        .chain([0])
        .collect()
}

fn reset() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Upload,
        kind: std::io::ErrorKind::ConnectionReset,
    }
}

/// The application commits before every tick while the first file is in flight. The WAL step
/// and the slices take turns, so each commit is staged by the tick after it or the one after
/// that, checkpoints run, and the file takes the slices it takes with no commits at all.
/// Thirty commits of two frames would leave 60 frames in a WAL no checkpoint ran on.
#[test]
fn commits_between_slices_are_staged_and_checkpointed_while_the_file_goes_up() {
    let mut ops = in_flight();
    for _ in 0..30 {
        ops.push(Op::Commit {
            pages: 2,
            grow: false,
        });
        ops.push(Op::Tick);
    }
    let report = run(&ops);
    let all = events(&report);
    let during = &all[5..35];
    let slice = |event: &&Event| matches!(event, Event::Sending { .. });
    assert!(
        during
            .chunks(2)
            .all(|pair| matches!(pair, [first, second] if !slice(first) && slice(second))),
        "{during:?}"
    );
    let checkpoints = during
        .iter()
        .filter(|event| matches!(event, Event::Checkpointed { .. }))
        .count();
    assert!(checkpoints >= 5, "{during:?}");
    assert!(
        report.max_wal_frames <= 16,
        "{} frames",
        report.max_wal_frames
    );
    assert_eq!(sent(&report)[..23], whole_file());
    assert_eq!(report.reach.get("send.commit_between_slices"), 15);
    assert_eq!(report.reach.get("send.turns.judged"), 15);
    assert_eq!(report.reach.get("send.turns.excused"), 0);
}

/// The process dies, or the power goes, with a file in flight: nothing of it is on the
/// replica, the next process removes what its upload left and sends it from its first byte.
#[test]
fn a_crash_between_two_slices_sends_the_file_again_from_its_first_byte() {
    for kind in [CrashKind::Process, CrashKind::Power] {
        let mut ops = in_flight();
        ops.push(Op::Crash(kind));
        ops.extend(ticks(40));
        ops.push(Op::Restore);
        let report = run(&ops);
        let mut want = vec![SLICE, 2 * SLICE, 3 * SLICE];
        want.extend(whole_file());
        assert_eq!(sent(&report)[..want.len()], want, "{kind:?}");
        assert_eq!(report.crashes, 1);
        assert_eq!(report.reach.get("send.then_the_process_ended"), 1);
    }
}

/// A write of a slice fails: the upload task waits out its retry and the file goes up from
/// its first byte, into a new upload.
#[test]
fn a_store_fault_between_two_slices_sends_the_file_again_from_its_first_byte() {
    let mut ops = in_flight();
    ops.push(Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::Error(reset()),
    });
    ops.extend(ticks(40));
    ops.push(Op::Restore);
    let report = run(&ops);
    let mut want = vec![SLICE, 2 * SLICE, 3 * SLICE];
    want.extend(whole_file());
    assert_eq!(sent(&report)[..want.len()], want);
    assert!(events(&report).iter().any(|event| matches!(
        event,
        Event::Retrying {
            fault: Fault::StoreTransient,
            ..
        }
    )));
    assert_eq!(report.reach.get("send.then_a_fault"), 1);
}

/// A drain asked for with a file in flight goes on sending it, from where it was, and ends
/// once it is published.
#[test]
fn a_drain_finishes_the_file_in_flight() {
    let mut ops = in_flight();
    ops.push(Op::Shutdown);
    ops.extend(ticks(10));
    ops.push(Op::Restore);
    let report = run(&ops);
    assert_eq!(sent(&report)[..23], whole_file());
    assert_eq!(report.shutdowns, 1);
    let all = events(&report);
    let published = all
        .iter()
        .position(|event| matches!(event, Event::Uploaded(Did::Put(_))));
    let next_boot = all
        .iter()
        .skip(1)
        .position(|event| matches!(event, Event::Booted { .. }));
    assert!(published < next_boot.map(|at| at + 1), "{all:?}");
}

/// The staged file changes, or goes, under its send. The bytes already sent were verified on
/// their own reads, the reads after them refuse the file, and its upload is aborted: nothing
/// of it is published. A second refusal drops it, and the snapshot that takes its place goes
/// up from its own first byte.
#[test]
fn a_staged_file_damaged_under_its_send_is_never_published() {
    for damage in [MetaOp::FlipNewestByte, MetaOp::DeleteAll] {
        let mut ops = in_flight();
        ops.push(Op::Meta(damage));
        ops.extend(ticks(100));
        ops.push(Op::Restore);
        let report = run(&ops);
        let all = events(&report);
        let at = |want: fn(&Event) -> bool| all.iter().position(|event| want(event));
        let refused = at(|event| {
            matches!(
                event,
                Event::Retrying {
                    fault: Fault::StagedLost,
                    ..
                }
            )
        });
        let dropped = at(|event| {
            matches!(
                event,
                Event::Resnapshot(SnapshotCause::NewestStagedFileUnreadable)
            )
        });
        let published = at(|event| matches!(event, Event::Uploaded(Did::Put(_))));
        assert!(refused.is_some(), "{damage:?}: {all:?}");
        assert!(
            refused < dropped && dropped < published,
            "{damage:?}: {all:?}"
        );
        assert_eq!(report.reach.get("send.then_a_fault"), 1, "{damage:?}");
    }
}

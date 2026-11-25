//! A staged file sent a slice a tick, in short explicit scenarios: what the history does
//! between two slices (a commit, a crash, a power loss, a store fault, a drain, damage to the
//! staged file) and what the engine makes of the file in flight. Every run ends with the drain
//! and the judges of any other run, so the replica restores every transaction afterwards.

use std::num::NonZeroU64;

use lsr_core::{Did, Event, Fault, MaintainedEvent, SnapshotCause, StopCause, StoreError, StoreOp};
use lsr_sim::generate::MetaOp;
use lsr_sim::run::{Seen, run_ops};
use lsr_sim::sqlite::CrashKind;
use lsr_sim::{FaultShape, Op, Opts, RunReport, Seed, Target};

/// A tick's bytes. The first file (the snapshot of the eight-page database) is 2,940 bytes:
/// 22 slices and the tick that ends it.
const SLICE: u64 = 128;

fn run(ops: &[Op]) -> RunReport {
    run_sliced(ops, SLICE)
}

/// The run with `slice` bytes of staged files a tick.
fn run_sliced(ops: &[Op], slice: u64) -> RunReport {
    let opts = Opts {
        slice_bytes: NonZeroU64::new(slice),
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
/// has the tick after each eight slices, so every commit is staged within eight slices of
/// landing, checkpoints run, and the file takes the slices it takes with no commits at all.
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
    // Three slices went before the first commit: five more, the WAL step's tick, eight, its
    // tick again, and the six that are left.
    let turns: Vec<usize> = (0..21).filter(|at| !slice(&during[*at])).collect();
    assert_eq!(turns, [5, 14], "{during:?}");
    let checkpoints = during
        .iter()
        .filter(|event| matches!(event, Event::Checkpointed { .. }))
        .count();
    assert!(checkpoints >= 2, "{during:?}");
    // Nine commits of two frames land between two of the WAL step's turns.
    assert!(
        report.max_wal_frames <= 20,
        "{} frames",
        report.max_wal_frames
    );
    assert_eq!(sent(&report)[..23], whole_file());
    assert_eq!(report.reach.get("send.commit_between_slices"), 24);
    assert_eq!(report.reach.get("send.turns.judged"), 24);
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

/// A staged file is damaged after the last byte of it was read for its send and before the
/// tick that publishes it. Every byte sent was verified on its own read, so the file is
/// published as it was staged: the replica's copy is whole and the staged one is not. No read
/// of the upload met the damage, so the upload does not put the edit behind the engine. The
/// next process finds its newest staged file unreadable and drops it, which leaves staging
/// behind the replica, and snapshots above the replica's copy: that snapshot has the edit for
/// its cause. Before, the judge forgot the edit at the upload and called the snapshot needless.
#[test]
fn a_staged_file_damaged_after_its_last_read_is_published_whole_and_costs_a_snapshot() {
    // A tick's bytes are the size of the file the first checkpoint's frame makes, the second
    // staged file: one tick reads it whole, the next publishes it.
    let second: u64 = 660;
    let mut ops = vec![Op::Commit {
        pages: 3,
        grow: true,
    }];
    // The first file goes up in five ticks, the snapshot follows, and the checkpoint the idle
    // engine runs stages the second file, beside the first. The twelfth tick reads it.
    ops.extend(ticks(12));
    ops.push(Op::Meta(MetaOp::FlipNewestByte));
    // The publish. The process ends before anything prunes the staged file.
    ops.extend([Op::Tick, Op::Restart]);
    ops.extend(ticks(20));
    ops.push(Op::Restore);
    let report = run_sliced(&ops, second);
    let all = events(&report);
    assert_eq!(all[11], &Event::Sending { bytes: second }, "{all:?}");
    assert!(
        matches!(all[12], Event::Uploaded(Did::Put(info)) if info.size == second),
        "{all:?}"
    );
    // The send met no damage; the boot after it did, twice, and snapshots.
    assert!(
        matches!(
            all[13],
            Event::Retrying {
                fault: Fault::StagedLost,
                ..
            }
        ),
        "{all:?}"
    );
    let snapshot = Event::Booted {
        cause: Some(SnapshotCause::NewestStagedFileUnreadable),
    };
    assert!(all[14..].contains(&&snapshot), "{all:?}");
    assert_eq!(
        report
            .reach
            .get("needless_snapshot.forced.NewestStagedFileUnreadable"),
        1
    );
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

/// Another writer takes the id of a file this engine has staged and not sent. Maintenance has
/// its turn before the file is out and lists the replica with the other writer's file on it:
/// not past what the engine staged, and not the engine's own. Nothing is compacted from that
/// listing; the engine's put of the id is refused and it stops. The run's judges then restore
/// every transaction once the other writer's file is gone. Compacted into level 1, the other
/// writer's pages stood in for the commit, which the next boot dropped from staging: the
/// replica restored a database no commit made (the nightly soak found it four times in 7,046
/// seeds).
#[test]
fn another_writers_file_at_the_id_of_an_unsent_file_is_not_compacted() {
    let commit = Op::Commit {
        pages: 1,
        grow: false,
    };
    // Sixteen bytes a tick. The first file goes up, and maintenance settles after it.
    let mut ops = in_flight();
    ops.extend(ticks(400));
    // A commit, and the checkpoint the idle time made due, whose own frame is a file too: the
    // 54th tick publishes the second of them, and no maintenance pass follows that put.
    ops.push(commit.clone());
    ops.extend(ticks(54));
    // The next commit is in the WAL, not yet staged, when the other writer takes its id.
    ops.push(commit);
    ops.push(Op::SecondWriter { ticks: 60 });
    ops.extend(ticks(40));
    ops.push(Op::Restore);
    let report = run_sliced(&ops, 16);
    assert_eq!(report.reach.get("remote.second_writer"), 1);
    let seen: Vec<&Seen> = report
        .seen
        .iter()
        .filter(|seen| !matches!(seen, Seen::Tick { .. }))
        .collect();
    let stopped = seen
        .iter()
        .position(|seen| **seen == Seen::Stop(StopCause::OtherWriter))
        .expect("the engine never stopped for the other writer");
    let staged = seen[..stopped]
        .iter()
        .rposition(|seen| matches!(seen, Seen::Event(Event::Synced { .. })))
        .expect("the commit was never staged");
    // From the sync that staged the file to the stop: the turn maintenance had, and slices.
    let unsent = &seen[staged + 1..stopped];
    let maintained: Vec<&MaintainedEvent> = unsent
        .iter()
        .filter_map(|seen| {
            if let Seen::Event(Event::Maintained { event, .. }) = seen {
                Some(event)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        maintained,
        [&MaintainedEvent::Waiting { held: None }],
        "{unsent:?}"
    );
    assert!(
        unsent
            .iter()
            .any(|seen| matches!(seen, Seen::Event(Event::Sending { .. }))),
        "{unsent:?}"
    );
}

/// A first file sent over many ticks, with the clock passing the levels' windows meanwhile: the
/// levels wait for the first snapshot, which is built from the file once it is published, so
/// no level 1, 2 or 3 file is put before the snapshot is.
#[test]
fn a_first_file_in_flight_does_not_let_the_levels_run_before_the_snapshot() {
    let mut ops = in_flight();
    // A maintenance turn between the slices lists an empty replica, and the clock then passes
    // every level's window before the file is whole.
    ops.push(Op::ClockJump(40_000));
    ops.extend(ticks(12));
    ops.push(Op::ClockJump(40_000));
    ops.extend(ticks(40));
    let report = run(&ops);
    let puts: Vec<u8> = report
        .remote_events
        .iter()
        .filter_map(|event| match event {
            lsr_sim::RecorderEvent::Put { key, .. } => Some(key.level().get()),
            lsr_sim::RecorderEvent::Delete { .. } => None,
        })
        .collect();
    let snapshot = puts.iter().position(|level| *level == 9);
    let first_level = puts.iter().position(|level| (1..=3).contains(level));
    assert!(snapshot.is_some(), "no snapshot: {puts:?}");
    assert!(
        first_level.is_none_or(|level| snapshot < Some(level)),
        "a level was written before the first snapshot: {puts:?}"
    );
}

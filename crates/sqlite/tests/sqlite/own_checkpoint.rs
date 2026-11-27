//! The engine's own checkpoint against an application that is writing, on real SQLite.
//!
//! A PASSIVE holds the write lock only from its barrier to the barrier's release, and the
//! sequence bump comes after. A writer that was waiting for the lock takes it in between, so
//! the bump is refused, and that writer's commit then restarts the WAL the PASSIVE just copied
//! whole. A writer may also hold the lock at the moment the barrier asks for it. Neither is a
//! failure, and neither may cost a snapshot: the read transaction retaken under the barrier
//! pins the position, and the restart is followed.
//!
//! The engine's backend is [`Hooked`], so the application acts at a chosen call of the
//! engine's, which no thread timing could place.

use std::cell::RefCell;
use std::num::NonZeroU32;
use std::rc::Rc;

use lsr_core::{CheckpointConfig, CheckpointMode, Event, SnapshotCause, SourceOp, Tick, WalSource};
use lsr_sqlite::testing::{Rig, RigConfig};
use rusqlite::Connection;
use tempfile::TempDir;

use crate::truncate_proof::{HOOK, Hooked, connect};

/// What the application's second connection did at the engine's calls.
#[derive(Debug, Default)]
struct Seen {
    /// Write transactions it began at the chosen call.
    began: u32,
    /// Those it committed, at the engine's next call.
    committed: u32,
    /// Those it rolled back there.
    rolled_back: u32,
    /// Whether it holds the write lock now.
    holding: bool,
    /// Whether it holds a read transaction now.
    reading: bool,
    /// Whether a barrier was taken since the last sequence bump: the bump is a PASSIVE's.
    barrier: bool,
}

/// How the application ends the write transaction it holds, at the engine's next call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End {
    Commit,
    Rollback,
}

/// Arms the hook: the application takes the write lock right before the engine's call `at`
/// (when `when` allows it) and ends its transaction right before the call after.
fn arm(
    writer: Connection,
    at: SourceOp,
    end: End,
    when: impl Fn(&Seen) -> bool + 'static,
) -> Rc<RefCell<Seen>> {
    let seen = Rc::new(RefCell::new(Seen::default()));
    let state = Rc::clone(&seen);
    HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |op: SourceOp| {
            let mut seen = state.borrow_mut();
            if seen.holding {
                seen.holding = false;
                match end {
                    End::Commit => {
                        writer.execute_batch("COMMIT").unwrap();
                        seen.committed = seen.committed.saturating_add(1);
                    }
                    End::Rollback => {
                        writer.execute_batch("ROLLBACK").unwrap();
                        seen.rolled_back = seen.rolled_back.saturating_add(1);
                    }
                }
            }
            if op == at && when(&seen) {
                writer
                    .execute_batch("BEGIN IMMEDIATE; INSERT INTO t(v) VALUES (randomblob(700))")
                    .unwrap();
                seen.holding = true;
                seen.began = seen.began.saturating_add(1);
            }
            match op {
                SourceOp::BeginBarrier => seen.barrier = true,
                SourceOp::BumpSeq => seen.barrier = false,
                SourceOp::Open
                | SourceOp::Len
                | SourceOp::Committed
                | SourceOp::Sync
                | SourceOp::PageCount
                | SourceOp::ReadPage
                | SourceOp::EndBarrier
                | SourceOp::Checkpoint
                | SourceOp::BeginWriteLock
                | SourceOp::EndWriteLock => {}
            }
        }));
    });
    seen
}

fn disarm() {
    HOOK.with(|hook| *hook.borrow_mut() = None);
}

fn rig(dir: &TempDir, checkpoint: CheckpointConfig) -> Rig<Hooked> {
    let mut cfg = RigConfig::default();
    cfg.actor.checkpoint = checkpoint;
    let mut rig = Rig::new(dir.path(), cfg).unwrap();
    rig.settle().unwrap();
    rig
}

fn snapshots(rig: &Rig<Hooked>, from: usize) -> Vec<SnapshotCause> {
    rig.ticks()
        .iter()
        .skip(from)
        .filter_map(|tick| match tick {
            Tick::Ran {
                event: Event::Resnapshot(cause),
                ..
            } => Some(*cause),
            Tick::Ran { .. }
            | Tick::Stopped(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_) => None,
        })
        .collect()
}

fn checkpoints(rig: &Rig<Hooked>, from: usize, mode: CheckpointMode) -> usize {
    rig.ticks()
        .iter()
        .skip(from)
        .filter(|tick| {
            matches!(
                tick,
                Tick::Ran {
                    event: Event::Checkpointed { mode: ran, .. },
                    ..
                } if *ran == mode
            )
        })
        .count()
}

fn rounds(rig: &mut Rig<Hooked>, rounds: u32) -> std::collections::BTreeSet<(u32, u32)> {
    let mut generations = std::collections::BTreeSet::new();
    for _ in 0..rounds {
        for _ in 0..4 {
            rig.insert(5000).unwrap();
        }
        rig.settle().unwrap();
        let salts = rig.backend().unwrap().committed().unwrap().end().salts();
        generations.insert((salts.salt1, salts.salt2));
    }
    generations
}

/// A writer takes the write lock between the PASSIVE's barrier release and its sequence bump,
/// every time, and commits right after: the bump is refused and the commit restarts the WAL
/// the PASSIVE copied whole. The engine follows each restart. Before, the refused bump failed
/// the checkpoint with the position unpinned, and every such restart cost a snapshot of the
/// whole database, a retry with a doubling wait, and in the end a degraded checkpoint task.
#[test]
fn a_writer_that_takes_the_lock_before_the_bump_costs_no_snapshot() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig(
        &dir,
        CheckpointConfig {
            interval: None,
            min_pages: NonZeroU32::new(8),
            truncate_pages: None,
        },
    );
    let writer = connect(rig.db(), rig.vfs().name());
    let from = rig.ticks().len();
    let seen = arm(writer, SourceOp::BumpSeq, End::Commit, |_| true);
    let generations = rounds(&mut rig, 12);
    disarm();
    rig.settle().unwrap();
    let seen = seen.borrow();
    assert!(
        seen.began >= 3 && seen.committed == seen.began,
        "the writer overtook the bump: {seen:?}"
    );
    assert!(
        generations.len() >= 3,
        "its commits restarted the WAL: {generations:?}"
    );
    assert_eq!(snapshots(&rig, from), vec![], "no snapshot is owed");
    assert_eq!(rig.faults(), &[], "a refused bump is not a failure");
    assert_eq!(rig.degraded(), &[], "nothing degrades");
    assert!(
        checkpoints(&rig, from, CheckpointMode::Passive) >= 3,
        "the checkpoints ran"
    );
    assert_eq!(
        rig.actor().app_checkpoints().total(),
        0,
        "the application checkpointed nothing"
    );
    let checked = rig.check().unwrap();
    assert_eq!(checked.latest, rig.acked(), "{checked:?}");
}

/// A writer holds the write lock every time the PASSIVE asks for its barrier, more times in a
/// row than a failing task is given before it is called stalled, and lets go without
/// committing. The checkpoint waits and asks again; it fails nothing, degrades nothing, and
/// runs once the lock is free.
#[test]
fn a_write_lock_held_at_every_barrier_defers_the_checkpoint_and_fails_nothing() {
    let held = lsr_core::STALLED_AFTER.get() + 4;
    let dir = TempDir::new().unwrap();
    let mut rig = rig(
        &dir,
        CheckpointConfig {
            interval: None,
            min_pages: NonZeroU32::new(8),
            truncate_pages: None,
        },
    );
    let writer = connect(rig.db(), rig.vfs().name());
    let from = rig.ticks().len();
    let seen = arm(writer, SourceOp::BeginBarrier, End::Rollback, move |seen| {
        seen.began < held
    });
    let _generations = rounds(&mut rig, 3);
    disarm();
    let seen = seen.borrow();
    assert_eq!(
        (seen.began, seen.rolled_back),
        (held, held),
        "the writer held the lock at each barrier"
    );
    assert_eq!(rig.faults(), &[], "a busy write lock is not a failure");
    assert_eq!(rig.degraded(), &[], "nothing degrades");
    assert_eq!(snapshots(&rig, from), vec![], "no snapshot is owed");
    assert!(
        checkpoints(&rig, from, CheckpointMode::Passive) >= 1,
        "the checkpoint ran once the lock was free"
    );
    let checked = rig.check().unwrap();
    assert_eq!(checked.latest, rig.acked(), "{checked:?}");
}

/// Arms the hook for a TRUNCATE whose bump is overtaken: a reader at the WAL's end keeps the
/// threshold PASSIVE from copying everything, so the TRUNCATE comes due; the reader ends at the
/// next barrier, the TRUNCATE's own; and the writer takes the write lock right before the
/// TRUNCATE's bump (the bump after two checkpoint calls), once, and commits at the engine's
/// next call.
fn arm_truncate_overtaken(writer: Connection, reader: Connection) -> Rc<RefCell<Seen>> {
    let seen = Rc::new(RefCell::new(Seen::default()));
    let state = Rc::clone(&seen);
    let (mut reads, mut calls) = (0_u32, 0_u32);
    HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |op: SourceOp| {
            let mut seen = state.borrow_mut();
            if seen.holding {
                seen.holding = false;
                writer.execute_batch("COMMIT").unwrap();
                seen.committed = seen.committed.saturating_add(1);
            }
            match op {
                SourceOp::BeginBarrier => {
                    calls = 0;
                    match reads {
                        0 => reader
                            .execute_batch("BEGIN; SELECT count(*) FROM t")
                            .unwrap(),
                        1 => reader.execute_batch("COMMIT").unwrap(),
                        _ => {}
                    }
                    seen.reading = reads == 0;
                    reads = reads.saturating_add(1).min(2);
                }
                SourceOp::Checkpoint => calls = calls.saturating_add(1),
                SourceOp::BumpSeq => {
                    if calls == 2 && seen.began == 0 {
                        writer
                            .execute_batch(
                                "BEGIN IMMEDIATE; INSERT INTO t(v) VALUES (randomblob(700))",
                            )
                            .unwrap();
                        seen.holding = true;
                        seen.began = seen.began.saturating_add(1);
                    }
                }
                SourceOp::Open
                | SourceOp::Len
                | SourceOp::Committed
                | SourceOp::Sync
                | SourceOp::PageCount
                | SourceOp::ReadPage
                | SourceOp::EndBarrier
                | SourceOp::BeginWriteLock
                | SourceOp::EndWriteLock => {}
            }
        }));
    });
    seen
}

/// A TRUNCATE empties the WAL, and a writer takes the write lock before the sequence bump: the
/// generation that writer starts is followed or, when nothing proves it continues the sealed
/// one, snapshotted; the engine fails nothing and the replica restores every commit.
#[test]
fn a_truncate_overtaken_at_its_bump_fails_nothing() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig(
        &dir,
        CheckpointConfig {
            interval: None,
            min_pages: None,
            truncate_pages: NonZeroU32::new(8),
        },
    );
    let writer = connect(rig.db(), rig.vfs().name());
    let reader = connect(rig.db(), rig.vfs().name());
    let from = rig.ticks().len();
    let seen = arm_truncate_overtaken(writer, reader);
    let _generations = rounds(&mut rig, 3);
    disarm();
    rig.settle().unwrap();
    let seen = seen.borrow();
    assert_eq!(
        (seen.began, seen.committed),
        (1, 1),
        "the writer overtook the TRUNCATE's bump: {seen:?}"
    );
    assert!(
        checkpoints(&rig, from, CheckpointMode::Truncate) >= 1,
        "the TRUNCATE ran"
    );
    assert_eq!(rig.faults(), &[], "a refused bump is not a failure");
    // The writer's first frame after the TRUNCATE may carry fresh salts (SQLite picks new ones
    // when that connection has run no checkpoint itself), and then nothing proves the new
    // generation continues the sealed one: at most that one snapshot.
    let taken = snapshots(&rig, from);
    assert!(
        taken
            .iter()
            .all(|cause| *cause == SnapshotCause::GenerationChanged)
            && taken.len() <= 1,
        "{taken:?}"
    );
    let checked = rig.check().unwrap();
    assert_eq!(checked.latest, rig.acked(), "{checked:?}");
}

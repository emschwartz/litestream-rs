use lsr_core::{CheckpointMode, Checkpointer, WalSource};
use lsr_ltx::{PageNo, PageSize};

use super::*;

fn ps() -> PageSize {
    PageSize::new(512).unwrap()
}

fn tx(pgnos: &[u32], fill: u8) -> Vec<(PageNo, Vec<u8>)> {
    pgnos
        .iter()
        .map(|n| (PageNo::new(*n, ps()).unwrap(), vec![fill; 512]))
        .collect()
}

fn db() -> SimSqlite {
    let db = SimSqlite::new(ps(), 4, 5);
    db.commit(&tx(&[1, 2], 1), 4).unwrap();
    db
}

#[test]
fn a_commit_armed_at_a_call_lands_right_before_it_and_not_at_the_barrier() {
    let db = db();
    let mut wal = db.source();
    let mut ck = db.checkpointer();
    db.arm_commit_in_tick(tx(&[3, 5], 2), 5, InTickSite::AtCall, Box::new(|| (0, 0)));
    db.app_tick_begins();
    db.arm_app_in_tick(InTickAt::Call(3), InTickOp::Commit);
    let first = wal.committed().unwrap().end();
    // The barrier is call 1: the commit waits for call 3, the next committed read.
    let barrier = ck.begin_barrier().unwrap();
    assert_eq!(db.mx_frame(), first.frame_count());
    ck.end_barrier(barrier).unwrap();
    let second = wal.committed().unwrap().end();
    assert_eq!(second.frame_count(), first.frame_count() + 2);
    assert_eq!(
        db.db_size(),
        5,
        "a commit inside a tick may grow the database"
    );
    assert_eq!(
        db.take_app_in_tick().map(|ran| ran.done),
        Some(InTickDone::Commit)
    );
    assert!(db.take_commit_in_tick().is_some());
}

#[test]
fn a_commit_armed_while_the_barrier_holds_writers_out_is_refused() {
    let db = db();
    let mut ck = db.checkpointer();
    db.arm_commit_in_tick(tx(&[3], 2), 4, InTickSite::AtCall, Box::new(|| (0, 0)));
    db.app_tick_begins();
    db.arm_app_in_tick(InTickAt::Call(1), InTickOp::Commit);
    let barrier = ck.begin_barrier().unwrap();
    let frames = db.mx_frame();
    ck.end_barrier(barrier).unwrap();
    assert_eq!(db.mx_frame(), frames);
    assert_eq!(
        db.take_app_in_tick().map(|ran| ran.done),
        Some(InTickDone::Commit)
    );
    assert_eq!(db.take_commit_in_tick(), None, "SQLite refused it");
}

#[test]
fn a_tick_with_fewer_calls_runs_nothing_and_the_take_disarms() {
    let db = db();
    let mut wal = db.source();
    db.app_tick_begins();
    db.arm_app_in_tick(InTickAt::Call(5), InTickOp::ReaderBegin);
    wal.committed().unwrap();
    assert_eq!(db.app_calls(), 1);
    assert_eq!(db.take_app_in_tick(), None);
    db.app_tick_begins();
    for _ in 0..8 {
        wal.committed().unwrap();
    }
    assert_eq!(db.take_app_in_tick(), None, "disarmed with the take");
}

#[test]
fn a_reader_begun_inside_a_tick_holds_back_the_engine_s_checkpoint() {
    let db = db();
    let mut wal = db.source();
    let mut ck = db.checkpointer();
    db.app_tick_begins();
    db.arm_app_in_tick(InTickAt::Call(1), InTickOp::ReaderBegin);
    wal.committed().unwrap();
    db.commit(&tx(&[1], 3), 4).unwrap();
    let run = ck.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(
        run.busy,
        "the reader's mark keeps the last commit from being backfilled"
    );
    let Some(InTickRan {
        done: InTickDone::ReaderBegan(id),
        restarted: false,
        emptied: false,
        ..
    }) = db.take_app_in_tick()
    else {
        panic!("the reader began");
    };
    assert!(db.other_process_reader_end(id));
}

#[test]
fn a_truncate_inside_a_tick_says_it_restarted_and_emptied_the_wal() {
    let db = db();
    let mut wal = db.source();
    db.app_tick_begins();
    db.arm_app_in_tick(
        InTickAt::Call(0),
        InTickOp::Checkpoint(OtherProcessMode::Truncate),
    );
    wal.committed().unwrap();
    let ran = db.take_app_in_tick().unwrap();
    assert!(matches!(
        ran.done,
        InTickDone::Checkpointed {
            numbers: Some(CheckpointNumbers { busy: false, .. }),
            synced: true,
        }
    ));
    assert!(ran.restarted && ran.emptied);
}

#[test]
fn the_wal_is_not_deleted_under_the_engine_s_read_transaction() {
    let db = db();
    let mut wal = db.source();
    db.app_tick_begins();
    db.arm_app_in_tick(InTickAt::Call(1), InTickOp::DeleteWal);
    wal.committed().unwrap();
    wal.committed().unwrap();
    assert_eq!(
        db.take_app_in_tick().map(|ran| ran.done),
        Some(InTickDone::WalDeleted(false))
    );
    assert!(db.wal_file().is_some());
}

#[test]
fn an_operation_anchored_to_a_kind_runs_before_the_first_call_of_it() {
    let db = db();
    let mut wal = db.source();
    let mut ck = db.checkpointer();
    db.app_tick_begins();
    db.arm_app_in_tick(
        InTickAt::Before(lsr_core::SourceOp::Checkpoint),
        InTickOp::ReaderBegin,
    );
    wal.committed().unwrap();
    wal.committed().unwrap();
    assert_eq!(db.take_app_in_tick(), None, "no checkpoint yet");
    db.app_tick_begins();
    db.arm_app_in_tick(
        InTickAt::Before(lsr_core::SourceOp::Checkpoint),
        InTickOp::ReaderBegin,
    );
    wal.committed().unwrap();
    db.commit(&tx(&[3], 4), 4).unwrap();
    // The reader begins before the checkpoint does, on a mark below the last commit.
    let run = ck.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(run.busy);
    assert!(matches!(
        db.take_app_in_tick().map(|ran| ran.done),
        Some(InTickDone::ReaderBegan(_))
    ));
}

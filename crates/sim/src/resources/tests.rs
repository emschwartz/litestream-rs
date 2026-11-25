use lsr_core::{Checkpointer, Event};

use super::*;
use crate::sqlite::SimSqlite;
use crate::testkit::ps;

fn stats(event: Option<Event>) -> TickStats {
    TickStats {
        event,
        remote: RecorderCounts::default(),
        staging: RecorderCounts::default(),
        passive: 0,
        checkpoints: 0,
    }
}

const LIMITS: Limits = Limits {
    staged_bytes: 100,
    slice_bytes: 1000,
};

#[test]
fn the_counting_checkpointer_counts_by_mode_and_forwards() {
    let sqlite = SimSqlite::new(ps(), 4, 1);
    let tally = CheckpointTally::default();
    let mut ck = CountingCheckpointer::new(sqlite.checkpointer(), tally.clone());
    ck.checkpoint(CheckpointMode::Passive).unwrap();
    ck.checkpoint(CheckpointMode::Passive).unwrap();
    ck.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(tally.count(CheckpointMode::Passive), 2);
    assert_eq!(tally.count(CheckpointMode::Truncate), 1);
    assert_eq!(tally.total(), 3);
}

#[test]
fn two_passive_checkpoints_in_a_tick_break_the_bound() {
    let mut s = stats(None);
    s.passive = 1;
    per_tick(&s, LIMITS).unwrap();
    s.passive = 2;
    let err = per_tick(&s, LIMITS).unwrap_err();
    assert!(matches!(
        err,
        Violation::ResourceBound {
            what: Bound::PassivePerTick,
            ..
        }
    ));
}

#[test]
fn staging_past_the_limit_breaks_the_bound() {
    let mut s = stats(None);
    s.staging.written_bytes = 101;
    let err = per_tick(&s, LIMITS).unwrap_err();
    assert!(matches!(
        err,
        Violation::ResourceBound {
            what: Bound::StagedBytesPerTick,
            ..
        }
    ));
}

#[test]
fn a_tick_may_list_but_only_so_often() {
    let mut s = stats(Some(Event::Synced { files: 1 }));
    s.remote.lists = 1;
    per_tick(&s, LIMITS).unwrap();
    s.remote.lists = LIST_CALLS_PER_TICK + 1;
    assert!(per_tick(&s, LIMITS).is_err());
}

#[test]
fn an_idle_tick_may_touch_no_remote_store() {
    let mut s = stats(Some(Event::Idle));
    per_tick(&s, LIMITS).unwrap();
    s.staging.lists = 1;
    per_tick(&s, LIMITS).unwrap();
    s.remote.reads = 1;
    assert!(per_tick(&s, LIMITS).is_err());
}

#[test]
fn an_idle_window_wants_no_staged_file_no_checkpoint_and_no_unexplained_listing() {
    let mut window = IdleWindow::default();
    window.add(&stats(Some(Event::Idle)));
    window.check().unwrap();
    let mut listing = stats(Some(Event::Booted { cause: None }));
    listing.remote.lists = 3;
    window.add(&listing);
    assert!(window.check().is_ok(), "a boot lists");
    let mut staged = stats(Some(Event::Idle));
    staged.staging.commits = 1;
    let mut w2 = IdleWindow::default();
    w2.add(&staged);
    assert!(w2.check().is_err());
    let mut ckd = stats(Some(Event::Idle));
    ckd.checkpoints = 1;
    let mut w3 = IdleWindow::default();
    w3.add(&ckd);
    assert!(w3.check().is_err());
    let mut listed = stats(Some(Event::Synced { files: 0 }));
    listed.remote.lists = 1;
    let mut w4 = IdleWindow::default();
    w4.add(&listed);
    assert!(w4.check().is_err());
}

#[test]
fn memory_bounds_one_write() {
    let mut big = RecorderCounts::default();
    memory(big, big).unwrap();
    big.max_write_bytes = WRITE_CHUNK_LIMIT + 1;
    assert!(memory(big, RecorderCounts::default()).is_err());
    assert!(memory(RecorderCounts::default(), big).is_err());
}

#[test]
fn delta_subtracts_counters_and_keeps_maxima() {
    let a = RecorderCounts {
        lists: 2,
        written_bytes: 10,
        max_write_bytes: 5,
        ..RecorderCounts::default()
    };
    let b = RecorderCounts {
        lists: 5,
        written_bytes: 40,
        max_write_bytes: 9,
        ..RecorderCounts::default()
    };
    let d = delta(a, b);
    assert_eq!((d.lists, d.written_bytes, d.max_write_bytes), (3, 30, 9));
}

//! The observer, read by read: each kind of application checkpoint, the replicator's own checkpoints
//! seen as nothing, and a property over a model of SQLite's backfill under a read lock.

use lsr_ltx::PageSize;
use lsr_wal::{CommittedEnd, Salts};
use proptest::prelude::*;

use super::*;
use crate::position::ReadLockHold;

const T: Option<NonZeroU32> = Some(SQLITE_AUTOCHECKPOINT);

fn salts(n: u32) -> Salts {
    Salts {
        salt1: n,
        salt2: n.wrapping_mul(7),
    }
}

fn read(generation: u32, frames: u32, backfilled: u32) -> CommittedRead {
    CommittedRead::new(
        CommittedEnd::new(frames, salts(generation), PageSize::MIN),
        backfilled,
        ReadLockHold::first(),
    )
    .unwrap()
}

/// What SQLite's checkpoint call reports: the frames in the WAL and the frames backfilled as
/// the read lock is retaken.
fn ran(log: u32, checkpointed: u32) -> Option<CheckpointRun> {
    Some(CheckpointRun {
        busy: false,
        log,
        checkpointed,
        epoch: ReadLockHold::first().next(),
    })
}

/// Polls each read in turn and returns what each showed.
fn polls(
    observer: &mut AppCheckpointObserver,
    reads: &[CommittedRead],
) -> Vec<Option<AppCheckpoint>> {
    reads.iter().map(|r| observer.poll(*r)).collect()
}

#[test]
fn the_first_read_is_a_baseline() {
    let mut observer = AppCheckpointObserver::default();
    assert_eq!(observer.poll(read(1, 1500, 0)), None);
    assert_eq!(observer.counts(), AppCheckpoints::NONE);
}

/// The read lock above read mark 0: the application's checkpoint copies up to it, once.
#[test]
fn a_backfill_the_replicator_did_not_run_is_a_app_checkpoint() {
    let mut observer = AppCheckpointObserver::default();
    let seen = polls(
        &mut observer,
        &[read(1, 4, 0), read(1, 900, 0), read(1, 1010, 4)],
    );
    assert_eq!(
        seen,
        [
            None,
            None,
            Some(AppCheckpoint::Backfilled { from: 0, to: 4 })
        ]
    );
    let counts = observer.counts();
    assert_eq!((counts.backfilled, counts.ran(), counts.total()), (1, 1, 1));
}

/// Someone backfilled the rest and restarted the log between two reads.
#[test]
fn a_restart_after_an_unfinished_backfill_is_a_app_checkpoint() {
    let mut observer = AppCheckpointObserver::default();
    let seen = polls(&mut observer, &[read(1, 40, 10), read(2, 3, 0)]);
    assert_eq!(seen, [None, Some(AppCheckpoint::Restarted)]);
    assert_eq!(observer.counts().restarted, 1);
}

/// A writer restarting a log that was already fully backfilled ran no checkpoint.
#[test]
fn a_restart_after_a_full_backfill_is_not_one() {
    let mut observer = AppCheckpointObserver::default();
    let seen = polls(&mut observer, &[read(1, 40, 40), read(2, 3, 0)]);
    assert_eq!(seen, [None, None]);
}

/// The read lock on read mark 0: the replicator's full checkpoint, then its own one-frame write
/// restarted the WAL, and the WAL grew past the threshold over two reads with nothing
/// backfilled.
#[test]
fn growth_past_the_threshold_on_read_mark_zero_is_a_stall() {
    let mut observer = AppCheckpointObserver::default();
    observer.poll(read(1, 900, 0));
    observer.ours(Some(read(2, 1, 0)), ran(1000, 1000));
    let seen = polls(
        &mut observer,
        &[
            read(2, 12, 0),
            read(2, 1008, 0),
            read(2, 1020, 0),
            read(2, 1300, 0),
        ],
    );
    assert_eq!(
        seen,
        [
            None,
            None,
            Some(AppCheckpoint::Stalled { frames: 1020 }),
            None
        ],
        "once per episode"
    );
    assert_eq!(observer.counts().stalled, 1);
    assert_eq!(
        observer.counts().ran(),
        0,
        "a stall is no checkpoint that ran"
    );
}

/// The read lock above read mark 0: once the application's checkpoint reached the read lock, its next attempts copy
/// nothing more.
#[test]
fn growth_past_the_threshold_after_a_capped_backfill_is_a_stall() {
    let mut observer = AppCheckpointObserver::default();
    let seen = polls(
        &mut observer,
        &[read(1, 4, 0), read(1, 1080, 4), read(1, 1200, 4)],
    );
    assert_eq!(
        seen,
        [
            None,
            Some(AppCheckpoint::Backfilled { from: 0, to: 4 }),
            Some(AppCheckpoint::Stalled { frames: 1200 })
        ]
    );
    assert_eq!(
        observer.counts().last,
        Some(AppCheckpoint::Stalled { frames: 1200 })
    );
}

/// The read lock above read mark 0 with frames below it unbackfilled: an application attempt would have copied
/// them, so growth with nothing copied means the application runs no automatic checkpoint.
#[test]
fn growth_with_frames_below_the_read_lock_unbackfilled_is_no_stall() {
    let mut observer = AppCheckpointObserver::default();
    let seen = polls(
        &mut observer,
        &[read(1, 4, 0), read(1, 1080, 0), read(1, 1200, 0)],
    );
    assert_eq!(seen, [None, None, None]);
}

/// The replicator's checkpoint stopped short of the end: the read lock is retaken at the frames it
/// left, and an application attempt would copy up to them.
#[test]
fn the_read_lock_after_a_short_checkpoint_of_ours_is_where_it_stopped() {
    let mut observer = AppCheckpointObserver::default();
    observer.ours(Some(read(1, 1001, 600)), ran(1000, 600));
    let app_off = polls(&mut observer, &[read(1, 1100, 600), read(1, 1200, 600)]);
    assert_eq!(app_off, [None, None]);
    let app_on = polls(&mut observer, &[read(1, 1300, 1000), read(1, 1400, 1000)]);
    assert_eq!(
        app_on,
        [
            Some(AppCheckpoint::Backfilled {
                from: 600,
                to: 1000
            }),
            Some(AppCheckpoint::Stalled { frames: 1400 })
        ]
    );
}

/// An application stated to run no automatic checkpoint is never inferred to stall.
#[test]
fn no_stall_without_a_threshold() {
    let mut observer = AppCheckpointObserver::new(None);
    observer.ours(Some(read(2, 1, 0)), ran(10, 10));
    let seen = polls(
        &mut observer,
        &[read(2, 12, 0), read(2, 1500, 0), read(2, 3000, 0)],
    );
    assert_eq!(seen, [None, None, None]);
}

/// The WAL crossing the threshold once, then the replicator's checkpoint: no stall.
#[test]
fn crossing_the_threshold_once_before_the_replicator_checkpoints_is_no_stall() {
    let mut observer = AppCheckpointObserver::default();
    observer.ours(Some(read(2, 1, 0)), ran(10, 10));
    assert_eq!(observer.poll(read(2, 1100, 0)), None);
    observer.ours(Some(read(3, 1, 0)), ran(1100, 1100));
    assert_eq!(observer.poll(read(3, 20, 0)), None);
    assert_eq!(observer.counts(), AppCheckpoints::NONE);
}

/// Only the replicator checkpoints: its backfills, its restarts and its truncates are its
/// own, at any WAL size.
#[test]
fn the_replicators_own_checkpoints_are_never_noticed() {
    let mut observer = AppCheckpointObserver::default();
    observer.poll(read(1, 4, 0));
    let mut generation = 1;
    for round in 0..10u32 {
        let frames = 600 + round * 100;
        assert_eq!(observer.poll(read(generation, frames, 0)), None);
        // A PASSIVE stopped short by an application reader (its own frame after it), then one that
        // finishes, whose own frame restarts the fully backfilled log.
        observer.ours(
            Some(read(generation, frames + 1, frames / 2)),
            ran(frames, frames / 2),
        );
        assert_eq!(
            observer.poll(read(generation, frames + 5, frames / 2)),
            None
        );
        generation += 1;
        observer.ours(Some(read(generation, 1, 0)), ran(frames + 5, frames + 5));
        assert_eq!(observer.poll(read(generation, 3, 0)), None);
        if round % 2 == 0 {
            // A TRUNCATE of ours leaves an empty generation, then its own frame.
            generation += 1;
            observer.ours(Some(read(generation, 1, 0)), ran(0, 0));
        }
    }
    // A failed read after a checkpoint leaves no baseline: the next read sets one.
    observer.ours(None, ran(10, 10));
    assert_eq!(observer.poll(read(generation + 5, 1500, 0)), None);
    assert_eq!(observer.counts(), AppCheckpoints::NONE);
}

#[test]
fn counts_carry_across_a_replaced_engine() {
    let first = AppCheckpoints {
        backfilled: 2,
        restarted: 1,
        stalled: 0,
        last: Some(AppCheckpoint::Restarted),
    };
    let none = first.then(AppCheckpoints::NONE);
    assert_eq!(none, first);
    let later = AppCheckpoints {
        stalled: 1,
        last: Some(AppCheckpoint::Stalled { frames: 1100 }),
        ..AppCheckpoints::NONE
    };
    let both = first.then(later);
    assert_eq!((both.ran(), both.stalled, both.last), (3, 1, later.last));
}

/// The read lock's read mark in the model: SQLite's read mark 0, or a mark at a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadMark {
    Zero,
    At(u32),
}

/// SQLite's backfill and restart rules under one read lock and no other reader (the rules of
/// `crates/sim/src/sqlite.rs`: backfill limit, restart, mark choice).
#[derive(Clone, Copy, Debug)]
struct Model {
    generation: u32,
    frames: u32,
    backfilled: u32,
    read_mark: ReadMark,
    app_threshold: Option<u32>,
}

/// What the application did between two polls.
#[derive(Clone, Copy, Debug, Default)]
struct Interval {
    app_backfilled: bool,
    app_attempt_did_nothing: bool,
    read_lock_above_zero_unreached: bool,
}

impl Model {
    fn read(&self) -> CommittedRead {
        read(self.generation, self.frames, self.backfilled)
    }

    fn app_commit(&mut self, frames: u32, interval: &mut Interval) {
        self.write(frames);
        let Some(threshold) = self.app_threshold else {
            return;
        };
        if self.frames < threshold {
            return;
        }
        let before = self.backfilled;
        if let ReadMark::At(mark) = self.read_mark {
            self.backfilled = self.backfilled.max(mark.min(self.frames));
        }
        if self.backfilled > before {
            interval.app_backfilled = true;
        } else {
            interval.app_attempt_did_nothing = true;
        }
    }

    /// The replicator's checkpoint: `full` copies everything, else half (an application reader
    /// stopped it). The read lock is retaken as SQLite picks a mark, then the replicator writes one
    /// frame of its own (which restarts a fully backfilled log under a read lock on read mark 0). Returns
    /// what the checkpoint call reported.
    fn ours(&mut self, full: bool) -> Option<CheckpointRun> {
        let to = if full { self.frames } else { self.frames / 2 };
        self.backfilled = self.backfilled.max(to);
        let run = ran(self.frames, self.backfilled);
        self.read_mark = if self.backfilled == self.frames {
            ReadMark::Zero
        } else {
            ReadMark::At(self.frames)
        };
        self.write(1);
        run
    }

    /// A commit of `frames` frames, under SQLite's restart rule.
    fn write(&mut self, frames: u32) {
        if self.read_mark == ReadMark::Zero && self.frames > 0 && self.backfilled == self.frames {
            self.generation = self.generation.saturating_add(1);
            self.frames = 0;
            self.backfilled = 0;
        }
        self.frames = self.frames.saturating_add(frames);
    }
}

#[derive(Clone, Copy, Debug)]
enum Step {
    AppCommit(u32),
    Ours { full: bool },
    Poll,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (1u32..80).prop_map(Step::AppCommit),
        1 => any::<bool>().prop_map(|full| Step::Ours { full }),
        3 => Just(Step::Poll),
    ]
}

proptest! {
    /// Every application backfill between two polls is observed once, the replicator's own
    /// checkpoints never are, a stall is inferred only where an application attempt did nothing, and
    /// an application running no automatic checkpoint shows nothing while the read lock sits above read mark 0
    /// with frames below it unbackfilled.
    #[test]
    fn app_checkpoints_are_seen_and_the_replicators_are_not(
        steps in prop::collection::vec(step(), 1..120),
        threshold in prop::option::of(100u32..400),
        read_lock_at_start in 0u32..3,
        told in any::<bool>(),
    ) {
        let mut model = Model {
            generation: 1,
            frames: read_lock_at_start,
            backfilled: 0,
            read_mark: if read_lock_at_start == 0 { ReadMark::Zero } else { ReadMark::At(read_lock_at_start) },
            app_threshold: threshold,
        };
        let assumed = if told { threshold.and_then(NonZeroU32::new) } else { T };
        let mut observer = AppCheckpointObserver::new(assumed);
        observer.poll(model.read());
        let mut interval = Interval::default();
        for step in steps {
            match step {
                Step::AppCommit(frames) => {
                    if matches!(model.read_mark, ReadMark::At(mark) if model.backfilled < mark) {
                        interval.read_lock_above_zero_unreached = true;
                    }
                    model.app_commit(frames, &mut interval);
                }
                Step::Ours { full } => {
                    let run = model.ours(full);
                    observer.ours(Some(model.read()), run);
                    interval = Interval::default();
                }
                Step::Poll => {
                    let seen = observer.poll(model.read());
                    match seen {
                        Some(AppCheckpoint::Backfilled { .. }) => {
                            prop_assert!(interval.app_backfilled, "{interval:?}");
                        }
                        Some(AppCheckpoint::Stalled { .. }) => {
                            if told || threshold.is_some() {
                                prop_assert!(threshold.is_some(), "told none, inferred one");
                                prop_assert!(interval.app_attempt_did_nothing, "{interval:?}");
                            } else {
                                // The blind spot: told nothing, the application runs none.
                                prop_assert!(!interval.read_lock_above_zero_unreached);
                            }
                        }
                        Some(AppCheckpoint::Restarted) => {
                            prop_assert!(false, "only a full backfill restarts here");
                        }
                        None => prop_assert!(!interval.app_backfilled, "{interval:?}"),
                    }
                    interval = Interval::default();
                }
            }
        }
    }
}

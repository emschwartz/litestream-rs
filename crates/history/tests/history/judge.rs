//! The judge over hand-built trees: each property fails where it should and nowhere else.

use lsr_history::{AheadMode, Anomaly, Digest, Judge, Log, NodeId, Signals};

fn d(n: u8) -> Digest {
    Digest::of_pages(512, [[n; 4].as_slice()])
}

/// Root at 0; commits 1, 2, 3 at times 10, 20, 30.
fn line() -> (Log, [NodeId; 4]) {
    let mut log = Log::new(d(0), 0);
    let a = log.commit(d(1), 10);
    let b = log.commit(d(2), 20);
    let c = log.commit(d(3), 30);
    (log, [NodeId::ROOT, a, b, c])
}

#[test]
fn a_restore_no_commit_made_is_not_in_history() {
    let (log, _) = line();
    let mut judge = Judge::new(AheadMode::Count);
    assert!(matches!(
        judge.latest(&log, d(9)),
        Err(Anomaly::NotInHistory { .. })
    ));
    assert!(matches!(
        judge.as_of(&log, 25, d(9), None),
        Err(Anomaly::NotInHistory { .. })
    ));
}

#[test]
fn latest_moves_forward_and_may_repeat() {
    let (log, [_, a, b, c]) = line();
    let mut judge = Judge::new(AheadMode::Fatal);
    assert_eq!(judge.latest(&log, d(1)), Ok(a));
    assert_eq!(judge.latest(&log, d(1)), Ok(a));
    assert_eq!(judge.latest(&log, d(3)), Ok(c));
    assert!(matches!(
        judge.latest(&log, d(2)),
        Err(Anomaly::WentBackwards { from, to }) if from == c.get() && to == b.get()
    ));
}

#[test]
fn an_excused_regression_passes_once() {
    let (log, _) = line();
    let mut judge = Judge::new(AheadMode::Fatal);
    judge.latest(&log, d(3)).unwrap();
    judge.excuse_regression();
    judge.latest(&log, d(1)).unwrap();
    judge.latest(&log, d(3)).unwrap();
    judge.latest(&log, d(2)).unwrap_err();
    assert_eq!(judge.counts().excused, 1);
}

/// Under `Signals::Required` a recorded rewind explains going back only when the engine
/// reported a loss after it: before the report the same step is unsignaled.
#[test]
fn a_rewind_the_engine_did_not_report_does_not_excuse_going_back() {
    let (log, [_, _, _, c, x]) = forked();
    let mut quiet = Judge::new(AheadMode::Count).with_signals(Signals::Required);
    quiet.latest(&log, d(3)).unwrap();
    assert!(matches!(
        quiet.latest(&log, d(7)),
        Err(Anomaly::Unsignaled { from, to }) if from == c.get() && to == x.get()
    ));
    let mut told = Judge::new(AheadMode::Count).with_signals(Signals::Required);
    told.latest(&log, d(3)).unwrap();
    told.signal(&log);
    assert_eq!(told.latest(&log, d(7)), Ok(x));
    assert_eq!(told.counts().rewound, 1);
}

/// A rewind the engine has not reported yet does not block a step the driver also excused (a
/// bucket rollback and a power loss between two looks): the excuse lets it through, owed.
#[test]
fn an_excuse_covers_a_step_a_rewind_not_yet_reported_would_explain() {
    let (log, [_, _, _, _, x]) = forked();
    let mut judge = Judge::new(AheadMode::Count).with_signals(Signals::Required);
    judge.latest(&log, d(3)).unwrap();
    judge.excuse_regression();
    assert_eq!(judge.latest(&log, d(7)), Ok(x));
    assert_eq!(judge.unreported(), 1);
}

/// Two rewinds explain the same step and the engine reported after the first: the step is
/// covered, though the second rewind (a later power loss the engine has not looked at yet)
/// has no report of its own.
#[test]
fn a_report_after_any_explaining_rewind_covers_the_step() {
    let (mut log, [_, a, _, c]) = line();
    let mut judge = Judge::new(AheadMode::Count).with_signals(Signals::Required);
    judge.latest(&log, d(3)).unwrap();
    log.recover(a, 40);
    judge.signal(&log);
    let x = log.commit(d(7), 50);
    log.recover(a, 60);
    log.commit(d(8), 70);
    assert_eq!(judge.latest(&log, d(7)), Ok(x), "from {}", c.get());
}

/// A report that came before the rewind does not cover it.
#[test]
fn a_report_before_the_rewind_does_not_cover_it() {
    let (mut log, [_, a, _, c]) = line();
    let mut judge = Judge::new(AheadMode::Count).with_signals(Signals::Required);
    judge.latest(&log, d(3)).unwrap();
    judge.signal(&log);
    log.recover(a, 40);
    log.commit(d(7), 50);
    assert!(matches!(
        judge.latest(&log, d(7)),
        Err(Anomaly::Unsignaled { from, .. }) if from == c.get()
    ));
}

/// The driver's excuse lets the step through at once (the engine cannot have seen a replica
/// changed under it yet) and is owed a report afterwards; `unreported` counts what stays owed.
#[test]
fn an_excused_regression_is_owed_a_report() {
    let (log, _) = line();
    let mut judge = Judge::new(AheadMode::Fatal).with_signals(Signals::Required);
    judge.latest(&log, d(3)).unwrap();
    judge.excuse_regression();
    judge.latest(&log, d(1)).unwrap();
    assert_eq!(judge.unreported(), 1);
    judge.signal(&log);
    assert_eq!(judge.unreported(), 0);
    let mut ignored = Judge::new(AheadMode::Fatal);
    ignored.latest(&log, d(3)).unwrap();
    ignored.excuse_regression();
    ignored.latest(&log, d(1)).unwrap();
    assert_eq!(ignored.unreported(), 0, "without signals nothing is owed");
}

/// Root, a, b, c; a crash rewinds to a; the app commits x (time 50).
fn forked() -> (Log, [NodeId; 5]) {
    let (mut log, [root, a, b, c]) = line();
    log.recover(a, 40);
    let x = log.commit(d(7), 50);
    (log, [root, a, b, c, x])
}

#[test]
fn a_recorded_rewind_explains_going_back_and_the_lost_branch_is_ahead() {
    let (log, [_, a, _, c, x]) = forked();
    assert_eq!(log.rewinds().len(), 1);
    let mut judge = Judge::new(AheadMode::Count);
    // The replica still holds c, which the database lost.
    assert_eq!(judge.latest(&log, d(3)), Ok(c));
    assert_eq!(judge.counts().ahead_latest, 1);
    // Then it snapshots the rewound database, and moves on along the new branch.
    assert_eq!(judge.latest(&log, d(1)), Ok(a));
    assert_eq!(judge.latest(&log, d(7)), Ok(x));
    assert_eq!(judge.counts().rewound, 1);
    let mut fatal = Judge::new(AheadMode::Fatal);
    assert!(matches!(
        fatal.latest(&log, d(3)),
        Err(Anomaly::Ahead { node, .. }) if node == c.get()
    ));
}

#[test]
fn a_rewind_explains_nothing_below_its_target() {
    let (log, _) = forked();
    let mut judge = Judge::new(AheadMode::Count);
    judge.latest(&log, d(3)).unwrap();
    // The database still holds a; the replica going back to the root is a loss.
    assert!(matches!(
        judge.latest(&log, d(0)),
        Err(Anomaly::WentBackwards { .. })
    ));
}

#[test]
fn a_rewind_after_the_restore_does_not_explain_it() {
    let (mut log, [_, a, _, _]) = line();
    let mut judge = Judge::new(AheadMode::Count);
    judge.latest(&log, d(1)).unwrap();
    log.commit(d(4), 35);
    log.recover(a, 40);
    // Going from a to the root: the rewind kept a, so it explains nothing.
    judge.latest(&log, d(0)).unwrap_err();
}

#[test]
fn a_pending_commit_that_landed_moves_the_head_forward() {
    let (mut log, [_, _, _, c]) = line();
    let p = log.pending(d(4), 35);
    assert_eq!(log.head(), c);
    log.recover(p, 40);
    assert_eq!(log.head(), p);
    assert!(log.rewinds().is_empty());
    assert!(!log.acknowledged(p));
}

#[test]
fn as_of_is_never_newer_than_the_instant() {
    let (log, [root, a, b, _]) = line();
    let mut judge = Judge::new(AheadMode::Fatal);
    assert_eq!(judge.as_of(&log, 25, d(2), None), Ok(b));
    assert_eq!(judge.as_of(&log, 25, d(1), None), Ok(a));
    assert_eq!(judge.as_of(&log, 5, d(0), None), Ok(root));
    // Committed at 20, so not "strictly before 20".
    assert!(matches!(
        judge.as_of(&log, 20, d(2), None),
        Err(Anomaly::AsOfTooNew { begun: 20, .. })
    ));
}

/// A commit is an interval: begun at 22, acknowledged at 28. `AsOf(25)` may see it (another
/// connection could have, and Fatal mode does not call it ahead), but `AsOf(22)` may not: it
/// began at the instant. One time per commit read after the insert returned would call the
/// first a state from the future.
#[test]
fn as_of_judges_a_commit_by_when_it_began() {
    let mut log = Log::new(d(0), 0);
    let a = log.commit(d(1), 10);
    let b = log.commit_between(d(2), 22, 28);
    let mut judge = Judge::new(AheadMode::Fatal);
    assert_eq!(judge.as_of(&log, 25, d(2), None), Ok(b));
    assert_eq!(judge.as_of(&log, 25, d(1), None), Ok(a));
    assert!(matches!(
        judge.as_of(&log, 22, d(2), None),
        Err(Anomaly::AsOfTooNew { begun: 22, .. })
    ));
}

#[test]
fn as_of_within_a_bound_is_the_node_live_then() {
    let (log, [_, a, b, _]) = line();
    let mut judge = Judge::new(AheadMode::Fatal);
    assert_eq!(judge.as_of(&log, 25, d(2), Some(3)), Ok(b));
    // a was the head from 10 to 20: within 4 ms before 25 it was not, within 6 it was.
    assert!(matches!(
        judge.as_of(&log, 25, d(1), Some(4)),
        Err(Anomaly::AsOfTooOld { .. })
    ));
    assert_eq!(judge.as_of(&log, 25, d(1), Some(6)), Ok(a));
}

#[test]
fn as_of_after_a_rewind_on_the_lost_branch_is_ahead() {
    let (log, [_, _, _, c, _]) = forked();
    let mut judge = Judge::new(AheadMode::Count);
    // At 45 the database held a; c was cut at 40.
    assert_eq!(judge.as_of(&log, 45, d(3), None), Ok(c));
    assert_eq!(judge.counts().ahead_as_of, 1);
    // At 35 the database held c.
    assert_eq!(judge.as_of(&log, 35, d(3), None), Ok(c));
    assert_eq!(judge.counts().ahead_as_of, 1);
}

#[test]
fn a_clock_that_went_back_leaves_as_of_untimed() {
    let (mut log, _) = line();
    log.note_time(5);
    let mut judge = Judge::new(AheadMode::Fatal);
    judge.as_of(&log, 15, d(3), None).unwrap();
    assert_eq!(judge.counts().as_of_untimed, 1);
}

#[test]
fn lag_is_bounded_by_the_head() {
    let (log, _) = line();
    let mut judge = Judge::new(AheadMode::Fatal);
    judge.lag(&log, d(3), 100, 0).unwrap();
    assert!(matches!(
        judge.lag(&log, d(2), 100, 0),
        Err(Anomaly::Lagging { .. })
    ));
    judge.lag(&log, d(2), 29, 0).unwrap();
    judge.lag(&log, d(2), 35, 10).unwrap();
}

#[test]
fn repeated_content_is_judged_by_any_node_that_holds_it() {
    let mut log = Log::new(d(0), 0);
    let a = log.commit(d(1), 10);
    let b = log.commit(d(1), 20);
    let mut judge = Judge::new(AheadMode::Fatal);
    assert_eq!(judge.latest(&log, d(1)), Ok(b));
    assert!(log.is_ancestor(a, b));
    assert_eq!(log.find(&d(1)), &[a, b]);
}

#[test]
fn a_sweep_of_restore_points_must_climb_the_tree() {
    let (log, _) = forked();
    let mut judge = Judge::new(AheadMode::Fatal);
    // 1..3 on the lost branch, then a snapshot of the rewound database and the new branch.
    let points = [
        (1, d(1), false),
        (2, d(2), false),
        (3, d(3), false),
        (4, d(1), false),
        (5, d(7), false),
    ];
    judge.sweep(&log, &points).unwrap();
    assert_eq!(judge.counts().points, 5);
    // A point that goes back on its branch, with no rewind to explain it.
    let (straight, _) = line();
    let back = [(1, d(2), false), (2, d(1), false)];
    assert!(matches!(
        judge.sweep(&straight, &back),
        Err(Anomaly::WentBackwards { .. })
    ));
    // A point the driver put there is judged for membership only.
    let foreign = [(1, d(2), false), (2, d(1), true), (3, d(3), false)];
    judge.sweep(&log, &foreign).unwrap();
    assert!(matches!(
        judge.sweep(&log, &[(1, d(9), true)]),
        Err(Anomaly::NotInHistory { .. })
    ));
}

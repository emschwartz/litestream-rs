//! When each task runs, and when the actor asks to be ticked again.

use super::*;
use crate::source::{CheckpointEffect, CheckpointScript};

fn checkpoints_at(min_pages: u32) -> ActorConfig {
    let mut config = cfg();
    config.checkpoint.min_pages = Some(nz(min_pages));
    config
}

/// A failed checkpoint waits out its task's backoff: a sync in between
/// stages the new transaction and does not checkpoint.
#[test]
fn a_failed_checkpoint_waits_out_its_backoff() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(checkpoints_at(1));
    rig.settle(&mut actor, T0);
    rig.ck
        .inject(SourceOp::Checkpoint, SourceError::Io(ErrorKind::Other));
    rig.commit(2);
    let failed = rig.tick(&mut actor, T0 + 1);
    let Some(Event::Retrying { until, .. }) = event(&failed) else {
        panic!("the checkpoint should fail: {failed:?}");
    };
    let until = until.unix_millis();
    let calls = rig.ck.calls().len();
    rig.commit(3);
    let tick = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }));
    assert_eq!(rig.ck.calls().len(), calls, "{:?}", rig.ck.calls());
    rig.settle(&mut actor, until);
    assert!(rig.ck.calls().len() > calls, "the retry is due at {until}");
}

/// A tick with nothing new in the WAL does not open it.
#[test]
fn an_idle_tick_does_not_open_the_wal() {
    for config in [cfg(), checkpoints_at(1)] {
        let mut rig = Rig::new();
        let mut actor = Actor::new(config);
        rig.settle(&mut actor, T0);
        rig.wal
            .inject(SourceOp::Open, SourceError::Io(ErrorKind::Other));
        let tick = rig.tick(&mut actor, T0 + 1);
        assert_eq!(event(&tick), Some(&Event::Idle));
    }
}

/// A checkpoint reports whether SQLite was busy, as SQLite said.
#[test]
fn a_checkpoint_reports_busy_exactly_when_it_was() {
    for busy in [false, true] {
        let mut rig = Rig::new();
        let mut actor = Actor::new(checkpoints_at(1));
        rig.settle(&mut actor, T0);
        rig.ck.script(CheckpointScript {
            busy,
            effect: CheckpointEffect::Nothing,
        });
        rig.commit(2);
        let tick = rig.tick(&mut actor, T0 + 1);
        assert_eq!(
            event(&tick),
            Some(&Event::Checkpointed {
                mode: CheckpointMode::Passive,
                busy,
            })
        );
    }
}

/// A failed prune waits out its task's backoff before it deletes again.
#[test]
fn a_failed_prune_waits_out_its_backoff() {
    let (mut rig, mut actor) = caught_up();
    rig.staging
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    rig.commit(2);
    rig.settle(&mut actor, T0 + 1);
    rig.commit(3);
    rig.settle(&mut actor, T0 + 2);
    let first = rig.staging.deleted();
    assert_eq!(first.len(), 1, "one failed prune of file 1: {first:?}");
    rig.settle(&mut actor, T0 + 2_000);
    assert_eq!(staged(&rig), vec![2, 3]);
}

/// A WAL that moved to a new generation with no new transaction stages
/// nothing, and the tick goes on to the other tasks instead of reporting an
/// empty sync.
#[test]
fn a_restarted_wal_with_nothing_new_is_not_reported_as_a_sync() {
    let (mut rig, mut actor) = caught_up();
    rig.wal.unpin_reader();
    rig.wal.backfill(u32::MAX);
    assert!(rig.wal.try_restart(), "a fully backfilled WAL restarts");
    let tick = rig.tick(&mut actor, T0 + 1);
    assert!(
        !matches!(event(&tick), Some(Event::Synced { .. })),
        "{tick:?}"
    );
}

/// Puts the upload task into its backoff with a failed upload of 2, so the
/// staged file keeps no other task busy.
fn upload_backing_off(rig: &mut Rig, actor: &mut Actor) {
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.commit(2);
    let synced = rig.tick(actor, T0 + 10);
    assert!(
        matches!(
            event(&synced),
            Some(Event::Synced { files: 1 } | Event::Checkpointed { .. })
        ),
        "{synced:?}"
    );
    let failed = rig.tick(actor, T0 + 10);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { .. })),
        "the upload should fail: {failed:?}"
    );
}

/// The read epoch moved while a sync staged its file: the sync proves
/// nothing, so the checkpoint it would have allowed waits for the next tick,
/// which is asked for at once.
#[test]
fn a_sync_whose_epoch_moved_does_not_checkpoint_and_asks_for_the_next_tick() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(checkpoints_at(1));
    rig.settle(&mut actor, T0);
    upload_backing_off(&mut rig, &mut actor);
    rig.settle(&mut actor, T0 + 10);
    let calls = rig.ck.calls().len();
    *rig.staging.moves_epoch.lock().unwrap() = Some(rig.wal.clone());
    rig.commit(3);
    let tick = rig.tick(&mut actor, T0 + 20);
    assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }));
    assert_eq!(rig.ck.calls().len(), calls, "{:?}", rig.ck.calls());
    assert_eq!(next_of(&tick), Some(Next::Now));
    let next = rig.tick(&mut actor, T0 + 20);
    assert!(
        matches!(event(&next), Some(Event::Checkpointed { .. })),
        "{next:?}"
    );
}

/// A dropped staged file leaves a snapshot pending, and the actor asks for
/// the tick that takes it at once.
#[test]
fn a_pending_snapshot_asks_for_the_next_tick_at_once() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    assert!(matches!(
        event(&rig.tick(&mut actor, T0 + 1)),
        Some(Event::Synced { .. })
    ));
    tear(&rig, 2, truncated);
    let lost = rig.tick(&mut actor, T0 + 1);
    assert_eq!(
        event(&lost),
        Some(&Event::Resnapshot(
            SnapshotCause::NewestStagedFileUnreadable
        ))
    );
    assert_eq!(next_of(&lost), Some(Next::Now));
}

/// A prune whose candidates fill a listing page continues on the next page.
#[test]
fn a_prune_continues_past_a_full_page() {
    let (mut rig, mut actor) = caught_up();
    rig.staging.page_cap.store(2, Ordering::SeqCst);
    for n in 2..=6 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    let later = T0 + 60_000;
    rig.settle(&mut actor, later);
    assert_eq!(level_one_max(&rig), Some(6));
    rig.settle(&mut actor, later);
    assert_eq!(staged(&rig), vec![6]);
}

/// The checkpoint interval comes due on a WAL with a single frame: there is
/// nothing to checkpoint, so the actor does not ask to be ticked again at
/// that instant.
#[test]
fn an_interval_due_with_nothing_to_checkpoint_does_not_spin() {
    let mut config = cfg();
    config.checkpoint.interval = Some(Duration::from_secs(1));
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    let tick = rig.tick(&mut actor, T0 + 1_000);
    assert_eq!(event(&tick), Some(&Event::Idle));
    assert_ne!(next_of(&tick), Some(Next::Now));
}

/// Checkpoints at the truncate threshold of one frame, every one busy (a
/// reader SQLite cannot move past), with the interval given.
fn busy_at_the_truncate_threshold(interval: Option<Duration>) -> (Rig, Actor) {
    let mut config = cfg();
    config.checkpoint.truncate_pages = Some(nz(1));
    config.checkpoint.interval = interval;
    let mut rig = Rig::new();
    for _ in 0..4 {
        rig.ck.script(CheckpointScript {
            busy: true,
            effect: CheckpointEffect::Nothing,
        });
    }
    let actor = Actor::new(config);
    (rig, actor)
}

fn truncates(rig: &Rig) -> usize {
    rig.ck
        .calls()
        .iter()
        .filter(|call| matches!(call, CheckpointCall::Checkpoint(CheckpointMode::Truncate)))
        .count()
}

/// A TRUNCATE that found the WAL unchanged is not tried again until the WAL
/// changes, when no interval is set.
#[test]
fn a_busy_truncate_waits_for_the_next_write() {
    let (mut rig, mut actor) = busy_at_the_truncate_threshold(None);
    rig.settle(&mut actor, T0);
    assert_eq!(truncates(&rig), 1, "{:?}", rig.ck.calls());
    rig.settle(&mut actor, T0 + 60_000);
    assert_eq!(truncates(&rig), 1, "{:?}", rig.ck.calls());
}

/// With an interval, the busy TRUNCATE waits one interval: not before it, and
/// once it has passed.
#[test]
fn a_busy_truncate_waits_one_interval() {
    let (mut rig, mut actor) = busy_at_the_truncate_threshold(Some(Duration::from_secs(10)));
    rig.settle(&mut actor, T0);
    assert_eq!(truncates(&rig), 1, "{:?}", rig.ck.calls());
    rig.settle(&mut actor, T0 + 5_000);
    assert_eq!(truncates(&rig), 1, "{:?}", rig.ck.calls());
    rig.settle(&mut actor, T0 + 10_000);
    assert_eq!(truncates(&rig), 2, "{:?}", rig.ck.calls());
}

/// A wait that has passed is let go even when its task finds no work: the
/// WAL restarted under SQLite, so there is nothing left to checkpoint, and
/// the actor settles instead of asking for a tick at once forever.
#[test]
fn a_passed_wait_with_no_work_left_is_not_a_deadline() {
    for gate in [false, true] {
        let (mut rig, mut actor) = busy_at_the_truncate_threshold(Some(Duration::from_secs(10)));
        if !gate {
            rig.ck
                .inject(SourceOp::Checkpoint, SourceError::Io(ErrorKind::Other));
        }
        rig.settle(&mut actor, T0);
        let waiting = if gate {
            actor.engine.gate.until()
        } else {
            actor.kept.tasks.get(Task::Checkpoint).until()
        };
        let until = waiting.unwrap_or_else(|| panic!("no wait is held: {:?}", rig.ck.calls()));
        rig.wal.force_restart();
        rig.settle(&mut actor, T0 + 1);
        let seen = rig.settle(&mut actor, until.unix_millis());
        assert_ne!(seen.last().and_then(next_of), Some(Next::Now), "{seen:?}");
    }
}

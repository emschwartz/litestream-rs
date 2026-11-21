//! A checkpoint that fails after one of its own steps staged a file: the file
//! is uploaded and a drain waits for it.

use super::*;
use crate::source::{CheckpointEffect, CheckpointScript};

/// The newest transaction in staging and on the replica.
fn newest(rig: &Rig) -> (Option<u64>, Option<u64>) {
    let max = |store: &Counting| {
        Rig::files(store, Level::ZERO)
            .iter()
            .map(|f| f.key.range().max().get())
            .max()
    };
    (max(&rig.staging), max(&rig.remote))
}

/// A PASSIVE whose seal stages a commit the application made after the tick's sync
/// read the committed end, and whose step `op` then fails with `err`.
fn passive_failed_after_the_seal(op: SourceOp, err: SourceError) -> (Rig, Actor) {
    let mut config = cfg();
    config.checkpoint.min_pages = Some(nz(1));
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    *rig.staging.commits_on_upload.lock().unwrap() = Some((rig.wal.clone(), 3));
    rig.ck.inject(op, err);
    rig.commit(2);
    let failed = rig.tick(&mut actor, T0 + 1);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { .. })),
        "{op:?} should fail the checkpoint: {failed:?}"
    );
    assert!(
        rig.ck
            .calls()
            .contains(&CheckpointCall::Checkpoint(CheckpointMode::Passive)),
        "{op:?}: {:?}",
        rig.ck.calls()
    );
    let (staged, replicated) = newest(&rig);
    assert!(
        staged > replicated,
        "{op:?}: the seal staged a file the replica lacks"
    );
    (rig, actor)
}

/// A TRUNCATE whose boundary snapshot is staged and whose write lock then
/// fails to release.
fn truncate_failed_after_the_snapshot() -> (Rig, Actor) {
    let mut config = cfg();
    config.checkpoint.truncate_pages = Some(nz(1));
    let mut rig = Rig::new();
    // The PASSIVE tried first at the threshold is held back, so a TRUNCATE follows.
    rig.ck.script(CheckpointScript {
        busy: true,
        effect: CheckpointEffect::Nothing,
    });
    rig.ck
        .inject(SourceOp::EndWriteLock, SourceError::Io(ErrorKind::Other));
    let mut actor = Actor::new(config);
    let seen = rig.settle(&mut actor, T0);
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Retrying { .. }))),
        "the write lock's release should fail: {seen:?}"
    );
    assert!(rig.ck.calls().contains(&CheckpointCall::EndWriteLock));
    (rig, actor)
}

/// A named way to reach a failed checkpoint that staged a file.
type Case = (&'static str, Box<dyn Fn() -> (Rig, Actor)>);

fn cases() -> Vec<Case> {
    let passive = |op, err| -> Box<dyn Fn() -> (Rig, Actor)> {
        Box::new(move || passive_failed_after_the_seal(op, err))
    };
    vec![
        (
            "checkpoint busy",
            passive(SourceOp::Checkpoint, SourceError::Busy),
        ),
        (
            "checkpoint io",
            passive(SourceOp::Checkpoint, SourceError::Io(ErrorKind::Other)),
        ),
        (
            "end barrier",
            passive(SourceOp::EndBarrier, SourceError::Io(ErrorKind::Other)),
        ),
        ("bump busy", passive(SourceOp::BumpSeq, SourceError::Busy)),
        (
            "bump io",
            passive(SourceOp::BumpSeq, SourceError::Io(ErrorKind::Other)),
        ),
        (
            "end write lock",
            Box::new(truncate_failed_after_the_snapshot),
        ),
    ]
}

/// The file a failed checkpoint staged is uploaded without waiting for the
/// application's next commit or the checkpoint's retry.
#[test]
fn a_file_a_failed_checkpoint_staged_is_uploaded() {
    for (name, case) in cases() {
        let (mut rig, mut actor) = case();
        rig.settle(&mut actor, T0 + 2);
        let (staged, replicated) = newest(&rig);
        assert_eq!(replicated, staged, "{name}");
        assert_eq!(actor.staged().map(Txid::get), staged, "{name}");
        assert_eq!(actor.replicated().map(Txid::get), staged, "{name}");
    }
}

/// A drain after a failed checkpoint ends only once the file it staged is on
/// the replica, and names that file's transaction.
#[test]
fn a_drain_after_a_failed_checkpoint_waits_for_the_file_it_staged() {
    for (name, case) in cases() {
        let (mut rig, mut actor) = case();
        actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 2)));
        let seen = rig.follow(&mut actor, T0 + 2, 50);
        let (staged, replicated) = newest(&rig);
        assert_eq!(replicated, staged, "{name}: {seen:?}");
        assert_eq!(
            seen.last(),
            Some(&Tick::Drained {
                txid: staged.and_then(Txid::new)
            }),
            "{name}: {seen:?}"
        );
    }
}

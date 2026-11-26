//! The WAL is synced before any frame read from it is staged. A failed sync stages nothing,
//! degrades the sync task with `WalSyncFailed`, and is tried again; nothing is remembered
//! across a restart and the engine runs no checkpoint of its own to repair it
//! (`docs/decisions/D173.md`).

use super::*;

/// The level-0 files staged, as (max transaction id, the salts of the WAL generation its
/// frames came from, if any).
fn ranges(rig: &Rig) -> Vec<(u64, Option<(u32, u32)>)> {
    Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .map(|f| {
            let bytes = block_on(rig.staging.read(&f.key, 0, ReadLen::CHUNK)).unwrap();
            let head: [u8; lsr_ltx::HEADER_LEN] = bytes[..lsr_ltx::HEADER_LEN].try_into().unwrap();
            let header = lsr_ltx::Header::decode(&head).unwrap();
            let from = header
                .fields()
                .wal
                .filter(|o| o.size() > 0)
                .map(|o| o.salts());
            (f.key.range().max().get(), from)
        })
        .collect()
}

/// The newest transaction among `ranges`.
fn newest(ranges: &[(u64, Option<(u32, u32)>)]) -> Option<u64> {
    ranges.iter().map(|(txid, _)| *txid).max()
}

fn truncates(rig: &Rig) -> usize {
    rig.ck
        .calls()
        .iter()
        .filter(|c| **c == CheckpointCall::Checkpoint(CheckpointMode::Truncate))
        .count()
}

/// A caught-up actor under the default responses whose next sync of the WAL fails, with one
/// more transaction committed; the tick that failed.
fn failed_sync() -> (Rig, Actor, Tick) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.commit(2);
    let failed = rig.tick(&mut actor, T0 + 1);
    (rig, actor, failed)
}

/// A failed sync of the WAL stages nothing and degrades the sync task with `WalSyncFailed`.
/// The retry syncs again, and once that succeeds the frames are staged as any others: no
/// checkpoint of the engine's own runs and no snapshot is owed.
#[test]
fn a_failed_wal_sync_stages_nothing_and_is_tried_again() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded {
        task, cause, until, ..
    }) = event(&failed)
    else {
        panic!("the sync should fail: {failed:?}");
    };
    assert_eq!((*task, *cause), (Task::Sync, DegradedCause::WalSyncFailed));
    assert_eq!(
        actor.degraded(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::WalSyncFailed),
        "health shows it"
    );
    let before = ranges(&rig);
    let seen = rig.settle(&mut actor, until.unix_millis());
    assert!(
        seen.iter()
            .all(|t| !matches!(event(t), Some(Event::Resnapshot(_)))),
        "no snapshot is owed: {seen:?}"
    );
    assert_eq!(truncates(&rig), 0, "{:?}", rig.ck.calls());
    let after = ranges(&rig);
    assert!(
        newest(&after) > newest(&before),
        "the frames are staged once a sync succeeds: {before:?} then {after:?}"
    );
    assert_eq!(actor.degraded(Task::Sync), None, "the next sync clears it");
}

/// The checkpoint's seal syncs the WAL too: a failure there is the same fault, tried again the
/// same way, with nothing owed after it.
#[test]
fn a_wal_sync_failed_in_a_checkpoint_seal_is_tried_again() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    actor.fault(Task::Checkpoint, Fault::WalSyncFailed, ts(T0 + 1));
    let before = ranges(&rig);
    rig.commit(2);
    rig.settle(&mut actor, T0 + 100_000);
    assert_eq!(truncates(&rig), 0, "{:?}", rig.ck.calls());
    assert!(newest(&ranges(&rig)) > newest(&before));
}

/// Under the strict responses the failed sync stops the engine with its cause.
#[test]
fn a_failed_wal_sync_stops_under_strict_responses() {
    let (mut rig, mut actor) = caught_up();
    let before = ranges(&rig);
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.commit(2);
    assert_eq!(
        rig.tick(&mut actor, T0 + 1),
        Tick::Stopped(StopCause::WalSyncFailed)
    );
    assert_eq!(ranges(&rig), before);
}

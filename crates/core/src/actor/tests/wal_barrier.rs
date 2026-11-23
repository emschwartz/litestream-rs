//! The WAL is synced before any frame read from it is staged, and a failed sync is never
//! trusted again: no frame of that generation is staged; a TRUNCATE checkpoint and a snapshot
//! from the database file follow (`actor::unproven`).

use super::*;
use crate::source::{Barrier, CheckpointEffect, CheckpointRun, CheckpointScript, WriteLock};

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

/// The generation the failure meets: the rig's first.
const FAILED: (u32, u32) = (10, 20);

fn truncates(rig: &Rig) -> usize {
    checkpoints(rig, CheckpointMode::Truncate)
}

fn fulls(rig: &Rig) -> usize {
    checkpoints(rig, CheckpointMode::Full)
}

fn checkpoints(rig: &Rig, mode: CheckpointMode) -> usize {
    rig.ck
        .calls()
        .iter()
        .filter(|c| **c == CheckpointCall::Checkpoint(mode))
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
/// The retry does not sync again (a sync that succeeds after a failed one proves nothing about
/// the pages the failure covered): it checkpoints with TRUNCATE, and the next staged file is a
/// snapshot, so no frame of the failed generation reaches staging.
#[test]
fn a_failed_wal_sync_stages_nothing_of_its_generation_and_ends_in_a_snapshot() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded {
        task, cause, until, ..
    }) = event(&failed)
    else {
        panic!("the sync should fail: {failed:?}");
    };
    assert_eq!(
        (*task, *cause),
        (Task::Sync, PersistentCause::WalSyncFailed)
    );
    assert_eq!(
        actor.degraded(Task::Sync).map(|d| d.cause),
        Some(PersistentCause::WalSyncFailed),
        "health shows it"
    );
    let before = ranges(&rig);
    assert_eq!(truncates(&rig), 0);
    let seen = rig.settle(&mut actor, until.unix_millis());
    assert!(
        seen.iter()
            .any(|t| event(t) == Some(&Event::Resnapshot(SnapshotCause::WalSyncFailed))),
        "{seen:?}"
    );
    assert_eq!(truncates(&rig), 1, "{:?}", rig.ck.calls());
    let after = ranges(&rig);
    let new: Vec<_> = after.iter().filter(|r| !before.contains(r)).collect();
    assert!(!new.is_empty(), "a snapshot follows: {after:?}");
    assert!(
        new.iter().all(|(_, from)| *from != Some(FAILED)),
        "nothing from the failed generation: {before:?} then {after:?}"
    );
    assert_eq!(actor.degraded(Task::Sync), None, "the snapshot clears it");
}

/// A TRUNCATE the readers refuse is the same fault again: still degraded, still nothing staged,
/// and no sync step runs in its place.
#[test]
fn a_busy_truncate_after_a_failed_wal_sync_stays_degraded_and_stages_nothing() {
    let (mut rig, mut actor, failed) = failed_sync();
    let before = ranges(&rig);
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    // The cure's FULL copies and proves the WAL; its TRUNCATE then meets a reader.
    rig.ck.script(CheckpointScript {
        busy: false,
        effect: CheckpointEffect::Natural,
    });
    rig.ck.script(CheckpointScript {
        busy: true,
        effect: CheckpointEffect::Nothing,
    });
    rig.commit(3);
    let retried = rig.tick(&mut actor, until.unix_millis());
    let Some(Event::Degraded { cause, .. }) = event(&retried) else {
        panic!("{retried:?}");
    };
    assert_eq!(*cause, PersistentCause::WalSyncFailed);
    assert_eq!(truncates(&rig), 1);
    assert_eq!(ranges(&rig), before, "nothing staged past the failed sync");
}

/// The checkpoint's seal syncs the WAL too: a failure there is the same debt, paid the same way.
#[test]
fn a_wal_sync_failed_in_a_checkpoint_seal_is_never_retried_past() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    actor.fault(Task::Checkpoint, Fault::WalSyncFailed, ts(T0 + 1));
    let before = ranges(&rig);
    rig.commit(2);
    let seen = rig.settle(&mut actor, T0 + 100_000);
    assert_eq!(truncates(&rig), 1, "{:?}", rig.ck.calls());
    let after = ranges(&rig);
    assert!(
        after
            .iter()
            .filter(|r| !before.contains(r))
            .all(|(_, from)| *from != Some(FAILED)),
        "{before:?} then {after:?}; {seen:?}"
    );
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

/// Once the TRUNCATE completed and its snapshot is staged, the debt is paid: the next commit
/// is staged from the new generation's frames by an ordinary step, with no second TRUNCATE.
#[test]
fn after_a_completed_truncate_staging_resumes_from_the_new_generation() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    rig.settle(&mut actor, until.unix_millis());
    assert_eq!(truncates(&rig), 1);
    let before = ranges(&rig);
    rig.commit(4);
    let seen = rig.settle(&mut actor, until.unix_millis() + 1);
    assert_eq!(truncates(&rig), 1, "no second TRUNCATE: {seen:?}");
    let new: Vec<_> = ranges(&rig)
        .into_iter()
        .filter(|r| !before.contains(r))
        .collect();
    assert!(
        !new.is_empty()
            && new
                .iter()
                .all(|(_, from)| from.is_some_and(|s| s != FAILED)),
        "the commit is staged from the new generation's frames: {before:?} then {new:?}"
    );
    assert_eq!(actor.degraded(Task::Sync), None);
}

/// A TRUNCATE that did not complete pays nothing: the next retry truncates again, and nothing
/// is staged until one completes. The cure proved the WAL before the busy TRUNCATE, so the retry
/// runs the TRUNCATE alone, with no second FULL.
#[test]
fn after_a_busy_truncate_the_next_retry_truncates_again() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    let before = ranges(&rig);
    // The cure's FULL copies and proves the WAL; its TRUNCATE then meets a reader.
    rig.ck.script(CheckpointScript {
        busy: false,
        effect: CheckpointEffect::Natural,
    });
    rig.ck.script(CheckpointScript {
        busy: true,
        effect: CheckpointEffect::Nothing,
    });
    let retried = rig.tick(&mut actor, until.unix_millis());
    let Some(Event::Degraded { until, .. }) = event(&retried) else {
        panic!("{retried:?}");
    };
    assert_eq!(ranges(&rig), before);
    let seen = rig.settle(&mut actor, until.unix_millis());
    assert_eq!(truncates(&rig), 2, "{seen:?}");
    assert_eq!(fulls(&rig), 1, "{:?}", rig.ck.calls());
    assert!(
        ranges(&rig)
            .iter()
            .filter(|r| !before.contains(r))
            .all(|(_, from)| *from != Some(FAILED)),
        "{:?}",
        ranges(&rig)
    );
}

/// A second failed sync, in the generation the first TRUNCATE started, is a new debt: another
/// TRUNCATE, and nothing from that generation is staged past it.
#[test]
fn a_second_failed_sync_in_a_later_generation_truncates_again() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    rig.settle(&mut actor, until.unix_millis());
    assert_eq!(truncates(&rig), 1);
    let generation = rig.wal.committed().unwrap().end().salts();
    let generation = (generation.salt1, generation.salt2);
    let before = ranges(&rig);
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.commit(5);
    let failed = rig.tick(&mut actor, until.unix_millis() + 1);
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    rig.settle(&mut actor, until.unix_millis());
    assert_eq!(truncates(&rig), 2);
    assert!(
        ranges(&rig)
            .iter()
            .filter(|r| !before.contains(r))
            .all(|(_, from)| *from != Some(generation)),
        "{before:?} then {:?}",
        ranges(&rig)
    );
}

/// The kernel evicts the pages the failed sync covered, so a read of the WAL returns the disk's
/// older bytes (`crate::cure`): the newest frame no longer passes its checksum. The cure reads
/// the WAL with checked reads before it checkpoints, finds the chain broken below the committed
/// end, and runs no TRUNCATE, which would have copied those older bytes into the database file.
#[test]
fn a_cure_that_finds_an_evicted_frame_runs_no_checkpoint() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    let len = rig.wal.file_bytes().len();
    rig.wal.corrupt_byte(len - 8);
    let retried = rig.tick(&mut actor, until.unix_millis());
    let Some(Event::Degraded { cause, .. }) = event(&retried) else {
        panic!("{retried:?}");
    };
    assert_eq!(*cause, PersistentCause::WalCorrupt);
    assert_eq!(truncates(&rig), 0, "{:?}", rig.ck.calls());
}

/// A checkpointer that does something to the WAL right before the first FULL checkpoint reaches
/// it: between the cure's checked read and SQLite's own read of the same frames.
struct BeforeFull<'a> {
    inner: &'a mut FakeCheckpointer,
    wal: FakeWal,
    act: Option<fn(&FakeWal)>,
}

impl Checkpointer for BeforeFull<'_> {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.inner.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.inner.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        if mode == CheckpointMode::Full
            && let Some(act) = self.act.take()
        {
            act(&self.wal);
        }
        self.inner.checkpoint(mode)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.inner.bump_seq()
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        self.inner.begin_write_lock()
    }

    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError> {
        self.inner.end_write_lock(lock)
    }
}

/// One tick whose first FULL checkpoint is preceded by `act`.
fn tick_before_full(rig: &mut Rig, actor: &mut Actor, now: i64, act: fn(&FakeWal)) -> Tick {
    let mut ck = BeforeFull {
        inner: &mut rig.ck,
        wal: rig.wal.clone(),
        act: Some(act),
    };
    let env = Env {
        wal: &mut rig.wal,
        db: &mut rig.db,
        ck: &mut ck,
        staging: &rig.staging,
        remote: &rig.remote,
        clock: &FakeClock::new(ts(now)),
        journal: None,
    };
    block_on(actor.tick(env, ts(now)))
}

/// The kernel evicts the newest frame's page after the cure's checked read and before SQLite's
/// FULL checkpoint reads it: the checkpoint copies the disk's older bytes into the database file.
fn evict_newest_page(wal: &FakeWal) {
    let len = wal.file_bytes().len();
    wal.corrupt_byte(len.saturating_sub(8));
}

/// A page evicted between the cure's scan and its FULL checkpoint is copied as the disk's older
/// bytes; the read-back finds it unlike its verified frame. That is lost commits: the verdict is
/// raised before any TRUNCATE (so the journal holds it first), the newest staged transaction is
/// kept as the restore point, and the next attempt runs the TRUNCATE alone, without scanning the
/// frame again, then the snapshot.
#[test]
fn a_page_evicted_before_the_full_checkpoint_copies_it_is_lost_commits() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    let staged = ranges(&rig).last().map(|(txid, _)| *txid).unwrap();
    let lost = tick_before_full(&mut rig, &mut actor, until.unix_millis(), evict_newest_page);
    let Some(Event::Degraded { cause, until, .. }) = event(&lost) else {
        panic!("{lost:?}");
    };
    assert_eq!(
        *cause,
        PersistentCause::WalLostCommits {
            pages: nz(1),
            first: nz(1),
        }
    );
    assert_eq!(truncates(&rig), 0, "the verdict comes before the TRUNCATE");
    let ahead = actor.ahead().expect("the verdict stands");
    assert_eq!(ahead.kind, lsr_journal::AheadKind::WalLostCommits);
    assert_eq!(ahead.restore_point.get(), staged);
    let seen = rig.settle(&mut actor, until.unix_millis());
    assert!(
        seen.iter()
            .any(|t| event(t) == Some(&Event::Resnapshot(SnapshotCause::WalSyncFailed))),
        "{seen:?}"
    );
    assert_eq!(
        (fulls(&rig), truncates(&rig)),
        (1, 1),
        "{:?}",
        rig.ck.calls()
    );
    assert_eq!(actor.degraded(Task::Sync), None);
}

/// A commit that lands between the cure's scan and its FULL checkpoint is copied unscanned: the
/// checkpoint's counts are not the scanned frames', so nothing is proven. The debt stands, the
/// task says so, no TRUNCATE runs, and the next attempt scans again and completes.
#[test]
fn a_commit_between_the_scan_and_the_full_checkpoint_proves_nothing() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    let grown = tick_before_full(&mut rig, &mut actor, until.unix_millis(), |wal| {
        wal.append_tx(&[(pg(2), vec![9; PS.bytes()])], nz(3));
    });
    let Some(Event::Degraded { cause, until, .. }) = event(&grown) else {
        panic!("{grown:?}");
    };
    assert_eq!(*cause, PersistentCause::WalSyncFailed);
    assert_eq!(truncates(&rig), 0);
    assert_eq!(actor.ahead(), None, "no verdict");
    let seen = rig.settle(&mut actor, until.unix_millis());
    assert!(
        seen.iter()
            .any(|t| event(t) == Some(&Event::Resnapshot(SnapshotCause::WalSyncFailed))),
        "{seen:?}"
    );
    assert_eq!(
        (fulls(&rig), truncates(&rig)),
        (2, 1),
        "{:?}",
        rig.ck.calls()
    );
}

/// A FULL checkpoint behind a reader copies only part of the WAL and says busy: the failed
/// sync's fault again, and no TRUNCATE runs over frames nothing proved.
#[test]
fn a_busy_full_checkpoint_is_the_failed_sync_again() {
    let (mut rig, mut actor, failed) = failed_sync();
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    rig.ck.script(CheckpointScript {
        busy: true,
        effect: CheckpointEffect::Backfill(1),
    });
    let retried = rig.tick(&mut actor, until.unix_millis());
    let Some(Event::Degraded { cause, .. }) = event(&retried) else {
        panic!("{retried:?}");
    };
    assert_eq!(*cause, PersistentCause::WalSyncFailed);
    assert_eq!((fulls(&rig), truncates(&rig)), (1, 0));
}

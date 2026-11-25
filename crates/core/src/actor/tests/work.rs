//! The engine's running totals of its own work (`crate::counts`), as the sync steps and
//! checkpoints of real ticks leave them.

use super::*;
use crate::counts::CallTotals;

/// A sync step that stages one commit counts one call and the WAL bytes of its frame.
#[test]
fn a_sync_step_counts_one_call_and_its_frames() {
    let (mut rig, mut actor) = caught_up();
    let before = actor.work();
    let offset = actor.synced_wal_offset();
    rig.commit(2);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 1)),
        Some(&Event::Synced { files: 1 })
    );
    let after = actor.work();
    let frame = u64::try_from(lsr_wal::FRAME_HEADER_LEN + PS.bytes()).unwrap();
    assert_eq!(after.sync().calls(), before.sync().calls() + 1);
    assert_eq!(after.sync().errors(), before.sync().errors());
    assert_eq!(after.wal_bytes(), before.wal_bytes() + frame);
    assert_eq!(actor.synced_wal_offset(), offset + frame);
    assert!(!after.disk_full());
}

/// A sync step that cannot stage for want of room fails as a full disk, and the next step
/// that stages clears it.
#[test]
fn a_full_disk_is_reported_until_a_step_stages() {
    let (mut rig, mut actor) = caught_up();
    rig.staging.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::StorageFull,
        },
    );
    rig.commit(2);
    let failed = rig.tick(&mut actor, T0 + 1);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { .. })),
        "{failed:?}"
    );
    let work = actor.work();
    assert!(work.disk_full());
    assert_eq!(work.sync().errors(), 1);
    rig.settle(&mut actor, T0 + 600_000);
    assert!(!actor.work().disk_full());
}

/// A boot the store fails is a sync attempt that failed, as Go's `syncLocked` counts the
/// failed `init` (`db.go:1360-1376`, `db.go:1210`): an alert on the error counter must see a
/// store that refuses the credentials or cannot be reached, which no later step reports.
#[test]
fn a_boot_the_store_fails_counts_as_a_failed_sync_step() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.remote
        .inner
        .inject_failure(StoreOp::List, transient(StoreOp::List));
    let failed = rig.tick(&mut actor, T0);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { .. })),
        "{failed:?}"
    );
    assert!(!actor.booted());
    let work = actor.work();
    assert_eq!((work.sync().calls(), work.sync().errors()), (1, 1));
    assert!(!work.disk_full());
    // The boot that succeeds is no sync step: the first real step counts itself.
    rig.settle(&mut actor, T0 + 600_000);
    assert!(actor.booted());
    assert_eq!(actor.work().sync().errors(), 1);
}

/// Every boot attempt the store fails counts, not only the first.
#[test]
fn each_boot_attempt_that_fails_counts_once() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    for (n, at) in [T0, T0 + 600_000].into_iter().enumerate() {
        rig.remote
            .inner
            .inject_failure(StoreOp::List, transient(StoreOp::List));
        rig.tick(&mut actor, at);
        assert_eq!(actor.work().sync().errors(), u64::try_from(n + 1).unwrap());
        assert_eq!(actor.work().sync().calls(), u64::try_from(n + 1).unwrap());
    }
}

/// Each checkpoint SQLite is asked for is counted under its mode, failures included.
#[test]
fn checkpoints_are_counted_per_mode() {
    let mut rig = Rig::new();
    let mut config = cfg();
    config.checkpoint.min_pages = Some(nz(1));
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    let before = actor.work().checkpoints(CheckpointMode::Passive);
    rig.ck
        .inject(SourceOp::Checkpoint, SourceError::Io(ErrorKind::Other));
    rig.commit(2);
    rig.tick(&mut actor, T0 + 1);
    let failed = actor.work().checkpoints(CheckpointMode::Passive);
    assert_eq!(failed.calls(), before.calls() + 1);
    assert_eq!(failed.errors(), before.errors() + 1);
    rig.commit(3);
    rig.settle(&mut actor, T0 + 600_000);
    let after = actor.work().checkpoints(CheckpointMode::Passive);
    assert!(after.calls() > failed.calls(), "{after:?}");
    assert_eq!(after.errors(), failed.errors());
    assert_eq!(
        actor.work().checkpoints(CheckpointMode::Truncate),
        CallTotals::default()
    );
}

/// Across a WAL restart the new generation's frames count from its header, not as a
/// difference from the old generation's offset, which lies further on.
#[test]
fn wal_bytes_across_a_restart_count_the_new_generation_from_its_header() {
    let (mut rig, mut actor) = caught_up();
    for n in 2..=4 {
        rig.commit(n);
        rig.settle(&mut actor, T0 + 1);
    }
    let before = actor.work().wal_bytes();
    rig.wal.force_restart();
    rig.commit(5);
    let committed = rig.wal.committed_end().offset().get();
    rig.settle(&mut actor, T0 + 2);
    let header = u64::try_from(lsr_wal::HEADER_LEN).unwrap();
    assert_eq!(actor.work().wal_bytes() - before, committed - header);
    assert_eq!(actor.synced_wal_offset(), committed);
}

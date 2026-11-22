//! The live pages a scheduled verify compares a restore with, and the
//! restore point it pins.

use super::*;
use crate::verify::{Compared, verify_against};

fn newest_on_replica(rig: &Rig) -> Option<Txid> {
    Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max())
        .max()
}

/// Nothing is read until asked, and once asked the pages are those of the
/// transaction the replica holds, which a restore of it reproduces.
#[test]
fn live_pages_are_read_once_asked_at_the_replica_s_transaction() {
    let (mut rig, mut actor) = caught_up();
    rig.tick(&mut actor, T0 + 1);
    assert_eq!(actor.take_live_pages(), None);
    actor.read_live_pages();
    rig.settle(&mut actor, T0 + 2);
    let live = actor.take_live_pages().unwrap();
    assert_eq!(Some(live.txid()), newest_on_replica(&rig));
    assert_eq!(live.commit(), nz(3));
    assert_eq!(actor.take_live_pages(), None, "handed over once");
    let verified = block_on(verify_against(&rig.remote.inner, &live)).unwrap();
    assert!(
        matches!(verified.compared(), Compared::Same { pages: 3, .. }),
        "{:?}",
        verified.compared()
    );
}

/// A commit not yet on the replica is waited for: the pages are read only
/// once its upload is confirmed, at its transaction.
#[test]
fn live_pages_wait_for_the_upload_of_a_new_commit() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(7);
    actor.read_live_pages();
    let sync = rig.tick(&mut actor, T0 + 1);
    assert_eq!(event(&sync), Some(&Event::Synced { files: 1 }));
    assert_eq!(actor.take_live_pages(), None, "the replica lacks it");
    rig.settle(&mut actor, T0 + 1);
    rig.tick(&mut actor, T0 + 2);
    let live = actor.take_live_pages().unwrap();
    assert_eq!(Some(live.txid()), newest_on_replica(&rig));
    assert_eq!(live.txid(), actor.staged().unwrap());
    let verified = block_on(verify_against(&rig.remote.inner, &live)).unwrap();
    assert!(
        matches!(verified.compared(), Compared::Same { .. }),
        "{:?}",
        verified.compared()
    );
}

/// While an upload is behind, a quiet tick does not open the WAL for the
/// live pages: they are read only at the transaction the replica holds, which
/// no sync can prove until the upload lands.
#[test]
fn live_pages_behind_an_upload_do_not_open_a_quiet_wal() {
    let (mut rig, mut actor) = caught_up();
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.commit(7);
    let sync = rig.tick(&mut actor, T0 + 1);
    assert_eq!(event(&sync), Some(&Event::Synced { files: 1 }));
    let failed = rig.tick(&mut actor, T0 + 1);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { .. })),
        "{failed:?}"
    );
    rig.settle(&mut actor, T0 + 1);
    actor.read_live_pages();
    rig.wal
        .inject(SourceOp::Open, SourceError::Io(ErrorKind::Other));
    let tick = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&tick), Some(&Event::Idle));
    assert_eq!(actor.take_live_pages(), None);
}

/// A draining engine reads no live pages: a drain only syncs and uploads.
#[test]
fn a_drain_reads_no_live_pages() {
    let (mut rig, mut actor) = caught_up();
    actor.read_live_pages();
    actor.begin_shutdown(ts(T0 + 60_000));
    rig.follow(&mut actor, T0 + 1, 20);
    assert_eq!(actor.take_live_pages(), None);
}

/// A verify's kept restore point is the restore point retention keeps; a degraded task's
/// older one wins while both stand, and each goes on its own.
#[test]
fn a_verify_restore_point_is_kept_beside_a_task_restore_point_and_the_older_wins() {
    let (mut rig, mut actor) = caught_up();
    assert_eq!(actor.kept_restore_point(), None);
    let first = newest_on_replica(&rig).unwrap();
    rig.commit(8);
    rig.settle(&mut actor, T0 + 1);
    let second = newest_on_replica(&rig).unwrap();
    assert!(second > first);
    for kept in [first, second] {
        actor.engine.maintain.stale = false;
        actor.keep_restore_point_for_verify(Some(kept));
        assert_eq!(actor.kept_restore_point(), Some(kept));
        assert!(
            !actor.engine.maintain.stale,
            "keeping or moving a restore point only holds deletes back: nothing to list again"
        );
    }
    actor.kept.task_restore_point = TaskRestorePoint::Held(Some(first));
    assert_eq!(actor.kept_restore_point(), Some(first));
    actor.engine.maintain.stale = false;
    actor.keep_restore_point_for_verify(None);
    assert_eq!(actor.kept_restore_point(), Some(first));
    assert!(
        actor.engine.maintain.stale,
        "a released kept restore point lists again"
    );
    actor.keep_restore_point_for_verify(Some(second));
    actor.kept.task_restore_point = TaskRestorePoint::Free;
    assert_eq!(actor.kept_restore_point(), Some(second));
}

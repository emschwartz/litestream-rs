//! The WAL is synced before any frame read from it is staged.

use super::*;

/// A failed sync of the WAL stages nothing: the frames it covered may not be on disk, and a
/// replica holding them would be ahead of the database after a power loss. The tick reports
/// the fault and retries; the next sync that succeeds stages the transaction.
#[test]
fn a_failed_wal_sync_stages_nothing() {
    let (mut rig, mut actor) = caught_up();
    let before = staged(&rig);
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.commit(2);
    let failed = rig.tick(&mut actor, T0 + 1);
    let Some(Event::Retrying { fault, until, .. }) = event(&failed) else {
        panic!("the sync should fail: {failed:?}");
    };
    assert_eq!(*fault, Fault::LocalFsyncFailed);
    assert_eq!(staged(&rig), before, "nothing is staged past a failed sync");
    let until = until.unix_millis();
    rig.settle(&mut actor, until);
    assert!(
        staged(&rig).last() > before.last(),
        "the retry stages it: {:?} after {before:?}",
        staged(&rig)
    );
}

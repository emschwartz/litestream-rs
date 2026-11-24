//! A WAL truncated while a running engine reads is no power loss.

use super::*;
use crate::SnapshotCauseKind;
use crate::position::ReadLockHold;
use crate::source::CommittedRead;

/// A WAL the application's TRUNCATE checkpoint empties between the engine's reads of one step:
/// the length read is the first thing to see the empty file.
struct TruncatedAtLen {
    inner: FakeWal,
    shrunk: bool,
}

impl WalSource for TruncatedAtLen {
    type Wal = <FakeWal as WalSource>::Wal;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        self.inner.open()
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        if !self.shrunk {
            self.shrunk = true;
            self.inner.truncate_file(0);
        }
        self.inner.len()
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        self.inner.committed()
    }

    fn epoch(&self) -> ReadLockHold {
        self.inner.epoch()
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        self.inner.sync()
    }
}

/// The frames a running step cannot find in a file that shrank under it were checkpointed
/// into the database, not lost by the disk: the step does not call the WAL rewound and does
/// not snapshot for it. It judges the file as it is now, an empty one.
#[test]
fn a_wal_truncated_during_a_step_is_not_a_rewound_wal() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    rig.commit(3);
    let mut wal = TruncatedAtLen {
        inner: rig.wal.clone(),
        shrunk: false,
    };
    let env = Env {
        wal: &mut wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &rig.staging,
        remote: &rig.remote,
        clock: &FakeClock::new(ts(T0 + 2)),
    };
    let tick = block_on(actor.tick(env, ts(T0 + 2)));
    assert!(
        !matches!(
            event(&tick),
            Some(Event::Resnapshot(SnapshotCause::LocalRewound { .. }))
        ),
        "{tick:?}"
    );
    assert_eq!(actor.snapshots().get(SnapshotCauseKind::LocalRewound), 0);
}

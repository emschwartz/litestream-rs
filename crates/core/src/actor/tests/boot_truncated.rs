//! A WAL truncated while a boot reads is no power loss.

use std::io::Cursor;

use super::*;
use crate::position::ReadLockHold;
use crate::source::CommittedRead;

/// A WAL the application's TRUNCATE checkpoint empties while the boot reads it: the engine
/// stats the file whole and reads its header, and every frame it reads after is gone.
struct TruncatedAfterOpen(FakeWal);

impl WalSource for TruncatedAfterOpen {
    type Wal = <FakeWal as WalSource>::Wal;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        let bytes = self.0.file_bytes();
        self.0.truncate_file(0);
        Ok(Some(Cursor::new(
            bytes.get(..32).unwrap_or(&bytes).to_vec(),
        )))
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        self.0.len()
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        self.0.committed()
    }

    fn epoch(&self) -> ReadLockHold {
        self.0.epoch()
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        self.0.sync()
    }
}

/// The frames a boot cannot read from a file that shrank under it are no sign of a power
/// loss: the boot fails as `WalMoved` and runs again, over the file as it is now. It does not
/// snapshot the database for frames the disk never lost.
#[test]
fn a_wal_truncated_during_the_boot_is_not_a_rewound_wal() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    let mut wal = TruncatedAfterOpen(rig.wal.clone());
    let mut booted = Actor::new(cfg());
    let env = Env {
        wal: &mut wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &rig.staging,
        remote: &rig.remote,
        clock: &FakeClock::new(ts(T0 + 2)),
    };
    let tick = block_on(booted.tick(env, ts(T0 + 2)));
    assert!(
        matches!(
            event(&tick),
            Some(Event::Retrying {
                fault: Fault::WalMoved,
                ..
            })
        ),
        "{tick:?}"
    );
    assert_eq!(booted.snapshots().total(), 0);
}

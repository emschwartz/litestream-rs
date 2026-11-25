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

    fn modified(&mut self) -> Result<Option<Timestamp>, SourceError> {
        self.inner.modified()
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
        notice: &mut |_| {},
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

/// A WAL the application's TRUNCATE checkpoint empties after the step read its length and
/// before it read the frames: the length the step holds is the old one.
struct TruncatedAfterLen {
    inner: FakeWal,
    shrunk: bool,
    /// Where the checkpoint cuts the file: into the newest frame the engine has synced, so the
    /// header stays and frames it holds are gone.
    cut: usize,
}

impl WalSource for TruncatedAfterLen {
    type Wal = <FakeWal as WalSource>::Wal;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        self.inner.open()
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        let len = self.inner.len();
        if !self.shrunk {
            self.shrunk = true;
            self.inner.truncate_file(self.cut);
        }
        len
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

    fn modified(&mut self) -> Result<Option<Timestamp>, SourceError> {
        self.inner.modified()
    }
}

/// The length of the rig's WAL file now, in bytes.
fn wal_bytes(rig: &Rig) -> usize {
    let bytes = rig
        .wal
        .clone()
        .len()
        .ok()
        .flatten()
        .map_or(0, WalFileLen::bytes);
    usize::try_from(bytes).unwrap_or(0)
}

/// A step whose file shrinks between its length read and its frame read ends in the WAL-moved
/// fault, retried over the file as it is, not in a snapshot for a rewound WAL.
#[test]
fn a_wal_shrinking_between_the_length_and_the_frames_is_retried() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    let synced_end = wal_bytes(&rig);
    rig.commit(3);
    let mut wal = TruncatedAfterLen {
        inner: rig.wal.clone(),
        shrunk: false,
        cut: synced_end.saturating_sub(1),
    };
    let env = Env {
        wal: &mut wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &rig.staging,
        remote: &rig.remote,
        notice: &mut |_| {},
        clock: &FakeClock::new(ts(T0 + 2)),
    };
    let tick = block_on(actor.tick(env, ts(T0 + 2)));
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
    assert_eq!(actor.snapshots().get(SnapshotCauseKind::LocalRewound), 0);
}

/// A WAL that was already short when the step first measured it is a real rewind (a power loss
/// that took frames the engine had synced): the step snapshots for it. Retrying it as moved
/// would repeat the same reads for ever, as the file never shrinks further.
#[test]
fn a_wal_already_short_at_the_first_length_read_is_a_rewound_wal() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    let synced_end = wal_bytes(&rig);
    rig.commit(3);
    rig.wal.truncate_file(synced_end.saturating_sub(1));
    let tick = rig.tick(&mut actor, T0 + 2);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Resnapshot(SnapshotCause::LocalRewound { .. }))
        ),
        "{tick:?}"
    );
}

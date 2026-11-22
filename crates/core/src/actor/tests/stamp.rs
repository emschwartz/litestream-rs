//! A staged file's created-at time follows every transaction it carries.
//!
//! The tick's `now` is read before the tick looks at the WAL, so the application can commit in
//! between. A file stamped with that `now` is recorded as created before a transaction it
//! carries, and `AsOf` for an instant between the two applies a commit made after the instant
//! (`history/tests/history/replicator.rs`, 636 ms under load). The stamp is read from
//! `Env::clock` after the committed read that bounds the file.

use super::*;
use crate::position::ReadLockHold;
use crate::source::{Barrier, CheckpointRun, WriteLock};

const LATE: Duration = Duration::from_millis(636);

/// The application's commit, made while the clock moves on: the clock advances by [`LATE`], the
/// transaction lands, and the instant it landed is kept.
#[derive(Clone)]
struct LateCommit {
    wal: FakeWal,
    clock: Arc<FakeClock>,
    armed: Arc<std::sync::Mutex<Option<u8>>>,
    at: Arc<std::sync::Mutex<Option<Timestamp>>>,
}

impl LateCommit {
    fn new(rig: &Rig, now: i64) -> Self {
        Self {
            wal: rig.wal.clone(),
            clock: Arc::new(FakeClock::new(ts(now))),
            armed: Arc::default(),
            at: Arc::default(),
        }
    }

    fn arm(&self, n: u8) {
        *self.armed.lock().unwrap() = Some(n);
    }

    fn fire(&self) {
        if let Some(n) = self.armed.lock().unwrap().take() {
            self.clock.advance(LATE);
            self.wal.append_tx(&[(pg(1), vec![n; PS.bytes()])], nz(3));
            *self.at.lock().unwrap() = Some(self.clock.now());
        }
    }

    fn committed_at(&self) -> Timestamp {
        self.at.lock().unwrap().expect("the late commit landed")
    }
}

/// A WAL the application commits to right before the engine reads its committed end.
struct BeforeCommittedRead(LateCommit);

impl WalSource for BeforeCommittedRead {
    type Wal = <FakeWal as WalSource>::Wal;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        self.0.wal.open()
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        self.0.wal.len()
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        self.0.fire();
        self.0.wal.committed()
    }

    fn epoch(&self) -> ReadLockHold {
        self.0.wal.epoch()
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        self.0.wal.sync()
    }
}

/// A checkpointer the application commits through right before the write barrier keeps it out:
/// the commit the checkpoint's seal stages.
struct BeforeBarrier<'a> {
    inner: &'a mut FakeCheckpointer,
    late: LateCommit,
}

impl Checkpointer for BeforeBarrier<'_> {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.late.fire();
        self.inner.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.inner.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
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

/// The created-at time of each staged level-0 file that carries a transaction past `after`.
fn stamps_past(rig: &Rig, after: Option<&u64>) -> Vec<(u64, Timestamp)> {
    Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .filter(|f| Some(&f.key.range().max().get()) > after)
        .map(|f| {
            (
                f.key.range().max().get(),
                f.created_at().expect("stamped").at(),
            )
        })
        .collect()
}

/// The application commits after the tick read its clock and before the sync step reads the
/// committed end: the staged file carries that commit, so it is stamped no earlier than it.
#[test]
fn a_commit_after_the_tick_began_is_not_stamped_before_it_landed() {
    let (mut rig, mut actor) = caught_up();
    let before = staged(&rig);
    let late = LateCommit::new(&rig, T0 + 1);
    late.arm(2);
    let mut wal = BeforeCommittedRead(late.clone());
    let env = Env {
        wal: &mut wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &rig.staging,
        remote: &rig.remote,
        clock: late.clock.as_ref(),
    };
    let tick = block_on(actor.tick(env, ts(T0 + 1)));
    assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }), "{tick:?}");
    let stamps = stamps_past(&rig, before.last());
    assert!(!stamps.is_empty(), "the late commit is staged");
    for (txid, stamp) in stamps {
        assert!(
            stamp >= late.committed_at(),
            "transaction {txid} is stamped {stamp:?}, before its commit at {:?}",
            late.committed_at()
        );
    }
}

/// The application commits inside the tick, right before the checkpoint's barrier: the seal
/// stages that commit, so the seal is stamped no earlier than it.
#[test]
fn a_commit_before_the_checkpoint_barrier_is_not_stamped_before_it_landed() {
    let mut rig = Rig::new();
    let mut config = cfg();
    config.checkpoint.min_pages = Some(nz(1));
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    rig.commit(2);
    let before = staged(&rig);
    let late = LateCommit::new(&rig, T0 + 1);
    late.arm(3);
    let mut ck = BeforeBarrier {
        inner: &mut rig.ck,
        late: late.clone(),
    };
    let env = Env {
        wal: &mut rig.wal,
        db: &mut rig.db,
        ck: &mut ck,
        staging: &rig.staging,
        remote: &rig.remote,
        clock: late.clock.as_ref(),
    };
    let tick = block_on(actor.tick(env, ts(T0 + 1)));
    assert!(
        matches!(event(&tick), Some(Event::Checkpointed { .. })),
        "{tick:?}"
    );
    let stamps = stamps_past(&rig, before.last());
    let (sealed, stamp) = *stamps.last().expect("the seal staged a file");
    assert!(
        stamps.len() >= 2,
        "the sync's file and the seal: {stamps:?}"
    );
    assert!(
        stamp >= late.committed_at(),
        "the seal {sealed} is stamped {stamp:?}, before the commit it carries at {:?}",
        late.committed_at()
    );
}

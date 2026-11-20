use std::future::Future;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use lsr_ltx::{PageNo, PageSize, TxidRange};
use lsr_wal::Salts;

use super::*;
use crate::conformance::block_on;
use crate::layout::{FileKey, Level};
use crate::mem::MemStore;
use crate::source::{CheckpointCall, FakeCheckpointer, FakeDb, FakeWal, SourceError, SourceOp};
use crate::store::{Listing, PageLimit, ReadLen, StoreError, StoreOp};

const PS: PageSize = PageSize::MIN;
const T0: i64 = 1_000_000;

fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn ts(n: i64) -> Timestamp {
    Timestamp::from_unix_millis(n)
}

/// A store that counts every call it is asked to make.
#[derive(Clone, Default)]
struct Counting {
    inner: MemStore,
    calls: Arc<AtomicU32>,
}

impl Counting {
    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn count(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl ObjectStore for Counting {
    type Upload = <MemStore as ObjectStore>::Upload;

    fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
        self.count();
        self.inner.list(level, after, limit)
    }

    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        self.count();
        self.inner.read(key, offset, len)
    }

    fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> impl Future<Output = Result<Self::Upload, StoreError>> + Send {
        self.count();
        self.inner.begin_upload(key, created_at)
    }

    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.count();
        self.inner.delete(keys)
    }
}

struct Rig {
    wal: FakeWal,
    db: FakeDb,
    ck: FakeCheckpointer,
    staging: Counting,
    remote: Counting,
}

impl Rig {
    fn new() -> Self {
        let wal = FakeWal::new(
            PS,
            Salts {
                salt1: 10,
                salt2: 20,
            },
        );
        let mut db = FakeDb::new(PS);
        db.set_page_count(3);
        for n in 1..=3 {
            db.write_page(pg(n), &vec![u8::try_from(n).unwrap(); PS.bytes()]);
        }
        let ck = FakeCheckpointer::new(wal.clone());
        let rig = Self {
            wal,
            db,
            ck,
            staging: Counting::default(),
            remote: Counting::default(),
        };
        rig.commit(1);
        rig
    }

    fn commit(&self, n: u8) {
        self.wal.append_tx(&[(pg(1), vec![n; PS.bytes()])], nz(3));
    }

    fn tick(&mut self, actor: &mut Actor, now: i64) -> Tick {
        let env = Env {
            wal: &mut self.wal,
            db: &mut self.db,
            ck: &mut self.ck,
            staging: &self.staging,
            remote: &self.remote,
        };
        block_on(actor.tick(env, ts(now)))
    }

    /// Ticks at `now` while the actor says it has work.
    fn settle(&mut self, actor: &mut Actor, now: i64) -> Vec<Tick> {
        let mut seen = Vec::new();
        for _ in 0..100 {
            let tick = self.tick(actor, now);
            let more = matches!(
                tick,
                Tick::Ran {
                    next: Next::Now,
                    ..
                }
            );
            seen.push(tick);
            if !more {
                return seen;
            }
        }
        panic!("never settled: {seen:?}");
    }

    /// Ticks, following the actor's own wake times, until it halts, drains,
    /// waits on the WAL, or `limit` ticks have run.
    fn follow(&mut self, actor: &mut Actor, mut now: i64, limit: usize) -> Vec<Tick> {
        let mut seen = Vec::new();
        for _ in 0..limit {
            let tick = self.tick(actor, now);
            let next = match &tick {
                Tick::Ran { next, .. } => *next,
                Tick::Halted(_) | Tick::Drained => Next::OnWalChange,
            };
            seen.push(tick);
            match next {
                Next::Now => {}
                Next::At(at) => now = now.max(at.unix_millis()),
                Next::OnWalChange => break,
            }
        }
        seen
    }

    fn io(&self) -> (u32, u32, usize) {
        (
            self.staging.calls(),
            self.remote.calls(),
            self.ck.calls().len(),
        )
    }

    fn files(store: &Counting, level: Level) -> Vec<FileInfo> {
        block_on(Inventory::load(&store.inner))
            .unwrap()
            .level(level)
            .to_vec()
    }
}

fn cfg() -> ActorConfig {
    ActorConfig {
        sync: SyncConfig::default(),
        checkpoint: CheckpointConfig {
            interval: None,
            min_pages: None,
            truncate_pages: None,
        },
        upload: UploadConfig::default(),
        maintenance: Maintenance::go_default(),
        retry_interval: Duration::from_secs(1),
        shutdown_grace: Duration::from_secs(10),
    }
}

fn event(tick: &Tick) -> Option<&Event> {
    match tick {
        Tick::Ran { event, .. } => Some(event),
        Tick::Halted(_) | Tick::Drained => None,
    }
}

fn next_of(tick: &Tick) -> Option<Next> {
    match tick {
        Tick::Ran { next, .. } => Some(*next),
        Tick::Halted(_) | Tick::Drained => None,
    }
}

const fn transient(op: StoreOp) -> StoreError {
    StoreError::Transient {
        op,
        kind: ErrorKind::TimedOut,
    }
}

/// A booted actor with everything committed synced and uploaded.
fn caught_up() -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    (rig, actor)
}

#[test]
fn boot_lists_then_recovers_then_uploads() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::FirstSync)
        })
    );
    assert!(rig.remote.calls() > 0, "boot lists the replica");
    assert!(Rig::files(&rig.remote, Level::ZERO).is_empty());
    assert_eq!(actor.phase(), Phase::Running);
    assert_eq!(
        event(&rig.tick(&mut actor, T0)),
        Some(&Event::Synced { files: 1 })
    );
    let upload = rig.tick(&mut actor, T0);
    assert!(
        matches!(event(&upload), Some(Event::Uploaded(Did::Put(_)))),
        "{upload:?}"
    );
    assert_eq!(Rig::files(&rig.remote, Level::ZERO).len(), 1);
}

#[test]
fn sync_precedes_checkpoint_same_tick() {
    let mut rig = Rig::new();
    let mut config = cfg();
    config.checkpoint.min_pages = Some(nz(1));
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    let staged = Rig::files(&rig.staging, Level::ZERO).len();
    let calls = rig.ck.calls().len();
    rig.commit(2);
    let tick = rig.tick(&mut actor, T0 + 1);
    assert!(
        matches!(event(&tick), Some(Event::Checkpointed { .. })),
        "{tick:?}"
    );
    assert!(rig.ck.calls().len() > calls);
    assert!(
        Rig::files(&rig.staging, Level::ZERO).len() > staged,
        "the new transaction is staged in the same tick, before the checkpoint"
    );
}

#[test]
fn maintenance_waits_for_idle_uploads() {
    let mut rig = Rig::new();
    let mut config = cfg();
    config.upload.files_per_tick = Some(nz(1));
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    for n in 2..5 {
        rig.commit(n);
        rig.tick(&mut actor, T0 + 1);
    }
    let seen = rig.settle(&mut actor, T0 + 2);
    let last_upload = seen
        .iter()
        .rposition(|t| matches!(event(t), Some(Event::Uploaded(_))))
        .unwrap();
    let first_maintain = seen
        .iter()
        .position(|t| matches!(event(t), Some(Event::Maintained(_))))
        .unwrap();
    assert!(last_upload < first_maintain, "{seen:?}");
}

#[test]
fn snapshot_job_writes_level_9() {
    let (rig, _actor) = caught_up();
    let snapshots = Rig::files(&rig.remote, Level::SNAPSHOT);
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].key.level(), Level::SNAPSHOT);
}

#[test]
fn retry_backs_off_per_lane() {
    let (mut rig, mut actor) = caught_up();
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.commit(2);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 10)),
        Some(&Event::Synced { files: 1 })
    );
    let first = rig.tick(&mut actor, T0 + 10);
    assert!(matches!(event(&first), Some(Event::Retrying { attempt, .. }) if attempt.get() == 1));
    assert_eq!(next_of(&first), Some(Next::At(ts(T0 + 1_010))));
    // The sync lane does not wait on the upload lane.
    rig.commit(3);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 20)),
        Some(&Event::Synced { files: 1 })
    );
    let second = rig.tick(&mut actor, T0 + 1_010);
    assert!(matches!(event(&second), Some(Event::Retrying { attempt, .. }) if attempt.get() == 2));
    assert_eq!(next_of(&second), Some(Next::At(ts(T0 + 3_010))));
    let third = rig.tick(&mut actor, T0 + 3_010);
    assert!(
        matches!(event(&third), Some(Event::Uploaded(_))),
        "{third:?}"
    );
}

#[test]
fn throttle_retry_after_honored() {
    let (mut rig, mut actor) = caught_up();
    let throttled = StoreError::Throttled {
        op: StoreOp::Upload,
        retry_after: Some(Duration::from_secs(7)),
    };
    rig.remote.inner.inject_failure(StoreOp::Upload, throttled);
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    let tick = rig.tick(&mut actor, T0 + 10);
    assert!(
        matches!(event(&tick), Some(Event::Retrying { .. })),
        "{tick:?}"
    );
    assert_eq!(next_of(&tick), Some(Next::At(ts(T0 + 7_010))));
}

#[test]
fn halt_is_terminal() {
    let (mut rig, mut actor) = caught_up();
    rig.remote.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Auth {
            op: StoreOp::Upload,
        },
    );
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    assert_eq!(
        rig.tick(&mut actor, T0 + 10),
        Tick::Halted(HaltCause::Credentials)
    );
    let io = rig.io();
    rig.commit(3);
    for at in [T0 + 10, T0 + 100_000] {
        assert_eq!(
            rig.tick(&mut actor, at),
            Tick::Halted(HaltCause::Credentials)
        );
    }
    assert_eq!(rig.io(), io, "a halted actor does no I/O");
    assert_eq!(actor.phase(), Phase::Halted(HaltCause::Credentials));
}

#[test]
fn resnapshot_policy_forces_snapshot_step() {
    let (mut rig, mut actor) = caught_up();
    let staged = Rig::files(&rig.staging, Level::ZERO).len();
    let tick = actor.fault(Lane::Sync, Fault::PageSizeChanged, ts(T0 + 10));
    assert_eq!(
        event(&tick),
        Some(&Event::Resnapshot(SnapshotCause::PageSizeChanged))
    );
    assert_eq!(next_of(&tick), Some(Next::Now));
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 10)),
        Some(&Event::Synced { files: 1 })
    );
    assert_eq!(Rig::files(&rig.staging, Level::ZERO).len(), staged + 1);
}

#[test]
fn clock_back_clamped_once() {
    let (mut rig, mut actor) = caught_up();
    let clamped = Event::Clamped(Clamped {
        prev: ts(T0),
        now: ts(T0 - 500),
    });
    assert_eq!(event(&rig.tick(&mut actor, T0 - 500)), Some(&clamped));
    for at in [T0 - 500, T0 - 600] {
        let tick = rig.tick(&mut actor, at);
        assert!(!matches!(event(&tick), Some(Event::Clamped(_))), "{tick:?}");
    }
    rig.tick(&mut actor, T0 + 10);
    let again = rig.tick(&mut actor, T0 + 5);
    assert!(
        matches!(event(&again), Some(Event::Clamped(_))),
        "a new step back is reported"
    );
}

#[test]
fn drain_finishes_uploads() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    actor.begin_shutdown(ts(T0 + 10));
    let seen = rig.follow(&mut actor, T0 + 10, 20);
    assert_eq!(seen.last(), Some(&Tick::Drained), "{seen:?}");
    assert!(
        !seen
            .iter()
            .any(|t| matches!(event(t), Some(Event::Maintained(_))))
    );
    let newest = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max())
        .max();
    let staged = Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max())
        .max();
    assert_eq!(newest, staged);
}

#[test]
fn drain_deadline_halts() {
    let (mut rig, mut actor) = caught_up();
    for _ in 0..20 {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    }
    rig.commit(2);
    actor.begin_shutdown(ts(T0 + 10));
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert_eq!(
        seen.last(),
        Some(&Tick::Halted(HaltCause::Shutdown)),
        "{seen:?}"
    );
}

/// The fault events a run produced: each is one call of `policy`.
fn routed(seen: &[Tick]) -> usize {
    seen.iter()
        .filter(|t| {
            matches!(t, Tick::Halted(_))
                || matches!(
                    event(t),
                    Some(Event::Retrying { .. } | Event::Resnapshot(_))
                )
        })
        .count()
}

#[derive(Clone, Copy, Debug)]
enum Site {
    Remote(StoreOp),
    Staging(StoreOp),
    Wal(SourceOp),
}

fn store_errors(op: StoreOp) -> Vec<StoreError> {
    vec![
        transient(op),
        StoreError::Throttled {
            op,
            retry_after: None,
        },
        StoreError::Auth { op },
        StoreError::Refused { op, status: 500 },
    ]
}

fn source_errors() -> Vec<SourceError> {
    vec![
        SourceError::Io(ErrorKind::Other),
        SourceError::Busy,
        SourceError::DiskFull,
        SourceError::FsyncFailed,
    ]
}

#[test]
fn policy_called_once_per_fault() {
    let sites = [
        Site::Remote(StoreOp::List),
        Site::Remote(StoreOp::Upload),
        Site::Staging(StoreOp::Read),
        Site::Staging(StoreOp::Upload),
        Site::Wal(SourceOp::Committed),
        Site::Wal(SourceOp::Open),
    ];
    for site in sites {
        let count = match site {
            Site::Remote(op) | Site::Staging(op) => store_errors(op).len(),
            Site::Wal(_) => source_errors().len(),
        };
        for which in 0..count {
            let mut rig = Rig::new();
            match site {
                Site::Remote(op) => rig
                    .remote
                    .inner
                    .inject_failure(op, store_errors(op).remove(which)),
                Site::Staging(op) => rig
                    .staging
                    .inner
                    .inject_failure(op, store_errors(op).remove(which)),
                Site::Wal(op) => rig.wal.inject(op, source_errors().remove(which)),
            }
            let mut actor = Actor::new(cfg());
            let seen = rig.follow(&mut actor, T0, 60);
            assert_eq!(routed(&seen), 1, "{site:?} #{which}: {seen:?}");
        }
    }
}

proptest::proptest! {
    #[test]
    fn idle_tick_does_no_io(
        history in proptest::collection::vec((0u8..3, 0i64..5_000), 0..8),
        idle in 1usize..6,
    ) {
        let mut rig = Rig::new();
        let mut config = cfg();
        config.checkpoint.min_pages = Some(nz(2));
        let mut actor = Actor::new(config);
        let mut now = T0;
        for (what, wait) in history {
            now = now.saturating_add(wait);
            match what {
                0 => rig.commit(7),
                1 => { rig.tick(&mut actor, now); }
                _ => { rig.settle(&mut actor, now); }
            }
        }
        now = now.saturating_add(1);
        rig.settle(&mut actor, now);
        let io = rig.io();
        for _ in 0..idle {
            let tick = rig.tick(&mut actor, now);
            proptest::prop_assert_eq!(event(&tick), Some(&Event::Idle));
        }
        proptest::prop_assert_eq!(rig.io(), io);
    }
}

#[test]
fn job_match_is_exhaustive() {
    for (name, source) in [
        ("actor.rs", include_str!("../actor.rs")),
        ("actor/state.rs", include_str!("state.rs")),
    ] {
        for (line, text) in source.lines().enumerate() {
            assert!(
                !text.contains(concat!("_ ", "=>")),
                "{name}:{}: a wildcard arm hides a variant",
                line + 1
            );
        }
    }
}

#[test]
fn checkpoint_call_is_recorded() {
    // The checkpointer's log is how the tests above count checkpoints.
    let mut rig = Rig::new();
    let mut config = cfg();
    config.checkpoint.min_pages = Some(nz(1));
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    assert!(
        rig.ck
            .calls()
            .iter()
            .any(|c| matches!(c, CheckpointCall::Checkpoint(CheckpointMode::Passive)))
    );
}

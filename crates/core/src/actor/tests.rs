use std::future::Future;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use lsr_ltx::{PageNo, PageSize, TxidRange};
use lsr_wal::Salts;

use super::*;
use crate::conformance::block_on;
use crate::layout::StoredStamp;
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

    fn stamp(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<StoredStamp, StoreError>> + Send {
        self.count();
        self.inner.stamp(file)
    }

    fn read_with_stamp(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, StoredStamp), StoreError>> + Send {
        self.count();
        self.inner.read_with_stamp(file, len)
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
    assert!(
        matches!(event(&first), Some(Event::Retrying { until, .. }) if *until == ts(T0 + 1_010))
    );
    // The prune of staged file 1 is not the upload lane's to hold.
    let pruned = rig.settle(&mut actor, T0 + 10);
    assert_eq!(
        pruned.last().and_then(next_of),
        Some(Next::At(ts(T0 + 1_010)))
    );
    // The sync lane does not wait on the upload lane.
    rig.commit(3);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 20)),
        Some(&Event::Synced { files: 1 })
    );
    let second = rig.tick(&mut actor, T0 + 1_010);
    assert!(
        matches!(event(&second), Some(Event::Retrying { until, .. }) if *until == ts(T0 + 3_010))
    );
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
    // A hint longer than the 1 s backoff, then one shorter than the 2 s.
    for millis in [7_000, 500] {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, throttled(StoreOp::Upload, millis));
    }
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    let tick = rig.tick(&mut actor, T0 + 10);
    let until = ts(T0 + 7_010);
    assert!(
        matches!(event(&tick), Some(Event::Retrying { until: u, .. }) if *u == until),
        "{tick:?}"
    );
    let pruned = rig.settle(&mut actor, T0 + 10);
    assert_eq!(pruned.last().and_then(next_of), Some(Next::At(until)));
    let second = next_retry(&mut rig, &mut actor, T0 + 7_010);
    assert_eq!(second, Some(ts(T0 + 9_010)), "the backoff, not the hint");
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

/// The last transaction of every staged level-0 file, oldest first.
fn staged(rig: &Rig) -> Vec<u64> {
    Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .collect()
}

fn level_one_max(rig: &Rig) -> Option<u64> {
    Rig::files(&rig.remote, Level::new(1)?)
        .last()
        .map(|f| f.key.range().max().get())
}

/// Commits `n` and runs the actor until it has staged and uploaded it.
fn commit_and_settle(rig: &mut Rig, actor: &mut Actor, n: u8, now: i64) {
    rig.commit(n);
    rig.settle(actor, now);
}

#[test]
fn job_retry_reports_the_levels_wait() {
    let (mut rig, mut actor) = caught_up();
    let at = T0 + 60_000;
    rig.commit(2);
    assert_eq!(
        event(&rig.tick(&mut actor, at)),
        Some(&Event::Synced { files: 1 })
    );
    assert!(matches!(
        event(&rig.tick(&mut actor, at)),
        Some(Event::Uploaded(_))
    ));
    // The level-1 compaction's output write fails; the wait is the level's.
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let failed = (0..5)
        .map(|_| rig.tick(&mut actor, at))
        .find(|t| matches!(event(t), Some(Event::Retrying { .. })))
        .unwrap();
    let until = ts(at + 10_000);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { until: u, .. }) if *u == until),
        "{failed:?}"
    );
    let seen = rig.settle(&mut actor, at);
    let next = seen.last().and_then(next_of);
    assert!(
        matches!(next, Some(Next::At(t)) if t <= until),
        "the next wake is no later than the level's: {seen:?}"
    );
}

const LANES: [Lane; 5] = [
    Lane::Sync,
    Lane::Checkpoint,
    Lane::Upload,
    Lane::Prune,
    Lane::Maintain,
];

/// Every deadline the actor holds, read from where each is kept.
fn held(actor: &Actor) -> Vec<Timestamp> {
    let running = actor.phase() == Phase::Running;
    let mut held: Vec<Timestamp> = LANES
        .iter()
        .filter(|lane| running || matches!(lane, Lane::Sync | Lane::Upload))
        .filter_map(|lane| actor.lanes.get(*lane).until())
        .collect();
    if running {
        if let Gate::Until(at) = actor.gate {
            held.push(at);
        }
        held.extend(actor.maintain.until);
    }
    if let Phase::Draining { deadline } = actor.phase() {
        held.push(deadline);
    }
    held
}

proptest::proptest! {
    #[test]
    fn next_is_never_after_a_held_deadline(
        waits in proptest::collection::vec(proptest::option::of(-3_000i64..3_000), 5),
        gate in 0u8..3,
        gate_at in -3_000i64..3_000,
        maintain in proptest::option::of(-3_000i64..3_000),
        draining in proptest::bool::ANY,
    ) {
        let (mut rig, mut actor) = caught_up();
        let now = T0.saturating_add(10_000);
        let at = |offset: i64| ts(now.saturating_add(offset));
        for (lane, wait) in LANES.iter().zip(waits) {
            if let Some(wait) = wait {
                actor.lanes.get_mut(*lane).fail(|_| at(wait));
            }
        }
        actor.gate = match gate {
            0 => Gate::Open,
            1 => Gate::Until(at(gate_at)),
            _ => Gate::NextWrite,
        };
        actor.maintain.until = maintain.map(at);
        if draining {
            actor.begin_shutdown(ts(now));
        }
        let soonest = held(&actor).into_iter().min();
        match actor.next(ts(now)) {
            Next::Now => {}
            Next::At(t) => {
                proptest::prop_assert!(soonest.is_some_and(|s| t <= s && s > ts(now)), "{t:?} {soonest:?}");
            }
            Next::OnWalChange => proptest::prop_assert_eq!(soonest, None),
        }
        // A tick with a passed deadline runs that lane or lets the deadline go.
        let tick = rig.tick(&mut actor, now);
        if let Tick::Ran { next, .. } = tick {
            let passed = held(&actor).into_iter().any(|t| t <= ts(now));
            proptest::prop_assert!(!passed || next == Next::Now, "{tick:?}");
        }
    }
}

#[test]
fn prune_keeps_exactly_the_uncompacted_and_the_tip() {
    let (mut rig, mut actor) = caught_up();
    assert_eq!(level_one_max(&rig), Some(1));
    for n in 2..=4 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    // Confirmed through 4, level 1 through 1, tip 4: only 1 goes.
    assert_eq!(staged(&rig), vec![2, 3, 4]);
    // A new level-1 window compacts 2..=4; the tip stays although compacted.
    let later = T0 + 60_000;
    rig.settle(&mut actor, later);
    assert_eq!(level_one_max(&rig), Some(4));
    rig.settle(&mut actor, later);
    assert_eq!(staged(&rig), vec![4]);
}

#[test]
fn the_tip_stays_even_when_compacted() {
    let (rig, _actor) = caught_up();
    assert_eq!(level_one_max(&rig), Some(1));
    assert_eq!(staged(&rig), vec![1]);
}

#[test]
fn nothing_is_pruned_before_the_first_listing() {
    let (mut rig, mut actor) = caught_up();
    for n in 2..=4 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    // The old actor compacts 2..=4 but its prune fails.
    rig.staging
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    rig.settle(&mut actor, T0 + 60_000);
    assert_eq!(level_one_max(&rig), Some(4));
    let before = staged(&rig);
    assert_eq!(before, vec![2, 3, 4]);
    // A restart: the new actor knows nothing of level 1 until it lists.
    let mut actor = Actor::new(cfg());
    let mut now = T0 + 60_002;
    loop {
        let tick = rig.tick(&mut actor, now);
        if matches!(event(&tick), Some(Event::Maintained(_))) {
            break;
        }
        assert_eq!(staged(&rig), before, "pruned before listing: {tick:?}");
        now += 1;
    }
    rig.settle(&mut actor, now);
    assert_eq!(staged(&rig), vec![4]);
}

#[test]
fn a_crash_between_prunes_boots_clean() {
    let (mut rig, mut actor) = caught_up();
    for n in 2..=6 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    let later = T0 + 60_000;
    // Compact 2..=6, then stop before the actor prunes: delete only the
    // oldest by hand, as a prune that died after its first call would.
    rig.staging
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    rig.settle(&mut actor, later);
    assert_eq!(level_one_max(&rig), Some(6));
    let oldest = Rig::files(&rig.staging, Level::ZERO)[0].key;
    block_on(rig.staging.inner.delete(&[oldest])).unwrap();
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, later + 1);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, later + 1);
    assert!(
        seen.iter().all(|t| !matches!(t, Tick::Halted(_))),
        "{seen:?}"
    );
    assert_eq!(staged(&rig), vec![6]);
}

#[test]
fn a_failed_prune_blocks_nothing_else() {
    let (mut rig, mut actor) = caught_up();
    rig.staging
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    rig.commit(2);
    let mut seen = rig.settle(&mut actor, T0 + 1);
    rig.commit(3);
    seen.extend(rig.settle(&mut actor, T0 + 2));
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Retrying { .. }))),
        "the prune failed: {seen:?}"
    );
    let remote: Vec<u64> = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .collect();
    assert_eq!(remote.last(), Some(&3), "uploads ran while the prune waits");
    rig.settle(&mut actor, T0 + 2_000);
    assert_eq!(staged(&rig), vec![2, 3]);
}

/// Stages 2..=4 and follows the actor to the day's snapshot, whose
/// retention deletes then fail with `errors`, in order. Returns when the
/// first failure was and when its retry is due.
fn retention_failure(
    rig: &mut Rig,
    actor: &mut Actor,
    errors: [StoreError; 2],
) -> (i64, Timestamp) {
    for n in 2..=4 {
        rig.commit(n);
        rig.settle(actor, T0 + 1);
    }
    for err in errors {
        rig.remote.inner.inject_failure(StoreOp::Delete, err);
    }
    let mut now = T0 + 60_000;
    loop {
        let tick = rig.tick(actor, now);
        if let Some(Event::Retrying { until, .. }) = event(&tick) {
            return (now, *until);
        }
        match next_of(&tick) {
            Some(Next::Now) => {}
            Some(Next::At(at)) => now = at.unix_millis(),
            next => panic!("no retention delete ran: {tick:?} {next:?}"),
        }
    }
}

/// Settles at `now` and returns the first retry's due instant.
fn next_retry(rig: &mut Rig, actor: &mut Actor, now: i64) -> Option<Timestamp> {
    rig.settle(actor, now)
        .iter()
        .find_map(|tick| match event(tick) {
            Some(Event::Retrying { until, .. }) => Some(*until),
            Some(_) | None => None,
        })
}

const fn throttled(op: StoreOp, millis: u64) -> StoreError {
    StoreError::Throttled {
        op,
        retry_after: Some(Duration::from_millis(millis)),
    }
}

#[test]
fn a_failing_retention_delete_waits_and_doubles() {
    let (mut rig, mut actor) = caught_up();
    let errors = [transient(StoreOp::Delete), transient(StoreOp::Delete)];
    let (failed_at, until) = retention_failure(&mut rig, &mut actor, errors);
    assert_eq!(until, ts(failed_at + 1_000));
    // Before its wait the actor neither relists nor retries.
    let io = rig.io();
    for at in [failed_at, failed_at + 999] {
        let tick = rig.tick(&mut actor, at);
        assert_eq!(event(&tick), Some(&Event::Idle), "{tick:?}");
        assert_eq!(next_of(&tick), Some(Next::At(until)));
    }
    assert_eq!(rig.io(), io, "relisted or retried before the wait");
    // At the wait it relists, fails again, and the wait doubles.
    let second = next_retry(&mut rig, &mut actor, until.unix_millis());
    assert_eq!(second, Some(ts(until.unix_millis() + 2_000)));
}

#[test]
fn a_throttled_retention_delete_waits_the_later_of_both() {
    let (mut rig, mut actor) = caught_up();
    // A hint longer than the 1 s backoff, then one shorter than the 2 s.
    let errors = [
        throttled(StoreOp::Delete, 30_000),
        throttled(StoreOp::Delete, 500),
    ];
    let (failed_at, until) = retention_failure(&mut rig, &mut actor, errors);
    assert_eq!(until, ts(failed_at + 30_000));
    let second = next_retry(&mut rig, &mut actor, until.unix_millis());
    assert_eq!(second, Some(ts(until.unix_millis() + 2_000)));
}

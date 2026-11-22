use std::future::Future;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use lsr_ltx::{PageNo, PageSize, ReadDecoder, TxidRange};
use lsr_wal::Salts;

use super::*;
use crate::conformance::block_on;
use crate::env::FakeClock;
use crate::layout::CreatedAt;
use crate::layout::{FileKey, Level};
use crate::mem::MemStore;
use crate::plan::RestoreTarget;
use crate::restore::{MemSink, restore};
use crate::source::{CheckpointCall, FakeCheckpointer, FakeDb, FakeWal, SourceError, SourceOp};
use crate::store::{Listing, PageLimit, ReadLen, StoreError, StoreOp, Upload};

mod audit;

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
    /// The listing calls among them.
    lists: Arc<AtomicU32>,
    reads: Arc<std::sync::Mutex<Vec<FileKey>>>,
    deletes: Arc<std::sync::Mutex<Vec<FileKey>>>,
    /// When non-zero, the most keys one listing page returns: a store may
    /// return fewer than the limit.
    page_cap: Arc<AtomicU32>,
    /// A WAL whose read-lock hold the next upload begun moves, as a checkpoint
    /// by another connection would mid-step.
    moves_epoch: Arc<std::sync::Mutex<Option<FakeWal>>>,
    /// A WAL the application commits a transaction to (page 1 filled with the byte)
    /// as the next upload begins: a commit that lands after a sync step read
    /// the committed end.
    commits_on_upload: Arc<std::sync::Mutex<Option<(FakeWal, u8)>>>,
    /// Keys whose next read returns its bytes with the middle one flipped,
    /// the stored bytes untouched: one bad read.
    flip_next_read: Arc<std::sync::Mutex<Vec<FileKey>>>,
}

impl Counting {
    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    fn count(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }

    fn lists(&self) -> u32 {
        self.lists.load(Ordering::SeqCst)
    }

    /// How often `key`'s bytes were read from their start.
    fn reads_of(&self, key: &FileKey) -> usize {
        let reads = self.reads.lock().unwrap();
        reads.iter().filter(|read| *read == key).count()
    }

    /// The next read of `key` returns one byte flipped.
    fn flip_next_read(&self, key: FileKey) {
        self.flip_next_read.lock().unwrap().push(key);
    }

    /// Every key a delete was asked for, in order.
    fn deleted(&self) -> Vec<FileKey> {
        self.deletes.lock().unwrap().clone()
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
        self.lists.fetch_add(1, Ordering::SeqCst);
        let cap = self.page_cap.load(Ordering::SeqCst);
        let limit = u16::try_from(cap)
            .ok()
            .and_then(PageLimit::new)
            .filter(|cap| cap.get() < limit.get())
            .unwrap_or(limit);
        self.inner.list(level, after, limit)
    }

    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        self.count();
        if offset == 0 {
            self.reads.lock().unwrap().push(*key);
        }
        let flip = {
            let mut pending = self.flip_next_read.lock().unwrap();
            let at = pending.iter().position(|k| k == key);
            at.map(|at| pending.remove(at)).is_some()
        };
        let read = self.inner.read(key, offset, len);
        async move {
            let mut bytes = read.await?;
            let middle = bytes.len() / 2;
            if flip && let Some(byte) = bytes.get_mut(middle) {
                *byte ^= 0x01;
            }
            Ok(bytes)
        }
    }

    fn created_at(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<CreatedAt, StoreError>> + Send {
        self.count();
        self.inner.created_at(file)
    }

    fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, CreatedAt), StoreError>> + Send {
        self.count();
        self.reads.lock().unwrap().push(file.key);
        self.inner.read_with_created_at(file, len)
    }

    fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> impl Future<Output = Result<Self::Upload, StoreError>> + Send {
        self.count();
        if let Some(wal) = self.moves_epoch.lock().unwrap().take() {
            wal.set_epoch(wal.epoch().next());
        }
        if let Some((wal, n)) = self.commits_on_upload.lock().unwrap().take() {
            wal.append_tx(&[(pg(1), vec![n; PS.bytes()])], nz(3));
        }
        self.inner.begin_upload(key, created_at)
    }

    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.count();
        self.deletes.lock().unwrap().extend_from_slice(keys);
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
            clock: &FakeClock::new(ts(now)),
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

    /// Ticks, following the actor's own wake times, until it stops, drains,
    /// waits on the WAL, or `limit` ticks have run.
    fn follow(&mut self, actor: &mut Actor, mut now: i64, limit: usize) -> Vec<Tick> {
        let mut seen = Vec::new();
        for _ in 0..limit {
            let tick = self.tick(actor, now);
            let next = match &tick {
                Tick::Ran { next, .. } => *next,
                Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_) => {
                    Next::OnWalChange
                }
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
        // The tests written before the configurable responses expect a stop.
        responses: Responses::STRICT,
    }
}

/// [`cfg`] with the default responses: keep trying, stop on another writer.
fn go_like() -> ActorConfig {
    ActorConfig {
        responses: Responses::GO_LIKE,
        ..cfg()
    }
}

fn event(tick: &Tick) -> Option<&Event> {
    match tick {
        Tick::Ran { event, .. } => Some(event),
        Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_) => None,
    }
}

fn next_of(tick: &Tick) -> Option<Next> {
    match tick {
        Tick::Ran { next, .. } => Some(*next),
        Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_) => None,
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

/// An empty replica names no page size, so boot asks it nothing more: one
/// listing per level and no second pass.
#[test]
fn a_first_boot_lists_each_level_once() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.tick(&mut actor, T0);
    assert_eq!(rig.remote.calls(), u32::try_from(Level::ALL.len()).unwrap());
}

/// An operator's request makes the next sync a snapshot although the WAL
/// has not moved.
#[test]
fn a_requested_snapshot_is_staged_on_a_quiet_wal() {
    let (mut rig, mut actor) = caught_up();
    let before = staged(&rig);
    assert_eq!(event(&rig.tick(&mut actor, T0 + 1)), Some(&Event::Idle));
    actor.request_snapshot();
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 1)),
        Some(&Event::Synced { files: 1 })
    );
    let after = staged(&rig);
    assert_eq!(after.len(), before.len() + 1, "{after:?}");
    // The request is spent: the next tick uploads, and nothing snapshots again.
    rig.settle(&mut actor, T0 + 1);
    assert_eq!(staged(&rig).last(), after.last());
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
        .position(|t| matches!(event(t), Some(Event::Maintained { .. })))
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
fn retry_backs_off_per_task() {
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
    // The prune of staged file 1 is not the upload task's to hold.
    let pruned = rig.settle(&mut actor, T0 + 10);
    assert_eq!(
        pruned.last().and_then(next_of),
        Some(Next::At(ts(T0 + 1_010)))
    );
    // The sync task does not wait on the upload task.
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
fn stop_is_terminal() {
    let (mut rig, mut actor) = caught_up();
    rig.remote.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Denied {
            op: StoreOp::Upload,
            why: crate::store::Denied::Credentials,
        },
    );
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    assert_eq!(
        rig.tick(&mut actor, T0 + 10),
        Tick::Stopped(StopCause::StoreDenied(crate::store::Denied::Credentials))
    );
    let io = rig.io();
    rig.commit(3);
    for at in [T0 + 10, T0 + 100_000] {
        assert_eq!(
            rig.tick(&mut actor, at),
            Tick::Stopped(StopCause::StoreDenied(crate::store::Denied::Credentials))
        );
    }
    assert_eq!(rig.io(), io, "a stopped actor does no I/O");
    assert_eq!(
        actor.phase(),
        Phase::Stopped(StopCause::StoreDenied(crate::store::Denied::Credentials))
    );
}

fn big() -> PageSize {
    PageSize::new(1024).unwrap()
}

/// The application vacuums to `size` while the engine is down: a new WAL and
/// database file at that size, with one commit.
fn resize(rig: &mut Rig, size: PageSize) {
    let page = |n| PageNo::new(n, size).unwrap();
    rig.wal = FakeWal::new(
        size,
        Salts {
            salt1: 30,
            salt2: 40,
        },
    );
    rig.db = FakeDb::new(size);
    rig.db.set_page_count(3);
    for n in 1..=3 {
        rig.db.write_page(page(n), &vec![9; size.bytes()]);
    }
    rig.ck = FakeCheckpointer::new(rig.wal.clone());
    rig.wal
        .append_tx(&[(page(1), vec![9; size.bytes()])], nz(3));
}

fn page_size_changed() -> Tick {
    Tick::Stopped(StopCause::PageSizeChanged {
        was: PS,
        now: big(),
    })
}

/// Every key at every level of both stores.
fn everything(rig: &Rig) -> (Vec<FileInfo>, Vec<FileInfo>) {
    let all = |store: &Counting| {
        block_on(Inventory::load(&store.inner))
            .unwrap()
            .files()
            .to_vec()
    };
    (all(&rig.staging), all(&rig.remote))
}

/// Before the boot only the boot runs, and it waits on the sync task: a boot
/// that failed asks to run at the end of its wait, not at once.
#[test]
fn a_failed_boot_waits_without_spinning() {
    let (mut rig, _) = caught_up();
    rig.staging
        .inner
        .inject_failure(StoreOp::List, transient(StoreOp::List));
    let mut actor = Actor::new(cfg());
    let tick = rig.tick(&mut actor, T0 + 10);
    let Some(Event::Retrying { until, .. }) = event(&tick) else {
        panic!("{tick:?}");
    };
    assert_eq!(next_of(&tick), Some(Next::At(*until)), "{tick:?}");
}

#[test]
fn a_page_size_change_stops_at_boot_before_anything_is_staged() {
    let (mut rig, _) = caught_up();
    let before = everything(&rig);
    resize(&mut rig, big());
    let mut actor = Actor::new(cfg());
    assert_eq!(rig.tick(&mut actor, T0 + 10), page_size_changed());
    assert_eq!(everything(&rig), before);
}

#[test]
fn with_staging_gone_the_replica_names_the_page_size() {
    let (mut rig, _) = caught_up();
    rig.staging = Counting::default();
    let before = everything(&rig);
    resize(&mut rig, big());
    let mut actor = Actor::new(cfg());
    assert_eq!(rig.tick(&mut actor, T0 + 10), page_size_changed());
    assert_eq!(everything(&rig), before);
}

/// With staging gone and the replica's newest transaction held only above level 0, the boot
/// reads the page size from the replica's newest file. When that header did not decode (a
/// rotten object, or one read a proxy changed), the boot took no page size at all and
/// snapshotted the new size onto the prefix, where every restore past it then stops. Found by
/// a simulator hold whose reads lie (seed 0x13): any other file's header names the prefix's
/// page size, and with none readable the boot must not guess.
#[test]
fn a_newest_replica_header_that_does_not_decode_still_names_the_page_size() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    rig.follow(&mut actor, T0 + 10, 50);
    // Every file ending at the newest transaction is junk, and its level-0 file is gone.
    let (_, files) = everything(&rig);
    let newest = files.iter().map(|f| f.key.range().max()).max().unwrap();
    block_on(async {
        for file in files.iter().filter(|f| f.key.range().max() == newest) {
            rig.remote.inner.delete(&[file.key]).await.unwrap();
            if file.key.level() == Level::ZERO {
                continue;
            }
            let mut up = rig
                .remote
                .inner
                .begin_upload(&file.key, ts(T0))
                .await
                .unwrap();
            up.write(&[0x5a; 200]).await.unwrap();
            up.commit().await.unwrap();
        }
    });
    rig.staging = Counting::default();
    let before = everything(&rig);
    resize(&mut rig, big());
    let mut actor = Actor::new(cfg());
    let ticks = rig.follow(&mut actor, T0 + 20, 50);
    assert!(
        ticks
            .iter()
            .all(|tick| !matches!(event(tick), Some(Event::Uploaded(_)))),
        "{ticks:?}"
    );
    assert_eq!(ticks.last(), Some(&page_size_changed()), "{ticks:?}");
    assert_eq!(everything(&rig).1, before.1, "nothing reaches the replica");
}

/// Staging holds an older file at the new page size (a staging backup put
/// back) while the replica's newest transaction is later and at the old one.
/// The boot judged the page size by the staged file alone and snapshotted the
/// new size onto the replica's prefix: restores then stopped on the change
/// (seed 0xdc8).
#[test]
fn a_replica_ahead_of_staging_names_the_page_size() {
    let mut other = Rig::new();
    resize(&mut other, big());
    other.settle(&mut Actor::new(cfg()), T0);
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    rig.commit(3);
    rig.follow(&mut actor, T0 + 10, 50);
    rig.staging = other.staging;
    let before = everything(&rig);
    resize(&mut rig, big());
    let mut actor = Actor::new(cfg());
    let ticks = rig.follow(&mut actor, T0 + 20, 50);
    assert_eq!(ticks.last(), Some(&page_size_changed()), "{ticks:?}");
    assert_eq!(everything(&rig).1, before.1, "nothing reaches the replica");
}

/// An upload's commit is ambiguous, and before the listing that follows,
/// another writer's verifying object replaces transaction 2 on the replica.
/// The listing finds it unlike the staged file: the staged files from 2 on
/// must go and the next file is a snapshot above 2. When that delete failed,
/// the uploader had already taken 2 as confirmed and uploaded the staged 3,
/// built on the staged 2, onto the other object (seed 0xd79).
#[test]
fn a_failed_drop_after_a_divergence_uploads_nothing_built_on_it() {
    let (mut other, mut writer) = caught_up();
    other.commit(9);
    other.follow(&mut writer, T0 + 10, 20);
    let two = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(2).unwrap()));
    let theirs = block_on(other.remote.inner.read(&two, 0, ReadLen::CHUNK)).unwrap();
    let (mut rig, mut actor) = caught_up();
    rig.remote
        .inner
        .inject_failure_after_publish(transient(StoreOp::Upload));
    rig.commit(2);
    let mut now = T0 + 10;
    for _ in 0..20 {
        let tick = rig.tick(&mut actor, now);
        if let Some(Event::Retrying { until, .. }) = event(&tick) {
            now = until.unix_millis();
            break;
        }
    }
    block_on(async {
        let mut up = rig.remote.inner.begin_upload(&two, ts(T0)).await.unwrap();
        up.write(&theirs).await.unwrap();
        up.commit().await.unwrap();
    });
    rig.staging
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    rig.commit(3);
    let ticks = rig.follow(&mut actor, now, 60);
    let put_three = ticks.iter().position(|tick| {
        matches!(event(tick), Some(Event::Uploaded(Did::Put(info))) if info.key.range().max() == Txid::new(3).unwrap())
    });
    let resnapshot = ticks
        .iter()
        .position(|tick| matches!(event(tick), Some(Event::Resnapshot(_))));
    let (Some(put_three), Some(resnapshot)) = (put_three, resnapshot) else {
        panic!("{ticks:?}");
    };
    assert!(resnapshot < put_three, "{ticks:?}");
}

/// Expected to fail until the engine keeps a durable record (BOOT-21, BOOT-22; D9 in
/// `docs/BEHAVIOR.md`). A running engine whose ambiguous commit makes it list the replica finds
/// another writer's file above its own newest staged file and stops on `OtherWriter`, the one
/// stop the default responses make. A restart forgets the stop: the new boot takes the other
/// writer's file as history it is behind (`behind_remote`), snapshots after it and uploads, so
/// the replica's one history interleaves two databases. The restart must stop again, or refuse.
#[test]
#[ignore = "known wrong, BOOT-21 and BOOT-22: an OtherWriter stop is forgotten by a restart; needs a durable engine-owned record"]
fn a_restart_after_an_other_writer_stop_stops_again() {
    let (mut other, mut writer) = caught_up();
    other.commit(9);
    other.follow(&mut writer, T0 + 10, 20);
    other.commit(10);
    other.follow(&mut writer, T0 + 20, 20);
    let three = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(3).unwrap()));
    let theirs = block_on(other.remote.inner.read(&three, 0, ReadLen::CHUNK)).unwrap();
    let (mut rig, mut actor) = caught_up();
    block_on(async {
        let mut up = rig.remote.inner.begin_upload(&three, ts(T0)).await.unwrap();
        up.write(&theirs).await.unwrap();
        up.commit().await.unwrap();
    });
    rig.remote
        .inner
        .inject_failure_after_publish(transient(StoreOp::Upload));
    rig.commit(2);
    let ticks = rig.follow(&mut actor, T0 + 30, 60);
    assert_eq!(
        ticks.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "the running engine stops first: {ticks:?}"
    );
    let before = everything(&rig).1;
    let mut restarted = Actor::new(cfg());
    let ticks = rig.follow(&mut restarted, T0 + 40, 60);
    assert_eq!(
        ticks.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{ticks:?}"
    );
    assert_eq!(
        everything(&rig).1,
        before,
        "nothing is put over the other writer"
    );
}

#[test]
fn an_empty_replica_takes_any_page_size() {
    let mut rig = Rig::new();
    resize(&mut rig, big());
    let mut actor = Actor::new(cfg());
    let ticks = rig.settle(&mut actor, T0);
    assert!(
        ticks.iter().all(|tick| !matches!(tick, Tick::Stopped(_))),
        "{ticks:?}"
    );
    let (_, remote) = everything(&rig);
    assert!(!remote.is_empty());
}

#[test]
fn a_wal_that_returns_at_another_page_size_stops_before_staging() {
    let (mut rig, _) = caught_up();
    rig.wal.remove_file();
    let mut actor = Actor::new(cfg());
    rig.follow(&mut actor, T0 + 10, 50);
    let before = everything(&rig);
    resize(&mut rig, big());
    let ticks = rig.follow(&mut actor, T0 + 20, 50);
    assert_eq!(ticks.last(), Some(&page_size_changed()), "{ticks:?}");
    assert_eq!(everything(&rig), before);
}

/// Each step back of the clock is reported once, by the reading that made it;
/// readings that only fail to move are not steps.
#[test]
fn clock_back_clamped_once() {
    let (mut rig, mut actor) = caught_up();
    let clamped = Event::Clamped(Clamped {
        prev: ts(T0),
        now: ts(T0 - 500),
    });
    assert_eq!(event(&rig.tick(&mut actor, T0 - 500)), Some(&clamped));
    let tick = rig.tick(&mut actor, T0 - 500);
    assert!(!matches!(event(&tick), Some(Event::Clamped(_))), "{tick:?}");
    let again = Event::Clamped(Clamped {
        prev: ts(T0),
        now: ts(T0 - 600),
    });
    assert_eq!(event(&rig.tick(&mut actor, T0 - 600)), Some(&again));
    rig.tick(&mut actor, T0 + 10);
    let again = rig.tick(&mut actor, T0 + 5);
    assert!(
        matches!(event(&again), Some(Event::Clamped(_))),
        "a new step back is reported"
    );
}

/// A clock stepped back by an hour does not freeze a one-second retry for
/// that hour: engine time keeps moving with the wall clock (at half speed
/// until the wall clock catches up), the wake it asks for is seconds of wall
/// clock away, and the upload goes through.
#[test]
fn a_backward_clock_step_does_not_freeze_a_retry_wait() {
    let (mut rig, mut actor) = caught_up();
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.commit(2);
    let at = T0 + 1_000;
    assert_eq!(
        event(&rig.tick(&mut actor, at)),
        Some(&Event::Synced { files: 1 })
    );
    let failed = rig.tick(&mut actor, at);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { until, .. }) if *until == ts(at + 1_000)),
        "{failed:?}"
    );
    let back = at - 3_600_000;
    assert!(matches!(
        event(&rig.tick(&mut actor, back)),
        Some(Event::Clamped(_))
    ));
    let waiting = rig.settle(&mut actor, back);
    assert_eq!(
        waiting.last().and_then(next_of),
        Some(Next::At(ts(back + 2_000))),
        "{waiting:?}"
    );
    let seen = rig.settle(&mut actor, back + 2_000);
    assert!(
        seen.iter()
            .any(|tick| matches!(event(tick), Some(Event::Uploaded(Did::Put(_))))),
        "{seen:?}"
    );
    assert_eq!(restored(&rig), 2);
}

/// After a step back the engine's time slews back onto the wall clock:
/// once the wall clock has run twice the step, the two agree again, and a
/// retry wait takes its own length of wall clock.
#[test]
fn engine_time_rejoins_the_wall_clock_after_a_step_back() {
    let (mut rig, mut actor) = caught_up();
    let back = T0 - 10_000;
    rig.tick(&mut actor, back);
    rig.settle(&mut actor, back + 20_000);
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.commit(2);
    let at = back + 20_000;
    rig.tick(&mut actor, at);
    let failed = rig.tick(&mut actor, at);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { until, .. }) if *until == ts(at + 1_000)),
        "{failed:?}"
    );
}

#[test]
fn drain_finishes_uploads() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = rig.follow(&mut actor, T0 + 10, 20);
    assert!(
        matches!(seen.last(), Some(Tick::Drained { txid: Some(_) })),
        "{seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|t| matches!(event(t), Some(Event::Maintained { .. })))
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
fn drain_deadline_stops() {
    let (mut rig, mut actor) = caught_up();
    for _ in 0..20 {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    }
    rig.commit(2);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    // Transaction 2 is synced and staged, not uploaded: no WAL bytes left,
    // one staged file.
    assert_eq!(
        seen.last(),
        Some(&Tick::DeadlineReached(Unfinished {
            unsynced_wal_bytes: 0,
            staged_not_uploaded: 1,
        })),
        "{seen:?}"
    );
    assert_eq!(
        rig.tick(&mut actor, T0 + 60_000),
        Tick::Stopped(StopCause::Shutdown)
    );
}

/// A zero deadline with a commit not yet synced ends at once, counting the
/// commit's WAL bytes, and retries nothing (Go's `TestDB_Close_SyncRetry`,
/// zero timeout).
#[test]
fn a_zero_deadline_reports_unsynced_bytes_and_does_no_work() {
    let (mut rig, mut actor) = caught_up();
    let before = rig.wal.committed_end().offset().get();
    rig.commit(2);
    let after = rig.wal.committed_end().offset().get();
    let calls = rig.remote.calls();
    actor.begin_shutdown(ts(T0 + 10));
    let tick = rig.tick(&mut actor, T0 + 10);
    assert_eq!(
        tick,
        Tick::DeadlineReached(Unfinished {
            unsynced_wal_bytes: after - before,
            staged_not_uploaded: 0,
        })
    );
    assert_eq!(rig.remote.calls(), calls);
}

/// A second shutdown can shorten a drain and never lengthen it.
#[test]
fn a_second_shutdown_keeps_the_earlier_deadline() {
    let (_rig, mut actor) = caught_up();
    actor.begin_shutdown(ts(T0 + 1_000));
    actor.begin_shutdown(ts(T0 + 5_000));
    assert_eq!(
        actor.phase(),
        Phase::Draining {
            deadline: ts(T0 + 1_000)
        }
    );
    actor.begin_shutdown(ts(T0 + 10));
    assert_eq!(
        actor.phase(),
        Phase::Draining {
            deadline: ts(T0 + 10)
        }
    );
}

/// A backfill the actor did not run is counted; its own checkpoints are not.
#[test]
fn an_app_checkpoint_is_counted_and_its_own_is_not() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    rig.settle(&mut actor, T0 + 10);
    assert_eq!(actor.app_checkpoints_ran(), 0);
    let frames = rig.wal.committed_end().frame_count();
    assert!(rig.wal.backfill(frames) > 0);
    rig.commit(3);
    rig.settle(&mut actor, T0 + 20);
    assert_eq!(actor.app_checkpoints_ran(), 1);
    // A restart nobody here asked for is a second one.
    rig.wal.force_restart();
    rig.commit(4);
    rig.settle(&mut actor, T0 + 30);
    assert_eq!(actor.app_checkpoints_ran(), 2);
}

#[test]
fn the_actor_s_own_checkpoint_is_not_an_app_checkpoint() {
    let mut rig = Rig::new();
    let cfg = ActorConfig {
        checkpoint: CheckpointConfig {
            interval: None,
            min_pages: NonZeroU32::new(1),
            truncate_pages: None,
        },
        ..cfg()
    };
    let mut actor = Actor::new(cfg);
    let mut seen = rig.settle(&mut actor, T0);
    for n in 2..6 {
        rig.commit(n);
        seen.extend(rig.settle(&mut actor, T0 + i64::from(n) * 1_000));
    }
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Checkpointed { .. }))),
        "{seen:?}"
    );
    assert_eq!(actor.app_checkpoints_ran(), 0, "{seen:?}");
}

/// A drain whose upload fails a few times retries inside the deadline and
/// ends drained, with every committed transaction on the replica.
#[test]
fn a_drain_retries_a_transient_upload_and_drains() {
    let (mut rig, mut actor) = caught_up();
    for _ in 0..3 {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    }
    rig.commit(2);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert_eq!(
        seen.last(),
        Some(&Tick::Drained { txid: Txid::new(2) }),
        "{seen:?}"
    );
    let retries = seen
        .iter()
        .filter(|t| matches!(event(t), Some(Event::Retrying { .. })))
        .count();
    assert_eq!(retries, 3, "{seen:?}");
    let newest = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .max();
    assert_eq!(newest, Some(2));
}

/// A drain against a store that throttles with a named wait waits it out
/// inside the deadline and ends drained.
#[test]
fn a_drain_waits_out_a_throttled_upload_and_drains() {
    let (mut rig, mut actor) = caught_up();
    for _ in 0..2 {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, throttled(StoreOp::Upload, 2_000));
    }
    rig.commit(2);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert_eq!(
        seen.last(),
        Some(&Tick::Drained { txid: Txid::new(2) }),
        "{seen:?}"
    );
    let waits: Vec<Timestamp> = seen
        .iter()
        .filter_map(|t| match event(t) {
            Some(Event::Retrying { until, .. }) => Some(*until),
            Some(_) | None => None,
        })
        .collect();
    assert_eq!(waits, vec![ts(T0 + 2_010), ts(T0 + 4_010)], "{seen:?}");
    let newest = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .max();
    assert_eq!(newest, Some(2));
}

/// The fault events a run produced: each is one call of `policy`.
fn routed(seen: &[Tick]) -> usize {
    seen.iter()
        .filter(|t| {
            matches!(t, Tick::Stopped(_))
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
        StoreError::Denied {
            op,
            why: crate::store::Denied::Credentials,
        },
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

/// When maintenance next lists after an upload at `now` under [`cfg`].
fn listing_due(now: i64) -> i64 {
    listing_after_upload(ts(now), &cfg().maintenance).unix_millis()
}

/// Commits `n` and runs the actor until it has staged and uploaded it.
fn commit_and_settle(rig: &mut Rig, actor: &mut Actor, n: u8, now: i64) {
    rig.commit(n);
    rig.settle(actor, now);
}

#[test]
fn job_retry_reports_the_levels_wait() {
    let (mut rig, mut actor) = caught_up();
    let uploaded = T0 + 60_000;
    rig.commit(2);
    assert_eq!(
        event(&rig.tick(&mut actor, uploaded)),
        Some(&Event::Synced { files: 1 })
    );
    assert!(matches!(
        event(&rig.tick(&mut actor, uploaded)),
        Some(Event::Uploaded(_))
    ));
    let at = listing_due(uploaded);
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

const TASKS: [Task; 5] = [
    Task::Sync,
    Task::Checkpoint,
    Task::Upload,
    Task::Prune,
    Task::Maintenance,
];

/// Every deadline the actor holds, read from where each is kept.
fn held(actor: &Actor) -> Vec<Timestamp> {
    let running = actor.phase() == Phase::Running;
    let mut held: Vec<Timestamp> = TASKS
        .iter()
        .filter(|task| running || matches!(task, Task::Sync | Task::Upload))
        .filter_map(|task| actor.kept.tasks.get(*task).until())
        .collect();
    if running {
        if let Gate::Until(at) = actor.engine.gate {
            held.push(at);
        }
        held.extend(actor.engine.maintain.until);
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
        for (task, wait) in TASKS.iter().zip(waits) {
            if let Some(wait) = wait {
                actor.kept.tasks.get_mut(*task).fail(|_| at(wait));
            }
        }
        actor.engine.gate = match gate {
            0 => Gate::Open,
            1 => Gate::Until(at(gate_at)),
            _ => Gate::NextWrite,
        };
        actor.engine.maintain.until = maintain.map(at);
        if draining {
            actor.begin_shutdown(actor.shutdown_deadline(ts(now)));
        }
        let soonest = held(&actor).into_iter().min();
        match actor.next(ts(now)) {
            Next::Now => {}
            Next::At(t) => {
                proptest::prop_assert!(soonest.is_some_and(|s| t <= s && s > ts(now)), "{t:?} {soonest:?}");
            }
            Next::OnWalChange => proptest::prop_assert_eq!(soonest, None),
        }
        // A tick with a passed deadline runs that task or lets the deadline go.
        let tick = rig.tick(&mut actor, now);
        if let Tick::Ran { next, .. } = tick {
            let passed = held(&actor).into_iter().any(|t| t <= ts(now));
            proptest::prop_assert!(!passed || next == Next::Now, "{tick:?}");
        }
    }
}

#[test]
fn prune_keeps_exactly_the_uncompacted_and_the_newest_staged_file() {
    let (mut rig, mut actor) = caught_up();
    assert_eq!(level_one_max(&rig), Some(1));
    for n in 2..=4 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    // Confirmed through 4, level 1 through 1, newest staged file 4: only 1 goes.
    assert_eq!(staged(&rig), vec![2, 3, 4]);
    // A new level-1 window compacts 2..=4; the newest staged file stays although compacted.
    let later = T0 + 60_000;
    rig.settle(&mut actor, later);
    assert_eq!(level_one_max(&rig), Some(4));
    rig.settle(&mut actor, later);
    assert_eq!(staged(&rig), vec![4]);
}

#[test]
fn the_newest_staged_file_stays_even_when_compacted() {
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
        if matches!(event(&tick), Some(Event::Maintained { .. })) {
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
        seen.iter().all(|t| !matches!(t, Tick::Stopped(_))),
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

impl Rig {
    /// A rig whose replica lists last-modified times, so every created-at time a
    /// decision needs is a request.
    fn lazy() -> Self {
        let mut rig = Self::new();
        rig.remote = Counting {
            inner: MemStore::with_listed_last_modified(),
            ..Counting::default()
        };
        rig
    }
}

/// Commits `n`, syncs and uploads it, and stops before the tick that would
/// list the replica for maintenance.
fn stage_and_upload(rig: &mut Rig, actor: &mut Actor, n: u8, now: i64) {
    rig.commit(n);
    assert_eq!(
        event(&rig.tick(actor, now)),
        Some(&Event::Synced { files: 1 })
    );
    assert!(matches!(
        event(&rig.tick(actor, now)),
        Some(Event::Uploaded(_))
    ));
    // The staged copy is pruned before maintenance lists.
    assert!(matches!(
        event(&rig.tick(actor, now)),
        Some(Event::Maintained {
            event: MaintainedEvent::Deleted(1),
            ..
        })
    ));
}

#[test]
fn created_at_failure_is_a_maintain_fault() {
    let mut rig = Rig::lazy();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    stage_and_upload(&mut rig, &mut actor, 2, T0 + 10);
    let before = Rig::files(&rig.remote, Level::ZERO);
    // A throttle hint longer than the 1 s backoff, then one shorter than the 2 s.
    for millis in [30_000, 500] {
        rig.remote
            .inner
            .inject_failure(StoreOp::Read, throttled(StoreOp::Read, millis));
    }
    let at = listing_due(T0 + 10);
    let tick = rig.tick(&mut actor, at);
    let until = ts(at + 30_000);
    assert!(
        matches!(event(&tick), Some(Event::Retrying { until: u, .. }) if *u == until),
        "{tick:?}"
    );
    assert_eq!(Rig::files(&rig.remote, Level::ZERO), before, "a job ran");
    // Before its wait the task neither relists nor reads a created-at time.
    let io = rig.io();
    let idle = rig.tick(&mut actor, at + 29_000);
    assert_eq!(event(&idle), Some(&Event::Idle), "{idle:?}");
    assert_eq!(rig.io(), io);
    // At the wait it lists again and the next failure doubles the wait.
    let second = rig.tick(&mut actor, until.unix_millis());
    assert!(
        matches!(event(&second), Some(Event::Retrying { until: u, .. }) if *u == ts(until.unix_millis() + 2_000)),
        "{second:?}"
    );
    // Faults spent, the same tick succeeds: the listing was still owed.
    let again = rig.tick(&mut actor, until.unix_millis() + 2_000);
    assert!(
        matches!(event(&again), Some(Event::Maintained { created_at_read, .. }) if *created_at_read > 0),
        "{again:?}"
    );
}

#[test]
fn the_maintenance_event_counts_the_created_at_it_read() {
    let run = |mut rig: Rig| {
        let mut actor = Actor::new(cfg());
        let mut ticks = rig.settle(&mut actor, T0);
        for n in 2..=4 {
            rig.commit(n);
            ticks.extend(rig.settle(&mut actor, T0 + i64::from(n) * 10));
        }
        let waits: Vec<u64> = ticks
            .iter()
            .filter_map(|tick| match event(tick) {
                Some(Event::Maintained {
                    created_at_read, ..
                }) => Some(*created_at_read),
                Some(_) | None => None,
            })
            .collect();
        (waits, rig.remote.inner.created_at_calls())
    };
    let (stored, stored_calls) = run(Rig::new());
    assert!(stored.iter().all(|read| *read == 0), "{stored:?}");
    assert_eq!(stored_calls, 0);
    let (lazy, lazy_calls) = run(Rig::lazy());
    assert!(lazy.iter().any(|read| *read > 0), "{lazy:?}");
    assert!(lazy_calls > 0);
}

/// The level-0 files of `store` as (first, last) transactions.
fn ranges(store: &Counting) -> Vec<(u64, u64)> {
    Rig::files(store, Level::ZERO)
        .iter()
        .map(|f| (f.key.range().min().get(), f.key.range().max().get()))
        .collect()
}

/// Rewrites staged `n` through `torn`, as a power loss leaves a file whose
/// commit never returned.
fn tear(rig: &Rig, n: u64, torn: fn(Vec<u8>) -> Vec<u8>) {
    let store = &rig.staging.inner;
    let info = Rig::files(&rig.staging, Level::ZERO)
        .into_iter()
        .find(|f| f.key.range().max().get() == n)
        .unwrap();
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let bytes = block_on(store.read(&info.key, 0, all)).unwrap();
    block_on(store.delete(&[info.key])).unwrap();
    let bad = torn(bytes);
    block_on(async {
        let mut up = store.begin_upload(&info.key, ts(T0)).await.unwrap();
        up.write(&bad).await.unwrap();
        up.commit().await.unwrap();
    });
}

fn truncated(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.truncate(bytes.len() / 2);
    bytes
}

fn flipped(mut bytes: Vec<u8>) -> Vec<u8> {
    let at = bytes.len() / 2;
    bytes[at] ^= 0xFF;
    bytes
}

/// Page 1 of the latest state the replica restores to.
fn restored(rig: &Rig) -> u8 {
    let sink = MemSink::new();
    block_on(restore(
        &rig.remote.inner,
        RestoreTarget::Latest,
        sink.clone(),
    ))
    .unwrap();
    sink.published().unwrap().page(1).unwrap()[0]
}

/// The last transaction of each snapshot at the replica's level 0: a file
/// holding every page of the database.
fn snapshots(rig: &Rig) -> Vec<u64> {
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .filter(|f| {
            let bytes = block_on(rig.remote.inner.read(&f.key, 0, all)).unwrap();
            let mut decoder = ReadDecoder::new(&bytes[..]);
            while decoder.next_event().unwrap().is_some() {}
            let done = decoder.finish().unwrap();
            done.page_count() == u64::from(done.header().commit())
        })
        .map(|f| f.key.range().max().get())
        .collect()
}

fn no_stop(seen: &[Tick]) {
    assert!(
        seen.iter().all(|t| !matches!(t, Tick::Stopped(_))),
        "{seen:?}"
    );
}

/// Stages one transaction per commit in `commits` while the replica's next
/// upload fails, so none of them reaches it before the retry.
fn stage_held(rig: &mut Rig, actor: &mut Actor, commits: &[u8], now: i64) {
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    for &n in commits {
        rig.commit(n);
        rig.settle(actor, now);
    }
}

/// A torn staged file the replica lacks goes with every staged file after
/// it. The WAL still follows the newest file left, so it holds the frames of
/// the dropped ones: they are staged again from it, with no snapshot.
#[test]
fn a_torn_staged_file_the_replica_lacks_is_dropped_and_staged_again_from_the_wal() {
    for torn in [truncated, flipped] {
        let (mut rig, mut actor) = caught_up();
        stage_held(&mut rig, &mut actor, &[2, 3, 4], T0 + 1);
        assert_eq!(
            (staged(&rig), ranges(&rig.remote)),
            (vec![2, 3, 4], vec![(1, 1)])
        );
        tear(&rig, 3, torn);
        let mut actor = Actor::new(cfg());
        let again = first_bad_read(&mut rig, &mut actor, T0 + 2);
        assert_eq!(staged(&rig), vec![2, 3, 4]);
        let boot = rig.tick(&mut actor, again);
        assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
        assert_eq!(staged(&rig), vec![2]);
        let seen = rig.settle(&mut actor, again);
        no_stop(&seen);
        assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
        assert_eq!(snapshots(&rig), vec![1]);
        assert_eq!(restored(&rig), 4);
    }
}

/// The same drop when the WAL has since restarted: nothing live holds the
/// dropped frames, so the next file is a snapshot, named for the lost file.
#[test]
fn a_torn_staged_file_whose_frames_the_wal_lost_costs_a_snapshot() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3, 4], T0 + 1);
    tear(&rig, 3, truncated);
    rig.wal.force_restart();
    rig.commit(5);
    let mut actor = Actor::new(cfg());
    let again = first_bad_read(&mut rig, &mut actor, T0 + 2);
    let boot = rig.tick(&mut actor, again);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::NewestStagedFileUnreadable)
        })
    );
    let seen = rig.settle(&mut actor, again);
    no_stop(&seen);
    assert_eq!(restored(&rig), 5);
}

#[test]
fn a_torn_staged_file_the_replica_holds_is_dropped_and_the_snapshot_takes_the_next() {
    for torn in [truncated, flipped] {
        let (mut rig, mut actor) = caught_up();
        commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
        commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
        stage_held(&mut rig, &mut actor, &[4], T0 + 1);
        assert_eq!(
            (staged(&rig), ranges(&rig.remote)),
            (vec![2, 3, 4], vec![(1, 1), (2, 2), (3, 3)])
        );
        tear(&rig, 3, torn);
        let mut actor = Actor::new(cfg());
        let again = first_bad_read(&mut rig, &mut actor, T0 + 2);
        assert_eq!(staged(&rig), vec![2, 3, 4]);
        let boot = rig.tick(&mut actor, again);
        assert_eq!(
            event(&boot),
            Some(&Event::Booted {
                cause: Some(SnapshotCause::NewestStagedFileUnreadable)
            })
        );
        assert_eq!(staged(&rig), vec![2]);
        let seen = rig.settle(&mut actor, again);
        no_stop(&seen);
        assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3), (4, 4)]);
        assert_eq!(snapshots(&rig), vec![1, 4]);
        assert_eq!(restored(&rig), 4);
    }
}

#[test]
fn the_upload_task_drops_a_staged_file_that_turned_unreadable_and_snapshots() {
    for torn in [truncated, flipped] {
        let (mut rig, mut actor) = caught_up();
        rig.commit(2);
        assert!(matches!(
            event(&rig.tick(&mut actor, T0 + 1)),
            Some(Event::Synced { .. })
        ));
        tear(&rig, 2, torn);
        let again = first_bad_read(&mut rig, &mut actor, T0 + 1);
        let lost = rig.tick(&mut actor, again);
        assert_eq!(
            event(&lost),
            Some(&Event::Resnapshot(
                SnapshotCause::NewestStagedFileUnreadable
            ))
        );
        assert_eq!(staged(&rig), vec![1]);
        let seen = rig.settle(&mut actor, again);
        no_stop(&seen);
        assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2)]);
        assert_eq!(snapshots(&rig), vec![1, 2]);
        assert_eq!(restored(&rig), 2);
    }
}

#[test]
fn a_crash_between_the_drop_and_the_snapshot_boots_clean() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    rig.tick(&mut actor, T0 + 1);
    tear(&rig, 2, truncated);
    let again = first_bad_read(&mut rig, &mut actor, T0 + 1);
    let lost = rig.tick(&mut actor, again);
    assert_eq!(
        event(&lost),
        Some(&Event::Resnapshot(
            SnapshotCause::NewestStagedFileUnreadable
        ))
    );
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, again + 1);
    // Nothing staged was lost this time: the WAL carries on from staged 1.
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, again + 1);
    no_stop(&seen);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2)]);
    assert_eq!(restored(&rig), 2);
}

mod checkpoint_failed;
mod degraded;
mod index_reset;
mod listing;
mod live;
mod newest_replica_file;
mod no_wal_yet;
mod pages;
mod rejected_snapshot;
mod stamp;
mod tasks;
mod wal_barrier;
mod witness;

/// Ticks at `now` onto a staged file that fails verification for the first
/// time: one failed read deletes nothing and the task retries. Returns when
/// the retry is due, the tick whose read can drop it.
fn first_bad_read(rig: &mut Rig, actor: &mut Actor, now: i64) -> i64 {
    let tick = rig.tick(actor, now);
    match event(&tick) {
        Some(Event::Retrying {
            fault: Fault::StagedLost,
            until,
        }) => until.unix_millis(),
        _ => panic!("{tick:?}"),
    }
}

/// Whether any tick dropped staged files as unreadable.
fn lost_staged(seen: &[Tick]) -> bool {
    let lost = SnapshotCause::NewestStagedFileUnreadable;
    seen.iter().any(|tick| match event(tick) {
        Some(Event::Resnapshot(cause) | Event::Booted { cause: Some(cause) }) => *cause == lost,
        Some(_) | None => false,
    })
}

fn l0(n: u64) -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(Txid::new(n).unwrap()))
}

/// One read of a sound staged file returns a flipped byte during boot's
/// sweep. A read that fails verification is evidence about the read: boot
/// deletes nothing on it, and the file is uploaded as it is.
#[test]
fn one_bad_read_of_a_staged_file_at_boot_drops_nothing() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3, 4], T0 + 1);
    rig.staging.flip_next_read(l0(3));
    let mut actor = Actor::new(cfg());
    let first = rig.tick(&mut actor, T0 + 2);
    // The bad read deletes nothing: when the WAL could stage the files again,
    // a deletion would not show as a snapshot, only as files gone.
    assert_eq!(staged(&rig), vec![2, 3, 4], "{first:?}");
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    no_stop(&seen);
    assert!(!lost_staged(&seen), "{seen:?}");
    assert_eq!(restored(&rig), 4);
}

/// One read of a sound staged file returns a flipped byte while the upload
/// task copies it: nothing is dropped, and a later tick uploads it.
#[test]
fn one_bad_read_of_a_staged_file_while_uploading_drops_nothing() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(2);
    assert!(matches!(
        event(&rig.tick(&mut actor, T0 + 1)),
        Some(Event::Synced { .. })
    ));
    rig.staging.flip_next_read(l0(2));
    let seen = rig.follow(&mut actor, T0 + 1, 50);
    no_stop(&seen);
    assert!(!lost_staged(&seen), "{seen:?}");
    assert!(ranges(&rig.remote).contains(&(2, 2)));
    assert!(!snapshots(&rig).contains(&2));
    assert_eq!(restored(&rig), 2);
}

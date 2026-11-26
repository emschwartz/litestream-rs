//! The scheduled verify end to end: a real SQLite database, a file replica, and the fake
//! runtime, whose clock moves only when a test says so. Every wait is on a condition (the
//! health snapshot, or a store's own condition variable), never on the clock.

use crate::common;

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::{PATIENCE, Site, config, fake, moved_until, wait};
use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    CreatedAt, FileInfo, FileKey, L0Retention, Level, Listing, Maintenance, ObjectStore, PageLimit,
    ReadLen, StoreError, Upload,
};
use lsr_ltx::{Encoder, Event as LtxEvent, ReadDecoder, TxidRange};
use lsr_replica::{
    ChannelSink, Config, DynStore, Event, EventReceiver, Health, Mismatch, NonZeroDuration, Phase,
    Replicator, Timestamp, Txid, Verification, VerifyOutcome,
};
use lsr_store_fs::FsStore;

const POLL: Duration = Duration::from_secs(1);
const EVERY: Duration = Duration::from_secs(60);

fn verifying() -> Config {
    Config {
        verify_interval: NonZeroDuration::new(EVERY),
        ..config()
    }
}

/// [`verifying`] with a maintenance schedule that writes its first level-1 file and its first
/// level-9 snapshot as soon as it can, then nothing for a day (the next day boundary is 6,400
/// s after [`common::START`], and these tests run for minutes). A snapshot reads the live
/// database, and a restore prefers it to the level-0 files it covers, so a test that
/// corrupts a level-0 file needs the snapshot written before the transaction it corrupts
/// and none after.
fn verifying_quietly() -> Config {
    let day = Duration::from_secs(86_400);
    let two_days = day.saturating_mul(2);
    Config {
        maintenance: Maintenance::new(vec![day], two_days, two_days, L0Retention::Keep).unwrap(),
        ..verifying()
    }
}

/// The most polls the first level-1 file and level-9 snapshot take to land.
const SETTLE_POLLS: usize = 10;

/// Moves the clock a poll at a time until the replica holds a level-9 snapshot and a level-1
/// file (the snapshot is planned first, the level-1 file a poll or more later): from then on
/// [`verifying_quietly`] writes neither. Stopping at the snapshot alone left the level-1 file
/// to land in a later poll, where it could cover a level-0 file a test had just removed.
fn settle_maintenance(replicator: &Replicator, clock: &lsr_replica::FakeClockHandle, site: &Site) {
    let store = FsStore::new(&site.replica);
    let mut jumped = false;
    for polls in 0..SETTLE_POLLS {
        let snapshots = block_on(store.list(Level::SNAPSHOT, None, PageLimit::MAX)).unwrap();
        let one = Level::new(1).unwrap();
        let level_one = block_on(store.list(one, None, PageLimit::MAX)).unwrap();
        if !snapshots.files.is_empty() && !level_one.files.is_empty() {
            return;
        }
        // The engine syncs a commit once per poll interval, so a commit made right after the
        // boot's file can reach the replica after the boot's first maintenance pass: that pass
        // found nothing above the snapshot and the level's next window is the next day
        // boundary, 6,400 s after [`common::START`]. Crossing it is what lands the level-1 file.
        if polls >= 3 && !snapshots.files.is_empty() && !jumped {
            jumped = true;
            advance(replicator, clock, Duration::from_secs(6_400));
        }
        advance(replicator, clock, POLL);
    }
    let held: Vec<String> = Level::ALL
        .iter()
        .flat_map(|level| {
            let listed = block_on(store.list(*level, None, PageLimit::MAX)).unwrap();
            listed
                .files
                .into_iter()
                .map(|file| format!("{:?}", file.key))
        })
        .collect();
    panic!("no level-9 snapshot and level-1 file after {SETTLE_POLLS} polls: {held:?}");
}

fn replicated(health: &Health) -> Option<Txid> {
    health.replicated.map(|(txid, _)| txid)
}

fn caught_up(health: &Health) -> bool {
    health.synced.is_some()
        && health.synced.map(|(txid, _)| txid) == replicated(health)
        && health.unreplicated_since.is_none()
}

fn runs(health: &Health) -> u64 {
    let runs = health.verification.runs;
    [runs.failed, runs.merged, runs.incomplete]
        .into_iter()
        .fold(runs.passed, u64::saturating_add)
}

fn events(replicator: &Replicator) -> EventReceiver {
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(4096).unwrap());
    replicator.set_events(Box::new(sink));
    events
}

/// Whether a scheduled verify event was sent since the last look.
fn any_verified(events: &EventReceiver) -> bool {
    events
        .drain()
        .iter()
        .any(|event| matches!(event, Event::Verified { .. }))
}

/// The next scheduled verify event, waiting for it (the thread writes the health snapshot
/// before it sends the event). It looks at the queue each time the health changes, so the
/// bound of [`wait`] ends a wait for an event that never comes.
fn next_verified(replicator: &Replicator, events: &EventReceiver) -> (Txid, VerifyOutcome) {
    let found = Cell::new(None);
    wait(replicator, |_| {
        while found.get().is_none()
            && let Some(event) = events.try_recv()
        {
            if let Event::Verified { txid, outcome } = event {
                found.set(Some((txid, outcome)));
            }
        }
        found.get().is_some()
    });
    found.get().unwrap()
}

/// Moves the clock and waits for the thread to have turned at the new time.
fn advance(replicator: &Replicator, clock: &lsr_replica::FakeClockHandle, by: Duration) {
    clock.advance(by);
    let now = clock.now();
    wait(replicator, |h| h.reported_at >= now);
}

/// Advances the clock to `at`, which must still be ahead: a deadline measured from when a run
/// was due, not a fixed step after a catch-up whose own length (how many polls the replicator
/// needed) depends on thread timing.
fn advance_to(replicator: &Replicator, clock: &lsr_replica::FakeClockHandle, at: Timestamp) {
    let now = clock.now();
    assert!(now < at, "the catch-up ran past {at:?}: it is {now:?}");
    advance(replicator, clock, at.saturating_duration_since(now));
}

/// Rewrites one page of the newest level-0 file on the replica, through the store, as a
/// stale page would look: every file still decodes and verifies, and the restore of its
/// transaction holds a page the database never had, provided that restore reads this file
/// (no snapshot or compacted file ends at the same transaction: see
/// [`verifying_quietly`]). Returns the transaction and the page.
fn rewrite_newest_page(site: &Site) -> (Txid, u32) {
    let store = FsStore::new(&site.replica);
    block_on(async {
        let listing = store.list(Level::ZERO, None, PageLimit::MAX).await.unwrap();
        let info = *listing.files.last().unwrap();
        let len = ReadLen::new(ReadLen::MAX_RAW).unwrap();
        let bytes = store.read(&info.key, 0, len).await.unwrap();
        let mut decoder = ReadDecoder::new(bytes.as_slice());
        let (mut header, mut pages) = (None, Vec::new());
        while let Some(event) = decoder.next_event().unwrap() {
            match event {
                LtxEvent::Header(h) => header = Some(h),
                LtxEvent::Page(page) => pages.push((page.pgno(), page.data().to_vec())),
            }
        }
        let header = header.unwrap();
        let (pgno, page) = pages.last_mut().unwrap();
        let stale = *pgno;
        for byte in page.iter_mut().rev().take(16) {
            *byte ^= 0x5A;
        }
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        for (pgno, page) in &pages {
            encoder.write_page(*pgno, page).unwrap();
        }
        let (file, _trailer) = encoder.finish(None).unwrap();
        let mut upload = store
            .begin_upload(&info.key, header.fields().timestamp, WriteMode::Replace)
            .await
            .unwrap();
        upload.write(&file).await.unwrap();
        upload.commit().await.unwrap();
        (info.key.range().max(), stale.get())
    })
}

/// Off unless configured, as Go's `validation.interval`: however long the replicator runs,
/// nothing is verified and the health says so.
#[test]
fn the_scheduled_verify_is_off_by_default() {
    assert_eq!(Config::default().verify_interval, None);
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::builder(&site.db, site.store())
        .config(config())
        .runtime(runtime)
        .start()
        .unwrap();
    let events = events(&replicator);
    site.commit(3);
    advance(&replicator, &clock, POLL);
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    for _ in 0..3 {
        advance(&replicator, &clock, EVERY);
    }
    let health = replicator.health();
    assert_eq!(health.verification, Verification::default());
    assert!(!any_verified(&events));
}

/// A run falls due while nothing else would wake the thread: no poll for an hour, no timed
/// checkpoint, maintenance a day away. The thread wakes for the run.
#[test]
fn a_run_due_between_polls_wakes_the_thread() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let rare_polls = Config {
        poll_interval: NonZeroDuration::new(Duration::from_secs(3600)).unwrap(),
        checkpoint: lsr_core::CheckpointConfig {
            interval: None,
            ..lsr_core::CheckpointConfig::default()
        },
        ..verifying_quietly()
    };
    let replicator = Replicator::builder(&site.db, site.store())
        .config(rare_polls)
        .runtime(runtime)
        .start()
        .unwrap();
    wait(&replicator, caught_up);
    // Parked, so the run cannot start with a turn that begins after the clock moves.
    let parked = crate::waiting::seen_or(&replicator, &clock, &|_| false).unwrap();
    assert!(parked.is_none());
    clock.advance(EVERY);
    let health = wait(&replicator, |h| runs(h) == 1);
    assert_eq!(health.verification.runs.passed, 1, "{health:?}");
}

/// The verify thread ends with the replicator: once it is stopped, nothing holds the store.
#[test]
fn the_verify_thread_lets_the_store_go_once_the_replicator_stops() {
    let site = Site::new();
    let store = site.store();
    let held = Arc::downgrade(&store);
    let (runtime, clock) = fake();
    let replicator = Replicator::builder(&site.db, store)
        .config(verifying())
        .runtime(runtime)
        .start()
        .unwrap();
    wait(&replicator, caught_up);
    clock.advance(EVERY);
    wait(&replicator, |h| runs(h) == 1);
    assert!(matches!(
        common::stopped(&replicator, &clock),
        lsr_replica::StopOutcome::Drained { .. }
    ));
    // The verify thread drops its handle on the store as it ends, on its own time.
    let mut looks = 0_u32;
    while held.strong_count() > 0 {
        looks += 1;
        assert!(
            looks < 10_000_000,
            "the verify thread still holds the store"
        );
        std::thread::yield_now();
    }
}

/// A wall clock that steps back before the first run does not add the step to the wait: the
/// run comes within a few intervals of clock, never the hour of the step.
#[test]
fn a_clock_step_back_does_not_put_off_the_scheduled_verify() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::builder(&site.db, site.store())
        .config(verifying())
        .runtime(runtime)
        .start()
        .unwrap();
    site.commit(3);
    advance(&replicator, &clock, POLL);
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    clock.step_back(Duration::from_secs(3600));
    let health = moved_until(&replicator, &clock, (POLL, 240), |h| runs(h) >= 1);
    assert_eq!(health.verification.runs.passed, 1, "{health:?}");
}

/// The first run is one interval after the start and the next one interval after it; each
/// compares the restore of the transaction the replica holds with the live database, and a
/// clean replica passes.
///
/// The transaction a run verifies is bounded, not pinned: at least the one the replica held
/// when the test saw the engine caught up, at most the one it holds once the run is seen. The
/// replicator's thread runs on its own, and a checkpoint stages a file of its own (a new
/// transaction with no commit of the test's), which can land on either side of the run's read
/// of the live database. Pinning the run to the earlier id failed under load (expected 3, saw
/// 4); pinning it to the later one fails the other way.
#[test]
fn the_scheduled_verify_runs_on_its_interval_and_a_clean_replica_passes() {
    let site = Site::new();
    let (runtime, clock) = fake();
    // A poll short of the first run, an interval after the replicator starts.
    let almost = clock.now().saturating_add(EVERY.saturating_sub(POLL));
    let replicator = Replicator::builder(&site.db, site.store())
        .config(verifying())
        .runtime(runtime)
        .start()
        .unwrap();
    let events = events(&replicator);
    site.commit(3);
    advance(&replicator, &clock, POLL);
    let health = moved_until(&replicator, &clock, (POLL, 240), caught_up);
    advance_to(&replicator, &clock, almost);
    assert_eq!(runs(&replicator.health()), 0, "not due yet");
    clock.advance(POLL);
    let first = wait(&replicator, |h| runs(h) == 1);
    // The next run is due an interval after this one ended, on the clock as it reads now (the
    // fake clock does not move while the test waits): a poll short of it.
    let almost = clock.now().saturating_add(EVERY.saturating_sub(POLL));
    let check = first.verification;
    assert_eq!(check.runs.passed, 1, "{check:?}");
    let verified = check.last_good.map(|(txid, _)| txid);
    assert!(
        replicated(&health) <= verified && verified <= replicated(&first),
        "{verified:?} outside {:?}..={:?}",
        replicated(&health),
        replicated(&first)
    );
    assert_eq!(check.failing, None);
    assert_eq!(check.kept_restore_point, None);
    assert_eq!(check.worst_finding, None);
    assert!(!first.is_degraded());
    let seen = next_verified(&replicator, &events);
    assert!(
        matches!(seen, (txid, VerifyOutcome::Passed { pages }) if Some(txid) == verified && pages > 0),
        "{seen:?}"
    );
    site.commit(2);
    advance(&replicator, &clock, POLL);
    let newer = moved_until(&replicator, &clock, (POLL, 240), caught_up);
    advance_to(&replicator, &clock, almost);
    assert_eq!(
        runs(&replicator.health()),
        1,
        "an interval after the last run"
    );
    clock.advance(POLL);
    let second = wait(&replicator, |h| runs(h) == 2);
    assert_eq!(second.verification.runs.passed, 2);
    let verified_again = second.verification.last_good.map(|(txid, _)| txid);
    assert!(
        replicated(&newer) <= verified_again && verified_again <= replicated(&second),
        "{verified_again:?} outside {:?}..={:?}",
        replicated(&newer),
        replicated(&second)
    );
    assert!(
        verified_again > verified,
        "the second run verified the second batch"
    );
    site.assert_restores(&*site.store());
}

/// A page of the replica rewritten through the store (every file still verifies) is found
/// by the next run: a degraded state naming the page and the transaction, an event, and
/// retention pinned to the last run that passed. Replication goes on, and a later run that
/// passes (after a snapshot rewrites every page) clears it.
#[test]
fn a_wrong_page_in_the_replica_is_degraded_with_the_page_and_txid_and_pins_the_last_good_point() {
    let site = Site::new();
    let store = site.store();
    let (runtime, clock) = fake();
    let replicator = Replicator::builder(&site.db, Arc::clone(&store))
        .config(verifying_quietly())
        .runtime(runtime)
        .start()
        .unwrap();
    let events = events(&replicator);
    // The boot's first file goes up before any commit: a commit that beats the boot is inside
    // that one file, nothing is left to compact, and the schedule then writes no level-1 file
    // and no level-9 snapshot for a day (see `settle_maintenance`).
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    site.commit(3);
    advance(&replicator, &clock, POLL);
    let good = replicated(&moved_until(&replicator, &clock, (POLL, 240), caught_up)).unwrap();
    // Before the transaction it corrupts: a snapshot ending there would hide the page.
    settle_maintenance(&replicator, &clock, &site);
    clock.advance(EVERY);
    wait(&replicator, |h| h.verification.runs.passed == 1);
    site.commit(2);
    advance(&replicator, &clock, POLL);
    let health = moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && replicated(h) > Some(good)
    });
    let (txid, page) = rewrite_newest_page(&site);
    assert_eq!(Some(txid), replicated(&health));
    assert!(matches!(
        next_verified(&replicator, &events).1,
        VerifyOutcome::Passed { .. }
    ));
    clock.advance(EVERY);
    let failed = wait(&replicator, |h| h.verification.runs.failed == 1);
    let failing = failed.verification.failing.unwrap();
    assert_eq!(failing.txid, txid);
    assert!(
        matches!(failing.found, Mismatch::Pages { first, pages: 1, .. } if first == page),
        "{failing:?}"
    );
    assert_eq!(failed.verification.kept_restore_point, Some(good));
    assert!(failed.is_degraded());
    assert_eq!(failed.phase, Phase::Running, "a degraded state, not a stop");
    let seen = next_verified(&replicator, &events);
    assert!(
        matches!(seen, (at, VerifyOutcome::Failed(Mismatch::Pages { first, .. })) if at == txid && first == page),
        "{seen:?}"
    );
    // Replication goes on.
    site.commit(1);
    advance(&replicator, &clock, POLL);
    moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && replicated(h) > Some(txid)
    });
    // A snapshot rewrites every page; the next run passes and lets the read lock go.
    replicator.request_snapshot();
    site.commit(1);
    advance(&replicator, &clock, POLL);
    let fixed = moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && h.resnapshot.is_none()
    });
    clock.advance(EVERY);
    let passed = wait(&replicator, |h| h.verification.runs.passed == 2);
    assert_eq!(passed.verification.failing, None);
    assert_eq!(passed.verification.kept_restore_point, None);
    assert_eq!(
        passed.verification.last_good.map(|(txid, _)| txid),
        replicated(&fixed)
    );
    assert!(!passed.is_degraded());
}

/// A file replica whose listings, made on the verify thread only, wait until the test opens
/// the gate: a scheduled verify stuck in a slow store call.
struct Gated {
    inner: FsStore,
    open: Mutex<bool>,
    entered: AtomicBool,
    changed: Condvar,
}

impl Gated {
    fn new(site: &Site) -> Self {
        Self {
            inner: FsStore::new(&site.replica),
            open: Mutex::new(false),
            entered: AtomicBool::new(false),
            changed: Condvar::new(),
        }
    }

    fn hold_if_verifying(&self) {
        if std::thread::current().name() != Some("lsr-verify") {
            return;
        }
        self.entered.store(true, Ordering::SeqCst);
        self.changed.notify_all();
        let open = self.open.lock().unwrap();
        let (_open, timeout) = self
            .changed
            .wait_timeout_while(open, PATIENCE, |open| !*open)
            .unwrap();
        assert!(!timeout.timed_out(), "the test never opened the gate");
    }

    fn wait_entered(&self) {
        let open = self.open.lock().unwrap();
        let (_open, timeout) = self
            .changed
            .wait_timeout_while(open, PATIENCE, |_| !self.entered.load(Ordering::SeqCst))
            .unwrap();
        assert!(!timeout.timed_out(), "the verify never listed");
    }

    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl ObjectStore for Gated {
    type Upload = <FsStore as ObjectStore>::Upload;

    /// Forwards to the store it wraps.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.hold_if_verifying();
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.inner.read(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        self.inner
            .begin_sized_upload(key, created_at, mode, len)
            .await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

/// A run whose restore matches the live database still fails when its listing finds the
/// newest history out of reach: here a hole opens after the transaction it restores, while
/// it waits inside the store.
#[test]
fn a_run_that_restores_but_finds_the_newest_out_of_reach_fails_as_unrestorable() {
    let site = Site::new();
    let gated = Arc::new(Gated::new(&site));
    let store: Arc<dyn DynStore> = Arc::<Gated>::clone(&gated);
    let (runtime, clock) = fake();
    let replicator = Replicator::builder(&site.db, store)
        .config(verifying_quietly())
        .runtime(runtime)
        .start()
        .unwrap();
    // The boot's first file goes up before any commit: a commit that beats the boot is inside
    // that one file, nothing is left to compact, and the schedule then writes no level-1 file
    // and no level-9 snapshot for a day (see `settle_maintenance`).
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    site.commit(1);
    advance(&replicator, &clock, POLL);
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    settle_maintenance(&replicator, &clock, &site);
    clock.advance(EVERY);
    gated.wait_entered();
    let mut last = replicated(&replicator.health());
    for _ in 0..2 {
        site.commit(1);
        advance(&replicator, &clock, POLL);
        last = replicated(&moved_until(&replicator, &clock, (POLL, 240), |h| {
            caught_up(h) && replicated(h) > last
        }));
    }
    // The second newest level-0 file goes: the newest is past a hole.
    let files = FsStore::new(&site.replica);
    let zero = block_on(files.list(Level::ZERO, None, PageLimit::MAX)).unwrap();
    let hole = zero.files.iter().rev().nth(1).unwrap().key;
    block_on(files.delete(&[hole])).unwrap();
    gated.release();
    let done = wait(&replicator, |h| runs(h) == 1);
    let failing = done.verification.failing.unwrap();
    assert_eq!(
        failing.found,
        Mismatch::Unrestorable,
        "{:?}",
        done.verification
    );
}

/// While a run waits inside a store call, commits are still synced and uploaded at each
/// poll: the run's store calls happen on its own thread, never the sync task's.
#[test]
fn a_verify_stuck_in_the_store_does_not_delay_syncs() {
    let site = Site::new();
    let gated = Arc::new(Gated::new(&site));
    let store: Arc<dyn DynStore> = Arc::<Gated>::clone(&gated);
    let (runtime, clock) = fake();
    let replicator = Replicator::builder(&site.db, store)
        .config(verifying())
        .runtime(runtime)
        .start()
        .unwrap();
    site.commit(3);
    advance(&replicator, &clock, POLL);
    let verified_at = replicated(&moved_until(&replicator, &clock, (POLL, 240), caught_up));
    clock.advance(EVERY);
    gated.wait_entered();
    let mut last = verified_at;
    for _ in 0..3 {
        site.commit(2);
        advance(&replicator, &clock, POLL);
        let health = moved_until(&replicator, &clock, (POLL, 240), |h| {
            caught_up(h) && replicated(h) > last
        });
        assert_eq!(runs(&health), 0, "the run is still in the store");
        last = replicated(&health);
    }
    gated.release();
    let done = wait(&replicator, |h| runs(h) == 1);
    assert_eq!(done.verification.runs.passed, 1, "{:?}", done.verification);
    assert_eq!(
        done.verification.last_good.map(|(txid, _)| txid),
        verified_at
    );
    site.assert_restores(&*site.store());
}

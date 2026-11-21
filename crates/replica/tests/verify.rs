//! The scheduled verify end to end: a real SQLite database, a file replica, and the fake
//! runtime, whose clock moves only when a test says so. Every wait is on a condition (the
//! health snapshot, or a store's own condition variable), never on the clock.

#![expect(
    clippy::unwrap_used,
    reason = "clippy.toml exempts only #[test] bodies; in these helpers a failure is the test failing"
)]

mod common;

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use common::{PATIENCE, Site, config, fake, wait};
use lsr_core::conformance::block_on;
use lsr_core::{
    FileInfo, FileKey, Level, Listing, ObjectStore, PageLimit, ReadLen, StoreError, StoredStamp,
    Upload,
};
use lsr_ltx::{Encoder, Event as LtxEvent, ReadDecoder, TxidRange};
use lsr_replica::{
    ChannelSink, Config, DynStore, Event, EventReceiver, Health, Mismatch, NonZeroDuration, Phase,
    Replicator, Timestamp, Txid, Verification, VerifyOutcome,
};
use lsr_store_fs::FsStore;

const POLL: Duration = Duration::from_secs(1);
const EVERY: Duration = Duration::from_secs(60);
/// Two polls short of the interval.
const ALMOST: Duration = Duration::from_secs(58);

fn verifying() -> Config {
    Config {
        verify_interval: NonZeroDuration::new(EVERY),
        ..config()
    }
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

/// The next scheduled verify event, waiting for it: the thread writes the health snapshot
/// before it sends the event.
fn next_verified(events: &EventReceiver) -> (Txid, VerifyOutcome) {
    loop {
        if let Event::Verified { txid, outcome } = events.recv().unwrap() {
            return (txid, outcome);
        }
    }
}

/// Moves the clock and waits for the thread to have turned at the new time.
fn advance(replicator: &Replicator, clock: &lsr_replica::FakeClockHandle, by: Duration) {
    clock.advance(by);
    let now = clock.now();
    wait(replicator, |h| h.observed_at >= now);
}

/// Rewrites one page of the newest level-0 file on the replica, through the store, as a
/// stale page would look: every file still decodes and verifies, and the restore of its
/// transaction holds a page the database never had. Returns the transaction and the page.
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
                LtxEvent::Page { pgno, data } => pages.push((pgno, data.to_vec())),
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
            .begin_upload(&info.key, header.fields().timestamp)
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
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    let events = events(&replicator);
    site.commit(3);
    advance(&replicator, &clock, POLL);
    wait(&replicator, caught_up);
    for _ in 0..3 {
        advance(&replicator, &clock, EVERY);
    }
    let health = replicator.health();
    assert_eq!(health.verification, Verification::default());
    assert!(!any_verified(&events));
}

/// The first run is one interval after the start and the next one interval after it; each
/// compares the restore of the transaction the replica holds with the live database, and a
/// clean replica passes.
#[test]
fn the_scheduled_verify_runs_on_its_interval_and_a_clean_replica_passes() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, site.store(), verifying(), runtime).unwrap();
    let events = events(&replicator);
    site.commit(3);
    advance(&replicator, &clock, POLL);
    let health = wait(&replicator, caught_up);
    advance(&replicator, &clock, ALMOST);
    assert_eq!(runs(&replicator.health()), 0, "not due yet");
    clock.advance(POLL);
    let first = wait(&replicator, |h| runs(h) == 1);
    let check = first.verification;
    assert_eq!(check.runs.passed, 1, "{check:?}");
    assert_eq!(check.last_good.map(|(txid, _)| txid), replicated(&health));
    assert_eq!(check.failing, None);
    assert_eq!(check.pinned, None);
    assert_eq!(check.worst_finding, None);
    assert!(!first.is_degraded());
    let seen = next_verified(&events);
    assert!(
        matches!(seen, (txid, VerifyOutcome::Passed { pages }) if Some(txid) == replicated(&health) && pages > 0),
        "{seen:?}"
    );
    site.commit(2);
    advance(&replicator, &clock, POLL);
    let newer = wait(&replicator, caught_up);
    advance(&replicator, &clock, ALMOST);
    assert_eq!(
        runs(&replicator.health()),
        1,
        "an interval after the last run"
    );
    clock.advance(POLL);
    let second = wait(&replicator, |h| runs(h) == 2);
    assert_eq!(second.verification.runs.passed, 2);
    assert_eq!(
        second.verification.last_good.map(|(txid, _)| txid),
        replicated(&newer)
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
    let replicator =
        Replicator::start_with(&site.db, Arc::clone(&store), verifying(), runtime).unwrap();
    let events = events(&replicator);
    site.commit(3);
    advance(&replicator, &clock, POLL);
    let good = replicated(&wait(&replicator, caught_up)).unwrap();
    clock.advance(EVERY);
    wait(&replicator, |h| h.verification.runs.passed == 1);
    site.commit(2);
    advance(&replicator, &clock, POLL);
    let health = wait(&replicator, |h| caught_up(h) && replicated(h) > Some(good));
    let (txid, page) = rewrite_newest_page(&site);
    assert_eq!(Some(txid), replicated(&health));
    assert!(matches!(
        next_verified(&events).1,
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
    assert_eq!(failed.verification.pinned, Some(good));
    assert!(failed.is_degraded());
    assert_eq!(failed.phase, Phase::Running, "a degraded state, not a stop");
    let seen = next_verified(&events);
    assert!(
        matches!(seen, (at, VerifyOutcome::Failed(Mismatch::Pages { first, .. })) if at == txid && first == page),
        "{seen:?}"
    );
    // Replication goes on.
    site.commit(1);
    advance(&replicator, &clock, POLL);
    wait(&replicator, |h| caught_up(h) && replicated(h) > Some(txid));
    // A snapshot rewrites every page; the next run passes and lets the pin go.
    replicator.request_snapshot();
    site.commit(1);
    advance(&replicator, &clock, POLL);
    let fixed = wait(&replicator, |h| caught_up(h) && h.resnapshot.is_none());
    clock.advance(EVERY);
    let passed = wait(&replicator, |h| h.verification.runs.passed == 2);
    assert_eq!(passed.verification.failing, None);
    assert_eq!(passed.verification.pinned, None);
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

    async fn stamp(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
        self.inner.stamp(file).await
    }

    async fn read_with_stamp(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, StoredStamp), StoreError> {
        self.inner.read_with_stamp(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        self.inner.begin_upload(key, created_at).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

/// While a run waits inside a store call, commits are still synced and uploaded at each
/// poll: the run's store calls happen on its own thread, never the sync lane's.
#[test]
fn a_verify_stuck_in_the_store_does_not_delay_syncs() {
    let site = Site::new();
    let gated = Arc::new(Gated::new(&site));
    let store: Arc<dyn DynStore> = Arc::<Gated>::clone(&gated);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, verifying(), runtime).unwrap();
    site.commit(3);
    advance(&replicator, &clock, POLL);
    let verified_at = replicated(&wait(&replicator, caught_up));
    clock.advance(EVERY);
    gated.wait_entered();
    let mut last = verified_at;
    for _ in 0..3 {
        site.commit(2);
        advance(&replicator, &clock, POLL);
        let health = wait(&replicator, |h| caught_up(h) && replicated(h) > last);
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

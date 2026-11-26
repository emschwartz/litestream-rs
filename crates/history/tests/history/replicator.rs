//! The public API end to end, judged from outside: `ReplicatorBuilder::start` on a real SQLite file,
//! the application's own connection writing the self-certifying chain, restarts (a stop that
//! drains, then a new replicator on the same files), and real restores into files, on a file
//! replica and on MinIO.
//!
//! The clock is `Runtime::fake`: it moves only when the test moves it, and every wait is on a
//! condition of the health snapshot. After each drained stop and at the end, `Latest` must be
//! the local head (lag bound 0); at random moments `Latest`, two `AsOf` instants and every
//! restore point are judged.

use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use lsr_core::ObjectStore;
use lsr_history::{AheadMode, Counts, Judge, Log, chain};
use lsr_replica::{
    DynStore, FakeClockHandle, Health, Phase, Replicator, Runtime, StopOutcome, Timestamp,
};
use rusqlite::Connection;
use tempfile::TempDir;

use crate::real::{Rng, Scratch, judge};

pub const START: Timestamp = Timestamp::from_unix_millis(1_700_000_000_000);
pub const PATIENCE: Duration = Duration::from_secs(600);
pub const POLL: Duration = Duration::from_secs(1);
/// Health looks before a wait gives up: the replicator wakes a waiter on every change and at
/// least every `CALLER_SLICE`, so this is about a minute of nothing changing.
const LOOKS: u32 = 1_200;

/// Polls a driver may move the clock while it waits: past the longest retry wait
/// (`lsr_core::MAX_BACKOFF`, five minutes), so a backoff is always waited out.
const MAX_POLLS: u32 = 400;

/// Waits until `done` holds, moving the clock a poll at a time, each time only once the
/// replica thread's park has read the clock as it reads now: the thread is waiting on that
/// clock, not working. Whatever the engine has timed on the clock (a retry's backoff after a
/// failed upload, the next poll) is waited out; moving the clock once and watching stalled
/// whenever a backoff outlasted the one move.
pub fn moved_until(
    replicator: &Replicator,
    clock: &FakeClockHandle,
    done: impl Fn(&Health) -> bool,
) -> Health {
    for _ in 0..MAX_POLLS {
        if let Some(health) = seen_or(replicator, clock, &done) {
            return health;
        }
        clock.advance(POLL);
    }
    seen_or(replicator, clock, &done).unwrap_or_else(|| panic_with(&replicator.health()))
}

/// Waits until `done` holds, returning that health, or until the thread's park has read the
/// clock as it reads now, returning `None`; fails the test after [`LOOKS`] looks.
fn seen_or(
    replicator: &Replicator,
    clock: &FakeClockHandle,
    done: &impl Fn(&Health) -> bool,
) -> Option<Health> {
    let now = clock.now();
    let (looks, met) = (Cell::new(0_u32), Cell::new(false));
    let seen = replicator.wait_until(PATIENCE, |health| {
        looks.set(looks.get().saturating_add(1));
        met.set(done(health));
        met.get() || clock.seen_by_park() == Some(now) || looks.get() >= LOOKS
    });
    match seen {
        Some(health) if met.get() => Some(health),
        Some(_) if clock.seen_by_park() == Some(now) => None,
        Some(health) => panic_with(&health),
        None => panic_with(&replicator.health()),
    }
}

fn panic_with(health: &Health) -> ! {
    panic!("the replicator never got there: {health:?}")
}

/// One driven run: the engine writes through `store`, restores read `restore` (the same store,
/// or the one behind a faulty network, so restores are judged while the fault holds).
pub struct Run<'a, S, R = S> {
    store: &'a Arc<S>,
    restore: &'a R,
    db: &'a Path,
    app: Connection,
    runtime: Runtime,
    pub clock: FakeClockHandle,
    replicator: Option<Replicator>,
    log: Log,
    pub judge: Judge,
    tip: chain::Chain,
    pub rng: Rng,
    scratch: Scratch,
}

impl<'a, S, R> Run<'a, S, R>
where
    S: ObjectStore<Upload: 'static> + Send + Sync + 'static,
    R: ObjectStore<Upload: 'static>,
{
    pub fn new(store: &'a Arc<S>, restore: &'a R, dir: &Path, db: &'a Path, seed: u64) -> Self {
        let app = Connection::open(db).unwrap();
        app.busy_timeout(Duration::from_secs(5)).unwrap();
        app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .unwrap();
        lsr_sqlite::app_connection(&app).unwrap();
        app.execute_batch(chain::SCHEMA).unwrap();
        let (runtime, clock) = Runtime::fake(START, seed);
        let tip = chain::Chain::new();
        Self {
            store,
            restore,
            db,
            app,
            runtime,
            log: Log::new(tip.digest(), clock.now().unix_millis()),
            clock,
            replicator: None,
            judge: Judge::new(AheadMode::Count),
            tip,
            rng: Rng::new(seed),
            scratch: Scratch::new(&dir.join("restores")),
        }
    }

    pub fn start(&mut self) {
        let store: Arc<dyn DynStore> = Arc::<S>::clone(self.store);
        let replicator = Replicator::builder(self.db, store)
            .runtime(self.runtime.clone())
            .start()
            .unwrap();
        let _booted = replicator.wait_booted(PATIENCE);
        self.replicator = Some(replicator);
    }

    pub fn replicator(&self) -> &Replicator {
        self.replicator.as_ref().unwrap()
    }

    pub fn commit(&mut self) {
        let payload = self.rng.payload();
        let next = self.tip.next(&payload);
        let begun = self.clock.now().unix_millis();
        next.insert(&self.app, &payload).unwrap();
        self.log
            .commit_between(next.digest(), begun, self.clock.now().unix_millis());
        self.tip = next;
    }

    /// The clock moves a poll, and then as far as the replicator needs to catch up: whatever it
    /// has synced is on the replica. The drained stops are where lag is judged, so this asks no
    /// more. Caught up counts only once the thread has parked on the clock as it reads now:
    /// before that, the health is from before the move and says caught up about commits the
    /// engine has not seen yet.
    pub fn poll(&mut self) {
        self.clock.advance(POLL);
        let clock = &self.clock;
        moved_until(self.replicator(), clock, |h| {
            h.synced.map(|(txid, _)| txid) == h.replicated.map(|(txid, _)| txid)
                && h.unreplicated_since.is_none()
                && clock.seen_by_park() == Some(clock.now())
        });
    }

    /// Stops with a drain; everything committed reached the replica, so `Latest` is the head.
    pub fn stop(&mut self) {
        let replicator = self.replicator.take().unwrap();
        let stop = replicator.stop(PATIENCE);
        moved_until(&replicator, &self.clock, |h| {
            matches!(h.phase, Phase::Stopped(_) | Phase::Gone(_))
        });
        let stopped = stop.wait();
        assert!(
            matches!(stopped, StopOutcome::Drained { .. }),
            "{stopped:?}"
        );
        self.judged(true, Some(0));
    }

    pub fn judged(&mut self, sweep: bool, lag: Option<i64>) {
        let now = self.clock.now();
        let instants = self.rng.instants(2, START.unix_millis(), now);
        judge(
            self.restore,
            &self.scratch,
            &self.log,
            &mut self.judge,
            now,
            &instants,
            sweep,
            lag,
        )
        .unwrap_or_else(|anomaly| panic!("{anomaly}"));
    }

    fn steps(&mut self, n: u32) {
        for _ in 0..n {
            match self.rng.below(12) {
                0..=5 => self.commit(),
                6..=8 => self.poll(),
                9 | 10 => self.judged(false, None),
                _ => {
                    self.stop();
                    self.start();
                }
            }
        }
    }
}

/// Drives `steps` steps from `seed` over `store` and returns what the judge counted.
fn run<S: ObjectStore<Upload: 'static> + Send + Sync + 'static>(
    store: &Arc<S>,
    seed: u64,
    steps: u32,
) -> Counts {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let mut run = Run::new(store, store.as_ref(), dir.path(), &db, seed);
    run.start();
    run.steps(steps);
    run.commit();
    run.stop();
    run.judge.counts()
}

fn assert_judged(counts: Counts) {
    eprintln!("{counts:?}");
    assert!(
        counts.latest > 2 && counts.lag > 0 && counts.points > 0,
        "{counts:?}"
    );
    assert_eq!(
        counts.ahead_latest.saturating_add(counts.ahead_as_of),
        0,
        "no power loss, nothing ahead"
    );
}

/// The public API on a file replica: every real restore is the chain the application
/// committed, `Latest` never goes back, and a drained stop leaves `Latest` at the head.
#[test]
fn every_restore_of_a_file_replica_is_a_node_of_the_history() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(lsr_store_fs::FsStore::new(dir.path()));
    assert_judged(run(&store, 7, 60));
}

/// The same against MinIO (`just test-s3`): fails without it, unless `LSR_SKIP=minio`.
#[test]
#[ignore = "needs MinIO; just test-s3"]
fn every_restore_of_a_minio_replica_is_a_node_of_the_history() {
    use lsr_store_s3::{
        BucketName, Endpoint, Prefix, Region, S3Config, S3Credentials, S3Store, SecretString,
    };
    let Some(minio) = lsr_core::needs::minio() else {
        return;
    };
    let scratch = TempDir::new().unwrap();
    let leaf = scratch
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let prefix = format!(
        "history-{}-{}",
        std::process::id(),
        leaf.trim_start_matches('.')
    );
    let config = S3Config::new(
        Endpoint::new(&minio.endpoint).unwrap(),
        BucketName::new("lsr-test").unwrap(),
        Prefix::new(&prefix).unwrap(),
    )
    .with_region(Region::new("us-east-1").unwrap());
    let credentials = S3Credentials::new(minio.access, SecretString::new(minio.secret));
    let store = Arc::new(S3Store::new(config, credentials).unwrap());
    assert_judged(run(&store, 7, 60));
}

//! The public API end to end, judged from outside: `Replicator::start` on a real SQLite file,
//! the application's own connection writing the self-certifying chain, restarts (a stop that
//! drains, then a new replicator on the same files), and real restores into files, on a file
//! replica and on MinIO.
//!
//! The clock is `Runtime::fake`: it moves only when the test moves it, and every wait is on a
//! condition of the health snapshot. After each drained stop and at the end, `Latest` must be
//! the local head (lag bound 0); at random moments `Latest`, two `AsOf` instants and every
//! restore point are judged.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use lsr_core::ObjectStore;
use lsr_history::{AheadMode, Counts, Judge, Log, chain};
use lsr_replica::{Config, DynStore, Health, Phase, Replicator, Runtime, Stopped, Timestamp};
use rusqlite::Connection;
use tempfile::TempDir;

use crate::real::{Rng, Scratch, judge};

const START: Timestamp = Timestamp::from_unix_millis(1_700_000_000_000);
const PATIENCE: Duration = Duration::from_secs(600);
const POLL: Duration = Duration::from_secs(1);
/// Health looks before a wait gives up: the replicator wakes a waiter on every change and at
/// least every `CALLER_SLICE`, so this is about a minute of nothing changing.
const LOOKS: u32 = 1_200;

fn wait(replicator: &Replicator, done: impl Fn(&Health) -> bool) -> Health {
    let looks = std::cell::Cell::new(0_u32);
    let seen = replicator.wait_until(PATIENCE, |health| {
        looks.set(looks.get().saturating_add(1));
        done(health) || looks.get() >= LOOKS
    });
    match seen {
        Some(health) if done(&health) => health,
        other => panic!("the replicator never got there: {other:?}"),
    }
}

struct Run<'a, S> {
    store: &'a Arc<S>,
    db: &'a Path,
    app: Connection,
    runtime: Runtime,
    clock: lsr_replica::FakeClockHandle,
    replicator: Option<Replicator>,
    log: Log,
    judge: Judge,
    tip: chain::Chain,
    rng: Rng,
    scratch: Scratch,
}

impl<'a, S: ObjectStore<Upload: 'static> + Send + Sync + 'static> Run<'a, S> {
    fn new(store: &'a Arc<S>, dir: &Path, db: &'a Path, seed: u64) -> Self {
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

    fn start(&mut self) {
        let store: Arc<dyn DynStore> = Arc::<S>::clone(self.store);
        let replicator =
            Replicator::start_with(self.db, store, Config::default(), self.runtime.clone())
                .unwrap();
        let _booted = replicator.wait_booted(PATIENCE);
        self.replicator = Some(replicator);
    }

    fn replicator(&self) -> &Replicator {
        self.replicator.as_ref().unwrap()
    }

    fn commit(&mut self) {
        let payload = self.rng.payload();
        let next = self.tip.next(&payload);
        let begun = self.clock.now().unix_millis();
        next.insert(&self.app, &payload).unwrap();
        self.log
            .commit_between(next.digest(), begun, self.clock.now().unix_millis());
        self.tip = next;
    }

    /// The clock moves one poll and the replicator is left to catch up: whatever it has synced
    /// is on the replica. The drained stops are where lag is judged, so this asks no more.
    fn poll(&mut self) {
        self.clock.advance(POLL);
        wait(self.replicator(), |h| {
            h.synced.map(|(txid, _)| txid) == h.replicated.map(|(txid, _)| txid)
                && h.unreplicated_since.is_none()
        });
    }

    /// Stops with a drain; everything committed reached the replica, so `Latest` is the head.
    fn stop(&mut self) {
        let replicator = self.replicator.take().unwrap();
        let stop = replicator.stop(PATIENCE);
        wait(&replicator, |h| {
            matches!(h.phase, Phase::Stopped(_) | Phase::Gone(_))
        });
        let stopped = stop.wait();
        assert!(matches!(stopped, Stopped::Drained { .. }), "{stopped:?}");
        self.judged(true, Some(0));
    }

    fn judged(&mut self, sweep: bool, lag: Option<i64>) {
        let now = self.clock.now();
        let instants = self.rng.instants(2, START.unix_millis(), now);
        let store: &S = self.store;
        judge(
            store,
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
    let mut run = Run::new(store, dir.path(), &db, seed);
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

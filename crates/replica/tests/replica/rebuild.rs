//! The engine rebuilt after a panic, end to end: what health and a stop report while the
//! fresh engine waits to boot, what a panic must not lose, and a database file that is not
//! the one the replicator started on by the time the connections reopen.
//!
//! Every test runs on `Runtime::fake`. A stop that should end at its deadline is awaited on a
//! thread of its own with a bound in real time, so a stop that never ends fails the test
//! instead of hanging it.

use crate::common;

use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use common::{Site, config, fake, moved_until, stopped, wait};
use lsr_replica::{
    Config, DegradedCause, DynStore, FinalSync, Health, NonZeroDuration, Phase, Replicator,
    Response, Responses, SnapshotCause, StopCause, Stopped, Task,
};
use rusqlite::Connection;

const POLL: Duration = Duration::from_secs(1);

/// Real time a stop that ends at its deadline may take to say so; reached only by a stop that
/// never ends.
const BOUND: Duration = Duration::from_secs(60);

fn caught_up(health: &Health) -> bool {
    health.synced.is_some()
        && health.synced.map(|(txid, _)| txid) == health.replicated.map(|(txid, _)| txid)
        && health.unreplicated_since.is_none()
}

fn retrying_every(interval: Duration) -> Config {
    Config {
        retry_interval: NonZeroDuration::new(interval).unwrap(),
        ..config()
    }
}

/// The stop's outcome, or `None` if it did not end within [`BOUND`] of real time.
fn ended(sync: FinalSync) -> Option<Stopped> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || tx.send(sync.wait()));
    rx.recv_timeout(BOUND).ok()
}

/// While a fresh engine waits to boot, health keeps what the engine the panic dropped knew:
/// a staged file the replica lacks is still behind, and a stop whose deadline passes in that
/// wait reports it, as the engine itself counts what it cannot read: high, never low.
#[test]
fn a_panic_keeps_what_is_left_in_health_and_the_deadline_report() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let retry = Duration::from_secs(60);
    let replicator =
        Replicator::start_with(&site.db, store, retrying_every(retry), runtime).unwrap();
    let staged = wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    assert!(staged.unreplicated_since.is_some());
    // The upload's retry panics; the fresh engine waits a minute to boot.
    faulty.panic_always.store(true, Ordering::SeqCst);
    clock.advance(retry);
    let rebuilt = wait(&replicator, |h| h.counters.panics == 1);
    assert!(rebuilt.synced >= staged.synced, "{rebuilt:?}");
    assert_eq!(rebuilt.replicated, None, "{rebuilt:?}");
    assert_eq!(
        rebuilt.unreplicated_since, staged.unreplicated_since,
        "{rebuilt:?}"
    );
    // The next rebuild panics too: the same cause, so the degraded state keeps its start.
    let first = rebuilt.task(Task::Sync).unwrap();
    // Short of the wait, the fresh engine has not booted, so it has not panicked again.
    clock.advance(POLL);
    let parked = lsr_replica::waiting::seen_or(&replicator, &clock, &|h| h.counters.panics > 1);
    assert!(parked.unwrap().is_none(), "the fresh engine booted early");
    clock.advance(retry.saturating_mul(2));
    let again = wait(&replicator, |h| h.counters.panics == 2);
    let second = again.task(Task::Sync).unwrap();
    assert_eq!(second.cause, DegradedCause::Bug, "{again:?}");
    assert_eq!(second.since, first.since, "{again:?}");
    assert!(second.retries > first.retries, "{again:?}");
    let stop = replicator.stop(Duration::from_secs(5));
    clock.advance(Duration::from_secs(5));
    let stopped = ended(stop);
    let Some(Stopped::DeadlineReached {
        staged_not_uploaded,
        ..
    }) = stopped
    else {
        panic!("{stopped:?}");
    };
    assert!(staged_not_uploaded >= 1, "{stopped:?}");
}

/// The deadline report while a fresh engine waits counts every staged file past the
/// newest transaction the replica held, the one staged in the tick that panicked among
/// them, and none the replica already holds.
#[test]
fn a_deadline_report_after_a_panic_counts_the_staged_files_past_the_replica() {
    use lsr_core::{Level, ObjectStore as _, PageLimit};
    let site = Site::new();
    let faulty = site.faulty();
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let retry = Duration::from_secs(60);
    let replicator =
        Replicator::start_with(&site.db, store, retrying_every(retry), runtime).unwrap();
    let first = moved_until(&replicator, &clock, (POLL, 240), caught_up);
    // The engine syncs new commits once per poll interval, so the replica holds a file of its
    // own beyond the first sync only after a commit and the interval it waits for.
    site.commit(1);
    let held = moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && h.synced > first.synced
    })
    .replicated;
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    site.commit(1);
    let failing = moved_until(&replicator, &clock, (POLL, 240), |h| {
        h.synced > held && h.counters.retries > 0
    });
    // The engine stages this commit before the retry, whose upload then panics: a retry owed
    // goes first in a tick, so a commit the engine has not yet synced is not staged by it.
    site.commit(1);
    moved_until(&replicator, &clock, (POLL, 30), |h| {
        h.synced > failing.synced
    });
    faulty.panic_always.store(true, Ordering::SeqCst);
    // The clock moves only once the engine is parked on it: a move while it still works is lost.
    let parked = lsr_replica::waiting::seen_or(&replicator, &clock, &|_| false);
    assert!(parked.unwrap().is_none(), "the engine never parked");
    clock.advance(retry);
    wait(&replicator, |h| h.counters.panics == 1);
    let meta = config().meta_dir_for(&site.db).unwrap();
    let listed = lsr_replica::block_on(lsr_store_fs::FsStore::new(&meta).list(
        Level::ZERO,
        None,
        PageLimit::MAX,
    ))
    .unwrap();
    let held = held.map(|(txid, _)| txid);
    let past = listed
        .files
        .iter()
        .filter(|info| Some(info.key.range().max()) > held)
        .count();
    assert!(past >= 2, "{listed:?}");
    assert!(
        listed.files.len() > past,
        "nothing the replica holds is staged"
    );
    let stop = replicator.stop(Duration::from_secs(5));
    clock.advance(Duration::from_secs(5));
    let stopped = ended(stop);
    let Some(Stopped::DeadlineReached {
        staged_not_uploaded,
        ..
    }) = stopped
    else {
        panic!("{stopped:?}");
    };
    assert_eq!(usize::try_from(staged_not_uploaded).unwrap(), past);
}

/// A panic in a drain does not wait out the rebuild's wait: the stop ends at its deadline.
#[test]
fn a_panic_during_a_drain_still_stops_at_the_deadline() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let retry = Duration::from_secs(10);
    let replicator =
        Replicator::start_with(&site.db, store, retrying_every(retry), runtime).unwrap();
    wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    // The drain waits for the upload's retry, which panics; the rebuild's wait (another ten
    // seconds) ends after the stop's deadline.
    let stop = replicator.stop(Duration::from_secs(15));
    wait(&replicator, |h| h.phase == Phase::Draining);
    faulty.panic_always.store(true, Ordering::SeqCst);
    clock.advance(retry);
    wait(&replicator, |h| h.counters.panics == 1);
    clock.advance(Duration::from_secs(5));
    let stopped = ended(stop);
    assert!(
        matches!(stopped, Some(Stopped::DeadlineReached { .. })),
        "{stopped:?}"
    );
}

/// A database removed after a panic cannot be reopened; each wake tries again, and the stop
/// still ends at its deadline.
#[test]
fn a_database_removed_after_a_panic_still_stops_at_the_deadline() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    wait(&replicator, |h| h.counters.panics == 1);
    std::fs::remove_file(&site.db).unwrap();
    let stop = replicator.stop(Duration::from_secs(5));
    for _ in 0..20 {
        clock.advance(POLL);
    }
    let stopped = ended(stop);
    assert!(
        matches!(stopped, Some(Stopped::DeadlineReached { .. })),
        "{stopped:?}"
    );
}

/// An operator's snapshot request is kept for every fresh engine until one takes the
/// snapshot, even when the engine it was handed to panics before its boot.
#[test]
fn an_operator_snapshot_request_survives_a_panic_before_the_boot() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let first = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    assert!(matches!(stopped(&first, &clock), Stopped::Drained { .. }));
    drop(first);
    let faulty = site.faulty();
    faulty.panics.store(2, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    wait(&replicator, |h| h.counters.panics == 1);
    // Taken by the thread before the boot it wakes for, which panics too.
    replicator.request_snapshot();
    clock.advance(POLL);
    let again = wait(&replicator, |h| h.counters.panics == 2);
    assert_eq!(
        again.resnapshot,
        Some(SnapshotCause::OperatorRequested),
        "{again:?}"
    );
    clock.advance(Duration::from_secs(2));
    wait(&replicator, |h| h.phase == Phase::Running);
    site.commit(1);
    clock.advance(POLL);
    moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && h.resnapshot.is_none()
    });
    assert!(matches!(
        stopped(&replicator, &clock),
        Stopped::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A wall clock that steps back while the fresh engine waits to boot does not add the step to
/// the wait: the engine boots within minutes of clock, never the hour of the step. (The
/// engine's own retry wait runs at half speed until the clock catches up, so it takes up to
/// twice its length.)
#[test]
fn a_clock_step_back_does_not_lengthen_the_wait_before_a_rebuild() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let retry = Duration::from_secs(60);
    let replicator =
        Replicator::start_with(&site.db, store, retrying_every(retry), runtime).unwrap();
    wait(&replicator, |h| h.counters.panics == 1);
    clock.step_back(Duration::from_secs(3600));
    let ten = Duration::from_secs(10);
    moved_until(&replicator, &clock, (ten, 60), |h| {
        h.phase == Phase::Running
    });
    assert!(matches!(
        stopped(&replicator, &clock),
        Stopped::Drained { .. }
    ));
}

/// A task the dropped engine reported degraded is still reported while the fresh engine waits
/// to boot.
#[test]
fn a_degraded_task_is_still_reported_while_the_engine_is_rebuilt() {
    let site = Site::new();
    let faulty = site.faulty();
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    faulty.refuse.store(true, Ordering::SeqCst);
    site.commit(1);
    clock.advance(POLL);
    let degraded = wait(&replicator, |h| h.task(Task::Upload).is_some());
    assert_eq!(
        degraded.task(Task::Upload).map(|d| d.cause),
        Some(DegradedCause::StoreDenied(lsr_core::Denied::Credentials))
    );
    // The upload's retry panics.
    faulty.panic_always.store(true, Ordering::SeqCst);
    clock.release();
    let rebuilt = wait(&replicator, |h| h.counters.panics >= 1);
    assert_eq!(
        rebuilt.task(Task::Upload),
        degraded.task(Task::Upload),
        "{rebuilt:?}"
    );
    assert_eq!(
        rebuilt.task(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::Bug)
    );
    faulty.panic_always.store(false, Ordering::SeqCst);
    faulty.refuse.store(false, Ordering::SeqCst);
    clock.release();
    moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && !h.is_degraded()
    });
    assert!(matches!(
        stopped(&replicator, &clock),
        Stopped::Drained { .. }
    ));
}

/// Moves another database over the replicated one's path, journal files and all, as a restore in
/// place would.
fn replace(site: &Site) -> std::path::PathBuf {
    let other = site.path("other");
    let conn = Connection::open(&other).unwrap();
    conn.execute_batch("CREATE TABLE other(x)").unwrap();
    drop(conn);
    for sidecar in ["-wal", "-shm"] {
        let mut path = site.db.clone().into_os_string();
        path.push(sidecar);
        std::fs::remove_file(path).unwrap();
    }
    std::fs::rename(&other, &site.db).unwrap();
    site.db.clone()
}

fn has_replicator_tables(db: &std::path::Path) -> bool {
    let conn = Connection::open(db).unwrap();
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name LIKE '_litestream%'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .unwrap()
        > 0
}

/// The connections reopen by path after a panic; a different file at that path is the
/// database gone from under the replicator (`WalUnlinked`, at its configured response), never
/// a second database replicated into the same replica, and it is not written to.
#[test]
fn a_database_replaced_after_a_panic_is_degraded_and_not_reopened() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    wait(&replicator, |h| h.counters.panics == 1);
    let replaced = replace(&site);
    clock.advance(POLL);
    let health = wait(&replicator, |h| {
        h.phase == Phase::Running
            || h.task(Task::Sync).map(|d| d.cause) == Some(DegradedCause::WalUnlinked)
    });
    assert_eq!(
        health.task(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::WalUnlinked),
        "{health:?}"
    );
    assert!(!has_replicator_tables(&replaced));
    // It keeps trying, at the doubling wait, and keeps saying why.
    clock.advance(Duration::from_secs(2));
    let later = wait(&replicator, |h| {
        h.task(Task::Sync).is_some_and(|d| d.retries >= 2)
    });
    assert_eq!(
        later.task(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::WalUnlinked)
    );
    assert!(matches!(
        replicator.stop(Duration::ZERO).wait(),
        Stopped::DeadlineReached { .. }
    ));
}

/// A database deleted after the panic is gone as a replaced one is: not reopened, and reported
/// as the WAL unlinked, not retried as a failure to open.
#[test]
fn a_database_deleted_after_a_panic_is_degraded_and_not_reopened() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    wait(&replicator, |h| h.counters.panics == 1);
    std::fs::remove_file(&site.db).unwrap();
    clock.advance(POLL);
    let health = wait(&replicator, |h| {
        h.phase == Phase::Running
            || h.task(Task::Sync).map(|d| d.cause) == Some(DegradedCause::WalUnlinked)
    });
    assert_eq!(
        health.task(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::WalUnlinked),
        "{health:?}"
    );
    assert!(!site.db.exists(), "not reopened, which would create it");
    assert!(matches!(
        replicator.stop(Duration::ZERO).wait(),
        Stopped::DeadlineReached { .. }
    ));
}

/// Under `wal_unlinked: Stop` the replaced database stops the replicator with that cause.
#[test]
fn a_database_replaced_after_a_panic_stops_when_configured_to() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let stopping = Config {
        responses: Responses {
            wal_unlinked: Response::Stop,
            ..Responses::GO_LIKE
        },
        ..config()
    };
    let replicator = Replicator::start_with(&site.db, store, stopping, runtime).unwrap();
    wait(&replicator, |h| h.counters.panics == 1);
    let replaced = replace(&site);
    clock.advance(POLL);
    let health = wait(&replicator, |h| {
        matches!(h.phase, Phase::Stopped(_) | Phase::Running)
    });
    assert_eq!(health.phase, Phase::Stopped(StopCause::WalUnlinked));
    assert!(!has_replicator_tables(&replaced));
    assert_eq!(
        stopped(&replicator, &clock),
        Stopped::Stopped(StopCause::WalUnlinked)
    );
}

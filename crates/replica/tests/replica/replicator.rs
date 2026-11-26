//! The public API end to end: a real SQLite database (`lsr_sqlite`), the application's own
//! connection writing to it, and a file replica. Each test ends, where the state allows, in a
//! restore of the replica that equals the database.
//!
//! The deterministic tests run on `Runtime::fake`, whose clock moves only when the test says
//! so: the thread polls the WAL when the test advances the clock by the poll interval, and a
//! stop with a zero wait meets a thread that has not polled since the last commit. Every wait
//! is on a condition of the health snapshot, never on the clock.

use crate::common;

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{
    PATIENCE, Site, assert_same, booted, config, fake, moved_until, stopped,
    stopped_on_system_clock, truncate_is_busy, wait,
};
use lsr_core::{CheckpointMode, Event as EngineEvent, Fault};
use lsr_replica::StartOptions;
use lsr_replica::{
    AppCheckpoint, AppCheckpoints, AppNotice, ChannelSink, CheckpointConfig, Config, DegradedCause,
    Depth, DynStore, Event, EventSink, Gone, IfMissing, Phase, ReaderTracking, Readiness,
    Replicator, Responses, SetupError, StartError, StopCause, StopOutcome, Task,
    restore_if_missing, verify,
};
use rusqlite::Connection;

const POLL: Duration = Duration::from_secs(1);

fn caught_up(health: &lsr_replica::Health) -> bool {
    health.synced.is_some()
        && health.synced.map(|(txid, _)| txid) == health.replicated.map(|(txid, _)| txid)
        && health.unreplicated_since.is_none()
}

/// `start` returns with the read transaction held, so a checkpoint from outside cannot
/// truncate the WAL; a stop after commits drains them all, and a restore equals the database.
/// Afterwards the replicator's connections are closed: nothing holds the WAL.
#[test]
fn start_returns_with_the_read_lock_held_and_stop_drains_every_commit() {
    let site = Site::new();
    let store = site.store();
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert!(truncate_is_busy(&site.db), "the read lock holds the WAL");
    site.commit(5);
    let stopped = stopped_on_system_clock(&replicator);
    let StopOutcome::Drained { txid: Some(txid) } = stopped else {
        panic!("{stopped:?}");
    };
    assert_eq!(
        replicator.health().replicated.map(|(txid, _)| txid),
        Some(txid)
    );
    assert_eq!(
        replicator.health().phase,
        Phase::Stopped(StopCause::Shutdown)
    );
    site.assert_restores(&*store);
    assert!(!truncate_is_busy(&site.db), "the connections are closed");
}

/// A stop asked for before the first boot drains without the clock moving: the boot, the
/// sync and the upload each run at once, and the tick after the last upload reports the drain
/// instead of waiting for the next poll.
#[test]
fn a_stop_before_the_first_boot_drains_without_waiting_for_a_poll() {
    for _ in 0..10 {
        let site = Site::new();
        // A staged file the replica does not hold, and a commit after it.
        let faulty = site.faulty();
        faulty.fail_uploads.store(true, Ordering::SeqCst);
        let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
        let (runtime, _clock) = fake();
        let first = Replicator::start(
            &site.db,
            store,
            StartOptions {
                config: config(),
                runtime: Some(runtime),
                ..StartOptions::default()
            },
        )
        .unwrap();
        wait(&first, |h| h.synced.is_some() && h.counters.retries > 0);
        assert!(matches!(
            first.stop(Duration::ZERO).wait(),
            StopOutcome::DeadlineReached { .. }
        ));
        drop(first);
        site.commit(2);
        let (runtime, clock) = fake();
        let replicator = Replicator::start(
            &site.db,
            site.store(),
            StartOptions {
                config: config(),
                runtime: Some(runtime),
                ..StartOptions::default()
            },
        )
        .unwrap();
        let stopped = stopped(&replicator, &clock);
        assert!(
            matches!(stopped, StopOutcome::Drained { txid: Some(_) }),
            "{stopped:?}"
        );
        site.assert_restores(&*site.store());
    }
}

/// The awaited form of a stop needs no runtime: a plain executor drives it. The fake clock
/// is released: no test step moves it, so a drain that has to wait (a read busy under the
/// test's own commit, a failed upload) would otherwise wait for ever.
#[test]
fn a_stop_can_be_awaited() {
    let site = Site::new();
    let store = site.store();
    let (runtime, clock) = fake();
    clock.release();
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    site.commit(2);
    let stopped = lsr_core::conformance::block_on(replicator.stop(PATIENCE));
    assert!(
        matches!(stopped, StopOutcome::Drained { txid: Some(_) }),
        "{stopped:?}"
    );
    site.assert_restores(&*store);
}

/// A stop asked to end with a snapshot writes one level-9 file of the whole database after the
/// last upload, and the health says so.
#[test]
fn a_stop_can_end_with_a_level_nine_snapshot() {
    let site = Site::new();
    let store = site.store();
    let (runtime, clock) = fake();
    clock.release();
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    site.commit(2);
    replicator.request_final_snapshot();
    let stopped = lsr_core::conformance::block_on(replicator.stop(PATIENCE));
    let StopOutcome::Drained { txid: Some(txid) } = stopped else {
        panic!("{stopped:?}");
    };
    let health = replicator.health();
    let lsr_replica::FinalSnapshot::Written(info) = health.final_snapshot else {
        panic!("{:?}", health.final_snapshot);
    };
    assert_eq!(info.key.level(), lsr_core::Level::SNAPSHOT);
    assert_eq!(info.key.range().max(), txid);
    site.assert_restores(&*store);
}

/// The same stop while the engine waits out a failed upload: the drain's retry needs the
/// clock to move, and on a held fake clock it never does (this hung `just check` under load,
/// where the engine's first reads met the test's commit).
#[test]
fn a_stop_awaited_during_a_retry_wait_drains() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    clock.release();
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    site.commit(2);
    let retried = replicator.wait_until(PATIENCE, |health| health.counters.retries > 0);
    assert!(retried.is_some_and(|health| health.counters.retries > 0));
    faulty.fail_uploads.store(false, Ordering::SeqCst);
    let stopped = lsr_core::conformance::block_on(replicator.stop(PATIENCE));
    assert!(
        matches!(stopped, StopOutcome::Drained { txid: Some(_) }),
        "{stopped:?}"
    );
    site.assert_restores(&*store);
}

#[test]
fn wait_booted_reports_the_first_boot() {
    let site = Site::new();
    let store = site.store();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(booted(&replicator), Readiness::Ready);
    let health = wait(&replicator, caught_up);
    assert_eq!(health.phase, Phase::Running);
    assert!(!health.is_degraded());
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*store);
}

/// A sink given at the start sees the first boot's event, here the recovery of the position
/// an earlier replicator left. A sink set after the start sees it only if the thread takes the
/// sink before it boots: the daemon set its log sink that way and lost the boot's log line in
/// about one boot in twenty. Waiting for the boot before setting the sink makes that loss
/// certain, which is what the first half shows.
#[test]
fn a_sink_given_at_start_sees_the_first_boot() {
    let site = Site::new();
    let store = site.store();
    let first = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            ..StartOptions::default()
        },
    )
    .unwrap();
    site.commit(2);
    assert!(matches!(
        stopped_on_system_clock(&first),
        StopOutcome::Drained { txid: Some(_) }
    ));
    drop(first);
    let recovered = Event::Engine(EngineEvent::Booted { cause: None });

    let (runtime, clock) = fake();
    let late = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(booted(&late), Readiness::Ready);
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    late.set_events(Box::new(sink));
    assert!(matches!(
        stopped(&late, &clock),
        StopOutcome::Drained { .. }
    ));
    drop(late);
    let seen = events.drain();
    assert!(!seen.contains(&recovered), "{seen:?}");

    let (runtime, clock) = fake();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            events: Some(Box::new(sink)),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(booted(&replicator), Readiness::Ready);
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    let seen = events.drain();
    // The write gate's events come first: writes are held from the start until the boot.
    let first = seen.iter().find(|event| matches!(event, Event::Engine(_)));
    assert_eq!(first, Some(&recovered), "{seen:?}");
    assert_eq!(
        seen.first(),
        Some(&Event::WritesBlocked {
            why: lsr_replica::WhyBlocked::Booting
        }),
        "{seen:?}"
    );
    assert_eq!(events.dropped(), 0);
    site.assert_restores(&*store);
}

/// A boot the store refuses is degraded and keeps trying (the default responses); once the
/// store takes the credentials again, the next retry boots.
#[test]
fn a_refused_boot_is_degraded_and_recovers() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.refuse.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        booted(&replicator),
        Readiness::Degraded(DegradedCause::StoreDenied(lsr_core::Denied::Credentials))
    );
    assert_eq!(
        replicator.health().task(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::StoreDenied(lsr_core::Denied::Credentials))
    );
    faulty.refuse.store(false, Ordering::SeqCst);
    clock.advance(POLL);
    wait(&replicator, |h| {
        h.phase == Phase::Running && !h.is_degraded()
    });
    assert_eq!(booted(&replicator), Readiness::Ready);
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A replicator whose first boot the store refuses has replicated nothing: its lag counts
/// from the start, never zero, so an alert on the lag fires while the store is unreachable.
#[test]
fn the_lag_counts_from_the_start_while_the_first_boot_fails() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.refuse.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let started = clock.now();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let refused = wait(&replicator, |h| h.task(Task::Sync).is_some());
    assert_eq!(refused.phase, Phase::Booting, "{refused:?}");
    assert_eq!(refused.unreplicated_since, Some(started), "{refused:?}");
    faulty.refuse.store(false, Ordering::SeqCst);
    clock.advance(POLL);
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
}

/// Go's `SyncStatus`: what is staged locally and what the replica holds, before the replica
/// has anything, after a local sync whose upload failed, after the upload, and after new
/// writes until they too are on the replica.
#[test]
fn health_compares_the_local_and_replica_positions() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    // A local sync only: staged, not on the replica, and the lag is counted from the start.
    let local = wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    assert_eq!(local.replicated, None);
    assert!(local.unreplicated_since.is_some());
    // The upload goes through at the retry.
    faulty.fail_uploads.store(false, Ordering::SeqCst);
    clock.advance(POLL);
    let full = moved_until(&replicator, &clock, (POLL, 240), caught_up);
    // The sizes are the files' own, read while the thread is parked.
    let parked = crate::waiting::seen_or(&replicator, &clock, &|_| false).unwrap();
    assert!(parked.is_none());
    let health = replicator.health();
    let wal = site.db.with_file_name("app.db-wal");
    assert_eq!(health.db_bytes, std::fs::metadata(&site.db).unwrap().len());
    assert_eq!(health.wal_bytes, std::fs::metadata(&wal).unwrap().len());
    assert!(full.wal_bytes > 0 && full.db_bytes > 0, "{full:?}");
    // New writes: seen at the next poll, then replicated.
    site.commit(3);
    clock.advance(POLL);
    let after = moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && h.synced.map(|(txid, _)| txid) > full.synced.map(|(txid, _)| txid)
    });
    assert!(after.counters.uploads > full.counters.uploads);
    assert!(after.counters.store.bytes_uploaded > full.counters.store.bytes_uploaded);
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A stop whose deadline has already passed, with commits not yet synced and a staged file
/// not yet uploaded, ends at once with both counts (Go's `Close` with a zero timeout makes no
/// retry). It still closes the connections, and the next start continues from what is staged.
#[test]
fn a_zero_deadline_reports_what_was_left_and_the_next_start_continues() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    let uploads = replicator.health().counters.store.begin_upload.calls;
    site.commit(3);
    let cut_off = replicator.stop(Duration::ZERO).wait();
    let StopOutcome::DeadlineReached {
        unsynced_wal_bytes,
        staged_not_uploaded,
    } = cut_off
    else {
        panic!("{cut_off:?}");
    };
    assert!(unsynced_wal_bytes > 0, "{cut_off:?}");
    assert!(staged_not_uploaded > 0, "{cut_off:?}");
    assert_eq!(
        replicator.health().counters.store.begin_upload.calls,
        uploads,
        "no retry"
    );
    assert!(!truncate_is_busy(&site.db), "the connections are closed");
    // That checkpoint emptied the WAL; a commit gives SQLite a new one to replicate from.
    site.commit(1);
    drop(replicator);
    // A start whose first boot panics has no engine to measure, yet counts what staging
    // holds past the replica, which is every staged file.
    faulty.panic_always.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let panicked = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    wait(&panicked, |h| h.counters.panics > 0);
    let again = panicked.stop(Duration::ZERO).wait();
    assert!(
        matches!(again, StopOutcome::DeadlineReached { staged_not_uploaded: n, .. } if n == staged_not_uploaded),
        "{again:?}"
    );
    drop(panicked);
    faulty.panic_always.store(false, Ordering::SeqCst);
    let (runtime, clock) = fake();
    let next = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(booted(&next), Readiness::Ready);
    assert!(matches!(
        stopped(&next, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A staged file damaged before the first upload ever landed (a power loss can tear one): the
/// next start reads it twice, drops it with every file after it, and boots from the database
/// with a snapshot. The replica is empty, so staging vouched for nothing on it and no takeover
/// record is needed. Built with optimization, rustc 1.95 to at least 1.102 compiled the boot's
/// gate so that the empty replica read as transaction zero: the engine asked for a takeover
/// through zero on every attempt and never booted. `just test-opt` runs this test optimized.
#[test]
fn a_staged_file_damaged_before_the_first_upload_is_dropped_and_the_next_start_boots() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    let cut_off = replicator.stop(Duration::ZERO).wait();
    assert!(
        matches!(cut_off, StopOutcome::DeadlineReached { staged_not_uploaded, .. } if staged_not_uploaded > 0),
        "{cut_off:?}"
    );
    drop(replicator);
    let meta = config().meta_dir_for(&site.db).unwrap();
    let newest = newest_staged(&meta);
    let mut bytes = std::fs::read(&newest).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&newest, bytes).unwrap();
    site.commit(1);
    let (runtime, clock) = fake();
    let next = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    // The first read of the damaged file only retries the boot (one read never condemns a
    // file); the clock moves through that wait, and through the doubling waits a boot that asks
    // for a takeover it never gets would take.
    moved_until(&next, &clock, (POLL, 120), |h| h.phase != Phase::Booting);
    assert_eq!(next.wait_booted(PATIENCE), Readiness::Ready);
    assert!(matches!(
        stopped(&next, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// The staged LTX file with the highest name under `meta`: names sort by transaction.
fn newest_staged(meta: &std::path::Path) -> std::path::PathBuf {
    let mut found = Vec::new();
    let mut dirs = vec![meta.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|ext| ext == "ltx") {
                found.push(path);
            }
        }
    }
    found.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    found.pop().expect("a staged file")
}

/// A second stop shortens the first: a drain that waits on a failing upload ends at the
/// earlier deadline, and both stops report the same end.
#[test]
fn a_second_stop_keeps_the_earlier_deadline() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    let first = replicator.stop(Duration::from_secs(60));
    wait(&replicator, |h| h.phase == Phase::Draining);
    let second = replicator.stop(Duration::ZERO).wait();
    assert!(
        matches!(second, StopOutcome::DeadlineReached { .. }),
        "{second:?}"
    );
    assert_eq!(first.wait(), second);
}

/// Dropping one clone does nothing; dropping the last drains in the background with no wait
/// on the caller, closes the connections, and a start on the same file waits for it and then
/// continues the same replica.
#[test]
fn dropping_the_last_handle_drains_and_closes() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let before = wait(&replicator, caught_up).replicated;
    let clone = replicator.clone();
    drop(replicator);
    site.commit(2);
    assert_eq!(clone.health().phase, Phase::Running);
    // The replicator may be mid-read when the application commits and wait out a retry on the
    // fake clock; moving it until the commits are replicated leaves the drop nothing to retry.
    moved_until(&clone, &clock, (POLL, 240), |h| h.replicated != before);
    drop(clone);
    let (runtime, clock) = fake();
    let next = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let health = wait(&next, caught_up);
    assert_eq!(health.resnapshot, None);
    assert!(matches!(
        stopped(&next, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// Under the strict responses a refusing store stops the replicator: health, the boot wait and
/// the stop all name the cause.
#[test]
fn a_stopped_replicator_reports_its_cause() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.refuse.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let strict = Config {
        responses: Responses::STRICT,
        ..config()
    };
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: strict,
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        booted(&replicator),
        Readiness::Stopped(StopCause::StoreDenied(lsr_core::Denied::Credentials))
    );
    assert_eq!(
        replicator.health().phase,
        Phase::Stopped(StopCause::StoreDenied(lsr_core::Denied::Credentials))
    );
    assert_eq!(
        stopped(&replicator, &clock),
        StopOutcome::Stopped(StopCause::StoreDenied(lsr_core::Denied::Credentials))
    );
}

/// A database page changed in the file without a WAL frame degrades syncing; the operator's
/// acceptance of the file as it is ends the degradation.
#[test]
fn accepting_the_database_pages_ends_a_page_mismatch() {
    use std::os::unix::fs::FileExt as _;
    let site = Site::new();
    site.commit(20);
    site.app
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
        .unwrap();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    wait(&replicator, caught_up);
    // Page 3 lives in the database file alone: the WAL is empty since the checkpoint.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&site.db)
        .unwrap();
    file.write_all_at(&[0xEE; 4096], 2 * 4096).unwrap();
    replicator.request_snapshot();
    let mismatched = |h: &lsr_replica::Health| {
        h.task(Task::Sync)
            .is_some_and(|d| matches!(d.cause, DegradedCause::DatabasePageMismatch { .. }))
    };
    wait(&replicator, mismatched);
    replicator.accept_database_pages();
    replicator.request_snapshot();
    wait(&replicator, |h| h.task(Task::Sync).is_none());
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
}

/// An operator's snapshot is staged at once, with no new commit, and reported as an event.
#[test]
fn request_snapshot_stages_a_snapshot_and_events_report_it() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    replicator.set_events(Box::new(sink));
    let before = wait(&replicator, caught_up);
    replicator.request_snapshot();
    wait(&replicator, |h| {
        caught_up(h) && h.counters.syncs > before.counters.syncs
    });
    let seen = events.drain();
    assert!(
        seen.contains(&Event::Engine(EngineEvent::Synced { files: 1 })),
        "{seen:?}"
    );
    assert_eq!(events.dropped(), 0);
    // An operator's acceptance of the database pages changes nothing on a healthy database.
    replicator.accept_database_pages();
    let health = wait(&replicator, caught_up);
    assert_eq!(health.task(Task::Sync), None);
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

fn checkpoints(events: &[Event], mode: CheckpointMode) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(event, Event::Engine(EngineEvent::Checkpointed { mode: m, .. }) if *m == mode)
        })
        .count()
}

/// Under tracked readers no TRUNCATE runs while a guard lives: it is reported busy and waits
/// its backoff, poll after poll. Once the guard is gone, replication drains as usual.
#[test]
fn a_reader_guard_holds_off_truncate() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let tracked = Config {
        readers: ReaderTracking::Tracked,
        checkpoint: CheckpointConfig {
            interval: None,
            min_pages: None,
            truncate_pages: NonZeroU32::new(4),
        },
        ..config()
    };
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: tracked,
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(1024).unwrap());
    replicator.set_events(Box::new(sink));
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    // An application reader on an old snapshot keeps PASSIVE from emptying the WAL, so the policy
    // goes on to TRUNCATE.
    let reader = Connection::open(&site.db).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _rows: i64 = reader
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .unwrap();
    let guard = replicator.long_reader();
    site.commit(10);
    let mut seen = Vec::new();
    for _ in 0..5 {
        clock.advance(POLL);
        let now = clock.now();
        wait(&replicator, |h| h.reported_at >= now);
        seen.extend(events.drain());
    }
    let busy = seen.iter().any(|event| {
        matches!(
            event,
            Event::Engine(EngineEvent::Retrying {
                fault: Fault::ReadersHeld,
                ..
            })
        )
    });
    assert!(busy, "{seen:?}");
    assert_eq!(checkpoints(&seen, CheckpointMode::Truncate), 0, "{seen:?}");
    drop(guard);
    reader.execute_batch("COMMIT").unwrap();
    // With the guard gone the held-off TRUNCATE runs, and the write lock its boundary
    // snapshot took is released: the application commits again at once.
    let mut truncates = 0;
    for _ in 0..5 {
        clock.advance(POLL);
        let now = clock.now();
        wait(&replicator, |h| h.reported_at >= now);
        truncates += checkpoints(&events.drain(), CheckpointMode::Truncate);
    }
    assert!(truncates > 0, "no TRUNCATE once the guard was dropped");
    site.commit(1);
    // The drain may have a retry's backoff to wait out (the TRUNCATE the guard held off), and
    // this clock moves only when the test moves it.
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

#[test]
fn app_connection_sets_up_only_this_database() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let pooled = Connection::open(&site.db).unwrap();
    replicator.app_connection(&pooled).unwrap();
    // Writes are held until the boot is judged, and a connection set up fails at once on them.
    assert_eq!(booted(&replicator), Readiness::Ready);
    lsr_sqlite::check_app_connection(&pooled).unwrap();
    let other = Site::new();
    assert!(matches!(
        replicator.app_connection(&other.app),
        Err(SetupError::OtherDatabase)
    ));
    assert!(matches!(
        replicator.app_connection(&Connection::open_in_memory().unwrap()),
        Err(SetupError::NoFile)
    ));
    pooled
        .execute("INSERT INTO t(v) VALUES (x'00')", [])
        .unwrap();
    // The insert may meet the replicator's read: the drain retries, so the clock must move.
    let stopped = stopped(&replicator, &clock);
    assert!(
        matches!(stopped, StopOutcome::Drained { .. }),
        "{stopped:?}"
    );
    site.assert_restores(&*site.store());
}

/// Restore and verify run over the same store the replicator used. The drain may meet a
/// busy database (the application's commits race the replicator's reads) and retry, so the
/// clock moves while the stop is pending.
#[test]
fn verify_and_restore_if_missing_use_the_shared_store() {
    let site = Site::new();
    let store = site.store();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        Arc::clone(&store),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    site.commit(4);
    let stopped = stopped(&replicator, &clock);
    assert!(
        matches!(stopped, StopOutcome::Drained { .. }),
        "{stopped:?}"
    );
    let report = verify(&*store, Depth::Contents).unwrap();
    assert_eq!(report.worst(), None, "{report:?}");
    assert_eq!(
        restore_if_missing(&*store, &site.db, &Config::default()).unwrap(),
        IfMissing::Exists
    );
    let to = site.path("if-missing");
    assert!(matches!(
        restore_if_missing(&*store, &to, &Config::default()).unwrap(),
        IfMissing::Restored(_)
    ));
    assert_same(&site.app, &to);
    // A target that cannot be looked at is not missing: here a path under a regular file.
    let under_a_file = to.join("db");
    let refused = restore_if_missing(&*store, &under_a_file, &Config::default());
    assert!(
        matches!(
            refused,
            Err(lsr_replica::RestoreError::Destination(
                lsr_replica::local::CreateError::Inspect { .. }
            ))
        ),
        "{refused:?}"
    );
}

/// A panic inside a tick is caught: the engine is rebuilt after the wait and boots again,
/// and replication goes on with nothing lost.
#[test]
fn a_panic_rebuilds_the_engine_and_replication_continues() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let rebuilt = wait(&replicator, |h| h.counters.panics == 1);
    assert_eq!(
        rebuilt.task(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::Bug)
    );
    clock.advance(POLL);
    wait(&replicator, |h| {
        h.phase == Phase::Running && !h.is_degraded()
    });
    assert_eq!(booted(&replicator), Readiness::Ready);
    site.commit(2);
    clock.advance(POLL);
    let health = moved_until(&replicator, &clock, (POLL, 240), caught_up);
    assert!(!health.is_degraded(), "{health:?}");
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

#[test]
fn a_panic_under_strict_responses_stops_with_bug() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let strict = Config {
        responses: Responses::STRICT,
        ..config()
    };
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: strict,
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(booted(&replicator), Readiness::Stopped(StopCause::Bug));
    assert_eq!(
        stopped(&replicator, &clock),
        StopOutcome::Stopped(StopCause::Bug)
    );
}

/// A checkpoint the application ran behind the replicator's back is counted and reported.
#[test]
fn an_app_checkpoint_is_counted_and_reported() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    replicator.set_events(Box::new(sink));
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    site.app
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_row| Ok(()))
        .unwrap();
    site.commit(1);
    clock.advance(POLL);
    let health = moved_until(&replicator, &clock, (POLL, 240), |h| {
        h.app_checkpoints_ran == 1 && caught_up(h)
    });
    assert_eq!(health.app_checkpoints_ran, 1);
    assert_eq!(health.app_checkpoints.backfilled, 1);
    assert_eq!(
        health.app_notice(),
        Some(AppNotice::Checkpointing { ran: 1 })
    );
    assert!(!health.is_degraded(), "a notice, not a degraded state");
    // Reported once: later turns that see the same total say nothing more.
    for _ in 0..3 {
        clock.advance(POLL);
        let now = clock.now();
        wait(&replicator, |h| h.reported_at >= now);
    }
    let reports: Vec<_> = events
        .drain()
        .into_iter()
        .filter(|event| matches!(event, Event::AppCheckpoint { .. }))
        .collect();
    assert!(
        matches!(
            reports.as_slice(),
            [Event::AppCheckpoint {
                seen: AppCheckpoint::Backfilled { .. },
                total: 1
            }]
        ),
        "{reports:?}"
    );
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A history in which the WAL grows past SQLite's automatic-checkpoint threshold while the read
/// lock stands where no checkpoint can copy anything: one checkpoint of ours (by the interval)
/// restarts the log, and the frame-count trigger is out of reach, so six 200-row commits take
/// the new generation past 1000 frames with no checkpoint of ours between two reads. That is
/// what an application at the default threshold leaves when its attempts fail, and what one at
/// `wal_autocheckpoint = 0` leaves too; only the threshold the replicator is told tells them
/// apart (`lsr_core::app`). The health after the commits.
fn grown_on_read_mark_zero(
    app_autocheckpoint: Option<NonZeroU32>,
) -> (lsr_replica::Health, Vec<Event>) {
    let site = Site::new();
    let (runtime, clock) = fake();
    let mut config = config();
    config.app_autocheckpoint = app_autocheckpoint;
    config.checkpoint.min_pages = NonZeroU32::new(5000);
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config,
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    replicator.set_events(Box::new(sink));
    wait(&replicator, caught_up);
    site.commit(1);
    clock.advance(Duration::from_secs(61));
    let mut health = wait(&replicator, |h| caught_up(h) && h.counters.checkpoints >= 1);
    for _ in 0..6 {
        let before = health.synced.map(|(txid, _)| txid);
        site.commit(200);
        clock.advance(POLL);
        health = wait(&replicator, |h| {
            caught_up(h) && h.synced.map(|(txid, _)| txid) > before
        });
    }
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    (health, events.drain())
}

/// The application's connections keep `wal_autocheckpoint = 0` (`app_connection` sets it), and the
/// replicator is told so (`app_autocheckpoint: None`): however far the WAL grows past SQLite's
/// threshold before the replicator's own checkpoint, no application checkpoint is seen, none is
/// inferred, and no notice is raised.
///
/// This test used to state nothing, so the replicator assumed SQLite's default of 1000 frames,
/// and it flaked (CI job 137273518319: `stalled: 1, last: Some(Stalled { frames: 1205 })`).
/// After one checkpoint of ours restarted the log, the test could see the health of the sync
/// that took the WAL past 1000 frames and commit again before the checkpoint that sync
/// triggered ran; the next read then saw growth past the threshold with nothing copied, which
/// for an application at 1000 is a stalled attempt. The history above makes that order certain.
#[test]
fn an_app_at_wal_autocheckpoint_0_raises_no_notice() {
    let (health, seen) = grown_on_read_mark_zero(None);
    assert!(health.counters.checkpoints >= 1, "{health:?}");
    assert_eq!(health.app_checkpoints, AppCheckpoints::NONE);
    assert_eq!(health.app_notice(), None);
    assert!(
        !seen
            .iter()
            .any(|event| matches!(event, Event::AppCheckpoint { .. })),
        "{seen:?}"
    );
}

/// The same history with the replicator told the application runs SQLite's default threshold:
/// the growth is the stalled attempt it would be, and it is reported, once. The inference
/// the test above relies on being told off is reached.
#[test]
fn an_app_at_the_default_threshold_whose_wal_grows_on_read_mark_zero_is_reported_stalled() {
    let (health, _) = grown_on_read_mark_zero(Some(lsr_core::SQLITE_AUTOCHECKPOINT));
    assert_eq!(health.app_checkpoints.stalled, 1, "{health:?}");
    assert!(
        matches!(
            health.app_checkpoints.last,
            Some(lsr_core::AppCheckpoint::Stalled { .. })
        ),
        "{health:?}"
    );
    // Health, not the event stream: the event sink is bounded and drops events it is not
    // drained of, and how many sync events a run makes depends on its timing.
}

/// Go's idle replica monitor: with nothing to do, every poll still writes the health
/// snapshot (so its time stays current), and none of them stages or uploads anything.
#[test]
fn an_idle_replicator_keeps_its_health_current() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let settled = moved_until(&replicator, &clock, (POLL, 240), caught_up);
    for _ in 0..5 {
        clock.advance(POLL);
        let now = clock.now();
        let health = wait(&replicator, |h| h.reported_at >= now);
        assert_eq!(health.synced, settled.synced);
        assert_eq!(health.counters.uploads, settled.counters.uploads);
        assert_eq!(health.counters.syncs, settled.counters.syncs);
        assert_eq!(health.unreplicated_since, None);
    }
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
}

/// One replicator per file: a second start on the same file, even by another path, is
/// refused while the first runs.
#[test]
fn a_second_start_on_the_same_file_is_refused() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let linked = site.dir.path().join("linked.db");
    std::fs::hard_link(&site.db, &linked).unwrap();
    let (runtime, _clock) = fake();
    assert!(matches!(
        Replicator::start(
            &linked,
            site.store(),
            StartOptions {
                config: config(),
                runtime: Some(runtime),
                ..StartOptions::default()
            }
        ),
        Err(StartError::AlreadyReplicated)
    ));
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
}

#[test]
fn start_refuses_a_missing_database_and_an_unusable_meta_dir() {
    let site = Site::new();
    let missing = site.dir.path().join("missing.db");
    assert!(matches!(
        Replicator::start(
            &missing,
            site.store(),
            StartOptions {
                config: config(),
                ..StartOptions::default()
            }
        ),
        Err(StartError::Open {
            kind: std::io::ErrorKind::NotFound
        })
    ));
    // A path that names no file has no default meta directory: refused for what it is.
    let sub = site.dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    let no_name = Replicator::start(
        &sub.join(".."),
        site.store(),
        StartOptions {
            config: Config::default(),
            ..StartOptions::default()
        },
    );
    assert!(
        matches!(no_name, Err(StartError::NoFileName)),
        "{no_name:?}"
    );
    let file = site.dir.path().join("a-file");
    std::fs::write(&file, b"x").unwrap();
    let bad_meta = Config {
        meta_dir: Some(file.join("meta")),
        ..config()
    };
    assert!(matches!(
        Replicator::start(
            &site.db,
            site.store(),
            StartOptions {
                config: bad_meta,
                ..StartOptions::default()
            }
        ),
        Err(StartError::MetaDir { .. })
    ));
    // Nothing was left registered: a good start goes through.
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert!(matches!(
        stopped_on_system_clock(&replicator),
        StopOutcome::Drained { .. }
    ));
}

struct Panicking;

impl EventSink for Panicking {
    fn event(&mut self, _event: &Event) {
        panic!("an application sink that panics");
    }
}

/// A sink that panics costs its events, never the replication.
#[test]
fn a_panicking_sink_is_counted_and_replication_goes_on() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    replicator.set_events(Box::new(Panicking));
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    site.commit(2);
    clock.advance(POLL);
    let health = moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && h.counters.sink_panics > 0
    });
    assert_eq!(health.phase, Phase::Running);
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A panic payload that panics again when dropped escapes the thread's own catch: the thread
/// ends, and every handle reports it gone rather than hanging.
struct Bomb;

impl Drop for Bomb {
    fn drop(&mut self) {
        panic!("the panic payload's drop panics");
    }
}

struct Throws;

impl EventSink for Throws {
    fn event(&mut self, _event: &Event) {
        std::panic::panic_any(Bomb);
    }
}

#[test]
fn a_thread_that_dies_is_reported_gone() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    replicator.set_events(Box::new(Throws));
    site.commit(1);
    clock.advance(POLL);
    let health = wait(&replicator, |h| h.phase == Phase::Gone(Gone::ThreadDied));
    assert_eq!(health.phase, Phase::Gone(Gone::ThreadDied));
    // The file is free for the next replicator at once, without a stop of the dead one.
    let (runtime, next_clock) = fake();
    let next = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        stopped(&replicator, &clock),
        StopOutcome::Gone(Gone::ThreadDied)
    );
    assert_eq!(booted(&replicator), Readiness::Gone(Gone::ThreadDied));
    assert!(matches!(
        stopped(&next, &next_clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A sink whose drop panics: the thread drops it with the engine, after the loop and its exit
/// guard are done.
struct ExplodesOnDrop;

impl EventSink for ExplodesOnDrop {
    fn event(&mut self, _event: &Event) {}
}

impl Drop for ExplodesOnDrop {
    fn drop(&mut self) {
        panic!("the sink's drop panics");
    }
}

/// A thread that dies after its loop ended, with no outcome published, is still seen gone by
/// every handle, and the next replicator of the file takes its place.
#[test]
fn a_thread_that_dies_without_an_outcome_is_reported_gone() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    replicator.set_events(Box::new(ExplodesOnDrop));
    site.commit(1);
    moved_until(&replicator, &clock, (POLL, 240), caught_up);
    assert_eq!(
        stopped(&replicator, &clock),
        StopOutcome::Gone(Gone::ThreadDied)
    );
    assert_eq!(replicator.health().phase, Phase::Gone(Gone::ThreadDied));
    let (runtime, clock) = fake();
    let next = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    assert!(matches!(
        stopped(&next, &clock),
        StopOutcome::Drained { .. }
    ));
    site.assert_restores(&*site.store());
}

/// A drain that meets a retry waits out its backoff on the replicator's clock: on a fake
/// clock it ends once the clock reaches the retry, not before. A test that stops a
/// replicator whose drain may retry moves the clock while it waits ([`stopped`]).
#[test]
fn a_drain_that_waits_out_a_retry_ends_once_the_clock_reaches_it() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    site.commit(4);
    // The first sync can meet a busy database (a commit mid-way through the wal-index header)
    // and wait out a backoff before it stages, so the clock moves while the test waits for the
    // failed upload.
    moved_until(&replicator, &clock, (POLL, 240), |h| {
        h.synced.is_some() && h.counters.store.begin_upload.errors > 0
    });
    faulty.fail_uploads.store(false, Ordering::SeqCst);
    let stopped = stopped(&replicator, &clock);
    assert!(
        matches!(stopped, StopOutcome::Drained { txid: Some(_) }),
        "{stopped:?}"
    );
    site.assert_restores(&*site.store());
}

/// A failed upload leaves the engine waiting out a retry's backoff on the fake clock. A test
/// that only watches health after it never sees the commit replicated; `moved_until` moves the
/// clock while the thread waits on it and does. The condition names the commit's transaction:
/// "caught up" alone holds on the health from before the commit.
#[test]
fn a_retry_s_backoff_is_waited_out_by_moving_the_clock() {
    let site = Site::new();
    let faulty = site.faulty();
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start(
        &site.db,
        store,
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let start = moved_until(&replicator, &clock, (POLL, 240), caught_up);
    let before = start.replicated.map(|(txid, _)| txid);
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    site.commit(1);
    clock.advance(POLL);
    // Two failures: the first retry may already be due when the fault clears, the next one
    // (two seconds of backoff) is not.
    let failing = moved_until(&replicator, &clock, (POLL, 240), |h| {
        h.counters.retries >= start.counters.retries + 2 && h.synced.map(|(txid, _)| txid) > before
    });
    assert!(
        failing.replicated.map(|(txid, _)| txid) == before,
        "{failing:?}"
    );
    faulty.fail_uploads.store(false, Ordering::SeqCst);
    let health = moved_until(&replicator, &clock, (POLL, 240), |h| {
        caught_up(h) && h.replicated.map(|(txid, _)| txid) > before
    });
    assert!(
        health.counters.uploads > start.counters.uploads,
        "{health:?}"
    );
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
}

/// A replicator prints its database and state, and a runtime its clock: what a panic message
/// or a log of either shows.
#[test]
fn a_replicator_and_its_runtime_print_what_they_hold() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let printed_runtime = format!("{runtime:?}");
    assert!(
        printed_runtime.starts_with("Runtime { now: "),
        "{printed_runtime}"
    );
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: config(),
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let printed = format!("{replicator:?}");
    assert!(
        printed.starts_with("Replicator { shared: Shared { database: "),
        "{printed}"
    );
    assert!(printed.contains("state: "), "{printed}");
    assert!(matches!(
        stopped(&replicator, &clock),
        StopOutcome::Drained { .. }
    ));
}

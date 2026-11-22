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
    PATIENCE, Site, assert_same, booted, config, fake, stopped, stopped_moving, truncate_is_busy,
    wait,
};
use lsr_core::{CheckpointMode, Event as EngineEvent, Fault};
use lsr_replica::{
    AppCheckpoint, AppCheckpoints, AppNotice, Booted, ChannelSink, CheckpointConfig, Config,
    DegradedCause, Depth, DynStore, Event, EventSink, Gone, Phase, ReaderTracking, Replicator,
    Responses, SetupError, StartError, StopCause, Stopped, Task, restore_if_missing, verify,
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
    let replicator = Replicator::start(&site.db, Arc::clone(&store), config()).unwrap();
    assert!(truncate_is_busy(&site.db), "the read lock holds the WAL");
    site.commit(5);
    let stopped = stopped(&replicator);
    let Stopped::Drained { txid: Some(txid) } = stopped else {
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
        let first = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
        wait(&first, |h| h.synced.is_some() && h.counters.retries > 0);
        assert!(matches!(
            first.stop(Duration::ZERO).wait(),
            Stopped::DeadlineReached { .. }
        ));
        drop(first);
        site.commit(2);
        let (runtime, _clock) = fake();
        let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
        let stopped = stopped(&replicator);
        assert!(
            matches!(stopped, Stopped::Drained { txid: Some(_) }),
            "{stopped:?}"
        );
        site.assert_restores(&*site.store());
    }
}

/// The awaited form of a stop needs no runtime: a plain executor drives it.
#[test]
fn a_stop_can_be_awaited() {
    let site = Site::new();
    let store = site.store();
    let (runtime, _clock) = fake();
    let replicator =
        Replicator::start_with(&site.db, Arc::clone(&store), config(), runtime).unwrap();
    site.commit(2);
    let stopped = lsr_core::conformance::block_on(replicator.stop(PATIENCE));
    assert!(
        matches!(stopped, Stopped::Drained { txid: Some(_) }),
        "{stopped:?}"
    );
    site.assert_restores(&*store);
}

#[test]
fn wait_booted_reports_the_first_boot() {
    let site = Site::new();
    let store = site.store();
    let (runtime, _clock) = fake();
    let replicator =
        Replicator::start_with(&site.db, Arc::clone(&store), config(), runtime).unwrap();
    assert_eq!(booted(&replicator), Booted::Booted);
    let health = wait(&replicator, caught_up);
    assert_eq!(health.phase, Phase::Running);
    assert!(!health.is_degraded());
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let first = Replicator::start(&site.db, Arc::clone(&store), config()).unwrap();
    site.commit(2);
    assert!(matches!(
        stopped(&first),
        Stopped::Drained { txid: Some(_) }
    ));
    drop(first);
    let recovered = Event::Engine(EngineEvent::Booted { cause: None });

    let (runtime, _clock) = fake();
    let late = Replicator::start_with(&site.db, Arc::clone(&store), config(), runtime).unwrap();
    assert_eq!(booted(&late), Booted::Booted);
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    late.set_events(Box::new(sink));
    assert!(matches!(stopped(&late), Stopped::Drained { .. }));
    drop(late);
    let seen = events.drain();
    assert!(!seen.contains(&recovered), "{seen:?}");

    let (runtime, _clock) = fake();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    let replicator = Replicator::start_with_events(
        &site.db,
        Arc::clone(&store),
        config(),
        runtime,
        Box::new(sink),
    )
    .unwrap();
    assert_eq!(booted(&replicator), Booted::Booted);
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
    let seen = events.drain();
    assert_eq!(seen.first(), Some(&recovered), "{seen:?}");
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
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    assert_eq!(
        booted(&replicator),
        Booted::Degraded(DegradedCause::StoreDenied(lsr_core::Denied::Credentials))
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
    assert_eq!(booted(&replicator), Booted::Booted);
    wait(&replicator, caught_up);
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    let refused = wait(&replicator, |h| h.task(Task::Sync).is_some());
    assert_eq!(refused.phase, Phase::Booting, "{refused:?}");
    assert_eq!(refused.unreplicated_since, Some(started), "{refused:?}");
    faulty.refuse.store(false, Ordering::SeqCst);
    clock.advance(POLL);
    wait(&replicator, caught_up);
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    // A local sync only: staged, not on the replica, and the lag is counted from the start.
    let local = wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    assert_eq!(local.replicated, None);
    assert!(local.unreplicated_since.is_some());
    // The upload goes through at the retry.
    faulty.fail_uploads.store(false, Ordering::SeqCst);
    clock.advance(POLL);
    let full = wait(&replicator, caught_up);
    assert!(full.wal_bytes > 0 && full.db_bytes > 0, "{full:?}");
    // New writes: seen at the next poll, then replicated.
    site.commit(3);
    clock.advance(POLL);
    let after = wait(&replicator, |h| {
        caught_up(h) && h.synced.map(|(txid, _)| txid) > full.synced.map(|(txid, _)| txid)
    });
    assert!(after.counters.uploads > full.counters.uploads);
    assert!(after.counters.store.bytes_uploaded > full.counters.store.bytes_uploaded);
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    let uploads = replicator.health().counters.store.begin_upload.calls;
    site.commit(3);
    let cut_off = replicator.stop(Duration::ZERO).wait();
    let Stopped::DeadlineReached {
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
    let (runtime, _clock) = fake();
    let next = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    assert_eq!(booted(&next), Booted::Booted);
    assert!(matches!(stopped(&next), Stopped::Drained { .. }));
    site.assert_restores(&*site.store());
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
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    let first = replicator.stop(Duration::from_secs(60));
    wait(&replicator, |h| h.phase == Phase::Draining);
    let second = replicator.stop(Duration::ZERO).wait();
    assert!(
        matches!(second, Stopped::DeadlineReached { .. }),
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
    let (runtime, _clock) = fake();
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    wait(&replicator, caught_up);
    let clone = replicator.clone();
    drop(replicator);
    site.commit(2);
    assert_eq!(clone.health().phase, Phase::Running);
    drop(clone);
    let (runtime, _clock) = fake();
    let next = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    let health = wait(&next, caught_up);
    assert_eq!(health.resnapshot, None);
    assert!(matches!(stopped(&next), Stopped::Drained { .. }));
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
    let (runtime, _clock) = fake();
    let strict = Config {
        responses: Responses::STRICT,
        ..config()
    };
    let replicator = Replicator::start_with(&site.db, store, strict, runtime).unwrap();
    assert_eq!(
        booted(&replicator),
        Booted::Stopped(StopCause::StoreDenied(lsr_core::Denied::Credentials))
    );
    assert_eq!(
        replicator.health().phase,
        Phase::Stopped(StopCause::StoreDenied(lsr_core::Denied::Credentials))
    );
    assert_eq!(
        stopped(&replicator),
        Stopped::Stopped(StopCause::StoreDenied(lsr_core::Denied::Credentials))
    );
}

/// An operator's snapshot is staged at once, with no new commit, and reported as an event.
#[test]
fn request_snapshot_stages_a_snapshot_and_events_report_it() {
    let site = Site::new();
    let (runtime, _clock) = fake();
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
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
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, site.store(), tracked, runtime).unwrap();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(1024).unwrap());
    replicator.set_events(Box::new(sink));
    wait(&replicator, caught_up);
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
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
    site.assert_restores(&*site.store());
}

#[test]
fn app_connection_sets_up_only_this_database() {
    let site = Site::new();
    let (runtime, _clock) = fake();
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    let pooled = Connection::open(&site.db).unwrap();
    replicator.app_connection(&pooled).unwrap();
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
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator =
        Replicator::start_with(&site.db, Arc::clone(&store), config(), runtime).unwrap();
    site.commit(4);
    let stopped = stopped_moving(&replicator, &clock, POLL);
    assert!(matches!(stopped, Stopped::Drained { .. }), "{stopped:?}");
    let report = verify(&*store, Depth::Contents).unwrap();
    assert_eq!(report.worst(), None, "{report:?}");
    assert!(restore_if_missing(&*store, &site.db).unwrap().is_none());
    let to = site.path("if-missing");
    assert!(restore_if_missing(&*store, &to).unwrap().is_some());
    assert_same(&site.app, &to);
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
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    let rebuilt = wait(&replicator, |h| h.counters.panics == 1);
    assert_eq!(
        rebuilt.task(Task::Sync).map(|d| d.cause),
        Some(DegradedCause::Bug)
    );
    clock.advance(POLL);
    wait(&replicator, |h| {
        h.phase == Phase::Running && !h.is_degraded()
    });
    assert_eq!(booted(&replicator), Booted::Booted);
    site.commit(2);
    clock.advance(POLL);
    let health = wait(&replicator, caught_up);
    assert!(!health.is_degraded(), "{health:?}");
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
    site.assert_restores(&*site.store());
}

#[test]
fn a_panic_under_strict_responses_stops_with_bug() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.panics.store(1, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let strict = Config {
        responses: Responses::STRICT,
        ..config()
    };
    let replicator = Replicator::start_with(&site.db, store, strict, runtime).unwrap();
    assert_eq!(booted(&replicator), Booted::Stopped(StopCause::Bug));
    assert_eq!(stopped(&replicator), Stopped::Stopped(StopCause::Bug));
}

/// A checkpoint the application ran behind the replicator's back is counted and reported.
#[test]
fn an_app_checkpoint_is_counted_and_reported() {
    let site = Site::new();
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    let (sink, events) = ChannelSink::new(NonZeroUsize::new(256).unwrap());
    replicator.set_events(Box::new(sink));
    wait(&replicator, caught_up);
    site.app
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_row| Ok(()))
        .unwrap();
    site.commit(1);
    clock.advance(POLL);
    let health = wait(&replicator, |h| h.app_checkpoints_ran == 1 && caught_up(h));
    assert_eq!(health.app_checkpoints_ran, 1);
    assert_eq!(health.app_checkpoints.backfilled, 1);
    assert_eq!(
        health.app_notice(),
        Some(AppNotice::Checkpointing { ran: 1 })
    );
    assert!(!health.is_degraded(), "a notice, not a degraded state");
    let seen = events.drain();
    assert!(
        seen.iter().any(|event| matches!(
            event,
            Event::AppCheckpoint {
                seen: AppCheckpoint::Backfilled { .. },
                total: 1
            }
        )),
        "{seen:?}"
    );
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, site.store(), config, runtime).unwrap();
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
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    let settled = wait(&replicator, caught_up);
    for _ in 0..5 {
        clock.advance(POLL);
        let now = clock.now();
        let health = wait(&replicator, |h| h.reported_at >= now);
        assert_eq!(health.synced, settled.synced);
        assert_eq!(health.counters.uploads, settled.counters.uploads);
        assert_eq!(health.counters.syncs, settled.counters.syncs);
        assert_eq!(health.unreplicated_since, None);
    }
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
}

/// One replicator per file: a second start on the same file, even by another path, is
/// refused while the first runs.
#[test]
fn a_second_start_on_the_same_file_is_refused() {
    let site = Site::new();
    let (runtime, _clock) = fake();
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    let linked = site.dir.path().join("linked.db");
    std::fs::hard_link(&site.db, &linked).unwrap();
    let (runtime, _clock) = fake();
    assert!(matches!(
        Replicator::start_with(&linked, site.store(), config(), runtime),
        Err(StartError::AlreadyReplicated)
    ));
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
}

#[test]
fn start_refuses_a_missing_database_and_an_unusable_meta_dir() {
    let site = Site::new();
    let missing = site.dir.path().join("missing.db");
    assert!(matches!(
        Replicator::start(&missing, site.store(), config()),
        Err(StartError::Open {
            kind: std::io::ErrorKind::NotFound
        })
    ));
    let file = site.dir.path().join("a-file");
    std::fs::write(&file, b"x").unwrap();
    let bad_meta = Config {
        meta_dir: Some(file.join("meta")),
        ..config()
    };
    assert!(matches!(
        Replicator::start(&site.db, site.store(), bad_meta),
        Err(StartError::MetaDir { .. })
    ));
    // Nothing was left registered: a good start goes through.
    let replicator = Replicator::start(&site.db, site.store(), config()).unwrap();
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    replicator.set_events(Box::new(Panicking));
    wait(&replicator, caught_up);
    site.commit(2);
    clock.advance(POLL);
    let health = wait(&replicator, |h| caught_up(h) && h.counters.sink_panics > 0);
    assert_eq!(health.phase, Phase::Running);
    assert!(matches!(stopped(&replicator), Stopped::Drained { .. }));
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
    let replicator = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    wait(&replicator, caught_up);
    replicator.set_events(Box::new(Throws));
    site.commit(1);
    clock.advance(POLL);
    let health = wait(&replicator, |h| h.phase == Phase::Gone(Gone::ThreadDied));
    assert_eq!(health.phase, Phase::Gone(Gone::ThreadDied));
    assert_eq!(stopped(&replicator), Stopped::Gone(Gone::ThreadDied));
    assert_eq!(booted(&replicator), Booted::Gone(Gone::ThreadDied));
    // The file is free for the next replicator.
    let (runtime, _clock) = fake();
    let next = Replicator::start_with(&site.db, site.store(), config(), runtime).unwrap();
    assert!(matches!(stopped(&next), Stopped::Drained { .. }));
    site.assert_restores(&*site.store());
}

/// A drain that meets a retry waits out its backoff on the replicator's clock: on a fake
/// clock it ends once the clock reaches the retry, not before. A test that stops a
/// replicator whose drain may retry moves the clock while it waits ([`stopped_moving`]).
#[test]
fn a_drain_that_waits_out_a_retry_ends_once_the_clock_reaches_it() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    site.commit(4);
    wait(&replicator, |h| {
        h.synced.is_some() && h.counters.retries > 0
    });
    faulty.fail_uploads.store(false, Ordering::SeqCst);
    let stopped = stopped_moving(&replicator, &clock, POLL);
    assert!(
        matches!(stopped, Stopped::Drained { txid: Some(_) }),
        "{stopped:?}"
    );
    site.assert_restores(&*site.store());
}

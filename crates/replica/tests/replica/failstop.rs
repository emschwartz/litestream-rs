//! Fail-stop for the library: while writes are held the replicator keeps the database's write
//! lock (`docs/decisions/D192.md`), so every writer gets SQLite's "database is locked"
//! (`SQLITE_BUSY`) whether or not `Replicator::app_connection` set it up, in this process or
//! another, while reads go on; a trip that clears by itself lets writes through without anyone
//! acting. Every probe waits for no lock (`busy_timeout = 0`), so a write the lock blocks fails
//! at once.

#![expect(
    clippy::unwrap_used,
    reason = "the helpers fail the test on a setup error, as the tests do"
)]

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use lsr_core::Denied;
use lsr_replica::{
    Booted, CheckpointConfig, Config, DynStore, Event, EventSink, Gate, IfReplicationStops, Phase,
    Replicator, Response, Responses, StopCause, Task, Trip, WhyBlocked, Writes,
};
use rusqlite::Connection;

use crate::common::{self, PATIENCE, Site, config, fake, moved_until, wait};

const POLL: Duration = Duration::from_secs(1);

/// SQLite's "database is locked".
fn locked(err: &rusqlite::Error) -> bool {
    err.sqlite_error().map(|e| e.code) == Some(rusqlite::ErrorCode::DatabaseBusy)
}

fn insert(conn: &Connection) -> rusqlite::Result<usize> {
    conn.execute("INSERT INTO t(v) VALUES (randomblob(100))", [])
}

fn rows(site: &Site) -> usize {
    Site::rows(&site.app).len()
}

/// A connection nothing set up, that waits for no lock.
fn probe(site: &Site) -> Connection {
    let conn = Connection::open(&site.db).unwrap();
    conn.busy_timeout(Duration::ZERO).unwrap();
    conn
}

/// A connection set up the way an application on a raw handle (sqlx, say) sets one up, with
/// a long busy timeout of its own.
fn raw(site: &Site, replicator: &Replicator) -> Connection {
    let conn = Connection::open(&site.db).unwrap();
    conn.busy_timeout(Duration::from_secs(30)).unwrap();
    #[expect(
        unsafe_code,
        reason = "rusqlite hands out the raw handle only through an unsafe fn"
    )]
    // SAFETY: the handle is used only while `conn` lives, by this thread.
    let handle = unsafe { conn.handle() };
    #[expect(
        unsafe_code,
        reason = "the test hands over the raw handle an application would"
    )]
    // SAFETY: `handle` is open on this crate's SQLite and used by no other thread during the call.
    let setup = unsafe {
        lsr_replica::app_connection_raw(handle, replicator.database(), &replicator.writes_held())
    };
    setup.unwrap();
    conn
}

/// The write-gate events a replicator emitted, in order.
#[derive(Clone, Default)]
struct Gates(Arc<Mutex<Vec<Event>>>);

impl EventSink for Gates {
    fn event(&mut self, event: &Event) {
        let keep = matches!(
            event,
            Event::WritesBlocked { .. }
                | Event::WritesAllowed
                | Event::Engine(
                    lsr_core::Event::WritesHeld { .. } | lsr_core::Event::Checkpointed { .. }
                )
        );
        if keep {
            self.0.lock().unwrap().push(event.clone());
        }
    }
}

impl Gates {
    fn changes(&self) -> Vec<Event> {
        let events = self.0.lock().unwrap();
        events
            .iter()
            .filter(|e| matches!(e, Event::WritesBlocked { .. } | Event::WritesAllowed))
            .cloned()
            .collect()
    }

    fn any(&self, want: impl Fn(&Event) -> bool) -> bool {
        self.0.lock().unwrap().iter().any(want)
    }
}

/// Wakes the thread blocked in [`block_on`].
struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs a future to its end on this thread, with no runtime.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
        std::thread::park();
    }
}

/// Under `hold-start` (the default), a store that refuses before the first listing holds
/// writes: a connection `app_connection` set up, one set up on a raw handle, and one nothing
/// set up all get "database is locked" at once, and a read goes on. The first listing lets all
/// three through.
#[test]
fn a_refusal_at_boot_blocks_every_writer_until_the_first_listing() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.refuse.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    replicator.app_connection(&site.app).unwrap();
    let raw = raw(&site, &replicator);
    let probe = probe(&site);
    let refused = WhyBlocked::Tripped(Trip::RefusedAtStart(Denied::Credentials));
    let held = wait(&replicator, |h| h.writes == Writes::Blocked(refused));
    assert!(matches!(held.held_by.gate, Gate::Held(_)));
    assert!(replicator.writes_held().is_held());
    let before = rows(&site);
    for conn in [&site.app, &raw, &probe] {
        let err = insert(conn).unwrap_err();
        assert!(locked(&err), "{err}");
    }
    assert_eq!(rows(&site), before, "reads go on");

    faulty.refuse.store(false, Ordering::SeqCst);
    clock.advance(POLL);
    wait(&replicator, |h| h.writes == Writes::Allowed);
    assert!(!replicator.writes_held().is_held());
    for conn in [&site.app, &raw, &probe] {
        insert(conn).unwrap();
    }
    assert_eq!(rows(&site), before + 3);
}

/// Writes are held from the start until the boot is judged, under the default: `ready` (the
/// future) and `wait_booted` answer `Booted` only once they go through again, and the change
/// is told once each way.
#[test]
fn writes_are_held_while_the_boot_is_pending() {
    let site = Site::new();
    let faulty = site.faulty();
    let stall = faulty.stall_lists.lock().unwrap();
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let gates = Gates::default();
    let replicator =
        Replicator::start_with_events(&site.db, store, config(), runtime, Box::new(gates.clone()))
            .unwrap();
    let probe = probe(&site);
    assert_eq!(
        replicator.health().writes,
        Writes::Blocked(WhyBlocked::Booting)
    );
    assert_eq!(replicator.health().held_by.gate, Gate::Pending);
    let err = insert(&probe).unwrap_err();
    assert!(locked(&err), "{err}");
    assert_eq!(replicator.wait_booted(Duration::ZERO), Booted::TimedOut);

    drop(stall);
    assert_eq!(block_on(replicator.ready(PATIENCE)), Booted::Booted);
    assert_eq!(replicator.health().writes, Writes::Allowed);
    assert_eq!(replicator.wait_booted(Duration::ZERO), Booted::Booted);
    insert(&probe).unwrap();
    let stopped = replicator.stop(PATIENCE).wait();
    assert!(
        matches!(stopped, lsr_replica::Stopped::Drained { .. }),
        "{stopped:?}"
    );
    assert_eq!(
        gates.changes(),
        vec![
            Event::WritesBlocked {
                why: WhyBlocked::Booting
            },
            Event::WritesAllowed,
        ]
    );
}

/// Under `report` writes are never held, not even while the boot is pending, though health
/// says why it tripped.
#[test]
fn report_never_blocks_a_write() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.refuse.store(true, Ordering::SeqCst);
    let stall = faulty.stall_lists.lock().unwrap();
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let report = Config {
        if_replication_stops: IfReplicationStops::Report,
        ..config()
    };
    let replicator = Replicator::start_with(&site.db, store, report, runtime).unwrap();
    let probe = probe(&site);
    assert_eq!(replicator.health().writes, Writes::Allowed);
    insert(&probe).unwrap();
    drop(stall);
    let tripped = wait(&replicator, |h| h.held_by.trip.is_some());
    assert_eq!(tripped.held_by.gate, Gate::Allowed);
    assert_eq!(tripped.writes, Writes::Allowed);
    insert(&probe).unwrap();
}

/// A replicator that started cleanly and then stops for an unsafe cause: `hold-start` has
/// released the application and only reports; `stop-app` blocks every writer at once.
#[test]
fn an_unsafe_stop_while_running_blocks_writes_only_under_stop_app() {
    for (mode, blocks) in [
        (IfReplicationStops::HoldStart, false),
        (IfReplicationStops::StopApp, true),
    ] {
        let site = Site::new();
        let faulty = site.faulty();
        let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
        let (runtime, clock) = fake();
        let strict = Config {
            responses: Responses::STRICT,
            if_replication_stops: mode,
            ..config()
        };
        let replicator = Replicator::start_with(&site.db, store, strict, runtime).unwrap();
        let probe = probe(&site);
        wait(&replicator, |h| h.held_by.gate == Gate::Allowed);
        // A commit to upload, made before the store refuses: once it does, a stop can hold
        // writes at any moment.
        insert(&probe).unwrap();
        faulty.refuse.store(true, Ordering::SeqCst);
        clock.advance(POLL);
        let stopped = wait(&replicator, |h| matches!(h.phase, Phase::Stopped(_)));
        let trip = Trip::Stopped(StopCause::StoreDenied(Denied::Credentials));
        assert_eq!(stopped.held_by.trip, Some(trip));
        let health = wait(&replicator, |h| {
            h.writes != Writes::Blocking(WhyBlocked::Tripped(trip))
        });
        let held = health.writes == Writes::Blocked(WhyBlocked::Tripped(trip));
        assert_eq!(held, blocks, "{mode:?}: {:?}", health.writes);
        let result = insert(&probe);
        assert_eq!(
            result.as_ref().is_err_and(locked),
            blocks,
            "{mode:?}: {result:?}"
        );
    }
}

/// A panic that stops the replicator under `stop-app` (`bug: stop`) keeps writes blocked: the
/// lock is the thread's own, not the engine's, and outlives it.
#[test]
fn a_panic_that_stops_the_replicator_keeps_writes_blocked_under_stop_app() {
    let site = Site::new();
    let faulty = site.faulty();
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let strict = Config {
        responses: Responses {
            bug: Response::Stop,
            ..Responses::GO_LIKE
        },
        if_replication_stops: IfReplicationStops::StopApp,
        ..config()
    };
    let replicator = Replicator::start_with(&site.db, store, strict, runtime).unwrap();
    let probe = probe(&site);
    wait(&replicator, |h| h.writes == Writes::Allowed);
    insert(&probe).unwrap();
    faulty.panic_always.store(true, Ordering::SeqCst);
    clock.advance(POLL);
    let health = wait(&replicator, |h| {
        matches!(
            h.writes,
            Writes::Blocked(WhyBlocked::Tripped(Trip::Stopped(StopCause::Bug)))
        )
    });
    assert!(health.counters.panics >= 1, "{health:?}");
    let err = insert(&probe).unwrap_err();
    assert!(locked(&err), "{err}");
}

/// While writes are held the engine's checkpoint waits: no checkpoint task degraded and no
/// checkpoint counted, only the engine's `WritesHeld` event. Once writes go through it checkpoints.
#[test]
fn a_checkpoint_waits_while_writes_are_held_and_runs_after() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    // The first checkpoint comes due 20 minutes in, after the trip at 10, while writes are held.
    let stop_app = Config {
        if_replication_stops: IfReplicationStops::StopApp,
        checkpoint: CheckpointConfig {
            interval: Some(Duration::from_secs(20 * 60)),
            min_pages: None,
            ..CheckpointConfig::default()
        },
        ..config()
    };
    let gates = Gates::default();
    let replicator =
        Replicator::start_with_events(&site.db, store, stop_app, runtime, Box::new(gates.clone()))
            .unwrap();
    wait(&replicator, |h| h.writes == Writes::Allowed);
    site.commit(2);
    let held = moved_until(&replicator, &clock, (POLL * 30, 120), |h| {
        matches!(
            h.writes,
            Writes::Blocked(WhyBlocked::Tripped(Trip::UnreplicatedTooLong { .. }))
        )
    });
    let checkpoints = held.counters.checkpoints;
    let waited = moved_until(&replicator, &clock, (POLL * 30, 120), |_| {
        gates.any(|e| matches!(e, Event::Engine(lsr_core::Event::WritesHeld { .. })))
    });
    assert!(waited.task(Task::Checkpoint).is_none(), "{waited:?}");
    assert_eq!(waited.counters.checkpoints, checkpoints);

    faulty.fail_uploads.store(false, Ordering::SeqCst);
    moved_until(&replicator, &clock, (POLL * 30, 120), |h| {
        h.writes == Writes::Allowed
    });
    site.commit(1);
    moved_until(&replicator, &clock, (POLL * 30, 120), |h| {
        h.counters.checkpoints > checkpoints
    });
}

/// Health's lag survives a restart: a commit staged and never uploaded keeps its age when the
/// replicator starts again, where it used to count from the new start (issue #115).
///
/// The age it keeps is the staged file's: the clock's reading when the engine staged it. No
/// time health reports before the restart is that reading. `synced` carries the time the
/// tick began, which is earlier when the clock moves while the tick runs, and the first
/// sighting of the commit's WAL frames is earlier still and is this process's alone
/// (`docs/decisions.md` D80, D85). Comparing with either failed under load, so the bound is
/// the clock itself: the file was staged before the first replicator went away.
#[test]
fn a_restart_keeps_the_age_of_a_commit_off_the_replica() {
    let site = Site::new();
    let faulty = site.faulty();
    faulty.fail_uploads.store(true, Ordering::SeqCst);
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, clock) = fake();
    let replicator =
        Replicator::start_with(&site.db, Arc::clone(&store), config(), runtime.clone()).unwrap();
    wait(&replicator, |h| h.phase == Phase::Running);
    site.commit(1);
    clock.advance(POLL);
    wait(&replicator, |h| {
        h.unreplicated_since.is_some() && h.synced > h.replicated
    });
    drop(replicator);
    let stopped_at = clock.now();
    clock.advance(Duration::from_secs(120));
    let again = Replicator::start_with(&site.db, store, config(), runtime).unwrap();
    let after = wait(&again, |h| {
        h.phase == Phase::Running && h.unreplicated_since.is_some()
    });
    assert!(
        after.unreplicated_since.unwrap() <= stopped_at,
        "stopped at {stopped_at:?}, dated {:?} after the restart",
        after.unreplicated_since
    );
}
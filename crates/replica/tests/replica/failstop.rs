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
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use lsr_core::Denied;
use lsr_replica::{
    CheckpointConfig, Config, DynStore, Event, EventSink, Gate, IfReplicationStops, Phase,
    Readiness, Replicator, Response, Responses, StopCause, Task, Trip, WhyBlocked, Writes,
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
struct Gates(Arc<(Mutex<Vec<Event>>, Condvar)>);

impl EventSink for Gates {
    fn event(&mut self, event: &Event) {
        let keep = matches!(
            event,
            Event::WritesBlocked { .. }
                | Event::WritesAllowed
                | Event::Engine(
                    lsr_core::Event::WritesHeld { .. }
                        | lsr_core::Event::Checkpointed { .. }
                        | lsr_core::Event::CheckingIdentity { .. }
                )
        );
        if keep {
            self.0.0.lock().unwrap().push(event.clone());
            self.0.1.notify_all();
        }
    }
}

impl Gates {
    fn changes(&self) -> Vec<Event> {
        let events = self.0.0.lock().unwrap();
        events
            .iter()
            .filter(|e| matches!(e, Event::WritesBlocked { .. } | Event::WritesAllowed))
            .cloned()
            .collect()
    }

    fn any(&self, want: impl Fn(&Event) -> bool) -> bool {
        self.0.0.lock().unwrap().iter().any(want)
    }

    /// Every event kept so far.
    fn events(&self) -> Vec<Event> {
        self.0.0.lock().unwrap().clone()
    }

    /// Waits until `want` holds of the events kept. Health is published before the event that
    /// reports the same change is sent, so a test that waited on health may not yet see the
    /// event; it waits on the event instead.
    fn wait_for(&self, want: impl Fn(&[Event]) -> bool) {
        let events = self.0.0.lock().unwrap();
        let (events, timeout) = self
            .0
            .1
            .wait_timeout_while(events, PATIENCE, |events| !want(events))
            .unwrap();
        assert!(!timeout.timed_out(), "the events never came: {events:?}");
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
    let replicator = Replicator::builder(&site.db, store)
        .config(config())
        .runtime(runtime)
        .start()
        .unwrap();
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
/// future) and `wait_booted` answer `Readiness::Ready` only once they go through again, and the change
/// is told once each way.
#[test]
fn writes_are_held_while_the_boot_is_pending() {
    let site = Site::new();
    let faulty = site.faulty();
    let stall = faulty.stall_lists.lock().unwrap();
    let store: Arc<dyn DynStore> = Arc::<common::Faulty>::clone(&faulty);
    let (runtime, _clock) = fake();
    let gates = Gates::default();
    let replicator = Replicator::builder(&site.db, store)
        .config(config())
        .runtime(runtime)
        .events(Box::new(gates.clone()))
        .start()
        .unwrap();
    let probe = probe(&site);
    assert_eq!(
        replicator.health().writes,
        Writes::Blocked(WhyBlocked::Booting)
    );
    assert_eq!(replicator.health().held_by.gate, Gate::Pending);
    let err = insert(&probe).unwrap_err();
    assert!(locked(&err), "{err}");
    assert_eq!(replicator.wait_booted(Duration::ZERO), Readiness::TimedOut);

    drop(stall);
    assert_eq!(block_on(replicator.ready(PATIENCE)), Readiness::Ready);
    assert_eq!(replicator.health().writes, Writes::Allowed);
    assert_eq!(replicator.wait_booted(Duration::ZERO), Readiness::Ready);
    insert(&probe).unwrap();
    let stopped = replicator.stop(PATIENCE).wait();
    assert!(
        matches!(stopped, lsr_replica::StopOutcome::Drained { .. }),
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

/// Holds the replicator's thread in the event sink on the engine's `Booted` event, the moment
/// between the boot's decision and the step that stages its snapshot, until the test lets go.
#[derive(Clone, Default)]
struct HoldAtBoot(Arc<(Mutex<(bool, bool)>, Condvar)>);

impl EventSink for HoldAtBoot {
    fn event(&mut self, event: &Event) {
        if !matches!(event, Event::Engine(lsr_core::Event::Booted { .. })) {
            return;
        }
        let (state, signal) = &*self.0;
        let mut state = state.lock().unwrap();
        state.0 = true;
        signal.notify_all();
        while !state.1 {
            state = signal.wait(state).unwrap();
        }
    }
}

impl HoldAtBoot {
    /// Returns once the thread is held.
    fn reached(&self) {
        let (state, signal) = &*self.0;
        let mut state = state.lock().unwrap();
        while !state.0 {
            state = signal.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let (state, signal) = &*self.0;
        state.lock().unwrap().1 = true;
        signal.notify_all();
    }
}

/// The hold ends once the start is decided, not once its snapshot is staged
/// (`docs/decisions/D201.md`): while the thread is held right after the boot's decision, before
/// the step that reads the database for the snapshot, the start is judged, writes go through and
/// a second connection commits. The snapshot and the files after it restore every commit.
#[test]
fn writes_go_through_before_the_boot_snapshot_is_staged() {
    let site = Site::new();
    site.commit(3);
    let (runtime, _clock) = fake();
    let hold = HoldAtBoot::default();
    let replicator = Replicator::builder(&site.db, site.store())
        .config(config())
        .runtime(runtime)
        .events(Box::new(hold.clone()))
        .start()
        .unwrap();
    hold.reached();
    let health = replicator.health();
    assert_eq!(health.writes, Writes::Allowed);
    assert!(
        health.resnapshot.is_some(),
        "the snapshot is still owed: {health:?}"
    );
    assert_eq!(replicator.wait_booted(Duration::ZERO), Readiness::Ready);
    let probe = probe(&site);
    insert(&probe).unwrap();
    hold.release();
    // The wait `Replicator::ready`'s docs give for the first snapshot on the replica.
    let on_replica = replicator.wait_until(PATIENCE, |h| {
        h.resnapshot.is_none() && h.unreplicated_since.is_none() && h.replicated.is_some()
    });
    assert!(on_replica.is_some(), "{:?}", replicator.health());
    site.assert_restores(site.store().as_ref());
    site.commit(2);
    let stopped = replicator.stop(PATIENCE).wait();
    assert!(
        matches!(stopped, lsr_replica::StopOutcome::Drained { .. }),
        "{stopped:?}"
    );
    assert_eq!(rows(&site), 6);
    site.assert_restores(site.store().as_ref());
}

/// A start over a brand-new meta directory compares the database with the replica before it
/// decides, as long as a restore takes, inside one engine tick: the thread names that hold while
/// the tick runs, in health and as an event right after the engine's own notice, and lets writes
/// go once the check has decided (`docs/decisions/D201.md`).
#[test]
fn a_start_over_a_new_meta_directory_names_the_identity_check_while_it_holds() {
    let site = Site::new();
    site.commit(2);
    let (runtime, _clock) = fake();
    let first = Replicator::builder(&site.db, site.store())
        .config(config())
        .runtime(runtime)
        .start()
        .unwrap();
    let stopped = first.stop(PATIENCE).wait();
    assert!(
        matches!(stopped, lsr_replica::StopOutcome::Drained { .. }),
        "{stopped:?}"
    );
    drop(first);
    // No frame left to vouch for the replica's files, and no meta directory to vouch for them.
    site.app
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    std::fs::remove_dir_all(site.dir.path().join(".app.db-litestream")).unwrap();
    let (runtime, _clock) = fake();
    let gates = Gates::default();
    let replicator = Replicator::builder(&site.db, site.store())
        .config(config())
        .runtime(runtime)
        .events(Box::new(gates.clone()))
        .start()
        .unwrap();
    assert_eq!(block_on(replicator.ready(PATIENCE)), Readiness::Ready);
    gates.wait_for(|events| events.contains(&Event::WritesAllowed));
    let seen: Vec<Event> = gates.events();
    let checking = WhyBlocked::CheckingIdentity;
    let at = seen
        .iter()
        .position(|e| matches!(e, Event::Engine(lsr_core::Event::CheckingIdentity { .. })))
        .unwrap_or_else(|| panic!("no identity check: {seen:?}"));
    assert_eq!(
        seen.get(at.saturating_add(1)),
        Some(&Event::WritesBlocked { why: checking }),
        "{seen:?}"
    );
    assert_eq!(
        gates.changes(),
        vec![
            Event::WritesBlocked {
                why: WhyBlocked::Booting
            },
            Event::WritesBlocked { why: checking },
            Event::WritesAllowed,
        ]
    );
    drop(replicator);
}

/// Replicates `site` at 4096-byte pages and stops, then rewrites the database at 8192. With
/// `keep_wal` a commit leaves a WAL at the new size, which the next boot reads before it decides;
/// without, the WAL has no header, the boot decides on a snapshot, and only the snapshot's step
/// meets the new size.
fn resize(site: &Site, keep_wal: bool) {
    let (runtime, _clock) = fake();
    site.commit(2);
    let first = Replicator::builder(&site.db, site.store())
        .config(config())
        .runtime(runtime)
        .start()
        .unwrap();
    let stopped = first.stop(PATIENCE).wait();
    assert!(
        matches!(stopped, lsr_replica::StopOutcome::Drained { .. }),
        "{stopped:?}"
    );
    drop(first);
    site.app
        .execute_batch(
            "PRAGMA journal_mode = DELETE; PRAGMA page_size = 8192; VACUUM; PRAGMA journal_mode = WAL;",
        )
        .unwrap();
    if keep_wal {
        site.commit(1);
    }
}

fn resize_stops(mode: IfReplicationStops) -> Config {
    Config {
        responses: Responses {
            page_size_changed: Response::Stop,
            ..Responses::GO_LIKE
        },
        if_replication_stops: mode,
        ..config()
    }
}

/// A stop the boot finds before it decides (here a WAL at a page size the replica does not
/// hold) still holds the start: the boot answers the stop, and writes are never let through.
#[test]
fn a_stop_found_before_the_decision_still_holds_the_start() {
    let site = Site::new();
    resize(&site, true);
    let (runtime, _clock) = fake();
    let gates = Gates::default();
    let replicator = Replicator::builder(&site.db, site.store())
        .config(resize_stops(IfReplicationStops::HoldStart))
        .runtime(runtime)
        .events(Box::new(gates.clone()))
        .start()
        .unwrap();
    let trip = Trip::Stopped(StopCause::PageSizeChanged {
        was: lsr_ltx::PageSize::new(4096).unwrap(),
        now: lsr_ltx::PageSize::new(8192).unwrap(),
    });
    assert_eq!(
        block_on(replicator.ready(PATIENCE)),
        Readiness::Stopped(StopCause::PageSizeChanged {
            was: lsr_ltx::PageSize::new(4096).unwrap(),
            now: lsr_ltx::PageSize::new(8192).unwrap(),
        })
    );
    wait(&replicator, |h| {
        h.writes == Writes::Blocked(WhyBlocked::Tripped(trip))
    });
    gates.wait_for(|events| {
        events.contains(&Event::WritesBlocked {
            why: WhyBlocked::Tripped(trip),
        })
    });
    assert!(
        !gates.any(|e| matches!(e, Event::WritesAllowed)),
        "{:?}",
        gates.changes()
    );
    let err = insert(&probe(&site)).unwrap_err();
    assert!(locked(&err), "{err}");
}

/// A stop only the boot snapshot can find comes after the release, and is handled as any later
/// stop: health says the replicator stopped and why, and under `stop-app` writes are blocked
/// again, with the stop's own reason.
#[test]
fn a_stop_the_boot_snapshot_finds_blocks_writes_again_under_stop_app() {
    let site = Site::new();
    resize(&site, false);
    let (runtime, _clock) = fake();
    let gates = Gates::default();
    let replicator = Replicator::builder(&site.db, site.store())
        .config(resize_stops(IfReplicationStops::StopApp))
        .runtime(runtime)
        .events(Box::new(gates.clone()))
        .start()
        .unwrap();
    assert_eq!(block_on(replicator.ready(PATIENCE)), Readiness::Ready);
    let trip = Trip::Stopped(StopCause::PageSizeChanged {
        was: lsr_ltx::PageSize::new(4096).unwrap(),
        now: lsr_ltx::PageSize::new(8192).unwrap(),
    });
    let health = wait(&replicator, |h| {
        h.writes == Writes::Blocked(WhyBlocked::Tripped(trip))
    });
    assert!(
        matches!(
            health.phase,
            Phase::Stopped(StopCause::PageSizeChanged { .. })
        ),
        "{health:?}"
    );
    gates.wait_for(|events| {
        events.contains(&Event::WritesBlocked {
            why: WhyBlocked::Tripped(trip),
        })
    });
    assert_eq!(
        gates.changes(),
        vec![
            Event::WritesBlocked {
                why: WhyBlocked::Booting
            },
            Event::WritesAllowed,
            Event::WritesBlocked {
                why: WhyBlocked::Tripped(trip)
            },
        ]
    );
    let err = insert(&probe(&site)).unwrap_err();
    assert!(locked(&err), "{err}");
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
    let replicator = Replicator::builder(&site.db, store)
        .config(report)
        .runtime(runtime)
        .start()
        .unwrap();
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
        let replicator = Replicator::builder(&site.db, store)
            .config(strict)
            .runtime(runtime)
            .start()
            .unwrap();
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
    let replicator = Replicator::builder(&site.db, store)
        .config(strict)
        .runtime(runtime)
        .start()
        .unwrap();
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
    let replicator = Replicator::builder(&site.db, store)
        .config(stop_app)
        .runtime(runtime)
        .events(Box::new(gates.clone()))
        .start()
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
    let replicator = Replicator::builder(&site.db, Arc::clone(&store))
        .config(config())
        .runtime(runtime.clone())
        .start()
        .unwrap();
    wait(&replicator, |h| h.phase == Phase::Running);
    site.commit(1);
    clock.advance(POLL);
    wait(&replicator, |h| {
        h.unreplicated_since.is_some() && h.synced > h.replicated
    });
    drop(replicator);
    let stopped_at = clock.now();
    clock.advance(Duration::from_secs(120));
    let again = Replicator::builder(&site.db, store)
        .config(config())
        .runtime(runtime)
        .start()
        .unwrap();
    let after = wait(&again, |h| {
        h.phase == Phase::Running && h.unreplicated_since.is_some()
    });
    assert!(
        after.unreplicated_since.unwrap() <= stopped_at,
        "stopped at {stopped_at:?}, dated {:?} after the restart",
        after.unreplicated_since
    );
}

/// Every directory under `dir`, `dir` included, set to `mode`.
fn chmod_dirs(dir: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let mut stack = vec![dir.to_path_buf()];
    let mut all = Vec::new();
    while let Some(at) = stack.pop() {
        for entry in std::fs::read_dir(&at).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                stack.push(entry.path());
            }
        }
        all.push(at);
    }
    for at in all {
        std::fs::set_permissions(at, std::fs::Permissions::from_mode(mode)).unwrap();
    }
}

/// A replicator restarted over and over, each process living a few seconds, over a staging
/// directory that takes no new file (a full filesystem, modelled as a directory that refuses
/// creates): nothing is ever staged, so no staged file carries the age of the commit from one
/// process to the next. The age health reports must still grow across the restarts, dated from
/// the database's last write, so an alert on it fires within the bound's worth of restarts
/// (issue #331, `docs/decisions/D197.md`).
#[test]
fn a_crash_loop_over_a_staging_disk_that_takes_nothing_reports_a_growing_age() {
    let site = Site::new();
    let store = site.store();
    let (runtime, clock) = fake();
    let first = Replicator::builder(&site.db, Arc::clone(&store))
        .config(config())
        .runtime(runtime.clone())
        .start()
        .unwrap();
    wait(&first, |h| h.phase == Phase::Running);
    site.commit(1);
    clock.advance(POLL);
    let caught = wait(&first, |h| {
        h.replicated.is_some()
            && h.replicated.map(|(txid, _)| txid) == h.synced.map(|(txid, _)| txid)
            && h.unreplicated_since.is_none()
    });
    let written_at = caught.replicated.unwrap().1;
    drop(first);
    let staging = site.dir.path().join(".app.db-litestream");
    chmod_dirs(&staging, 0o555);
    let refused = std::fs::File::create_new(staging.join("ltx").join("0").join("probe"));
    assert!(
        refused.is_err(),
        "the staging directory still takes files (a process that ignores modes, such as root, \
         cannot run this test)"
    );
    site.commit(2);
    // The commits were written at the first process's last reading of the fake clock.
    let written = std::time::UNIX_EPOCH
        + Duration::from_millis(u64::try_from(written_at.unix_millis()).unwrap());
    for name in ["app.db", "app.db-wal"] {
        let file = std::fs::File::options()
            .write(true)
            .open(site.dir.path().join(name))
            .unwrap();
        file.set_modified(written).unwrap();
    }
    let mut oldest = None;
    for _ in 0..6 {
        clock.advance(Duration::from_secs(120));
        let again = Replicator::builder(&site.db, Arc::clone(&store))
            .config(config())
            .runtime(runtime.clone())
            .start()
            .unwrap();
        let health = wait(&again, |h| h.unreplicated_since == Some(written_at));
        oldest = health.unreplicated_since;
        drop(again);
    }
    chmod_dirs(&staging, 0o755);
    let age = clock.now().unix_millis() - oldest.unwrap().unix_millis();
    assert!(
        age >= 6 * 120_000 - 1_000,
        "the age after six restarts is {age} ms, dated {oldest:?}"
    );
}

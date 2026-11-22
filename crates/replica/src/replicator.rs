//! The handle, the state it shares with its thread, and the stop's outcome.
//!
//! [`Shared`] is everything the thread and the handles both touch: a [`State`] the thread
//! writes after every tick (read by `health`, `wait_booted` and [`FinalSync`] under one lock,
//! never across I/O), a condition variable signalled with each write, and the pending commands.
//! Commands are not a queue but slots, each idempotent (stop with the earliest deadline asked,
//! a snapshot asked, the newest event sink), so they are bounded by construction; setting one
//! unparks the thread.
//!
//! One replicator per database file in a process: a registry keyed by device and inode, not by
//! path, refuses a second `start` while the first is running and waits for one that is
//! draining, up to its shutdown grace. The thread takes itself out of the registry after it has
//! closed its connections.
//!
//! The last handle is found by a token every clone shares, not by `Arc::strong_count` on the
//! shared state (the thread holds that too). Dropping it asks for a stop with the shutdown
//! grace and returns at once.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{fmt, io};

use lsr_core::{Clock, StopCause, Task, Unfinished};
use lsr_ltx::Timestamp;
use lsr_sqlite::{DatabaseFile, Sqlite};

use crate::config::Config;
use crate::error::StartError;
use crate::events::EventSink;
use crate::health::{Booted, Gone, Health, Phase, Stopped};
use crate::readers::ReaderGuard;
use crate::runtime::{CALLER_SLICE, Park, Runtime, later};
use crate::store::{DynStore, Meters};
use crate::thread::Worker;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves plain data behind, so a poisoned lock is still usable.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the thread writes and the handles read.
#[derive(Debug)]
pub(crate) struct State {
    pub(crate) health: Health,
    /// The first boot finished.
    pub(crate) booted: bool,
    /// How the thread ended; set once.
    pub(crate) outcome: Option<Stopped>,
    /// When the tick now running began; `None` between ticks.
    pub(crate) tick_began: Option<Timestamp>,
    /// What is not on the replica, as of the last tick.
    pub(crate) left: Unfinished,
    /// A stop was asked for, so a `start` on the same file may wait for this one.
    pub(crate) stopping: bool,
}

/// The commands waiting for the thread.
#[derive(Default)]
pub(crate) struct Commands {
    pub(crate) stop: Option<Timestamp>,
    pub(crate) snapshot: bool,
    pub(crate) accept_pages: bool,
    pub(crate) sink: Option<Box<dyn EventSink>>,
}

/// Everything the thread and the handles share.
pub(crate) struct Shared {
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) park: Arc<dyn Park>,
    pub(crate) database: DatabaseFile,
    pub(crate) grace: Duration,
    pub(crate) readers: Arc<AtomicUsize>,
    pub(crate) meters: Arc<Meters>,
    /// Fail-stop's gate is held: the commit hook `app_connection` installs refuses the
    /// application's commits while this is set.
    pub(crate) writes_refused: Arc<AtomicBool>,
    pid: u32,
    state: Mutex<State>,
    changed: Condvar,
    commands: Mutex<Commands>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("database", &self.database)
            .field("state", &*lock(&self.state))
            .finish_non_exhaustive()
    }
}

/// Every running replicator in this process, by database file.
static ACTIVE: Mutex<BTreeMap<DatabaseFile, Arc<Shared>>> = Mutex::new(BTreeMap::new());

impl Shared {
    fn new(database: DatabaseFile, config: &Config, runtime: &Runtime) -> Self {
        let now = runtime.clock.now();
        Self {
            clock: Arc::clone(&runtime.clock),
            park: Arc::clone(&runtime.park),
            database,
            grace: config.shutdown_grace,
            readers: Arc::new(AtomicUsize::new(0)),
            meters: Arc::new(Meters::default()),
            writes_refused: Arc::new(AtomicBool::new(false)),
            pid: std::process::id(),
            state: Mutex::new(State {
                health: Health::new(now),
                booted: false,
                outcome: None,
                tick_began: None,
                left: Unfinished {
                    unsynced_wal_bytes: 0,
                    staged_not_uploaded: 0,
                },
                stopping: false,
            }),
            changed: Condvar::new(),
            commands: Mutex::new(Commands::default()),
            thread: Mutex::new(None),
        }
    }

    fn forked(&self) -> bool {
        std::process::id() != self.pid
    }

    /// Changes the state and wakes every waiter.
    pub(crate) fn update<T>(&self, change: impl FnOnce(&mut State) -> T) -> T {
        let out = change(&mut lock(&self.state));
        self.changed.notify_all();
        out
    }

    pub(crate) fn take_commands(&self) -> Commands {
        std::mem::take(&mut *lock(&self.commands))
    }

    fn command(&self, set: impl FnOnce(&mut Commands)) {
        set(&mut lock(&self.commands));
        self.park.unpark();
    }

    fn ask_stop(&self, deadline: Timestamp) {
        self.update(|state| state.stopping = true);
        self.command(|commands| {
            let earlier = commands.stop.map_or(deadline, |was| was.min(deadline));
            commands.stop = Some(earlier);
        });
    }

    /// The thread is gone without having said how it ended.
    fn thread_died(&self) -> bool {
        lock(&self.thread)
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
    }

    /// Waits until `answer` gives one or `deadline` passes on the clock. The clock is read
    /// again at least every [`CALLER_SLICE`], since a fake clock signals nobody when it moves.
    fn wait_for<T>(
        &self,
        deadline: Option<Timestamp>,
        mut answer: impl FnMut(&State, bool) -> Option<T>,
    ) -> Option<T> {
        let mut state = lock(&self.state);
        loop {
            let died = state.outcome.is_none() && self.thread_died();
            if let Some(found) = answer(&state, died) {
                return Some(found);
            }
            if deadline.is_some_and(|deadline| self.clock.now() >= deadline) {
                return None;
            }
            state = self
                .changed
                .wait_timeout(state, CALLER_SLICE)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Takes this replicator out of the registry, if it is the one registered.
    pub(crate) fn unregister(self: &Arc<Self>) {
        let mut active = lock(&ACTIVE);
        if active
            .get(&self.database)
            .is_some_and(|entry| Arc::ptr_eq(entry, self))
        {
            active.remove(&self.database);
        }
    }

    /// The thread's last act: the outcome, out of the registry, every waiter woken.
    pub(crate) fn finish(self: &Arc<Self>, outcome: Stopped) {
        self.unregister();
        self.update(|state| {
            let outcome = *state.outcome.get_or_insert(outcome);
            state.health.phase = match outcome {
                Stopped::Drained { .. } | Stopped::DeadlineReached { .. } => {
                    Phase::Stopped(StopCause::Shutdown)
                }
                Stopped::Stopped(cause) => Phase::Stopped(cause),
                Stopped::Gone(gone) => Phase::Gone(gone),
            };
            state.tick_began = None;
        });
    }
}

/// Registers `shared`, waiting for a draining replicator of the same file first.
fn register(shared: &Arc<Shared>) -> Result<(), StartError> {
    for _ in 0..2 {
        let previous = {
            let mut active = lock(&ACTIVE);
            match active.get(&shared.database) {
                None => {
                    active.insert(shared.database.clone(), Arc::clone(shared));
                    return Ok(());
                }
                Some(previous) => Arc::clone(previous),
            }
        };
        if !lock(&previous.state).stopping {
            return Err(StartError::AlreadyReplicated);
        }
        let deadline = later(previous.clock.now(), previous.grace);
        previous.wait_for(Some(deadline), |state, died| {
            (state.outcome.is_some() || died).then_some(())
        });
        previous.unregister_if_dead();
    }
    Err(StartError::AlreadyReplicated)
}

impl Shared {
    /// A thread that died without finishing leaves its entry behind; take it out.
    fn unregister_if_dead(self: &Arc<Self>) {
        if self.thread_died() {
            self.unregister();
        }
    }
}

/// What [`Replicator::launch`] hands the thread.
struct Launch<'a> {
    database: &'a Path,
    meta: &'a Path,
    store: Arc<dyn DynStore>,
    config: Config,
    runtime: Runtime,
    events: Option<Box<dyn EventSink>>,
}

/// One replicated database. Cheap to clone: every clone is the same thread.
#[derive(Clone)]
pub struct Replicator {
    shared: Arc<Shared>,
    /// Held for its drop: the last clone to go asks for the stop.
    _last: Arc<LastHandle>,
}

impl fmt::Debug for Replicator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replicator")
            .field("shared", &self.shared)
            .finish()
    }
}

/// Shared by every clone of one replicator; its drop is the last handle's.
struct LastHandle {
    shared: Arc<Shared>,
}

impl Drop for LastHandle {
    /// A best-effort `stop(shutdown_grace)`: no I/O and no wait on this thread.
    fn drop(&mut self) {
        let deadline = later(self.shared.clock.now(), self.shared.grace);
        self.shared.ask_stop(deadline);
    }
}

impl Replicator {
    /// Opens the replication connections, takes the read transaction that pins the WAL,
    /// starts the thread, and returns. The first boot (listing the replica, recovering the
    /// position) runs on the thread afterwards and shows in [`Replicator::health`]; no store
    /// call happens on the caller's thread. Call it before the application's connection pool opens.
    ///
    /// If a replicator for the same file (device and inode, not path) is still draining in
    /// this process, waits for it, up to its shutdown grace.
    ///
    /// # Errors
    /// [`StartError`]; nothing is left open or pinned.
    pub fn start(
        database: &Path,
        store: Arc<dyn DynStore>,
        config: Config,
    ) -> Result<Self, StartError> {
        Self::start_with(database, store, config, Runtime::system())
    }

    /// [`Replicator::start`] with every source of time, waiting and chance supplied.
    ///
    /// # Errors
    /// As [`Replicator::start`].
    pub fn start_with(
        database: &Path,
        store: Arc<dyn DynStore>,
        config: Config,
        runtime: Runtime,
    ) -> Result<Self, StartError> {
        Self::start_inner(database, store, config, runtime, None)
    }

    /// [`Replicator::start_with`] with the event sink in place before the thread starts, so
    /// it sees every event, the first boot's included. A sink set later with
    /// [`Replicator::set_events`] sees only what comes after the thread picks it up, which
    /// may be after the first boot.
    ///
    /// # Errors
    /// As [`Replicator::start`].
    pub fn start_with_events(
        database: &Path,
        store: Arc<dyn DynStore>,
        config: Config,
        runtime: Runtime,
        events: Box<dyn EventSink>,
    ) -> Result<Self, StartError> {
        Self::start_inner(database, store, config, runtime, Some(events))
    }

    fn start_inner(
        database: &Path,
        store: Arc<dyn DynStore>,
        config: Config,
        runtime: Runtime,
        events: Option<Box<dyn EventSink>>,
    ) -> Result<Self, StartError> {
        let open = |err: io::Error| StartError::Open { kind: err.kind() };
        let file = DatabaseFile::of(database).map_err(open)?;
        let meta = config.meta_dir_for(database).ok_or(StartError::Open {
            kind: io::ErrorKind::InvalidInput,
        })?;
        let shared = Arc::new(Shared::new(file, &config, &runtime));
        register(&shared)?;
        let launch = Launch {
            database,
            meta: &meta,
            store,
            config,
            runtime,
            events,
        };
        let started = Self::launch(&shared, launch);
        if started.is_err() {
            shared.unregister();
        }
        started?;
        Ok(Self {
            _last: Arc::new(LastHandle {
                shared: Arc::clone(&shared),
            }),
            shared,
        })
    }

    fn launch(shared: &Arc<Shared>, launch: Launch<'_>) -> Result<(), StartError> {
        let Launch {
            database,
            meta,
            store,
            config,
            runtime,
            events,
        } = launch;
        std::fs::create_dir_all(meta).map_err(|err| StartError::MetaDir { kind: err.kind() })?;
        let sqlite = Sqlite::open(database)?;
        let worker = Worker::new(
            Arc::clone(shared),
            sqlite,
            database,
            meta,
            store,
            config,
            runtime,
        )
        .with_sink(events);
        let handle = crate::stack::spawn("lsr-replica", crate::stack::ENGINE_STACK, move || {
            worker.run()
        })
        .map_err(|err| StartError::Thread { kind: err.kind() })?;
        *lock(&shared.thread) = Some(handle);
        Ok(())
    }

    /// Blocks until the first boot has finished, keeps failing (degraded), or the replicator
    /// stopped, or until `within` passes on the replicator's clock.
    #[must_use]
    pub fn wait_booted(&self, within: Duration) -> Booted {
        if self.shared.forked() {
            return Booted::Gone(Gone::Forked);
        }
        let deadline = later(self.shared.clock.now(), within);
        self.shared
            .wait_for(Some(deadline), boot_answer)
            .unwrap_or(Booted::TimedOut)
    }

    /// Blocks until `done` holds for the health snapshot, or `within` passes on the
    /// replicator's clock; returns the snapshot that satisfied it.
    pub fn wait_until(&self, within: Duration, done: impl Fn(&Health) -> bool) -> Option<Health> {
        if self.shared.forked() {
            return None;
        }
        let deadline = later(self.shared.clock.now(), within);
        let meters = Arc::clone(&self.shared.meters);
        self.shared.wait_for(Some(deadline), |state, died| {
            let health = snapshot(state, died, &meters);
            done(&health).then_some(health)
        })
    }

    /// Stops after syncing and uploading everything committed. The deadline is the earlier of
    /// this call's and any earlier stop's, so a second stop with a zero wait shortens a drain.
    /// The future needs no runtime: `.await` it or call [`FinalSync::wait`]. It resolves at the
    /// deadline on the caller's side even if the thread is still inside a request (which can
    /// take up to that request's own deadline); the thread then finishes in the background.
    pub fn stop(&self, within: Duration) -> FinalSync {
        if self.shared.forked() {
            return FinalSync::ready(&self.shared, Stopped::Gone(Gone::Forked));
        }
        let deadline = later(self.shared.clock.now(), within);
        self.shared.ask_stop(deadline);
        FinalSync::new(&self.shared, deadline)
    }

    /// A copy of the replicator's state.
    #[must_use]
    pub fn health(&self) -> Health {
        let state = lock(&self.shared.state);
        let died = state.outcome.is_none() && self.shared.thread_died();
        let mut health = snapshot(&state, died, &self.shared.meters);
        if self.shared.forked() {
            health.phase = Phase::Gone(Gone::Forked);
        }
        health
    }

    /// Makes the next sync a snapshot.
    pub fn request_snapshot(&self) {
        self.shared.command(|commands| commands.snapshot = true);
    }

    /// An operator accepts the database file as it is: every page the page checksum table
    /// found changed without a WAL frame (the degraded cause
    /// `DegradedCause::DatabasePageMismatch`) is trusted with the bytes it now holds, and the
    /// degradation and the kept restore point it held go.
    pub fn accept_database_pages(&self) {
        self.shared.command(|commands| commands.accept_pages = true);
    }

    /// Sends every event from the thread's next turn on to `sink`, replacing the sink before
    /// it. Events the thread emitted before it took the sink, the first boot's among them,
    /// went to the sink before it (or nowhere): give the first sink to
    /// [`Replicator::start_with_events`] to see them all.
    pub fn set_events(&self, sink: Box<dyn EventSink>) {
        self.shared.command(|commands| commands.sink = Some(sink));
    }

    /// Held for the length of a long read; while any guard lives the replicator runs no
    /// TRUNCATE checkpoint. It matters only under `ReaderTracking::Tracked`, where the application
    /// promises to hold one for every long read.
    pub fn long_reader(&self) -> ReaderGuard {
        ReaderGuard::new(&self.shared.readers)
    }

    /// The database file this replicator follows, for [`crate::app_connection_raw`].
    #[must_use]
    pub fn database(&self) -> &DatabaseFile {
        &self.shared.database
    }

    /// Sets `SQLITE_FCNTL_PERSIST_WAL` on and `wal_autocheckpoint = 0` on one of the application's
    /// connections, after checking that it opens this replicator's database (device and
    /// inode). Call it on every connection the application opens, pooled ones included.
    ///
    /// # Errors
    /// [`lsr_sqlite::SetupError`]: the connection has no file or opens another one, or a
    /// setting failed.
    ///
    /// It also installs a commit hook (`sqlite3_commit_hook`, replacing any the application set)
    /// that refuses every commit while fail-stop holds the application
    /// ([`crate::Health::fail_stop`], `Config::if_replication_stops`): SQLite rolls the
    /// transaction back and the application's COMMIT fails with
    /// `SQLITE_CONSTRAINT_COMMITHOOK`. Reads go on. Connections not set up here are not
    /// covered; an application on another binding reads the gate from [`Self::health`].
    #[cfg(feature = "rusqlite")]
    pub fn app_connection(
        &self,
        conn: &rusqlite::Connection,
    ) -> Result<(), lsr_sqlite::SetupError> {
        lsr_sqlite::app_connection_for(conn, &self.shared.database)?;
        lsr_sqlite::refuse_commits_while(conn, &self.shared.writes_refused);
        Ok(())
    }
}

fn snapshot(state: &State, died: bool, meters: &Meters) -> Health {
    let mut health = state.health.clone();
    health.counters.store = meters.read();
    if died {
        health.phase = Phase::Gone(Gone::ThreadDied);
    }
    health
}

fn boot_answer(state: &State, died: bool) -> Option<Booted> {
    if died {
        return Some(Booted::Gone(Gone::ThreadDied));
    }
    match state.outcome {
        Some(Stopped::Gone(gone)) => return Some(Booted::Gone(gone)),
        Some(Stopped::Stopped(cause)) => return Some(Booted::Stopped(cause)),
        Some(Stopped::Drained { .. } | Stopped::DeadlineReached { .. }) if !state.booted => {
            return Some(Booted::Stopped(StopCause::Shutdown));
        }
        Some(Stopped::Drained { .. } | Stopped::DeadlineReached { .. }) | None => {}
    }
    if state.booted {
        return Some(Booted::Booted);
    }
    if let Phase::Stopped(cause) = state.health.phase {
        return Some(Booted::Stopped(cause));
    }
    state
        .health
        .task(Task::Sync)
        .map(|degraded| Booted::Degraded(degraded.cause))
}

/// The end of a stop, as the thread reports it or, at the deadline, as the caller sees it.
fn stop_answer(state: &State, died: bool, now: Timestamp, deadline: Timestamp) -> Option<Stopped> {
    if let Some(outcome) = state.outcome {
        return Some(outcome);
    }
    if died {
        return Some(Stopped::Gone(Gone::ThreadDied));
    }
    // Only a tick that began before the deadline and is still running is cut off: the thread
    // is inside a request. One that began at or after it does no store I/O and ends at once.
    let stuck = state.tick_began.is_some_and(|began| began < deadline);
    (now >= deadline && stuck).then_some(Stopped::DeadlineReached {
        unsynced_wal_bytes: state.left.unsynced_wal_bytes,
        staged_not_uploaded: state.left.staged_not_uploaded,
    })
}

#[derive(Debug, Default)]
struct Watch {
    result: Option<Stopped>,
    waker: Option<Waker>,
    watching: bool,
}

/// The outcome of [`Replicator::stop`]: a future that needs no runtime, or [`FinalSync::wait`]
/// to block for it.
#[derive(Debug)]
#[must_use = "a stop's outcome says whether everything committed reached the replica"]
pub struct FinalSync {
    shared: Arc<Shared>,
    deadline: Timestamp,
    watch: Arc<Mutex<Watch>>,
}

impl FinalSync {
    fn new(shared: &Arc<Shared>, deadline: Timestamp) -> Self {
        Self {
            shared: Arc::clone(shared),
            deadline,
            watch: Arc::new(Mutex::new(Watch::default())),
        }
    }

    fn ready(shared: &Arc<Shared>, outcome: Stopped) -> Self {
        let sync = Self::new(shared, shared.clock.now());
        lock(&sync.watch).result = Some(outcome);
        sync
    }

    /// Blocks until the stop ends, or until its deadline passes with the thread still inside
    /// a request.
    pub fn wait(self) -> Stopped {
        if let Some(result) = lock(&self.watch).result {
            return result;
        }
        settle(&self.shared, self.deadline)
    }
}

fn settle(shared: &Shared, deadline: Timestamp) -> Stopped {
    let clock = Arc::clone(&shared.clock);
    shared
        .wait_for(None, |state, died| {
            stop_answer(state, died, clock.now(), deadline)
        })
        .unwrap_or(Stopped::Gone(Gone::ThreadDied))
}

impl Future for FinalSync {
    type Output = Stopped;

    /// The first poll that has no answer starts a watcher thread that blocks on the answer
    /// and wakes this future; without one, nothing would wake it at the deadline while the
    /// replicator's thread is stuck in a request.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Stopped> {
        let mut watch = lock(&self.watch);
        if let Some(result) = watch.result {
            return Poll::Ready(result);
        }
        watch.waker = Some(cx.waker().clone());
        if !watch.watching {
            watch.watching = true;
            let (shared, deadline) = (Arc::clone(&self.shared), self.deadline);
            let slot = Arc::clone(&self.watch);
            let spawned =
                crate::stack::spawn("lsr-final-sync", crate::stack::WAIT_STACK, move || {
                    let result = settle(&shared, deadline);
                    let waker = {
                        let mut watch = lock(&slot);
                        watch.result = Some(result);
                        watch.waker.take()
                    };
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                });
            if spawned.is_err() {
                // No thread to wait on: wait here, on the poller's thread.
                drop(watch);
                return Poll::Ready(settle(&self.shared, self.deadline));
            }
        }
        Poll::Pending
    }
}

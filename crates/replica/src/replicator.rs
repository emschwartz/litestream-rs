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
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{fmt, io};

use lsr_core::{Clock, Gate, StopCause, Task, Unfinished, blocks_writes};
use lsr_ltx::Timestamp;
use lsr_sqlite::{DatabaseFile, Sqlite, SqliteConfig, WriteGate, WritesHeld};

use crate::config::Config;
use crate::error::StartError;
use crate::events::EventSink;
use crate::health::{Booted, Gone, Health, Phase, Stopped, Writes};
use crate::meta::{MetaLock, read_takeover};
use crate::readers::ReaderGuard;
use crate::runtime::{CALLER_SLICE, Park, Runtime, later, lock};
use crate::store::{DynStore, Meters};
use crate::thread::Worker;

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
    pub(crate) final_retention: bool,
    pub(crate) final_snapshot: bool,
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
    /// Whether the write gate holds the application's writes now: the busy handler
    /// `app_connection` installs and the engine's checkpointer read it.
    pub(crate) writes_held: WritesHeld,
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
            writes_held: WritesHeld::new(),
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

    /// Waits until `done` holds for a health snapshot or `deadline` passes. The predicate runs
    /// with no lock held, so it may call any method of the handle, `health()` included.
    fn wait_for_health(
        &self,
        deadline: Timestamp,
        done: impl Fn(&Health) -> bool,
    ) -> Option<Health> {
        loop {
            let health = {
                let state = lock(&self.state);
                let died = state.outcome.is_none() && self.thread_died();
                snapshot(&state, died, &self.meters)
            };
            if done(&health) {
                return Some(health);
            }
            if self.clock.now() >= deadline {
                return None;
            }
            // A change between the snapshot and this wait is seen at the latest after one slice.
            let state = lock(&self.state);
            drop(
                self.changed
                    .wait_timeout(state, CALLER_SLICE)
                    .unwrap_or_else(PoisonError::into_inner),
            );
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
        let started = Self::launch(&shared, database, &meta, store, config, runtime, events);
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

    fn launch(
        shared: &Arc<Shared>,
        database: &Path,
        meta: &Path,
        store: Arc<dyn DynStore>,
        config: Config,
        runtime: Runtime,
        events: Option<Box<dyn EventSink>>,
    ) -> Result<(), StartError> {
        std::fs::create_dir_all(meta).map_err(|err| StartError::MetaDir { kind: err.kind() })?;
        let meta_lock = MetaLock::take(meta)?;
        let takeover = read_takeover(meta);
        let sqlite = Sqlite::open(database)?;
        let mut gate = WriteGate::open(
            database,
            &SqliteConfig::default(),
            shared.writes_held.clone(),
        )?;
        // Every connection must hold the file `start` registered, by its own descriptor: a file
        // swapped in at the path between the opens would be gated or replicated in its place.
        let registered =
            |opened: Option<DatabaseFile>| opened.is_none_or(|file| file == shared.database);
        let replaced = || StartError::Open {
            kind: io::ErrorKind::NotFound,
        };
        let engine = sqlite.database_file().map_err(|_split| replaced())?;
        if !registered(engine) || !registered(gate.database_file()) {
            return Err(replaced());
        }
        if let Some(why) = blocks_writes(config.if_replication_stops, Gate::Pending) {
            // Taken before `start` returns, so nothing the application writes lands before the
            // boot is judged. An application write transaction already open keeps it out; the
            // thread tries again on every turn.
            if gate.hold().is_ok() {
                shared.update(|state| state.health.writes = Writes::Blocked(why));
            }
        }
        let worker = Worker::new(
            Arc::clone(shared),
            sqlite,
            gate,
            database,
            meta,
            store,
            config,
            runtime,
            events,
            meta_lock,
            takeover,
        );
        let handle = crate::stack::spawn("lsr-replica", crate::stack::ENGINE_STACK, move || {
            worker.run()
        })
        .map_err(|err| StartError::Thread { kind: err.kind() })?;
        *lock(&shared.thread) = Some(handle);
        Ok(())
    }

    /// Blocks until the replicator is ready (the start was judged and writes go through, not
    /// the first snapshot taken), or cannot be, or until `within` passes on the replicator's
    /// clock: [`Replicator::ready`] for a caller without async.
    #[must_use]
    pub fn wait_booted(&self, within: Duration) -> Booted {
        if self.shared.forked() {
            return Booted::Gone(Gone::Forked);
        }
        settle_boot(&self.shared, later(self.shared.clock.now(), within))
    }

    /// Resolves once the replicator is ready: the first boot listed the replica and decided how
    /// this start proceeds (continue from a position, or snapshot for a named cause), so the
    /// write hold that covers it is over (`Booted::Booted`). Or once it cannot be: the boot
    /// keeps failing (`Degraded`), the replicator stopped, its thread is gone, or `within`
    /// passed on the replicator's clock (`TimedOut`). It answers as
    /// [`Replicator::wait_booted`] does. The future needs no runtime: `.await` it or call
    /// [`Ready::wait`]; a poll that finds no answer starts a thread that waits for it.
    ///
    /// Ready does not mean the first snapshot exists: a start that decided on one takes it in
    /// the background, while the application writes (`docs/decisions/D201.md`), and a stop
    /// only that snapshot can find comes later, as any later stop does. An application that
    /// must wait for the first snapshot to reach the replica waits on health: the snapshot is
    /// staged once `resnapshot` is `None`, and on the replica once `unreplicated_since` is
    /// also `None`:
    ///
    /// ```no_run
    /// # fn wait(replicator: &lsr_replica::Replicator) {
    /// # use std::time::Duration;
    /// let on_replica = replicator.wait_until(Duration::from_secs(600), |h| {
    ///     h.resnapshot.is_none() && h.unreplicated_since.is_none() && h.replicated.is_some()
    /// });
    /// # }
    /// ```
    pub fn ready(&self, within: Duration) -> Ready {
        let deadline = later(self.shared.clock.now(), within);
        let watched = Watched::new(&self.shared, deadline, settle_boot, "lsr-ready");
        if self.shared.forked() {
            return Ready(watched.answered(Booted::Gone(Gone::Forked)));
        }
        Ready(watched)
    }

    /// Blocks until `done` holds for the health snapshot, or `within` passes on the
    /// replicator's clock; returns the snapshot that satisfied it.
    pub fn wait_until(&self, within: Duration, done: impl Fn(&Health) -> bool) -> Option<Health> {
        if self.shared.forked() {
            return None;
        }
        let deadline = later(self.shared.clock.now(), within);
        self.shared.wait_for_health(deadline, done)
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

    /// Makes the next stop end with a retention pass once everything committed is uploaded, as
    /// Go's `replicate -once -enforce-retention` does: the daemon's retention rules with level-0
    /// deletion off, until nothing more is to be deleted. [`Health::final_retention`] says how it
    /// ended. Ask before calling [`Replicator::stop`].
    pub fn request_final_retention(&self) {
        self.shared
            .command(|commands| commands.final_retention = true);
    }

    /// Makes the next stop end with a level-9 snapshot of the whole database once everything
    /// committed is uploaded, as Go's `replicate -once -force-snapshot` does. It is written
    /// even when the stop's deadline has passed, and is not retried: [`Health::final_snapshot`]
    /// says whether it was written. Ask before calling [`Replicator::stop`].
    pub fn request_final_snapshot(&self) {
        self.shared
            .command(|commands| commands.final_snapshot = true);
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

    /// Whether the replicator holds the application's writes now, as a handle that follows
    /// it: what [`crate::app_connection_raw`] gives an application connection's busy handler.
    /// [`Health::writes`] says the same, with the reason.
    #[must_use]
    pub fn writes_held(&self) -> WritesHeld {
        self.shared.writes_held.clone()
    }

    /// Optional setup for one of the application's connections: checks that it opens this
    /// replicator's database (device and inode), sets `wal_autocheckpoint = 0`, and installs a
    /// busy handler that makes a write fail at once while the replicator holds the
    /// application's writes, instead of after the connection's busy timeout. Otherwise the
    /// handler waits as SQLite's default one does, for the busy timeout the connection had
    /// when this was called.
    ///
    /// Skipping it is safe. Fail-stop holds writes with a write transaction of its own
    /// ([`Health::writes`]), which stops every writer whether or not it was set up, in this
    /// process or another: a connection not set up here meets "database is locked" after its
    /// busy timeout, and runs automatic checkpoints, which cost the replica nothing
    /// ([`crate::Health::app_notice`]). Setting `PRAGMA busy_timeout` afterwards replaces the
    /// handler and loses only the fast failure. SQLite's own error is not changed: an
    /// application that wants to say more listens for `Event::WritesBlocked`.
    ///
    /// # Errors
    /// [`lsr_sqlite::SetupError`]: the connection has no file or opens another one, or a
    /// setting failed.
    #[cfg(feature = "rusqlite")]
    pub fn app_connection(
        &self,
        conn: &rusqlite::Connection,
    ) -> Result<(), lsr_sqlite::SetupError> {
        lsr_sqlite::app_connection_for(conn, &self.shared.database)?;
        lsr_sqlite::fail_fast_while(conn, &self.shared.writes_held)
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
        // A drain before the first boot finished is answered by the phase below, which the
        // outcome set to `Stopped(Shutdown)` under the same lock.
        Some(Stopped::Drained { .. } | Stopped::DeadlineReached { .. }) | None => {}
    }
    // Ready once the boot is judged too: until then writes are held (`Gate::Pending`), so an
    // application told it may go would meet "database is locked".
    if state.booted && state.health.held_by.gate != Gate::Pending {
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

/// [`boot_answer`] by `deadline`, blocking the calling thread.
fn settle_boot(shared: &Shared, deadline: Timestamp) -> Booted {
    shared
        .wait_for(Some(deadline), boot_answer)
        .unwrap_or(Booted::TimedOut)
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

/// [`stop_answer`], blocking the calling thread.
fn settle_stop(shared: &Shared, deadline: Timestamp) -> Stopped {
    let clock = Arc::clone(&shared.clock);
    shared
        .wait_for(None, |state, died| {
            stop_answer(state, died, clock.now(), deadline)
        })
        .unwrap_or(Stopped::Gone(Gone::ThreadDied))
}

#[derive(Debug)]
struct Watch<T> {
    result: Option<T>,
    waker: Option<Waker>,
    watching: bool,
}

/// One answer from the thread's state, as a future that needs no runtime or by blocking:
/// what [`FinalSync`] and [`Ready`] share. `settle` blocks until the answer, by `deadline`.
#[derive(Debug)]
struct Watched<T> {
    shared: Arc<Shared>,
    deadline: Timestamp,
    settle: fn(&Shared, Timestamp) -> T,
    /// The watcher thread's name.
    name: &'static str,
    watch: Arc<Mutex<Watch<T>>>,
}

impl<T: Copy + Send + 'static> Watched<T> {
    fn new(
        shared: &Arc<Shared>,
        deadline: Timestamp,
        settle: fn(&Shared, Timestamp) -> T,
        name: &'static str,
    ) -> Self {
        Self {
            shared: Arc::clone(shared),
            deadline,
            settle,
            name,
            watch: Arc::new(Mutex::new(Watch {
                result: None,
                waker: None,
                watching: false,
            })),
        }
    }

    /// Already answered.
    fn answered(self, result: T) -> Self {
        lock(&self.watch).result = Some(result);
        self
    }

    fn wait(self) -> T {
        if let Some(result) = lock(&self.watch).result {
            return result;
        }
        (self.settle)(&self.shared, self.deadline)
    }

    /// The first poll that has no answer starts a watcher thread that blocks on the answer
    /// and wakes this future; without one, nothing would wake it at the deadline while the
    /// replicator's thread is stuck in a request.
    fn poll(&self, cx: &Context<'_>) -> Poll<T> {
        let mut watch = lock(&self.watch);
        if let Some(result) = watch.result {
            return Poll::Ready(result);
        }
        watch.waker = Some(cx.waker().clone());
        if !watch.watching {
            watch.watching = true;
            let (shared, deadline, settle) = (Arc::clone(&self.shared), self.deadline, self.settle);
            let slot = Arc::clone(&self.watch);
            let spawned = crate::stack::spawn(self.name, crate::stack::WAIT_STACK, move || {
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
                return Poll::Ready((self.settle)(&self.shared, self.deadline));
            }
        }
        Poll::Pending
    }
}

/// The outcome of [`Replicator::stop`]: a future that needs no runtime, or [`FinalSync::wait`]
/// to block for it.
#[derive(Debug)]
#[must_use = "a stop's outcome says whether everything committed reached the replica"]
pub struct FinalSync(Watched<Stopped>);

impl FinalSync {
    fn new(shared: &Arc<Shared>, deadline: Timestamp) -> Self {
        Self(Watched::new(
            shared,
            deadline,
            settle_stop,
            "lsr-final-sync",
        ))
    }

    fn ready(shared: &Arc<Shared>, outcome: Stopped) -> Self {
        Self(
            Watched::new(shared, shared.clock.now(), settle_stop, "lsr-final-sync")
                .answered(outcome),
        )
    }

    /// Blocks until the stop ends, or until its deadline passes with the thread still inside
    /// a request.
    pub fn wait(self) -> Stopped {
        self.0.wait()
    }
}

impl Future for FinalSync {
    type Output = Stopped;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Stopped> {
        self.0.poll(cx)
    }
}

/// The outcome of [`Replicator::ready`]: a future that needs no runtime, or [`Ready::wait`] to
/// block for it, which is [`Replicator::wait_booted`].
#[derive(Debug)]
#[must_use = "the outcome says whether the replicator is ready, and why not"]
pub struct Ready(Watched<Booted>);

impl Ready {
    /// Blocks until the replicator is ready or cannot be, or the wait runs out.
    pub fn wait(self) -> Booted {
        self.0.wait()
    }
}

impl Future for Ready {
    type Output = Booted;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Booted> {
        self.0.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(millis: i64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    fn ticking_since(began: Timestamp) -> State {
        State {
            health: Health::new(at(0)),
            booted: true,
            outcome: None,
            tick_began: Some(began),
            left: Unfinished {
                unsynced_wal_bytes: 7,
                staged_not_uploaded: 2,
            },
            stopping: true,
        }
    }

    /// A handle carried into a forked child answers every call as gone, without a thread to
    /// ask: the thread did not survive the fork.
    #[test]
    fn a_handle_in_a_forked_child_reports_the_replicator_gone() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        std::fs::write(&path, b"").unwrap();
        let handle = |pid_offset: u32| {
            let file = DatabaseFile::of(&path).unwrap();
            let mut shared = Shared::new(file, &Config::default(), &Runtime::system());
            shared.pid = shared.pid.wrapping_add(pid_offset);
            let shared = Arc::new(shared);
            Replicator {
                _last: Arc::new(LastHandle {
                    shared: Arc::clone(&shared),
                }),
                shared,
            }
        };
        assert_eq!(handle(0).health().phase, Phase::Booting);
        let forked = handle(1);
        assert_eq!(forked.health().phase, Phase::Gone(Gone::Forked));
        assert_eq!(
            forked.wait_booted(Duration::from_secs(60)),
            Booted::Gone(Gone::Forked)
        );
        assert_eq!(forked.wait_until(Duration::from_secs(60), |_| true), None);
        assert_eq!(
            forked.stop(Duration::from_secs(60)).wait(),
            Stopped::Gone(Gone::Forked)
        );
    }

    /// A predicate may read the handle: it runs with no lock held, so `health()` inside it
    /// answers instead of waiting on the lock the wait itself holds.
    #[test]
    fn a_wait_until_predicate_may_call_health() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db");
        std::fs::write(&path, b"").unwrap();
        let file = DatabaseFile::of(&path).unwrap();
        let shared = Arc::new(Shared::new(file, &Config::default(), &Runtime::system()));
        let handle = Replicator {
            _last: Arc::new(LastHandle {
                shared: Arc::clone(&shared),
            }),
            shared,
        };
        let seen = handle.wait_until(Duration::from_secs(60), |_| {
            handle.health().phase == Phase::Booting
        });
        assert_eq!(seen.map(|health| health.phase), Some(Phase::Booting));
        assert_eq!(
            handle.wait_until(Duration::ZERO, |_| handle.health().phase != Phase::Booting),
            None
        );
    }

    /// Only a tick that began before the deadline is inside a request the deadline cuts off;
    /// one that began at or after it ends on its own.
    #[test]
    fn a_stop_is_cut_off_only_by_a_tick_from_before_its_deadline() {
        let deadline = at(1_000);
        let cut = Some(Stopped::DeadlineReached {
            unsynced_wal_bytes: 7,
            staged_not_uploaded: 2,
        });
        let early = ticking_since(at(999));
        assert_eq!(stop_answer(&early, false, deadline, deadline), cut);
        assert_eq!(stop_answer(&early, false, at(999), deadline), None);
        for began in [deadline, at(1_001)] {
            assert_eq!(
                stop_answer(&ticking_since(began), false, at(2_000), deadline),
                None
            );
        }
    }
}

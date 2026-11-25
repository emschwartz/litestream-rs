//! The replicator thread: one per database, driving the engine one tick at a time.
//!
//! Each turn takes the pending commands, ticks the engine once, writes the health snapshot,
//! and parks until the earliest of the engine's own next wake, the next poll of the WAL
//! (`Config::poll_interval`) and a command. Work that is ready runs at once, without a park.
//!
//! A panic inside a tick is caught here and never leaves the thread. The engine and its
//! SQLite connections are dropped, since the panic may have unwound through either; under
//! `Responses::bug = KeepTrying` a fresh engine boots again after the retry wait (doubling, at
//! most once per wait), with the connections reopened, and under `Stop` the replicator stops
//! with `StopCause::Bug`. A panic outside that catch ends the thread; its exit guard then
//! reports `Gone::ThreadDied` to every waiter.
//!
//! What a fresh engine must not forget is the engine's own to keep: the fresh one is built
//! from the dropped one (`Actor::rebuilt`), which carries over a stop's deadline, an
//! operator's snapshot request not yet staged, each task's degraded state and the restore
//! point retention keeps for them, the scheduled verify's pending read, and the application
//! checkpoint counts. This thread keeps only the stop's deadline, to cap its own waits, and
//! hands the scheduled verify's read lock over after every tick.
//!
//! Health's progress (what is synced, what the replica holds, what a stop would leave) is
//! `Progress`, which only a booted engine overwrites: a fresh engine knows nothing yet, and
//! an engine that cannot read the WAL knows no better. Until one has booted, what is left is
//! counted high, never low, as the engine counts what it cannot read: the last booted
//! engine's count, raised to the WAL file's length and to the staged files past the last
//! transaction the replica held.
//!
//! Every wait this thread returns is capped at a stop's deadline (`park_until`), and a turn
//! that finds the deadline passed while the engine waits to be rebuilt ends there.
//!
//! Fail-stop's write gate is this thread's too: a third connection ([`WriteGate`]), apart
//! from the engine's two, that holds a write transaction while [`lsr_core::blocks_writes`]
//! says the application's writes are held. After every tick, and at the start of every turn,
//! the gate is made to match the latest judgement; a hold that meets an open application write
//! transaction is tried again on the next turn, at most one poll interval later. The gate is
//! never dropped with the engine: it holds through a panic, a rebuild and a stop for a bug,
//! and goes only when the thread ends. Each change is an event (`WritesBlocked`,
//! `WritesAllowed`) sent after the health snapshot that shows it.
//!
//! The connections reopen by path, but the replicator was registered by device and inode.
//! A reopen that finds another file at the path (or none) does not open it, and one whose
//! opened descriptor names another file does not keep it: the database the replicator
//! follows is gone from under it, which is `WalUnlinked` at its configured response, never a
//! second database replicated into the same replica.

use std::num::NonZeroU32;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lsr_core::{
    Actor, AppCheckpoints, AppGate, Degraded, Did, Env, Event as EngineEvent, Level, ObjectStore,
    PageLimit, PersistentCause, Phase as EnginePhase, Response, StopCause, TakeoverRecord, Task,
    Tick, Unfinished, WalSource, WhyBlocked, blocks_writes, retry_wait,
};
use lsr_ltx::{Timestamp, Txid};
use lsr_sqlite::{DatabaseFile, Sqlite, WriteGate};
use lsr_store_fs::FsStore;

use crate::config::Config;
use crate::events::{Event, EventSink, TakeoverUnusable};
use crate::exec::block_on;
use crate::health::{Counters, FailStop, Gone, Phase, Stopped, Writes};
use crate::meta::{MetaLock, remove_takeover, write_takeover};
use crate::readers::Guarded;
use crate::replicator::Shared;
use crate::runtime::{Runtime, later};
use crate::scheduled::Scheduled;
use crate::store::{DynStore, Metered};

/// What a turn decided.
enum Turn {
    /// Work is ready: turn again at once.
    Again,
    /// Park until this time, or until a command when `None`.
    Wait(Option<Timestamp>),
    /// The thread is done.
    Done(Stopped),
}

/// The engine is gone (a panic dropped it) and a fresh one waits before booting.
#[derive(Clone, Copy, Debug)]
struct Rebuilt {
    /// Why it waits: `Bug` after a panic, `WalUnlinked` once the path names another file.
    cause: PersistentCause,
    attempts: NonZeroU32,
    since: Timestamp,
    until: Timestamp,
}

/// Why the connections did not reopen.
#[derive(Clone, Copy, Debug)]
enum Reopen {
    /// The open failed; the file at the path is still the one registered.
    Failed,
    /// The path names another file than the one registered, or none.
    Replaced,
}

/// An engine that has booted: the only one whose positions [`Progress`] takes.
#[derive(Clone, Copy)]
struct BootedEngine<'a>(&'a Actor);

/// The engine, if it has booted.
fn booted(actor: &Actor) -> Option<BootedEngine<'_>> {
    actor.booted().then_some(BootedEngine(actor))
}

/// What health says is synced, replicated and left, as the last booted engine knew it.
#[derive(Debug)]
struct Progress {
    synced: Option<(Txid, Timestamp)>,
    replicated: Option<(Txid, Timestamp)>,
    unreplicated_since: Option<Timestamp>,
    left: Unfinished,
    /// Whether a booted engine has been read yet.
    advanced: bool,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            synced: None,
            replicated: None,
            unreplicated_since: None,
            left: Unfinished {
                unsynced_wal_bytes: 0,
                staged_not_uploaded: 0,
            },
            advanced: false,
        }
    }
}

impl Progress {
    /// `written` is when the database was last written, read with the first booted reading and
    /// not asked for after it.
    fn advance(
        &mut self,
        engine: BootedEngine<'_>,
        left: Unfinished,
        written: Option<Timestamp>,
        now: Timestamp,
    ) {
        let staged = engine.0.staged();
        let replicated = engine.0.replicated();
        if self.synced.map(|(txid, _)| txid) != staged {
            self.synced = staged.map(|txid| (txid, now));
        }
        if self.replicated.map(|(txid, _)| txid) != replicated {
            self.replicated = replicated.map(|txid| (txid, now));
        }
        // Only what is not staged: a staged file the replica lacks is the engine's own lag
        // clock, dated from that file, which is never later than this reading.
        let behind = left.unsynced_wal_bytes > 0;
        // The first booted reading finds lag that began before this process did, when the
        // database's last write says so: no commit is newer than it, so the age from there is
        // never too long. A process restarted every few seconds over a staging directory that
        // takes no file (staged nothing, so no staged file dates the commit) would otherwise
        // start the age over each time (issue #331).
        self.advanced = true;
        self.unreplicated_since = if behind {
            let first = self.unreplicated_since.or(Some(now));
            match written {
                Some(written) => first.map(|first| first.min(written)),
                None => first,
            }
        } else {
            None
        };
        self.left = left;
    }

    /// No booted engine vouches for the replica: nothing proves it holds every commit, so the
    /// lag counts from the earliest reading that could not tell. Before the first boot that is
    /// the start, and a database that was caught up keeps no claim of zero lag while a rebuilt
    /// engine waits to boot.
    fn unproven(&mut self, now: Timestamp) {
        self.unreplicated_since = self.unreplicated_since.or(Some(now));
    }
}

/// What the health snapshot remembers between ticks.
#[derive(Debug, Default)]
struct Seen {
    counters: Counters,
    /// The application checkpoint total last reported as an event.
    app_reported: u64,
    /// The skipped scratch sweep was reported as an event.
    scratch_reported: bool,
    stopped_reported: bool,
}

pub(crate) struct Worker {
    shared: Arc<Shared>,
    sqlite: Option<Sqlite>,
    database: PathBuf,
    staging: FsStore,
    remote: Metered,
    actor: Actor,
    config: Config,
    runtime: Runtime,
    sink: Option<Box<dyn EventSink>>,
    next_poll: Timestamp,
    rebuilt: Option<Rebuilt>,
    /// The thread itself stopped the replicator: a panic under `Responses::bug = Stop`, or a
    /// replaced database under `Responses::wal_unlinked = Stop`.
    stopped: Option<StopCause>,
    /// The earliest stop asked for: every wait of this thread is capped at it.
    stop: Option<Timestamp>,
    progress: Progress,
    seen: Seen,
    /// The scheduled verify, when `Config::verify_interval` is set.
    scheduled: Option<Scheduled>,
    /// The meta directory, where the takeover record is kept (`crate::meta`).
    meta: PathBuf,
    /// The meta directory's lock, held for the thread's life.
    _meta_lock: Option<MetaLock>,
    /// Whether fail-stop has let the application run yet (`lsr_core::AppGate`).
    app_gate: AppGate,
    /// The connection that holds the application's writes.
    gate: WriteGate,
    /// Why the application's writes are to be held, as last judged; `None` lets them through.
    blocked: Option<WhyBlocked>,
    /// The last reason writes were held for, which a gate whose release failed still shows.
    why: WhyBlocked,
    /// What the last `WritesBlocked` or `WritesAllowed` event said.
    reported: Writes,
    /// When a hold or release that failed is tried again at the latest.
    gate_retry: Option<Timestamp>,
    /// The clock's last reading, to notice it stepping back.
    last_reading: Option<Timestamp>,
}

/// Owns the worker for the thread's life, so that however the thread ends, the worker and
/// its SQLite connections are gone before the outcome is published; reports
/// `Gone::ThreadDied` if the thread unwinds past the loop's own catch.
struct ExitGuard {
    shared: Arc<Shared>,
    worker: Option<Worker>,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        if self.worker.take().is_some() {
            self.shared.finish(Stopped::Gone(Gone::ThreadDied));
        }
    }
}

impl Worker {
    #[expect(
        clippy::too_many_arguments,
        reason = "what one thread owns, each from its own open in the launch"
    )]
    pub(crate) fn new(
        shared: Arc<Shared>,
        sqlite: Sqlite,
        gate: WriteGate,
        database: &Path,
        meta: &Path,
        store: Arc<dyn DynStore>,
        config: Config,
        runtime: Runtime,
    ) -> Self {
        let mut actor = Actor::new(config.actor());
        actor.set_app_autocheckpoint(config.app_autocheckpoint);
        let now = runtime.clock.now();
        let scheduled = config.verify_interval.map(|every| {
            let store = Metered::new(
                Arc::clone(&store),
                Arc::clone(&shared.meters),
                Arc::clone(&runtime.clock),
            );
            Scheduled::new(every.get(), store, Arc::clone(&runtime.park), now)
        });
        let remote = Metered::new(
            store,
            Arc::clone(&shared.meters),
            Arc::clone(&runtime.clock),
        );
        // Until the first judgement the boot is pending, which holds writes but under `report`.
        let blocked = blocks_writes(config.if_replication_stops, lsr_core::Gate::Pending);
        Self {
            shared,
            sqlite: Some(sqlite),
            database: database.to_owned(),
            staging: FsStore::new(meta),
            remote,
            actor,
            next_poll: later(now, config.poll_interval.get()),
            config,
            runtime,
            sink: None,
            rebuilt: None,
            stopped: None,
            stop: None,
            progress: Progress::default(),
            seen: Seen::default(),
            scheduled,
            meta: meta.to_owned(),
            _meta_lock: None,
            last_reading: None,
            app_gate: AppGate::default(),
            blocked,
            gate,
            why: WhyBlocked::Booting,
            reported: Writes::Allowed,
            gate_retry: None,
        }
    }

    /// The meta directory's lock to hold, and the takeover record an earlier run, a
    /// `lsr trust-local` or a restore left there, which the engine judges at its boot. One that
    /// cannot be used is reported and ignored, as if there were none.
    pub(crate) fn with_meta(
        mut self,
        lock: MetaLock,
        takeover: Result<Option<TakeoverRecord>, TakeoverUnusable>,
    ) -> Self {
        self._meta_lock = Some(lock);
        match takeover {
            Ok(Some(takeover)) => self.actor.remember_takeover(takeover),
            Ok(None) => {}
            Err(unusable) => self.emit(&Event::TakeoverRecordUnusable(unusable)),
        }
        self
    }

    /// Writes the takeover the engine asked for before it deletes staged files, and hands
    /// it back, so its next tick may delete them. A write that fails hands nothing back: the
    /// engine's next tick fails `LocalIo` and waits, and deletes nothing unrecorded. A record
    /// the engine spent is deleted first, so a restart cannot hand it back.
    fn keep_takeover(&mut self) {
        if let Some(spent) = self.actor.takeover_spent() {
            match remove_takeover(&self.meta) {
                Ok(()) => self.actor.forget_takeover(spent),
                Err(err) => self.emit(&Event::TakeoverNotRemoved { kind: err.kind() }),
            }
        }
        let Some(record) = self.actor.takeover_wanted() else {
            return;
        };
        match write_takeover(&self.meta, record) {
            Ok(()) => self.actor.remember_takeover(record),
            Err(err) => self.emit(&Event::TakeoverNotRecorded { kind: err.kind() }),
        }
    }

    /// The sink events go to from the first turn, before any [`crate::Replicator::set_events`].
    pub(crate) fn with_sink(mut self, sink: Option<Box<dyn EventSink>>) -> Self {
        self.sink = sink;
        self
    }

    /// The thread's body. Its connections close before the outcome is published, so an application
    /// that waits on the stop finds the database free of this replicator.
    pub(crate) fn run(self) {
        let shared = Arc::clone(&self.shared);
        let mut guard = ExitGuard {
            shared: Arc::clone(&shared),
            worker: Some(self),
        };
        let outcome = guard.worker.as_mut().map(Self::main_loop);
        // Dropped here, before the outcome goes out; the guard then has nothing to report.
        guard.worker = None;
        drop(guard);
        if let Some(outcome) = outcome {
            shared.finish(outcome);
        }
    }

    fn main_loop(&mut self) -> Stopped {
        loop {
            self.take_commands();
            let now = self.runtime.clock.now();
            match self.turn(now) {
                Turn::Again => {}
                Turn::Wait(until) => self.runtime.park.park_from(self.verify_wake(until), now),
                Turn::Done(outcome) => return outcome,
            }
        }
    }

    fn take_commands(&mut self) {
        let commands = self.shared.take_commands();
        let actor = &mut self.actor;
        if commands.snapshot {
            actor.request_snapshot();
        }
        if commands.final_retention {
            actor.request_final_retention();
        }
        if commands.final_snapshot {
            actor.request_final_snapshot();
        }
        if commands.accept_pages {
            actor.accept_database_pages();
        }
        if let Some(sink) = commands.sink {
            self.sink = Some(sink);
        }
        if let Some(deadline) = commands.stop {
            let deadline = self.stop.map_or(deadline, |was| was.min(deadline));
            self.stop = Some(deadline);
            actor.begin_shutdown(deadline);
        }
    }

    /// A wait until `at`, never past a stop's deadline, nor past the retry of a gate that does
    /// not yet hold as judged.
    fn park_until(&self, at: Option<Timestamp>) -> Turn {
        let at = match (at, self.gate_retry) {
            (Some(at), Some(retry)) => Some(at.min(retry)),
            (at, retry) => at.or(retry),
        };
        Turn::Wait(match (at, self.stop) {
            (Some(at), Some(deadline)) => Some(at.min(deadline)),
            (at, deadline) => at.or(deadline),
        })
    }

    /// Moves this thread's own wall-clock waits (the rebuild's, the next poll, the next
    /// scheduled verify) back by a step the clock took back, so each keeps what was left of it
    /// instead of adding the step; the engine's timers follow the clock themselves
    /// (`lsr_core`'s actor clock). A stop's deadline is the caller's instant and stays.
    fn follow_clock(&mut self, now: Timestamp) {
        if let Some(step) = self
            .last_reading
            .map(|last| last.saturating_duration_since(now))
            .filter(|step| !step.is_zero())
        {
            let back = |at: Timestamp| at.checked_sub(step).unwrap_or(now);
            if let Some(rebuilt) = self.rebuilt.as_mut() {
                rebuilt.until = back(rebuilt.until);
            }
            self.next_poll = back(self.next_poll);
            if let Some(scheduled) = self.scheduled.as_mut() {
                scheduled.step_back(back);
            }
        }
        self.last_reading = Some(now);
    }

    fn turn(&mut self, now: Timestamp) -> Turn {
        self.follow_clock(now);
        self.sync_gate(now);
        if let Some(cause) = self.stopped {
            return self.stopped(cause);
        }
        if let Some(rebuilt) = self.rebuilt {
            // No engine runs to see the deadline pass: it ends here.
            if self.stop.is_some_and(|deadline| now >= deadline) {
                return Turn::Done(deadline_reached(self.counted_high()));
            }
            if rebuilt.until > now {
                return self.park_until(Some(rebuilt.until));
            }
        }
        if let Some(scheduled) = self.scheduled.as_mut() {
            scheduled.before_tick(&mut self.actor, now);
        }
        self.shared.update(|state| state.tick_began = Some(now));
        let ticked = catch_unwind(AssertUnwindSafe(|| self.tick(now)));
        self.shared.update(|state| state.tick_began = None);
        match ticked {
            Ok(Ok(tick)) => self.after(tick, now),
            Ok(Err(reopen)) => self.reopen_failed(reopen, now),
            Err(_panic) => self.panicked(now),
        }
    }

    /// The connections, reopened if a panic closed them, while the path still names the
    /// registered file. That is checked before the open, which writes to the file it opens,
    /// and again after it, for a file swapped in between: on both descriptors SQLite opened,
    /// so a swap after either open cannot pass, and on the path, which the WAL is read by.
    fn connection(&mut self) -> Result<Sqlite, Reopen> {
        if let Some(sqlite) = &self.sqlite {
            return Ok(sqlite.clone());
        }
        let registered = |path: &Path| match DatabaseFile::of(path) {
            Ok(file) if file == self.shared.database => Ok(()),
            Ok(_) => Err(Reopen::Replaced),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(Reopen::Replaced),
            Err(_) => Err(Reopen::Failed),
        };
        registered(&self.database)?;
        let sqlite = Sqlite::open(&self.database).map_err(|_open| Reopen::Failed)?;
        match sqlite.database_file() {
            Ok(Some(opened)) if opened == self.shared.database => {}
            Ok(Some(_)) | Err(_) => return Err(Reopen::Replaced),
            Ok(None) => {}
        }
        // The WAL is still read by path, so the path must name the file the descriptors do.
        registered(&self.database)?;
        self.sqlite = Some(sqlite.clone());
        Ok(sqlite)
    }

    /// One engine tick, or why the closed connections could not be reopened.
    fn tick(&mut self, now: Timestamp) -> Result<Tick, Reopen> {
        let sqlite = self.connection()?;
        let (mut wal, mut db) = (sqlite.clone(), sqlite.clone());
        let mut ck = Guarded::new(
            sqlite,
            self.config.readers,
            Arc::clone(&self.shared.readers),
            self.shared.writes_held.clone(),
        );
        let (sink, sink_panics) = (&mut self.sink, &mut self.seen.counters.sink_panics);
        let (shared, held) = (&self.shared, self.gate.is_held());
        let booting = matches!(self.blocked, Some(WhyBlocked::Booting));
        let (why, reported) = (&mut self.why, &mut self.reported);
        // The identity check restores the replica's state and reads every local page, as long
        // as a restore takes, inside this tick: health and the log name it as it starts.
        let mut notice = |event: &EngineEvent| {
            emit_to(sink, sink_panics, &Event::Engine(event.clone()));
            if !(booting && matches!(event, EngineEvent::CheckingIdentity { .. })) {
                return;
            }
            let checking = WhyBlocked::CheckingIdentity;
            let writes = if held {
                Writes::Blocked(checking)
            } else {
                Writes::Blocking(checking)
            };
            shared.update(|state| state.health.writes = writes);
            if held {
                *why = checking;
                *reported = writes;
                emit_to(sink, sink_panics, &Event::WritesBlocked { why: checking });
            }
        };
        let env = Env {
            wal: &mut wal,
            db: &mut db,
            ck: &mut ck,
            staging: &self.staging,
            remote: &self.remote,
            clock: self.runtime.clock.as_ref(),
            notice: &mut notice,
        };
        Ok(block_on(self.actor.tick(env, now)))
    }

    fn after(&mut self, tick: Tick, now: Timestamp) -> Turn {
        self.next_poll = later(now, self.config.poll_interval.get());
        self.seen.counters.ticks = self.seen.counters.ticks.saturating_add(1);
        if let Tick::Ran { event, .. } = &tick {
            self.count(event);
            if let EngineEvent::Booted { .. } = event {
                self.booted();
            }
        }
        let verified = self
            .scheduled
            .as_mut()
            .and_then(|scheduled| scheduled.after_tick(&mut self.actor, now));
        let verify = self
            .scheduled
            .as_ref()
            .and_then(Scheduled::kept_restore_point);
        self.actor.keep_restore_point_for_verify(verify);
        self.keep_takeover();
        // The snapshot is written before the event goes out, so a sink that reads health on an
        // event sees the state after it.
        self.observe(now);
        if let Some(event) = verified {
            self.emit(&event);
        }
        match tick {
            Tick::Ran { event, next } => self.ran(event, next, now),
            Tick::Drained { txid } => Turn::Done(Stopped::Drained { txid }),
            Tick::DeadlineReached(left) => Turn::Done(deadline_reached(left)),
            Tick::Stopped(cause) => self.stopped(cause),
        }
    }

    /// The engine booted: the rebuild is over.
    fn booted(&mut self) {
        self.rebuilt = None;
        self.shared.update(|state| state.booted = true);
    }

    fn ran(&mut self, event: EngineEvent, next: lsr_core::Next, now: Timestamp) -> Turn {
        let idle = event == EngineEvent::Idle;
        // A drain that made progress ticks again at once: the engine reports `Drained` only on
        // the tick after its last upload, and would otherwise name the drain's deadline (or the
        // next poll) as its wake. A retry is not progress: it waits its backoff.
        let progress = match event {
            EngineEvent::Booted { .. }
            | EngineEvent::Synced { .. }
            | EngineEvent::Uploaded(_)
            | EngineEvent::Resnapshot(_)
            | EngineEvent::TakeoverWanted(_)
            | EngineEvent::CheckingIdentity { .. } => true,
            EngineEvent::Checkpointed { .. }
            | EngineEvent::Maintained { .. }
            | EngineEvent::Idle
            | EngineEvent::Degraded { .. }
            | EngineEvent::Retrying { .. }
            | EngineEvent::WritesHeld { .. }
            | EngineEvent::Clamped(_) => false,
        };
        if !idle {
            self.emit(&Event::Engine(event));
        }
        self.report_app();
        self.report_scratch();
        if self.stop.is_some() && progress {
            return Turn::Again;
        }
        match next {
            lsr_core::Next::Now if !idle => Turn::Again,
            lsr_core::Next::Now | lsr_core::Next::OnWalChange => {
                self.park_until(Some(self.next_poll))
            }
            lsr_core::Next::At(at) => self.park_until(Some(at.min(self.next_poll).max(now))),
        }
    }

    /// A stopped engine waits for the stop, which then reports its cause.
    fn stopped(&mut self, cause: StopCause) -> Turn {
        if !std::mem::replace(&mut self.seen.stopped_reported, true) {
            if let Some(through) = self.actor.refused_through() {
                self.emit(&Event::Refused { through });
            }
            self.emit(&Event::Stopped(cause));
        }
        if self.stop.is_some() {
            Turn::Done(Stopped::Stopped(cause))
        } else {
            self.park_until(None)
        }
    }

    fn count(&mut self, event: &EngineEvent) {
        let counters = &mut self.seen.counters;
        let bump = |n: &mut u64| *n = n.saturating_add(1);
        match event {
            EngineEvent::Synced { files } if *files > 0 => bump(&mut counters.syncs),
            EngineEvent::Checkpointed { busy, .. } => {
                bump(&mut counters.checkpoints);
                if *busy {
                    bump(&mut counters.checkpoints_busy);
                }
            }
            EngineEvent::Uploaded(Did::Put(_) | Did::AlreadyPut(_)) => bump(&mut counters.uploads),
            EngineEvent::Retrying { .. } | EngineEvent::Degraded { .. } => {
                bump(&mut counters.retries);
            }
            EngineEvent::Synced { .. }
            | EngineEvent::Uploaded(_)
            | EngineEvent::Booted { .. }
            | EngineEvent::Maintained { .. }
            | EngineEvent::Idle
            | EngineEvent::Resnapshot(_)
            | EngineEvent::Clamped(_)
            // A wait on held writes is not a failure: nothing is counted.
            | EngineEvent::WritesHeld { .. }
            | EngineEvent::TakeoverWanted(_)
            | EngineEvent::CheckingIdentity { .. } => {}
        }
    }

    const fn app_ran_total(&self) -> u64 {
        self.actor.app_checkpoints_ran()
    }

    const fn app_total(&self) -> AppCheckpoints {
        self.actor.app_checkpoints()
    }

    fn report_app(&mut self) {
        let app = self.app_total();
        if let Some(seen) = app.last
            && app.total() > self.seen.app_reported
        {
            self.seen.app_reported = app.total();
            self.emit(&Event::AppCheckpoint {
                seen,
                total: app.total(),
            });
        }
    }

    /// Reports once that the replica refused the boot's sweep of scratch files.
    fn report_scratch(&mut self) {
        if self.actor.scratch_sweep_denied() && !self.seen.scratch_reported {
            self.seen.scratch_reported = true;
            self.emit(&Event::ScratchSweepDenied);
        }
    }

    /// Hands `event` to the sink; a panic in it is counted, never fatal.
    fn emit(&mut self, event: &Event) {
        emit_to(&mut self.sink, &mut self.seen.counters.sink_panics, event);
    }

    /// A caught panic: drop the engine and the connections it may have unwound through, then
    /// rebuild after the wait, or stop, as the configured response says.
    fn panicked(&mut self, now: Timestamp) -> Turn {
        self.seen.counters.panics = self.seen.counters.panics.saturating_add(1);
        self.sqlite = None;
        let rebuild = self.config.responses.bug == Response::KeepTrying;
        self.emit(&Event::Panicked { rebuilt: rebuild });
        if !rebuild {
            self.stopped = Some(StopCause::Bug);
            self.observe(now);
            return self.stopped(StopCause::Bug);
        }
        // The dropped engine's reports and what was asked of it go to the fresh one.
        self.actor = self.actor.rebuilt();
        let until = self.schedule_rebuild(PersistentCause::Bug, now);
        self.observe(now);
        self.park_until(Some(until))
    }

    /// The next rebuild's wait, for `cause`; returns when it ends.
    fn schedule_rebuild(&mut self, cause: PersistentCause, now: Timestamp) -> Timestamp {
        let attempts = self
            .rebuilt
            .map_or(NonZeroU32::MIN, |was| was.attempts.saturating_add(1));
        let since = self
            .rebuilt
            .filter(|was| was.cause == cause)
            .map_or(now, |was| was.since);
        let until = later(now, retry_wait(attempts, self.config.retry_interval.get()));
        self.rebuilt = Some(Rebuilt {
            cause,
            attempts,
            since,
            until,
        });
        until
    }

    /// The connections were not reopened after a panic: wait and try again, or, for a file
    /// that is not the registered one, take `WalUnlinked`'s configured response.
    fn reopen_failed(&mut self, reopen: Reopen, now: Timestamp) -> Turn {
        let cause = match reopen {
            Reopen::Failed => self.rebuilt.map_or(PersistentCause::Bug, |was| was.cause),
            Reopen::Replaced => PersistentCause::WalUnlinked,
        };
        if let Some(cause) = cause.stop().filter(|_| self.config.responses.stops(cause)) {
            self.stopped = Some(cause);
            self.observe(now);
            return self.stopped(cause);
        }
        let until = self.schedule_rebuild(cause, now);
        let replaced = match reopen {
            Reopen::Replaced => true,
            Reopen::Failed => false,
        };
        if let Some(rebuilt) = self.rebuilt.filter(|_| replaced) {
            let event = EngineEvent::Degraded {
                task: Task::Sync,
                cause,
                since: rebuilt.since,
                until,
            };
            self.count(&event);
            self.emit(&Event::Engine(event));
        }
        self.observe(now);
        self.park_until(Some(until))
    }

    /// Writes the health snapshot.
    fn observe(&mut self, now: Timestamp) {
        let measured = self.left();
        let left = match (booted(&self.actor), measured) {
            (Some(engine), Some(left)) => {
                let written = if self.progress.advanced {
                    None
                } else {
                    self.sqlite
                        .clone()
                        .and_then(|mut sqlite| sqlite.modified().ok().flatten())
                };
                self.progress.advance(engine, left, written, now);
                left
            }
            (Some(_) | None, _) => {
                self.progress.unproven(now);
                self.counted_high()
            }
        };
        let (wal_bytes, db_bytes) = self.sizes();
        let fail_stop = self.fail_stop();
        self.keep_gate(now);
        let snapshot = SnapshotParts {
            now,
            phase: self.phase(),
            fail_stop,
            degraded: self.degraded(),
            wal_bytes,
            db_bytes,
            left,
        };
        self.publish(&snapshot);
        self.report_writes();
    }

    fn publish(&self, parts: &SnapshotParts) {
        let resnapshot = self.actor.pending_snapshot();
        let snapshots = self.actor.snapshots();
        let final_retention = self.actor.final_retention();
        let work = self.actor.work();
        let wal_synced_offset = self.actor.synced_wal_offset();
        let final_snapshot = self.actor.final_snapshot();
        let read_lock_violations = self.actor.read_lock_violations();
        let scratch_sweeps_denied = u64::from(self.actor.scratch_sweep_denied());
        let ran = self.app_ran_total();
        let app = self.app_total();
        let verification = self
            .scheduled
            .as_ref()
            .map(Scheduled::health)
            .unwrap_or_default();
        let (seen, progress) = (&self.seen, &self.progress);
        let unreplicated = self.actor.unreplicated_since();
        let writes = self.writes();
        self.shared.update(|state| {
            let health = &mut state.health;
            health.reported_at = parts.now;
            health.phase = parts.phase;
            health.degraded = parts.degraded;
            health.synced = progress.synced;
            health.replicated = progress.replicated;
            // The engine's clock survives a restart (it dates from the oldest pending staged
            // file); this thread's own reading covers what no booted engine has judged yet.
            health.unreplicated_since = match (progress.unreplicated_since, unreplicated) {
                (Some(seen), Some(engine)) => Some(seen.min(engine)),
                (seen, engine) => seen.or(engine),
            };
            health.held_by = parts.fail_stop;
            health.writes = writes;
            health.wal_bytes = parts.wal_bytes;
            health.db_bytes = parts.db_bytes;
            health.resnapshot = resnapshot;
            health.app_checkpoints_ran = ran;
            health.app_checkpoints = app;
            health.wal_synced_offset = wal_synced_offset;
            health.counters = Counters {
                snapshots,
                work,
                read_lock_violations,
                scratch_sweeps_denied,
                ..seen.counters
            };
            health.final_retention = final_retention;
            health.final_snapshot = final_snapshot;
            health.verification = verification;
            state.left = parts.left;
        });
    }

    /// Fail-stop's trip and gate now (`lsr_core::failstop`), and whether the application's
    /// writes are to be held. A stop this thread made itself (a panic or a replaced database
    /// under a stop response) trips as an engine stop does.
    fn fail_stop(&mut self) -> FailStop {
        let trip = match self.stopped {
            Some(cause) => lsr_core::trip(Some(cause), None, None),
            None => self.actor.trip(),
        };
        let judged = self.actor.boot_judged() || trip.is_some();
        let gate = self
            .app_gate
            .judge(self.config.if_replication_stops, trip, judged);
        self.blocked = blocks_writes(self.config.if_replication_stops, gate);
        FailStop { trip, gate }
    }

    /// Takes or lets go of the write lock as last judged. One that fails (an application write
    /// transaction is open, or SQLite failed) is tried again at the next turn, which comes
    /// within one poll interval.
    fn keep_gate(&mut self, now: Timestamp) {
        if let Some(why) = self.blocked {
            self.why = why;
        }
        let hold = match self.blocked {
            Some(WhyBlocked::Booting | WhyBlocked::CheckingIdentity | WhyBlocked::Tripped(_)) => {
                true
            }
            None => false,
        };
        let kept = if hold {
            self.gate.hold()
        } else {
            self.gate.release()
        };
        self.gate_retry = kept
            .is_err()
            .then(|| later(now, self.config.poll_interval.get()));
    }

    /// Whether writes are held now: the lock's own state, with the reason judged.
    fn writes(&self) -> Writes {
        match (self.gate.is_held(), self.blocked) {
            (true, blocked) => Writes::Blocked(blocked.unwrap_or(self.why)),
            (false, Some(why)) => Writes::Blocking(why),
            (false, None) => Writes::Allowed,
        }
    }

    /// The gate made to match the last judgement between ticks: a hold an application
    /// transaction kept out is retried here, and a change shows in health and as an event.
    fn sync_gate(&mut self, now: Timestamp) {
        self.keep_gate(now);
        let writes = self.writes();
        if writes != self.reported {
            self.shared.update(|state| state.health.writes = writes);
            self.report_writes();
        }
    }

    /// Emits `WritesBlocked` or `WritesAllowed` when the lock was taken, its reason changed,
    /// or it was let go since the last one.
    fn report_writes(&mut self) {
        let writes = self.writes();
        let was = std::mem::replace(&mut self.reported, writes);
        let event = match (was, writes) {
            (Writes::Blocked(old), Writes::Blocked(why)) if old == why => None,
            (_, Writes::Blocked(why)) => Some(Event::WritesBlocked { why }),
            (Writes::Blocked(_), Writes::Allowed | Writes::Blocking(_)) => {
                Some(Event::WritesAllowed)
            }
            (Writes::Allowed | Writes::Blocking(_), Writes::Allowed | Writes::Blocking(_)) => None,
        };
        if let Some(event) = event {
            self.emit(&event);
        }
    }

    /// What is not on the replica now, as the engine measures it; `None` when it cannot: no
    /// connection, or a failed read of the wal-index.
    fn left(&self) -> Option<Unfinished> {
        let committed = self.sqlite.clone()?.committed().ok()?;
        let actor = &self.actor;
        let staged = actor.staged().map_or(0, Txid::get);
        let replicated = actor.replicated().map_or(0, Txid::get);
        Some(Unfinished {
            unsynced_wal_bytes: actor.unsynced_bytes(committed),
            staged_not_uploaded: u32::try_from(staged.saturating_sub(replicated))
                .unwrap_or(u32::MAX),
        })
    }

    /// What is left when no booted engine can measure it, counted high: the last measure,
    /// raised to the WAL file's length and to the staged files past the newest transaction
    /// the replica was known to hold.
    fn counted_high(&self) -> Unfinished {
        let kept = self.progress.left;
        let confirmed = self.progress.replicated.map(|(txid, _)| txid);
        let listed = block_on(self.staging.list(Level::ZERO, None, PageLimit::MAX));
        let staged = listed.map_or(0, |listing| {
            let past = listing
                .files
                .iter()
                .filter(|info| Some(info.key.range().max()) > confirmed)
                .count();
            u32::try_from(past).unwrap_or(u32::MAX)
        });
        Unfinished {
            unsynced_wal_bytes: kept.unsynced_wal_bytes.max(self.wal_len()),
            staged_not_uploaded: kept.staged_not_uploaded.max(staged),
        }
    }

    /// The WAL's length: through the connection, or from a stat of its path when there is
    /// none. No descriptor is opened.
    fn wal_len(&self) -> u64 {
        match self.sqlite.clone() {
            Some(mut wal) => wal
                .len()
                .ok()
                .flatten()
                .map_or(0, lsr_core::WalFileLen::bytes),
            None => {
                let mut path = self.database.clone().into_os_string();
                path.push("-wal");
                std::fs::metadata(path).map_or(0, |meta| meta.len())
            }
        }
    }

    /// The WAL's and the database's sizes, from a stat: no descriptor is opened.
    fn sizes(&self) -> (u64, u64) {
        let db = std::fs::metadata(&self.database).map_or(0, |meta| meta.len());
        (self.wal_len(), db)
    }

    fn degraded(&self) -> [Option<Degraded>; 5] {
        let mut tasks = Task::ALL.map(|task| self.actor.degraded(task));
        if let (Some(rebuilt), Some(sync)) = (self.rebuilt, tasks.first_mut()) {
            *sync = Some(Degraded {
                cause: rebuilt.cause,
                since: rebuilt.since,
                retries: rebuilt.attempts.get(),
            });
        }
        tasks
    }

    /// `until`, or the next scheduled verify when that is earlier.
    fn verify_wake(&self, until: Option<Timestamp>) -> Option<Timestamp> {
        let due = self.scheduled.as_ref().and_then(Scheduled::wake);
        match (until, due) {
            (Some(until), Some(due)) => Some(until.min(due)),
            (until, due) => until.or(due),
        }
    }

    fn phase(&self) -> Phase {
        if let Some(cause) = self.stopped {
            return Phase::Stopped(cause);
        }
        match self.actor.phase() {
            EnginePhase::Boot => Phase::Booting,
            EnginePhase::Running => Phase::Running,
            EnginePhase::Draining { .. } => Phase::Draining,
            EnginePhase::Stopped(cause) => Phase::Stopped(cause),
        }
    }
}

/// The parts of a health snapshot computed before it is written.
struct SnapshotParts {
    now: Timestamp,
    phase: Phase,
    fail_stop: FailStop,
    degraded: [Option<Degraded>; 5],
    wal_bytes: u64,
    db_bytes: u64,
    left: Unfinished,
}

/// Hands `event` to `sink`, if there is one; a panic in it is counted in `panics`, never fatal.
fn emit_to(sink: &mut Option<Box<dyn EventSink>>, panics: &mut u64, event: &Event) {
    let Some(sink) = sink.as_mut() else {
        return;
    };
    if catch_unwind(AssertUnwindSafe(|| sink.event(event))).is_err() {
        *panics = panics.saturating_add(1);
    }
}

const fn deadline_reached(left: Unfinished) -> Stopped {
    Stopped::DeadlineReached {
        unsynced_wal_bytes: left.unsynced_wal_bytes,
        staged_not_uploaded: left.staged_not_uploaded,
    }
}

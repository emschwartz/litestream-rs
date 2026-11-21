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
//! with `HaltCause::Bug`. A panic outside that catch ends the thread; its exit guard then
//! reports `Gone::ThreadDied` to every waiter.

use std::num::NonZeroU32;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lsr_core::{
    Actor, ActorConfig, Degraded, Did, Env, Event as EngineEvent, HaltCause, HostCheckpoints, Lane,
    PersistentCause, Phase as EnginePhase, Response, Tick, Unfinished, WalSource, retry_wait,
};
use lsr_ltx::{Timestamp, Txid};
use lsr_sqlite::Sqlite;
use lsr_store_fs::FsStore;

use crate::config::Config;
use crate::events::{Event, EventSink};
use crate::exec::block_on;
use crate::health::{Counters, Gone, Phase, Stopped};
use crate::readers::Guarded;
use crate::replicator::Shared;
use crate::runtime::{Runtime, later};
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

/// The engine was rebuilt after a panic and waits before booting again.
#[derive(Clone, Copy, Debug)]
struct Rebuilt {
    attempts: NonZeroU32,
    since: Timestamp,
    until: Timestamp,
}

/// What the health snapshot remembers between ticks.
#[derive(Debug, Default)]
struct Seen {
    synced: Option<(Txid, Timestamp)>,
    replicated: Option<(Txid, Timestamp)>,
    unreplicated_since: Option<Timestamp>,
    counters: Counters,
    /// Foreign checkpoints counted by engines a panic dropped.
    foreign_before: u64,
    /// The foreign checkpoint total last reported as an event.
    foreign_reported: u64,
    /// Host checkpoints seen by engines a panic dropped.
    host_before: HostCheckpoints,
    /// The host checkpoint total last reported as an event.
    host_reported: u64,
    stopped_reported: bool,
}

/// A fresh engine, told the host's automatic checkpoint threshold.
fn engine(actor_cfg: &ActorConfig, config: &Config) -> Actor {
    let mut actor = Actor::new(actor_cfg.clone());
    actor.set_host_autocheckpoint(config.host_autocheckpoint);
    actor
}

pub(crate) struct Worker {
    shared: Arc<Shared>,
    sqlite: Option<Sqlite>,
    database: PathBuf,
    staging: FsStore,
    remote: Metered,
    actor: Actor,
    actor_cfg: ActorConfig,
    config: Config,
    runtime: Runtime,
    sink: Option<Box<dyn EventSink>>,
    stop: Option<Timestamp>,
    next_poll: Timestamp,
    rebuilt: Option<Rebuilt>,
    /// A panic stopped the engine (`Responses::bug = Stop`).
    bug_stop: bool,
    seen: Seen,
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
    pub(crate) fn new(
        shared: Arc<Shared>,
        sqlite: Sqlite,
        database: &Path,
        meta: &Path,
        store: Arc<dyn DynStore>,
        config: Config,
        runtime: Runtime,
    ) -> Self {
        let actor_cfg = config.actor();
        let now = runtime.clock.now();
        let remote = Metered::new(
            store,
            Arc::clone(&shared.meters),
            Arc::clone(&runtime.clock),
        );
        Self {
            shared,
            sqlite: Some(sqlite),
            database: database.to_owned(),
            staging: FsStore::new(meta),
            remote,
            actor: engine(&actor_cfg, &config),
            actor_cfg,
            next_poll: later(now, config.poll_interval.get()),
            config,
            runtime,
            sink: None,
            stop: None,
            rebuilt: None,
            bug_stop: false,
            seen: Seen::default(),
        }
    }

    /// The thread's body. Its connections close before the outcome is published, so a host
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
                Turn::Wait(until) => self.runtime.park.park(until),
                Turn::Done(outcome) => return outcome,
            }
        }
    }

    fn take_commands(&mut self) {
        let commands = self.shared.take_commands();
        if commands.snapshot {
            self.actor.request_snapshot();
        }
        if commands.accept_pages {
            self.actor.accept_database_pages();
        }
        if let Some(sink) = commands.sink {
            self.sink = Some(sink);
        }
        if let Some(deadline) = commands.stop {
            let deadline = self.stop.map_or(deadline, |was| was.min(deadline));
            self.stop = Some(deadline);
            self.actor.begin_shutdown(deadline);
        }
    }

    fn turn(&mut self, now: Timestamp) -> Turn {
        if self.bug_stop {
            return self.halted(HaltCause::Bug);
        }
        if let Some(rebuilt) = self.rebuilt.filter(|rebuilt| rebuilt.until > now) {
            return self.waiting_to_rebuild(rebuilt, now);
        }
        self.shared.update(|state| state.tick_began = Some(now));
        let ticked = catch_unwind(AssertUnwindSafe(|| self.tick(now)));
        self.shared.update(|state| state.tick_began = None);
        match ticked {
            Ok(Some(tick)) => self.after(tick, now),
            Ok(None) => self.reopen_failed(now),
            Err(_panic) => self.panicked(now),
        }
    }

    /// One engine tick; `None` when the connections are closed and cannot be reopened.
    fn tick(&mut self, now: Timestamp) -> Option<Tick> {
        if self.sqlite.is_none() {
            self.sqlite = Sqlite::open(&self.database).ok();
        }
        let sqlite = self.sqlite.as_ref()?;
        let (mut wal, mut db) = (sqlite.clone(), sqlite.clone());
        let mut ck = Guarded::new(
            sqlite.clone(),
            self.config.readers,
            Arc::clone(&self.shared.readers),
        );
        let env = Env {
            wal: &mut wal,
            db: &mut db,
            ck: &mut ck,
            staging: &self.staging,
            remote: &self.remote,
        };
        Some(block_on(self.actor.tick(env, now)))
    }

    fn after(&mut self, tick: Tick, now: Timestamp) -> Turn {
        self.next_poll = later(now, self.config.poll_interval.get());
        self.seen.counters.ticks = self.seen.counters.ticks.saturating_add(1);
        if let Tick::Ran { event, .. } = &tick {
            self.count(event);
            if let EngineEvent::Booted { .. } = event {
                self.rebuilt = None;
                self.shared.update(|state| state.booted = true);
            }
        }
        // The snapshot is written before the event goes out, so a sink that reads health on an
        // event sees the state after it.
        self.observe(now);
        match tick {
            Tick::Ran { event, next } => self.ran(event, next, now),
            Tick::Drained { txid } => Turn::Done(Stopped::Drained { txid }),
            Tick::DeadlineReached(left) => Turn::Done(deadline_reached(left)),
            Tick::Halted(cause) => self.halted(cause),
        }
    }

    fn ran(&mut self, event: EngineEvent, next: lsr_core::Next, now: Timestamp) -> Turn {
        let idle = event == EngineEvent::Idle;
        // A drain that made progress ticks again at once: the engine reports `Drained` only on
        // the tick after its last upload, and would otherwise name the drain's deadline (or the
        // next poll) as its wake. A retry is not progress: it waits its backoff.
        let progress = matches!(
            event,
            EngineEvent::Booted { .. }
                | EngineEvent::Synced { .. }
                | EngineEvent::Uploaded(_)
                | EngineEvent::Resnapshot(_)
        );
        if !idle {
            self.emit(&Event::Engine(event));
        }
        self.report_foreign();
        self.report_host();
        if self.stop.is_some() && progress {
            return Turn::Again;
        }
        match next {
            lsr_core::Next::Now if !idle => Turn::Again,
            lsr_core::Next::Now | lsr_core::Next::OnWalChange => Turn::Wait(Some(self.next_poll)),
            lsr_core::Next::At(at) => Turn::Wait(Some(at.min(self.next_poll).max(now))),
        }
    }

    /// A stopped engine waits for the stop, which then reports its cause.
    fn halted(&mut self, cause: HaltCause) -> Turn {
        if !std::mem::replace(&mut self.seen.stopped_reported, true) {
            self.emit(&Event::Stopped(cause));
        }
        if self.stop.is_some() {
            Turn::Done(Stopped::Stopped(cause))
        } else {
            Turn::Wait(None)
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
            EngineEvent::Uploaded(Did::Put(_)) => bump(&mut counters.uploads),
            EngineEvent::Retrying { .. } | EngineEvent::Degraded { .. } => {
                bump(&mut counters.retries);
            }
            EngineEvent::Synced { .. }
            | EngineEvent::Uploaded(_)
            | EngineEvent::Booted { .. }
            | EngineEvent::Maintained { .. }
            | EngineEvent::Idle
            | EngineEvent::Resnapshot(_)
            | EngineEvent::Clamped(_) => {}
        }
    }

    fn foreign_total(&self) -> u64 {
        self.seen
            .foreign_before
            .saturating_add(self.actor.foreign_checkpoints())
    }

    fn report_foreign(&mut self) {
        let total = self.foreign_total();
        if total > self.seen.foreign_reported {
            self.seen.foreign_reported = total;
            self.emit(&Event::ForeignCheckpoint { total });
        }
    }

    fn host_total(&self) -> HostCheckpoints {
        self.seen.host_before.then(self.actor.host_checkpoints())
    }

    fn report_host(&mut self) {
        let host = self.host_total();
        if let Some(seen) = host.last
            && host.total() > self.seen.host_reported
        {
            self.seen.host_reported = host.total();
            self.emit(&Event::HostCheckpoint {
                seen,
                total: host.total(),
            });
        }
    }

    /// Hands `event` to the sink; a panic in it is counted, never fatal.
    fn emit(&mut self, event: &Event) {
        let Some(sink) = self.sink.as_mut() else {
            return;
        };
        if catch_unwind(AssertUnwindSafe(|| sink.event(event))).is_err() {
            self.seen.counters.sink_panics = self.seen.counters.sink_panics.saturating_add(1);
        }
    }

    /// A caught panic: drop the engine and the connections it may have unwound through, then
    /// rebuild after the wait, or stop, as the configured response says.
    fn panicked(&mut self, now: Timestamp) -> Turn {
        self.seen.counters.panics = self.seen.counters.panics.saturating_add(1);
        self.seen.foreign_before = self.foreign_total();
        self.seen.host_before = self.host_total();
        self.sqlite = None;
        let rebuild = self.config.responses.bug == Response::KeepTrying;
        self.emit(&Event::Panicked { rebuilt: rebuild });
        if !rebuild {
            self.bug_stop = true;
            self.observe(now);
            return self.halted(HaltCause::Bug);
        }
        self.actor = engine(&self.actor_cfg, &self.config);
        self.seen.foreign_reported = self.seen.foreign_reported.max(self.foreign_total());
        self.seen.host_reported = self.seen.host_reported.max(self.host_total().total());
        if let Some(deadline) = self.stop {
            self.actor.begin_shutdown(deadline);
        }
        self.schedule_rebuild(now);
        self.observe(now);
        Turn::Wait(self.rebuilt.map(|rebuilt| rebuilt.until))
    }

    fn schedule_rebuild(&mut self, now: Timestamp) {
        let attempts = self
            .rebuilt
            .map_or(NonZeroU32::MIN, |was| was.attempts.saturating_add(1));
        let since = self.rebuilt.map_or(now, |was| was.since);
        let wait = retry_wait(attempts, self.config.retry_interval.get());
        self.rebuilt = Some(Rebuilt {
            attempts,
            since,
            until: later(now, wait),
        });
    }

    /// The connections could not be reopened after a panic: wait and try again.
    fn reopen_failed(&mut self, now: Timestamp) -> Turn {
        self.schedule_rebuild(now);
        self.observe(now);
        Turn::Wait(self.rebuilt.map(|rebuilt| rebuilt.until))
    }

    /// Waiting out a rebuild's wait; a stop whose deadline passes meanwhile ends here.
    fn waiting_to_rebuild(&self, rebuilt: Rebuilt, now: Timestamp) -> Turn {
        match self.stop {
            Some(deadline) if now >= deadline => {
                let left = self.shared.update(|state| state.left);
                Turn::Done(deadline_reached(left))
            }
            Some(deadline) => Turn::Wait(Some(rebuilt.until.min(deadline))),
            None => Turn::Wait(Some(rebuilt.until)),
        }
    }

    /// Writes the health snapshot.
    fn observe(&mut self, now: Timestamp) {
        let staged = self.actor.staged();
        let replicated = self.actor.replicated();
        if self.seen.synced.map(|(txid, _)| txid) != staged {
            self.seen.synced = staged.map(|txid| (txid, now));
        }
        if self.seen.replicated.map(|(txid, _)| txid) != replicated {
            self.seen.replicated = replicated.map(|txid| (txid, now));
        }
        let left = self.left();
        let behind = left.unsynced_wal_bytes > 0 || staged > replicated;
        self.seen.unreplicated_since = if behind {
            self.seen.unreplicated_since.or(Some(now))
        } else {
            None
        };
        let (wal_bytes, db_bytes) = self.sizes();
        let degraded = self.degraded();
        let phase = self.phase();
        let snapshot = SnapshotParts {
            now,
            phase,
            degraded,
            wal_bytes,
            db_bytes,
            left,
        };
        self.publish(&snapshot);
    }

    fn publish(&self, parts: &SnapshotParts) {
        let resnapshot = self.actor.pending_snapshot();
        let foreign = self.foreign_total();
        let host = self.host_total();
        let seen = &self.seen;
        self.shared.update(|state| {
            let health = &mut state.health;
            health.observed_at = parts.now;
            health.phase = parts.phase;
            health.degraded = parts.degraded;
            health.synced = seen.synced;
            health.replicated = seen.replicated;
            health.unreplicated_since = seen.unreplicated_since;
            health.wal_bytes = parts.wal_bytes;
            health.db_bytes = parts.db_bytes;
            health.resnapshot = resnapshot;
            health.foreign_checkpoints = foreign;
            health.host_checkpoints = host;
            health.counters = seen.counters;
            state.left = parts.left;
        });
    }

    /// What is not on the replica now.
    fn left(&self) -> Unfinished {
        let mut wal = self.sqlite.clone();
        let unsynced_wal_bytes = wal
            .as_mut()
            .and_then(|wal| wal.committed().ok())
            .map_or(0, |committed| self.actor.unsynced_bytes(committed));
        let staged = self.actor.staged().map_or(0, Txid::get);
        let replicated = self.actor.replicated().map_or(0, Txid::get);
        Unfinished {
            unsynced_wal_bytes,
            staged_not_uploaded: u32::try_from(staged.saturating_sub(replicated))
                .unwrap_or(u32::MAX),
        }
    }

    /// The WAL's and the database's sizes, from a stat: no descriptor is opened.
    fn sizes(&self) -> (u64, u64) {
        let wal = self
            .sqlite
            .clone()
            .and_then(|mut wal| wal.len().ok().flatten())
            .map_or(0, lsr_core::WalFileLen::bytes);
        let db = std::fs::metadata(&self.database).map_or(0, |meta| meta.len());
        (wal, db)
    }

    fn degraded(&self) -> [Option<Degraded>; 5] {
        let mut lanes = Lane::ALL.map(|lane| self.actor.degraded(lane));
        if let (Some(rebuilt), Some(sync)) = (self.rebuilt, lanes.first_mut()) {
            *sync = Some(Degraded {
                cause: PersistentCause::Bug,
                since: rebuilt.since,
                retries: rebuilt.attempts.get(),
            });
        }
        lanes
    }

    fn phase(&self) -> Phase {
        if self.bug_stop {
            return Phase::Stopped(HaltCause::Bug);
        }
        match self.actor.phase() {
            EnginePhase::Boot => Phase::Booting,
            EnginePhase::Running => Phase::Running,
            EnginePhase::Draining { .. } => Phase::Draining,
            EnginePhase::Halted(cause) => Phase::Stopped(cause),
        }
    }
}

/// The parts of a health snapshot computed before it is written.
struct SnapshotParts {
    now: Timestamp,
    phase: Phase,
    degraded: [Option<Degraded>; 5],
    wal_bytes: u64,
    db_bytes: u64,
    left: Unfinished,
}

const fn deadline_reached(left: Unfinished) -> Stopped {
    Stopped::DeadlineReached {
        unsynced_wal_bytes: left.unsynced_wal_bytes,
        staged_not_uploaded: left.staged_not_uploaded,
    }
}

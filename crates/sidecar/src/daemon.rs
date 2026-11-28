//! One process, many databases: a replicator per database, a waiter for
//! databases that do not exist yet, and one drain at the end.
//!
//! **Start.** In config order, per database: open its store (credentials from a source that
//! does not answer yet open a store that asks again with each request, logged and reported
//! by health, as Go's SDK fetches them per request); with
//! `-restore-if-db-not-exists` (or the key) and no file, restore the newest state,
//! a replica with no files being Go's "no backup found, starting fresh"; then
//! `Replicator::start`, which returns once the read transaction pins the
//! WAL; the log sink goes in with it, before the thread runs, so no boot line is lost. A
//! database that fails any of these is logged and left out; the others keep
//! running (Go exits instead), and the exit code says so at the end.
//!
//! **Waiting.** A database whose file does not exist yet is checked again every
//! `monitor-interval` by one waiter thread, which starts it once the file appears
//! (Go `db.go:611-614`). It looks with `metadata`, which opens no descriptor. A start that
//! meets the application writing, or another replicator holding the meta directory's lock
//! (an overlapping deploy, docs/decisions/D178.md), leaves the database waiting the same way
//! when a waiter runs; under `-once` it is a failed start.
//!
//! **Stop.** The first SIGINT, SIGTERM or SIGHUP, the `-exec` child's exit (see
//! [`crate::supervise`]), or with `-once` the start finishing, ends the waiter and
//! asks every replicator to stop within what is left of one deadline,
//! `shutdown-sync-timeout` from that moment: all drain at once, so N databases take
//! the budget once, not N times (Go closes them in turn). A second signal cuts every
//! drain to nothing, and the cut is a state, not a one-off action: a database the waiter
//! starts after it, and every drain the stop asks for after it, gets no time either.
//! Each database's outcome is one log line.
//!
//! **Health.** Every database is on the [`Board`] from its first start attempt, so
//! `/healthz` and `/metrics` see it waiting, failed or running, and still see each
//! replicator's final state while the drain runs.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use lsr_config::{DbPlan, Plan, ReplicaPlan};
use lsr_core::Clock;
use lsr_replica::StartOptions;
use lsr_replica::{
    Config, DynStore, FinalRetention, FinalSnapshot, FinalSnapshotFailure, Gate, IfMissing,
    IfReplicationStops, Replicator, RestoreError, Runtime, StartError, StopOutcome, Trip,
    Unfinished, lock, restore_if_missing,
};

use crate::exec::ChildEnd;
use crate::health::{Board, DbState, Entry};
use crate::logging::{DbEvents, Logger, Value};
use crate::signals;

/// The line a drain that ran out of time logs: the replica holds nothing committed after the
/// transaction it names, so a restore lacks those commits until the next start uploads them.
const NOT_DRAINED: &str = "database not drained: deadline reached, the replica lacks every commit after last_replicated_txid";

/// How one database ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum End {
    /// Its replicator stopped this way.
    Stopped(StopOutcome),
    /// It never started: its store, restore or start failed.
    DidNotStart,
    /// Its file never appeared.
    NeverAppeared,
    /// It drained, through `txid`, but the level-9 snapshot `-force-snapshot` asked for was not
    /// written, for `why`.
    SnapshotNotWritten {
        /// The newest transaction on the replica.
        txid: Option<lsr_ltx::Txid>,
        /// Why not.
        why: FinalSnapshotFailure,
    },
    /// It drained, through `txid`, but the retention pass `-enforce-retention` asked for did not
    /// finish: how it ended instead.
    RetentionNotEnforced {
        /// The newest transaction on the replica.
        txid: Option<lsr_ltx::Txid>,
        /// How the pass ended.
        ended: FinalRetention,
    },
}

/// Every database's end, in config order, and the `-exec` child's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finished {
    /// Each database and how it ended.
    pub dbs: Vec<(PathBuf, End)>,
    /// How the `-exec` child ended, when there was one.
    pub child: Option<ChildEnd>,
    /// Fail-stop held the `-exec` application, and why: it was stopped, or never started
    /// (`crate::failstop`).
    pub app_held: Option<Trip>,
}

impl Finished {
    /// Whether everything committed reached its replica (every replicator drained, none
    /// failed to start; a database that never appeared had nothing to replicate) and the
    /// child, if any, ended cleanly.
    #[must_use]
    pub fn clean(&self) -> bool {
        let dbs = self.dbs.iter().all(|(_, end)| match end {
            End::Stopped(StopOutcome::Drained { .. }) | End::NeverAppeared => true,
            End::Stopped(
                StopOutcome::DeadlineReached(_) | StopOutcome::Stopped(_) | StopOutcome::Gone(_),
            )
            | End::DidNotStart
            | End::SnapshotNotWritten { .. }
            | End::RetentionNotEnforced { .. } => false,
        });
        dbs && self.child.is_none_or(ChildEnd::clean)
    }

    /// Whether `-exec` asked for a child, started or not: such a run exits as Go's does, 0
    /// when clean and 1 otherwise.
    #[must_use]
    pub const fn ran_exec(&self) -> bool {
        self.child.is_some()
    }
}

struct Slot {
    plan: DbPlan,
    store: Option<Arc<dyn DynStore>>,
    restore: bool,
    state: Entry,
    /// The last start failure logged, so a retried database logs each error once.
    last_error: Option<String>,
}

/// Why a start failed: the log message and the error.
struct Failure {
    msg: &'static str,
    error: String,
    /// Whether the cause can pass by itself, so the start is tried again as Go's next sync does.
    transient: bool,
}

/// Every replicator started so far, and whether a second signal has cut the drains.
#[derive(Clone, Debug, Default)]
pub(crate) struct Started(Arc<Mutex<Drains>>);

#[derive(Debug, Default)]
struct Drains {
    replicators: Vec<Replicator>,
    cut: bool,
}

impl Started {
    /// Keeps `replicator` for a cut; one started after the cut is stopped with no time.
    fn add(&self, replicator: &Replicator) {
        let mut drains = lock(&self.0);
        if drains.cut {
            drop(replicator.stop(Duration::ZERO));
        }
        drains.replicators.push(replicator.clone());
    }

    /// Cuts every drain to nothing, now and from now on.
    pub(crate) fn cut(&self) {
        let mut drains = lock(&self.0);
        drains.cut = true;
        for replicator in &drains.replicators {
            drop(replicator.stop(Duration::ZERO));
        }
    }

    /// The time a drain gets: `left`, or nothing once cut.
    #[must_use]
    pub(crate) fn budget(&self, left: Duration) -> Duration {
        if lock(&self.0).cut {
            Duration::ZERO
        } else {
            left
        }
    }
}

/// How the daemon runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// The long-running daemon: a waiter retries a start that fails for a cause that passes by
    /// itself, and each replicator runs its whole maintenance schedule.
    Serving,
    /// `-once`: no waiter, so such a start is a failed start (it would end clean without
    /// replicating otherwise), and no replicator runs maintenance, as Go switches its monitors
    /// off before it opens the store (`docs/decisions/D221.md`).
    Once,
}

/// The daemon over a plan.
pub(crate) struct Daemon {
    mode: Mode,
    log: Arc<Logger>,
    clock: Arc<dyn Clock>,
    started: Started,
    board: Board,
    grace: Duration,
    slots: Arc<Mutex<Vec<Slot>>>,
    /// `if-replication-stops: stop-app`: a gate held while the `-exec` child runs stops it.
    stop_app: bool,
}

impl std::fmt::Debug for Daemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Daemon")
            .field("grace", &self.grace)
            .finish_non_exhaustive()
    }
}

impl Daemon {
    /// Opens every store and starts every database that exists; each appears on `board`.
    /// When this returns, every database that exists has its read transaction held.
    #[must_use]
    pub(crate) fn start(
        plan: &Plan,
        restore_all: bool,
        log: Arc<Logger>,
        started: Started,
        board: Board,
        mode: Mode,
    ) -> Self {
        let daemon = Self {
            mode,
            log,
            clock: Runtime::system().clock,
            started,
            board,
            grace: plan.shutdown_sync_timeout(),
            slots: Arc::new(Mutex::new(Vec::new())),
            stop_app: plan
                .dbs()
                .iter()
                .any(|db| db.config().if_replication_stops == IfReplicationStops::StopApp),
        };
        if plan.dbs().is_empty() {
            daemon
                .log
                .error("no databases specified in configuration", &[]);
        }
        for db in plan.dbs() {
            let restore = db.restore_if_missing() || (restore_all && !db.from_dir());
            let slot = daemon.open(db.clone(), restore);
            let (path, kind) = (db.path().to_owned(), db.replica().kind());
            daemon.board.push(path, kind, slot.state.clone());
            lock(&daemon.slots).push(slot);
        }
        daemon
    }

    fn open(&self, plan: DbPlan, restore: bool) -> Slot {
        let path = plan.path().to_owned();
        let store = match lsr_config::open_store_noting(plan.replica()) {
            Ok(opened) => {
                if let Some(why) = &opened.deferred {
                    // The store asks the chain again with each request, and health reports
                    // the failure meanwhile; this line says why.
                    let fields = [
                        ("path", path.as_path().into()),
                        ("error", why.to_string().into()),
                    ];
                    self.log
                        .warn("credentials not available yet, retrying", &fields);
                }
                Some(opened.store)
            }
            Err(why) => {
                let fields = [
                    ("path", path.as_path().into()),
                    ("error", why.to_string().into()),
                ];
                self.log.error("cannot open replica", &fields);
                None
            }
        };
        let mut slot = Slot {
            plan,
            store,
            restore,
            state: Entry::Failed,
            last_error: None,
        };
        if slot.store.is_some() {
            match self.try_start(&slot, true) {
                Ok(state) => slot.state = state,
                Err(failure) => {
                    // The waiter retries a slot that is waiting, as it does a file that
                    // appears mid-creation.
                    let failure = Failure {
                        transient: failure.transient && self.mode == Mode::Serving,
                        ..failure
                    };
                    if failure.transient {
                        slot.state = Entry::Waiting;
                    }
                    self.failed(&mut slot, failure, false);
                }
            }
        }
        slot
    }

    /// Starts one database if its file exists; `first` logs the wait the first time.
    fn try_start(&self, slot: &Slot, first: bool) -> Result<Entry, Failure> {
        let path = slot.plan.path();
        let Some(store) = &slot.store else {
            return Ok(Entry::Failed);
        };
        if slot.restore && !exists(path) {
            self.restore(path, store, slot.plan.config())?;
        }
        if !exists(path) {
            if first {
                self.log
                    .info("waiting for database", &[("path", path.into())]);
            }
            return Ok(Entry::Waiting);
        }
        // The log sink goes in with the start: one set afterwards can miss the boot's line.
        let mut config = slot.plan.config().clone();
        if self.mode == Mode::Once {
            config.maintenance = config.maintenance.one_off();
        }
        let started = Replicator::start(
            path,
            Arc::clone(store),
            StartOptions {
                config,
                runtime: Some(Runtime::system()),
                events: Some(Box::new(DbEvents::new(Arc::clone(&self.log), path))),
                ..StartOptions::default()
            },
        );
        match started {
            Ok(replicator) => {
                self.started.add(&replicator);
                self.initialized(&slot.plan);
                Ok(Entry::Running(replicator))
            }
            Err(error) => Err(Failure {
                msg: "cannot start database",
                error: error.to_string(),
                transient: passes_by_itself(&error),
            }),
        }
    }

    /// Logs a failed start, once per distinct error for a database that is retried.
    fn failed(&self, slot: &mut Slot, failure: Failure, retried: bool) {
        let retried = retried || failure.transient;
        if slot.last_error.as_ref() == Some(&failure.error) {
            return;
        }
        let fields = [
            ("path", slot.plan.path().into()),
            ("error", failure.error.clone().into()),
            ("retrying", retried.into()),
        ];
        self.log.error(failure.msg, &fields);
        slot.last_error = Some(failure.error);
    }

    fn initialized(&self, plan: &DbPlan) {
        let path = plan.path();
        self.log.info("initialized db", &[("path", path.into())]);
        let kind = ("type", plan.replica().kind().into());
        match plan.replica() {
            ReplicaPlan::File(dir) => {
                let fields = [kind, ("path", dir.as_path().into())];
                self.log.info("replicating to", &fields);
            }
            ReplicaPlan::S3 { config, .. } => {
                let fields = [
                    kind,
                    ("bucket", config.bucket().as_str().into()),
                    ("path", config.prefix().as_str().into()),
                    ("region", config.region().as_str().into()),
                    ("endpoint", config.endpoint().url().as_str().into()),
                ];
                self.log.info("replicating to", &fields);
                // A large part size or concurrency is visible here, where it is set.
                let fields = [
                    ("part_size", config.part_size().get().into()),
                    (
                        "concurrency",
                        u64::try_from(config.concurrency().get())
                            .unwrap_or(u64::MAX)
                            .into(),
                    ),
                    ("max_buffered_bytes", config.max_buffered().into()),
                ];
                self.log
                    .info("multipart upload may hold in memory", &fields);
            }
        }
    }

    /// Go's `restoreIfNeeded`.
    ///
    /// A restore records a trust record through what it restored, in the meta directory
    /// `config` names, so the replicator started on it builds on the replica instead of
    /// refusing it.
    fn restore(
        &self,
        path: &Path,
        store: &Arc<dyn DynStore>,
        config: &Config,
    ) -> Result<(), Failure> {
        self.log.info(
            "attempting restore before replication",
            &[("path", path.into())],
        );
        match restore_if_missing(&**store, path, config) {
            Ok(IfMissing::Restored(restored)) => {
                self.log.info("restore completed", &[("path", path.into())]);
                let msg = format!("takeover recorded through {}", restored.reached());
                self.log.info(&msg, &[("path", path.into())]);
            }
            Ok(IfMissing::Exists) => self.log.info(
                "database exists, skipping restore",
                &[("path", path.into())],
            ),
            Ok(IfMissing::EmptyReplica) => {
                self.log
                    .info("no backup found, starting fresh", &[("path", path.into())]);
            }
            Err(error @ RestoreError::TrustRecord { .. }) => {
                return Err(Failure {
                    msg: "restore completed but the takeover record could not be written",
                    error: error.to_string(),
                    transient: false,
                });
            }
            Err(error) => {
                return Err(Failure {
                    msg: "restore failed",
                    error: error.to_string(),
                    transient: false,
                });
            }
        }
        Ok(())
    }

    /// Starts the waiter for databases whose file does not exist yet, if there are any.
    #[must_use]
    pub(crate) fn wait_for_files(self: &Arc<Self>) -> Option<Waiter> {
        let interval = {
            let slots = lock(&self.slots);
            slots
                .iter()
                .filter(|s| matches!(s.state, Entry::Waiting))
                .map(|s| s.plan.config().poll_interval.get())
                .min()?
        };
        let (stop, stopped) = mpsc::sync_channel(1);
        let daemon = Arc::clone(self);
        let spawned =
            lsr_replica::stack::spawn("lsr-waiter", lsr_replica::stack::ENGINE_STACK, move || {
                daemon.wait_loop(interval, &stopped)
            });
        match spawned {
            Ok(thread) => Some(Waiter { stop, thread }),
            Err(error) => {
                let fields = [("error", error.to_string().into())];
                self.log
                    .error("cannot watch for databases to appear", &fields);
                None
            }
        }
    }

    fn wait_loop(&self, interval: Duration, stopped: &Receiver<()>) {
        loop {
            match stopped.recv_timeout(interval) {
                Err(RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            }
            if !self.retry_waiting() {
                return;
            }
        }
    }

    /// One pass over the waiting databases; whether any still wait.
    fn retry_waiting(&self) -> bool {
        let mut slots = lock(&self.slots);
        let mut left = false;
        for (index, slot) in slots.iter_mut().enumerate() {
            if matches!(slot.state, Entry::Waiting) {
                // A file that appears mid-creation may not open yet, nor one the application
                // holds locked: retry, as Go does.
                match self.try_start(slot, false) {
                    Ok(state) => slot.state = state,
                    Err(failure) => self.failed(slot, failure, true),
                }
                self.board.set(index, slot.state.clone());
                left |= matches!(slot.state, Entry::Waiting);
            }
        }
        left
    }

    /// Drains every running database within what is left of the one deadline begun at
    /// `begun` (the signal, the child's exit, or now), and reports each. Each running one
    /// ends, as asked, with a level-9 snapshot (`-once -force-snapshot`) and then a
    /// retention pass (`-once -enforce-retention`).
    #[must_use]
    pub(crate) fn stop(
        &self,
        waiter: Option<Waiter>,
        begun: lsr_ltx::Timestamp,
        snapshot: bool,
        retention: bool,
    ) -> Finished {
        if let Some(waiter) = waiter {
            waiter.end();
        }
        let slots = std::mem::take(&mut *lock(&self.slots));
        let syncs: Vec<_> = slots
            .iter()
            .map(|slot| match &slot.state {
                Entry::Running(replicator) => {
                    if snapshot {
                        replicator.request_final_snapshot();
                    }
                    if retention {
                        replicator.request_final_retention();
                    }
                    Some(replicator.stop(self.left(begun)))
                }
                Entry::Waiting | Entry::Failed => None,
            })
            .collect();
        let mut dbs = Vec::new();
        for (slot, sync) in slots.iter().zip(syncs) {
            let path = slot.plan.path().to_owned();
            let mut replicated = None;
            let end = match (sync, &slot.state) {
                (Some(sync), Entry::Running(replicator)) => {
                    let stopped = sync.wait();
                    let health = replicator.health();
                    replicated = health.replicated.map(|(txid, _)| txid);
                    ending(stopped, health.final_snapshot, health.final_retention)
                }
                (Some(sync), Entry::Waiting | Entry::Failed) => End::Stopped(sync.wait()),
                (None, Entry::Waiting) => End::NeverAppeared,
                (None, Entry::Failed | Entry::Running(_)) => End::DidNotStart,
            };
            self.ended(&path, &end, replicated);
            dbs.push((path, end));
        }
        Finished {
            dbs,
            child: None,
            app_held: None,
        }
    }

    /// Every running database's fail-stop gate (`crate::failstop`): the `-exec` child's start
    /// and stop wait on them. A database waiting for its file, or that failed to start, has
    /// none: it does not hold the child, as it does not in Go.
    pub(crate) fn gates(&self) -> Vec<(PathBuf, Gate)> {
        self.board
            .views()
            .into_iter()
            .filter_map(|view| match view.state {
                DbState::Running(health) => Some((view.path, health.fail_stop.gate())),
                DbState::Waiting | DbState::Failed => None,
            })
            .collect()
    }

    /// Whether a gate held while the `-exec` child runs stops it.
    pub(crate) const fn stop_app(&self) -> bool {
        self.stop_app
    }

    #[must_use]
    pub(crate) fn now(&self) -> lsr_ltx::Timestamp {
        self.clock.now()
    }

    /// What is left of the shutdown budget begun at `begun`; nothing once a second signal
    /// has cut the drains.
    #[must_use]
    pub(crate) fn left(&self, begun: lsr_ltx::Timestamp) -> Duration {
        let spent = self
            .clock
            .now()
            .unix_millis()
            .saturating_sub(begun.unix_millis());
        let spent = Duration::from_millis(u64::try_from(spent).unwrap_or(0));
        self.started.budget(self.grace.saturating_sub(spent))
    }

    /// Logs how the database at `path` ended. `replicated` is the newest transaction its replica
    /// was confirmed to hold, which a drain that did not finish names: a restore lacks every
    /// commit after it.
    fn ended(&self, path: &Path, end: &End, replicated: Option<lsr_ltx::Txid>) {
        let at = ("path", Value::from(path));
        match end {
            End::Stopped(StopOutcome::Drained { txid }) => {
                let txid = txid.map_or_else(|| "none".to_owned(), |t| t.to_string());
                self.log
                    .info("database drained", &[at, ("txid", txid.into())]);
            }
            End::Stopped(StopOutcome::DeadlineReached(Unfinished {
                unsynced_wal_bytes,
                staged_not_uploaded,
            })) => {
                let replicated = replicated.map_or_else(|| "none".to_owned(), |t| t.to_string());
                let fields = [
                    at,
                    ("last_replicated_txid", replicated.into()),
                    ("unsynced_wal_bytes", (*unsynced_wal_bytes).into()),
                    (
                        "staged_not_uploaded",
                        u64::from(*staged_not_uploaded).into(),
                    ),
                ];
                self.log.error(NOT_DRAINED, &fields);
            }
            End::Stopped(StopOutcome::Stopped(cause)) => {
                let fields = [at, ("cause", format!("{cause:?}").into())];
                self.log
                    .error("database had stopped before the drain", &fields);
            }
            End::Stopped(StopOutcome::Gone(gone)) => {
                let fields = [at, ("cause", format!("{gone:?}").into())];
                self.log.error("database replicator is gone", &fields);
            }
            End::SnapshotNotWritten { txid, why } => {
                let txid = txid.map_or_else(|| "none".to_owned(), |t| t.to_string());
                let fields = [at, ("txid", txid.into()), ("error", why.to_string().into())];
                self.log
                    .error("database drained, snapshot not written", &fields);
            }
            End::RetentionNotEnforced { txid, ended } => {
                let txid = txid.map_or_else(|| "none".to_owned(), |t| t.to_string());
                let fields = [
                    at,
                    ("txid", txid.into()),
                    ("error", retention_text(ended).into()),
                ];
                self.log
                    .error("database drained, retention not enforced", &fields);
            }
            End::DidNotStart => self.log.error("database did not start", &[at]),
            End::NeverAppeared => self.log.info("database never appeared", &[at]),
        }
    }

    #[must_use]
    pub(crate) fn log(&self) -> &Logger {
        &self.log
    }

    /// Logs the signal that began the shutdown.
    pub(crate) fn signalled(&self, signal: i32) {
        let fields = [("signal", signals::name(signal).into())];
        self.log
            .info("signal received, litestream shutting down", &fields);
    }
}

/// The waiter thread and its stop.
#[derive(Debug)]
pub(crate) struct Waiter {
    stop: SyncSender<()>,
    thread: JoinHandle<()>,
}

impl Waiter {
    fn end(self) {
        if self.stop.try_send(()).is_err() {
            // A stop is already queued, or the thread is done.
        }
        if self.thread.join().is_err() {
            // The waiter panicked; its slots stay as they were.
        }
    }
}

/// Whether the database file is there, without opening it.
fn exists(path: &Path) -> bool {
    !matches!(std::fs::symlink_metadata(path), Err(e) if e.kind() == io::ErrorKind::NotFound)
}

/// Whether a start failed for a cause that passes by itself: another connection held a lock
/// SQLite would not wait for (the application writing while the database opens), or another
/// replicator holds the meta directory's lock and will release it when it exits (an
/// overlapping deploy, docs/decisions/D178.md).
fn passes_by_itself(error: &StartError) -> bool {
    match error {
        StartError::Sqlite { code, .. } => code.is_some_and(|code| matches!(code & 0xff, 5 | 6)),
        StartError::MetaLocked => true,
        StartError::Open { .. }
        | StartError::NotWal { .. }
        | StartError::AlreadyReplicated
        | StartError::MetaDir { .. }
        | StartError::MetaLock { .. }
        | StartError::NoFileName
        | StartError::UnsupportedVfs(_)
        | StartError::PageSize { .. }
        | StartError::SqliteBuild { .. }
        | StartError::Thread { .. } => false,
    }
}

/// A drained database's end, with the snapshot and the retention pass it was asked to end
/// with: a snapshot that failed, or a pass that did not finish, is not clean. Retention
/// switched off finishes, as in Go; a pass skipped for a failed snapshot is the snapshot's end.
fn ending(stopped: StopOutcome, snapshot: FinalSnapshot, retention: FinalRetention) -> End {
    match (stopped, snapshot, retention) {
        (StopOutcome::Drained { txid }, FinalSnapshot::Failed(why), _) => {
            End::SnapshotNotWritten { txid, why }
        }
        (
            StopOutcome::Drained { txid },
            FinalSnapshot::NotAsked | FinalSnapshot::Asked { .. } | FinalSnapshot::Written(_),
            ended @ (FinalRetention::Held { .. }
            | FinalRetention::Failed { .. }
            | FinalRetention::Stuck { .. }),
        ) => End::RetentionNotEnforced { txid, ended },
        (
            stopped,
            FinalSnapshot::NotAsked
            | FinalSnapshot::Asked { .. }
            | FinalSnapshot::Written(_)
            | FinalSnapshot::Failed(_),
            FinalRetention::NotAsked
            | FinalRetention::Asked { .. }
            | FinalRetention::Done { .. }
            | FinalRetention::Off
            | FinalRetention::Skipped
            | FinalRetention::Held { .. }
            | FinalRetention::Failed { .. }
            | FinalRetention::Stuck { .. },
        ) => End::Stopped(stopped),
    }
}

/// Why a retention pass did not finish, for the log.
fn retention_text(ended: &FinalRetention) -> String {
    match ended {
        FinalRetention::Held { deleted, hold } => {
            format!("deletions refused after {deleted} keys: {hold}")
        }
        FinalRetention::Failed { deleted, fault } => {
            format!("failed after {deleted} keys: {fault:?}")
        }
        FinalRetention::Stuck { deleted, key } => format!(
            "stopped after {deleted} keys: the listing kept showing {key} after it was deleted"
        ),
        FinalRetention::NotAsked
        | FinalRetention::Asked { .. }
        | FinalRetention::Done { .. }
        | FinalRetention::Off
        | FinalRetention::Skipped => "none".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use lsr_config::{Expand, GoConfig, ProcessEnv};

    use super::*;
    use crate::logging::Logger;

    /// The daemon and an application that calls `lsr_config::load_plan` read one file to the
    /// same databases, replicator settings and stores.
    #[test]
    fn the_daemon_and_load_plan_read_one_file_to_the_same_settings() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("litestream.yml");
        let text = format!(
            "snapshot:\n  interval: 2h\n  retention: 48h\nl0-retention-check-interval: 30s\n\
             shutdown-sync-timeout: 7s\n\
             litestream-rs:\n  app-autocheckpoint: off\n\
             dbs:\n\
             \x20 - path: {root}/a.db\n    truncate-page-n: 5000\n    busy-timeout: 3s\n    restore-if-db-not-exists: true\n\
             \x20   replica:\n      type: s3\n      bucket: b\n      path: p\n      region: us-east-1\n\
             \x20     access-key-id: AKID\n      secret-access-key: SECRET\n      part-size: 8MB\n      concurrency: 3\n\
             \x20 - path: {root}/b.db\n    replica:\n      path: {root}/replica\n",
            root = dir.path().display()
        );
        std::fs::write(&file, text).unwrap();
        let loaded = crate::load_file(&file, lsr_config::Expand::Off).unwrap();
        let daemon = crate::prepare(None, false, &loaded.config).unwrap().plan;
        let library = lsr_config::load_plan(&file, lsr_config::Expand::Off, &ProcessEnv).unwrap();
        assert_eq!(daemon.dbs().len(), 2);
        assert_eq!(library.dbs().len(), daemon.dbs().len());
        assert_eq!(
            library.shutdown_sync_timeout(),
            daemon.shutdown_sync_timeout()
        );
        for (from_daemon, from_library) in daemon.dbs().iter().zip(library.dbs()) {
            assert_eq!(from_library.path(), from_daemon.path());
            assert_eq!(from_library.config(), from_daemon.config());
            assert_eq!(
                from_library.restore_if_missing(),
                from_daemon.restore_if_missing()
            );
            assert_eq!(
                format!("{:?}", from_library.replica()),
                format!("{:?}", from_daemon.replica())
            );
        }
        assert!(daemon.dbs().first().unwrap().restore_if_missing());
    }

    fn daemon(started: &Started) -> Daemon {
        let text = "shutdown-sync-timeout: 30s\ndbs: []\n";
        let loaded = GoConfig::parse(text, Expand::Off, &ProcessEnv).unwrap();
        let plan = lsr_config::plan(&loaded.config, &ProcessEnv).unwrap();
        let log = Arc::new(Logger::to(
            lsr_config::LogLevel::Error,
            lsr_config::LogFormat::Text,
            Runtime::system().clock,
            Box::new(io::sink()),
        ));
        Daemon::start(
            &plan,
            false,
            log,
            started.clone(),
            Board::default(),
            Mode::Serving,
        )
    }

    /// Lines written to a buffer the test reads back.
    #[derive(Clone, Default)]
    struct Lines(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Lines {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .map_err(|_poisoned| io::Error::other("poisoned"))?
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// An S3 replica logs the most memory one buffered upload may hold, so a large part size
    /// or concurrency is visible when the daemon starts (`docs/decisions/D309.md`).
    #[test]
    fn an_s3_replica_logs_what_a_buffered_upload_may_hold() {
        let lines = Lines::default();
        let text = "dbs:\n  - path: /data/a.db\n    replica:\n      type: s3\n      bucket: b\n      path: p\n      \
                    region: us-east-1\n      access-key-id: AKID\n      secret-access-key: SECRET\n      \
                    part-size: 100MiB\n      concurrency: 10\n";
        let loaded = GoConfig::parse(text, Expand::Off, &ProcessEnv).unwrap();
        let plan = lsr_config::plan(&loaded.config, &ProcessEnv).unwrap();
        let log = Arc::new(Logger::to(
            lsr_config::LogLevel::Info,
            lsr_config::LogFormat::Text,
            Runtime::system().clock,
            Box::new(lines.clone()),
        ));
        let empty = GoConfig::parse("dbs: []\n", Expand::Off, &ProcessEnv).unwrap();
        let none = lsr_config::plan(&empty.config, &ProcessEnv).unwrap();
        let daemon = Daemon::start(
            &none,
            false,
            log,
            Started::default(),
            Board::default(),
            Mode::Serving,
        );
        daemon.initialized(plan.dbs().first().unwrap());
        let out = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
        assert!(
            out.contains("msg=\"multipart upload may hold in memory\""),
            "{out}"
        );
        assert!(out.contains("part_size=104857600"), "{out}");
        assert!(out.contains("concurrency=10"), "{out}");
        assert!(out.contains("max_buffered_bytes=1153433600"), "{out}");
    }

    /// A drain cut by its deadline says in words that the replica lacks the newest commits,
    /// and names the newest transaction the replica holds, so an operator knows what a restore
    /// lacks.
    #[test]
    fn a_drain_cut_by_its_deadline_names_the_last_replicated_transaction() {
        let lines = Lines::default();
        let text = "dbs: []\n";
        let loaded = GoConfig::parse(text, Expand::Off, &ProcessEnv).unwrap();
        let plan = lsr_config::plan(&loaded.config, &ProcessEnv).unwrap();
        let log = Arc::new(Logger::to(
            lsr_config::LogLevel::Error,
            lsr_config::LogFormat::Text,
            Runtime::system().clock,
            Box::new(lines.clone()),
        ));
        let daemon = Daemon::start(
            &plan,
            false,
            log,
            Started::default(),
            Board::default(),
            Mode::Serving,
        );
        let end = End::Stopped(StopOutcome::DeadlineReached(Unfinished {
            unsynced_wal_bytes: 115_009_832,
            staged_not_uploaded: 0,
        }));
        daemon.ended(Path::new("/data/big.db"), &end, lsr_ltx::Txid::new(0xa67));
        let out = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
        assert!(
            out.contains(
                "msg=\"database not drained: deadline reached, the replica lacks every commit after last_replicated_txid\""
            ),
            "{out}"
        );
        assert!(
            out.contains("last_replicated_txid=0000000000000a67"),
            "{out}"
        );
        assert!(out.contains("unsynced_wal_bytes=115009832"), "{out}");
    }

    /// An application holding the write lock while the database opens makes the start busy.
    /// The database is retried by the waiter, not left failed for good, and runs once the
    /// application commits (Go sets up its schema again on every sync until it can).
    #[test]
    fn a_start_that_meets_the_application_writing_is_retried() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let app = rusqlite::Connection::open(&db).unwrap();
        app.pragma_update(None, "journal_mode", "wal").unwrap();
        app.execute_batch("CREATE TABLE t (x);").unwrap();
        app.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1);")
            .unwrap();
        let text = format!(
            "dbs:\n  - path: {}\n    replica:\n      url: file://{}\n",
            db.display(),
            dir.path().join("replica").display(),
        );
        let loaded = GoConfig::parse(&text, Expand::Off, &ProcessEnv).unwrap();
        let plan = lsr_config::plan(&loaded.config, &ProcessEnv).unwrap();
        let log = Arc::new(Logger::to(
            lsr_config::LogLevel::Error,
            lsr_config::LogFormat::Text,
            Runtime::system().clock,
            Box::new(io::sink()),
        ));
        let board = Board::default();
        let started = Started::default();
        let daemon = Daemon::start(&plan, false, log, started, board.clone(), Mode::Serving);
        let state = |board: &Board| board.views().remove(0).state;
        assert!(matches!(state(&board), DbState::Waiting));
        assert!(daemon.retry_waiting(), "still locked: still waiting");
        app.execute_batch("COMMIT").unwrap();
        assert!(!daemon.retry_waiting());
        assert!(matches!(state(&board), DbState::Running(_)));
        drop(daemon.stop(None, daemon.now(), false, false));
    }

    /// A second daemon on a meta directory another one holds waits for the lock instead of
    /// failing, and runs once the holder has stopped (an overlapping deploy on one volume).
    #[test]
    fn a_start_on_a_locked_meta_directory_waits_for_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("meta");
        let make = |name: &str| {
            let db = dir.path().join(name);
            let app = rusqlite::Connection::open(&db).unwrap();
            app.pragma_update(None, "journal_mode", "wal").unwrap();
            app.execute_batch("CREATE TABLE t (x);").unwrap();
            let text = format!(
                "dbs:\n  - path: {}\n    meta-path: {}\n    replica:\n      url: file://{}\n",
                db.display(),
                meta.display(),
                dir.path().join("replica").join(name).display(),
            );
            let loaded = GoConfig::parse(&text, Expand::Off, &ProcessEnv).unwrap();
            let plan = lsr_config::plan(&loaded.config, &ProcessEnv).unwrap();
            let log = Arc::new(Logger::to(
                lsr_config::LogLevel::Error,
                lsr_config::LogFormat::Text,
                Runtime::system().clock,
                Box::new(io::sink()),
            ));
            let board = Board::default();
            let daemon = Daemon::start(
                &plan,
                false,
                log,
                Started::default(),
                board.clone(),
                Mode::Serving,
            );
            (app, daemon, board)
        };
        let state = |board: &Board| board.views().remove(0).state;
        let (_old_app, old, old_board) = make("old.db");
        assert!(matches!(state(&old_board), DbState::Running(_)));
        let (_new_app, new, new_board) = make("new.db");
        assert!(matches!(state(&new_board), DbState::Waiting));
        assert!(new.retry_waiting(), "the lock is held: still waiting");
        drop(old.stop(None, old.now(), false, false));
        assert!(!new.retry_waiting());
        assert!(matches!(state(&new_board), DbState::Running(_)));
        drop(new.stop(None, new.now(), false, false));
    }

    fn start_from(text: &str, mode: Mode) -> (Daemon, Board) {
        let loaded = GoConfig::parse(text, Expand::Off, &ProcessEnv).unwrap();
        let plan = lsr_config::plan(&loaded.config, &ProcessEnv).unwrap();
        let log = Arc::new(Logger::to(
            lsr_config::LogLevel::Error,
            lsr_config::LogFormat::Text,
            Runtime::system().clock,
            Box::new(io::sink()),
        ));
        let board = Board::default();
        let daemon = Daemon::start(&plan, false, log, Started::default(), board.clone(), mode);
        (daemon, board)
    }

    fn wal_db(path: &Path) -> rusqlite::Connection {
        let app = rusqlite::Connection::open(path).unwrap();
        app.pragma_update(None, "journal_mode", "wal").unwrap();
        app.execute_batch("CREATE TABLE t (x);").unwrap();
        app
    }

    /// Under `-once` no waiter runs, so a start that meets the application writing, or a
    /// locked meta directory, is a failed start: not clean.
    #[test]
    fn without_a_waiter_a_transient_start_failure_is_a_failed_start() {
        let dir = tempfile::tempdir().unwrap();
        let text = |name: &str, meta: &str| {
            format!(
                "dbs:\n  - path: {}\n    meta-path: {}\n    replica:\n      url: file://{}\n",
                dir.path().join(name).display(),
                dir.path().join(meta).display(),
                dir.path().join("replica").join(name).display(),
            )
        };
        // Busy: the application holds the write lock while the database opens.
        let busy = wal_db(&dir.path().join("busy.db"));
        busy.execute_batch("BEGIN IMMEDIATE; INSERT INTO t VALUES (1);")
            .unwrap();
        let (daemon, board) = start_from(&text("busy.db", "busy-meta"), Mode::Once);
        assert!(matches!(board.views().remove(0).state, DbState::Failed));
        let finished = daemon.stop(None, daemon.now(), false, false);
        assert!(!finished.clean());
        // Locked: another daemon holds the meta directory.
        let _holder = wal_db(&dir.path().join("a.db"));
        let _other = wal_db(&dir.path().join("b.db"));
        let (first, _) = start_from(&text("a.db", "shared-meta"), Mode::Once);
        let (second, board) = start_from(&text("b.db", "shared-meta"), Mode::Once);
        assert!(matches!(board.views().remove(0).state, DbState::Failed));
        let finished = second.stop(None, second.now(), false, false);
        assert!(!finished.clean());
        drop(first.stop(None, first.now(), false, false));
    }

    /// A cut is a state: after a second signal, every drain the stop asks for gets nothing,
    /// including one for a database started after the signal, not the whole deadline.
    #[test]
    fn a_cut_leaves_no_time_for_any_later_drain() {
        let started = Started::default();
        let daemon = daemon(&started);
        let begun = daemon.now();
        assert!(daemon.left(begun) > Duration::from_secs(29));
        started.cut();
        assert_eq!(daemon.left(begun), Duration::ZERO);
        assert_eq!(started.budget(Duration::from_secs(30)), Duration::ZERO);
        assert_eq!(daemon.left(daemon.now()), Duration::ZERO);
    }
}

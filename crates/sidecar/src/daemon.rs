//! One process, many databases: a replicator per database, a waiter for
//! databases that do not exist yet, and one drain at the end.
//!
//! **Start.** In config order, per database: open its store; with
//! `-restore-if-db-not-exists` (or the key) and no file, restore the newest state,
//! a replica with no files being Go's "no backup found, starting fresh"; then
//! `Replicator::start_with_events`, which returns once the read transaction pins the
//! WAL; the log sink goes in with it, before the thread runs, so no boot line is lost. A
//! database that fails any of these is logged and left out; the others keep
//! running (Go exits instead), and the exit code says so at the end.
//!
//! **Waiting.** A database whose file does not exist yet is checked again every
//! `monitor-interval` by one waiter thread, which starts it once the file appears
//! (Go `db.go:611-614`). It looks with `metadata`, which opens no descriptor.
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
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use lsr_config::{DbPlan, Plan, ReplicaPlan};
use lsr_core::{Clock, PlanOutcome, RestoreError as CoreRestoreError};
use lsr_replica::{
    Config, DynStore, Gate, IfReplicationStops, Replicator, RestoreError, Runtime, Stopped, Trip,
    record_takeover, restore_if_missing,
};
use lsr_store_fs::FsStore;
use lsr_store_s3::{CredentialChain, CredentialError, S3Store};

use crate::exec::ChildEnd;
use crate::health::{Board, DbState, Entry};
use crate::logging::{DbEvents, Logger, Value};
use crate::signals;

/// How one database ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum End {
    /// Its replicator stopped this way.
    Stopped(Stopped),
    /// It never started: its store, restore or start failed.
    DidNotStart,
    /// Its file never appeared.
    NeverAppeared,
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
            End::Stopped(Stopped::Drained { .. }) | End::NeverAppeared => true,
            End::Stopped(
                Stopped::DeadlineReached { .. } | Stopped::Stopped(_) | Stopped::Gone(_),
            )
            | End::DidNotStart => false,
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

/// Where one database stands.
enum State {
    Waiting,
    Running(Replicator),
    Failed,
}

impl State {
    fn entry(&self) -> Entry {
        match self {
            Self::Waiting => Entry::Waiting,
            Self::Running(replicator) => Entry::Running(replicator.clone()),
            Self::Failed => Entry::Failed,
        }
    }
}

struct Slot {
    plan: DbPlan,
    store: Option<Arc<dyn DynStore>>,
    restore: bool,
    state: State,
    /// The last start failure logged, so a retried database logs each error once.
    last_error: Option<String>,
}

/// Why a start failed: the log message and the error.
struct Failure {
    msg: &'static str,
    error: String,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Each critical section leaves plain data behind.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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

/// The daemon over a plan.
pub(crate) struct Daemon {
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
    ) -> Self {
        let daemon = Self {
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
            daemon.board.push(path, kind, slot.state.entry());
            lock(&daemon.slots).push(slot);
        }
        daemon
    }

    fn open(&self, plan: DbPlan, restore: bool) -> Slot {
        let path = plan.path().to_owned();
        let store = match open_store(plan.replica()) {
            Ok(store) => Some(store),
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
            state: State::Failed,
            last_error: None,
        };
        if slot.store.is_some() {
            match self.try_start(&slot, true) {
                Ok(state) => slot.state = state,
                Err(failure) => self.failed(&mut slot, failure, false),
            }
        }
        slot
    }

    /// Starts one database if its file exists; `first` logs the wait the first time.
    fn try_start(&self, slot: &Slot, first: bool) -> Result<State, Failure> {
        let path = slot.plan.path();
        let Some(store) = &slot.store else {
            return Ok(State::Failed);
        };
        if slot.restore && !exists(path) {
            self.restore(path, store, slot.plan.config())?;
        }
        if !exists(path) {
            if first {
                self.log
                    .info("waiting for database", &[("path", path.into())]);
            }
            return Ok(State::Waiting);
        }
        // The log sink goes in with the start: one set afterwards can miss the boot's line.
        let started = Replicator::start_with_events(
            path,
            Arc::clone(store),
            slot.plan.config().clone(),
            Runtime::system(),
            Box::new(DbEvents::new(Arc::clone(&self.log), path)),
        );
        match started {
            Ok(replicator) => {
                self.started.add(&replicator);
                self.initialized(&slot.plan);
                Ok(State::Running(replicator))
            }
            Err(error) => Err(Failure {
                msg: "cannot start database",
                error: error.to_string(),
            }),
        }
    }

    /// Logs a failed start, once per distinct error for a database that is retried.
    fn failed(&self, slot: &mut Slot, failure: Failure, retried: bool) {
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
            }
        }
    }

    /// Go's `restoreIfNeeded`.
    ///
    /// A restore records a takeover through what it restored, so the replicator started on
    /// it builds on the replica instead of refusing it: the library writes it in the default
    /// meta directory, and a configured `meta-dir` gets it here too.
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
        match restore_if_missing(&**store, path) {
            Ok(Some(restored)) => {
                self.log.info("restore completed", &[("path", path.into())]);
                let through = restored.reached();
                if config.meta_dir.is_some()
                    && let Some(meta) = config.meta_dir_for(path)
                {
                    record_takeover(&meta, through).map_err(|error| Failure {
                        msg: "restore completed but the takeover record could not be written",
                        error: error.to_string(),
                    })?;
                }
                let msg = format!("takeover recorded through {through}");
                self.log.info(&msg, &[("path", path.into())]);
            }
            Ok(None) => self.log.info(
                "database exists, skipping restore",
                &[("path", path.into())],
            ),
            Err(RestoreError::Restore(CoreRestoreError::NoPlan(none)))
                if *none.outcome() == PlanOutcome::EmptyReplica =>
            {
                self.log
                    .info("no backup found, starting fresh", &[("path", path.into())]);
            }
            Err(error) => {
                return Err(Failure {
                    msg: "restore failed",
                    error: error.to_string(),
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
                .filter(|s| matches!(s.state, State::Waiting))
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
            let mut slots = lock(&self.slots);
            let mut left = false;
            for (index, slot) in slots.iter_mut().enumerate() {
                if matches!(slot.state, State::Waiting) {
                    // A file that appears mid-creation may not open yet: retry, as Go does.
                    match self.try_start(slot, false) {
                        Ok(state) => slot.state = state,
                        Err(failure) => self.failed(slot, failure, true),
                    }
                    self.board.set(index, slot.state.entry());
                    left |= matches!(slot.state, State::Waiting);
                }
            }
            if !left {
                return;
            }
        }
    }

    /// Drains every running database within what is left of the one deadline begun at
    /// `begun` (the signal, the child's exit, or now), and reports each.
    #[must_use]
    pub(crate) fn stop(&self, waiter: Option<Waiter>, begun: lsr_ltx::Timestamp) -> Finished {
        if let Some(waiter) = waiter {
            waiter.end();
        }
        let slots = std::mem::take(&mut *lock(&self.slots));
        let syncs: Vec<_> = slots
            .iter()
            .map(|slot| match &slot.state {
                State::Running(replicator) => Some(replicator.stop(self.left(begun))),
                State::Waiting | State::Failed => None,
            })
            .collect();
        let mut dbs = Vec::new();
        for (slot, sync) in slots.iter().zip(syncs) {
            let path = slot.plan.path().to_owned();
            let end = match (sync, &slot.state) {
                (Some(sync), _) => End::Stopped(sync.wait()),
                (None, State::Waiting) => End::NeverAppeared,
                (None, State::Failed | State::Running(_)) => End::DidNotStart,
            };
            self.ended(&path, &end);
            dbs.push((path, end));
        }
        Finished {
            dbs,
            child: None,
            app_held: None,
        }
    }

    /// The daemon's clock, read now.
    #[must_use]
    /// Every running database's fail-stop gate (`crate::failstop`): the `-exec` child's start
    /// and stop wait on them. A database waiting for its file, or that failed to start, has
    /// none: it does not hold the child, as it does not in Go.
    pub(crate) fn gates(&self) -> Vec<(PathBuf, Gate)> {
        self.board
            .views()
            .into_iter()
            .filter_map(|view| match view.state {
                DbState::Running(health) => Some((view.path, health.held_by.gate)),
                DbState::Waiting | DbState::Failed => None,
            })
            .collect()
    }

    /// Whether a gate held while the `-exec` child runs stops it.
    pub(crate) const fn stop_app(&self) -> bool {
        self.stop_app
    }

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

    fn ended(&self, path: &Path, end: &End) {
        let at = ("path", Value::from(path));
        match end {
            End::Stopped(Stopped::Drained { txid }) => {
                let txid = txid.map_or_else(|| "none".to_owned(), |t| t.to_string());
                self.log
                    .info("database drained", &[at, ("txid", txid.into())]);
            }
            End::Stopped(Stopped::DeadlineReached {
                unsynced_wal_bytes,
                staged_not_uploaded,
            }) => {
                let fields = [
                    at,
                    ("unsynced_wal_bytes", (*unsynced_wal_bytes).into()),
                    (
                        "staged_not_uploaded",
                        u64::from(*staged_not_uploaded).into(),
                    ),
                ];
                self.log
                    .error("database not drained: deadline reached", &fields);
            }
            End::Stopped(Stopped::Stopped(cause)) => {
                let fields = [at, ("cause", format!("{cause:?}").into())];
                self.log
                    .error("database had stopped before the drain", &fields);
            }
            End::Stopped(Stopped::Gone(gone)) => {
                let fields = [at, ("cause", format!("{gone:?}").into())];
                self.log.error("database replicator is gone", &fields);
            }
            End::DidNotStart => self.log.error("database did not start", &[at]),
            End::NeverAppeared => self.log.info("database never appeared", &[at]),
        }
    }

    /// The log.
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

/// Why a replica's store could not be opened. Each variant is the source's own message, so the
/// log line a person reads is unchanged.
#[derive(Debug, thiserror::Error)]
enum OpenStoreError {
    /// No usable S3 credentials.
    #[error("{0}")]
    Credentials(#[from] CredentialError),
    /// The S3 config cannot address its bucket.
    #[error("{0}")]
    Store(#[from] lsr_store_s3::ConfigError),
}

/// The store for a replica; S3 credentials are resolved now, from the file or the
/// environment and shared files (Go copies `LITESTREAM_*` to `AWS_*`; the chain reads both).
fn open_store(replica: &ReplicaPlan) -> Result<Arc<dyn DynStore>, OpenStoreError> {
    match replica {
        ReplicaPlan::File(dir) => Ok(Arc::new(FsStore::new(dir))),
        ReplicaPlan::S3 {
            config,
            credentials,
        } => {
            let chain = match credentials {
                lsr_config::Credentials::FromConfig(given) => {
                    CredentialChain::from_process().with_explicit(given.clone())
                }
                lsr_config::Credentials::FromEnvironment => CredentialChain::from_process(),
            };
            let resolved = chain.resolve()?;
            let store = S3Store::new((**config).clone(), resolved.into_credentials())?;
            Ok(Arc::new(store))
        }
    }
}

#[cfg(test)]
mod tests {
    use lsr_config::{Expand, GoConfig, ProcessEnv};

    use super::*;
    use crate::logging::Logger;

    #[test]
    fn a_store_that_cannot_open_says_why_in_the_sources_words() {
        let error = OpenStoreError::from(lsr_store_s3::ConfigError::BadUrl);
        assert!(matches!(error, OpenStoreError::Store(_)));
        assert_eq!(error.to_string(), "malformed replica url");
        let dir = ReplicaPlan::File(std::path::PathBuf::from("/replica"));
        open_store(&dir).unwrap();
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
        Daemon::start(&plan, false, log, started.clone(), Board::default())
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

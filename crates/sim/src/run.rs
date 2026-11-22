//! The run loop: one seed, one history, one verdict.
//!
//! [`run`] builds a [`World`] and an [`Actor`], applies each operation of the
//! seed's history in order, and judges the replica at every `Restore`, after
//! every drain and once at the end. The actor is driven the way an application would
//! drive it: each tick's future goes through [`World::run_tick`], the clock
//! follows the `Next` it returns, and a crash drops the actor and builds a new
//! one. A failure names the seed and the step; [`shrink`] finds the shortest
//! prefix that still fails.

use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use lsr_core::{
    Actor, ActorConfig, AppCheckpoints, CheckpointConfig, Clock, Entropy, Env, Event, Fault,
    MAX_BACKOFF, Maintenance, Next, PersistentCause, Responses, RetentionMode, SeededEntropy,
    StopCause, SyncConfig, Task, Tick, UploadConfig,
};
use lsr_ltx::{PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{Salts, WalOffset};

use crate::crash_law::CrashLaw;
use crate::generate::{
    AfterRestart, EnvFault, MetaOp, Op, RemoteOp, Target, WalOp, Weights, history,
};
use crate::liveness::Justifier;
use crate::oracle::{Bound, Incarnation, Violation, Windows, bug_fault, judge_put, within};
use crate::reach::{Reach, variant};
use crate::resources::{
    CheckpointTally, CountingCheckpointer, IdleWindow, Limits, TickStats, delta, memory, per_tick,
};
use crate::scenario::{CauseKind, FaultKind};
use crate::seed::{Seed, replay_line};
use crate::sqlite::{AutoTally, CrashKind, ReaderId};
use crate::world::{CrashPlan, StagedCopy, StagedEditError, TickEnd, World};

/// Ticks a drain may take before it counts as stuck.
const DRAIN_TICKS: u32 = 10_000;
/// Ticks after a boot that may pass before the engine is idle.
const SETTLE_TICKS: u32 = 200;
/// What a tick that reports Idle but asks for another at once costs in virtual
/// time: the driver's loop is not free, and the retry waits it spins through
/// last seconds (a retry wait answers `Idle` with `Next::Now`).
const SPIN_STEP: Duration = Duration::from_millis(50);
/// Idle ticks the idle property watches.
const IDLE_TICKS: u32 = 100;
/// The WAL bytes a sync step stops at: small, so a step often splits a backlog.
const SYNC_CAP: u64 = 1 << 14;
/// Frames at which the engine truncates.
const TRUNCATE_PAGES: u32 = 32;
/// Frames at which the engine runs a passive checkpoint.
const PASSIVE_PAGES: u32 = 8;
/// The first page size of every run.
const FIRST_PAGE_SIZE: u32 = 512;
/// The first database size of every run, in pages.
const FIRST_DB_PAGES: u32 = 8;

/// A run that broke a property.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    /// The seed.
    pub seed: Seed,
    /// The operation being applied; the history's length for a failure in the
    /// final drain or check.
    pub step: u32,
    /// The history length that reproduces the failure: one past the failing
    /// step, or the whole history for a failure in the final drain or check.
    pub steps: u32,
    /// What broke.
    pub violation: Violation,
    /// The operations and ticks up to the failure, when the run was asked to
    /// trace.
    pub trace: Vec<String>,
}

impl Failure {
    /// The message a failing test prints: the failure, then how to replay it in
    /// `steps` operations.
    #[must_use]
    pub fn message(&self, steps: u32) -> String {
        format!(
            "seed 0x{} failed at step {}: {}\nreplay: {}",
            self.seed,
            self.step,
            self.violation,
            replay_line(self.seed, Some(steps))
        )
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message(self.steps))
    }
}

/// What the engine or the oracle did that a scenario can expect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Seen {
    /// A tick's event.
    Event(Event),
    /// A tick that stopped the engine.
    Stop(StopCause),
    /// A task that was degraded is not any more: its work succeeded on the
    /// same engine, with no restart between.
    Recovered(Task),
    /// A snapshot file was staged, taking this transaction.
    Snapshot(Txid),
    /// The oracle judged the replica and found nothing wrong.
    Oracle,
    /// A tick ran: the clock when it began, and the calls it made to the
    /// replica. Recorded before the tick's own event.
    Tick {
        /// The clock when the tick began.
        at: Timestamp,
        /// List, read, begin, commit and delete calls to the replica.
        remote_calls: u64,
        /// How many of them were listings.
        remote_lists: u64,
    },
}

/// What a passing run did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunReport {
    /// Operations applied.
    pub steps: u32,
    /// Ticks the actor ran, crashed ones included.
    pub ticks: u32,
    /// Crashes of either kind.
    pub crashes: u32,
    /// Power losses among them.
    pub power_losses: u32,
    /// Drains the operator asked for.
    pub shutdowns: u32,
    /// Commits the app made inside a tick, before a checkpoint's barrier.
    pub commits_in_tick: u32,
    /// Oracle restores of the whole replica.
    pub checks: u32,
    /// The longest the WAL grew, in frames.
    pub max_wal_frames: u32,
    /// Idle ticks that asked to run again at once: a real driver would spin on
    /// them. Each costs the virtual clock `SPIN_STEP`.
    pub spins: u32,
    /// Each operation, when the run was asked to trace.
    pub trace: Vec<String>,
    /// Each event, stop and oracle verdict in order, when the run was asked to
    /// record them.
    pub seen: Vec<Seen>,
    /// Every commit and delete the replica accepted, when the run was asked to
    /// record.
    pub remote_events: Vec<crate::store::RecorderEvent>,
    /// Every commit and delete staging accepted, when the run was asked to
    /// record.
    pub staging_events: Vec<crate::store::RecorderEvent>,
    /// The replica's objects a corrupt publish changed, when the run was
    /// asked to record.
    pub remote_damaged: Vec<lsr_core::FileKey>,
    /// The replica's objects a commit published and then reported as failed
    /// or wrong (the engine may delete them as untrusted), when the run was
    /// asked to record.
    pub remote_rejected: Vec<lsr_core::FileKey>,
    /// What every engine of the run saw of the app's checkpoints.
    pub app_checkpoints: AppCheckpoints,
    /// The app's automatic checkpoints, as the model counted them.
    pub app_autocheckpoints: AutoTally,
    /// What the run reached, by name; see [`crate::reach`].
    pub reach: Reach,
}

/// What a run may be told besides its seed.
#[derive(Clone, Debug, Default)]
pub struct Opts {
    /// A violation to raise just before this step, to test the shrinker.
    pub plant: Option<(u32, Violation)>,
    /// Record each operation in [`RunReport::trace`].
    pub trace: bool,
    /// Record what the engine and the oracle did in [`RunReport::seen`].
    pub record: bool,
    /// The scenario's persistent fault is expected: a stop for it ends the
    /// run's ticks, and a task it keeps degraded ends a drain or the idle
    /// check, without being a bug.
    pub allow_stop: bool,
    /// What each persistent fault does; the default keeps trying.
    pub responses: Responses,
    /// Whether the engine's retention deletes, from the first boot; the
    /// default enforces it, as Go does.
    pub retention: RetentionMode,
    /// Switches retention just before this step: on the running engine, as
    /// an operator would, and for every engine booted after it.
    pub switch_retention: Option<(u32, RetentionMode)>,
    /// The operations were drawn, not written: a step with nothing to act on
    /// is skipped, and the model is checked against SQLite's rules before
    /// every step. [`run_with`] sets it.
    pub drawn: bool,
    /// With `drawn`, a power loss keeps the block law's draw instead of a
    /// drawn prefix ([`Weights::block_crash_law`]). [`run_with`] sets it.
    pub block_crash_law: bool,
    /// With `drawn`, the disk lies about the engine's fsync of the WAL from
    /// the start ([`Weights::lying_wal_sync`]). [`run_with`] sets it.
    pub lying_wal_sync: bool,
    /// What the app does after each restart ([`Weights::after_restart`]).
    /// [`run_with`] sets it.
    pub after_restart: AfterRestart,
    /// Which judges run: the oracle, the history judge, or both (the default).
    pub judges: Judges,
}

/// Which judges a run consults. Both by default; one alone measures what it catches unaided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Judges {
    /// The oracle ([`crate::oracle`]): restores judged through the engine's WAL origins.
    pub oracle: bool,
    /// The history judge ([`crate::history`]): restores judged by content alone.
    pub history: bool,
}

impl Default for Judges {
    fn default() -> Self {
        Self {
            oracle: true,
            history: true,
        }
    }
}

/// The configuration every run gives the actor.
#[must_use]
pub fn actor_config() -> ActorConfig {
    ActorConfig {
        sync: SyncConfig {
            cap: NonZeroU64::new(SYNC_CAP),
            ..SyncConfig::default()
        },
        checkpoint: CheckpointConfig {
            interval: Some(Duration::from_secs(60)),
            min_pages: NonZeroU32::new(PASSIVE_PAGES),
            truncate_pages: NonZeroU32::new(TRUNCATE_PAGES),
        },
        upload: UploadConfig::default(),
        maintenance: Maintenance::go_default(),
        retry_interval: Duration::from_secs(1),
        shutdown_grace: Duration::from_secs(10),
        responses: Responses::GO_LIKE,
    }
}

/// How long `maintenance` keeps snapshots and level-0 files.
fn windows(maintenance: &Maintenance) -> Windows {
    Windows {
        snapshots: maintenance.snapshot_retention(),
        level_zero: match maintenance.l0_retention() {
            lsr_core::L0Retention::After(window) => Some(window),
            lsr_core::L0Retention::Keep => None,
        },
    }
}

/// What one actor step came to.
#[derive(PartialEq, Eq)]
enum Step {
    Ran,
    /// A task kept trying a persistent fault.
    Degraded(PersistentCause),
    /// A drain finished, naming the newest transaction on the replica.
    Drained(Option<Txid>),
    Stopped(StopCause),
    Crashed,
}

struct Runner {
    w: World,
    actor: Actor,
    rng: SeededEntropy,
    spilled: Vec<(PageNo, Vec<u8>)>,
    /// A freshly started driver has not seen the WAL yet, so it ticks once more
    /// before it believes a wait on a WAL change.
    unwatched: bool,
    readers: Vec<ReaderId>,
    tally: CheckpointTally,
    last_polls: u32,
    staging_seen: usize,
    max_tx_pages: u64,
    report: RunReport,
    trace: bool,
    /// The last tick waits on a WAL change alone: no timer is pending.
    quiet: bool,
    record: bool,
    allow_stop: bool,
    responses: Responses,
    /// Whether retention deletes, for the running engine and the next boot.
    retention: RetentionMode,
    /// Which tasks the current engine reported degraded after its last tick.
    degraded: [bool; 5],
    /// The latest time the current engine has been told: its retry waits are
    /// measured from it, since an earlier reading is clamped.
    latest: Option<Timestamp>,
    saved: Option<StagedCopy>,
    /// The commit each file in `saved` held when it was copied, noted again
    /// when the copy is put back.
    saved_notes: Vec<(Txid, Option<crate::model::WalEnd>, crate::CommitId)>,
    /// The app's automatic checkpoint threshold each engine is told.
    app_autocheckpoint: Option<NonZeroU32>,
    /// What engines replaced so far saw of the app's checkpoints.
    app_before: AppCheckpoints,
    /// Persistent causes an operation the history drew makes legitimate: a
    /// stop or a degraded task for one of them is not a bug.
    expected: Vec<StopCause>,
    /// A drawn step that has nothing to act on is skipped, not a failure.
    lenient: bool,
    /// Page sizes the database had before a drawn resize: an engine that
    /// remembers one may stop on the change.
    expected_sizes: Vec<PageSize>,
    /// The engine incarnation running now: what its level-0 puts are judged
    /// against ([`judge_put`]).
    life: Incarnation,
    /// Replica recorder events already judged by [`judge_put`].
    puts_judged: usize,
    /// The staging recorder's length when a staging backup was last put
    /// back: staged files are the engine's only if it wrote them since.
    put_back: usize,
    /// A store fault fails calls at a rate and has not healed: a drain may
    /// end at its deadline.
    rate_active: bool,
    /// Boots that listed the replica and failed (each is tried again, and
    /// costs the replica a boot's reads).
    failed_boots: u32,
    /// Bucket rollbacks and removals of the newest file: each may cost the
    /// engine a probe past a listing that ends before what it confirmed, and
    /// a relist once it learns of the loss.
    regressions: u32,
    /// The snapshot liveness oracle.
    liveness: crate::liveness::Liveness,
    judges: Judges,
    /// What the app does after a restart, and its own log of commits.
    app: after_restart::App,
    /// The WAL's committed end, in frames, as the current operation began.
    op_began_at: u32,
}

fn below(rng: &SeededEntropy, upper: u64) -> u64 {
    rng.below(upper.max(1))
}

fn small(rng: &SeededEntropy, upper: u64) -> u32 {
    u32::try_from(below(rng, upper)).unwrap_or(u32::MAX)
}

impl Runner {
    fn new(seed: Seed) -> Self {
        let page_size = PageSize::new(FIRST_PAGE_SIZE).unwrap_or(PageSize::MIN);
        let mut w = World::new(seed, page_size, FIRST_DB_PAGES);
        w.start_driver();
        Self {
            w,
            actor: Actor::new(actor_config()),
            rng: SeededEntropy::new(seed.stream(11)),
            spilled: Vec::new(),
            unwatched: true,
            readers: Vec::new(),
            tally: CheckpointTally::default(),
            last_polls: 4,
            staging_seen: 0,
            max_tx_pages: 1,
            report: RunReport::default(),
            trace: false,
            quiet: false,
            record: false,
            allow_stop: false,
            responses: Responses::GO_LIKE,
            retention: RetentionMode::Enforce,
            degraded: [false; 5],
            latest: None,
            saved: None,
            saved_notes: Vec::new(),
            app_autocheckpoint: Some(lsr_core::SQLITE_AUTOCHECKPOINT),
            app_before: AppCheckpoints::NONE,
            expected: Vec::new(),
            lenient: false,
            expected_sizes: Vec::new(),
            life: Incarnation::default(),
            puts_judged: 0,
            put_back: 0,
            rate_active: false,
            failed_boots: 0,
            regressions: 0,
            liveness: crate::liveness::Liveness::new(),
            judges: Judges::default(),
            app: after_restart::App::new(seed),
            op_began_at: 0,
        }
    }

    fn now(&self) -> Timestamp {
        self.w.clock.now()
    }

    fn reboot(&mut self) {
        self.life = Incarnation {
            staged_from: self.w.staging_recorder.len(),
            ..Incarnation::default()
        };
        self.w.start_driver();
        let base = actor_config();
        self.app_before = self.app_seen();
        self.actor = Actor::new(ActorConfig {
            responses: self.responses,
            maintenance: base.maintenance.clone().with_retention(self.retention),
            ..base
        });
        self.actor.set_app_autocheckpoint(self.app_autocheckpoint);
        self.unwatched = true;
        self.degraded = [false; 5];
        self.latest = None;
        self.liveness.rebooted();
    }

    fn app_seen(&self) -> AppCheckpoints {
        self.app_before.then(self.actor.app_checkpoints())
    }

    fn limits(&self) -> Limits {
        let size = u64::try_from(self.w.sqlite.page_size().bytes()).unwrap_or(u64::MAX);
        let tx = self.max_tx_pages.saturating_mul(size.saturating_add(64));
        let db = u64::from(self.w.sqlite.db_size()).saturating_mul(size.saturating_add(64));
        Limits {
            staged_bytes: SYNC_CAP.saturating_add(tx).max(db).saturating_add(4096),
        }
    }

    fn run_one(&mut self, plan: Option<CrashPlan>) -> TickEnd<Tick> {
        let mut wal = self.w.sqlite.source();
        let mut db = self.w.sqlite.pages();
        let mut ck = CountingCheckpointer::new(self.w.sqlite.checkpointer(), self.tally.clone());
        let staging = self.w.staging.clone();
        let remote = self.w.remote.clone();
        let clock = std::sync::Arc::clone(&self.w.clock);
        let now = self.now();
        self.w.high.note(now);
        let actor = &mut self.actor;
        let fut = async {
            let env = Env {
                wal: &mut wal,
                db: &mut db,
                ck: &mut ck,
                staging: &staging,
                remote: &remote,
                clock: clock.as_ref(),
            };
            actor.tick(env, now).await
        };
        self.w.run_tick(fut, plan)
    }

    /// Tells the model about each snapshot the tick staged: the database it was
    /// cut from is the newest commit as the tick began, since the app does nothing during
    /// a tick, unless the app committed inside it (`in_tick`, with the staging
    /// event it came before): from that event on, it is that commit.
    fn note_snapshots(
        &mut self,
        began: Option<crate::CommitId>,
        in_tick: Option<(crate::CommitId, usize)>,
        at: usize,
    ) {
        let events = self.w.staging_recorder.events();
        for (index, event) in events.iter().enumerate().skip(self.staging_seen) {
            let newest_commit = match in_tick {
                Some((commit, from)) if index >= from => Some(commit),
                Some(_) | None => began,
            };
            if let crate::RecorderEvent::Put {
                key,
                header: Some(_),
                origin: Some(origin),
                ..
            } = event
                && origin.size() > 0
                && key.level().get() == 0
            {
                let (salt1, salt2) = origin.salts();
                let end = origin.offset().saturating_add(origin.size());
                self.w
                    .model
                    .note_stage(key.range().max(), Salts { salt1, salt2 }, end, at);
            }
            if let crate::RecorderEvent::Put {
                key,
                header: Some(_),
                origin,
                ..
            } = event
                && origin.is_none_or(|origin| origin.size() == 0)
                && key.level().get() == 0
                && let Some(commit) = newest_commit
            {
                let rewound = self.w.model.rewound();
                self.w
                    .model
                    .note_snapshot(key.range().max(), commit, rewound, at);
                if self.record {
                    self.report.seen.push(Seen::Snapshot(key.range().max()));
                }
            } else if self.record
                && let crate::RecorderEvent::Put {
                    key,
                    header: Some(_),
                    ..
                } = event
                && key.level().get() == 0
                && self.w.staged_is_full_image(key)
            {
                self.report.seen.push(Seen::Snapshot(key.range().max()));
            }
        }
        self.staging_seen = events.len();
    }

    fn follow(&self, next: Next) {
        match next {
            Next::Now => {}
            Next::At(at) => {
                if at > self.now() {
                    self.w.clock.set(at);
                }
            }
            Next::OnWalChange => self.w.clock.advance(Duration::from_secs(1)),
        }
    }

    fn crashed(&mut self, kind: CrashKind) {
        if kind == CrashKind::Power {
            self.liveness.note(Justifier::PowerLoss);
        }
        self.reboot();
        self.app.restarted(self.op_began_at);
        self.report.reach.bump(format!("crash.{}", variant(&kind)));
        self.report.crashes = self.report.crashes.saturating_add(1);
        if kind == CrashKind::Power {
            self.report.power_losses = self.report.power_losses.saturating_add(1);
            self.spilled.clear();
            self.readers.clear();
        }
    }

    /// Judges every level-0 put the replica took since the last call as
    /// made by the incarnation running now ([`judge_put`]).
    fn judge_puts(&mut self) -> Result<(), Violation> {
        let remote = self.w.remote_recorder.events();
        let staging = self.w.staging_recorder.events();
        for put in remote.iter().skip(self.puts_judged) {
            let judged = judge_put(&self.w.model, &staging, &self.w.remote_raw, put, self.life)?;
            self.report.reach.bump(format!("witness.{judged:?}"));
        }
        self.puts_judged = remote.len();
        Ok(())
    }

    /// Every level-0 file this tick staged from the WAL holds frames that are durable: the
    /// engine's sync before staging, which a disk only fails to honour when it lies (that
    /// scenario says so) or after an earlier failed sync of the same generation.
    fn judge_staged_durable(&self) -> Result<(), Violation> {
        let events = self.w.staging_recorder.events();
        for event in events.iter().skip(self.staging_seen) {
            if let crate::RecorderEvent::Put {
                key,
                header: Some(_),
                origin: Some(origin),
                ..
            } = event
                && origin.size() > 0
                && key.level().get() == 0
            {
                let (salt1, salt2) = origin.salts();
                let end = origin.offset().saturating_add(origin.size());
                if !self
                    .w
                    .sqlite
                    .wal_range_durable(Salts { salt1, salt2 }, origin.offset(), end)
                {
                    return Err(Violation::StagedNotDurable {
                        key: key.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn note_tick(&mut self, began: Timestamp, remote0: crate::store::RecorderCounts) {
        if !self.record {
            return;
        }
        let spent = |c: crate::store::RecorderCounts| {
            [c.lists, c.reads, c.begins, c.commits, c.deletes]
                .into_iter()
                .fold(0_u64, u64::saturating_add)
        };
        let now = self.w.remote_recorder.counts();
        self.report.seen.push(Seen::Tick {
            at: began,
            remote_calls: spent(now).saturating_sub(spent(remote0)),
            remote_lists: now.lists.saturating_sub(remote0.lists),
        });
    }

    /// Counts a tick that began unbooted, listed the replica whole and ended
    /// unbooted: a boot that failed after its listing.
    fn note_failed_boot(&mut self, booting: bool, lists_before: u64) {
        let levels = u64::try_from(lsr_core::Level::ALL.len()).unwrap_or(u64::MAX);
        let lists = self.w.remote_recorder.counts().lists;
        if booting && lists.saturating_sub(lists_before) >= levels && !self.actor.booted() {
            self.failed_boots = self.failed_boots.saturating_add(1);
        }
    }

    fn step(&mut self, plan: Option<CrashPlan>) -> Result<(Step, TickStats), Violation> {
        let at = self.w.remote_recorder.len();
        let newest_commit = self.w.model.newest_commit();
        let remote0 = self.w.remote_recorder.counts();
        let staging0 = self.w.staging_recorder.counts();
        let passive0 = self.tally.count(lsr_core::CheckpointMode::Passive);
        let total0 = self.tally.total();
        let began = self.now();
        let hits0 = self.fault_hits();
        let booting = !self.actor.booted();
        self.liveness_before_tick(booting);
        let salts = self.w.sqlite.salts();
        let commits = self.w.model.commits().len();
        let ended = self.run_one(plan);
        self.note_failed_boot(booting, remote0.lists);
        if booting && self.actor.booted() {
            self.life.boot_salts = Some(salts);
            self.life.boot_commits = commits;
        }
        self.judge_stores()?;
        let crashed = matches!(ended, TickEnd::Crashed { .. });
        self.liveness_mid_tick(hits0, crashed)?;
        self.report.ticks = self.report.ticks.saturating_add(1);
        let in_tick = self.took_commit_in_tick();
        self.note_snapshots(newest_commit, in_tick, at);
        self.note_tick(began, remote0);
        if self.trace {
            self.report.trace.push(format!("    tick: {ended:?}"));
        }
        let (step, event) = match ended {
            TickEnd::Done(Tick::Ran { event, next }) => {
                let step = self.ran(&event, next, began)?;
                (step, Some(event))
            }
            TickEnd::Done(Tick::Stopped(cause)) => {
                self.report.reach.bump(format!("stop.{}", variant(&cause)));
                if self.record {
                    self.report.seen.push(Seen::Stop(cause));
                }
                (Step::Stopped(cause), None)
            }
            TickEnd::Done(Tick::Drained { txid }) => (Step::Drained(txid), None),
            TickEnd::Done(Tick::DeadlineReached(_)) => {
                let cause = StopCause::Shutdown;
                if self.record {
                    self.report.seen.push(Seen::Stop(cause));
                }
                (Step::Stopped(cause), None)
            }
            TickEnd::Crashed {
                polls, kind, lost, ..
            } => {
                self.note_lost(&lost);
                self.last_polls = polls.max(1);
                self.crashed(kind);
                (Step::Crashed, None)
            }
        };
        self.liveness_after_tick(self.actor.snapshots(), true, !crashed)?;
        let stats = TickStats {
            event,
            remote: delta(remote0, self.w.remote_recorder.counts()),
            staging: delta(staging0, self.w.staging_recorder.counts()),
            passive: self
                .tally
                .count(lsr_core::CheckpointMode::Passive)
                .saturating_sub(passive0),
            checkpoints: self.tally.total().saturating_sub(total0),
        };
        per_tick(&stats, self.limits())?;
        self.report.max_wal_frames = self.report.max_wal_frames.max(self.w.sqlite.mx_frame());
        Ok((step, stats))
    }

    /// Judges what the tick put on the replica and left staged.
    fn judge_stores(&mut self) -> Result<(), Violation> {
        self.judge_puts()?;
        self.judge_staged_durable()
    }

    /// Shows the liveness oracle the WAL before a tick, and, before a boot,
    /// what the boot's witness will find unprovable.
    fn liveness_before_tick(&mut self, booting: bool) {
        self.liveness.see_wal(
            self.w.sqlite.wal_header_salts(),
            self.w.sqlite.our_reader_pins_wal(),
        );
        if booting && let Some(what) = self.w.pending_unprovable() {
            self.liveness.note(Justifier::PendingUnprovable);
            self.report
                .reach
                .bump(format!("liveness.pending_unprovable.{}", variant(&what)));
        }
    }

    /// Takes in a tick that ran: records it, checks its retry wait, and
    /// moves the clock as a driver would.
    fn ran(&mut self, event: &Event, next: Next, began: Timestamp) -> Result<Step, Violation> {
        if self.record {
            self.report.seen.push(Seen::Event(event.clone()));
        }
        self.note_event(event);
        self.judge_snapshot(event)?;
        let began = self.latest.map_or(began, |latest| latest.max(began));
        self.latest = Some(began);
        capped(event, began)?;
        self.note_recovered();
        if let Event::Uploaded(did) = event {
            self.w.acked.on_did(did);
        }
        if matches!(event, Event::Idle) && matches!(next, Next::Now) {
            self.report.spins = self.report.spins.saturating_add(1);
            self.w.clock.advance(SPIN_STEP);
        }
        self.quiet = matches!(next, Next::OnWalChange);
        if self.unwatched && !matches!(next, Next::Now) {
            self.unwatched = false;
        } else {
            self.follow(next);
        }
        match event {
            // A bug kept trying is still a bug, whatever the run expects.
            Event::Degraded {
                cause: PersistentCause::Bug,
                ..
            } => Err(bug_fault("the engine met a bug and rebuilt itself")),
            Event::Degraded { cause, .. } => Ok(Step::Degraded(*cause)),
            Event::Booted { .. }
            | Event::Synced { .. }
            | Event::Checkpointed { .. }
            | Event::Uploaded(_)
            | Event::Maintained { .. }
            | Event::Idle
            | Event::Retrying { .. }
            | Event::Resnapshot(_)
            | Event::Clamped(_) => Ok(Step::Ran),
        }
    }

    /// Hands a snapshot the event announces to the liveness oracle, and
    /// tells it when a fault failed a step.
    fn judge_snapshot(&mut self, event: &Event) -> Result<(), Violation> {
        // A boot or a step under a wal-index reset ahead of the WAL header
        // leaves a position in the header's generation that proves nothing:
        // the header the next commit writes is a change it cannot follow.
        if matches!(event, Event::Booted { .. } | Event::Synced { .. })
            && self.w.sqlite.index_reset_ahead()
        {
            self.liveness.note_in_tick(Justifier::PositionUnproven);
        }
        let (cause, at_boot) = match event {
            Event::Booted { cause: Some(cause) } => (cause, true),
            Event::Resnapshot(cause) => (cause, false),
            Event::Degraded { .. } | Event::Retrying { .. } => {
                self.liveness.note_in_tick(Justifier::PositionUnproven);
                return Ok(());
            }
            Event::Booted { cause: None }
            | Event::Synced { .. }
            | Event::Checkpointed { .. }
            | Event::Uploaded(_)
            | Event::Maintained { .. }
            | Event::Idle
            | Event::Clamped(_) => return Ok(()),
        };
        self.liveness
            .judge(CauseKind::from(cause), at_boot, &mut self.report.reach)
    }

    /// Counts what an event shows: its kind, and the fault, cause or
    /// snapshot it names.
    fn note_event(&mut self, event: &Event) {
        let reach = &mut self.report.reach;
        reach.bump(format!("event.{}", variant(event)));
        match event {
            Event::Retrying { fault, .. } => {
                reach.bump(format!("retrying.{}", variant(&FaultKind::from(fault))));
            }
            Event::Degraded { cause, .. } => reach.bump(format!("degraded.{}", variant(cause))),
            Event::Booted { cause: Some(cause) } | Event::Resnapshot(cause) => {
                reach.bump(format!("snapshot.{}", variant(&CauseKind::from(cause))));
            }
            Event::Maintained {
                event: lsr_core::MaintainedEvent::Repaired(key),
                ..
            } => self.w.repaired.push(*key),
            Event::Booted { cause: None }
            | Event::Synced { .. }
            | Event::Checkpointed { .. }
            | Event::Uploaded(_)
            | Event::Maintained { .. }
            | Event::Idle
            | Event::Clamped(_) => {}
        }
    }

    /// Tells the liveness oracle what the tick that just ran did, before its
    /// end is taken in. A crashed tick has no event to judge, and ends with
    /// a new engine whose counter starts again: its stores are taken in
    /// first, then what the crash did to the WAL, which came after anything
    /// the tick staged.
    fn liveness_mid_tick(&mut self, hits0: (u64, u64), crashed: bool) -> Result<(), Violation> {
        self.note_tick_faults(hits0);
        self.liveness_after_tick(self.actor.snapshots(), !crashed, crashed)?;
        let (wal, pinned) = (
            self.w.sqlite.wal_header_salts(),
            self.w.sqlite.our_reader_pins_wal(),
        );
        if crashed {
            self.liveness.see_wal(wal, pinned);
        } else {
            self.liveness.see_wal_after_tick(wal, pinned);
        }
        Ok(())
    }

    /// Hands the liveness oracle what the tick staged and uploaded, with the
    /// engine's snapshot counter as the tick left it, when `now`; `ended` is
    /// false for a tick that crashed.
    fn liveness_after_tick(
        &mut self,
        snapshots: lsr_core::SnapshotCounts,
        ended: bool,
        now: bool,
    ) -> Result<(), Violation> {
        if !now {
            return Ok(());
        }
        let staging = self.w.staging_recorder.events();
        let remote = self.w.remote_recorder.events();
        let world = crate::liveness::WithEngine {
            world: &self.w,
            staged: self.actor.staged().map(lsr_ltx::Txid::get),
        };
        self.liveness.after_tick(
            (&staging, &remote),
            &world,
            snapshots,
            ended,
            &mut self.report.reach,
        )
    }

    /// Scheduled faults that hit a call on either store, summed.
    fn fault_hits(&self) -> (u64, u64) {
        let sum = |faults: &crate::FaultHandle| {
            faults.hits().iter().fold(0_u64, |sum, (_, _, hits)| {
                sum.saturating_add(u64::from(*hits))
            })
        };
        (sum(&self.w.staging_faults), sum(&self.w.remote_faults))
    }

    /// Notes for the liveness oracle the faults that hit in the tick that just ran.
    fn note_tick_faults(&mut self, (staging0, remote0): (u64, u64)) {
        let (staging, remote) = self.fault_hits();
        if staging > staging0 {
            self.liveness.note_in_tick(Justifier::StagingFault);
        }
        if remote > remote0 {
            self.liveness.note_in_tick(Justifier::RemoteFault);
        }
    }

    fn note_lost(&mut self, lost: &[(lsr_core::FileKey, crate::store::Lost)]) {
        if !lost.is_empty() {
            self.liveness.note(Justifier::StagedLost);
        }
        for (_, how) in lost {
            self.report.reach.bump(format!("staged.{}", variant(how)));
        }
    }

    fn tick(&mut self) -> Result<(), Violation> {
        match self.step(None)?.0 {
            Step::Stopped(cause) if self.stop_expected(cause) => Ok(()),
            Step::Stopped(cause) => Err(bug_fault(format!("the engine stopped: {cause:?}"))),
            Step::Degraded(cause) => self.degraded_expected(cause),
            Step::Ran | Step::Drained(_) | Step::Crashed => Ok(()),
        }
    }

    /// A task degraded by a persistent fault the run does not expect is a
    /// bug, as a stop for it would be. `Bug` is never expected. A stall is a
    /// retried fault that kept failing: as expected as the retries were before
    /// they were reported as a stall.
    fn degraded_expected(&self, cause: PersistentCause) -> Result<(), Violation> {
        let expected = cause.stop().is_none_or(|stop| self.stop_expected(stop));
        if cause != PersistentCause::Bug && expected {
            Ok(())
        } else {
            Err(bug_fault(format!("a task is degraded: {cause:?}")))
        }
    }

    /// Records each task the tick took out of its degraded state.
    fn note_recovered(&mut self) {
        for (was, task) in self.degraded.iter_mut().zip(Task::ALL) {
            let now = self.actor.degraded(task).is_some();
            if *was && !now && self.record {
                self.report.seen.push(Seen::Recovered(task));
            }
            *was = now;
        }
    }

    fn crash_tick(&mut self, kind: CrashKind) -> Result<(), Violation> {
        let mut plan = self.w.pick_crash(self.last_polls);
        plan.kind = kind;
        if matches!(self.step(Some(plan))?.0, Step::Crashed) {
            return Ok(());
        }
        // The tick ended first: the machine fails right after it instead.
        let lost = self.w.crash(kind);
        self.note_lost(&lost);
        if self.trace {
            self.report
                .trace
                .push(format!("    then crashed: {kind:?}, lost: {lost:?}"));
        }
        self.crashed(kind);
        Ok(())
    }

    /// Stops the engine and runs it until it says it is drained.
    ///
    /// A task the scenario's fault keeps degraded may outlast the grace: the
    /// drain then ends at its deadline, as a stop for that fault would end it.
    fn drain(&mut self) -> Result<(), Violation> {
        self.actor
            .begin_shutdown(self.actor.shutdown_deadline(self.now()));
        let mut degraded = false;
        for _ in 0..DRAIN_TICKS {
            match self.step(None)?.0 {
                Step::Drained(txid) => {
                    self.drained(txid)?;
                    return self.judge_drained();
                }
                Step::Stopped(StopCause::Shutdown) if degraded || self.rate_active => {
                    return Ok(());
                }
                Step::Stopped(StopCause::Shutdown) => {
                    self.reboot();
                    self.actor
                        .begin_shutdown(self.actor.shutdown_deadline(self.now()));
                }
                Step::Degraded(cause) => {
                    self.degraded_expected(cause)?;
                    degraded = true;
                }
                Step::Stopped(cause) if self.stop_expected(cause) => return Ok(()),
                Step::Stopped(cause) => {
                    return Err(bug_fault(format!(
                        "the engine stopped in a drain: {cause:?}"
                    )));
                }
                Step::Ran | Step::Crashed => {}
            }
        }
        within(
            Bound::DrainTicks,
            u64::from(DRAIN_TICKS).saturating_add(1),
            u64::from(DRAIN_TICKS),
        )
    }

    /// A drain that says it finished names a transaction that covers every
    /// staged level-0 file and that the replica's newest level-0 file reaches:
    /// nothing staged was left behind.
    fn drained(&self, txid: Option<Txid>) -> Result<(), Violation> {
        // A staging backup put back under a running engine holds files it
        // never staged and does not upload.
        let staged = self
            .w
            .newest_engine_staged(self.put_back)
            .map_err(|e| bug_fault(e.to_string()))?;
        let staged = staged.map(|file| file.key.range().max());
        let remote = self
            .w
            .remote_level_zero()
            .map_err(|e| bug_fault(e.to_string()))?
            .last()
            .map(|file| file.key.range().max());
        // A bucket rollback can take the drained file after the engine
        // confirmed it; the engine learns of it only at its next listing.
        let rolled_back = txid.is_some_and(|txid| self.w.lost.contains(&txid));
        if staged > txid || (remote != txid && !rolled_back) {
            return Err(bug_fault(format!(
                "a drain finished at {txid:?} with {staged:?} staged and {remote:?} on the replica"
            )));
        }
        Ok(())
    }

    /// The app's commit inside the tick that just ran, if it made one: the
    /// spilled pages went with it.
    fn took_commit_in_tick(&mut self) -> Option<(crate::CommitId, usize)> {
        let taken = self.w.take_commit_in_tick()?;
        self.report.commits_in_tick = self.report.commits_in_tick.saturating_add(1);
        self.spilled.clear();
        let n = u64::try_from(taken.pages).unwrap_or(u64::MAX);
        self.max_tx_pages = self.max_tx_pages.max(n);
        Some((taken.id, taken.staged_from))
    }

    /// One tick in which the app commits `count` pages right before a
    /// checkpoint takes its barrier, if the tick takes one.
    fn commit_in_tick(&mut self, count: u8) -> Result<(), Violation> {
        let size = self.w.sqlite.db_size();
        let mut all = self.spilled.clone();
        all.extend(self.pick_pages(u32::from(count), size));
        self.w.arm_commit_in_tick(all, size);
        self.tick()
    }

    fn page_bytes(&self, fill: u32, created_at: u64) -> Vec<u8> {
        let size = self.w.sqlite.page_size().bytes();
        let mut data = vec![u8::try_from(fill % 256).unwrap_or(0); size];
        for (slot, byte) in data.iter_mut().zip(created_at.to_be_bytes()) {
            *slot = byte;
        }
        data
    }

    fn pick_pages(&self, count: u32, upto: u32) -> Vec<(PageNo, Vec<u8>)> {
        let size = self.w.sqlite.page_size();
        let mut numbers: Vec<u32> = (0..count)
            .map(|_| small(&self.rng, u64::from(upto)).saturating_add(1))
            .collect();
        numbers.sort_unstable();
        numbers.dedup();
        numbers
            .into_iter()
            .filter_map(|n| {
                let page = PageNo::new(n, size)?;
                let created_at = u64::from(self.report.steps) << 16 | u64::from(n);
                Some((page, self.page_bytes(small(&self.rng, 256), created_at)))
            })
            .collect()
    }

    fn all_pages(&self, upto: u32) -> Vec<(PageNo, Vec<u8>)> {
        let size = self.w.sqlite.page_size();
        (1..=upto)
            .filter_map(|n| {
                let page = PageNo::new(n, size)?;
                let created_at = u64::from(self.report.steps) << 16 | u64::from(n);
                Some((page, self.page_bytes(small(&self.rng, 256), created_at)))
            })
            .collect()
    }

    fn app_commit(&mut self, mut pages: Vec<(PageNo, Vec<u8>)>, db_size: u32) {
        let mut all = std::mem::take(&mut self.spilled);
        all.append(&mut pages);
        let before = self.w.sqlite.db_size();
        if let Some(id) = self.w.app_commit(&all, db_size) {
            self.app
                .logged(id, &all, db_size, self.w.sqlite.page_size());
            if db_size < before {
                self.report.reach.bump("commit.shrinks");
            }
            let n = u64::try_from(all.len()).unwrap_or(u64::MAX);
            self.max_tx_pages = self.max_tx_pages.max(n);
        } else {
            self.spilled = all;
        }
    }

    fn commit(&mut self, count: u8, grow: bool) {
        let size = self.w.sqlite.db_size();
        let new_size = if grow {
            size.saturating_add(small(&self.rng, 3).saturating_add(1))
        } else {
            size
        };
        let mut pages = self.pick_pages(u32::from(count), size);
        if grow {
            let added: Vec<_> = self
                .all_pages(new_size)
                .into_iter()
                .filter(|(page, _)| page.get() > size)
                .collect();
            pages.extend(added);
        }
        self.app_commit(pages, new_size);
    }

    /// SQLite leaves WAL mode, which a page-size change needs, only with
    /// every connection closed, the engine's read lock among them
    /// (`sqlite_rules::our_reader_blocks_leaving_wal_mode`). So the operator stops
    /// the engine, the app ends its readers and vacuums, and the engine starts
    /// again; a write transaction the app holds refuses the vacuum.
    fn resize(&mut self, new: PageSize) {
        let old = self.w.sqlite.page_size();
        if self.lenient && !self.expected_sizes.contains(&old) {
            self.expected_sizes.push(old);
        }
        self.w.crash(CrashKind::Process);
        for id in std::mem::take(&mut self.readers) {
            self.w.sqlite.other_process_reader_end(id);
        }
        if self.w.app_vacuum(new).is_some() {
            self.spilled.clear();
        }
        self.crashed(CrashKind::Process);
    }

    /// The one page size of every file staged or published: a replica prefix
    /// holds one, so a change the replica did not follow stops the engine
    /// before it stages anything. `None` while there is no file.
    fn replica_page_size(&self) -> Result<Option<PageSize>, Violation> {
        let sizes = self.w.page_sizes(self.put_back).map_err(bug_fault)?;
        let Some((_, first)) = sizes.first() else {
            return Ok(None);
        };
        if let Some((key, other)) = sizes.iter().find(|(_, size)| size != first) {
            return Err(bug_fault(format!(
                "{key} has page size {other:?} where the replica has {first:?}"
            )));
        }
        Ok(Some(*first))
    }

    /// Whether a stop is one the run expects: a scenario's, or a page-size
    /// change between the files staged or published and the database.
    fn stop_expected(&self, cause: StopCause) -> bool {
        if let StopCause::PageSizeChanged { was, now } = cause {
            // The files of the old size may all be gone (a bucket rollback, a
            // wiped staging directory) while the engine still remembers it.
            let remembered = self.expected_sizes.contains(&was);
            return now == self.w.sqlite.page_size()
                && (remembered || self.replica_page_size().ok().flatten() == Some(was));
        }
        self.allow_stop
            || self
                .expected
                .iter()
                .any(|want| std::mem::discriminant(want) == std::mem::discriminant(&cause))
    }

    /// The replica's listings start to lag ([`Op::LaggingListings`]).
    fn lag_listings(&mut self, writes: u8, window: u8) {
        self.report.reach.bump("remote.lagging_listings");
        // Nothing is expected: a lag alone is no reason to stop, to report a
        // regression or to write a transaction twice.
        self.w
            .remote_faults
            .lag_listings(u32::from(writes), u32::from(window));
    }

    /// Makes `cause` legitimate for the rest of the run.
    fn expect(&mut self, cause: StopCause) {
        if !self.expected.contains(&cause) {
            self.expected.push(cause);
        }
    }

    fn vacuum(&mut self, to: Option<PageSize>) {
        match to {
            Some(new) => self.resize(new),
            None if self.spilled.is_empty() => {
                let size = self.w.sqlite.db_size();
                let smaller = size > 1 && below(&self.rng, 3) == 0;
                let new_size = if smaller {
                    size.saturating_sub(1)
                } else {
                    size
                };
                let pages = self.all_pages(new_size);
                self.app_commit(pages, new_size);
            }
            None => {}
        }
    }

    /// The app drops up to `pages` pages from the end of the database, when it
    /// holds no open transaction.
    fn shrink(&mut self, pages: u8) {
        let size = self.w.sqlite.db_size();
        if !self.spilled.is_empty() || size < 2 {
            return;
        }
        let to = size.saturating_sub(u32::from(pages)).max(1);
        if self.w.app_shrink(to).is_some() {
            self.report.reach.bump("commit.shrinks");
        }
    }

    /// Another writer's file sits on the replica for `ticks` ticks; then the
    /// operator removes it, if it still holds that writer's bytes, and starts
    /// the engine again. The engine may have listed it, so nothing it
    /// acknowledged before the removal is held against `Latest`.
    fn second_writer(&mut self, ticks: u8) -> Result<(), Violation> {
        // A file at or below what this engine has staged collides with one of
        // its own transactions (after a bucket rollback, say): telling the two
        // writers apart needs writer identity on the replica, which the engine
        // does not have, so the step puts nothing then.
        let ours = self
            .w
            .newest_staged()
            .map_err(|err| bug_fault(format!("listing staging failed: {err}")))?
            .map(|info| info.key.range().max())
            .max(self.actor.staged());
        let theirs = self
            .w
            .remote_level_zero()
            .map_err(|err| bug_fault(format!("listing the replica failed: {err}")))?
            .last()
            .and_then(|info| info.key.range().max().next());
        // Likewise one at or below what any level of the replica already holds.
        let held = lsr_core::conformance::block_on(lsr_core::Inventory::load(&self.w.remote_raw))
            .map_err(|err| bug_fault(format!("listing the replica failed: {err}")))?
            .newest();
        if theirs.is_none_or(|theirs| Some(theirs) <= ours.max(held)) {
            return Ok(());
        }
        let put = self
            .w
            .remote_put_foreign()
            .map_err(|err| bug_fault(format!("the second writer's put failed: {err}")))?;
        let Some((key, bytes)) = put else {
            return Ok(());
        };
        self.report.reach.bump("remote.second_writer");
        self.expect(StopCause::OtherWriter);
        // Only an engine that was running when the file appeared, and has not
        // booted since, owes the stop: a boot takes a replica ahead as history.
        let mut watching = self.actor.booted();
        for _ in 0..ticks {
            watching &= self.actor.booted();
            let lists = self.w.remote_recorder.counts().lists;
            let read_faults = self.remote_read_faults();
            let step = self.step(None)?.0;
            let stopped = step == Step::Stopped(StopCause::OtherWriter);
            match step {
                Step::Stopped(cause) if self.stop_expected(cause) => {}
                Step::Stopped(cause) => {
                    return Err(bug_fault(format!("the engine stopped: {cause:?}")));
                }
                Step::Degraded(cause) => self.degraded_expected(cause)?,
                Step::Ran | Step::Drained(_) | Step::Crashed => {}
            }
            if stopped {
                break;
            }
            // A running engine that listed the replica in this tick while the
            // other writer's file stood past its own position holds the proof
            // of another writer: it must stop. A boot's listing is recovery's to
            // judge, and an engine already at that transaction replaces the
            // file with its own upload (no writer identity can tell them apart).
            // A whole listing is one call per level at least; a listing cut
            // short by a fault showed the engine nothing. A listing alone is
            // no proof either (one that lags a delete shows a file that is
            // gone): the engine reads the file before it stops, and a read a
            // fault cut short proved nothing.
            let levels = u64::try_from(lsr_core::Level::ALL.len()).unwrap_or(u64::MAX);
            let listed = self.w.remote_recorder.counts().lists.saturating_sub(lists) >= levels
                && self.remote_read_faults() == read_faults;
            let theirs = self.w.remote_holds(key, &bytes).map_err(|err| {
                bug_fault(format!("reading the second writer's file failed: {err}"))
            })?;
            // Confirming the other writer's object (it stands over a staged
            // file of the same id) and snapshotting above it is not behind.
            let ours = self.actor.staged().max(self.actor.replicated());
            let behind = ours < Some(key.range().max());
            if watching && listed && theirs && behind {
                return Err(Violation::OtherWriterMissed {
                    key: key.to_string(),
                });
            }
        }
        self.remove_foreign(key, &bytes)
    }

    /// How many replica reads a scheduled fault has failed or changed so far.
    fn remote_read_faults(&self) -> u32 {
        self.w
            .remote_faults
            .hits()
            .iter()
            .filter(|(op, _, _)| *op == lsr_core::StoreOp::Read)
            .fold(0, |sum, (_, _, hits)| sum.saturating_add(*hits))
    }

    /// The operator removes the other writer's newest file if it still holds
    /// `bytes`, and restarts the engine.
    fn remove_foreign(&mut self, key: lsr_core::FileKey, bytes: &[u8]) -> Result<(), Violation> {
        let removed = self
            .w
            .remote_remove_if(key, bytes)
            .map_err(|err| bug_fault(format!("removing the second writer's file failed: {err}")))?;
        // A boot may have taken the file as history and built on it; the
        // operator's removal then takes that transaction for good.
        if removed {
            self.w.lost.insert(key.range().max());
            let index = self.w.foreign.len().saturating_sub(1);
            self.w
                .foreign_removed
                .push((index, self.w.remote_recorder.len()));
        }
        self.w.acked.note_rewound();
        self.w.crash(CrashKind::Process);
        self.crashed(CrashKind::Process);
        Ok(())
    }

    fn spill(&mut self) {
        let size = self.w.sqlite.db_size();
        let pages = self.pick_pages(small(&self.rng, 3).saturating_add(1), size);
        if self.w.sqlite.spill(&pages) {
            self.spilled.extend(pages);
        }
    }

    fn reader(&mut self, begin: bool) {
        if begin {
            self.readers
                .push(self.w.sqlite.other_process_reader_begin());
        } else if !self.readers.is_empty() {
            let id = self.readers.remove(0);
            self.w.sqlite.other_process_reader_end(id);
        }
    }

    fn store_fault(
        &mut self,
        target: Target,
        op: lsr_core::StoreOp,
        ahead: u8,
        shape: crate::FaultShape,
    ) {
        if target == Target::Remote
            && matches!(
                shape,
                crate::FaultShape::CorruptPublish
                    | crate::FaultShape::ChecksumMismatch
                    | crate::FaultShape::FlipByteOnRead { .. }
            )
        {
            self.expect(StopCause::RemoteCorrupt);
        }
        if matches!(shape, crate::FaultShape::Rate { .. }) {
            self.rate_active = true;
        }
        let faults = match target {
            Target::Staging => &self.w.staging_faults,
            Target::Remote => &self.w.remote_faults,
        };
        faults.push_in(op, u32::from(ahead), shape);
    }

    fn scenario_bool(&self, what: &str, done: bool) -> Result<(), Violation> {
        self.step_done(what, Ok(done))
    }

    /// A scenario step that had nothing to act on fails the scenario; in a
    /// drawn history it is skipped.
    fn step_done(&self, what: &str, done: Result<bool, StagedEditError>) -> Result<(), Violation> {
        match done {
            Ok(false) if self.lenient => Ok(()),
            other => Self::scenario_step(what, other),
        }
    }

    fn remote(&mut self, op: RemoteOp) -> Result<(), Violation> {
        let done = match op {
            RemoteOp::PutUnrecognized => self.w.remote_put_unrecognized(),
            RemoteOp::RemoveUnrecognized => {
                let w = &self.w;
                w.remote_level_zero().and_then(|files| {
                    files.last().map_or(Ok(false), |newest| {
                        w.remote_delete(&[newest.key]).map(|()| true)
                    })
                })
            }
            RemoteOp::RollBack => self.w.remote_roll_back(),
            RemoteOp::DropCreatedAt => self.w.remote_drop_created_at(),
        };
        if matches!(op, RemoteOp::RollBack | RemoteOp::RemoveUnrecognized) {
            self.w.history.excuse_regression();
            if matches!(done, Ok(true)) {
                self.regressions = self.regressions.saturating_add(1);
            }
        }
        if matches!(op, RemoteOp::RollBack) {
            self.w.acked.note_rewound();
            self.expect(StopCause::RemoteRegressed);
        }
        self.step_done(&format!("{op:?}"), done.map_err(StagedEditError::from))
    }

    fn env_fault(&mut self, fault: EnvFault) -> Result<(), Violation> {
        match fault {
            EnvFault::DeleteWal => {
                if self.w.sqlite.delete_wal() {
                    self.w.model.on_checkpoint();
                }
            }
            EnvFault::WipeStaging => {
                if self.w.wipe_staging().is_ok() {
                    self.w.crash(CrashKind::Process);
                    self.crashed(CrashKind::Process);
                }
            }
            EnvFault::Source(op, err) => self.w.sqlite.inject(op, err),
            EnvFault::ShmMidUpdate => {
                self.scenario_bool("ShmMidUpdate", self.w.sqlite.shm_mid_update())?;
            }
            EnvFault::ShmEndInsideTx => {
                self.scenario_bool("ShmEndInsideTx", self.w.sqlite.shm_end_inside_tx())?;
            }
            EnvFault::OverwriteLastFrame => {
                let frame = self.w.sqlite.mx_frame();
                let done = self.w.sqlite.overwrite_frame_between_scan_and_read(frame);
                self.scenario_bool("OverwriteLastFrame", done)?;
            }
            EnvFault::UnlinkWal => self.scenario_bool("UnlinkWal", self.w.sqlite.unlink_wal())?,
            EnvFault::EmptyDatabase => self.w.sqlite.empty_database_once(),
            EnvFault::TruncateFails => self.w.sqlite.fail_next_truncate(),
            EnvFault::DriverWritesNothing => self.w.sqlite.driver_writes_nothing_once(),
            EnvFault::FlipDatabasePage(pgno) => {
                let flipped = self.w.sqlite.flip_db_page(pgno);
                self.scenario_bool("FlipDatabasePage", flipped)?;
                self.w.adopt_local_state();
            }
        }
        Ok(())
    }

    fn clock_jump(&mut self, millis: i64) {
        let to = Timestamp::from_unix_millis(self.now().unix_millis().saturating_add(millis));
        self.w.clock.set(to);
        self.w.history.note_time(to);
    }

    /// Applies `op`, checks the model still follows SQLite's recovery, then judges the replica
    /// by content at random (see [`crate::history`]).
    fn apply_judged(&mut self, op: &Op) -> Result<(), Violation> {
        self.op_began_at = self.w.sqlite.mx_frame();
        self.apply(op)?;
        self.after_restart();
        self.recovery_agrees()?;
        if self.judges.history && self.w.history.draw() {
            return self.judge_history(crate::history::Ask::default());
        }
        Ok(())
    }

    /// The history judge over the raw replica now.
    fn judge_history(&mut self, ask: crate::history::Ask) -> Result<(), Violation> {
        if !self.judges.history {
            return Ok(());
        }
        let damaged = self.w.remote_faults.damaged();
        let now = self.now();
        self.w.history.judge(&self.w.remote_raw, now, &damaged, ask)
    }

    /// After a drain that finished: `Latest` is the local head, unless a bucket rollback
    /// took files the engine had already confirmed.
    fn judge_drained(&mut self) -> Result<(), Violation> {
        let drained = self.w.lost.is_empty();
        self.judge_history(crate::history::Ask {
            sweep: false,
            drained,
        })
    }

    /// Both judges over the whole replica; where one fails and the other passes, the
    /// disagreement is counted in reach, keyed by the failing judge and its violation.
    fn restore_all(&mut self) -> Result<(), Violation> {
        self.report.checks = self.report.checks.saturating_add(1);
        let oracle = self
            .judges
            .oracle
            .then(|| self.w.check(Some(windows(&actor_config().maintenance))));
        let history = self.judge_history(crate::history::Ask {
            sweep: true,
            drained: false,
        });
        match (&oracle, &history) {
            (Some(Err(violation)), Ok(())) if self.judges.history => {
                let key = format!("history.disagree.oracle_only.{}", variant(violation));
                self.report.reach.bump(key);
            }
            (Some(Ok(_)), Err(violation)) => {
                let key = format!("history.disagree.history_only.{}", variant(violation));
                self.report.reach.bump(key);
            }
            (Some(_) | None, _) => {}
        }
        if let Some(checked) = oracle {
            checked?.tally(&mut self.report.reach);
        }
        history?;
        self.replica_page_size()?;
        if self.record {
            self.report.seen.push(Seen::Oracle);
        }
        Ok(())
    }

    fn restart(&mut self, kind: CrashKind) {
        self.w.crash(kind);
        self.crashed(kind);
    }

    fn scenario_step(what: &str, done: Result<bool, StagedEditError>) -> Result<(), Violation> {
        match done {
            Ok(true) => Ok(()),
            Ok(false) => Err(bug_fault(format!(
                "the scenario step {what} had nothing to act on"
            ))),
            Err(err) => Err(bug_fault(format!("the scenario step {what} failed: {err}"))),
        }
    }

    fn meta(&mut self, op: MetaOp) -> Result<(), Violation> {
        let w = &self.w;
        let done = match op {
            MetaOp::Save => w
                .copy_staging()
                .map(|copy| {
                    self.saved_notes = copy
                        .origins()
                        .into_iter()
                        .filter_map(|(key, origin)| {
                            let txid = key.range().max();
                            let commit = w.model.staged_commit(txid, origin)?;
                            Some((txid, origin, commit))
                        })
                        .collect();
                    self.saved = Some(copy);
                    true
                })
                .map_err(StagedEditError::from),
            MetaOp::Restore => {
                let done = match &self.saved {
                    Some(copy) => w.replace_staging(copy).map(|()| true),
                    None => Ok(false),
                };
                if matches!(done, Ok(true)) {
                    self.put_back = self.w.staging_recorder.len();
                    self.renote_saved();
                }
                done
            }
            MetaOp::DeleteNewest => w.delete_newest_staged().map_err(StagedEditError::from),
            MetaOp::FlipNewestByte => w.edit_newest_staged(|bytes| {
                let middle = bytes.len() / 2;
                if let Some(byte) = bytes.get_mut(middle) {
                    *byte ^= 0xff;
                }
            }),
            MetaOp::StripNewestOrigin => w.strip_newest_origin(),
            MetaOp::DeleteAll => w
                .wipe_staging()
                .map(|()| true)
                .map_err(StagedEditError::from),
            MetaOp::TruncateNewest => w.edit_newest_staged(|bytes| {
                let half = bytes.len() / 2;
                bytes.truncate(half);
            }),
            MetaOp::ForgeNewest => w.forge_newest_staged(),
        };
        self.step_done(&format!("{op:?}"), done)
    }

    /// A restored backup holds the files it copied: the oracle maps each
    /// id to the commit its copy held, not to whatever was staged since.
    fn renote_saved(&mut self) {
        let at = self.w.remote_recorder.len();
        for (txid, origin, commit) in &self.saved_notes {
            self.w.model.note_restored(*txid, *origin, *commit, at);
        }
    }

    fn wal_damage(&mut self, op: WalOp) -> Result<(), Violation> {
        let sqlite = &self.w.sqlite;
        let applied = match op {
            WalOp::TearHeader => sqlite.tear_wal_header(),
            WalOp::TruncateAtFrame(frames) => sqlite
                .truncate_wal_at(WalOffset::from_frame_index(frames, sqlite.page_size()).get()),
            WalOp::RewriteSameLength(seed) => sqlite.rewrite_same_length(seed),
            WalOp::RegrowSameSalts(seed) => sqlite.regrow_same_salts(seed),
            WalOp::RegrowWithoutCommitMarks(seed) => sqlite.regrow_without_commit_marks(seed),
            WalOp::FlipLastFrameBit => {
                // Ten bytes into the page, past the 24-byte frame header.
                let start = WalOffset::from_frame_index(
                    sqlite.mx_frame().saturating_sub(1),
                    sqlite.page_size(),
                );
                sqlite.flip_bit(start.get().saturating_add(34).saturating_mul(8))
            }
        };
        self.expect(StopCause::WalCorrupt);
        self.expect(StopCause::WalTruncated);
        self.step_done(&format!("{op:?}"), Ok(applied))?;
        self.w.adopt_local_state();
        Ok(())
    }

    /// The WAL is damaged, the engine runs `ticks` ticks, then the app heals
    /// it: a flipped bit back, then its connections let go, another process
    /// truncates, and it commits a page.
    fn wal_damage_healed(&mut self, op: WalOp, ticks: u8) -> Result<(), Violation> {
        self.wal_damage_judged(op, ticks, false)
    }

    /// [`Self::wal_damage_healed`], and with `judged` the replica is judged while the damage
    /// holds, in the middle of the ticks and after them ([`hold`]).
    fn wal_damage_judged(&mut self, op: WalOp, ticks: u8, judged: bool) -> Result<(), Violation> {
        // A regrowth rewrites the generation from frame 1; frames a checkpoint
        // already copied would then differ from the database file, which
        // makes the database two things at once (SQLite's readers and its
        // next checkpoint would disagree), so it is drawn only before any copy.
        let regrows = matches!(
            op,
            WalOp::RegrowSameSalts(_) | WalOp::RegrowWithoutCommitMarks(_)
        );
        if regrows && self.w.sqlite.backfilled() > 0 {
            return Ok(());
        }
        // A cut below the committed end leaves the wal-index naming frames the
        // file no longer holds, which no SQLite reader can make sense of; a
        // drawn cut takes only frames past it (a rolled-back or spilled tail).
        let op = match op {
            WalOp::TruncateAtFrame(past) => {
                WalOp::TruncateAtFrame(self.w.sqlite.mx_frame().saturating_add(past % 4))
            }
            other @ (WalOp::TearHeader
            | WalOp::RewriteSameLength(_)
            | WalOp::RegrowSameSalts(_)
            | WalOp::RegrowWithoutCommitMarks(_)
            | WalOp::FlipLastFrameBit) => other,
        };
        self.wal_damage(op)?;
        self.report.reach.bump(format!("wal.{}", variant(&op)));
        for at in 0..ticks {
            self.tick()?;
            if judged && at == ticks / 2 {
                self.judge_held(crate::generate::Hold::Wal(op), None)?;
            }
        }
        if judged {
            self.judge_held(crate::generate::Hold::Wal(op), None)?;
        }
        if op == WalOp::FlipLastFrameBit {
            self.wal_damage(op)?;
        }
        self.app_lets_go();
        if self.w.sqlite.our_reader_end() {
            self.report.reach.bump("wal.our_reader_ended");
        }
        self.w
            .other_process_checkpoint(crate::sqlite::OtherProcessMode::Truncate);
        self.commit(1, false);
        Ok(())
    }

    /// Applies `op`, telling the liveness oracle what it did by itself and
    /// how it left the WAL's header.
    fn apply(&mut self, op: &Op) -> Result<(), Violation> {
        self.liveness.note_op(op);
        self.liveness.see_wal(
            self.w.sqlite.wal_header_salts(),
            self.w.sqlite.our_reader_pins_wal(),
        );
        self.apply_op(op)?;
        // An operation that runs ticks between its edits (another writer's
        // file put, then removed after the engine ran) edits again after them.
        self.liveness.note_op(op);
        self.liveness.see_wal(
            self.w.sqlite.wal_header_salts(),
            self.w.sqlite.our_reader_pins_wal(),
        );
        Ok(())
    }

    fn apply_op(&mut self, op: &Op) -> Result<(), Violation> {
        match op {
            Op::Commit { pages, grow } => self.commit(*pages, *grow),
            Op::CommitInTick { pages } => self.commit_in_tick(*pages)?,
            Op::Spill => self.spill(),
            Op::Rollback => {
                if self.w.sqlite.rollback() {
                    self.spilled.clear();
                }
            }
            Op::Vacuum { page_size } => self.vacuum(*page_size),
            Op::OtherProcessReader { begin } => self.reader(*begin),
            Op::OtherProcessCheckpoint(mode) => {
                self.w.other_process_checkpoint(*mode);
            }
            Op::Crash(kind) => self.crash_tick(*kind)?,
            Op::StoreFault {
                target,
                op,
                ahead,
                shape,
            } => self.store_fault(*target, *op, *ahead, shape.clone()),
            Op::EnvFault(fault) => self.env_fault(*fault)?,
            Op::ClockJump(millis) => self.clock_jump(*millis),
            Op::Tick => self.tick()?,
            Op::Shutdown => {
                self.report.shutdowns = self.report.shutdowns.saturating_add(1);
                self.drain()?;
                self.reboot();
                self.app.restarted(self.op_began_at);
            }
            Op::RequestSnapshot => self.actor.request_snapshot(),
            Op::Restore => self.restore_all()?,
            Op::Restart => self.restart(CrashKind::Process),
            Op::PowerLoss => self.restart(CrashKind::Power),
            Op::LyingWalSync => self.w.sqlite.lie_on_our_sync(),
            Op::Meta(op) => self.meta(*op)?,
            Op::Wal(op) => self.wal_damage(*op)?,
            Op::Remote(op) => self.remote(*op)?,
            Op::AppAutocheckpoint(frames) => {
                self.w.sqlite.set_app_autocheckpoint(*frames);
                self.app_autocheckpoint = frames.and_then(NonZeroU32::new);
                self.actor.set_app_autocheckpoint(self.app_autocheckpoint);
            }
            Op::Shrink { pages } => self.shrink(*pages),
            Op::SecondWriter { ticks } => self.second_writer(*ticks)?,
            Op::StagingDamage(op) => {
                self.w.crash(CrashKind::Process);
                self.crashed(CrashKind::Process);
                self.meta(*op)?;
            }
            Op::WalDamage { op, ticks } => self.wal_damage_healed(*op, *ticks)?,
            Op::Hold { hold, ticks } => self.hold(*hold, *ticks)?,
            Op::Heal => {
                self.rate_active = false;
                self.w.remote_faults.heal();
                self.w.staging_faults.heal();
            }
            Op::LaggingListings { writes, window } => self.lag_listings(*writes, *window),
            Op::OurReader { begin } => {
                let done = if *begin {
                    self.w.sqlite.our_reader_begin()
                } else {
                    self.w.sqlite.our_reader_end()
                };
                Self::scenario_step(&format!("{op:?}"), Ok(done))?;
            }
        }
        Ok(())
    }

    /// The app does nothing: it rolls back what it spilled and closes every
    /// reader. A reader that holds the WAL makes a checkpoint every interval
    /// fail busy, and each attempt bumps a frame that is synced and uploaded,
    /// so the idle bounds are about an app that has let go.
    fn app_lets_go(&mut self) {
        if self.w.sqlite.rollback() {
            self.spilled.clear();
        }
        while !self.readers.is_empty() {
            self.reader(false);
        }
    }

    /// The bounds that hold once the engine has drained and booted again: it
    /// settles, the WAL is short, and then it spends nothing while the app does
    /// nothing.
    fn idle(&mut self) -> Result<(), Violation> {
        // A spill or reader the app held pinned the WAL; the engine can only
        // truncate it after the app lets go, so the length bound is waived.
        let held = !self.readers.is_empty() || !self.spilled.is_empty();
        self.app_lets_go();
        self.reboot();
        let mut settled = false;
        for _ in 0..SETTLE_TICKS {
            let spins = self.report.spins;
            let (step, stats) = self.step(None)?;
            if self.ends_idle(&step)? {
                return Ok(());
            }
            if matches!(stats.event, Some(Event::Idle)) && self.report.spins == spins && self.quiet
            {
                settled = true;
                break;
            }
        }
        within(Bound::DrainTicks, u64::from(!settled), 0)?;
        let limit = u64::from(TRUNCATE_PAGES)
            .saturating_add(self.max_tx_pages)
            .saturating_add(1);
        if !held {
            within(Bound::WalLength, u64::from(self.w.sqlite.mx_frame()), limit)?;
        }
        let mut window = IdleWindow::default();
        for _ in 0..IDLE_TICKS {
            let (step, stats) = self.step(None)?;
            if self.ends_idle(&step)? {
                return Ok(());
            }
            window.add(&stats);
        }
        window.check()
    }

    /// Whether a step ends the idle check: an expected stop, or a task the
    /// scenario's fault keeps degraded (an engine retrying is not idle).
    fn ends_idle(&self, step: &Step) -> Result<bool, Violation> {
        match step {
            Step::Stopped(cause) if self.stop_expected(*cause) => Ok(true),
            Step::Stopped(cause) => Err(bug_fault(format!(
                "the engine stopped while idle: {cause:?}"
            ))),
            Step::Degraded(cause) => self.degraded_expected(*cause).map(|()| true),
            Step::Ran | Step::Drained(_) | Step::Crashed => Ok(false),
        }
    }

    /// What a drawn run's power losses keep of the unsynced WAL writes: a
    /// drawn prefix, or the block law's draw; and whether the engine's own
    /// sync of the WAL keeps anything.
    fn draw_power_law(&self, seed: Seed, opts: &Opts) {
        self.w.sqlite.tear_on_power_loss(seed.stream(14));
        if opts.block_crash_law {
            self.w.sqlite.crash_by_law(CrashLaw::drawn(seed.stream(17)));
        }
        if opts.lying_wal_sync {
            self.w.sqlite.lie_on_our_sync();
        }
    }

    /// Every power loss so far was recovered alike by SQLite and by the
    /// engine's WAL parser, and the rules' image matched SQLite's.
    fn recovery_agrees(&self) -> Result<(), Violation> {
        match self.w.sqlite.recovery_disagreements().first() {
            Some(first) => Err(bug_fault(format!(
                "SQLite's recovery disagrees with the simulator: {first}"
            ))),
            None => Ok(()),
        }
    }

    /// The end of a history: drain, judge the replica, check the resources.
    fn finish(&mut self) -> Result<(), Violation> {
        if self.lenient && !self.w.model_matches_sqlite() {
            return Err(bug_fault("the model no longer matches SQLite's rules"));
        }
        // Liveness is judged once every fault has cleared.
        self.rate_active = false;
        self.w.remote_faults.heal();
        self.w.staging_faults.heal();
        self.w.remote_faults.end_lag();
        self.drain()?;
        self.restore_all()?;
        self.report.reach.add(
            "remote.stale_listings",
            u64::from(self.w.remote_faults.stale_listings()),
        );
        memory(
            self.w.remote_recorder.counts(),
            self.w.staging_recorder.counts(),
        )?;
        let hits = |faults: &crate::store::FaultHandle| {
            faults
                .hits()
                .iter()
                .fold(0_u64, |sum, (_, _, n)| sum.saturating_add(u64::from(*n)))
        };
        let spent = crate::budget::Spent {
            boots: u64::from(self.report.crashes)
                .saturating_add(u64::from(self.report.shutdowns))
                .saturating_add(u64::from(self.report.checks))
                .saturating_add(u64::from(self.failed_boots))
                .saturating_add(u64::from(self.regressions)),
            // A listing a lag changed hid a write, which may be done again
            // once: the cost of the stale listing, not amplification.
            fault_hits: hits(&self.w.remote_faults)
                .saturating_add(u64::from(self.w.remote_faults.stale_listings())),
        };
        crate::budget::check(
            &self.w.remote_recorder.events(),
            self.w.remote_recorder.counts(),
            spent,
            &self.w.lost,
            &self.w.foreign_sizes,
        )?;
        self.idle()?;
        self.replica_page_size().map(drop)
    }
}

impl Runner {
    /// Half the drawn runs let each kind of batch hold 2 to 8 items on both
    /// stores, from the seed's own stream, so callers take their batched
    /// paths.
    fn draw_batch_widths(&mut self, seed: Seed) {
        let rng = SeededEntropy::new(seed.stream(15));
        if rng.below(2) == 0 {
            return;
        }
        for kind in [
            lsr_core::BatchKind::Reads,
            lsr_core::BatchKind::Lists,
            lsr_core::BatchKind::ResolvedCreatedAt,
        ] {
            for faults in [&self.w.remote_faults, &self.w.staging_faults] {
                let width = usize::try_from(rng.below(7).saturating_add(2)).unwrap_or(2);
                if let Some(width) = std::num::NonZeroUsize::new(width) {
                    faults.set_batch_width(kind, width);
                }
            }
        }
        self.report.reach.bump("batch.wide");
    }

    /// Counts, at the end of a run, what the stores really did: puts per
    /// level, deletes, and every scheduled fault that hit a call.
    fn tally_stores(&mut self) {
        let reach = &mut self.report.reach;
        let (kept, cut) = self.w.sqlite.torn_losses();
        reach.add("power.kept_unsynced_wal", u64::from(kept));
        reach.add("power.kept_cut_frame", u64::from(cut));
        let (kept, holes) = self.w.sqlite.block_losses();
        reach.add("power.block_kept_unsynced", u64::from(kept));
        reach.add("power.block_hole", u64::from(holes));
        reach.add("power.resurrected_commit", u64::from(self.w.resurrected));
        reach.add(
            "batch.remote_calls",
            u64::from(self.w.remote_faults.batched()),
        );
        reach.add(
            "batch.staging_calls",
            u64::from(self.w.staging_faults.batched()),
        );
        for event in self.w.remote_recorder.events() {
            match event {
                crate::RecorderEvent::Put { key, .. } => {
                    reach.bump(format!("put.level.{}", key.level().get()));
                }
                crate::RecorderEvent::Delete { keys, .. } => {
                    reach.bump("delete.calls");
                    reach.add("delete.keys", u64::try_from(keys.len()).unwrap_or(u64::MAX));
                }
            }
        }
        for (target, faults) in [
            ("staging", &self.w.staging_faults),
            ("remote", &self.w.remote_faults),
        ] {
            for (op, shape, hits) in faults.hits() {
                let name = format!("fault.{target}.{op:?}.{}", variant(&shape));
                reach.add(name, u64::from(hits));
            }
        }
    }
}

/// Every retry wait is capped: a persistent fault's at [`MAX_BACKOFF`], a
/// retried one's at the later of that and the wait the store named.
fn capped(event: &Event, began: Timestamp) -> Result<(), Violation> {
    let (until, named) = match event {
        Event::Degraded { until, .. } => (*until, None),
        Event::Retrying {
            until,
            fault: Fault::StoreThrottled { retry_after },
        } => (*until, *retry_after),
        Event::Retrying { until, .. } => (*until, None),
        Event::Booted { .. }
        | Event::Synced { .. }
        | Event::Checkpointed { .. }
        | Event::Uploaded(_)
        | Event::Maintained { .. }
        | Event::Idle
        | Event::Resnapshot(_)
        | Event::Clamped(_) => return Ok(()),
    };
    let cap = named.map_or(MAX_BACKOFF, |named| named.max(MAX_BACKOFF));
    let millis = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let wait = until.unix_millis().saturating_sub(began.unix_millis());
    within(
        Bound::RetryWait,
        u64::try_from(wait).unwrap_or(0),
        millis(cap),
    )
}

/// Runs the history of `steps` operations that `seed` names.
///
/// # Errors
/// The first [`Failure`].
#[expect(
    clippy::result_large_err,
    reason = "a failure is reported once, at the end of a run"
)]
pub fn run(seed: Seed, steps: u32, weights: &Weights) -> Result<RunReport, Failure> {
    run_with(seed, steps, weights, &Opts::default())
}

/// [`run`] with options.
///
/// # Errors
/// The first [`Failure`].
#[expect(
    clippy::result_large_err,
    reason = "a failure is reported once, at the end of a run"
)]
pub fn run_with(
    seed: Seed,
    steps: u32,
    weights: &Weights,
    opts: &Opts,
) -> Result<RunReport, Failure> {
    let opts = Opts {
        drawn: true,
        block_crash_law: weights.block_crash_law,
        lying_wal_sync: weights.lying_wal_sync,
        after_restart: weights.after_restart,
        ..opts.clone()
    };
    run_ops(seed, &history(seed, steps, weights), &opts)
}

/// Runs the given operations, then the final drain and judgment.
///
/// # Errors
/// The first [`Failure`].
#[expect(
    clippy::result_large_err,
    reason = "a failure is reported once, at the end of a run"
)]
pub fn run_ops(seed: Seed, ops: &[Op], opts: &Opts) -> Result<RunReport, Failure> {
    let steps = u32::try_from(ops.len()).unwrap_or(u32::MAX);
    let mut runner = Runner::new(seed);
    runner.lenient = opts.drawn;
    if opts.drawn {
        runner.draw_power_law(seed, opts);
        runner.draw_batch_widths(seed);
    }
    runner.trace = opts.trace;
    runner.record = opts.record;
    runner.allow_stop = opts.allow_stop;
    runner.responses = opts.responses;
    runner.retention = opts.retention;
    runner.judges = opts.judges;
    runner.app.set(opts.after_restart);
    runner.reboot();
    for (index, op) in ops.iter().enumerate() {
        let step = u32::try_from(index).unwrap_or(u32::MAX);
        let fail = |violation| Failure {
            seed,
            step,
            steps: step.saturating_add(1),
            violation,
            trace: Vec::new(),
        };
        if let Some((at, violation)) = &opts.plant
            && *at == step
        {
            return Err(fail(violation.clone()));
        }
        if let Some((at, retention)) = opts.switch_retention
            && at == step
        {
            runner.retention = retention;
            runner.actor.set_retention(retention);
        }
        if opts.trace {
            runner.report.trace.push(format!("{step}: {op:?}"));
        }
        if runner.lenient && !runner.w.model_matches_sqlite() {
            return Err(Failure {
                trace: runner.report.trace,
                ..fail(bug_fault("the model no longer matches SQLite's rules"))
            });
        }
        if let Err(violation) = runner.apply_judged(op) {
            return Err(Failure {
                trace: runner.report.trace,
                ..fail(violation)
            });
        }
        runner.report.steps = step.saturating_add(1);
    }
    runner.finish().map_err(|violation| Failure {
        seed,
        step: steps,
        steps,
        violation,
        trace: std::mem::take(&mut runner.report.trace),
    })?;
    runner.tally_stores();
    runner.w.history.tally(&mut runner.report.reach);
    runner.report.app_checkpoints = runner.app_seen();
    runner.report.app_autocheckpoints = runner.w.sqlite.app_autocheckpoints();
    if opts.record {
        runner.report.remote_events = runner.w.remote_recorder.events();
        runner.report.staging_events = runner.w.staging_recorder.events();
        runner.report.remote_damaged = runner.w.remote_faults.damaged();
        runner.report.remote_rejected = runner.w.remote_faults.rejected();
    }
    Ok(runner.report)
}

/// The fewest operations of `seed`'s history that still fail, found by
/// halving: a failure at step `k` reproduces in the first `k + 1` operations,
/// and a shorter prefix either passes or fails earlier. `None` when the run
/// passes.
#[must_use]
pub fn shrink(seed: Seed, steps: u32, weights: &Weights, opts: &Opts) -> Option<u32> {
    let failed = run_with(seed, steps, weights, opts).err()?;
    let mut high = failed.step.saturating_add(1).min(steps.max(1));
    let mut low = 0_u32;
    while low.saturating_add(1) < high {
        let mid = low.saturating_add(high.saturating_sub(low) / 2);
        if run_with(seed, mid, weights, opts).is_err() {
            high = mid;
        } else {
            low = mid;
        }
    }
    Some(high)
}

mod after_restart;
mod hold;

#[cfg(test)]
mod tests;

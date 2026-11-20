//! The run loop: one seed, one history, one verdict.
//!
//! [`run`] builds a [`World`] and an [`Actor`], applies each operation of the
//! seed's history in order, and judges the replica at every `Restore`, after
//! every drain and once at the end. The actor is driven the way a host would
//! drive it: each tick's future goes through [`World::run_tick`], the clock
//! follows the `Next` it returns, and a crash drops the actor and builds a new
//! one. A failure names the seed and the step; [`shrink`] finds the shortest
//! prefix that still fails.

use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use lsr_core::{
    Actor, ActorConfig, CheckpointConfig, Clock, Entropy, Env, Event, HaltCause, Maintenance, Next,
    SeededEntropy, SyncConfig, Tick, UploadConfig,
};
use lsr_ltx::{PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{Salts, WalOffset};

use crate::generate::{EnvFault, MetaOp, Op, Target, WalOp, Weights, history};
use crate::oracle::{Bound, Violation, bug_fault, within};
use crate::resources::{
    CheckpointTally, CountingCheckpointer, IdleWindow, Limits, TickStats, delta, memory, per_tick,
};
use crate::seed::{Seed, replay_line};
use crate::sqlite::{CrashKind, ReaderId};
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
    /// A tick that halted the engine.
    Halt(HaltCause),
    /// A snapshot file was staged, taking this transaction.
    Snapshot(Txid),
    /// The oracle judged the replica and found nothing wrong.
    Oracle,
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
    /// Oracle restores of the whole replica.
    pub checks: u32,
    /// The longest the WAL grew, in frames.
    pub max_wal_frames: u32,
    /// Idle ticks that asked to run again at once: a real driver would spin on
    /// them. Each costs the virtual clock `SPIN_STEP`.
    pub spins: u32,
    /// Each operation, when the run was asked to trace.
    pub trace: Vec<String>,
    /// Each event, halt and oracle verdict in order, when the run was asked to
    /// record them.
    pub seen: Vec<Seen>,
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
    /// A halt is expected: it ends the run's ticks without being a bug.
    pub allow_halt: bool,
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
    }
}

/// What one actor step came to.
enum Step {
    Ran,
    Drained,
    Halted(HaltCause),
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
    allow_halt: bool,
    saved: Option<StagedCopy>,
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
            allow_halt: false,
            saved: None,
        }
    }

    fn now(&self) -> Timestamp {
        self.w.clock.now()
    }

    fn reboot(&mut self) {
        self.w.start_driver();
        self.actor = Actor::new(actor_config());
        self.unwatched = true;
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
        let now = self.now();
        let actor = &mut self.actor;
        let fut = async {
            let env = Env {
                wal: &mut wal,
                db: &mut db,
                ck: &mut ck,
                staging: &staging,
                remote: &remote,
            };
            actor.tick(env, now).await
        };
        self.w.run_tick(fut, plan)
    }

    /// Tells the model about each snapshot the tick staged: the database it was
    /// cut from is the tip as the tick began, since the app does nothing during
    /// a tick.
    fn note_snapshots(&mut self, tip: Option<crate::CommitId>, at: usize) {
        let events = self.w.staging_tap.events();
        for event in events.iter().skip(self.staging_seen) {
            if let crate::TapEvent::Put {
                key,
                header: Some(_),
                origin: Some(origin),
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
            if let crate::TapEvent::Put {
                key,
                header: Some(_),
                origin,
            } = event
                && origin.is_none_or(|origin| origin.size() == 0)
                && key.level().get() == 0
                && let Some(commit) = tip
            {
                let rewound = self.w.model.rewound();
                self.w
                    .model
                    .note_snapshot(key.range().max(), commit, rewound, at);
                if self.record {
                    self.report.seen.push(Seen::Snapshot(key.range().max()));
                }
            } else if self.record
                && let crate::TapEvent::Put {
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
        self.reboot();
        self.report.crashes = self.report.crashes.saturating_add(1);
        if kind == CrashKind::Power {
            self.report.power_losses = self.report.power_losses.saturating_add(1);
            self.spilled.clear();
            self.readers.clear();
        }
    }

    fn step(&mut self, plan: Option<CrashPlan>) -> Result<(Step, TickStats), Violation> {
        let at = self.w.remote_tap.len();
        let tip = self.w.model.tip();
        let remote0 = self.w.remote_tap.counts();
        let staging0 = self.w.staging_tap.counts();
        let passive0 = self.tally.count(lsr_core::CheckpointMode::Passive);
        let total0 = self.tally.total();
        let ended = self.run_one(plan);
        self.report.ticks = self.report.ticks.saturating_add(1);
        self.note_snapshots(tip, at);
        if self.trace {
            self.report.trace.push(format!("    tick: {ended:?}"));
        }
        let (step, event) = match ended {
            TickEnd::Done(Tick::Ran { event, next }) => {
                if self.record {
                    self.report.seen.push(Seen::Event(event.clone()));
                }
                if let Event::Uploaded(did) = &event {
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
                (Step::Ran, Some(event))
            }
            TickEnd::Done(Tick::Halted(cause)) => {
                if self.record {
                    self.report.seen.push(Seen::Halt(cause));
                }
                (Step::Halted(cause), None)
            }
            TickEnd::Done(Tick::Drained) => (Step::Drained, None),
            TickEnd::Crashed { polls, kind, .. } => {
                self.last_polls = polls.max(1);
                self.crashed(kind);
                (Step::Crashed, None)
            }
        };
        let stats = TickStats {
            event,
            remote: delta(remote0, self.w.remote_tap.counts()),
            staging: delta(staging0, self.w.staging_tap.counts()),
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

    fn tick(&mut self) -> Result<(), Violation> {
        match self.step(None)?.0 {
            Step::Halted(cause) if self.halt_expected(cause) => Ok(()),
            Step::Halted(cause) => Err(bug_fault(format!("the engine halted: {cause:?}"))),
            Step::Ran | Step::Drained | Step::Crashed => Ok(()),
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
        if self.trace {
            self.report
                .trace
                .push(format!("    then crashed: {kind:?}, lost: {lost:?}"));
        }
        self.crashed(kind);
        Ok(())
    }

    /// Stops the engine and runs it until it says it is drained.
    fn drain(&mut self) -> Result<(), Violation> {
        self.actor.begin_shutdown(self.now());
        for _ in 0..DRAIN_TICKS {
            match self.step(None)?.0 {
                Step::Drained => return Ok(()),
                Step::Halted(HaltCause::Shutdown) => {
                    self.reboot();
                    self.actor.begin_shutdown(self.now());
                }
                Step::Halted(cause) if self.halt_expected(cause) => return Ok(()),
                Step::Halted(cause) => {
                    return Err(bug_fault(format!(
                        "the engine halted in a drain: {cause:?}"
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

    fn page_bytes(&self, fill: u32, stamp: u64) -> Vec<u8> {
        let size = self.w.sqlite.page_size().bytes();
        let mut data = vec![u8::try_from(fill % 256).unwrap_or(0); size];
        for (slot, byte) in data.iter_mut().zip(stamp.to_be_bytes()) {
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
                let stamp = u64::from(self.report.steps) << 16 | u64::from(n);
                Some((page, self.page_bytes(small(&self.rng, 256), stamp)))
            })
            .collect()
    }

    fn all_pages(&self, upto: u32) -> Vec<(PageNo, Vec<u8>)> {
        let size = self.w.sqlite.page_size();
        (1..=upto)
            .filter_map(|n| {
                let page = PageNo::new(n, size)?;
                let stamp = u64::from(self.report.steps) << 16 | u64::from(n);
                Some((page, self.page_bytes(small(&self.rng, 256), stamp)))
            })
            .collect()
    }

    fn app_commit(&mut self, mut pages: Vec<(PageNo, Vec<u8>)>, db_size: u32) {
        let mut all = std::mem::take(&mut self.spilled);
        all.append(&mut pages);
        if self.w.app_commit(&all, db_size).is_some() {
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
    /// every connection closed, the engine's read pin among them
    /// (`sqlite_rules::our_reader_blocks_leaving_wal_mode`). So the operator stops
    /// the engine, the app ends its readers and vacuums, and the engine starts
    /// again; a write transaction the app holds refuses the vacuum.
    fn resize(&mut self, new: PageSize) {
        self.w.crash(CrashKind::Process);
        for id in std::mem::take(&mut self.readers) {
            self.w.sqlite.stranger_reader_end(id);
        }
        if self.w.app_vacuum(new).is_some() {
            self.spilled.clear();
        }
        self.crashed(CrashKind::Process);
    }

    /// The one page size of every file staged or published: a replica prefix
    /// holds one, so a change the replica did not follow halts the engine
    /// before it stages anything. `None` while there is no file.
    fn replica_page_size(&self) -> Result<Option<PageSize>, Violation> {
        let sizes = self.w.page_sizes().map_err(bug_fault)?;
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

    /// Whether a halt is one the run expects: a scenario's, or a page-size
    /// change between the files staged or published and the database.
    fn halt_expected(&self, cause: HaltCause) -> bool {
        if let HaltCause::PageSizeChanged { was, now } = cause {
            return now == self.w.sqlite.page_size()
                && self.replica_page_size().ok().flatten() == Some(was);
        }
        self.allow_halt
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

    fn spill(&mut self) {
        let size = self.w.sqlite.db_size();
        let pages = self.pick_pages(small(&self.rng, 3).saturating_add(1), size);
        if self.w.sqlite.spill(&pages) {
            self.spilled.extend(pages);
        }
    }

    fn reader(&mut self, begin: bool) {
        if begin {
            self.readers.push(self.w.sqlite.stranger_reader_begin());
        } else if !self.readers.is_empty() {
            let id = self.readers.remove(0);
            self.w.sqlite.stranger_reader_end(id);
        }
    }

    fn store_fault(
        &self,
        target: Target,
        op: lsr_core::StoreOp,
        ahead: u8,
        shape: crate::FaultShape,
    ) {
        let faults = match target {
            Target::Staging => &self.w.staging_faults,
            Target::Remote => &self.w.remote_faults,
        };
        faults.push_in(op, u32::from(ahead), shape);
    }

    fn env_fault(&mut self, fault: EnvFault) {
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
        }
    }

    fn clock_jump(&self, millis: i64) {
        let to = self.now().unix_millis().saturating_add(millis);
        self.w.clock.set(Timestamp::from_unix_millis(to));
    }

    fn restore_all(&mut self) -> Result<(), Violation> {
        self.report.checks = self.report.checks.saturating_add(1);
        self.w.check()?;
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
                    self.saved = Some(copy);
                    true
                })
                .map_err(StagedEditError::from),
            MetaOp::Restore => match &self.saved {
                Some(copy) => w.replace_staging(copy).map(|()| true),
                None => Ok(false),
            },
            MetaOp::DeleteNewest => w.delete_newest_staged().map_err(StagedEditError::from),
            MetaOp::FlipNewestByte => w.edit_newest_staged(|bytes| {
                let middle = bytes.len() / 2;
                if let Some(byte) = bytes.get_mut(middle) {
                    *byte ^= 0xff;
                }
            }),
            MetaOp::StripNewestOrigin => w.strip_newest_origin(),
            MetaOp::TruncateNewest => w.edit_newest_staged(|bytes| {
                let half = bytes.len() / 2;
                bytes.truncate(half);
            }),
        };
        Self::scenario_step(&format!("{op:?}"), done)
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
        };
        Self::scenario_step(&format!("{op:?}"), Ok(applied))?;
        self.w.adopt_local_state();
        Ok(())
    }

    fn apply(&mut self, op: &Op) -> Result<(), Violation> {
        match op {
            Op::Commit { pages, grow } => self.commit(*pages, *grow),
            Op::Spill => self.spill(),
            Op::Rollback => {
                if self.w.sqlite.rollback() {
                    self.spilled.clear();
                }
            }
            Op::Vacuum { page_size } => self.vacuum(*page_size),
            Op::StrangerReader { begin } => self.reader(*begin),
            Op::StrangerCheckpoint(mode) => {
                self.w.stranger_checkpoint(*mode);
            }
            Op::Crash(kind) => self.crash_tick(*kind)?,
            Op::StoreFault {
                target,
                op,
                ahead,
                shape,
            } => self.store_fault(*target, *op, *ahead, shape.clone()),
            Op::EnvFault(fault) => self.env_fault(*fault),
            Op::ClockJump(millis) => self.clock_jump(*millis),
            Op::Tick => self.tick()?,
            Op::Shutdown => {
                self.report.shutdowns = self.report.shutdowns.saturating_add(1);
                self.drain()?;
                self.reboot();
            }
            Op::RequestSnapshot => self.actor.request_snapshot(),
            Op::Restore => self.restore_all()?,
            Op::Restart => self.restart(CrashKind::Process),
            Op::PowerLoss => self.restart(CrashKind::Power),
            Op::Meta(op) => self.meta(*op)?,
            Op::Wal(op) => self.wal_damage(*op)?,
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
            if let Step::Halted(cause) = step {
                if self.halt_expected(cause) {
                    return Ok(());
                }
                return Err(bug_fault(format!(
                    "the engine halted while idle: {cause:?}"
                )));
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
            if let Step::Halted(cause) = step {
                if self.halt_expected(cause) {
                    return Ok(());
                }
                return Err(bug_fault(format!(
                    "the engine halted while idle: {cause:?}"
                )));
            }
            window.add(&stats);
        }
        window.check()
    }

    /// The end of a history: drain, judge the replica, check the resources.
    fn finish(&mut self) -> Result<(), Violation> {
        self.drain()?;
        self.restore_all()?;
        memory(self.w.remote_tap.counts(), self.w.staging_tap.counts())?;
        self.idle()?;
        self.replica_page_size().map(drop)
    }
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
    run_ops(seed, &history(seed, steps, weights), opts)
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
    runner.trace = opts.trace;
    runner.record = opts.record;
    runner.allow_halt = opts.allow_halt;
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
        if opts.trace {
            runner.report.trace.push(format!("{step}: {op:?}"));
        }
        if let Err(violation) = runner.apply(op) {
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

#[cfg(test)]
mod tests;

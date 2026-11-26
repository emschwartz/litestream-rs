//! Scenarios: a fixed list of operations and the events the engine and the
//! oracle must show while running it.
//!
//! A scenario runs through the same [`run_ops`] as a random history, so the
//! oracle and the resource properties judge it too; then its expectations are
//! matched, in order, against what the run recorded. [`scenario_of`] has one
//! arm per [`SnapshotCause`] and no wildcard: a new cause does not compile
//! until it names its scenarios.
//!
//! A scenario runs under one set of [`Responses`], the default unless
//! [`Scenario::under`] says otherwise. [`Expect::Stopped`] reads that set: a
//! cause whose response is to stop must stop the engine, one whose response
//! is to keep trying must degrade a task, and [`Expect::Recovered`] then asks
//! for that task to recover once the fault clears (it is skipped where the
//! engine stops). So one list of expectations states both behaviours, and the
//! fault tests run each persistent fault's scenarios under both presets.

use lsr_core::{
    CheckpointMode, DegradedCause, Level, MaintainedEvent, Responses, SnapshotCause, StopCause,
};
use lsr_ltx::PageSize;

use crate::generate::{EnvFault, MetaOp, Op, RemoteOp, WalOp};
use crate::run::{Opts, RunReport, Seen, run_ops};
use crate::seed::Seed;
use crate::sqlite::{CrashKind, OtherProcessMode};

/// A [`SnapshotCause`] without its payload; core's name for it is
/// [`lsr_core::SnapshotCauseKind`].
pub use lsr_core::SnapshotCauseKind as CauseKind;

/// An engine event a scenario expects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventMatch {
    /// The engine booted; the first sync is a snapshot for this cause, or
    /// (`None`) is not one.
    Booted(Option<CauseKind>),
    /// The next sync became a snapshot for this cause.
    Resnapshot(CauseKind),
    /// A snapshot file was staged. The causes that set the next sync's kind
    /// without an event of their own (an operator's request, the checkpoint
    /// boundary) show only here.
    Snapshotted,
    /// A snapshot file was staged taking exactly this transaction.
    SnapshotAt(u64),
    /// A checkpoint of this mode ran.
    Checkpointed(CheckpointMode),
    /// A step failed with a fault whose policy is to retry it.
    Retrying(FaultKind),
    /// A checkpoint was due and the write lock was busy, so it waits
    /// (`lsr_core::CheckpointOutcome::LockBusy`).
    CheckpointDeferred,
    /// A step that writes to the database waited: the replicator held the application's
    /// writes (`lsr_core::Fault::WritesHeld`).
    WritesHeld,
    /// The clock read earlier than the last time used and was clamped.
    Clamped,
    /// The read-back after a boot deleted a file of this level that did not
    /// verify.
    Repaired(Level),
}

/// A judgment of a whole run that the expectations cannot make.
pub type Check = fn(&RunReport) -> Result<(), String>;

/// One expectation of a scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// An event.
    Event(EventMatch),
    /// The engine met this cause: it stopped for it where the scenario's
    /// responses stop on it, and a task is degraded by it where they keep
    /// trying.
    Stopped(StopCause),
    /// A task that a kept-trying cause degraded recovered on the same
    /// engine. Skipped where the scenario's responses stop.
    Recovered,
    /// The oracle restored and judged the replica and found nothing wrong.
    Oracle,
}

/// Whether `responses` stop the engine on `cause`; the end of a drain always does.
#[must_use]
pub fn stops(responses: &Responses, cause: StopCause) -> bool {
    DegradedCause::of(cause).is_none_or(|cause| responses.stops(cause))
}

impl Expect {
    fn matches(&self, seen: &Seen, responses: &Responses) -> bool {
        use lsr_core::Event;
        match (self, seen) {
            (
                Self::Event(EventMatch::Booted(want)),
                Seen::Event(lsr_core::Event::Booted { cause }),
            ) => cause.as_ref().map(CauseKind::from) == *want,
            (Self::Event(EventMatch::Resnapshot(want)), Seen::Event(Event::Resnapshot(cause))) => {
                CauseKind::from(cause) == *want
            }
            (Self::Event(EventMatch::Snapshotted), Seen::Snapshot(_)) => true,
            (Self::Event(EventMatch::SnapshotAt(want)), Seen::Snapshot(got)) => got.get() == *want,
            (
                Self::Event(EventMatch::Checkpointed(want)),
                Seen::Event(Event::Checkpointed { mode, .. }),
            ) => want == mode,
            (
                Self::Event(EventMatch::Retrying(want)),
                Seen::Event(Event::Retrying { fault, .. }),
            ) => FaultKind::from(fault) == *want,
            (Self::Event(EventMatch::Clamped), Seen::Event(Event::Clamped(_)))
            | (Self::Event(EventMatch::WritesHeld), Seen::Event(Event::WritesHeld { .. }))
            | (
                Self::Event(EventMatch::CheckpointDeferred),
                Seen::Event(Event::CheckpointDeferred { .. }),
            ) => true,
            (
                Self::Event(EventMatch::Repaired(want)),
                Seen::Event(Event::Maintained {
                    event: MaintainedEvent::Repaired(key),
                    ..
                }),
            ) => key.level() == *want,
            (Self::Stopped(want), Seen::Stop(got)) => stops(responses, *want) && want == got,
            (Self::Stopped(want), Seen::Event(Event::Degraded { cause, .. })) => {
                !stops(responses, *want) && Some(*want) == cause.stop()
            }
            (Self::Recovered, Seen::Recovered(_)) | (Self::Oracle, Seen::Oracle) => true,
            (Self::Event(_) | Self::Stopped(_) | Self::Recovered | Self::Oracle, _) => false,
        }
    }
}

/// A fixed list of operations and what must be seen while it runs.
#[derive(Clone, Debug)]
pub struct Scenario {
    /// The name, as a test and a replay line show it.
    pub name: &'static str,
    /// The seed: it draws the salts SQLite picks and what a power loss drops.
    pub seed: Seed,
    /// The operations.
    pub ops: Vec<Op>,
    /// What must be seen, in this order, with anything else allowed between.
    pub expect: Vec<Expect>,
    /// A judgment of the whole run, for what no single event shows: a call
    /// that must not happen, an order of calls.
    pub check: Option<Check>,
    /// What each persistent fault does in this run.
    pub responses: Responses,
    /// The replicator holds the application's writes under this fail-stop setting
    /// ([`crate::run::Opts::write_gate`]); `None` runs no write gate.
    pub write_gate: Option<lsr_core::IfReplicationStops>,
}

impl Scenario {
    /// The same scenario under other responses.
    #[must_use]
    pub const fn under(mut self, responses: Responses) -> Self {
        self.responses = responses;
        self
    }

    /// Whether the scenario's persistent fault keeps a task trying under its
    /// responses.
    #[must_use]
    pub fn keeps_trying(&self) -> bool {
        self.expect
            .iter()
            .any(|want| matches!(want, Expect::Stopped(cause) if !stops(&self.responses, *cause)))
    }
}

/// Runs a scenario and matches its expectations.
///
/// # Errors
/// The failing property or the first expectation that never showed, with what
/// was seen.
pub fn run_scenario(scenario: &Scenario) -> Result<RunReport, String> {
    let opts = Opts {
        record: true,
        allow_stop: scenario
            .expect
            .iter()
            .any(|e| matches!(e, Expect::Stopped(_))),
        responses: scenario.responses,
        write_gate: scenario.write_gate,
        ..Opts::default()
    };
    // The failure's own replay line names the seed's random history, which
    // is not this scenario: its operations are fixed, so the test replays it.
    let report = run_ops(scenario.seed, &scenario.ops, &opts).map_err(|failure| {
        format!(
            "{name}: failed at step {}: {}\nreplay: cargo nextest run -p lsr-sim --run-ignored all {name}",
            failure.step,
            failure.violation,
            name = scenario.name
        )
    })?;
    let mut seen = report.seen.iter();
    let keeps_trying = scenario.keeps_trying();
    let wanted = scenario
        .expect
        .iter()
        .enumerate()
        .filter(|(_, want)| keeps_trying || **want != Expect::Recovered);
    for (index, want) in wanted {
        if !seen.any(|got| want.matches(got, &scenario.responses)) {
            return Err(format!(
                "{}: expectation {index} ({want:?}) never showed; seen {:?}",
                scenario.name, report.seen
            ));
        }
    }
    if let Some(check) = scenario.check {
        check(&report)
            .map_err(|why| format!("{}: {why}; seen {:?}", scenario.name, report.seen))?;
    }
    Ok(report)
}

/// The scenarios that make the engine take a snapshot for `cause`.
#[must_use]
pub fn scenario_of(cause: SnapshotCause) -> &'static [fn() -> Scenario] {
    match cause {
        SnapshotCause::FirstSync => &[empty_meta_boot],
        SnapshotCause::BehindRemote { .. } => &[vm_restore_stops_until_trust_local],
        SnapshotCause::NewestStagedFileUnreadable => &[
            newest_staged_file_byte_flip,
            newest_staged_file_truncated,
            newest_staged_file_no_wal_origin,
        ],
        SnapshotCause::WalMissing => &[
            wal_deleted_while_down,
            wal_header_torn,
            restored_with_no_wal,
            restart_between_steps,
        ],
        SnapshotCause::GenerationChanged => &[
            app_checkpoint_while_down,
            restart_during_read_lock_lapse,
            pinned_on_zero_single_restart,
            truncate_failed_after_the_index_reset,
        ],
        SnapshotCause::PrevFrameMismatch => &[wal_rewritten_same_length],
        SnapshotCause::LastPageMismatch => {
            &[wal_regrown_same_salts_diff_bytes, meta_wiped_wal_regrown]
        }
        SnapshotCause::LocalRewound { .. } => &[power_loss_wal_rewind],
        SnapshotCause::CheckpointBoundary => &[truncate_threshold_crossed],
        SnapshotCause::OperatorRequested => &[cli_snapshot],
        SnapshotCause::StagedUnverified { .. } => &[pending_file_that_does_not_chain],
        SnapshotCause::AheadOfReplica { .. } => &[
            older_copy_taken_after_the_last_counter_bump,
            meta_lost_with_commits_not_uploaded,
            meta_wiped_wal_restarted,
        ],
    }
}

const SEED: u64 = 6;
const SETTLE: usize = 8;

fn ticks(n: usize) -> Vec<Op> {
    vec![Op::Tick; n]
}

fn commit(pages: u8, grow: bool) -> Op {
    Op::Commit { pages, grow }
}

/// A snapshot and then two commits as one more file, replicated and drained;
/// the engine is down, staging holds both files, and the next tick boots a new
/// engine.
fn synced() -> Vec<Op> {
    vec![
        commit(3, false),
        Op::Shutdown,
        commit(2, true),
        commit(4, false),
        Op::Shutdown,
    ]
}

fn build(name: &'static str, ops: Vec<Vec<Op>>, expect: Vec<Expect>) -> Scenario {
    Scenario {
        name,
        seed: Seed::new(SEED),
        ops: ops.into_iter().flatten().collect(),
        expect,
        check: None,
        responses: Responses::GO_LIKE,
        write_gate: None,
    }
}

fn snapshot_after_boot(cause: CauseKind) -> Vec<Expect> {
    vec![
        Expect::Event(EventMatch::Booted(Some(cause))),
        Expect::Oracle,
    ]
}

fn after_restart(name: &'static str, damage: Vec<Op>, cause: CauseKind) -> Scenario {
    after_restart_then(name, damage, Vec::new(), cause)
}

/// As [`after_restart`], with `then` after the boot tick: the app's write that
/// heals a WAL the engine's own header write could not (a torn header).
fn after_restart_then(
    name: &'static str,
    damage: Vec<Op>,
    then: Vec<Op>,
    cause: CauseKind,
) -> Scenario {
    build(
        name,
        vec![
            synced(),
            damage,
            vec![Op::Tick],
            then,
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        snapshot_after_boot(cause),
    )
}

/// No staging directory at all: the first sync is a snapshot.
#[must_use]
pub fn empty_meta_boot() -> Scenario {
    build(
        "empty_meta_boot",
        vec![vec![commit(3, false)], ticks(SETTLE), vec![Op::Restore]],
        snapshot_after_boot(CauseKind::FirstSync),
    )
}

/// The staging directory is wiped while the replica keeps its files: boot
/// reads the replica's newest level-0 file as the newest staged file, the WAL still matches
/// it, and the engine continues with no snapshot.
#[must_use]
pub fn meta_wiped_remote_kept() -> Scenario {
    let mut s = build(
        "meta_wiped_remote_kept",
        vec![
            synced(),
            vec![
                Op::EnvFault(EnvFault::WipeStaging),
                Op::Tick,
                commit(2, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Booted(None)), Expect::Oracle],
    );
    s.check = Some(last_boot_continues);
    s
}

/// The boot after the wipe (the last before the oracle judged) took no
/// snapshot and staged none before the oracle, and asked the replica for one
/// listing per level (the upload task's), the upload task's read-back of the newest file,
/// and one read of that file as the newest staged file.
fn last_boot_continues(report: &RunReport) -> Result<(), String> {
    let judged = report
        .seen
        .iter()
        .position(|seen| matches!(seen, Seen::Oracle))
        .ok_or("the oracle never judged")?;
    let seen = report.seen.get(..judged).unwrap_or_default();
    let boot = seen
        .iter()
        .rposition(|seen| matches!(seen, Seen::Event(lsr_core::Event::Booted { .. })))
        .ok_or("no boot")?;
    let (before, after) = seen.split_at(boot);
    if !matches!(
        after.first(),
        Some(Seen::Event(lsr_core::Event::Booted { cause: None }))
    ) {
        return Err(format!("the boot after the wipe was {:?}", after.first()));
    }
    if after.iter().any(|seen| matches!(seen, Seen::Snapshot(_))) {
        return Err("a snapshot was staged after the wipe".to_owned());
    }
    let levels = u64::try_from(Level::ALL.len()).map_err(|err| err.to_string())?;
    match before.last() {
        Some(Seen::Tick {
            remote_calls,
            remote_lists,
            ..
        }) if *remote_lists == levels && remote_calls.checked_sub(levels) == Some(2) => Ok(()),
        other => Err(format!("the boot's replica calls were {other:?}")),
    }
}

/// The staging directory is wiped and, while the engine is down, an outside
/// TRUNCATE and a commit restart the WAL. The WAL cannot vouch for the replica's newest file and
/// the meta directory is brand new, so the boot asks whether the database is the replica's
/// state: it is that state plus the commit, written after the replica's newest file, so the boot
/// snapshots above it (`AheadOfReplica`). It used to stop here, until the operator trusted the
/// local database (`docs/decisions/D194.md`).
#[must_use]
pub fn meta_wiped_wal_restarted() -> Scenario {
    let restarted = vec![
        Op::OurReader { begin: false },
        Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
        commit(2, false),
        Op::EnvFault(EnvFault::WipeStaging),
    ];
    after_restart(
        "meta_wiped_wal_restarted",
        restarted,
        CauseKind::AheadOfReplica,
    )
}

/// The staging directory is wiped and the WAL's frames are regrown in the
/// same generation with other bytes: only the newest staged file's page bytes, read back from
/// the replica, show it.
#[must_use]
pub fn meta_wiped_wal_regrown() -> Scenario {
    let regrown = vec![
        Op::Wal(WalOp::RegrowSameSalts(3)),
        Op::EnvFault(EnvFault::WipeStaging),
    ];
    after_restart(
        "meta_wiped_wal_regrown",
        regrown,
        CauseKind::LastPageMismatch,
    )
}

/// Issue 329: the replica refuses an upload, so a commit is staged and not uploaded; the process
/// dies with its meta directory (`WipeStaging`); the application's last close checkpoints
/// and empties the WAL; the replicator starts with a brand-new meta directory. Nothing local
/// vouches for the replica's newest file, and the database is not the replica's state, but it
/// was written after that file and holds most of its pages: the boot snapshots above it
/// (`AheadOfReplica`) instead of stopping, and every state restores.
#[must_use]
pub fn meta_lost_with_commits_not_uploaded() -> Scenario {
    let refused = Op::StoreFault {
        target: crate::generate::Target::Remote,
        op: lsr_core::StoreOp::Upload,
        ahead: 0,
        shape: crate::store::FaultShape::Error(lsr_core::StoreError::Transient {
            op: lsr_core::StoreOp::Upload,
            kind: std::io::ErrorKind::ConnectionReset,
        }),
    };
    build(
        "meta_lost_with_commits_not_uploaded",
        vec![
            synced(),
            vec![
                Op::Tick,
                refused,
                commit(2, false),
                Op::Tick,
                Op::EnvFault(EnvFault::WipeStaging),
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                Op::Tick,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        snapshot_after_boot(CauseKind::AheadOfReplica),
    )
}

/// The history of [`faults::older_copy_on_a_new_host`]: the application's database is made
/// with the replicator's counter at 1, backed up, committed on to a counter of `later_seq`, and
/// replicated; then the backup goes back in place of the database and the working folder is
/// gone. The expectations are the caller's.
fn older_copy_history(name: &'static str, later_seq: u32, adopt: bool) -> Scenario {
    build(
        name,
        vec![
            vec![Op::Structured { seq: 1, data: 1 }, Op::Shutdown],
            vec![Op::SaveDatabase],
            vec![
                Op::Structured {
                    seq: later_seq,
                    data: 2,
                },
                Op::Shutdown,
            ],
            vec![
                Op::EnvFault(EnvFault::WipeStaging),
                Op::OurReader { begin: false },
                Op::PutBackDatabase { adopt },
            ],
            vec![Op::Tick],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        Vec::new(),
    )
}

/// The window the counter rule leaves, pinned: the same history with the counter unchanged
/// since the backup (the backup was taken after the replica's last counter bump, and the
/// commits after it did not reach another). The older database passes for the replica plus
/// more and the boot snapshots above it (`AheadOfReplica`), as it would for a database the
/// application did write on (issue 329).
#[must_use]
pub fn older_copy_taken_after_the_last_counter_bump() -> Scenario {
    let mut s = older_copy_history("older_copy_taken_after_the_last_counter_bump", 1, true);
    s.expect = snapshot_after_boot(CauseKind::AheadOfReplica);
    s
}

/// The newest staged file is gone but an older one remains: the replica is ahead.
#[must_use]
pub fn newest_staged_file_lost() -> Scenario {
    // The replica's newest file is above what staging holds now, and the live
    // WAL still holds its range: it vouches for it, and the boot continues
    // from it with no snapshot (it took a `BehindRemote` snapshot before the
    // boot asked the WAL).
    let mut s = build(
        "newest_staged_file_lost",
        vec![
            synced(),
            vec![Op::Meta(MetaOp::DeleteNewest), Op::Tick],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Booted(None)), Expect::Oracle],
    );
    s.check = Some(no_refusal);
    s
}

/// One deploy on a host whose volume outlives the machine (fly.io replaces the
/// VM on every deploy; the database and its meta directory stay): the app
/// commits and the replicator ships it, then `stop` ends the process.
fn deploy(stop: Vec<Op>) -> Vec<Op> {
    let mut ops = vec![commit(2, false)];
    ops.extend(ticks(3));
    ops.extend([commit(1, false), commit(2, false)]);
    ops.extend(stop);
    ops
}

/// Three deploys that each end with SIGTERM: the final sync completes and the
/// process exits. The app closed its connections before the replicator
/// stopped, so the replicator's close is the last and keeps the WAL (its own
/// connections set `PERSIST_WAL`). Each new process
/// continues where the last stopped: no snapshot after the first, no stop.
#[must_use]
pub fn deploy_cycle_after_sigterm() -> Scenario {
    let mut s = build(
        "deploy_cycle_after_sigterm",
        vec![
            deploy(vec![Op::Shutdown]),
            deploy(vec![Op::Shutdown]),
            deploy(vec![Op::Shutdown]),
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Booted(None)), Expect::Oracle],
    );
    s.check = Some(deploys_continue);
    s
}

/// Three deploys that each end in SIGKILL during the final sync (fly.io's kill
/// timeout is below the shutdown sync's): the process dies at a point of the
/// sync or upload it was in. The next process continues: no snapshot, no stop.
#[must_use]
pub fn deploy_cycle_after_sigkill() -> Scenario {
    let kill = || vec![Op::Crash(CrashKind::Process)];
    let mut s = build(
        "deploy_cycle_after_sigkill",
        vec![
            deploy(kill()),
            deploy(kill()),
            deploy(kill()),
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Booted(None)), Expect::Oracle],
    );
    s.check = Some(deploys_continue);
    s
}

/// SIGKILL just after the engine's own PASSIVE checkpoint let the WAL restart,
/// before the new generation's first file was staged; a file staged in the
/// old generation was still pending (its upload's answer lost, though the
/// replica holds it). The next process cannot tell this WAL from one the app
/// restarted after frames the engine never saw, and needs no proof: the
/// pending file is on the replica, the boot reads it whole and goes on into
/// the new generation (BOOT-11 in `docs/BEHAVIOR.md`). No snapshot, no stop.
#[must_use]
pub fn deploy_cycle_sigkill_after_a_wal_restart_continues() -> Scenario {
    let lost_answer = Op::StoreFault {
        target: crate::generate::Target::Remote,
        op: lsr_core::StoreOp::Upload,
        ahead: 0,
        shape: crate::store::FaultShape::ErrorAfterPublish(lsr_core::StoreError::Transient {
            op: lsr_core::StoreOp::Upload,
            kind: std::io::ErrorKind::ConnectionReset,
        }),
    };
    let mut s = build(
        "deploy_cycle_sigkill_after_a_wal_restart_continues",
        vec![
            deploy(vec![Op::Shutdown]),
            vec![commit(2, false), Op::Tick, lost_answer, Op::Tick],
            vec![Op::Crash(CrashKind::Process)],
            ticks(SETTLE),
            vec![commit(1, false)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Booted(None)), Expect::Oracle],
    );
    s.check = Some(no_refusal);
    s
}

/// A deploy whose app closes its connections after the replicator's: its last
/// close checkpoints the WAL into the database and deletes it. The next
/// process finds no WAL and snapshots (`WalMissing`, BOOT-7 in
/// `docs/BEHAVIOR.md`). It never stops: staging still holds the replica's
/// newest file.
#[must_use]
pub fn deploy_cycle_with_the_wal_deleted_at_close() -> Scenario {
    let closed = vec![
        Op::Shutdown,
        Op::OurReader { begin: false },
        Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
        Op::EnvFault(EnvFault::DeleteWal),
    ];
    let mut s = build(
        "deploy_cycle_with_the_wal_deleted_at_close",
        vec![
            deploy(closed),
            vec![Op::Tick, commit(1, false)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Booted(Some(CauseKind::WalMissing))),
            Expect::Oracle,
        ],
    );
    s.check = Some(no_refusal);
    s
}

/// Every boot after the first continued, nothing resnapshotted, and nothing
/// stopped but the drains the deploys asked for.
fn deploys_continue(report: &RunReport) -> Result<(), String> {
    let mut boots = 0_u32;
    for seen in &report.seen {
        match seen {
            Seen::Event(lsr_core::Event::Booted { cause }) => {
                boots = boots.saturating_add(1);
                if boots > 1
                    && let Some(cause) = cause
                {
                    return Err(format!("boot {boots} snapshotted: {cause:?}"));
                }
            }
            Seen::Event(lsr_core::Event::Resnapshot(cause)) => {
                return Err(format!("a running engine resnapshotted: {cause:?}"));
            }
            Seen::Stop(cause) if *cause != StopCause::Shutdown => {
                return Err(format!("stopped: {cause:?}"));
            }
            Seen::Event(_)
            | Seen::Stop(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    if boots < 2 {
        return Err(format!("only {boots} boots"));
    }
    Ok(())
}

/// No boot refused replica files: every one continued or snapshotted.
fn no_refusal(report: &RunReport) -> Result<(), String> {
    if report
        .seen
        .iter()
        .any(|seen| matches!(seen, Seen::Stop(StopCause::OtherWriter)))
    {
        return Err("a boot refused files the WAL vouches for".to_owned());
    }
    Ok(())
}

/// A VM restored to an older image while down: staging put back from a copy
/// taken before the replica moved on, and the WAL no longer holding the
/// replica's newest files. Nothing local tells them from another writer's:
/// the boot stops, and once the operator runs `lsr trust-local` it snapshots above them.
#[must_use]
pub fn vm_restore_stops_until_trust_local() -> Scenario {
    let mut s = build(
        "vm_restore_stops_until_trust_local",
        vec![
            vec![commit(3, false), Op::Shutdown, Op::Meta(MetaOp::Save)],
            vec![commit(2, false), commit(2, false), Op::Shutdown],
            vec![
                Op::Meta(MetaOp::Restore),
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                commit(1, false),
                Op::Restart,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Stopped(StopCause::OtherWriter),
            Expect::Event(EventMatch::Booted(Some(CauseKind::BehindRemote))),
            Expect::Oracle,
        ],
    );
    s.check = Some(stops_then_trusts_local);
    s
}

/// A stop for another writer, then a `BehindRemote` boot after `lsr trust-local`.
fn stops_then_trusts_local(report: &RunReport) -> Result<(), String> {
    let stop = report
        .seen
        .iter()
        .position(|seen| matches!(seen, Seen::Stop(StopCause::OtherWriter)))
        .ok_or("the boot never refused")?;
    let snapshot = report.seen.iter().skip(stop).any(|seen| {
        matches!(
            seen,
            Seen::Event(lsr_core::Event::Booted {
                cause: Some(SnapshotCause::BehindRemote { .. })
            })
        )
    });
    if snapshot {
        Ok(())
    } else {
        Err("no snapshot above the replica's files after trust-local".to_owned())
    }
}

/// Another writer put a file on the replica while the engine was down. The
/// boot stops, and stays stopped while the file stands; once the operator
/// removes it and restarts, the engine replicates.
#[must_use]
pub fn foreign_file_while_down_stops() -> Scenario {
    let mut s = build(
        "foreign_file_while_down_stops",
        vec![
            synced(),
            vec![
                Op::Shutdown,
                Op::Remote(RemoteOp::PutUnrecognized),
                Op::Restart,
            ],
            ticks(SETTLE),
            vec![
                Op::Remote(RemoteOp::RemoveUnrecognized),
                Op::Restart,
                commit(1, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Stopped(StopCause::OtherWriter), Expect::Oracle],
    );
    s.check = Some(refused_until_removed);
    s
}

/// The restart's boot stopped for another writer without building above the
/// file (no snapshot between the last upload before the stop and the stop),
/// and the engine replicated again once the file went.
fn refused_until_removed(report: &RunReport) -> Result<(), String> {
    let seen = &report.seen;
    let stop = seen
        .iter()
        .position(|seen| matches!(seen, Seen::Stop(StopCause::OtherWriter)))
        .ok_or("the boot never refused")?;
    let before = seen.get(..stop).unwrap_or_default();
    let last_upload = before
        .iter()
        .rposition(|seen| matches!(seen, Seen::Event(lsr_core::Event::Uploaded(_))))
        .unwrap_or(0);
    let booted_over = before.iter().skip(last_upload).any(|seen| {
        matches!(
            seen,
            Seen::Snapshot(_) | Seen::Event(lsr_core::Event::Booted { cause: Some(_) })
        )
    });
    if booted_over {
        return Err("the engine booted over the other writer's file".to_owned());
    }
    let uploaded_after = seen
        .iter()
        .skip(stop)
        .any(|seen| matches!(seen, Seen::Event(lsr_core::Event::Uploaded(_))));
    if uploaded_after {
        Ok(())
    } else {
        Err("the engine never replicated after the file went".to_owned())
    }
}

/// A staging backup holding the first transaction 1 is put back after the bucket
/// rolled back and the engine took transaction 1 again, from a later commit, so the
/// replica holds other bytes under that key. The boot reads the replica's file against the
/// staged one, finds them different, deletes the staged copy and snapshots above the
/// replica's newest file: the replica's object is never written over.
#[must_use]
pub fn staging_put_back_with_other_bytes_is_not_written_over_the_replica() -> Scenario {
    build(
        "staging_put_back_with_other_bytes_is_not_written_over_the_replica",
        vec![
            vec![Op::Shutdown, commit(3, false), Op::Meta(MetaOp::Save)],
            vec![
                Op::Remote(RemoteOp::RollBack),
                Op::EnvFault(EnvFault::WipeStaging),
            ],
            // Boot, sync, upload, snapshot and one maintenance listing: the engine stops short
            // of the 60 s checkpoint that would stage a second file.
            ticks(5),
            vec![
                Op::Shutdown,
                Op::Meta(MetaOp::Restore),
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                commit(1, false),
                Op::Restart,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Booted(Some(
                CauseKind::NewestStagedFileUnreadable,
            ))),
            Expect::Oracle,
        ],
    )
}

/// A backup of the staging directory is put back after the replica moved on,
/// while the live WAL still holds the replica's newest file: the WAL vouches
/// for it, and the boot continues from it (it took a `BehindRemote` snapshot
/// before the boot asked the WAL).
#[must_use]
pub fn restored_old_meta() -> Scenario {
    let mut s = build(
        "restored_old_meta",
        vec![
            vec![commit(3, false), Op::Shutdown, Op::Meta(MetaOp::Save)],
            vec![commit(2, false), commit(2, false), Op::Shutdown],
            vec![Op::Meta(MetaOp::Restore)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Booted(None)), Expect::Oracle],
    );
    s.check = Some(no_refusal);
    s
}

/// An upload of the engine's own published and was reported as other bytes, and the delete that
/// would clear it failed: the replica holds the transaction, unconfirmed. Its staged copy then
/// turns unreadable, so the running engine drops it, and the process dies before the snapshot
/// that replaces it is staged, with the WAL restarted meanwhile so it no longer holds the file.
/// The takeover the engine keeps before the drop must reach that file (the newest put it
/// sent), or the next boot takes it for another writer's and stops.
#[must_use]
pub fn a_running_drop_of_an_unconfirmed_upload_keeps_a_takeover_through_it() -> Scenario {
    use crate::generate::Target;
    use crate::store::FaultShape;
    let transient = |op| {
        FaultShape::Error(lsr_core::StoreError::Transient {
            op,
            kind: std::io::ErrorKind::ConnectionReset,
        })
    };
    let fail = |target, op, shape| Op::StoreFault {
        target,
        op,
        ahead: 0,
        shape,
    };
    let mut s = build(
        "a_running_drop_of_an_unconfirmed_upload_keeps_a_takeover_through_it",
        vec![
            vec![
                commit(3, false),
                Op::Shutdown,
                commit(2, false),
                Op::Tick,
                Op::Tick,
            ],
            vec![
                fail(
                    Target::Remote,
                    lsr_core::StoreOp::Upload,
                    FaultShape::ChecksumMismatch,
                ),
                fail(
                    Target::Remote,
                    lsr_core::StoreOp::Delete,
                    transient(lsr_core::StoreOp::Delete),
                ),
                Op::Tick,
                Op::Meta(MetaOp::FlipNewestByte),
                fail(
                    Target::Staging,
                    lsr_core::StoreOp::Upload,
                    transient(lsr_core::StoreOp::Upload),
                ),
                Op::Tick,
                Op::Tick,
                Op::Tick,
                Op::Tick,
            ],
            vec![
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                commit(1, false),
                Op::Restart,
            ],
            ticks(SETTLE * 2),
            vec![Op::Restore],
        ],
        vec![Expect::Oracle],
    );
    s.check = Some(no_refusal);
    s
}

/// One byte of the newest staged file changed on disk. The replica already holds the newest staged file, so
/// the torn copy is deleted and the snapshot takes the transaction after it.
#[must_use]
pub fn newest_staged_file_byte_flip() -> Scenario {
    let flip = vec![Op::Meta(MetaOp::FlipNewestByte)];
    torn_newest_staged_file("newest_staged_file_byte_flip", flip)
}

/// The newest staged file ends early, cut directly; otherwise as [`newest_staged_file_byte_flip`].
#[must_use]
pub fn newest_staged_file_truncated() -> Scenario {
    let cut = vec![Op::Meta(MetaOp::TruncateNewest)];
    torn_newest_staged_file("newest_staged_file_truncated", cut)
}

/// The newest staged file of [`synced`] is damaged while the engine is down. [`synced`]
/// stages and uploads transactions 1 and 2, so the torn newest staged file is 2, which the
/// replica holds: the snapshot must take 3.
fn torn_newest_staged_file(name: &'static str, damage: Vec<Op>) -> Scenario {
    build(
        name,
        vec![synced(), damage, ticks(SETTLE), vec![Op::Restore]],
        vec![
            Expect::Event(EventMatch::Booted(Some(
                CauseKind::NewestStagedFileUnreadable,
            ))),
            Expect::Event(EventMatch::SnapshotAt(3)),
            Expect::Oracle,
        ],
    )
}

/// The newest staged file's header names no WAL range, so there is no position to resume from.
#[must_use]
pub fn newest_staged_file_no_wal_origin() -> Scenario {
    let stripped = vec![Op::Meta(MetaOp::StripNewestOrigin)];
    after_restart(
        "newest_staged_file_no_wal_origin",
        stripped,
        CauseKind::NewestStagedFileUnreadable,
    )
}

/// The WAL file was deleted while the engine was down.
#[must_use]
pub fn wal_deleted_while_down() -> Scenario {
    let gone = vec![
        Op::OurReader { begin: false },
        Op::EnvFault(EnvFault::DeleteWal),
    ];
    after_restart_then(
        "wal_deleted_while_down",
        gone,
        vec![commit(1, false)],
        CauseKind::WalMissing,
    )
}

/// A database a restore just published: the database file holds every
/// transaction, the WAL is empty with no header, the staging directory is gone
/// and the replica holds the newest staged file. The real driver writes nothing at open once
/// `_litestream_seq` exists, so the boot finds no header and the first sync is a
/// `WalMissing` snapshot. It writes the header itself and runs at once, with no
/// write from the app and no retry.
#[must_use]
pub fn restored_with_no_wal() -> Scenario {
    let mut s = build(
        "restored_with_no_wal",
        vec![
            synced(),
            vec![
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                Op::EnvFault(EnvFault::DriverWritesNothing),
                // A restore leaves a takeover record through what it restored,
                // before the process that replicates starts.
                Op::TrustLocal,
                Op::EnvFault(EnvFault::WipeStaging),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Booted(Some(CauseKind::WalMissing))),
            Expect::Event(EventMatch::Snapshotted),
            Expect::Oracle,
        ],
    );
    s.check = Some(snapshots_with_no_retry);
    s
}

/// The engine retried nothing: the snapshot did not wait on a backoff.
fn snapshots_with_no_retry(report: &RunReport) -> Result<(), String> {
    use lsr_core::Event;
    if let Some(retry) = report
        .seen
        .iter()
        .find(|seen| matches!(seen, Seen::Event(Event::Retrying { .. })))
    {
        return Err(format!("the engine retried: {retry:?}"));
    }
    Ok(())
}

/// The WAL header is zeroed.
#[must_use]
pub fn wal_header_torn() -> Scenario {
    let torn = vec![Op::Wal(WalOp::TearHeader)];
    let heal = vec![
        Op::OurReader { begin: false },
        Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
        commit(1, false),
    ];
    after_restart_then("wal_header_torn", torn, heal, CauseKind::WalMissing)
}

/// A VACUUM into another page size while the engine is down. A replica
/// prefix holds one page size, so the boot stops, or keeps retrying, before
/// it stages anything; the run then checks that nothing of the new size was
/// staged or published and that every state from before the change still
/// restores.
#[must_use]
pub fn vacuum_page_size_change() -> Scenario {
    let changed = StopCause::PageSizeChanged {
        was: PageSize::MIN,
        now: PageSize::new(2048).unwrap_or(PageSize::MIN),
    };
    build(
        "vacuum_page_size_change",
        vec![
            synced(),
            vec![
                Op::Vacuum {
                    page_size: PageSize::new(2048),
                },
                commit(1, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Stopped(changed), Expect::Oracle],
    )
}

/// As [`vacuum_page_size_change`], then the operator vacuums back to the
/// replica's page size: the restart that takes boots and replicates again.
#[must_use]
pub fn vacuum_page_size_change_and_back() -> Scenario {
    let changed = StopCause::PageSizeChanged {
        was: PageSize::MIN,
        now: PageSize::new(2048).unwrap_or(PageSize::MIN),
    };
    build(
        "vacuum_page_size_change_and_back",
        vec![
            synced(),
            vec![
                Op::Vacuum {
                    page_size: PageSize::new(2048),
                },
                commit(1, false),
            ],
            ticks(SETTLE),
            vec![
                Op::Vacuum {
                    page_size: Some(PageSize::MIN),
                },
                commit(2, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Stopped(changed),
            Expect::Event(EventMatch::Booted(Some(CauseKind::GenerationChanged))),
            Expect::Oracle,
        ],
    )
}

/// An outside TRUNCATE and a commit restart the WAL while the engine is down.
#[must_use]
pub fn app_checkpoint_while_down() -> Scenario {
    let restarted = vec![
        Op::OurReader { begin: false },
        Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
        commit(2, false),
    ];
    after_restart(
        "app_checkpoint_while_down",
        restarted,
        CauseKind::GenerationChanged,
    )
}

/// A file is staged and its upload fails; the process dies, and while it is
/// down the pending file's header loses its WAL range, so it chains to
/// nothing, the app checkpoints, the WAL restarts and the app commits. The
/// boot reads the pending file whole, finds it does not follow the replica's
/// newest file, deletes it unpublished, and names it as what the snapshot
/// replaces.
#[must_use]
pub fn pending_file_that_does_not_chain() -> Scenario {
    let failed_upload = Op::StoreFault {
        target: crate::generate::Target::Remote,
        op: lsr_core::StoreOp::Upload,
        ahead: 0,
        shape: crate::store::FaultShape::Error(lsr_core::StoreError::Transient {
            op: lsr_core::StoreOp::Upload,
            kind: std::io::ErrorKind::ConnectionReset,
        }),
    };
    build(
        "pending_file_that_does_not_chain",
        vec![
            synced(),
            vec![Op::Tick, failed_upload, commit(2, false), Op::Tick],
            vec![
                Op::Restart,
                Op::Meta(MetaOp::StripNewestOrigin),
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                commit(2, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        snapshot_after_boot(CauseKind::StagedUnverified),
    )
}

/// The engine's read lock lapses, an outside RESTART runs, and the process
/// dies before it notices.
#[must_use]
pub fn restart_during_read_lock_lapse() -> Scenario {
    build(
        "restart_during_read_lock_lapse",
        vec![
            vec![commit(3, false)],
            ticks(SETTLE),
            vec![
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
                commit(2, false),
                Op::Restart,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        snapshot_after_boot(CauseKind::GenerationChanged),
    )
}

/// An outside TRUNCATE and one commit restart the WAL between two ticks: the
/// next step reports a changed generation (not a violated read lock).
#[must_use]
pub fn pinned_on_zero_single_restart() -> Scenario {
    build(
        "pinned_on_zero_single_restart",
        vec![
            vec![commit(3, false)],
            ticks(4),
            vec![
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                commit(2, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Resnapshot(CauseKind::GenerationChanged)),
            Expect::Oracle,
        ],
    )
}

/// An outside TRUNCATE resets the wal-index and then fails to truncate the
/// file, which keeps its old header (#26). With nothing committed past the
/// position the idle engine settles and a drain finishes. The next boot reads
/// the WAL no further than the wal-index commits, which names no frame of the
/// header's generation, so it can prove nothing there and snapshots; the
/// commit that writes a new header is a restart the lapsed read transaction
/// cannot prove, so another snapshot.
#[must_use]
pub fn truncate_failed_after_the_index_reset() -> Scenario {
    let mut s = build(
        "truncate_failed_after_the_index_reset",
        vec![
            vec![commit(3, false)],
            ticks(4),
            vec![
                Op::OurReader { begin: false },
                Op::EnvFault(EnvFault::TruncateFails),
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                Op::OurReader { begin: true },
            ],
            ticks(SETTLE),
            vec![Op::Shutdown, Op::Tick],
            ticks(SETTLE),
            vec![commit(2, false)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Booted(Some(CauseKind::GenerationChanged))),
            Expect::Event(EventMatch::Resnapshot(CauseKind::GenerationChanged)),
            Expect::Oracle,
        ],
    );
    s.check = Some(settled_under_the_reset_index);
    s
}

/// Until the commit, no step retried a moved WAL and no drain ran out of time.
fn settled_under_the_reset_index(report: &RunReport) -> Result<(), String> {
    use lsr_core::{Event, Fault};
    for seen in &report.seen {
        match seen {
            Seen::Event(Event::Resnapshot(_)) => return Ok(()),
            Seen::Event(Event::Retrying {
                fault: Fault::WalMoved,
                ..
            }) => return Err("a step retried a moved WAL under the reset index".to_owned()),
            Seen::Stop(cause) => return Err(format!("stopped: {cause:?}")),
            Seen::Event(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    Err("the commit after the reset never resnapshotted".to_owned())
}

/// The generation's frames are rewritten with valid checksums but none of
/// them is a commit: the frame before the newest staged file's end is not one.
#[must_use]
pub fn wal_rewritten_same_length() -> Scenario {
    let rewritten = vec![Op::Wal(WalOp::RegrowWithoutCommitMarks(3))];
    after_restart(
        "wal_rewritten_same_length",
        rewritten,
        CauseKind::PrevFrameMismatch,
    )
}

/// The same generation's frames hold other bytes and one more commit follows.
#[must_use]
pub fn wal_regrown_same_salts_diff_bytes() -> Scenario {
    let regrown = vec![Op::Wal(WalOp::RegrowSameSalts(3))];
    after_restart(
        "wal_regrown_same_salts_diff_bytes",
        regrown,
        CauseKind::LastPageMismatch,
    )
}

/// One transaction larger than the truncate threshold: the checkpoint that
/// truncates the WAL ends the old generation, so the engine snapshots.
#[must_use]
pub fn truncate_threshold_crossed() -> Scenario {
    build(
        "truncate_threshold_crossed",
        vec![
            vec![commit(3, false)],
            ticks(4),
            vec![Op::OtherProcessReader { begin: true }],
            (0..12).map(|_| commit(30, true)).collect(),
            ticks(SETTLE),
            vec![Op::OtherProcessReader { begin: false }],
            ticks(SETTLE * 2),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Checkpointed(CheckpointMode::Truncate)),
            Expect::Event(EventMatch::Snapshotted),
            Expect::Oracle,
        ],
    )
}

/// An operator asks for a snapshot.
#[must_use]
pub fn cli_snapshot() -> Scenario {
    build(
        "cli_snapshot",
        vec![
            synced(),
            ticks(2),
            vec![Op::RequestSnapshot],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Booted(None)),
            Expect::Event(EventMatch::Snapshotted),
            Expect::Oracle,
        ],
    )
}

/// Commits were acknowledged and replicated on a disk that lies about the
/// engine's fsync of the WAL, then a power loss took the WAL's tail: the local
/// WAL is behind the replica's position.
#[must_use]
pub fn power_loss_wal_rewind() -> Scenario {
    build(
        "power_loss_wal_rewind",
        vec![
            // The engine syncs the WAL before it stages, so only a disk that lies about that
            // sync can rewind the database below what was staged.
            vec![Op::LyingWalSync],
            vec![commit(3, false), commit(3, false)],
            ticks(SETTLE),
            vec![commit(3, false), commit(3, false)],
            ticks(SETTLE),
            vec![Op::PowerLoss],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        snapshot_after_boot(CauseKind::LocalRewound),
    )
}

mod coverage;
mod faults;
mod outage;
mod write_gate;
pub use coverage::*;
pub use faults::*;
pub use outage::*;
pub use write_gate::*;

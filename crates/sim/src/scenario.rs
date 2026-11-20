//! Scenarios: a fixed list of operations and the events the engine and the
//! oracle must show while running it.
//!
//! A scenario runs through the same [`run_ops`] as a random history, so the
//! oracle and the resource properties judge it too; then its expectations are
//! matched, in order, against what the run recorded. [`scenario_of`] has one
//! arm per [`SnapshotCause`] and no wildcard: a new cause does not compile
//! until it names its scenarios.

use lsr_core::{CheckpointMode, HaltCause, SnapshotCause};
use lsr_ltx::PageSize;

use crate::generate::{EnvFault, MetaOp, Op, WalOp};
use crate::run::{Opts, RunReport, Seen, run_ops};
use crate::seed::Seed;
use crate::sqlite::StrangerMode;

/// A [`SnapshotCause`] without its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CauseKind {
    /// [`SnapshotCause::FirstSync`].
    FirstSync,
    /// [`SnapshotCause::BehindRemote`].
    BehindRemote,
    /// [`SnapshotCause::LocalTipUnreadable`].
    LocalTipUnreadable,
    /// [`SnapshotCause::WalMissing`].
    WalMissing,
    /// [`SnapshotCause::GenerationChanged`].
    GenerationChanged,
    /// [`SnapshotCause::PrevFrameMismatch`].
    PrevFrameMismatch,
    /// [`SnapshotCause::LastPageMismatch`].
    LastPageMismatch,
    /// [`SnapshotCause::LocalRewound`].
    LocalRewound,
    /// [`SnapshotCause::CheckpointBoundary`].
    CheckpointBoundary,
    /// [`SnapshotCause::OperatorRequested`].
    OperatorRequested,
}

impl From<&SnapshotCause> for CauseKind {
    fn from(cause: &SnapshotCause) -> Self {
        match cause {
            SnapshotCause::FirstSync => Self::FirstSync,
            SnapshotCause::BehindRemote { .. } => Self::BehindRemote,
            SnapshotCause::LocalTipUnreadable => Self::LocalTipUnreadable,
            SnapshotCause::WalMissing => Self::WalMissing,
            SnapshotCause::GenerationChanged => Self::GenerationChanged,
            SnapshotCause::PrevFrameMismatch => Self::PrevFrameMismatch,
            SnapshotCause::LastPageMismatch => Self::LastPageMismatch,
            SnapshotCause::LocalRewound { .. } => Self::LocalRewound,
            SnapshotCause::CheckpointBoundary => Self::CheckpointBoundary,
            SnapshotCause::OperatorRequested => Self::OperatorRequested,
        }
    }
}

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
}

/// One expectation of a scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// An event.
    Event(EventMatch),
    /// The engine halted for this cause.
    Halt(HaltCause),
    /// The oracle restored and judged the replica and found nothing wrong.
    Oracle,
}

impl Expect {
    fn matches(&self, seen: &Seen) -> bool {
        use lsr_core::Event;
        match (self, seen) {
            (Self::Event(EventMatch::Booted(want)), Seen::Event(Event::Booted { cause })) => {
                cause.as_ref().map(CauseKind::from) == *want
            }
            (Self::Event(EventMatch::Resnapshot(want)), Seen::Event(Event::Resnapshot(cause))) => {
                CauseKind::from(cause) == *want
            }
            (Self::Event(EventMatch::Snapshotted), Seen::Snapshot(_)) => true,
            (Self::Event(EventMatch::SnapshotAt(want)), Seen::Snapshot(got)) => got.get() == *want,
            (
                Self::Event(EventMatch::Checkpointed(want)),
                Seen::Event(Event::Checkpointed { mode, .. }),
            ) => want == mode,
            (Self::Halt(want), Seen::Halt(got)) => want == got,
            (Self::Oracle, Seen::Oracle) => true,
            (Self::Event(_) | Self::Halt(_) | Self::Oracle, _) => false,
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
}

/// Runs a scenario and matches its expectations.
///
/// # Errors
/// The failing property or the first expectation that never showed, with what
/// was seen.
pub fn run_scenario(scenario: &Scenario) -> Result<RunReport, String> {
    let opts = Opts {
        record: true,
        allow_halt: scenario.expect.iter().any(|e| matches!(e, Expect::Halt(_))),
        ..Opts::default()
    };
    let report = run_ops(scenario.seed, &scenario.ops, &opts)
        .map_err(|failure| format!("{}: {failure}", scenario.name))?;
    let mut seen = report.seen.iter();
    for (index, want) in scenario.expect.iter().enumerate() {
        if !seen.any(|got| want.matches(got)) {
            return Err(format!(
                "{}: expectation {index} ({want:?}) never showed; seen {:?}",
                scenario.name, report.seen
            ));
        }
    }
    Ok(report)
}

/// The scenarios that make the engine take a snapshot for `cause`.
#[must_use]
pub fn scenario_of(cause: SnapshotCause) -> &'static [fn() -> Scenario] {
    match cause {
        SnapshotCause::FirstSync => &[empty_meta_boot, meta_wiped_remote_kept],
        SnapshotCause::BehindRemote { .. } => &[newest_staged_file_lost, restored_old_meta],
        SnapshotCause::LocalTipUnreadable => &[tip_byte_flip, tip_truncated, tip_no_wal_origin],
        SnapshotCause::WalMissing => &[wal_deleted_while_down, wal_header_torn],
        SnapshotCause::GenerationChanged => &[
            app_checkpoint_while_down,
            restart_during_read_lock_lapse,
            pinned_on_zero_single_restart,
        ],
        SnapshotCause::PrevFrameMismatch => &[wal_rewritten_same_length],
        SnapshotCause::LastPageMismatch => &[wal_regrown_same_salts_diff_bytes],
        SnapshotCause::LocalRewound { .. } => &[power_loss_wal_rewind],
        SnapshotCause::CheckpointBoundary => &[truncate_threshold_crossed],
        SnapshotCause::OperatorRequested => &[cli_snapshot],
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

/// As [`after_restart`], with `then` after the boot tick: a WAL that was
/// deleted or torn comes back only when the app writes again.
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

/// The staging directory is wiped while the replica keeps its files: with no
/// tip there is no position to compare, so this is a first sync, not a replica
/// ahead of us.
#[must_use]
pub fn meta_wiped_remote_kept() -> Scenario {
    let wipe = vec![Op::EnvFault(EnvFault::WipeStaging)];
    after_restart("meta_wiped_remote_kept", wipe, CauseKind::FirstSync)
}

/// The newest staged file is gone but an older one remains: the replica is ahead.
#[must_use]
pub fn newest_staged_file_lost() -> Scenario {
    let lost = vec![Op::Meta(MetaOp::DeleteNewest)];
    after_restart("newest_staged_file_lost", lost, CauseKind::BehindRemote)
}

/// A backup of the staging directory is put back after the replica moved on.
#[must_use]
pub fn restored_old_meta() -> Scenario {
    build(
        "restored_old_meta",
        vec![
            vec![commit(3, false), Op::Shutdown, Op::Meta(MetaOp::Save)],
            vec![commit(2, false), commit(2, false), Op::Shutdown],
            vec![Op::Meta(MetaOp::Restore)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        snapshot_after_boot(CauseKind::BehindRemote),
    )
}

/// One byte of the tip changed on disk. The replica already holds the tip, so
/// the torn copy is deleted and the snapshot takes the transaction after it.
#[must_use]
pub fn tip_byte_flip() -> Scenario {
    let flip = vec![Op::Meta(MetaOp::FlipNewestByte)];
    torn_tip("tip_byte_flip", flip)
}

/// The tip file ends early, cut directly; otherwise as [`tip_byte_flip`].
#[must_use]
pub fn tip_truncated() -> Scenario {
    let cut = vec![Op::Meta(MetaOp::TruncateNewest)];
    torn_tip("tip_truncated", cut)
}

/// The tip of [`synced`] is damaged while the engine is down. [`synced`]
/// stages and uploads transactions 1 and 2, so the torn tip is 2, which the
/// replica holds: the snapshot must take 3.
fn torn_tip(name: &'static str, damage: Vec<Op>) -> Scenario {
    build(
        name,
        vec![synced(), damage, ticks(SETTLE), vec![Op::Restore]],
        vec![
            Expect::Event(EventMatch::Booted(Some(CauseKind::LocalTipUnreadable))),
            Expect::Event(EventMatch::SnapshotAt(3)),
            Expect::Oracle,
        ],
    )
}

/// The tip's header names no WAL range, so there is no position to resume from.
#[must_use]
pub fn tip_no_wal_origin() -> Scenario {
    let stripped = vec![Op::Meta(MetaOp::StripNewestOrigin)];
    after_restart("tip_no_wal_origin", stripped, CauseKind::LocalTipUnreadable)
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

/// The WAL header is zeroed.
#[must_use]
pub fn wal_header_torn() -> Scenario {
    let torn = vec![Op::Wal(WalOp::TearHeader)];
    let heal = vec![
        Op::OurReader { begin: false },
        Op::StrangerCheckpoint(StrangerMode::Truncate),
        commit(1, false),
    ];
    after_restart_then("wal_header_torn", torn, heal, CauseKind::WalMissing)
}

/// A VACUUM into another page size while the engine is down. A replica
/// prefix holds one page size, so the boot halts before it stages anything;
/// the run then checks that nothing of the new size was staged or published
/// and that every state from before the change still restores.
#[must_use]
pub fn vacuum_page_size_change() -> Scenario {
    let changed = HaltCause::PageSizeChanged {
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
        vec![Expect::Halt(changed), Expect::Oracle],
    )
}

/// An outside TRUNCATE and a commit restart the WAL while the engine is down.
#[must_use]
pub fn app_checkpoint_while_down() -> Scenario {
    let restarted = vec![
        Op::OurReader { begin: false },
        Op::StrangerCheckpoint(StrangerMode::Truncate),
        commit(2, false),
    ];
    after_restart(
        "app_checkpoint_while_down",
        restarted,
        CauseKind::GenerationChanged,
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
                Op::StrangerCheckpoint(StrangerMode::Restart),
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
/// next step reports a changed generation (not a violated pin).
#[must_use]
pub fn pinned_on_zero_single_restart() -> Scenario {
    build(
        "pinned_on_zero_single_restart",
        vec![
            vec![commit(3, false)],
            ticks(4),
            vec![
                Op::OurReader { begin: false },
                Op::StrangerCheckpoint(StrangerMode::Truncate),
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

/// The generation's frames are rewritten with valid checksums but none of
/// them is a commit: the frame before the tip's end is not one.
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
            vec![Op::StrangerReader { begin: true }],
            (0..12).map(|_| commit(30, true)).collect(),
            ticks(SETTLE),
            vec![Op::StrangerReader { begin: false }],
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

/// Commits were acknowledged and replicated, then a power loss took the WAL's
/// unsynced tail: the local WAL is behind the replica's position.
#[must_use]
pub fn power_loss_wal_rewind() -> Scenario {
    build(
        "power_loss_wal_rewind",
        vec![
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

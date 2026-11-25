//! The real-SQLite fault suite: every belief about SQLite the engine relies on, checked on a live
//! database through the fault VFS, with core's actor ticking over the backend and the
//! simulator's oracle restoring the replica at every transaction (`lsr_sqlite::testing`).
//!
//! Written once over [`Backend`]; each `#[test]` runs it for the rusqlite read transaction
//! ([`Sqlite`]). A case whose answer depends on the backend states both answers in one table
//! (`read_mark_zero_answers`), keyed by [`Backend::READ_LOCK_ON_MARK_ZERO`], never in a skip.
//!
//! Each case also runs in both [`Placement`]s (`both`): the application in this process, set up as
//! replication asks, and the application in a child process at SQLite's defaults (automatic
//! checkpoints at 1000 frames), with the read lock, the engine and the oracle
//! here, as beside a sidecar. One case's answer differs by placement and states both
//! (`abandoned_frames_answers`; both are a snapshot). In this process only:
//! `persist_wal_keeps_a_wal_a_page_size_change_makes_stale`, which is about one connection's
//! own setting, and the power-loss file (`power_loss.rs`), whose loss undoes what the
//! application wrote through this process's fault VFS. The killed-writer file (`kill.rs`) runs in
//! both. The cases only an application at SQLite's defaults has are in `app_at_defaults.rs`.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use std::num::NonZeroU32;
use std::path::Path;
use std::process::{Command, Stdio};

use lsr_core::{
    CheckpointMode, Event, Fault, PersistentCause, Policy, RecoverEvent, Recovered, Responses,
    SnapshotCause, SourceError, StopCause, Tick, policy,
};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{
    Backend, Effect, FileKind, Op, Placement, Rig, RigConfig, RigError, StepDid, Stepper,
};
use rusqlite::ffi;
use tempfile::TempDir;

use crate::common;

/// This file's roles for the binary's `child_process_entry` (`common::spawn`): whether `role`
/// is one, after running it.
pub fn child(role: &str, db: &Path) -> bool {
    match role {
        "application-restarts-the-wal" => app_restarts_the_wal(db),
        _ => return false,
    }
    true
}

fn rig<B: Backend>(dir: &TempDir, placement: Placement) -> Rig<B> {
    Rig::new(dir.path(), RigConfig::placed(placement)).unwrap()
}

/// Runs `case` with the application in each placement.
fn both(case: impl Fn(Placement)) {
    for placement in Placement::ALL {
        case(placement);
    }
}

// ----------------------------------------------------------------- the fault matrix

/// The faults an injected I/O error may become: each retries (`policy`), none stops.
const RETRYABLE: [Fault; 6] = [
    Fault::LocalIo,
    Fault::LocalFsyncFailed,
    Fault::LocalDiskFull,
    Fault::ReadTxLost,
    Fault::SqliteBusy,
    Fault::WalMoved,
];

/// What one scheduled fault did to one cycle.
#[derive(Debug)]
struct Outcome {
    nth: u32,
    faults: Vec<Fault>,
    /// The cycle settled only after the next application commit (see
    /// `an_idle_database_settles_after_a_checkpoint_reset_the_wal_index`).
    needed_a_write: bool,
    /// A task was degraded on the way: the engine's own sync of the WAL failed, which it never
    /// retries past (it checkpoints with TRUNCATE and snapshots instead).
    degraded: Vec<PersistentCause>,
}

/// What a cycle needed beyond ticking to settle.
#[derive(Debug, Default)]
struct Needed {
    write: bool,
}

/// Settles, or, when the actor keeps retrying `WalMoved`, commits once more and settles again:
/// the state a checkpoint that reset the wal-index leaves until SQLite writes a WAL header.
fn settle_or_write<B: Backend>(rig: &mut Rig<B>, needed: &mut Needed) -> Result<(), RigError> {
    match rig.settle() {
        Err(RigError::Unsettled(_)) if rig.faults().last() == Some(&Fault::WalMoved) => {
            needed.write = true;
            rig.insert(100)?;
            rig.settle()
        }
        other => other,
    }
}

/// Fails the first `op` on a `kind` file of each tick of a clean cycle in turn (each step of a
/// sync, a PASSIVE and a TRUNCATE), runs the cycle, and checks: the fault fired, every fault
/// the actor reported retries and is one an I/O error may become, the actor never stopped, the
/// cycle completed, and the oracle restores every transaction on the replica.
fn each_step<B: Backend>(
    placement: Placement,
    kind: FileKind,
    op: Op,
    effect: Effect,
) -> Vec<Outcome> {
    let dir = TempDir::new().unwrap();
    let mut dry = rig::<B>(&dir, placement);
    dry.cycle().unwrap();
    dry.check().unwrap();
    let steps = dry.first_calls(kind, op);
    let mut outcomes = Vec::new();
    for nth in steps {
        let dir = TempDir::new().unwrap();
        let mut cfg = RigConfig::placed(placement);
        cfg.actor.responses = Responses::GO_LIKE;
        let mut rig = Rig::<B>::new(dir.path(), cfg).unwrap();
        rig.vfs().arm(lsr_sqlite::testing::Fault {
            kind,
            op,
            nth: NonZeroU32::new(nth).unwrap(),
            effect,
        });
        let mut needed = Needed::default();
        let done = rig.cycle_with(|rig| settle_or_write(rig, &mut needed));
        let at = format!(
            "{} {placement:?} {kind:?} {op:?} {effect:?} at call {nth}",
            B::NAME
        );
        assert_eq!(rig.vfs().fired().len(), 1, "{at}: the fault fired once");
        done.unwrap_or_else(|err| panic!("{at}: {err}; faults {:?}", rig.faults()));
        for fault in rig.faults() {
            assert!(RETRYABLE.contains(fault), "{at}: {fault:?}");
            assert!(matches!(policy(fault), Policy::Retry(_)), "{at}: {fault:?}");
        }
        assert!(
            !rig.ticks().iter().any(|t| matches!(t, Tick::Stopped(_))),
            "{at}: no stop"
        );
        for cause in rig.degraded() {
            assert_eq!(*cause, PersistentCause::WalSyncFailed, "{at}");
        }
        rig.check().unwrap_or_else(|err| panic!("{at}: {err}"));
        outcomes.push(Outcome {
            nth,
            faults: rig.faults().to_vec(),
            needed_a_write: needed.write,
            degraded: rig.degraded().to_vec(),
        });
    }
    assert!(
        !outcomes.is_empty(),
        "{placement:?} {kind:?} {op:?} runs in a cycle"
    );
    outcomes
}

/// [`each_step`] in both placements, the outcomes of both in turn.
fn each_step_both<B: Backend>(kind: FileKind, op: Op, effect: Effect) -> Vec<Outcome> {
    Placement::ALL
        .into_iter()
        .flat_map(|placement| each_step::<B>(placement, kind, op, effect))
        .collect()
}

/// A read error on the database file (the engine's page reads, SQLite's own) or on the WAL
/// (SQLite's checkpoint reads it; the engine reads the WAL with its own descriptor).
#[test]
fn a_read_error_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        let outcomes = each_step_both::<Sqlite>(kind, Op::Read, Effect::Error(Op::Read.error()));
        assert!(
            outcomes.iter().all(|o| !o.faults.is_empty()),
            "every step's read is the engine's to see: {outcomes:?}"
        );
    }
}

/// A write error: SQLite's backfill of the database file, and the WAL frames of the engine's
/// own sequence bump.
#[test]
fn a_write_error_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        each_step_both::<Sqlite>(kind, Op::Write, Effect::Error(Op::Write.error()));
    }
}

/// SQLite belief: a short `write(2)` is `SQLITE_FULL` (`unixWrite`), and the torn bytes past
/// the committed end are never read as a frame.
#[test]
fn a_short_write_at_each_step_of_a_cycle_is_disk_full_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        let outcomes = each_step_both::<Sqlite>(kind, Op::Write, Effect::ShortWrite);
        assert!(
            outcomes
                .iter()
                .flat_map(|o| o.faults.first())
                .all(|f| *f == Fault::LocalDiskFull),
            "{outcomes:?}"
        );
    }
}

/// A failed fsync of the WAL before a backfill, or of the database after one: the checkpoint
/// fails and SQLite does not advance `nBackfill`, so nothing is lost and the next one redoes it.
/// A failed fsync of the WAL by the engine itself, before it stages: the sync task is degraded
/// with `WalSyncFailed`, nothing of that WAL generation is staged until a sync succeeds, and the
/// next try goes on, as Go does (`docs/decisions/D173.md`). Under the strict responses it would
/// stop.
#[test]
fn a_sync_error_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        let outcomes = each_step_both::<Sqlite>(kind, Op::Sync, Effect::Error(Op::Sync.error()));
        assert!(
            outcomes
                .iter()
                .all(|o| match (o.faults.first(), o.degraded.first()) {
                    (Some(Fault::LocalIo), None) => true,
                    (_, Some(PersistentCause::WalSyncFailed)) => kind == FileKind::Wal,
                    (Some(_) | None, _) => false,
                }),
            "{outcomes:?}"
        );
        if kind == FileKind::Wal {
            assert!(
                outcomes.iter().any(|o| !o.degraded.is_empty()),
                "the engine syncs the WAL in a cycle: {outcomes:?}"
            );
        }
    }
}

/// A failed truncate: the database file's after a checkpoint, the WAL's in a TRUNCATE (after
/// SQLite has already reset the wal-index, so the file keeps the old generation's header).
#[test]
fn a_truncate_error_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        each_step_both::<Sqlite>(kind, Op::Truncate, Effect::Error(Op::Truncate.error()));
    }
}

/// A wal-index lock held elsewhere: SQLite retries a read mark itself, and a busy checkpoint or
/// write lock is the engine's `SqliteBusy`.
#[test]
fn a_wal_index_lock_refused_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    each_step_both::<Sqlite>(FileKind::Main, Op::ShmLock, Effect::Error(ffi::SQLITE_BUSY));
}

/// SQLite belief: a connection in WAL mode takes its shared lock on the database file once,
/// at its first read, and keeps it, so no step of a sync, checkpoint or snapshot takes a file
/// lock (the wal-index locks are `xShmLock`). A lock error can only meet the backend's open,
/// where it is a typed error and nothing stays pinned.
#[test]
fn no_step_takes_a_file_lock_and_a_lock_error_at_open_is_an_open_error() {
    both(lock_error_at_open::<Sqlite>);
}

fn lock_error_at_open<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir, placement);
    rig.cycle().unwrap();
    assert!(rig.first_calls(FileKind::Main, Op::Lock).is_empty());
    assert!(rig.first_calls(FileKind::Wal, Op::Lock).is_empty());
    let vfs = *rig.vfs();
    rig.stop_replicator();
    vfs.set_window(true);
    vfs.arm(lsr_sqlite::testing::Fault {
        kind: FileKind::Main,
        op: Op::Lock,
        nth: NonZeroU32::MIN,
        effect: Effect::Error(Op::Lock.error()),
    });
    assert!(B::open_backend(rig.db(), vfs.name()).is_err());
    vfs.set_window(false);
    assert_eq!(vfs.fired().len(), 1);
    rig.restart().unwrap();
    rig.insert(500).unwrap();
    rig.settle().unwrap();
    rig.check().unwrap();
}

/// Found by the matrix: a TRUNCATE whose `ftruncate` fails has already reset the wal-index
/// (new salts, no frames) while the file keeps the old header, and so does a sequence bump
/// whose header write fails after SQLite restarted the log (the write matrix, the first WAL
/// write of a cycle). Until the application writes again the engine reads "WAL moved" on every tick and
/// retries for ever; an idle database never settles and a shutdown never drains, though nothing
/// is lost. (A RESTART that succeeds leaves the header for the next writer to reset, and a
/// TRUNCATE that succeeds empties the file, so neither reaches this state.)
#[test]
fn an_idle_database_settles_after_a_checkpoint_reset_the_wal_index() {
    let outcomes = each_step_both::<Sqlite>(
        FileKind::Wal,
        Op::Truncate,
        Effect::Error(Op::Truncate.error()),
    );
    let stuck: Vec<u32> = outcomes
        .iter()
        .filter(|o| o.needed_a_write)
        .map(|o| o.nth)
        .collect();
    assert!(
        stuck.is_empty(),
        "settled only after a write at calls {stuck:?}"
    );
}

// ----------------------------------------------------------------- the read lock, step by step

fn stepper<B: Backend>(dir: &TempDir, placement: Placement) -> Stepper<B> {
    Stepper::placed(dir.path(), 4096, placement).unwrap()
}

fn wrote(did: &StepDid) -> bool {
    matches!(did, StepDid::Wrote { .. })
}

fn assert_continue(verdict: &lsr_core::Verdict) {
    assert_eq!(verdict.event, None, "no ReadLockViolated or LocalRewound");
    assert!(
        matches!(verdict.recovered, Recovered::Continue(_)),
        "expected Continue, got {:?}",
        verdict.recovered
    );
}

/// SQLite belief: a read transaction on a read mark above zero keeps every frame at or below its
/// snapshot in place, whatever a second connection checkpoints. Every byte of the WAL up to the
/// engine's synced end is the same after each mode and the commit that follows it.
#[test]
fn no_synced_frame_is_overwritten_while_pinned() {
    both(no_synced_frame_is_overwritten::<Sqlite>);
}

fn no_synced_frame_is_overwritten<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    assert!(wrote(&s.sync().unwrap()), "the snapshot");
    for mode in ["PASSIVE", "FULL", "RESTART", "TRUNCATE"] {
        s.write(700).unwrap();
        assert!(wrote(&s.sync().unwrap()));
        let end = usize::try_from(s.synced_end().unwrap()).unwrap();
        let before = s.wal_bytes().unwrap()[..end].to_vec();
        s.app_checkpoint(mode).unwrap();
        s.write(300).unwrap();
        let after = s.wal_bytes().unwrap();
        assert!(
            after.len() >= end,
            "{mode}: the WAL keeps its synced frames"
        );
        assert!(after[..end] == before[..], "{mode}: no synced byte changed");
        assert_continue(&s.verdict().unwrap());
        assert!(wrote(&s.sync().unwrap()), "{mode}: incremental after");
    }
}

/// A commit after the engine's last sync and before its TRUNCATE is backfilled and truncated with
/// the rest, since the checkpoint ends the engine's read transaction first. The boundary snapshot
/// then reads that commit's pages from the database file: they are the application's writes, not
/// damage, so the snapshot is taken and no `DatabasePageMismatch` is raised. Found by the
/// simulator's application operations inside a tick (seed 0xf49a2).
#[test]
fn a_commit_between_the_proof_and_the_truncate_is_not_taken_for_damage() {
    both(commit_between_proof_and_truncate::<Sqlite>);
}

fn commit_between_proof_and_truncate<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    assert!(wrote(&s.sync().unwrap()), "the snapshot");
    s.write(700).unwrap();
    assert!(wrote(&s.sync().unwrap()), "the proof");
    s.write(300).unwrap();
    let outcome = s.checkpoint(CheckpointMode::Truncate);
    assert!(
        matches!(outcome, Ok(lsr_core::CheckpointOutcome::Truncated)),
        "{placement:?}: {outcome:?}"
    );
}

/// SQLite belief: TRUNCATE needs every reader gone; against the read lock it reports busy and leaves
/// the WAL whole.
#[test]
fn a_other_process_truncate_reports_busy_and_the_wal_keeps_its_length() {
    both(other_process_truncate::<Sqlite>);
}

fn other_process_truncate<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    s.sync().unwrap();
    s.write(900).unwrap();
    s.sync().unwrap();
    let len = s.wal_bytes().unwrap().len();
    let (busy, _, _) = s.app_checkpoint("TRUNCATE").unwrap();
    assert_eq!(busy, 1);
    assert_eq!(s.wal_bytes().unwrap().len(), len);
    assert_continue(&s.verdict().unwrap());
}

/// SQLite belief: the application's last connection closing does not checkpoint or delete the WAL
/// while another connection (the read lock) is open, since it cannot take the exclusive lock
/// `sqlite3WalClose` needs. The engine continues where it was.
#[test]
fn the_read_lock_survives_the_apps_last_connection_closing() {
    both(read_lock_survives_app_close::<Sqlite>);
}

fn read_lock_survives_app_close<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    s.sync().unwrap();
    s.write(800).unwrap();
    s.sync().unwrap();
    let before = s.wal_bytes().unwrap();
    // Swap the application's connection for a fresh one, closing the only application connection there was.
    s.app_open(0).unwrap();
    assert_eq!(s.wal_bytes().unwrap(), before, "no checkpoint, no delete");
    assert_continue(&s.verdict().unwrap());
    s.write(800).unwrap();
    assert!(matches!(s.sync().unwrap(), StepDid::Wrote { txs: 1, .. }));
}

fn shm_path(db: &Path) -> std::path::PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push("-shm");
    name.into()
}

/// Bytes of a file, read by a second process (`dd`), so no descriptor of ours opens or closes
/// on `-shm`.
fn peek(path: &Path, len: usize) -> Vec<u8> {
    let out = Command::new("dd")
        .arg(format!("if={}", path.display()))
        .args(["bs=1", "status=none"])
        .arg(format!("count={len}"))
        .output()
        .unwrap();
    assert!(out.status.success());
    out.stdout
}

/// Writes `bytes` at `offset` of a file from a second process (`dd`).
fn poke(path: &Path, offset: usize, bytes: &[u8]) {
    use std::io::Write;
    let mut child = Command::new("dd")
        .arg(format!("of={}", path.display()))
        .args(["bs=1", "conv=notrunc", "status=none"])
        .arg(format!("seek={offset}"))
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    assert!(child.wait().unwrap().success());
}

/// A wal-index whose header copies agree but name a format this code does not know is not
/// read: the committed end is `Busy` (retry), never a guess.
#[test]
fn a_wal_index_with_an_unknown_version_is_refused() {
    both(unknown_wal_index_version::<Sqlite>);
}

fn unknown_wal_index_version<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    s.write(500).unwrap();
    s.backend.committed().unwrap();
    let shm = shm_path(s.db());
    let header = peek(&shm, 96);
    let version = (u32::from_ne_bytes(header[..4].try_into().unwrap()) + 1).to_ne_bytes();
    poke(&shm, 0, &version);
    poke(&shm, 48, &version);
    assert_eq!(s.backend.committed().err(), Some(SourceError::Busy));
    let now = peek(&shm, 96);
    assert_eq!(now[4..48], header[4..48], "the rest untouched");
    assert_eq!(now[52..], header[52..], "the rest untouched");
    poke(&shm, 0, &header[..4]);
    poke(&shm, 48, &header[48..52]);
    s.backend.committed().unwrap();
}

/// A wal-index header that fails its checksum, or whose copies differ, is not ready: `Busy`.
/// The engine never runs SQLite's recovery itself; the bytes it found are the bytes it leaves,
/// and the application's next read is what rebuilds the index.
#[test]
fn an_invalid_wal_index_is_not_ready_never_recovered_by_us() {
    both(invalid_wal_index::<Sqlite>);
}

fn invalid_wal_index<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    s.write(500).unwrap();
    let good = s.backend.committed().unwrap();
    let shm = shm_path(s.db());
    let header = peek(&shm, 136);
    // Copy 1's checksum flipped: the copies differ (torn). Then both: they agree, bad checksum.
    let flipped = [header[88] ^ 0xff];
    poke(&shm, 88, &flipped);
    assert_eq!(s.backend.committed().err(), Some(SourceError::Busy));
    poke(&shm, 40, &flipped);
    assert_eq!(s.backend.committed().err(), Some(SourceError::Busy));
    let mut bad = header.clone();
    bad[40] = flipped[0];
    bad[88] = flipped[0];
    assert_eq!(peek(&shm, 136), bad, "nothing of ours rewrote the index");
    // The application's next statement sees the bad header and rebuilds the index from the WAL.
    s.write(10).unwrap();
    let after = s.backend.committed().unwrap();
    assert_eq!(after.end().salts(), good.end().salts());
    assert!(after.end().frame_count() > good.end().frame_count());
}

/// What an outside checkpoint does to the read lock, by mode and by the read lock's read mark. A read lock on a
/// mark above zero blocks RESTART and TRUNCATE, and the next commit appends. A read lock retaken over
/// a fully backfilled WAL sits on read mark 0, which does not stop a restart (walformat.html):
/// RESTART and TRUNCATE go through, and after any mode the next commit, itself on read mark 0,
/// restarts the log; the engine follows because the new generation is `salt1 + 1` in the same
/// read-lock hold. A backend whose read lock sits on a mark above zero even then answers "blocked" in both
/// columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    /// RESTART and TRUNCATE busy, the WAL header and length unchanged, the next commit appends.
    Blocked,
    /// Nothing busy; the next commit is `salt1 + 1` and the engine follows.
    Restarts,
}

const fn read_mark_zero_answers(on_read_mark_zero: bool) -> Answer {
    if on_read_mark_zero {
        Answer::Restarts
    } else {
        Answer::Blocked
    }
}

fn outside_checkpoint<B: Backend>(placement: Placement, mode: &str, read_mark_zero: bool) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    s.sync().unwrap();
    s.write(500).unwrap();
    s.sync().unwrap();
    if read_mark_zero {
        let run = s.backend.checkpoint(CheckpointMode::Passive).unwrap();
        assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
        assert!(s.backend.committed().unwrap().fully_backfilled());
        // Take the read lock again for the position in the new read-lock hold.
        assert!(matches!(s.sync().unwrap(), StepDid::Idle));
    }
    let salts = s.salts().unwrap();
    let len = s.wal_bytes().unwrap().len();
    let at = format!(
        "{} {placement:?} outside {mode}, read mark zero {read_mark_zero}",
        B::NAME
    );
    let (busy, _, _) = s.app_checkpoint(mode).unwrap();
    s.write(300).unwrap();
    let verdict = s.verdict().unwrap();
    assert_eq!(verdict.event, None, "{at}");
    match read_mark_zero_answers(read_mark_zero && B::READ_LOCK_ON_MARK_ZERO) {
        Answer::Blocked => {
            assert!(mode == "PASSIVE" || busy == 1, "{at}: busy");
            assert_eq!(s.salts().unwrap(), salts, "{at}: same generation");
            assert!(s.wal_bytes().unwrap().len() >= len, "{at}");
            assert_continue(&verdict);
        }
        Answer::Restarts => {
            assert_eq!(busy, 0, "{at}");
            assert_eq!(
                s.salts().unwrap().salt1,
                salts.salt1.wrapping_add(1),
                "{at}"
            );
            assert!(
                matches!(verdict.recovered, Recovered::WalRestarted(_)),
                "{at}: {verdict:?}"
            );
        }
    }
    let StepDid::Wrote { txs, .. } = s.sync().unwrap() else {
        panic!("{at}: the commit after the checkpoint is staged");
    };
    assert_eq!(txs, 1, "{at}: incremental, no snapshot");
}

/// Upstream #946 and #1580 were beliefs about outside checkpoints; this is the whole table.
#[test]
fn outside_passive_restart_and_truncate_while_pinned_and_on_read_mark_zero() {
    for placement in Placement::ALL {
        for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
            for read_mark_zero in [false, true] {
                outside_checkpoint::<Sqlite>(placement, mode, read_mark_zero);
            }
        }
    }
}

/// SQLite belief: a reader on read mark 0 holds `WAL_READ_LOCK(0)`, which every backfill needs
/// exclusively (`walCheckpoint`), because such a reader reads the database file directly. So
/// once an application writer that began on a mark above zero appends frames after a read-mark-0 read lock, no
/// checkpoint can backfill them and no writer can restart the log over them while the read lock
/// holds: the frames the continuity proof cannot see are never abandoned, and the engine
/// continues. (One might expect read mark 0 not to
/// block a restart; it blocks every restart but the one at the very next commit, which the
/// proof follows.) A read lock on a mark above zero blocks the same way.
#[test]
fn the_read_lock_on_read_mark_zero_blocks_every_backfill_past_it() {
    both(read_mark_zero_blocks_backfill::<Sqlite>);
}

/// A read lock on read mark 0 at the synced end, then an application reader's write after it, in the same
/// generation (the reader began on a mark above zero, so its write does not restart the log).
/// The reader is the application's connection 1, left open.
fn frames_after_a_read_mark_zero_read_lock<B: Backend>(s: &mut Stepper<B>) {
    s.sync().unwrap();
    for _ in 0..3 {
        s.write(500).unwrap();
    }
    s.sync().unwrap();
    s.app_open(1).unwrap();
    s.app_exec(1, "BEGIN; SELECT count(*) FROM t;").unwrap();
    let run = s.backend.checkpoint(CheckpointMode::Passive).unwrap();
    assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
    assert!(
        matches!(s.sync().unwrap(), StepDid::Idle),
        "re-pinned on read mark 0"
    );
    s.app_exec(1, "INSERT INTO t(v) VALUES (randomblob(500)); COMMIT;")
        .unwrap();
}

fn read_mark_zero_blocks_backfill<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    frames_after_a_read_mark_zero_read_lock(&mut s);
    let salts = s.salts().unwrap();
    let before = s.backend.committed().unwrap();
    for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
        let (_busy, log, done) = s.app_checkpoint(mode).unwrap();
        assert_eq!(
            u32::try_from(done).unwrap(),
            before.backfilled(),
            "{mode} backfilled nothing past the read lock (log {log})"
        );
    }
    s.write(10).unwrap();
    assert_eq!(
        s.salts().unwrap(),
        salts,
        "no restart over the frames after the read lock"
    );
    assert_continue(&s.verdict().unwrap());
    let StepDid::Wrote { txs, .. } = s.sync().unwrap() else {
        panic!("the frames after the read lock are staged");
    };
    assert_eq!(txs, 2);
}

/// The check that catches frames abandoned after the read lock was taken, on real SQLite. The read lock's locks are broken the one way the
/// descriptor rule exists to prevent: a descriptor on `-shm` opened and closed in this process
/// drops every POSIX lock the process holds on it. Another process then appends after the read lock,
/// backfills, and restarts the log over those frames before the engine read them. The proof
/// still holds on paper (same read-lock hold, `salt1 + 1`), and that check sees the old
/// generation's frame past the synced end: `ReadLockViolated` and a snapshot, never a silent
/// continue past the lost transaction. The answer differs by placement (`abandoned_frames_answers`);
/// both are a snapshot.
#[test]
fn the_abandoned_frames_check_fires_when_a_broken_lock_lets_frames_after_the_read_lock_be_abandoned()
 {
    both(abandoned_frames::<Sqlite>);
}

/// The child's work for the abandoned-frames check: append, backfill, restart.
fn app_restarts_the_wal(db: &Path) {
    let conn = rusqlite::Connection::open(db).unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn.execute_batch("INSERT INTO t(v) VALUES (randomblob(500));")
        .unwrap();
    let (_busy, log, done): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(log, done, "the broken lock let the backfill through");
    conn.execute_batch("INSERT INTO t(v) VALUES (randomblob(10));")
        .unwrap();
}

/// How the abandoned-frames case ends, by where the application runs: how many times the child restarted the
/// log, and whether the proof alone would have followed it (so only the abandoned-frames check stops it).
///
/// In process, the descriptor this process closes drops the application's locks on `-shm` too, its
/// dead-man-switch lock included, so the child is the only process left holding the
/// wal-index: it runs SQLite's recovery, which rebuilds the index with nothing backfilled.
/// Its first write appends, its checkpoint backfills it, and its second write restarts the
/// log once (`salt1 + 1`), which the proof would follow: `ReadLockViolated`. With the application in
/// another process, the application keeps the wal-index alive, the child sees the log fully
/// backfilled, and its first write restarts at once: two restarts (`salt1 + 2`), which no
/// proof follows, so the snapshot needs no such check.
const fn abandoned_frames_answers(placement: Placement) -> (u32, bool) {
    match placement {
        Placement::InProcess => (1, true),
        Placement::SeparateProcess => (2, false),
    }
}

fn abandoned_frames<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir, placement);
    s.sync().unwrap();
    for _ in 0..4 {
        s.write(500).unwrap();
    }
    s.sync().unwrap();
    let run = s.backend.checkpoint(CheckpointMode::Passive).unwrap();
    assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
    assert!(
        matches!(s.sync().unwrap(), StepDid::Idle),
        "pinned on read mark 0"
    );
    let salts = s.salts().unwrap();
    #[expect(
        clippy::disallowed_methods,
        reason = "the test opens and closes -shm on purpose, to break the read lock's locks"
    )]
    drop(std::fs::File::open(shm_path(s.db())).unwrap());
    let child = common::spawn(
        "child_process_entry",
        "application-restarts-the-wal",
        s.db(),
    );
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    let (restarts, tripped) = abandoned_frames_answers(placement);
    assert_eq!(
        s.salts().unwrap().salt1,
        salts.salt1.wrapping_add(restarts),
        "{placement:?}: restarted"
    );
    let verdict = s.verdict().unwrap();
    let synced = lsr_wal::WalOffset::new(
        s.synced_end().unwrap(),
        lsr_ltx::PageSize::new(4096).unwrap(),
    )
    .unwrap();
    let expected = tripped.then_some(RecoverEvent::ReadLockViolated { synced });
    assert_eq!(verdict.event, expected, "{placement:?}: {verdict:?}");
    assert!(
        matches!(
            verdict.recovered,
            Recovered::Snapshot {
                cause: SnapshotCause::GenerationChanged,
                ..
            }
        ),
        "{verdict:?}"
    );
}

// ----------------------------------------------------------------- the actor on a live database

/// SQLite belief: VACUUM in WAL mode rewrites the database through the WAL, so a sync sees
/// every page it moved and the database's new size; the oracle restores the shrunk database.
#[test]
fn a_vacuum_replicates_through_the_wal() {
    both(vacuum::<Sqlite>);
}

fn vacuum<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir, placement);
    for _ in 0..30 {
        rig.insert(3000).unwrap();
    }
    rig.settle().unwrap();
    rig.exec("DELETE FROM t WHERE id % 3 != 0").unwrap();
    rig.settle().unwrap();
    let before = rig.backend().unwrap().page_count().unwrap();
    rig.exec("VACUUM").unwrap();
    rig.settle().unwrap();
    rig.insert(100).unwrap();
    rig.settle().unwrap();
    rig.exec("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert!(
        rig.backend().unwrap().page_count().unwrap() < before,
        "VACUUM shrank the file"
    );
    rig.check().unwrap();
}

/// SQLite belief: a page size cannot change while the database is in WAL mode; `PRAGMA
/// page_size` then VACUUM keeps the old size. A change needs the application to leave WAL mode, which
/// it can only do with the replicator stopped; once SQLite writes the first WAL header in the
/// new size the engine stops with the page size the replica holds and the one the database has
/// (a replica prefix holds one page size, as Go's), and the replica keeps what it had.
#[test]
fn a_vacuum_cannot_change_the_page_size_in_wal_mode_and_a_change_stops_at_boot() {
    both(page_size_change::<Sqlite>);
}

fn page_size_change<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir, placement);
    rig.insert(3000).unwrap();
    rig.settle().unwrap();
    rig.exec("PRAGMA page_size = 8192; VACUUM;").unwrap();
    rig.settle().unwrap();
    let size = rig.query("PRAGMA page_size").unwrap();
    assert_eq!(size, [4096], "WAL mode keeps the page size");
    rig.check().unwrap();
    rig.stop_replicator();
    rig.exec(
        "PRAGMA journal_mode = DELETE; PRAGMA page_size = 8192; VACUUM; PRAGMA journal_mode = WAL;",
    )
    .unwrap();
    assert!(!rig.wal().exists() || std::fs::metadata(rig.wal()).unwrap().len() == 0);
    rig.restart().unwrap();
    // The new page size shows in the first WAL header SQLite writes; until then the WAL is
    // empty and the boot waits for it.
    rig.insert(3000).unwrap();
    let cause = rig.until_stop().unwrap();
    assert!(
        matches!(cause, StopCause::PageSizeChanged { was, now } if was.get() == 4096 && now.get() == 8192),
        "{cause:?}"
    );
    rig.check().unwrap();
}

/// SQLite belief, and why `app_connection` does not turn `SQLITE_FCNTL_PERSIST_WAL` on
/// (docs/decisions.md D123; `leaving_wal.rs` has the cases that hold it to that): the flag keeps
/// the WAL file when the database leaves WAL mode, frames and all, and SQLite does not discard
/// a WAL whose page size differs from the database's when it comes back. A page-size change
/// through `journal_mode = DELETE` and VACUUM with the flag on leaves the database unreadable
/// ("malformed database schema") until the stale `-wal` is removed.
#[test]
fn persist_wal_keeps_a_wal_a_page_size_change_makes_stale() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    lsr_sqlite::testing::persist_wal(&conn).unwrap();
    conn.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (randomblob(3000));")
        .unwrap();
    let changed = conn.execute_batch(
        "PRAGMA journal_mode = DELETE; PRAGMA page_size = 8192; VACUUM; PRAGMA journal_mode = WAL;",
    );
    let err = changed.unwrap_err();
    assert_eq!(
        err.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseCorrupt),
        "{err}"
    );
    drop(conn);
    std::fs::remove_file(dir.path().join("app.db-wal")).unwrap();
    let conn = rusqlite::Connection::open(&db).unwrap();
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "the database file itself is sound");
}

/// A WAL deleted by another process while the read lock holds it: SQLite keeps writing the unlinked
/// file through its open descriptors, so every later commit is invisible to anything that opens
/// the path. The backend must say so (`SourceError::Unlinked`), and the engine stops
/// (`WalUnlinked`) rather than snapshot a database it can no longer follow.
#[test]
fn a_wal_deleted_by_another_process_while_pinned_stops_the_engine() {
    both(wal_deleted::<Sqlite>);
}

fn remove_by_another_process(path: &Path) {
    let status = Command::new("rm").arg(path).status().unwrap();
    assert!(status.success(), "rm {}", path.display());
}

fn wal_deleted<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir, placement);
    rig.insert(3000).unwrap();
    rig.settle().unwrap();
    remove_by_another_process(&rig.wal());
    rig.exec_unseen("INSERT INTO t(v) VALUES (randomblob(700))")
        .unwrap();
    assert_eq!(
        rig.backend().unwrap().committed().err(),
        Some(SourceError::Unlinked)
    );
    assert_eq!(rig.until_stop().unwrap(), StopCause::WalUnlinked);
    rig.check().unwrap();
}

/// Upstream `TestDatabaseDeletion`: the database, its WAL and `-shm` deleted while replicating.
/// The engine stops with a typed cause instead of replicating from what its descriptors still
/// see, and the replica keeps every transaction it had.
#[test]
fn a_deleted_database_stops_and_the_replica_keeps_its_history() {
    both(database_deleted::<Sqlite>);
}

fn database_deleted<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir, placement);
    for _ in 0..12 {
        rig.insert(2000).unwrap();
        rig.settle().unwrap();
    }
    let before = rig.check().unwrap();
    let (db, wal) = (rig.db().to_owned(), rig.wal());
    for path in [wal, shm_path(&db), db] {
        remove_by_another_process(&path);
    }
    rig.exec_unseen("INSERT INTO t(v) VALUES (randomblob(700))")
        .unwrap();
    assert_eq!(rig.until_stop().unwrap(), StopCause::WalUnlinked);
    let after = rig.check().unwrap();
    assert_eq!(after.latest, before.latest);
    assert_eq!(after.restored, before.restored);
}

/// The replicator closing after the application: with `SQLITE_FCNTL_PERSIST_WAL` on its own two
/// connections, the last close checkpoints but keeps the WAL file, so the next start continues
/// the same generation with no snapshot. Without it, SQLite deletes the WAL and the next boot
/// snapshots (`WalMissing`).
#[test]
fn the_wal_survives_the_replicator_closing_last() {
    both(replicator_closes_last::<Sqlite>);
}

fn replicator_closes_last<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir, placement);
    for _ in 0..3 {
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
    }
    rig.close_app().unwrap();
    rig.stop_replicator();
    assert!(rig.wal().exists(), "the WAL outlives the last connection");
    rig.open_app().unwrap();
    rig.restart().unwrap();
    rig.insert(1500).unwrap();
    rig.settle().unwrap();
    let booted: Vec<&Tick> = rig
        .ticks()
        .iter()
        .filter(|t| {
            matches!(
                t,
                Tick::Ran {
                    event: Event::Booted { .. },
                    ..
                }
            )
        })
        .collect();
    assert_eq!(booted.len(), 2);
    assert!(
        matches!(
            booted[1],
            Tick::Ran {
                event: Event::Booted { cause: None },
                ..
            }
        ),
        "{booted:?}"
    );
    rig.check().unwrap();
}

/// Upstream `TestConcurrentOperations`: four application connections on four threads (of the application's
/// process) commit at once while the actor ticks over them, then the application's outside checkpoints and the actor's own
/// PASSIVE and TRUNCATE run a full cycle; the oracle restores every transaction.
#[test]
fn concurrent_writers_and_outside_checkpoints_replicate() {
    both(concurrent::<Sqlite>);
}

fn concurrent<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut cfg = RigConfig::default();
    // While threads commit, no checkpoint may let the log restart: the rig reads each commit
    // from the WAL file after the fact, and a restart would overwrite frames before it looked.
    cfg.actor.checkpoint = lsr_core::CheckpointConfig {
        interval: None,
        min_pages: None,
        truncate_pages: None,
    };
    cfg.placement = placement;
    let mut rig = Rig::<B>::new(dir.path(), cfg).unwrap();
    rig.insert(100).unwrap();
    rig.settle().unwrap();
    rig.writers_while_ticking(4, 25).unwrap();
    rig.settle().unwrap();
    assert_eq!(rig.query("SELECT count(*) FROM t").unwrap(), [101]);
    for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
        rig.exec(&format!("PRAGMA wal_checkpoint({mode})")).unwrap();
        rig.insert(900).unwrap();
        rig.settle().unwrap();
    }
    rig.restart_with(lsr_sqlite::testing::rig_actor_config())
        .unwrap();
    rig.cycle().unwrap();
    let checked = rig.check().unwrap();
    // Commits that land between two ticks share a file, so there are fewer restore points.
    assert!(checked.restored > 20, "{checked:?}");
}

/// Upstream `TestReplica_RestoreAndReplicateAfterDataLoss` (Go issue #781): the application loses its
/// disk, restores the replica into a fresh database, and replication resumes into the same
/// replica; a second restore holds the writes made after the first.
#[test]
fn a_restore_after_data_loss_replicates_into_the_same_replica() {
    both(restore_then_replicate::<Sqlite>);
}

fn restored_bytes(replica: &Path) -> (Vec<u8>, lsr_ltx::Txid) {
    let sink = lsr_core::MemSink::new();
    let restored = lsr_core::conformance::block_on(lsr_core::restore(
        &lsr_store_fs::FsStore::new(replica),
        lsr_core::RestoreTarget::Latest,
        sink.clone(),
    ))
    .unwrap();
    let db = sink.published().unwrap();
    let bytes = (1..=db.page_count())
        .flat_map(|pgno| db.page(pgno).unwrap())
        .collect();
    (bytes, restored.reached())
}

fn restore_then_replicate<B: Backend>(placement: Placement) {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir, placement);
    for _ in 0..10 {
        rig.insert(1200).unwrap();
        rig.settle().unwrap();
    }
    let before = rig.check().unwrap();
    let (bytes, reached) = restored_bytes(rig.replica_root());
    rig.replace_database(&bytes, &dir.path().join("meta"), reached)
        .unwrap();
    for _ in 0..10 {
        rig.insert(1200).unwrap();
        rig.settle().unwrap();
    }
    let after = rig.check().unwrap();
    assert!(after.latest > before.latest, "{before:?} then {after:?}");
    assert!(
        after.restored >= before.restored,
        "the old history stays restorable"
    );
    let copy = dir.path().join("second.db");
    std::fs::write(&copy, restored_bytes(rig.replica_root()).0).unwrap();
    let rows: i64 = rusqlite::Connection::open(&copy)
        .unwrap()
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        rows, 20,
        "the second restore holds the writes after the first"
    );
}

/// The WAL header's salts and checkpoint sequence, read from the file (SQLite takes no lock on
/// `-wal`).
fn wal_header(wal: &Path) -> ([u8; 8], u32) {
    use std::io::Read;
    #[expect(clippy::disallowed_methods, reason = "the WAL carries no POSIX lock")]
    let mut file = std::fs::File::open(wal).unwrap();
    let mut header = [0; 24];
    file.read_exact(&mut header).unwrap();
    let seq = u32::from_be_bytes(header[12..16].try_into().unwrap());
    (header[16..24].try_into().unwrap(), seq)
}

/// SQLite belief: a restarted WAL's header can be written again with the same salts and another
/// checkpoint sequence, so another chain seed. A restart resets the wal-index to no frame under
/// new salts and counts the checkpoint in the restarting connection's own `nCkpt`; that
/// connection writes the header when its first frame spills, and a rollback (or a writer killed
/// before its commit) leaves the index at no frame. The next connection to write frame 1 writes
/// the header again, with the index's salts (fresh random ones only when its own count is still
/// zero) and its own count. A position the engine took at the start of that generation must
/// follow the new header: nothing of the generation was replicated, so nothing is lost, and the
/// commit after it replicates. (Go v0.5.18 killed at its WAL restart, then the application's next
/// commit, stalled the alternation on WalCorrupt for good.)
#[test]
fn a_header_written_again_with_the_same_salts_replicates_what_follows() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<Sqlite>(&dir, Placement::InProcess);
    for _ in 0..5 {
        rig.insert(3000).unwrap();
    }
    rig.settle().unwrap();
    // With the replicator gone nothing holds a read mark, so a checkpoint backfills everything
    // and the next write restarts the WAL.
    rig.stop_replicator();
    let app = lsr_sqlite::testing::harness::open_app(rig.db(), rig.vfs(), "NORMAL").unwrap();
    let other = lsr_sqlite::testing::harness::open_app(rig.db(), rig.vfs(), "NORMAL").unwrap();
    let wal = rig.wal();
    // The application restarts the WAL once, the other connection twice: their counts differ
    // and neither is zero.
    let app_seq = restart_by_rollback(&mut rig, &app, &wal);
    restart_by_rollback(&mut rig, &other, &wal);
    let other_seq = restart_by_rollback(&mut rig, &other, &wal);
    assert_ne!(other_seq, app_seq);
    let spilled = wal_header(&wal);
    rig.restart().unwrap();
    rig.settle().unwrap();
    // The application writes frame 1, and the header with it: same salts, its own count.
    app.execute("INSERT INTO t(v) VALUES (randomblob(3000))", [])
        .unwrap();
    rig.observe().unwrap();
    let (salts, seq) = wal_header(&wal);
    assert_eq!(salts, spilled.0, "the same generation");
    assert_eq!(
        seq, app_seq,
        "another checkpoint sequence, so another chain seed"
    );
    drop((app, other));
    rig.settle().unwrap();
    assert!(rig.degraded().is_empty(), "{:?}", rig.degraded());
    rig.check().unwrap();
}

/// Commits a row on `conn`, backfills the WAL, then restarts it from a transaction that spills
/// its first frame and rolls back: the wal-index is left at no frame under new salts, the
/// header written by `conn`. Returns that header's checkpoint sequence, `conn`'s count. The rig
/// reads the commit before the checkpoint can take it out of the WAL.
fn restart_by_rollback(rig: &mut Rig<Sqlite>, conn: &rusqlite::Connection, wal: &Path) -> u32 {
    conn.execute("INSERT INTO t(v) VALUES (randomblob(3000))", [])
        .unwrap();
    rig.observe().unwrap();
    let (busy, log, done): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!((busy, log), (0, done), "a whole backfill");
    let before = wal_header(wal);
    conn.execute_batch(
        "PRAGMA cache_size = 2; BEGIN; INSERT INTO t(v) SELECT randomblob(3000) FROM t, t AS u; ROLLBACK;",
    )
    .unwrap();
    let after = wal_header(wal);
    assert_ne!(after.0, before.0, "the spill restarted the WAL");
    after.1
}

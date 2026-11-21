//! The real-SQLite fault suite: every belief about SQLite the engine relies on, checked on a live
//! database through the fault VFS, with core's actor ticking over the backend and the
//! simulator's oracle restoring the replica at every transaction (`lsr_sqlite::testing`).
//!
//! Written once over [`Backend`]; each `#[test]` runs it for the rusqlite read transaction
//! ([`Sqlite`]). A case whose answer depends on the backend states both answers in one table
//! (`mark_zero_answers`), keyed by [`Backend::PIN_ON_MARK_ZERO`], never in a skip.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "clippy.toml exempts only #[test] bodies; these are the test file's own helpers, where a failure is the test failing"
)]

use std::num::NonZeroU32;
use std::path::Path;
use std::process::{Command, Stdio};

use lsr_core::{
    CheckpointMode, Event, Fault, HaltCause, Policy, RecoverEvent, Recovered, SnapshotCause,
    SourceError, Tick, policy,
};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::harness::open_app;
use lsr_sqlite::testing::{
    Backend, Effect, FileKind, Op, Rig, RigConfig, RigError, StepDid, Stepper,
};
use rusqlite::{FileControl, ffi};
use tempfile::TempDir;

#[expect(
    dead_code,
    reason = "this file waits on a child's exit, not on a line it prints"
)]
mod common;

/// The child processes' entry: a no-op in the test run, the role's work in a child
/// (`common::spawn`).
#[test]
fn child_process_entry() {
    let Some((role, db)) = common::role() else {
        return;
    };
    match role.as_str() {
        "host-restarts-the-wal" => host_restarts_the_wal(&db),
        other => panic!("unknown role {other}"),
    }
}

fn rig<B: Backend>(dir: &TempDir) -> Rig<B> {
    Rig::new(dir.path(), RigConfig::default()).unwrap()
}

// ----------------------------------------------------------------- the fault matrix

/// The faults an injected I/O error may become: each retries (`policy`), none halts.
const RETRYABLE: [Fault; 5] = [
    Fault::LocalIo,
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
}

/// Settles, or, when the actor keeps retrying `WalMoved`, commits once more and settles again:
/// the state a checkpoint that reset the wal-index leaves until SQLite writes a WAL header.
fn settle_or_write<B: Backend>(rig: &mut Rig<B>, needed: &mut bool) -> Result<(), RigError> {
    match rig.settle() {
        Err(RigError::Unsettled(_)) if rig.faults().last() == Some(&Fault::WalMoved) => {
            *needed = true;
            rig.insert(100)?;
            rig.settle()
        }
        other => other,
    }
}

/// Fails the first `op` on a `kind` file of each tick of a clean cycle in turn (each step of a
/// sync, a PASSIVE and a TRUNCATE), runs the cycle, and checks: the fault fired, every fault
/// the actor reported retries and is one an I/O error may become, the actor never halted, the
/// cycle completed, and the oracle restores every transaction on the replica.
fn each_step<B: Backend>(kind: FileKind, op: Op, effect: Effect) -> Vec<Outcome> {
    let dir = TempDir::new().unwrap();
    let mut dry = rig::<B>(&dir);
    dry.cycle().unwrap();
    dry.check().unwrap();
    let steps = dry.first_calls(kind, op);
    let mut outcomes = Vec::new();
    for nth in steps {
        let dir = TempDir::new().unwrap();
        let mut rig = rig::<B>(&dir);
        rig.vfs().arm(lsr_sqlite::testing::Fault {
            kind,
            op,
            nth: NonZeroU32::new(nth).unwrap(),
            effect,
        });
        let mut needed_a_write = false;
        let done = rig.cycle_with(|rig| settle_or_write(rig, &mut needed_a_write));
        let at = format!("{} {kind:?} {op:?} {effect:?} at call {nth}", B::NAME);
        assert_eq!(rig.vfs().fired().len(), 1, "{at}: the fault fired once");
        done.unwrap_or_else(|err| panic!("{at}: {err}; faults {:?}", rig.faults()));
        for fault in rig.faults() {
            assert!(RETRYABLE.contains(fault), "{at}: {fault:?}");
            assert!(matches!(policy(fault), Policy::Retry(_)), "{at}: {fault:?}");
        }
        assert!(
            !rig.ticks().iter().any(|t| matches!(t, Tick::Halted(_))),
            "{at}: no halt"
        );
        rig.check().unwrap_or_else(|err| panic!("{at}: {err}"));
        outcomes.push(Outcome {
            nth,
            faults: rig.faults().to_vec(),
            needed_a_write,
        });
    }
    assert!(!outcomes.is_empty(), "{kind:?} {op:?} runs in a cycle");
    outcomes
}

/// A read error on the database file (the engine's page reads, SQLite's own) or on the WAL
/// (SQLite's checkpoint reads it; the engine reads the WAL with its own descriptor).
#[test]
fn a_read_error_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        let outcomes = each_step::<Sqlite>(kind, Op::Read, Effect::Error(Op::Read.error()));
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
        each_step::<Sqlite>(kind, Op::Write, Effect::Error(Op::Write.error()));
    }
}

/// SQLite belief: a short `write(2)` is `SQLITE_FULL` (`unixWrite`), and the torn bytes past
/// the committed end are never read as a frame.
#[test]
fn a_short_write_at_each_step_of_a_cycle_is_disk_full_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        let outcomes = each_step::<Sqlite>(kind, Op::Write, Effect::ShortWrite);
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
#[test]
fn a_sync_error_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        let outcomes = each_step::<Sqlite>(kind, Op::Sync, Effect::Error(Op::Sync.error()));
        assert!(
            outcomes
                .iter()
                .all(|o| o.faults.first() == Some(&Fault::LocalIo)),
            "{outcomes:?}"
        );
    }
}

/// A failed truncate: the database file's after a checkpoint, the WAL's in a TRUNCATE (after
/// SQLite has already reset the wal-index, so the file keeps the old generation's header).
#[test]
fn a_truncate_error_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    for kind in [FileKind::Main, FileKind::Wal] {
        each_step::<Sqlite>(kind, Op::Truncate, Effect::Error(Op::Truncate.error()));
    }
}

/// A wal-index lock held elsewhere: SQLite retries a read mark itself, and a busy checkpoint or
/// write lock is the engine's `SqliteBusy`.
#[test]
fn a_wal_index_lock_refused_at_each_step_of_a_cycle_is_retried_and_loses_nothing() {
    each_step::<Sqlite>(FileKind::Main, Op::ShmLock, Effect::Error(ffi::SQLITE_BUSY));
}

/// SQLite belief: a connection in WAL mode takes its shared lock on the database file once,
/// at its first read, and keeps it, so no step of a sync, checkpoint or snapshot takes a file
/// lock (the wal-index locks are `xShmLock`). A lock error can only meet the backend's open,
/// where it is a typed error and nothing stays pinned.
#[test]
fn no_step_takes_a_file_lock_and_a_lock_error_at_open_is_an_open_error() {
    lock_error_at_open::<Sqlite>();
}

fn lock_error_at_open<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir);
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
/// write of a cycle). Until the host writes again the engine reads "WAL moved" on every tick and
/// retries for ever; an idle database never settles and a shutdown never drains, though nothing
/// is lost. (A RESTART that succeeds leaves the header for the next writer to reset, and a
/// TRUNCATE that succeeds empties the file, so neither reaches this state.)
#[test]
fn an_idle_database_settles_after_a_checkpoint_reset_the_wal_index() {
    let outcomes = each_step::<Sqlite>(
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

// ----------------------------------------------------------------- the pin, step by step

fn stepper<B: Backend>(dir: &TempDir) -> Stepper<B> {
    Stepper::new(dir.path(), 4096).unwrap()
}

fn wrote(did: &StepDid) -> bool {
    matches!(did, StepDid::Wrote { .. })
}

fn assert_continue(verdict: &lsr_core::Verdict) {
    assert_eq!(verdict.event, None, "no PinViolated or LocalRewound");
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
    no_synced_frame_is_overwritten::<Sqlite>();
}

fn no_synced_frame_is_overwritten<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
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

/// SQLite belief: TRUNCATE needs every reader gone; against the pin it reports busy and leaves
/// the WAL whole.
#[test]
fn a_stranger_truncate_reports_busy_and_the_wal_keeps_its_length() {
    stranger_truncate::<Sqlite>();
}

fn stranger_truncate<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
    s.sync().unwrap();
    s.write(900).unwrap();
    s.sync().unwrap();
    let len = s.wal_bytes().unwrap().len();
    let (busy, _, _) = s.app_checkpoint("TRUNCATE").unwrap();
    assert_eq!(busy, 1);
    assert_eq!(s.wal_bytes().unwrap().len(), len);
    assert_continue(&s.verdict().unwrap());
}

/// SQLite belief: the host's last connection closing does not checkpoint or delete the WAL
/// while another connection (the pin) is open, since it cannot take the exclusive lock
/// `sqlite3WalClose` needs. The engine continues where it was.
#[test]
fn the_pin_survives_the_hosts_last_connection_closing() {
    pin_survives_host_close::<Sqlite>();
}

fn pin_survives_host_close<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
    s.sync().unwrap();
    s.write(800).unwrap();
    s.sync().unwrap();
    let before = s.wal_bytes().unwrap();
    let vfs = *s.vfs();
    let db = s.db().to_owned();
    // Swap the host's connection for a fresh one, closing the only host connection there was.
    s.app = open_app(&db, &vfs, "NORMAL").unwrap();
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
    unknown_wal_index_version::<Sqlite>();
}

fn unknown_wal_index_version<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
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
/// and the host's next read is what rebuilds the index.
#[test]
fn an_invalid_wal_index_is_not_ready_never_recovered_by_us() {
    invalid_wal_index::<Sqlite>();
}

fn invalid_wal_index<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
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
    // The host's next statement sees the bad header and rebuilds the index from the WAL.
    s.write(10).unwrap();
    let after = s.backend.committed().unwrap();
    assert_eq!(after.end().salts(), good.end().salts());
    assert!(after.end().frame_count() > good.end().frame_count());
}

/// What an outside checkpoint does to the pin, by mode and by the pin's read mark. A pin on a
/// mark above zero blocks RESTART and TRUNCATE, and the next commit appends. A pin retaken over
/// a fully backfilled WAL sits on read mark 0, which does not stop a restart (walformat.html):
/// RESTART and TRUNCATE go through, and after any mode the next commit, itself on mark 0,
/// restarts the log; the engine follows because the new generation is `salt1 + 1` in the same
/// read epoch. A backend that pins on a mark above zero even then answers "blocked" in both
/// columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    /// RESTART and TRUNCATE busy, the WAL header and length unchanged, the next commit appends.
    Blocked,
    /// Nothing busy; the next commit is `salt1 + 1` and the engine follows.
    Restarts,
}

const fn mark_zero_answers(on_mark_zero: bool) -> Answer {
    if on_mark_zero {
        Answer::Restarts
    } else {
        Answer::Blocked
    }
}

fn outside_checkpoint<B: Backend>(mode: &str, mark_zero: bool) {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
    s.sync().unwrap();
    s.write(500).unwrap();
    s.sync().unwrap();
    if mark_zero {
        let run = s.backend.checkpoint(CheckpointMode::Passive).unwrap();
        assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
        assert!(s.backend.committed().unwrap().fully_backfilled());
        // Re-pin the position in the new read epoch.
        assert!(matches!(s.sync().unwrap(), StepDid::Idle));
    }
    let salts = s.salts().unwrap();
    let len = s.wal_bytes().unwrap().len();
    let at = format!("{} outside {mode}, mark zero {mark_zero}", B::NAME);
    let (busy, _, _) = s.app_checkpoint(mode).unwrap();
    s.write(300).unwrap();
    let verdict = s.verdict().unwrap();
    assert_eq!(verdict.event, None, "{at}");
    match mark_zero_answers(mark_zero && B::PIN_ON_MARK_ZERO) {
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
fn outside_passive_restart_and_truncate_while_pinned_and_on_mark_zero() {
    for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
        for mark_zero in [false, true] {
            outside_checkpoint::<Sqlite>(mode, mark_zero);
        }
    }
}

/// SQLite belief: a reader on read mark 0 holds `WAL_READ_LOCK(0)`, which every backfill needs
/// exclusively (`walCheckpoint`), because such a reader reads the database file directly. So
/// once a host writer that began on a mark above zero appends frames after a mark-0 pin, no
/// checkpoint can backfill them and no writer can restart the log over them while the pin
/// holds: the frames the continuity proof cannot see are never abandoned, and the engine
/// continues. (The ruling on the pin, docs/plans/phase-3-syncing.md, worried that mark 0 does
/// not block a restart; it blocks every restart but the one at the very next commit, which the
/// proof follows.) A pin on a mark above zero blocks the same way.
#[test]
fn the_pin_on_mark_zero_blocks_every_backfill_past_it() {
    mark_zero_blocks_backfill::<Sqlite>();
}

/// A pin on read mark 0 at the synced end, then a host reader's write after it, in the same
/// generation (the reader began on a mark above zero, so its write does not restart the log).
fn frames_after_a_mark_zero_pin<B: Backend>(s: &mut Stepper<B>) -> rusqlite::Connection {
    s.sync().unwrap();
    for _ in 0..3 {
        s.write(500).unwrap();
    }
    s.sync().unwrap();
    let vfs = *s.vfs();
    let other = open_app(s.db(), &vfs, "NORMAL").unwrap();
    other
        .execute_batch("BEGIN; SELECT count(*) FROM t;")
        .unwrap();
    let run = s.backend.checkpoint(CheckpointMode::Passive).unwrap();
    assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
    assert!(
        matches!(s.sync().unwrap(), StepDid::Idle),
        "re-pinned on mark 0"
    );
    other
        .execute_batch("INSERT INTO t(v) VALUES (randomblob(500)); COMMIT;")
        .unwrap();
    other
}

fn mark_zero_blocks_backfill<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
    let _other = frames_after_a_mark_zero_pin(&mut s);
    let salts = s.salts().unwrap();
    let before = s.backend.committed().unwrap();
    for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
        let (_busy, log, done) = s.app_checkpoint(mode).unwrap();
        assert_eq!(
            u32::try_from(done).unwrap(),
            before.backfilled(),
            "{mode} backfilled nothing past the pin (log {log})"
        );
    }
    s.write(10).unwrap();
    assert_eq!(
        s.salts().unwrap(),
        salts,
        "no restart over the frames after the pin"
    );
    assert_continue(&s.verdict().unwrap());
    let StepDid::Wrote { txs, .. } = s.sync().unwrap() else {
        panic!("the frames after the pin are staged");
    };
    assert_eq!(txs, 2);
}

/// The continuity proof's tripwire on real SQLite. The pin's locks are broken the one way the
/// descriptor rule exists to prevent: a descriptor on `-shm` opened and closed in this process
/// drops every POSIX lock the process holds on it. Another process then appends after the pin,
/// backfills, and restarts the log over those frames before the engine read them. The proof
/// still holds on paper (same read epoch, `salt1 + 1`), and the tripwire sees the old
/// generation's frame past the synced end: `PinViolated` and a snapshot, never a silent
/// continue past the lost transaction.
#[test]
fn the_tripwire_fires_when_a_broken_lock_lets_frames_after_the_pin_be_abandoned() {
    tripwire::<Sqlite>();
}

/// The child's work for the tripwire: append, backfill, restart.
fn host_restarts_the_wal(db: &Path) {
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

fn tripwire<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut s = stepper::<B>(&dir);
    s.sync().unwrap();
    for _ in 0..4 {
        s.write(500).unwrap();
    }
    s.sync().unwrap();
    let run = s.backend.checkpoint(CheckpointMode::Passive).unwrap();
    assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
    assert!(
        matches!(s.sync().unwrap(), StepDid::Idle),
        "pinned on mark 0"
    );
    let salts = s.salts().unwrap();
    #[expect(
        clippy::disallowed_methods,
        reason = "the test opens and closes -shm on purpose, to break the pin's locks"
    )]
    drop(std::fs::File::open(shm_path(s.db())).unwrap());
    let child = common::spawn("child_process_entry", "host-restarts-the-wal", s.db());
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert_eq!(
        s.salts().unwrap().salt1,
        salts.salt1.wrapping_add(1),
        "restarted"
    );
    let verdict = s.verdict().unwrap();
    let synced = lsr_wal::WalOffset::new(
        s.synced_end().unwrap(),
        lsr_ltx::PageSize::new(4096).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict.event, Some(RecoverEvent::PinViolated { synced }));
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
    vacuum::<Sqlite>();
}

fn vacuum<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir);
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
/// page_size` then VACUUM keeps the old size. A change needs the host to leave WAL mode, which
/// it can only do with the replicator stopped; once SQLite writes the first WAL header in the
/// new size the engine halts with the page size the replica holds and the one the database has
/// (a replica prefix holds one page size, as Go's), and the replica keeps what it had.
#[test]
fn a_vacuum_cannot_change_the_page_size_in_wal_mode_and_a_change_halts_at_boot() {
    page_size_change::<Sqlite>();
}

fn page_size_change<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir);
    rig.insert(3000).unwrap();
    rig.settle().unwrap();
    rig.exec("PRAGMA page_size = 8192; VACUUM;").unwrap();
    rig.settle().unwrap();
    let size: u32 = rig
        .app()
        .unwrap()
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    assert_eq!(size, 4096, "WAL mode keeps the page size");
    rig.check().unwrap();
    rig.stop_replicator();
    // PERSIST_WAL would keep the old page size's WAL through the change
    // (`persist_wal_keeps_a_wal_a_page_size_change_makes_stale`): the host turns it off first.
    let mut off = 0;
    rig.app()
        .unwrap()
        .file_control(Some(rusqlite::MAIN_DB), FileControl::PersistWal(&mut off))
        .unwrap();
    rig.exec(
        "PRAGMA journal_mode = DELETE; PRAGMA page_size = 8192; VACUUM; PRAGMA journal_mode = WAL;",
    )
    .unwrap();
    assert!(!rig.wal().exists() || std::fs::metadata(rig.wal()).unwrap().len() == 0);
    rig.restart().unwrap();
    // The new page size shows in the first WAL header SQLite writes; until then the WAL is
    // empty and the boot waits for it.
    rig.insert(3000).unwrap();
    let cause = rig.until_halt().unwrap();
    assert!(
        matches!(cause, HaltCause::PageSizeChanged { was, now } if was.get() == 4096 && now.get() == 8192),
        "{cause:?}"
    );
    rig.check().unwrap();
}

/// SQLite belief, and a hazard for hosts: `SQLITE_FCNTL_PERSIST_WAL` (which `app_connection`
/// turns on) keeps the WAL file when the database leaves WAL mode, frames and all, and SQLite
/// does not discard a WAL whose page size differs from the database's when it comes back. A
/// page-size change through `journal_mode = DELETE` and VACUUM with the setting on leaves the
/// database unreadable ("malformed database schema") until the stale `-wal` is removed.
#[test]
fn persist_wal_keeps_a_wal_a_page_size_change_makes_stale() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    lsr_sqlite::app_connection(&conn).unwrap();
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

/// A WAL deleted by another process while the pin holds it: SQLite keeps writing the unlinked
/// file through its open descriptors, so every later commit is invisible to anything that opens
/// the path. The backend must say so (`SourceError::Unlinked`), and the engine halts
/// (`WalUnlinked`) rather than snapshot a database it can no longer follow.
#[test]
fn a_wal_deleted_by_another_process_while_pinned_halts_the_engine() {
    wal_deleted::<Sqlite>();
}

fn remove_by_another_process(path: &Path) {
    let status = Command::new("rm").arg(path).status().unwrap();
    assert!(status.success(), "rm {}", path.display());
}

fn wal_deleted<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir);
    rig.insert(3000).unwrap();
    rig.settle().unwrap();
    remove_by_another_process(&rig.wal());
    rig.app()
        .unwrap()
        .execute_batch("INSERT INTO t(v) VALUES (randomblob(700))")
        .unwrap();
    assert_eq!(
        rig.backend().unwrap().committed().err(),
        Some(SourceError::Unlinked)
    );
    assert_eq!(rig.until_halt().unwrap(), HaltCause::WalUnlinked);
    rig.check().unwrap();
}

/// Upstream `TestDatabaseDeletion`: the database, its WAL and `-shm` deleted while replicating.
/// The engine halts with a typed cause instead of replicating from what its descriptors still
/// see, and the replica keeps every transaction it had.
#[test]
fn a_deleted_database_halts_and_the_replica_keeps_its_history() {
    database_deleted::<Sqlite>();
}

fn database_deleted<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir);
    for _ in 0..12 {
        rig.insert(2000).unwrap();
        rig.settle().unwrap();
    }
    let before = rig.check().unwrap();
    let (db, wal) = (rig.db().to_owned(), rig.wal());
    for path in [wal, shm_path(&db), db] {
        remove_by_another_process(&path);
    }
    rig.app()
        .unwrap()
        .execute_batch("INSERT INTO t(v) VALUES (randomblob(700))")
        .unwrap();
    assert_eq!(rig.until_halt().unwrap(), HaltCause::WalUnlinked);
    let after = rig.check().unwrap();
    assert_eq!(after.latest, before.latest);
    assert_eq!(after.restored, before.restored);
}

/// The replicator closing after the host: with `SQLITE_FCNTL_PERSIST_WAL` on its own two
/// connections, the last close checkpoints but keeps the WAL file, so the next start continues
/// the same generation with no snapshot. Without it, SQLite deletes the WAL and the next boot
/// snapshots (`WalMissing`).
#[test]
fn the_wal_survives_the_replicator_closing_last() {
    replicator_closes_last::<Sqlite>();
}

fn replicator_closes_last<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir);
    for _ in 0..3 {
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
    }
    rig.close_app();
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

/// Upstream `TestConcurrentOperations`: four host connections on four threads commit at once
/// while the actor ticks over them, then the host's outside checkpoints and the actor's own
/// PASSIVE and TRUNCATE run a full cycle; the oracle restores every transaction.
#[test]
fn concurrent_writers_and_outside_checkpoints_replicate() {
    concurrent::<Sqlite>();
}

fn concurrent<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut cfg = RigConfig::default();
    // While threads commit, no checkpoint may let the log restart: the rig reads each commit
    // from the WAL file after the fact, and a restart would overwrite frames before it looked.
    cfg.actor.checkpoint = lsr_core::CheckpointConfig {
        interval: None,
        min_pages: None,
        truncate_pages: None,
    };
    let mut rig = Rig::<B>::new(dir.path(), cfg).unwrap();
    rig.insert(100).unwrap();
    rig.settle().unwrap();
    let (db, name) = (rig.db().to_owned(), rig.vfs().name().to_owned());
    std::thread::scope(|scope| {
        let writers: Vec<_> = (0..4_u32)
            .map(|w| {
                let (db, name) = (&db, &name);
                scope.spawn(move || {
                    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE;
                    let conn =
                        rusqlite::Connection::open_with_flags_and_vfs(db, flags, name.as_str())
                            .unwrap();
                    conn.busy_timeout(std::time::Duration::from_secs(30))
                        .unwrap();
                    lsr_sqlite::app_connection(&conn).unwrap();
                    for n in 0..25_u32 {
                        conn.execute(
                            "INSERT INTO t(v) VALUES (randomblob(?1))",
                            [200 + w * 300 + n * 7],
                        )
                        .unwrap();
                    }
                })
            })
            .collect();
        while !writers
            .iter()
            .all(std::thread::ScopedJoinHandle::is_finished)
        {
            assert!(!matches!(rig.tick().unwrap(), Tick::Halted(_)));
        }
        for writer in writers {
            writer.join().unwrap();
        }
    });
    rig.settle().unwrap();
    let rows: i64 = rig
        .app()
        .unwrap()
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 101);
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

/// Upstream `TestReplica_RestoreAndReplicateAfterDataLoss` (Go issue #781): the host loses its
/// disk, restores the replica into a fresh database, and replication resumes into the same
/// replica; a second restore holds the writes made after the first.
#[test]
fn a_restore_after_data_loss_replicates_into_the_same_replica() {
    restore_then_replicate::<Sqlite>();
}

fn restored_bytes(replica: &Path) -> Vec<u8> {
    let sink = lsr_core::MemSink::new();
    lsr_core::conformance::block_on(lsr_core::restore(
        &lsr_store_fs::FsStore::new(replica),
        lsr_core::RestoreTarget::Latest,
        sink.clone(),
    ))
    .unwrap();
    let db = sink.published().unwrap();
    (1..=db.page_count())
        .flat_map(|pgno| db.page(pgno).unwrap())
        .collect()
}

fn restore_then_replicate<B: Backend>() {
    let dir = TempDir::new().unwrap();
    let mut rig = rig::<B>(&dir);
    for _ in 0..10 {
        rig.insert(1200).unwrap();
        rig.settle().unwrap();
    }
    let before = rig.check().unwrap();
    let bytes = restored_bytes(rig.replica_root());
    rig.replace_database(&bytes, &dir.path().join("meta"))
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
    std::fs::write(&copy, restored_bytes(rig.replica_root())).unwrap();
    let rows: i64 = rusqlite::Connection::open(&copy)
        .unwrap()
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        rows, 20,
        "the second restore holds the writes after the first"
    );
}

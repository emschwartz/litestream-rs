//! A TRUNCATE checkpoint on real SQLite with application commits landing around it, and no copy
//! of the database after it (docs/decisions/D263.md).
//!
//! The engine's backend is wrapped so that, before each of the engine's calls into it, a hook
//! runs: an application commit at one chosen call, and another connection's PASSIVE checkpoint
//! (and, from the first call on, its TRUNCATE) at every call. The fault VFS adds the one moment
//! no engine call reaches, inside the TRUNCATE's own call: after SQLite truncated the WAL. Every
//! state the replica restores to must be one some commit made, and the last must be the
//! database as it stands.

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::conformance::block_on;
use lsr_core::{
    Barrier, CheckpointMode, CheckpointOutcome, CheckpointRun, Checkpointer, CommittedRead,
    MemSink, PageSource, ReadLockHold, RestoreTarget, SourceError, SourceOp, WalFileLen, WalSource,
};
use lsr_ltx::{DbPages, PageNo};
use lsr_sqlite::testing::{Backend, Stepper};
use lsr_sqlite::{OpenError, Sqlite};
use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;

/// What runs before each of the engine's calls.
type Hook = Box<dyn FnMut(SourceOp)>;

thread_local! {
    /// What runs before each of the engine's calls, when a case arms it.
    pub(crate) static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

thread_local! {
    /// What each of the engine's TRUNCATE calls returned.
    static TRUNCATES: RefCell<Vec<Result<CheckpointRun, SourceError>>> =
        const { RefCell::new(Vec::new()) };
}

fn hook(op: SourceOp) {
    let taken = HOOK.with(|hook| hook.borrow_mut().take());
    if let Some(mut run) = taken {
        run(op);
        HOOK.with(|hook| *hook.borrow_mut() = Some(run));
    }
}

/// The engine's real backend, with [`HOOK`] run before each call.
#[derive(Clone, Debug)]
pub(crate) struct Hooked(Sqlite);

impl WalSource for Hooked {
    type Wal = <Sqlite as WalSource>::Wal;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        hook(SourceOp::Open);
        self.0.open()
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        hook(SourceOp::Len);
        self.0.len()
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        hook(SourceOp::Committed);
        self.0.committed()
    }

    fn epoch(&self) -> ReadLockHold {
        self.0.epoch()
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        hook(SourceOp::Sync);
        self.0.sync()
    }

    fn modified(&mut self) -> Result<Option<lsr_ltx::Timestamp>, SourceError> {
        self.0.modified()
    }
}

impl PageSource for Hooked {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        hook(SourceOp::PageCount);
        self.0.page_count()
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        hook(SourceOp::ReadPage);
        self.0.read_page(pgno, buf)
    }
}

impl Checkpointer for Hooked {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        hook(SourceOp::BeginBarrier);
        self.0.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        hook(SourceOp::EndBarrier);
        self.0.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        hook(SourceOp::Checkpoint);
        let run = self.0.checkpoint(mode);
        if mode == CheckpointMode::Truncate {
            TRUNCATES.with(|runs| runs.borrow_mut().push(run));
        }
        run
    }

    fn renew_read(&mut self) -> Result<ReadLockHold, SourceError> {
        hook(SourceOp::RenewRead);
        self.0.renew_read()
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        hook(SourceOp::BumpSeq);
        self.0.bump_seq()
    }
}

impl Backend for Hooked {
    type OpenError = OpenError;
    const NAME: &'static str = "hooked rusqlite read transaction";
    const READ_LOCK_ON_MARK_ZERO: bool = true;

    fn open_limited(
        path: &Path,
        vfs: &str,
        journal_size_limit: Option<std::num::NonZeroU64>,
    ) -> Result<Self, OpenError> {
        Sqlite::open_limited(path, vfs, journal_size_limit).map(Self)
    }
}

/// A connection of the application's or another process's, through the rig's VFS, that never
/// waits on a lock.
pub(crate) fn connect(db: &Path, vfs: &str) -> Connection {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
    let conn = Connection::open_with_flags_and_vfs(db, flags, vfs).unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn
}

/// The database as a fresh reader sees it.
fn image(db: &Path, vfs: &str) -> Vec<u8> {
    connect(db, vfs)
        .serialize(rusqlite::MAIN_DB)
        .unwrap()
        .to_vec()
}

fn insert(conn: &Connection) -> rusqlite::Result<usize> {
    conn.execute("INSERT INTO t(v) VALUES (randomblob(700))", [])
}

/// Where a case's own commit lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum At {
    /// Nowhere: the dry run that counts the engine's calls.
    Nowhere,
    /// Right before the engine's call with this index, counted from the TRUNCATE's.
    Call(usize),
    /// Inside the TRUNCATE's call, after SQLite truncated the WAL.
    AfterTheTruncate,
    /// Right before the engine's call with this index: a commit that restarts the WAL, then
    /// the backfill count set to its frame count, as a writer leaves it that restarted the WAL
    /// and died before clearing the count (`walRestartHdr` writes the header first).
    StaleBackfill(usize),
    /// A descriptor of the test's own on `-shm`, opened before the engine's first call and
    /// held: what a second copy of SQLite in the process opens.
    SecondOpener,
}

/// What another process does at each of the engine's calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Other {
    /// A PASSIVE checkpoint: it backfills whatever no reader's mark holds back.
    Passive,
    /// A PASSIVE checkpoint, then (once the engine's own TRUNCATE has run) a TRUNCATE, which
    /// restarts the WAL whenever no reader holds a mark above 0 and everything is backfilled.
    PassiveAndTruncate,
    /// Nothing.
    Nothing,
}

/// What one case saw.
#[derive(Debug)]
struct Seen {
    outcome: CheckpointOutcome,
    /// The engine's calls from the TRUNCATE's to the end of the step after it, in order.
    calls: Vec<SourceOp>,
    /// Whether the case's commit was made, and what SQLite said.
    commit: Option<Result<(), String>>,
    /// The engine's call the commit was made before, for a commit at a call.
    commit_before: Option<SourceOp>,
    /// Whether a file staged from the TRUNCATE on is a copy of the whole database.
    snapshotted: bool,
}

/// Images of every state the database has been in, as a fresh reader saw each.
type Images = Arc<Mutex<Vec<Vec<u8>>>>;

fn note(images: &Images, db: &Path, vfs: &str) {
    images
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(image(db, vfs));
}

/// A commit's result, as SQLite reported it.
type Commit = Arc<Mutex<Option<Result<(), String>>>>;

/// A database of a few dozen pages and an engine with a proven position on it.
fn prepared(dir: &TempDir, images: &Images) -> Stepper<Hooked> {
    let mut stepper = Stepper::<Hooked>::new(dir.path(), 1024).unwrap();
    let (db, vfs) = (stepper.db().to_owned(), stepper.vfs().name().to_owned());
    note(images, &db, &vfs);
    for _ in 0..12 {
        stepper.write(700).unwrap();
        note(images, &db, &vfs);
    }
    stepper.sync().unwrap();
    stepper.sync().unwrap();
    stepper.write(700).unwrap();
    note(images, &db, &vfs);
    stepper.sync().unwrap();
    stepper
}

/// Arms the case's commit inside the TRUNCATE's call, through the fault VFS.
fn arm_after_the_truncate(stepper: &Stepper<Hooked>, images: &Images, commit: &Commit) {
    let (db, vfs) = (stepper.db().to_owned(), stepper.vfs().name().to_owned());
    let (images, commit) = (Arc::clone(images), Arc::clone(commit));
    let writer = connect(&db, &vfs);
    stepper.vfs().after_next_wal_truncate(move || {
        let done = insert(&writer).map(drop).map_err(|e| e.to_string());
        if done.is_ok() {
            note(&images, &db, &vfs);
        }
        *commit.lock().unwrap_or_else(PoisonError::into_inner) = Some(done);
    });
}

/// Arms [`HOOK`]: the case's commit at its call, another process's checkpoints at every call,
/// and an image after each. Returns the calls it sees.
fn arm_hook(
    stepper: &Stepper<Hooked>,
    at: At,
    others: Other,
    images: &Images,
    commit: &Commit,
) -> Rc<RefCell<Vec<SourceOp>>> {
    let (db, vfs) = (stepper.db().to_owned(), stepper.vfs().name().to_owned());
    let writer = connect(&db, &vfs);
    let other = connect(&db, &vfs);
    let calls = Rc::new(RefCell::new(Vec::new()));
    let (seen, images, commit) = (Rc::clone(&calls), Arc::clone(images), Arc::clone(commit));
    HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |op: SourceOp| {
            let index = seen.borrow().len();
            seen.borrow_mut().push(op);
            if at == At::Call(index) {
                let done = insert(&writer).map(drop).map_err(|e| e.to_string());
                *commit.lock().unwrap_or_else(PoisonError::into_inner) = Some(done);
            }
            if at == At::StaleBackfill(index) {
                let status = crate::common::spawn("child_process_entry", STALE_BACKFILL, &db)
                    .wait()
                    .unwrap();
                let done = if status.success() {
                    Ok(())
                } else {
                    Err(status.to_string())
                };
                *commit.lock().unwrap_or_else(PoisonError::into_inner) = Some(done);
            }
            if at == At::SecondOpener && index == 0 {
                hold_a_second_descriptor(&db);
            }
            // Another process checkpoints at every call, its TRUNCATE too from the second on.
            if others != Other::Nothing {
                let _passive = other.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
            }
            if others == Other::PassiveAndTruncate && index > 0 {
                let _truncate = other.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
            }
            // Every commit, the engine's own bump included, is followed by a call.
            note(&images, &db, &vfs);
        }));
    });
    calls
}

/// Every state the staged files restore to is one of `images`, and the newest is `live`.
fn judge(stepper: &Stepper<Hooked>, images: &[Vec<u8>], live: &[u8], case: &str) {
    let mut ends: Vec<_> = stepper
        .staging
        .stored()
        .iter()
        .map(|f| f.key.range().max())
        .collect();
    ends.sort_unstable();
    ends.dedup();
    let newest = ends.last().copied().expect("the engine staged something");
    for end in ends {
        let restored = restored(stepper, RestoreTarget::Txid(end));
        assert!(
            images.contains(&restored),
            "{case}: the state at transaction {end} is one no commit made"
        );
        if end == newest {
            assert!(
                restored == live,
                "{case}: the newest state is not the database's"
            );
        }
    }
}

/// One case: the engine's TRUNCATE protocol and one sync after it with the hook armed, judged.
fn run(at: At, others: Other) -> Seen {
    let dir = TempDir::new().unwrap();
    let images: Images = Arc::new(Mutex::new(Vec::new()));
    let mut stepper = prepared(&dir, &images);
    let commit: Commit = Arc::new(Mutex::new(None));
    if at == At::AfterTheTruncate {
        arm_after_the_truncate(&stepper, &images, &commit);
    }
    let calls = arm_hook(&stepper, at, others, &images, &commit);
    let outcome = stepper.checkpoint(CheckpointMode::Truncate).unwrap();
    stepper.sync().unwrap();
    HOOK.with(|hook| *hook.borrow_mut() = None);
    // What the last sync left uncovered, if a commit came after its committed read.
    stepper.sync().unwrap();
    let (db, vfs) = (stepper.db().to_owned(), stepper.vfs().name().to_owned());
    note(&images, &db, &vfs);
    let images = images
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    judge(
        &stepper,
        &images,
        &image(&db, &vfs),
        &format!("{at:?}, {others:?}"),
    );
    let commit = commit
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let calls = calls.borrow().clone();
    // Only a snapshot reads the database file's pages.
    let snapshotted = calls.contains(&SourceOp::ReadPage);
    let commit_before = match at {
        At::Call(index) | At::StaleBackfill(index) => calls.get(index).copied(),
        At::Nowhere | At::AfterTheTruncate | At::SecondOpener => None,
    };
    Seen {
        outcome,
        calls,
        commit,
        commit_before,
        snapshotted,
    }
}

/// The database the staged files restore to at `target`, page by page.
fn restored(stepper: &Stepper<Hooked>, target: RestoreTarget) -> Vec<u8> {
    let sink = MemSink::new();
    block_on(lsr_core::restore(&stepper.staging, target, sink.clone())).unwrap();
    let db = sink.published().unwrap();
    (1..=db.page_count())
        .flat_map(|pgno| db.page(pgno).unwrap())
        .collect()
}

/// The index of the engine's `nth` checkpoint call (from 0): the PASSIVE's, then the TRUNCATE's.
fn nth_checkpoint(calls: &[SourceOp], nth: usize) -> usize {
    calls
        .iter()
        .enumerate()
        .filter(|(_, op)| **op == SourceOp::Checkpoint)
        .nth(nth)
        .map(|(at, _)| at)
        .expect("the engine's checkpoint calls")
}

/// The wal-index beside `db`.
fn shm_of(db: &Path) -> std::path::PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push("-shm");
    name.into()
}

/// Opens `-shm` a second time, as another copy of SQLite in the process would, and never
/// closes it.
#[expect(
    clippy::disallowed_methods,
    clippy::mem_forget,
    reason = "the second descriptor is the case; never closed, since closing it would drop \
              every POSIX lock the process holds on the file, SQLite's own among them"
)]
fn hold_a_second_descriptor(db: &Path) {
    std::mem::forget(std::fs::File::open(shm_of(db)).unwrap());
}

/// The child's role for [`At::StaleBackfill`].
const STALE_BACKFILL: &str = "commit-then-stale-backfill";

/// This file's roles for the binary's `child_process_entry` (`common::spawn`): whether `role`
/// is one, after running it.
pub fn child(role: &str, db: &Path) -> bool {
    if role != STALE_BACKFILL {
        return false;
    }
    stale_backfill(db);
    true
}

/// Another process commits, which restarts the WAL the engine's PASSIVE copied whole, then sets
/// the wal-index's backfill count to its frame count (`WalCkptInfo.nBackfill` at byte 96,
/// `WalIndexHdr.mxFrame` at byte 16, both native-endian) and exits: the state a writer leaves
/// that restarted the WAL and died before `walRestartHdr` cleared the count, once a later
/// commit brought the frame count up to the stale one.
#[expect(
    clippy::disallowed_methods,
    reason = "a child process of its own: closing the descriptor drops only its locks"
)]
fn stale_backfill(db: &Path) {
    use std::os::unix::fs::FileExt;
    let conn = Connection::open(db).unwrap();
    // Page 1 alone: the database keeps its length, so what the TRUNCATE drops is this commit,
    // not pages SQLite still counts.
    conn.execute_batch("PRAGMA user_version = 7").unwrap();
    let shm = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(shm_of(db))
        .unwrap();
    let mut frames = [0; 4];
    shm.read_exact_at(&mut frames, 16).unwrap();
    assert_ne!(u32::from_ne_bytes(frames), 0, "the commit's frames");
    shm.write_all_at(&frames, 96).unwrap();
}

/// A commit at every one of the engine's calls from the TRUNCATE's barrier to the end of the
/// sync after it, and one inside the TRUNCATE's call after SQLite truncated the WAL, each with
/// another process checkpointing throughout: every state restores to one a commit made. With
/// the other process running only PASSIVE checkpoints no case copies the database: a commit
/// either is sealed, makes the TRUNCATE busy, or lands in the generation the engine follows.
#[test]
fn a_commit_anywhere_around_a_truncate_restores_whole() {
    for others in [Other::Passive, Other::PassiveAndTruncate] {
        TRUNCATES.with(|runs| runs.borrow_mut().clear());
        let dry = run(At::Nowhere, others);
        assert_eq!(dry.outcome, CheckpointOutcome::Truncated, "{others:?}");
        // Another process's TRUNCATE right after ours resets the generation again before the
        // engine reads its header: nothing proves the generation continues, so it is copied.
        assert_eq!(
            dry.snapshotted,
            others == Other::PassiveAndTruncate,
            "{:?}",
            dry.calls
        );
        let truncate = nth_checkpoint(&dry.calls, 1);
        let mut committed = 0;
        for index in 0..dry.calls.len() {
            let seen = run(At::Call(index), others);
            if seen.commit == Some(Ok(())) {
                committed += 1;
            }
            // A commit between the TRUNCATE and the engine's next read of the WAL header may
            // have written that header with fresh salts, so nothing proves the generation
            // continues the sealed one: that one case is snapshotted.
            if others == Other::Passive {
                assert_eq!(
                    seen.snapshotted,
                    index == truncate + 1,
                    "{index}: {:?}",
                    seen.calls
                );
            }
        }
        assert!(
            committed > dry.calls.len() / 2,
            "{others:?}: only {committed} of {} commits landed",
            dry.calls.len()
        );
        // With another process truncating too, the moment after a truncation can fall while
        // the engine's barrier holds the write lock, where the commit would wait forever.
        if others != Other::Passive {
            continue;
        }
        let inside = run(At::AfterTheTruncate, others);
        assert_eq!(
            inside.commit,
            Some(Ok(())),
            "{others:?}: the commit right after the TRUNCATE"
        );
    }
}

/// The SQLite fact the proof stands on, on the build lsr links: with the engine's read held, a
/// commit after the barrier's release leaves the TRUNCATE busy, its frames in the WAL, and the
/// next step stages them. An upgrade that changed it fails here.
#[test]
fn a_commit_after_the_barrier_makes_the_truncate_busy() {
    let dry = run(At::Nowhere, Other::Passive);
    let truncate = nth_checkpoint(&dry.calls, 1);
    TRUNCATES.with(|runs| runs.borrow_mut().clear());
    let seen = run(At::Call(truncate), Other::Passive);
    assert_eq!(seen.commit_before, Some(SourceOp::Checkpoint));
    assert_eq!(seen.commit, Some(Ok(())), "the commit after the release");
    let runs = TRUNCATES.with(|runs| runs.borrow().clone());
    assert!(
        matches!(runs.as_slice(), [Ok(run)] if run.busy),
        "SQLite's TRUNCATE: {runs:?}"
    );
    assert_ne!(seen.outcome, CheckpointOutcome::Truncated);
    assert!(!seen.snapshotted);
}

/// A writer that restarted the WAL and died before clearing the backfill count leaves frames
/// nobody copied counted as copied, and SQLite's TRUNCATE then succeeds over them. The engine
/// does not take that success as its proof: the generation was reset twice, so the next sync
/// snapshots what the database holds.
#[test]
fn a_restart_whose_writer_died_mid_reset_is_not_followed() {
    let dry = run(At::Nowhere, Other::Passive);
    let truncate = nth_checkpoint(&dry.calls, 1);
    TRUNCATES.with(|runs| runs.borrow_mut().clear());
    let seen = run(At::StaleBackfill(truncate), Other::Nothing);
    assert_eq!(seen.commit, Some(Ok(())));
    let runs = TRUNCATES.with(|runs| runs.borrow().clone());
    assert!(
        matches!(runs.as_slice(), [Ok(run)] if !run.busy),
        "SQLite truncated over the uncopied frames: {runs:?}"
    );
    assert_eq!(seen.outcome, CheckpointOutcome::Truncated);
    assert!(seen.snapshotted, "the next sync copied the database");
}

/// Another open descriptor on the wal-index, as a second copy of SQLite in the process holds,
/// voids the proof: the next sync snapshots the new generation instead of following it.
#[test]
fn a_second_opener_of_the_wal_index_voids_the_proof() {
    let seen = run(At::SecondOpener, Other::Passive);
    let runs = TRUNCATES.with(|runs| runs.borrow().clone());
    assert!(
        runs.iter()
            .any(|run| matches!(run, Ok(run) if !run.one_sqlite)),
        "{runs:?}"
    );
    assert_eq!(seen.outcome, CheckpointOutcome::Truncated);
    assert!(seen.snapshotted, "the next sync copied the database");
}

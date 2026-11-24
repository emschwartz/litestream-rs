//! The boundary snapshot, the copy of the database the engine stages after a TRUNCATE checkpoint
//! moved the WAL, on real SQLite with application commits landing inside it.
//!
//! The engine's backend is wrapped so that, before each of the engine's calls into it, a hook
//! runs: an application commit at one chosen call, and another connection's PASSIVE checkpoint
//! (and, from the TRUNCATE on, its TRUNCATE) at every call. The fault VFS adds the one moment no
//! engine call reaches, inside the TRUNCATE's own call: after SQLite truncated the WAL and
//! before the engine's read transaction is retaken. Every state the replica restores to must be
//! one some commit made, and the last must be the database as it stands.

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
    static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
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
struct Hooked(Sqlite);

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
        self.0.checkpoint(mode)
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
fn connect(db: &Path, vfs: &str) -> Connection {
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
    /// Inside the TRUNCATE's call, after SQLite truncated the WAL and before the engine's read
    /// transaction is retaken.
    AfterTheTruncate,
}

/// What another process does at each of the engine's calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Other {
    /// A PASSIVE checkpoint: it backfills whatever no reader's mark holds back.
    Passive,
    /// A PASSIVE checkpoint, then (once the engine's own TRUNCATE has run) a TRUNCATE, which
    /// restarts the WAL whenever no reader holds a mark above 0 and everything is backfilled.
    PassiveAndTruncate,
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
            // Another process checkpoints at every call; its TRUNCATE only once the engine's
            // own has run, so it races the copy, not the proof.
            let _passive = other.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
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
    let commit_before = match at {
        At::Call(index) => calls.get(index).copied(),
        At::Nowhere | At::AfterTheTruncate => None,
    };
    Seen {
        outcome,
        calls,
        commit,
        commit_before,
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

/// The copy holds no lock that keeps the application out: a commit made while the boundary
/// snapshot reads the database succeeds, and the next step stages it.
#[test]
fn a_write_commits_while_the_boundary_snapshot_is_copied() {
    let dry = run(At::Nowhere, Other::Passive);
    assert_eq!(dry.outcome, CheckpointOutcome::Truncated);
    let first_page = dry
        .calls
        .iter()
        .position(|op| *op == SourceOp::ReadPage)
        .expect("the snapshot read a page");
    let seen = run(At::Call(first_page.saturating_add(1)), Other::Passive);
    assert_eq!(seen.outcome, CheckpointOutcome::Truncated);
    assert_eq!(seen.commit_before, Some(SourceOp::ReadPage));
    assert_eq!(
        seen.commit,
        Some(Ok(())),
        "the application's commit during the copy"
    );
}

/// A commit at every one of the engine's calls from the TRUNCATE to the end of the sync after
/// it, and one inside the TRUNCATE's call before the read is retaken, each with another process
/// checkpointing throughout: every state restores to one a commit made.
#[test]
fn a_commit_anywhere_around_the_boundary_snapshot_restores_whole() {
    for others in [Other::Passive, Other::PassiveAndTruncate] {
        let dry = run(At::Nowhere, others);
        assert_eq!(dry.outcome, CheckpointOutcome::Truncated);
        assert!(dry.calls.contains(&SourceOp::ReadPage), "{:?}", dry.calls);
        let mut committed = 0;
        for index in 0..dry.calls.len() {
            let seen = run(At::Call(index), others);
            if seen.commit == Some(Ok(())) {
                committed += 1;
            }
        }
        assert!(
            committed > dry.calls.len() / 2,
            "{others:?}: only {committed} of {} commits landed",
            dry.calls.len()
        );
        let inside = run(At::AfterTheTruncate, others);
        assert_eq!(inside.outcome, CheckpointOutcome::Truncated);
        assert_eq!(
            inside.commit,
            Some(Ok(())),
            "{others:?}: the commit between the TRUNCATE and the retaken read"
        );
    }
}

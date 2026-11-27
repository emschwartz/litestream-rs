//! The oracle across checkpoints: what a restore must equal when replicators commit
//! `_litestream_seq` bumps of their own between the application's commits.
//!
//! Every checkpoint, Go's and Rust's, ends with one statement of the replicator's own,
//! `INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq =
//! seq + 1` (Go `db.go:1667`, Rust `crates/sqlite/src/checkpoint.rs`). That commit is a
//! database state the test cannot record, because it happens inside another process at a time
//! the test does not choose. So a restore is compared with the recorded states on every page
//! but one, and that one is checked by meaning instead:
//!
//! - The masked page is the root page of `_litestream_seq`, which the fixture creates before any
//!   replicator starts. The table holds at most one row of two integers, so its root is a leaf
//!   (checked: the page's b-tree type byte is 0x0d in every restore) and it is the table's
//!   only page. No application row, index entry or freelist page can live on it: without
//!   VACUUM or auto-vacuum (the workload runs neither) a root page never moves and is never
//!   freed. The restore's schema is checked to name the same root page.
//! - In WAL mode a commit does not touch page 1's change counter, so the bump writes that one
//!   page and nothing else; if it wrote any other page, the masked comparison would fail.
//! - The masked page's content is then pinned by its value. Each recorded state carries the
//!   `seq` the live database held just before the application's commit (its floor) and the
//!   `seq` of the copy its pages came from (which may hold bumps committed after the commit,
//!   since the copied WAL can run past the read). A restore that matches state `k` on every
//!   other page lies between the application's commit `k` and commit `k + 1`, so its `seq` is
//!   at least state `k`'s floor and at most state `k + 1`'s copy (or the live database's, for
//!   the last state); and when it equals state `k`'s copy, the restore must equal state `k`
//!   on every page, the masked one included (the same bumps write the same bytes). Across a
//!   run the restored `seq` never goes down.
//!
//! So nothing the application wrote is ignored, and the only freedom left is which number of
//! checkpoints a restore has seen between two application commits, within the bounds above.
//!
//! A state is captured from a copy of the database file and WAL taken inside a read
//! transaction on the application's connection: while it is open no checkpointer can restart
//! or truncate the WAL, and frames it backfills into the database file are still in the WAL
//! the copy recovers from. A copy taken without it can tear across a checkpoint.

use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use lsr_compat::{AppFixture, hash_pages, integrity_check};
use rusqlite::{Connection, OptionalExtension};

const TABLE_LEAF: u8 = 0x0d;

/// One database state: its page hashes and the `_litestream_seq` value it held (0 with no
/// row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    pub pages: Vec<String>,
    pub seq: i64,
}

fn seq_in(conn: &Connection) -> i64 {
    conn.query_row("SELECT seq FROM _litestream_seq WHERE id = 1", [], |r| {
        r.get(0)
    })
    .optional()
    .expect("read _litestream_seq")
    .unwrap_or(0)
}

/// The live database's `_litestream_seq` value, through the application's connection.
pub fn live_seq(fixture: &AppFixture) -> i64 {
    seq_in(&fixture.app)
}

fn seq_root_in(conn: &Connection) -> u32 {
    conn.query_row(
        "SELECT rootpage FROM sqlite_master WHERE type = 'table' AND name = '_litestream_seq'",
        [],
        |r| r.get(0),
    )
    .expect("_litestream_seq exists")
}

/// The live database's `_litestream_seq` root page.
pub fn seq_root(fixture: &AppFixture) -> u32 {
    let root = seq_root_in(&fixture.app);
    assert!(root > 1, "_litestream_seq's root is page {root}");
    root
}

fn scratch(fixture: &AppFixture, name: &str) -> PathBuf {
    let dir = fixture.dir().join("capture");
    fs::create_dir_all(&dir).expect("capture dir");
    for suffix in ["", "-wal", "-shm"] {
        drop(fs::remove_file(dir.join(format!("{name}{suffix}"))));
    }
    dir.join(name)
}

/// The database file and its wal-index, each opened once and kept open while the
/// application's connection lives, so every copy and wal-index read goes through them.
/// Closing any descriptor of a file drops every POSIX lock this process holds on it, the
/// application's SQLite locks included: its shared lock on the database file is what stops
/// another process's last connection from checkpointing and deleting the WAL and wal-index
/// on close, and its lock on the wal-index's dead-man-switch byte is what stops the next
/// process to open the database from taking the wal-index for new and rebuilding it from the
/// WAL alone. Seen once while copies opened and closed their own descriptors: a final Go turn
/// left the application with no WAL.
pub struct Held {
    db: File,
    shm: File,
}

impl Held {
    /// Opens the fixture's database file and wal-index, which the application created.
    pub fn open(fixture: &AppFixture) -> Self {
        let open = |path: PathBuf| {
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .expect("open a file the application created")
        };
        Self {
            db: open(fixture.db.clone()),
            shm: open(PathBuf::from(format!("{}-shm", fixture.db.display()))),
        }
    }

    /// The wal-index, for `uncommitted.rs`.
    pub const fn shm(&self) -> &File {
        &self.shm
    }

    fn copy_db(&self, to: &Path) {
        let mut from = &self.db;
        from.seek(SeekFrom::Start(0)).expect("seek the database");
        let mut out = File::create(to).expect("create the copy");
        io::copy(&mut from, &mut out).expect("copy the database");
    }
}

/// The database as of now, copied inside a read transaction (see the module doc).
pub fn capture(fixture: &AppFixture, held: &Held) -> State {
    capture_wal(fixture, held, "copy.db", None)
}

/// The state SQLite recovers from the database file and the first `wal_len` bytes of the WAL
/// (all of it for `None`), copied inside a read transaction to `name` in the capture
/// directory. With no wal-index beside the copy, SQLite rebuilds one from the WAL alone, so
/// the copy holds every valid transaction in those bytes, committed in the live wal-index or
/// not (`uncommitted.rs` relies on this).
pub fn capture_wal(fixture: &AppFixture, held: &Held, name: &str, wal_len: Option<u64>) -> State {
    let copy = scratch(fixture, name);
    let wal = PathBuf::from(format!("{}-wal", fixture.db.display()));
    let copy_wal = format!("{}-wal", copy.display());
    fixture.app.execute_batch("BEGIN").expect("begin");
    let _: i64 = fixture
        .app
        .query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get(0))
        .expect("take the read lock");
    held.copy_db(&copy);
    fs::copy(&wal, &copy_wal).expect("copy the wal");
    if let Some(len) = wal_len {
        fs::OpenOptions::new()
            .write(true)
            .open(&copy_wal)
            .and_then(|f| f.set_len(len))
            .expect("cut the copied wal");
    }
    fixture.app.execute_batch("COMMIT").expect("end the read");
    let conn = Connection::open(&copy).expect("open the copy");
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .expect("fold the copy's wal");
    let seq = seq_in(&conn);
    drop(conn);
    State {
        pages: hash_pages(&copy).expect("hash").page_hashes,
        seq,
    }
}

/// A restored database's state, after checking that `root` is still `_litestream_seq`'s only
/// page there.
pub fn restored(path: &Path, root: u32) -> State {
    integrity_check(path).expect("integrity_check");
    let conn = Connection::open(path).expect("open the restore");
    assert_eq!(
        seq_root_in(&conn),
        root,
        "the restore moved _litestream_seq"
    );
    let seq = seq_in(&conn);
    drop(conn);
    let bytes = fs::read(path).expect("read the restore");
    let digest = hash_pages(path).expect("hash");
    let size = usize::try_from(digest.page_size).expect("small");
    let at = usize::try_from(root - 1).expect("small") * size;
    assert_eq!(
        bytes.get(at).copied(),
        Some(TABLE_LEAF),
        "_litestream_seq's root page {root} is not a lone table leaf"
    );
    State {
        pages: digest.page_hashes,
        seq,
    }
}

/// The recorded states and how far the restores have matched.
#[derive(Debug)]
pub struct Oracle {
    /// After every application commit, the prepared database first.
    pub states: Vec<State>,
    /// For each state, the live `seq` read just before its commit.
    floors: Vec<i64>,
    /// The state the newest checked restore matched.
    matched: usize,
    /// The newest checked restore's `seq`.
    seq: i64,
    /// `_litestream_seq`'s root page, one-based.
    root: u32,
}

impl Oracle {
    pub fn new(first: State, root: u32) -> Self {
        Self {
            floors: vec![first.seq],
            states: vec![first],
            matched: 0,
            seq: 0,
            root,
        }
    }

    pub const fn root(&self) -> u32 {
        self.root
    }

    /// Records the state after a commit; `floor` is the live `seq` read just before it.
    pub fn record(&mut self, state: State, floor: i64) {
        self.states.push(state);
        self.floors.push(floor);
    }

    /// Whether `a` and `b` agree on every page but `_litestream_seq`'s root.
    pub fn same_but_seq(&self, a: &[String], b: &[String]) -> bool {
        let skip = usize::try_from(self.root - 1).expect("small");
        a.len() == b.len()
            && a.iter()
                .zip(b)
                .enumerate()
                .all(|(i, (x, y))| i == skip || x == y)
    }

    /// Checks one restore, in transaction order, against the states at or after the previous
    /// match; `live` is the database's `seq` now (no replicator runs). The error says what
    /// differs. A restore that fails leaves the oracle as it was, so the next restore is
    /// judged against the same match.
    pub fn verdict(&mut self, got: &State, live: i64) -> Result<(), String> {
        let found = (self.matched..self.states.len())
            .find(|&k| self.same_but_seq(&self.states[k].pages, &got.pages));
        let Some(k) = found else {
            let earlier = (0..self.states.len())
                .find(|&k| self.same_but_seq(&self.states[k].pages, &got.pages));
            return Err(format!(
                "the restore is no state at or after commit {} on the application's pages: {}",
                self.matched,
                earlier.map_or("no recorded state at all".to_owned(), |k| format!(
                    "it is the state after commit {k}"
                ))
            ));
        };
        let low = self.floors[k];
        let high = self.states.get(k + 1).map_or(live, |s| s.seq);
        if !((low..=high).contains(&got.seq) && got.seq >= self.seq) {
            return Err(format!(
                "the restore is commit {k}'s state with _litestream_seq {}, outside \
                 {low}..={high} or below the previous restore's {}",
                got.seq, self.seq
            ));
        }
        if got.seq == self.states[k].seq && got.pages != self.states[k].pages {
            return Err(format!(
                "commit {k}'s state with its own seq {} differs on _litestream_seq's page",
                got.seq
            ));
        }
        self.matched = k;
        self.seq = got.seq;
        Ok(())
    }
}

//! The application the benchmark replicates: one SQLite database in WAL mode, written by
//! transactions that each also bump a marker row.
//!
//! The marker is how the observer knows which commits a level-0 file holds. It is a table of
//! one row whose value is the number of commits so far as eight digits, so its page never
//! changes shape and sits at a known page number; every commit rewrites it, so every level-0
//! file that holds a commit holds that page, and the value on it is the last commit the file
//! holds. The value is the last eight bytes of the page: SQLite stores a leaf's cells from
//! the end of the page, and the page holds only this cell.

use std::path::Path;

use rusqlite::Connection;

use crate::error::CompatError;
use crate::workload::SplitMix;

/// The page size of every benchmark database.
pub(super) const PAGE_SIZE: u32 = 4096;
/// Bytes in a row's payload: four rows fill a page.
const PAYLOAD: usize = 1000;
const PRELOAD_CHUNK: u32 = 5000;
/// Commits of a large preload between two WAL truncations (about 200 MiB of WAL).
const CHECKPOINT_EVERY_CHUNKS: u32 = 40;

/// The writer's connection and what it has written.
#[derive(Debug)]
pub(super) struct App {
    conn: Connection,
    rng: SplitMix,
    rows: u64,
    commits: u64,
    marker_page: u32,
}

impl App {
    /// Creates the database in WAL mode, with autocheckpoint off (the replicator owns
    /// checkpoints) and `synchronous=NORMAL` (the usual setting for WAL), preloads
    /// `preload_rows` rows, then truncates the WAL so replication starts from a settled file.
    pub(super) fn create(path: &Path, seed: u64, preload_rows: u32) -> Result<Self, CompatError> {
        Self::create_with(path, seed, preload_rows, None)
    }

    /// As [`App::create`], for a database too large to leave its WAL until the end: the WAL
    /// is truncated every `CHECKPOINT_EVERY_CHUNKS` commits, so the file is the only large thing on disk.
    pub(super) fn create_large(
        path: &Path,
        seed: u64,
        preload_rows: u32,
    ) -> Result<Self, CompatError> {
        Self::create_with(path, seed, preload_rows, Some(CHECKPOINT_EVERY_CHUNKS))
    }

    fn create_with(
        path: &Path,
        seed: u64,
        preload_rows: u32,
        checkpoint_every: Option<u32>,
    ) -> Result<Self, CompatError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "page_size", PAGE_SIZE)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "wal_autocheckpoint", 0)?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, data BLOB NOT NULL);
             CREATE TABLE marker(id INTEGER PRIMARY KEY, v TEXT NOT NULL);
             INSERT INTO marker VALUES (1, '00000000');",
        )?;
        let marker_page: u32 = conn.query_row(
            "SELECT rootpage FROM sqlite_master WHERE name = 'marker'",
            [],
            |r| r.get(0),
        )?;
        let mut app = Self {
            conn,
            rng: SplitMix::new(seed),
            rows: 0,
            commits: 0,
            marker_page,
        };
        let mut left = preload_rows;
        let mut chunks = 0_u32;
        while left > 0 {
            let n = left.min(PRELOAD_CHUNK);
            app.insert_in_tx(n, false)?;
            left = left.saturating_sub(n);
            chunks = chunks.saturating_add(1);
            if checkpoint_every.is_some_and(|every| chunks.is_multiple_of(every)) {
                app.checkpoint()?;
            }
        }
        app.checkpoint()?;
        Ok(app)
    }

    /// The page number of the marker row.
    pub(super) const fn marker_page(&self) -> u32 {
        self.marker_page
    }

    /// Commits so far, the marker's value.
    pub(super) const fn commits(&self) -> u64 {
        self.commits
    }

    /// The database size in pages.
    pub(super) fn page_count(&self) -> Result<u32, CompatError> {
        Ok(self.conn.query_row("PRAGMA page_count", [], |r| r.get(0))?)
    }

    /// `PRAGMA wal_checkpoint(TRUNCATE)`: every committed page in the main file, the WAL empty.
    pub(super) fn checkpoint(&self) -> Result<(), CompatError> {
        let (busy, _log, _done): (i64, i64, i64) =
            self.conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
        if busy != 0 {
            return Err(CompatError::Fixture(
                "the checkpoint was blocked by another connection".to_owned(),
            ));
        }
        Ok(())
    }

    fn insert_in_tx(&mut self, n: u32, mark: bool) -> Result<(), CompatError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("INSERT INTO items(data) VALUES (?1)")?;
            for _ in 0..n {
                stmt.execute([payload(&mut self.rng)])?;
                self.rows = self.rows.saturating_add(1);
            }
        }
        if mark {
            bump_marker(&tx, &mut self.commits)?;
        }
        Ok(tx.commit()?)
    }

    /// One commit that inserts `n` rows and bumps the marker.
    pub(super) fn insert_rows(&mut self, n: u32) -> Result<(), CompatError> {
        self.insert_in_tx(n, true)
    }

    /// One commit that rewrites `n` random rows in place and bumps the marker.
    pub(super) fn update_rows(&mut self, n: u32) -> Result<(), CompatError> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("UPDATE items SET data = ?2 WHERE id = ?1")?;
            for _ in 0..n {
                let id = self.rng.below(self.rows).saturating_add(1);
                let id = i64::try_from(id).unwrap_or(1);
                stmt.execute((id, payload(&mut self.rng)))?;
            }
        }
        bump_marker(&tx, &mut self.commits)?;
        Ok(tx.commit()?)
    }
}

/// A row's payload: a quarter random bytes and the rest words, so pages compress about as
/// well as real application data does (about 1.5 to 2 times with LZ4).
fn payload(rng: &mut SplitMix) -> Vec<u8> {
    let mut data = Vec::with_capacity(PAYLOAD);
    for _ in 0..(PAYLOAD / 4).div_ceil(8) {
        data.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    data.truncate(PAYLOAD / 4);
    data.extend_from_slice(rng.text(PAYLOAD - PAYLOAD / 4).as_bytes());
    data
}

fn bump_marker(tx: &rusqlite::Transaction<'_>, commits: &mut u64) -> Result<(), CompatError> {
    *commits = commits.saturating_add(1);
    tx.execute(
        "UPDATE marker SET v = ?1 WHERE id = 1",
        [format!("{:08}", *commits)],
    )?;
    Ok(())
}

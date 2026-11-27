//! The self-certifying workload: table `chain`, where row `k` holds `H(k, row k-1, payload)`.
//!
//! The driver inserts one row per commit and logs the new row's hash as the node's [`Digest`].
//! A restored file is judged by reading it with SQLite alone: `PRAGMA integrity_check`, then the
//! rows from 1 up, each hash recomputed from the row before. The last hash names the node, so a
//! restore that mixes two histories, drops a middle row or keeps a stale page fails here or is
//! a state the log never saw.

use std::path::Path;

use rusqlite::{Connection, OpenFlags, OptionalExtension};

use crate::Digest;

/// The workload's table.
pub const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS chain(k INTEGER PRIMARY KEY, h BLOB NOT NULL, payload BLOB NOT NULL)";

/// The chain's tip: its newest row number and hash; row 0 is the genesis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Chain {
    k: u64,
    h: Digest,
}

impl Default for Chain {
    fn default() -> Self {
        Self::new()
    }
}

impl Chain {
    /// The chain before its first row.
    #[must_use]
    pub fn new() -> Self {
        Self {
            k: 0,
            h: Digest::genesis(),
        }
    }

    /// The tip's row number.
    #[must_use]
    pub const fn k(&self) -> u64 {
        self.k
    }

    /// The tip's hash: the node's name in the log.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.h
    }

    /// The chain with one more row holding `payload`.
    #[must_use]
    pub fn next(&self, payload: &[u8]) -> Self {
        let k = self.k.saturating_add(1);
        Self {
            k,
            h: Digest::link(k, &self.h, payload),
        }
    }

    /// Inserts the tip of `self` (made by [`Self::next`] with `payload`) on `conn`.
    ///
    /// # Errors
    /// SQLite's.
    pub fn insert(&self, conn: &Connection, payload: &[u8]) -> rusqlite::Result<()> {
        let k = i64::try_from(self.k).unwrap_or(i64::MAX);
        conn.execute(
            "INSERT INTO chain(k, h, payload) VALUES (?1, ?2, ?3)",
            rusqlite::params![k, self.h.bytes().as_slice(), payload],
        )
        .map(drop)
    }

    /// The SQL text that inserts the tip of `self` with `payload`, for a driver that can only
    /// send statements (`X'..'` blob literals).
    #[must_use]
    pub fn insert_sql(&self, payload: &[u8]) -> String {
        format!(
            "INSERT INTO chain(k, h, payload) VALUES ({}, X'{}', X'{}')",
            self.k,
            hex::encode(self.h.bytes()),
            hex::encode(payload)
        )
    }
}

/// The chain `conn` holds, verified row by row from the genesis. A database without the table is
/// at the genesis.
///
/// # Errors
/// What does not verify: a gap in the row numbers, a hash that is not the link of the row before,
/// or SQLite's error.
pub fn verify(conn: &Connection) -> Result<Chain, String> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'chain'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let mut chain = Chain::new();
    if exists.is_none() {
        return Ok(chain);
    }
    let mut stmt = conn
        .prepare("SELECT k, h, payload FROM chain ORDER BY k")
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let k: i64 = row.get(0).map_err(|e| e.to_string())?;
        let h: Vec<u8> = row.get(1).map_err(|e| e.to_string())?;
        let payload: Vec<u8> = row.get(2).map_err(|e| e.to_string())?;
        let want = chain.next(&payload);
        if u64::try_from(k).ok() != Some(want.k) {
            return Err(format!("row {k} follows row {}", chain.k));
        }
        if Digest::from_slice(&h) != Some(want.h) {
            return Err(format!("row {k}'s hash does not certify row {}", chain.k));
        }
        chain = want;
    }
    Ok(chain)
}

/// Judges a restored file with SQLite alone: it passes `PRAGMA integrity_check` and its chain
/// verifies. Returns the tip's digest.
///
/// # Errors
/// Why the file is not a sound restore of the workload.
pub fn read(path: &Path) -> Result<Digest, String> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags).map_err(|e| e.to_string())?;
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    if check != "ok" {
        return Err(format!("integrity_check: {check}"));
    }
    verify(&conn).map(|chain| chain.digest())
}

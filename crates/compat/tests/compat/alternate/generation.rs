//! Which WAL generation the newest staged file and the WAL itself are in, read from files with
//! no process running, and what that says a boot must do.
//!
//! A boot continues from the newest staged level-0 file (the newest staged file). When the newest staged file was made in
//! the WAL generation the WAL file still holds (same salts, file long enough), both engines
//! must continue it: no snapshot. When the previous process checkpointed and restarted or
//! truncated the WAL and died before it staged anything in the new generation, nothing at boot
//! can prove that every frame between the newest staged file and the restart reached a file, so both engines'
//! rules take a snapshot:
//! - Rust, `crates/core/src/recover.rs`: rule 6, salts changed, with a newest staged file read at boot (no
//!   read-transaction pin to prove the restart) is `GenerationChanged`; rule 4, no WAL header
//!   at all (a TRUNCATE left the file empty), is `WalMissing`.
//! - Go, `verifyWithExecutor` (`db.go:1782-1925`): a WAL shorter than the newest staged file's end is "wal
//!   truncated by another process" (the in-memory `syncedToWALEnd` that excuses it is gone
//!   after a restart); otherwise salts that changed are "wal header salt reset, snapshotting",
//!   "last page does not exist in last ltx file, wal overwritten by another process", or "full
//!   or restart checkpoint detected, snapshotting", whichever branch the offsets reach. Go
//!   can also continue a changed generation when its last-page heuristic matches and it finds
//!   no full checkpoint; the test reports that and lets the restores judge it.

use std::fs;
use std::path::{Path, PathBuf};

use lsr_compat::{AppFixture, list};
use lsr_core::Level;
use lsr_ltx::{Header, Txid, WalOrigin};

/// The WAL file's salts, or `None` when it holds no full header.
pub fn wal_salts(fixture: &AppFixture) -> Option<(u32, u32)> {
    let bytes = fs::read(wal_path(fixture)).ok()?;
    let word = |at: usize| {
        bytes
            .get(at..at + 4)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(u32::from_be_bytes)
    };
    Some((word(16)?, word(20)?))
}

pub fn wal_path(fixture: &AppFixture) -> PathBuf {
    PathBuf::from(format!("{}-wal", fixture.db.display()))
}

/// The WAL file's length in bytes (0 when missing).
pub fn wal_len(fixture: &AppFixture) -> u64 {
    fs::metadata(wal_path(fixture)).map_or(0, |m| m.len())
}

fn newest_in(dir: &Path) -> Option<(Txid, Header)> {
    let listing = list(dir).ok()?;
    let file = listing
        .files
        .iter()
        .filter(|f| f.key.level() == Level::ZERO)
        .max_by_key(|f| f.key.range().max())?;
    let bytes = fs::read(dir.join(&file.rel)).ok()?;
    let head: &[u8; lsr_ltx::HEADER_LEN] = bytes.get(..lsr_ltx::HEADER_LEN)?.try_into().ok()?;
    Some((file.key.range().max(), Header::decode(head).ok()?))
}

/// The WAL range of the newest level-0 file staged or uploaded.
pub fn newest_staged_origin(fixture: &AppFixture) -> Option<(Txid, Option<WalOrigin>)> {
    let staged = newest_in(&fixture.meta);
    let uploaded = newest_in(&fixture.replica);
    let newest = match (staged, uploaded) {
        (Some(a), Some(b)) => Some(if b.0 > a.0 { b } else { a }),
        (a, b) => a.or(b),
    }?;
    Some((newest.0, newest.1.fields().wal))
}

/// What the next boot must do, read with no process running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// Nothing staged yet: the first sync is a snapshot.
    First,
    /// The newest staged file is in the WAL's generation: continue, no snapshot.
    Continue,
    /// The WAL restarted (salts changed) after the newest staged file.
    Restarted,
    /// The WAL is shorter than the newest staged file's end, or holds no header (a TRUNCATE).
    Truncated,
}

impl Expect {
    /// Whether this boot may snapshot by the engines' rules.
    pub const fn may_snapshot(self) -> bool {
        match self {
            Self::First | Self::Restarted | Self::Truncated => true,
            Self::Continue => false,
        }
    }

    /// The Rust causes this boot may log (`Debug` of `SnapshotCause`).
    pub const fn rust_causes(self) -> &'static [&'static str] {
        match self {
            Self::First => &["FirstSync"],
            Self::Continue => &[],
            Self::Restarted => &["GenerationChanged"],
            Self::Truncated => &["WalMissing", "GenerationChanged"],
        }
    }

    /// The Go reasons this boot may log.
    pub const fn go_reasons(self) -> &'static [&'static str] {
        match self {
            Self::First | Self::Continue => &[],
            Self::Restarted => &[
                "wal header salt reset, snapshotting",
                "last page does not exist in last ltx file, wal overwritten by another process",
                "full or restart checkpoint detected, snapshotting",
                "wal truncated by another process",
            ],
            Self::Truncated => &[
                "wal truncated by another process",
                "wal header salt reset, snapshotting",
            ],
        }
    }
}

/// What the next boot must do, from the newest staged file and the WAL as they are now.
pub fn expect(fixture: &AppFixture) -> Expect {
    let Some((txid, origin)) = newest_staged_origin(fixture) else {
        return Expect::First;
    };
    let origin =
        origin.unwrap_or_else(|| panic!("the newest staged file {txid} names no WAL range"));
    let Some(salts) = wal_salts(fixture) else {
        return Expect::Truncated;
    };
    if salts != origin.salts() {
        if wal_len(fixture) < origin.offset() + origin.size() {
            return Expect::Truncated;
        }
        return Expect::Restarted;
    }
    Expect::Continue
}

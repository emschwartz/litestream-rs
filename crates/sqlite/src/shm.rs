//! The wal-index header, read through SQLite's own mapping of `-shm`.
//!
//! Mechanism (measured in `docs/spikes.md`, "Phase 3 spikes", the wal-index header spike). The reader's `sqlite3_file`
//! is asked for region 0 with `xShmMap(file, 0, 32768, 0, &p)`. That call opens nothing only
//! when the connection's WAL has already mapped `-shm`; [`guard`] checks that first, in this
//! order: a read transaction is open (`walIndexReadHdr` maps region 0 when one starts), the
//! journal mode is WAL, and the locking mode is normal (exclusive mode keeps the wal-index in
//! heap memory). Unguarded, `xShmMap` on a connection that never mapped it creates and opens a
//! `-shm` descriptor (`unixOpenSharedMemory`, `sqlite3.c:45606`) which only a close undoes.
//!
//! The region is mapped once, at open, and kept: it belongs to the process's one shm node for
//! the inode and stays mapped while the reader's WAL is open, across the ends and retakes of
//! its read transaction. The reader never changes its journal mode and is closed only with the
//! [`crate::Sqlite`] that owns it, which is what makes the region valid.
//!
//! One read is `walIndexTryHdr`'s: twelve relaxed 32-bit loads of copy 0, a full fence, twelve
//! of copy 1, a fence, then `nBackfill` (word 24). The checks are SQLite's: equal copies,
//! `isInit`, the known `iVersion`, and `aCksum` over the first 40 bytes. Under write load about
//! one read in thirty sees a writer between its two copies; every failed check is [`NotReady`],
//! which the trait methods report as `SourceError::Busy` for the caller to retry. Atomic loads
//! are the least wrong reading of memory that C and other processes write, the same stance as
//! SQLite's `volatile` and `memcpy`. `mxFrame`, `nBackfill` and the checksum are native byte
//! order; the salts are the WAL header's bytes copied verbatim, so big-endian.

use std::ffi::{c_int, c_void};
use std::path::Path;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicU32, Ordering, fence};

use rusqlite::{Connection, TransactionState, ffi};

use crate::error::{OpenError, Refusal};
use crate::pages::FileHandle;

/// The wal-index region size every SQLite caller passes (`WALINDEX_PGSZ`, 32 KiB).
const REGION_BYTES: c_int = 32_768;
const REGION_WORDS: usize = 8_192;
/// One `WalIndexHdr` copy is 48 bytes, twelve words.
const COPY_WORDS: usize = 12;
/// `WalCkptInfo.nBackfill`, byte 96.
const N_BACKFILL_WORD: usize = 24;
/// `WALINDEX_MAX_VERSION` in SQLite 3.54.1 (`sqlite3.c:67150`).
const WALINDEX_VERSION: u32 = 3_007_000;

/// A header read that a retry may fix, as SQLite's own readers retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NotReady {
    /// The two copies differ: a writer was between its two `memcpy`s.
    Torn,
    /// `isInit` is zero: no connection has built the header.
    Uninitialised,
    /// The copies agree but `aCksum` does not match them.
    BadChecksum,
    /// A wal-index format this code does not know.
    UnknownVersion(u32),
}

/// The fields of one checked header read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) change: u32,
    pub(crate) mx_frame: u32,
    pub(crate) salt1: u32,
    pub(crate) salt2: u32,
    pub(crate) backfilled: u32,
}

/// Region 0 of a connection's wal-index mapping. Valid while that connection's WAL is open,
/// which its owner guarantees (module doc).
#[derive(Debug)]
pub(crate) struct Region {
    words: NonNull<AtomicU32>,
}

impl Region {
    /// The region as 8,192 shared 32-bit words.
    #[expect(unsafe_code, reason = "the region is C memory handed out by xShmMap")]
    pub(crate) fn words(&self) -> &[AtomicU32] {
        // SAFETY: `map` checked the pointer non-null and aligned; region 0 is 32 KiB of mapped
        // memory owned by the shm node, which outlives the connection that owns this region
        // (module doc). Others write it concurrently, which atomic loads tolerate.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast_const(), REGION_WORDS) }
    }
}

fn query(conn: &Connection, path: &Path, sql: &str) -> Result<String, OpenError> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(|source| OpenError::Sqlite {
            path: path.to_owned(),
            source,
        })
}

/// The states in which the connection's WAL has already mapped `-shm`, so `xShmMap` opens nothing.
pub(crate) fn guard(conn: &Connection, path: &Path) -> Result<(), OpenError> {
    let state = conn
        .transaction_state(Some(rusqlite::MAIN_DB))
        .map_err(|source| OpenError::Sqlite {
            path: path.to_owned(),
            source,
        })?;
    if !matches!(state, TransactionState::Read | TransactionState::Write) {
        return Err(Refusal::NoReadTransaction.into());
    }
    let mode = query(conn, path, "PRAGMA main.journal_mode")?;
    if mode != "wal" {
        return Err(Refusal::NotWal { mode }.into());
    }
    let mode = query(conn, path, "PRAGMA main.locking_mode")?;
    if mode != "normal" {
        return Err(Refusal::NotNormalLocking { mode }.into());
    }
    Ok(())
}

/// Region 0 of `file`'s wal-index, with no extend. Call only after [`guard`] passed.
#[expect(unsafe_code, reason = "a call through the VFS's xShmMap")]
pub(crate) fn map(file: &FileHandle) -> Result<Region, Refusal> {
    let mut region: *mut c_void = ptr::null_mut();
    // SAFETY: the method belongs to `file`, which is live; with `pShm` already set (the guard)
    // it only returns `apRegion[0]` under the node's mutex (`sqlite3.c:45760`).
    let rc = unsafe { (file.shm_map)(file.file.as_ptr(), 0, REGION_BYTES, 0, &raw mut region) };
    if rc != ffi::SQLITE_OK {
        return Err(Refusal::MapFailed { rc });
    }
    let words = NonNull::new(region.cast::<AtomicU32>()).ok_or(Refusal::Unmapped)?;
    if !words.is_aligned() {
        return Err(Refusal::Misaligned);
    }
    Ok(Region { words })
}

/// SQLite's `walChecksumBytes` in native order over the first 40 bytes of a header copy.
fn header_checksum(copy: &[u32; COPY_WORDS]) -> (u32, u32) {
    let (pairs, _rest) = copy.as_chunks::<2>();
    pairs
        .iter()
        .take(5)
        .fold((0_u32, 0_u32), |(s1, s2), &[a, b]| {
            let s1 = s1.wrapping_add(a).wrapping_add(s2);
            let s2 = s2.wrapping_add(b).wrapping_add(s1);
            (s1, s2)
        })
}

fn load_copy(words: &[AtomicU32], first: usize) -> [u32; COPY_WORDS] {
    let mut copy = [0_u32; COPY_WORDS];
    for (slot, word) in copy.iter_mut().zip(words.iter().skip(first)) {
        *slot = word.load(Ordering::Relaxed);
    }
    copy
}

/// One header read in `walIndexTryHdr`'s order, checked as it checks.
pub(crate) fn read_header(words: &[AtomicU32]) -> Result<Header, NotReady> {
    let first = load_copy(words, 0);
    fence(Ordering::SeqCst);
    let second = load_copy(words, COPY_WORDS);
    fence(Ordering::SeqCst);
    let backfilled = words
        .get(N_BACKFILL_WORD)
        .map_or(0, |w| w.load(Ordering::Relaxed));
    if first != second {
        return Err(NotReady::Torn);
    }
    let [
        version,
        _pad,
        change,
        init,
        mx_frame,
        _pages,
        _c1,
        _c2,
        s1,
        s2,
        k1,
        k2,
    ] = first;
    if init.to_ne_bytes().first() == Some(&0) {
        return Err(NotReady::Uninitialised);
    }
    if version != WALINDEX_VERSION {
        return Err(NotReady::UnknownVersion(version));
    }
    if header_checksum(&first) != (k1, k2) {
        return Err(NotReady::BadChecksum);
    }
    Ok(Header {
        change,
        mx_frame,
        salt1: u32::from_be_bytes(s1.to_ne_bytes()),
        salt2: u32::from_be_bytes(s2.to_ne_bytes()),
        backfilled,
    })
}

#[cfg(test)]
mod tests;

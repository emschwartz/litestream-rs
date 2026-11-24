//! The SQLite this crate runs on is the one its layout reads were written for.
//!
//! The `identity` module reads the descriptor out of SQLite's private `struct unixFile`, and
//! `shm` reads the wal-index header through SQLite's own mapping: both follow the bundled
//! 3.54.1 source. A build that links another SQLite (a system library through pkg-config,
//! SQLCipher, a loadable-extension build) could lay those out otherwise, and a read of the
//! wrong field is a wrong descriptor or a wrong committed end, not an error. So the library
//! that answers at run time must be that exact build: its version number and its source id
//! (the check-in hash, which tells two builds of one version apart) are compared with the ones
//! these modules were written for, and anything else refuses to open.
//!
//! This sees only the SQLite this crate's calls reach. A second copy linked into the
//! application for its own connections breaks the one-SQLite rule (see the crate doc) without
//! changing what this crate's calls answer.

use std::ffi::c_int;

use crate::error::OpenError;

/// `SQLITE_VERSION_NUMBER` of the source `identity` and `shm` were written against.
const WRITTEN_FOR_NUMBER: c_int = 3_054_001;

/// `SQLITE_SOURCE_ID` of that source (`sqlite3.h` in `libsqlite3-sys` 0.40.0's bundle).
const WRITTEN_FOR_SOURCE_ID: &str =
    "2026-10-16 14:45:20 555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76";

/// Checks the SQLite that answers this crate's calls.
///
/// # Errors
/// [`OpenError::SqliteBuild`] when it is not the build the layout reads were written for.
pub(crate) fn check_linked() -> Result<(), OpenError> {
    check(crate::capi::version_number(), &crate::capi::source_id())
}

fn check(found: c_int, source_id: &str) -> Result<(), OpenError> {
    if found == WRITTEN_FOR_NUMBER && source_id == WRITTEN_FOR_SOURCE_ID {
        return Ok(());
    }
    Err(OpenError::SqliteBuild {
        found,
        source_id: source_id.to_owned(),
        expected: WRITTEN_FOR_NUMBER,
    })
}

#[cfg(test)]
mod tests;

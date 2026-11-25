//! The SQLite this crate runs on is one its layout reads have been verified against.
//!
//! The `identity` module reads the descriptor out of SQLite's private `struct unixFile`, and
//! `shm` reads the wal-index header through SQLite's own mapping. Neither layout is part of
//! SQLite's interface, so a build nobody has checked could lay them out otherwise, and a read of
//! the wrong field is a wrong descriptor or a wrong committed end, not an error. The library that
//! answers at run time must therefore be one of [`VERIFIED`]: its version number and its source
//! id (the check-in hash, which tells two builds of one version apart and is the same for every
//! compile of one check-in, a distribution's included) are compared with each entry, and
//! anything else refuses to open.
//!
//! Each entry is a build the whole lsr-sqlite suite has run against: CI runs it once per
//! libsqlite3-sys version in the workspace's range (`just sqlite-build`), on that version's
//! bundled SQLite. A new build gets onto the list by reviewing `identity` and `shm` against
//! its `sqlite3.c` (`struct unixFile`, `WalIndexHdr`, `WalCkptInfo`, `WALINDEX_MAX_VERSION`),
//! adding its libsqlite3-sys version to the CI matrix and its entry here, and seeing the suite
//! pass on it (docs/decisions/D200.md).
//!
//! This sees only the SQLite this crate's calls reach. A second copy linked into the
//! application for its own connections breaks the one-SQLite rule (see the crate doc) without
//! changing what this crate's calls answer.

use std::ffi::c_int;

use crate::error::OpenError;

/// One SQLite build the layout reads have been verified against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Build {
    /// `SQLITE_VERSION_NUMBER`.
    pub(crate) number: c_int,
    /// `SQLITE_SOURCE_ID`.
    pub(crate) source_id: &'static str,
}

/// Every verified build, oldest first, with the libsqlite3-sys versions that bundle it. Nothing
/// older than 3.51.3: 3.51.2 shipped a bug in the WAL reset.
pub(crate) const VERIFIED: [Build; 5] = [
    // libsqlite3-sys 0.37.0.
    Build {
        number: 3_051_003,
        source_id: "2026-03-13 10:38:09 737ae4a34738ffa0c3ff7f9bb18df914dd1cad163f28fd6b6e114a344fe6d618",
    },
    // libsqlite3-sys 0.38.0.
    Build {
        number: 3_053_001,
        source_id: "2026-05-05 10:34:17 c88b22011a54b4f6fbd149e9f8e4de77658ce58143a1af0e3785e4e6475127e9",
    },
    // libsqlite3-sys 0.38.1 and 0.38.2.
    Build {
        number: 3_053_002,
        source_id: "2026-06-03 19:12:13 d6e03d8c777cfa2d35e3b60d8ec3e0187f3e9f99d8e2ee9cac695fd6fcdf1a24",
    },
    // libsqlite3-sys 0.39.0.
    Build {
        number: 3_054_000,
        source_id: "2026-10-09 15:46:58 be8d059e9a49089ab2dce5ed26dd87aaf598fdc5fbe0b107c0dd758464bcd5e3",
    },
    // libsqlite3-sys 0.39.1 and 0.40.0.
    Build {
        number: 3_054_001,
        source_id: "2026-10-16 14:45:20 555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76",
    },
];

/// The version numbers of [`VERIFIED`], for the refusal (a test holds the two equal).
const VERIFIED_NUMBERS: [c_int; 5] = [3_051_003, 3_053_001, 3_053_002, 3_054_000, 3_054_001];

/// Checks the SQLite that answers this crate's calls.
///
/// # Errors
/// [`OpenError::SqliteBuild`] when it is not a verified build.
pub(crate) fn check_linked() -> Result<(), OpenError> {
    check(crate::capi::version_number(), &crate::capi::source_id())
}

fn check(found: c_int, source_id: &str) -> Result<(), OpenError> {
    if VERIFIED
        .iter()
        .any(|build| build.number == found && build.source_id == source_id)
    {
        return Ok(());
    }
    Err(OpenError::SqliteBuild {
        found,
        source_id: source_id.to_owned(),
        verified: &VERIFIED_NUMBERS,
    })
}

#[cfg(test)]
mod tests;

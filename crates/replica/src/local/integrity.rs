//! SQLite's own check of a restored database, run on the scratch file before
//! [super::FileSink::publish] links it to the target.
//!
//! The LTX checksums prove the pages are the ones the replica holds; they say
//! nothing about whether those pages form a valid SQLite database (a writer
//! bug upstream, or a replica assembled by hand, can be checksum-clean and
//! still a broken b-tree). `PRAGMA quick_check` walks every b-tree page;
//! `PRAGMA integrity_check` also matches every index against its table. Go's
//! `restore -integrity-check` offers the same two, off by default.
//!
//! The connection opens the scratch with `mode=ro&immutable=1`: SQLite takes
//! no lock and creates no `-wal` or `-shm` beside it, even for a database
//! whose header says WAL mode. That is sound only because the scratch is the
//! sink's own, locked against other restores and never opened by anyone
//! else. The connection is closed before this returns, so it never outlives
//! the check into the rename.
//!
//! SQLite reports at most [CHECK_LIMIT] problems (the pragma's argument); the
//! first [KEPT_MESSAGES] are kept. Recent SQLite returns them as one row of
//! newline-separated lines under a `*** in database main ***` heading, older
//! SQLite as one row each; both read the same here.

use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use rusqlite::{Connection, ErrorCode};
use thiserror::Error;

/// How many problems are kept in an [IntegrityFailure].
pub const KEPT_MESSAGES: usize = 5;

/// The pragma's limit: one past what is kept, so a cut list is known as cut.
const CHECK_LIMIT: usize = KEPT_MESSAGES + 1;

/// Which SQLite check a restore runs before it publishes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum IntegrityCheck {
    /// No check (Go's default, `-integrity-check none`).
    #[default]
    Off,
    /// `PRAGMA quick_check`: every b-tree page, not the index contents.
    Quick,
    /// `PRAGMA integrity_check`: as quick, and every index against its table.
    Full,
}

impl IntegrityCheck {
    /// The pragma this check runs, or `None` for [IntegrityCheck::Off].
    #[must_use]
    pub const fn pragma(self) -> Option<&'static str> {
        match self {
            Self::Off => None,
            Self::Quick => Some("quick_check"),
            Self::Full => Some("integrity_check"),
        }
    }
}

/// SQLite found the restored database invalid; the restore is refused and
/// nothing is published.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
#[error(
    "SQLite's {} refused the restored database: {}{}",
    .pragma,
    .messages.join("; "),
    if *.more { "; and more" } else { "" }
)]
pub struct IntegrityFailure {
    pragma: &'static str,
    messages: Vec<String>,
    more: bool,
}

impl IntegrityFailure {
    /// The failure for every problem SQLite listed: the first
    /// [KEPT_MESSAGES] of them, and a placeholder when it listed none.
    fn keeping_the_first(pragma: &'static str, mut messages: Vec<String>) -> Self {
        let more = messages.len() > KEPT_MESSAGES;
        messages.truncate(KEPT_MESSAGES);
        if messages.is_empty() {
            messages.push("no rows and no \"ok\"".to_owned());
        }
        Self {
            pragma,
            messages,
            more,
        }
    }

    /// The pragma that failed (`quick_check` or `integrity_check`).
    #[must_use]
    pub const fn pragma(&self) -> &'static str {
        self.pragma
    }

    /// SQLite's first problems, at most [KEPT_MESSAGES], never empty.
    #[must_use]
    pub fn messages(&self) -> &[String] {
        &self.messages
    }

    /// Whether SQLite reported more problems than [Self::messages] holds.
    #[must_use]
    pub const fn more(&self) -> bool {
        self.more
    }
}

/// Why [check] did not pass: SQLite's verdict, or the check could not run.
#[derive(Debug)]
pub(crate) enum CheckError {
    /// SQLite found the database invalid.
    Refused(IntegrityFailure),
    /// SQLite or the file system failed before a verdict.
    Failed(io::Error),
}

/// A `file:` URI for an absolute path: every byte but the unreserved ones and
/// `/` percent-encoded, so `?`, `#` and `%` in a name stay part of the path.
fn immutable_uri(path: &Path) -> io::Result<String> {
    let absolute = std::path::absolute(path)?;
    let mut uri = String::from("file://");
    for &byte in absolute.as_os_str().as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
            uri.push(char::from(byte));
        } else {
            uri.push_str(&format!("%{byte:02X}"));
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    Ok(uri)
}

/// Whether a SQLite error is SQLite judging the file, not the environment
/// failing: those refuse the restore like a failed check.
const fn is_verdict(code: ErrorCode) -> bool {
    matches!(code, ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase)
}

/// SQLite's problem lines, without the per-database headings.
fn problems(rows: &[String]) -> impl Iterator<Item = &str> {
    rows.iter()
        .flat_map(|row| row.lines())
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("*** in database"))
}

fn run_pragma(path: &Path, pragma: &str) -> rusqlite::Result<Vec<String>> {
    // The URI's `mode=ro` makes the connection read-only whatever the
    // default flags ask for, and stops SQLite creating a missing file.
    let conn = Connection::open(immutable_uri(path).map_err(|err| {
        rusqlite::Error::InvalidPath(format!("{}: {err}", path.display()).into())
    })?)?;
    let rows = {
        let mut stmt = conn.prepare(&format!("PRAGMA {pragma}({CHECK_LIMIT})"))?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .take(CHECK_LIMIT)
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    conn.close().map_err(|(_conn, err)| err)?;
    Ok(rows)
}

/// Runs [check] on the database at [path] and closes the connection.
///
/// # Errors
/// [CheckError::Refused] when SQLite finds the database invalid; any other
/// SQLite failure as [CheckError::Failed].
pub(crate) fn check(path: &Path, check: IntegrityCheck) -> Result<(), CheckError> {
    let Some(pragma) = check.pragma() else {
        return Ok(());
    };
    let messages = match run_pragma(path, pragma) {
        Ok(rows) if rows.len() == 1 && rows.first().is_some_and(|row| row == "ok") => {
            return Ok(());
        }
        Ok(rows) => problems(&rows).map(str::to_owned).collect::<Vec<_>>(),
        Err(err) if err.sqlite_error_code().is_some_and(is_verdict) => vec![err.to_string()],
        Err(err) => return Err(CheckError::Failed(io::Error::other(err))),
    };
    Err(CheckError::Refused(IntegrityFailure::keeping_the_first(
        pragma, messages,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure_of(count: usize) -> IntegrityFailure {
        let messages = (0..count).map(|n| format!("problem {n}")).collect();
        IntegrityFailure::keeping_the_first("quick_check", messages)
    }

    #[test]
    fn a_list_is_cut_only_past_what_is_kept() {
        let full = failure_of(KEPT_MESSAGES);
        assert_eq!(full.messages().len(), KEPT_MESSAGES);
        assert!(!full.more());
        let cut = failure_of(KEPT_MESSAGES + 1);
        assert_eq!(cut.messages(), full.messages());
        assert!(cut.more());
        assert_eq!(failure_of(0).messages(), ["no rows and no \"ok\""]);
    }

    #[test]
    fn problems_drop_the_headings_and_blank_lines() {
        let rows = [
            "*** in database main ***\nPage 2: bad\n\n  Page 3: worse  ".to_owned(),
            String::new(),
            "row 4 missing".to_owned(),
        ];
        assert_eq!(
            problems(&rows).collect::<Vec<_>>(),
            ["Page 2: bad", "Page 3: worse", "row 4 missing"]
        );
    }

    /// A file SQLite cannot open is the environment failing, not a verdict
    /// on a database: the restore fails rather than being refused.
    #[test]
    fn a_missing_file_fails_the_check_without_a_verdict() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("db");
        assert!(matches!(
            check(&missing, IntegrityCheck::Quick),
            Err(CheckError::Failed(_))
        ));
        assert!(!missing.exists(), "the check created the file it checks");
    }
}

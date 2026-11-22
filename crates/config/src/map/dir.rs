//! A `dir` entry, scanned once at start (Go's `NewDBsFromDirectoryConfig`,
//! `FindSQLiteDatabases`, `newDBFromDirectoryEntry`; `cmd/litestream/main.go:851-1104`).
//!
//! The walk is Go's `filepath.Walk` (Go 1.24), step for step, since its quirks decide which
//! databases replicate:
//! - Every path, the root included, is looked at with `lstat`. A `dir` that is a symbolic
//!   link is therefore not a directory: it is matched as a file, its target's header read,
//!   and a link to a directory finds nothing. A `dir` that is a database file is that one
//!   database, at relative path `.`.
//! - A directory's names are read, in byte order, before the walk decides to skip it, so
//!   an unreadable subdirectory fails the scan even without `recursive`.
//! - Links inside are never followed as directories; a link to a database is a database.
//! - A name is matched with Go's `filepath.Match` (`pattern.rs`), whose errors depend on the
//!   name: the scan fails at the first file whose match reaches a malformed part.
//! - A file is a database when one `read` of its first 16 bytes gives SQLite's header,
//!   bytes past a short read counting as zero, so a 15-byte `SQLite format 3` qualifies.
//!
//! That header read is the one place a database file is opened outside SQLite, and it
//! happens here, before any replicator holds the file (closing a descriptor drops every
//! POSIX lock the process holds on that file).
//!
//! For each database at relative path `rel`:
//! - the replica's `url` path, else its `path`, gains `/rel` (a file replica with no
//!   path replicates to `rel`, made absolute);
//! - `meta-dir` gives `<meta-dir>/<rel>-litestream`, `meta-path` gives
//!   `<meta-path>/<dir of rel>/.<name>-litestream`, and neither gives Go's default
//!   beside the database.
//!
//! An entry that finds nothing is refused, as Go refuses it without `watch`
//! (and `watch: true` was refused at load).

use std::io::{self, Read as _};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use url::Url;

use super::pattern::{self, BadPattern};
use super::{Found as Collected, MapProblemKind, ReplicaPlan, replica};
use crate::config::{DbConfig, DirSource, ReplicaConfig, ReplicaKind};
use crate::error::KeyPath;
use crate::expand::{Env, clean, expand_path};

/// What Go's `url.URL.String` escapes in a path (`shouldEscape` in `encodePath` mode): all
/// but letters, digits, `-_.~` and `$&+,/:;=@`. The relative path is escaped this way before
/// the `url` crate sees it, which would otherwise read `\` as `/` and keep `%41` as an escape.
const GO_PATH_ESCAPES: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'$')
    .remove(b'&')
    .remove(b'+')
    .remove(b',')
    .remove(b'/')
    .remove(b':')
    .remove(b';')
    .remove(b'=')
    .remove(b'@');

/// The header every SQLite database file starts with.
const HEADER: &[u8; 16] = b"SQLite format 3\0";

/// Why a `dir` entry gives no databases.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DirProblem {
    /// Matching a file's name reached a malformed part of the pattern (Go's
    /// `filepath.ErrBadPattern`, which fails Go's walk at that file).
    #[error("syntax error in pattern {pattern:?}, matching {}", .path.display())]
    BadPattern {
        /// The pattern.
        pattern: String,
        /// The file whose name reached the malformed part.
        path: PathBuf,
    },
    /// The walk failed.
    #[error("failed to scan directory {}: {kind:?}", .path.display())]
    Scan {
        /// Where.
        path: PathBuf,
        /// What the file system said.
        kind: io::ErrorKind,
    },
    /// Nothing matched.
    #[error("no SQLite databases found in directory {} with pattern {pattern}", .dir.display())]
    NoDatabases {
        /// The directory.
        dir: PathBuf,
        /// The pattern.
        pattern: String,
    },
    /// The replica URL does not take the relative path.
    #[error("parse replica url: it does not parse")]
    ReplicaUrl,
}

/// One database the scan found.
pub(super) struct Found {
    pub(super) path: PathBuf,
    pub(super) meta: Option<PathBuf>,
    pub(super) replica: Option<ReplicaPlan>,
}

fn refuse(at: &KeyPath, found: &mut Collected, problem: DirProblem) {
    found.problem(at, MapProblemKind::Dir(problem));
}

/// Every database the entry names, in walk order.
pub(super) fn scan(
    at: &KeyPath,
    source: &DirSource,
    db: &DbConfig,
    env: &dyn Env,
    found: &mut Collected,
) -> Vec<Found> {
    let dir = match expand_path(source.dir(), env) {
        Ok(dir) => dir,
        Err(error) => {
            found.problem(&at.key("dir"), MapProblemKind::Path(error));
            return Vec::new();
        }
    };
    let mut paths = Vec::new();
    let pattern = source.pattern().as_bytes();
    if let Err(problem) = walk(&dir, true, source.recursive(), pattern, &mut paths) {
        let key = if matches!(problem, DirProblem::BadPattern { .. }) {
            "pattern"
        } else {
            "dir"
        };
        refuse(&at.key(key), found, problem);
        return Vec::new();
    }
    if paths.is_empty() {
        let pattern = source.pattern().to_owned();
        refuse(at, found, DirProblem::NoDatabases { dir, pattern });
        return Vec::new();
    }
    let mut out = Vec::new();
    for path in paths {
        let rel = relative(&dir, &path);
        let meta = meta_for(at, source, db, &path, &rel, env, found);
        let replica = db
            .replica()
            .and_then(|written| with_relative(at, written, &rel, found))
            .and_then(|written| replica::map(&at.key("replica"), &written, env, found));
        out.push(Found {
            path,
            meta,
            replica,
        });
    }
    out
}

/// Go's `filepath.Walk` from `path` with `FindSQLiteDatabases`' callback, keeping databases.
fn walk(
    path: &Path,
    root: bool,
    recursive: bool,
    pattern: &[u8],
    out: &mut Vec<PathBuf>,
) -> Result<(), DirProblem> {
    let scan = |path: &Path| {
        let path = path.to_owned();
        move |error: io::Error| DirProblem::Scan {
            path,
            kind: error.kind(),
        }
    };
    let meta = std::fs::symlink_metadata(path).map_err(scan(path))?;
    if !meta.is_dir() {
        return candidate(path, pattern, out);
    }
    // Go reads the names before its callback can skip the directory.
    let mut names: Vec<PathBuf> = std::fs::read_dir(path)
        .map_err(scan(path))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_, _>>()
        .map_err(scan(path))?;
    if !root && !recursive {
        return Ok(());
    }
    names.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    for name in names {
        walk(&name, false, recursive, pattern, out)?;
    }
    Ok(())
}

/// A file the walk reached: kept when its name matches and it has SQLite's header.
fn candidate(path: &Path, pattern: &[u8], out: &mut Vec<PathBuf>) -> Result<(), DirProblem> {
    let name = path.file_name().map(|n| n.as_bytes()).unwrap_or_default();
    match pattern::matches(pattern, name) {
        Err(BadPattern) => Err(DirProblem::BadPattern {
            pattern: String::from_utf8_lossy(pattern).into_owned(),
            path: path.to_owned(),
        }),
        Ok(false) => Ok(()),
        Ok(true) => {
            if is_sqlite(path) {
                out.push(path.to_owned());
            }
            Ok(())
        }
    }
}

/// Go's `IsSQLiteDatabase`: one `read` of up to 16 bytes into a zeroed buffer, compared
/// with SQLite's header (an empty read is not a database).
fn is_sqlite(path: &Path) -> bool {
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut header = [0_u8; 16];
    matches!(file.read(&mut header), Ok(n) if n > 0) && &header == HEADER
}

/// Go's `filepath.Rel(root, path)` for a path the walk found under `root`: `.` for the
/// root itself.
fn relative(root: &Path, path: &Path) -> String {
    let rel = path
        .strip_prefix(root)
        .map(|rel| rel.to_string_lossy().into_owned())
        .unwrap_or_default();
    if rel.is_empty() { ".".to_owned() } else { rel }
}

/// Go's per-database meta path; `None` for Go's default beside the database.
fn meta_for(
    at: &KeyPath,
    source: &DirSource,
    db: &DbConfig,
    path: &Path,
    rel: &str,
    env: &dyn Env,
    found: &mut Collected,
) -> Option<PathBuf> {
    let (key, base) = match (source.meta_dir(), db.meta_path()) {
        (Some(meta_dir), _) => ("meta-dir", meta_dir),
        (None, Some(meta_path)) => ("meta-path", meta_path),
        (None, None) => return super::default_meta(path),
    };
    let base = match expand_path(base, env) {
        Ok(base) => base,
        Err(error) => {
            found.problem(&at.key(key), MapProblemKind::Path(error));
            return None;
        }
    };
    Some(if key == "meta-dir" {
        base.join(format!("{rel}-litestream"))
    } else {
        derive_meta(&base, rel)
    })
}

/// Go's `deriveMetaPathForDirectoryEntry`.
fn derive_meta(base: &Path, rel: &str) -> PathBuf {
    let rel = clean(rel);
    if rel == "." || rel.is_empty() {
        return base.to_owned();
    }
    let (dir, file) = rel.rsplit_once('/').unwrap_or(("", rel.as_str()));
    PathBuf::from(clean(&format!(
        "{}/{dir}/.{file}-litestream",
        base.display()
    )))
}

/// Go's `cloneReplicaConfigWithRelativePath`.
fn with_relative(
    at: &KeyPath,
    written: &ReplicaConfig,
    rel: &str,
    found: &mut Collected,
) -> Option<ReplicaConfig> {
    let mut copy = written.clone();
    if rel.is_empty() || rel == "." {
        return Some(copy);
    }
    if let Some(text) = &written.url {
        let Ok(mut url) = Url::parse(text) else {
            refuse(&at.key("replica").key("url"), found, DirProblem::ReplicaUrl);
            return None;
        };
        let rel = utf8_percent_encode(rel, GO_PATH_ESCAPES).to_string();
        let joined = if url.path().is_empty() {
            rel
        } else {
            clean(&format!("{}/{rel}", url.path()))
        };
        url.set_path(&format!("/{}", joined.trim_start_matches('/')));
        copy.url = Some(url.to_string());
        return Some(copy);
    }
    copy.path = Some(match (&written.path, written.kind) {
        (Some(base), ReplicaKind::File | ReplicaKind::S3) if !base.is_empty() => {
            clean(&format!("{base}/{rel}"))
        }
        (Some(_) | None, ReplicaKind::File | ReplicaKind::S3) => rel.to_owned(),
    });
    Some(copy)
}

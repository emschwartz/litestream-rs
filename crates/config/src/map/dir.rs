//! A `dir` entry, scanned once at start (Go's `NewDBsFromDirectoryConfig`,
//! `FindSQLiteDatabases`, `newDBFromDirectoryEntry`; `cmd/litestream/main.go:851-1104`).
//!
//! The walk is Go's `filepath.Walk`: names in lexical order, symbolic links not
//! followed, subdirectories entered only with `recursive`. A file is a database
//! when its name matches `pattern` (Go's `filepath.Match`, on the name alone) and
//! its first 16 bytes are `SQLite format 3\0`. That header read is the one place
//! a database file is opened outside SQLite, and it happens here, before any
//! replicator holds the file (closing a descriptor drops every POSIX lock the
//! process holds on that file).
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
use std::iter::Peekable;
use std::path::{Path, PathBuf};
use std::str::Chars;

use url::Url;

use super::{Found as Collected, MapProblemKind, ReplicaPlan, replica};
use crate::config::{DbConfig, DirSource, ReplicaConfig, ReplicaKind};
use crate::error::KeyPath;
use crate::expand::{Env, clean, expand_path};

/// The header every SQLite database file starts with.
const HEADER: &[u8; 16] = b"SQLite format 3\0";

/// Why a `dir` entry gives no databases.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DirProblem {
    /// The pattern is not a valid `filepath.Match` pattern.
    #[error("syntax error in pattern {pattern:?}")]
    BadPattern {
        /// The pattern.
        pattern: String,
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
    let Some(pattern) = Glob::new(source.pattern()) else {
        let pattern = source.pattern().to_owned();
        refuse(
            &at.key("pattern"),
            found,
            DirProblem::BadPattern { pattern },
        );
        return Vec::new();
    };
    let mut paths = Vec::new();
    if let Err((path, kind)) = walk(&dir, source.recursive(), &pattern, &mut paths) {
        refuse(&at.key("dir"), found, DirProblem::Scan { path, kind });
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
        let meta = meta_for(at, source, db, &rel, env, found);
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

type WalkError = (PathBuf, io::ErrorKind);

/// Go's `filepath.Walk` over `dir`, keeping matching databases.
fn walk(
    dir: &Path,
    recursive: bool,
    pattern: &Glob,
    out: &mut Vec<PathBuf>,
) -> Result<(), WalkError> {
    fn fail(path: &Path) -> impl Fn(io::Error) -> WalkError + '_ {
        move |error| (path.to_owned(), error.kind())
    }
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(fail(dir))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_, _>>()
        .map_err(fail(dir))?;
    names.sort();
    for path in names {
        let meta = std::fs::symlink_metadata(&path).map_err(fail(&path))?;
        if meta.is_dir() {
            if recursive {
                walk(&path, recursive, pattern, out)?;
            }
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if pattern.matches(name) && is_sqlite(&path) {
            out.push(path);
        }
    }
    Ok(())
}

/// Go's `IsSQLiteDatabase`: the first 16 bytes are SQLite's header.
fn is_sqlite(path: &Path) -> bool {
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut header = [0_u8; 16];
    file.read_exact(&mut header).is_ok() && &header == HEADER
}

/// `path` relative to `root`, with `/` separators.
fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .map(|rel| rel.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Go's per-database meta path; `None` for Go's default beside the database.
fn meta_for(
    at: &KeyPath,
    source: &DirSource,
    db: &DbConfig,
    rel: &str,
    env: &dyn Env,
    found: &mut Collected,
) -> Option<PathBuf> {
    let (key, base) = match (source.meta_dir(), db.meta_path()) {
        (Some(meta_dir), _) => ("meta-dir", meta_dir),
        (None, Some(meta_path)) => ("meta-path", meta_path),
        (None, None) => {
            return super::default_meta(&expand_path(source.dir(), env).ok()?.join(rel));
        }
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
        let joined = if url.path().is_empty() {
            rel.to_owned()
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

/// One part of a `filepath.Match` pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Star,
    Any,
    Char(char),
    Class {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
}

/// A compiled `filepath.Match` pattern (Unix: `/` never matches a wildcard).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Glob(Vec<Part>);

impl Glob {
    /// `None` for Go's `ErrBadPattern`.
    pub(crate) fn new(pattern: &str) -> Option<Self> {
        let mut parts = Vec::new();
        let mut chars = pattern.chars().peekable();
        while let Some(c) = chars.next() {
            parts.push(match c {
                '*' => Part::Star,
                '?' => Part::Any,
                '\\' => Part::Char(chars.next()?),
                '[' => class(&mut chars)?,
                other => Part::Char(other),
            });
        }
        Some(Self(parts))
    }

    /// Whether the whole name matches.
    pub(crate) fn matches(&self, name: &str) -> bool {
        let name: Vec<char> = name.chars().collect();
        matches_from(&self.0, &name)
    }
}

/// A `[...]` class after its `[`.
fn class(chars: &mut Peekable<Chars<'_>>) -> Option<Part> {
    let negated = chars.next_if_eq(&'^').is_some();
    let mut ranges = Vec::new();
    loop {
        let lo = match chars.next()? {
            ']' if !ranges.is_empty() => break,
            ']' => return None,
            '\\' => chars.next()?,
            c => c,
        };
        let hi = if chars.next_if_eq(&'-').is_some() {
            match chars.next()? {
                '\\' => chars.next()?,
                ']' => return None,
                c => c,
            }
        } else {
            lo
        };
        if lo > hi {
            return None;
        }
        ranges.push((lo, hi));
    }
    Some(Part::Class { negated, ranges })
}

fn matches_from(parts: &[Part], name: &[char]) -> bool {
    match parts.split_first() {
        None => name.is_empty(),
        Some((Part::Star, rest)) => (0..=name.len())
            .take_while(|&n| name.iter().take(n).all(|&c| c != '/'))
            .any(|n| matches_from(rest, name.get(n..).unwrap_or(&[]))),
        Some((part, rest)) => match name.split_first() {
            Some((&c, tail)) if one(part, c) => matches_from(rest, tail),
            Some(_) | None => false,
        },
    }
}

fn one(part: &Part, c: char) -> bool {
    match part {
        Part::Star => true,
        Part::Any => c != '/',
        Part::Char(want) => *want == c,
        Part::Class { negated, ranges } => {
            c != '/' && ranges.iter().any(|&(lo, hi)| lo <= c && c <= hi) != *negated
        }
    }
}

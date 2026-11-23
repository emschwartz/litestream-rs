//! `dir:` database scanning against Go: generated directory trees and `dir` entries, each
//! read by Go's own `NewDBsFromDirectoryConfig` (the dumper, `lsr_compat::go_dump`) and by
//! `lsr_config::plan`, which must find the same databases, in the same order, with the same
//! meta directories and replicas, or both refuse.
//!
//! The generator covers what decides the answer: names a pattern may or may not match
//! (classes, escapes, dots, spaces, non-ASCII), the 16-byte header (whole, exact, one byte
//! short, absent), nesting with and without `recursive`, and every kind of symbolic link
//! (to a database, to a directory, dangling, and the `dir` itself as a link). FIFOs and
//! unreadable files are left out: both sides block on a FIFO's open, and the tests run as root.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use lsr_compat::{Differential, Dump, DumpEnv, go_dump, go_dump_binary, rust_dump};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;

const NAMES: &[&str] = &[
    "a.db",
    "b.db",
    "db",
    "x.sqlite",
    "a.db-wal",
    "A.DB",
    "-x.db",
    "[x].db",
    "a b.db",
    ".h.db",
    "\u{e9}.db",
    "sub",
    "sub.db",
    "z",
    "*.db",
    "a\\b.db",
    "a%41.db",
    "q?.db",
    "h#.db",
    "c!.db",
];

const PATTERNS: &[&str] = &[
    "*.db",
    "*",
    "*.sqlite",
    "?.db",
    "[a-c]*",
    "[^a]*",
    "[!a]*",
    "[-a]*",
    "[a-]*",
    "\\*.db",
    "[]a]*",
    "*[",
    "x\\",
    "[\\-a]*",
    "[a-c-e]*",
    "*.[dD][bB]",
    "",
    "[^]*",
    "a*b*",
    "[z-a]*",
    "\\[x].db",
    ".*",
    "*\\",
];

#[derive(Clone, Copy, Debug)]
enum Kind {
    /// A file with SQLite's header and more after it.
    Db,
    /// Exactly the 16 header bytes.
    Header16,
    /// The header without its final NUL.
    Header15,
    Other,
    Empty,
    Dir,
    /// A link to a database outside the tree.
    LinkDb,
    /// A link to a directory outside the tree holding a database.
    LinkDir,
    Dangling,
}

#[derive(Clone, Copy, Debug)]
enum DirSpelling {
    Absolute,
    TrailingSlash,
    Relative,
    /// A symbolic link to the tree's root.
    Link,
    /// A database file, not a directory.
    File,
    Missing,
}

#[derive(Clone, Copy, Debug)]
enum Replica {
    FileUrl,
    FilePath,
    S3Url,
    S3Bucket,
}

#[derive(Clone, Copy, Debug)]
enum Meta {
    Beside,
    Dir,
    Path,
}

#[derive(Clone, Debug)]
struct Case {
    entries: Vec<(Vec<usize>, Kind)>,
    dir: DirSpelling,
    pattern: String,
    recursive: bool,
    replica: Replica,
    meta: Meta,
}

fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        4 => Just(Kind::Db),
        1 => Just(Kind::Header16),
        1 => Just(Kind::Header15),
        1 => Just(Kind::Other),
        1 => Just(Kind::Empty),
        2 => Just(Kind::Dir),
        1 => Just(Kind::LinkDb),
        1 => Just(Kind::LinkDir),
        1 => Just(Kind::Dangling),
    ]
}

fn pattern() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => proptest::sample::select(PATTERNS).prop_map(str::to_owned),
        1 => "[-*?\\[\\]^!\\\\a.bdz]{1,6}",
    ]
}

fn case() -> impl Strategy<Value = Case> {
    let entry = (prop::collection::vec(0..NAMES.len(), 1..=3), kind());
    (
        prop::collection::vec(entry, 0..12),
        prop_oneof![
            6 => Just(DirSpelling::Absolute),
            1 => Just(DirSpelling::TrailingSlash),
            1 => Just(DirSpelling::Relative),
            1 => Just(DirSpelling::Link),
            1 => Just(DirSpelling::File),
            1 => Just(DirSpelling::Missing),
        ],
        pattern(),
        any::<bool>(),
        prop_oneof![
            Just(Replica::FileUrl),
            Just(Replica::FilePath),
            Just(Replica::S3Url),
            Just(Replica::S3Bucket),
        ],
        prop_oneof![Just(Meta::Beside), Just(Meta::Dir), Just(Meta::Path)],
    )
        .prop_map(|(entries, dir, pattern, recursive, replica, meta)| Case {
            entries,
            dir,
            pattern,
            recursive,
            replica,
            meta,
        })
}

const HEADER: &[u8; 16] = b"SQLite format 3\0";

fn write_db(path: &Path) {
    let mut bytes = HEADER.to_vec();
    bytes.resize(512, 0);
    std::fs::write(path, bytes).unwrap();
}

/// Builds the tree under `base/root`; returns the path of every node made. A node whose
/// parent is not a directory is skipped, as is a name already taken.
fn build(base: &Path, case: &Case) -> PathBuf {
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let outside = base.join("outside");
    std::fs::create_dir_all(outside.join("d")).unwrap();
    write_db(&outside.join("o.db"));
    write_db(&outside.join("d").join("in.db"));
    for (names, kind) in &case.entries {
        let Some((&last, parents)) = names.split_last() else {
            continue;
        };
        let mut path = root.clone();
        let mut ok = true;
        for &name in parents {
            path.push(NAMES[name]);
            match std::fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_dir() => {}
                Ok(_) => {
                    ok = false;
                    break;
                }
                Err(_) => std::fs::create_dir(&path).unwrap(),
            }
        }
        path.push(NAMES[last]);
        if !ok || std::fs::symlink_metadata(&path).is_ok() {
            continue;
        }
        match kind {
            Kind::Db => write_db(&path),
            Kind::Header16 => std::fs::write(&path, HEADER).unwrap(),
            Kind::Header15 => std::fs::write(&path, &HEADER[..15]).unwrap(),
            Kind::Other => std::fs::write(&path, b"not a database at all, longer").unwrap(),
            Kind::Empty => std::fs::write(&path, b"").unwrap(),
            Kind::Dir => std::fs::create_dir(&path).unwrap(),
            Kind::LinkDb => symlink(outside.join("o.db"), &path).unwrap(),
            Kind::LinkDir => symlink(outside.join("d"), &path).unwrap(),
            Kind::Dangling => symlink(base.join("nowhere"), &path).unwrap(),
        }
    }
    root
}

/// A YAML double-quoted scalar.
fn quoted(text: &str) -> String {
    let mut out = String::from("\"");
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn config_text(base: &Path, root: &Path, case: &Case) -> String {
    let dir = match case.dir {
        DirSpelling::Absolute => root.display().to_string(),
        DirSpelling::TrailingSlash => format!("{}/", root.display()),
        DirSpelling::Relative => "root".to_owned(),
        DirSpelling::Link => {
            let link = base.join("link");
            symlink(root, &link).unwrap();
            link.display().to_string()
        }
        DirSpelling::File => {
            let file = base.join("lone.db");
            write_db(&file);
            file.display().to_string()
        }
        DirSpelling::Missing => base.join("missing").display().to_string(),
    };
    let mut text = String::from("dbs:\n");
    writeln!(text, "  - dir: {}", quoted(&dir)).unwrap();
    writeln!(text, "    pattern: {}", quoted(&case.pattern)).unwrap();
    writeln!(text, "    recursive: {}", case.recursive).unwrap();
    match case.meta {
        Meta::Beside => {}
        Meta::Dir => writeln!(
            text,
            "    meta-dir: {}",
            quoted(&base.join("meta").display().to_string())
        )
        .unwrap(),
        Meta::Path => writeln!(
            text,
            "    meta-path: {}",
            quoted(&base.join("mp").display().to_string())
        )
        .unwrap(),
    }
    text.push_str("    replica:\n");
    let replica = base.join("replica").display().to_string();
    match case.replica {
        Replica::FileUrl => {
            writeln!(text, "      url: {}", quoted(&format!("file://{replica}"))).unwrap()
        }
        Replica::FilePath => {
            writeln!(text, "      type: file\n      path: {}", quoted(&replica)).unwrap()
        }
        Replica::S3Url => text.push_str("      url: s3://bucket/some/prefix\n"),
        // Always with a prefix: an empty one is refused on purpose, `dir` or not, where Go
        // writes keys with a leading slash (CFG-18, D40).
        Replica::S3Bucket => text.push_str(
            "      type: s3\n      bucket: bucket\n      path: pre\n      region: us-east-1\n",
        ),
    }
    text
}

fn listing(root: &Path) -> String {
    fn walk(dir: &Path, depth: usize, out: &mut String) {
        let Ok(read) = std::fs::read_dir(dir) else {
            return;
        };
        let mut names: Vec<_> = read.map(|e| e.unwrap().path()).collect();
        names.sort();
        for path in names {
            let meta = std::fs::symlink_metadata(&path).unwrap();
            let name = path.file_name().unwrap().to_string_lossy();
            let what = if meta.is_dir() {
                "dir".to_owned()
            } else if meta.file_type().is_symlink() {
                format!("-> {}", std::fs::read_link(&path).unwrap().display())
            } else {
                format!("{} bytes", meta.len())
            };
            writeln!(out, "{}{name:?} {what}", "  ".repeat(depth)).unwrap();
            if meta.is_dir() {
                walk(&path, depth.saturating_add(1), out);
            }
        }
    }
    let mut out = String::new();
    walk(root, 0, &mut out);
    out
}

/// Both answers for one case, with the config and the tree for the report.
fn answers(go: &Path, case: &Case) -> (Dump, Dump, String) {
    let base = tempfile::tempdir().unwrap();
    let base = base.path().canonicalize().unwrap();
    let root = build(&base, case);
    let text = config_text(&base, &root, case);
    let config = base.join("litestream.yml");
    std::fs::write(&config, &text).unwrap();
    let env = DumpEnv {
        vars: BTreeMap::new(),
        home: base.join("home"),
        cwd: base.clone(),
        credentials: false,
    };
    let go_out = go_dump(go, &config, &env).unwrap();
    let rust_out = rust_dump(&config, &env);
    let report = format!("config:\n{text}\ntree:\n{}", listing(&root));
    (go_out, rust_out, report)
}

fn compare(go: &Path, case: &Case) -> Result<(), TestCaseError> {
    let (go_out, rust_out, report) = answers(go, case);
    prop_assert!(
        go_out.agrees_with(&rust_out),
        "Go and Rust disagree\n{report}\nGo:   {go_out:?}\nRust: {rust_out:?}"
    );
    Ok(())
}

/// The cases the generator might not reach often, run every time.
fn pinned_cases() -> Vec<Case> {
    let one = |entries: Vec<(Vec<usize>, Kind)>, dir, pattern: &str, recursive| Case {
        entries,
        dir,
        pattern: pattern.to_owned(),
        recursive,
        replica: Replica::FileUrl,
        meta: Meta::Beside,
    };
    vec![
        one(vec![(vec![0], Kind::Db)], DirSpelling::Link, "*.db", false),
        one(vec![(vec![0], Kind::Db)], DirSpelling::File, "*.db", false),
        one(
            vec![(vec![0], Kind::Header15)],
            DirSpelling::Absolute,
            "*.db",
            false,
        ),
        one(
            vec![(vec![6], Kind::Db)],
            DirSpelling::Absolute,
            "[-a]*",
            false,
        ),
        one(
            vec![(vec![11, 0], Kind::Db)],
            DirSpelling::Absolute,
            "*.db",
            true,
        ),
        one(
            vec![(vec![11], Kind::LinkDir)],
            DirSpelling::Absolute,
            "*",
            true,
        ),
    ]
}

/// The differential itself. 512 generated cases plus the pinned ones; a disagreement prints
/// the config, the tree and both answers, shrunk.
#[test]
#[ignore = "runs the pinned Go dumper; just compat"]
fn go_and_rust_scan_the_same_databases() {
    let go = must(Resource::GoBuilds, || {
        go_dump_binary().map_err(|e| e.to_string())
    });
    let mut failures = Vec::new();
    for case in pinned_cases() {
        if let Err(error) = compare(&go, &case) {
            failures.push(error.to_string());
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    let run = Differential::new("dir entries");
    let compared = run.free(512, &case(), |case| {
        compare(&go, &case).map(|()| "compared".to_owned())
    });
    if let Err(error) = compared {
        panic!("{error}");
    }
}

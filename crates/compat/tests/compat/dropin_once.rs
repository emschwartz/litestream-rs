//! `replicate -once -force-snapshot` against Go: the pinned `litestream` and `lsr`, each on a
//! database of its own with the same rows and a file replica, in short explicit scenarios.
//!
//! After `-once -force-snapshot`, each replica holds exactly one level-9 file, starting at
//! transaction 1 and ending at the replica's newest transaction, and Go restores each
//! replica to the database's rows. Run again with nothing new, each writes the same file
//! again: still one level-9 file, under the same name. Go restoring lsr's replica is the
//! strict-writer half.

use std::path::{Path, PathBuf};
use std::process::Command;

use lsr_compat::{GoVersion, go_binary, lsr_binary};
use lsr_core::needs::{Resource, must};
use lsr_core::parse_file_name;

struct Side {
    dir: tempfile::TempDir,
    app: rusqlite::Connection,
}

impl Side {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let app = rusqlite::Connection::open(dir.path().join("app.db")).unwrap();
        app.pragma_update(None, "journal_mode", "wal").unwrap();
        app.execute_batch("CREATE TABLE t (x BLOB);").unwrap();
        let config = format!(
            "dbs:\n  - path: {}\n    replica:\n      url: file://{}\n",
            dir.path().join("app.db").display(),
            dir.path().join("replica").display(),
        );
        std::fs::write(dir.path().join("litestream.yml"), config).unwrap();
        Self { dir, app }
    }

    fn write(&self, rows: u32) {
        for _ in 0..rows {
            self.app
                .execute("INSERT INTO t VALUES (randomblob(3000))", [])
                .unwrap();
        }
    }

    fn replica(&self) -> PathBuf {
        self.dir.path().join("replica")
    }

    /// `replicate -once` with `flags`, by `binary`: its exit code and standard error.
    fn once(&self, binary: &Path, flags: &[&str]) -> (Option<i32>, String) {
        let output = Command::new(binary)
            .arg("replicate")
            .arg("-once")
            .args(flags)
            .arg("-config")
            .arg(self.dir.path().join("litestream.yml"))
            .env_clear()
            .env("HOME", self.dir.path())
            .env("PATH", "/usr/bin:/bin")
            .output()
            .unwrap();
        (
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    fn level(&self, level: u8) -> Vec<String> {
        let dir = self.replica().join("ltx").join(level.to_string());
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .map(|d| {
                d.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// The newest transaction on the replica at levels 0 to 8.
    fn newest(&self) -> u64 {
        (0..9_u8)
            .flat_map(|level| self.level(level))
            .filter_map(|name| parse_file_name(&name).ok())
            .map(|range| range.max().get())
            .max()
            .unwrap()
    }

    fn go_restores(&self, go: &Path) -> Vec<Vec<u8>> {
        let out = self
            .dir
            .path()
            .join(format!("restored-{}.db", self.level(9).len()));
        drop(std::fs::remove_file(&out));
        let output = Command::new(go)
            .arg("restore")
            .arg("-o")
            .arg(&out)
            .arg(format!("file://{}", self.replica().display()))
            .env_clear()
            .env("HOME", self.dir.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        rows(&rusqlite::Connection::open(&out).unwrap())
    }

    fn rows(&self) -> Vec<Vec<u8>> {
        rows(&self.app)
    }
}

fn rows(db: &rusqlite::Connection) -> Vec<Vec<u8>> {
    let mut stmt = db.prepare("SELECT x FROM t ORDER BY rowid").unwrap();
    stmt.query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn binaries() -> (PathBuf, PathBuf) {
    let go = must(Resource::GoBuilds, || {
        go_binary(GoVersion::Head).map_err(|e| e.to_string())
    });
    (go, lsr_binary().unwrap())
}

/// The one level-9 file a replica must hold after `-force-snapshot`: from 1 to its newest.
fn the_snapshot(side: &Side) -> String {
    let snapshots = side.level(9);
    assert_eq!(snapshots.len(), 1, "{snapshots:?}");
    let range = parse_file_name(&snapshots[0]).unwrap();
    assert_eq!(range.min().get(), 1, "{snapshots:?}");
    assert_eq!(range.max().get(), side.newest(), "{snapshots:?}");
    snapshots[0].clone()
}

/// Both write one level-9 file of the whole database, and Go restores each replica to the
/// database's rows.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_lsr_end_once_with_one_level_nine_snapshot() {
    let (go, lsr) = binaries();
    for binary in [&go, &lsr] {
        let side = Side::new();
        side.write(20);
        let (code, stderr) = side.once(binary, &["-force-snapshot"]);
        assert_eq!(code, Some(0), "{}: {stderr}", binary.display());
        the_snapshot(&side);
        assert_eq!(side.go_restores(&go), side.rows(), "{}", binary.display());
    }
}

/// Run again with nothing new, both write the snapshot again under the same name, where the
/// daemon's planner would call it idle.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn a_second_run_at_the_same_position_writes_the_same_snapshot_on_both() {
    let (go, lsr) = binaries();
    for binary in [&go, &lsr] {
        let side = Side::new();
        side.write(5);
        assert_eq!(side.once(binary, &["-force-snapshot"]).0, Some(0));
        let first = the_snapshot(&side);
        let (code, stderr) = side.once(binary, &["-force-snapshot"]);
        assert_eq!(code, Some(0), "{}: {stderr}", binary.display());
        assert_eq!(the_snapshot(&side), first, "{}", binary.display());
        assert_eq!(side.go_restores(&go), side.rows(), "{}", binary.display());
    }
}

/// `-once` alone writes no level-9 file on either side.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn once_alone_writes_no_snapshot_on_either_side() {
    let (go, lsr) = binaries();
    for binary in [&go, &lsr] {
        let side = Side::new();
        side.write(5);
        assert_eq!(side.once(binary, &[]).0, Some(0));
        assert!(side.level(9).is_empty(), "{}", binary.display());
    }
}

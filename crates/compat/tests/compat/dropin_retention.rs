//! `replicate -once -enforce-retention` against Go, in short explicit scenarios.
//!
//! The pinned Go build writes three level-9 snapshots into a file replica (three runs of
//! `-once -force-snapshot` with writes between), the snapshots are aged by their files'
//! modification times (both file clients read a file's created-at time there), and Go gets a
//! copy of the directory. Each side then runs `replicate -once -enforce-retention` with
//! snapshots kept a day, the level-9 files left are compared, and Go restores each side's
//! replica to the database's rows.
//!
//! - **Aged against a recent snapshot:** the newest snapshot is an hour old and the others are
//!   days old. Both delete the old two and keep the newest.
//! - **The departure (docs/decisions/D167.md):** every snapshot is days old, an idle replica.
//!   Go measures from now and deletes all but the newest. litestream-rs measures from the
//!   replica's newest created-at time and keeps the snapshot within a day of it, exit 0.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use lsr_compat::{GoVersion, go_binary, lsr_binary};
use lsr_core::needs::{Resource, must};
use lsr_core::parse_file_name;

const HOUR: Duration = Duration::from_secs(60 * 60);

fn config(dir: &Path) -> String {
    format!(
        "snapshot:\n  interval: 24h\n  retention: 24h\ndbs:\n  - path: {}\n    replica:\n      url: file://{}\n",
        dir.join("app.db").display(),
        dir.join("replica").display(),
    )
}

fn once(binary: &Path, dir: &Path, flags: &[&str]) -> (Option<i32>, String) {
    std::fs::write(dir.join("litestream.yml"), config(dir)).unwrap();
    let output = Command::new(binary)
        .arg("replicate")
        .arg("-once")
        .args(flags)
        .arg("-config")
        .arg(dir.join("litestream.yml"))
        .env_clear()
        .env("HOME", dir)
        .env("PATH", "/usr/bin:/bin")
        .output()
        .unwrap();
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn snapshots(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join("replica").join("ltx").join("9"))
        .map(|d| d.map(|e| e.unwrap().path()).collect())
        .unwrap_or_default();
    files.sort();
    files
}

fn names(dir: &Path) -> Vec<String> {
    snapshots(dir)
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect()
}

/// Every level of `dir`'s replica with each file's age, for a failure message: which file
/// carries a fresh time decides what retention measures from.
#[expect(
    clippy::disallowed_methods,
    reason = "a failure message compares file times with the wall clock the files were stamped by"
)]
fn listing(dir: &Path) -> String {
    let now = SystemTime::now();
    let mut out = String::new();
    for level in ["0", "1", "2", "3", "9"] {
        let path = dir.join("replica").join("ltx").join(level);
        let mut files: Vec<PathBuf> = std::fs::read_dir(path)
            .map(|d| d.map(|e| e.unwrap().path()).collect())
            .unwrap_or_default();
        files.sort();
        for file in files {
            let age = now
                .duration_since(std::fs::metadata(&file).unwrap().modified().unwrap())
                .unwrap_or_default();
            out.push_str(&format!(
                "  L{level} {} {}s old\n",
                file.file_name().unwrap().to_string_lossy(),
                age.as_secs()
            ));
        }
    }
    out
}

fn rows(db: &Path) -> Vec<Vec<u8>> {
    let conn = rusqlite::Connection::open(db).unwrap();
    let mut stmt = conn.prepare("SELECT x FROM t ORDER BY rowid").unwrap();
    stmt.query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// A directory holding a database and a replica with three Go snapshots whose files are
/// `ages` old, oldest first. Go's runs each leave a frame of their own behind, which a plain
/// `lsr replicate -once` then syncs, so the replica holds everything committed and level 0
/// last changed when the newest snapshot was taken: a replica that has seen no write since.
fn replica_with_snapshots(go: &Path, lsr: &Path, ages: [Duration; 3]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let app = rusqlite::Connection::open(dir.path().join("app.db")).unwrap();
    app.pragma_update(None, "journal_mode", "wal").unwrap();
    app.execute_batch("CREATE TABLE t (x BLOB);").unwrap();
    for _ in 0..3 {
        for _ in 0..5 {
            app.execute("INSERT INTO t VALUES (randomblob(3000))", [])
                .unwrap();
        }
        let (code, stderr) = once(go, dir.path(), &["-force-snapshot"]);
        assert_eq!(code, Some(0), "{stderr}");
    }
    drop(app);
    let (code, stderr) = once(lsr, dir.path(), &[]);
    assert_eq!(code, Some(0), "{stderr}");
    let files = snapshots(dir.path());
    assert_eq!(files.len(), 3, "{files:?}");
    // Now, as the file system has it: the newest snapshot was just written.
    let now = std::fs::metadata(&files[2]).unwrap().modified().unwrap();
    for (file, age) in files.iter().zip(ages) {
        set_age(file, now, age);
    }
    let level0 = dir.path().join("replica").join("ltx").join("0");
    for entry in std::fs::read_dir(level0).unwrap() {
        set_age(&entry.unwrap().path(), now, ages[2]);
    }
    dir
}

fn set_age(file: &Path, now: SystemTime, age: Duration) {
    std::fs::File::options()
        .write(true)
        .open(file)
        .unwrap()
        .set_modified(now.checked_sub(age).unwrap())
        .unwrap();
}

/// A copy of `from`, with every file's modification time kept.
fn copy(from: &Path) -> tempfile::TempDir {
    let to = tempfile::tempdir().unwrap();
    copy_dir(from, to.path());
    // Paths in the config name the copy.
    std::fs::write(to.path().join("litestream.yml"), config(to.path())).unwrap();
    to
}

fn copy_dir(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
            let modified = entry.metadata().unwrap().modified().unwrap();
            std::fs::File::options()
                .write(true)
                .open(&target)
                .unwrap()
                .set_modified(modified)
                .unwrap();
        }
    }
}

fn go_restores(go: &Path, dir: &Path) -> Vec<Vec<u8>> {
    let out = dir.join("restored.db");
    let output = Command::new(go)
        .arg("restore")
        .arg("-o")
        .arg(&out)
        .arg(format!("file://{}", dir.join("replica").display()))
        .env_clear()
        .env("HOME", dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    rows(&out)
}

/// Each side's level-9 file names after `-once -enforce-retention`, Go's first: Go on a copy
/// of `fixture`, litestream-rs on `fixture` itself (a copied wal-index is not the one SQLite
/// left, and reading it can stage a file the replica never needed). Each side's replica
/// restores under Go to the database's rows.
fn after_retention(go: &Path, lsr: &Path, fixture: &Path) -> [Vec<String>; 2] {
    let go_side = copy(fixture);
    let sides = [(go, go_side.path()), (lsr, fixture)];
    let left = sides.map(|(binary, dir)| {
        let (code, stderr) = once(binary, dir, &["-enforce-retention"]);
        assert_eq!(code, Some(0), "{}: {stderr}", binary.display());
        names(dir)
    });
    // Read only now: closing the last connection to a database checkpoints its WAL, a change
    // the next run would stage.
    let want = rows(&go_side.path().join("app.db"));
    for (binary, dir) in sides {
        assert_eq!(go_restores(go, dir), want, "{}", binary.display());
    }
    left
}

fn binaries() -> (PathBuf, PathBuf) {
    let go = must(Resource::GoBuilds, || {
        go_binary(GoVersion::Head).map_err(|e| e.to_string())
    });
    (go, lsr_binary().unwrap())
}

/// With the newest snapshot an hour old, both delete the two days-old ones and keep it.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_lsr_delete_the_same_aged_snapshots() {
    let (go, lsr) = binaries();
    let fixture = replica_with_snapshots(&go, &lsr, [72 * HOUR, 48 * HOUR, HOUR]);
    let all = names(fixture.path());
    let [go_left, lsr_left] = after_retention(&go, &lsr, fixture.path());
    let listed = listing(fixture.path());
    assert_eq!(go_left, all[2..].to_vec(), "after retention:\n{listed}");
    assert_eq!(lsr_left, go_left, "after retention:\n{listed}");
}

/// The departure, stated as a test: on an idle replica whose snapshots are all days old, Go
/// keeps only the newest; litestream-rs, measuring from the newest snapshot's time, also keeps
/// the one within a day of it.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn on_an_idle_replica_lsr_keeps_a_snapshot_go_deletes() {
    let (go, lsr) = binaries();
    let fixture = replica_with_snapshots(&go, &lsr, [120 * HOUR, 90 * HOUR, 72 * HOUR]);
    let all = names(fixture.path());
    let [go_left, lsr_left] = after_retention(&go, &lsr, fixture.path());
    let listed = listing(fixture.path());
    assert_eq!(
        go_left,
        all[2..].to_vec(),
        "Go keeps only the newest; lsr replica after retention:\n{listed}"
    );
    assert_eq!(
        lsr_left,
        all[1..].to_vec(),
        "lsr keeps the newest two; lsr replica after retention:\n{listed}"
    );
    for name in &lsr_left {
        assert_eq!(parse_file_name(name).unwrap().min().get(), 1);
    }
}

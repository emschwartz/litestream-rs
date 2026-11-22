//! `lsr replicate` in a crash loop: many short boots, each ended by SIGKILL, as an OOM kill
//! every few seconds would end them. Whatever a boot leaves for the next must be bounded by
//! the replica's state, never grow with the number of boots: no snapshot without a change,
//! no growth of the replica or of staging, and no scratch file a killed upload left behind.
//! A kill lands wherever it lands, so the scratch files a killed upload leaves are planted
//! before each boot, named as `lsr_store_fs` names them, at the size of the database.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use std::path::{Path, PathBuf};

use rusqlite::Connection;
use tempfile::TempDir;

use crate::common::{Daemon, database, lsr, restores_to, write};

/// Short boots after the first.
const BOOTS: u32 = 8;

/// A pid no real process here has, so a planted name is never this run's own.
const PLANTED_PID: &str = "4000000000";

/// Files under `dir` (recursively) whose name `pick` accepts, with their total size.
fn files(dir: &Path, pick: &dyn Fn(&str) -> bool) -> (Vec<PathBuf>, u64) {
    let mut found = Vec::new();
    let mut bytes = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(at) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&at) else {
            continue;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                stack.push(entry.path());
            } else if pick(&entry.file_name().to_string_lossy()) {
                bytes += meta.len();
                found.push(entry.path());
            }
        }
    }
    (found, bytes)
}

/// The store's own scratch form, `<key>.ltx.<pid>-<n>.tmp`.
fn own_scratch(name: &str) -> bool {
    name.strip_suffix(".tmp")
        .and_then(|stem| stem.split_once(".ltx."))
        .is_some_and(|(_, unique)| unique.contains('-'))
}

fn objects(name: &str) -> bool {
    name.ends_with(".ltx")
}

/// What a killed upload leaves at `level` under `root`: a scratch file of `size` bytes.
fn plant(root: &Path, level: u8, boot: u32, size: u64) -> PathBuf {
    let dir = root.join("ltx").join(level.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!(
        "0000000000000001-0000000000000001.ltx.{PLANTED_PID}-{boot}.tmp"
    ));
    std::fs::write(&path, vec![0x5a; usize::try_from(size).unwrap()]).unwrap();
    path
}

fn replicate(config: &Path) -> Daemon {
    let mut command = lsr();
    command
        .arg("replicate")
        .args(["-config", config.to_str().unwrap(), "-log-level", "debug"]);
    Daemon::spawn(command)
}

/// One database, its staging directory and its file replica.
struct World {
    dir: TempDir,
    db: PathBuf,
    replica: PathBuf,
    staging: PathBuf,
    config: PathBuf,
    /// The database's size as SQLite sees it (most of it is still in the WAL).
    size: u64,
}

impl World {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("app.db");
        let replica = dir.path().join("replica");
        let staging = dir.path().join(".app.db-litestream");
        let config = dir.path().join("litestream.yml");
        let text = format!(
            "dbs:\n  - path: {}\n    replica:\n      path: {}\n",
            db.display(),
            replica.display()
        );
        std::fs::write(&config, text).unwrap();
        let app = database(&db);
        write(&app, 40);
        let pragma = |name: &str| -> i64 {
            app.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))
                .unwrap()
        };
        let size = u64::try_from(pragma("page_count") * pragma("page_size")).unwrap();
        Self {
            dir,
            db,
            replica,
            staging,
            config,
            size,
        }
    }

    /// `BOOTS` boots, each killed once it has recovered its position, each after the
    /// leftovers of a killed upload were planted; every line about a snapshot they logged.
    fn short_boots(&self) -> Vec<String> {
        let mut snapshots = Vec::new();
        for boot in 1..=BOOTS {
            let planted = [
                plant(&self.staging, 0, boot, self.size),
                plant(&self.replica, 0, boot, self.size),
                plant(&self.replica, 9, boot, self.size),
            ];
            let mut daemon = replicate(&self.config);
            daemon.wait_for(&["msg=\"replica position recovered"]);
            daemon.signal("KILL");
            let (_status, lines) = daemon.finish();
            snapshots.extend(
                lines
                    .into_iter()
                    .filter(|l| l.contains("snapshot required")),
            );
            for path in &planted {
                assert!(!path.exists(), "boot {boot} left {}", path.display());
            }
        }
        snapshots
    }
}

#[test]
fn a_crash_loop_of_short_boots_with_no_change_leaves_nothing_growing() {
    let world = World::new();
    let (replica, staging, size) = (&world.replica, &world.staging, world.size);
    // The first boot snapshots and uploads it; then it is killed.
    let mut daemon = replicate(&world.config);
    daemon.wait_for(&["msg=uploaded", "Put("]);
    daemon.signal("KILL");
    drop(daemon.finish());
    // Go's scratch name and a stranger's file are not this store's: they stay.
    let level0 = replica.join("ltx").join("0");
    let foreign = [
        level0.join("0000000000000001-0000000000000001.ltx.tmp"),
        level0.join("notes.tmp"),
    ];
    for path in &foreign {
        std::fs::write(path, b"not ours").unwrap();
    }
    let snapshots = world.short_boots();
    assert_eq!(
        snapshots,
        Vec::<String>::new(),
        "snapshots with nothing changed"
    );

    // A last boot that drains cleanly: nothing of this store's scratch form is left.
    let mut daemon = replicate(&world.config);
    daemon.wait_for(&["msg=\"replica position recovered"]);
    daemon.signal("TERM");
    let (status, lines) = daemon.finish();
    assert_eq!(status.code(), Some(0), "{lines:#?}");
    for root in [replica, staging] {
        let (left, _) = files(root, &own_scratch);
        assert_eq!(
            left,
            Vec::<PathBuf>::new(),
            "scratch under {}",
            root.display()
        );
    }
    for path in &foreign {
        assert!(
            path.exists(),
            "removed a file not its own: {}",
            path.display()
        );
    }
    // A copy per level the default schedule fills (0 to 3 and the snapshot level), and slack.
    let (_, replica_bytes) = files(replica, &objects);
    assert!(
        replica_bytes <= 6 * size,
        "replica {replica_bytes} bytes, database {size}"
    );
    let (_, staged_bytes) = files(staging, &objects);
    assert!(
        staged_bytes <= 2 * size,
        "staging {staged_bytes} bytes, database {size}"
    );
    restores_to(
        world.dir.path(),
        replica,
        &Connection::open(&world.db).unwrap(),
    );
}

/// Boots that each die once they have uploaded something, with commits between them and,
/// every third boot, a WAL restart while no engine runs (the application's checkpoint): what
/// a process killed every few seconds under a writing application sees. Each restart costs one
/// snapshot (nothing proves the restarted generation lost nothing), and nothing else may: a
/// boot that continues takes none. A last process that lives long enough to compact and prune
/// leaves staging within a few copies of the database, the replica within a copy per
/// snapshot and level, and no scratch file; the restore is the database.
#[test]
fn a_crash_loop_with_writes_and_wal_restarts_stays_bounded() {
    let world = World::new();
    let app = database(&world.db);
    let mut restarts = 0;
    let mut snapshots = Vec::new();
    for boot in 1..=BOOTS {
        if boot % 3 == 0 {
            app.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
                .unwrap();
            restarts += 1;
        }
        write(&app, 30);
        let planted = [
            plant(&world.staging, 0, boot, world.size),
            plant(&world.replica, 0, boot, world.size),
        ];
        let mut daemon = replicate(&world.config);
        daemon.wait_for(&["msg=uploaded", "Put("]);
        daemon.signal("KILL");
        let (_status, lines) = daemon.finish();
        snapshots.extend(
            lines
                .into_iter()
                .filter(|l| l.contains("snapshot required")),
        );
        for path in &planted {
            assert!(!path.exists(), "boot {boot} left {}", path.display());
        }
    }
    // The first boot's first sync, and one per restart.
    assert!(
        snapshots.len() <= 1 + restarts,
        "{} snapshots for {restarts} restarts: {snapshots:#?}",
        snapshots.len()
    );
    let mut daemon = replicate(&world.config);
    daemon.wait_for(&["msg=maintenance", "Compacted("]);
    daemon.wait_for(&["msg=maintenance", "Deleted("]);
    daemon.signal("TERM");
    let (status, lines) = daemon.finish();
    assert_eq!(status.code(), Some(0), "{lines:#?}");
    let size = u64::try_from(
        app.query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))
            .unwrap()
            * app
                .query_row("PRAGMA page_size", [], |r| r.get::<_, i64>(0))
                .unwrap(),
    )
    .unwrap();
    for root in [&world.replica, &world.staging] {
        let (left, _) = files(root, &own_scratch);
        assert_eq!(
            left,
            Vec::<PathBuf>::new(),
            "scratch under {}",
            root.display()
        );
    }
    let (_, staged_bytes) = files(&world.staging, &objects);
    assert!(
        staged_bytes <= 3 * size,
        "staging {staged_bytes} bytes, database {size}"
    );
    let copies = u64::try_from(snapshots.len()).unwrap() + 5;
    let (_, replica_bytes) = files(&world.replica, &objects);
    assert!(
        replica_bytes <= copies * size,
        "replica {replica_bytes} bytes, database {size}, {copies} copies"
    );
    restores_to(world.dir.path(), &world.replica, &app);
}

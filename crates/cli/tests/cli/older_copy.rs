//! An older copy of the database put on a new host over the replica of the newer one, through
//! the built `lsr`: host A replicates, a file backup is taken, A commits more and replicates,
//! A is lost, and the backup is copied to host B, which starts `lsr` on the same replica with
//! a working folder of its own (`docs/decisions/D214.md`). The replica's newest state must not
//! go back to the backup's: the start stops and asks. The ordinary history of
//! `ahead_of_replica.rs` (the working folder lost while the database is newer) still carries on.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use lsr_replica::{DynStore, restore};
use lsr_store_fs::FsStore;
use rusqlite::{Connection, OptionalExtension};
use tempfile::TempDir;

use crate::common::{Daemon, database, lsr, rows, write};

/// How the backup reaches host B.
#[derive(Clone, Copy)]
enum Copy {
    /// Plain `cp`: the copy's modified time is now.
    Plain,
    /// `cp -p`: the copy keeps the backup's modified time.
    Preserving,
}

fn replicate(config: &Path) -> Daemon {
    let mut command = lsr();
    command
        .arg("replicate")
        .args(["-config", config.to_str().unwrap(), "-log-level", "debug"]);
    Daemon::spawn(command)
}

fn config(dir: &Path, db: &Path, replica: &Path, name: &str, interval: &str) -> PathBuf {
    let path = dir.join(name);
    let text = format!(
        "dbs:\n  - path: {}\n    checkpoint-interval: {interval}\n    replica:\n      path: {}\n",
        db.display(),
        replica.display()
    );
    std::fs::write(&path, text).unwrap();
    path
}

/// Reads lines until one holds every one of `parts`, past every line seen so far.
fn next_with(daemon: &mut Daemon, parts: &[&str]) -> String {
    loop {
        let line = daemon.next_line(parts);
        if parts.iter().all(|part| line.contains(part)) {
            return line;
        }
    }
}

/// The replicator's counter in `conn`'s database: 0 before its first bump.
fn counter(conn: &Connection) -> i64 {
    conn.query_row("SELECT seq FROM _litestream_seq WHERE id = 1", [], |row| {
        row.get(0)
    })
    .optional()
    .unwrap()
    .unwrap_or(0)
}

/// The replica's newest state, restored into `dir`.
fn newest_state(dir: &Path, replica: &Path, name: &str) -> Connection {
    let store: Arc<dyn DynStore> = Arc::new(FsStore::new(replica));
    let to = dir.join(format!("newest-{name}.db"));
    restore(&*store, &to, &lsr_replica::Restore::latest()).unwrap();
    Connection::open(&to).unwrap()
}

/// What the history leaves: the replica holds `replicated` (the rows host A last had) and the
/// backup holds fewer; `b` is host B's database, started with a working folder of its own.
struct History {
    dir: TempDir,
    replica: PathBuf,
    b_config: PathBuf,
    lost: Vec<(i64, Vec<u8>)>,
}

/// Host A through its last upload, the backup copied to B (not yet started). `bumped` is whether
/// A's replicator checkpointed after the backup, which raises the replica's counter above it.
fn history(copy: Copy, bumped: bool) -> History {
    let dir = TempDir::new().unwrap();
    let a_db = dir.path().join("a.db");
    let b_db = dir.path().join("b.db");
    let replica = dir.path().join("replica");
    // A checkpoints every 50 ms when it is to bump the counter, and not within the test when it is not.
    let interval = if bumped { "50ms" } else { "1h" };
    let a_config = config(dir.path(), &a_db, &replica, "a.yml", interval);
    let b_config = config(dir.path(), &b_db, &replica, "b.yml", "1h");

    let app = database(&a_db);
    write(&app, 60);
    let mut daemon = replicate(&a_config);
    // The backup is taken once the replica holds everything the application has written.
    let mut probe = 0_u32;
    let mut replica_holds = |daemon: &mut Daemon, want: usize, above: Option<i64>| loop {
        probe = probe.saturating_add(1);
        let seen = newest_state(dir.path(), &replica, &format!("probe{probe}"));
        if rows(&seen).len() >= want && above.is_none_or(|above| counter(&seen) > above) {
            return;
        }
        next_with(daemon, &["msg=uploaded", "Put("]);
    };
    daemon.wait_for(&["msg=uploaded", "Put("]);
    replica_holds(&mut daemon, 60, None);
    if bumped {
        daemon.wait_for(&["msg=checkpoint"]);
    }
    // A file backup of the database, as `sqlite3 .backup` takes it.
    let backup = dir.path().join("backup.db");
    app.backup(
        rusqlite::MAIN_DB,
        &backup,
        None::<fn(rusqlite::backup::Progress)>,
    )
    .unwrap();
    let at_backup = counter(&Connection::open(&backup).unwrap());
    let backed_up = rows(&Connection::open(&backup).unwrap());
    // A goes on: more commits, replicated, and (when asked) a checkpoint's counter bump too.
    write(&app, 3);
    replica_holds(&mut daemon, 63, bumped.then_some(at_backup));
    let lost = rows(&app);
    assert!(lost.len() > backed_up.len());
    daemon.signal("KILL");
    drop(daemon.finish());
    drop(app);

    // A is lost. The operator copies the backup to B.
    // Written out rather than `fs::copy`, which on macOS keeps the source's modified time.
    std::fs::write(&b_db, std::fs::read(&backup).unwrap()).unwrap();
    if matches!(copy, Copy::Preserving) {
        let kept = std::fs::metadata(&backup).unwrap().modified().unwrap();
        std::fs::File::options()
            .write(true)
            .open(&b_db)
            .unwrap()
            .set_modified(kept)
            .unwrap();
    }
    History {
        dir,
        replica,
        b_config,
        lost,
    }
}

/// Host B starts and the replica's newest state is read after it ends.
fn start_b(history: &History, stops: bool, name: &str) -> Vec<(i64, Vec<u8>)> {
    let mut daemon = replicate(&history.b_config);
    if stops {
        daemon.wait_for(&["replicate refused"]);
    } else {
        daemon.wait_for(&[
            "level=WARN",
            "has no record of the replica's newest transaction",
        ]);
        next_with(&mut daemon, &["msg=uploaded", "Put("]);
    }
    daemon.signal("TERM");
    let (_status, lines) = daemon.finish();
    assert_eq!(
        lines.iter().any(|line| line.contains("replicate refused")),
        stops,
        "{lines:#?}"
    );
    rows(&newest_state(history.dir.path(), &history.replica, name))
}

/// The review's history, with the counter raised by A's checkpoints after the backup: the
/// older copy is not the replica plus more, so the start stops and the replica's newest state
/// is what A last replicated.
#[test]
fn an_older_copy_put_on_a_new_host_stops_and_the_replica_keeps_its_newest_state() {
    let history = history(Copy::Plain, true);
    let after = start_b(&history, true, "first");
    assert_eq!(after, history.lost);
    // A supervisor starts it again: the first start left a WAL and a working folder behind, and
    // the second stops the same way.
    let again = start_b(&history, true, "second");
    assert_eq!(again, history.lost);
}

/// The same with `cp -p`: the copy's modified time is older than the replica's newest file, so
/// the modified-time test stops it before the counter is asked.
#[test]
fn an_older_copy_with_its_modified_time_kept_stops_too() {
    let history = history(Copy::Preserving, true);
    let after = start_b(&history, true, "after");
    assert_eq!(after, history.lost);
}

/// The window the rule leaves, pinned: a backup taken after A's last checkpoint holds the
/// replica's counter, so the start carries on, and the commits made after the backup go missing
/// from the newest state (they stay reachable by transaction id until retention).
#[test]
fn a_backup_taken_after_the_last_counter_bump_still_carries_on() {
    let history = history(Copy::Plain, false);
    let after = start_b(&history, false, "after");
    assert!(
        after.len() < history.lost.len(),
        "the newest state went back"
    );
}

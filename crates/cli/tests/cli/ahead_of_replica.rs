//! Issue 329 through the built `lsr`: the replica becomes unreachable while the application
//! writes, the replicator is killed, the application's last close checkpoints the WAL, and the
//! replicator starts again with a brand-new meta directory. The database is the replica's
//! newest state plus the commits that never reached it, so the start carries on with a snapshot
//! and a warning instead of refusing (`docs/decisions/D194.md`), and every transaction the
//! replica holds restores.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use lsr_core::conformance::block_on;
use lsr_core::{Inventory, Level};
use lsr_replica::Restore;
use lsr_replica::{DynStore, RestoreTarget, Txid, restore};
use lsr_store_fs::FsStore;
use rusqlite::Connection;
use tempfile::TempDir;

use crate::common::{Daemon, database, lsr, rows, write};

fn replicate(config: &Path) -> Daemon {
    let mut command = lsr();
    command
        .arg("replicate")
        .args(["-config", config.to_str().unwrap(), "-log-level", "debug"]);
    Daemon::spawn(command)
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

/// The replica's newest level-0 transaction.
fn newest(replica: &Path) -> u64 {
    let inventory = block_on(Inventory::load(&FsStore::new(replica))).unwrap();
    inventory
        .level(Level::ZERO)
        .iter()
        .map(|info| info.key.range().max().get())
        .max()
        .unwrap()
}

/// Points the name the configuration gives the replica at `to`, in one step: a new link renamed
/// over the old one. Whatever the replicator is in the middle of, it finds the old place or the
/// new one, never a missing directory, which an upload would create again under the test.
fn point(link: &Path, to: &Path) {
    let next = link.with_extension("next");
    std::os::unix::fs::symlink(to, &next).unwrap();
    std::fs::rename(&next, link).unwrap();
}

/// The rows of the replica's state at `target`, restored into `dir`.
fn restored(dir: &Path, replica: &Path, target: RestoreTarget, name: &str) -> Vec<(i64, Vec<u8>)> {
    let store: Arc<dyn DynStore> = Arc::new(FsStore::new(replica));
    let to = dir.join(format!("restored-{name}.db"));
    restore(
        &*store,
        &to,
        &lsr_replica::Restore {
            target,
            ..Restore::default()
        },
    )
    .unwrap();
    rows(&Connection::open(&to).unwrap())
}

#[test]
fn a_new_meta_directory_after_an_unreachable_replica_and_a_kill_carries_on() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    let held = dir.path().join("replica-held");
    let blocked = dir.path().join("replica-blocked");
    let staging: PathBuf = dir.path().join(".app.db-litestream");
    let config = dir.path().join("litestream.yml");
    let text = format!(
        "dbs:\n  - path: {}\n    replica:\n      path: {}\n",
        db.display(),
        replica.display()
    );
    std::fs::write(&config, text).unwrap();
    std::fs::create_dir(&held).unwrap();
    std::fs::write(&blocked, b"not a directory").unwrap();
    point(&replica, &held);

    // The replica takes the first commits.
    let app = database(&db);
    write(&app, 4);
    let mut daemon = replicate(&config);
    daemon.wait_for(&["msg=uploaded", "Put("]);
    write(&app, 4);
    next_with(&mut daemon, &["msg=uploaded", "Put("]);
    let tip = newest(&replica);
    let at_tip = rows(&app);

    // The replica becomes unreachable: a file stands where its directory was, which refuses
    // every write whoever runs the test. The application goes on writing; the uploads fail.
    point(&replica, &blocked);
    write(&app, 5);
    next_with(&mut daemon, &["msg=retrying"]);
    daemon.signal("KILL");
    drop(daemon.finish());

    // The application closes, opens and closes again: the last close checkpoints the WAL into
    // the database, so the WAL no longer holds the replica's newest transaction.
    let want = rows(&app);
    drop(app);
    drop(database(&db));

    // The replica is reachable again, and the meta directory is gone.
    point(&replica, &held);
    std::fs::remove_dir_all(&staging).unwrap();

    let mut daemon = replicate(&config);
    let warned = daemon
        .wait_for(&[
            "level=WARN",
            "has no record of the replica's newest transaction",
        ])
        .1;
    assert!(warned.contains(&format!("tip={tip}")), "{warned}");
    next_with(&mut daemon, &["msg=uploaded", "Put("]);
    daemon.signal("TERM");
    let (status, lines) = daemon.finish();
    assert_eq!(status.code(), Some(0), "{lines:#?}");
    assert!(
        !lines.iter().any(|line| line.contains("replicate refused")),
        "{lines:#?}"
    );

    // Every transaction restores to a state the application committed: a prefix of the rows,
    // never shorter than the one before, the replica's old newest state as it was, and the
    // newest state the database as it is.
    let newest = newest(&replica);
    assert!(newest > tip, "nothing uploaded above {tip}");
    let mut last = 0;
    for n in 1..=newest {
        let txid = Txid::new(n).unwrap();
        let got = restored(
            dir.path(),
            &replica,
            RestoreTarget::Txid(txid),
            &n.to_string(),
        );
        assert!(got.len() >= last, "transaction {n} went back");
        assert_eq!(got.as_slice(), &want[..got.len()], "transaction {n}");
        if n == tip {
            assert_eq!(got, at_tip, "transaction {n}, the old newest");
        }
        last = got.len();
    }
    assert_eq!(
        restored(dir.path(), &replica, RestoreTarget::Latest, "latest"),
        want
    );
}

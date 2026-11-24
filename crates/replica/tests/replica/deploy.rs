//! The deploy cycle of a host whose volume outlives the machine (fly.io replaces the VM on
//! every deploy): a process runs the application and the replicator in-process on a database
//! and meta directory that stay, and ends either by SIGTERM (the application closes its
//! connections, the replicator stops, the final sync completes and the process exits) or by
//! SIGKILL during the final sync (fly.io's kill timeout is below the shutdown sync's). Each
//! deploy is a real child process; a new one on the same files must continue: no snapshot after
//! the first boot, no stop, and the replica restores what the database holds.
//!
//! The order of a SIGTERM's closes matters. The replicator's connections keep the WAL when
//! they close last; an application connection that closes last deletes it (SQLite's default,
//! which `app_connection` leaves alone, docs/decisions.md D123), and the next deploy then
//! snapshots (`WalMissing`) and goes on.

use crate::common;

use std::io::{BufRead, BufReader, Lines, Write};
use std::path::{Path, PathBuf};
use std::process::{ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use common::{assert_same, booted, config, fake, stopped};
use lsr_replica::{Event, EventSink, Replicator, RestoreTarget, restore};
use lsr_store_fs::FsStore;
use rusqlite::Connection;

/// The directory a deploy child works in: `app.db`, its meta directory beside it, and
/// `replica/`.
const CHILD_DIR: &str = "LSR_DEPLOY_DIR";

/// How a deploy ends.
#[derive(Clone, Copy, Debug)]
enum End {
    /// SIGTERM: the child closes the application's connection, stops its replicator, which
    /// drains, and exits.
    Term,
    /// SIGTERM in the other order: the replicator stops first and the application's connection is
    /// the database's last to close.
    TermAppLast,
    /// SIGKILL once the child has begun its final sync with work left to ship.
    Kill,
}

/// Runs one deploy to its end and returns the events its replicator reported.
fn deploy(dir: &Path, end: End) -> Vec<String> {
    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .args([
            "deploy::deploy_child",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_DIR, dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut events = until(&mut lines, "lsr-deploy: ready");
    let mut stdin = child.stdin.take().unwrap();
    match end {
        End::Term | End::TermAppLast => {
            let verb: &[u8] = if matches!(end, End::Term) {
                b"term\n"
            } else {
                b"term-app-last\n"
            };
            stdin.write_all(verb).unwrap();
            drop(stdin);
            events.extend(until(&mut lines, "lsr-deploy: exited"));
            assert!(child.wait().unwrap().success());
        }
        End::Kill => {
            stdin.write_all(b"kill\n").unwrap();
            events.extend(until(&mut lines, "lsr-deploy: stopping"));
            child.kill().unwrap();
            drop(child.wait());
        }
    }
    events
}

/// The child's event lines up to `marker`, which it must print.
fn until(lines: &mut Lines<BufReader<ChildStdout>>, marker: &str) -> Vec<String> {
    let mut events = Vec::new();
    for line in lines.by_ref().map_while(Result::ok) {
        if line.contains(marker) {
            return events;
        }
        // libtest's own `test ... ` has no newline after it, so the first event shares its line.
        if let Some((_, event)) = line.split_once("lsr-event: ") {
            events.push(event.to_owned());
        }
    }
    panic!("the deploy child ended before {marker:?}: {events:?}");
}

/// A later deploy's events: its boot continued, nothing snapshotted again and nothing
/// stopped or was refused.
fn continued(n: usize, events: &[String]) {
    let boots: Vec<&String> = events.iter().filter(|e| e.contains("Booted")).collect();
    assert!(
        boots
            .first()
            .is_some_and(|boot| boot.contains("Booted { cause: None }")),
        "deploy {n} boot: {events:?}"
    );
    for event in events {
        assert!(
            !event.contains("Resnapshot")
                && !event.contains("Refused")
                && !event.contains("Stopped(")
                && !(event.contains("Booted") && !event.contains("cause: None")),
            "deploy {n}: {event} in {events:?}"
        );
    }
}

/// The replica restores what the database holds, after the last deploy exited.
fn restores(dir: &Path) {
    let to = dir.join("restored.db");
    restore(
        &FsStore::new(dir.join("replica")),
        RestoreTarget::Latest,
        &to,
    )
    .unwrap();
    let app = Connection::open(dir.join("app.db")).unwrap();
    assert_same(&app, &to);
}

/// Three deploys ending in SIGTERM, then a fourth: every boot after the first continues.
#[test]
fn deploys_ending_in_sigterm_continue_with_no_snapshot_and_no_stop() {
    let dir = tempfile::tempdir().unwrap();
    for n in 0..4 {
        let events = deploy(dir.path(), End::Term);
        if n > 0 {
            continued(n, &events);
        }
    }
    restores(dir.path());
}

/// A deploy whose application connection closes after the replicator's: SQLite deletes the WAL at
/// that close, so the next deploy finds none and snapshots (`WalMissing`). It never stops,
/// nothing is refused, and the deploy after it, closed in the documented order, continues.
#[test]
fn a_deploy_whose_application_closes_last_snapshots_once_and_does_not_stop() {
    let dir = tempfile::tempdir().unwrap();
    deploy(dir.path(), End::TermAppLast);
    assert!(!dir.path().join("app.db-wal").exists());
    let events = deploy(dir.path(), End::Term);
    let boots: Vec<&String> = events.iter().filter(|e| e.contains("Booted")).collect();
    assert!(
        matches!(boots.as_slice(), [boot] if boot.contains("WalMissing")),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| e.contains("Refused") || e.contains("Stopped(")),
        "{events:?}"
    );
    continued(2, &deploy(dir.path(), End::Term));
    restores(dir.path());
}

/// Three deploys ending in SIGKILL during the final sync, then one ending in SIGTERM: every
/// boot after the first continues, and the last drain leaves the replica whole.
#[test]
fn deploys_killed_during_the_final_sync_continue_with_no_snapshot_and_no_stop() {
    let dir = tempfile::tempdir().unwrap();
    for n in 0..4 {
        let end = if n < 3 { End::Kill } else { End::Term };
        let events = deploy(dir.path(), end);
        if n > 0 {
            continued(n, &events);
        }
    }
    restores(dir.path());
}

/// Prints each event on its own line for the parent to read.
struct Printer;

impl EventSink for Printer {
    fn event(&mut self, event: &Event) {
        println!("lsr-event: {event:?}");
        drop(std::io::stdout().flush());
    }
}

/// The child's half of the deploy tests: started by them with [`CHILD_DIR`], and nothing at
/// all when run as a test of its own. The application commits and the replicator ships; on
/// `term` the application's connection closes, the replicator stops and the process exits; on
/// `term-app-last` the replicator stops first; on
/// `kill` the application commits more and the replicator begins its final sync, and the
/// parent kills the process.
#[test]
fn deploy_child() {
    let Some(dir) = std::env::var_os(CHILD_DIR).map(PathBuf::from) else {
        return;
    };
    let db = dir.join("app.db");
    let app = Connection::open(&db).unwrap();
    app.busy_timeout(Duration::from_secs(5)).unwrap();
    app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    lsr_sqlite::app_connection(&app).unwrap();
    app.execute_batch("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    let store = Arc::new(FsStore::new(dir.join("replica")));
    let (runtime, clock) = fake();
    let replicator =
        Replicator::start_with_events(&db, store, config(), runtime, Box::new(Printer)).unwrap();
    booted(&replicator);
    let insert = |n: u32| {
        for _ in 0..n {
            app.execute("INSERT INTO t(v) VALUES (randomblob(3000))", [])
                .unwrap();
        }
    };
    insert(3);
    // Every deploy stages something before it is told to end: a kill that lands before the
    // first deploy staged anything leaves nothing to continue from, and the next boot's first
    // sync would be right.
    common::wait(&replicator, |health| health.synced.is_some());
    println!("lsr-deploy: ready");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    if line.trim() == "kill" {
        insert(2);
        let _final_sync = replicator.stop(common::PATIENCE);
        println!("lsr-deploy: stopping");
        std::io::stdout().flush().unwrap();
        // The parent kills the process here, somewhere in the final sync.
        let mut rest = String::new();
        drop(std::io::stdin().read_line(&mut rest));
        return;
    }
    // A boot that owes a snapshot can find the database busy with the application's inserts and
    // retry after a wait (`SqliteBusy`), and the drain waits with it: the clock moves meanwhile.
    let stop = || {
        stopped(&replicator, &clock);
    };
    if line.trim() == "term-app-last" {
        stop();
        drop(replicator);
        drop(app);
    } else {
        drop(app);
        stop();
        drop(replicator);
    }
    println!("lsr-deploy: exited");
    std::io::stdout().flush().unwrap();
}

//! `lsr replicate` as Go's daemon, run as a real process: many databases from one
//! config, a database created after start, one that fails to start, a second signal,
//! `-once` and `-restore-if-db-not-exists`, the log formats, and `lsr config check`.
//! Every wait is on a line the daemon prints or on the process ending.

use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use rusqlite::Connection;
use tempfile::TempDir;

use crate::common::{Daemon, database, lsr, restores_to, rows, write};

/// `lsr replicate <args>`.
fn start(args: &[&str]) -> Daemon {
    let mut command = lsr();
    command.arg("replicate").args(args);
    Daemon::spawn(command)
}

fn with_config(config: &Path) -> Daemon {
    start(&["-config", config.to_str().unwrap()])
}

/// Waits for the process to end; its status and every line it printed, as one text.
fn finish(daemon: Daemon) -> (ExitStatus, String) {
    let (status, lines) = daemon.finish();
    (status, lines.join("\n"))
}

/// Waits for a line holding every one of `parts`, failing at the `cap`th `retrying`
/// line on the way: a daemon that retries without waiting prints them without end.
fn wait_retrying_under(daemon: &mut Daemon, parts: &[&str], cap: usize) -> String {
    let mut retries = 0;
    loop {
        let line = daemon.next_line(parts);
        if line.contains("msg=retrying") {
            retries = usize::saturating_add(retries, 1);
            assert!(retries < cap, "{retries} retries: {line}");
        }
        if parts.iter().all(|p| line.contains(p)) {
            return line;
        }
    }
}

/// Go's `file://` URL for a replica directory, the two-argument form's replica.
fn file_url(dir: &Path) -> String {
    format!("file://{}", dir.display())
}

fn config(dir: &Path, text: &str) -> PathBuf {
    let path = dir.join("litestream.yml");
    std::fs::write(&path, text).unwrap();
    path
}

fn entry(db: &Path, replica: &Path) -> String {
    format!(
        "  - path: {}\n    replica:\n      path: {}\n",
        db.display(),
        replica.display()
    )
}

#[test]
fn two_databases_from_one_config_drain_on_sigterm_and_restore() {
    let dir = TempDir::new().unwrap();
    let (a, b) = (dir.path().join("a.db"), dir.path().join("b.db"));
    let (ra, rb) = (dir.path().join("ra"), dir.path().join("rb"));
    let (app_a, app_b) = (database(&a), database(&b));
    let cfg = config(
        dir.path(),
        &format!("dbs:\n{}{}", entry(&a, &ra), entry(&b, &rb)),
    );
    let mut daemon = with_config(&cfg);
    daemon.wait_for(&["msg=\"initialized db\"", &format!("path={}", a.display())]);
    daemon.wait_for(&["msg=\"initialized db\"", &format!("path={}", b.display())]);
    write(&app_a, 20);
    write(&app_b, 30);
    daemon.signal("TERM");
    let (status, out) = finish(daemon);
    assert_eq!(status.code(), Some(0), "{out}");
    assert!(out.contains("signal=terminated"), "{out}");
    assert_eq!(out.matches("msg=\"database drained\"").count(), 2, "{out}");
    restores_to(dir.path(), &ra, &app_a);
    restores_to(dir.path(), &rb, &app_b);
}

/// Go skips a database that does not exist yet and starts it when it appears.
#[test]
fn a_database_created_after_start_is_picked_up() {
    let dir = TempDir::new().unwrap();
    let late = dir.path().join("late.db");
    let replica = dir.path().join("replica");
    let cfg = config(
        dir.path(),
        &format!(
            "dbs:\n  - path: {}\n    monitor-interval: 20ms\n    replica:\n      path: {}\n",
            late.display(),
            replica.display()
        ),
    );
    let mut daemon = with_config(&cfg);
    daemon.wait_for(&["msg=\"waiting for database\""]);
    let app = database(&late);
    write(&app, 10);
    daemon.wait_for(&[
        "msg=\"initialized db\"",
        &format!("path={}", late.display()),
    ]);
    write(&app, 10);
    daemon.signal("INT");
    let (status, out) = finish(daemon);
    assert_eq!(status.code(), Some(0), "{out}");
    restores_to(dir.path(), &replica, &app);
}

/// Go exits when one database fails; here the others keep replicating and the exit
/// code says so at the end.
#[test]
fn one_database_failing_to_start_leaves_the_other_replicating() {
    let dir = TempDir::new().unwrap();
    let (good, bad) = (dir.path().join("good.db"), dir.path().join("bad.db"));
    std::fs::write(&bad, vec![0x5a_u8; 8192]).unwrap();
    let (rg, rbad) = (dir.path().join("rg"), dir.path().join("rbad"));
    let app = database(&good);
    let cfg = config(
        dir.path(),
        &format!("dbs:\n{}{}", entry(&bad, &rbad), entry(&good, &rg)),
    );
    let mut daemon = with_config(&cfg);
    daemon.wait_for(&["level=ERROR", "msg=\"cannot start database\"", "bad.db"]);
    daemon.wait_for(&["msg=\"initialized db\"", "good.db"]);
    write(&app, 25);
    daemon.signal("TERM");
    let (status, out) = finish(daemon);
    assert_eq!(status.code(), Some(1), "{out}");
    assert!(out.contains("msg=\"database did not start\""), "{out}");
    assert_eq!(out.matches("msg=\"database drained\"").count(), 1, "{out}");
    restores_to(dir.path(), &rg, &app);
}

/// The first signal drains with the shared deadline (an hour here); the second cuts
/// it, as Go's second signal interrupts its shutdown.
#[test]
fn a_second_signal_cuts_the_drain() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    std::fs::create_dir_all(&replica).unwrap();
    // A file where the level directories belong: every store call fails, so the
    // drain can never finish on its own.
    std::fs::write(replica.join("ltx"), b"in the way").unwrap();
    let app = database(&db);
    // Committed before the start: a boot that cannot list the replica holds the application's
    // writes for the bound (decision D207), so a write after the start would wait for it.
    write(&app, 5);
    let cfg = config(
        dir.path(),
        &format!("shutdown-sync-timeout: 1h\ndbs:\n{}", entry(&db, &replica)),
    );
    let mut daemon = with_config(&cfg);
    daemon.wait_for(&["msg=\"initialized db\""]);
    daemon.signal("TERM");
    daemon.wait_for(&["msg=\"signal received, litestream shutting down\""]);
    daemon.signal("HUP");
    let (status, out) = finish(daemon);
    assert_eq!(status.code(), Some(1), "{out}");
    assert!(
        out.contains(
            "msg=\"database not drained: deadline reached, the replica lacks every commit after last_replicated_txid\""
        ),
        "{out}"
    );
    assert!(out.contains("last_replicated_txid=none"), "{out}");
}

/// `-once -force-snapshot` whose level-9 snapshot cannot be written exits 1 and says so. The
/// outcome is read once the drain has ended: read before, it said the snapshot was still asked
/// for, and the run was reported clean.
#[cfg(unix)]
#[test]
fn once_with_a_snapshot_that_cannot_be_written_is_not_clean() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    let level9 = replica.join("ltx").join("9");
    std::fs::create_dir_all(&level9).unwrap();
    std::fs::set_permissions(&level9, std::fs::Permissions::from_mode(0o555)).unwrap();
    let app = database(&db);
    write(&app, 15);
    let cfg = config(dir.path(), &format!("dbs:\n{}", entry(&db, &replica)));
    let (status, out) = finish(start(&[
        "-once",
        "-force-snapshot",
        "-config",
        cfg.to_str().unwrap(),
    ]));
    std::fs::set_permissions(&level9, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(status.code(), Some(1), "{out}");
    assert!(
        out.contains("msg=\"database drained, snapshot not written\""),
        "{out}"
    );
}

/// `-once`: every database synced and uploaded, then exit. `-once -force-snapshot` ends each
/// with one level-9 file of the whole database, and `-once -enforce-retention` with a retention
/// pass, which on a fresh replica has nothing to delete (docs/decisions/D167.md).
#[test]
fn once_replicates_everything_committed_and_exits() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    let app = database(&db);
    write(&app, 15);
    let cfg = config(dir.path(), &format!("dbs:\n{}", entry(&db, &replica)));
    let cfg = cfg.to_str().unwrap();
    let (status, out) = finish(start(&["-once", "-config", cfg]));
    assert_eq!(status.code(), Some(0), "{out}");
    restores_to(dir.path(), &replica, &app);
    let level9 = replica.join("ltx").join("9");
    assert!(std::fs::read_dir(&level9).map_or(true, |d| d.count() == 0));
    write(&app, 16);
    let (status, out) = finish(start(&["-once", "-force-snapshot", "-config", cfg]));
    assert_eq!(status.code(), Some(0), "{out}");
    let snapshots: Vec<_> = std::fs::read_dir(&level9).unwrap().collect();
    assert_eq!(snapshots.len(), 1, "{out}");
    let again = TempDir::new().unwrap();
    restores_to(again.path(), &replica, &app);
    write(&app, 17);
    let (status, out) = finish(start(&["-once", "-enforce-retention", "-config", cfg]));
    assert_eq!(status.code(), Some(0), "{out}");
    let last = TempDir::new().unwrap();
    restores_to(last.path(), &replica, &app);
}

#[test]
fn restore_if_db_not_exists_restores_or_starts_fresh() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    let db_arg = db.to_str().unwrap();
    let replica_url = format!("file://{}", replica.display());
    let replica_arg = replica_url.as_str();
    let flags = ["-once", "-restore-if-db-not-exists", db_arg, replica_arg];
    let (status, out) = finish(start(&flags));
    assert_eq!(status.code(), Some(0), "{out}");
    assert!(
        out.contains("msg=\"no backup found, starting fresh\""),
        "{out}"
    );
    let want = {
        let app = database(&db);
        write(&app, 12);
        let (status, out) = finish(start(&["-once", db_arg, replica_arg]));
        assert_eq!(status.code(), Some(0), "{out}");
        rows(&app)
    };
    for sidecar in ["", "-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{sidecar}", db.display()));
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    std::fs::remove_dir_all(dir.path().join(".app.db-litestream")).unwrap();
    // The application starts once the daemon has restored and pinned the database, and writes.
    let mut daemon = start(&["-restore-if-db-not-exists", db_arg, replica_arg]);
    daemon.wait_for(&["msg=\"restore completed\""]);
    daemon.wait_for(&["msg=\"initialized db\""]);
    let app = database(&db);
    assert_eq!(rows(&app), want);
    write(&app, 8);
    daemon.signal("TERM");
    let (status, out) = finish(daemon);
    assert_eq!(status.code(), Some(0), "{out}");
    restores_to(dir.path(), &replica, &app);
}

/// A database the daemon restored, with no write from the application: the WAL has no header,
/// and the first sync is a snapshot that writes one itself. It is staged at once, with no
/// retry; before, the sync task logged `retrying` some 70,000 times in four seconds.
#[test]
fn a_restored_database_snapshots_with_no_write_and_no_retry() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    let db_arg = db.to_str().unwrap();
    let replica_url = format!("file://{}", replica.display());
    let replica_arg = replica_url.as_str();
    {
        let app = database(&db);
        write(&app, 6);
        let (status, out) = finish(start(&["-once", db_arg, replica_arg]));
        assert_eq!(status.code(), Some(0), "{out}");
    }
    for sidecar in ["", "-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{sidecar}", db.display()));
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    std::fs::remove_dir_all(dir.path().join(".app.db-litestream")).unwrap();
    let flags = [
        "-log-level",
        "debug",
        "-restore-if-db-not-exists",
        db_arg,
        replica_arg,
    ];
    let mut daemon = start(&flags);
    daemon.wait_for(&["msg=\"restore completed\""]);
    daemon.wait_for(&["msg=\"initialized db\""]);
    wait_retrying_under(&mut daemon, &["msg=\"wal synced\"", "files=1"], 10);
    wait_retrying_under(&mut daemon, &["msg=uploaded", "Put("], 10);
    daemon.signal("TERM");
    let (status, out) = finish(daemon);
    assert_eq!(status.code(), Some(0), "{out}");
    let retries = out.lines().filter(|l| l.contains("msg=retrying")).count();
    assert_eq!(retries, 0, "{out}");
    restores_to(dir.path(), &replica, &Connection::open(&db).unwrap());
}

/// Go's `TestReadConfig/ReadsFromStdin` and `ReadsFromStdinSelectedByEnv`.
#[test]
fn the_config_can_come_from_standard_input() {
    use std::io::Write as _;
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    let app = database(&db);
    write(&app, 3);
    let text = format!("dbs:\n{}", entry(&db, &replica));
    for (args, env) in [
        (&["replicate", "-once", "-config", "-"][..], None),
        (&["replicate", "-once"][..], Some("-")),
    ] {
        let mut command = lsr();
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        if let Some(value) = env {
            command.env("LITESTREAM_CONFIG", value);
        }
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        let stdout = String::from_utf8(out.stdout).unwrap();
        assert_eq!(out.status.code(), Some(0), "{args:?}: {stdout}");
        assert!(stdout.contains("msg=\"database drained\""), "{stdout}");
    }
    restores_to(dir.path(), &replica, &app);
}

/// `logging.type: json` writes one slog JSON object per line.
#[test]
fn json_logs_parse_with_go_s_keys() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    drop(database(&db));
    let cfg = config(
        dir.path(),
        &format!(
            "logging:\n  type: json\ndbs:\n{}",
            entry(&db, &dir.path().join("r"))
        ),
    );
    let (status, out) = finish(start(&["-once", "-config", cfg.to_str().unwrap()]));
    assert_eq!(status.code(), Some(0), "{out}");
    for line in out.lines() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        for key in ["time", "level", "msg"] {
            assert!(value.get(key).is_some(), "{key} missing: {line}");
        }
    }
    assert!(out.contains("\"msg\":\"initialized db\""), "{out}");
}

/// Go's `litestream replicate DB REPLICA_URL`: one database, Go's up line once its read
/// transaction is held, and each stop signal drains it and exits 0, so a restore of what
/// it wrote equals the database. This is the process the compatibility tests kill.
#[test]
fn the_two_argument_form_drains_on_each_stop_signal() {
    for name in ["TERM", "INT", "HUP"] {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("app.db");
        let replica = dir.path().join("replica");
        let app = database(&db);
        let url = file_url(&replica);
        let mut daemon = start(&[db.to_str().unwrap(), &url]);
        daemon.wait_for(&["msg=\"replicating to\"", "type=file"]);
        write(&app, 20);
        daemon.signal(name);
        let (status, out) = finish(daemon);
        assert_eq!(status.code(), Some(0), "SIG{name}: {out}");
        assert!(out.contains("msg=\"database drained\""), "SIG{name}: {out}");
        restores_to(dir.path(), &replica, &app);
    }
}

/// Go's replica argument is a URL: a plain directory is "file replica url required"
/// there, and refused here before any database is opened.
#[test]
fn the_two_argument_form_refuses_a_plain_directory_as_go_does() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let replica = dir.path().join("replica");
    let out = lsr()
        .arg("replicate")
        .arg(&db)
        .arg(&replica)
        .output()
        .unwrap();
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("file replica url required"), "{stderr}");
    assert!(!replica.exists() && !db.exists());
}

#[test]
fn go_s_flag_refusals_are_usage_errors() {
    // The positional form needs no file: Go checks these combinations after the config is
    // read, so a missing default config would be reported first.
    for args in [
        &["-force-snapshot", "a.db", "s3://b/p"][..],
        &["-enforce-retention", "a.db", "s3://b/p"],
        &["a.db", "s3://b/p", "-exec", "x"],
        &["only.db"],
        &["-bogus"],
    ] {
        let out = lsr().arg("replicate").args(args).output().unwrap();
        assert_eq!(out.status.code(), Some(1), "{args:?}");
    }
    let out = lsr().args(["replicate", "-h"]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert!(
        String::from_utf8(out.stdout)
            .unwrap()
            .contains("-restore-if-db-not-exists")
    );
}

#[test]
fn a_missing_or_refused_config_does_not_start() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("missing.yml");
    let out = lsr()
        .args(["replicate", "-config", missing.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let cfg = config(
        dir.path(),
        "dbs:\n  - path: /a.db\n    replica:\n      type: gs\n      bucket: b\n",
    );
    let out = lsr()
        .args(["replicate", "-config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("dbs[0].replica.type"), "{stderr}");
    let cfg = config(
        dir.path(),
        "exec: \"myapp; rm -rf /\"\ndbs:\n  - path: /a.db\n    replica:\n      path: /r\n",
    );
    let out = lsr()
        .args(["replicate", "-config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
}

/// `lsr config check` on upstream's `etc/litestream.yml` (all comments: it loads).
#[test]
fn config_check_reads_go_s_example_file() {
    let example = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/tests/data/litestream.yml"
    );
    let out = lsr().args(["config", "check", example]).output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.starts_with("ok: "), "{stdout}");
    let dir = TempDir::new().unwrap();
    let cfg = config(
        dir.path(),
        "sync_interval: 1s\ndbs:\n  - path: /a.db\n    busy-timeout: 5s\n    replica:\n      path: /r\n",
    );
    let out = lsr().args(["config", "check"]).arg(&cfg).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("error: sync_interval"), "{stdout}");
    assert!(stdout.contains("warning: dbs[0].busy-timeout"), "{stdout}");
    let out = lsr()
        .args(["config", "check"])
        .arg(dir.path().join("none.yml"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(3));
    let out = lsr().args(["config", "check"]).output().unwrap();
    assert_eq!(out.status.code(), Some(64));
}

/// Every config the daemon refuses before it opens a database, `config check` refuses
/// too, with the same reason and exit code: the schedule core refuses (issue #37), an
/// `exec` line Go would run differently, an `addr` Go cannot bind, and an entry with no
/// replica.
#[test]
fn config_check_refuses_what_the_daemon_refuses() {
    let dir = TempDir::new().unwrap();
    let db = "dbs:\n  - path: /a.db\n    replica:\n      path: /r\n";
    let cases = [
        (
            "levels:\n  - interval: 30s\n  - interval: 5m\n  - interval: 1h\nsnapshot:\n  interval: 1h\n",
            "snapshot interval 3600s is not above the top level's (3600s)",
        ),
        (
            "exec: \"myapp; rm -rf /\"\n",
            "exec command \"myapp; rm -rf /\"",
        ),
        (
            "addr: \"localhost\"\n",
            "must specify port for bind address",
        ),
    ];
    for (head, reason) in cases {
        let cfg = config(dir.path(), &format!("{head}{db}"));
        let check = lsr().args(["config", "check"]).arg(&cfg).output().unwrap();
        let start = lsr()
            .args(["replicate", "-config"])
            .arg(&cfg)
            .output()
            .unwrap();
        let stdout = String::from_utf8(check.stdout).unwrap();
        let stderr = String::from_utf8(start.stderr).unwrap();
        assert_eq!(start.status.code(), Some(1), "{head}: {stderr}");
        assert!(stderr.contains(reason), "{head}: {stderr}");
        assert_eq!(check.status.code(), Some(2), "{head}: {stdout}");
        assert!(stdout.contains(reason), "{head}: {stdout}");
        assert!(stdout.contains("refused: "), "{head}: {stdout}");
    }
    let cfg = config(dir.path(), "dbs:\n  - path: /a.db\n");
    let check = lsr().args(["config", "check"]).arg(&cfg).output().unwrap();
    let stdout = String::from_utf8(check.stdout).unwrap();
    assert_eq!(check.status.code(), Some(2), "{stdout}");
    assert!(
        stdout.contains("error: dbs[0]: must specify replica"),
        "{stdout}"
    );
}

/// Go reads the config before it checks `-once`, `-force-snapshot` and
/// `-enforce-retention` together, so a missing config is the error reported.
#[test]
fn a_missing_config_is_reported_before_a_flag_combination() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("missing.yml");
    let out = lsr()
        .args([
            "replicate",
            "-force-snapshot",
            "-config",
            missing.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("config file not found"), "{stderr}");
    assert!(!stderr.contains("-force-snapshot"), "{stderr}");
}

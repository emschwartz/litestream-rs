//! `lsr replicate` as Go's daemon, run as a real process: many databases from one
//! config, a database created after start, one that fails to start, a second signal,
//! `-once` and `-restore-if-db-not-exists`, the log formats, and `lsr config check`.
//! Every wait is on a line the daemon prints or on the process ending.

// `cfg(test)` marks the helpers as test code, so clippy.toml's allow-*-in-tests cover them.
#[cfg(test)]
mod daemon {
    use std::io::{BufRead, BufReader};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitStatus, Stdio};
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use lsr_replica::{DynStore, RestoreTarget, restore};
    use lsr_store_fs::FsStore;
    use rusqlite::Connection;
    use tempfile::TempDir;

    /// How long any one line may take: a failure bound, never a pacing wait.
    const PATIENCE: Duration = Duration::from_secs(60);

    fn lsr() -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_lsr"));
        command
            .env_remove("LOG_LEVEL")
            .env_remove("LITESTREAM_CONFIG");
        command
    }

    /// A running daemon and every stdout line it prints.
    struct Daemon {
        child: Child,
        lines: Receiver<String>,
        seen: Vec<String>,
    }

    /// A test that fails mid-run leaves no daemon behind.
    impl Drop for Daemon {
        fn drop(&mut self) {
            if matches!(self.child.try_wait(), Ok(None)) {
                drop(self.child.kill());
                drop(self.child.wait());
            }
        }
    }

    impl Daemon {
        fn start(args: &[&str]) -> Self {
            let mut child = lsr()
                .arg("replicate")
                .args(args)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (send, lines) = mpsc::sync_channel(4096);
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { return };
                    if send.send(line).is_err() {
                        return;
                    }
                }
            });
            Self {
                child,
                lines,
                seen: Vec::new(),
            }
        }

        fn with_config(config: &Path) -> Self {
            Self::start(&["-config", config.to_str().unwrap()])
        }

        /// Waits for a line holding every one of `parts`.
        fn wait_for(&mut self, parts: &[&str]) -> String {
            let found = |line: &String| parts.iter().all(|p| line.contains(p));
            if let Some(line) = self.seen.iter().find(|l| found(l)) {
                return line.clone();
            }
            loop {
                match self.lines.recv_timeout(PATIENCE) {
                    Ok(line) => {
                        self.seen.push(line.clone());
                        if found(&line) {
                            return line;
                        }
                    }
                    Err(error) => panic!("no line with {parts:?} ({error}): {:#?}", self.seen),
                }
            }
        }

        /// Waits for a line holding every one of `parts`, failing at the `cap`th `retrying`
        /// line on the way: a daemon that retries without waiting prints them without end.
        fn wait_retrying_under(&mut self, parts: &[&str], cap: usize) -> String {
            let mut retries = 0;
            loop {
                match self.lines.recv_timeout(PATIENCE) {
                    Ok(line) => {
                        self.seen.push(line.clone());
                        if line.contains("msg=retrying") {
                            retries = usize::saturating_add(retries, 1);
                            assert!(retries < cap, "{retries} retries: {line}");
                        }
                        if parts.iter().all(|p| line.contains(p)) {
                            return line;
                        }
                    }
                    Err(error) => panic!("no line with {parts:?} ({error}): {:#?}", self.seen),
                }
            }
        }

        fn signal(&self, name: &str) {
            let status = Command::new("kill")
                .arg(format!("-{name}"))
                .arg(self.child.id().to_string())
                .status()
                .unwrap();
            assert!(status.success());
        }

        /// Waits for the process to end, and every line it printed.
        fn finish(mut self) -> (ExitStatus, String) {
            let status = self.child.wait().unwrap();
            self.seen.extend(self.lines.iter());
            (status, self.seen.join("\n"))
        }
    }

    /// A host database: WAL, the host settings a replicated database needs, one table.
    fn database(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .unwrap();
        lsr_sqlite::app_connection(&conn).unwrap();
        conn.execute_batch("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        conn
    }

    fn write(conn: &Connection, rows: usize) {
        for _ in 0..rows {
            conn.execute("INSERT INTO t(v) VALUES (randomblob(2000))", [])
                .unwrap();
        }
    }

    fn rows(conn: &Connection) -> Vec<(i64, Vec<u8>)> {
        let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// Restores the replica in `dir` and checks it equals `conn`.
    fn restores_to(dir: &Path, replica: &Path, conn: &Connection) {
        let store: std::sync::Arc<dyn DynStore> = std::sync::Arc::new(FsStore::new(replica));
        let to = dir.join(format!(
            "restored-{}.db",
            replica.file_name().unwrap().to_string_lossy()
        ));
        restore(&*store, RestoreTarget::Latest, &to).unwrap();
        assert_eq!(rows(&Connection::open(&to).unwrap()), rows(conn));
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
        let mut daemon = Daemon::with_config(&cfg);
        daemon.wait_for(&["msg=\"initialized db\"", &format!("path={}", a.display())]);
        daemon.wait_for(&["msg=\"initialized db\"", &format!("path={}", b.display())]);
        write(&app_a, 20);
        write(&app_b, 30);
        daemon.signal("TERM");
        let (status, out) = daemon.finish();
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
        let mut daemon = Daemon::with_config(&cfg);
        daemon.wait_for(&["msg=\"waiting for database\""]);
        let app = database(&late);
        write(&app, 10);
        daemon.wait_for(&[
            "msg=\"initialized db\"",
            &format!("path={}", late.display()),
        ]);
        write(&app, 10);
        daemon.signal("INT");
        let (status, out) = daemon.finish();
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
        let mut daemon = Daemon::with_config(&cfg);
        daemon.wait_for(&["level=ERROR", "msg=\"cannot start database\"", "bad.db"]);
        daemon.wait_for(&["msg=\"initialized db\"", "good.db"]);
        write(&app, 25);
        daemon.signal("TERM");
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(3), "{out}");
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
        let cfg = config(
            dir.path(),
            &format!("shutdown-sync-timeout: 1h\ndbs:\n{}", entry(&db, &replica)),
        );
        let mut daemon = Daemon::with_config(&cfg);
        daemon.wait_for(&["msg=\"initialized db\""]);
        write(&app, 5);
        daemon.signal("TERM");
        daemon.wait_for(&["msg=\"signal received, litestream shutting down\""]);
        daemon.signal("HUP");
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(3), "{out}");
        assert!(
            out.contains("msg=\"database not drained: deadline reached\""),
            "{out}"
        );
    }

    /// `-once`: every database synced and uploaded, then exit. `-force-snapshot` and
    /// `-enforce-retention` are refused (the replicator's API has neither a level-9
    /// snapshot on demand nor a one-off retention pass).
    #[test]
    fn once_replicates_everything_committed_and_exits() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("app.db");
        let replica = dir.path().join("replica");
        let app = database(&db);
        write(&app, 15);
        let cfg = config(dir.path(), &format!("dbs:\n{}", entry(&db, &replica)));
        let cfg = cfg.to_str().unwrap();
        let (status, out) = Daemon::start(&["-once", "-config", cfg]).finish();
        assert_eq!(status.code(), Some(0), "{out}");
        restores_to(dir.path(), &replica, &app);
        for flag in ["-force-snapshot", "-enforce-retention"] {
            let out = lsr()
                .args(["replicate", "-once", flag, "-config", cfg])
                .output()
                .unwrap();
            assert_eq!(out.status.code(), Some(2), "{flag}");
        }
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
        let (status, out) = Daemon::start(&flags).finish();
        assert_eq!(status.code(), Some(0), "{out}");
        assert!(
            out.contains("msg=\"no backup found, starting fresh\""),
            "{out}"
        );
        let want = {
            let app = database(&db);
            write(&app, 12);
            let (status, out) = Daemon::start(&["-once", db_arg, replica_arg]).finish();
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
        // The host starts once the daemon has restored and pinned the database, and writes.
        let mut daemon = Daemon::start(&["-restore-if-db-not-exists", db_arg, replica_arg]);
        daemon.wait_for(&["msg=\"restore completed\""]);
        daemon.wait_for(&["msg=\"initialized db\""]);
        let app = database(&db);
        assert_eq!(rows(&app), want);
        write(&app, 8);
        daemon.signal("TERM");
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(0), "{out}");
        restores_to(dir.path(), &replica, &app);
    }

    /// A database the daemon restored, with no write from the host: the WAL has no header,
    /// and the first sync is a snapshot that writes one itself. It is staged at once, with no
    /// retry; before, the sync lane logged `retrying` some 70,000 times in four seconds.
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
            let (status, out) = Daemon::start(&["-once", db_arg, replica_arg]).finish();
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
        let mut daemon = Daemon::start(&flags);
        daemon.wait_for(&["msg=\"restore completed\""]);
        daemon.wait_for(&["msg=\"initialized db\""]);
        daemon.wait_retrying_under(&["msg=\"wal synced\"", "files=1"], 10);
        daemon.wait_retrying_under(&["msg=uploaded", "Put("], 10);
        daemon.signal("TERM");
        let (status, out) = daemon.finish();
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
        let (status, out) = Daemon::start(&["-once", "-config", cfg.to_str().unwrap()]).finish();
        assert_eq!(status.code(), Some(0), "{out}");
        for line in out.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            for key in ["time", "level", "msg"] {
                assert!(value.get(key).is_some(), "{key} missing: {line}");
            }
        }
        assert!(out.contains("\"msg\":\"initialized db\""), "{out}");
    }

    #[test]
    fn go_s_flag_refusals_are_usage_errors() {
        for args in [
            &["-force-snapshot"][..],
            &["-enforce-retention"],
            &["a.db", "s3://b/p", "-exec", "x"],
            &["only.db"],
            &["-bogus"],
        ] {
            let out = lsr().arg("replicate").args(args).output().unwrap();
            assert_eq!(out.status.code(), Some(64), "{args:?}");
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
        assert_eq!(out.status.code(), Some(3));
        let cfg = config(
            dir.path(),
            "dbs:\n  - path: /a.db\n    replica:\n      type: gs\n      bucket: b\n",
        );
        let out = lsr()
            .args(["replicate", "-config", cfg.to_str().unwrap()])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
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
        assert_eq!(out.status.code(), Some(2));
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
            assert_eq!(start.status.code(), Some(2), "{head}: {stderr}");
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
}

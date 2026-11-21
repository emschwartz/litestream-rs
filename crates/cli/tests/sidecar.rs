//! `lsr replicate` with `-exec` and with `addr`, run as real processes: the child starts
//! after the pin, its exit drains and the daemon exits 0 or 1 as Go does, signals reach it;
//! `/metrics` and `/healthz` on exactly the address given, with their bounds. Every wait is
//! on a line the daemon (or its child) prints, a response, or the process ending.
//!
//! The `-exec` child is this test binary again, running [`sidecar::exec_child`] with its mode
//! in `LSR_EXEC_CHILD`, as Go's own tests use theirs (`TestShutdownExecHelper`).

// `cfg(test)` marks the helpers as test code, so clippy.toml's allow-*-in-tests cover them.
#[cfg(test)]
mod sidecar {
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{SocketAddr, TcpStream};
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
            .env_remove("LITESTREAM_CONFIG")
            .env_remove("LSR_EXEC_CHILD");
        command
    }

    /// A running daemon and every stdout line it (and its child) prints.
    struct Daemon {
        child: Child,
        lines: Receiver<String>,
        seen: Vec<String>,
    }

    impl Daemon {
        fn spawn(mut command: Command) -> Self {
            let mut child = command
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

        /// Waits for a line holding every one of `parts`; its index among the lines seen.
        fn wait_for(&mut self, parts: &[&str]) -> (usize, String) {
            let found = |line: &String| parts.iter().all(|p| line.contains(p));
            if let Some(at) = self.seen.iter().position(found) {
                return (at, self.seen[at].clone());
            }
            loop {
                match self.lines.recv_timeout(PATIENCE) {
                    Ok(line) => {
                        self.seen.push(line.clone());
                        if found(&line) {
                            return (self.seen.len().saturating_sub(1), line);
                        }
                    }
                    Err(error) => panic!("no line with {parts:?} ({error}): {:#?}", self.seen),
                }
            }
        }

        fn signal(&self, name: &str) {
            kill(name, self.child.id());
        }

        /// Waits for the process to end; its status and every line it printed.
        fn finish(mut self) -> (ExitStatus, Vec<String>) {
            let status = self.child.wait().unwrap();
            self.seen.extend(self.lines.iter());
            (status, self.seen)
        }
    }

    fn kill(name: &str, pid: u32) {
        let status = Command::new("kill")
            .arg(format!("-{name}"))
            .arg(pid.to_string())
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// The index of the first line holding `part`.
    fn index(lines: &[String], part: &str) -> usize {
        lines
            .iter()
            .position(|l| l.contains(part))
            .unwrap_or_else(|| panic!("no {part:?} in {lines:#?}"))
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
        let to = dir.join("restored.db");
        restore(&*store, RestoreTarget::Latest, &to).unwrap();
        assert_eq!(rows(&Connection::open(&to).unwrap()), rows(conn));
    }

    /// One database at `dir/app.db` replicated to `dir/replica`, with `extra` top-level keys.
    struct Setup {
        dir: TempDir,
        db: PathBuf,
        replica: PathBuf,
        config: PathBuf,
    }

    fn setup(extra: &str, db_keys: &str) -> Setup {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("app.db");
        let replica = dir.path().join("replica");
        let config = dir.path().join("litestream.yml");
        let text = format!(
            "{extra}dbs:\n  - path: {}\n{db_keys}    replica:\n      path: {}\n",
            db.display(),
            replica.display()
        );
        std::fs::write(&config, text).unwrap();
        Setup {
            dir,
            db,
            replica,
            config,
        }
    }

    // -exec ----------------------------------------------------------------------------

    /// This test binary as the `-exec` child, in `mode`.
    fn child_line() -> String {
        let exe = std::env::current_exe().unwrap();
        format!(
            "'{}' --exact sidecar::exec_child --nocapture --test-threads 1",
            exe.display()
        )
    }

    fn with_child(setup: &Setup, mode: &str, code: i32) -> Command {
        let mut command = lsr();
        command
            .args(["replicate", "-config"])
            .arg(&setup.config)
            .args(["-exec", &child_line()])
            .env("LSR_EXEC_CHILD", mode)
            .env("LSR_EXEC_DB", &setup.db)
            .env("LSR_EXEC_CODE", code.to_string());
        command
    }

    /// The child's side; does nothing unless the daemon started it. Modes: `pin` tries a
    /// TRUNCATE checkpoint (BUSY while the daemon's read transaction is held), writes ten
    /// rows and exits; `signal` waits for a signal and exits; `abort` waits for a signal
    /// and then dies by SIGABRT, a signal the daemon did not send; `ignore` ignores
    /// signals; `die` keeps the default actions; `exit` exits at once. Each prints
    /// `child:` lines on stdout.
    #[test]
    fn exec_child() {
        let Ok(mode) = std::env::var("LSR_EXEC_CHILD") else {
            return;
        };
        let code: i32 = std::env::var("LSR_EXEC_CODE").unwrap().parse().unwrap();
        let say = |line: String| {
            let mut out = std::io::stdout().lock();
            writeln!(out, "child: {line}").unwrap();
            out.flush().unwrap();
        };
        match mode.as_str() {
            "pin" => {
                let db = std::env::var("LSR_EXEC_DB").unwrap();
                let conn = database(Path::new(&db));
                // rusqlite waits 5 s on a busy lock by default; the answer is wanted now.
                conn.busy_timeout(Duration::ZERO).unwrap();
                let busy: i64 = conn
                    .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
                    .unwrap();
                say(format!("truncate busy={busy}"));
                write(&conn, 10);
                drop(conn);
                say("wrote".to_owned());
                std::process::exit(code);
            }
            "signal" | "abort" | "ignore" => {
                use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
                let mut signals =
                    signal_hook::iterator::Signals::new([SIGINT, SIGTERM, SIGHUP]).unwrap();
                say(format!("ready pid={}", std::process::id()));
                for signal in signals.forever() {
                    match mode.as_str() {
                        "signal" => {
                            say(format!("got {signal}"));
                            std::process::exit(code);
                        }
                        "abort" => {
                            say(format!("aborting on {signal}"));
                            std::process::abort();
                        }
                        _ => say(format!("ignored {signal}")),
                    }
                }
            }
            "exit" => {
                say("exiting".to_owned());
                std::process::exit(code);
            }
            "die" => {
                say(format!("ready pid={}", std::process::id()));
                loop {
                    std::thread::park();
                }
            }
            other => panic!("unknown mode {other}"),
        }
    }

    /// The child starts only once the daemon's read transaction pins the WAL (its TRUNCATE
    /// is BUSY), its writes are drained when it exits, and the daemon exits as Go does: 0
    /// for a child that exited 0, 1 for any other code (`replicate.go:92`, `main.go:97-103`),
    /// with the child's own code in the log.
    #[test]
    fn the_child_runs_after_the_pin_and_its_exit_drains_then_exits_0_or_1_as_go() {
        for (code, exit) in [(0, 0), (7, 1)] {
            let setup = setup("", "");
            let app = database(&setup.db);
            write(&app, 20);
            let mut daemon = Daemon::spawn(with_child(&setup, "pin", code));
            let (pinned, _) = daemon.wait_for(&["child: truncate busy=1"]);
            let (status, out) = daemon.finish();
            assert_eq!(status.code(), Some(exit), "{out:#?}");
            assert!(index(&out, "msg=\"initialized db\"") < pinned, "{out:#?}");
            let exited = index(&out, "msg=\"subprocess exited, litestream shutting down\"");
            assert!(index(&out, "child: wrote") < exited, "{out:#?}");
            assert!(exited < index(&out, "msg=\"database drained\""), "{out:#?}");
            assert!(
                out.iter().any(|l| l.contains("msg=\"subprocess ended\"")
                    && l.contains(&format!("code={code}"))),
                "{out:#?}"
            );
            restores_to(setup.dir.path(), &setup.replica, &app);
            assert_eq!(rows(&app).len(), 30);
        }
    }

    /// SIGTERM and SIGHUP reach the child; the daemon drains only after the child exits
    /// (Go `main.go:140-153`), and the host's last writes are on the replica.
    #[test]
    fn a_signal_is_forwarded_and_the_drain_waits_for_the_child() {
        for (signal, number) in [("TERM", 15), ("HUP", 1)] {
            let setup = setup("", "");
            let app = database(&setup.db);
            let mut daemon = Daemon::spawn(with_child(&setup, "signal", 0));
            daemon.wait_for(&["child: ready"]);
            write(&app, 12);
            daemon.signal(signal);
            let (status, out) = daemon.finish();
            assert_eq!(status.code(), Some(0), "{out:#?}");
            let got = index(&out, &format!("child: got {number}"));
            assert!(index(&out, "msg=\"signal received") < got, "{out:#?}");
            assert!(got < index(&out, "msg=\"database drained\""), "{out:#?}");
            restores_to(setup.dir.path(), &setup.replica, &app);
        }
    }

    /// A child that exits non-zero after the forwarded signal fails the run with 1, as Go's
    /// "cannot wait for exec process" does (`replicate.go:150-153`).
    #[test]
    fn a_child_failing_after_the_forwarded_signal_exits_1() {
        let setup = setup("", "");
        drop(database(&setup.db));
        let mut daemon = Daemon::spawn(with_child(&setup, "signal", 3));
        daemon.wait_for(&["child: ready"]);
        daemon.signal("TERM");
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(1), "{out:#?}");
        index(&out, "child: got 15");
        assert!(
            out.iter()
                .any(|l| l.contains("msg=\"subprocess ended\"") && l.contains("code=3")),
            "{out:#?}"
        );
    }

    /// During a shutdown begun by a signal, a child that dies by any signal is clean, the
    /// forwarded one or another (Go `replicate.go:152` checks only for "signal:"); a child
    /// killed by a signal outside a shutdown fails the run with 1.
    #[test]
    fn a_child_dying_by_a_signal_is_clean_in_a_shutdown_and_exits_1_outside_one() {
        let setup = setup("", "");
        drop(database(&setup.db));
        for (mode, signal) in [("die", "number=15"), ("abort", "number=6")] {
            let mut daemon = Daemon::spawn(with_child(&setup, mode, 0));
            daemon.wait_for(&["child: ready"]);
            daemon.signal("TERM");
            let (status, out) = daemon.finish();
            assert_eq!(status.code(), Some(0), "{mode}: {out:#?}");
            let ended = &out[index(&out, "msg=\"subprocess ended\"")];
            assert!(ended.contains(signal), "{mode}: {ended}");
            assert!(ended.contains("during=shutdown"), "{mode}: {ended}");
        }

        let mut daemon = Daemon::spawn(with_child(&setup, "die", 0));
        let (_, ready) = daemon.wait_for(&["child: ready"]);
        let pid: u32 = ready.rsplit('=').next().unwrap().parse().unwrap();
        kill("KILL", pid);
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(1), "{out:#?}");
        let ended = &out[index(&out, "msg=\"subprocess ended\"")];
        assert!(
            ended.contains("number=9") && ended.contains("during=run"),
            "{ended}"
        );
        index(&out, "msg=\"database drained\"");
    }

    /// A child that exits 0 while a database did not start still fails the run with 1, as
    /// Go joins the close error to the child's result.
    #[test]
    fn a_clean_child_with_a_database_that_did_not_start_exits_1() {
        let setup = setup("", "");
        std::fs::write(&setup.db, vec![0x5a_u8; 8192]).unwrap();
        let mut daemon = Daemon::spawn(with_child(&setup, "exit", 0));
        daemon.wait_for(&["child: exiting"]);
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(1), "{out:#?}");
        index(&out, "msg=\"database did not start\"");
        assert!(
            out.iter()
                .any(|l| l.contains("msg=\"subprocess ended\"") && l.contains("code=0")),
            "{out:#?}"
        );
    }

    /// A child that ignores the forwarded signal is killed at the shutdown deadline, as Go
    /// kills it when its shutdown times out; the run is not clean.
    #[test]
    fn a_child_that_ignores_the_signal_is_killed_at_the_deadline() {
        let setup = setup("shutdown-sync-timeout: 200ms\n", "");
        drop(database(&setup.db));
        let mut daemon = Daemon::spawn(with_child(&setup, "ignore", 0));
        daemon.wait_for(&["child: ready"]);
        daemon.signal("TERM");
        daemon.wait_for(&["child: ignored 15"]);
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(1), "{out:#?}");
        index(&out, "killing it");
        let ended = &out[index(&out, "msg=\"subprocess ended\"")];
        assert!(ended.contains("during=kill"), "{ended}");
    }

    /// What Go would run differently from what the line says is refused before any database
    /// is touched; a program that cannot start ends the run after the drain.
    #[test]
    fn exec_lines_go_would_misread_are_refused() {
        let setup = setup("", "");
        drop(database(&setup.db));
        for line in ["myapp; rm -rf /tmp/x", "myapp 2>log", "myapp 'open", "  "] {
            let out = lsr()
                .args(["replicate", "-config"])
                .arg(&setup.config)
                .args(["-exec", line])
                .output()
                .unwrap();
            assert_eq!(out.status.code(), Some(2), "{line:?}");
            let stderr = String::from_utf8(out.stderr).unwrap();
            assert!(stderr.contains("exec command"), "{line:?}: {stderr}");
        }
        assert!(!setup.replica.exists());
        let with_key = setup.dir.path().join("exec.yml");
        let text = std::fs::read_to_string(&setup.config).unwrap();
        std::fs::write(&with_key, format!("exec: myapp\n{text}")).unwrap();
        let out = lsr()
            .args(["replicate", "-once", "-config"])
            .arg(&with_key)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(64));

        let mut command = lsr();
        command
            .args(["replicate", "-config"])
            .arg(&setup.config)
            .args(["-exec", "/nonexistent/lsr-test-program"]);
        let (status, out) = Daemon::spawn(command).finish();
        // Go returns "cannot start exec command" and exits 1 (`replicate.go:477`).
        assert_eq!(status.code(), Some(1), "{out:#?}");
        index(&out, "msg=\"cannot start exec command\"");
        index(&out, "msg=\"database drained\"");
    }

    // addr: /metrics and /healthz -------------------------------------------------------

    /// One response: the status and the body.
    fn get(addr: SocketAddr, request: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (status, body.to_owned())
    }

    fn fetch(addr: SocketAddr, path: &str) -> (u16, String) {
        get(addr, &format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n"))
    }

    /// Asks until `done` holds: the health is written after the tick whose event was seen.
    fn until(addr: SocketAddr, path: &str, done: impl Fn(u16, &str) -> bool) -> (u16, String) {
        for _ in 0..100_000 {
            let (status, body) = fetch(addr, path);
            if done(status, &body) {
                return (status, body);
            }
        }
        panic!("{path} never matched");
    }

    /// The address the daemon logs as bound.
    fn bound(daemon: &mut Daemon) -> SocketAddr {
        let (_, line) = daemon.wait_for(&["msg=\"serving metrics on\""]);
        let addr = line
            .split(' ')
            .find_map(|field| field.strip_prefix("addr="))
            .unwrap();
        addr.parse().unwrap()
    }

    /// One sample of the text format.
    #[derive(Debug)]
    struct Sample {
        name: String,
        labels: BTreeMap<String, String>,
        value: f64,
    }

    /// A parser for the Prometheus text format, written for this test: every sample's family
    /// is declared by a `# TYPE` line before it, label values are quoted with the format's
    /// escapes, and every value is a number.
    fn parse_metrics(text: &str) -> (BTreeMap<String, String>, Vec<Sample>) {
        let mut types = BTreeMap::new();
        let mut samples = Vec::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').unwrap();
                assert!(["counter", "gauge", "histogram"].contains(&kind), "{line}");
                types.insert(name.to_owned(), kind.to_owned());
                continue;
            }
            if line.starts_with("# HELP ") {
                continue;
            }
            let (series, value) = line.rsplit_once(' ').unwrap();
            let (name, labels) = match series.split_once('{') {
                None => (series, BTreeMap::new()),
                Some((name, rest)) => (name, parse_labels(rest.strip_suffix('}').unwrap())),
            };
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| {
                    name.strip_suffix(s)
                        .filter(|f| types.get(*f).is_some_and(|k| k == "histogram"))
                })
                .unwrap_or(name);
            assert!(types.contains_key(family), "no TYPE before {line}");
            samples.push(Sample {
                name: name.to_owned(),
                labels,
                value: value.parse().unwrap(),
            });
        }
        (types, samples)
    }

    fn parse_labels(text: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        let mut chars = text.chars().peekable();
        while chars.peek().is_some() {
            let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
            assert_eq!(chars.next(), Some('"'));
            let mut value = String::new();
            while let Some(c) = chars.next() {
                match c {
                    '"' => break,
                    '\\' => match chars.next().unwrap() {
                        'n' => value.push('\n'),
                        other => value.push(other),
                    },
                    c => value.push(c),
                }
            }
            labels.insert(key, value);
            if chars.peek() == Some(&',') {
                chars.next();
            }
        }
        labels
    }

    fn label_keys(sample: &Sample) -> Vec<&str> {
        sample.labels.keys().map(String::as_str).collect()
    }

    fn one<'a>(samples: &'a [Sample], name: &str, labels: &[(&str, &str)]) -> &'a Sample {
        samples
            .iter()
            .find(|s| {
                s.name == name
                    && labels
                        .iter()
                        .all(|(k, v)| s.labels.get(*k).is_some_and(|got| got == v))
            })
            .unwrap_or_else(|| panic!("no {name} {labels:?} in {samples:#?}"))
    }

    /// `/metrics` parses and carries Go's series names and labels; `/healthz` is 200 with
    /// every database running; only `GET` of the two paths is served; the listener is on
    /// exactly the address given, and the log names it.
    #[test]
    fn metrics_and_health_are_served_on_exactly_the_given_address() {
        let setup = setup("addr: 127.0.0.1:0\n", "");
        let app = database(&setup.db);
        let mut command = lsr();
        command
            .args(["replicate", "-log-level", "debug", "-config"])
            .arg(&setup.config);
        let mut daemon = Daemon::spawn(command);
        let addr = bound(&mut daemon);
        assert!(addr.ip().is_loopback());
        daemon.wait_for(&["msg=\"initialized db\""]);
        write(&app, 10);
        daemon.wait_for(&["msg=uploaded"]);
        let db = setup.db.display().to_string();
        let (status, text) = until(addr, "/metrics", |_, text| {
            let (_, samples) = parse_metrics(text);
            samples
                .iter()
                .any(|s| s.name == "litestream_txid" && s.value > 0.0)
                && samples.iter().any(|s| {
                    s.name == "litestream_replica_operation_total"
                        && s.labels.get("operation").is_some_and(|op| op == "PUT")
                        && s.value > 0.0
                })
        });
        assert_eq!(status, 200);
        go_s_series_are_there(&text, &db);

        let (status, body) = fetch(addr, "/healthz?verbose=1");
        assert_eq!(status, 200, "{body}");
        let health: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(health["status"], "ok");
        assert_eq!(health["dbs"][0]["path"], db.as_str());
        assert_eq!(health["dbs"][0]["state"], "running");
        assert!(health["dbs"][0]["synced_txid"].is_string(), "{body}");
        assert_eq!(health["dbs"][0]["degraded"], serde_json::json!([]));

        let post = get(addr, "POST /metrics HTTP/1.1\r\nHost: test\r\n\r\n");
        assert_eq!(post.0, 405);
        assert_eq!(fetch(addr, "/debug/pprof/").0, 404);
        assert_eq!(get(addr, "GET /metrics SPDY/3\r\n\r\n").0, 400);
        // Linux routes all of 127/8 to the loopback: a listener on every interface would
        // answer here too.
        #[cfg(target_os = "linux")]
        {
            let other = SocketAddr::from(([127, 0, 0, 2], addr.port()));
            assert!(TcpStream::connect(other).is_err(), "{other} answered");
        }
        daemon.signal("TERM");
        let (status, out) = daemon.finish();
        assert_eq!(status.code(), Some(0), "{out:#?}");
    }

    /// Go's series, types and label sets, and a few of the new ones, for database `db`.
    fn go_s_series_are_there(text: &str, db: &str) {
        let (types, samples) = parse_metrics(text);
        for (name, kind) in [
            ("litestream_db_size", "gauge"),
            ("litestream_txid", "gauge"),
            ("litestream_sync_count", "counter"),
            ("litestream_replica_operation_total", "counter"),
            ("litestream_replica_operation_bytes", "counter"),
            ("litestream_replica_operation_duration_seconds", "histogram"),
        ] {
            assert_eq!(types.get(name).map(String::as_str), Some(kind), "{name}");
        }
        for name in [
            "litestream_db_size",
            "litestream_txid",
            "litestream_sync_count",
        ] {
            assert_eq!(label_keys(one(&samples, name, &[("db", db)])), ["db"]);
        }
        let put = [("replica_type", "file"), ("operation", "PUT")];
        let total = one(&samples, "litestream_replica_operation_total", &put);
        assert_eq!(label_keys(total), ["operation", "replica_type"]);
        assert!(one(&samples, "litestream_replica_operation_bytes", &put).value > 0.0);
        let count = one(
            &samples,
            "litestream_replica_operation_duration_seconds_count",
            &put,
        );
        assert!((count.value - total.value).abs() < f64::EPSILON);
        one(
            &samples,
            "lsr_db_state",
            &[("db", db), ("state", "running")],
        );
        one(&samples, "lsr_unreplicated_age_seconds", &[("db", db)]);
        one(&samples, "lsr_wal_bytes", &[("db", db)]);
    }

    /// An empty host is every interface, as Go binds it, and the log says so rather than
    /// Go's `localhost`.
    #[test]
    fn an_empty_host_binds_every_interface_and_the_log_says_which() {
        let setup = setup("addr: \":0\"\n", "");
        drop(database(&setup.db));
        let mut command = lsr();
        command.args(["replicate", "-config"]).arg(&setup.config);
        let mut daemon = Daemon::spawn(command);
        let addr = bound(&mut daemon);
        assert!(addr.ip().is_unspecified(), "{addr}");
        let local = SocketAddr::from(([127, 0, 0, 1], addr.port()));
        assert_eq!(fetch(local, "/healthz").0, 200);
        daemon.signal("TERM");
        assert_eq!(daemon.finish().0.code(), Some(0));
        let bad = setup.dir.path().join("bad.yml");
        let text = std::fs::read_to_string(&setup.config).unwrap();
        std::fs::write(&bad, text.replace("addr: \":0\"", "addr: localhost")).unwrap();
        let out = lsr()
            .args(["replicate", "-config"])
            .arg(&bad)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert!(stderr.contains("must specify port"), "{stderr}");
    }

    /// The WAL deleted under the daemon: a lane degrades (200, in both outputs with its cause
    /// and lane), or, with the response set to stop, the database stops (503, `lsr_stopped`).
    #[test]
    fn health_is_200_while_degraded_and_503_once_stopped() {
        for stop in [false, true] {
            let responses = if stop {
                "litestream-rs:\n  responses:\n    wal-unlinked: stop\n"
            } else {
                ""
            };
            let extra = format!("addr: 127.0.0.1:0\nshutdown-sync-timeout: 100ms\n{responses}");
            let setup = setup(&extra, "    monitor-interval: 20ms\n");
            let app = database(&setup.db);
            write(&app, 5);
            drop(app);
            let mut command = lsr();
            command.args(["replicate", "-config"]).arg(&setup.config);
            let mut daemon = Daemon::spawn(command);
            let addr = bound(&mut daemon);
            daemon.wait_for(&["msg=\"initialized db\""]);
            assert_eq!(fetch(addr, "/healthz").0, 200);
            std::fs::remove_file(format!("{}-wal", setup.db.display())).unwrap();
            let db = setup.db.display().to_string();
            if stop {
                daemon.wait_for(&["msg=\"replication stopped\"", "WalUnlinked"]);
                let (status, body) = until(addr, "/healthz", |status, _| status == 503);
                let health: serde_json::Value = serde_json::from_str(&body).unwrap();
                assert_eq!(status, 503);
                assert_eq!(health["status"], "down");
                assert_eq!(health["down"], serde_json::json!([db]));
                assert_eq!(health["dbs"][0]["stop_cause"], "wal_unlinked");
                let (_, text) = fetch(addr, "/metrics");
                let (_, samples) = parse_metrics(&text);
                let labels = [("db", db.as_str()), ("cause", "wal_unlinked")];
                assert!((one(&samples, "lsr_stopped", &labels).value - 1.0).abs() < f64::EPSILON);
            } else {
                daemon.wait_for(&["msg=\"replication degraded\"", "WalUnlinked"]);
                let (status, body) =
                    until(addr, "/healthz", |_, body| body.contains("wal_unlinked"));
                assert_eq!(status, 200, "{body}");
                let health: serde_json::Value = serde_json::from_str(&body).unwrap();
                let degraded = &health["dbs"][0]["degraded"][0];
                assert_eq!(degraded["cause"], "wal_unlinked");
                let lane = degraded["lane"].as_str().unwrap().to_owned();
                let (_, text) = fetch(addr, "/metrics");
                let (_, samples) = parse_metrics(&text);
                let labels = [
                    ("db", db.as_str()),
                    ("lane", &lane),
                    ("cause", "wal_unlinked"),
                ];
                assert!((one(&samples, "lsr_degraded", &labels).value - 1.0).abs() < f64::EPSILON);
            }
            daemon.signal("TERM");
            let (status, out) = daemon.finish();
            assert_ne!(status.code(), Some(0), "{out:#?}");
        }
    }

    /// A request head past the bound is answered 431 from a fixed buffer, and a connection
    /// past the bound is answered 503; the server keeps serving afterwards.
    #[test]
    fn an_oversized_request_and_too_many_connections_are_refused() {
        let setup = setup("addr: 127.0.0.1:0\n", "");
        drop(database(&setup.db));
        let mut command = lsr();
        command.args(["replicate", "-config"]).arg(&setup.config);
        let mut daemon = Daemon::spawn(command);
        let addr = bound(&mut daemon);
        let long = format!(
            "GET /metrics HTTP/1.1\r\nX-Padding: {}\r\n",
            "a".repeat(lsr_sidecar::http::MAX_REQUEST)
        );
        assert_eq!(get(addr, &long).0, 431);
        assert_eq!(fetch(addr, "/healthz").0, 200);

        // Two more than the bound: the two connections above may still hold their places
        // for a moment after their answers arrived.
        let held: Vec<TcpStream> = (0..lsr_sidecar::http::MAX_CONNECTIONS + 2)
            .map(|_| TcpStream::connect(addr).unwrap())
            .collect();
        let (status, body) = fetch(addr, "/healthz");
        assert_eq!(status, 503, "{body}");
        assert!(body.contains("too many connections"), "{body}");
        drop(held);
        until(addr, "/healthz", |status, _| status == 200);
        daemon.signal("TERM");
        assert_eq!(daemon.finish().0.code(), Some(0));
    }
}

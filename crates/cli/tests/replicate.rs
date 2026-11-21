//! `lsr replicate` as a process: it says when it runs, a signal stops it with a final sync,
//! and a restore of what it wrote equals the database. This is the process the compatibility
//! tests kill.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod replicate {
    use std::io::{BufRead, BufReader, Read};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;

    use lsr_replica::{DynStore, RestoreTarget, restore};
    use lsr_store_fs::FsStore;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn rows(conn: &Connection) -> Vec<(i64, Vec<u8>)> {
        let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn database(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .unwrap();
        lsr_sqlite::app_connection(&conn).unwrap();
        conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        conn
    }

    /// Starts `lsr replicate` and waits for its line saying it runs.
    fn spawn(db: &Path, replica: &Path) -> (Child, BufReader<std::process::ChildStdout>) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_lsr"))
            .arg("replicate")
            .arg(db)
            .arg(replica)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        assert!(line.starts_with("replicating "), "{line:?}");
        (child, stdout)
    }

    fn signal(child: &Child, name: &str) {
        let status = Command::new("kill")
            .arg(format!("-{name}"))
            .arg(child.id().to_string())
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// Each stop signal ends the process cleanly after a final sync: exit 0, a line saying
    /// it drained, and a replica that restores to the database.
    #[test]
    fn a_stop_signal_drains_and_exits_cleanly() {
        for name in ["TERM", "INT", "HUP"] {
            let dir = TempDir::new().unwrap();
            let db = dir.path().join("app.db");
            let replica = dir.path().join("replica");
            let app = database(&db);
            let (mut child, mut stdout) = spawn(&db, &replica);
            for _ in 0..20 {
                app.execute("INSERT INTO t(v) VALUES (randomblob(3000))", [])
                    .unwrap();
            }
            signal(&child, name);
            let status = child.wait().unwrap();
            let mut rest = String::new();
            stdout.read_to_string(&mut rest).unwrap();
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            assert_eq!(status.code(), Some(0), "SIG{name}: {rest} {stderr}");
            assert!(rest.contains("drained"), "SIG{name}: {rest}");
            let store: Arc<dyn DynStore> = Arc::new(FsStore::new(&replica));
            let to = dir.path().join("restored.db");
            restore(&*store, RestoreTarget::Latest, &to).unwrap();
            assert_eq!(
                rows(&Connection::open(&to).unwrap()),
                rows(&app),
                "SIG{name}"
            );
        }
    }

    #[test]
    fn a_missing_database_is_an_error_before_any_signal() {
        let dir = TempDir::new().unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_lsr"))
            .arg("replicate")
            .arg(dir.path().join("missing.db"))
            .arg(dir.path().join("replica"))
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(3));
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert!(stderr.contains("starting the replicator"), "{stderr}");
    }

    #[test]
    fn replicate_takes_a_database_and_a_replica() {
        let out = Command::new(env!("CARGO_BIN_EXE_lsr"))
            .args(["replicate", "only-one"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(64));
    }
}

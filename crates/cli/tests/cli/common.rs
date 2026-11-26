//! The process harness the `lsr replicate` tests share: the binary with a clean
//! environment, a running daemon whose stdout lines are read on a thread, signals, a
//! application database, and a restore compared with it. Every wait is on a line the daemon
//! prints or on the process ending; [`PATIENCE`] only bounds a failure. Each test file
//! uses all of it, so nothing here is dead in either binary.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use lsr_replica::{DynStore, restore};
use lsr_store_fs::FsStore;
use rusqlite::Connection;

/// How long any one line may take: a failure bound, never a pacing wait.
pub const PATIENCE: Duration = Duration::from_secs(60);

/// The `lsr` binary, without the variables that would change what a test runs.
pub fn lsr() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lsr"));
    command
        .env_remove("LOG_LEVEL")
        .env_remove("LITESTREAM_CONFIG")
        .env_remove("LSR_EXEC_CHILD");
    command
}

/// A running daemon and every stdout line it (and an `-exec` child) prints.
pub struct Daemon {
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
    /// Runs `command` with its stdout read line by line.
    pub fn spawn(mut command: Command) -> Self {
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

    /// The next line, failing the test after [`PATIENCE`].
    pub fn next_line(&mut self, waiting_for: &[&str]) -> String {
        match self.lines.recv_timeout(PATIENCE) {
            Ok(line) => {
                self.seen.push(line.clone());
                line
            }
            Err(error) => panic!("no line with {waiting_for:?} ({error}): {:#?}", self.seen),
        }
    }

    /// Waits for a line holding every one of `parts`; its index among the lines seen.
    pub fn wait_for(&mut self, parts: &[&str]) -> (usize, String) {
        let found = |line: &String| parts.iter().all(|p| line.contains(p));
        if let Some(at) = self.seen.iter().position(found) {
            return (at, self.seen[at].clone());
        }
        loop {
            let line = self.next_line(parts);
            if found(&line) {
                return (self.seen.len().saturating_sub(1), line);
            }
        }
    }

    /// Sends the signal `name` (`TERM`, `INT`, ...).
    pub fn signal(&self, name: &str) {
        kill(name, self.child.id());
    }

    /// Waits for the process to end; its status and every line it printed.
    pub fn finish(mut self) -> (ExitStatus, Vec<String>) {
        let status = self.child.wait().unwrap();
        let rest: Vec<String> = self.lines.iter().collect();
        let mut seen = std::mem::take(&mut self.seen);
        seen.extend(rest);
        (status, seen)
    }
}

/// Sends the signal `name` to `pid`.
pub fn kill(name: &str, pid: u32) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(status.success());
}

/// An application database: WAL, the application settings a replicated database needs, one table.
pub fn database(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    lsr_sqlite::app_connection(&conn).unwrap();
    conn.execute_batch("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    conn
}

/// `rows` commits of one 2000-byte row each.
pub fn write(conn: &Connection, rows: usize) {
    for _ in 0..rows {
        conn.execute("INSERT INTO t(v) VALUES (randomblob(2000))", [])
            .unwrap();
    }
}

/// Every row of the table, in order.
pub fn rows(conn: &Connection) -> Vec<(i64, Vec<u8>)> {
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Restores the replica into `dir` (a file named after the replica) and checks it
/// equals `conn`.
pub fn restores_to(dir: &Path, replica: &Path, conn: &Connection) {
    let store: Arc<dyn DynStore> = Arc::new(FsStore::new(replica));
    let to = dir.join(format!(
        "restored-{}.db",
        replica.file_name().unwrap().to_string_lossy()
    ));
    restore(&*store, &to, &lsr_replica::Restore::latest()).unwrap();
    assert_eq!(rows(&Connection::open(&to).unwrap()), rows(conn));
}

//! Child processes for the suite: the test binary runs itself again, with one test selected
//! and its role in the environment, so a writer that is killed or a host that breaks a lock is
//! another process, as in production.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

const ROLE: &str = "LSR_SQLITE_CHILD_ROLE";
const DB: &str = "LSR_SQLITE_CHILD_DB";

/// The role this process was started in, and its database; `None` in the test run itself.
pub fn role() -> Option<(String, PathBuf)> {
    let role = std::env::var(ROLE).ok()?;
    let db = std::env::var_os(DB)?;
    Some((role, PathBuf::from(db)))
}

/// Starts this test binary again running only `test`, in `role` on `db`, its stdout piped.
pub fn spawn(test: &str, role: &str, db: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(DB, db)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Reads the child's stdout until a line that ends with `line` (a blocking read, no clock).
pub fn wait_for_line(child: &mut Child, line: &str) {
    let out = child.stdout.take().unwrap();
    for got in BufReader::new(out).lines() {
        // libtest prints "test <name> ... " before the test runs, on the same line.
        if got.unwrap().ends_with(line) {
            return;
        }
    }
    panic!("the child exited before printing {line:?}");
}

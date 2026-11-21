//! Reading a replica through upstream's read-replica VFS.
//!
//! Upstream ships the VFS (`cmd/litestream-vfs`) as a SQLite loadable extension, not a
//! program: `scripts/build-go-litestream.sh` builds it per Go build as
//! `.upstream/bin/litestream-vfs-<name>.so`. The extension names its replica by the
//! `LITESTREAM_REPLICA_URL` environment variable (`file://<dir>` for a file replica) and
//! registers a VFS called `litestream`; a connection opened with `vfs=litestream` sees the
//! replica's newest transaction, read-only.
//!
//! Loading an extension from Rust needs `unsafe` (rusqlite's `load_extension`), which only
//! the SQLite crate may use, so the host is a small C program, `c/vfs-read.c`, built with the
//! system C compiler against the system SQLite. It prints `PRAGMA integrity_check`, the VFS's
//! `litestream_txid()` and every row of every table in one line format, and writes every page
//! exactly as the VFS's `xRead` returns it. Run without the extension over an ordinary file,
//! it prints the same format, so the rows of a reference database compare line for line.
//!
//! The VFS presents page 1 as a rollback-journal database (header bytes 18 and 19 set to 1)
//! with a random change counter (bytes 24 to 27) on every read, so SQLite never trusts a
//! cached page across polls (`vfs.go`, `ReadAt`). [`VfsView::pages_as`] puts the reference's
//! six bytes back before hashing; every other byte of every page is compared.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::error::CompatError;
use crate::pages::{PageDigest, hash_pages};
use crate::replicator::wait_until;

/// Longest a read through the VFS may take (it polls the replica once a second).
const READ_TIMEOUT: Duration = Duration::from_secs(120);

/// Header bytes of page 1 the VFS rewrites on every read.
const REWRITTEN: [std::ops::Range<usize>; 2] = [18..20, 24..28];

/// What the C host saw of one database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VfsView {
    /// The rows of `PRAGMA integrity_check`.
    pub integrity: Vec<String>,
    /// `litestream_txid()` parsed from its 16 hex digits; `None` without the extension.
    pub txid: Option<u64>,
    /// One line per row of every table, tables in name order, rows in scan order.
    pub rows: Vec<String>,
    /// The file holding every page as read.
    pub pages: PathBuf,
}

/// The outcome of one read through the VFS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VfsRead {
    /// The host read every table and page.
    Read(VfsView),
    /// The extension or SQLite refused: the host's exit status and error line.
    Refused {
        /// What the host said.
        detail: String,
    },
}

impl VfsView {
    /// The page hashes of what the VFS served, with the header bytes it rewrites on page 1
    /// (see the module doc) taken from `reference`, a database file of the same page size.
    ///
    /// # Errors
    /// [`CompatError`] when a file cannot be read or is not whole pages, or when page 1 does
    /// not carry the rollback-journal marker the VFS sets.
    pub fn pages_as(&self, reference: &Path) -> Result<PageDigest, CompatError> {
        let mut served = fs::read(&self.pages).map_err(CompatError::io(&self.pages))?;
        let source = fs::read(reference).map_err(CompatError::io(reference))?;
        let journal = served.get(18..20) == Some([1, 1].as_slice());
        if !journal {
            return Err(CompatError::NotPages {
                path: self.pages.clone(),
                why: "page 1 does not carry the VFS's journal-mode marker".to_owned(),
            });
        }
        for range in REWRITTEN {
            let (Some(to), Some(from)) = (served.get_mut(range.clone()), source.get(range)) else {
                return Err(CompatError::NotPages {
                    path: self.pages.clone(),
                    why: "shorter than a header".to_owned(),
                });
            };
            to.copy_from_slice(from);
        }
        let fixed = self.pages.with_extension("fixed");
        fs::write(&fixed, &served).map_err(CompatError::io(&fixed))?;
        hash_pages(&fixed)
    }
}

/// Builds the C host into `into` with the system C compiler and SQLite (`cc ... -lsqlite3`).
///
/// # Errors
/// [`CompatError::Process`] when the compiler or the SQLite headers are missing.
pub fn build_vfs_reader(into: &Path) -> Result<PathBuf, CompatError> {
    let source = crate::workspace_root()
        .join("crates")
        .join("compat")
        .join("c")
        .join("vfs-read.c");
    let binary = into.join("vfs-read");
    let output = Command::new("cc")
        .arg("-O1")
        .arg("-o")
        .arg(&binary)
        .arg(&source)
        .arg("-lsqlite3")
        .output()
        .map_err(CompatError::io("cc"))?;
    if output.status.success() {
        Ok(binary)
    } else {
        Err(CompatError::Process {
            command: format!("cc {}", source.display()),
            detail: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// Opens the file replica at `replica` through the VFS extension `library` and reads it all;
/// the pages go to `<scratch>/vfs.pages`, the VFS's log to `<scratch>/vfs.log`.
///
/// # Errors
/// [`CompatError`] when the host cannot run, does not finish in time, or prints what it
/// should not.
pub fn read_with_vfs(
    reader: &Path,
    library: &Path,
    replica: &Path,
    scratch: &Path,
) -> Result<VfsRead, CompatError> {
    let replica = fs::canonicalize(replica).map_err(CompatError::io(replica))?;
    let mut cmd = Command::new(reader);
    cmd.arg("file:replica.db?vfs=litestream")
        .arg(scratch.join("vfs.pages"))
        .arg(library)
        .env(
            "LITESTREAM_REPLICA_URL",
            format!("file://{}", replica.display()),
        )
        .env("LITESTREAM_LOG_FILE", scratch.join("vfs.log"));
    run(cmd, scratch, "vfs")
}

/// Reads the ordinary database file `db` the same way, without the extension; the pages go to
/// `<scratch>/plain.pages`.
///
/// # Errors
/// [`CompatError`] when the host cannot run or refuses an ordinary database.
pub fn read_plain(reader: &Path, db: &Path, scratch: &Path) -> Result<VfsView, CompatError> {
    let mut cmd = Command::new(reader);
    cmd.arg(db).arg(scratch.join("plain.pages"));
    match run(cmd, scratch, "plain")? {
        VfsRead::Read(view) => Ok(view),
        VfsRead::Refused { detail } => Err(CompatError::Process {
            command: format!("{} {}", reader.display(), db.display()),
            detail,
        }),
    }
}

/// Runs the host with its output in files (a full dump is megabytes, too much for a pipe the
/// harness only drains at the end) and parses what it printed.
fn run(mut cmd: Command, scratch: &Path, name: &str) -> Result<VfsRead, CompatError> {
    let out = scratch.join(format!("{name}.out"));
    let err = scratch.join(format!("{name}.err"));
    let stdout = fs::File::create(&out).map_err(CompatError::io(&out))?;
    let stderr = fs::File::create(&err).map_err(CompatError::io(&err))?;
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .map_err(CompatError::io(scratch))?;
    let mut status = None;
    let waited = wait_until("the VFS read", READ_TIMEOUT, || {
        status = child.try_wait().map_err(CompatError::io(scratch))?;
        Ok(status.is_some())
    });
    if waited.is_err() {
        // The timeout is the error reported; a failed kill changes nothing about it.
        let _killed = child.kill();
        let _reaped = child.wait();
    }
    waited?;
    let pages = scratch.join(format!("{name}.pages"));
    let said = fs::read_to_string(&err).map_err(CompatError::io(&err))?;
    match status {
        Some(s) if s.success() => {}
        Some(s) => {
            let line = said.lines().last().unwrap_or("").to_owned();
            return Ok(VfsRead::Refused {
                detail: format!("{s}: {line}"),
            });
        }
        None => return Err(CompatError::Timeout("the VFS read".to_owned())),
    }
    let text = fs::read_to_string(&out).map_err(CompatError::io(&out))?;
    parse(&text, pages).map(VfsRead::Read)
}

fn parse(text: &str, pages: PathBuf) -> Result<VfsView, CompatError> {
    let mut view = VfsView {
        integrity: Vec::new(),
        txid: None,
        rows: Vec::new(),
        pages,
    };
    for line in text.lines() {
        let bad = || CompatError::Fixture(format!("the VFS host printed {line:?}"));
        let (kind, rest) = line.split_once('\t').ok_or_else(bad)?;
        match kind {
            "integrity" => view.integrity.push(rest.to_owned()),
            "txid" => view.txid = Some(u64::from_str_radix(rest, 16).map_err(|_hex| bad())?),
            "row" => view.rows.push(rest.to_owned()),
            _ => return Err(bad()),
        }
    }
    Ok(view)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_record_kind() {
        let text = "integrity\tok\ntxid\t000000000000002a\nrow\ta\ti1,t6869\n";
        let view = parse(text, PathBuf::from("p")).expect("parse");
        assert_eq!(view.integrity, ["ok"]);
        assert_eq!(view.txid, Some(42));
        assert_eq!(view.rows, ["a\ti1,t6869"]);
    }

    #[test]
    fn refuses_an_unknown_record() {
        assert!(matches!(
            parse("page\t1\n", PathBuf::from("p")),
            Err(CompatError::Fixture(_))
        ));
        assert!(matches!(
            parse("no tab\n", PathBuf::from("p")),
            Err(CompatError::Fixture(_))
        ));
        assert!(matches!(
            parse("txid\tnot hex\n", PathBuf::from("p")),
            Err(CompatError::Fixture(_))
        ));
    }
}

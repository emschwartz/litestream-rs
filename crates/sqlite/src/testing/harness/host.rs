//! Where the host application runs: in the test process, or in a child process at SQLite's
//! defaults, as an application beside a sidecar runs.
//!
//! [`Placement::InProcess`] is the suite as phase 3 wrote it: the host's connections live in
//! the test process, through the rig's fault VFS, set up as replication asks
//! (`crate::app_connection`: `PERSIST_WAL` on, automatic checkpoints off).
//!
//! [`Placement::SeparateProcess`] moves only the host. The test binary runs itself again as
//! `<exe> child_process_entry --exact --nocapture --test-threads=1` with the database in
//! [`HOST_DB`]; that test's first statement is `if lsr_sqlite::testing::serve_host() { return;
//! }`, so the child serves the host and nothing else. Its connections keep SQLite's defaults
//! (automatic checkpoint every 1000 frames, no `PERSIST_WAL`, the default VFS, busy timeout
//! zero, which is also SQLite's default) and only `synchronous` is set, as the rig's in-process
//! host sets it. The pin, the engine, the stores and the oracle stay in the test process, so
//! every lock the host takes or drops is another process's, as with a sidecar.
//!
//! The protocol is one request line on the child's stdin and one reply line on its stdout,
//! marked with [`MARK`] so libtest's own lines are skipped: `<conn> open`, `<conn> close`,
//! `<conn> exec <sql>`, `<conn> query <sql>` (the first row's integer columns),
//! `<conn> persist-wal-off`, `0 touch` (the host process opens and closes the database and
//! `-shm` with `std::fs`, dropping its own POSIX locks), `0 writers <n> <rows>` (concurrent
//! writers on threads of their own, while the host keeps serving) and `0 exit`. A reply is
//! `ok`, `row <ints>`, or `err <extended code> <message>`, which becomes the same `rusqlite`
//! error the in-process host returns; the writers end with one more line, `done` and a reply.
//! A thread forwards reply lines into bounded channels, so the test waits on a reply (a
//! blocking receive), never on the clock.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, ffi};

use super::{RigError, open_app, suffixed};
use crate::testing::vfs::FaultVfs;

/// Where the host application runs. See the module doc.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Placement {
    /// In the test process, set up as replication asks, through the rig's fault VFS.
    #[default]
    InProcess,
    /// In a child process at SQLite's defaults; the pin and the engine stay in the test process.
    SeparateProcess,
}

impl Placement {
    /// Both placements, for a case that runs in each.
    pub const ALL: [Self; 2] = [Self::InProcess, Self::SeparateProcess];
}

/// The environment variable that makes a test binary the host: the database's path.
pub const HOST_DB: &str = "LSR_SQLITE_HOST_DB";
const HOST_SYNCHRONOUS: &str = "LSR_SQLITE_HOST_SYNCHRONOUS";
/// The marker every reply line of the host process starts with.
pub const MARK: &str = "lsr-host ";
/// What the line that ends the writers starts with, after [`MARK`].
const DONE: &str = "done ";
/// The test a test binary runs as the host process.
pub const CHILD_ENTRY: &str = "child_process_entry";
/// How long a concurrent writer waits on the others' write lock.
const WRITER_BUSY: Duration = Duration::from_secs(30);

/// The connection id of the host's own connection.
pub(super) const APP: u32 = 0;

/// The host's connections, wherever they live.
#[derive(Debug)]
pub(super) enum Host {
    /// In this process.
    Local {
        db: PathBuf,
        vfs: FaultVfs,
        synchronous: &'static str,
        conns: BTreeMap<u32, Connection>,
    },
    /// In a child process.
    Child(HostProcess),
}

impl Host {
    /// A host with no connection open yet.
    pub(super) fn new(
        placement: Placement,
        db: &Path,
        vfs: FaultVfs,
        synchronous: &'static str,
    ) -> Result<Self, RigError> {
        match placement {
            Placement::InProcess => Ok(Self::Local {
                db: db.to_owned(),
                vfs,
                synchronous,
                conns: BTreeMap::new(),
            }),
            Placement::SeparateProcess => Ok(Self::Child(HostProcess::spawn(db, synchronous)?)),
        }
    }

    /// Where it runs.
    pub(super) const fn placement(&self) -> Placement {
        match self {
            Self::Local { .. } => Placement::InProcess,
            Self::Child(_) => Placement::SeparateProcess,
        }
    }

    /// Opens connection `id` (closing one already open under it first).
    pub(super) fn open(&mut self, id: u32) -> Result<(), RigError> {
        match self {
            Self::Local {
                db,
                vfs,
                synchronous,
                conns,
            } => {
                drop(conns.remove(&id));
                conns.insert(id, open_app(db, vfs, synchronous)?);
                Ok(())
            }
            Self::Child(child) => {
                if child.open.contains(&id) {
                    child.request(id, "close", "")?;
                }
                child.request(id, "open", "")?;
                child.open.insert(id);
                Ok(())
            }
        }
    }

    /// Closes connection `id`, if it is open.
    pub(super) fn close(&mut self, id: u32) -> Result<(), RigError> {
        match self {
            Self::Local { conns, .. } => match conns.remove(&id) {
                Some(conn) => conn.close().map_err(|(_conn, err)| RigError::Sqlite(err)),
                None => Ok(()),
            },
            Self::Child(child) => {
                if child.open.remove(&id) {
                    child.request(id, "close", "")?;
                }
                Ok(())
            }
        }
    }

    /// Drops every connection, as a process that died would: in this process without
    /// checking the close (a power loss leaves nothing to close cleanly), in the child by
    /// closing each.
    pub(super) fn discard_all(&mut self) -> Result<(), RigError> {
        match self {
            Self::Local { conns, .. } => {
                conns.clear();
                Ok(())
            }
            Self::Child(child) => {
                for id in std::mem::take(&mut child.open) {
                    child.request(id, "close", "")?;
                }
                Ok(())
            }
        }
    }

    /// The host process, when the host runs in one.
    pub(super) const fn child_mut(&mut self) -> Option<&mut HostProcess> {
        match self {
            Self::Local { .. } => None,
            Self::Child(child) => Some(child),
        }
    }

    /// Whether connection `id` is open.
    pub(super) fn is_open(&self, id: u32) -> bool {
        match self {
            Self::Local { conns, .. } => conns.contains_key(&id),
            Self::Child(child) => child.open.contains(&id),
        }
    }

    /// The connection, when it lives in this process.
    pub(super) fn local(&self, id: u32) -> Option<&Connection> {
        match self {
            Self::Local { conns, .. } => conns.get(&id),
            Self::Child(_) => None,
        }
    }

    /// Runs `sql` (one or more statements, no newline) on connection `id`.
    pub(super) fn exec(&mut self, id: u32, sql: &str) -> Result<(), RigError> {
        match self {
            Self::Local { conns, .. } => Ok(local(conns, id)?.execute_batch(sql)?),
            Self::Child(child) => child.request(id, "exec", sql).map(drop),
        }
    }

    /// The first row of `sql` on connection `id`, every column an integer.
    pub(super) fn query(&mut self, id: u32, sql: &str) -> Result<Vec<i64>, RigError> {
        match self {
            Self::Local { conns, .. } => Ok(first_row(local(conns, id)?, sql)?),
            Self::Child(child) => {
                let row = child.request(id, "query", sql)?;
                row.split_whitespace()
                    .map(|v| v.parse().map_err(|_bad| reply_error(&row)))
                    .collect()
            }
        }
    }

    /// Turns `SQLITE_FCNTL_PERSIST_WAL` off on connection `id`.
    pub(super) fn persist_wal_off(&mut self, id: u32) -> Result<(), RigError> {
        match self {
            Self::Local { conns, .. } => Ok(persist_wal_off(local(conns, id)?)?),
            Self::Child(child) => child.request(id, "persist-wal-off", "").map(drop),
        }
    }

    /// The host's process opens and closes the database and `-shm` with `std::fs`. In this
    /// process that would drop the pin's locks too, which is the hazard the descriptor rule
    /// exists for, so only the separate host does it.
    pub(super) fn touch(&mut self) -> Result<(), RigError> {
        match self {
            Self::Local { .. } => Err(RigError::State(
                "open the database files with std::fs in the pin's own process",
            )),
            Self::Child(child) => child.request(APP, "touch", "").map(drop),
        }
    }
}

fn local(conns: &BTreeMap<u32, Connection>, id: u32) -> Result<&Connection, RigError> {
    conns
        .get(&id)
        .ok_or(RigError::State("use a host connection that is not open"))
}

fn first_row(conn: &Connection, sql: &str) -> Result<Vec<i64>, rusqlite::Error> {
    conn.query_row(sql, [], |row| {
        (0..row.as_ref().column_count())
            .map(|i| row.get::<_, i64>(i))
            .collect()
    })
}

fn persist_wal_off(conn: &Connection) -> Result<(), rusqlite::Error> {
    let mut off = 0;
    conn.file_control(
        Some(rusqlite::MAIN_DB),
        rusqlite::FileControl::PersistWal(&mut off),
    )
}

fn reply_error(reply: &str) -> RigError {
    RigError::Host(format!("an unexpected reply: {reply:?}"))
}

/// The host in a child process: its pipes, the reply channels and which connections it holds.
#[derive(Debug)]
pub(super) struct HostProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    replies: Receiver<String>,
    done: Receiver<String>,
    open: BTreeSet<u32>,
}

impl HostProcess {
    fn spawn(db: &Path, synchronous: &str) -> Result<Self, RigError> {
        let spawn = |err: std::io::Error| RigError::Host(format!("cannot start the host: {err}"));
        let exe = std::env::current_exe().map_err(spawn)?;
        let mut child = Command::new(exe)
            .args([CHILD_ENTRY, "--exact", "--nocapture", "--test-threads=1"])
            .env(HOST_DB, db)
            .env(HOST_SYNCHRONOUS, synchronous)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(spawn)?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(RigError::Host("the host's pipes".to_owned()));
        };
        let (tx, replies) = sync_channel(4);
        let (done_tx, done) = sync_channel(1);
        std::thread::spawn(move || forward(stdout, &tx, &done_tx));
        Ok(Self {
            child,
            stdin: Some(stdin),
            replies,
            done,
            open: BTreeSet::new(),
        })
    }

    fn send(&mut self, id: u32, verb: &str, arg: &str) -> Result<(), RigError> {
        let stdin = self
            .stdin
            .as_mut()
            .ok_or(RigError::State("talk to a host that was told to exit"))?;
        writeln!(stdin, "{id} {verb} {arg}")
            .and_then(|()| stdin.flush())
            .map_err(|err| RigError::Host(format!("the host's stdin: {err}")))
    }

    /// One request and its reply: what followed `ok` or `row`, or the host's error.
    fn request(&mut self, id: u32, verb: &str, arg: &str) -> Result<String, RigError> {
        self.send(id, verb, arg)?;
        let reply = self
            .replies
            .recv()
            .map_err(|_gone| RigError::Host(format!("the host exited during {verb}")))?;
        parse_reply(&reply)
    }

    /// Starts `writers` concurrent writers of `rows` commits each; [`Self::writers_done`] says
    /// when they finished.
    pub(super) fn start_writers(&mut self, writers: u32, rows: u32) -> Result<(), RigError> {
        self.request(APP, "writers", &format!("{writers} {rows}"))
            .map(drop)
    }

    /// Whether the writers [`Self::start_writers`] started have all finished, without waiting.
    pub(super) fn writers_done(&mut self) -> Result<bool, RigError> {
        match self.done.try_recv() {
            Ok(reply) => parse_reply(&reply).map(|_row| true),
            Err(TryRecvError::Empty) => Ok(false),
            Err(TryRecvError::Disconnected) => Err(RigError::Host(
                "the host exited while its writers ran".to_owned(),
            )),
        }
    }
}

impl Drop for HostProcess {
    fn drop(&mut self) {
        drop(self.send(APP, "exit", ""));
        // Its stdin closing ends the host's loop even if the exit line was lost.
        drop(self.stdin.take());
        drop(self.child.wait());
    }
}

/// Forwards the host's reply lines, without their marker, until it exits: the writers' end
/// to `done`, every other reply to `tx`.
fn forward(stdout: ChildStdout, tx: &SyncSender<String>, done: &SyncSender<String>) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { return };
        // libtest prints "test <name> ... " on the line the first reply joins.
        let Some((_, reply)) = line.split_once(MARK) else {
            continue;
        };
        let sent = match reply.strip_prefix(DONE) {
            Some(end) => done.send(end.to_owned()),
            None => tx.send(reply.to_owned()),
        };
        if sent.is_err() {
            return;
        }
    }
}

fn parse_reply(reply: &str) -> Result<String, RigError> {
    let (word, rest) = reply.split_once(' ').unwrap_or((reply, ""));
    match word {
        "ok" | "row" => Ok(rest.to_owned()),
        "err" => {
            let (code, message) = rest.split_once(' ').unwrap_or((rest, ""));
            let code = code.parse().map_err(|_bad| reply_error(reply))?;
            Err(RigError::Sqlite(rusqlite::Error::SqliteFailure(
                ffi::Error::new(code),
                Some(message.to_owned()),
            )))
        }
        _ => Err(reply_error(reply)),
    }
}

/// Serves as the host when this process was started as one (see the module doc), and says
/// whether it was. A test binary that uses [`Placement::SeparateProcess`] calls it first in
/// its `child_process_entry` test.
#[must_use]
pub fn serve_host() -> bool {
    let Some(db) = std::env::var_os(HOST_DB) else {
        return false;
    };
    let mut server = Server {
        db: PathBuf::from(db),
        synchronous: std::env::var(HOST_SYNCHRONOUS).unwrap_or_else(|_unset| "NORMAL".to_owned()),
        conns: BTreeMap::new(),
    };
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let Some(reply) = server.handle(&line) else {
            break;
        };
        if !say(&reply_line(reply)) {
            break;
        }
    }
    true
}

/// Writes one reply line under stdout's lock, so the writers' line never splits another.
fn say(line: &str) -> bool {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{MARK}{line}")
        .and_then(|()| out.flush())
        .is_ok()
}

type Reply = Result<String, rusqlite::Error>;

fn reply_line(reply: Reply) -> String {
    reply.unwrap_or_else(|err| {
        let code = err.sqlite_error().map_or(-1, |e| e.extended_code);
        let message = err.to_string().replace('\n', " ");
        format!("err {code} {message}")
    })
}

/// The host process's side: its database and its connections.
struct Server {
    db: PathBuf,
    synchronous: String,
    conns: BTreeMap<u32, Connection>,
}

impl Server {
    /// The reply to one request line, or `None` to exit.
    fn handle(&mut self, line: &str) -> Option<Reply> {
        let mut parts = line.splitn(3, ' ');
        let (id, verb) = (parts.next()?, parts.next()?);
        let arg = parts.next().unwrap_or("");
        let Ok(id) = id.parse::<u32>() else {
            return Some(Ok(format!("bad request {line:?}")));
        };
        let ok = |()| "ok".to_owned();
        let reply = match verb {
            "exit" => return None,
            "open" => self.open(id).map(ok),
            "close" => self.close(id).map(ok),
            "exec" => self.conn(id).and_then(|c| c.execute_batch(arg)).map(ok),
            "query" => self.conn(id).and_then(|c| first_row(c, arg)).map(|row| {
                let row: Vec<String> = row.iter().map(i64::to_string).collect();
                format!("row {}", row.join(" "))
            }),
            "persist-wal-off" => self.conn(id).and_then(persist_wal_off).map(ok),
            "touch" => Ok(touch(&self.db)),
            "writers" => self.writers(arg),
            other => Ok(format!("bad verb {other:?}")),
        };
        Some(reply)
    }

    fn open(&mut self, id: u32) -> Result<(), rusqlite::Error> {
        let conn = connect(&self.db, &self.synchronous, Duration::ZERO)?;
        drop(self.conns.insert(id, conn));
        Ok(())
    }

    fn close(&mut self, id: u32) -> Result<(), rusqlite::Error> {
        match self.conns.remove(&id) {
            Some(conn) => conn.close().map_err(|(_conn, err)| err),
            None => Ok(()),
        }
    }

    fn conn(&self, id: u32) -> Result<&Connection, rusqlite::Error> {
        self.conns.get(&id).ok_or(rusqlite::Error::InvalidQuery)
    }

    /// Starts `writers` threads, each on a connection of its own, committing `rows` rows each
    /// at once; a thread that waits for them says [`DONE`] and how they ended.
    fn writers(&self, arg: &str) -> Reply {
        let mut nums = arg.split(' ').map(str::parse::<u32>);
        let (Some(Ok(writers)), Some(Ok(rows))) = (nums.next(), nums.next()) else {
            return Err(rusqlite::Error::InvalidQuery);
        };
        let (db, synchronous) = (self.db.clone(), self.synchronous.clone());
        std::thread::spawn(move || {
            let ended = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..writers)
                    .map(|w| {
                        let (db, synchronous) = (&db, synchronous.as_str());
                        scope.spawn(move || write_rows(db, synchronous, w, rows))
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or(Err(rusqlite::Error::InvalidQuery)))
                    .collect::<Result<Vec<()>, _>>()
                    .map(|_all| "ok".to_owned())
            });
            say(&format!("{DONE}{}", reply_line(ended)));
        });
        Ok("ok".to_owned())
    }
}

fn connect(db: &Path, synchronous: &str, busy: Duration) -> Result<Connection, rusqlite::Error> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let conn = Connection::open_with_flags(db, flags)?;
    conn.busy_timeout(busy)?;
    conn.pragma_update(None, "synchronous", synchronous)?;
    Ok(conn)
}

/// One of the concurrent writers: `rows` commits on a connection of its own.
fn write_rows(db: &Path, synchronous: &str, writer: u32, rows: u32) -> Result<(), rusqlite::Error> {
    let conn = connect(db, synchronous, WRITER_BUSY)?;
    for n in 0..rows {
        conn.execute(
            "INSERT INTO t(v) VALUES (randomblob(?1))",
            [writer_row_bytes(writer, n)],
        )?;
    }
    Ok(())
}

/// The size of writer `writer`'s `n`th row: every writer's rows differ in size.
pub(super) fn writer_row_bytes(writer: u32, n: u32) -> u32 {
    writer
        .saturating_mul(300)
        .saturating_add(n.saturating_mul(7))
        .saturating_add(200)
}

/// Opens and closes the database and `-shm` with `std::fs` in the host's process.
fn touch(db: &Path) -> String {
    for path in [db.to_owned(), suffixed(db, "-shm")] {
        #[expect(
            clippy::disallowed_methods,
            reason = "the host's own process drops its own locks on purpose; the pin is another process's"
        )]
        let opened = std::fs::File::open(&path);
        if let Err(err) = opened {
            return format!("bad touch of {}: {err}", path.display());
        }
    }
    "ok".to_owned()
}

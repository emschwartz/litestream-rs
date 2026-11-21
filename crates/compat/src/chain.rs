//! A chain the Rust actor writes from a live SQLite database, for the Go compatibility checks.
//!
//! Test support, over the production backend [`Sqlite`]. A [`Site`] is the application's side
//! (the database, an open application connection, the meta directory Go would use and the
//! replica directory); a [`Chain`] is the actor over a [`Sqlite`] of that site, staging L0 files
//! in `<meta>/ltx/0` (Go's own layout, `db.go:600`) and publishing to the replica.
//!
//! The clock is virtual and ends near the real time (`clock_ending_now`): the maintenance
//! windows are seconds long, and waiting for them would make the run slow and its shape racy.
//! Every file's creation time (its mtime, what Go's `-timestamp` restore reads) is that virtual
//! time, which stays in the past so a Go process started afterwards never sees a file from the future.
//!
//! After every commit the actor ticks until it reports idle, and the run records the page hashes
//! of a copy of the database taken at that moment ([`Point`]): the claim under test is that a
//! restore at that transaction, by any reader, has those pages.

use std::collections::BTreeSet;
use std::fs;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lsr_core::conformance::block_on;
use lsr_core::{
    Actor, ActorConfig, CheckpointConfig, CheckpointMode, Env, Event, L0Retention, Level,
    Maintenance, Responses, SyncConfig, Tick, UploadConfig,
};
use lsr_ltx::{Timestamp, Txid};
use lsr_sqlite::Sqlite;
use lsr_store_fs::FsStore;
use rusqlite::Connection;
use tempfile::TempDir;

use crate::error::CompatError;
use crate::pages::hash_pages;
use crate::replica;
use crate::workload::SplitMix;

/// Virtual milliseconds between two commits of a run.
pub const STEP_MS: i64 = 100;

/// Most ticks one settle may take before the run is called stuck.
const MAX_TICKS: u32 = 400;

/// The newest L0 file's WAL position is checked against this many bytes of cap, so a large
/// commit spans several files.
const SYNC_CAP: u64 = 64 * 1024;

/// One application commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A row with a blob of this many bytes (more than a page spills to overflow pages).
    Insert {
        /// Blob length.
        bytes: u32,
    },
    /// Replaces the newest row's blob.
    UpdateTail {
        /// New blob length.
        bytes: u32,
    },
    /// A one-page change in a second table.
    Tiny,
    /// Deletes four of every five rows: pages move to the freelist, the file does not shrink.
    DeleteMost,
    /// `VACUUM`: rewrites the file smaller.
    Vacuum,
}

/// A checkpoint the application runs itself, behind the replicator's back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outside {
    /// `PRAGMA wal_checkpoint(PASSIVE)`.
    Passive,
    /// `PRAGMA wal_checkpoint(RESTART)`.
    Restart,
    /// `PRAGMA wal_checkpoint(TRUNCATE)`.
    Truncate,
}

impl Outside {
    const fn pragma(self) -> &'static str {
        match self {
            Self::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            Self::Restart => "PRAGMA wal_checkpoint(RESTART)",
            Self::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        }
    }
}

/// The application's side: a database in WAL mode with autocheckpoint off, an open connection
/// that stays open across replicator restarts, and the directories a replicator uses.
#[derive(Debug)]
pub struct Site {
    dir: TempDir,
    /// The database file; its name fixes the meta directory.
    pub db: PathBuf,
    /// Go's meta directory beside the database; L0 files live in `ltx/0` under it.
    pub meta: PathBuf,
    /// The file replica.
    pub replica: PathBuf,
    /// The application's connection.
    pub app: Connection,
    page_size: u32,
    rng: SplitMix,
}

impl Site {
    /// A new database at `page_size` with two tables and one committed row.
    ///
    /// # Errors
    /// [`CompatError`] when the directory or SQLite refuses.
    pub fn create(page_size: u32, seed: u64) -> Result<Self, CompatError> {
        let dir = tempfile::tempdir().map_err(CompatError::io(std::env::temp_dir()))?;
        let db = dir.path().join("app.db");
        let meta = dir.path().join(".app.db-litestream");
        let replica = dir.path().join("replica");
        let app = Connection::open(&db)?;
        app.busy_timeout(Duration::from_secs(2))?;
        app.pragma_update(None, "page_size", page_size)?;
        app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))?;
        app.pragma_update(None, "wal_autocheckpoint", 0)?;
        app.execute_batch(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);
             CREATE TABLE u(id INTEGER PRIMARY KEY, w BLOB);
             INSERT INTO t(v) VALUES (randomblob(100));",
        )?;
        Ok(Self {
            dir,
            db,
            meta,
            replica,
            app,
            page_size,
            rng: SplitMix::new(seed),
        })
    }

    /// The directory holding everything of this site.
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// The page size.
    pub const fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Applies one commit.
    ///
    /// # Errors
    /// [`CompatError::Sqlite`] when SQLite refuses.
    pub fn commit(&mut self, shape: Shape) -> Result<(), CompatError> {
        match shape {
            Shape::Insert { bytes } => {
                self.app
                    .execute("INSERT INTO t(v) VALUES (randomblob(?1))", [bytes])?;
            }
            Shape::UpdateTail { bytes } => {
                self.app.execute(
                    "UPDATE t SET v = randomblob(?1) WHERE id = (SELECT max(id) FROM t)",
                    [bytes],
                )?;
            }
            Shape::Tiny => {
                self.app
                    .execute("INSERT INTO u(w) VALUES (randomblob(10))", [])?;
            }
            Shape::DeleteMost => {
                self.app.execute("DELETE FROM t WHERE id % 5 != 0", [])?;
            }
            Shape::Vacuum => self.app.execute_batch("VACUUM")?,
        }
        Ok(())
    }

    /// Runs a checkpoint on the application connection; returns SQLite's busy, log and
    /// checkpointed counts.
    ///
    /// # Errors
    /// [`CompatError::Sqlite`] when SQLite refuses.
    pub fn checkpoint(&self, mode: Outside) -> Result<(i64, i64, i64), CompatError> {
        Ok(self.app.query_row(mode.pragma(), [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?)
    }

    /// The database's size in pages, as SQLite counts it (WAL included).
    ///
    /// # Errors
    /// [`CompatError::Sqlite`] when SQLite refuses.
    pub fn page_count(&self) -> Result<u32, CompatError> {
        Ok(self
            .app
            .query_row("PRAGMA page_count", [], |row| row.get(0))?)
    }

    /// The page hashes of the database as of now: the main file and the WAL copied together,
    /// checkpointed away from the original, hashed.
    ///
    /// # Errors
    /// [`CompatError`] when a copy, SQLite or the hashing fails.
    pub fn hash_now(&self) -> Result<Vec<String>, CompatError> {
        let scratch = self.dir.path().join("scratch");
        fs::create_dir_all(&scratch).map_err(CompatError::io(&scratch))?;
        let copy = scratch.join("copy.db");
        for suffix in ["", "-wal", "-shm"] {
            let stale = scratch.join(format!("copy.db{suffix}"));
            match fs::remove_file(&stale) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(CompatError::io(&stale)(err)),
            }
        }
        fs::copy(&self.db, &copy).map_err(CompatError::io(&copy))?;
        let wal = PathBuf::from(format!("{}-wal", self.db.display()));
        let wal_copy = scratch.join("copy.db-wal");
        fs::copy(&wal, &wal_copy).map_err(CompatError::io(&wal_copy))?;
        let conn = Connection::open(&copy)?;
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))?;
        drop(conn);
        Ok(hash_pages(&copy)?.page_hashes)
    }
}

/// The database as it was when the actor last went idle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Point {
    /// The newest staged transaction at that moment.
    pub txid: Txid,
    /// The virtual time, in Unix milliseconds, a Go `-timestamp` restore can target.
    pub at_ms: i64,
    /// The page hashes of the database then.
    pub page_hashes: Vec<String>,
}

/// What a finished run leaves.
#[derive(Debug)]
pub struct Written {
    /// The application's side, with its connection still open.
    pub site: Site,
    /// One point per transaction the run reached.
    pub points: Vec<Point>,
    /// Every non-idle event the actor reported.
    pub events: Vec<Event>,
    /// A copy of every LTX file that ever appeared in the replica or the staging directory.
    pub archive: Vec<PathBuf>,
}

/// The actor, its live database and its two stores.
#[derive(Debug)]
pub struct Chain {
    /// The application's side.
    pub site: Site,
    live: Sqlite,
    staging: FsStore,
    remote: FsStore,
    actor: Actor,
    clock: i64,
    points: Vec<Point>,
    events: Vec<Event>,
    archive: Vec<PathBuf>,
    seen: BTreeSet<String>,
}

/// The real time in Unix milliseconds.
///
/// # Errors
/// [`CompatError::Fixture`] when the system clock predates 1970.
#[expect(
    clippy::disallowed_methods,
    reason = "the real time a Go process will see, to place a virtual clock before it"
)]
pub fn real_now_ms() -> Result<i64, CompatError> {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| CompatError::Fixture(format!("the system clock predates 1970: {e}")))?;
    i64::try_from(since.as_millis())
        .map_err(|e| CompatError::Fixture(format!("time overflow: {e}")))
}

/// A start time such that `commits` commits of [`STEP_MS`] end a second before now.
///
/// # Errors
/// [`CompatError::Fixture`] when the system clock predates 1970.
pub fn clock_ending_now(commits: u32) -> Result<i64, CompatError> {
    let span = STEP_MS.saturating_mul(i64::from(commits));
    Ok(real_now_ms()?.saturating_sub(span).saturating_sub(1000))
}

fn config() -> Result<ActorConfig, CompatError> {
    let secs = Duration::from_secs;
    let maintenance = Maintenance::new(
        vec![secs(1), secs(2)],
        secs(3),
        secs(10),
        L0Retention::After(secs(2)),
    )
    .map_err(|e| CompatError::Fixture(format!("maintenance schedule: {e}")))?;
    Ok(ActorConfig {
        sync: SyncConfig {
            cap: NonZeroU64::new(SYNC_CAP),
            node_id: 0,
        },
        checkpoint: CheckpointConfig {
            interval: Some(secs(2)),
            min_pages: NonZeroU32::new(40),
            truncate_pages: NonZeroU32::new(120),
        },
        upload: UploadConfig::default(),
        maintenance,
        retry_interval: Duration::from_millis(100),
        shutdown_grace: secs(10),
        // A fixture run must fail loudly on any persistent fault, not degrade.
        responses: Responses::STRICT,
    })
}

fn pct(n: u32, p: u32) -> u32 {
    n.saturating_mul(p).checked_div(100).unwrap_or(0)
}

impl Chain {
    /// An actor for `site` whose virtual clock starts at `clock_ms`. The actor boots on its first
    /// tick, so a site that already has a replica and meta directory is continued.
    ///
    /// # Errors
    /// [`CompatError`] when SQLite or the schedule refuses.
    pub fn start(site: Site, clock_ms: i64) -> Result<Self, CompatError> {
        let live = Sqlite::open(&site.db)?;
        Ok(Self {
            staging: FsStore::new(&site.meta),
            remote: FsStore::new(&site.replica),
            actor: Actor::new(config()?),
            site,
            live,
            clock: clock_ms,
            points: Vec::new(),
            events: Vec::new(),
            archive: Vec::new(),
            seen: BTreeSet::new(),
        })
    }

    /// The virtual time now.
    pub const fn clock_ms(&self) -> i64 {
        self.clock
    }

    /// Runs the actor until it reports nothing ready.
    ///
    /// # Errors
    /// [`CompatError::Fixture`] when the actor retries, resnapshots into a halt or does not
    /// settle: a healthy run on a local disk does none of these.
    pub fn settle(&mut self) -> Result<(), CompatError> {
        for _ in 0..MAX_TICKS {
            let (mut wal, mut db, mut ck) =
                (self.live.clone(), self.live.clone(), self.live.clone());
            let env = Env {
                wal: &mut wal,
                db: &mut db,
                ck: &mut ck,
                staging: &self.staging,
                remote: &self.remote,
            };
            let now = Timestamp::from_unix_millis(self.clock);
            match block_on(self.actor.tick(env, now)) {
                Tick::Ran {
                    event: Event::Idle, ..
                } => return Ok(()),
                Tick::Ran {
                    event: Event::Retrying { fault, until },
                    ..
                } => {
                    return Err(CompatError::Fixture(format!(
                        "the actor is retrying {fault:?} until {until:?}"
                    )));
                }
                Tick::Ran { event, .. } => self.events.push(event),
                Tick::Halted(cause) => {
                    return Err(CompatError::Fixture(format!("the actor halted: {cause:?}")));
                }
                Tick::Drained { .. } | Tick::DeadlineReached(_) => {
                    return Err(CompatError::Fixture("the actor drained unasked".to_owned()));
                }
            }
        }
        Err(CompatError::Timeout("the actor to go idle".to_owned()))
    }

    /// The newest staged transaction.
    fn staged_tip(&self) -> Result<Txid, CompatError> {
        replica::list(&self.site.meta)?
            .files
            .iter()
            .filter(|file| file.key.level() == Level::ZERO)
            .map(|file| file.key.range().max())
            .max()
            .ok_or_else(|| CompatError::Fixture("nothing was staged".to_owned()))
    }

    /// Keeps a copy of every file not seen before: compaction and retention delete files that
    /// Go must still be shown to accept.
    fn archive_new(&mut self) -> Result<(), CompatError> {
        for root in [&self.site.meta, &self.site.replica] {
            for file in replica::list(root)?.files {
                let name = format!("{}-{}", file.key.level(), file.key.file_name());
                if !self.seen.insert(name.clone()) {
                    continue;
                }
                let dir = self.site.dir().join("archive");
                fs::create_dir_all(&dir).map_err(CompatError::io(&dir))?;
                let to = dir.join(name);
                match fs::copy(root.join(&file.rel), &to) {
                    Ok(_) => self.archive.push(to),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(CompatError::io(&to)(err)),
                }
            }
        }
        Ok(())
    }

    /// One application commit, then the actor catches up and the state is recorded.
    ///
    /// # Errors
    /// [`CompatError`] from SQLite, the actor or the copy.
    pub fn commit(&mut self, shape: Shape) -> Result<(), CompatError> {
        self.site.commit(shape)?;
        self.catch_up()
    }

    /// Advances the clock, settles, archives new files and records a point when the newest
    /// transaction changed.
    ///
    /// # Errors
    /// [`CompatError`] from the actor or the copy.
    pub fn catch_up(&mut self) -> Result<(), CompatError> {
        self.clock = self.clock.saturating_add(STEP_MS);
        self.settle()?;
        self.archive_new()?;
        let txid = self.staged_tip()?;
        if self.points.last().is_none_or(|last| last.txid != txid) {
            let page_hashes = self.site.hash_now()?;
            self.points.push(Point {
                txid,
                at_ms: self.clock,
                page_hashes,
            });
        }
        Ok(())
    }

    /// Commits big inserts until the actor has run a TRUNCATE checkpoint and synced what it
    /// wrote, so the run ends on a WAL restart. The actor only truncates after a PASSIVE
    /// checkpoint failed at the truncate threshold, so an application reader holds the log
    /// until a checkpoint reports busy and then lets go.
    ///
    /// # Errors
    /// [`CompatError`] when SQLite refuses or no truncate happened within the bound.
    pub fn grow_until_truncate(&mut self) -> Result<(), CompatError> {
        let from = self.events.len();
        let bytes = self.site.page_size().saturating_mul(8);
        let reader = Connection::open(&self.site.db)?;
        reader.execute_batch("BEGIN; SELECT count(*) FROM t;")?;
        let mut reader = Some(reader);
        for _ in 0..80 {
            self.commit(Shape::Insert { bytes })?;
            let ran = |mode, busy| {
                self.events
                    .iter()
                    .skip(from)
                    .any(|e| *e == Event::Checkpointed { mode, busy })
            };
            if ran(CheckpointMode::Truncate, false) {
                return Ok(());
            }
            if ran(CheckpointMode::Passive, true) || ran(CheckpointMode::Truncate, true) {
                reader = None;
            }
        }
        drop(reader);
        let seen: Vec<&Event> = self.events.iter().skip(from).collect();
        Err(CompatError::Fixture(format!(
            "no truncate checkpoint ran: {seen:?}"
        )))
    }

    /// A seeded run of `commits` commits that grows the database, frees most of it, vacuums it
    /// smaller and grows it again, with checkpoints the application runs itself between them.
    ///
    /// # Errors
    /// [`CompatError`] from any commit.
    pub fn run(&mut self, commits: u32) -> Result<(), CompatError> {
        let page = u64::from(self.site.page_size());
        let outside = [
            (pct(commits, 12), Outside::Passive),
            (pct(commits, 37), Outside::Restart),
            (pct(commits, 62), Outside::Truncate),
            (pct(commits, 87), Outside::Passive),
        ];
        let (delete_at, vacuum_at) = (pct(commits, 40), pct(commits, 45));
        for i in 0..commits {
            let bytes =
                |site: &mut Site, bound: u64| u32::try_from(site.rng.below(bound)).unwrap_or(0);
            let shape = if i == delete_at {
                Shape::DeleteMost
            } else if i == vacuum_at {
                Shape::Vacuum
            } else {
                match self.site.rng.below(10) {
                    0 | 1 => Shape::Tiny,
                    2 | 3 => Shape::UpdateTail {
                        bytes: bytes(&mut self.site, page.saturating_mul(3)),
                    },
                    4 => Shape::Insert {
                        bytes: bytes(&mut self.site, page.saturating_mul(5))
                            .saturating_add(u32::try_from(page).unwrap_or(0)),
                    },
                    _ => Shape::Insert {
                        bytes: bytes(&mut self.site, page).saturating_add(16),
                    },
                }
            };
            self.site.commit(shape)?;
            for (at, mode) in outside {
                if at == i {
                    self.site.checkpoint(mode)?;
                }
            }
            self.catch_up()?;
        }
        Ok(())
    }

    /// Drains the actor (everything committed is staged and published) and hands back what the
    /// run recorded; the live connections close, the application's stays open.
    ///
    /// # Errors
    /// [`CompatError::Fixture`] when the drain halts or does not finish.
    pub fn finish(mut self) -> Result<Written, CompatError> {
        let deadline = self
            .actor
            .shutdown_deadline(Timestamp::from_unix_millis(self.clock));
        self.actor.begin_shutdown(deadline);
        for _ in 0..MAX_TICKS {
            let (mut wal, mut db, mut ck) =
                (self.live.clone(), self.live.clone(), self.live.clone());
            let env = Env {
                wal: &mut wal,
                db: &mut db,
                ck: &mut ck,
                staging: &self.staging,
                remote: &self.remote,
            };
            let now = Timestamp::from_unix_millis(self.clock);
            match block_on(self.actor.tick(env, now)) {
                Tick::Drained { .. } => {
                    self.archive_new()?;
                    let Self {
                        site,
                        points,
                        events,
                        archive,
                        ..
                    } = self;
                    return Ok(Written {
                        site,
                        points,
                        events,
                        archive,
                    });
                }
                Tick::Ran { event, .. } => self.events.push(event),
                Tick::Halted(cause) => {
                    return Err(CompatError::Fixture(format!("the drain halted: {cause:?}")));
                }
                Tick::DeadlineReached(left) => {
                    return Err(CompatError::Fixture(format!(
                        "the drain ran out of time: {left:?}"
                    )));
                }
            }
        }
        Err(CompatError::Timeout("the actor to drain".to_owned()))
    }
}

//! The fault suite's harness: core's actor over a live database, judged by the simulator's oracle.
//!
//! A [`Rig`] owns one database in a directory of its own: the host's connection (`app`) and the
//! backend, both opened through a fresh [`FaultVfs`]; an [`Actor`] ticked on a [`FakeClock`];
//! staging and replica on `FsStore`s, each behind a `Tap` that records what it took.
//!
//! Ground truth never comes from the code under test. The database's first image is read from
//! the file before any connection is open. After every application statement and every tick,
//! the rig reads the WAL file (SQLite holds no lock on it) up to the frame count SQLite reports
//! to the application connection (`PRAGMA wal_checkpoint(NOOP)`), and gives each new committed
//! transaction's pages to the simulator's [`Model`]. A sync of the WAL that succeeded (counted by
//! the VFS) makes every commit before it durable, which is what a power loss keeps. The oracle
//! (`lsr_sim::check`) then restores the replica at every transaction it holds and compares the
//! pages with the model's image of the commit that transaction covers.
//!
//! What the actor's ticks see is only what the backend shows them; faults fire only inside
//! ticks unless a test opens the VFS window itself, so a schedule counts the engine's calls.
//!
//! The host's connections live where [`RigConfig::placement`] says ([`Placement`], module
//! `host`): in this process, set up as replication asks, or in a child process at SQLite's
//! defaults, as beside a sidecar, reached over a pipe. Either way the rig reads the host's
//! frame count from the host's own connection and the frames from the WAL file, so the model
//! and the oracle are the same in both.
//!
//! [`Stepper`] is the sync engine without the actor: `recover_traced`, `sync_step` and
//! `checkpoint` over a backend and an in-memory staging store, for cases that check the
//! engine's verdict at one point (the pin cases, the tripwire).

use std::collections::BTreeMap;
use std::io;
use std::num::{NonZeroI32, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use lsr_core::conformance::block_on;
use lsr_core::{
    Actor, ActorConfig, CheckpointConfig, CheckpointMode, Checkpointer, Clock, Env, Event,
    FakeClock, Fault, FileInfo, HaltCause, Inventory, L0Retention, Maintenance, MemStore, Next,
    ObjectStore, PageSource, ReadLen, Responses, StoreError, SyncConfig, Tick, Upload,
    UploadConfig, WalSource,
};
use lsr_ltx::{PageNo, PageSize, Timestamp, Txid};
use lsr_sim::{
    Acked, AppCommit, Checked, CommitId, Model, PageImage, Tap, TapEvent, TapLog, Violation,
};
use lsr_store_fs::FsStore;
use lsr_wal::{Salts, WalOffset};
use rusqlite::{Connection, OpenFlags};

use crate::testing::vfs::{FaultVfs, FileKind, Op, VfsError, seed_prng};
use crate::{OpenError, Sqlite, SqliteConfig};

mod host;
mod stepper;

pub use host::{CHILD_ENTRY, HOST_DB, MARK, Placement, serve_host};
pub use stepper::{StepDid, Stepper};

use host::{APP, Host};

/// The host connection a cycle's long reader uses.
const READER: u32 = 1;

/// What the suite needs of a backend: the engine's three doors and a way to open them.
pub trait Backend: WalSource + PageSource + Checkpointer + Clone {
    /// Why it did not open.
    type OpenError: std::error::Error + Send + Sync + 'static;
    /// The name the suite's messages use.
    const NAME: &'static str;
    /// Whether the pin, retaken over a fully backfilled WAL, sits on read mark 0, which does not
    /// block a WAL restart (walformat.html). The cases whose answer depends on it state both.
    const PIN_ON_MARK_ZERO: bool;

    /// Opens the database at `path` through the VFS named `vfs`, holding the pin.
    ///
    /// # Errors
    /// [`Backend::OpenError`].
    fn open_backend(path: &Path, vfs: &str) -> Result<Self, Self::OpenError>;
}

impl Backend for Sqlite {
    type OpenError = OpenError;
    const NAME: &'static str = "rusqlite read transaction";
    const PIN_ON_MARK_ZERO: bool = true;

    fn open_backend(path: &Path, vfs: &str) -> Result<Self, OpenError> {
        Self::open_with(
            path,
            &SqliteConfig {
                vfs: Some(vfs.to_owned()),
            },
        )
    }
}

/// Why a rig step failed.
#[derive(Debug, thiserror::Error)]
pub enum RigError {
    /// SQLite refused an application statement.
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// The backend did not open.
    #[error("the backend did not open: {0}")]
    Open(Box<dyn std::error::Error + Send + Sync>),
    /// A file the rig reads or writes itself.
    #[error("I/O on {path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// What failed.
        source: io::Error,
    },
    /// The fault VFS.
    #[error("the fault VFS: {0}")]
    Vfs(#[from] VfsError),
    /// A store the rig copies or lists.
    #[error("a store: {0}")]
    Store(String),
    /// The actor stopped.
    #[error("the actor halted: {0:?}")]
    Halted(HaltCause),
    /// The actor was still busy after this many ticks.
    #[error("the actor did not settle in {0} ticks")]
    Unsettled(u32),
    /// The oracle found the replica wrong.
    #[error("the oracle: {0}")]
    Oracle(#[from] Violation),
    /// The rig's own state does not allow the step (a step after a crash, before the reboot).
    #[error("the rig cannot {0} now")]
    State(&'static str),
    /// The host process (`Placement::SeparateProcess`) did not start, answer or stay up.
    #[error("the host process: {0}")]
    Host(String),
}

/// How a rig is built.
#[derive(Clone, Debug)]
pub struct RigConfig {
    /// The database's page size.
    pub page_size: u32,
    /// The application connection's `PRAGMA synchronous`.
    pub synchronous: &'static str,
    /// The actor's configuration.
    pub actor: ActorConfig,
    /// `SQLITE_TESTCTRL_PRNG_SEED`, applied when the rig is built.
    pub seed: NonZeroI32,
    /// Keep the model and judge with the oracle. Off for databases too big to hold in memory.
    pub oracle: bool,
    /// Record what writes replace, so a power loss can undo them.
    pub power: bool,
    /// Where the host application runs.
    pub placement: Placement,
}

impl Default for RigConfig {
    fn default() -> Self {
        Self {
            page_size: 4096,
            synchronous: "NORMAL",
            actor: rig_actor_config(),
            seed: DEFAULT_SEED,
            oracle: true,
            power: false,
            placement: Placement::InProcess,
        }
    }
}

impl RigConfig {
    /// The default rig with the host in `placement`.
    #[must_use]
    pub fn placed(placement: Placement) -> Self {
        Self {
            placement,
            ..Self::default()
        }
    }
}

/// The actor configuration of the suite: small checkpoint thresholds so a short workload runs
/// a PASSIVE and a TRUNCATE, no time trigger (checkpoints follow the workload, not the fake
/// clock), and compaction on, with every level-0 file and snapshot kept for a day so the oracle
/// can restore every transaction the run made. Every persistent fault stops the engine
/// (`Responses::STRICT`), so a case can name the cause it stops with.
#[must_use]
pub fn rig_actor_config() -> ActorConfig {
    let secs = Duration::from_secs;
    let day = secs(24 * 60 * 60);
    let maintenance = Maintenance::new(
        vec![secs(30), secs(300)],
        secs(60 * 60),
        day,
        L0Retention::After(day),
    )
    .unwrap_or_else(|_bad| Maintenance::go_default());
    ActorConfig {
        sync: SyncConfig {
            cap: NonZeroU64::new(256 * 1024),
            node_id: 0,
        },
        checkpoint: CheckpointConfig {
            interval: None,
            min_pages: NonZeroU32::new(8),
            truncate_pages: NonZeroU32::new(24),
        },
        upload: UploadConfig::default(),
        maintenance,
        retry_interval: Duration::from_secs(1),
        shutdown_grace: secs(30),
        responses: Responses::STRICT,
    }
}

/// The PRNG seed rigs use unless told otherwise.
const DEFAULT_SEED: NonZeroI32 = match NonZeroI32::new(0x5eed) {
    Some(seed) => seed,
    None => NonZeroI32::MIN,
};
/// Ticks a settle may take.
const SETTLE_TICKS: u32 = 2_000;
/// What a tick that asks to run again at once costs the fake clock.
const SPIN_STEP: Duration = Duration::from_millis(50);
const WAL_HEADER: usize = 32;
const FRAME_HEADER: usize = 24;

static NEXT_VFS: AtomicU32 = AtomicU32::new(0);

/// A fresh fault VFS with a name no other rig in this process uses.
///
/// # Errors
/// [`VfsError`].
pub fn fresh_vfs() -> Result<FaultVfs, VfsError> {
    let n = NEXT_VFS.fetch_add(1, Ordering::Relaxed);
    FaultVfs::register(&format!("lsr-fault-{}-{n}", std::process::id()))
}

/// One committed transaction read from the WAL file.
#[derive(Debug)]
struct Tx {
    salts: Salts,
    end: WalOffset,
    pages: Vec<(PageNo, Vec<u8>)>,
    db_size: u32,
}

fn be32(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    bytes.get(at..end)?.try_into().ok().map(u32::from_be_bytes)
}

/// Reads `path` whole. Only ever the WAL, on which SQLite takes no lock, or a database no
/// connection has open.
fn read_file(path: &Path) -> Result<Vec<u8>, RigError> {
    #[expect(
        clippy::disallowed_methods,
        reason = "the WAL carries no POSIX lock, and the rig reads a database only before any connection opens it"
    )]
    let read = std::fs::read(path);
    match read {
        Ok(bytes) => Ok(bytes),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(source) => Err(RigError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

/// The WAL as SQLite committed it, read past what was already given to the model.
#[derive(Debug)]
struct Mirror {
    wal: PathBuf,
    page_size: PageSize,
    /// The generation last read and how many of its frames are consumed.
    seen: Option<(Salts, u32)>,
}

impl Mirror {
    /// The committed transactions after the consumed frames, up to `mx_frame`, the count SQLite
    /// reports. A frame belongs to the generation while its salts equal the header's.
    fn drain(&mut self, mx_frame: u32) -> Result<Vec<Tx>, RigError> {
        let bytes = read_file(&self.wal)?;
        let (Some(salt1), Some(salt2)) = (be32(&bytes, 16), be32(&bytes, 20)) else {
            return Ok(Vec::new());
        };
        let salts = Salts { salt1, salt2 };
        let frame_len = FRAME_HEADER.saturating_add(self.page_size.bytes());
        let mut consumed = match self.seen {
            Some((seen, consumed)) if seen == salts => consumed,
            Some(_) | None => 0,
        };
        let (mut txs, mut pending) = (Vec::new(), Vec::new());
        let mut index = consumed;
        while index < mx_frame {
            let at = usize::try_from(index)
                .ok()
                .and_then(|i| i.checked_mul(frame_len))
                .and_then(|o| o.checked_add(WAL_HEADER));
            let Some(frame) = at.and_then(|at| bytes.get(at..at.saturating_add(frame_len))) else {
                break;
            };
            if be32(frame, 8) != Some(salt1) || be32(frame, 12) != Some(salt2) {
                break;
            }
            let (Some(pgno), Some(commit)) = (be32(frame, 0), be32(frame, 4)) else {
                break;
            };
            let Some(pgno) = PageNo::new(pgno, self.page_size) else {
                break;
            };
            pending.push((pgno, frame.get(FRAME_HEADER..).unwrap_or_default().to_vec()));
            index = index.saturating_add(1);
            if commit != 0 {
                consumed = index;
                txs.push(Tx {
                    salts,
                    end: WalOffset::from_frame_index(index, self.page_size),
                    pages: std::mem::take(&mut pending),
                    db_size: commit,
                });
            }
        }
        self.seen = Some((salts, consumed));
        Ok(txs)
    }
}

/// SQLite's frame count for the WAL, as the application connection sees it.
fn wal_frames(host: &mut Host) -> Result<u32, RigError> {
    let row = host.query(APP, "PRAGMA wal_checkpoint(NOOP)")?;
    let log = row.get(1).copied().unwrap_or(0);
    Ok(u32::try_from(log).unwrap_or(0))
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// A host connection through `vfs`, set up as replication asks (`crate::app_connection`).
///
/// # Errors
/// SQLite's.
pub fn open_app(db: &Path, vfs: &FaultVfs, synchronous: &str) -> Result<Connection, RigError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let app = Connection::open_with_flags_and_vfs(db, flags, vfs.name())?;
    app.busy_timeout(Duration::ZERO)?;
    app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))?;
    app.pragma_update(None, "synchronous", synchronous)?;
    crate::app_connection(&app).map_err(|err| RigError::Open(Box::new(err)))?;
    Ok(app)
}

/// Creates the database at `db` (one table, `t(id INTEGER PRIMARY KEY, v BLOB)`) and returns
/// its file's bytes, read once every connection is closed.
fn create(db: &Path, vfs: &FaultVfs, page_size: u32) -> Result<Vec<u8>, RigError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let conn = Connection::open_with_flags_and_vfs(db, flags, vfs.name())?;
    conn.pragma_update(None, "page_size", page_size)?;
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))?;
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")?;
    // The last connection's close checkpoints everything into the file and deletes the WAL.
    conn.close().map_err(|(_conn, err)| err)?;
    read_file(db)
}

/// The actor over a live database, judged by the oracle. See the module doc.
#[derive(Debug)]
pub struct Rig<B: Backend> {
    db: PathBuf,
    cfg: RigConfig,
    vfs: FaultVfs,
    host: Host,
    backend: Option<B>,
    actor: Actor,
    clock: FakeClock,
    staging: Tap<FsStore>,
    remote: Tap<FsStore>,
    staging_log: TapLog,
    remote_log: TapLog,
    remote_root: PathBuf,
    model: Option<Model>,
    mirror: Mirror,
    acked: Acked,
    wal_syncs: u64,
    staging_seen: usize,
    staged_max: Option<Txid>,
    ticks: Vec<Tick>,
    /// Each tick's window calls, per kind of file and call, counted after it.
    calls: Vec<BTreeMap<(FileKind, Op), u32>>,
    faults: Vec<Fault>,
    everywhere: bool,
}

impl<B: Backend> Rig<B> {
    /// A rig in `root`, which should be empty: the database `app.db`, staging in `meta/` and
    /// the replica in `replica/`.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn new(root: &Path, cfg: RigConfig) -> Result<Self, RigError> {
        seed_prng(cfg.seed);
        let vfs = fresh_vfs()?;
        vfs.set_window(false);
        let db = root.join("app.db");
        let bytes = create(&db, &vfs, cfg.page_size)?;
        let page_size =
            PageSize::new(cfg.page_size).ok_or(RigError::State("use that page size"))?;
        let model = cfg
            .oracle
            .then(|| Model::new(PageImage::from_bytes(page_size, &bytes)));
        if cfg.power {
            vfs.track_durability();
        }
        let mut host = Host::new(cfg.placement, &db, vfs, cfg.synchronous)?;
        host.open(APP)?;
        let backend =
            B::open_backend(&db, vfs.name()).map_err(|err| RigError::Open(Box::new(err)))?;
        let staging = Tap::new(FsStore::new(root.join("meta")));
        let remote_root = root.join("replica");
        let remote = Tap::new(FsStore::new(&remote_root));
        let mut rig = Self {
            mirror: Mirror {
                wal: suffixed(&db, "-wal"),
                page_size,
                seen: None,
            },
            db,
            actor: Actor::new(cfg.actor.clone()),
            cfg,
            wal_syncs: vfs.syncs(FileKind::Wal),
            vfs,
            host,
            backend: Some(backend),
            clock: FakeClock::new(Timestamp::from_unix_millis(1_700_000_000_000)),
            staging_log: staging.log(),
            remote_log: remote.log(),
            staging,
            remote,
            remote_root,
            model,
            acked: Acked::new(),
            staging_seen: 0,
            staged_max: None,
            ticks: Vec::new(),
            calls: Vec::new(),
            faults: Vec::new(),
            everywhere: false,
        };
        rig.observe()?;
        Ok(rig)
    }

    /// The database file.
    #[must_use]
    pub fn db(&self) -> &Path {
        &self.db
    }

    /// The WAL file.
    #[must_use]
    pub fn wal(&self) -> PathBuf {
        suffixed(&self.db, "-wal")
    }

    /// The replica's directory.
    #[must_use]
    pub fn replica_root(&self) -> &Path {
        &self.remote_root
    }

    /// The rig's fault VFS.
    #[must_use]
    pub const fn vfs(&self) -> &FaultVfs {
        &self.vfs
    }

    /// The host's connection.
    ///
    /// # Errors
    /// [`RigError::State`] after a crash, before the reboot, or with the host in another
    /// process.
    pub fn app(&self) -> Result<&Connection, RigError> {
        self.host
            .local(APP)
            .ok_or(RigError::State("reach the app's connection"))
    }

    /// Where the host runs.
    #[must_use]
    pub const fn placement(&self) -> Placement {
        self.host.placement()
    }

    /// The first row of `sql` on the host's connection, every column an integer.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn query(&mut self, sql: &str) -> Result<Vec<i64>, RigError> {
        self.host.query(APP, sql)
    }

    /// Runs `sql` on the host's connection without reading what it committed, for a commit
    /// the model cannot follow (a WAL deleted under it).
    ///
    /// # Errors
    /// [`RigError`].
    pub fn exec_unseen(&mut self, sql: &str) -> Result<(), RigError> {
        self.host.exec(APP, sql)
    }

    /// Turns `SQLITE_FCNTL_PERSIST_WAL` off on the host's connection (a host at SQLite's
    /// defaults never had it on).
    ///
    /// # Errors
    /// [`RigError`].
    pub fn host_persist_wal_off(&mut self) -> Result<(), RigError> {
        self.host.persist_wal_off(APP)
    }

    /// The host's process opens and closes the database and `-shm` with `std::fs`, dropping
    /// its own POSIX locks on them. Only for a host in another process.
    ///
    /// # Errors
    /// [`RigError::State`] in process; the host's error.
    pub fn host_touches_the_files(&mut self) -> Result<(), RigError> {
        self.host.touch()
    }

    /// The backend.
    ///
    /// # Errors
    /// [`RigError::State`] after a crash, before the reboot.
    pub fn backend(&mut self) -> Result<&mut B, RigError> {
        self.backend
            .as_mut()
            .ok_or(RigError::State("reach the backend"))
    }

    /// The actor, to read what it counted.
    #[must_use]
    pub const fn actor(&self) -> &Actor {
        &self.actor
    }

    /// Every tick so far, in order.
    #[must_use]
    pub fn ticks(&self) -> &[Tick] {
        &self.ticks
    }

    /// For each tick in which `op` ran on a `kind` file, the number of its first such call,
    /// counted as an armed fault counts: the schedule that fails each step of a run in turn.
    #[must_use]
    pub fn first_calls(&self, kind: FileKind, op: Op) -> Vec<u32> {
        let mut before = 0;
        let mut firsts = Vec::new();
        for calls in &self.calls {
            let after = calls.get(&(kind, op)).copied().unwrap_or(0);
            if after > before {
                firsts.push(before.saturating_add(1));
            }
            before = after;
        }
        firsts
    }

    /// What the replica and the staging directory really took, in order.
    #[must_use]
    pub fn store_events(&self) -> (Vec<TapEvent>, Vec<TapEvent>) {
        (self.remote_log.events(), self.staging_log.events())
    }

    /// Every fault a tick reported as retrying, in order.
    #[must_use]
    pub fn faults(&self) -> &[Fault] {
        &self.faults
    }

    /// The model of what SQLite committed, when the rig keeps one.
    #[must_use]
    pub const fn model(&self) -> Option<&Model> {
        self.model.as_ref()
    }

    /// The newest transaction the actor said is on the replica.
    #[must_use]
    pub const fn acked(&self) -> Option<Txid> {
        self.acked.highest()
    }

    /// The fake clock.
    #[must_use]
    pub const fn clock(&self) -> &FakeClock {
        &self.clock
    }

    /// Lets faults fire outside ticks too (application statements included).
    pub const fn faults_everywhere(&mut self, on: bool) {
        self.everywhere = on;
    }

    /// Runs `sql` on the host's connection, then reads what it committed.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn exec(&mut self, sql: &str) -> Result<(), RigError> {
        if !self.host.is_open(APP) {
            return Err(RigError::State("reach the app"));
        }
        self.vfs.set_window(self.everywhere);
        let done = self.host.exec(APP, sql);
        self.vfs.set_window(false);
        self.observe()?;
        done
    }

    /// Inserts one row of `bytes` random bytes.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn insert(&mut self, bytes: u32) -> Result<(), RigError> {
        self.exec(&format!("INSERT INTO t(v) VALUES (randomblob({bytes}))"))
    }

    /// A WAL sync that succeeded since the last look makes every commit before it durable. The
    /// rig looks after every statement and tick, so no commit lands between a sync and its look
    /// except the ones after it in the same tick, which are not in the model yet.
    fn note_syncs(&mut self) {
        let syncs = self.vfs.syncs(FileKind::Wal);
        if syncs != self.wal_syncs {
            self.wal_syncs = syncs;
            if let Some(model) = self.model.as_mut() {
                model.on_checkpoint();
            }
        }
    }

    /// Reads what SQLite committed since the last look into the model (module doc).
    ///
    /// # Errors
    /// [`RigError`].
    pub fn observe(&mut self) -> Result<(), RigError> {
        self.note_syncs();
        if self.model.is_none() || !self.host.is_open(APP) || !self.vfs.powered() {
            return Ok(());
        }
        let frames = wal_frames(&mut self.host)?;
        let Some(model) = self.model.as_mut() else {
            return Ok(());
        };
        let at = self.remote_log.len();
        for tx in self.mirror.drain(frames)? {
            let app = AppCommit {
                salts: tx.salts,
                end: tx.end,
            };
            model.commit(&tx.pages, tx.db_size, app, at);
        }
        Ok(())
    }
}

impl<B: Backend> Rig<B> {
    /// One tick of the actor, with the faults window open, then the model catches up.
    ///
    /// # Errors
    /// [`RigError`] from the rig's own reads; what the actor did is the returned tick.
    pub fn tick(&mut self) -> Result<Tick, RigError> {
        let Some(backend) = self.backend.as_ref() else {
            return Err(RigError::State("tick"));
        };
        let (mut wal, mut db, mut ck) = (backend.clone(), backend.clone(), backend.clone());
        let at = self.remote_log.len();
        let tip = self.model.as_ref().and_then(Model::tip);
        let now = self.clock.now();
        self.vfs.set_window(true);
        let env = Env {
            wal: &mut wal,
            db: &mut db,
            ck: &mut ck,
            staging: &self.staging,
            remote: &self.remote,
        };
        let tick = block_on(self.actor.tick(env, now));
        self.vfs.set_window(self.everywhere);
        // After a power loss in the tick only its syncs count: what it wrote after them is gone.
        self.observe()?;
        self.note_staged(tip, at);
        self.follow(&tick);
        self.ticks.push(tick.clone());
        self.calls.push(self.vfs.all_calls());
        Ok(tick)
    }

    fn follow(&mut self, tick: &Tick) {
        let Tick::Ran { event, next } = tick else {
            return;
        };
        match event {
            Event::Uploaded(did) => self.acked.on_did(did),
            Event::Retrying { fault, .. } => self.faults.push(*fault),
            Event::Booted { .. }
            | Event::Degraded { .. }
            | Event::Synced { .. }
            | Event::Checkpointed { .. }
            | Event::Maintained { .. }
            | Event::Idle
            | Event::Resnapshot(_)
            | Event::Clamped(_) => {}
        }
        match next {
            Next::Now if matches!(event, Event::Idle) => self.clock.advance(SPIN_STEP),
            Next::Now => {}
            Next::At(at) => {
                if *at > self.clock.now() {
                    self.clock.set(*at);
                }
            }
            Next::OnWalChange => self.clock.advance(Duration::from_secs(1)),
        }
    }

    /// Tells the model which commit each staged file holds (as the simulator's run loop does):
    /// a file with WAL frames by their position, a snapshot of an empty WAL by the tip as the
    /// tick began, since nothing else wrote during the tick.
    fn note_staged(&mut self, tip: Option<CommitId>, at: usize) {
        let events = self.staging_log.events();
        for event in events.iter().skip(self.staging_seen) {
            let TapEvent::Put {
                key,
                header: Some(_),
                origin,
            } = event
            else {
                continue;
            };
            if key.level().get() != 0 {
                continue;
            }
            let max = key.range().max();
            self.staged_max = self.staged_max.max(Some(max));
            let Some(model) = self.model.as_mut() else {
                continue;
            };
            match origin.filter(|origin| origin.size() > 0) {
                Some(origin) => {
                    let (salt1, salt2) = origin.salts();
                    let end = origin.offset().saturating_add(origin.size());
                    model.note_stage(max, Salts { salt1, salt2 }, end, at);
                }
                None => {
                    if let Some(commit) = tip {
                        let rewound = model.rewound();
                        model.note_snapshot(max, commit, rewound, at);
                    }
                }
            }
        }
        self.staging_seen = events.len();
    }

    /// Ticks until the actor is idle with everything staged on the replica.
    ///
    /// # Errors
    /// [`RigError::Halted`], [`RigError::Unsettled`], or the rig's own.
    pub fn settle(&mut self) -> Result<(), RigError> {
        for _ in 0..SETTLE_TICKS {
            match self.tick()? {
                Tick::Halted(cause) => return Err(RigError::Halted(cause)),
                Tick::Drained { .. } => return Ok(()),
                Tick::DeadlineReached(_) => return Err(RigError::Halted(HaltCause::Shutdown)),
                Tick::Ran {
                    event: Event::Idle,
                    next,
                } if !matches!(next, Next::Now) && self.acked.highest() >= self.staged_max => {
                    return Ok(());
                }
                Tick::Ran { .. } => {}
            }
        }
        Err(RigError::Unsettled(SETTLE_TICKS))
    }

    /// Whether a tick from the `from`th on ran a checkpoint of `mode` that `busy` describes.
    #[must_use]
    pub fn checkpointed_since(&self, from: usize, mode: CheckpointMode, busy: bool) -> bool {
        self.ticks.iter().skip(from).any(|tick| {
            matches!(tick, Tick::Ran {
                event: Event::Checkpointed { mode: m, busy: b },
                ..
            } if *m == mode && *b == busy)
        })
    }

    /// The suite's cycle: commits, each followed by `settle`, until the actor has synced, run a
    /// PASSIVE that finished and a TRUNCATE that finished, then one more commit. The actor
    /// truncates only after a PASSIVE at the truncate threshold could not finish, so a host
    /// reader holds the log until a checkpoint reports busy, then lets go; when a PASSIVE
    /// finishes again instead (a fault moved the run off its course), the reader comes back.
    ///
    /// # Errors
    /// The first error `settle` returns, or [`RigError::State`] when no TRUNCATE finished.
    pub fn cycle_with(
        &mut self,
        mut settle: impl FnMut(&mut Self) -> Result<(), RigError>,
    ) -> Result<(), RigError> {
        for _ in 0..5 {
            self.insert(3000)?;
            settle(self)?;
        }
        let begin = self.ticks.len();
        // When the host's long reader took its snapshot, by tick.
        let mut reader: Option<usize> = None;
        let mut released = Some(begin);
        for _ in 0..80 {
            let retake = released.is_some_and(|at| {
                at == begin || self.checkpointed_since(at, CheckpointMode::Passive, false)
            });
            if retake {
                self.host.open(READER)?;
                self.host.exec(READER, "BEGIN; SELECT count(*) FROM t;")?;
                reader = Some(self.ticks.len());
                released = None;
            }
            self.insert(3000)?;
            settle(self)?;
            if self.checkpointed_since(begin, CheckpointMode::Truncate, false) {
                self.host.close(READER)?;
                self.insert(3000)?;
                return settle(self);
            }
            let blocked = reader.is_some_and(|taken| {
                self.checkpointed_since(taken, CheckpointMode::Passive, true)
                    || self.checkpointed_since(taken, CheckpointMode::Truncate, true)
            });
            if blocked {
                self.host.close(READER)?;
                reader = None;
                released = Some(self.ticks.len());
            }
        }
        Err(RigError::State("run a TRUNCATE checkpoint"))
    }

    /// [`Self::cycle_with`] with [`Self::settle`].
    ///
    /// # Errors
    /// As [`Self::cycle_with`].
    pub fn cycle(&mut self) -> Result<(), RigError> {
        self.cycle_with(Self::settle)
    }

    /// Ticks until the actor halts, and returns why.
    ///
    /// # Errors
    /// [`RigError::Unsettled`] when it settles or keeps running instead.
    pub fn until_halt(&mut self) -> Result<HaltCause, RigError> {
        for _ in 0..SETTLE_TICKS {
            if let Tick::Halted(cause) = self.tick()? {
                return Ok(cause);
            }
        }
        Err(RigError::Unsettled(SETTLE_TICKS))
    }

    /// One tick that must not halt.
    fn tick_unhalted(&mut self) -> Result<(), RigError> {
        match self.tick()? {
            Tick::Halted(cause) => Err(RigError::Halted(cause)),
            Tick::Ran { .. } | Tick::Drained { .. } | Tick::DeadlineReached(_) => Ok(()),
        }
    }

    /// `writers` host connections, on threads of their own in the host's process, commit
    /// `rows` rows each at once while the actor ticks; returns once every writer is done.
    ///
    /// # Errors
    /// [`RigError::Halted`] when a tick halts, a writer's error, or the rig's own.
    pub fn writers_while_ticking(&mut self, writers: u32, rows: u32) -> Result<(), RigError> {
        let Some(child) = self.host.child_mut() else {
            return self.local_writers(writers, rows);
        };
        child.start_writers(writers, rows)?;
        loop {
            self.tick_unhalted()?;
            let child = self
                .host
                .child_mut()
                .ok_or(RigError::State("lose the host process"))?;
            if child.writers_done()? {
                return Ok(());
            }
        }
    }

    fn local_writers(&mut self, writers: u32, rows: u32) -> Result<(), RigError> {
        let (db, name) = (self.db.clone(), self.vfs.name().to_owned());
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..writers)
                .map(|w| {
                    let (db, name) = (&db, name.as_str());
                    scope.spawn(move || local_writer(db, name, w, rows))
                })
                .collect();
            while !handles
                .iter()
                .all(std::thread::ScopedJoinHandle::is_finished)
            {
                self.tick_unhalted()?;
            }
            for handle in handles {
                handle
                    .join()
                    .map_err(|_panic| RigError::State("join a writer that panicked"))??;
            }
            Ok(())
        })
    }

    /// The oracle's verdict on the replica as it is now.
    ///
    /// # Errors
    /// [`RigError::Oracle`] with the violation, or the copy's error.
    pub fn check(&self) -> Result<Checked, RigError> {
        let model = self
            .model
            .as_ref()
            .ok_or(RigError::State("judge without a model"))?;
        let copy = MemStore::new();
        block_on(copy_store(&FsStore::new(&self.remote_root), &copy))?;
        Ok(lsr_sim::check(model, &self.remote_log, &copy, &self.acked)?)
    }

    /// The replicator's process stops and starts again: the actor and backend are new, the
    /// host's connection, the files and both stores stay.
    ///
    /// # Errors
    /// [`RigError::Open`].
    pub fn restart(&mut self) -> Result<(), RigError> {
        self.backend = None;
        self.backend = Some(
            B::open_backend(&self.db, self.vfs.name()).map_err(|e| RigError::Open(Box::new(e)))?,
        );
        self.actor = Actor::new(self.cfg.actor.clone());
        self.observe()
    }

    /// [`Self::restart`] with a new actor configuration, kept for later restarts.
    ///
    /// # Errors
    /// As [`Self::restart`].
    pub fn restart_with(&mut self, actor: ActorConfig) -> Result<(), RigError> {
        self.cfg.actor = actor;
        self.restart()
    }

    /// Drops the backend, as a replicator that stops; [`Self::restart`] starts a new one. The
    /// host stays.
    pub fn stop_replicator(&mut self) {
        self.backend = None;
    }

    /// Closes the host's connection.
    ///
    /// # Errors
    /// SQLite's close.
    pub fn close_app(&mut self) -> Result<(), RigError> {
        self.host.close(APP)
    }

    /// Opens the host's connection again.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn open_app(&mut self) -> Result<(), RigError> {
        self.host.open(APP)?;
        self.observe()
    }
}

impl<B: Backend> Rig<B> {
    /// After an `Effect::PowerLoss` fired: every connection dies, the power comes back, the host
    /// opens the database again (SQLite recovers the WAL), and a new replicator boots on the
    /// same staging directory and replica. The model drops what the loss took.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn reboot_after_power_loss(&mut self) -> Result<(), RigError> {
        self.backend = None;
        self.host.discard_all()?;
        self.vfs.restore_power()?;
        self.host.open(APP)?;
        let frames = wal_frames(&mut self.host)?;
        let bytes = read_file(&self.wal())?;
        let salts = match (be32(&bytes, 16), be32(&bytes, 20)) {
            (Some(salt1), Some(salt2)) => Some(Salts { salt1, salt2 }),
            (Some(_) | None, _) => None,
        };
        if let Some(model) = self.model.as_mut() {
            model.on_power_loss(salts.unwrap_or(Salts { salt1: 0, salt2: 0 }), frames);
        }
        self.mirror.seen = salts.map(|salts| (salts, frames));
        self.acked.note_rewound();
        self.wal_syncs = self.vfs.syncs(FileKind::Wal);
        self.restart()
    }

    /// Replaces the local database with `bytes`, as a host that lost its disk and restored the
    /// replica into a fresh one: every connection closes, the database, its WAL and `-shm` and
    /// the staging directory are deleted, `bytes` become the database file, and the host and a
    /// new replicator open it. The model continues from its tip, which is what `bytes` must hold.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn replace_database(&mut self, bytes: &[u8], staging: &Path) -> Result<(), RigError> {
        self.backend = None;
        self.host.discard_all()?;
        let io_at = |path: &Path| {
            let path = path.to_owned();
            move |source| RigError::Io { path, source }
        };
        for path in [self.db.clone(), self.wal(), suffixed(&self.db, "-shm")] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_at(&path)(err)),
            }
        }
        match std::fs::remove_dir_all(staging) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_at(staging)(err)),
        }
        std::fs::write(&self.db, bytes).map_err(io_at(&self.db))?;
        self.mirror.seen = None;
        self.staging_seen = self.staging_log.len();
        self.host.open(APP)?;
        self.restart()
    }
}

/// One in-process concurrent writer: a host connection through the rig's VFS, set up as
/// replication asks, waiting on the others' write lock.
fn local_writer(db: &Path, vfs: &str, writer: u32, rows: u32) -> Result<(), RigError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE;
    let conn = Connection::open_with_flags_and_vfs(db, flags, vfs)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    crate::app_connection(&conn).map_err(|err| RigError::Open(Box::new(err)))?;
    for n in 0..rows {
        conn.execute(
            "INSERT INTO t(v) VALUES (randomblob(?1))",
            [host::writer_row_bytes(writer, n)],
        )?;
    }
    Ok(())
}

/// Copies every file of `from` into `to`, so the oracle can judge a replica on disk.
async fn copy_store(from: &FsStore, to: &MemStore) -> Result<(), RigError> {
    let store = |err: StoreError| RigError::Store(err.to_string());
    let inventory = Inventory::load(from)
        .await
        .map_err(|err| RigError::Store(err.to_string()))?;
    let chunk = ReadLen::new(1 << 20).ok_or(RigError::State("read in chunks"))?;
    for file in inventory.files() {
        copy_file(from, to, file, chunk).await.map_err(store)?;
    }
    Ok(())
}

async fn copy_file(
    from: &FsStore,
    to: &MemStore,
    file: &FileInfo,
    chunk: ReadLen,
) -> Result<(), StoreError> {
    let mut upload = to
        .begin_upload(&file.key, Timestamp::from_unix_millis(0))
        .await?;
    let mut offset = 0_u64;
    loop {
        let bytes = from.read(&file.key, offset, chunk).await?;
        if bytes.is_empty() {
            break;
        }
        offset = offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        upload.write(&bytes).await?;
    }
    upload.commit().await.map(drop)
}

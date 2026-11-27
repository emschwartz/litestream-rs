//! One run's worth of machinery, built from a seed.
//!
//! [`World`] owns the SQLite rules, the clock, both stores with their wrappers,
//! and the model and acknowledgement record the oracle judges against. It owns
//! no loop logic: the run loop builds the actor from these handles and
//! hands each tick's future to [`World::run_tick`], the one seam between this
//! harness and the actor.
//!
//! Staging is `Faults<YieldEachCall<Recorder<Volatile<StagingRaw>>>>` (`MemStore`, or the real
//! directory store on the in-memory filesystem: [`crate::staging`]) and the replica is
//! `Faults<YieldEachCall<Recorder<MemStore>>>`. Both keep a raw `MemStore` handle for the
//! oracle and for power loss, so neither a fault nor a yield touches a check.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::conformance::block_on;
use lsr_core::{
    Entropy, FakeClock, FileInfo, FileKey, MemStore, RecordingSleeper, SeededEntropy, StoreError,
};
use lsr_ltx::{PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{Salts, WalOffset};

use crate::clock::{ClockLaw, SimClock};
use crate::exec::{Polls, RunEnd, run_until};
use crate::history::History;
use crate::model::{CommitId, Model, PageImage, Recovery};
use crate::oracle::{Acked, Checked, Ground, Violation, Windows, check_with};
use crate::remote::{Bucket, Parts, RemoteBackend, S3Remote};
use crate::seed::Seed;
use crate::side::{SideMaker, SqliteSide};
use crate::sqlite::{AppCommit, CrashKind, InTickSite, OtherProcessMode};
use crate::staging::StagingRaw;
use crate::store::{
    FaultHandle, Faults, HighWater, Lost, Recorder, RecorderLog, Volatile, VolatileHandle,
    YieldEachCall,
};
use lsr_core::WriteMode;

/// The staging store the actor sees.
pub type StagingStore = Faults<YieldEachCall<Recorder<Volatile<StagingRaw>>>>;
/// The replica store the actor sees.
pub type RemoteStore = Faults<YieldEachCall<Recorder<RemoteBackend>>>;

/// Why the harness could not rewrite a staged file.
#[derive(Debug, thiserror::Error)]
pub enum StagedEditError {
    /// The store refused.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The listing gave no exact created-at time, so a rewrite would lose the file's
    /// creation time. The sim's stores always give one: this is the harness
    /// broken, not a fault to tolerate.
    #[error("{0} was listed without an exact created-at time")]
    NoCreatedAt(FileKey),
}

fn listed_at(info: &FileInfo) -> Result<Timestamp, StagedEditError> {
    info.created_at()
        .map(|created_at| created_at.at())
        .ok_or(StagedEditError::NoCreatedAt(info.key))
}

fn is_full_image(bytes: &[u8]) -> bool {
    use lsr_ltx::Decoder;
    let mut decoder = Decoder::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let Ok((used, _)) = decoder.feed(rest) else {
            return false;
        };
        let Some(next) = rest.get(used..).filter(|_| used > 0) else {
            return false;
        };
        rest = next;
    }
    decoder
        .finish()
        .is_ok_and(|done| done.page_count() == u64::from(done.header().commit()))
}

fn without_origin(bytes: &[u8]) -> Option<Vec<u8>> {
    rewritten(bytes, |fields| fields.wal = None)
}

fn rewritten(bytes: &[u8], edit: impl FnOnce(&mut lsr_ltx::HeaderFields)) -> Option<Vec<u8>> {
    reencoded(bytes, edit, |_| {})
}

fn reencoded(
    bytes: &[u8],
    edit: impl FnOnce(&mut lsr_ltx::HeaderFields),
    mut page: impl FnMut(&mut Vec<u8>),
) -> Option<Vec<u8>> {
    use lsr_ltx::{Decoder, Encoder, Event, Header};
    let mut decoder = Decoder::new();
    let mut pages = Vec::new();
    let mut header = None;
    let mut rest = bytes;
    while !rest.is_empty() {
        let (used, event) = decoder.feed(rest).ok()?;
        match event {
            Some(Event::Header(found)) => header = Some(found),
            Some(Event::Page(page)) => pages.push((page.pgno(), page.data().to_vec())),
            None => {}
        }
        if used == 0 {
            return None;
        }
        rest = rest.get(used..)?;
    }
    let verified = decoder.finish().ok()?;
    let mut fields = header?.fields();
    edit(&mut fields);
    let mut encoder = Encoder::new(Vec::new(), Header::new(fields).ok()?).ok()?;
    for (pgno, data) in &mut pages {
        page(data);
        encoder.write_page(*pgno, data).ok()?;
    }
    let (out, _) = encoder.finish(verified.trailer().post_apply()).ok()?;
    Some(out)
}

/// Flips the byte at `from`, or the first one after it whose flip changes what the file
/// reads as. The LTX checksum covers decompressed pages, so a flipped LZ4 match offset can
/// land on other bytes of the same value and leave another encoding of the same file: a boot
/// accepts that by design (`docs/decisions/D175.md`), and the judge of uploads
/// (`oracle/witness.rs`) compares bytes, so it is not damage either of them can be held to.
fn flip_byte_from(bytes: &mut [u8], from: usize) {
    let whole = reencoded(bytes, |_| {}, |_| {});
    for at in from..bytes.len() {
        if let Some(byte) = bytes.get_mut(at) {
            *byte ^= 0xff;
        }
        if whole.is_none() || reencoded(bytes, |_| {}, |_| {}) != whole {
            return;
        }
        if let Some(byte) = bytes.get_mut(at) {
            *byte ^= 0xff;
        }
    }
}

fn read_all(store: &impl lsr_core::ObjectStore, key: &FileKey) -> Result<Vec<u8>, StoreError> {
    use lsr_core::ReadLen;
    let Some(chunk) = ReadLen::new(1 << 20) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    loop {
        let offset = u64::try_from(out.len()).unwrap_or(u64::MAX);
        let part = block_on(store.read(key, offset, chunk))?;
        let done = u64::try_from(part.len()).unwrap_or(0) < chunk.get();
        out.extend_from_slice(&part);
        if done {
            return Ok(out);
        }
    }
}

/// A copy of the staging directory.
#[derive(Clone, Debug, Default)]
pub struct StagedCopy(Vec<(FileInfo, Vec<u8>)>);

impl StagedCopy {
    /// Each copied file's key and the WAL range its header names (generation
    /// and end), if it names one.
    #[must_use]
    pub fn origins(&self) -> Vec<(FileKey, Option<crate::model::WalEnd>)> {
        self.0
            .iter()
            .map(|(info, bytes)| (info.key, crate::store::wal_end(bytes)))
            .collect()
    }
}

/// Where a tick is cut short, and how.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CrashPlan {
    /// The pending this crash drops the future at.
    pub at_poll: Polls,
    /// What kind of crash.
    pub kind: CrashKind,
}

/// Chooses a crash point in a tick that awaited `polls` times, and a kind: a
/// process crash three times in four, a power loss otherwise. Draws two numbers.
#[must_use]
pub fn pick_crash(entropy: &dyn Entropy, polls: u32) -> CrashPlan {
    let at = entropy.below(u64::from(polls).max(1));
    let kind = if entropy.below(4) == 0 {
        CrashKind::Power
    } else {
        CrashKind::Process
    };
    CrashPlan {
        at_poll: Polls::new(u32::try_from(at).unwrap_or(u32::MAX)),
        kind,
    }
}

/// How a tick ended.
#[derive(Debug, PartialEq, Eq)]
pub enum TickEnd<T> {
    /// The tick ran to its end.
    Done(T),
    /// The tick was cut off.
    Crashed {
        /// The pendings it survived.
        polls: u32,
        /// The crash.
        kind: CrashKind,
        /// Staged files a power loss dropped or tore; empty for a process crash.
        lost: Vec<(FileKey, Lost)>,
    },
}

/// The app's commit inside a tick, as the model recorded it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedInTick {
    /// The commit.
    pub id: CommitId,
    /// The staging recorder's length at the commit: what the tick staged from that
    /// event on was cut after it.
    pub staged_from: usize,
    /// How many pages it wrote.
    pub pages: usize,
}

/// Everything one run holds.
#[derive(Debug)]
pub struct World {
    /// The seed the run came from.
    pub seed: Seed,
    /// The SQLite the engine runs against, through the seam (`crate::side`): the rule model,
    /// for now the only backend.
    pub sqlite: Arc<dyn SqliteSide>,
    /// The driver's clock: it moves between ticks, and the recorders read it.
    pub clock: Arc<FakeClock>,
    /// The clock the actor and the app read inside a tick, over [`Self::clock`]
    /// under the run's [`ClockLaw`].
    pub sim_clock: Arc<SimClock>,
    /// Time spent inside the tick under way ([`crate::clock::Stall`]): a hung replica call's
    /// wait, or a pause of the process.
    pub stall: crate::clock::Stall,
    /// The latest time the actor has been told: the run loop notes each
    /// tick's, and the recorders read it.
    pub high: HighWater,
    /// The sleeper the actor waits on.
    pub sleeper: RecordingSleeper,
    /// The actor's randomness.
    pub entropy: SeededEntropy,
    /// The staging store, with faults and crash points.
    pub staging: StagingStore,
    /// The replica store, with faults and crash points.
    pub remote: RemoteStore,
    /// The staging store with nothing in front of it: `MemStore`, or the directory store on the
    /// in-memory filesystem ([`World::use_fs_staging`]).
    pub staging_raw: StagingRaw,
    /// The replica with nothing in front of it: what the oracle restores from.
    pub remote_raw: Bucket,
    /// The S3 store under the wrappers, when the replica is on S3: what a process's end does
    /// to its parts and to what it remembered ([`S3Remote::process_ended`]).
    pub remote_s3: Option<Arc<S3Remote>>,
    /// What the staging store really did.
    pub staging_recorder: RecorderLog,
    /// What the replica really did.
    pub remote_recorder: RecorderLog,
    /// Schedules staging faults.
    pub staging_faults: FaultHandle,
    /// Schedules replica faults.
    pub remote_faults: FaultHandle,
    /// Staging's power-loss bookkeeping.
    pub volatile: VolatileHandle,
    /// What the database really was.
    pub model: Model,
    /// What the engine has acknowledged.
    pub acked: Acked,
    /// The commit log and the content judge (`lsr-history`).
    pub history: History,
    /// Transactions a bucket rollback took from the replica for good.
    pub lost: BTreeSet<Txid>,
    /// Power losses whose recovery brought back a commit an earlier loss took.
    pub resurrected: u32,
    /// Files another writer put: its transaction, and the one it copied.
    pub foreign: Vec<(Txid, Txid)>,
    /// Another writer's file the operator removed, by its index in
    /// [`Self::foreign`], with the replica recorder's length then: a file
    /// compacted after it cannot hold its pages. The index, not the
    /// transaction, names the file: one id can be put, removed and put again
    /// with no recorder event between.
    pub foreign_removed: Vec<(usize, usize)>,
    /// Another writer's files, with the replica recorder's length when each
    /// was put.
    pub foreign_put: Vec<(Txid, usize)>,
    /// Another writer's files and their sizes: no recorder saw them, but the
    /// engine may compact them.
    pub foreign_sizes: Vec<(FileKey, u64)>,
    /// Files the engine deleted as a repair, after they failed to verify.
    pub repaired: Vec<FileKey>,
    /// The replica's store has no conditional writes: every create-only commit is refused
    /// ([`lsr_core::MemStore::refuse_create_only`], D79).
    pub no_create_only: bool,
    crash_entropy: SeededEntropy,
    in_tick: Option<CommittedInTick>,
    // When the app's armed in-tick commit landed, by the app's own clock read.
    in_tick_time: Arc<Mutex<Option<Timestamp>>>,
    file_times: FileTimes,
}

/// When the database file and the WAL were last written, as their modified times would say:
/// the driver notes the files after every operation (`World::note_writes`), stamping what
/// changed with the time the operation began, never a later one. The engine reads it through
/// `WalSource::modified` (`crate::side::SideSource::with_modified`), whatever the backend, so
/// the stamps share the run's clock with the replica's file headers.
#[derive(Debug)]
struct FileTimes {
    db: Vec<u8>,
    wal: Option<Vec<u8>>,
    db_at: Timestamp,
    wal_at: Timestamp,
    shared: Arc<Mutex<Option<Timestamp>>>,
}

impl FileTimes {
    fn new(db: Vec<u8>, wal: Option<Vec<u8>>, at: Timestamp) -> Self {
        let times = Self {
            db,
            wal,
            db_at: at,
            wal_at: at,
            shared: Arc::default(),
        };
        times.publish();
        times
    }

    fn note(&mut self, db: Vec<u8>, wal: Option<Vec<u8>>, at: Timestamp) {
        if db != self.db {
            self.db = db;
            self.db_at = at;
        }
        if wal != self.wal {
            self.wal = wal;
            self.wal_at = at;
        }
        self.publish();
    }

    /// As the real source answers: the WAL counts only while it holds a frame.
    fn publish(&self) {
        let framed = self
            .wal
            .as_ref()
            .is_some_and(|wal| wal.len() > lsr_wal::HEADER_LEN);
        let at = if framed {
            self.db_at.max(self.wal_at)
        } else {
            self.db_at
        };
        *self.shared.lock().unwrap_or_else(PoisonError::into_inner) = Some(at);
    }
}

/// The replica stack over `backend`: the recorder on the world's clock, the yield on each call
/// and the object-level faults, which corrupt a published object through `raw`.
fn remote_stack(
    backend: RemoteBackend,
    raw: &Bucket,
    (clock, stall): (&Arc<FakeClock>, &crate::clock::Stall),
    high: &HighWater,
    seed: u64,
) -> (RemoteStore, RecorderLog) {
    let mut recorder = Recorder::new(backend).with_clock(Arc::clone(clock), high.clone());
    if raw.wire().is_some() {
        recorder = recorder.landed_through(raw.landed());
    }
    let log = recorder.log();
    let store = Faults::new(YieldEachCall::new(recorder), Vec::new(), seed)
        .corrupting_through(raw.clone())
        .hanging_through(stall.clone(), crate::remote::client_timeout());
    (store, log)
}

/// The first instant of a run's clock.
pub const EPOCH_MILLIS: i64 = 1_700_000_000_000;

impl World {
    /// A world with a database of `db_pages` pages, empty stores and no faults.
    #[must_use]
    pub fn new(seed: Seed, page_size: PageSize, db_pages: u32) -> Self {
        Self::over(seed, SideMaker::default().make(seed, page_size, db_pages))
    }

    /// A world over `sqlite`, with empty stores and no faults.
    #[must_use]
    pub fn over(seed: Seed, sqlite: Arc<dyn SqliteSide>) -> Self {
        let page_size = sqlite.page_size();
        let clock = Arc::new(FakeClock::new(Timestamp::from_unix_millis(EPOCH_MILLIS)));
        let high = HighWater::default();
        let stall = crate::clock::Stall::default();
        let staging_raw = StagingRaw::Mem(MemStore::new());
        let remote_mem = MemStore::new();
        let remote_raw = Bucket::Mem(remote_mem.clone());
        let volatile = Volatile::new(staging_raw.clone());
        let volatile_handle = volatile.handle();
        let staging_recorder = Recorder::new(volatile).with_clock(Arc::clone(&clock), high.clone());
        let staging_recorder_log = staging_recorder.log();
        let staging = Faults::new(
            YieldEachCall::new(staging_recorder),
            Vec::new(),
            seed.stream(2),
        )
        .volatile_on_late_error(volatile_handle.clone());
        let (remote, remote_recorder_log) = remote_stack(
            RemoteBackend::Mem(remote_mem),
            &remote_raw,
            (&clock, &stall),
            &high,
            seed.stream(3),
        );
        let initial = PageImage::from_bytes(page_size, &sqlite.image());
        let model = Model::new(initial);
        let history = History::new(
            &model,
            lsr_core::Clock::now(clock.as_ref()),
            seed.stream(40),
            lsr_history::AheadMode::Count,
        );
        let sim_clock = Arc::new(SimClock::new(
            Arc::clone(&clock),
            high.clone(),
            ClockLaw::PerTick,
            seed.stream(16),
            stall.clone(),
        ));
        let file_times = FileTimes::new(
            sqlite.db_file(),
            sqlite.wal_file(),
            Timestamp::from_unix_millis(EPOCH_MILLIS),
        );
        Self {
            seed,
            sqlite,
            clock,
            sim_clock,
            stall,
            high,
            sleeper: RecordingSleeper::new(),
            entropy: SeededEntropy::new(seed.stream(4)),
            staging_faults: staging.handle(),
            remote_faults: remote.handle(),
            staging,
            remote,
            staging_raw,
            remote_raw,
            remote_s3: None,
            staging_recorder: staging_recorder_log,
            remote_recorder: remote_recorder_log,
            volatile: volatile_handle,
            model,
            acked: Acked::new(),
            history,
            lost: BTreeSet::new(),
            resurrected: 0,
            foreign: Vec::new(),
            foreign_removed: Vec::new(),
            foreign_put: Vec::new(),
            foreign_sizes: Vec::new(),
            repaired: Vec::new(),
            no_create_only: false,
            crash_entropy: SeededEntropy::new(seed.stream(5)),
            in_tick: None,
            in_tick_time: Arc::default(),
            file_times,
        }
    }

    /// Stamps the database file and the WAL with `at` where they changed since the last call.
    pub fn note_writes(&mut self, at: Timestamp) {
        let (db, wal) = (self.sqlite.db_file(), self.sqlite.wal_file());
        self.file_times.note(db, wal, at);
    }

    /// The modified time the engine reads, shared with its WAL source.
    #[must_use]
    pub fn modified(&self) -> Arc<Mutex<Option<Timestamp>>> {
        Arc::clone(&self.file_times.shared)
    }

    /// Puts the replica on the S3 store over a fresh in-memory bucket behind its fault layer
    /// (`crate::remote`), with the same wrappers in front of it as before; with `parts`, the
    /// store sends in parts whose landings the seed draws. Call before the first boot: the
    /// replica starts empty, and its fault schedule and batch widths start over.
    pub fn use_s3_remote(&mut self, parts: Option<Parts>) {
        let parts = parts.map(|parts| (parts, self.seed.stream(26)));
        let Some((backend, bucket)) = crate::remote::s3(&self.clock, parts, &self.stall) else {
            return;
        };
        if let RemoteBackend::S3(remote) = &backend {
            self.remote_s3 = Some(Arc::clone(remote));
        }
        let (remote, log) = remote_stack(
            backend,
            &bucket,
            (&self.clock, &self.stall),
            &self.high,
            self.seed.stream(3),
        );
        self.remote_faults = remote.handle();
        self.remote = remote;
        self.remote_raw = bucket;
        self.remote_recorder = log;
    }

    /// The app commits `pages`, leaving `db_size` pages; `None` when SQLite's
    /// rules refuse (the write lock is held, or `db_size` is zero).
    ///
    /// A page that comes back after a shrink and that the commit does not write
    /// holds whatever the rules' file still has, so the model takes it from
    /// their image instead of assuming the app wrote it.
    pub fn app_commit(&mut self, pages: &[(PageNo, Vec<u8>)], db_size: u32) -> Option<CommitId> {
        let app = self.sqlite.commit(pages, db_size)?;
        Some(self.committed(pages, db_size, app))
    }

    /// As [`Self::app_commit`], by another application connection with a checkpoint count of
    /// its own ([`SimSqlite::other_writer_commit`]).
    pub fn other_writer_commit(
        &mut self,
        pages: &[(PageNo, Vec<u8>)],
        db_size: u32,
    ) -> Option<CommitId> {
        let app = self.sqlite.other_writer_commit(pages, db_size)?;
        Some(self.committed(pages, db_size, app))
    }

    fn committed(
        &mut self,
        pages: &[(PageNo, Vec<u8>)],
        db_size: u32,
        app: crate::sqlite::AppCommit,
    ) -> CommitId {
        let before = self.model.newest_image().db_size();
        self.retire_generation(app.salts);
        let (written, db_size) = match self.sqlite.take_written() {
            Some(real) => (real.pages, real.db_size),
            None => {
                let mut written = pages.to_vec();
                if db_size > before {
                    written.extend(self.re_added(pages, before, db_size));
                }
                (written, db_size)
            }
        };
        let id = self
            .model
            .commit(&written, db_size, app, self.remote_recorder.len());
        self.log_history();
        id
    }

    fn log_history(&mut self) {
        let now = lsr_core::Clock::now(self.clock.as_ref());
        self.history.log_commits(&self.model, now);
    }

    /// From now on the actor and the app read the clock under `law`.
    pub fn set_clock_law(&mut self, law: ClockLaw) {
        self.sim_clock = Arc::new(SimClock::new(
            Arc::clone(&self.clock),
            self.high.clone(),
            law,
            self.seed.stream(16),
            self.stall.clone(),
        ));
    }

    /// Arms a commit of `pages`, leaving `db_size` pages, that the app makes
    /// inside the next tick at `site`. A commit that grows the database must
    /// carry every page it adds. The app reads its clock as it commits, and
    /// the history logs the commit at that time.
    pub fn arm_commit_in_tick(
        &self,
        pages: Vec<(PageNo, Vec<u8>)>,
        db_size: u32,
        site: InTickSite,
    ) {
        let (remote, staging) = (self.remote_recorder.clone(), self.staging_recorder.clone());
        let (clock, time) = (Arc::clone(&self.sim_clock), Arc::clone(&self.in_tick_time));
        let created_at = Box::new(move || {
            let now = lsr_core::Clock::now(clock.as_ref());
            *time.lock().unwrap_or_else(PoisonError::into_inner) = Some(now);
            (remote.len(), staging.len())
        });
        self.sqlite
            .arm_commit_in_tick(pages, db_size, site, created_at);
    }

    /// The app's commit inside the last tick, once: see [`World::arm_commit_in_tick`].
    pub fn take_commit_in_tick(&mut self) -> Option<CommittedInTick> {
        self.in_tick.take()
    }

    fn record_commit_in_tick(&mut self) -> Option<CommittedInTick> {
        let done = self.sqlite.take_commit_in_tick()?;
        let (remote_at, staged_from) = done.created_at;
        self.retire_generation(done.app.salts);
        let id = self
            .model
            .commit(&done.pages, done.db_size, done.app, remote_at);
        let landed = self
            .in_tick_time
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match landed {
            Some(at) => self.history.log_commits(&self.model, at),
            None => self.log_history(),
        }
        Some(CommittedInTick {
            id,
            staged_from,
            pages: done.pages.len(),
        })
    }

    /// A commit under new salts means the WAL restarted, which needs a finished
    /// backfill: every commit before it is in the database file, synced.
    fn retire_generation(&mut self, salts: Salts) {
        let old = self
            .model
            .newest_commit()
            .and_then(|id| self.model.rec(id))
            .map(|rec| rec.salts);
        if old.is_some_and(|old| old != salts) {
            self.model.on_checkpoint();
        }
    }

    /// The pages in `after_old..=db_size` that `pages` leaves unwritten, as
    /// the rules hold them now.
    fn re_added(
        &self,
        pages: &[(PageNo, Vec<u8>)],
        old_size: u32,
        db_size: u32,
    ) -> Vec<(PageNo, Vec<u8>)> {
        let size = self.sqlite.page_size();
        let image = self.sqlite.image();
        (old_size.saturating_add(1)..=db_size)
            .filter(|pgno| !pages.iter().any(|(page, _)| page.get() == *pgno))
            .filter_map(|pgno| {
                let at = usize::try_from(pgno.checked_sub(1)?).ok()?;
                let bytes = image.chunks_exact(size.bytes()).nth(at)?;
                Some((PageNo::new(pgno, size)?, bytes.to_vec()))
            })
            .collect()
    }

    /// The app shrinks the database. SQLite rewrites page 1 for it, so the model
    /// takes page 1 from the rules' own image rather than guessing its bytes.
    pub fn app_shrink(&mut self, db_size: u32) -> Option<CommitId> {
        let app = self.sqlite.shrink(db_size)?;
        self.retire_generation(app.salts);
        let (pages, db_size) = match self.sqlite.take_written() {
            Some(real) => (real.pages, real.db_size),
            None => (self.first_page(), db_size),
        };
        let id = self
            .model
            .commit(&pages, db_size, app, self.remote_recorder.len());
        self.log_history();
        Some(id)
    }

    fn first_page(&self) -> Vec<(PageNo, Vec<u8>)> {
        let size = self.sqlite.page_size();
        let image = self.sqlite.image();
        let first = image.get(..size.bytes()).unwrap_or_default().to_vec();
        PageNo::new(1, size)
            .map(|one| (one, first))
            .into_iter()
            .collect()
    }

    /// VACUUM into pages of `new` with every connection closed: the whole
    /// database is rewritten and the WAL starts empty. `None` when the rules
    /// refuse (the page size is unchanged, or a connection holds a transaction).
    pub fn app_vacuum(&mut self, new: PageSize) -> Option<CommitId> {
        if !self.sqlite.rewrite_page_size(new) {
            return None;
        }
        let image = PageImage::from_bytes(new, &self.sqlite.image());
        let app = AppCommit {
            salts: self.sqlite.salts(),
            end: WalOffset::from_frame_index(0, new),
        };
        let id = self.model.vacuum(image, app, self.remote_recorder.len());
        self.log_history();
        Some(id)
    }

    /// Records a commit the engine made itself: a checkpoint's bump of page 1
    /// (`db.go:1668`) puts a one-page commit into the WAL that the app never
    /// made, and a file of the sync that follows ends on it. Its pages are the
    /// newest commit's, so the model keeps the newest commit's image under the new end. `at` is the
    /// replica recorder's length when the tick started, so no earlier put can be
    /// taken to cover it.
    pub fn sync_model_at(&mut self, at: usize) {
        self.sync_model_from(at, self.model.commits().len());
    }

    /// [`World::sync_model_at`] for a tick whose model commits from index `from` on were already
    /// taken up (the app's, and the engine's bump the app's in-tick operation saw): an engine
    /// commit that one of them already names is not recorded again.
    fn sync_model_from(&mut self, at: usize, from: usize) {
        let written = self.sqlite.take_written();
        // Each commit of the engine's own, not only the one the tick left the WAL at: a tick can
        // make one, take a snapshot that ends on it, restart the WAL and make another. A file
        // that ends on the first is a commit's end too.
        // A bump rewrites page 1 as it is, so each leaves the newest commit's image.
        for commit in self.sqlite.take_engine_commits() {
            self.retire_generation(commit.salts);
            let seen = self.model.commits().get(from..).is_some_and(|taken| {
                taken
                    .iter()
                    .any(|rec| rec.salts == commit.salts && rec.end.get() == commit.end.get())
            });
            if !seen && !self.newest_ends_at(commit) {
                let size = self.model.newest_image().db_size();
                self.model.commit(&[], size, commit, at);
                self.log_history();
            }
        }
        let frames = self.sqlite.mx_frame();
        let salts = self.sqlite.salts();
        self.retire_generation(salts);
        if frames == 0 {
            return;
        }
        let end = WalOffset::from_frame_index(frames, self.sqlite.page_size());
        if !self.newest_ends_at(AppCommit { salts, end }) {
            let (pages, size) = written.map_or_else(
                || (Vec::new(), self.sqlite.db_size()),
                |real| (real.pages, real.db_size),
            );
            self.model
                .commit(&pages, size, AppCommit { salts, end }, at);
            self.log_history();
        }
    }

    fn newest_ends_at(&self, commit: AppCommit) -> bool {
        self.model
            .newest_commit()
            .and_then(|id| self.model.rec(id))
            .is_some_and(|rec| rec.salts == commit.salts && rec.end.get() == commit.end.get())
    }

    /// The driver starts: see `SimSqlite::start_driver`. The bump frame it may
    /// write is a commit the app never made, which the model takes up now.
    pub fn start_driver(&mut self) {
        let at = self.remote_recorder.len();
        self.sqlite.start_driver();
        self.sync_model_at(at);
    }

    /// A checkpoint from outside the engine. One that copied frames fsynced the
    /// WAL first, so every commit so far survives a power loss; one held off by
    /// a reader copied nothing and did not.
    pub fn other_process_checkpoint(&mut self, mode: OtherProcessMode) -> bool {
        let ran = self.sqlite.other_process_checkpoint(mode).is_some();
        if ran && self.sqlite.wal_synced() {
            self.model.on_checkpoint();
        }
        ran
    }

    /// Draws a crash for a tick that awaited `polls` times.
    pub fn pick_crash(&self, polls: u32) -> CrashPlan {
        pick_crash(&self.crash_entropy, polls)
    }

    /// Runs one tick's future. With a `plan` the future is dropped at its
    /// pending; the crash then happens: SQLite loses what the crash loses, and a
    /// power loss also drops or tears the staged files that were not yet durable
    /// and tells the model which commits went with them.
    pub fn run_tick<F: Future>(&mut self, fut: F, plan: Option<CrashPlan>) -> TickEnd<F::Output> {
        let budget = plan.map_or(Polls::UNBOUNDED, |p| p.at_poll);
        let at = self.remote_recorder.len();
        self.sqlite.app_tick_begins();
        let ran = run_until(fut, budget);
        // The app's commit inside the tick came before the engine's own frames
        // that followed it, so the model takes it up first.
        let from = self.model.commits().len();
        self.record_app_in_tick();
        self.in_tick = self.record_commit_in_tick();
        // Before the crash takes anything: a power loss decides which of the
        // engine's own commits survive, so the model must know them first.
        self.sync_model_from(at, from);
        match ran {
            RunEnd::Done(out) => TickEnd::Done(out),
            RunEnd::Crashed { polls } => {
                let kind = plan.map_or(CrashKind::Process, |p| p.kind);
                let lost = self.crash(kind);
                TickEnd::Crashed { polls, kind, lost }
            }
        }
    }

    /// Applies a crash to everything that is not the actor.
    pub fn crash(&mut self, kind: CrashKind) -> Vec<(FileKey, Lost)> {
        self.sqlite.crash(kind);
        match kind {
            CrashKind::Process => Vec::new(),
            CrashKind::Power => {
                let lost = block_on(
                    self.staging_raw
                        .power_loss(&self.volatile, &self.crash_entropy),
                )
                .unwrap_or_default();
                self.log_history();
                self.model.on_power_loss(
                    self.sqlite.salts(),
                    self.sqlite.mx_frame(),
                    self.remote_recorder.len(),
                );
                self.adopt_recovered();
                let now = lsr_core::Clock::now(self.clock.as_ref());
                self.history.power_loss(&self.model, now);
                lost
            }
        }
    }

    /// Deletes every staged file: the disk is gone but the database is not, as
    /// when the staging directory is on a volume that was replaced.
    ///
    /// # Errors
    /// The store's error when it cannot list or delete.
    pub fn wipe_staging(&self) -> Result<(), StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        for level in Level::ALL {
            let listing = block_on(self.staging_raw.list(level, None, PageLimit::MAX))?;
            let keys: Vec<FileKey> = listing.files.iter().map(|f| f.key).collect();
            if !keys.is_empty() {
                block_on(self.staging_raw.delete(&keys))?;
            }
        }
        Ok(())
    }

    /// Every staged file, copied aside.
    ///
    /// # Errors
    /// The store's error when it cannot list or read.
    pub fn copy_staging(&self) -> Result<StagedCopy, StoreError> {
        let mut files = Vec::new();
        for info in self.staged_files()? {
            files.push((info, self.read_staged(&info.key)?));
        }
        Ok(StagedCopy(files))
    }

    /// Replaces the staging directory with `copy`, as a restored backup would.
    ///
    /// # Errors
    /// The store's error when it cannot delete or write; [`StagedEditError::NoCreatedAt`]
    /// when a saved file has no exact created-at time.
    pub fn replace_staging(&self, copy: &StagedCopy) -> Result<(), StagedEditError> {
        self.wipe_staging()?;
        for (info, bytes) in &copy.0 {
            self.write_staged(info, bytes)?;
        }
        Ok(())
    }

    /// The newest level-0 file in staging.
    ///
    /// # Errors
    /// The store's error when it cannot list.
    pub fn newest_staged(&self) -> Result<Option<FileInfo>, StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let listing = block_on(self.staging_raw.list(Level::ZERO, None, PageLimit::MAX))?;
        Ok(listing.files.last().copied())
    }

    /// Whether the local database holds a commit past the one the newest level-0 file in
    /// staging holds. A boot then continues from a position short of the WAL's committed end,
    /// which no read transaction pinned: the generation can restart under it before the engine
    /// stages the rest. Nothing staged, or staging that cannot be read, is not behind.
    #[must_use]
    pub fn staging_behind_newest_commit(&self) -> bool {
        let Ok(Some(newest)) = self.newest_staged() else {
            return false;
        };
        let Ok(bytes) = self.read_staged(&newest.key) else {
            return false;
        };
        let txid = newest.key.range().max();
        let staged = self
            .model
            .staged_commit(txid, crate::store::wal_end(&bytes));
        staged.is_some() && staged != self.model.newest_commit()
    }

    /// The level-0 files in staging; none when it cannot be listed.
    #[must_use]
    pub fn staged_level_zero(&self) -> Vec<FileKey> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        block_on(self.staging_raw.list(Level::ZERO, None, PageLimit::MAX))
            .map(|listing| listing.files.iter().map(|file| file.key).collect())
            .unwrap_or_default()
    }

    /// The header stamp of the first level-0 file in staging past the replica's newest
    /// transaction, when it is one of `left` (what an earlier life left), no staged file stands
    /// before it, and its WAL span is in another generation than the replica's level-0 file at
    /// that transaction, or the replica has none there: a file the boot has nothing to compare
    /// with, so it counts it as a commit (`crate::failstop::FailStopJudge::unplaced`).
    #[must_use]
    pub fn unplaced_staged(&self, left: &[FileKey]) -> Option<Timestamp> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let header = |store: &dyn Fn(&FileKey) -> Result<Vec<u8>, StoreError>, key: &FileKey| {
            store(key).ok().and_then(|bytes| {
                bytes
                    .first_chunk::<{ lsr_ltx::HEADER_LEN }>()
                    .and_then(|raw| lsr_ltx::Header::decode(raw).ok())
            })
        };
        let tip = self.remote_newest()?;
        let staged = block_on(self.staging_raw.list(Level::ZERO, None, PageLimit::MAX))
            .ok()?
            .files;
        let first = staged
            .iter()
            .find(|file| file.key.range().max().get() > tip)?;
        let min = first.key.range().min();
        if !left.contains(&first.key) || staged.iter().any(|file| file.key.range().max() < min) {
            return None;
        }
        let mine = header(&|key| read_all(&self.staging_raw, key), &first.key)?;
        let theirs = block_on(self.remote_raw.list(Level::ZERO, None, PageLimit::MAX))
            .ok()?
            .files
            .into_iter()
            .find(|file| file.key.range().max().get() == tip)
            .and_then(|file| header(&|key| read_all(&self.remote_raw, key), &file.key));
        let salts = |header: lsr_ltx::Header| header.fields().wal.map(|wal| wal.salts());
        let placed =
            theirs.is_some_and(|theirs| salts(theirs).is_some() && salts(theirs) == salts(mine));
        (!placed).then_some(mine.fields().timestamp)
    }

    /// The newest transaction any replica file holds, at any level, read
    /// around the wrappers; `None` when the replica is empty or cannot list.
    #[must_use]
    pub fn remote_newest(&self) -> Option<u64> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        Level::ALL
            .into_iter()
            .filter_map(|level| block_on(self.remote_raw.list(level, None, PageLimit::MAX)).ok())
            .flat_map(|listing| listing.files)
            .map(|file| file.key.range().max().get())
            .max()
    }

    /// How many files the replica holds at every level, read around the wrappers: what a restore
    /// of its whole state could read.
    #[must_use]
    pub fn remote_file_count(&self) -> u64 {
        use lsr_core::{Level, ObjectStore, PageLimit};
        Level::ALL
            .into_iter()
            .filter_map(|level| block_on(self.remote_raw.list(level, None, PageLimit::MAX)).ok())
            .map(|listing| u64::try_from(listing.files.len()).unwrap_or(u64::MAX))
            .fold(0, u64::saturating_add)
    }

    /// Whether a file on the replica, at any level, ends at `txid`: a
    /// restore to `txid` has a file to end on.
    #[must_use]
    pub fn remote_ends_at(&self, txid: u64) -> bool {
        use lsr_core::{Level, ObjectStore, PageLimit};
        Level::ALL
            .into_iter()
            .filter_map(|level| block_on(self.remote_raw.list(level, None, PageLimit::MAX)).ok())
            .flat_map(|listing| listing.files)
            .any(|file| file.key.range().max().get() == txid)
    }

    /// Every level-0 file on the replica, oldest first, read around the
    /// wrappers.
    ///
    /// # Errors
    /// The store's error when it cannot list.
    pub fn remote_level_zero(&self) -> Result<Vec<FileInfo>, StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let listing = block_on(self.remote_raw.list(Level::ZERO, None, PageLimit::MAX))?;
        Ok(listing.files)
    }

    /// Deletes every replica file, at every level, that ends at the newest
    /// transaction; `None` when the replica is empty, otherwise the lowest
    /// transaction no kept file covers any more.
    ///
    /// # Errors
    /// The store's error when it cannot list or delete.
    pub fn remote_roll_back(&mut self) -> Result<Option<Txid>, StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let mut files = Vec::new();
        for level in Level::ALL {
            files.extend(block_on(self.remote_raw.list(level, None, PageLimit::MAX))?.files);
        }
        let Some(newest) = files.iter().map(|f| f.key.range().max()).max() else {
            return Ok(None);
        };
        let (gone, kept): (Vec<FileInfo>, Vec<FileInfo>) =
            files.iter().partition(|f| f.key.range().max() == newest);
        let keys: Vec<FileKey> = gone.iter().map(|f| f.key).collect();
        self.remote_delete(&keys)?;
        // What no kept file covers is gone from the replica for good.
        let mut lowest = newest;
        for key in &keys {
            let range = key.range();
            for raw in range.min().get()..=range.max().get() {
                let covered = kept.iter().any(|f| {
                    let r = f.key.range();
                    r.min().get() <= raw && raw <= r.max().get()
                });
                if let (false, Some(txid)) = (covered, Txid::new(raw)) {
                    self.lost.insert(txid);
                    lowest = lowest.min(txid);
                }
            }
        }
        Ok(Some(lowest))
    }

    /// Makes the store forget the stored created-at time of the newest-ending
    /// level 0-8 file (the higher level on a tie), as a bucket copy without
    /// metadata does: it reports the copy's time from then on, the latest the
    /// clock has read, which no transaction in the file follows. Returns
    /// whether there was such a file.
    ///
    /// # Errors
    /// The store's error when it cannot list.
    pub fn remote_drop_created_at(&mut self) -> Result<bool, StoreError> {
        use lsr_core::{CreatedAtSource, Level, ObjectStore, PageLimit};
        let mut files = Vec::new();
        for level in Level::ALL.into_iter().filter(|level| !level.is_snapshot()) {
            files.extend(block_on(self.remote_raw.list(level, None, PageLimit::MAX))?.files);
        }
        let newest = files
            .iter()
            .filter_map(|f| f.created_at().map(|created_at| (f.key, created_at)))
            .filter(|(_, created_at)| created_at.origin() != CreatedAtSource::Fallback)
            .max_by_key(|(key, _)| (key.range().max(), key.level()));
        let Some((key, created_at)) = newest else {
            return Ok(false);
        };
        let copied = self
            .high
            .at_least(lsr_core::Clock::now(self.clock.as_ref()))
            .max(created_at.at());
        self.remote_raw.drop_created_at(&key);
        self.remote_raw.set_last_modified(&key, copied);
        Ok(true)
    }

    /// Deletes these replica keys around the wrappers.
    ///
    /// # Errors
    /// The store's error when it cannot delete.
    pub fn remote_delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        use lsr_core::ObjectStore;
        block_on(self.remote_raw.delete(keys))
    }

    /// Puts a level-0 file named one transaction past the replica's newest,
    /// holding that file's pages.
    ///
    /// # Errors
    /// The store's error when it cannot list or write.
    pub fn remote_put_unrecognized(&mut self) -> Result<bool, StoreError> {
        self.remote_put_foreign().map(|put| put.is_some())
    }

    /// [`Self::remote_put_unrecognized`], returning the key and bytes put.
    ///
    /// # Errors
    /// The store's error when it cannot list or write.
    pub fn remote_put_foreign(&mut self) -> Result<Option<(FileKey, Vec<u8>)>, StoreError> {
        use lsr_core::ObjectStore;
        use lsr_ltx::TxidRange;
        let Some(newest) = self.remote_level_zero()?.last().map(|f| f.key) else {
            return Ok(None);
        };
        let Some(next) = newest.range().max().next() else {
            return Ok(None);
        };
        let key = FileKey::staged(next);
        // Another writer's file is a sound one: the same pages under the next
        // transaction, from a WAL that is not this database's, so it names no
        // origin in it and the live WAL cannot vouch for it.
        let Some(bytes) = rewritten(&read_all(&self.remote_raw, &newest)?, |fields| {
            fields.txids = TxidRange::single(next);
            fields.wal = None;
        }) else {
            return Ok(None);
        };
        let at = lsr_core::Clock::now(self.clock.as_ref());
        let mut up = block_on(self.remote_raw.begin_upload(&key, at, WriteMode::Replace))?;
        block_on(lsr_core::Upload::write(&mut up, &bytes))?;
        block_on(lsr_core::Upload::commit(up))?;
        self.foreign.push((next, newest.range().max()));
        self.foreign_put.push((next, self.remote_recorder.len()));
        self.foreign_sizes
            .push((key, u64::try_from(bytes.len()).unwrap_or(u64::MAX)));
        Ok(Some((key, bytes)))
    }

    /// Whether replica `key` holds exactly `bytes`.
    ///
    /// # Errors
    /// The store's error when it cannot read.
    pub fn remote_holds(&self, key: FileKey, bytes: &[u8]) -> Result<bool, StoreError> {
        match read_all(&self.remote_raw, &key) {
            Ok(now) => Ok(now == bytes),
            Err(StoreError::NotFound { .. }) => Ok(false),
            Err(other) => Err(other),
        }
    }

    /// Whether staging does not hold what the replica holds under `key`: the file is gone
    /// from staging (an operation emptied it, and what staging holds no longer vouches for
    /// the files this engine put on the replica), or it has different bytes (a staged copy
    /// damaged or put back from a backup under a key the replica took).
    ///
    /// # Errors
    /// Either store's error other than a missing key.
    pub fn staging_departs_from_remote(&self, key: FileKey) -> Result<bool, StoreError> {
        let found = |read: Result<Vec<u8>, StoreError>| match read {
            Ok(bytes) => Ok(Some(bytes)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(other) => Err(other),
        };
        let staged = found(read_all(&self.staging_raw, &key))?;
        let remote = found(read_all(&self.remote_raw, &key))?;
        Ok(match (staged, remote) {
            (Some(staged), Some(remote)) => staged != remote,
            (None, Some(_)) => true,
            (Some(_) | None, None) => false,
        })
    }

    /// Deletes replica `key` if it still holds exactly `bytes`; whether it did.
    ///
    /// # Errors
    /// The store's error when it cannot read or delete.
    pub fn remote_remove_if(&self, key: FileKey, bytes: &[u8]) -> Result<bool, StoreError> {
        match read_all(&self.remote_raw, &key) {
            Ok(now) if now == bytes => self.remote_delete(&[key]).map(|()| true),
            Ok(_) | Err(StoreError::NotFound { .. }) => Ok(false),
            Err(other) => Err(other),
        }
    }

    /// Deletes the newest level-0 file; false when there is none.
    ///
    /// # Errors
    /// The store's error when it cannot list or delete.
    pub fn delete_newest_staged(&self) -> Result<bool, StoreError> {
        use lsr_core::ObjectStore;
        let Some(info) = self.newest_staged()? else {
            return Ok(false);
        };
        block_on(self.staging_raw.delete(&[info.key]))?;
        Ok(true)
    }

    /// Rewrites the newest level-0 file's bytes through `edit`; false when
    /// there is none.
    ///
    /// # Errors
    /// The store's error when it cannot list, read or write;
    /// [`StagedEditError::NoCreatedAt`] when the file has no exact created-at time.
    pub fn edit_newest_staged(
        &self,
        edit: impl FnOnce(&mut Vec<u8>),
    ) -> Result<bool, StagedEditError> {
        let Some(info) = self.newest_staged()? else {
            return Ok(false);
        };
        let mut bytes = self.read_staged(&info.key)?;
        edit(&mut bytes);
        self.write_staged(&info, &bytes)?;
        Ok(true)
    }

    /// Flips one byte of the newest level-0 file, the middle one or the first after it whose
    /// flip the file's own checksum can see ([`flip_byte_from`]); false when there is none.
    ///
    /// # Errors
    /// As [`Self::edit_newest_staged`].
    pub fn flip_newest_staged(&self) -> Result<bool, StagedEditError> {
        self.edit_newest_staged(|bytes| {
            let middle = bytes.len() / 2;
            flip_byte_from(bytes, middle);
        })
    }

    /// Whether the staged file `key` holds every page of its database, which
    /// is what a snapshot looks like: the engine writes one as a level-0 file
    /// whose header still names the WAL range it was cut from. False when the
    /// file is gone or does not decode. A file a local course staged, still in its slot
    /// ([`crate::slot`]), is never one: a course stages no snapshot (docs/decisions/D263.md).
    #[must_use]
    pub fn staged_is_full_image(&self, key: &FileKey) -> bool {
        self.read_staged(key)
            .is_ok_and(|bytes| is_full_image(&bytes))
    }

    /// Rewrites the newest level-0 file without its WAL origin, as a file
    /// from a writer that does not record one reads; false when there is none
    /// or it cannot be re-encoded.
    ///
    /// # Errors
    /// As [`Self::edit_newest_staged`].
    pub fn strip_newest_origin(&self) -> Result<bool, StagedEditError> {
        let mut done = false;
        let found = self.edit_newest_staged(|bytes| {
            if let Some(stripped) = without_origin(bytes) {
                *bytes = stripped;
                done = true;
            }
        })?;
        Ok(found && done)
    }

    /// Every staged file at every level, read around the wrappers.
    ///
    /// # Errors
    /// The store's error when it cannot list.
    pub fn staged_files(&self) -> Result<Vec<FileInfo>, StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let mut files = Vec::new();
        for level in Level::ALL {
            files.extend(block_on(self.staging_raw.list(level, None, PageLimit::MAX))?.files);
        }
        Ok(files)
    }

    /// The newest staged level-0 file the engine wrote since staging recorder
    /// event `since`: its bytes are those of a put the recorder took then. A
    /// file put back from a backup, or put in place of one, is not the
    /// engine's to upload.
    ///
    /// # Errors
    /// The store's error when it cannot list or read.
    pub fn newest_engine_staged(&self, since: usize) -> Result<Option<FileInfo>, StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let puts = self.engine_puts(since);
        let listing = block_on(self.staging_raw.list(Level::ZERO, None, PageLimit::MAX))?;
        for info in listing.files.iter().rev() {
            let bytes = read_all(&self.staging_raw, &info.key)?;
            if puts.contains(&(info.key, crate::store::content_digest(&bytes))) {
                return Ok(Some(*info));
            }
        }
        Ok(None)
    }

    fn engine_puts(&self, since: usize) -> BTreeSet<(FileKey, u64)> {
        self.staging_recorder
            .events()
            .iter()
            .skip(since)
            .filter_map(|event| match event {
                crate::RecorderEvent::Put { key, digest, .. } => Some((*key, *digest)),
                crate::RecorderEvent::Delete { .. } => None,
            })
            .collect()
    }

    /// The page size each replica file's header names, and each staged file
    /// the engine wrote since staging recorder event `since` (its bytes are
    /// those of a put the recorder took then): a file put back from a backup
    /// before that, or put in place of one, is not the engine's. A file whose
    /// header does not decode is left out.
    ///
    /// # Errors
    /// What the store said when it could not list or read.
    pub fn page_sizes(&self, since: usize) -> Result<Vec<(FileKey, PageSize)>, String> {
        use lsr_core::Inventory;
        let staged_puts = self.engine_puts(since);
        let mut out = Vec::new();
        let staging =
            block_on(Inventory::load(&self.staging_raw)).map_err(|err| err.to_string())?;
        let remote = block_on(Inventory::load(&self.remote_raw)).map_err(|err| err.to_string())?;
        for (inventory, all) in [(staging, false), (remote, true)] {
            for info in inventory.files() {
                let bytes = if all {
                    read_all(&self.remote_raw, &info.key)
                } else {
                    read_all(&self.staging_raw, &info.key)
                }
                .map_err(|err| err.to_string())?;
                let engines =
                    all || staged_puts.contains(&(info.key, crate::store::content_digest(&bytes)));
                let header = bytes
                    .first_chunk::<{ lsr_ltx::HEADER_LEN }>()
                    .and_then(|raw| lsr_ltx::Header::decode(raw).ok());
                if let Some(header) = header.filter(|_| engines) {
                    out.push((info.key, header.page_size()));
                }
            }
        }
        Ok(out)
    }

    /// The digest of the staged file at `key` (`crate::store::content_digest`), read around the
    /// wrappers; none when staging holds no file there or cannot read it.
    #[must_use]
    pub fn staged_digest(&self, key: &FileKey) -> Option<u64> {
        read_all(&self.staging_raw, key)
            .ok()
            .map(|bytes| crate::store::content_digest(&bytes))
    }

    fn read_staged(&self, key: &FileKey) -> Result<Vec<u8>, StoreError> {
        read_all(&self.staging_raw, key)
    }

    fn write_staged(&self, info: &FileInfo, bytes: &[u8]) -> Result<(), StagedEditError> {
        use lsr_core::{ObjectStore, Upload};
        let mut up = block_on(self.staging_raw.begin_upload(
            &info.key,
            listed_at(info)?,
            WriteMode::Replace,
        ))?;
        block_on(up.write(bytes))?;
        block_on(up.commit())?;
        Ok(())
    }

    /// [`World::adopt_local_state`] for damage that corrupted the database
    /// (a flipped bit): the model follows it, and the history judge refuses
    /// any restore that gives it.
    pub fn adopt_corrupt_state(&mut self) {
        if self.model_matches_sqlite() {
            return;
        }
        self.adopt_local_state();
        self.history.adopted_corruption(&self.model);
    }

    /// Takes the database as the rules now read it as the model's newest
    /// commit, when damage to the WAL changed it: the damage is the database's
    /// new truth, and the replica is judged against that.
    pub fn adopt_local_state(&mut self) {
        if self.model_matches_sqlite() {
            return;
        }
        let page_size = self.sqlite.page_size();
        let image = PageImage::from_bytes(page_size, &self.sqlite.image());
        let app = AppCommit {
            salts: self.sqlite.salts(),
            end: WalOffset::from_frame_index(self.sqlite.mx_frame(), page_size),
        };
        self.model.vacuum(image, app, self.remote_recorder.len());
        self.log_history();
    }

    /// After the WAL's generation was regrown with its commit marks kept: every mark is a
    /// commit of the writer that regrew it, so the model takes each one, in order, as a new
    /// image at its place in the WAL. A file staged up to an earlier mark is then that mark's
    /// commit, not the one made at the same place before the regrowth, whose pages the WAL no
    /// longer holds.
    pub fn adopt_regrown(&mut self) {
        if self.model_matches_sqlite() {
            return;
        }
        let page_size = self.sqlite.page_size();
        let salts = self.sqlite.salts();
        let at = self.remote_recorder.len();
        for (end, bytes) in self.sqlite.commit_images() {
            let image = PageImage::from_bytes(page_size, &bytes);
            let app = AppCommit {
                salts,
                end: WalOffset::from_frame_index(end, page_size),
            };
            self.model.vacuum(image, app, at);
        }
        self.log_history();
    }

    /// The rules' database after a power loss is SQLite's own recovery of the
    /// bytes the loss left, which may bring back a commit a previous loss took
    /// ([`Model::adopt_recovered`]); one no commit made is left for the
    /// model check to report.
    fn adopt_recovered(&mut self) {
        let page_size = self.sqlite.page_size();
        let app = AppCommit {
            salts: self.sqlite.salts(),
            end: WalOffset::from_frame_index(self.sqlite.mx_frame(), page_size),
        };
        let image = self.sqlite.image();
        let at = self.remote_recorder.len();
        if let Recovery::Resurrected(_) = self.model.adopt_recovered(&image, false, app, at) {
            self.resurrected = self.resurrected.saturating_add(1);
        }
    }

    /// Whether the model's newest image is the database the rules hold: a check
    /// of the harness against itself.
    #[must_use]
    pub fn model_matches_sqlite(&self) -> bool {
        let newest = self.model.newest_image();
        let real = self.sqlite.image();
        let size = newest.page_size().bytes();
        real.len() == size.saturating_mul(usize::try_from(newest.db_size()).unwrap_or(usize::MAX))
            && real.chunks_exact(size).enumerate().all(|(i, page)| {
                u32::try_from(i)
                    .ok()
                    .and_then(|i| i.checked_add(1))
                    .and_then(|pgno| newest.page(pgno))
                    .is_some_and(|want| want.as_slice() == page)
            })
    }

    /// Judges the replica, and retention's deletes against `retention`.
    ///
    /// # Errors
    /// The first [`Violation`].
    pub fn check(&self, retention: Option<Windows>) -> Result<Checked, Violation> {
        let mut exempt = self.remote_faults.rejected();
        exempt.extend(self.remote_recorder.rejected());
        let damaged = self.remote_faults.damaged();
        let flipped = self.remote_faults.flipped();
        for key in &self.repaired {
            let bad_reads = flipped.iter().filter(|seen| *seen == key).count();
            if !damaged.contains(key) && bad_reads < 2 {
                return Err(Violation::DeletedOnOneRead {
                    key: key.to_string(),
                });
            }
        }
        exempt.extend(&self.repaired);
        let ground = Ground {
            staging: Some(&self.staging_recorder),
            retention,
            exempt: &exempt,
            lost: &self.lost,
            foreign: &self.foreign,
            foreign_removed: &self.foreign_removed,
            foreign_put: &self.foreign_put,
            damaged: &damaged,
        };
        check_with(
            &self.model,
            &self.remote_recorder,
            &self.remote_raw,
            &self.acked,
            ground,
        )
    }
}

mod in_tick;
#[cfg(test)]
mod tests;

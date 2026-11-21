//! One run's worth of machinery, built from a seed.
//!
//! [`World`] owns the SQLite rules, the clock, both stores with their wrappers,
//! and the model and acknowledgement record the oracle judges against. It owns
//! no loop logic: the run loop builds the actor from these handles and
//! hands each tick's future to [`World::run_tick`], the one seam between this
//! harness and the actor.
//!
//! Staging is `Faults<Yield<Tap<Stage<MemStore>>>>` and the replica is
//! `Faults<Yield<Tap<MemStore>>>`. Both keep a raw `MemStore` handle for the
//! oracle and for power loss, so neither a fault nor a yield touches a check.

use lsr_core::conformance::block_on;
use lsr_core::{
    Entropy, FakeClock, FileInfo, FileKey, MemStore, RecordingSleeper, SeededEntropy, StoreError,
};
use lsr_ltx::{PageNo, PageSize, Timestamp};
use lsr_wal::{Salts, WalOffset};

use crate::exec::{Polls, RunEnd, run_until};
use crate::model::{CommitId, Model, PageImage};
use crate::oracle::{Acked, Checked, Violation, check};
use crate::seed::Seed;
use crate::sqlite::{AppCommit, CrashKind, OtherProcessMode, SimSqlite};
use crate::store::{FaultHandle, Faults, Lost, Stage, StageHandle, Tap, TapLog, Yield};

/// The staging store the actor sees.
pub type StagingStore = Faults<Yield<Tap<Stage<MemStore>>>>;
/// The replica store the actor sees.
pub type RemoteStore = Faults<Yield<Tap<MemStore>>>;

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

/// The exact created-at time the listing carried, so a rewrite keeps the file's
/// creation time.
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

/// The same file with no WAL origin in its header.
fn without_origin(bytes: &[u8]) -> Option<Vec<u8>> {
    rewritten(bytes, |fields| fields.wal = None)
}

/// The same pages under a header `edit` changed.
fn rewritten(bytes: &[u8], edit: impl FnOnce(&mut lsr_ltx::HeaderFields)) -> Option<Vec<u8>> {
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
    for (pgno, data) in &pages {
        encoder.write_page(*pgno, data).ok()?;
    }
    let (out, _) = encoder.finish(verified.trailer().post_apply()).ok()?;
    Some(out)
}

/// Every byte of `key`.
fn read_all(store: &MemStore, key: &FileKey) -> Result<Vec<u8>, StoreError> {
    use lsr_core::{ObjectStore, ReadLen};
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
    /// The staging tap's length at the commit: what the tick staged from that
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
    /// SQLite's rules.
    pub sqlite: SimSqlite,
    /// The clock the actor reads.
    pub clock: FakeClock,
    /// The sleeper the actor waits on.
    pub sleeper: RecordingSleeper,
    /// The actor's randomness.
    pub entropy: SeededEntropy,
    /// The staging store, with faults and crash points.
    pub staging: StagingStore,
    /// The replica store, with faults and crash points.
    pub remote: RemoteStore,
    /// The staging store with nothing in front of it.
    pub staging_raw: MemStore,
    /// The replica with nothing in front of it: what the oracle restores from.
    pub remote_raw: MemStore,
    /// What the staging store really did.
    pub staging_tap: TapLog,
    /// What the replica really did.
    pub remote_tap: TapLog,
    /// Schedules staging faults.
    pub staging_faults: FaultHandle,
    /// Schedules replica faults.
    pub remote_faults: FaultHandle,
    /// Staging's power-loss bookkeeping.
    pub stage: StageHandle,
    /// What the database really was.
    pub model: Model,
    /// What the engine has acknowledged.
    pub acked: Acked,
    crash_entropy: SeededEntropy,
    in_tick: Option<CommittedInTick>,
}

/// The first instant of a run's clock.
const EPOCH_MILLIS: i64 = 1_700_000_000_000;

impl World {
    /// A world with a database of `db_pages` pages, empty stores and no faults.
    #[must_use]
    pub fn new(seed: Seed, page_size: PageSize, db_pages: u32) -> Self {
        let sqlite = SimSqlite::new(page_size, db_pages, seed.stream(1));
        let staging_raw = MemStore::new();
        let remote_raw = MemStore::new();
        let stage = Stage::new(staging_raw.clone());
        let stage_handle = stage.handle();
        let staging_tap = Tap::new(stage);
        let staging_tap_log = staging_tap.log();
        let staging = Faults::new(Yield::new(staging_tap), Vec::new(), seed.stream(2))
            .volatile_on_late_error(stage_handle.clone());
        let remote_tap = Tap::new(remote_raw.clone());
        let remote_tap_log = remote_tap.log();
        let remote = Faults::new(Yield::new(remote_tap), Vec::new(), seed.stream(3))
            .corrupting_through(remote_raw.clone());
        let initial = PageImage::from_bytes(page_size, &sqlite.image());
        Self {
            seed,
            sqlite,
            clock: FakeClock::new(Timestamp::from_unix_millis(EPOCH_MILLIS)),
            sleeper: RecordingSleeper::new(),
            entropy: SeededEntropy::new(seed.stream(4)),
            staging_faults: staging.handle(),
            remote_faults: remote.handle(),
            staging,
            remote,
            staging_raw,
            remote_raw,
            staging_tap: staging_tap_log,
            remote_tap: remote_tap_log,
            stage: stage_handle,
            model: Model::new(initial),
            acked: Acked::new(),
            crash_entropy: SeededEntropy::new(seed.stream(5)),
            in_tick: None,
        }
    }

    /// The app commits `pages`, leaving `db_size` pages; `None` when SQLite's
    /// rules refuse (the write lock is held, or `db_size` is zero).
    ///
    /// A page that comes back after a shrink and that the commit does not write
    /// holds whatever the rules' file still has, so the model takes it from
    /// their image instead of assuming the app wrote it.
    pub fn app_commit(&mut self, pages: &[(PageNo, Vec<u8>)], db_size: u32) -> Option<CommitId> {
        let before = self.model.tip_image().db_size();
        let app = self.sqlite.commit(pages, db_size)?;
        self.retire_generation(app.salts);
        let mut written = pages.to_vec();
        if db_size > before {
            written.extend(self.re_added(pages, before, db_size));
        }
        Some(
            self.model
                .commit(&written, db_size, app, self.remote_tap.len()),
        )
    }

    /// Arms a commit of `pages`, leaving `db_size` pages, that the app makes
    /// inside the next tick, right before a checkpoint takes its barrier. It
    /// must not grow the database: the pages a growth re-adds are read from the
    /// rules after the tick, when the engine may have changed them.
    pub fn arm_commit_in_tick(&self, pages: Vec<(PageNo, Vec<u8>)>, db_size: u32) {
        let (remote, staging) = (self.remote_tap.clone(), self.staging_tap.clone());
        let created_at = Box::new(move || (remote.len(), staging.len()));
        self.sqlite.arm_commit_in_tick(pages, db_size, created_at);
    }

    /// The app's commit inside the last tick, once: see [`World::arm_commit_in_tick`].
    pub fn take_commit_in_tick(&mut self) -> Option<CommittedInTick> {
        self.in_tick.take()
    }

    /// Records the armed commit in the model if the tick made it, and
    /// disarms it.
    fn record_commit_in_tick(&mut self) -> Option<CommittedInTick> {
        let done = self.sqlite.take_commit_in_tick()?;
        let (remote_at, staged_from) = done.created_at;
        self.retire_generation(done.app.salts);
        let id = self
            .model
            .commit(&done.pages, done.db_size, done.app, remote_at);
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
            .tip()
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
        let size = self.sqlite.page_size();
        let image = self.sqlite.image();
        let first = image.get(..size.bytes()).unwrap_or_default().to_vec();
        let pages: Vec<(PageNo, Vec<u8>)> = PageNo::new(1, size)
            .map(|one| (one, first))
            .into_iter()
            .collect();
        Some(
            self.model
                .commit(&pages, db_size, app, self.remote_tap.len()),
        )
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
        Some(self.model.vacuum(image, app, self.remote_tap.len()))
    }

    /// Records a commit the engine made itself: a checkpoint's bump of page 1
    /// (`db.go:1668`) puts a one-page commit into the WAL that the app never
    /// made, and a file of the sync that follows ends on it. Its pages are the
    /// tip's, so the model keeps the tip's image under the new end. `at` is the
    /// replica tap's length when the tick started, so no earlier put can be
    /// taken to cover it.
    pub fn sync_model_at(&mut self, at: usize) {
        let frames = self.sqlite.mx_frame();
        let salts = self.sqlite.salts();
        self.retire_generation(salts);
        if frames == 0 {
            return;
        }
        let end = WalOffset::from_frame_index(frames, self.sqlite.page_size());
        let known = self
            .model
            .tip()
            .and_then(|id| self.model.rec(id))
            .is_some_and(|rec| rec.salts == salts && rec.end.get() == end.get());
        if !known {
            self.model
                .commit(&[], self.sqlite.db_size(), AppCommit { salts, end }, at);
        }
    }

    /// The driver starts: see `SimSqlite::start_driver`. The bump frame it may
    /// write is a commit the app never made, which the model takes up now.
    pub fn start_driver(&mut self) {
        let at = self.remote_tap.len();
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
        let at = self.remote_tap.len();
        let ran = run_until(fut, budget);
        // The app's commit inside the tick came before the engine's own frames
        // that followed it, so the model takes it up first.
        self.in_tick = self.record_commit_in_tick();
        // Before the crash takes anything: a power loss decides which of the
        // engine's own commits survive, so the model must know them first.
        self.sync_model_at(at);
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
                    self.stage
                        .power_loss(&self.staging_raw, &self.crash_entropy),
                )
                .unwrap_or_default();
                self.model
                    .on_power_loss(self.sqlite.salts(), self.sqlite.mx_frame());
                self.acked.note_rewound();
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
    /// transaction; false when the replica is empty.
    ///
    /// # Errors
    /// The store's error when it cannot list or delete.
    pub fn remote_roll_back(&self) -> Result<bool, StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let mut files = Vec::new();
        for level in Level::ALL {
            files.extend(block_on(self.remote_raw.list(level, None, PageLimit::MAX))?.files);
        }
        let Some(newest) = files.iter().map(|f| f.key.range().max()).max() else {
            return Ok(false);
        };
        let keys: Vec<FileKey> = files
            .iter()
            .map(|f| f.key)
            .filter(|key| key.range().max() == newest)
            .collect();
        self.remote_delete(&keys)?;
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
    pub fn remote_put_unrecognized(&self) -> Result<bool, StoreError> {
        use lsr_core::{Level, ObjectStore};
        use lsr_ltx::TxidRange;
        let Some(newest) = self.remote_level_zero()?.last().map(|f| f.key) else {
            return Ok(false);
        };
        let Some(next) = newest.range().max().next() else {
            return Ok(false);
        };
        let key = FileKey::new(Level::ZERO, TxidRange::single(next));
        // Another writer's file is a sound one: the same pages under the next
        // transaction.
        let Some(bytes) = rewritten(&read_all(&self.remote_raw, &newest)?, |fields| {
            fields.txids = TxidRange::single(next);
        }) else {
            return Ok(false);
        };
        let at = lsr_core::Clock::now(&self.clock);
        let mut up = block_on(self.remote_raw.begin_upload(&key, at))?;
        block_on(lsr_core::Upload::write(&mut up, &bytes))?;
        block_on(lsr_core::Upload::commit(up))?;
        Ok(true)
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

    /// Whether the staged file `key` holds every page of its database, which
    /// is what a snapshot looks like: the engine writes one as a level-0 file
    /// whose header still names the WAL range it was cut from. False when the
    /// file is gone or does not decode.
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

    fn staged_files(&self) -> Result<Vec<FileInfo>, StoreError> {
        use lsr_core::{Level, ObjectStore, PageLimit};
        let mut files = Vec::new();
        for level in Level::ALL {
            files.extend(block_on(self.staging_raw.list(level, None, PageLimit::MAX))?.files);
        }
        Ok(files)
    }

    /// The page size each staged and replica file's header names, a file
    /// whose header does not decode left out.
    ///
    /// # Errors
    /// What the store said when it could not list or read.
    pub fn page_sizes(&self) -> Result<Vec<(FileKey, PageSize)>, String> {
        use lsr_core::{Inventory, ObjectStore, ReadLen};
        use lsr_ltx::{HEADER_LEN, Header};
        let len = u32::try_from(HEADER_LEN).ok().and_then(ReadLen::new);
        let mut out = Vec::new();
        for store in [&self.staging_raw, &self.remote_raw] {
            let inventory = block_on(Inventory::load(store)).map_err(|err| err.to_string())?;
            for info in inventory.files() {
                let Some(len) = len else { break };
                let bytes =
                    block_on(store.read(&info.key, 0, len)).map_err(|err| err.to_string())?;
                let header = <[u8; HEADER_LEN]>::try_from(bytes)
                    .ok()
                    .and_then(|raw| Header::decode(&raw).ok());
                if let Some(header) = header {
                    out.push((info.key, header.page_size()));
                }
            }
        }
        Ok(out)
    }

    fn read_staged(&self, key: &FileKey) -> Result<Vec<u8>, StoreError> {
        read_all(&self.staging_raw, key)
    }

    fn write_staged(&self, info: &FileInfo, bytes: &[u8]) -> Result<(), StagedEditError> {
        use lsr_core::{ObjectStore, Upload};
        let mut up = block_on(self.staging_raw.begin_upload(&info.key, listed_at(info)?))?;
        block_on(up.write(bytes))?;
        block_on(up.commit())?;
        Ok(())
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
        self.model.vacuum(image, app, self.remote_tap.len());
    }

    /// Whether the model's newest image is the database the rules hold: a check
    /// of the harness against itself.
    #[must_use]
    pub fn model_matches_sqlite(&self) -> bool {
        let tip = self.model.tip_image();
        let real = self.sqlite.image();
        let size = tip.page_size().bytes();
        real.len() == size.saturating_mul(usize::try_from(tip.db_size()).unwrap_or(usize::MAX))
            && real.chunks_exact(size).enumerate().all(|(i, page)| {
                u32::try_from(i)
                    .ok()
                    .and_then(|i| i.checked_add(1))
                    .and_then(|pgno| tip.page(pgno))
                    .is_some_and(|want| want.as_slice() == page)
            })
    }

    /// Judges the replica.
    ///
    /// # Errors
    /// The first [`Violation`].
    pub fn check(&self) -> Result<Checked, Violation> {
        check(&self.model, &self.remote_tap, &self.remote_raw, &self.acked)
    }
}

#[cfg(test)]
mod tests;

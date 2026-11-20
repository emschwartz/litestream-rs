//! One run's worth of machinery, built from a seed.
//!
//! [`World`] owns the SQLite rules, the clock, both stores with their wrappers,
//! and the model and acknowledgement record the oracle judges against. It owns
//! no loop logic: the run loop (task 6f) builds the actor from these handles and
//! hands each tick's future to [`World::run_tick`], the one seam between this
//! harness and the actor.
//!
//! Staging is `Faults<Yield<Tap<Stage<MemStore>>>>` and the replica is
//! `Faults<Yield<Tap<MemStore>>>`. Both keep a raw `MemStore` handle for the
//! oracle and for power loss, so neither a fault nor a yield touches a check.

use lsr_core::conformance::block_on;
use lsr_core::{
    Entropy, FakeClock, FileKey, MemStore, RecordingSleeper, SeededEntropy, StoreError,
};
use lsr_ltx::{PageNo, PageSize, Timestamp};
use lsr_wal::{Salts, WalOffset};

use crate::exec::{Polls, RunEnd, run_until};
use crate::model::{CommitId, Model, PageImage};
use crate::oracle::{Acked, Checked, Violation, check};
use crate::seed::Seed;
use crate::sqlite::{AppCommit, CrashKind, SimSqlite, StrangerMode};
use crate::store::{FaultHandle, Faults, Lost, Stage, StageHandle, Tap, TapLog, Yield};

/// The staging store the actor sees.
pub type StagingStore = Faults<Yield<Tap<Stage<MemStore>>>>;
/// The replica store the actor sees.
pub type RemoteStore = Faults<Yield<Tap<MemStore>>>;

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
        let remote = Faults::new(Yield::new(remote_tap), Vec::new(), seed.stream(3));
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
    pub fn stranger_checkpoint(&mut self, mode: StrangerMode) -> bool {
        let ran = self.sqlite.stranger_checkpoint(mode).is_some();
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

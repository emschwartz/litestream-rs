//! The sync engine's steps one at a time over a backend, without the actor.

use std::path::{Path, PathBuf};

use lsr_core::conformance::block_on;
use lsr_core::{
    CheckpointMode, CheckpointOutcome, FileInfo, MemStore, PageChecksumTable, Position,
    PositionSource, Probes, ReachedEnd, Step, StepEnd, SyncConfig, Synced, SyncedThrough, Verdict,
    WalView, checkpoint, recover_traced, sync_step,
};
use lsr_ltx::{Timestamp, Txid};
use lsr_wal::{Salts, WalReader};

use super::app::{APP, App, Placement};
use super::{Backend, RigError, be32, create, fresh_vfs, read_file, suffixed};
use crate::testing::vfs::FaultVfs;

/// What one sync did.
#[derive(Debug)]
pub enum StepDid {
    /// Nothing new.
    Idle,
    /// One file staged, holding `txs` transactions.
    Wrote {
        /// The file.
        file: FileInfo,
        /// Transactions in it.
        txs: u32,
    },
}

/// An application connection and a backend on one database, and the engine's last proven position.
/// The application is in this process (`crate::app_connection` set up, busy timeout zero) or in
/// another one at SQLite's defaults ([`Placement`]); its connection 0 is the application's,
/// any other id a second application connection a case opens.
#[derive(Debug)]
pub struct Stepper<B: Backend> {
    db: PathBuf,
    vfs: FaultVfs,
    app: App,
    /// The backend under test.
    pub backend: B,
    /// What the engine staged.
    pub staging: MemStore,
    through: Option<SyncedThrough>,
    position: Option<Position>,
    /// The page checksum table, as the actor keeps one.
    pages: PageChecksumTable,
    now: i64,
}

impl<B: Backend> Stepper<B> {
    /// A database `app.db` in `root` with one table and one row, and a backend on it, the
    /// application in this process.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn new(root: &Path, page_size: u32) -> Result<Self, RigError> {
        Self::placed(root, page_size, Placement::InProcess)
    }

    /// [`Self::new`] with the application in `placement`.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn placed(root: &Path, page_size: u32, placement: Placement) -> Result<Self, RigError> {
        let vfs = fresh_vfs()?;
        vfs.set_window(false);
        let db = root.join("app.db");
        create(&db, &vfs, page_size)?;
        let mut app = App::new(placement, &db, vfs, "NORMAL")?;
        app.open(APP)?;
        let backend =
            B::open_backend(&db, vfs.name()).map_err(|err| RigError::Open(Box::new(err)))?;
        let mut stepper = Self {
            db,
            vfs,
            app,
            backend,
            staging: MemStore::new(),
            through: None,
            position: None,
            pages: PageChecksumTable::new(),
            now: 1_000,
        };
        stepper.write(100)?;
        Ok(stepper)
    }

    /// The database file.
    #[must_use]
    pub fn db(&self) -> &Path {
        &self.db
    }

    /// The rig's fault VFS.
    #[must_use]
    pub const fn vfs(&self) -> &FaultVfs {
        &self.vfs
    }

    /// Where the application runs.
    #[must_use]
    pub const fn placement(&self) -> Placement {
        self.app.placement()
    }

    /// Opens application connection `id` (0 is the application's: opening it again closes the old
    /// one first).
    ///
    /// # Errors
    /// SQLite's.
    pub fn app_open(&mut self, id: u32) -> Result<(), RigError> {
        self.app.open(id)
    }

    /// Runs `sql` on application connection `id`.
    ///
    /// # Errors
    /// SQLite's.
    pub fn app_exec(&mut self, id: u32, sql: &str) -> Result<(), RigError> {
        self.app.exec(id, sql)
    }

    /// Closes application connection `id`.
    ///
    /// # Errors
    /// SQLite's.
    pub fn app_close(&mut self, id: u32) -> Result<(), RigError> {
        self.app.close(id)
    }

    /// The application's process opens and closes the database and `-shm` with `std::fs`. Only for a
    /// application in another process.
    ///
    /// # Errors
    /// [`RigError::State`] in process; the application's error.
    pub fn app_touches_the_files(&mut self) -> Result<(), RigError> {
        self.app.touch()
    }

    /// The WAL file.
    #[must_use]
    pub fn wal(&self) -> PathBuf {
        suffixed(&self.db, "-wal")
    }

    /// One application insert of `bytes` random bytes.
    ///
    /// # Errors
    /// SQLite's.
    pub fn write(&mut self, bytes: u32) -> Result<(), RigError> {
        self.app.exec(
            APP,
            &format!("INSERT INTO t(v) VALUES (randomblob({bytes}))"),
        )
    }

    /// The WAL file's bytes.
    ///
    /// # Errors
    /// The read's.
    pub fn wal_bytes(&self) -> Result<Vec<u8>, RigError> {
        read_file(&self.wal())
    }

    /// The WAL header's salts, read from the file.
    ///
    /// # Errors
    /// The read's; [`RigError::State`] when the file has no header.
    pub fn salts(&self) -> Result<Salts, RigError> {
        let bytes = self.wal_bytes()?;
        match (be32(&bytes, 16), be32(&bytes, 20)) {
            (Some(salt1), Some(salt2)) => Ok(Salts { salt1, salt2 }),
            (Some(_) | None, _) => Err(RigError::State("read a WAL header")),
        }
    }

    /// A checkpoint run by the application, as `PRAGMA wal_checkpoint(<mode>)`: busy, log, checkpointed.
    ///
    /// # Errors
    /// SQLite's.
    pub fn app_checkpoint(&mut self, mode: &str) -> Result<(i64, i64, i64), RigError> {
        let row = self
            .app
            .query(APP, &format!("PRAGMA wal_checkpoint({mode})"))?;
        match row[..] {
            [busy, log, done] => Ok((busy, log, done)),
            _ => Err(RigError::State("read a checkpoint's three counts")),
        }
    }

    /// The last proven position's transaction.
    #[must_use]
    pub fn txid(&self) -> Option<Txid> {
        self.position.map(|p| p.txid())
    }

    /// The synced end of the last proven position, in bytes.
    #[must_use]
    pub fn synced_end(&self) -> Option<u64> {
        self.position.map(|p| p.synced().offset().get())
    }

    fn view(&mut self) -> Result<(WalReader<B::Wal>, WalView, lsr_core::CommittedRead), RigError> {
        let source = |err| RigError::Store(format!("{err}"));
        let file = self
            .backend
            .open()
            .map_err(source)?
            .ok_or(RigError::State("open a missing WAL"))?;
        let wal = WalReader::open(file).map_err(|err| RigError::Store(format!("{err}")))?;
        let committed = self.backend.committed().map_err(source)?;
        let view = WalView {
            header: Some(*wal.header()),
            len: self.backend.len().map_err(source)?,
            committed: Some(committed),
            epoch: committed.epoch(),
        };
        Ok((wal, view, committed))
    }

    /// What the engine would decide now from the last proven position.
    ///
    /// # Errors
    /// The backend's, as [`RigError::Store`].
    pub fn verdict(&mut self) -> Result<Verdict, RigError> {
        let (mut wal, view, committed) = self.view()?;
        let Some(position) = self.position.as_ref() else {
            return Ok(recover_traced(None, &view, None));
        };
        let probes = Probes::read_against(&mut wal, position, &committed)
            .map_err(|err| RigError::Store(format!("{err:?}")))?;
        Ok(recover_traced(
            Some(PositionSource::Running(position, probes)),
            &view,
            None,
        ))
    }

    /// Recovers, then runs the step the verdict calls for.
    ///
    /// # Errors
    /// [`RigError::Store`] for a stop or a failed step.
    pub fn sync(&mut self) -> Result<StepDid, RigError> {
        let verdict = self.verdict()?;
        let step = step_for(verdict.recovered, self.position.as_ref())
            .map_err(|stop| RigError::Store(format!("the verdict stops: {stop:?}")))?;
        let (mut wal, _, committed) = self.view()?;
        self.now = self.now.saturating_add(10);
        let mut db = self.backend.clone();
        let synced = block_on(sync_step(
            step,
            &mut wal,
            committed,
            &mut db,
            &mut self.pages,
            &self.staging,
            &SyncConfig::default(),
            Timestamp::from_unix_millis(self.now),
        ))
        .map_err(|failed| RigError::Store(format!("{failed:?}")))?;
        match synced {
            Synced::Idle(reached) => {
                self.confirm(reached);
                Ok(StepDid::Idle)
            }
            Synced::Wrote {
                file,
                end: StepEnd::Through(reached),
                txs,
                ..
            } => {
                self.confirm(reached);
                Ok(StepDid::Wrote { file, txs })
            }
            Synced::Wrote {
                end: StepEnd::Limited(_),
                ..
            } => Err(RigError::State("sync past the default cap")),
            Synced::IndexReset
            | Synced::Wrote {
                end: StepEnd::Unproven(_),
                ..
            } => Err(RigError::State(
                "prove a position under a wal-index reset ahead of its header",
            )),
        }
    }

    /// Takes a step's end as the engine does: the proof when the read-lock hold
    /// still holds, the unpinned position when it moved.
    fn confirm(&mut self, reached: ReachedEnd) {
        match reached.confirm(&self.backend) {
            Ok(through) => {
                self.position = Some(*through.position());
                self.through = Some(through);
            }
            Err(position) => {
                self.position = Some(position);
                self.through = None;
            }
        }
    }

    /// Runs the engine's checkpoint protocol from the last proven position.
    ///
    /// # Errors
    /// [`RigError::State`] before a sync; [`RigError::Store`] for a failed checkpoint.
    pub fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointOutcome, RigError> {
        let proof = self
            .through
            .take()
            .ok_or(RigError::State("checkpoint before a sync"))?;
        self.now = self.now.saturating_add(10);
        let (mut wal, mut db, mut ck) = (
            self.backend.clone(),
            self.backend.clone(),
            self.backend.clone(),
        );
        let done = block_on(checkpoint(
            proof,
            mode,
            &mut wal,
            &mut db,
            &mut self.pages,
            &mut ck,
            &self.staging,
            &SyncConfig::default(),
            &lsr_core::FakeClock::new(Timestamp::from_unix_millis(self.now)),
        ))
        .map_err(|failed| RigError::Store(format!("{failed:?}")))?;
        self.position = Some(done.staged.position());
        self.through = done.staged.into_proof();
        Ok(done.outcome)
    }
}

/// The step a recovery verdict calls for, as the actor builds it: a snapshot keeps the clock
/// from stepping back past the position it replaces.
pub fn step_for(
    recovered: lsr_core::Recovered,
    position: Option<&lsr_core::Position>,
) -> Result<Step, lsr_core::StopCause> {
    match recovered {
        lsr_core::Recovered::Continue(position) => Ok(Step::Incremental(position)),
        lsr_core::Recovered::WalRestarted(position) => Ok(Step::NewGeneration(position)),
        lsr_core::Recovered::Snapshot { after, cause } => Ok(Step::Snapshot {
            after,
            prev_timestamp: position.map(lsr_core::Position::timestamp),
            cause,
        }),
        lsr_core::Recovered::Stop(cause) => Err(cause),
    }
}

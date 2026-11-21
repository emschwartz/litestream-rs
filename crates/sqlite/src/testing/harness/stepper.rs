//! The sync engine's steps one at a time over a backend, without the actor.

use std::path::{Path, PathBuf};

use lsr_core::conformance::block_on;
use lsr_core::{
    CheckpointMode, CheckpointOutcome, FileInfo, MemStore, Probes, Step, StepEnd, SyncConfig,
    Synced, SyncedThrough, Tip, Verdict, WalView, checkpoint, recover_traced, sync_step,
};
use lsr_ltx::{Timestamp, Txid};
use lsr_wal::{Salts, WalReader};
use rusqlite::Connection;

use super::{Backend, RigError, be32, create, fresh_vfs, open_app, read_file, suffixed};
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

/// A host connection and a backend on one database, and the engine's last proven position.
#[derive(Debug)]
pub struct Stepper<B: Backend> {
    db: PathBuf,
    vfs: FaultVfs,
    /// The host's connection (`crate::app_connection` set up, busy timeout zero).
    pub app: Connection,
    /// The backend under test.
    pub backend: B,
    /// What the engine staged.
    pub staging: MemStore,
    through: Option<SyncedThrough>,
    now: i64,
}

impl<B: Backend> Stepper<B> {
    /// A database `app.db` in `root` with one table and one row, and a backend on it.
    ///
    /// # Errors
    /// [`RigError`].
    pub fn new(root: &Path, page_size: u32) -> Result<Self, RigError> {
        let vfs = fresh_vfs()?;
        vfs.set_window(false);
        let db = root.join("app.db");
        create(&db, &vfs, page_size)?;
        let app = open_app(&db, &vfs, "NORMAL")?;
        let backend =
            B::open_backend(&db, vfs.name()).map_err(|err| RigError::Open(Box::new(err)))?;
        let stepper = Self {
            db,
            vfs,
            app,
            backend,
            staging: MemStore::new(),
            through: None,
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

    /// The WAL file.
    #[must_use]
    pub fn wal(&self) -> PathBuf {
        suffixed(&self.db, "-wal")
    }

    /// One application insert of `bytes` random bytes.
    ///
    /// # Errors
    /// SQLite's.
    pub fn write(&self, bytes: u32) -> Result<(), RigError> {
        self.app
            .execute("INSERT INTO t(v) VALUES (randomblob(?1))", [bytes])?;
        Ok(())
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

    /// A checkpoint run by the host, as `PRAGMA wal_checkpoint(<mode>)`: busy, log, checkpointed.
    ///
    /// # Errors
    /// SQLite's.
    pub fn app_checkpoint(&self, mode: &str) -> Result<(i64, i64, i64), RigError> {
        Ok(self
            .app
            .query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?)
    }

    /// The last proven position's transaction.
    #[must_use]
    pub fn txid(&self) -> Option<Txid> {
        self.through.as_ref().map(|t| t.position().txid())
    }

    /// The synced end of the last proven position, in bytes.
    #[must_use]
    pub fn synced_end(&self) -> Option<u64> {
        self.through
            .as_ref()
            .map(|t| t.position().synced().offset().get())
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
            epoch: self.backend.epoch(),
        };
        Ok((wal, view, committed))
    }

    /// What the engine would decide now from the last proven position.
    ///
    /// # Errors
    /// The backend's, as [`RigError::Store`].
    pub fn verdict(&mut self) -> Result<Verdict, RigError> {
        let (mut wal, view, committed) = self.view()?;
        let Some(position) = self.through.as_ref().map(SyncedThrough::position) else {
            return Ok(recover_traced(None, &view, None));
        };
        let probes = Probes::read_against(&mut wal, position, &committed)
            .map_err(|err| RigError::Store(format!("{err:?}")))?;
        Ok(recover_traced(
            Some(Tip::Running(position, probes)),
            &view,
            None,
        ))
    }

    /// Recovers, then runs the step the verdict calls for.
    ///
    /// # Errors
    /// [`RigError::Store`] for a halt or a failed step.
    pub fn sync(&mut self) -> Result<StepDid, RigError> {
        let verdict = self.verdict()?;
        let step = Step::try_from(verdict.recovered)
            .map_err(|halt| RigError::Store(format!("the verdict halts: {halt:?}")))?;
        let (mut wal, _, committed) = self.view()?;
        self.now = self.now.saturating_add(10);
        let epoch = self.backend.epoch();
        let mut db = self.backend.clone();
        let synced = block_on(sync_step(
            step,
            &mut wal,
            committed,
            epoch,
            &mut db,
            &self.staging,
            &SyncConfig::default(),
            Timestamp::from_unix_millis(self.now),
        ))
        .map_err(|failed| RigError::Store(format!("{failed:?}")))?;
        match synced {
            Synced::Idle(through) => {
                self.through = Some(through);
                Ok(StepDid::Idle)
            }
            Synced::Wrote {
                file,
                end: StepEnd::Through(through),
                txs,
                ..
            } => {
                self.through = Some(through);
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
            &mut ck,
            &self.staging,
            &SyncConfig::default(),
            Timestamp::from_unix_millis(self.now),
        ))
        .map_err(|failed| RigError::Store(format!("{failed:?}")))?;
        self.through = Some(done.through);
        Ok(done.outcome)
    }
}

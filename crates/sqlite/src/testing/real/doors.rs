//! The engine's three doors on the backend: each call first counts as one of the tick's calls
//! (where the application's armed operation may run), then takes an injected error of its
//! kind, then goes to the engine's real handle, which is looked up on every call so a crash
//! that closed it is seen at once.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use lsr_core::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, ReadLockHold,
    SourceError, SourceOp, WalFileLen, WalSource,
};
use lsr_ltx::{DbPages, PageNo, Timestamp};

use super::state::{Real, lock};
use crate::Sqlite;

/// What every door shares: the backend.
#[derive(Clone)]
pub(super) struct Door(pub(super) Arc<Mutex<Real>>);

/// The engine has no handle: its process is down and the database would not open.
const CLOSED: SourceError = SourceError::Io(io::ErrorKind::NotConnected);

impl Door {
    fn before(&self, kind: Option<SourceOp>) -> Result<Sqlite, SourceError> {
        let mut real = lock(&self.0);
        real.call(kind);
        if let Some(op) = kind
            && let Some(at) = real.injected.iter().position(|(of, _)| *of == op)
        {
            let taken = real.injected.remove(at);
            real.taken.push(taken);
            return Err(taken.1);
        }
        real.engine().ok_or(CLOSED)
    }
}

pub(super) struct RealSource(pub(super) Door);

/// The WAL file the engine opened; each read counts as one of the tick's calls.
pub(super) struct RealWal {
    file: File,
    door: Door,
}

impl Read for RealWal {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        lock(&self.door.0).call(None);
        self.file.read(buf)
    }
}

impl Seek for RealWal {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

impl WalSource for RealSource {
    type Wal = RealWal;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        let mut engine = self.0.before(Some(SourceOp::Open))?;
        let door = self.0.clone();
        Ok(engine.open()?.map(|file| RealWal { file, door }))
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        self.0.before(Some(SourceOp::Len))?.len()
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        self.0.before(Some(SourceOp::Committed))?.committed()
    }

    fn epoch(&self) -> ReadLockHold {
        lock(&self.0.0)
            .engine
            .as_ref()
            .map_or_else(ReadLockHold::first, WalSource::epoch)
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        let mut engine = self.0.before(Some(SourceOp::Sync))?;
        let (lie, vfs) = {
            let real = lock(&self.0.0);
            (real.lie_ours, real.vfs)
        };
        vfs.lie_on_syncs(lie);
        let synced = engine.sync();
        vfs.lie_on_syncs(false);
        synced
    }

    fn modified(&mut self) -> Result<Option<Timestamp>, SourceError> {
        self.0.before(None)?.modified()
    }
}

pub(super) struct RealPages(pub(super) Door);

impl PageSource for RealPages {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        self.0.before(Some(SourceOp::PageCount))?.page_count()
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        self.0
            .before(Some(SourceOp::ReadPage))?
            .read_page(pgno, buf)
    }
}

pub(super) struct RealCheckpointer(pub(super) Door);

impl Checkpointer for RealCheckpointer {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.0.before(Some(SourceOp::BeginBarrier))?.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.0
            .before(Some(SourceOp::EndBarrier))?
            .end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        self.0.before(Some(SourceOp::Checkpoint))?.checkpoint(mode)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.0.before(Some(SourceOp::BumpSeq))?.bump_seq()
    }
}

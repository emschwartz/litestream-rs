//! The seam between the simulator's world and the SQLite it runs the engine against.
//!
//! The world and the runner reach SQLite only through [`SqliteSide`]: the engine's three
//! handles (its WAL source, its page source and its checkpointer), the application's and other
//! processes' actions, the faults a history draws, the crash laws and what the judges read
//! back. [`SimSqlite`], the rule model, is one implementation. A backend over real SQLite
//! (`lsr-sqlite`'s testing harness, which depends on this crate and so cannot be named here)
//! is another, and answers `false` or `None` for a drawn action it cannot perform, which a
//! drawn history skips as it skips any step with nothing to act on.
//!
//! The engine's handles are boxed ([`SideSource`], [`SidePages`], [`SideCheckpointer`]) so
//! that the world names one type whatever the backend: each call goes through one dynamic
//! dispatch to the backend's own method and changes nothing it answers.
//!
//! A real backend cannot write the world's page bytes as given: it makes each commit as SQL,
//! and SQLite decides which pages change. [`SqliteSide::take_written`] then reports what the
//! commit wrote, read back through SQLite, and the world records that in the model instead of
//! what it asked for.

use std::fmt::Debug;
use std::io::{Read, Seek};
use std::sync::Arc;

use lsr_core::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, ReadLockHold,
    SourceError, SourceOp, WalFileLen, WalSource, WriteLock,
};
use lsr_ltx::{DbPages, PageNo, PageSize};
use lsr_wal::Salts;

use crate::Seed;
use crate::crash_law::CrashLaw;
use crate::sqlite::{
    AppCommit, AutoTally, CheckpointNumbers, CrashKind, CreatedAt, InTickAt, InTickCommit,
    InTickOp, InTickRan, InTickSite, OtherProcessMode, ReaderId, SimSqlite, StagingLen,
};

/// An open WAL file, whatever the backend opened.
pub trait ReadSeek: Read + Seek {}

impl<T: Read + Seek> ReadSeek for T {}

/// The engine's WAL source over any backend's.
pub struct SideSource(Box<dyn WalSource<Wal = Box<dyn ReadSeek>>>);

impl Debug for SideSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SideSource")
    }
}

/// A backend's WAL source with its open file boxed.
struct BoxedWal<S>(S);

impl<S: WalSource> WalSource for BoxedWal<S>
where
    S::Wal: 'static,
{
    type Wal = Box<dyn ReadSeek>;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        Ok(self
            .0
            .open()?
            .map(|wal| -> Box<dyn ReadSeek> { Box::new(wal) }))
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        self.0.len()
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        self.0.committed()
    }

    fn epoch(&self) -> ReadLockHold {
        self.0.epoch()
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        self.0.sync()
    }
}

impl SideSource {
    /// Boxes `source`.
    pub fn new<S: WalSource + 'static>(source: S) -> Self
    where
        S::Wal: 'static,
    {
        Self(Box::new(BoxedWal(source)))
    }
}

impl WalSource for SideSource {
    type Wal = Box<dyn ReadSeek>;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        self.0.open()
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        self.0.len()
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        self.0.committed()
    }

    fn epoch(&self) -> ReadLockHold {
        self.0.epoch()
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        self.0.sync()
    }
}

/// The engine's page source over any backend's.
pub struct SidePages(Box<dyn PageSource>);

impl Debug for SidePages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SidePages")
    }
}

impl SidePages {
    /// Boxes `pages`.
    pub fn new<P: PageSource + 'static>(pages: P) -> Self {
        Self(Box::new(pages))
    }
}

impl PageSource for SidePages {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        self.0.page_count()
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        self.0.read_page(pgno, buf)
    }
}

/// The engine's checkpointer over any backend's.
pub struct SideCheckpointer(Box<dyn Checkpointer>);

impl Debug for SideCheckpointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SideCheckpointer")
    }
}

impl SideCheckpointer {
    /// Boxes `ck`.
    pub fn new<C: Checkpointer + 'static>(ck: C) -> Self {
        Self(Box::new(ck))
    }
}

impl Checkpointer for SideCheckpointer {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.0.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.0.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        self.0.checkpoint(mode)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.0.bump_seq()
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        self.0.begin_write_lock()
    }

    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError> {
        self.0.end_write_lock(lock)
    }
}

/// Builds a run's SQLite from the run's seed, its page size and its database's first size.
pub type MakeSide = dyn Fn(Seed, PageSize, u32) -> Arc<dyn SqliteSide> + Send + Sync;

/// Which SQLite a run's world uses: the rule model ([`SimSqlite`]) unless a maker is given.
#[derive(Clone, Default)]
pub struct SideMaker(Option<Arc<MakeSide>>);

impl Debug for SideMaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "SideMaker(given)"
        } else {
            "SideMaker(model)"
        })
    }
}

impl SideMaker {
    /// Runs use what `make` builds.
    pub fn new(make: Arc<MakeSide>) -> Self {
        Self(Some(make))
    }

    /// The SQLite for a run of `seed` over `db_pages` pages of `page_size`.
    #[must_use]
    pub fn make(&self, seed: Seed, page_size: PageSize, db_pages: u32) -> Arc<dyn SqliteSide> {
        match &self.0 {
            Some(make) => make(seed, page_size, db_pages),
            None => Arc::new(SimSqlite::new(page_size, db_pages, seed.stream(1))),
        }
    }
}

/// What a backend's commit actually wrote ([`SqliteSide::take_written`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Written {
    /// Every page whose bytes changed, with its new bytes, and every page past the old end.
    pub pages: Vec<(PageNo, Vec<u8>)>,
    /// The database size the change left.
    pub db_size: u32,
}

/// Declares [`SqliteSide`] and implements it for [`SimSqlite`] by delegation, from one list,
/// so the two cannot drift apart. Each method means what the model's method of the same name
/// says it means.
macro_rules! side {
    ($( #[doc = $doc:literal] fn $name:ident(&self $(, $arg:ident: $ty:ty)* $(,)?) $(-> $ret:ty)?; )*) => {
        /// Everything the world and the runner ask of SQLite (module doc).
        pub trait SqliteSide: Debug {
            /// The engine's WAL source.
            fn source(&self) -> SideSource;
            /// The engine's page source.
            fn pages(&self) -> SidePages;
            /// The engine's checkpointer.
            fn checkpointer(&self) -> SideCheckpointer;
            /// What changed in the database a new reader sees since the last call, as SQLite
            /// reads it back: a backend whose commits write other pages than the world asked
            /// for (real SQLite, through SQL) reports them here, and the model takes these up
            /// instead. `None` from a backend that writes exactly what it is asked (the model).
            fn take_written(&self) -> Option<Written> {
                None
            }
            $( #[doc = $doc] fn $name(&self $(, $arg: $ty)*) $(-> $ret)?; )*
        }

        impl SqliteSide for SimSqlite {
            fn source(&self) -> SideSource {
                SideSource::new(SimSqlite::source(self))
            }
            fn pages(&self) -> SidePages {
                SidePages::new(SimSqlite::pages(self))
            }
            fn checkpointer(&self) -> SideCheckpointer {
                SideCheckpointer::new(SimSqlite::checkpointer(self))
            }
            $( fn $name(&self $(, $arg: $ty)*) $(-> $ret)? { SimSqlite::$name(self $(, $arg)*) } )*
        }
    };
}

side! {
    /// What the driver does at process start.
    fn start_driver(&self);
    /// The engine's own commits (its sequence bumps) since the last call, oldest first; a backend that reports what it wrote has none.
    fn take_engine_commits(&self) -> Vec<AppCommit>;
    /// Our read transaction begins, outside a tick.
    fn our_reader_begin(&self) -> bool;
    /// Our read transaction ends, outside a tick.
    fn our_reader_end(&self) -> bool;
    /// The application commits `pages`, leaving `db_size` pages.
    fn commit(&self, pages: &[(PageNo, Vec<u8>)], db_size: u32) -> Option<AppCommit>;
    /// Another application connection, which stays open, commits `pages` under its own checkpoint count.
    fn other_writer_commit(&self, pages: &[(PageNo, Vec<u8>)], db_size: u32) -> Option<AppCommit>;
    /// Arms a commit the application makes inside the next tick.
    fn arm_commit_in_tick(&self, pages: Vec<(PageNo, Vec<u8>)>, db_size: u32, site: InTickSite, created_at: CreatedAt);
    /// The commit armed for the last tick, once.
    fn take_commit_in_tick(&self) -> Option<InTickCommit>;
    /// From now on the disk lies about our WAL sync.
    fn lie_on_our_sync(&self);
    /// Our WAL sync is honest again.
    fn honest_on_our_sync(&self);
    /// The application's automatic checkpoint threshold.
    fn set_app_autocheckpoint(&self, frames: Option<u32>);
    /// The application's automatic checkpoints so far.
    fn app_autocheckpoints(&self) -> AutoTally;
    /// The application writes frames of a transaction it does not commit.
    fn spill(&self, pages: &[(PageNo, Vec<u8>)]) -> bool;
    /// The application rolls its open transaction back.
    fn rollback(&self) -> bool;
    /// The application shrinks the database to `db_size` pages.
    fn shrink(&self, db_size: u32) -> Option<AppCommit>;
    /// The application vacuums into pages of `new`.
    fn rewrite_page_size(&self, new: PageSize) -> bool;
    /// Another process begins a read transaction.
    fn other_process_reader_begin(&self) -> ReaderId;
    /// That process's read transaction ends.
    fn other_process_reader_end(&self, id: ReaderId) -> bool;
    /// Another process checkpoints.
    fn other_process_checkpoint(&self, mode: OtherProcessMode) -> Option<CheckpointNumbers>;
    /// The last connection closes and the WAL is deleted.
    fn delete_wal(&self) -> bool;
    /// The WAL header is torn.
    fn tear_wal_header(&self) -> bool;
    /// The WAL file is cut to `len` bytes.
    fn truncate_wal_at(&self, len: u64) -> bool;
    /// One WAL bit flips.
    fn flip_bit(&self, bit: u64) -> bool;
    /// The WAL is rewritten with other bytes at the same length.
    fn rewrite_same_length(&self, seed: u64) -> bool;
    /// The WAL regrows under the same salts.
    fn regrow_same_salts(&self, seed: u64) -> bool;
    /// The WAL regrows under the same salts with no commit marks.
    fn regrow_without_commit_marks(&self, seed: u64) -> bool;
    /// The next wal-index read finds its two header copies unequal.
    fn shm_mid_update(&self) -> bool;
    /// The next wal-index read reports an end inside a transaction.
    fn shm_end_inside_tx(&self) -> bool;
    /// A frame is overwritten between the engine's scan and its read.
    fn overwrite_frame_between_scan_and_read(&self, frame: u32) -> bool;
    /// The WAL file is unlinked while SQLite keeps it open.
    fn unlink_wal(&self) -> bool;
    /// The wal-index reports an empty database once.
    fn empty_database_once(&self);
    /// One byte of a database page flips.
    fn flip_db_page(&self, pgno: u32) -> bool;
    /// Every queued injected source error is dropped.
    fn heal_injections(&self);
    /// The next TRUNCATE fails to truncate the file.
    fn fail_next_truncate(&self);
    /// The next driver start writes nothing.
    fn driver_writes_nothing_once(&self);
    /// The next call of `op` fails with `err`.
    fn inject(&self, op: SourceOp, err: SourceError);
    /// Injected source errors a call took, in order.
    fn taken_injections(&self) -> Vec<(SourceOp, SourceError)>;
    /// A power loss keeps a drawn prefix of the unsynced WAL writes.
    fn tear_on_power_loss(&self, seed: u64);
    /// A power loss keeps what the crash law draws.
    fn crash_by_law(&self, law: CrashLaw);
    /// Power losses under the block law that kept a block, and those with a hole.
    fn block_losses(&self) -> (u32, u32);
    /// Power losses under the block law that found unsynced database blocks.
    fn db_torn_losses(&self) -> u32;
    /// Every recovery the engine's parser and SQLite disagreed on.
    fn recovery_disagreements(&self) -> Vec<String>;
    /// Power losses that kept unsynced bytes, and those that cut a frame.
    fn torn_losses(&self) -> (u32, u32);
    /// A crash.
    fn crash(&self, kind: CrashKind);
    /// The database as a new reader sees it.
    fn image(&self) -> Vec<u8>;
    /// The database at each commit mark of the WAL's generation, ascending, with its frame end.
    fn commit_images(&self) -> Vec<(u32, Vec<u8>)>;
    /// The database file's bytes.
    fn db_file(&self) -> Vec<u8>;
    /// The WAL file's bytes, if there is a file.
    fn wal_file(&self) -> Option<Vec<u8>>;
    /// The page size.
    fn page_size(&self) -> PageSize;
    /// Whether WAL bytes `[start, end)` of `salts` would survive a power loss now.
    fn wal_range_durable(&self, salts: Salts, start: u64, end: u64) -> bool;
    /// Whether a power loss now would keep the whole WAL.
    fn wal_synced(&self) -> bool;
    /// `mxFrame`.
    fn mx_frame(&self) -> u32;
    /// `nBackfill`.
    fn backfilled(&self) -> u32;
    /// The WAL header's salts as a reader finds them.
    fn wal_header_salts(&self) -> Option<(u32, u32)>;
    /// Whether the wal-index is reset ahead of the WAL header.
    fn index_reset_ahead(&self) -> bool;
    /// Whether our read transaction pins the WAL.
    fn our_reader_pins_wal(&self) -> bool;
    /// The wal-index salts.
    fn salts(&self) -> Salts;
    /// The database size of the last commit.
    fn db_size(&self) -> u32;
    /// Arms an application operation inside the next tick.
    fn arm_app_in_tick(&self, when: InTickAt, op: InTickOp);
    /// Where an in-tick commit learns the staging log's length.
    fn app_staging_len(&self, len: StagingLen);
    /// A tick begins.
    fn app_tick_begins(&self);
    /// A tick ended: what the in-tick operation did.
    fn app_tick_ends(&self) -> Option<InTickRan>;
    /// The calls into SQLite the tick so far made.
    fn app_calls(&self) -> u32;
    /// What the in-tick operation did, once.
    fn take_app_in_tick(&self) -> Option<InTickRan>;
}

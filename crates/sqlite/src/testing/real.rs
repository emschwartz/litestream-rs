//! [`RealSide`]: the simulator's SQLite seam ([`SqliteSide`]) over real SQLite, so a drawn
//! history runs the engine against this crate's [`crate::Sqlite`] on a real database through
//! [`FaultVfs`], instead of against the simulator's rule model.
//!
//! Mechanism. The database lives in a directory the caller owns, through a fresh fault VFS with
//! durability tracked (a power loss keeps what the [`CrashLaw`] keeps). The application is one
//! connection at `synchronous = NORMAL`; other processes' readers and checkpoints are
//! connections of their own; the engine's doors (module `doors`) wrap its real handle, count
//! each call for the application's operation inside a tick (module `in_tick`), and take
//! injected source errors. SQLite's random numbers are pinned to the run's seed, so a run
//! replays.
//!
//! Commits are SQL. The world draws page-level commits of arbitrary bytes, which real SQLite
//! cannot write while it keeps the b-tree the engine's own table lives in. So each drawn page
//! is a row (`p(id, v)`, the page number as its id), a commit upserts those rows, a shrink
//! deletes rows past the size under `auto_vacuum = FULL`, and a spill writes rows big enough to
//! outgrow a small page cache in a transaction left open. SQLite then decides which pages
//! change, and [`SqliteSide::take_written`] reports what did: the database read back through
//! SQLite (`sqlite3_serialize` on a fresh connection) and compared page by page with what the
//! world last learned. Nothing the world learns is read by the engine's code: the wal-index
//! comes from the VFS's own mapping and the WAL header from the file, each parsed here (module
//! `index`). Each read-back is a short reader, as any application reader is.
//!
//! What stays with the model, and why. A drawn action this backend cannot make answers `false`
//! or `None`, and a drawn history skips it as a step with nothing to act on:
//! - our read transaction ending or beginning outside a tick (`our_reader_begin`,
//!   `our_reader_end`): the engine's handle holds its read transaction for its whole life and
//!   offers no way to drop it from outside;
//! - a wal-index read that finds its two header copies unequal, an end inside a transaction, a
//!   frame overwritten between the engine's scan and its read, and an empty database reported
//!   once: each is a moment inside one of SQLite's own calls that no connection can make happen
//!   on demand, deterministically;
//! - a driver start that writes nothing: the real driver's setup is what it is;
//! - deleting the WAL: SQLite deletes it only on the last close, and the engine's handle is open
//!   for as long as its process lives;
//! - the WAL damage draws (a torn header, a cut, a flipped bit, rewrites and regrowths), an
//!   unlinked WAL and a flipped database page: not yet made on real files. The database cannot be
//!   written here outside SQLite at all, since any descriptor closed on it drops the
//!   application's locks.
//!
//! The application's automatic checkpoints are not counted ([`SqliteSide::app_autocheckpoints`]
//! is zero), the database file's bytes are not read ([`SqliteSide::db_file`] is empty, for the
//! reason above), a power loss's tear counts are zero (the block law still tears the database
//! file a partial backfill left unsynced, as the fault VFS's own test shows; it is not counted
//! here), and the commit images of a regrown WAL are never asked for, since no regrowth is
//! drawn here. No recovery disagreement is ever reported: SQLite's own recovery is the only one
//! here, so there is nothing to disagree with.

mod doors;
mod in_tick;
mod index;
mod state;

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::num::{NonZeroI32, NonZeroU32};
use std::path::Path;
use std::sync::{Arc, Mutex};

use lsr_core::{SourceError, SourceOp};
use lsr_ltx::{PageNo, PageSize};
use lsr_sim::crash_law::CrashLaw;
use lsr_sim::side::{SideCheckpointer, SidePages, SideSource, SqliteSide, Written};
use lsr_sim::sqlite::{
    AppCommit, AutoTally, CheckpointNumbers, CrashKind, CreatedAt, InTickAt, InTickCommit,
    InTickOp, InTickRan, InTickSite, OtherProcessMode, ReaderId, StagingLen,
};
use lsr_wal::Salts;
use rusqlite::{Connection, OpenFlags};

use self::doors::{Door, RealCheckpointer, RealPages, RealSource};
use self::in_tick::{Armed, Tick};
use self::index::{Index, header_salts};
use self::state::{Real, TABLE, lock};
use super::harness::{RigError, fresh_vfs, open_app};
use super::vfs::{Effect, Fault, FileKind, Op, seed_prng};

/// The simulator's SQLite over real SQLite (module doc).
pub struct RealSide {
    real: Arc<Mutex<Real>>,
}

impl fmt::Debug for RealSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealSide")
            .field("db", &lock(&self.real).db)
            .finish()
    }
}

/// Creates the database at `db` with `pages` rows of zeros and returns the application's
/// connection to it.
fn create(
    db: &Path,
    vfs: super::vfs::FaultVfs,
    size: PageSize,
    pages: u32,
) -> Result<Connection, RigError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let conn = Connection::open_with_flags_and_vfs(db, flags, vfs.name())?;
    conn.pragma_update(
        None,
        "page_size",
        u32::try_from(size.bytes()).unwrap_or(4096),
    )?;
    conn.pragma_update(None, "auto_vacuum", "FULL")?;
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))?;
    conn.execute_batch(TABLE)?;
    let zeros = vec![0u8; size.bytes().checked_div(4).unwrap_or(1)];
    for id in 2..=pages {
        conn.execute(
            "INSERT INTO p(id, v) VALUES (?1, ?2)",
            rusqlite::params![id, zeros],
        )?;
    }
    drop(conn);
    let app = open_app(db, &vfs, "NORMAL")?;
    app.pragma_update(None, "cache_size", 10)?;
    Ok(app)
}

/// An application connection as the backend's application opens it: replication's setup at
/// `synchronous = NORMAL`, with a page cache small enough for a spill to reach the WAL.
fn open_app_conn(db: &Path, vfs: super::vfs::FaultVfs) -> Option<Connection> {
    let app = open_app(db, &vfs, "NORMAL").ok()?;
    app.pragma_update(None, "cache_size", 10).ok()?;
    Some(app)
}

impl RealSide {
    /// A database of about `db_pages` pages of `page_size` at `dir/app.db`, which must not
    /// exist yet, with SQLite's random numbers pinned to `seed`.
    ///
    /// # Errors
    /// [`RigError`] when the VFS, the directory or the database cannot be set up.
    pub fn create(
        dir: &Path,
        page_size: PageSize,
        db_pages: u32,
        seed: u64,
    ) -> Result<Self, RigError> {
        let pinned = i32::from_le_bytes(
            seed.to_le_bytes()
                .get(..4)
                .and_then(|b| b.try_into().ok())
                .unwrap_or([1, 0, 0, 0]),
        );
        seed_prng(NonZeroI32::new(pinned).unwrap_or(NonZeroI32::MIN));
        let vfs = fresh_vfs()?;
        vfs.set_window(true);
        let dir = std::fs::canonicalize(dir).map_err(|source| RigError::Io {
            path: dir.to_owned(),
            source,
        })?;
        let db = dir.join("app.db");
        let app = create(&db, vfs, page_size, db_pages)?;
        vfs.track_durability();
        let mut real = Real {
            db,
            vfs,
            page_size,
            app: Some(app),
            other_writer: None,
            app_tx: false,
            readers: BTreeMap::new(),
            next_reader: 0,
            engine: None,
            seen: Vec::new(),
            owed: VecDeque::new(),
            injected: Vec::new(),
            taken: Vec::new(),
            lie_ours: false,
            tick: Tick::default(),
        };
        real.seen = real.image();
        #[expect(
            clippy::arc_with_non_send_sync,
            reason = "the simulator runs on one thread; the Arc shares the backend with its doors"
        )]
        let real = Arc::new(Mutex::new(real));
        Ok(Self { real })
    }

    fn with<T>(&self, f: impl FnOnce(&mut Real) -> T) -> T {
        f(&mut lock(&self.real))
    }

    fn door(&self) -> Door {
        Door(Arc::clone(&self.real))
    }

    fn index(&self) -> Index {
        self.with(|real| Index::read(real))
    }
}

impl Real {
    /// A crash: the engine's process, or with a power loss every connection, goes away.
    fn crash(&mut self, kind: CrashKind) {
        // What the application did inside the crashed tick stays for the world to take up,
        // as the model keeps it: a reader it began outlives our process.
        self.engine = None;
        if kind == CrashKind::Process {
            return;
        }
        self.vfs.cut_power();
        self.readers.clear();
        self.other_writer = None;
        self.app = None;
        self.app_tx = false;
        // A loss whose undo failed still leaves files to open; the model check reports what differs.
        let _restored = self.vfs.restore_power().is_ok();
        self.app = open_app_conn(&self.db, self.vfs);
        self.owed.clear();
        self.seen = self.image();
    }

    /// VACUUM into pages of `new` with every other connection closed; false when the size is
    /// unchanged, a connection holds a transaction, or SQLite refuses.
    fn vacuum(&mut self, new: PageSize) -> bool {
        if new == self.page_size || self.app_tx || !self.readers.is_empty() {
            return false;
        }
        self.engine = None;
        self.other_writer = None;
        let Some(app) = self.app.as_ref() else {
            return false;
        };
        let done = app
            .execute_batch(&format!(
                "PRAGMA journal_mode = DELETE; PRAGMA page_size = {}; VACUUM;",
                new.bytes()
            ))
            .is_ok();
        let wal = app
            .query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .is_ok();
        if done && wal {
            self.page_size = new;
            self.owed.clear();
            self.seen = self.image();
        }
        done && wal
    }
}

impl SqliteSide for RealSide {
    fn source(&self) -> SideSource {
        SideSource::new(RealSource(self.door()))
    }
    fn pages(&self) -> SidePages {
        SidePages::new(RealPages(self.door()))
    }
    fn checkpointer(&self) -> SideCheckpointer {
        SideCheckpointer::new(RealCheckpointer(self.door()))
    }
    fn take_written(&self) -> Option<Written> {
        Some(self.with(Real::take_written))
    }
    fn start_driver(&self) {
        self.with(|real| {
            real.engine = None;
            real.engine();
        });
    }
    // The real backend runs no write gate: a run with one is the model's alone.
    fn hold_writes(&self) -> bool {
        false
    }
    fn release_writes(&self) {}
    fn writes_held(&self) -> bool {
        false
    }
    fn writer_busy(&self) -> bool {
        false
    }
    fn our_reader_begin(&self) -> bool {
        false
    }
    fn our_reader_end(&self) -> bool {
        false
    }
    fn commit(&self, pages: &[(PageNo, Vec<u8>)], db_size: u32) -> Option<AppCommit> {
        self.with(|real| real.commit(pages, db_size))
    }
    fn other_writer_commit(&self, pages: &[(PageNo, Vec<u8>)], _db_size: u32) -> Option<AppCommit> {
        self.with(|real| real.other_writer_commit(pages))
    }
    fn arm_commit_in_tick(
        &self,
        pages: Vec<(PageNo, Vec<u8>)>,
        db_size: u32,
        site: InTickSite,
        created_at: CreatedAt,
    ) {
        self.with(|real| {
            real.tick.commit = Some(Armed {
                pages,
                db_size,
                site,
                created_at,
            });
        });
    }
    fn take_commit_in_tick(&self) -> Option<InTickCommit> {
        self.with(|real| {
            real.commit_unseen();
            real.tick.commit = None;
            real.tick.done.take()
        })
    }
    fn lie_on_our_sync(&self) {
        self.with(|real| real.lie_ours = true);
    }
    fn honest_on_our_sync(&self) {
        self.with(|real| real.lie_ours = false);
    }
    fn set_app_autocheckpoint(&self, frames: Option<u32>) {
        self.with(|real| {
            if let Some(app) = real.app.as_ref() {
                let _set = app
                    .pragma_update(None, "wal_autocheckpoint", frames.unwrap_or(0))
                    .is_ok();
            }
        });
    }
    fn take_engine_commits(&self) -> Vec<AppCommit> {
        Vec::new()
    }
    fn app_autocheckpoints(&self) -> AutoTally {
        AutoTally::default()
    }
    fn spill(&self, pages: &[(PageNo, Vec<u8>)]) -> bool {
        self.with(|real| real.spill(pages))
    }
    fn rollback(&self) -> bool {
        self.with(Real::rollback)
    }
    fn shrink(&self, db_size: u32) -> Option<AppCommit> {
        self.with(|real| real.shrink(db_size))
    }
    fn rewrite_page_size(&self, new: PageSize) -> bool {
        self.with(|real| real.vacuum(new))
    }
    fn other_process_reader_begin(&self) -> ReaderId {
        self.with(Real::reader_begin)
    }
    fn other_process_reader_end(&self, id: ReaderId) -> bool {
        self.with(|real| real.reader_end(id))
    }
    fn other_process_checkpoint(&self, mode: OtherProcessMode) -> Option<CheckpointNumbers> {
        self.with(|real| real.other_checkpoint(mode))
    }
    fn delete_wal(&self) -> bool {
        false
    }
    fn tear_wal_header(&self) -> bool {
        false
    }
    fn truncate_wal_at(&self, _len: u64) -> bool {
        false
    }
    fn flip_bit(&self, _bit: u64) -> bool {
        false
    }
    fn rewrite_same_length(&self, _seed: u64) -> bool {
        false
    }
    fn regrow_same_salts(&self, _seed: u64) -> bool {
        false
    }
    fn regrow_without_commit_marks(&self, _seed: u64) -> bool {
        false
    }
    fn shm_mid_update(&self) -> bool {
        false
    }
    fn shm_end_inside_tx(&self) -> bool {
        false
    }
    fn overwrite_frame_between_scan_and_read(&self, _frame: u32) -> bool {
        false
    }
    fn unlink_wal(&self) -> bool {
        false
    }
    fn empty_database_once(&self) {}
    fn flip_db_page(&self, _pgno: u32) -> bool {
        false
    }
    fn heal_injections(&self) {
        self.with(|real| real.injected.clear());
    }
    fn fail_next_truncate(&self) {
        self.with(|real| {
            real.vfs.arm(Fault {
                kind: FileKind::Wal,
                op: Op::Truncate,
                nth: NonZeroU32::MIN,
                effect: Effect::Error(crate::ffi::SQLITE_IOERR_TRUNCATE),
            });
        });
    }
    fn driver_writes_nothing_once(&self) {}
    fn inject(&self, op: SourceOp, err: SourceError) {
        self.with(|real| real.injected.push((op, err)));
    }
    fn taken_injections(&self) -> Vec<(SourceOp, SourceError)> {
        self.with(|real| real.taken.clone())
    }
    fn tear_on_power_loss(&self, seed: u64) {
        self.with(|real| real.vfs.set_crash_law(CrashLaw::drawn(seed)));
    }
    fn crash_by_law(&self, law: CrashLaw) {
        self.with(|real| real.vfs.set_crash_law(law));
    }
    fn block_losses(&self) -> (u32, u32) {
        self.with(|real| real.vfs.wal_block_losses())
    }
    fn db_torn_losses(&self) -> u32 {
        0
    }
    fn commit_images(&self) -> Vec<(u32, Vec<u8>)> {
        Vec::new()
    }
    fn recovery_disagreements(&self) -> Vec<String> {
        Vec::new()
    }
    fn torn_losses(&self) -> (u32, u32) {
        (0, 0)
    }
    fn crash(&self, kind: CrashKind) {
        self.with(|real| real.crash(kind));
    }
    fn image(&self) -> Vec<u8> {
        self.with(|real| real.image())
    }
    fn db_file(&self) -> Vec<u8> {
        Vec::new()
    }
    fn wal_file(&self) -> Option<Vec<u8>> {
        self.with(|real| real.wal_file())
    }
    fn page_size(&self) -> PageSize {
        self.with(|real| real.page_size)
    }
    #[expect(
        clippy::expect_used,
        reason = "test support: a WAL the VFS never saw is a harness bug, not an answer"
    )]
    fn wal_range_durable(&self, salts: Salts, start: u64, end: u64) -> bool {
        // As the model answers: frames of another generation are no longer SQLite's, and a
        // disk lying about our sync is the scenario's point.
        self.with(|real| {
            real.lie_ours
                || header_salts(real.wal_file().as_deref()) != Some((salts.salt1, salts.salt2))
                || real
                    .vfs
                    .durable(&real.wal_path(), start, end)
                    .expect("the VFS has seen the WAL")
        })
    }
    fn wal_synced(&self) -> bool {
        self.with(|real| real.wal_synced())
    }
    fn mx_frame(&self) -> u32 {
        self.index().mx_frame
    }
    fn backfilled(&self) -> u32 {
        self.index().backfilled
    }
    fn wal_header_salts(&self) -> Option<(u32, u32)> {
        self.with(|real| header_salts(real.wal_file().as_deref()))
    }
    fn index_reset_ahead(&self) -> bool {
        let index = self.index();
        index.mx_frame == 0
            && self.wal_header_salts() != Some((index.salts.salt1, index.salts.salt2))
    }
    fn our_reader_pins_wal(&self) -> bool {
        self.with(|real| real.engine.is_some())
    }
    fn salts(&self) -> Salts {
        self.index().salts
    }
    fn db_size(&self) -> u32 {
        let index = self.index();
        if index.db_size != 0 {
            return index.db_size;
        }
        self.with(|real| {
            let pages = real
                .seen
                .len()
                .checked_div(real.page_size.bytes())
                .unwrap_or(0);
            u32::try_from(pages).unwrap_or(u32::MAX)
        })
    }
    fn arm_app_in_tick(&self, when: InTickAt, op: InTickOp) {
        self.with(|real| real.tick.op = Some((when, op)));
    }
    fn app_staging_len(&self, len: StagingLen) {
        self.with(|real| real.tick.staging_len = Some(len));
    }
    fn app_tick_begins(&self) {
        self.with(|real| {
            real.tick.calls = 0;
            real.tick.ran = None;
        });
    }
    fn app_tick_ends(&self) -> Option<InTickRan> {
        self.with(|real| {
            real.tick.op = None;
            real.tick.ran
        })
    }
    fn app_calls(&self) -> u32 {
        self.with(|real| real.tick.calls)
    }
    fn take_app_in_tick(&self) -> Option<InTickRan> {
        self.with(|real| {
            real.tick.op = None;
            real.tick.ran.take()
        })
    }
}

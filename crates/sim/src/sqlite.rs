//! A rule-based model of SQLite's WAL, its wal-index and its locks.
//!
//! The scripted WAL fakes in `lsr-core` have a test say what a checkpoint does. [`SimSqlite`]
//! instead follows SQLite's rules, so another process's checkpoint, a writer's restart
//! and a reader's mark interact as they do in SQLite. The rules were read from the
//! bundled amalgamation (rusqlite 0.43, SQLite 3.54.1: `walTryBeginRead`,
//! `walCheckpoint`, `sqlite3WalCheckpoint`, `walRestartLog`, `walFrames`) and are
//! checked against it by `tests/sim/sqlite_rules.rs`:
//!
//! - **Backfill limit:** a checkpoint copies each page's newest frame when that frame is at
//!   or below the smallest mark above 0 held by any reader (a page rewritten above
//!   it is not copied at all), and nothing while a reader holds read mark 0 with
//!   `nBackfill < mxFrame`. RESTART and TRUNCATE are busy unless everything is
//!   backfilled and no reader holds a mark above 0; they need the write lock and
//!   degrade to PASSIVE, busy, when another connection holds it.
//! - **Restart:** a writer starting a transaction on read mark 0 with
//!   `nBackfill > 0` and no reader on a mark above 0 restarts the WAL: salt1 + 1,
//!   salt2 new, frames from 1. A TRUNCATE that finishes restarts the same way and
//!   empties the file; a RESTART that finishes changes nothing on disk, and the
//!   next writer restarts. The header's checkpoint sequence is the *writing
//!   connection's* checkpoint count (`Wal.nCkpt`: its restarts, plus the header's
//!   value when it ran recovery), and a connection whose count is 0 writes both
//!   salts random.
//! - **Mark choice:** a read transaction takes read mark 0 when
//!   `nBackfill == mxFrame`, else mark `mxFrame`. A writer that held read mark 0
//!   takes mark `mxFrame` (after any restart) before its first frame.
//!
//! A reader takes each page's newest frame at or below `mxFrame`, as SQLite's
//! `walFindFrame` does, but ignores SQLite's floor (`minFrame`, `nBackfill + 1` when the
//! read began). The two differ only for a page whose newest frame is above an old
//! commit's size, so no checkpoint copied it, and that a later growth leaves unwritten.
//! SQLite never grows that way: a growing commit writes every page it adds, spilled and
//! freed ones included (`a_growing_commit_writes_every_page_it_adds`), so callers that
//! grow the database must write the added pages too.
//!
//! - **Automatic checkpoint:** with a threshold set
//!   ([`SimSqlite::set_app_autocheckpoint`]), every app commit that leaves `mxFrame` at or
//!   past it runs a PASSIVE on the app's connection under the backfill limit (`sqlite3WalDefaultHook`).
//!   SQLite reports none of them to the app, so the model counts them
//!   ([`SimSqlite::app_autocheckpoints`]).
//!
//! The connections are lsr-sqlite's: the app (commits, spills, another process's
//! checkpoints), our read connection (which also runs our checkpoints, between ending and
//! retaking its read transaction), our writer connection (the barrier and
//! `bump_seq`, one at a time), and other-process readers. The app, our reader and our writer
//! each carry a checkpoint count, so the first frame after our own TRUNCATE, written by our
//! writer whose count is 0, gets random salts, as in SQLite.
//!
//! Durability follows `synchronous=NORMAL`: a commit is not fsynced; the WAL is
//! fsynced when a header is written (frame 1 of a generation) and before a
//! checkpoint copies frames. A checkpoint that backfills to `mxFrame` truncates and
//! fsyncs the database file before it returns; a partial one (a reader holds back the
//! safe frame) writes database pages and advances `nBackfill` without syncing them
//! (`walCheckpoint`), so a power loss can tear the database file, under the same law as
//! the WAL, until the next full backfill. Our
//! own `WalSource::sync` (the engine's barrier before staging) makes the whole
//! WAL durable too. A power crash keeps the WAL as of its last fsync, or a drawn prefix of the
//! later writes ([`SimSqlite::tear_on_power_loss`]), or the block law's draw
//! over them ([`SimSqlite::crash_by_law`], [`crate::crash_law`]), and then
//! SQLite's recovery runs. The recovery is real SQLite's ([`crate::recovery`]): the
//! database and WAL bytes go to a temporary directory and SQLite recovers them;
//! the engine's WAL parser, run on the same bytes, and the rules' image are
//! checked against it.
//!
//! Every method is total: an operation that is impossible in the current state
//! returns `false` or `None` and changes nothing. Nothing reads a clock; the
//! salts SQLite would draw at random come from a splitmix64 stream seeded in
//! [`SimSqlite::new`].

use std::collections::BTreeMap;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::mem;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_core::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, ReadLockHold,
    SourceError, SourceOp, WalFileLen, WalSource,
};
use lsr_ltx::{DbPages, PageNo, PageSize, Timestamp};
use lsr_wal::testing::{WalBuilder, page};

use crate::crash_law::{self, BlockSize, CrashLaw, Rng};
use crate::recovery;
use lsr_wal::{
    ByteOrder, CommittedEnd, FRAME_HEADER_LEN, HEADER_LEN, Salts, ScanLimit, TxOutcome,
    WalChecksum, WalHeader, WalOffset, WalReader,
};

mod in_tick;
#[cfg(test)]
mod tests;

pub use in_tick::{InTickAt, InTickDone, InTickOp, InTickRan, StagingLen};

const MAGIC_LITTLE: u32 = 0x377f_0682;
const FORMAT_VERSION: u32 = 3_007_000;
const ORDER: ByteOrder = ByteOrder::Little;
/// Bytes of a frame header that the running checksum covers.
const CHECKSUMMED_FRAME_HEADER_LEN: usize = 8;

/// Another process's read transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReaderId(u32);

impl ReaderId {
    /// The reader numbered `n`: a backend other than the model numbers its own readers.
    #[must_use]
    pub const fn new(n: u32) -> Self {
        Self(n)
    }
}

/// A checkpoint mode as another process can ask for it (core's
/// [`CheckpointMode`] has no RESTART).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OtherProcessMode {
    /// `PRAGMA wal_checkpoint(PASSIVE)`.
    Passive,
    /// `PRAGMA wal_checkpoint(RESTART)`.
    Restart,
    /// `PRAGMA wal_checkpoint(TRUNCATE)`.
    Truncate,
    /// `PRAGMA wal_checkpoint(FULL)`: copies everything, waiting for readers, and leaves the WAL.
    Full,
}

impl From<CheckpointMode> for OtherProcessMode {
    fn from(mode: CheckpointMode) -> Self {
        match mode {
            CheckpointMode::Passive => Self::Passive,
            CheckpointMode::Truncate => Self::Truncate,
            CheckpointMode::Full => Self::Full,
        }
    }
}

/// The three numbers `PRAGMA wal_checkpoint` returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointNumbers {
    /// The checkpoint could not do all its mode asks for.
    pub busy: bool,
    /// `mxFrame` afterwards.
    pub log: u32,
    /// `nBackfill` afterwards.
    pub checkpointed: u32,
}

/// The app's automatic checkpoints so far.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AutoTally {
    /// Checkpoints run after a commit at or past the threshold.
    pub attempts: u64,
    /// Those that raised `nBackfill`.
    pub backfilled: u64,
}

/// What an app commit wrote: the generation and the committed end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppCommit {
    /// The salts of the generation the commit is in.
    pub salts: Salts,
    /// The WAL offset just past the commit frame.
    pub end: WalOffset,
}

/// Which crash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashKind {
    /// Our process dies: our read transaction, barrier, gate and writer
    /// connections vanish; the files, the wal-index and the app stay.
    Process,
    /// The machine loses power: every connection vanishes, the WAL is what was
    /// last fsynced, and the app's next open runs SQLite's recovery.
    Power,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Conn {
    App,
    Ours,
    Writer,
    OtherProcess(ReaderId),
    /// Another process's connection that runs one checkpoint and closes (the sqlite3 CLI, a
    /// cron job). Its checkpoint count dies with it, so the app's next header is written
    /// under the app's own count.
    Cli,
    /// Another application connection that stays open and commits (another process's writer):
    /// it keeps a checkpoint count of its own, so after a restart whose writer rolled back it
    /// writes the header again with the same salts and another seed.
    OtherWriter,
    /// The replicator's write gate (`lsr_sqlite::WriteGate`): a connection of our process
    /// that holds a write transaction and never writes, so no other connection can.
    Gate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mark {
    Zero,
    Slot(u32),
}

/// The wal-index and the writer's private frame count.
#[derive(Clone, Debug)]
struct Index {
    salts: Salts,
    mx_frame: u32,
    backfilled: u32,
    db_size: u32,
    // Frames in the file in this generation, spilled ones included.
    written: u32,
    // The checksum after `written` frames, and after `mx_frame`.
    running: WalChecksum,
    committed: WalChecksum,
    // The page number of each written frame (the wal-index hash).
    pgnos: Vec<u32>,
    // `mx_frame` after each commit of this generation.
    tx_ends: Vec<u32>,
}

impl Index {
    fn empty(salts: Salts, db_size: u32) -> Self {
        Self {
            salts,
            mx_frame: 0,
            backfilled: 0,
            db_size,
            written: 0,
            running: WalChecksum::ZERO,
            committed: WalChecksum::ZERO,
            pgnos: Vec::new(),
            tx_ends: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Overwrite {
    frame: u32,
    reads: u32,
    // The page is inverted until the next read puts it back.
    flipped: bool,
}

#[derive(Debug, Default)]
struct Faults {
    injections: Vec<(SourceOp, SourceError)>,
    // Injected failures a call took, in order, for the error domain's coverage marks.
    taken: Vec<(SourceOp, SourceError)>,
    shm_busy: bool,
    // The forced read and how many reads still see it: the loop's own check
    // of the WAL comes before the step that scans it.
    shm_forced: Option<(CommittedRead, u8)>,
    overwrite: Option<Overwrite>,
    empty_db: bool,
    // The next TRUNCATE that finishes fails to truncate the file.
    truncate_fails: bool,
    // The next driver start writes no WAL header: the real driver's open,
    // whose only write is `CREATE TABLE IF NOT EXISTS`.
    driver_writes_nothing: bool,
}

impl Faults {
    fn take(&mut self, op: SourceOp) -> Result<(), SourceError> {
        match self.injections.iter().position(|(queued, _)| *queued == op) {
            Some(at) => {
                let taken = self.injections.remove(at);
                self.taken.push(taken);
                Err(taken.1)
            }
            None => Ok(()),
        }
    }
}

/// What a failed sync of the WAL left unwritable ([`State::lost_writeback`]).
#[derive(Debug)]
struct LostWriteback {
    salts: Salts,
    durable: Vec<u8>,
    wal: Vec<u8>,
}

impl LostWriteback {
    /// Puts back, in `durable`, every byte the failed sync covered and did not write.
    fn keep_lost(&self, durable: &mut [u8]) {
        let dirty = self
            .wal
            .iter()
            .enumerate()
            .filter(|(at, byte)| self.durable.get(*at) != Some(*byte));
        for (at, _) in dirty {
            if let Some(byte) = durable.get_mut(at) {
                *byte = self.durable.get(at).copied().unwrap_or(0);
            }
        }
    }
}

#[derive(Debug)]
struct State {
    page_size: PageSize,
    db: Vec<u8>,
    // The database file as of its last fsync: a partial backfill's pages are not in it.
    db_durable: Vec<u8>,
    wal: Vec<u8>,
    // The WAL as of its last fsync.
    durable: Vec<u8>,
    // Our sync of the WAL returns success and makes nothing durable: a disk that lies.
    our_sync_lies: bool,
    // Our sync of the WAL failed: the durable bytes and the WAL as they were then, in that
    // generation. The kernel called the pages the failure covered clean, so no later sync
    // writes them (Linux's "fsyncgate"): until the generation ends they stay as they were.
    lost_writeback: Option<LostWriteback>,
    // The app's own connection has not yet fsynced the WAL since our sync failed. Linux reports
    // a writeback error to every file description open when it happened (errseq, since 4.13), so
    // the app's first WAL sync after ours fails too, and that checkpoint backfills nothing. A
    // process that opens the WAL after our sync saw the error (the CLI) is told nothing.
    app_unsynced_error: bool,
    // Every WAL byte range a failed sync of ours lost, by generation, for the life of the
    // simulated machine: a level-0 file staged from one was never durable, whatever the WAL
    // holds by the time anyone looks. v1 stages it all the same once a sync succeeds (D173), so
    // [`SimSqlite::wal_range_durable`] does not judge those ranges.
    lost_ranges: Vec<(Salts, u64, u64)>,
    present: bool,
    unlinked: bool,
    index: Index,
    readers: BTreeMap<Conn, Mark>,
    writer: Option<Conn>,
    app_checkpoints: u32,
    other_writer_checkpoints: u32,
    ours_checkpoints: u32,
    writer_checkpoints: u32,
    rng: u64,
    // When set, the values SQLite would draw at random are these instead.
    forced: Option<Salts>,
    epoch: ReadLockHold,
    next_reader: u32,
    next_token: u64,
    barrier: Option<u64>,
    faults: Faults,
    app_autocheckpoint: Option<u32>,
    app_auto: AutoTally,
    // The app's commit armed for the next barrier, and the one it made there.
    in_tick: Option<InTick>,
    in_tick_done: Option<InTickCommit>,
    // The engine's own commits (its sequence bumps) since the world last took them up, each
    // with the generation and end it made: a tick can make two with a WAL restart between, and
    // the model sees only the WAL as the tick left it.
    engine_commits: Vec<AppCommit>,
    // With a seed, a power loss keeps a drawn prefix of the unsynced WAL
    // writes instead of none.
    torn: Option<u64>,
    // Power losses that kept unsynced WAL bytes, and those that kept a frame
    // cut inside.
    torn_kept: (u32, u32),
    // With a law, a power loss keeps the block law's draw over the unsynced
    // WAL writes instead (it wins over `torn`).
    blocks: Option<(BlockSize, Rng)>,
    // What the WAL held before its last truncate: the stale bytes a block may
    // show past the data written.
    ghost: Vec<u8>,
    // Power losses under the block law that kept some unsynced block, and those
    // that lost a block before one they kept (a hole).
    block_kept: (u32, u32),
    // Power losses under the block law that found database blocks a partial backfill had
    // written and not synced.
    db_torn: u32,
    // Where the engine's WAL parser and SQLite's own recovery disagreed.
    recovery_disagreements: Vec<String>,
    app: in_tick::AppInTick,
}

/// Where the harness's logs stood: its own created-at times, opaque to the rules.
pub type CreatedAt = Box<dyn Fn() -> (usize, usize) + Send>;

/// Where inside a tick the application's armed commit lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InTickSite {
    /// Right before the engine reads the committed end: after the tick read its clock, so a
    /// file stamped with that reading would claim a time before the commit it carries.
    BeforeCommittedRead,
    /// After the tick's sync read the committed end, right before a checkpoint takes its write
    /// barrier: the commit the checkpoint's seal must stage.
    BeforeBarrier,
    /// Right before the call an [`InTickOp::Commit`] names (`in_tick`).
    AtCall,
}

/// A commit armed by [`SimSqlite::arm_commit_in_tick`].
struct InTick {
    pages: Vec<(PageNo, Vec<u8>)>,
    db_size: u32,
    site: InTickSite,
    created_at: CreatedAt,
}

impl std::fmt::Debug for InTick {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InTick")
            .field("pages", &self.pages.len())
            .field("db_size", &self.db_size)
            .field("site", &self.site)
            .finish_non_exhaustive()
    }
}

/// The app's commit made inside a tick, right before a checkpoint's barrier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InTickCommit {
    /// The pages it wrote.
    pub pages: Vec<(PageNo, Vec<u8>)>,
    /// The database's size in pages after it.
    pub db_size: u32,
    /// Where it sits in the WAL.
    pub app: AppCommit,
    /// The armed created-at time's value at the moment of the commit; for a commit armed
    /// at a call (`in_tick`), the second value as of the engine's next committed read.
    pub created_at: (usize, usize),
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn as_u64(pos: usize) -> u64 {
    u64::try_from(pos).unwrap_or(u64::MAX)
}

fn byte_pos(offset: u64) -> usize {
    usize::try_from(offset).unwrap_or(usize::MAX)
}

fn write_at(bytes: &mut Vec<u8>, at: usize, data: &[u8]) {
    let end = at.saturating_add(data.len());
    if bytes.len() < end {
        bytes.resize(end, 0);
    }
    if let Some(dst) = bytes.get_mut(at..end) {
        dst.copy_from_slice(data);
    }
}

fn splitmix(state: &mut u64) -> u32 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z.wrapping_shr(31);
    u32::try_from(z.wrapping_shr(32)).unwrap_or_default()
}

impl State {
    /// The app commits `pages` leaving `db_size` pages, then runs its automatic
    /// checkpoint.
    fn app_commit(&mut self, pages: &[(PageNo, Vec<u8>)], db_size: u32) -> Option<AppCommit> {
        self.commit_by(Conn::App, pages, db_size)
    }

    /// A commit by `by`; only the app's connection runs automatic checkpoints.
    fn commit_by(
        &mut self,
        by: Conn,
        pages: &[(PageNo, Vec<u8>)],
        db_size: u32,
    ) -> Option<AppCommit> {
        if db_size == 0 || !self.write(by, pages, Some(db_size)) {
            return None;
        }
        let commit = AppCommit {
            salts: self.index.salts,
            end: WalOffset::from_frame_index(self.index.mx_frame, self.page_size),
        };
        if by == Conn::App {
            self.autocheckpoint();
        }
        Some(commit)
    }

    /// Makes the armed in-tick commit if it is armed for `site`, and keeps what it made.
    fn commit_in_tick(&mut self, site: InTickSite) {
        if self.in_tick.as_ref().is_none_or(|armed| armed.site != site) {
            return;
        }
        let Some(armed) = self.in_tick.take() else {
            return;
        };
        if let Some(app) = self.app_commit(&armed.pages, armed.db_size) {
            self.in_tick_done = Some(InTickCommit {
                pages: armed.pages,
                db_size: armed.db_size,
                app,
                created_at: (armed.created_at)(),
            });
        }
    }

    /// One of the two salts SQLite draws from `sqlite3_randomness`: the forced value when a
    /// test has named one, else the next of the seeded stream.
    fn random_salt(&mut self, second: bool) -> u32 {
        match self.forced {
            Some(salts) if second => salts.salt2,
            Some(salts) => salts.salt1,
            None => splitmix(&mut self.rng),
        }
    }

    fn frame_at(&self, index: u32) -> usize {
        byte_pos(WalOffset::from_frame_index(index, self.page_size).get())
    }

    fn page_at(&self, pgno: u32) -> usize {
        usize::try_from(pgno.saturating_sub(1))
            .unwrap_or(usize::MAX)
            .saturating_mul(self.page_size.bytes())
    }

    fn db_pages(&self) -> u32 {
        let pages = self
            .db
            .len()
            .checked_div(self.page_size.bytes())
            .unwrap_or_default();
        u32::try_from(pages).unwrap_or(u32::MAX)
    }

    /// The mark a new read transaction takes: read mark 0 when the WAL is fully backfilled, else
    /// the last frame's.
    fn mark_now(&self) -> Mark {
        if self.index.backfilled == self.index.mx_frame {
            Mark::Zero
        } else {
            Mark::Slot(self.index.mx_frame)
        }
    }

    fn begin(&mut self, conn: Conn) -> bool {
        if self.readers.contains_key(&conn) {
            return false;
        }
        let mark = self.mark_now();
        self.readers.insert(conn, mark);
        true
    }

    fn end(&mut self, conn: Conn) -> bool {
        let held = self.readers.remove(&conn).is_some();
        if held && conn == Conn::Ours {
            self.epoch = self.epoch.next();
        }
        held
    }

    fn slot_held_by_other(&self, conn: Conn) -> bool {
        self.readers
            .iter()
            .any(|(held, mark)| *held != conn && matches!(mark, Mark::Slot(_)))
    }

    fn checkpoints(&mut self, conn: Conn) -> Option<&mut u32> {
        match conn {
            Conn::App => Some(&mut self.app_checkpoints),
            Conn::OtherWriter => Some(&mut self.other_writer_checkpoints),
            Conn::Ours => Some(&mut self.ours_checkpoints),
            Conn::Writer => Some(&mut self.writer_checkpoints),
            Conn::OtherProcess(_) | Conn::Cli | Conn::Gate => None,
        }
    }

    /// `walRestartHdr`, run by `by`.
    fn restart(&mut self, by: Conn) {
        if let Some(count) = self.checkpoints(by) {
            *count = count.wrapping_add(1);
        }
        let salt2 = self.random_salt(true);
        let salts = Salts {
            salt1: self.index.salts.salt1.wrapping_add(1),
            salt2,
        };
        self.index = Index::empty(salts, self.index.db_size);
    }

    /// The header of a new generation, written and fsynced by `by` with its frame 1.
    fn write_header(&mut self, by: Conn) {
        let seq = self.checkpoints(by).map_or(0, |count| *count);
        if seq == 0 {
            self.index.salts = Salts {
                salt1: self.random_salt(false),
                salt2: self.random_salt(true),
            };
        }
        let salts = self.index.salts;
        let mut head = Vec::with_capacity(HEADER_LEN);
        for word in [
            MAGIC_LITTLE,
            FORMAT_VERSION,
            self.page_size.get(),
            seq,
            salts.salt1,
            salts.salt2,
        ] {
            head.extend(word.to_be_bytes());
        }
        let checksum = WalChecksum::ZERO
            .extend(ORDER, &head)
            .unwrap_or(WalChecksum::ZERO);
        head.extend(checksum.s0().to_be_bytes());
        head.extend(checksum.s1().to_be_bytes());
        write_at(&mut self.wal, 0, &head);
        self.present = true;
        self.index.running = checksum;
        self.index.committed = checksum;
        self.durable.clone_from(&self.wal);
    }

    /// Writes one frame after `written` in the current generation.
    fn put_frame(&mut self, pgno: u32, commit: u32, data: &[u8]) {
        let mut page = data.to_vec();
        page.resize(self.page_size.bytes(), 0);
        let salts = self.index.salts;
        let mut head = Vec::with_capacity(FRAME_HEADER_LEN);
        for word in [pgno, commit, salts.salt1, salts.salt2] {
            head.extend(word.to_be_bytes());
        }
        let covered = head.get(..CHECKSUMMED_FRAME_HEADER_LEN).unwrap_or_default();
        let running = self.index.running;
        let next = running
            .extend(ORDER, covered)
            .and_then(|sum| sum.extend(ORDER, &page))
            .unwrap_or(running);
        head.extend(next.s0().to_be_bytes());
        head.extend(next.s1().to_be_bytes());
        head.extend(page);
        let at = self.frame_at(self.index.written);
        write_at(&mut self.wal, at, &head);
        self.index.running = next;
        self.index.written = self.index.written.saturating_add(1);
        self.index.pgnos.push(pgno);
    }

    /// The start of a write transaction by `by`: the restart rule, then the writer's mark choice.
    fn start_write(&mut self, by: Conn) {
        let mark = self
            .readers
            .get(&by)
            .copied()
            .unwrap_or_else(|| self.mark_now());
        let mark = match mark {
            Mark::Zero => {
                if self.index.backfilled > 0 && !self.slot_held_by_other(by) {
                    self.restart(by);
                }
                Mark::Slot(self.index.mx_frame)
            }
            Mark::Slot(at) => Mark::Slot(at),
        };
        self.readers.insert(by, mark);
        self.writer = Some(by);
    }

    /// Frames by `by`; `commit` is the database size of a committing write.
    fn write(&mut self, by: Conn, pages: &[(PageNo, Vec<u8>)], commit: Option<u32>) -> bool {
        if pages.is_empty() || self.writer.is_some_and(|holder| holder != by) {
            return false;
        }
        if self.writer.is_none() {
            self.start_write(by);
        }
        let last = pages.len().saturating_sub(1);
        for (at, (pgno, data)) in pages.iter().enumerate() {
            if self.index.written == 0 {
                self.write_header(by);
            }
            let field = match commit {
                Some(size) if at == last => size,
                Some(_) | None => 0,
            };
            self.put_frame(pgno.get(), field, data);
        }
        if let Some(size) = commit {
            self.index.mx_frame = self.index.written;
            self.index.committed = self.index.running;
            self.index.db_size = size;
            self.index.tx_ends.push(self.index.mx_frame);
            self.writer = None;
            self.readers.remove(&by);
            if by == Conn::Writer {
                self.engine_commits.push(AppCommit {
                    salts: self.index.salts,
                    end: WalOffset::from_frame_index(self.index.mx_frame, self.page_size),
                });
            }
        }
        true
    }

    fn rollback(&mut self, by: Conn) -> bool {
        if self.writer != Some(by) {
            return false;
        }
        let index = &mut self.index;
        index.written = index.mx_frame;
        index.running = index.committed;
        index.pgnos.truncate(byte_pos(u64::from(index.mx_frame)));
        self.writer = None;
        self.readers.remove(&by);
        true
    }

    fn frame_page(&self, index: u32) -> Vec<u8> {
        let at = self.frame_at(index).saturating_add(FRAME_HEADER_LEN);
        let mut data = self
            .wal
            .get(at..at.saturating_add(self.page_size.bytes()))
            .unwrap_or_default()
            .to_vec();
        data.resize(self.page_size.bytes(), 0);
        data
    }

    /// The newest frame index of each page among frames `(from, to]`.
    fn newest(&self, from: u32, to: u32) -> BTreeMap<u32, u32> {
        let mut newest = BTreeMap::new();
        for index in from..to {
            if let Some(pgno) = self.index.pgnos.get(byte_pos(u64::from(index))) {
                newest.insert(*pgno, index);
            }
        }
        newest
    }

    /// A sync of the WAL that succeeds: every byte durable, but those a failed sync of this
    /// generation lost.
    fn sync_wal(&mut self) {
        self.durable.clone_from(&self.wal);
        if let Some(lost) = self
            .lost_writeback
            .as_ref()
            .filter(|lost| lost.salts == self.index.salts)
        {
            lost.keep_lost(&mut self.durable);
        }
    }

    /// Copies into the database file the pages whose newest frame is in `(backfilled, upto]`.
    fn copy_frames(&mut self, upto: u32) {
        self.sync_wal();
        // SQLite walks every page's newest frame in the whole WAL and skips the ones past the
        // safe frame, so a page rewritten above it is not copied at all (its older version stays
        // out of the database file).
        for (pgno, index) in self.newest(0, self.index.mx_frame) {
            let frame = index.saturating_add(1);
            if frame <= self.index.backfilled
                || frame > upto
                || pgno == 0
                || pgno > self.index.db_size
            {
                continue;
            }
            let data = self.frame_page(index);
            let at = self.page_at(pgno);
            write_at(&mut self.db, at, &data);
        }
        // Only a backfill to `mxFrame` truncates and syncs the database file; a partial one
        // leaves its pages unsynced, and `nBackfill` advances either way.
        if upto == self.index.mx_frame {
            let len = self.page_at(self.index.db_size.saturating_add(1));
            self.db.resize(len, 0);
            self.db_durable.clone_from(&self.db);
        }
        self.index.backfilled = upto;
    }

    /// The backfill step of `walCheckpoint`, under the backfill limit.
    fn backfill(&mut self) {
        let index = &self.index;
        if index.backfilled >= index.mx_frame {
            return;
        }
        let safe = self
            .readers
            .values()
            .filter_map(|mark| match mark {
                Mark::Slot(at) => Some(*at),
                Mark::Zero => None,
            })
            .fold(index.mx_frame, u32::min);
        let zero_held = self.readers.values().any(|mark| *mark == Mark::Zero);
        if index.backfilled < safe && !zero_held {
            self.copy_frames(safe);
        }
    }

    /// `sqlite3WalCheckpoint` on connection `by`, which holds no read transaction.
    fn checkpoint(&mut self, by: Conn, mode: OtherProcessMode) -> CheckpointNumbers {
        // `walCheckpoint` syncs the WAL before it backfills; the error ends the checkpoint
        // there, and SQLite's autocheckpoint drops it.
        if by == Conn::App && mem::take(&mut self.app_unsynced_error) {
            return CheckpointNumbers {
                busy: true,
                log: self.index.mx_frame,
                checkpointed: self.index.backfilled,
            };
        }
        let degraded = mode != OtherProcessMode::Passive && self.writer.is_some_and(|w| w != by);
        let mut busy = degraded;
        self.backfill();
        if !degraded && mode != OtherProcessMode::Passive {
            // FULL waits only until every frame is copied; RESTART and TRUNCATE also wait for the
            // readers to leave the WAL, so the next writer may start it again.
            let restarts = mode != OtherProcessMode::Full;
            if self.index.backfilled < self.index.mx_frame
                || (restarts && self.slot_held_by_other(by))
            {
                busy = true;
            } else if mode == OtherProcessMode::Truncate {
                self.restart(by);
                // SQLite resets the wal-index before it truncates the file
                // (`walCheckpoint`): when the truncate fails, the file keeps
                // its old header and frames under a reset index.
                if !mem::take(&mut self.faults.truncate_fails) {
                    self.forget_wal();
                }
            }
        }
        CheckpointNumbers {
            busy,
            log: self.index.mx_frame,
            checkpointed: self.index.backfilled,
        }
    }

    /// Page `pgno` as a new reader sees it.
    fn image_page(&self, pgno: u32) -> Vec<u8> {
        self.image_page_through(pgno, self.index.mx_frame)
    }

    /// Page `pgno` as a reader whose snapshot ends after `frames` frames sees it.
    fn image_page_through(&self, pgno: u32, frames: u32) -> Vec<u8> {
        let newest = self
            .index
            .pgnos
            .iter()
            .take(byte_pos(u64::from(frames.min(self.index.mx_frame))))
            .rposition(|frame| *frame == pgno)
            .and_then(|at| u32::try_from(at).ok());
        let mut data = Vec::with_capacity(self.page_size.bytes());
        self.push_page(&mut data, pgno, newest);
        data
    }

    /// The database, `db_size` pages, as a reader whose snapshot ends after `frames` frames
    /// sees it. One pass over the WAL's page numbers for the whole image: asking for each page
    /// separately would scan the frames once per page.
    fn image_through(&self, frames: u32) -> Vec<u8> {
        let newest = self.newest(0, frames.min(self.index.mx_frame));
        let mut image = Vec::with_capacity(
            self.page_size
                .bytes()
                .saturating_mul(byte_pos(u64::from(self.index.db_size))),
        );
        for pgno in 1..=self.index.db_size {
            self.push_page(&mut image, pgno, newest.get(&pgno).copied());
        }
        image
    }

    /// Appends page `pgno`, a page size long: from the WAL frame `newest` when there is one,
    /// else from the database file.
    fn push_page(&self, out: &mut Vec<u8>, pgno: u32, newest: Option<u32>) {
        let start = out.len();
        let size = self.page_size.bytes();
        let (bytes, at) = match newest {
            Some(index) => (
                &self.wal,
                self.frame_at(index).saturating_add(FRAME_HEADER_LEN),
            ),
            None => (&self.db, self.page_at(pgno)),
        };
        out.extend_from_slice(bytes.get(at..at.saturating_add(size)).unwrap_or_default());
        out.resize(start.saturating_add(size), 0);
    }

    /// The automatic checkpoint, right after an app commit.
    fn autocheckpoint(&mut self) {
        let Some(at) = self.app_autocheckpoint else {
            return;
        };
        if self.index.mx_frame < at {
            return;
        }
        let before = self.index.backfilled;
        self.checkpoint(Conn::App, OtherProcessMode::Passive);
        let tally = &mut self.app_auto;
        tally.attempts = tally.attempts.saturating_add(1);
        if self.index.backfilled > before {
            tally.backfilled = tally.backfilled.saturating_add(1);
        }
    }

    /// SQLite's `walIndexRecover` over the file, by the app's connection. Real
    /// SQLite recovering the same bytes is the ground truth ([`crate::recovery`]):
    /// its `mxFrame`, size, salts and checksum make the wal-index, and the
    /// frames up to `mxFrame` give the page of each. The engine's WAL parser
    /// runs on the same bytes as a cross-check, and the rules' image of the
    /// recovered database is checked against the database SQLite recovered;
    /// any disagreement is kept ([`SimSqlite::recovery_disagreements`]).
    fn recover(&mut self) {
        // A connection opened now samples the error our sync already saw: it is told nothing.
        self.app_unsynced_error = false;
        let (parsed, seq) = self.parse_recovery();
        let wal = self.present.then_some(self.wal.as_slice());
        let index = match recovery::recover(self.page_size, &self.db, wal, recovery::Header::Fake) {
            Ok(truth) => {
                let index = self.index_from(&truth);
                self.cross_check(&parsed, &index);
                self.index = index;
                self.check_image(&truth.image);
                return self.adopt_seq(seq);
            }
            Err(err) => {
                self.recovery_disagreements
                    .push(format!("SQLite's recovery could not run: {err}"));
                parsed
            }
        };
        self.index = index;
        self.adopt_seq(seq);
    }

    fn adopt_seq(&mut self, seq: u32) {
        self.app_checkpoints = seq;
        self.ours_checkpoints = 0;
        self.writer_checkpoints = 0;
    }

    /// The engine's parser's reading of the file: the index it would recover
    /// and the header's checkpoint sequence.
    fn parse_recovery(&self) -> (Index, u32) {
        let mut index = Index::empty(self.index.salts, self.db_pages());
        let mut seq = 0;
        if let Ok(mut reader) = WalReader::open(Cursor::new(self.wal.as_slice()))
            && reader.header().page_size() == self.page_size
        {
            index.salts = reader.header().salts();
            seq = reader.header().checkpoint_seq();
            let mut cursor = reader.cursor_at_start();
            while let Ok(TxOutcome::Tx(tx)) = reader.next_tx(&cursor, ScanLimit::FirstInvalidFrame)
            {
                index
                    .pgnos
                    .extend(tx.frames().iter().map(|frame| frame.pgno().get()));
                index.db_size = tx.commit().get();
                cursor = *tx.end();
                index.tx_ends.push(cursor.next().frame_index());
            }
            index.mx_frame = cursor.next().frame_index();
            index.written = index.mx_frame;
            index.running = cursor.checksum();
            index.committed = cursor.checksum();
        }
        (index, seq)
    }

    /// The wal-index SQLite recovered, with each frame's page number and the
    /// commits read from the frames it kept.
    fn index_from(&self, truth: &recovery::Recovered) -> Index {
        let mut index = Index::empty(truth.salts, self.db_pages());
        for frame in 0..truth.mx_frame {
            let at = self.frame_at(frame);
            let field = |off: usize| {
                let from = at.saturating_add(off);
                self.wal
                    .get(from..from.saturating_add(4))
                    .and_then(|b| b.try_into().ok())
                    .map_or(0, u32::from_be_bytes)
            };
            index.pgnos.push(field(0));
            if field(4) != 0 {
                index.tx_ends.push(frame.saturating_add(1));
            }
        }
        if truth.mx_frame > 0 {
            index.db_size = truth.db_pages;
        }
        index.mx_frame = truth.mx_frame;
        index.written = truth.mx_frame;
        index.running = truth.checksum;
        index.committed = truth.checksum;
        index
    }

    fn cross_check(&mut self, parsed: &Index, truth: &Index) {
        let same = parsed.mx_frame == truth.mx_frame
            && parsed.pgnos == truth.pgnos
            && parsed.tx_ends == truth.tx_ends
            && parsed.db_size == truth.db_size
            && (truth.mx_frame == 0
                || (parsed.salts == truth.salts && parsed.committed == truth.committed));
        if !same {
            self.recovery_disagreements.push(format!(
                "the engine's parser recovered mxFrame {} (size {}), SQLite {} (size {})",
                parsed.mx_frame, parsed.db_size, truth.mx_frame, truth.db_size
            ));
        }
    }

    /// The rules' recovered image, page for page, against the database SQLite
    /// recovered. Page 1's first 100 bytes are the probe's own header unless a
    /// recovered frame wrote page 1.
    fn check_image(&mut self, image: &[u8]) {
        let page_1_in_wal = self.index.pgnos.contains(&1);
        let pages = self.index.db_size;
        let bad = (1..=pages).find(|pgno| {
            let at = self.page_at(*pgno);
            let skip = if *pgno == 1 && !page_1_in_wal { 100 } else { 0 };
            let want = self.image_page(*pgno);
            let got = image.get(at.saturating_add(skip)..at.saturating_add(self.page_size.bytes()));
            got != want.get(skip..)
        });
        if let Some(pgno) = bad {
            self.recovery_disagreements.push(format!(
                "page {pgno} of the rules' recovered image is not SQLite's"
            ));
        }
    }

    /// A full backfill and a fresh, empty WAL with every connection closed.
    fn close_all(&mut self) -> bool {
        if !self.readers.is_empty() || self.writer.is_some() {
            return false;
        }
        let mx_frame = self.index.mx_frame;
        if self.index.backfilled < mx_frame {
            self.copy_frames(mx_frame);
        }
        self.forget_wal();
        self.index = Index::empty(self.index.salts, self.index.db_size);
        self.app_checkpoints = 0;
        self.ours_checkpoints = 0;
        self.writer_checkpoints = 0;
        true
    }

    fn token(&mut self) -> u64 {
        self.next_token = self.next_token.saturating_add(1);
        self.next_token
    }

    /// `WritesHeld` while the gate holds: `lsr-replica`'s checkpointer refuses every write and
    /// checkpoint then, before SQLite is asked.
    fn writes_allowed(&self) -> Result<(), SourceError> {
        if self.writer == Some(Conn::Gate) {
            return Err(SourceError::WritesHeld);
        }
        Ok(())
    }

    fn take_writer(&mut self, conn: Conn) -> Result<u64, SourceError> {
        // One barrier at a time on the one writer connection, as the real checkpointer refuses
        // a second while a token is outstanding.
        if self.barrier.is_some() {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        if self.writer.is_some() || self.readers.contains_key(&conn) {
            return Err(SourceError::Busy);
        }
        self.begin(conn);
        self.writer = Some(conn);
        Ok(self.token())
    }

    fn drop_writer(&mut self, conn: Conn) {
        self.readers.remove(&conn);
        if self.writer == Some(conn) {
            self.writer = None;
        }
    }

    fn process_crash(&mut self) {
        self.end(Conn::Ours);
        self.drop_writer(Conn::Writer);
        // The gate's connection is our process's too.
        self.drop_writer(Conn::Gate);
        self.barrier = None;
        self.ours_checkpoints = 0;
        self.writer_checkpoints = 0;
    }

    fn power_crash(&mut self) {
        self.process_crash();
        self.readers.clear();
        self.writer = None;
        self.faults.shm_busy = false;
        self.faults.shm_forced = None;
        self.faults.overwrite = None;
        if self.unlinked {
            // Every write since the unlink went to a file no path names: a power loss leaves
            // nothing at the WAL path, neither those writes nor any older bytes.
            self.unlinked = false;
            self.present = false;
            self.durable.clear();
            self.wal.clear();
            self.ghost.clear();
        }
        self.wal = self.surviving_wal();
        self.db = self.surviving_db();
        self.recover();
    }

    /// What of the database file a power loss leaves: under the block law each block a
    /// partial backfill wrote since the last full one survives or not, as the WAL's do;
    /// otherwise none of those writes survive.
    fn surviving_db(&mut self) -> Vec<u8> {
        let out = match self.blocks.as_mut() {
            Some((block, rng)) => {
                let dirty = crash_law::differing_blocks(&self.db_durable, &self.db, *block);
                if !dirty.is_empty() {
                    self.db_torn = self.db_torn.saturating_add(1);
                }
                crash_law::crash_image(
                    &self.db_durable,
                    &self.db,
                    &dirty,
                    &self.db_durable,
                    *block,
                    rng,
                )
            }
            None => self.db_durable.clone(),
        };
        self.db_durable.clone_from(&out);
        out
    }

    /// Truncates the WAL to nothing, durably, keeping its bytes as the stale
    /// bytes a later crash may show.
    fn forget_wal(&mut self) {
        let wal = mem::take(&mut self.wal);
        if !wal.is_empty() {
            self.ghost = wal;
        }
        self.durable.clear();
    }

    /// What of the WAL a power loss leaves under the block law
    /// ([`crate::crash_law`]): the dirty blocks are the ones where the WAL
    /// differs from its bytes at the last fsync. The database file's own
    /// unsynced blocks, a partial backfill's, are drawn by [`State::surviving_db`].
    fn surviving_blocks(&mut self) -> Option<Vec<u8>> {
        let (block, rng) = self.blocks.as_mut()?;
        let dirty = crash_law::differing_blocks(&self.durable, &self.wal, *block);
        let out =
            crash_law::crash_image(&self.durable, &self.wal, &dirty, &self.ghost, *block, rng);
        let (kept, hole) = crash_law::kept_and_holes(&self.durable, &self.wal, &out, *block);
        if kept {
            self.block_kept.0 = self.block_kept.0.saturating_add(1);
        }
        if hole {
            self.block_kept.1 = self.block_kept.1.saturating_add(1);
        }
        self.durable.clone_from(&out);
        Some(out)
    }

    /// What of the WAL a power loss leaves: the bytes of the last fsync, or,
    /// with a torn seed, those with a drawn prefix of the later writes on
    /// top. Writes past the last fsync reach the disk in order, any number
    /// of them, and the last may be cut inside a frame; SQLite's recovery
    /// then keeps the frames whose checksum chain still holds
    /// (`synchronous=NORMAL`).
    fn surviving_wal(&mut self) -> Vec<u8> {
        if let Some(out) = self.surviving_blocks() {
            return out;
        }
        let Some(seed) = self.torn.as_mut() else {
            return self.durable.clone();
        };
        let same = self
            .wal
            .iter()
            .zip(&self.durable)
            .take_while(|(a, b)| a == b)
            .count();
        let unsynced = self.wal.len().saturating_sub(same);
        let frame = FRAME_HEADER_LEN.saturating_add(self.page_size.bytes());
        let frames = unsynced.checked_div(frame).unwrap_or(0);
        let draw = |seed: &mut u64, upper: usize| {
            usize::try_from(splitmix(seed))
                .unwrap_or(0)
                .checked_rem(upper.saturating_add(1))
                .unwrap_or(0)
        };
        let whole = draw(seed, frames).saturating_mul(frame);
        let torn = if draw(seed, 3) == 0 {
            draw(seed, frame)
        } else {
            0
        };
        let kept = same
            .saturating_add(whole)
            .saturating_add(torn)
            .min(self.wal.len());
        if kept > same {
            self.torn_kept.0 = self.torn_kept.0.saturating_add(1);
        }
        if torn > 0 {
            self.torn_kept.1 = self.torn_kept.1.saturating_add(1);
        }
        let mut out = self.wal.get(..kept).unwrap_or_default().to_vec();
        out.extend_from_slice(self.durable.get(kept..).unwrap_or_default());
        self.durable.clone_from(&out);
        out
    }

    /// The fault of `overwrite_frame_between_scan_and_read`: on the second read
    /// that touches the frame's page, the page changes first.
    fn on_read(&mut self, start: usize, end: usize) {
        let Some(mut armed) = self.faults.overwrite else {
            return;
        };
        let page = self
            .frame_at(armed.frame.saturating_sub(1))
            .saturating_add(FRAME_HEADER_LEN);
        let page_end = page.saturating_add(self.page_size.bytes());
        if armed.flipped {
            // The other writer's change was never durable: the page reads true
            // again, so only the one read saw it.
            self.invert(page, page_end);
            self.faults.overwrite = None;
            return;
        }
        if start >= page_end || end <= page {
            return;
        }
        armed.reads = armed.reads.saturating_add(1);
        if armed.reads >= 2 {
            armed.flipped = true;
            self.invert(page, page_end);
        }
        self.faults.overwrite = Some(armed);
    }

    fn invert(&mut self, from: usize, to: usize) {
        if let Some(bytes) = self.wal.get_mut(from..to) {
            bytes.iter_mut().for_each(|byte| *byte ^= 0xff);
        }
    }
}

/// SQLite in WAL mode, as rules over shared state; clones share it.
#[derive(Clone, Debug)]
pub struct SimSqlite {
    state: Arc<Mutex<State>>,
}

impl SimSqlite {
    /// A database of `db_pages` pages from [`page`] under `seed`, an empty WAL
    /// file, no read transaction, epoch 1. `seed` also seeds the salts.
    #[must_use]
    pub fn new(page_size: PageSize, db_pages: u32, seed: u64) -> Self {
        let mut db = Vec::new();
        for pgno in (1..=db_pages).filter_map(|n| PageNo::new(n, page_size)) {
            db.extend(page(seed, pgno, page_size));
        }
        let state = State {
            page_size,
            db_durable: db.clone(),
            db,
            wal: Vec::new(),
            durable: Vec::new(),
            present: true,
            unlinked: false,
            index: Index::empty(Salts { salt1: 0, salt2: 0 }, db_pages),
            readers: BTreeMap::new(),
            writer: None,
            app_checkpoints: 0,
            other_writer_checkpoints: 0,
            ours_checkpoints: 0,
            writer_checkpoints: 0,
            rng: seed,
            forced: None,
            epoch: ReadLockHold::first(),
            next_reader: 0,
            next_token: 0,
            barrier: None,
            faults: Faults::default(),
            app_autocheckpoint: None,
            our_sync_lies: false,
            lost_writeback: None,
            app_unsynced_error: false,
            lost_ranges: Vec::new(),
            app_auto: AutoTally::default(),
            in_tick: None,
            in_tick_done: None,
            engine_commits: Vec::new(),
            torn: None,
            torn_kept: (0, 0),
            blocks: None,
            ghost: Vec::new(),
            block_kept: (0, 0),
            db_torn: 0,
            recovery_disagreements: Vec::new(),
            app: in_tick::AppInTick::default(),
        };
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut lock(&self.state))
    }

    /// Our WAL handle.
    #[must_use]
    pub fn source(&self) -> SimSource {
        SimSource {
            state: Arc::clone(&self.state),
        }
    }

    /// Our database-file handle.
    #[must_use]
    pub fn pages(&self) -> SimPages {
        SimPages {
            state: Arc::clone(&self.state),
        }
    }

    /// Our checkpoint, barrier and write-lock handle.
    #[must_use]
    pub fn checkpointer(&self) -> SimCheckpointer {
        SimCheckpointer {
            state: Arc::clone(&self.state),
        }
    }

    /// Names the values the model uses wherever SQLite draws a salt at random (a header written by
    /// a connection whose checkpoint count is 0, and `salt2` of every restart), or `None` to go
    /// back to the seeded stream. A test that compares the model with real SQLite feeds it the
    /// salts SQLite chose; `salt1 + 1` on a restart stays the model's rule.
    pub fn force_random_salts(&self, salts: Option<Salts>) {
        self.with(|state| state.forced = salts);
    }

    /// What the driver does at process start (`db.go:1673`): a WAL under a header
    /// gets one frame so the read lock lands above read mark 0, then the read transaction
    /// that holds the WAL in place begins. Any earlier read lock ended with its process.
    pub fn start_driver(&self) {
        self.with(|state| {
            state.end(Conn::Ours);
            let skip = mem::take(&mut state.faults.driver_writes_nothing);
            if !skip
                && state.wal.len() < 32
                && let Some(first) = PageNo::new(1, state.page_size)
            {
                let data = state.image_page(1);
                let size = state.index.db_size.max(1);
                state.write(Conn::Writer, &[(first, data)], Some(size));
            }
            state.begin(Conn::Ours);
        });
    }

    /// Begins our read transaction under the mark-choice rule; false if it is held.
    pub fn our_reader_begin(&self) -> bool {
        self.with(|state| state.begin(Conn::Ours))
    }

    /// Ends our read transaction, which moves the epoch; false if none is held.
    pub fn our_reader_end(&self) -> bool {
        self.with(|state| state.end(Conn::Ours))
    }

    /// The app commits `pages` (after anything it spilled) with database size
    /// `db_size`. `None` when there are no pages, `db_size` is 0, or another
    /// connection holds the write lock.
    pub fn commit(&self, pages: &[(PageNo, Vec<u8>)], db_size: u32) -> Option<AppCommit> {
        self.with(|state| state.app_commit(pages, db_size))
    }

    /// Another application connection, which stays open, commits `pages` with database size
    /// `db_size` under its own checkpoint count. `None` while another writer holds the lock.
    pub fn other_writer_commit(
        &self,
        pages: &[(PageNo, Vec<u8>)],
        db_size: u32,
    ) -> Option<AppCommit> {
        self.with(|state| state.commit_by(Conn::OtherWriter, pages, db_size))
    }

    /// Arms a commit the app makes inside the engine's next tick, at `site`.
    /// `created_at` is read at that moment, so the harness can tell what
    /// happened before the commit and what after. It replaces any armed
    /// commit; [`SimSqlite::take_commit_in_tick`] disarms it.
    pub fn arm_commit_in_tick(
        &self,
        pages: Vec<(PageNo, Vec<u8>)>,
        db_size: u32,
        site: InTickSite,
        created_at: CreatedAt,
    ) {
        self.with(|state| {
            state.in_tick = Some(InTick {
                pages,
                db_size,
                site,
                created_at,
            });
        });
    }

    /// The engine's own commits since the last call, oldest first.
    pub fn take_engine_commits(&self) -> Vec<AppCommit> {
        self.with(|state| mem::take(&mut state.engine_commits))
    }

    /// The armed commit if a barrier made it (`None` when no barrier came or
    /// the commit was refused), and disarms it either way.
    pub fn take_commit_in_tick(&self) -> Option<InTickCommit> {
        self.with(|state| {
            state.app_commit_unseen();
            state.in_tick = None;
            state.in_tick_done.take()
        })
    }

    /// From now on our sync of the WAL (the engine's barrier before staging) succeeds and makes
    /// nothing durable, as on a disk that lies about fsync; SQLite's own syncs still hold.
    pub fn lie_on_our_sync(&self) {
        self.with(|state| state.our_sync_lies = true);
    }

    /// Our sync of the WAL makes its bytes durable again: the lying disk replaced.
    pub fn honest_on_our_sync(&self) {
        self.with(|state| state.our_sync_lies = false);
    }

    /// The app's `wal_autocheckpoint` in frames; `None`, the model's default, is 0.
    pub fn set_app_autocheckpoint(&self, frames: Option<u32>) {
        self.with(|state| state.app_autocheckpoint = frames.filter(|n| *n > 0));
    }

    /// The app's automatic checkpoints so far.
    #[must_use]
    pub fn app_autocheckpoints(&self) -> AutoTally {
        self.with(|state| state.app_auto)
    }

    /// The app writes frames of a transaction it has not committed (a cache
    /// spill), holding the write lock until it commits or rolls back.
    pub fn spill(&self, pages: &[(PageNo, Vec<u8>)]) -> bool {
        self.with(|state| state.write(Conn::App, pages, None))
    }

    /// The app rolls back its open transaction; spilled frames stay in the file
    /// past `mxFrame`.
    pub fn rollback(&self) -> bool {
        self.with(|state| state.rollback(Conn::App))
    }

    /// The app shrinks the database to `db_size` pages: a commit of page 1.
    pub fn shrink(&self, db_size: u32) -> Option<AppCommit> {
        let current = self.with(|state| state.index.db_size);
        if db_size == 0 || db_size >= current {
            return None;
        }
        let first = PageNo::new(1, self.page_size())?;
        let data = self.with(|state| state.image_page(1));
        self.commit(&[(first, data)], db_size)
    }

    /// VACUUM to a new page size, with every connection closed: a full
    /// backfill, the database re-cut into pages of `new`, an empty WAL.
    pub fn rewrite_page_size(&self, new: PageSize) -> bool {
        self.with(|state| {
            if new == state.page_size || !state.close_all() {
                return false;
            }
            state.page_size = new;
            let pages = state.db.len().div_ceil(new.bytes());
            state.db.resize(pages.saturating_mul(new.bytes()), 0);
            state.db_durable.clone_from(&state.db);
            state.index.db_size = state.db_pages();
            state.present = true;
            true
        })
    }

    /// Another process begins a read transaction under the mark-choice rule.
    pub fn other_process_reader_begin(&self) -> ReaderId {
        self.with(|state| {
            let id = ReaderId(state.next_reader);
            state.next_reader = state.next_reader.saturating_add(1);
            state.begin(Conn::OtherProcess(id));
            id
        })
    }

    /// The another process's read transaction ends; false if it was not held.
    pub fn other_process_reader_end(&self, id: ReaderId) -> bool {
        self.with(|state| state.end(Conn::OtherProcess(id)))
    }

    /// `PRAGMA wal_checkpoint(mode)` on another process's connection, which then closes. The
    /// app's open transaction does not stop it: its write lock degrades a RESTART or TRUNCATE
    /// to a busy PASSIVE, and its read mark holds the backfill back, as any connection's do.
    /// Always `Some`.
    pub fn other_process_checkpoint(&self, mode: OtherProcessMode) -> Option<CheckpointNumbers> {
        self.with(|state| Some(state.checkpoint(Conn::Cli, mode)))
    }

    /// The last connection closes: a full checkpoint, then the WAL is deleted
    /// and every connection's checkpoint count starts again from 0. False while
    /// any connection, ours included, holds a transaction.
    pub fn delete_wal(&self) -> bool {
        self.with(|state| {
            if !state.close_all() {
                return false;
            }
            state.present = false;
            true
        })
    }

    /// Zeroes header bytes 8..32 (page size, sequence, salts, checksum), on
    /// disk too.
    pub fn tear_wal_header(&self) -> bool {
        self.with(|state| {
            if state.wal.len() < HEADER_LEN {
                return false;
            }
            for bytes in [&mut state.wal, &mut state.durable] {
                if let Some(torn) = bytes.get_mut(8..HEADER_LEN) {
                    torn.fill(0);
                }
            }
            true
        })
    }

    /// Cuts the WAL file to `len` bytes; the wal-index does not notice.
    pub fn truncate_wal_at(&self, len: u64) -> bool {
        self.with(|state| {
            let len = byte_pos(len);
            if len >= state.wal.len() {
                return false;
            }
            state.wal.truncate(len);
            state.durable.truncate(len);
            true
        })
    }

    /// Flips bit `bit` of the WAL file (bit 0 is the low bit of byte 0).
    pub fn flip_bit(&self, bit: u64) -> bool {
        self.with(|state| {
            let at = byte_pos(bit.wrapping_shr(3));
            let mask = 1_u8.wrapping_shl(u32::try_from(bit & 7).unwrap_or_default());
            if at >= state.wal.len() {
                return false;
            }
            for bytes in [&mut state.wal, &mut state.durable] {
                if let Some(byte) = bytes.get_mut(at) {
                    *byte ^= mask;
                }
            }
            true
        })
    }

    /// Replaces the WAL with a valid one of the same length in a generation
    /// drawn from `seed`; the wal-index is left alone.
    pub fn rewrite_same_length(&self, seed: u64) -> bool {
        self.with(|state| {
            let len = state.wal.len();
            let first = PageNo::new(1, state.page_size);
            let (Some(first), true) = (first, len >= HEADER_LEN) else {
                return false;
            };
            let mut rng = seed;
            let salts = Salts {
                salt1: splitmix(&mut rng),
                salt2: splitmix(&mut rng),
            };
            let mut wal = WalBuilder::new(state.page_size, false, salts);
            while wal.bytes().len().saturating_add(wal.frame_len()) <= len {
                wal.frame(first, state.index.db_size.max(1), seed);
            }
            let mut bytes = wal.into_bytes();
            bytes.resize(len, 0);
            state.durable.clone_from(&bytes);
            state.wal = bytes;
            true
        })
    }

    /// Rewrites this generation's committed frames with new content from
    /// `seed` (same page numbers, commit fields and salts, a valid chain) and
    /// commits one more frame of page 1, as a writer that kept the salts would.
    pub fn regrow_same_salts(&self, seed: u64) -> bool {
        self.regrow_with(seed, true)
    }

    /// [`Self::regrow_same_salts`] with every old frame's commit field
    /// cleared, so the frame before the old end is valid but not a commit.
    pub fn regrow_without_commit_marks(&self, seed: u64) -> bool {
        self.regrow_with(seed, false)
    }

    fn regrow_with(&self, seed: u64, marks: bool) -> bool {
        self.with(|state| {
            let header = state
                .wal
                .first_chunk::<HEADER_LEN>()
                .and_then(|head| WalHeader::decode(head).ok());
            let Some(header) = header else {
                return false;
            };
            if state.writer.is_some()
                || state.index.mx_frame == 0
                || header.salts() != state.index.salts
            {
                return false;
            }
            regrow(state, header.checksum(), seed, marks);
            true
        })
    }

    /// The next wal-index read finds its two header copies unequal.
    pub fn shm_mid_update(&self) -> bool {
        self.with(|state| {
            state.faults.shm_busy = true;
            true
        })
    }

    /// The next wal-index read reports an end inside a transaction: one frame
    /// short of the last commit, or one spilled frame past it.
    pub fn shm_end_inside_tx(&self) -> bool {
        self.with(|state| {
            let index = &state.index;
            let previous = index
                .tx_ends
                .iter()
                .rev()
                .nth(1)
                .copied()
                .unwrap_or_default();
            let frames = if index.mx_frame.saturating_sub(previous) >= 2 {
                index.mx_frame.saturating_sub(1)
            } else if index.written > index.mx_frame {
                index.mx_frame.saturating_add(1)
            } else {
                return false;
            };
            let end = CommittedEnd::new(frames, index.salts, state.page_size);
            state.faults.shm_forced =
                CommittedRead::new(end, index.backfilled.min(frames), state.epoch)
                    .map(|read| (read, 2));
            state.faults.shm_forced.is_some()
        })
    }

    /// Frame `frame` (1-based) is overwritten between the scan that verified
    /// it and the read of its page: the second read touching its page sees
    /// every byte inverted.
    pub fn overwrite_frame_between_scan_and_read(&self, frame: u32) -> bool {
        self.with(|state| {
            if frame == 0 || frame > state.index.written {
                return false;
            }
            state.faults.overwrite = Some(Overwrite {
                frame,
                reads: 0,
                flipped: false,
            });
            true
        })
    }

    /// The WAL file is unlinked; SQLite keeps writing the open file, and our
    /// `open` and `len` report [`SourceError::Unlinked`] from now on.
    pub fn unlink_wal(&self) -> bool {
        self.with(|state| {
            if !state.present || state.unlinked {
                return false;
            }
            state.unlinked = true;
            true
        })
    }

    /// The wal-index reports no committed frame until the next page count,
    /// which reads zero: a database created and not yet written.
    pub fn empty_database_once(&self) {
        self.with(|state| {
            state.faults.empty_db = true;
        });
    }

    /// Flips one byte of page `pgno` in the database file, as a stray write
    /// would; false when the page is past the file or the current generation's
    /// committed frames hold it (a reader would never see the damage).
    pub fn flip_db_page(&self, pgno: u32) -> bool {
        self.with(|state| {
            let committed = state
                .index
                .pgnos
                .iter()
                .take(byte_pos(u64::from(state.index.mx_frame)));
            if pgno == 0 || pgno > state.db_pages() || committed.clone().any(|p| *p == pgno) {
                return false;
            }
            let at = state.page_at(pgno).saturating_add(100);
            // Damage on the medium: the flipped byte is what the disk holds, synced or not.
            if let Some(byte) = state.db_durable.get_mut(at) {
                *byte ^= 0x5a;
            }
            match state.db.get_mut(at) {
                Some(byte) => {
                    *byte ^= 0x5a;
                    true
                }
                None => false,
            }
        })
    }

    /// Drops every source failure injected and not yet met, as the store
    /// faults' heal does for theirs: from here the world imposes none.
    pub fn heal_injections(&self) {
        self.with(|state| state.faults.injections.clear());
    }

    /// The replicator's write gate takes the write lock (`BEGIN IMMEDIATE` on a connection of
    /// its own): from here no other connection commits, and our checkpointer refuses every
    /// write as `WritesHeld`, as `lsr-replica`'s does. False, changing nothing, while another
    /// connection is in a write transaction; true if the gate already holds.
    pub fn hold_writes(&self) -> bool {
        self.with(|state| {
            if state.writer == Some(Conn::Gate) {
                return true;
            }
            if state.writer.is_some() {
                return false;
            }
            state.begin(Conn::Gate);
            state.writer = Some(Conn::Gate);
            true
        })
    }

    /// The gate rolls back: writes go through again.
    pub fn release_writes(&self) {
        self.with(|state| state.drop_writer(Conn::Gate));
    }

    /// Whether the gate holds the write lock now.
    pub fn writes_held(&self) -> bool {
        self.with(|state| state.writer == Some(Conn::Gate))
    }

    /// Whether a connection other than the gate is in a write transaction now.
    pub fn writer_busy(&self) -> bool {
        self.with(|state| state.writer.is_some_and(|holder| holder != Conn::Gate))
    }

    /// The next TRUNCATE checkpoint that finishes, ours or another process's, resets
    /// the wal-index and then fails to truncate the file (`ftruncate` failing):
    /// the file keeps the old header and frames until the next commit writes a
    /// header. Our own checkpoint call reports the I/O error. The real-SQLite
    /// fault suite found this state (`crates/sqlite/tests/sqlite/faults.rs`).
    pub fn fail_next_truncate(&self) {
        self.with(|state| state.faults.truncate_fails = true);
    }

    /// The next driver start writes nothing, as the real driver's open writes
    /// nothing once `_litestream_seq` exists (a database a restore published):
    /// a WAL with no header stays without one until the engine or the app writes.
    pub fn driver_writes_nothing_once(&self) {
        self.with(|state| state.faults.driver_writes_nothing = true);
    }

    /// The next call of `op` fails with `err`.
    pub fn inject(&self, op: SourceOp, err: SourceError) {
        self.with(|state| state.faults.injections.push((op, err)));
    }

    /// Injected failures a call took, in order.
    pub fn taken_injections(&self) -> Vec<(SourceOp, SourceError)> {
        self.with(|state| state.faults.taken.clone())
    }

    /// From now on a power loss keeps a prefix of the unsynced WAL writes,
    /// drawn from `seed`, instead of none.
    pub fn tear_on_power_loss(&self, seed: u64) {
        self.with(|state| state.torn = Some(seed));
    }

    /// From now on a power loss keeps what `law` draws over the unsynced WAL
    /// writes ([`crate::crash_law`]); [`CrashLaw::AllOrNothing`] keeps none.
    pub fn crash_by_law(&self, law: CrashLaw) {
        self.with(|state| {
            state.blocks = match law {
                CrashLaw::AllOrNothing => None,
                CrashLaw::Blocks { block, seed } => Some((block, Rng(seed))),
            };
        });
    }

    /// Power losses under the block law that kept some unsynced WAL block, and
    /// those among them that lost a block before one they kept.
    #[must_use]
    pub fn block_losses(&self) -> (u32, u32) {
        self.with(|state| state.block_kept)
    }

    /// Power losses under the block law that found database blocks a partial backfill had
    /// written and not synced, which the law then kept or lost.
    #[must_use]
    pub fn db_torn_losses(&self) -> u32 {
        self.with(|state| state.db_torn)
    }

    /// Every recovery at which the engine's WAL parser and SQLite's own
    /// recovery, run on the same bytes, disagreed.
    #[must_use]
    pub fn recovery_disagreements(&self) -> Vec<String> {
        self.with(|state| state.recovery_disagreements.clone())
    }

    /// Power losses that kept some unsynced WAL bytes, and those among them
    /// that kept a frame cut inside.
    #[must_use]
    pub fn torn_losses(&self) -> (u32, u32) {
        self.with(|state| state.torn_kept)
    }

    /// A crash.
    pub fn crash(&self, kind: CrashKind) {
        self.with(|state| match kind {
            CrashKind::Process => state.process_crash(),
            CrashKind::Power => state.power_crash(),
        });
    }

    /// Puts `image` (the bytes [`Self::image`] gave once) in place of the database file, the
    /// WAL gone, as a plain copy over it would. False while a connection is open.
    pub fn put_back_database(&self, image: &[u8]) -> bool {
        self.with(|state| {
            if !state.close_all() {
                return false;
            }
            state.db = image.to_vec();
            state.db_durable = image.to_vec();
            let pages = state.db_pages();
            state.index = Index::empty(state.index.salts, pages);
            true
        })
    }

    /// The database as a new reader sees it, `db_size` pages.
    #[must_use]
    pub fn image(&self) -> Vec<u8> {
        self.with(|state| state.image_through(state.index.mx_frame))
    }

    /// The database at each commit mark of the WAL's generation, ascending: the frame count
    /// the mark ends at, and the `db_size` pages a reader that stopped there sees.
    #[must_use]
    pub fn commit_images(&self) -> Vec<(u32, Vec<u8>)> {
        self.with(|state| {
            state
                .index
                .tx_ends
                .iter()
                .filter(|end| **end <= state.index.mx_frame)
                .map(|end| (*end, state.image_through(*end)))
                .collect()
        })
    }

    /// The database file's bytes.
    #[must_use]
    pub fn db_file(&self) -> Vec<u8> {
        self.with(|state| state.db.clone())
    }

    /// The WAL file's bytes; `None` when there is no file.
    #[must_use]
    pub fn wal_file(&self) -> Option<Vec<u8>> {
        self.with(|state| state.present.then(|| state.wal.clone()))
    }

    /// The page size.
    #[must_use]
    pub fn page_size(&self) -> PageSize {
        self.with(|state| state.page_size)
    }

    /// Whether the WAL bytes `[start, end)` of generation `salts` would survive a power loss
    /// now: true too when the WAL is in another generation (those frames are no longer
    /// SQLite's), when the disk lies about our sync (that lie is the scenario's point), or when
    /// a failed sync of ours lost the range: v1 goes on past it.
    #[must_use]
    pub fn wal_range_durable(&self, salts: Salts, start: u64, end: u64) -> bool {
        self.with(|state| {
            if state.our_sync_lies {
                return true;
            }
            // Frames a failed sync lost are staged once a retried sync succeeds: v1 goes on after
            // a failed sync of the WAL, as Go does, and a power loss then takes them
            // (`docs/decisions/D173.md`). The boot's rewind check answers it, so the staged file
            // is not a violation.
            let lost = state
                .lost_ranges
                .iter()
                .any(|(s, from, to)| *s == salts && start < *to && *from < end);
            if lost {
                return true;
            }
            if state.index.salts != salts {
                return true;
            }
            let range = byte_pos(start)..byte_pos(end);
            state.wal.get(range.clone()).is_some()
                && state.wal.get(range.clone()) == state.durable.get(range)
        })
    }

    /// Whether a power loss now would keep the whole WAL: the last fsync came
    /// after the last frame. A checkpoint that copied nothing does not fsync.
    #[must_use]
    pub fn wal_synced(&self) -> bool {
        self.with(|state| state.durable == state.wal)
    }

    /// `mxFrame`.
    #[must_use]
    pub fn mx_frame(&self) -> u32 {
        self.with(|state| state.index.mx_frame)
    }

    /// `nBackfill`.
    #[must_use]
    pub fn backfilled(&self) -> u32 {
        self.with(|state| state.index.backfilled)
    }

    /// The WAL file's header as a reader would find it: its salts, or `None`
    /// when there is no file or no whole header with a page size. Cheap: it
    /// reads the 32 header bytes, not the file.
    #[must_use]
    pub fn wal_header_salts(&self) -> Option<(u32, u32)> {
        self.with(|state| {
            let wal = state.present.then_some(&state.wal)?;
            let word = |at: usize| {
                let bytes: [u8; 4] = wal.get(at..at.checked_add(4)?)?.try_into().ok()?;
                Some(u32::from_be_bytes(bytes))
            };
            let page_size = word(8)?;
            let salts = (word(16)?, word(20)?);
            (page_size != 0 && wal.len() >= 32).then_some(salts)
        })
    }

    /// Whether the wal-index counts no frame and names salts the WAL file's
    /// header does not: SQLite reset it and the next commit writes the header.
    #[must_use]
    pub fn index_reset_ahead(&self) -> bool {
        let header = self.wal_header_salts();
        self.with(|state| {
            let index = (state.index.salts.salt1, state.index.salts.salt2);
            state.index.mx_frame == 0 && header != Some(index)
        })
    }

    /// Whether our read transaction is held, on any read mark. A mark above
    /// zero keeps any writer from restarting the WAL; mark zero lets one
    /// generation change through and then blocks every backfill, so no commit
    /// can go with a generation either way (`lsr_core::recover_traced`, rule 6).
    /// With none held nothing pins the WAL.
    #[must_use]
    pub fn our_reader_pins_wal(&self) -> bool {
        self.with(|state| state.readers.contains_key(&Conn::Ours))
    }

    /// The wal-index salts.
    #[must_use]
    pub fn salts(&self) -> Salts {
        self.with(|state| state.index.salts)
    }

    /// The database size of the last commit.
    #[must_use]
    pub fn db_size(&self) -> u32 {
        self.with(|state| state.index.db_size)
    }
}

fn regrow(state: &mut State, header: WalChecksum, seed: u64, marks: bool) {
    let frames = state.index.mx_frame;
    let ends = state.index.tx_ends.clone();
    let pgnos = state.index.pgnos.clone();
    state.index.written = 0;
    state.index.running = header;
    state.index.pgnos.clear();
    let size = state.index.db_size;
    for (at, pgno) in (1..=frames).zip(pgnos) {
        let commit = if marks && ends.contains(&at) { size } else { 0 };
        let data = PageNo::new(pgno, state.page_size)
            .map(|named| page(seed, named, state.page_size))
            .unwrap_or_default();
        state.put_frame(pgno, commit, &data);
    }
    if let Some(first) = PageNo::new(1, state.page_size) {
        let data = page(seed, first, state.page_size);
        state.put_frame(1, size, &data);
    }
    state.index.mx_frame = state.index.written;
    state.index.committed = state.index.running;
    state.index.tx_ends.push(state.index.mx_frame);
}

/// Our WAL handle: [`WalSource`] over the shared state.
#[derive(Clone, Debug)]
pub struct SimSource {
    state: Arc<Mutex<State>>,
}

impl WalSource for SimSource {
    type Wal = SimWal;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::Open));
        state.faults.take(SourceOp::Open)?;
        if state.unlinked {
            return Err(SourceError::Unlinked);
        }
        Ok(state.present.then(|| SimWal {
            state: Arc::clone(&self.state),
            pos: 0,
        }))
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::Len));
        state.faults.take(SourceOp::Len)?;
        if state.unlinked {
            return Err(SourceError::Unlinked);
        }
        let len = u64::try_from(state.wal.len()).unwrap_or(u64::MAX);
        Ok(state.present.then(|| WalFileLen::new(len)))
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::Committed));
        // The application commits after the tick read its clock and before the engine reads
        // the committed end: the stamp of the file that carries it must come after it.
        state.commit_in_tick(InTickSite::BeforeCommittedRead);
        state.app_committed_read();
        state.faults.take(SourceOp::Committed)?;
        // The real source retakes a lapsed read transaction before it reads
        // (`ensure_read_lock_held`), so every answer belongs to a hold that is
        // still held; the retake is a new hold.
        state.begin(Conn::Ours);
        if mem::take(&mut state.faults.shm_busy) {
            return Err(SourceError::Busy);
        }
        if let Some((read, left)) = state.faults.shm_forced.take() {
            if let Some(left) = left.checked_sub(1).filter(|left| *left > 0) {
                state.faults.shm_forced = Some((read, left));
            }
            // The forced numbers, under the hold the read is taken in now.
            return CommittedRead::new(read.end(), read.backfilled(), state.epoch)
                .ok_or(SourceError::Busy);
        }
        let index = &state.index;
        if state.faults.empty_db {
            let end = CommittedEnd::new(0, index.salts, state.page_size);
            return CommittedRead::new(end, 0, state.epoch).ok_or(SourceError::Busy);
        }
        let end = CommittedEnd::new(index.mx_frame, index.salts, state.page_size);
        CommittedRead::new(end, index.backfilled, state.epoch).ok_or(SourceError::Busy)
    }

    fn epoch(&self) -> ReadLockHold {
        lock(&self.state).epoch
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::Sync));
        if let Err(err) = state.faults.take(SourceOp::Sync) {
            if err == SourceError::FsyncFailed {
                state.app_unsynced_error = true;
            }
            let salts = state.index.salts;
            if err == SourceError::FsyncFailed
                && state
                    .lost_writeback
                    .as_ref()
                    .is_none_or(|lost| lost.salts != salts)
            {
                let first = state
                    .wal
                    .iter()
                    .zip(state.durable.iter().chain(std::iter::repeat(&0)))
                    .position(|(now, was)| now != was);
                if let Some(first) = first {
                    let range = (state.index.salts, as_u64(first), as_u64(state.wal.len()));
                    state.lost_ranges.push(range);
                }
                state.lost_writeback = Some(LostWriteback {
                    salts: state.index.salts,
                    durable: state.durable.clone(),
                    wal: state.wal.clone(),
                });
            }
            return Err(err);
        }
        state.begin(Conn::Ours);
        if state.unlinked {
            return Err(SourceError::Unlinked);
        }
        if !state.our_sync_lies {
            state.sync_wal();
        }
        Ok(())
    }

    /// The rule model keeps no file times: the world stamps them on the run's clock
    /// (`crate::world::World::note_writes`) and the engine's source answers from there.
    fn modified(&mut self) -> Result<Option<Timestamp>, SourceError> {
        Ok(None)
    }
}

/// An open WAL file: reads see the file as it is at each read, as a file
/// descriptor does.
#[derive(Debug)]
pub struct SimWal {
    state: Arc<Mutex<State>>,
    pos: u64,
}

impl Read for SimWal {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = lock(&self.state);
        state.app_call(None);
        let start = byte_pos(self.pos);
        let end = start.saturating_add(buf.len()).min(state.wal.len());
        if start >= end {
            return Ok(0);
        }
        state.on_read(start, end);
        let src = state.wal.get(start..end).unwrap_or_default();
        let n = src.len();
        if let Some(dst) = buf.get_mut(..n) {
            dst.copy_from_slice(src);
        }
        self.pos = self
            .pos
            .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        Ok(n)
    }
}

impl Seek for SimWal {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let len = u64::try_from(lock(&self.state).wal.len()).unwrap_or(u64::MAX);
        let pos = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(delta) => len.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.pos.checked_add_signed(delta),
        };
        self.pos = pos.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok(self.pos)
    }
}

/// Our database-file handle: [`PageSource`] over the shared state.
#[derive(Clone, Debug)]
pub struct SimPages {
    state: Arc<Mutex<State>>,
}

impl PageSource for SimPages {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::PageCount));
        state.faults.take(SourceOp::PageCount)?;
        // As lsr-sqlite's `ensure_read_lock_held`: a lapsed read transaction is taken again.
        state.begin(Conn::Ours);
        if std::mem::take(&mut state.faults.empty_db) {
            return Ok(DbPages::EMPTY);
        }
        Ok(DbPages::new(state.db_pages()))
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::ReadPage));
        state.faults.take(SourceOp::ReadPage)?;
        state.begin(Conn::Ours);
        let at = state.page_at(pgno.get());
        let src = state
            .db
            .get(at..at.saturating_add(state.page_size.bytes()))
            .ok_or(SourceError::Short { pgno })?;
        buf.get_mut(..src.len())
            .ok_or(SourceError::Short { pgno })?
            .copy_from_slice(src);
        Ok(())
    }
}

/// Our checkpoint connections: [`Checkpointer`] over the shared state. Checkpoints run on our
/// read connection; the barrier and `bump_seq` on our writer connection.
#[derive(Clone, Debug)]
pub struct SimCheckpointer {
    state: Arc<Mutex<State>>,
}

impl Checkpointer for SimCheckpointer {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::BeginBarrier));
        // The application commits after the tick's sync read the committed end and before the
        // barrier keeps writers out: the commit the checkpoint's seal must stage.
        state.commit_in_tick(InTickSite::BeforeBarrier);
        state.writes_allowed()?;
        state.faults.take(SourceOp::BeginBarrier)?;
        let id = state.take_writer(Conn::Writer)?;
        state.barrier = Some(id);
        Ok(Barrier::new(id))
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::EndBarrier));
        // A ROLLBACK that fails with an I/O error still ends the transaction (SQLite rolls back
        // on the error), and lsr-sqlite spends the token either way: the barrier is gone and the
        // caller is told the release failed.
        let released = state.faults.take(SourceOp::EndBarrier);
        if state.barrier == Some(barrier.id()) {
            state.barrier = None;
            state.drop_writer(Conn::Writer);
        }
        released
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::Checkpoint));
        state.writes_allowed()?;
        // lsr-sqlite releases the read transaction before it asks SQLite and takes it again
        // after, whatever SQLite answered: a failed checkpoint still starts a new hold. A lost
        // retake ran the checkpoint and leaves the read transaction lapsed, for the next read
        // to take again.
        let injected = state.faults.take(SourceOp::Checkpoint);
        state.end(Conn::Ours);
        match injected {
            Err(SourceError::ReadTxLost) => {
                state.checkpoint(Conn::Ours, mode.into());
                return Err(SourceError::ReadTxLost);
            }
            Err(err) => {
                state.begin(Conn::Ours);
                return Err(err);
            }
            Ok(()) => {}
        }
        let failing = state.faults.truncate_fails;
        let numbers = state.checkpoint(Conn::Ours, mode.into());
        state.begin(Conn::Ours);
        if failing && !state.faults.truncate_fails {
            return Err(SourceError::Io(io::ErrorKind::Other));
        }
        Ok(CheckpointRun {
            busy: numbers.busy,
            log: numbers.log,
            checkpointed: numbers.checkpointed,
            epoch: state.epoch,
        })
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.app_call(Some(SourceOp::BumpSeq));
        state.writes_allowed()?;
        state.faults.take(SourceOp::BumpSeq)?;
        let first = PageNo::new(1, state.page_size).ok_or(SourceError::Busy)?;
        let data = state.image_page(1);
        let size = state.index.db_size.max(1);
        // The writer connection is in the barrier's transaction: the real checkpointer refuses.
        if state.barrier.is_some() {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        if state.write(Conn::Writer, &[(first, data)], Some(size)) {
            Ok(())
        } else {
            Err(SourceError::Busy)
        }
    }
}

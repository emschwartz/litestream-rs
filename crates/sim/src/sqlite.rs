//! A rule-based model of SQLite's WAL, its wal-index and its locks.
//!
//! The 4a fakes are scripted: a test says what a checkpoint does. [`SimSqlite`]
//! instead follows SQLite's rules, so a stranger's checkpoint, a writer's restart
//! and a reader's mark interact as they do in SQLite. The rules were read from the
//! bundled amalgamation (rusqlite 0.43, SQLite 3.54.1: `walTryBeginRead`,
//! `walCheckpoint`, `sqlite3WalCheckpoint`, `walRestartLog`, `walFrames`) and are
//! checked against it by `tests/sqlite_rules.rs`:
//!
//! - **R1, backfill:** a checkpoint copies each page's newest frame when that frame is at
//!   or below the smallest mark above 0 held by any reader (a page rewritten above
//!   it is not copied at all), and nothing while a reader holds mark 0 with
//!   `nBackfill < mxFrame`. RESTART and TRUNCATE are busy unless everything is
//!   backfilled and no reader holds a mark above 0; they need the write lock and
//!   degrade to PASSIVE, busy, when another connection holds it.
//! - **R2, restart:** a writer starting a transaction on mark 0 with
//!   `nBackfill > 0` and no reader on a mark above 0 restarts the WAL: salt1 + 1,
//!   salt2 new, frames from 1. A TRUNCATE that finishes restarts the same way and
//!   empties the file; a RESTART that finishes changes nothing on disk, and the
//!   next writer restarts. The header's checkpoint sequence is the *writing
//!   connection's* checkpoint count (`Wal.nCkpt`: its restarts, plus the header's
//!   value when it ran recovery), and a connection whose count is 0 writes both
//!   salts random.
//! - **R3, mark choice:** a read transaction takes mark 0 when
//!   `nBackfill == mxFrame`, else mark `mxFrame`. A writer that held mark 0
//!   takes mark `mxFrame` (after any restart) before its first frame.
//!
//! The connections are the app (commits, spills, a stranger's checkpoints), our
//! read connection, our pool connection (checkpoints and `bump_seq`), the
//! barrier and write-lock transactions, and stranger readers. Only the app and the
//! pool write frames or checkpoint, so only they carry a checkpoint count.
//!
//! Durability follows `synchronous=NORMAL`: a commit is not fsynced; the WAL is
//! fsynced when a header is written (frame 1 of a generation) and before a
//! checkpoint copies frames, and a checkpoint's database writes are durable. A
//! power crash keeps the WAL as of its last fsync and runs SQLite's recovery.
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
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, ReadEpoch,
    SourceError, SourceOp, WalFileLen, WalSource, WriteLock,
};
use lsr_ltx::{PageNo, PageSize};
use lsr_wal::testing::{WalBuilder, page};
use lsr_wal::{
    ByteOrder, CommittedEnd, FRAME_HEADER_LEN, HEADER_LEN, Salts, ScanLimit, TxOutcome,
    WalChecksum, WalHeader, WalOffset, WalReader,
};

#[cfg(test)]
mod tests;

const MAGIC_LITTLE: u32 = 0x377f_0682;
const FORMAT_VERSION: u32 = 3_007_000;
const ORDER: ByteOrder = ByteOrder::Little;
/// Bytes of a frame header that the running checksum covers.
const CHECKSUMMED_FRAME_HEADER_LEN: usize = 8;

/// A stranger's read transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReaderId(u32);

/// A checkpoint mode as another process can ask for it (core's
/// [`CheckpointMode`] has no RESTART).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrangerMode {
    /// `PRAGMA wal_checkpoint(PASSIVE)`.
    Passive,
    /// `PRAGMA wal_checkpoint(RESTART)`.
    Restart,
    /// `PRAGMA wal_checkpoint(TRUNCATE)`.
    Truncate,
}

impl From<CheckpointMode> for StrangerMode {
    fn from(mode: CheckpointMode) -> Self {
        match mode {
            CheckpointMode::Passive => Self::Passive,
            CheckpointMode::Truncate => Self::Truncate,
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
    /// Our process dies: our read transaction, barrier, write lock and pool
    /// connection vanish; the files, the wal-index and the app stay.
    Process,
    /// The machine loses power: every connection vanishes, the WAL is what was
    /// last fsynced, and the app's next open runs SQLite's recovery.
    Power,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Conn {
    App,
    Ours,
    Pool,
    Barrier,
    Lock,
    Stranger(ReaderId),
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
}

#[derive(Debug, Default)]
struct Faults {
    injections: Vec<(SourceOp, SourceError)>,
    shm_busy: bool,
    shm_forced: Option<CommittedRead>,
    overwrite: Option<Overwrite>,
}

impl Faults {
    fn take(&mut self, op: SourceOp) -> Result<(), SourceError> {
        match self.injections.iter().position(|(queued, _)| *queued == op) {
            Some(at) => Err(self.injections.remove(at).1),
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
struct State {
    page_size: PageSize,
    db: Vec<u8>,
    wal: Vec<u8>,
    // The WAL as of its last fsync.
    durable: Vec<u8>,
    present: bool,
    unlinked: bool,
    index: Index,
    readers: BTreeMap<Conn, Mark>,
    writer: Option<Conn>,
    app_checkpoints: u32,
    pool_checkpoints: u32,
    rng: u64,
    // When set, the values SQLite would draw at random are these instead.
    forced: Option<Salts>,
    epoch: ReadEpoch,
    next_reader: u32,
    next_token: u64,
    barrier: Option<u64>,
    write_lock: Option<u64>,
    faults: Faults,
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
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

    /// R3.
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
            Conn::Pool => Some(&mut self.pool_checkpoints),
            Conn::Ours | Conn::Barrier | Conn::Lock | Conn::Stranger(_) => None,
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

    /// The start of a write transaction by `by`: R2, then R3's writer mark.
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

    /// Copies into the database file the pages whose newest frame is in `(backfilled, upto]`.
    fn copy_frames(&mut self, upto: u32) {
        self.durable.clone_from(&self.wal);
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
        if upto == self.index.mx_frame {
            let len = self.page_at(self.index.db_size.saturating_add(1));
            self.db.resize(len, 0);
        }
        self.index.backfilled = upto;
    }

    /// R1's backfill step of `walCheckpoint`.
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
    fn checkpoint(&mut self, by: Conn, mode: StrangerMode) -> CheckpointNumbers {
        let degraded = mode != StrangerMode::Passive && self.writer.is_some_and(|w| w != by);
        let mut busy = degraded;
        self.backfill();
        if !degraded && mode != StrangerMode::Passive {
            if self.index.backfilled < self.index.mx_frame || self.slot_held_by_other(by) {
                busy = true;
            } else if mode == StrangerMode::Truncate {
                self.restart(by);
                self.wal.clear();
                self.durable.clear();
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
        let newest = self
            .index
            .pgnos
            .iter()
            .take(byte_pos(u64::from(self.index.mx_frame)))
            .rposition(|frame| *frame == pgno)
            .and_then(|at| u32::try_from(at).ok());
        newest.map_or_else(
            || {
                let at = self.page_at(pgno);
                let mut data = self
                    .db
                    .get(at..at.saturating_add(self.page_size.bytes()))
                    .unwrap_or_default()
                    .to_vec();
                data.resize(self.page_size.bytes(), 0);
                data
            },
            |index| self.frame_page(index),
        )
    }

    /// SQLite's `walIndexRecover` over the file, by the app's connection.
    fn recover(&mut self) {
        let mut index = Index::empty(self.index.salts, self.db_pages());
        self.app_checkpoints = 0;
        self.pool_checkpoints = 0;
        let bytes = mem::take(&mut self.wal);
        if let Ok(mut reader) = WalReader::open(Cursor::new(bytes.as_slice()))
            && reader.header().page_size() == self.page_size
        {
            index.salts = reader.header().salts();
            self.app_checkpoints = reader.header().checkpoint_seq();
            let mut cursor = reader.cursor_at_start();
            while let Ok(TxOutcome::Tx(tx)) = reader.next_tx(&cursor, ScanLimit::FirstInvalidFrame)
            {
                index
                    .pgnos
                    .extend(tx.frames().iter().map(|frame| frame.pgno().get()));
                index.db_size = tx.commit();
                cursor = *tx.end();
                index.tx_ends.push(cursor.next().frame_index());
            }
            index.mx_frame = cursor.next().frame_index();
            index.written = index.mx_frame;
            index.running = cursor.checksum();
            index.committed = cursor.checksum();
        }
        self.wal = bytes;
        self.index = index;
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
        self.wal.clear();
        self.durable.clear();
        self.index = Index::empty(self.index.salts, self.index.db_size);
        self.app_checkpoints = 0;
        self.pool_checkpoints = 0;
        true
    }

    fn token(&mut self) -> u64 {
        self.next_token = self.next_token.saturating_add(1);
        self.next_token
    }

    fn take_writer(&mut self, conn: Conn) -> Result<u64, SourceError> {
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
        for conn in [Conn::Barrier, Conn::Lock, Conn::Pool] {
            self.drop_writer(conn);
        }
        self.barrier = None;
        self.write_lock = None;
        self.pool_checkpoints = 0;
    }

    fn power_crash(&mut self) {
        self.process_crash();
        self.readers.clear();
        self.writer = None;
        self.faults.shm_busy = false;
        self.faults.shm_forced = None;
        self.faults.overwrite = None;
        if self.unlinked {
            self.unlinked = false;
            self.present = false;
            self.durable.clear();
        }
        self.wal.clone_from(&self.durable);
        self.recover();
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
        if start >= page_end || end <= page {
            return;
        }
        armed.reads = armed.reads.saturating_add(1);
        self.faults.overwrite = Some(armed);
        if armed.reads >= 2 {
            self.faults.overwrite = None;
            if let Some(bytes) = self.wal.get_mut(page..page_end) {
                bytes.iter_mut().for_each(|byte| *byte ^= 0xff);
            }
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
            db,
            wal: Vec::new(),
            durable: Vec::new(),
            present: true,
            unlinked: false,
            index: Index::empty(Salts { salt1: 0, salt2: 0 }, db_pages),
            readers: BTreeMap::new(),
            writer: None,
            app_checkpoints: 0,
            pool_checkpoints: 0,
            rng: seed,
            forced: None,
            epoch: ReadEpoch::new(1),
            next_reader: 0,
            next_token: 0,
            barrier: None,
            write_lock: None,
            faults: Faults::default(),
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
    /// gets one frame so the pin lands above mark 0, then the read transaction
    /// that holds the WAL in place begins. Any earlier pin ended with its process.
    pub fn start_driver(&self) {
        self.with(|state| {
            state.end(Conn::Ours);
            if state.wal.len() < 32
                && let Some(first) = PageNo::new(1, state.page_size)
            {
                let data = state.image_page(1);
                let size = state.index.db_size.max(1);
                state.write(Conn::Pool, &[(first, data)], Some(size));
            }
            state.begin(Conn::Ours);
        });
    }

    /// Begins our read transaction under R3; false if it is held.
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
        self.with(|state| {
            if db_size == 0 || !state.write(Conn::App, pages, Some(db_size)) {
                return None;
            }
            Some(AppCommit {
                salts: state.index.salts,
                end: WalOffset::from_frame_index(state.index.mx_frame, state.page_size),
            })
        })
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
            state.index.db_size = state.db_pages();
            state.present = true;
            true
        })
    }

    /// A stranger begins a read transaction under R3.
    pub fn stranger_reader_begin(&self) -> ReaderId {
        self.with(|state| {
            let id = ReaderId(state.next_reader);
            state.next_reader = state.next_reader.saturating_add(1);
            state.begin(Conn::Stranger(id));
            id
        })
    }

    /// The stranger's read transaction ends; false if it was not held.
    pub fn stranger_reader_end(&self, id: ReaderId) -> bool {
        self.with(|state| state.end(Conn::Stranger(id)))
    }

    /// `PRAGMA wal_checkpoint(mode)` on the app's connection; `None` while the
    /// app is in a transaction (SQLite says LOCKED).
    pub fn stranger_checkpoint(&self, mode: StrangerMode) -> Option<CheckpointNumbers> {
        self.with(|state| {
            let in_tx = state.writer == Some(Conn::App) || state.readers.contains_key(&Conn::App);
            (!in_tx).then(|| state.checkpoint(Conn::App, mode))
        })
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
            regrow(state, header.checksum(), seed);
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
            state.faults.shm_forced = CommittedRead::new(end, index.backfilled.min(frames));
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
            state.faults.overwrite = Some(Overwrite { frame, reads: 0 });
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

    /// The next call of `op` fails with `err`.
    pub fn inject(&self, op: SourceOp, err: SourceError) {
        self.with(|state| state.faults.injections.push((op, err)));
    }

    /// A crash.
    pub fn crash(&self, kind: CrashKind) {
        self.with(|state| match kind {
            CrashKind::Process => state.process_crash(),
            CrashKind::Power => state.power_crash(),
        });
    }

    /// The database as a new reader sees it, `db_size` pages.
    #[must_use]
    pub fn image(&self) -> Vec<u8> {
        self.with(|state| {
            (1..=state.index.db_size)
                .flat_map(|pgno| state.image_page(pgno))
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

fn regrow(state: &mut State, header: WalChecksum, seed: u64) {
    let frames = state.index.mx_frame;
    let ends = state.index.tx_ends.clone();
    let pgnos = state.index.pgnos.clone();
    state.index.written = 0;
    state.index.running = header;
    state.index.pgnos.clear();
    let size = state.index.db_size;
    for (at, pgno) in (1..=frames).zip(pgnos) {
        let commit = if ends.contains(&at) { size } else { 0 };
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
        state.faults.take(SourceOp::Len)?;
        if state.unlinked {
            return Err(SourceError::Unlinked);
        }
        let len = u64::try_from(state.wal.len()).unwrap_or(u64::MAX);
        Ok(state.present.then(|| WalFileLen::new(len)))
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::Committed)?;
        if mem::take(&mut state.faults.shm_busy) {
            return Err(SourceError::Busy);
        }
        if let Some(read) = state.faults.shm_forced.take() {
            return Ok(read);
        }
        let index = &state.index;
        let end = CommittedEnd::new(index.mx_frame, index.salts, state.page_size);
        CommittedRead::new(end, index.backfilled).ok_or(SourceError::Busy)
    }

    fn epoch(&self) -> ReadEpoch {
        lock(&self.state).epoch
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
    fn page_count(&mut self) -> Result<u32, SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::PageCount)?;
        Ok(state.db_pages())
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::ReadPage)?;
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

/// Our checkpoint connections: [`Checkpointer`] over the shared state. The
/// barrier and the write lock are transactions of their own; checkpoints and
/// `bump_seq` run on the pool connection.
#[derive(Clone, Debug)]
pub struct SimCheckpointer {
    state: Arc<Mutex<State>>,
}

impl Checkpointer for SimCheckpointer {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::BeginBarrier)?;
        let id = state.take_writer(Conn::Barrier)?;
        state.barrier = Some(id);
        Ok(Barrier::new(id))
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::EndBarrier)?;
        if state.barrier == Some(barrier.id()) {
            state.barrier = None;
            state.drop_writer(Conn::Barrier);
        }
        Ok(())
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::Checkpoint)?;
        state.end(Conn::Ours);
        let numbers = state.checkpoint(Conn::Pool, mode.into());
        state.begin(Conn::Ours);
        Ok(CheckpointRun {
            busy: numbers.busy,
            log: numbers.log,
            checkpointed: numbers.checkpointed,
            epoch: state.epoch,
        })
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::BumpSeq)?;
        let first = PageNo::new(1, state.page_size).ok_or(SourceError::Busy)?;
        let data = state.image_page(1);
        let size = state.index.db_size.max(1);
        if state.write(Conn::Pool, &[(first, data)], Some(size)) {
            Ok(())
        } else {
            Err(SourceError::Busy)
        }
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::BeginWriteLock)?;
        let id = state.take_writer(Conn::Lock)?;
        state.write_lock = Some(id);
        Ok(WriteLock::new(id))
    }

    fn end_write_lock(&mut self, lock_held: WriteLock) -> Result<(), SourceError> {
        let mut state = lock(&self.state);
        state.faults.take(SourceOp::EndWriteLock)?;
        if state.write_lock == Some(lock_held.id()) {
            state.write_lock = None;
            state.drop_writer(Conn::Lock);
        }
        Ok(())
    }
}

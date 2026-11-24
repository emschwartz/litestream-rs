//! In-memory sources for tests and the simulator.
//!
//! [`FakeWal`] models the two SQLite facts that make a WAL restart harmless under
//! a pinned read transaction (measured on SQLite 3.47.1), because the
//! sync engine's safety argument stands on them:
//!
//! 1. A read mark above 0 blocks every restart. Salts never change.
//! 2. Under read mark 0 the next write restarts the WAL once (salt1 + 1,
//!    checkpoint_seq + 1), and the new generation can be neither backfilled nor
//!    restarted while the read lock holds. An old generation that grows under a read-mark-0
//!    read lock is never backfilled, so it is never restarted either.
//!
//! The model is SQLite's own rule: a writer restarts only when it sees a fully
//! backfilled WAL (it reads on read mark 0) and no reader holds a mark above 0. Frames
//! written past the last commit (a spill) are not in `mxFrame`, so a rolled-back
//! spill does not stop the restart and its bytes stay in the file past the new end.
//! A reader on read mark 0 holds back the checkpoint's safe frame at 0, which is why
//! nothing is backfilled under it.
//!
//! A restart rewrites the header in place and leaves the old frames' bytes in
//! the file, as SQLite does; a truncating checkpoint empties the file.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_ltx::{DbPages, PageNo, PageSize, Timestamp};
use lsr_wal::{
    ByteOrder, CommittedEnd, FRAME_HEADER_LEN, HEADER_LEN, Salts, WalChecksum, WalOffset,
};

use super::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, SourceError,
    WalSource,
};
use crate::position::{ReadLockHold, WalFileLen};

const MAGIC_LITTLE: u32 = 0x377f_0682;
const FORMAT_VERSION: u32 = 3_007_000;
const ORDER: ByteOrder = ByteOrder::Little;

/// A source operation an error can be injected into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceOp {
    /// `WalSource::open`.
    Open,
    /// `WalSource::len`.
    Len,
    /// `WalSource::committed`.
    Committed,
    /// `PageSource::page_count`.
    PageCount,
    /// `PageSource::read_page`.
    ReadPage,
    /// `Checkpointer::begin_barrier`.
    BeginBarrier,
    /// `Checkpointer::end_barrier`.
    EndBarrier,
    /// `Checkpointer::checkpoint`.
    Checkpoint,
    /// `Checkpointer::bump_seq`.
    BumpSeq,
    /// The write lock Go takes for a TRUNCATE (`db.go:2759-2771`). Nothing calls it: the
    /// engine's copy after a TRUNCATE takes no lock (docs/decisions/D190.md), and the
    /// `Checkpointer` trait no longer has the method. Kept so recorded histories still read.
    BeginWriteLock,
    /// The release of that write lock (Go `db.go:2795`). Nothing calls it either.
    EndWriteLock,
    /// `WalSource::sync`.
    Sync,
}

/// One-shot errors, consumed oldest first by the next call of their operation.
#[derive(Debug, Default)]
struct Injections(Vec<(SourceOp, SourceError)>);

impl Injections {
    fn take(&mut self, op: SourceOp) -> Result<(), SourceError> {
        match self.0.iter().position(|(queued, _)| *queued == op) {
            Some(at) => Err(self.0.remove(at).1),
            None => Ok(()),
        }
    }
}

fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn byte_pos(offset: WalOffset) -> usize {
    usize::try_from(offset.get()).unwrap_or(usize::MAX)
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

#[derive(Debug)]
struct WalState {
    bytes: Vec<u8>,
    present: bool,
    page_size: PageSize,
    salts: Salts,
    checkpoint_seq: u32,
    // Checksum after the last frame written, the next frame's seed.
    running: WalChecksum,
    written: u32,
    mx_frame: u32,
    backfilled: u32,
    db_size: u32,
    // The read mark our own reader holds; None when it holds no read transaction.
    read_mark: Option<u32>,
    epoch: ReadLockHold,
    restarts: u32,
    forced: Option<CommittedRead>,
    /// A commit that lands right before the `committed` read the count says (1 is the next):
    /// a writer committing mid-tick.
    armed: Option<(u32, Armed)>,
    injections: Injections,
    /// The pages backfills copied into the database file, read from the WAL's bytes as they
    /// stood at the copy (an evicted frame's older bytes included), and the size the last copied
    /// commit gave it. A [`FakeDb`] linked to this WAL reads them.
    copied: BTreeMap<u32, Vec<u8>>,
    copied_size: u32,
    /// What [`WalSource::modified`] answers: set by the test, `None` until it is.
    modified: Option<Timestamp>,
}

/// A transaction armed to commit at a later read.
#[derive(Debug)]
struct Armed {
    pages: Vec<(PageNo, Vec<u8>)>,
    db_size: NonZeroU32,
}

impl WalState {
    fn header(&self) -> Vec<u8> {
        let mut head = Vec::with_capacity(HEADER_LEN);
        for word in [
            MAGIC_LITTLE,
            FORMAT_VERSION,
            self.page_size.get(),
            self.checkpoint_seq,
            self.salts.salt1,
            self.salts.salt2,
        ] {
            head.extend_from_slice(&word.to_be_bytes());
        }
        let sum = WalChecksum::ZERO
            .extend(ORDER, &head)
            .unwrap_or(WalChecksum::ZERO);
        head.extend_from_slice(&sum.s0().to_be_bytes());
        head.extend_from_slice(&sum.s1().to_be_bytes());
        head
    }

    fn write_header(&mut self) {
        let head = self.header();
        write_at(&mut self.bytes, 0, &head);
        self.running = head
            .get(..24)
            .and_then(|covered| WalChecksum::ZERO.extend(ORDER, covered))
            .unwrap_or(WalChecksum::ZERO);
    }

    fn start_generation(&mut self) {
        self.salts = Salts {
            salt1: self.salts.salt1.wrapping_add(1),
            salt2: self
                .salts
                .salt2
                .wrapping_mul(1_664_525)
                .wrapping_add(1_013_904_223),
        };
        self.checkpoint_seq = self.checkpoint_seq.wrapping_add(1);
        self.written = 0;
        self.mx_frame = 0;
        self.backfilled = 0;
        self.restarts = self.restarts.saturating_add(1);
    }

    fn restart_in_place(&mut self) {
        self.start_generation();
        self.write_header();
    }

    fn restart_is_due(&self) -> bool {
        self.mx_frame > 0
            && self.backfilled == self.mx_frame
            && self.read_mark.is_none_or(|mark| mark == 0)
    }

    fn append(&mut self, pages: &[(PageNo, Vec<u8>)], db_size: Option<NonZeroU32>) {
        if self.restart_is_due() {
            self.restart_in_place();
        }
        if self.bytes.is_empty() {
            self.write_header();
        }
        let last = pages.len().saturating_sub(1);
        for (index, (pgno, data)) in pages.iter().enumerate() {
            let mut page = data.clone();
            page.resize(self.page_size.bytes(), 0);
            let commit = if index == last {
                db_size.map_or(0, NonZeroU32::get)
            } else {
                0
            };
            let mut head = Vec::with_capacity(FRAME_HEADER_LEN);
            for word in [pgno.get(), commit, self.salts.salt1, self.salts.salt2] {
                head.extend_from_slice(&word.to_be_bytes());
            }
            let sum = head
                .get(..8)
                .and_then(|covered| self.running.extend(ORDER, covered))
                .and_then(|sum| sum.extend(ORDER, &page))
                .unwrap_or(self.running);
            head.extend_from_slice(&sum.s0().to_be_bytes());
            head.extend_from_slice(&sum.s1().to_be_bytes());
            head.extend_from_slice(&page);
            let at = WalOffset::from_frame_index(self.written, self.page_size);
            write_at(&mut self.bytes, byte_pos(at), &head);
            self.running = sum;
            self.written = self.written.saturating_add(1);
        }
        if let Some(size) = db_size {
            self.mx_frame = self.written;
            self.db_size = size.get();
        }
    }

    // SQLite's safe frame: no reader needs frames above its mark.
    fn backfill(&mut self, upto: u32) -> u32 {
        let safe = self
            .read_mark
            .map_or(self.mx_frame, |mark| mark.min(self.mx_frame));
        let new = self.backfilled.max(upto.min(safe));
        for index in self.backfilled..new {
            self.copy_frame(index);
        }
        self.backfilled = new;
        self.backfilled
    }

    /// Copies frame `index` (0-based) into the database file as SQLite's backfill does: the
    /// page as the WAL's bytes hold it now, unverified.
    fn copy_frame(&mut self, index: u32) {
        let at = byte_pos(WalOffset::from_frame_index(index, self.page_size));
        let page_at = at.saturating_add(FRAME_HEADER_LEN);
        let header = self.bytes.get(at..page_at);
        let word = |n: usize| {
            header
                .and_then(|h| h.get(n..n.saturating_add(4)))
                .and_then(|w| <[u8; 4]>::try_from(w).ok())
                .map_or(0, u32::from_be_bytes)
        };
        let (pgno, commit) = (word(0), word(4));
        let page = self
            .bytes
            .get(page_at..page_at.saturating_add(self.page_size.bytes()))
            .map(<[u8]>::to_vec);
        if let Some(page) = page {
            self.copied.insert(pgno, page);
        }
        if commit != 0 {
            self.copied.retain(|n, _| *n <= commit);
            self.copied_size = commit;
        }
    }

    fn take_read_lock(&mut self) {
        self.read_mark = Some(if self.backfilled == self.mx_frame {
            0
        } else {
            self.mx_frame
        });
    }

    fn end(&self) -> CommittedEnd {
        CommittedEnd::new(self.mx_frame, self.salts, self.page_size)
    }
}

/// An in-memory WAL file and wal-index that behaves as SQLite's does under a read mark.
///
/// A cheap handle: clones share one state, so a [`FakeCheckpointer`] and a test
/// drive the same WAL the engine reads.
#[derive(Clone, Debug)]
pub struct FakeWal {
    state: Arc<Mutex<WalState>>,
}

impl FakeWal {
    /// An empty WAL of the given generation: header written, no frames, no read transaction.
    #[must_use]
    pub fn new(page_size: PageSize, salts: Salts) -> Self {
        let mut state = WalState {
            bytes: Vec::new(),
            present: true,
            page_size,
            salts,
            checkpoint_seq: 0,
            running: WalChecksum::ZERO,
            written: 0,
            mx_frame: 0,
            backfilled: 0,
            db_size: 1,
            read_mark: None,
            epoch: ReadLockHold::first(),
            restarts: 0,
            forced: None,
            armed: None,
            injections: Injections::default(),
            copied: BTreeMap::new(),
            copied_size: 0,
            modified: None,
        };
        state.write_header();
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Sets when the database was last written, as [`WalSource::modified`] answers it.
    pub fn set_modified(&self, at: Option<Timestamp>) {
        lock(&self.state).modified = at;
    }

    /// Writes one committed transaction (each page zero-padded or cut to the page
    /// size, the last frame carrying `db_size`). A writer that finds the WAL fully
    /// backfilled and no read mark above 0 restarts it first, as SQLite does.
    pub fn append_tx(&self, pages: &[(PageNo, Vec<u8>)], db_size: NonZeroU32) -> CommittedEnd {
        let mut state = lock(&self.state);
        state.append(pages, Some(db_size));
        state.end()
    }

    /// Writes one committed transaction right before the `reads`-th `committed` read from
    /// now (1 is the next): a writer that commits while a tick runs, after the reads before it.
    pub fn append_tx_before_read(
        &self,
        reads: u32,
        pages: &[(PageNo, Vec<u8>)],
        db_size: NonZeroU32,
    ) {
        lock(&self.state).armed = Some((
            reads,
            Armed {
                pages: pages.to_vec(),
                db_size,
            },
        ));
    }

    /// Writes frames of a transaction that has not committed (a spill): they are in
    /// the file, below no committed end.
    pub fn append_uncommitted(&self, pages: &[(PageNo, Vec<u8>)]) {
        lock(&self.state).append(pages, None);
    }

    /// Copies frames into the database file, at most up to `upto` and never past
    /// what our own read mark allows (a reader on read mark 0 allows none). Returns
    /// `nBackfill` afterwards.
    pub fn backfill(&self, upto: u32) -> u32 {
        lock(&self.state).backfill(upto)
    }

    /// Restarts the WAL if SQLite would: fully backfilled, no write in progress, no
    /// reader above read mark 0. Returns whether it did.
    pub fn try_restart(&self) -> bool {
        let mut state = lock(&self.state);
        let due = state.restart_is_due();
        if due {
            state.restart_in_place();
        }
        due
    }

    /// Restarts the WAL unconditionally: what an external checkpoint that ran
    /// while we held no read transaction leaves behind.
    pub fn force_restart(&self) {
        lock(&self.state).restart_in_place();
    }

    /// Writes the header again with the same salts and checkpoint sequence `seq`, so another
    /// chain seed: what SQLite's next writer does to a generation whose wal-index holds no frame,
    /// with its own connection's checkpoint count. Only an empty generation can see it, so
    /// frames written since are dropped from the index and the file.
    pub fn rewrite_header(&self, seq: u32) {
        let mut state = lock(&self.state);
        state.checkpoint_seq = seq;
        state.written = 0;
        state.mx_frame = 0;
        state.backfilled = 0;
        state.write_header();
    }

    /// Takes the read transaction the way SQLite picks a mark: 0 when the WAL is
    /// fully backfilled, the frame count otherwise.
    pub fn hold_reader(&self) {
        lock(&self.state).take_read_lock();
    }

    /// Takes the read transaction on a chosen mark.
    pub fn hold_reader_at(&self, mark: u32) {
        lock(&self.state).read_mark = Some(mark);
    }

    /// Releases the read transaction.
    pub fn release_reader(&self) {
        lock(&self.state).read_mark = None;
    }

    /// The mark our reader holds, if any.
    #[must_use]
    pub fn read_mark(&self) -> Option<u32> {
        lock(&self.state).read_mark
    }

    /// The epoch `WalSource::epoch` reports.
    #[must_use]
    pub fn epoch(&self) -> ReadLockHold {
        lock(&self.state).epoch
    }

    /// Sets the epoch `WalSource::epoch` reports.
    pub fn set_epoch(&self, epoch: ReadLockHold) {
        lock(&self.state).epoch = epoch;
    }

    /// Answers `committed` with this value, whatever the file holds; `None` goes
    /// back to the truth. For wal-index states the file cannot show (an end
    /// inside a transaction).
    pub fn script_committed(&self, read: Option<CommittedRead>) {
        lock(&self.state).forced = read;
    }

    /// Deletes the WAL file.
    pub fn remove_file(&self) {
        let mut state = lock(&self.state);
        state.present = false;
        state.bytes.clear();
    }

    /// Cuts the file to `len` bytes, as an external truncate does.
    pub fn truncate_file(&self, len: usize) {
        lock(&self.state).bytes.truncate(len);
    }

    /// A power loss that keeps the first `keep` frames of the generation,
    /// which must end on a commit no checkpoint has passed: what SQLite's
    /// recovery leaves when the frames after them never reached the disk.
    /// The next transaction is written at frame `keep`, chained to it.
    pub fn lose_frames_from(&self, keep: u32) {
        let mut state = lock(&self.state);
        let at = byte_pos(WalOffset::from_frame_index(keep, state.page_size));
        state.bytes.truncate(at);
        let frame = at.saturating_sub(FRAME_HEADER_LEN.saturating_add(state.page_size.bytes()));
        let word = |bytes: &[u8], field: usize| {
            let from = frame.saturating_add(field);
            bytes
                .get(from..from.saturating_add(4))
                .and_then(|b| b.try_into().ok())
                .map_or(0, u32::from_be_bytes)
        };
        if keep == 0 {
            state.write_header();
        } else {
            state.running = WalChecksum::new(word(&state.bytes, 16), word(&state.bytes, 20));
            state.db_size = word(&state.bytes, 4);
        }
        state.written = keep;
        state.mx_frame = keep;
        state.backfilled = state.backfilled.min(keep);
    }

    /// Flips every bit of the byte at `at`.
    pub fn corrupt_byte(&self, at: usize) {
        if let Some(byte) = lock(&self.state).bytes.get_mut(at) {
            *byte ^= 0xff;
        }
    }

    /// The file's bytes.
    #[must_use]
    pub fn file_bytes(&self) -> Vec<u8> {
        lock(&self.state).bytes.clone()
    }

    /// The current generation's salts.
    #[must_use]
    pub fn salts(&self) -> Salts {
        lock(&self.state).salts
    }

    /// The header's checkpoint sequence.
    #[must_use]
    pub fn checkpoint_seq(&self) -> u32 {
        lock(&self.state).checkpoint_seq
    }

    /// How many times the WAL has restarted.
    #[must_use]
    pub fn restarts(&self) -> u32 {
        lock(&self.state).restarts
    }

    /// The frames SQLite has copied into the database file.
    #[must_use]
    pub fn backfilled(&self) -> u32 {
        lock(&self.state).backfilled
    }

    /// The committed end now.
    #[must_use]
    pub fn committed_end(&self) -> CommittedEnd {
        lock(&self.state).end()
    }

    /// Fails the next call of `op` with `err`, once.
    pub fn inject(&self, op: SourceOp, err: SourceError) {
        lock(&self.state).injections.0.push((op, err));
    }

    fn checkpoint_run(
        &self,
        mode: CheckpointMode,
        script: Option<CheckpointScript>,
    ) -> CheckpointRun {
        let mut state = lock(&self.state);
        state.read_mark = None;
        let effect = script.map_or(CheckpointEffect::Natural, |s| s.effect);
        let busy = script.is_some_and(|s| s.busy);
        match effect {
            CheckpointEffect::Natural => {
                let full = state.mx_frame;
                state.backfill(full);
                if matches!(mode, CheckpointMode::Truncate) && !busy {
                    state.restart_in_place();
                    state.bytes.clear();
                }
            }
            CheckpointEffect::Backfill(frames) => {
                state.backfill(frames);
            }
            CheckpointEffect::Restart => {
                let full = state.mx_frame;
                state.backfill(full);
                state.restart_in_place();
            }
            CheckpointEffect::Nothing => {}
        }
        let log = state.mx_frame;
        let checkpointed = state.backfilled;
        state.epoch = state.epoch.next();
        state.take_read_lock();
        CheckpointRun {
            busy,
            log,
            checkpointed,
            epoch: state.epoch,
        }
    }
}

impl WalSource for FakeWal {
    type Wal = Cursor<Vec<u8>>;

    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError> {
        let mut state = lock(&self.state);
        state.injections.take(SourceOp::Open)?;
        Ok(state.present.then(|| Cursor::new(state.bytes.clone())))
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        let mut state = lock(&self.state);
        state.injections.take(SourceOp::Len)?;
        Ok(state
            .present
            .then(|| WalFileLen::new(u64::try_from(state.bytes.len()).unwrap_or(u64::MAX))))
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        let mut state = lock(&self.state);
        state.injections.take(SourceOp::Committed)?;
        if let Some((reads, armed)) = state.armed.take() {
            match reads.checked_sub(1).filter(|left| *left > 0) {
                Some(left) => state.armed = Some((left, armed)),
                None => state.append(&armed.pages, Some(armed.db_size)),
            }
        }
        if let Some(read) = state.forced {
            return Ok(read);
        }
        CommittedRead::new(state.end(), state.backfilled, state.epoch).ok_or(SourceError::Busy)
    }

    fn epoch(&self) -> ReadLockHold {
        lock(&self.state).epoch
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        lock(&self.state).injections.take(SourceOp::Sync)
    }

    fn modified(&mut self) -> Result<Option<Timestamp>, SourceError> {
        Ok(lock(&self.state).modified)
    }
}

/// What a scripted checkpoint does to the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointEffect {
    /// What SQLite does with no other reader: backfill everything, and for
    /// `Truncate` restart and empty the file.
    Natural,
    /// Backfill up to this many frames and change nothing else.
    Backfill(u32),
    /// Backfill everything and restart (new salts, `salt1 + 1`, `checkpoint_seq + 1`).
    Restart,
    /// Change nothing.
    Nothing,
}

/// One scripted checkpoint outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointScript {
    /// What the run reports as `busy`.
    pub busy: bool,
    /// What it does to the WAL.
    pub effect: CheckpointEffect,
}

/// A call the checkpointer received.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointCall {
    /// `begin_barrier`.
    BeginBarrier,
    /// `end_barrier`.
    EndBarrier,
    /// `checkpoint` in this mode.
    Checkpoint(CheckpointMode),
    /// `bump_seq`.
    BumpSeq,
}

/// Records its calls and applies scripted checkpoint outcomes to a [`FakeWal`].
///
/// A checkpoint releases the read transaction, applies the next script (or the
/// natural effect), bumps the epoch and retakes the read transaction, as the
/// real one does.
#[derive(Debug)]
pub struct FakeCheckpointer {
    wal: FakeWal,
    calls: Vec<CheckpointCall>,
    scripts: Vec<CheckpointScript>,
    barrier_held: bool,
    next_id: u64,
}

impl FakeCheckpointer {
    /// A checkpointer over `wal`.
    #[must_use]
    pub const fn new(wal: FakeWal) -> Self {
        Self {
            wal,
            calls: Vec::new(),
            scripts: Vec::new(),
            barrier_held: false,
            next_id: 1,
        }
    }

    /// Queues the outcome of the next `checkpoint` call.
    pub fn script(&mut self, script: CheckpointScript) {
        self.scripts.push(script);
    }

    /// Every call so far, oldest first.
    #[must_use]
    pub fn calls(&self) -> &[CheckpointCall] {
        &self.calls
    }

    /// Fails the next call of `op` with `err`, once.
    pub fn inject(&self, op: SourceOp, err: SourceError) {
        self.wal.inject(op, err);
    }

    fn enter(&mut self, call: CheckpointCall, op: SourceOp) -> Result<(), SourceError> {
        self.calls.push(call);
        lock(&self.wal.state).injections.take(op)
    }

    fn fresh_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }
}

impl Checkpointer for FakeCheckpointer {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.enter(CheckpointCall::BeginBarrier, SourceOp::BeginBarrier)?;
        if self.barrier_held {
            return Err(SourceError::Busy);
        }
        self.barrier_held = true;
        Ok(Barrier::new(self.fresh_id()))
    }

    fn end_barrier(&mut self, _barrier: Barrier) -> Result<(), SourceError> {
        self.enter(CheckpointCall::EndBarrier, SourceOp::EndBarrier)?;
        self.barrier_held = false;
        Ok(())
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        self.enter(CheckpointCall::Checkpoint(mode), SourceOp::Checkpoint)?;
        let script = (!self.scripts.is_empty()).then(|| self.scripts.remove(0));
        Ok(self.wal.checkpoint_run(mode, script))
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.enter(CheckpointCall::BumpSeq, SourceOp::BumpSeq)?;
        let mut state = lock(&self.wal.state);
        let size = NonZeroU32::new(state.db_size).unwrap_or(NonZeroU32::MIN);
        if let Some(pgno) = PageNo::new(1, state.page_size) {
            state.append(&[(pgno, Vec::new())], Some(size));
        }
        Ok(())
    }
}

/// An in-memory database file. Linked to a [`FakeWal`] ([`FakeDb::link`]), it also holds what
/// that WAL's checkpoints copied: a page written here after a copy wins until the next copy.
#[derive(Debug)]
pub struct FakeDb {
    page_size: PageSize,
    pages: BTreeMap<u32, Vec<u8>>,
    count: u32,
    injections: Injections,
    wal: Option<FakeWal>,
}

impl FakeDb {
    /// An empty database.
    #[must_use]
    pub const fn new(page_size: PageSize) -> Self {
        Self {
            page_size,
            pages: BTreeMap::new(),
            count: 0,
            injections: Injections(Vec::new()),
            wal: None,
        }
    }

    /// Makes `wal`'s checkpoints write into this file, as SQLite's write into the database.
    pub fn link(&mut self, wal: &FakeWal) {
        self.wal = Some(wal.clone());
    }

    /// Writes a page (zero-padded or cut to the page size) and grows the file to hold it.
    pub fn write_page(&mut self, pgno: PageNo, data: &[u8]) {
        let mut page = data.to_vec();
        page.resize(self.page_size.bytes(), 0);
        if let Some(wal) = &self.wal {
            lock(&wal.state).copied.remove(&pgno.get());
        }
        self.pages.insert(pgno.get(), page);
        self.count = self.count.max(pgno.get());
    }

    /// The page count, with the size the linked WAL's last copied commit gave the file.
    fn count(&self) -> u32 {
        let copied = self
            .wal
            .as_ref()
            .map_or(0, |wal| lock(&wal.state).copied_size);
        self.count.max(copied)
    }

    /// Cuts or extends the file to `count` pages; pages above are dropped.
    pub fn set_page_count(&mut self, count: u32) {
        self.pages.retain(|pgno, _| *pgno <= count);
        self.count = count;
        if let Some(wal) = &self.wal {
            let mut state = lock(&wal.state);
            state.copied.retain(|pgno, _| *pgno <= count);
            state.copied_size = state.copied_size.min(count);
        }
    }

    /// Fails the next call of `op` with `err`, once.
    pub fn inject(&mut self, op: SourceOp, err: SourceError) {
        self.injections.0.push((op, err));
    }
}

impl PageSource for FakeDb {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        self.injections.take(SourceOp::PageCount)?;
        Ok(DbPages::new(self.count()))
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        self.injections.take(SourceOp::ReadPage)?;
        if pgno.get() > self.count() || buf.len() != self.page_size.bytes() {
            return Err(SourceError::Short { pgno });
        }
        let copied = self
            .wal
            .as_ref()
            .and_then(|wal| lock(&wal.state).copied.get(&pgno.get()).cloned());
        match copied.as_ref().or_else(|| self.pages.get(&pgno.get())) {
            Some(page) => buf.copy_from_slice(page),
            None => buf.fill(0),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

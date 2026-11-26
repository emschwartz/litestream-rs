//! The compactor: a contiguous run of LTX files becomes one file.
//!
//! [compact] reads up to [MAX_INPUTS] files from a source store and writes one
//! file to a destination store. Source and destination differ because level 0
//! is read from the local copy and every other level from the replica.
//!
//! Each input is read through one stream (one GET on S3, as Go's compactor opens each
//! input once), in bounded chunks, through its own decoder (the shared
//! `LtxRead`, which ends a file at its first short chunk); the pages of all
//! inputs are merged in ascending page order and the newest input holding a
//! page wins. A page above the output's commit is dropped, and the lock page
//! never appears because [PageNo] cannot hold it. Memory is one read window
//! ([`Role::COMPACT_WINDOW`]: 128 KiB from staging, at most 1 MiB from the replica, within the
//! role's [`Role::COMPACT_BUDGET`]) and one
//! page per input plus the encoder's output buffer, whatever the database size: the output page index (at most 30 bytes a page) is the only thing that
//! grows with it. Go's fan-in is unbounded (upstream #1467); a level with more
//! files than [MAX_INPUTS] is compacted into several files by the caller.
//!
//! The output header follows ltx v0.5.3 `compactor.go`: page size, commit, max
//! TXID and timestamp come from the last input, min TXID and pre-apply checksum
//! from the first; the node id and WAL origin are dropped. The output is tracked
//! only when every input is, and then carries the last input's post-apply
//! checksum; otherwise it is untracked with no checksums, which is what Go's
//! level compactor writes (Go's compactor rejects tracked checksums, so they stay off
//! until rollback to Go is no longer needed).
//!
//! Nothing is published unless every input's decoder finished with
//! [lsr_ltx::Verified] and every checksum link held. Headers are all read and
//! checked before the upload begins. On any error the upload is aborted; a
//! failed abort never replaces the original error. A commit that fails after
//! the object became visible is reported as [CompactError::Dest]: the caller
//! lists before it trusts the destination. Exception: the output's commit is
//! checked against the bytes written (`crate::publish`), so a `Dest` error
//! holding `Corrupt { cause: CommitSizeMismatch { .. } }` means the store
//! acknowledged a short object. A create-only output found its key free, so the
//! commit deletes the short object itself. A replacing output is left under
//! its key (`removed: false`): the caller must not trust a listing of the
//! output key, and removes it under a reason of its own.
//!
//! A page whose newest copy sits in an input that a later input shrank below
//! was not written by SQLite, which writes every page it grows into. Go's
//! merge and an in-order apply disagree on such a chain, so it is refused
//! ([InputFault::PageSurvivesShrink]) and the inputs are left alone. The same
//! goes for a regrow that skips a page above an earlier shrink and is held by
//! no later input ([InputFault::PageNotRewritten]): a merge would show the
//! base's copy where an in-order apply shows zeros.
//!
//! The output level must be above every input's level
//! ([InputsError::LevelNotAbove]), checked before any store call.
//!
//! [`Compacting`] is the same merge kept between the engine's ticks: each slice reads about a
//! tick's bytes of its inputs and hands the destination at most a tick's bytes of output
//! (`docs/decisions/D259.md`). A store that can hold a read open without borrowing itself
//! ([`ObjectStore::hold_stream`]) keeps each input's one stream across slices; any other has a
//! stream opened for each slice, at the offset the last one reached.

use std::convert::Infallible;
use std::num::{NonZeroU64, NonZeroUsize};

use lsr_ltx::{
    DbChecksum, DbPages, DecodeError, EncodeError, Encoder, Event, Header, HeaderFields, PageNo,
    PageSize, Trailer, TxidRange, Verified,
};
use thiserror::Error;

use crate::fault::role::{Replica, Role, STAGING_WINDOW};
use crate::fault::{BugSite, Fault};
use crate::layout::{FileInfo, FileKey, Level};
use crate::ltx_read::{BrokenLink, LinkEnd, LtxRead, Next, ReadFault, check_link};
use crate::publish::{Open, Writer};
use crate::store::{Flight, ObjectStore, ReadLen, ReadStream, Rest, StoreError, Upload, WriteMode};

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and size arithmetic on small bounded values"
)]
mod tests;

/// Most input files one compaction takes.
pub const MAX_INPUTS: usize = 32;

/// The most bytes asked of a replica per read of an input ([`Role::COMPACT_WINDOW`] says what
/// a staged one gets; [`Role::COMPACT_BUDGET`] shares the total out).
const CHUNK_BYTES: u32 = 1024 * 1024;

/// The size at which buffered output is handed to the upload. The buffer is kept for the
/// whole file (emptied, never replaced), so a small one is all the output ever holds.
const FLUSH_BYTES: usize = 256 * 1024;

// `ReadLen::or_chunk` falls back to 1 MiB for a size it refuses, which would
// silently change the memory per file this constant states.
const _: () = assert!(CHUNK_BYTES >= 1 && CHUNK_BYTES <= ReadLen::MAX_RAW);

/// Why a set of files is not a valid compaction input.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum InputsError {
    /// No files.
    #[error("no input files")]
    Empty,
    /// More files than [MAX_INPUTS].
    #[error("{found} input files, at most {MAX_INPUTS}")]
    TooMany {
        /// How many were given.
        found: usize,
    },
    /// A file does not start at the transaction after the previous one ends.
    #[error("{next} does not start right after {after}")]
    NotContiguous {
        /// The earlier file.
        after: FileKey,
        /// The file that does not follow it.
        next: FileKey,
    },
    /// The output level is not above an input's level, so the output could
    /// take the input's own key and "delete the inputs" would delete it. The
    /// rule is about levels, not about whether source and destination differ.
    #[error("output level {output} is not above the level of {input}")]
    LevelNotAbove {
        /// The requested output level.
        output: Level,
        /// An input at or above it.
        input: FileKey,
    },
}

/// Files to compact: 1 to [MAX_INPUTS], ascending, each starting at the
/// transaction right after the previous one ends. Stricter than Go's
/// `IsContiguous`, which lets inputs overlap.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CompactionInputs {
    files: Vec<FileInfo>,
    first: FileInfo,
    range: TxidRange,
}

impl CompactionInputs {
    /// Checks the set.
    ///
    /// # Errors
    /// [InputsError] naming the first violation.
    pub fn new(files: Vec<FileInfo>) -> Result<Self, InputsError> {
        let (Some(first), Some(last)) = (files.first(), files.last()) else {
            return Err(InputsError::Empty);
        };
        if files.len() > MAX_INPUTS {
            return Err(InputsError::TooMany { found: files.len() });
        }
        for pair in files.windows(2) {
            if let [before, after] = pair
                && before.key.range().max().next() != Some(after.key.range().min())
            {
                return Err(InputsError::NotContiguous {
                    after: before.key,
                    next: after.key,
                });
            }
        }
        let (first, last) = (*first, *last);
        let range = TxidRange::new(first.key.range().min(), last.key.range().max()).ok_or(
            InputsError::NotContiguous {
                after: first.key,
                next: last.key,
            },
        )?;
        Ok(Self {
            first,
            files,
            range,
        })
    }

    /// The transactions the output will cover.
    #[must_use]
    pub const fn range(&self) -> TxidRange {
        self.range
    }

    /// An input whose level is not below `output`, if any.
    fn level_not_below(&self, output: Level) -> Option<FileKey> {
        self.files
            .iter()
            .map(|info| info.key)
            .find(|key| key.level() >= output)
    }

    /// The inputs, oldest first.
    #[must_use]
    pub fn files(&self) -> &[FileInfo] {
        &self.files
    }
}

/// Why an input file cannot be merged.
#[derive(Debug, Error)]
pub enum InputFault {
    /// The bytes are not a valid LTX file: truncated, corrupt, or followed by
    /// more bytes.
    #[error("decode: {0}")]
    Decode(#[from] DecodeError),
    /// The header's transaction range is not the one in the file's name.
    #[error("header covers {header}, not the range in its name")]
    RangeMismatch {
        /// What the header says.
        header: TxidRange,
    },
    /// A later input has a different page size from the first.
    #[error("page size {found:?} where the inputs began with {expected:?}")]
    PageSizeChanged {
        /// The first input's page size.
        expected: PageSize,
        /// This input's.
        found: PageSize,
    },
    /// A tracked input does not start from the database state the previous
    /// tracked input ended in.
    #[error("pre-apply checksum {found} but the previous input ended at {expected}")]
    ChecksumChain {
        /// The previous input's post-apply checksum.
        expected: DbChecksum,
        /// This input's pre-apply checksum.
        found: DbChecksum,
    },
    /// The page's newest copy is in this input but a later input shrank the
    /// database below it and never rewrote it, which SQLite cannot produce.
    #[error("page {} survives a shrink below it", pgno.get())]
    PageSurvivesShrink {
        /// The page.
        pgno: PageNo,
    },
    /// This input shrank the database below the page, a later input grew it
    /// back over the page, and no input after the shrink holds the page. An
    /// in-order apply leaves it zero while a merge would leave whatever the
    /// base held, and a zero page is never invented here.
    #[error("page {} is regrown over without being rewritten", pgno.get())]
    PageNotRewritten {
        /// The first such page.
        pgno: PageNo,
    },
}

/// Why a compaction failed. The destination holds no new file unless the
/// commit itself failed after publishing, which is a [Self::Dest] error to be
/// settled by listing.
#[derive(Debug, Error)]
pub enum CompactError {
    /// An input vanished (retention or a rival compactor got there first): the
    /// caller plans again. Never retried here.
    #[error("input {key} vanished")]
    InputVanished {
        /// The missing input.
        key: FileKey,
    },
    /// Reading an input failed in some other way.
    #[error("reading an input: {0}")]
    Source(#[source] StoreError),
    /// Reading an input's local staged copy failed (level 1 reads level 0 from staging when
    /// its copies are the replica's). A staging error, never the replica's: a refused local
    /// read is the directory's permissions, a full local disk is a full disk.
    #[error("reading an input's local copy: {0}")]
    Local(#[source] StoreError),
    /// The inputs and the output level do not go together.
    #[error("{0}")]
    Inputs(#[from] InputsError),
    /// Writing the output failed.
    #[error("writing the output: {0}")]
    Dest(#[source] StoreError),
    /// An input is unusable.
    #[error("{key}: {fault}")]
    Input {
        /// The input.
        key: FileKey,
        /// What is wrong with it.
        fault: InputFault,
    },
    /// The merge produced something the strict encoder refuses, such as a
    /// snapshot with a missing page.
    #[error("encoding the output: {0}")]
    Encode(#[source] EncodeError),
    /// Level 9 holds snapshots, and the first input does not start at
    /// transaction 1.
    #[error("{first} is not a snapshot, so it cannot be compacted into the snapshot level")]
    SnapshotLevel {
        /// The first input.
        first: FileKey,
    },
    /// A condition the compactor rules out was met: the fault [`BugSite::hit`] made.
    #[error("a compactor invariant failed: {0:?}")]
    Bug(Fault),
}

/// A published compaction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Compacted {
    info: FileInfo,
    header: Header,
    trailer: Trailer,
    pages: u32,
    bytes_read: u64,
    peak_buffered: usize,
}

impl Compacted {
    /// The file the store now holds.
    #[must_use]
    pub const fn info(&self) -> FileInfo {
        self.info
    }

    /// The output's header.
    #[must_use]
    pub const fn header(&self) -> &Header {
        &self.header
    }

    /// The output's trailer.
    #[must_use]
    pub const fn trailer(&self) -> &Trailer {
        &self.trailer
    }

    /// Pages in the output.
    #[must_use]
    pub const fn pages(&self) -> u32 {
        self.pages
    }

    /// Bytes read from the source.
    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// The most bytes held at once: read chunks, pages and buffered output.
    #[must_use]
    pub const fn peak_buffered(&self) -> usize {
        self.peak_buffered
    }
}

/// The two sizes that bound memory; the engine takes the read size from the source's
/// role and the flush size from [CHUNK_BYTES], a test shrinks them to show the bound
/// scales with them.
#[derive(Clone, Copy, Debug)]
struct Budget {
    chunk: ReadLen,
    flush: usize,
}

impl Budget {
    /// The window the `source`'s role reads each of `inputs` inputs in, and the standard
    /// flush: the role's budget shared out, within the role's bounds.
    fn reading<S: Role>(inputs: usize) -> Self {
        let most = S::COMPACT_WINDOW.get();
        let share = S::COMPACT_BUDGET
            .checked_div(u64::try_from(inputs).unwrap_or(u64::MAX))
            .unwrap_or(most);
        let window = share.clamp(STAGING_WINDOW.get().min(most), most);
        Self {
            chunk: u32::try_from(window)
                .ok()
                .and_then(ReadLen::new)
                .unwrap_or(S::COMPACT_WINDOW),
            flush: FLUSH_BYTES,
        }
    }
}

/// The stream one call opened for an input whose store holds none ([`Reader`]): dropped when
/// the call ends, so a store that cannot hold one is read through one stream a slice.
type Slot<'s> = Option<Box<dyn ReadStream + 's>>;

/// One [`Slot`] per input, in input order.
type Slots<'s> = Vec<Slot<'s>>;

/// What one decoder step found.
enum Step {
    Header(Header),
    Page(PageNo),
    End(Verified),
}

/// One input's bytes: one stream from the source (a single GET on a store that bills by
/// request, as Go's compactor opens each input once), its pieces pushed through one decoder.
/// The stream borrows nothing ([`ObjectStore::hold_stream`]), so the read goes on in the
/// compaction's next slice; a store with none to hold is read where the read stands at each step.
struct Reader {
    read: LtxRead,
    held: Option<Box<dyn ReadStream>>,
    /// The source role's error for a failed read.
    store_fault: fn(StoreError) -> CompactError,
}

impl Reader {
    async fn new<S: Role>(source: &S, key: FileKey, chunk: ReadLen) -> Result<Self, CompactError> {
        let held = source.hold_stream(key, 0).await;
        let store_fault = S::compact_input;
        Ok(Self {
            read: LtxRead::new(key, chunk),
            held: held.map_err(|err| read_fault(err, store_fault))?,
            store_fault,
        })
    }

    const fn key(&self) -> FileKey {
        self.read.key()
    }

    fn fault(&self, fault: InputFault) -> CompactError {
        CompactError::Input {
            key: self.key(),
            fault,
        }
    }

    /// Advances to the next header, page or verified end, copying a page's
    /// bytes into `page`. Bytes read are added to `bytes_read` as they arrive.
    async fn step<'s, S: ObjectStore>(
        &mut self,
        (source, slot): (&'s S, &mut Slot<'s>),
        bytes_read: &mut u64,
        page: &mut Vec<u8>,
    ) -> Result<Step, CompactError> {
        let before = self.read.bytes_read();
        let stream: &mut (dyn ReadStream + '_) = match self.held.as_mut() {
            Some(held) => &mut **held,
            None => &mut **slot.get_or_insert_with(|| source.read_stream(self.read.key(), before)),
        };
        let next = self
            .read
            .next(stream, |event| {
                Ok::<_, Infallible>(match event {
                    Event::Header(header) => Step::Header(header),
                    Event::Page(found) => {
                        page.clear();
                        page.extend_from_slice(found.data());
                        Step::Page(found.pgno())
                    }
                })
            })
            .await;
        let read = self.read.bytes_read().saturating_sub(before);
        *bytes_read = bytes_read.saturating_add(read);
        match next {
            Ok(Next::Event(step)) => Ok(step),
            Ok(Next::End(verified)) => Ok(Step::End(verified)),
            Err(ReadFault::Store(err)) => Err(read_fault(err, self.store_fault)),
            Err(ReadFault::Decode(err)) => Err(self.fault(InputFault::Decode(err))),
            Err(ReadFault::Rejected(never)) => match never {},
        }
    }

    fn held(&self) -> usize {
        self.read.held()
    }
}

/// One input with its header read and its next page, if any, waiting.
struct Input {
    reader: Reader,
    header: Header,
    page: Vec<u8>,
    head: Option<PageNo>,
    verified: Option<Verified>,
}

impl Input {
    fn key(&self) -> FileKey {
        self.reader.key()
    }

    /// Reads the header. A decoder that yields anything else first has
    /// skipped it, which no valid decoder does; it is reported as poisoned.
    async fn open<'s, S: Role>(
        (source, slot): (&'s S, &mut Slot<'s>),
        budget: Budget,
        key: FileKey,
        bytes_read: &mut u64,
    ) -> Result<Self, CompactError> {
        let mut reader = Reader::new(source, key, budget.chunk).await?;
        let mut page = Vec::new();
        match reader.step((source, slot), bytes_read, &mut page).await? {
            Step::Header(header) => Ok(Self {
                reader,
                header,
                page,
                head: None,
                verified: None,
            }),
            Step::Page(_) | Step::End(_) => {
                Err(reader.fault(InputFault::Decode(DecodeError::Poisoned)))
            }
        }
    }

    /// Fills the waiting page, or records the verified end.
    async fn fill<'s, S: ObjectStore>(
        &mut self,
        read: (&'s S, &mut Slot<'s>),
        bytes_read: &mut u64,
    ) -> Result<(), CompactError> {
        if self.head.is_some() || self.verified.is_some() {
            return Ok(());
        }
        match self.reader.step(read, bytes_read, &mut self.page).await? {
            Step::Page(pgno) => self.head = Some(pgno),
            Step::End(verified) => self.verified = Some(verified),
            Step::Header(_) => {
                return Err(self.reader.fault(InputFault::Decode(DecodeError::Poisoned)));
            }
        }
        Ok(())
    }

    fn held(&self) -> usize {
        self.reader.held().saturating_add(self.page.len())
    }
}

/// All inputs, merged lazily. The first input is held apart so "the first" and
/// "the last" exist by construction.
struct Merge {
    budget: Budget,
    first: Input,
    rest: Vec<Input>,
    bytes_read: u64,
    peak: usize,
}

impl Merge {
    /// Opens every input and checks every header before anything is written.
    async fn open<'s, S: Role>(
        (source, slots): (&'s S, &mut Slots<'s>),
        inputs: &CompactionInputs,
        budget: Budget,
    ) -> Result<Self, CompactError> {
        slots.resize_with(inputs.files.len(), || None);
        let [first_slot, rest_slots @ ..] = slots.as_mut_slice() else {
            return Err(InputsError::Empty.into());
        };
        let mut bytes_read = 0;
        let first = Input::open(
            (source, first_slot),
            budget,
            inputs.first.key,
            &mut bytes_read,
        )
        .await?;
        check_range(&first)?;
        let mut merge = Self {
            budget,
            first,
            rest: Vec::new(),
            bytes_read,
            peak: 0,
        };
        for (info, slot) in inputs.files.iter().skip(1).zip(rest_slots) {
            let input =
                Input::open((source, slot), budget, info.key, &mut merge.bytes_read).await?;
            check_range(&input)?;
            let (expected, found) = (merge.first.header.page_size(), input.header.page_size());
            if expected != found {
                return Err(input
                    .reader
                    .fault(InputFault::PageSizeChanged { expected, found }));
            }
            merge.rest.push(input);
            merge.observe(0);
        }
        Ok(merge)
    }

    fn all(&self) -> impl Iterator<Item = &Input> {
        std::iter::once(&self.first).chain(self.rest.iter())
    }

    fn all_mut(&mut self) -> impl Iterator<Item = &mut Input> {
        std::iter::once(&mut self.first).chain(self.rest.iter_mut())
    }

    fn last(&self) -> &Input {
        self.rest.last().unwrap_or(&self.first)
    }

    fn observe(&mut self, buffered_output: usize) {
        let held = self.all().fold(buffered_output, |sum, input| {
            sum.saturating_add(input.held())
        });
        self.peak = self.peak.max(held);
    }

    /// The output header, from the first and last inputs.
    fn output_header(&self, level: Level, range: TxidRange) -> Result<Header, CompactError> {
        let (first, last) = (&self.first, self.last());
        if level.is_snapshot() && !first.header.is_snapshot() {
            return Err(CompactError::SnapshotLevel { first: first.key() });
        }
        let tracked = self.all().all(|input| input.header.tracked());
        Header::new(HeaderFields {
            tracked,
            page_size: first.header.page_size(),
            commit: last.header.commit(),
            txids: range,
            timestamp: last.header.fields().timestamp,
            pre_apply: first.header.fields().pre_apply.filter(|_| tracked),
            wal: None,
            node_id: 0,
        })
        .map_err(|err| {
            last.reader
                .fault(InputFault::Decode(DecodeError::Header(err)))
        })
    }

    /// The next page to write: the lowest page number any input holds, and the
    /// newest input holding it. Every copy of it is consumed; the winner's
    /// bytes stay in place until the next call.
    async fn next_page<'s, S: ObjectStore>(
        &mut self,
        (source, slots): (&'s S, &mut Slots<'s>),
        buffered_output: usize,
    ) -> Result<Option<(PageNo, usize)>, CompactError> {
        let mut bytes_read = self.bytes_read;
        let inputs = std::iter::once(&mut self.first).chain(self.rest.iter_mut());
        for (input, slot) in inputs.zip(slots.iter_mut()) {
            input.fill((source, slot), &mut bytes_read).await?;
        }
        self.bytes_read = bytes_read;
        self.observe(buffered_output);
        let Some(pgno) = self.all().filter_map(|input| input.head).min() else {
            return Ok(None);
        };
        let mut winner = 0;
        for (at, input) in self.all_mut().enumerate() {
            if input.head == Some(pgno) {
                winner = at;
                input.head = None;
            }
        }
        Ok(Some((pgno, winner)))
    }

    fn page(&self, at: usize) -> &[u8] {
        self.all().nth(at).map_or(&[], |input| &input.page)
    }

    /// SQLite writes every page it grows into, so a copy older than a later
    /// shrink below it is not a chain SQLite wrote.
    fn check_survives_shrink(&self, pgno: PageNo, winner: usize) -> Result<(), CompactError> {
        let shrunk = self
            .all()
            .skip(winner.saturating_add(1))
            .any(|later| !later.header.pages().contains(pgno));
        match self.all().nth(winner) {
            Some(input) if shrunk => {
                Err(input.reader.fault(InputFault::PageSurvivesShrink { pgno }))
            }
            Some(_) | None => Ok(()),
        }
    }

    /// Pages `lo..hi` (hi excluded, at most the output commit) were held by no
    /// input. If some input's commit is below one of them, the database
    /// shrank past it and grew back without rewriting it.
    fn check_unheld(&self, lo: u32, hi: u32) -> Result<(), CompactError> {
        let Some(shortest) = self.all().min_by_key(|input| input.header.commit()) else {
            return Ok(());
        };
        let size = self.first.header.page_size();
        match first_unrewritten(lo, hi, shortest.header.pages(), size) {
            Some(pgno) => Err(shortest.reader.fault(InputFault::PageNotRewritten { pgno })),
            None => Ok(()),
        }
    }

    /// Every neighbouring pair of tracked inputs must chain, checked once all
    /// are verified; returns the last input's verified summary.
    fn verified_chain(&self) -> Result<Verified, CompactError> {
        let mut before = &self.first;
        for after in &self.rest {
            if let Some(done) = before.verified
                && let Err(BrokenLink { expected, found }) =
                    check_link(LinkEnd::of(&done), &after.header)
            {
                return Err(after
                    .reader
                    .fault(InputFault::ChecksumChain { expected, found }));
            }
            before = after;
        }
        before.verified.ok_or_else(|| {
            before
                .reader
                .fault(InputFault::Decode(DecodeError::Poisoned))
        })
    }
}

/// The first page in `lo..hi` past the end of `shortest` (the smallest
/// database among the inputs) that a file can hold: never the lock page.
fn first_unrewritten(lo: u32, hi: u32, shortest: DbPages, size: PageSize) -> Option<PageNo> {
    let past = shortest.get().saturating_add(1);
    let pgno = PageNo::new(lo.max(past), size)
        .or_else(|| PageNo::new(lo.max(past).saturating_add(1), size))?;
    (pgno.get() < hi).then_some(pgno)
}

/// The header's range must be the one in the file's name.
fn check_range(input: &Input) -> Result<(), CompactError> {
    if input.header.txids() == input.key().range() {
        return Ok(());
    }
    Err(input.reader.fault(InputFault::RangeMismatch {
        header: input.header.txids(),
    }))
}

/// A compaction part-way to its destination: what it holds between two slices, in memory
/// only. Each input is read through its one held stream, so the output is one merge of the
/// objects opened when it began, whatever the replica took meanwhile. Nothing of it is visible
/// until [`Self::commit`], and that only after every input verified: a failure, a stop or a
/// crash starts it again from its first page, and the next start clears the upload's scratch.
pub(crate) struct Compacting {
    merge: Merge,
    header: Header,
    /// `None` once the tail is written: `rest` holds the bytes still to hand over.
    encoder: Option<Encoder<Vec<u8>>>,
    rest: Vec<u8>,
    trailer: Option<Trailer>,
    upload: Open,
    pages: u32,
    /// The first page the regrow check has not looked at.
    unchecked_from: u32,
    paged: bool,
    /// Bytes handed to the destination.
    pub(crate) sent: u64,
    /// The last slice stopped for the upload's parts (`docs/decisions/D233.md`).
    pub(crate) waits: bool,
}

impl std::fmt::Debug for Compacting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compacting")
            .field("header", &self.header)
            .field("sent", &self.sent)
            .finish_non_exhaustive()
    }
}

impl Compacting {
    /// Opens every input of `inputs`, read from `source` as its role, checks every header
    /// and begins the output at `level` in `dest` under `mode`.
    ///
    /// # Errors
    /// [`CompactError`]; nothing was written.
    pub(crate) async fn begin<S: Role, D: ObjectStore>(
        source: &S,
        dest: &D,
        inputs: &CompactionInputs,
        level: Level,
        mode: WriteMode,
    ) -> Result<Self, CompactError> {
        let budget = Budget::reading::<S>(inputs.files.len());
        Self::begin_with(
            (source, &mut Slots::new()),
            dest,
            inputs,
            level,
            mode,
            budget,
        )
        .await
    }

    async fn begin_with<'s, S: Role, D: ObjectStore>(
        read: (&'s S, &mut Slots<'s>),
        dest: &D,
        inputs: &CompactionInputs,
        level: Level,
        mode: WriteMode,
        budget: Budget,
    ) -> Result<Self, CompactError> {
        if let Some(input) = inputs.level_not_below(level) {
            return Err(InputsError::LevelNotAbove {
                output: level,
                input,
            }
            .into());
        }
        let merge = Merge::open(read, inputs, budget).await?;
        let header = merge.output_header(level, inputs.range())?;
        // Room for the flush size and the page that crosses it, so the buffer never grows.
        let out = Vec::with_capacity(
            budget
                .flush
                .saturating_add(header.page_size().bytes().saturating_mul(2)),
        );
        let encoder = Encoder::new(out, header).map_err(CompactError::Encode)?;
        let key = FileKey::new(level, inputs.range());
        let created_at = header.fields().timestamp;
        let upload = Writer::begin(dest, &key, created_at, mode, None)
            .await
            .map_err(CompactError::Dest)?
            .boxed();
        Ok(Self {
            merge,
            header,
            encoder: Some(encoder),
            rest: Vec::new(),
            trailer: None,
            upload,
            pages: 0,
            unchecked_from: 1,
            paged: false,
            sent: 0,
            waits: false,
        })
    }

    /// Hands the destination at most the next `budget` bytes of the output, reading at most
    /// about `budget` bytes of input for them. The output's trailer once every byte is handed
    /// over and the upload's parts are answered: [`Self::commit`] goes next. `None`: it goes
    /// on in a later slice. `patient` waits for the upload's parts inside the call, as a
    /// whole-file write does; otherwise a slice that finds them in the way stops, and
    /// `waits` says so.
    ///
    /// # Errors
    /// [`CompactError`]; the caller aborts ([`Self::abort`]).
    pub(crate) async fn slice<S: ObjectStore>(
        &mut self,
        source: &S,
        budget: u64,
        patient: bool,
    ) -> Result<Option<Trailer>, CompactError> {
        let mut slots = Slots::new();
        slots.resize_with(self.merge.rest.len().saturating_add(1), || None);
        self.slice_in((source, &mut slots), budget, patient).await
    }

    async fn slice_in<'s, S: ObjectStore>(
        &mut self,
        (source, slots): (&'s S, &mut Slots<'s>),
        budget: u64,
        patient: bool,
    ) -> Result<Option<Trailer>, CompactError> {
        self.waits = false;
        let read_until = self.merge.bytes_read.saturating_add(budget);
        // Zero once the budget is spent, as a snapshot's slice counts it (`crate::snapshot`).
        let mut left = NonZeroU64::new(budget);
        while let Some(room) = left {
            self.fill((source, &mut *slots), read_until).await?;
            let (len, encoding) = (self.out().len(), self.encoder.is_some());
            // Short of a flush, the slice read its share of the inputs.
            if encoding && len < self.merge.budget.flush {
                break;
            }
            // Every pass hands over a byte, so the count down cannot stand still.
            let Some(n) =
                NonZeroUsize::new(len.min(usize::try_from(room.get()).unwrap_or(usize::MAX)))
            else {
                break;
            };
            if !patient
                && self
                    .upload
                    .flight(Rest::More)
                    .await
                    .map_err(CompactError::Dest)?
                    == Flight::Full
            {
                self.waits = true;
                return Ok(None);
            }
            let n = n.get();
            let out = match self.encoder.as_mut() {
                Some(encoder) => encoder.writer_mut(),
                None => &mut self.rest,
            };
            self.upload
                .write(out.get(..n).unwrap_or_default())
                .await
                .map_err(CompactError::Dest)?;
            out.drain(..n);
            let n = u64::try_from(n).unwrap_or(u64::MAX);
            (left, self.sent) = (
                NonZeroU64::new(room.get().saturating_sub(n)),
                self.sent.saturating_add(n),
            );
        }
        let Some(trailer) = self.trailer.filter(|_| self.rest.is_empty()) else {
            return Ok(None);
        };
        if !patient
            && self
                .upload
                .flight(Rest::None)
                .await
                .map_err(CompactError::Dest)?
                != Flight::Idle
        {
            self.waits = true;
            return Ok(None);
        }
        Ok(Some(trailer))
    }

    /// The bytes waiting to be handed over: the encoder's, then the tail's.
    fn out(&mut self) -> &mut Vec<u8> {
        match self.encoder.as_mut() {
            Some(encoder) => encoder.writer_mut(),
            None => &mut self.rest,
        }
    }

    /// Merges and encodes until a flush's worth waits to be handed over, the tail is written,
    /// or the inputs have been read to `read_until` bytes.
    async fn fill<'s, S: ObjectStore>(
        &mut self,
        (source, slots): (&'s S, &mut Slots<'s>),
        read_until: u64,
    ) -> Result<(), CompactError> {
        let flush = self.merge.budget.flush;
        while let Some(encoder) = self.encoder.as_mut()
            && encoder.writer_mut().len() < flush
            && self.merge.bytes_read < read_until
        {
            if !self.paged {
                let Some((pgno, winner)) = self
                    .merge
                    .next_page((source, &mut *slots), encoder.writer_mut().len())
                    .await?
                else {
                    self.merge.check_unheld(
                        self.unchecked_from,
                        self.header.commit().saturating_add(1),
                    )?;
                    self.paged = true;
                    continue;
                };
                if !self.header.pages().contains(pgno) {
                    continue;
                }
                self.merge.check_unheld(self.unchecked_from, pgno.get())?;
                self.unchecked_from = pgno.get().saturating_add(1);
                self.merge.check_survives_shrink(pgno, winner)?;
                encoder
                    .write_page(pgno, self.merge.page(winner))
                    .map_err(CompactError::Encode)?;
                self.pages = self.pages.saturating_add(1);
                self.merge.observe(encoder.writer_mut().len());
                continue;
            }
            // The index leaves in pieces, as the pages did, so it is never copied whole into the buffer.
            if encoder.write_index_chunk().map_err(CompactError::Encode)? {
                continue;
            }
            let last = self.merge.verified_chain()?;
            let post_apply = last
                .trailer()
                .post_apply()
                .filter(|_| self.header.tracked());
            let finished = self
                .encoder
                .take()
                .map(|encoder| encoder.finish(post_apply));
            if let Some(finished) = finished {
                let (tail, trailer) = finished.map_err(CompactError::Encode)?;
                (self.rest, self.trailer) = (tail, Some(trailer));
            }
        }
        Ok(())
    }

    /// Publishes the output, `trailer` being what [`Self::slice`] answered.
    ///
    /// # Errors
    /// [`CompactError::Dest`]: settled by listing, as for a whole compaction.
    pub(crate) async fn commit<D: ObjectStore>(
        self,
        dest: &D,
        trailer: Trailer,
    ) -> Result<Compacted, CompactError> {
        let info = self
            .upload
            .commit(dest)
            .await
            .map_err(|err| CompactError::Dest(err.into_error()))?
            .info();
        Ok(Compacted {
            info,
            header: self.header,
            trailer,
            pages: self.pages,
            bytes_read: self.merge.bytes_read,
            peak_buffered: self.merge.peak,
        })
    }

    /// Ends the compaction: its upload is aborted, so nothing of it stays as scratch.
    pub(crate) async fn abort(self) {
        abort_quietly(self.upload).await;
    }
}

/// Merges `inputs` read from `source` into one file at `level` in `dest`.
///
/// # Errors
/// [CompactError]; the destination holds no new file unless a commit failed
/// after publishing (a [CompactError::Dest]), which the caller settles by
/// listing. No retry happens here.
pub async fn compact<S: ObjectStore, D: ObjectStore>(
    source: &S,
    dest: &D,
    inputs: &CompactionInputs,
    level: Level,
) -> Result<Compacted, CompactError> {
    compact_in(&Replica(source), dest, inputs, level, WriteMode::Replace).await
}

/// [`compact`] reading its inputs as the staged copies the engine's level-1 compaction
/// reads (the staging window), for the memory gate that runs it against a directory.
///
/// # Errors
/// As [`compact`], with a failed input read reported as [`CompactError::Local`].
#[cfg(feature = "testing")]
#[doc(hidden)]
pub async fn compact_staged<S: ObjectStore, D: ObjectStore>(
    source: &S,
    dest: &D,
    inputs: &CompactionInputs,
    level: Level,
) -> Result<Compacted, CompactError> {
    let staged = crate::fault::role::Staging(source);
    compact_in(&staged, dest, inputs, level, WriteMode::Replace).await
}

/// [`compact`] reading its inputs from `source` as its role: an input read
/// that fails is that store's error ([`Role::compact_input`]). The output is
/// written under `mode`: create-only, a key that is taken fails the commit with
/// [`StoreError::AlreadyExists`] and what it holds stands.
pub(crate) async fn compact_in<S: Role, D: ObjectStore>(
    source: &S,
    dest: &D,
    inputs: &CompactionInputs,
    level: Level,
    mode: WriteMode,
) -> Result<Compacted, CompactError> {
    let budget = Budget::reading::<S>(inputs.files.len());
    compact_with(source, dest, inputs, level, mode, budget).await
}

async fn compact_with<S: Role, D: ObjectStore>(
    source: &S,
    dest: &D,
    inputs: &CompactionInputs,
    level: Level,
    mode: WriteMode,
    budget: Budget,
) -> Result<Compacted, CompactError> {
    let mut slots = Slots::new();
    let mut job =
        Compacting::begin_with((source, &mut slots), dest, inputs, level, mode, budget).await?;
    // A patient slice with no limit on its bytes hands over the whole output, so one is all
    // it takes; one that stops short is a bug, never a loop that waits on it.
    let ended = job.slice_in((source, &mut slots), u64::MAX, true).await;
    match ended {
        Ok(Some(trailer)) => job.commit(dest, trailer).await,
        Ok(None) => {
            job.abort().await;
            Err(CompactError::Bug(BugSite::Encode.hit()))
        }
        Err(err) => {
            job.abort().await;
            Err(err)
        }
    }
}

/// A failed read of an input: gone, or the source role's error.
fn read_fault(err: StoreError, store_fault: fn(StoreError) -> CompactError) -> CompactError {
    match err {
        StoreError::NotFound { key } => CompactError::InputVanished { key },
        err @ (StoreError::Transient { .. }
        | StoreError::Throttled { .. }
        | StoreError::Denied { .. }
        | StoreError::AlreadyExists { .. }
        | StoreError::ScratchExhausted { .. }
        | StoreError::Corrupt { .. }
        | StoreError::CreateOnlyUnsupported { .. }
        | StoreError::ObjectTooLarge { .. }
        | StoreError::Refused { .. }) => store_fault(err),
    }
}

/// An abort that fails leaves scratch the store cleans up; the error that made
/// us abort is the one worth reporting.
async fn abort_quietly<U: Upload>(upload: Writer<U>) {
    match upload.abort().await {
        Ok(()) | Err(_) => {}
    }
}

//! The compactor: a contiguous run of LTX files becomes one file.
//!
//! [compact] reads up to [MAX_INPUTS] files from a source store and writes one
//! file to a destination store. Source and destination differ because level 0
//! is read from the local copy and every other level from the replica.
//!
//! Each input is read in bounded chunks through its own decoder (the shared
//! `LtxRead`, which ends a file at its first short chunk); the pages of all
//! inputs are merged in ascending page order and the newest input holding a
//! page wins. A page above the output's commit is dropped, and the lock page
//! never appears because [PageNo] cannot hold it. Memory is one read chunk and
//! one page per input plus the encoder's output buffer, whatever the database
//! size: the output page index (at most 30 bytes a page) is the only thing that
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
//! lists before it trusts the destination. Exception: a `Dest` error holding
//! `Corrupt { cause: CommitSizeMismatch { removed: true, .. } }` from a
//! [crate::Retrying] destination means the object was deleted, so the
//! destination holds nothing new; with `removed: false` the caller must not
//! trust a listing of the output key.
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

use std::convert::Infallible;
use std::mem;

use lsr_ltx::{
    DbChecksum, DbPages, DecodeError, EncodeError, Encoder, Event, Header, HeaderFields, PageNo,
    PageSize, Trailer, TxidRange, Verified,
};
use thiserror::Error;

use crate::layout::{FileInfo, FileKey, Level};
use crate::ltx_read::{BrokenLink, LinkEnd, LtxRead, Next, ReadFault, check_link};
use crate::store::{ObjectStore, ReadLen, StoreError, Upload};

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and size arithmetic on small bounded values"
)]
mod tests;

/// Most input files one compaction takes.
pub const MAX_INPUTS: usize = 32;

/// Bytes asked of the source per read, and the size at which buffered output is
/// handed to the upload.
const CHUNK_BYTES: u32 = 1024 * 1024;

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

/// The two sizes that bound memory; the public entry point uses [CHUNK_BYTES]
/// for both, a test shrinks them to show the bound scales with them.
#[derive(Clone, Copy, Debug)]
struct Budget {
    chunk: ReadLen,
    flush: usize,
}

impl Budget {
    fn standard() -> Self {
        Self {
            chunk: ReadLen::or_chunk(CHUNK_BYTES),
            flush: usize::try_from(CHUNK_BYTES).unwrap_or(usize::MAX),
        }
    }
}

/// What one decoder step found.
enum Step {
    Header(Header),
    Page(PageNo),
    End(Verified),
}

/// One input's bytes: chunked reads pushed through one decoder.
struct Reader {
    read: LtxRead,
}

impl Reader {
    const fn new(key: FileKey, chunk: ReadLen) -> Self {
        Self {
            read: LtxRead::new(key, chunk),
        }
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
    async fn step<S: ObjectStore>(
        &mut self,
        source: &S,
        bytes_read: &mut u64,
        page: &mut Vec<u8>,
    ) -> Result<Step, CompactError> {
        let before = self.read.bytes_read();
        let next = self
            .read
            .next(source, |event| {
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
            Err(ReadFault::Store(StoreError::NotFound { key })) => {
                mark!(Maint2);
                Err(CompactError::InputVanished { key })
            }
            Err(ReadFault::Store(err)) => Err(CompactError::Source(err)),
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
    async fn open<S: ObjectStore>(
        source: &S,
        budget: Budget,
        key: FileKey,
        bytes_read: &mut u64,
    ) -> Result<Self, CompactError> {
        let mut reader = Reader::new(key, budget.chunk);
        let mut page = Vec::new();
        match reader.step(source, bytes_read, &mut page).await? {
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
    async fn fill<S: ObjectStore>(
        &mut self,
        source: &S,
        bytes_read: &mut u64,
    ) -> Result<(), CompactError> {
        if self.head.is_some() || self.verified.is_some() {
            return Ok(());
        }
        match self.reader.step(source, bytes_read, &mut self.page).await? {
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
struct Merge<'a, S> {
    source: &'a S,
    budget: Budget,
    first: Input,
    rest: Vec<Input>,
    bytes_read: u64,
    peak: usize,
}

impl<'a, S: ObjectStore> Merge<'a, S> {
    /// Opens every input and checks every header before anything is written.
    async fn open(
        source: &'a S,
        inputs: &CompactionInputs,
        budget: Budget,
    ) -> Result<Self, CompactError> {
        let mut bytes_read = 0;
        let first = Input::open(source, budget, inputs.first.key, &mut bytes_read).await?;
        check_range(&first)?;
        let mut merge = Self {
            source,
            budget,
            first,
            rest: Vec::new(),
            bytes_read,
            peak: 0,
        };
        for info in inputs.files.iter().skip(1) {
            let input = Input::open(source, budget, info.key, &mut merge.bytes_read).await?;
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
    async fn next_page(
        &mut self,
        buffered_output: usize,
    ) -> Result<Option<(PageNo, usize)>, CompactError> {
        let source = self.source;
        let mut bytes_read = self.bytes_read;
        for input in std::iter::once(&mut self.first).chain(self.rest.iter_mut()) {
            input.fill(source, &mut bytes_read).await?;
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

/// Merges, encodes and uploads every page, then writes the tail. Returns the
/// output trailer and page count; nothing is committed yet.
async fn write_output<S: ObjectStore, U: Upload>(
    merge: &mut Merge<'_, S>,
    mut encoder: Encoder<Vec<u8>>,
    header: &Header,
    upload: &mut U,
) -> Result<(Trailer, u32), CompactError> {
    let flush = merge.budget.flush;
    let mut pages = 0_u32;
    let mut unchecked_from = 1_u32;
    // The header sits in the encoder's buffer; it goes out with the first flush.
    while let Some((pgno, winner)) = merge.next_page(encoder.writer_mut().len()).await? {
        if !header.pages().contains(pgno) {
            continue;
        }
        merge.check_unheld(unchecked_from, pgno.get())?;
        unchecked_from = pgno.get().saturating_add(1);
        merge.check_survives_shrink(pgno, winner)?;
        encoder
            .write_page(pgno, merge.page(winner))
            .map_err(CompactError::Encode)?;
        pages = pages.saturating_add(1);
        merge.observe(encoder.writer_mut().len());
        if encoder.writer_mut().len() >= flush {
            let out = mem::take(encoder.writer_mut());
            upload.write(&out).await.map_err(CompactError::Dest)?;
        }
    }
    merge.check_unheld(unchecked_from, header.commit().saturating_add(1))?;
    let last = merge.verified_chain()?;
    let post_apply = last.trailer().post_apply().filter(|_| header.tracked());
    let (tail, trailer) = encoder.finish(post_apply).map_err(CompactError::Encode)?;
    upload.write(&tail).await.map_err(CompactError::Dest)?;
    Ok((trailer, pages))
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
    compact_with(source, dest, inputs, level, Budget::standard()).await
}

async fn compact_with<S: ObjectStore, D: ObjectStore>(
    source: &S,
    dest: &D,
    inputs: &CompactionInputs,
    level: Level,
    budget: Budget,
) -> Result<Compacted, CompactError> {
    if let Some(input) = inputs.level_not_below(level) {
        return Err(InputsError::LevelNotAbove {
            output: level,
            input,
        }
        .into());
    }
    let mut merge = Merge::open(source, inputs, budget).await?;
    let header = merge.output_header(level, inputs.range())?;
    let encoder = Encoder::new(Vec::new(), header).map_err(CompactError::Encode)?;
    let key = FileKey::new(level, inputs.range());
    let created_at = header.fields().timestamp;
    let mut upload = dest
        .begin_upload(&key, created_at)
        .await
        .map_err(CompactError::Dest)?;
    let written = write_output(&mut merge, encoder, &header, &mut upload).await;
    let (trailer, pages) = match written {
        Ok(done) => done,
        Err(err) => {
            abort_quietly(upload).await;
            return Err(err);
        }
    };
    let info = upload.commit().await.map_err(CompactError::Dest)?;
    Ok(Compacted {
        info,
        header,
        trailer,
        pages,
        bytes_read: merge.bytes_read,
        peak_buffered: merge.peak,
    })
}

/// An abort that fails leaves scratch the store cleans up; the error that made
/// us abort is the one worth reporting.
async fn abort_quietly<U: Upload>(upload: U) {
    match upload.abort().await {
        Ok(()) | Err(_) => {}
    }
}

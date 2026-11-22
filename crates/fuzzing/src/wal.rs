//! The `wal` target: the SQLite WAL reader on any bytes.
//!
//! One input is a few parameter bytes and a WAL file. The target can rewrite
//! the file's header for a page size and byte order the input picks, and
//! re-chain every frame's checksum, so mutated page numbers, commit fields and
//! salts get past the checksums and reach the scan's decisions. The file then
//! sits in memory behind a [`probe::Probe`], which counts what each reader is
//! handed and lets the target change a byte under a reader that already scanned.
//!
//! The scan of the whole file under [`ScanLimit::FirstInvalidFrame`] is the
//! reference every other check is held to: a truncated copy, a committed end,
//! a resumed position and a flipped page byte must each give an exact,
//! predictable part of it.

mod probe;

use std::collections::BTreeMap;

use lsr_ltx::{PageChecksum, PageSize};
use lsr_wal::{
    ByteOrder, CommittedEnd, CommittedTx, FRAME_HEADER_LEN, FrameCursor, FrameFault, FrameRef,
    HEADER_LEN, OpenError, PageMap, PageMapError, ReadPageError, ResumeError, Salts, ScanLimit,
    Tail, TxOutcome, WalChecksum, WalError, WalHeader, WalOffset, WalReader,
};

use crate::bytes::{Reader, violation};
use probe::{Probe, Shared};

/// Bytes of parameters before the WAL file in an input.
pub const WAL_PREFIX_LEN: usize = 12;

/// Mode bit: the rewritten header names big-endian checksum words.
pub const WAL_BIG_ENDIAN: u8 = 0x08;
/// Mode bit: rewrite the header for the mode's page size, keeping its salts.
pub const WAL_REWRITE_HEADER: u8 = 0x10;
/// Mode bit: recompute every whole frame's checksum, keeping its other fields.
pub const WAL_RECHAIN: u8 = 0x20;

const MAGIC_LITTLE: u32 = 0x377f_0682;
const MAGIC_BIG: u32 = 0x377f_0683;
const VERSION: u32 = 3_007_000;
/// Frame header bytes the running checksum covers.
const CHECKSUMMED_FRAME_HEADER: usize = 8;
/// Where the stored checksum starts in a frame header.
const FRAME_CHECKSUM_AT: usize = 16;

/// The parameter bytes, in input order.
#[derive(Clone, Copy, Debug)]
struct Params {
    mode: u8,
    cut: u16,
    end: u16,
    pick: u8,
    resume: u16,
    flip_frame: u8,
    flip_at: u16,
    flip_mask: u8,
}

impl Params {
    fn parse(reader: &mut Reader<'_>) -> Option<Self> {
        Some(Self {
            mode: reader.u8()?,
            cut: reader.u16()?,
            end: reader.u16()?,
            pick: reader.u8()?,
            resume: reader.u16()?,
            flip_frame: reader.u8()?,
            flip_at: reader.u16()?,
            flip_mask: reader.u8()?,
        })
    }
}

/// The page size the mode's low three bits pick: 512 shifted left by them.
fn mode_page_size(mode: u8) -> Option<PageSize> {
    PageSize::new(512_u32.checked_shl(u32::from(mode & 7))?)
}

/// A valid header for the mode's page size and byte order, keeping the
/// file's checkpoint sequence and salts.
fn rewrite_header(file: &mut [u8], mode: u8) {
    let Some(page_size) = mode_page_size(mode) else {
        return;
    };
    let Some(header) = file.first_chunk_mut::<HEADER_LEN>() else {
        return;
    };
    let (magic, order) = if mode & WAL_BIG_ENDIAN == 0 {
        (MAGIC_LITTLE, ByteOrder::Little)
    } else {
        (MAGIC_BIG, ByteOrder::Big)
    };
    let mut fields = Vec::with_capacity(HEADER_LEN);
    for field in [magic, VERSION, page_size.get()] {
        fields.extend(field.to_be_bytes());
    }
    fields.extend(header.get(12..24).unwrap_or_default());
    let Some(sum) = WalChecksum::ZERO.extend(order, &fields) else {
        return;
    };
    fields.extend(sum.s0().to_be_bytes());
    fields.extend(sum.s1().to_be_bytes());
    for (dst, src) in header.iter_mut().zip(fields) {
        *dst = src;
    }
}

/// Gives every whole frame the checksum that chains from the one before it.
fn rechain(file: &mut [u8]) {
    let Some(Ok(header)) = file.first_chunk::<HEADER_LEN>().map(WalHeader::decode) else {
        return;
    };
    let order = header.byte_order();
    let mut running = header.checksum();
    let frame_len = header.page_size().bytes().saturating_add(FRAME_HEADER_LEN);
    let Some(frames) = file.get_mut(HEADER_LEN..) else {
        return;
    };
    for frame in frames.chunks_exact_mut(frame_len) {
        let Some((head, page)) = frame.split_at_mut_checked(FRAME_HEADER_LEN) else {
            return;
        };
        let Some(sum) = head
            .get(..CHECKSUMMED_FRAME_HEADER)
            .and_then(|covered| running.extend(order, covered))
            .and_then(|sum| sum.extend(order, page))
        else {
            return;
        };
        let stored = sum
            .s0()
            .to_be_bytes()
            .into_iter()
            .chain(sum.s1().to_be_bytes());
        for (dst, src) in head.iter_mut().skip(FRAME_CHECKSUM_AT).zip(stored) {
            *dst = src;
        }
        running = sum;
    }
}

/// How a scan ended after its last transaction.
#[derive(Debug)]
enum Ending {
    Stopped { at: FrameCursor, tail: Tail },
    Failed(WalError),
}

#[derive(Debug)]
struct Scan {
    txs: Vec<CommittedTx>,
    ending: Ending,
}

impl Scan {
    fn stopped(&self) -> Option<(FrameCursor, Tail)> {
        match self.ending {
            Ending::Stopped { at, tail } => Some((at, tail)),
            Ending::Failed(_) => None,
        }
    }
}

/// The file under test and what follows from its verified header.
struct Wal {
    shared: Shared,
    header: WalHeader,
    /// Whole frames the file holds after its header.
    frames: u32,
    /// The cursor before every committed transaction of the whole scan, then the one after the last.
    bounds: Vec<FrameCursor>,
}

impl Wal {
    fn page_size(&self) -> PageSize {
        self.header.page_size()
    }

    fn salts(&self) -> Salts {
        self.header.salts()
    }

    fn start(&self) -> FrameCursor {
        FrameCursor::at_start(&self.header)
    }

    /// More scan steps than this means a scan stopped making progress.
    fn steps(&self) -> u32 {
        self.frames.saturating_add(1)
    }

    /// The index of the boundary at frame index `frame`, if it is one.
    fn bound_at(&self, frame: u32) -> Option<usize> {
        self.bounds
            .iter()
            .position(|bound| bound.next().frame_index() == frame)
    }

    fn end_at(&self, frame: u32) -> ScanLimit {
        ScanLimit::Committed(CommittedEnd::new(frame, self.salts(), self.page_size()))
    }
}

fn frames_in(len: usize, page_size: PageSize) -> u32 {
    let frame_len = page_size.bytes().saturating_add(FRAME_HEADER_LEN);
    let whole = len
        .checked_sub(HEADER_LEN)
        .and_then(|rest| rest.checked_div(frame_len))
        .unwrap_or(0);
    u32::try_from(whole).unwrap_or(u32::MAX)
}

/// Opens the file, holding `open` to what `WalHeader::decode` says of the same bytes.
fn open(shared: &Shared) -> Option<WalReader<Probe>> {
    let decoded = shared.header().map(|bytes| WalHeader::decode(&bytes));
    match (WalReader::open(shared.probe()), decoded) {
        (Ok(reader), Some(Ok(header))) if *reader.header() == header => Some(reader),
        (Err(OpenError::Truncated { found }), None) if found == shared.len() => None,
        (Err(OpenError::Header(error)), Some(Err(decoded))) if error == decoded => None,
        (opened, decoded) => violation(&format!(
            "open and decode disagree: {:?} against {decoded:?}",
            opened.map(|reader| *reader.header())
        )),
    }
}

/// One transaction as the scan from `cursor` returned it: whole, contiguous, committed.
fn check_tx(cursor: &FrameCursor, tx: &CommittedTx) {
    let mut expected = cursor.next().frame_index();
    for frame in tx.frames() {
        if frame.offset().frame_index() != expected
            || frame.offset().page_size() != cursor.next().page_size()
        {
            violation("a transaction's frames are not contiguous from the cursor");
        }
        expected = expected.saturating_add(1);
    }
    if tx.frames().is_empty()
        || tx.end().next().frame_index() != expected
        || tx.end().salts() != cursor.salts()
    {
        violation("a transaction is empty or ends elsewhere than its last frame");
    }
}

/// Scans from `from` to the end under `limit`, at most `steps` calls.
fn scan(reader: &mut WalReader<Probe>, from: FrameCursor, limit: ScanLimit, steps: u32) -> Scan {
    let mut txs = Vec::new();
    let mut cursor = from;
    for _ in 0..steps {
        match reader.next_tx(&cursor, limit) {
            Ok(TxOutcome::Tx(tx)) => {
                check_tx(&cursor, &tx);
                cursor = *tx.end();
                txs.push(tx);
            }
            Ok(TxOutcome::End { at, tail }) => {
                if at != cursor {
                    violation("a scan ended somewhere other than after its last transaction");
                }
                let ending = Ending::Stopped { at, tail };
                return Scan { txs, ending };
            }
            Err(error) => {
                let ending = Ending::Failed(error);
                return Scan { txs, ending };
            }
        }
    }
    violation("a scan returned more transactions than the file holds frames")
}

/// The reference scan: it never fails, reads every byte at most once, and its
/// tail agrees with the file's length.
fn whole_scan(reader: &mut WalReader<Probe>, shared: &Shared, frames: u32) -> Scan {
    let start = reader.cursor_at_start();
    let whole = scan(
        reader,
        start,
        ScanLimit::FirstInvalidFrame,
        frames.saturating_add(1),
    );
    let len = u64::try_from(shared.len()).unwrap_or(u64::MAX);
    if shared.stats().delivered > len {
        violation("open and one scan read more bytes than the file holds");
    }
    let Some((at, tail)) = whole.stopped() else {
        violation(&format!("a scan without a committed end failed: {whole:?}"));
    };
    let frame_len = u64::try_from(
        at.next()
            .page_size()
            .bytes()
            .saturating_add(FRAME_HEADER_LEN),
    )
    .unwrap_or(u64::MAX);
    let consistent = match tail {
        Tail::Clean => at.next().get() == len,
        Tail::Uncommitted { frames } => {
            at.next()
                .get()
                .saturating_add(frame_len.saturating_mul(u64::from(frames)))
                == len
        }
        Tail::PartialFrame | Tail::InvalidFrame(_) => at.next().get() < len,
    };
    if !consistent {
        violation(&format!(
            "tail {tail:?} at {} does not fit a {len}-byte file",
            at.next()
        ));
    }
    whole
}

/// The whole scan's transactions that end at or before frame index `frame`.
fn ending_by(whole: &Scan, frame: u32) -> Vec<&CommittedTx> {
    whole
        .txs
        .iter()
        .filter(|tx| tx.end().next().frame_index() <= frame)
        .collect()
}

fn same_txs(found: &[CommittedTx], expected: &[&CommittedTx]) -> bool {
    found.len() == expected.len() && found.iter().zip(expected).all(|(a, b)| a == *b)
}

/// A copy cut short gives exactly the transactions that end before the cut.
fn check_truncation(wal: &Wal, whole: &Scan, params: &Params) {
    let mut bytes = wal.shared.bytes();
    let cut = usize::from(params.cut)
        .checked_rem(bytes.len().saturating_add(1))
        .unwrap_or(0);
    bytes.truncate(cut);
    let short = Shared::new(bytes);
    let Some(mut reader) = open(&short) else {
        if cut >= HEADER_LEN {
            violation("a valid header no longer opens once the file is cut after it");
        }
        return;
    };
    let start = reader.cursor_at_start();
    let found = scan(
        &mut reader,
        start,
        ScanLimit::FirstInvalidFrame,
        wal.steps(),
    );
    let cut = u64::try_from(cut).unwrap_or(u64::MAX);
    let expected: Vec<&CommittedTx> = whole
        .txs
        .iter()
        .filter(|tx| tx.end().next().get() <= cut)
        .collect();
    if !same_txs(&found.txs, &expected) {
        violation("a truncated WAL is not a prefix of the whole one");
    }
}

/// Scans to the committed end at frame index `frame` and holds the result to the whole scan.
fn check_end(wal: &Wal, reader: &mut WalReader<Probe>, whole: &Scan, frame: u32) {
    let limit = wal.end_at(frame);
    wal.shared.reset();
    let found = scan(reader, wal.start(), limit, wal.steps());
    let end = WalOffset::from_frame_index(frame, wal.page_size()).get();
    if wal.shared.stats().furthest > end {
        violation("a scan under a committed end read at or past the end");
    }
    if !same_txs(&found.txs, &ending_by(whole, frame)) {
        violation("a committed end gave other transactions than the whole scan");
    }
    let bound = wal.bound_at(frame).and_then(|at| wal.bounds.get(at));
    match (bound, &found.ending) {
        (
            Some(bound),
            Ending::Stopped {
                at,
                tail: Tail::Clean,
            },
        ) if at == bound => {}
        (None, Ending::Failed(_)) => {}
        (bound, ending) => violation(&format!(
            "committed end at frame {frame} (a boundary: {}) ended {ending:?}",
            bound.is_some()
        )),
    }
}

/// Committed ends: one derived from the scan, one anywhere, one from another
/// generation, and a cursor past its end.
fn check_committed(wal: &Wal, reader: &mut WalReader<Probe>, whole: &Scan, params: &Params) {
    let pick = usize::from(params.pick)
        .checked_rem(wal.bounds.len())
        .unwrap_or(0);
    if let Some(bound) = wal.bounds.get(pick) {
        check_end(wal, reader, whole, bound.next().frame_index());
    }
    let anywhere = u32::from(params.end)
        .checked_rem(wal.frames.saturating_add(2))
        .unwrap_or(0);
    check_end(wal, reader, whole, anywhere);
    let other = Salts {
        salt1: wal.salts().salt1 ^ 1,
        salt2: wal.salts().salt2,
    };
    let other_limit = ScanLimit::Committed(CommittedEnd::new(anywhere, other, wal.page_size()));
    wal.shared.reset();
    let result = reader.next_tx(&wal.start(), other_limit);
    if !matches!(result, Err(WalError::Restarted { .. })) || wal.shared.stats().delivered != 0 {
        violation(&format!("an end of another generation gave {result:?}"));
    }
    if let Some(last) = wal
        .bounds
        .last()
        .filter(|last| last.next().frame_index() > 0)
    {
        let result = reader.next_tx(last, wal.end_at(0));
        if !matches!(result, Err(WalError::CursorPastEnd { .. })) {
            violation(&format!("a cursor past the committed end gave {result:?}"));
        }
    }
}

/// Every boundary resumes to the scan's own cursor; a resumed position inside the
/// scanned region is a boundary; scanning on from it gives the rest of the scan.
fn check_resume(wal: &Wal, reader: &mut WalReader<Probe>, whole: &Scan, params: &Params) {
    for (at, bound) in wal.bounds.iter().enumerate() {
        let resumed = reader.resume(bound.next(), wal.salts(), unbounded(wal.salts()));
        if resumed.as_ref().ok() != Some(bound) {
            violation(&format!("resume at a boundary gave {resumed:?}"));
        }
        let before = at.checked_sub(1).and_then(|tx| whole.txs.get(tx));
        if let Some(last) = before.and_then(|tx| tx.frames().last()) {
            let found = reader.frame_before(bound.next(), wal.salts(), unbounded(wal.salts()));
            if found.as_ref().ok() != Some(last) {
                violation(&format!("frame_before a boundary gave {found:?}"));
            }
        }
    }
    let other = Salts {
        salt1: wal.salts().salt1,
        salt2: wal.salts().salt2 ^ 1,
    };
    let restarted = reader.resume(WalOffset::start(wal.page_size()), other, unbounded(other));
    if !matches!(restarted, Err(ResumeError::Restarted { .. })) {
        violation(&format!("resume in another generation gave {restarted:?}"));
    }
    let frame = u32::from(params.resume)
        .checked_rem(wal.frames.saturating_add(2))
        .unwrap_or(0);
    let last = wal.bounds.last().map_or(0, |b| b.next().frame_index());
    let resumed = reader.resume(
        WalOffset::from_frame_index(frame, wal.page_size()),
        wal.salts(),
        unbounded(wal.salts()),
    );
    match (wal.bound_at(frame), resumed) {
        (Some(at), Ok(cursor)) => check_rest(wal, reader, whole, at, cursor),
        (None, Ok(cursor)) if frame > last => {
            std::hint::black_box(scan(
                reader,
                cursor,
                ScanLimit::FirstInvalidFrame,
                wal.steps(),
            ));
        }
        (None, Err(_)) => {}
        (bound, resumed) => violation(&format!(
            "resume at frame {frame} (boundary {bound:?}, last {last}) gave {resumed:?}"
        )),
    }
}

/// Scanning on from boundary `at` gives the whole scan's transactions from there and its end.
fn check_rest(
    wal: &Wal,
    reader: &mut WalReader<Probe>,
    whole: &Scan,
    at: usize,
    cursor: FrameCursor,
) {
    let rest = scan(reader, cursor, ScanLimit::FirstInvalidFrame, wal.steps());
    let expected: Vec<&CommittedTx> = whole.txs.iter().skip(at).collect();
    if !same_txs(&rest.txs, &expected) || rest.stopped() != whole.stopped() {
        violation("a scan resumed at a boundary differs from the whole scan after it");
    }
}

/// The map of the whole scan is the newest frame per page within the last
/// commit, as a plain model computes it; skipped and repeated transactions are refused.
fn check_page_map(wal: &Wal, whole: &Scan) {
    let mut map = PageMap::new(wal.page_size());
    let mut model: BTreeMap<u32, FrameRef> = BTreeMap::new();
    for tx in &whole.txs {
        if let Err(error) = map.apply(tx) {
            violation(&format!(
                "the scan's own transaction does not apply: {error:?}"
            ));
        }
        for frame in tx.frames() {
            model.insert(frame.pgno().get(), *frame);
        }
        let commit = tx.commit();
        model.retain(|pgno, _| *pgno <= commit.get());
        if map.commit() != Some(commit) || map.pages().any(|f| f.pgno().get() > commit.get()) {
            violation("the page map holds a page above its commit");
        }
    }
    if !map.pages().eq(model.values()) {
        violation("the page map is not the newest frame of each page");
    }
    let (Some(first), Some(last)) = (whole.txs.first(), whole.txs.last()) else {
        return;
    };
    let span = first
        .frames()
        .first()
        .map(|f| (f.offset(), last.end().next()));
    if Some(map.range()) != span || map.salts() != Some(wal.salts()) {
        violation("the page map's range or generation is not the scan's");
    }
    if !matches!(map.apply(last), Err(PageMapError::NotAfterEnd { .. })) {
        violation("a transaction applied twice was accepted");
    }
    let mut skipping = PageMap::new(wal.page_size());
    if let (Ok(()), Some(third)) = (skipping.apply(first), whole.txs.get(2))
        && !matches!(skipping.apply(third), Err(PageMapError::Gap { .. }))
    {
        violation("a transaction after a skipped one was accepted");
    }
    let other = PageSize::new(if wal.page_size().get() == 512 {
        1024
    } else {
        512
    });
    if let Some(other) = other
        && !matches!(
            PageMap::new(other).apply(first),
            Err(PageMapError::PageSizeMismatch { .. })
        )
    {
        violation("a transaction of another page size was accepted");
    }
}

/// Every scanned page reads back to its digest.
fn check_pages(reader: &mut WalReader<Probe>, frames: &[FrameRef], buf: &mut [u8]) {
    for frame in frames {
        if let Err(error) = reader.read_page(frame, buf) {
            violation(&format!("a scanned page does not read back: {error:?}"));
        }
        if PageChecksum::of(frame.pgno(), buf) != frame.digest() {
            violation("a page read back does not match its digest");
        }
    }
}

/// `read_page` returns every scanned page; once a byte of one page flips, that
/// page is refused, the others are not, and a new scan stops at its frame.
fn check_read_page(wal: &Wal, reader: &mut WalReader<Probe>, whole: &Scan, params: &Params) {
    let frames: Vec<FrameRef> = whole
        .txs
        .iter()
        .flat_map(|tx| tx.frames())
        .copied()
        .collect();
    let mut buf = vec![0; wal.page_size().bytes()];
    check_pages(reader, &frames, &mut buf);
    let Some(target) = frames.get(
        usize::from(params.flip_frame)
            .checked_rem(frames.len())
            .unwrap_or(0),
    ) else {
        return;
    };
    let mut wrong = vec![0; wal.page_size().bytes().saturating_add(1)];
    if !matches!(
        reader.read_page(target, &mut wrong),
        Err(ReadPageError::BufferSize { .. })
    ) {
        violation("read_page accepted a buffer that is not one page");
    }
    let within = usize::from(params.flip_at)
        .checked_rem(wal.page_size().bytes())
        .unwrap_or(0)
        .saturating_add(FRAME_HEADER_LEN);
    let at = target
        .offset()
        .get()
        .saturating_add(u64::try_from(within).unwrap_or(u64::MAX));
    let mask = params.flip_mask.max(1);
    if !wal.shared.flip(at, mask) {
        violation("a scanned frame lies outside the file");
    }
    match reader.read_page(target, &mut buf) {
        Err(ReadPageError::ChangedSinceScan { pgno, offset })
            if pgno == target.pgno()
                && offset == target.offset()
                && buf.iter().all(|b| *b == 0) => {}
        other => violation(&format!(
            "a page changed after the scan read back as {other:?}"
        )),
    }
    let others: Vec<FrameRef> = frames
        .iter()
        .filter(|f| f.offset() != target.offset())
        .copied()
        .collect();
    check_pages(reader, &others, &mut buf);
    check_flipped_scan(wal, whole, target);
    wal.shared.flip(at, mask);
    check_pages(reader, &[*target], &mut buf);
}

/// A changed page breaks its frame's checksum (SQLite's checksum maps any one
/// changed word to a different sum), so a scan stops exactly before its
/// transaction, and a committed end past it reports the corruption there.
fn check_flipped_scan(wal: &Wal, whole: &Scan, target: &FrameRef) {
    let Some(mut reader) = open(&wal.shared) else {
        violation("a WAL with a page byte changed no longer opens");
    };
    let frame = target.offset().frame_index();
    let expected = ending_by(whole, frame);
    let resumes_at = expected.last().map_or(wal.start(), |tx| *tx.end());
    let found = scan(
        &mut reader,
        wal.start(),
        ScanLimit::FirstInvalidFrame,
        wal.steps(),
    );
    let stop = Some((resumes_at, Tail::InvalidFrame(FrameFault::ChecksumMismatch)));
    if !same_txs(&found.txs, &expected) || found.stopped() != stop {
        violation(&format!(
            "a flipped page byte gave {} transactions, {:?}",
            found.txs.len(),
            found.ending
        ));
    }
    let Some(last) = wal.bounds.last() else {
        return;
    };
    let found = scan(
        &mut reader,
        wal.start(),
        wal.end_at(last.next().frame_index()),
        wal.steps(),
    );
    let corrupt = matches!(
        found.ending,
        Ending::Failed(WalError::CorruptBelowCommitted { offset, fault: FrameFault::ChecksumMismatch })
            if offset == target.offset()
    );
    if !same_txs(&found.txs, &expected) || !corrupt {
        violation(&format!(
            "a flipped page byte below the committed end gave {:?}",
            found.ending
        ));
    }
}

/// Reads a WAL every way the reader offers and holds each to the whole scan.
///
/// Input: [`WAL_PREFIX_LEN`] parameter bytes, then the WAL file. The mode
/// byte's low three bits pick a page size (512 shifted left by them), and
/// [`WAL_BIG_ENDIAN`], [`WAL_REWRITE_HEADER`] and [`WAL_RECHAIN`] fix up the
/// file before it is read. Then big-endian: a `u16` truncation point, a `u16`
/// committed-end frame index, a byte picking one of the scan's boundaries, a
/// `u16` resume frame index, a byte picking a scanned frame, a `u16` byte
/// offset in its page and a byte to XOR there (zero counts as one). Each value
/// is reduced modulo what it indexes.
///
/// Nothing may panic, loop or read a byte more than once per scan. Under
/// [`ScanLimit::FirstInvalidFrame`] a truncated copy gives a prefix of the
/// whole file's transactions; a committed end gives exactly those before it
/// and reads nothing at or past it; the page map never holds a page above
/// the last commit; every scanned page reads back to its digest, and fails
/// with `ChangedSinceScan` after a byte of it flips.
pub fn wal(data: &[u8]) {
    let mut input = Reader::new(data);
    let Some(params) = Params::parse(&mut input) else {
        return;
    };
    let mut file = input.rest().to_vec();
    if params.mode & WAL_REWRITE_HEADER != 0 {
        rewrite_header(&mut file, params.mode);
    }
    if params.mode & WAL_RECHAIN != 0 {
        rechain(&mut file);
    }
    let shared = Shared::new(file);
    let Some(mut reader) = open(&shared) else {
        return;
    };
    let header = *reader.header();
    let frames = frames_in(shared.len(), header.page_size());
    let whole = whole_scan(&mut reader, &shared, frames);
    let bounds = std::iter::once(FrameCursor::at_start(&header))
        .chain(whole.txs.iter().map(|tx| *tx.end()))
        .collect();
    let wal = Wal {
        shared,
        header,
        frames,
        bounds,
    };
    check_truncation(&wal, &whole, &params);
    check_committed(&wal, &mut reader, &whole, &params);
    check_resume(&wal, &mut reader, &whole, &params);
    check_page_map(&wal, &whole);
    check_read_page(&wal, &mut reader, &whole, &params);
}

#[cfg(test)]
mod tests;

/// A committed end past any frame of generation `salts`: the oracle here compares a
/// resume with a scan of the whole file, which no wal-index read bounds.
fn unbounded(salts: Salts) -> lsr_wal::CommittedEnd {
    lsr_wal::CommittedEnd::new(u32::MAX, salts, lsr_ltx::PageSize::MIN)
}

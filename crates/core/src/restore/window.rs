//! A restore file read through a window of ranged reads ([`ObjectStore::read_span`]), for a
//! store that downloads several streams of one object faster than one ([`BatchKind::Spans`]).
//!
//! The file is cut into spans of [`SPAN_BYTES`] and up to the store's width of them are open at
//! once, each downloading ahead while the restore takes the one at the front. Bytes are handed out
//! in file order, so the decoder, its checks and the sink's writes are those of a single stream:
//! nothing here decides what a byte means, and pages are written in order (out-of-order writes
//! cost two to three times the kernel time on APFS). Memory is the open spans: the width times
//! one span, as the store bounds a span's read-ahead.
//!
//! The listed size is advisory, as for one stream. The last span reads to the object's end
//! ([`ObjectStore::read_stream`]), so an object longer than listed is read whole, and a span that
//! ends before its end is the end of the object: the spans after it are dropped unread.

use std::collections::VecDeque;

use crate::layout::{FileInfo, FileKey};
use crate::store::{
    BatchKind, BoxFuture, ObjectStore, ReadLen, ReadStream, SPAN_TO_END, StoreError,
};

/// Bytes a span covers: large enough that a request's round trip is small beside its transfer,
/// small enough that the store's width of them stays a bounded share of memory (40 MiB at S3's
/// default concurrency of 5).
pub(super) const SPAN_BYTES: u64 = 8 * 1024 * 1024;

/// One stream of `info` for a restore: a window of spans when the store reads several at once
/// and the file holds at least two spans, else the store's one stream.
pub(super) fn stream<'s, S: ObjectStore>(
    store: &'s S,
    info: &FileInfo,
) -> Box<dyn ReadStream + 's> {
    let width = store.batch_width(BatchKind::Spans).get();
    if width < 2 || info.size < SPAN_BYTES.saturating_mul(2) {
        return store.read_stream(info.key, 0);
    }
    Box::new(Window::new(store, info.key, info.size, width, SPAN_BYTES))
}

/// How many files ahead of the one being applied a restore opens: Go's own bound on the files it
/// opens at once (`restoreOpenConcurrency` in `replica.go`).
pub(super) const LOOKAHEAD: usize = 32;

/// The largest listed size of a file opened ahead, which a store reads ahead whole in one piece
/// ([`ReadLen::CHUNK`]): the memory of the files open ahead is at most [`LOOKAHEAD`] of them. A
/// larger file's transfer outweighs its round trip, and one of two spans or more has its window.
pub(super) const AHEAD_BYTES: u64 = 1024 * 1024;

/// The files of a restore in order, each with its stream: the small ones among the next
/// `depth` already opened ([`ObjectStore::read_span`] to [`SPAN_TO_END`]), the rest opened as
/// [`stream`] opens them, when their turn comes.
pub(super) struct Lookahead<'s, S> {
    store: &'s S,
    files: &'s [FileInfo],
    depth: usize,
    /// Largest listed size opened ahead.
    small: u64,
    /// The file to hand out next.
    at: usize,
    /// The next file to open ahead.
    opened: usize,
    /// One entry per file from `at` to `opened`: its stream, or `None` for a file left to open
    /// at its turn.
    ready: VecDeque<Option<Box<dyn ReadStream + 's>>>,
}

impl<'s, S: ObjectStore> Lookahead<'s, S> {
    /// The files from `from` on, [`LOOKAHEAD`] ahead when the store reads several streams at
    /// once ([`BatchKind::Spans`], as for a window) and none otherwise.
    pub(super) fn new(store: &'s S, files: &'s [FileInfo], from: usize) -> Self {
        let wide = store.batch_width(BatchKind::Spans).get() >= 2;
        Self::with(
            store,
            files,
            from,
            if wide { LOOKAHEAD } else { 0 },
            AHEAD_BYTES,
        )
    }

    pub(super) const fn with(
        store: &'s S,
        files: &'s [FileInfo],
        from: usize,
        depth: usize,
        small: u64,
    ) -> Self {
        Self {
            store,
            files,
            depth,
            small,
            at: from,
            opened: from,
            ready: VecDeque::new(),
        }
    }

    /// The next file, its index and its stream, after opening the small files up to `depth` ahead.
    pub(super) fn next_file(&mut self) -> Option<(usize, &'s FileInfo, Box<dyn ReadStream + 's>)> {
        while self.ready.len() < self.depth {
            let Some(info) = self.files.get(self.opened) else {
                break;
            };
            self.ready.push_back(
                (info.size <= self.small).then(|| self.store.read_span(info.key, 0, SPAN_TO_END)),
            );
            self.opened = self.opened.saturating_add(1);
        }
        let index = self.at;
        let info = self.files.get(index)?;
        self.at = index.saturating_add(1);
        let stream = match self.ready.pop_front().flatten() {
            Some(stream) => stream,
            None => stream(self.store, info),
        };
        Some((index, info, stream))
    }
}

/// An open span: its stream and the bytes it has still to hand out.
struct Open<'s> {
    stream: Box<dyn ReadStream + 's>,
    left: u64,
}

/// See the module doc.
pub(super) struct Window<'s, S> {
    store: &'s S,
    key: FileKey,
    /// Where the next span to open starts; past `size` once the last is open.
    next: u64,
    /// The listed size.
    size: u64,
    width: usize,
    /// Bytes a span covers: [`SPAN_BYTES`] but in tests.
    span: u64,
    open: VecDeque<Open<'s>>,
    /// The object ended, or a read failed.
    ended: bool,
}

impl<'s, S: ObjectStore> Window<'s, S> {
    pub(super) const fn new(
        store: &'s S,
        key: FileKey,
        size: u64,
        width: usize,
        span: u64,
    ) -> Self {
        Self {
            store,
            key,
            next: 0,
            size,
            width,
            span,
            open: VecDeque::new(),
            ended: false,
        }
    }

    /// Opens spans until `width` are open or the last one is.
    fn fill(&mut self) {
        while self.open.len() < self.width && self.next <= self.size {
            let start = self.next;
            let end = start.saturating_add(self.span);
            if end >= self.size {
                self.open.push_back(Open {
                    stream: self.store.read_stream(self.key, start),
                    left: u64::MAX,
                });
                self.next = u64::MAX;
                return;
            }
            // One GET per 8 MiB where one stream makes one GET per file: requests traded for wall
            // time, as the owner approved (docs/decisions/D312.md). A 68 GB restore makes about
            // 8,500 of them, under a cent at S3's read price. One stream from Linode to Tigris ran
            // at a median 89 MiB/s, four at 276 and eight at 364.
            self.open.push_back(Open {
                stream: self.store.read_span(self.key, start, end),
                left: end.saturating_sub(start),
            });
            self.next = end;
        }
    }

    /// The next `len` bytes, fewer only where the object ends.
    async fn take(&mut self, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let want = len.get();
        let mut out = Vec::new();
        while !self.ended {
            let have = u64::try_from(out.len()).unwrap_or(u64::MAX);
            let Some(need) = want.checked_sub(have).filter(|need| *need > 0) else {
                break;
            };
            self.fill();
            let Some(front) = self.open.front_mut() else {
                self.ended = true;
                break;
            };
            let ask = need.min(front.left);
            let Some(ask_len) = u32::try_from(ask).ok().and_then(ReadLen::new) else {
                self.open.pop_front();
                continue;
            };
            let piece = match front.stream.next(ask_len).await {
                Ok(piece) => piece,
                Err(err) => {
                    self.ended = true;
                    self.open.clear();
                    return Err(err);
                }
            };
            let got = u64::try_from(piece.len()).unwrap_or(u64::MAX);
            front.left = front.left.saturating_sub(got);
            if out.is_empty() {
                // A piece that is all the caller asked for is handed on, not copied.
                out = piece;
            } else {
                out.extend_from_slice(&piece);
            }
            if got < ask {
                // A span short of its end: the object ended there.
                self.ended = true;
                self.open.clear();
            } else if front.left == 0 {
                self.open.pop_front();
            }
        }
        Ok(out)
    }
}

impl<S: ObjectStore> ReadStream for Window<'_, S> {
    fn next(&mut self, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(self.take(len))
    }
}

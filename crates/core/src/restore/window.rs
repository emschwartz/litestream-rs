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
use crate::store::{BatchKind, BoxFuture, ObjectStore, ReadLen, ReadStream, StoreError};

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
            // time, as the owner approved (docs/decisions/D309.md). A 68 GB restore makes about
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

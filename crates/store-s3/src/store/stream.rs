//! [`S3Stream`]: an object from an offset to its end as one GET, its body read as the
//! caller takes pieces ([`ObjectStore::read_stream`](lsr_core::ObjectStore::read_stream)).
//!
//! The GET goes out at the first piece. A success's body is read from the connection a piece
//! at a time, so memory is one piece whatever the object's size, and the transport bounds the
//! body by progress and by its declared length at the floor rate (`Bounds`), never by
//! `MAX_BODY`. Any other answer is read whole and judged as a ranged read's is. The body must
//! reach the length it declared: a connection that ends before is `Transient`, never the end
//! of the object, so a retry goes on from the bytes already handed out instead of taking a
//! short file. A server that ignores the `Range` of a stream opened past the start sends the
//! whole object, and the bytes before the offset are skipped.

use std::io;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::thread;

use lsr_core::{BoxFuture, FileKey, ReadLen, ReadStream, StoreError, StoreOp};

use super::Inner;
use crate::classify::classify_transport;
use crate::transport::{Body, MAX_BODY, Transport};

/// One object read through one GET. It borrows nothing, so a reader can hold it between
/// ticks ([`ObjectStore::hold_stream`](lsr_core::ObjectStore::hold_stream)).
pub(crate) struct S3Stream<T> {
    inner: Arc<Inner<T>>,
    key: FileKey,
    from: u64,
    /// Where it ends (exclusive) for a span, `None` for the rest of the object.
    end: Option<u64>,
    state: State,
}

enum State {
    Unsent,
    /// The body, and how much of it the response declared that is still to come.
    Open {
        body: Box<dyn Body>,
        left: Option<u64>,
    },
    /// The object ended, or a failure spent the stream.
    Done,
}

impl<T: Transport> S3Stream<T> {
    pub(crate) const fn new(inner: Arc<Inner<T>>, key: FileKey, from: u64) -> Self {
        Self {
            inner,
            key,
            from,
            end: None,
            state: State::Unsent,
        }
    }

    /// Bytes `from` to `end` (exclusive) through one GET.
    pub(crate) const fn span(inner: Arc<Inner<T>>, key: FileKey, from: u64, end: u64) -> Self {
        Self {
            inner,
            key,
            from,
            end: Some(end),
            state: State::Unsent,
        }
    }

    fn open(&self) -> Result<State, StoreError> {
        let op = StoreOp::Read;
        let key = self.key;
        let request = self.inner.request(op, |b, now| match self.end {
            None => b.get_from(now, &key, self.from),
            Some(end) => b.get_span(now, &key, self.from, end),
        })?;
        let (mut head, mut body) = self
            .inner
            .transport
            .open(request)
            .map_err(|err| classify_transport(op, &err))?;
        if !(200..300).contains(&head.status()) {
            // An error's body names its code.
            consume(&mut *body, MAX_BODY, |bytes| {
                head.body.extend_from_slice(bytes)
            })?;
        }
        let Some(partial) = Inner::<T>::range_head(&key, self.from, &head)? else {
            return Ok(State::Done);
        };
        let mut left = head
            .header("content-length")
            .and_then(|value| value.trim().parse::<u64>().ok());
        if !partial && self.from > 0 {
            // A server that ignored the Range header sends the whole object.
            consume(&mut *body, self.from, |_| {})?;
            left = left.map(|declared| declared.saturating_sub(self.from));
        }
        if let Some(end) = self.end {
            // A whole object sent for a span runs past it: what is past `end` is never read.
            let span = end.saturating_sub(self.from);
            left = Some(left.map_or(span, |left| left.min(span)));
        }
        Ok(State::Open { body, left })
    }

    /// The next `len` bytes, fewer only at the end.
    fn piece(&mut self, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        if matches!(self.state, State::Unsent) {
            // A failed open spends the stream: the caller opens another.
            self.state = State::Done;
            self.state = self.open()?;
        }
        let State::Open { body, left } = &mut self.state else {
            return Ok(Vec::new());
        };
        let want = left.map_or(len.get(), |left| left.min(len.get()));
        let mut out = Vec::with_capacity(usize::try_from(want).unwrap_or(0));
        let read = consume(&mut **body, want, |bytes| out.extend_from_slice(bytes));
        let got = u64::try_from(out.len()).unwrap_or(u64::MAX);
        *left = left.map(|left| left.saturating_sub(got));
        let ended = got < want;
        if read.is_err() || (ended && left.is_some_and(|left| left > 0)) {
            self.state = State::Done;
            read?;
            return Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::UnexpectedEof,
            });
        }
        if ended || *left == Some(0) {
            self.state = State::Done;
        }
        Ok(out)
    }
}

impl<T: Transport> ReadStream for S3Stream<T> {
    fn next(&mut self, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move { self.piece(len) })
    }
}

/// Reads up to `count` bytes of `body` into `sink`; fewer only when the body ends first.
fn consume(body: &mut dyn Body, count: u64, mut sink: impl FnMut(&[u8])) -> Result<(), StoreError> {
    let mut buf = [0_u8; 16 * 1024];
    let mut left = count;
    while left > 0 {
        let take = usize::try_from(left).map_or(buf.len(), |left| left.min(buf.len()));
        let Some(window) = buf.get_mut(..take) else {
            break;
        };
        let read = body
            .read(window)
            .map_err(|err| classify_transport(StoreOp::Read, &err))?;
        let Some(bytes) = window.get(..read).filter(|bytes| !bytes.is_empty()) else {
            break;
        };
        sink(bytes);
        left = left.saturating_sub(u64::try_from(read).unwrap_or(u64::MAX));
    }
    Ok(())
}

/// The most pieces a span's thread reads ahead: a restore's 8 MiB span whole, so it downloads
/// while the spans before it are taken, and never more than 16 MiB however long a span is asked.
const AHEAD_PIECES: u64 = 16;

/// Bytes a span's thread reads at a time.
const AHEAD_PIECE: ReadLen = ReadLen::CHUNK;

/// A span read ahead on a thread of its own ([`ObjectStore::read_span`](lsr_core::ObjectStore::read_span)):
/// one GET, sent when the span is opened, its pieces queued for the caller [`AHEAD_PIECES`] deep,
/// so a restore that opens several spans has them all downloading while it takes each in turn.
/// Memory per span: the queued pieces (up to [`AHEAD_PIECES`]), the one the thread is reading
/// and the one being handed out.
/// The thread ends when the span does, when a read fails (the failure is the last piece), or at
/// its next piece once the caller drops the span; a read in progress first runs to its end or to
/// the transport's deadline.
pub(crate) struct Ahead {
    pieces: Receiver<Result<Vec<u8>, StoreError>>,
    held: Vec<u8>,
    at: usize,
    ended: bool,
}

impl Ahead {
    pub(crate) fn spawn<T: Transport + 'static>(
        inner: Arc<Inner<T>>,
        key: FileKey,
        start: u64,
        end: u64,
    ) -> Self {
        // The whole span waits read ahead, up to [`AHEAD_PIECES`].
        let depth = end
            .saturating_sub(start)
            .div_ceil(AHEAD_PIECE.get())
            .clamp(1, AHEAD_PIECES);
        let (queue, pieces) = mpsc::sync_channel(usize::try_from(depth).unwrap_or(1));
        let mut stream = S3Stream::span(inner, key, start, end);
        let full = usize::try_from(AHEAD_PIECE.get()).unwrap_or(usize::MAX);
        // A thread that cannot start drops the sender: the span's first piece fails as Transient.
        let spawned = thread::Builder::new()
            .name("lsr-span".to_owned())
            .spawn(move || {
                loop {
                    let piece = stream.piece(AHEAD_PIECE);
                    let last = !matches!(&piece, Ok(bytes) if bytes.len() == full);
                    if queue.send(piece).is_err() || last {
                        return;
                    }
                }
            });
        match spawned {
            Ok(_) | Err(_) => {}
        }
        Self {
            pieces,
            held: Vec::new(),
            at: 0,
            ended: false,
        }
    }

    /// The next `len` bytes, fewer only at the span's end.
    fn take(&mut self, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let want = usize::try_from(len.get()).unwrap_or(usize::MAX);
        let mut out = Vec::new();
        while out.len() < want {
            if let Some(rest) = self.held.get(self.at..).filter(|rest| !rest.is_empty()) {
                let take = rest.len().min(want.saturating_sub(out.len()));
                out.extend_from_slice(rest.get(..take).unwrap_or_default());
                self.at = self.at.saturating_add(take);
                continue;
            }
            if self.ended {
                break;
            }
            match self.pieces.recv() {
                Ok(Ok(piece)) => {
                    self.ended = u64::try_from(piece.len()).unwrap_or(u64::MAX) < AHEAD_PIECE.get();
                    self.held = piece;
                    self.at = 0;
                }
                Ok(Err(err)) => {
                    self.ended = true;
                    return Err(err);
                }
                Err(_gone) => {
                    // The thread never started, or stopped without a last piece.
                    self.ended = true;
                    return Err(StoreError::Transient {
                        op: StoreOp::Read,
                        kind: io::ErrorKind::Other,
                    });
                }
            }
        }
        Ok(out)
    }
}

impl ReadStream for Ahead {
    fn next(&mut self, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move { self.take(len) })
    }
}

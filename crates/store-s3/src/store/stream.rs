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

use lsr_core::{BoxFuture, FileKey, ReadLen, ReadStream, StoreError, StoreOp};

use super::Inner;
use crate::classify::classify_transport;
use crate::transport::{Body, MAX_BODY, Transport};

/// One object read through one GET. It borrows nothing, so a reader can hold it between
/// ticks ([`ObjectStore::hold_stream`](lsr_core::ObjectStore::hold_stream)).
pub(crate) struct S3Stream<T> {
    inner: Arc<Inner<T>>,
    key: FileKey,
    /// Where the GET starts.
    from: u64,
    state: State,
}

enum State {
    /// No request yet.
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
            state: State::Unsent,
        }
    }

    /// Sends the GET and judges its head.
    fn open(&self) -> Result<State, StoreError> {
        let op = StoreOp::Read;
        let key = self.key;
        let request = self
            .inner
            .request(op, |b, now| b.get_from(now, &key, self.from))?;
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
            // The connection failed, or ended before the body did.
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

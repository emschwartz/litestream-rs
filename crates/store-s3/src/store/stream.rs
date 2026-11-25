//! [`S3Stream`]: an object from an offset to its end as one GET, its body read as the
//! caller takes pieces ([`ObjectStore::read_stream`](lsr_core::ObjectStore::read_stream)).
//!
//! The GET goes out at the first piece. A success's body is read from the connection a piece
//! at a time, so memory is one piece whatever the object's size, and the transport bounds the
//! body by progress and by its declared length at the floor rate (`Bounds`), never by
//! `MAX_BODY`. Any other answer is judged as a ranged read's is. The body must reach the length
//! it declared: a connection that ends before is `Transient`, never the end of the object, so
//! a retry goes on from the bytes already handed out instead of taking a short file. A server
//! that ignores the `Range` of a stream opened past the start sends the whole object, and the
//! bytes before the offset are skipped.

use std::io;

use lsr_core::{BoxFuture, FileKey, ReadLen, ReadStream, StoreError, StoreOp};

use super::{Inner, content_range_start};
use crate::classify::{classify, classify_transport, is_range_past_end};
use crate::transport::{Body, Opened, Transport};

/// One object read through one GET.
pub(crate) struct S3Stream<'a, T> {
    inner: &'a Inner<T>,
    key: FileKey,
    /// Where the GET starts.
    from: u64,
    state: State<'a>,
}

enum State<'a> {
    /// No request yet.
    Unsent,
    /// The body, and how much of it the response declared that is still to come.
    Open {
        body: Box<dyn Body + 'a>,
        left: Option<u64>,
    },
    /// The object ended, or a failure spent the stream.
    Done,
}

impl<'a, T: Transport> S3Stream<'a, T> {
    pub(crate) const fn new(inner: &'a Inner<T>, key: FileKey, from: u64) -> Self {
        Self {
            inner,
            key,
            from,
            state: State::Unsent,
        }
    }

    /// Sends the GET and judges its head.
    fn open(&self) -> Result<State<'a>, StoreError> {
        let op = StoreOp::Read;
        let key = self.key;
        let request = self
            .inner
            .request(op, |b, now| b.get_from(now, &key, self.from))?;
        let opened = self
            .inner
            .transport
            .open(request)
            .map_err(|err| classify_transport(op, &err))?;
        let (head, mut body) = match opened {
            Opened::Whole(response) => {
                if is_range_past_end(&response) {
                    return Ok(State::Done);
                }
                classify(op, Some(&key), None, &response)?;
                // A success always streams; one held whole here is a transport's mistake.
                return Err(StoreError::Transient {
                    op,
                    kind: io::ErrorKind::InvalidData,
                });
            }
            Opened::Streaming(head, body) => (head, body),
        };
        classify(op, Some(&key), None, &head)?;
        let mut left = head
            .header("content-length")
            .and_then(|value| value.trim().parse::<u64>().ok());
        if head.status() == 206 {
            // A 206 for another range would hand back the wrong bytes under the right key.
            if head.header("content-range").and_then(content_range_start) != Some(self.from) {
                return Err(StoreError::Transient {
                    op,
                    kind: io::ErrorKind::InvalidData,
                });
            }
        } else if self.from > 0 {
            // A server that ignored the Range header sends the whole object.
            skip(&mut *body, self.from)?;
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
        let mut out = vec![0; usize::try_from(want).unwrap_or(0)];
        let mut filled = 0;
        let ended = loop {
            let Some(rest) = out.get_mut(filled..).filter(|rest| !rest.is_empty()) else {
                break false;
            };
            let read = match body.read(rest) {
                Ok(read) => read,
                Err(err) => {
                    self.state = State::Done;
                    return Err(classify_transport(StoreOp::Read, &err));
                }
            };
            if read == 0 {
                break true;
            }
            filled = filled.saturating_add(read);
        };
        out.truncate(filled);
        let got = u64::try_from(filled).unwrap_or(u64::MAX);
        *left = left.map(|left| left.saturating_sub(got));
        if ended && left.is_some_and(|left| left > 0) {
            // The connection ended before the body did.
            self.state = State::Done;
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

impl<T: Transport> ReadStream for S3Stream<'_, T> {
    fn next(&mut self, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move { self.piece(len) })
    }
}

/// Reads and drops `count` bytes of `body`; a body that ends first leaves nothing to read.
fn skip(body: &mut dyn Body, count: u64) -> Result<(), StoreError> {
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
        if read == 0 {
            break;
        }
        left = left.saturating_sub(u64::try_from(read).unwrap_or(u64::MAX));
    }
    Ok(())
}

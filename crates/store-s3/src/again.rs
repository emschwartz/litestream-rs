//! Parts an upload was handed and let go of, and how their bytes are read back.
//!
//! An upload written with [`Upload::write_from`](lsr_core::Upload::write_from) keeps no
//! part buffer. Of each part it keeps a [`Run`]: where the part lies in the object, a reader
//! of the caller's source that the part owns ([`Again`], asked for when the part's first byte
//! arrives), and for every [`CELL`] bytes what was seen of them ([`Seen`]: the length and a
//! CRC-64, 16 bytes for 256 KiB).
//!
//! A part is sent on one of the store's lanes (`lanes.rs`), and the thread that sends it
//! reads it twice more:
//! - the hash pass ([`Sealed::sha256`]) reads every cell for the SHA-256 the request signs
//!   and carries as `x-amz-content-sha256`;
//! - the send pass ([`Body`]) reads every cell again as the request's body.
//!
//! On both passes a cell's bytes are used only when they are what was seen on the first
//! read, checked here whatever the source checked. So the SHA-256 is taken over bytes that match
//! the first read cell by cell, the body is made of bytes that match it cell by cell, and
//! the store checks what it received against that hash. A cell that does not match, or that
//! the source does not give, ends the hash pass before a request is made, or ends the body
//! short of its `Content-Length`, which no server takes as a whole part; the part's result is
//! then the source's own error (`docs/decisions/D222.md`, `docs/decisions/D233.md`).
//!
//! Both passes stop between cells once the upload is over (failed, aborted or dropped).
//!
//! The source's future is driven to its end on the thread that sends the part, parked while
//! it is pending and unparked by its waker: the staging directory answers at once.

use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use lsr_core::{Again, Seeing, Seen, StoreError, StoreOp};
use sha2::{Digest as _, Sha256};

use crate::classify::invalid_data;
use crate::sigv4::PayloadHash;

/// Bytes one [`Seen`] covers, and so one second read asks for: 256 KiB, the size the engine
/// first read them in. A part being sent holds one cell's bytes at a time.
pub(crate) const CELL: u64 = 256 * 1024;

/// The cells of the largest part S3 takes (5 GiB): the most room a part's cells are given
/// up front, whatever part size the upload was asked for.
const MOST_CELLS: usize = 20_480;

/// A part being handed over: its place, what was seen of its cells and its reader.
pub(crate) struct Run {
    offset: u64,
    len: u64,
    cells: Vec<Seen>,
    cell: Seeing,
    reader: Box<dyn Again>,
}

/// A whole part, or an object's last one, let go of: what its request and its body need.
pub(crate) struct Sealed {
    offset: u64,
    len: u64,
    cells: Vec<Seen>,
    reader: Box<dyn Again>,
}

/// What a pass ends with when the upload is over: nobody reads it.
fn stopped() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Upload,
        kind: io::ErrorKind::Interrupted,
    }
}

/// One cell read again, given only when it is what was seen the first time.
fn cell(
    reader: &mut dyn Again,
    offset: u64,
    was: &Seen,
    stop: &AtomicBool,
) -> Result<Vec<u8>, StoreError> {
    if stop.load(Ordering::Relaxed) {
        return Err(stopped());
    }
    let bytes = block_on(reader.read(offset, was))?;
    // The source answers with the bytes seen or with an error. One that gives other bytes
    // broke that, and they must not be hashed or leave for the store.
    if Seen::of(&bytes) != *was {
        return Err(invalid_data(StoreOp::Upload));
    }
    Ok(bytes)
}

impl Run {
    /// A part that starts at `offset` in the object and holds at most `part` bytes, read
    /// again through `reader`: room for every cell is taken here, once.
    pub(crate) fn at(offset: u64, part: u64, reader: Box<dyn Again>) -> Self {
        let cells =
            usize::try_from(part.div_ceil(CELL)).map_or(MOST_CELLS, |cells| cells.min(MOST_CELLS));
        Self {
            offset,
            len: 0,
            cells: Vec::with_capacity(cells),
            cell: Seeing::new(),
            reader,
        }
    }

    /// How many bytes were taken in.
    pub(crate) const fn len(&self) -> u64 {
        self.len
    }

    /// The memory the cells hold.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn held(&self) -> usize {
        self.cells
            .capacity()
            .saturating_mul(std::mem::size_of::<Seen>())
    }

    /// Takes in the next bytes of the part.
    pub(crate) fn add(&mut self, mut bytes: &[u8]) {
        self.len = self
            .len
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        while !bytes.is_empty() {
            let room = usize::try_from(CELL.saturating_sub(self.cell.len())).unwrap_or(0);
            let (head, rest) = bytes.split_at(room.min(bytes.len()));
            self.cell.add(head);
            if self.cell.len() >= CELL {
                self.cells.push(std::mem::take(&mut self.cell).seen());
            }
            bytes = rest;
        }
    }

    /// The part, whole.
    pub(crate) fn seal(mut self) -> Sealed {
        if !self.cell.is_empty() {
            self.cells.push(self.cell.seen());
        }
        Sealed {
            offset: self.offset,
            len: self.len,
            cells: self.cells,
            reader: self.reader,
        }
    }
}

impl Sealed {
    /// The part's length.
    pub(crate) const fn len(&self) -> u64 {
        self.len
    }

    /// The memory the cells hold.
    pub(crate) fn held(&self) -> usize {
        self.cells
            .capacity()
            .saturating_mul(std::mem::size_of::<Seen>())
    }

    /// The hash pass: the SHA-256 of the part, over a second read of every cell.
    pub(crate) fn sha256(&mut self, stop: &AtomicBool) -> Result<PayloadHash, StoreError> {
        let mut hash = Sha256::new();
        let mut offset = self.offset;
        for was in &self.cells {
            hash.update(cell(&mut *self.reader, offset, was, stop)?);
            offset = offset.saturating_add(was.len());
        }
        Ok(PayloadHash::from_hasher(hash))
    }

    /// The send pass: the part's bytes, read again as they are sent.
    pub(crate) fn body<'a>(&'a mut self, stop: &'a AtomicBool) -> Body<'a> {
        Body {
            reader: &mut *self.reader,
            cells: self.cells.iter(),
            offset: self.offset,
            stop,
            cell: Vec::new(),
            given: 0,
            failed: None,
        }
    }
}

/// One part's request body: each cell read again when the one before it has left.
pub(crate) struct Body<'a> {
    reader: &'a mut dyn Again,
    cells: std::slice::Iter<'a, Seen>,
    /// Where the next cell starts in the object.
    offset: u64,
    stop: &'a AtomicBool,
    /// The cell being given out, and how much of it has been.
    cell: Vec<u8>,
    given: usize,
    failed: Option<StoreError>,
}

impl Body<'_> {
    /// Why a cell gave no bytes, when one did not: the part's result, whatever the request
    /// then came to.
    pub(crate) fn failed(self) -> Option<StoreError> {
        self.failed
    }

    /// Reads the next cell again; `false` when the part is whole.
    fn next_cell(&mut self) -> Result<bool, StoreError> {
        let Some(was) = self.cells.next() else {
            return Ok(false);
        };
        self.cell = cell(self.reader, self.offset, was, self.stop)?;
        self.offset = self.offset.saturating_add(was.len());
        self.given = 0;
        Ok(true)
    }
}

impl io::Read for Body<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.failed.is_some() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if self.given >= self.cell.len() {
            match self.next_cell() {
                Ok(true) => {}
                Ok(false) => return Ok(0),
                Err(err) => {
                    self.failed = Some(err);
                    // Never a clean end: the body stops short of the length it declared.
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        }
        let rest = self.cell.get(self.given..).unwrap_or_default();
        let count = rest.len().min(buf.len());
        for (to, from) in buf.iter_mut().zip(rest) {
            *to = *from;
        }
        self.given = self.given.saturating_add(count);
        Ok(count)
    }
}

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs a source's read to its end on the calling thread.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => thread::park(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use lsr_core::BoxFuture;

    use super::*;

    /// A reader over bytes a test holds, checking nothing: what it gives is its own affair.
    struct Gives(Vec<u8>);

    impl Again for Gives {
        fn read<'a>(
            &'a mut self,
            offset: u64,
            was: &'a Seen,
        ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
            let from = usize::try_from(offset).unwrap();
            let to = from.saturating_add(usize::try_from(was.len()).unwrap());
            let bytes = self.0[from..to].to_vec();
            Box::pin(std::future::ready(Ok(bytes)))
        }
    }

    fn none() -> Box<dyn Again> {
        Box::new(Gives(Vec::new()))
    }

    /// A part of `bytes` taken in whole, to be read back from `now`.
    fn sealed(bytes: &[u8], now: Vec<u8>) -> Sealed {
        let mut run = Run::at(0, u64::try_from(bytes.len()).unwrap(), Box::new(Gives(now)));
        run.add(bytes);
        run.seal()
    }

    fn bytes() -> Vec<u8> {
        (0..700_000_u32)
            .map(|n| n.to_le_bytes()[0] ^ 0x5a)
            .collect()
    }

    #[test]
    fn a_part_takes_room_for_its_cells_once_and_never_past_the_largest_part() {
        let held = |cells: usize| cells.saturating_mul(std::mem::size_of::<Seen>());
        assert_eq!(std::mem::size_of::<Seen>(), 16);
        assert_eq!(Run::at(0, 16 * 1024 * 1024, none()).held(), held(64));
        assert_eq!(Run::at(0, CELL.saturating_add(1), none()).held(), held(2));
        assert_eq!(Run::at(0, u64::MAX, none()).held(), held(MOST_CELLS));
    }

    #[test]
    fn both_passes_give_the_bytes_first_handed_over() {
        let bytes = bytes();
        let go = AtomicBool::new(false);
        let mut part = sealed(&bytes, bytes.clone());
        assert_eq!(part.len(), 700_000);
        assert_eq!(part.sha256(&go).unwrap(), PayloadHash::of(&bytes));
        let mut sent = Vec::new();
        let mut body = part.body(&go);
        body.read_to_end(&mut sent).unwrap();
        assert!(body.failed().is_none());
        assert_eq!(sent, bytes);
    }

    /// The source gives other bytes of the same length without saying so. Neither pass uses
    /// them: no digest is made of them, and the body ends short of them.
    #[test]
    fn a_cell_that_is_not_what_was_seen_is_neither_hashed_nor_sent() {
        let bytes = bytes();
        let go = AtomicBool::new(false);
        let mut changed = bytes.clone();
        // In the second cell.
        changed[300_000] ^= 1;
        let mut part = sealed(&bytes, changed);
        let unseen = StoreError::Transient {
            op: StoreOp::Upload,
            kind: io::ErrorKind::InvalidData,
        };
        assert_eq!(part.sha256(&go), Err(unseen.clone()));
        let mut sent = Vec::new();
        let mut body = part.body(&go);
        let ended = body.read_to_end(&mut sent).unwrap_err();
        assert_eq!(ended.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(body.failed(), Some(unseen));
        assert_eq!(sent, bytes[..262_144], "the cell before it and no more");
    }

    #[test]
    fn a_pass_stops_between_cells_once_the_upload_is_over() {
        let bytes = bytes();
        let over = AtomicBool::new(true);
        let mut part = sealed(&bytes, bytes.clone());
        assert_eq!(part.sha256(&over), Err(stopped()));
        let mut sent = Vec::new();
        let mut body = part.body(&over);
        body.read_to_end(&mut sent).unwrap_err();
        assert_eq!(body.failed(), Some(stopped()));
        assert!(sent.is_empty());
    }
}

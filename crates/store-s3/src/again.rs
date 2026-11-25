//! Parts an upload was handed and let go of, and the request body that reads them back.
//!
//! An upload written with [`Upload::write_from`](lsr_core::Upload::write_from) keeps no
//! part buffer. Of each part it keeps a [`Run`]: where the part lies in the object, its MD5
//! (the `Content-MD5` its request carries) and one [`ByteTally`] for every [`CELL`] bytes
//! of it, 72 bytes for 256 KiB. When the part is sent, a [`Body`] asks the caller's
//! [`Reread`] for each cell in turn, by its tally: the source answers with the bytes only
//! when they tally as they did the first time, so no byte of a second read leaves for the
//! store unchecked, and `Content-MD5`, taken over the first read, is checked by the store
//! against what it received. A cell the source does not give ends the body short of its
//! `Content-Length`, which no server takes as a whole part, and the source's error is the
//! part's result (`docs/decisions/D222.md`).
//!
//! The source's future is driven to its end on the thread that sends the part, parked while
//! it is pending and unparked by its waker: the staging directory answers at once.

use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use lsr_core::{ByteTally, Reread, StoreError, StoreOp};
use md5::{Digest as _, Md5};

use crate::classify::invalid_data;

/// Bytes one tally covers, and so one second read asks for: 256 KiB, the size the engine
/// first read them in. A part being sent holds one cell's bytes at a time.
pub(crate) const CELL: u64 = 256 * 1024;

/// The cells of the largest part S3 takes (5 GiB): the most room a part's tallies are
/// given up front, whatever part size the upload was asked for.
const MOST_CELLS: usize = 20_480;

/// A part being handed over: its place, its digest so far and the tallies of its cells.
pub(crate) struct Run {
    offset: u64,
    len: u64,
    md5: Md5,
    cells: Vec<ByteTally>,
    cell: ByteTally,
}

/// A whole part, or an object's last one, let go of: what its request and its body need.
pub(crate) struct Sealed {
    offset: u64,
    len: u64,
    md5: [u8; 16],
    cells: Vec<ByteTally>,
}

impl Run {
    /// A part that starts at `offset` in the object and holds at most `part` bytes: room
    /// for every cell's tally is taken here, once.
    pub(crate) fn at(offset: u64, part: u64) -> Self {
        let cells =
            usize::try_from(part.div_ceil(CELL)).map_or(MOST_CELLS, |cells| cells.min(MOST_CELLS));
        Self {
            offset,
            len: 0,
            md5: Md5::new(),
            cells: Vec::with_capacity(cells),
            cell: ByteTally::new(),
        }
    }

    /// How many bytes were taken in.
    pub(crate) const fn len(&self) -> u64 {
        self.len
    }

    /// Where the next part starts.
    pub(crate) const fn end(&self) -> u64 {
        self.offset.saturating_add(self.len)
    }

    /// The memory the tallies hold.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn held(&self) -> usize {
        self.cells
            .capacity()
            .saturating_mul(std::mem::size_of::<ByteTally>())
    }

    /// Takes in the next bytes of the part.
    pub(crate) fn add(&mut self, mut bytes: &[u8]) {
        self.md5.update(bytes);
        self.len = self
            .len
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        while !bytes.is_empty() {
            let room = usize::try_from(CELL.saturating_sub(self.cell.size())).unwrap_or(0);
            let (head, rest) = bytes.split_at(room.min(bytes.len()));
            self.cell.add(head);
            if self.cell.size() >= CELL {
                self.cells.push(std::mem::take(&mut self.cell));
            }
            bytes = rest;
        }
    }

    /// The part, whole.
    pub(crate) fn seal(mut self) -> Sealed {
        if self.cell.size() > 0 {
            self.cells.push(self.cell);
        }
        Sealed {
            offset: self.offset,
            len: self.len,
            md5: self.md5.finalize().into(),
            cells: self.cells,
        }
    }
}

impl Sealed {
    /// The part's length.
    pub(crate) const fn len(&self) -> u64 {
        self.len
    }

    /// The MD5 of the bytes first handed over.
    pub(crate) const fn md5(&self) -> [u8; 16] {
        self.md5
    }

    /// The memory the tallies hold.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn held(&self) -> usize {
        self.cells
            .capacity()
            .saturating_mul(std::mem::size_of::<ByteTally>())
    }

    /// The part's bytes, read again through `again` as they are sent.
    pub(crate) fn body<'a>(&'a self, again: &'a dyn Reread) -> Body<'a> {
        Body {
            again,
            cells: self.cells.iter(),
            offset: self.offset,
            cell: Vec::new(),
            given: 0,
            failed: None,
        }
    }
}

/// One part's request body: each cell read again when the one before it has left.
pub(crate) struct Body<'a> {
    again: &'a dyn Reread,
    cells: std::slice::Iter<'a, ByteTally>,
    /// Where the next cell starts in the object.
    offset: u64,
    /// The cell being given out, and how much of it has been.
    cell: Vec<u8>,
    given: usize,
    failed: Option<StoreError>,
}

impl Body<'_> {
    /// Why the source gave no bytes for a cell, when it did not: the part's result, whatever
    /// the request then came to.
    pub(crate) fn failed(self) -> Option<StoreError> {
        self.failed
    }

    /// Reads the next cell again; `false` when the part is whole.
    fn next_cell(&mut self) -> Result<bool, StoreError> {
        let Some(was) = self.cells.next() else {
            return Ok(false);
        };
        let bytes = block_on(self.again.read(self.offset, was))?;
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        // The source answers with the bytes tallied or with an error; a source that gives
        // another count broke that, and the body must not end short without saying so.
        if len != was.size() {
            return Err(invalid_data(StoreOp::Upload));
        }
        self.offset = self.offset.saturating_add(len);
        self.cell = bytes;
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
    use super::*;

    #[test]
    fn a_part_takes_room_for_its_tallies_once_and_never_past_the_largest_part() {
        let held = |cells: usize| cells.saturating_mul(std::mem::size_of::<ByteTally>());
        assert_eq!(Run::at(0, 16 * 1024 * 1024).held(), held(64));
        assert_eq!(Run::at(0, CELL.saturating_add(1)).held(), held(2));
        assert_eq!(Run::at(0, u64::MAX).held(), held(MOST_CELLS));
    }
}

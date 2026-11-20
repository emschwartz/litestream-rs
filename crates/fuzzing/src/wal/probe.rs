//! An in-memory WAL file the target can change under a reader, and that
//! records every byte the reader was handed.

use std::cell::{Cell, RefCell};
use std::io::{self, Read, Seek, SeekFrom};
use std::rc::Rc;

use lsr_wal::HEADER_LEN;

/// What the readers of one file were handed since the last [`Shared::reset`].
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Stats {
    /// Bytes delivered, summed over every read.
    pub(super) delivered: u64,
    /// One past the highest byte offset delivered; zero when nothing was.
    pub(super) furthest: u64,
}

/// The file bytes and the record of reads, shared by every probe over it.
#[derive(Debug)]
pub(super) struct Shared {
    file: Rc<RefCell<Vec<u8>>>,
    stats: Rc<Cell<Stats>>,
}

impl Shared {
    pub(super) fn new(file: Vec<u8>) -> Self {
        Self {
            file: Rc::new(RefCell::new(file)),
            stats: Rc::new(Cell::new(Stats::default())),
        }
    }

    /// A fresh reader positioned at the start.
    pub(super) fn probe(&self) -> Probe {
        Probe {
            file: Rc::clone(&self.file),
            stats: Rc::clone(&self.stats),
            pos: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.file.borrow().len()
    }

    /// A copy of the file as it is now.
    pub(super) fn bytes(&self) -> Vec<u8> {
        self.file.borrow().clone()
    }

    /// The first [`HEADER_LEN`] bytes, if the file holds that many.
    pub(super) fn header(&self) -> Option<[u8; HEADER_LEN]> {
        self.file.borrow().first_chunk::<HEADER_LEN>().copied()
    }

    pub(super) fn reset(&self) {
        self.stats.set(Stats::default());
    }

    pub(super) fn stats(&self) -> Stats {
        self.stats.get()
    }

    /// XORs `mask` into the byte at `at`; `false` when there is no such byte.
    pub(super) fn flip(&self, at: u64, mask: u8) -> bool {
        let mut file = self.file.borrow_mut();
        match usize::try_from(at).ok().and_then(|at| file.get_mut(at)) {
            Some(byte) => {
                *byte ^= mask;
                true
            }
            None => false,
        }
    }
}

/// A `Read + Seek` over a [`Shared`] file.
#[derive(Debug)]
pub(super) struct Probe {
    file: Rc<RefCell<Vec<u8>>>,
    stats: Rc<Cell<Stats>>,
    pos: u64,
}

impl Read for Probe {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let file = self.file.borrow();
        let available = usize::try_from(self.pos)
            .ok()
            .and_then(|pos| file.get(pos..))
            .unwrap_or_default();
        let n = buf.len().min(available.len());
        for (dst, src) in buf.iter_mut().zip(available).take(n) {
            *dst = *src;
        }
        let n64 = u64::try_from(n).unwrap_or(u64::MAX);
        let end = self.pos.saturating_add(n64);
        if n > 0 {
            let mut stats = self.stats.get();
            stats.delivered = stats.delivered.saturating_add(n64);
            stats.furthest = stats.furthest.max(end);
            self.stats.set(stats);
        }
        self.pos = end;
        Ok(n)
    }
}

impl Seek for Probe {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let len = u64::try_from(self.file.borrow().len()).unwrap_or(u64::MAX);
        let pos = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(delta) => len.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.pos.checked_add_signed(delta),
        };
        let pos = pos.ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.pos = pos;
        Ok(pos)
    }
}

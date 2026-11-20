//! Helpers shared by the tests: a hand-built WAL writer and scan drivers.
//!
//! The writer carries its own checksum so the crate is checked against an
//! independent implementation, not against itself.

use std::cell::Cell;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::rc::Rc;

use lsr_ltx::PageSize;
use lsr_wal::{CommittedTx, FrameCursor, ScanLimit, Tail, TxOutcome, WalError, WalReader};

pub type Mem = Cursor<Vec<u8>>;

pub fn size(bytes: u32) -> PageSize {
    PageSize::new(bytes).unwrap()
}

/// A page of deterministic content, different for every seed.
pub fn page(ps: u32, seed: u32) -> Vec<u8> {
    (0..ps)
        .map(|i| u8::try_from((i * 7 + seed * 31) % 251).unwrap())
        .collect()
}

pub fn sum(big: bool, mut s: (u32, u32), bytes: &[u8]) -> (u32, u32) {
    for unit in bytes.as_chunks::<8>().0 {
        let word = |at: usize| {
            let raw = [unit[at], unit[at + 1], unit[at + 2], unit[at + 3]];
            if big {
                u32::from_be_bytes(raw)
            } else {
                u32::from_le_bytes(raw)
            }
        };
        s.0 = s.0.wrapping_add(word(0)).wrapping_add(s.1);
        s.1 = s.1.wrapping_add(word(4)).wrapping_add(s.0);
    }
    s
}

/// The 32 header bytes with every field chosen by the caller and a correct checksum.
pub fn header_bytes(magic: u32, version: u32, ps: u32, salts: (u32, u32), big: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    for field in [magic, version, ps, 0, salts.0, salts.1] {
        bytes.extend(field.to_be_bytes());
    }
    let (c0, c1) = sum(big, (0, 0), &bytes);
    bytes.extend(c0.to_be_bytes());
    bytes.extend(c1.to_be_bytes());
    bytes
}

pub struct Wal {
    pub bytes: Vec<u8>,
    pub ps: u32,
    pub salts: (u32, u32),
    big: bool,
    running: (u32, u32),
}

impl Wal {
    pub fn new(ps: u32, big: bool, salts: (u32, u32)) -> Self {
        let magic = if big { 0x377f_0683 } else { 0x377f_0682 };
        let bytes = header_bytes(magic, 3_007_000, ps, salts, big);
        let running = sum(big, (0, 0), &bytes[..24]);
        Self {
            bytes,
            ps,
            salts,
            big,
            running,
        }
    }

    /// Appends a frame with explicit salts; the checksum chain continues regardless.
    pub fn frame_with(&mut self, pgno: u32, commit: u32, salts: (u32, u32), data: &[u8]) {
        let mut head = Vec::new();
        for field in [pgno, commit, salts.0, salts.1] {
            head.extend(field.to_be_bytes());
        }
        self.running = sum(self.big, self.running, &head[..8]);
        self.running = sum(self.big, self.running, data);
        head.extend(self.running.0.to_be_bytes());
        head.extend(self.running.1.to_be_bytes());
        self.bytes.extend(head);
        self.bytes.extend(data);
    }

    pub fn frame(&mut self, pgno: u32, commit: u32, seed: u32) {
        let data = page(self.ps, seed);
        self.frame_with(pgno, commit, self.salts, &data);
    }

    /// Frames for `pgnos`, the last one carrying `commit`.
    pub fn tx(&mut self, pgnos: &[u32], commit: u32, seed: u32) {
        for (i, pgno) in pgnos.iter().enumerate() {
            let last = i + 1 == pgnos.len();
            self.frame(*pgno, if last { commit } else { 0 }, seed + pgno);
        }
    }

    pub fn frame_len(&self) -> usize {
        usize::try_from(self.ps).unwrap() + 24
    }
}

pub fn open(bytes: &[u8]) -> WalReader<Mem> {
    WalReader::open(Cursor::new(bytes.to_vec())).unwrap()
}

#[derive(Debug)]
pub struct Scanned {
    pub txs: Vec<CommittedTx>,
    pub tail: Tail,
    pub at: FrameCursor,
}

/// Scans from `cursor` (the start when `None`) to the end, collecting transactions.
pub fn scan_from(
    reader: &mut WalReader<impl Read + Seek>,
    cursor: Option<FrameCursor>,
    limit: ScanLimit,
) -> Result<Scanned, WalError> {
    let mut cursor = cursor.unwrap_or_else(|| reader.cursor_at_start());
    let mut txs = Vec::new();
    loop {
        match reader.next_tx(&cursor, limit)? {
            TxOutcome::Tx(tx) => {
                cursor = *tx.end();
                txs.push(tx);
            }
            TxOutcome::End { at, tail } => return Ok(Scanned { txs, tail, at }),
        }
    }
}

pub fn scan(bytes: &[u8], limit: ScanLimit) -> Scanned {
    scan_from(&mut open(bytes), None, limit).unwrap()
}

/// A reader that fails every read after `ok` successful ones.
#[derive(Debug)]
pub struct Failing {
    pub inner: Mem,
    pub ok: usize,
}

impl Read for Failing {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.ok == 0 {
            return Err(io::Error::other("injected"));
        }
        self.ok -= 1;
        self.inner.read(buf)
    }
}

impl Seek for Failing {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

/// A reader that records, in a cell the test keeps, the highest byte offset it has read to.
#[derive(Debug)]
pub struct Watching {
    pub inner: Mem,
    pub highest: Rc<Cell<u64>>,
}

impl Read for Watching {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.highest
            .set(self.highest.get().max(self.inner.position()));
        Ok(n)
    }
}

impl Seek for Watching {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

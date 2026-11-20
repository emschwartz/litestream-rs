//! A WAL writer for tests: the bytes SQLite would have written, built by hand.
//!
//! It ships under the `testing` feature so the simulator and the Go-compat
//! tests build WALs with the same writer this crate is tested with. Its
//! checksum is its own few lines, not [`crate::WalChecksum::extend`], so the
//! reader is checked against an independent implementation.

use lsr_ltx::{PageNo, PageSize};

use crate::{ByteOrder, FRAME_HEADER_LEN, HEADER_LEN, Salts, WalChecksum};

const MAGIC_LITTLE: u32 = 0x377f_0682;
const MAGIC_BIG: u32 = 0x377f_0683;
const VERSION: u32 = 3_007_000;
/// Bytes of a frame header that the running checksum covers.
const CHECKSUMMED_FRAME_HEADER_LEN: usize = 8;

/// A WAL file under construction.
#[derive(Clone, Debug)]
pub struct WalBuilder {
    bytes: Vec<u8>,
    page_size: PageSize,
    salts: Salts,
    order: ByteOrder,
    running: WalChecksum,
}

impl WalBuilder {
    /// A WAL holding only its header, with the byte order and salts chosen.
    #[must_use]
    pub fn new(page_size: PageSize, big_endian: bool, salts: Salts) -> Self {
        let (order, magic) = if big_endian {
            (ByteOrder::Big, MAGIC_BIG)
        } else {
            (ByteOrder::Little, MAGIC_LITTLE)
        };
        let mut bytes = Vec::with_capacity(HEADER_LEN);
        for field in [magic, VERSION, page_size.get(), 0, salts.salt1, salts.salt2] {
            bytes.extend(field.to_be_bytes());
        }
        let running = fold(order, WalChecksum::ZERO, &bytes);
        bytes.extend(running.s0().to_be_bytes());
        bytes.extend(running.s1().to_be_bytes());
        debug_assert_eq!(bytes.len(), HEADER_LEN, "the header is HEADER_LEN bytes");
        Self {
            bytes,
            page_size,
            salts,
            order,
            running,
        }
    }

    /// Appends a frame with explicit salts; the checksum chain continues regardless.
    ///
    /// `data` is written as given, so a short or long page makes a frame the
    /// reader must reject.
    pub fn frame_with(&mut self, pgno: PageNo, commit: u32, salts: Salts, data: &[u8]) {
        self.frame_raw(pgno.get(), commit, salts, data);
    }

    /// Like [`Self::frame_with`] with any page number, zero and the lock page included: a
    /// corrupt WAL holds frames no valid `PageNo` can name.
    pub fn frame_raw(&mut self, pgno: u32, commit: u32, salts: Salts, data: &[u8]) {
        let mut head = Vec::with_capacity(FRAME_HEADER_LEN);
        for field in [pgno, commit, salts.salt1, salts.salt2] {
            head.extend(field.to_be_bytes());
        }
        let covered = head.get(..CHECKSUMMED_FRAME_HEADER_LEN).unwrap_or_default();
        self.running = fold(self.order, self.running, covered);
        self.running = fold(self.order, self.running, data);
        head.extend(self.running.s0().to_be_bytes());
        head.extend(self.running.s1().to_be_bytes());
        self.bytes.extend(head);
        self.bytes.extend(data);
    }

    /// Appends a frame in the header's generation, its page from [`page`].
    pub fn frame(&mut self, pgno: PageNo, commit: u32, seed: u64) {
        let data = page(seed, pgno, self.page_size);
        self.frame_with(pgno, commit, self.salts, &data);
    }

    /// Frames for `pgnos`, the last one carrying `db_size` as its commit size.
    pub fn tx(&mut self, pgnos: &[PageNo], db_size: u32, seed: u64) {
        let last = pgnos.len().saturating_sub(1);
        for (at, pgno) in pgnos.iter().enumerate() {
            self.frame(*pgno, if at == last { db_size } else { 0 }, seed);
        }
    }

    /// Bytes in one frame: its header and a page.
    #[must_use]
    pub const fn frame_len(&self) -> usize {
        self.page_size.bytes().saturating_add(FRAME_HEADER_LEN)
    }

    /// The file so far.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The file so far, by value.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// SQLite's checksum over whole 8-byte units; any remainder is ignored.
fn fold(order: ByteOrder, from: WalChecksum, bytes: &[u8]) -> WalChecksum {
    let word = |raw: [u8; 4]| match order {
        ByteOrder::Little => u32::from_le_bytes(raw),
        ByteOrder::Big => u32::from_be_bytes(raw),
    };
    let (mut s0, mut s1) = (from.s0(), from.s1());
    for [a, b, c, d, e, f, g, h] in bytes.as_chunks::<8>().0.iter().copied() {
        s0 = s0.wrapping_add(word([a, b, c, d])).wrapping_add(s1);
        s1 = s1.wrapping_add(word([e, f, g, h])).wrapping_add(s0);
    }
    WalChecksum::new(s0, s1)
}

/// A page of deterministic content (splitmix64), different for every seed and page number.
#[must_use]
pub fn page(seed: u64, pgno: PageNo, size: PageSize) -> Vec<u8> {
    let mut state = seed.wrapping_add(u64::from(pgno.get()).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let mut out = vec![0; size.bytes()];
    for chunk in out.chunks_mut(8) {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z.wrapping_shr(31);
        for (dst, src) in chunk.iter_mut().zip(z.to_le_bytes()) {
            *dst = src;
        }
    }
    out
}

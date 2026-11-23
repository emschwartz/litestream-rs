//! One slot's bytes, both ways (crate doc for the layout).

use std::num::NonZeroU32;

use crc::{CRC_32_ISCSI, Crc};
use lsr_ltx::{FileChecksum, Timestamp, Txid};

use crate::facts::{
    Ahead, Floor, Journal, Proof, ReplicaId, StoredBudget, Triple, Unchecked, Unproven, WalChecksum,
};
use crate::read::Seq;

/// A slot's length.
pub const SLOT: usize = 4096;
/// Every version's record fits in a slot's first 512 bytes, one sector under any block size.
pub const RECORD_MAX: usize = 512;
/// The record's first eight bytes.
pub const MAGIC: [u8; 8] = *b"LSRJRNL\0";
/// The newest version this binary writes: 3 while an unchecked notice stands, else 2 while an
/// ahead-of-WAL verdict stands, else 1.
pub const VERSION: u16 = 3;
/// The version of a record with a verdict and no unchecked notice.
const VERSION_2: u16 = 2;
/// The version of a record with neither, which every binary since the journal reads.
const VERSION_1: u16 = 1;

/// Magic, version, body length and sequence number.
const HEAD: usize = 20;
/// The checksum.
const TAIL: usize = 4;
/// The longest body any version may have.
const BODY_MAX: usize = RECORD_MAX - HEAD - TAIL;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

const HAS_FLOOR: u8 = 1;
const HAS_ANCHOR: u8 = 2;
const HAS_BUDGET: u8 = 4;
const HAS_PENDING: u8 = 8;
/// Version 2.
const HAS_AHEAD: u8 = 16;
/// Version 3.
const HAS_UNCHECKED: u8 = 32;

/// Why a slot holds no record.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum Invalid {
    /// The first eight bytes are not the magic (a zeroed or never-written slot among others).
    #[error("no magic")]
    Magic,
    /// Version 0, which no writer uses.
    #[error("version 0")]
    Version,
    /// The body length runs past the record's 512 bytes.
    #[error("body length {0} past the record")]
    Length(u16),
    /// The checksum does not match.
    #[error("checksum mismatch")]
    Checksum,
    /// A sequence number of zero, which no writer uses.
    #[error("sequence number 0")]
    Seq,
    /// A flag no writer of the version sets, or an anchor without a floor.
    #[error("flags {0:#04x}")]
    Flags(u8),
    /// A transaction id of zero.
    #[error("transaction id 0")]
    Txid,
    /// A file checksum without its flag bit.
    #[error("file checksum {0:#018x}")]
    FileChecksum(u64),
    /// A debt code no writer uses.
    #[error("debt code {0}")]
    Unproven(u8),
    /// A verdict counting zero pages or naming page zero.
    #[error("a verdict of {pages} pages from page {first}")]
    Ahead {
        /// The stored page count.
        pages: u32,
        /// The stored first page.
        first: u32,
    },
    /// The body is not exactly as long as its flags say.
    #[error("body of {len} bytes where the flags make {want}")]
    BodyLength {
        /// The stored length.
        len: u16,
        /// The length the flags make.
        want: usize,
    },
}

/// A slot that holds a record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decoded {
    /// This binary's version, or an older one.
    Ours {
        /// Its sequence number.
        seq: Seq,
        /// What it says.
        journal: Journal,
    },
    /// A newer version: valid on magic, length and checksum, its body unread.
    Newer {
        /// Its sequence number.
        seq: Seq,
        /// Its version.
        version: u16,
    },
}

/// Little-endian appends.
struct Out(Vec<u8>);

impl Out {
    fn put(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }

    fn triple(&mut self, triple: &Triple) {
        self.put(&triple.len.to_le_bytes());
        self.put(&triple.digest.to_le_bytes());
        self.put(&triple.checksum.get().to_le_bytes());
    }
}

/// The body of `journal`.
fn body(journal: &Journal) -> Vec<u8> {
    let anchor = journal.floor.and_then(|floor| floor.anchor());
    let flags = [
        (journal.floor.is_some(), HAS_FLOOR),
        (anchor.is_some(), HAS_ANCHOR),
        (journal.budget.is_some(), HAS_BUDGET),
        (journal.pending.is_some(), HAS_PENDING),
        (journal.ahead.is_some(), HAS_AHEAD),
        (journal.unchecked.is_some(), HAS_UNCHECKED),
    ]
    .iter()
    .filter(|(on, _)| *on)
    .fold(0, |acc, (_, bit)| acc | bit);
    let mut out = Out(vec![flags]);
    if let Some(floor) = journal.floor {
        out.put(&floor.replica().0);
        out.put(&floor.bound().get().to_le_bytes());
    }
    if let Some(anchor) = anchor {
        out.triple(&anchor);
    }
    if let Some(budget) = journal.budget {
        out.put(&[budget.tokens]);
        out.put(&budget.as_of.unix_millis().to_le_bytes());
    }
    if let Some(proof) = journal.pending {
        out.put(&proof.txid.get().to_le_bytes());
        out.triple(&proof.triple);
        out.put(&proof.wal.0.to_le_bytes());
        out.put(&proof.wal.1.to_le_bytes());
    }
    if let Some(ahead) = journal.ahead {
        out.put(&ahead.restore_point.get().to_le_bytes());
        out.put(&ahead.pages.get().to_le_bytes());
        out.put(&ahead.first.get().to_le_bytes());
        out.put(&ahead.made.unix_millis().to_le_bytes());
    }
    if let Some(unchecked) = journal.unchecked {
        out.put(&unchecked.made.unix_millis().to_le_bytes());
    }
    let debt = match journal.unproven {
        Unproven::No => 0,
        Unproven::Truncate => 1,
        Unproven::Snapshot => 2,
    };
    out.put(&[debt]);
    out.0
}

/// The slot holding `journal` with sequence number `seq`, zeros after the record.
#[must_use]
pub fn encode(journal: &Journal, seq: Seq) -> [u8; SLOT] {
    let body = body(journal);
    let len = u16::try_from(body.len()).unwrap_or(u16::MAX);
    let version = match (journal.unchecked, journal.ahead) {
        (Some(_), _) => VERSION,
        (None, Some(_)) => VERSION_2,
        (None, None) => VERSION_1,
    };
    let mut out = Out(Vec::with_capacity(RECORD_MAX));
    out.put(&MAGIC);
    out.put(&version.to_le_bytes());
    out.put(&len.to_le_bytes());
    out.put(&seq.get().to_le_bytes());
    out.put(&body);
    let crc = CRC32C.checksum(&out.0);
    out.put(&crc.to_le_bytes());
    let mut slot = [0_u8; SLOT];
    for (to, from) in slot.iter_mut().zip(out.0) {
        *to = from;
    }
    slot
}

/// A cursor over a body.
struct In<'a>(&'a [u8]);

impl In<'_> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0; N];
        if let Some((head, rest)) = self.0.split_first_chunk::<N>() {
            out = *head;
            self.0 = rest;
        }
        out
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }

    fn txid(&mut self) -> Result<Txid, Invalid> {
        Txid::new(self.u64()).ok_or(Invalid::Txid)
    }

    fn ahead(&mut self) -> Result<Ahead, Invalid> {
        let restore_point = self.txid()?;
        let pages = u32::from_le_bytes(self.take());
        let first = u32::from_le_bytes(self.take());
        let made = Timestamp::from_unix_millis(i64::from_le_bytes(self.take()));
        match (NonZeroU32::new(pages), NonZeroU32::new(first)) {
            (Some(pages), Some(first)) => Ok(Ahead {
                restore_point,
                pages,
                first,
                made,
            }),
            (None, _) | (_, None) => Err(Invalid::Ahead { pages, first }),
        }
    }

    fn triple(&mut self) -> Result<Triple, Invalid> {
        let len = self.u64();
        let digest = self.u64();
        let raw = self.u64();
        let checksum = FileChecksum::from_raw(raw).ok_or(Invalid::FileChecksum(raw))?;
        Ok(Triple {
            len,
            digest,
            checksum,
        })
    }
}

/// The length of a body with `flags`.
fn body_len(flags: u8) -> usize {
    let part = |bit: u8, len: usize| if flags & bit == 0 { 0 } else { len };
    // The flags byte and the debt byte, and each part present.
    [
        1,
        part(HAS_FLOOR, 16),
        part(HAS_ANCHOR, 24),
        part(HAS_BUDGET, 9),
        part(HAS_PENDING, 40),
        part(HAS_AHEAD, 24),
        part(HAS_UNCHECKED, 8),
        1,
    ]
    .iter()
    .sum()
}

/// Decodes a body of `version`, this binary's or older.
fn decode_body(body: &[u8], version: u16) -> Result<Journal, Invalid> {
    let mut input = In(body);
    let [flags] = input.take::<1>();
    let one = HAS_FLOOR | HAS_ANCHOR | HAS_BUDGET | HAS_PENDING;
    let known = if version >= VERSION {
        one | HAS_AHEAD | HAS_UNCHECKED
    } else if version >= VERSION_2 {
        one | HAS_AHEAD
    } else {
        one
    };
    if flags & !known != 0 || (flags & HAS_ANCHOR != 0 && flags & HAS_FLOOR == 0) {
        return Err(Invalid::Flags(flags));
    }
    let want = body_len(flags);
    if body.len() != want {
        let len = u16::try_from(body.len()).unwrap_or(u16::MAX);
        return Err(Invalid::BodyLength { len, want });
    }
    let floor = if flags & HAS_FLOOR == 0 {
        None
    } else {
        let replica = ReplicaId(input.take());
        let bound = input.txid()?;
        let anchor = if flags & HAS_ANCHOR == 0 {
            None
        } else {
            Some(input.triple()?)
        };
        Some(Floor::new(replica, bound, anchor))
    };
    let budget = (flags & HAS_BUDGET != 0).then(|| {
        let [tokens] = input.take::<1>();
        let as_of = Timestamp::from_unix_millis(i64::from_le_bytes(input.take()));
        StoredBudget { tokens, as_of }
    });
    let pending = if flags & HAS_PENDING == 0 {
        None
    } else {
        let txid = input.txid()?;
        let triple = input.triple()?;
        let wal = WalChecksum(
            u32::from_le_bytes(input.take()),
            u32::from_le_bytes(input.take()),
        );
        Some(Proof { txid, triple, wal })
    };
    let ahead = if flags & HAS_AHEAD == 0 {
        None
    } else {
        Some(input.ahead()?)
    };
    let unchecked = (flags & HAS_UNCHECKED != 0).then(|| Unchecked {
        made: Timestamp::from_unix_millis(i64::from_le_bytes(input.take())),
    });
    let unproven = match input.take::<1>() {
        [0] => Unproven::No,
        [1] => Unproven::Truncate,
        [2] => Unproven::Snapshot,
        [code] => return Err(Invalid::Unproven(code)),
    };
    Ok(Journal {
        floor,
        unproven,
        budget,
        pending,
        ahead,
        unchecked,
    })
}

/// The record in a slot.
///
/// # Errors
///
/// [`Invalid`]: why the slot holds none.
pub fn decode(slot: &[u8; SLOT]) -> Result<Decoded, Invalid> {
    let (head, rest) = slot.split_first_chunk::<HEAD>().ok_or(Invalid::Magic)?;
    let mut fields = In(head);
    if fields.take::<8>() != MAGIC {
        return Err(Invalid::Magic);
    }
    let version = u16::from_le_bytes(fields.take());
    let len = u16::from_le_bytes(fields.take());
    let raw_seq = fields.u64();
    if version == 0 {
        return Err(Invalid::Version);
    }
    let body_len = usize::from(len);
    if body_len > BODY_MAX {
        return Err(Invalid::Length(len));
    }
    let (body, after) = rest
        .split_at_checked(body_len)
        .ok_or(Invalid::Length(len))?;
    let stored = after.first_chunk::<TAIL>().ok_or(Invalid::Length(len))?;
    let mut digest = CRC32C.digest();
    digest.update(head);
    digest.update(body);
    if digest.finalize() != u32::from_le_bytes(*stored) {
        return Err(Invalid::Checksum);
    }
    let seq = Seq::new(raw_seq).ok_or(Invalid::Seq)?;
    if version > VERSION {
        return Ok(Decoded::Newer { seq, version });
    }
    let journal = decode_body(body, version)?;
    Ok(Decoded::Ours { seq, journal })
}

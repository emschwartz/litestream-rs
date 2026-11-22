//! The engine journal's record: the four facts it stores, their codec, and the read rule over
//! the file's two slots. No I/O; the writer that decides which slot to write, and when to sync,
//! lives beside the engine. The contract is `docs/journal.md`, checked by the exhaustive model in
//! `lsr_spec::journal`, which shares no code with this crate.
//!
//! A slot is 4096 bytes. Its record sits in the first [`RECORD_MAX`] bytes:
//!
//! ```text
//! magic "LSRJRNL\0" | version u16 | len u16 | seq u64 | body (len bytes) | crc32c u32
//! ```
//!
//! little-endian, the CRC-32C over every byte before it, zeros after it. Version 1's body is a
//! flags byte (floor, anchor, budget, pending), then each present part in that order, then the
//! debt. A slot of a newer version is valid on magic, length and checksum alone.

mod codec;
mod facts;
mod read;

pub use codec::{Decoded, Invalid, MAGIC, RECORD_MAX, SLOT, VERSION, decode, encode};
pub use facts::{
    Floor, Journal, Lowered, Proof, ReplicaId, StoredBudget, Triple, Unproven, WalChecksum,
    WouldLower,
};
pub use read::{Opened, Parsed, Seq, Slot, SlotFault, SlotRead, Verdict, Why, parse};

/// The journal file's length: two slots.
pub const FILE: usize = 8192;

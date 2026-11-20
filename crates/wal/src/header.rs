//! The 32-byte WAL header.

use thiserror::Error;

use lsr_ltx::PageSize;

use crate::checksum::{ByteOrder, WalChecksum};
use crate::conv::Fields;

/// Length of the WAL header.
pub const HEADER_LEN: usize = 32;

const MAGIC_LITTLE: u32 = 0x377f_0682;
const MAGIC_BIG: u32 = 0x377f_0683;
const VERSION: u32 = 3_007_000;
/// Header bytes the header checksum covers: everything before the checksum itself.
const CHECKSUMMED_LEN: usize = 24;

/// The pair of random values that name one WAL generation: every frame of the
/// generation carries them, and SQLite changes them when it restarts the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Salts {
    /// First salt; SQLite increments it on each restart.
    pub salt1: u32,
    /// Second salt; SQLite randomises it on each restart.
    pub salt2: u32,
}

/// Why a WAL header is not valid.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum HeaderError {
    /// The magic number is neither of SQLite's two.
    #[error("bad WAL magic {found:#010x}")]
    BadMagic {
        /// The magic found.
        found: u32,
    },
    /// The header's own checksum does not match: an interrupted header write.
    #[error("WAL header checksum mismatch")]
    ChecksumMismatch,
    /// The format version is not 3007000.
    #[error("unsupported WAL version {found}")]
    UnsupportedVersion {
        /// The version found.
        found: u32,
    },
    /// The page size is not a power of two from 512 to 65536.
    #[error("bad WAL page size {found}")]
    BadPageSize {
        /// The page size found.
        found: u32,
    },
}

/// A verified WAL header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalHeader {
    byte_order: ByteOrder,
    page_size: PageSize,
    checkpoint_seq: u32,
    salts: Salts,
    checksum: WalChecksum,
}

impl WalHeader {
    /// Checks magic, own checksum, version and page size, in that order.
    ///
    /// # Errors
    /// The first failing check, as a [`HeaderError`].
    pub fn decode(bytes: &[u8; HEADER_LEN]) -> Result<Self, HeaderError> {
        let mut fields = Fields::new(bytes);
        let magic = fields.u32();
        let version = fields.u32();
        let page_size = fields.u32();
        let checkpoint_seq = fields.u32();
        let salts = Salts {
            salt1: fields.u32(),
            salt2: fields.u32(),
        };
        let stored = WalChecksum::new(fields.u32(), fields.u32());
        let byte_order = match magic {
            MAGIC_LITTLE => ByteOrder::Little,
            MAGIC_BIG => ByteOrder::Big,
            found => return Err(HeaderError::BadMagic { found }),
        };
        let computed = bytes
            .get(..CHECKSUMMED_LEN)
            .and_then(|covered| WalChecksum::ZERO.extend(byte_order, covered));
        if computed != Some(stored) {
            return Err(HeaderError::ChecksumMismatch);
        }
        if version != VERSION {
            return Err(HeaderError::UnsupportedVersion { found: version });
        }
        let page_size =
            PageSize::new(page_size).ok_or(HeaderError::BadPageSize { found: page_size })?;
        Ok(Self {
            byte_order,
            page_size,
            checkpoint_seq,
            salts,
            checksum: stored,
        })
    }

    /// The byte order of this WAL's checksum words.
    #[must_use]
    pub const fn byte_order(&self) -> ByteOrder {
        self.byte_order
    }

    /// The database page size every frame carries.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// How many checkpoints SQLite had completed when it wrote this header.
    #[must_use]
    pub const fn checkpoint_seq(&self) -> u32 {
        self.checkpoint_seq
    }

    /// The salts of this WAL generation.
    #[must_use]
    pub const fn salts(&self) -> Salts {
        self.salts
    }

    /// The checksum the first frame continues from.
    #[must_use]
    pub const fn checksum(&self) -> WalChecksum {
        self.checksum
    }
}

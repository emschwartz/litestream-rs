//! The 100-byte LTX header.
//!
//! Layout, all big-endian: magic `LTX1` (0), flags u32 (4), page size u32 (8),
//! commit u32 (12), min TXID u64 (16), max TXID u64 (24), timestamp i64 in
//! milliseconds (32), pre-apply checksum u64 (40), WAL offset i64 (48), WAL size
//! i64 (56), WAL salts u32 x2 (64, 68), node ID u64 (72), then 20 reserved bytes,
//! written as zero and ignored on read as Go ignores them.
//!
//! The only flag is `NoChecksum` (bit 1): the writer did not track database
//! checksums, so the pre- and post-apply checksums are zero. Litestream 0.5
//! writes every file this way. Validation is Go's `Header.Validate` at ltx
//! v0.5.3, which v0.5.1 shares: a snapshot (min TXID 1) has no pre-apply
//! checksum, and neither does an untracked file; a tracked incremental has one.
//! The WAL fields are all zero or describe a real WAL range.

use thiserror::Error;

use crate::DbChecksum;
use crate::conv::Cursor;
use crate::{DbPages, PageSize, Timestamp, Txid, TxidRange};

/// Bytes in an encoded header.
pub const HEADER_LEN: usize = 100;

const MAGIC: [u8; 4] = *b"LTX1";
const FLAG_NO_CHECKSUM: u32 = 1 << 1;
const I64_MAX: u64 = i64::MAX.unsigned_abs();

/// Why a header failed validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum HeaderError {
    /// The file does not start with `LTX1`.
    #[error("not an LTX v3 file: magic {found:02x?}")]
    Magic {
        /// The first four bytes.
        found: [u8; 4],
    },
    /// A flag bit other than `NoChecksum` is set.
    #[error("unknown header flags {flags:#x}")]
    Flags {
        /// The flags field.
        flags: u32,
    },
    /// The page size is not a power of two in 512..=65536.
    #[error("invalid page size {size}")]
    PageSize {
        /// The page size field.
        size: u32,
    },
    /// A TXID is zero or the range is reversed.
    #[error("invalid TXID range {min}..={max}")]
    Txids {
        /// The min TXID field.
        min: u64,
        /// The max TXID field.
        max: u64,
    },
    /// The WAL fields are neither all zero nor a valid WAL range.
    #[error("invalid WAL origin: offset {offset}, size {size}, salts {salt1:#x} {salt2:#x}")]
    Wal {
        /// The WAL offset field.
        offset: i64,
        /// The WAL size field.
        size: i64,
        /// The first WAL salt.
        salt1: u32,
        /// The second WAL salt.
        salt2: u32,
    },
    /// The pre-apply checksum is present when it must be absent, absent when
    /// it must be present, or lacks the checksum flag.
    #[error("invalid pre-apply checksum {raw:#x} for this kind of file")]
    PreApply {
        /// The pre-apply field (zero when absent).
        raw: u64,
    },
}

/// Where in the source WAL a file's pages came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalOrigin {
    offset: u64,
    size: u64,
    salt1: u32,
    salt2: u32,
}

impl WalOrigin {
    /// Returns `None` unless `offset` is positive and both fit an `i64`, as Go stores them.
    #[must_use]
    pub const fn new(offset: u64, size: u64, salt1: u32, salt2: u32) -> Option<Self> {
        if offset > 0 && offset <= I64_MAX && size <= I64_MAX {
            Some(Self {
                offset,
                size,
                salt1,
                salt2,
            })
        } else {
            None
        }
    }

    /// Byte offset in the WAL of the first frame read.
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Bytes of WAL read.
    #[must_use]
    pub const fn size(self) -> u64 {
        self.size
    }

    /// The WAL header's two salts.
    #[must_use]
    pub const fn salts(self) -> (u32, u32) {
        (self.salt1, self.salt2)
    }
}

/// The fields of a header, for building one with [`Header::new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderFields {
    /// The writer tracked database checksums (the `NoChecksum` flag is clear).
    pub tracked: bool,
    /// The database page size.
    pub page_size: PageSize,
    /// The database size in pages after this file applies.
    pub commit: u32,
    /// The transactions this file covers.
    pub txids: TxidRange,
    /// When the last transaction committed.
    pub timestamp: Timestamp,
    /// The database checksum before this file applies: present exactly when
    /// the file is tracked and not a snapshot.
    pub pre_apply: Option<DbChecksum>,
    /// The WAL range the pages came from, if any.
    pub wal: Option<WalOrigin>,
    /// The writing node, zero when unset.
    pub node_id: u64,
}

/// A validated LTX header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header(HeaderFields);

impl Header {
    /// Checks the rules no single field type carries.
    ///
    /// # Errors
    ///
    /// [`HeaderError::PreApply`] when the pre-apply checksum's presence does
    /// not match the kind of file.
    pub fn new(fields: HeaderFields) -> Result<Self, HeaderError> {
        let snapshot = fields.txids.min() == Txid::MIN;
        if (fields.tracked && !snapshot) != fields.pre_apply.is_some() {
            return Err(HeaderError::PreApply {
                raw: fields.pre_apply.map_or(0, DbChecksum::get),
            });
        }
        Ok(Self(fields))
    }

    /// The header's fields.
    #[must_use]
    pub const fn fields(&self) -> HeaderFields {
        self.0
    }

    /// Whether the file holds a whole database: its first TXID is 1.
    #[must_use]
    pub fn is_snapshot(&self) -> bool {
        self.0.txids.min() == Txid::MIN
    }

    /// Whether the writer tracked database checksums.
    #[must_use]
    pub const fn tracked(&self) -> bool {
        self.0.tracked
    }

    /// The database page size.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.0.page_size
    }

    /// The database size in pages after this file applies.
    #[must_use]
    pub const fn commit(&self) -> u32 {
        self.0.commit
    }

    /// The database's length in pages after this file applies: the commit
    /// field, typed so membership and the last page follow the lock-page rule.
    #[must_use]
    pub const fn pages(&self) -> DbPages {
        DbPages::new(self.0.commit)
    }

    /// The transactions this file covers.
    #[must_use]
    pub const fn txids(&self) -> TxidRange {
        self.0.txids
    }

    /// Decodes and validates a header.
    ///
    /// # Errors
    ///
    /// The first [`HeaderError`] the bytes violate, checked in field order.
    pub fn decode(bytes: &[u8; HEADER_LEN]) -> Result<Self, HeaderError> {
        let mut cursor = Cursor::new(bytes);
        let found: [u8; 4] = cursor.take();
        if found != MAGIC {
            return Err(HeaderError::Magic { found });
        }
        let flags = cursor.u32();
        if flags & !FLAG_NO_CHECKSUM != 0 {
            return Err(HeaderError::Flags { flags });
        }
        let size = cursor.u32();
        let page_size = PageSize::new(size).ok_or(HeaderError::PageSize { size })?;
        let commit = cursor.u32();
        let (min, max) = (cursor.u64(), cursor.u64());
        let txids = Txid::new(min)
            .zip(Txid::new(max))
            .and_then(|(lo, hi)| TxidRange::new(lo, hi))
            .ok_or(HeaderError::Txids { min, max })?;
        let timestamp = Timestamp::from_unix_millis(cursor.i64());
        let pre_apply = match cursor.u64() {
            0 => None,
            raw => Some(DbChecksum::from_raw(raw).ok_or(HeaderError::PreApply { raw })?),
        };
        let wal = wal_origin(cursor.i64(), cursor.i64(), cursor.u32(), cursor.u32())?;
        let node_id = cursor.u64();
        let tracked = flags & FLAG_NO_CHECKSUM == 0;
        Self::new(HeaderFields {
            tracked,
            page_size,
            commit,
            txids,
            timestamp,
            pre_apply,
            wal,
            node_id,
        })
    }

    /// Encodes the header, reserved bytes zeroed.
    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let f = self.0;
        let flags = if f.tracked { 0 } else { FLAG_NO_CHECKSUM };
        let wal = f
            .wal
            .map_or((0, 0, 0, 0), |w| (w.offset, w.size, w.salt1, w.salt2));
        let mut out = [0; HEADER_LEN];
        out[0..4].copy_from_slice(&MAGIC);
        out[4..8].copy_from_slice(&flags.to_be_bytes());
        out[8..12].copy_from_slice(&f.page_size.get().to_be_bytes());
        out[12..16].copy_from_slice(&f.commit.to_be_bytes());
        out[16..24].copy_from_slice(&f.txids.min().get().to_be_bytes());
        out[24..32].copy_from_slice(&f.txids.max().get().to_be_bytes());
        out[32..40].copy_from_slice(&f.timestamp.unix_millis().to_be_bytes());
        out[40..48].copy_from_slice(&f.pre_apply.map_or(0, DbChecksum::get).to_be_bytes());
        out[48..56].copy_from_slice(&wal.0.to_be_bytes());
        out[56..64].copy_from_slice(&wal.1.to_be_bytes());
        out[64..68].copy_from_slice(&wal.2.to_be_bytes());
        out[68..72].copy_from_slice(&wal.3.to_be_bytes());
        out[72..80].copy_from_slice(&f.node_id.to_be_bytes());
        out
    }
}

fn wal_origin(
    offset: i64,
    size: i64,
    salt1: u32,
    salt2: u32,
) -> Result<Option<WalOrigin>, HeaderError> {
    let invalid = HeaderError::Wal {
        offset,
        size,
        salt1,
        salt2,
    };
    match (u64::try_from(offset), u64::try_from(size)) {
        (Ok(0), Ok(0)) if salt1 == 0 && salt2 == 0 => Ok(None),
        (Ok(offset), Ok(size)) => WalOrigin::new(offset, size, salt1, salt2)
            .map(Some)
            .ok_or(invalid),
        (Err(_), _) | (_, Err(_)) => Err(invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(min: u64, tracked: bool, pre: u64) -> HeaderFields {
        HeaderFields {
            tracked,
            page_size: PageSize::new(4096).unwrap(),
            commit: 9,
            txids: TxidRange::new(
                Txid::new(min).unwrap(),
                Txid::new(min.wrapping_add(2)).unwrap(),
            )
            .unwrap(),
            timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
            pre_apply: DbChecksum::from_raw(pre),
            wal: WalOrigin::new(32, 4120, 1, 2),
            node_id: 3,
        }
    }

    fn encoded(min: u64, tracked: bool, pre: u64) -> [u8; HEADER_LEN] {
        Header::new(fields(min, tracked, pre)).unwrap().encode()
    }

    #[test]
    fn header_round_trips() {
        for (min, tracked, pre) in [
            (1, true, 0),
            (1, false, 0),
            (4, false, 0),
            (4, true, (1 << 63) | 77),
        ] {
            let header = Header::new(fields(min, tracked, pre)).unwrap();
            assert_eq!(Header::decode(&header.encode()), Ok(header));
        }
    }

    #[test]
    fn reserved_bytes_are_ignored() {
        let mut bytes = encoded(4, false, 0);
        bytes[80..].fill(0xff);
        assert_eq!(
            Header::decode(&bytes).unwrap().fields(),
            fields(4, false, 0)
        );
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut bytes = encoded(1, false, 0);
        bytes[3] = b'2';
        assert_eq!(
            Header::decode(&bytes),
            Err(HeaderError::Magic { found: *b"LTX2" })
        );
    }

    #[test]
    fn unknown_flags_are_rejected() {
        let mut bytes = encoded(1, false, 0);
        bytes[7] |= 1;
        assert_eq!(Header::decode(&bytes), Err(HeaderError::Flags { flags: 3 }));
    }

    #[test]
    fn bad_page_sizes_are_rejected() {
        for size in [0_u32, 256, 4095, 131_072] {
            let mut bytes = encoded(1, false, 0);
            bytes[8..12].copy_from_slice(&size.to_be_bytes());
            assert_eq!(Header::decode(&bytes), Err(HeaderError::PageSize { size }));
        }
    }

    #[test]
    fn bad_txid_ranges_are_rejected() {
        for (min, max) in [(0_u64, 3_u64), (1, 0), (5, 4)] {
            let mut bytes = encoded(1, false, 0);
            bytes[16..24].copy_from_slice(&min.to_be_bytes());
            bytes[24..32].copy_from_slice(&max.to_be_bytes());
            assert_eq!(Header::decode(&bytes), Err(HeaderError::Txids { min, max }));
        }
    }

    #[test]
    fn bad_wal_fields_are_rejected() {
        let cases: [(i64, i64, u32, u32); 4] =
            [(-1, 0, 0, 0), (32, -1, 0, 0), (0, 10, 0, 0), (0, 0, 1, 0)];
        for (offset, size, salt1, salt2) in cases {
            let mut bytes = encoded(1, false, 0);
            bytes[48..56].copy_from_slice(&offset.to_be_bytes());
            bytes[56..64].copy_from_slice(&size.to_be_bytes());
            bytes[64..68].copy_from_slice(&salt1.to_be_bytes());
            bytes[68..72].copy_from_slice(&salt2.to_be_bytes());
            assert_eq!(
                Header::decode(&bytes),
                Err(HeaderError::Wal {
                    offset,
                    size,
                    salt1,
                    salt2
                })
            );
        }
    }

    #[test]
    fn all_zero_wal_fields_mean_no_origin() {
        let mut bytes = encoded(1, false, 0);
        bytes[48..72].fill(0);
        assert_eq!(Header::decode(&bytes).unwrap().fields().wal, None);
    }

    #[test]
    fn pre_apply_must_match_the_kind_of_file() {
        let flagged = (1_u64 << 63) | 9;
        // (min TXID, tracked, raw pre-apply): each one Go's Validate rejects.
        for (min, tracked, raw) in [
            (1, true, flagged),
            (1, false, flagged),
            (4, false, flagged),
            (4, true, 0),
            (4, true, 9),
        ] {
            let mut bytes = encoded(min, false, 0);
            if tracked {
                bytes[4..8].fill(0);
            }
            bytes[40..48].copy_from_slice(&raw.to_be_bytes());
            assert_eq!(
                Header::decode(&bytes),
                Err(HeaderError::PreApply { raw }),
                "min {min} tracked {tracked}"
            );
        }
    }
}

//! The fields of an LTX v3 header the harness reads. TEMPORARY: replace with `lsr-ltx`'s
//! `Header` (task 1). Offsets are from `docs/research/ltx-format.md`, all big-endian.

/// Length of an LTX header on disk.
pub const HEADER_LEN: usize = 100;

/// The header fields used to observe a replica (not a validating decoder).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtxHeader {
    /// Flag bits.
    pub flags: u32,
    /// Page size in bytes.
    pub page_size: u32,
    /// Database size in pages after this file.
    pub commit: u32,
    /// First TXID.
    pub min_txid: u64,
    /// Last TXID.
    pub max_txid: u64,
    /// Creation time, Unix milliseconds.
    pub timestamp_ms: i64,
    /// Offset in the source WAL where this file's frames start (L0 only).
    pub wal_offset: i64,
    /// Bytes of source WAL this file covers (L0 only).
    pub wal_size: i64,
    /// First WAL salt.
    pub wal_salt1: u32,
    /// Second WAL salt.
    pub wal_salt2: u32,
}

fn field<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..at.checked_add(N)?)?.try_into().ok()
}

impl LtxHeader {
    /// Reads the fields from the first bytes of a file; `None` when too short or without the
    /// `LTX1` magic.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.get(..4)? != b"LTX1" || bytes.len() < HEADER_LEN {
            return None;
        }
        Some(Self {
            flags: u32::from_be_bytes(field(bytes, 4)?),
            page_size: u32::from_be_bytes(field(bytes, 8)?),
            commit: u32::from_be_bytes(field(bytes, 12)?),
            min_txid: u64::from_be_bytes(field(bytes, 16)?),
            max_txid: u64::from_be_bytes(field(bytes, 24)?),
            timestamp_ms: i64::from_be_bytes(field(bytes, 32)?),
            wal_offset: i64::from_be_bytes(field(bytes, 48)?),
            wal_size: i64::from_be_bytes(field(bytes, 56)?),
            wal_salt1: u32::from_be_bytes(field(bytes, 64)?),
            wal_salt2: u32::from_be_bytes(field(bytes, 68)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<u8> {
        let mut b = vec![0u8; HEADER_LEN];
        b[..4].copy_from_slice(b"LTX1");
        b[8..12].copy_from_slice(&4096u32.to_be_bytes());
        b[12..16].copy_from_slice(&7u32.to_be_bytes());
        b[16..24].copy_from_slice(&3u64.to_be_bytes());
        b[24..32].copy_from_slice(&5u64.to_be_bytes());
        b[48..56].copy_from_slice(&16512i64.to_be_bytes());
        b[56..64].copy_from_slice(&4120i64.to_be_bytes());
        b[64..68].copy_from_slice(&0x09cb_9427u32.to_be_bytes());
        b[68..72].copy_from_slice(&0x6eab_051fu32.to_be_bytes());
        b
    }

    #[test]
    fn reads_the_fields() {
        let h = LtxHeader::parse(&sample()).expect("parses");
        assert_eq!(
            (h.page_size, h.commit, h.min_txid, h.max_txid),
            (4096, 7, 3, 5)
        );
        assert_eq!((h.wal_offset, h.wal_size), (16512, 4120));
        assert_eq!((h.wal_salt1, h.wal_salt2), (0x09cb_9427, 0x6eab_051f));
    }

    #[test]
    fn short_or_foreign_input_is_none() {
        assert!(LtxHeader::parse(&sample()[..99]).is_none());
        let mut b = sample();
        b[0] = b'X';
        assert!(LtxHeader::parse(&b).is_none());
    }
}

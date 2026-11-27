//! The wal-index and the WAL header as the harness reads them, with its own few lines of
//! parsing: none of the engine's code reads them for the world.
//!
//! The wal-index header (walformat.html) is in the machine's byte order: `mxFrame` at byte
//! 16, `nPage` at 20, the salts at 32 and 36 (copied verbatim from the WAL header, so
//! big-endian), `nBackfill` at 96. The WAL header is big-endian: the page size at byte 8 and
//! the salts at 16 and 20.

use lsr_ltx::PageSize;
use lsr_wal::{Salts, WalOffset};

use super::state::Real;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Index {
    pub(super) mx_frame: u32,
    pub(super) db_size: u32,
    pub(super) salts: Salts,
    pub(super) backfilled: u32,
}

fn word(bytes: &[u8], at: usize, big: bool) -> Option<u32> {
    let raw: [u8; 4] = bytes.get(at..at.checked_add(4)?)?.try_into().ok()?;
    Some(if big {
        u32::from_be_bytes(raw)
    } else {
        u32::from_ne_bytes(raw)
    })
}

impl Index {
    /// The wal-index as the VFS's mapping holds it; all zero while nothing maps it.
    pub(super) fn read(real: &Real) -> Self {
        let Some(head) = real.vfs.shm_head::<136>(&real.db) else {
            return Self {
                mx_frame: 0,
                db_size: 0,
                salts: Salts { salt1: 0, salt2: 0 },
                backfilled: 0,
            };
        };
        let get = |at, big| word(&head, at, big).unwrap_or(0);
        Self {
            mx_frame: get(16, false),
            db_size: get(20, false),
            salts: Salts {
                salt1: get(32, true),
                salt2: get(36, true),
            },
            backfilled: get(96, false),
        }
    }

    pub(super) fn end(self, page_size: PageSize) -> WalOffset {
        WalOffset::from_frame_index(self.mx_frame, page_size)
    }
}

/// The WAL file's salts as a reader finds them: `None` without a whole header that names a
/// page size.
pub(super) fn header_salts(wal: Option<&[u8]>) -> Option<(u32, u32)> {
    let wal = wal?;
    let page_size = word(wal, 8, true)?;
    let salts = (word(wal, 16, true)?, word(wal, 20, true)?);
    (page_size != 0 && wal.len() >= 32).then_some(salts)
}

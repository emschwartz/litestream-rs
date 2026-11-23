//! The verified cure after a failed WAL sync (`docs/decisions.md` D108).
//!
//! The pages a failed sync covered live in the page cache only, and the kernel may evict them,
//! after which a read returns the disk's older bytes. SQLite's checkpoint does not verify the
//! frames it copies, so a TRUNCATE after an eviction writes those older bytes into the database
//! file. The cure therefore reads the WAL first with checked reads (salts and the chained
//! checksum): an evicted frame fails its checksum, the chain breaks below the committed end, and
//! the engine runs no checkpoint of its own over it.

use lsr_wal::{PageMap, WalReader};

use crate::ahead::open_fault;
use crate::fault::Fault;
use crate::source::WalSource;
use crate::sync::scan_whole;

/// The committed frames of the WAL's current generation, read with checked reads: each page's
/// newest frame and its digest. `None` when the WAL is gone.
///
/// # Errors
/// `WalCorruptBelowCommitted` when a frame below the committed end fails its checks (an evicted
/// page reads as the disk's older bytes), `WalMoved` when the WAL changed under the read, and the
/// fault of a read that failed.
pub(crate) fn scan<W: WalSource>(wal: &mut W) -> Result<Option<PageMap>, Fault> {
    let Some(file) = wal.open().map_err(Fault::from_source)? else {
        return Ok(None);
    };
    let mut reader = WalReader::open(file).map_err(open_fault)?;
    let committed = wal.committed().map_err(Fault::from_source)?;
    if committed.end().salts() != reader.header().salts() {
        return Err(Fault::WalMoved);
    }
    scan_whole(&mut reader, committed).map(Some)
}

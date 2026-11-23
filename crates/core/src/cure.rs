//! The verified cure after a failed WAL sync (`docs/decisions.md` D108).
//!
//! The pages a failed sync covered live in the page cache only, and the kernel may evict them,
//! after which a read returns the disk's older bytes. SQLite's checkpoint does not verify the
//! frames it copies, so a checkpoint after an eviction writes those older bytes into the
//! database file. The cure therefore proves each step:
//!
//! 1. [`scan`] reads the WAL with checked reads (salts and the chained checksum), keeping each
//!    page's newest frame and its digest. An evicted frame fails its checksum, the chain breaks
//!    below the committed end, and the engine runs no checkpoint of its own over it.
//! 2. A FULL checkpoint copies the frames into the database file and syncs it, and leaves the
//!    WAL as it was, so its counts are real: [`copied`] requires that it copied exactly the
//!    scanned frames of the scanned generation, no more and no fewer. A commit between the scan
//!    and the checkpoint is `WalSyncFailed` (the debt stands and the task says so), a WAL that
//!    restarted is `WalMoved`; either way the step runs again from the scan.
//! 3. [`verify`] reads every scanned page back from the database file and compares it with its
//!    frame's digest. A page evicted between the scan and the checkpoint's own read of it was
//!    copied as the disk's older bytes, and fails here: commits the application made are lost,
//!    and the caller raises it (`docs/decisions/D108.md`). The verdict cannot tell the cure's
//!    checkpoint from one the application ran over the same frames.
//! 4. Only then the TRUNCATE empties the WAL.
//!
//! The counts FULL reports are held to real SQLite by `lsr-sqlite`'s `sim_model` tests.

use lsr_ltx::PageChecksum;
use lsr_wal::{PageMap, WalReader};

use crate::ahead::{Found, open_fault, tally};
use crate::fault::Fault;
use lsr_wal::CommittedEnd;

use crate::source::{CheckpointRun, CommittedRead, PageSource, WalSource};
use crate::sync::scan_whole;

/// The committed frames of the WAL's current generation, read with checked reads, and the
/// committed read the scan stopped at.
#[derive(Debug)]
pub(crate) struct Scanned {
    map: PageMap,
    committed: CommittedRead,
}

impl Scanned {
    /// The committed end the scan read to.
    pub(crate) const fn end(&self) -> CommittedEnd {
        self.committed.end()
    }
}

/// Reads the committed frames of the WAL's current generation with checked reads. `None` when
/// the WAL is gone.
///
/// # Errors
/// `WalCorruptBelowCommitted` when a frame below the committed end fails its checks (an evicted
/// page reads as the disk's older bytes), `WalMoved` when the WAL changed under the read, and the
/// fault of a read that failed.
pub(crate) fn scan<W: WalSource>(wal: &mut W) -> Result<Option<Scanned>, Fault> {
    let Some(file) = wal.open().map_err(Fault::from_source)? else {
        return Ok(None);
    };
    let mut reader = WalReader::open(file).map_err(open_fault)?;
    let committed = wal.committed().map_err(Fault::from_source)?;
    if committed.end().salts() != reader.header().salts() {
        return Err(Fault::WalMoved);
    }
    let map = scan_whole(&mut reader, committed)?;
    Ok(Some(Scanned { map, committed }))
}

/// Whether the FULL checkpoint `run`, followed by the committed read `after`, copied exactly the
/// scanned frames. A commit after the checkpoint changes nothing it copied; [`verify`]'s own read
/// of the committed end catches one that lands before the read-back ends.
///
/// # Errors
/// `WalMoved` when the WAL restarted; `WalSyncFailed` when the checkpoint saw more frames than
/// the scan read (a commit landed between the scan and the checkpoint) or copied fewer.
pub(crate) fn copied(
    scanned: &Scanned,
    run: CheckpointRun,
    after: CommittedRead,
) -> Result<(), Fault> {
    let end = scanned.end();
    debug_assert!(!run.busy, "a busy checkpoint is the caller's to refuse");
    if after.end().salts() != end.salts() {
        return Err(Fault::WalMoved);
    }
    let frames = end.frame_count();
    if run.log == frames && run.checkpointed == frames {
        Ok(())
    } else {
        Err(Fault::WalSyncFailed)
    }
}

/// Reads every scanned page back from the database file and compares it with the digest of
/// the frame that holds its newest version. `None` when every page matches. The map holds no
/// page above the last commit's size and never the lock page, which no checkpoint writes.
///
/// # Errors
/// The fault of a page read that failed; `WalMoved` when the WAL's committed end changed under
/// the reads (another checkpoint may then have copied newer frames over the pages read), with no
/// verdict.
pub(crate) fn verify<W: WalSource, P: PageSource>(
    wal: &mut W,
    db: &mut P,
    scanned: &Scanned,
) -> Result<Option<Found>, Fault> {
    let size = scanned.map.page_size();
    let mut buf = vec![0_u8; size.bytes()];
    let mut differ: Option<Found> = None;
    for frame in scanned.map.pages() {
        let pgno = frame.pgno();
        db.read_page(pgno, &mut buf).map_err(Fault::from_source)?;
        if PageChecksum::of(pgno, &buf) != frame.digest() {
            differ = Some(tally(differ, pgno));
        }
    }
    let after = wal.committed().map_err(Fault::from_source)?;
    if after.end() != scanned.end() {
        return Err(Fault::WalMoved);
    }
    Ok(differ)
}

#[cfg(test)]
mod tests {
    use lsr_ltx::PageSize;
    use lsr_wal::Salts;

    use super::*;
    use crate::position::ReadLockHold;

    const SALTS: Salts = Salts { salt1: 1, salt2: 2 };

    fn read(frames: u32, salts: Salts) -> CommittedRead {
        let end = CommittedEnd::new(frames, salts, PageSize::MIN);
        CommittedRead::new(end, frames, ReadLockHold::first()).unwrap()
    }

    fn scanned(frames: u32) -> Scanned {
        Scanned {
            map: PageMap::new(PageSize::MIN),
            committed: read(frames, SALTS),
        }
    }

    fn run(log: u32, checkpointed: u32) -> CheckpointRun {
        CheckpointRun {
            busy: false,
            log,
            checkpointed,
            epoch: ReadLockHold::first(),
        }
    }

    /// A FULL checkpoint proves the scan only when it saw exactly the scanned frames and copied
    /// every one: each count alone decides, and a WAL that restarted is another generation.
    #[test]
    fn a_full_checkpoint_proves_only_the_frames_it_saw_and_copied() {
        let scan = scanned(3);
        assert_eq!(copied(&scan, run(3, 3), read(3, SALTS)), Ok(()));
        assert_eq!(
            copied(&scan, run(4, 4), read(4, SALTS)),
            Err(Fault::WalSyncFailed),
            "a commit between the scan and the checkpoint"
        );
        assert_eq!(
            copied(&scan, run(3, 2), read(3, SALTS)),
            Err(Fault::WalSyncFailed),
            "fewer copied than seen"
        );
        let moved = Salts { salt1: 2, salt2: 9 };
        assert_eq!(
            copied(&scan, run(3, 3), read(3, moved)),
            Err(Fault::WalMoved)
        );
    }
}

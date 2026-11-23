//! Whether the local database is the replica's state: the check a boot with a brand-new meta
//! directory makes before it builds on replica files nothing else vouches for
//! (`docs/decisions/D170.md`).
//!
//! A deployment that does not keep the meta directory boots with no staged file, no journal
//! and no takeover record. Once the WAL has restarted it cannot vouch for the replica's newest
//! transaction T, and the boot gate (`crate::takeover`) would stop for another writer on every
//! redeploy. Equality settles it without an operator: when every page of the local committed
//! state, and the page count, equal the replica's state at T, the replica holds nothing this
//! database did not write, and a snapshot above T makes no foreign, empty or older database
//! the newest.
//!
//! The replica's state at T is restored into one checksum a page (`DigestSink`, as the
//! database-ahead check does, `crate::ahead`). The local state is the database file overlaid
//! with the WAL frames up to the committed end the boot read, the page count that end's last
//! commit, or the file's length with no frame. A boot holds no read lock, so the wal-index is
//! read again after the pages: a WAL that moved under them is `WalMoved`, and the boot runs
//! again. The first page that differs ends the check.

use std::collections::BTreeMap;
use std::io::{Read, Seek};

use lsr_ltx::{PageChecksum, PageNo, Txid};
use lsr_wal::WalReader;

use crate::ahead::restore_fault;
use crate::fault::Fault;
use crate::fault::role::Role;
use crate::plan::RestoreTarget;
use crate::restore::{RestoreOptions, restore_from};
use crate::source::{CommittedRead, PageSource, WalSource};
use crate::sync::scan_whole;
use crate::verify::DigestSink;

/// Whether the local committed state, read under `committed` (the boot's wal-index read) with
/// `reader` on the live WAL, equals the replica's state at `tip`: the same page size, page count
/// and pages.
///
/// # Errors
/// The [`Fault`] a read met: the replica could not give its state at `tip`, a local page could
/// not be read, or the WAL moved under the reads.
pub(crate) async fn local_is_replica<W: WalSource, P: PageSource, R: Role>(
    wal: &mut W,
    reader: Option<&mut WalReader<W::Wal>>,
    committed: CommittedRead,
    db: &mut P,
    remote: &R,
    tip: Txid,
) -> Result<bool, Fault> {
    let mut image = BTreeMap::new();
    let sink = DigestSink { pages: &mut image };
    let restored = restore_from(
        remote,
        None,
        RestoreTarget::Txid(tip),
        RestoreOptions::default(),
        sink,
    )
    .await;
    // A replica whose state at `tip` does not verify or plan proves nothing equal: the
    // refusal stands. A store that failed is the store's fault, and the boot runs again.
    let restored = match restored.map_err(|err| restore_fault::<R>(&err)) {
        Ok(restored) => restored,
        Err(Fault::RemoteCorrupt) => return Ok(false),
        Err(fault) => return Err(fault),
    };
    let page_size = restored.page_size();
    let same = match reader {
        Some(reader) if reader.header().page_size() != page_size => false,
        Some(reader) => {
            let covered = scan_whole(reader, committed)?;
            let size = match covered.commit() {
                Some(size) => size.get(),
                None => db.page_count().map_err(Fault::from_source)?.get(),
            };
            let local = Local {
                reader: Some((reader, &covered)),
                db,
            };
            size == restored.commit().get() && local.matches(&image, size, page_size)?
        }
        None => {
            let size = db.page_count().map_err(Fault::from_source)?.get();
            let local: Local<'_, '_, W::Wal, P> = Local { reader: None, db };
            size == restored.commit().get() && local.matches(&image, size, page_size)?
        }
    };
    let after = wal.committed().map_err(Fault::from_source)?;
    if after.epoch() != committed.epoch() || after.end() != committed.end() {
        return Err(Fault::WalMoved);
    }
    Ok(same)
}

/// The local committed state's pages: a WAL frame up to the committed end where one holds the
/// page, the database file otherwise.
struct Local<'a, 'm, F, P> {
    reader: Option<(&'a mut WalReader<F>, &'m lsr_wal::PageMap)>,
    db: &'a mut P,
}

impl<F: Read + Seek, P: PageSource> Local<'_, '_, F, P> {
    /// Whether pages 1 to `size` equal `image`, a page it lacks reading as zeros.
    fn matches(
        mut self,
        image: &BTreeMap<u32, PageChecksum>,
        size: u32,
        page_size: lsr_ltx::PageSize,
    ) -> Result<bool, Fault> {
        let zero = vec![0_u8; page_size.bytes()];
        let mut buf = vec![0_u8; page_size.bytes()];
        for n in 1..=size {
            // The lock page is the one number PageNo refuses; neither side holds it.
            let Some(pgno) = PageNo::new(n, page_size) else {
                continue;
            };
            self.read(pgno, &mut buf)?;
            let want = image
                .get(&n)
                .copied()
                .unwrap_or_else(|| PageChecksum::of(pgno, &zero));
            if PageChecksum::of(pgno, &buf) != want {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn read(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), Fault> {
        if let Some((reader, covered)) = self.reader.as_mut()
            && let Some(frame) = covered.get(pgno)
        {
            return reader.read_page(frame, buf).map_err(|_err| Fault::WalMoved);
        }
        self.db.read_page(pgno, buf).map_err(Fault::from_source)
    }
}

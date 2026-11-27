//! Whether the local database is the replica's state: the check a boot with a brand-new meta
//! directory makes before it builds on replica files nothing else vouches for
//! (`docs/decisions/D170.md`).
//!
//! A deployment that does not keep the meta directory boots with no staged file
//! and no takeover record. Once the WAL has restarted it cannot vouch for the replica's newest
//! transaction T, and the boot gate (`crate::takeover`) would stop for another writer on every
//! redeploy. Equality settles it without an operator: when every page of the local committed
//! state, and the page count, equal the replica's state at T, the replica holds nothing this
//! database did not write, and a snapshot above T makes no foreign, empty or older database
//! the newest.
//!
//! The replica's state at T is restored into one checksum a page (`DigestSink`, as the
//! database-ahead check does, `crate::ahead`). The restore reads a tick's bytes at a time ([`Check`]):
//! it is a whole download of the replica, minutes at tens of gigabytes, and a boot tick that held
//! it whole could not be stopped. When it has reads left the boot ends its tick, the next ticks
//! read on, and the boot after them runs again and compares (`docs/decisions/D267.md`). A
//! replica whose newest transaction moved meanwhile has another writer: this engine writes
//! nothing there before it boots. The local state is the database file overlaid
//! with the WAL frames up to the committed end the boot read, the page count that end's last
//! commit, or the file's length with no frame. A boot holds no read lock, so the wal-index is
//! read again after the pages: a WAL that moved under them is `WalMoved`, and the boot runs
//! again.
//!
//! A database that is not equal may still be this replica's database with commits the replica
//! never got: the lost meta directory held them staged, and the replica was unreachable or the
//! process was killed before it uploaded them (issue 329). Ancestry cannot be proven from two
//! images, so the check judges it from two pieces of evidence, both required
//! (`docs/decisions/D194.md`): the database was written after the replica's newest file (its
//! modified time, `WalSource::modified`, against that file's own header timestamp, with no
//! slack), and it holds at least half of the replica state's pages that are not zeros, with a
//! database of one or two pages never passing. Then it is [`Identity::Ahead`] and the boot
//! snapshots above the replica's newest transaction. An empty database, an unrelated one, an
//! image put back with its old modified time, and a live second writer whose files keep landing
//! after this database's last write all fail one of the two and still stop. So the pass over the
//! local pages runs to the end instead of stopping at the first page that differs.
//!
//! One more fact settles the history those two cannot, an older copy of the database (a file
//! backup put back after the replica went on without it): the replicator's own counter,
//! `_litestream_seq` (the [`counter`] module), which each checkpoint of Go's replicator and of
//! ours raises and nothing lowers. A counter below the replica's is an ancestor, so it is
//! [`Identity::Other`] and the boot stops. The local walk finds the counter's page before the
//! restore, the restore keeps that page's bytes, and both are read after it
//! (`docs/decisions/D214.md`). A counter that cannot be read on either side is no evidence.

pub(crate) mod counter;

use std::collections::BTreeMap;
use std::io::{Read, Seek};

use lsr_ltx::{Header, PageChecksum, PageNo, PageSize, Txid};
use lsr_wal::WalReader;

use crate::fault::Fault;
use crate::fault::role::Role;
use crate::inventory::InventoryError;
use crate::restore::{DatabaseSink, RestoreError, Stepped, Stepping};
use crate::source::{CommittedRead, PageSource, WalSource};
use crate::store::StoreError;
use crate::sync::scan_whole;
use crate::verify::DigestSink;

/// Why the check did not finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckError {
    /// A read failed or the WAL moved: retried as any sync step is.
    Fault(Fault),
    /// The replica could not give its state at its newest confirmed transaction because it is
    /// damaged, not because a call failed: retrying does not heal it.
    Unrestorable,
}

impl From<Fault> for CheckError {
    fn from(fault: Fault) -> Self {
        Self::Fault(fault)
    }
}

/// What a restore of the replica's newest confirmed state failing means for the check: a failed
/// call (a read, a listing page) or a replica changing under the restore is the store's fault,
/// retried as such; a history that does not plan, a file or database that does not verify, or a
/// listing no real store returns is the replica's damage.
pub(crate) fn restore_fault<R: Role>(err: &RestoreError) -> CheckError {
    match err {
        RestoreError::Store(StoreError::Corrupt { .. })
        | RestoreError::Inventory(
            InventoryError::ConflictingDuplicate { .. } | InventoryError::ListingStalled(_),
        )
        | RestoreError::NoPlan(_)
        | RestoreError::File { .. }
        | RestoreError::DatabaseChecksum { .. }
        | RestoreError::CreatedAtRepeated { .. } => CheckError::Unrestorable,
        RestoreError::Store(err) | RestoreError::Inventory(InventoryError::Store(err)) => {
            R::fault(err).into()
        }
        RestoreError::Unstable { .. } => Fault::StoreTransient.into(),
        RestoreError::Sink { .. } | RestoreError::Publish { .. } => Fault::LocalIo.into(),
    }
}

/// What the check found the local database to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Identity {
    /// The replica's state at the tip: the same page size, page count and pages.
    Same,
    /// Not that state, but plainly that state plus commits the replica never got: written after
    /// the tip's file, and holding at least half of that state's pages.
    Ahead,
    /// Neither: another database, an older one, or one nothing tells apart from them.
    Other,
}

/// The fewest pages the local database and the replica's state each hold for the database to
/// read as [`Identity::Ahead`]: a database of one or two pages shares half its pages with
/// nearly anything of the same schema.
const AHEAD_MIN_PAGES: u32 = 3;

/// The replica's state at `tip` restored into one checksum a page, with the bytes of the one page
/// the local counter lives on (`root`): the restore's [`DatabaseSink`].
/// The local counter (its root page and value) is read once, as the restore begins, so the
/// restore knows the page to keep.
#[derive(Debug, Default)]
pub(crate) struct Image {
    pages: BTreeMap<u32, PageChecksum>,
    counter: Option<(PageNo, u64)>,
    kept: Option<Vec<u8>>,
}

impl Image {
    fn digest(&mut self) -> DigestSink<'_> {
        DigestSink {
            pages: &mut self.pages,
            keep: self.counter.map(|(root, _)| (root.get(), &mut self.kept)),
        }
    }
}

impl DatabaseSink for Image {
    fn write_page(&mut self, page: lsr_ltx::PageRef<'_>) -> std::io::Result<()> {
        self.digest().write_page(page)
    }

    fn truncate(&mut self, pages: lsr_ltx::DbPages, page_size: PageSize) -> std::io::Result<()> {
        self.digest().truncate(pages, page_size)
    }

    fn reset(&mut self) -> std::io::Result<()> {
        self.digest().reset()
    }

    fn checksum(
        &mut self,
        pages: lsr_ltx::DbPages,
        page_size: PageSize,
    ) -> std::io::Result<lsr_ltx::DbChecksum> {
        self.digest().checksum(pages, page_size)
    }

    type PublishError = std::convert::Infallible;

    fn publish(self) -> Result<(), Self::PublishError> {
        Ok(())
    }
}

/// The check between the boot's ticks (`docs/decisions/D267.md`): the replica's state at `tip`
/// restored a tick's bytes at a time, then held for the boot that compares it with the local
/// database. In memory only: a new engine checks afresh.
#[derive(Debug)]
pub(crate) enum Check {
    /// Being restored.
    Restoring {
        /// The replica's newest transaction, the state restored.
        tip: Txid,
        /// The restore.
        restore: Box<Stepped<Image>>,
    },
    /// Restored.
    Restored {
        /// The state's transaction.
        tip: Txid,
        /// The state.
        image: Box<Image>,
        /// The last file applied, whose header gives the state's size and time.
        last: Header,
    },
    /// The replica could not give its state at `tip` because it is damaged: no evidence.
    Unrestorable {
        /// The transaction asked for.
        tip: Txid,
    },
}

impl Check {
    /// The replica's newest transaction when the check began.
    pub(crate) const fn tip(&self) -> Txid {
        match self {
            Self::Restoring { tip, .. }
            | Self::Restored { tip, .. }
            | Self::Unrestorable { tip } => *tip,
        }
    }

    /// Whether the restore has reads left.
    pub(crate) const fn going(&self) -> bool {
        matches!(self, Self::Restoring { .. })
    }

    /// Begins the restore of the replica's state at `tip`, keeping the page the local counter
    /// lives on (read under `committed` with `reader`), and reads its first `budget` bytes.
    ///
    /// # Errors
    /// The [`Fault`] a read met; a damaged replica is [`Check::Unrestorable`], not an error.
    pub(crate) async fn begin<F: Read + Seek, P: PageSource, R: Role>(
        reader: Option<&mut WalReader<F>>,
        (committed, page_size): (CommittedRead, PageSize),
        db: &mut P,
        remote: &R,
        (tip, budget): (Txid, u64),
    ) -> Result<Self, Fault> {
        let counter = match reader {
            Some(reader) if reader.header().page_size() != page_size => None,
            reader => Local::open(reader, committed, db)?.counter(page_size)?,
        };
        let image = Image {
            counter,
            ..Image::default()
        };
        let check = match Stepped::begin(remote, tip, image, budget).await {
            Ok(restore) => Self::Restoring {
                tip,
                restore: Box::new(restore),
            },
            Err(err) => Self::failed::<R>(tip, &err)?,
        };
        check.step(remote, budget).await
    }

    /// Reads the restore on by up to `budget` bytes; a check that is not restoring is as it was.
    ///
    /// # Errors
    /// As [`Self::begin`].
    pub(crate) async fn step<R: Role>(self, remote: &R, budget: u64) -> Result<Self, Fault> {
        let Self::Restoring { tip, restore } = self else {
            return Ok(self);
        };
        match restore.step(remote, budget).await {
            Ok(Stepping::Going(restore)) => Ok(Self::Restoring { tip, restore }),
            Ok(Stepping::Done { sink, last }) => Ok(Self::Restored {
                tip,
                image: Box::new(sink),
                last: *last.header(),
            }),
            Err(err) => Self::failed::<R>(tip, &err),
        }
    }

    fn failed<R: Role>(tip: Txid, err: &RestoreError) -> Result<Self, Fault> {
        match restore_fault::<R>(err) {
            CheckError::Unrestorable => Ok(Self::Unrestorable { tip }),
            CheckError::Fault(fault) => Err(fault),
        }
    }
}

/// What the local committed state, read under `committed` (the boot's wal-index read) with
/// `reader` on the live WAL, is against the replica's state the `check` restored, whose page
/// size is `page_size`. A check still restoring, or one that could not restore, is
/// [`Identity::Other`]: no evidence.
///
/// # Errors
/// The [`Fault`] a read met: a local page or the files' modified time could not be read, or
/// the WAL moved under the reads.
pub(crate) fn local_against_replica<W: WalSource, P: PageSource>(
    wal: &mut W,
    reader: Option<&mut WalReader<W::Wal>>,
    (committed, page_size): (CommittedRead, PageSize),
    db: &mut P,
    check: Check,
) -> Result<Identity, Fault> {
    let Check::Restored { image, last, .. } = check else {
        return Ok(Identity::Other);
    };
    let mut local = match reader {
        Some(reader) if reader.header().page_size() != page_size => None,
        reader => Some(Local::open(reader, committed, db)?),
    };
    let replica = last.pages().get();
    let compared = match local.as_mut() {
        Some(local) if last.page_size() == page_size => {
            Some(local.compare(&image.pages, replica, page_size)?)
        }
        Some(_) | None => None,
    };
    drop(local);
    let after = wal.committed().map_err(Fault::from_source)?;
    if after.epoch() != committed.epoch() || after.end() != committed.end() {
        return Err(Fault::WalMoved);
    }
    let Some(compared) = compared else {
        return Ok(Identity::Other);
    };
    if compared.same() {
        return Ok(Identity::Same);
    }
    if !compared.ahead_bar() {
        return Ok(Identity::Other);
    }
    // The replica's newest file's own header time, against the database's modified time: no
    // slack, and a time that cannot be read is no evidence.
    let written = wal.modified().map_err(Fault::from_source)?;
    if !written.is_some_and(|written| written > last.fields().timestamp) {
        return Ok(Identity::Other);
    }
    // An older copy of this database passes everything above; its counter is below the
    // replica's. A counter either side cannot read says nothing.
    let theirs = image.kept.as_deref().and_then(counter::value);
    let older = match (image.counter, theirs) {
        (Some((_, mine)), Some(theirs)) => mine < theirs,
        (Some(_) | None, Some(_) | None) => false,
    };
    Ok(if older {
        Identity::Other
    } else {
        Identity::Ahead
    })
}

/// What one pass over the local pages found against the replica's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Compared {
    /// Every page up to the shorter of the two equal, the page counts equal too.
    equal: bool,
    /// The local database's page count.
    local: u32,
    /// The replica state's page count.
    replica: u32,
    /// The replica state's pages that are not all zeros.
    nonzero: u32,
    /// Of those, the ones the local database holds unchanged.
    shared: u32,
}

impl Compared {
    const fn same(&self) -> bool {
        self.equal
    }

    /// Whether the database shares enough of the replica's state to be taken for it plus more:
    /// at least half of its pages that are not zeros, both sides holding [`AHEAD_MIN_PAGES`].
    fn ahead_bar(&self) -> bool {
        self.local >= AHEAD_MIN_PAGES
            && self.replica >= AHEAD_MIN_PAGES
            && self.nonzero > 0
            && u64::from(self.shared).saturating_mul(2) >= u64::from(self.nonzero)
    }
}

/// The local committed state's pages: a WAL frame up to the committed end where one holds the
/// page, the database file otherwise.
struct Local<'a, F, P> {
    reader: Option<(&'a mut WalReader<F>, lsr_wal::PageMap)>,
    db: &'a mut P,
    /// The committed page count: the last commit's, or the file's length with no frame.
    size: u32,
}

impl<'a, F: Read + Seek, P: PageSource> Local<'a, F, P> {
    fn open(
        reader: Option<&'a mut WalReader<F>>,
        committed: CommittedRead,
        db: &'a mut P,
    ) -> Result<Self, Fault> {
        match reader {
            Some(reader) if committed.end().offset().frame_index() > 0 => {
                let covered = scan_whole(reader, committed)?;
                let size = match covered.commit() {
                    Some(size) => size.get(),
                    None => db.page_count().map_err(Fault::from_source)?.get(),
                };
                Ok(Self {
                    reader: Some((reader, covered)),
                    db,
                    size,
                })
            }
            Some(_) | None => {
                let size = db.page_count().map_err(Fault::from_source)?.get();
                Ok(Self {
                    reader: None,
                    db,
                    size,
                })
            }
        }
    }

    /// The replicator's counter table in the local database: its root page and the value
    /// there, or `None` when the schema or the page does not show one.
    fn counter(&mut self, page_size: lsr_ltx::PageSize) -> Result<Option<(PageNo, u64)>, Fault> {
        let pages = self.size;
        let Some(root) = counter::locate(page_size, pages, &mut |pgno, buf| self.read(pgno, buf))?
        else {
            return Ok(None);
        };
        let mut buf = vec![0_u8; page_size.bytes()];
        self.read(root, &mut buf)?;
        Ok(counter::value(&buf).map(|value| (root, value)))
    }

    /// Pages 1 to the shorter of the local size and `replica` against `image`, a page it lacks
    /// reading as zeros.
    fn compare(
        &mut self,
        image: &BTreeMap<u32, PageChecksum>,
        replica: u32,
        page_size: lsr_ltx::PageSize,
    ) -> Result<Compared, Fault> {
        let local = self.size;
        let zero = vec![0_u8; page_size.bytes()];
        let mut buf = vec![0_u8; page_size.bytes()];
        let mut equal = local == replica;
        let mut shared = 0_u32;
        for n in 1..=local.min(replica) {
            // The lock page is the one number PageNo refuses; neither side holds it.
            let Some(pgno) = PageNo::new(n, page_size) else {
                continue;
            };
            self.read(pgno, &mut buf)?;
            let blank = PageChecksum::of(pgno, &zero);
            let want = image.get(&n).copied().unwrap_or(blank);
            let same = PageChecksum::of(pgno, &buf) == want;
            equal &= same;
            if same && want != blank {
                shared = shared.saturating_add(1);
            }
        }
        let nonzero = image
            .iter()
            .filter(|(n, sum)| {
                **n <= replica
                    && PageNo::new(**n, page_size)
                        .is_some_and(|pgno| **sum != PageChecksum::of(pgno, &zero))
            })
            .count();
        Ok(Compared {
            equal,
            local,
            replica,
            nonzero: u32::try_from(nonzero).unwrap_or(u32::MAX),
            shared,
        })
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use lsr_ltx::PageSize;

    use super::*;
    use crate::source::FakeDb;

    fn pg(n: u32) -> PageNo {
        PageNo::new(n, PageSize::MIN).unwrap()
    }

    fn compared(local: u32, replica: u32, nonzero: u32, shared: u32) -> Compared {
        Compared {
            equal: false,
            local,
            replica,
            nonzero,
            shared,
        }
    }

    /// The image a restore builds: a file that shrinks the database drops the pages above its
    /// end, and a restore that starts over holds nothing.
    #[test]
    fn the_image_drops_pages_past_a_shrink_and_everything_on_a_reset() {
        let size = PageSize::MIN;
        let page = vec![7_u8; size.bytes()];
        let mut image = Image {
            counter: Some((pg(3), 1)),
            ..Image::default()
        };
        for n in 1..=3 {
            image
                .write_page(lsr_ltx::PageRef::new(pg(n), size, &page).unwrap())
                .unwrap();
        }
        assert_eq!(image.pages.len(), 3);
        assert_eq!(image.kept.as_deref(), Some(&page[..]));
        image.truncate(lsr_ltx::DbPages::new(2), size).unwrap();
        assert_eq!(image.pages.keys().copied().collect::<Vec<_>>(), [1, 2]);
        image
            .write_page(lsr_ltx::PageRef::new(pg(3), size, &page).unwrap())
            .unwrap();
        image.reset().unwrap();
        assert!(image.pages.is_empty() && image.kept.is_none());
    }

    /// The bar for reading a database as the replica plus more: three pages on each side, some
    /// page of the replica that is not zeros, and at least half of those held unchanged.
    #[test]
    fn the_ahead_bar_asks_three_pages_a_side_and_half_the_nonzero_pages_shared() {
        assert!(compared(3, 3, 2, 1).ahead_bar(), "exactly half");
        assert!(compared(9, 3, 4, 2).ahead_bar(), "a longer database");
        assert!(!compared(3, 3, 3, 1).ahead_bar(), "under half");
        assert!(!compared(2, 3, 2, 2).ahead_bar(), "two local pages");
        assert!(!compared(3, 2, 2, 2).ahead_bar(), "two replica pages");
        assert!(!compared(3, 3, 0, 0).ahead_bar(), "a replica of only zeros");
    }

    /// The replica's non-zero pages are counted up to its own end: an image page past it (a
    /// restore that grew and shrank) is not one the local database could share.
    #[test]
    fn the_nonzero_count_stops_at_the_replicas_end() {
        let size = PageSize::MIN;
        let page = |fill: u8| vec![fill; size.bytes()];
        let mut db = FakeDb::new(size);
        db.write_page(pg(1), &page(1));
        db.write_page(pg(2), &page(2));
        let image: BTreeMap<u32, PageChecksum> = [(1, 1), (2, 2), (5, 5)]
            .into_iter()
            .map(|(n, fill)| (n, PageChecksum::of(pg(n), &page(fill))))
            .collect();
        let mut local: Local<'_, Cursor<Vec<u8>>, FakeDb> = Local {
            reader: None,
            db: &mut db,
            size: 2,
        };
        let found = local.compare(&image, 2, size).unwrap();
        assert_eq!(
            found,
            Compared {
                equal: true,
                local: 2,
                replica: 2,
                nonzero: 2,
                shared: 2,
            }
        );
    }
}

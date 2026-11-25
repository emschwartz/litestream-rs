//! The page checksum table: what each database-file page should hold.
//!
//! SQLite writes a page to the WAL and later copies it into the database file;
//! nothing in the file says it was copied right, or that nothing else wrote it
//! since. The engine reads database-file pages for a snapshot (every page the WAL
//! does not hold) and for the pages the database grew by that no scanned WAL frame
//! holds (the "grown pages" below; `plan_pages` in `sync.rs`). [`PageChecksumTable`] is checked
//! before every such page is used, so a page that changed since the engine last
//! knew it is caught instead of shipped silently.
//!
//! What feeds it:
//! - Every WAL page a sync step staged is recorded with the digest the scan
//!   already computed on its checked read ([`lsr_wal::FrameRef::digest`]); the
//!   database file holds that page once SQLite backfills it.
//! - At a boot that continues (`Continue` or `WalRestarted`, no snapshot), every
//!   page of the staged level-0 files, oldest first so the newest wins
//!   ([`seeded`]). Boot reads them from local staging; no replica call. A file that
//!   does not verify empties the table and seeding starts again after it.
//! - A page not recorded yet is recorded by its first read from the database
//!   file: trust on first use, as Go trusts every read (`db.go:1562`).
//!
//! When it is emptied: at every snapshot whose cause is not the checkpoint
//! boundary or an operator's request (after a rewind it holds digests of frames
//! a power loss removed; after an unproven restart, of frames the database may
//! never have got), and when the page size changes. [`PageChecksumTable::truncate`]
//! drops pages above the commit when the database shrinks.
//!
//! What it cannot check:
//! - A database page above the previous commit that no scanned frame holds
//!   (`plan_pages`' grown pages in `sync.rs`). The table holds no page above
//!   the last commit it saw, since a shrink forgets them, so every such read is
//!   a first read and is trusted; a later snapshot checks the page.
//! - Any page at its first read after the table was emptied.
//! - The page count, `PageSource::page_count`: a file cut short or grown is read
//!   as the size it has.
//!
//! So a database-file page reaches an LTX file unchecked only on its first read
//! since the table last forgot it; a page any frame has shown is always checked.
//!
//! A page that does not match is [`Mismatch`], which the step turns into
//! `Fault::DatabasePageMismatch`. The table remembers the bytes it found: under
//! the response that keeps trying, the retry finds those same bytes and ships
//! them (that is the database the application serves), while the page stays reported
//! until a verified WAL frame records it again, the table is emptied, or an
//! operator accepts it ([`PageChecksumTable::accept`]).
//!
//! Memory: one entry per recorded page, a 4-byte page number and an 8-byte
//! digest in a `BTreeMap`, about 13 to 29 bytes a page with the map's nodes:
//! 3.4 to 7.6 MiB for a 1 GiB database of 4 KiB pages (262,144 pages). Entries
//! never exceed the database's page count: every staged step truncates to its
//! commit.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use lsr_ltx::{Event, Header, PageChecksum, PageNo, PageSize, Txid, TxidRange};

use crate::fault::role::Role;
use crate::fault::{Fault, SnapshotCause};
use crate::identity::{CheckError, restore_fault};
use crate::layout::{FileKey, Level};
use crate::ltx_read::{LtxRead, ReadFault};
use crate::plan::RestoreTarget;
use crate::recover::read_header;
use crate::restore::{RestoreOptions, restore_once};
use crate::store::{ListAt, PageLimit, ReadLen};
use crate::verify::DigestSink;

/// How much of a staged file one read asks for while seeding.
const SEED_READ: ReadLen = ReadLen::CHUNK;

/// A database-file page whose bytes are not the ones the table holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mismatch {
    /// The page.
    pub pgno: PageNo,
    /// The digest the table held.
    pub recorded: PageChecksum,
    /// The digest of the bytes read.
    pub found: PageChecksum,
}

/// Page number to the checksum of the bytes the database file should hold.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageChecksumTable {
    page_size: Option<PageSize>,
    pages: BTreeMap<PageNo, PageChecksum>,
    /// Pages found different from `pages`, with the digest found: reported,
    /// and accepted as the file's bytes until a frame records the page again.
    mismatched: BTreeMap<PageNo, PageChecksum>,
}

impl PageChecksumTable {
    /// An empty table: every page is recorded on its first read.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            page_size: None,
            pages: BTreeMap::new(),
            mismatched: BTreeMap::new(),
        }
    }

    /// Empties the table when `page_size` is not the size it holds pages of.
    pub fn fit(&mut self, page_size: PageSize) {
        if self.page_size != Some(page_size) {
            self.clear();
            self.page_size = Some(page_size);
        }
    }

    /// Records a page from a verified WAL frame; a mismatch on it is over.
    pub fn record(&mut self, pgno: PageNo, digest: PageChecksum) {
        self.pages.insert(pgno, digest);
        self.mismatched.remove(&pgno);
    }

    /// Checks a page read from the database file, recording it when the table
    /// has none. A page already reported as mismatched passes when it still has
    /// the bytes it was reported with.
    ///
    /// # Errors
    /// [`Mismatch`] when the bytes differ from both.
    pub fn check(&mut self, pgno: PageNo, page: &[u8]) -> Result<(), Mismatch> {
        let found = PageChecksum::of(pgno, page);
        let Some(recorded) = self.pages.get(&pgno).copied() else {
            self.pages.insert(pgno, found);
            return Ok(());
        };
        if recorded == found || self.mismatched.get(&pgno) == Some(&found) {
            return Ok(());
        }
        self.mismatched.insert(pgno, found);
        Err(Mismatch {
            pgno,
            recorded,
            found,
        })
    }

    /// Forgets every page above `commit`, the database's size in pages.
    pub fn truncate(&mut self, commit: NonZeroU32) {
        self.pages.retain(|pgno, _| pgno.get() <= commit.get());
        self.mismatched.retain(|pgno, _| pgno.get() <= commit.get());
    }

    /// Forgets every page.
    pub fn clear(&mut self) {
        self.pages.clear();
        self.mismatched.clear();
    }

    /// An operator accepts the database file as it is: every mismatched page's
    /// bytes become the ones the table holds.
    pub fn accept(&mut self) {
        let accepted = std::mem::take(&mut self.mismatched);
        self.pages.extend(accepted);
    }

    /// The lowest page still reported as mismatched.
    #[must_use]
    pub fn mismatch(&self) -> Option<PageNo> {
        self.mismatched.keys().next().copied()
    }

    /// The digest the table holds for `pgno`.
    #[must_use]
    pub fn get(&self, pgno: PageNo) -> Option<PageChecksum> {
        self.pages.get(&pgno).copied()
    }

    /// How many pages are recorded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Whether no page is recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}

/// Whether a snapshot for `cause` empties the table first. An operator's
/// request takes a snapshot of a database whose pages the table still
/// describes. Every other cause follows a WAL whose frames the table may hold
/// and the database never got, or, at the checkpoint boundary, a database the
/// TRUNCATE wrote frames into that the table never saw: the checkpoint ends the
/// engine's read transaction, so a commit that landed after the last step is
/// backfilled with the rest, and its pages would read as damage
/// (docs/decisions.md D121).
#[must_use]
pub(crate) const fn snapshot_clears(cause: &SnapshotCause) -> bool {
    match cause {
        SnapshotCause::OperatorRequested => false,
        SnapshotCause::CheckpointBoundary
        | SnapshotCause::FirstSync
        | SnapshotCause::BehindRemote { .. }
        | SnapshotCause::NewestStagedFileUnreadable
        | SnapshotCause::WalMissing
        | SnapshotCause::GenerationChanged
        | SnapshotCause::PrevFrameMismatch
        | SnapshotCause::LastPageMismatch
        | SnapshotCause::LocalRewound { .. }
        | SnapshotCause::StagedUnverified { .. }
        | SnapshotCause::AheadOfReplica { .. } => true,
    }
}

/// The table a boot that continues starts from: every page of every staged
/// level-0 file from `from` on, oldest first, then truncated to `commit`, the
/// newest staged file's size.
///
/// `from` is the replica's newest transaction: boot vouched for its staged
/// copy (compared with the replica's object) and for every pending file
/// after it (witnessed against the live WAL, `crate::witness`). An older
/// staged file nothing vouched for may have been put back or replaced, and
/// seeding from it would report the database file damaged at a page it
/// never wrote; its pages are left to be learnt again.
///
/// # Errors
/// The staging store's fault when a list or read fails.
pub(crate) async fn seeded<L: Role>(
    staging: &L,
    from: Option<Txid>,
    commit: NonZeroU32,
) -> Result<Seeded, Fault> {
    let mut seeded = Seeded {
        table: PageChecksumTable::new(),
        quiet: BTreeSet::new(),
        unplaced: None,
    };
    let mut prev: Option<SeedRead> = None;
    let mut placed = false;
    let mut at = ListAt::first(Level::ZERO);
    loop {
        let page = staging
            .list(at.level, at.after, PageLimit::MAX)
            .await
            .map_err(|err| L::fault(&err))?;
        let vouched = page
            .files
            .iter()
            .filter(|info| from.is_none_or(|from| info.key.range().max() >= from));
        for info in vouched {
            let pending = from.is_none_or(|from| info.key.range().min() > from);
            let read = seed_file(staging, &info.key, &mut seeded.table, prev).await?;
            // A file whose WAL span ends where the one before it ended holds no frame the
            // replica's newest does not (a snapshot taken for a doubt on a quiet WAL), though
            // it rewrites pages the table never saw.
            let nothing_new = |read: SeedRead| {
                read.wal_end.is_some() && prev.is_some_and(|prev| prev.wal_end == read.wal_end)
            };
            if pending && read.is_some_and(|read| !read.changes || nothing_new(read)) {
                seeded.quiet.insert(info.key.range().max());
            } else if pending
                && !placed
                && prev.is_none()
                && from.is_some()
                && let Some(end) = read.and_then(|read| read.wal_end)
            {
                seeded.unplaced = Some((info.key, end));
            }
            placed |= pending;
            prev = read;
        }
        match at
            .following(page.next)
            .map_err(Fault::from_staging_listing)?
        {
            Some(next) => at = next,
            None => break,
        }
    }
    seeded.table.truncate(commit);
    Ok(seeded)
}

/// What a boot's seeding found: the table, and the pending staged files that change no page of
/// the state the files before them left (`crate::failstop`: they hold no commit of the
/// application's, only the engine's own writes).
#[derive(Debug)]
pub(crate) struct Seeded {
    /// The page checksum table.
    pub(crate) table: PageChecksumTable,
    /// The pending files that change nothing, by transaction.
    pub(crate) quiet: BTreeSet<Txid>,
    /// The first pending file when no staged file stands before it (the replica's newest file
    /// was pruned from staging, or staging was lost): the file and where its WAL span ends. Nothing local says what it follows, so [`Seeded::place`] asks the replica.
    pub(crate) unplaced: Option<(FileKey, WalEnd)>,
}

/// Where a file's WAL span ends: the salts and the byte offset.
type WalEnd = ((u32, u32), u64);

/// Where the WAL span in `header` ends, when it has one.
fn wal_end(header: &Header) -> Option<WalEnd> {
    let wal = header.fields().wal?;
    Some((wal.salts(), wal.offset().checked_add(wal.size())?))
}

impl Seeded {
    /// Marks the unplaced file quiet when it holds no commit the replica lacks, judged against
    /// the replica's newest file `tip`, the same rule that marks a file quiet after a staged
    /// one (a snapshot taken for a doubt on a quiet WAL, or after the database was found equal
    /// to the replica). Without it a restart loses that knowledge and dates the file from its
    /// stamp as a commit. Two proofs, cheapest first:
    ///
    /// - its WAL span ends where `tip`'s ends: it reads no frame past what the replica holds;
    /// - it follows a WAL generation other than `tip`'s (a restarted or deleted WAL: the span
    ///   says nothing about what came before) and the replica's state at `tip`, restored into
    ///   page checksums, is what the file would leave: the same size, every page it holds equal.
    ///   Costs a restore of the replica, only on a boot with such a file pending and no staged
    ///   file before it.
    ///
    /// A span in the same generation that ends elsewhere holds frames the replica lacks (or is
    /// older than it), a header with no span or a file that does not verify proves nothing,
    /// and a newest file the replica does not hold leaves the file counted: it may hold a
    /// commit.
    ///
    /// # Errors
    /// The replica's fault when the newest file's header cannot be read, other than that it is
    /// missing, or its state cannot be restored; the staging store's when the file cannot be
    /// read.
    pub(crate) async fn place<L: Role, R: Role>(
        &mut self,
        staging: &L,
        remote: &R,
        tip: Option<Txid>,
    ) -> Result<(), Fault> {
        let (Some((file, end)), Some(tip)) = (self.unplaced.take(), tip) else {
            return Ok(());
        };
        let key = FileKey::new(Level::ZERO, TxidRange::single(tip));
        let header = match read_header(remote, &key, R::fault).await {
            Ok(Some(header)) => header,
            Ok(None) => return Ok(()),
            // The tip the engine believes the replica holds is not there (an upload whose
            // answer was lost, a bucket rolled back): the boot lists the replica and decides
            // from that, and the file stays counted, as with any span that ends elsewhere.
            // Failing the boot instead would retry the same read for as long as the belief
            // lasts.
            Err(Fault::RemoteObjectMissing) => return Ok(()),
            Err(fault) => return Err(fault),
        };
        let quiet = match wal_end(&header) {
            Some(tip_end) if tip_end == end => true,
            Some((salts, _)) if salts == end.0 => false,
            Some(_) | None => leaves_replica_state(staging, remote, &file, tip).await?,
        };
        if quiet {
            self.quiet.insert(file.range().max());
        }
        Ok(())
    }
}

/// Whether staged `file`, applied over the replica's state at `tip`, changes nothing: the
/// same size and every page it holds the same. A replica state that cannot be restored, or a
/// file that does not verify, proves nothing.
async fn leaves_replica_state<L: Role, R: Role>(
    staging: &L,
    remote: &R,
    file: &FileKey,
    tip: Txid,
) -> Result<bool, Fault> {
    let mut image = BTreeMap::new();
    let sink = DigestSink { pages: &mut image };
    let restored = restore_once(
        remote,
        RestoreTarget::Txid(tip),
        RestoreOptions::default(),
        sink,
    )
    .await
    .map_err(|err| restore_fault::<R>(&err));
    let restored = match restored {
        Ok(restored) => restored,
        Err(CheckError::Unrestorable) => return Ok(false),
        Err(CheckError::Fault(fault)) => return Err(fault),
    };
    let mut table = PageChecksumTable::new();
    table.fit(restored.page_size());
    for (pgno, digest) in image {
        // A page number the restore wrote is in range for its page size.
        if let Some(pgno) = PageNo::new(pgno, restored.page_size()) {
            table.record(pgno, digest);
        }
    }
    let state = SeedRead {
        commit: restored.commit().get(),
        changes: false,
        wal_end: None,
    };
    let read = seed_file(staging, file, &mut table, Some(state)).await?;
    Ok(read.is_some_and(|read| !read.changes))
}

/// One seeded file: its database size, and whether it may change the database.
#[derive(Clone, Copy)]
struct SeedRead {
    commit: u32,
    changes: bool,
    /// Where in the WAL the file's frames end.
    wal_end: Option<WalEnd>,
}

/// Records every page of one staged file. A file that does not decode and
/// verify whole empties the table: what it rewrote is unknown, and `None` says so.
async fn seed_file<L: Role>(
    staging: &L,
    key: &FileKey,
    table: &mut PageChecksumTable,
    prev: Option<SeedRead>,
) -> Result<Option<SeedRead>, Fault> {
    let chunk = SEED_READ;
    let mut pages = Vec::new();
    let mut read = LtxRead::new(*key, chunk);
    let verified: Result<_, ReadFault> = read
        .read_verified(staging, |event| {
            if let Event::Page(page) = event {
                pages.push((page.pgno(), page.checksum()));
            }
            Ok(())
        })
        .await;
    match verified {
        Ok(verified) => {
            table.fit(verified.header().page_size());
            let commit = verified.header().fields().commit;
            let wal_end = wal_end(verified.header());
            let changes = prev.map(|prev| prev.commit) != Some(commit)
                || pages
                    .iter()
                    .any(|(pgno, digest)| table.get(*pgno) != Some(*digest));
            for (pgno, digest) in pages {
                table.record(pgno, digest);
            }
            Ok(Some(SeedRead {
                commit,
                changes,
                wal_end,
            }))
        }
        Err(ReadFault::Store(err)) => Err(L::fault(&err)),
        Err(ReadFault::Decode(_)) => {
            table.clear();
            Ok(None)
        }
        Err(ReadFault::Rejected(never)) => match never {},
    }
}

#[cfg(test)]
mod tests;

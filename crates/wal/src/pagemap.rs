//! The newest committed version of every page across a run of transactions.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::num::NonZeroU32;

use lsr_ltx::{PageNo, PageSize};

use crate::error::PageMapError;
use crate::frame::{FrameRef, WalOffset};
use crate::header::Salts;
use crate::scan::CommittedTx;

/// Which WAL frame holds each page's newest committed version, and the
/// database size after the last transaction applied.
///
/// A map holds one contiguous run of one generation: the first transaction
/// fixes where it starts and which salts it has, and every later one must start
/// exactly where the map ends and carry the same salts, so `range` never
/// spans a transaction the map did not see. A transaction that shrinks the
/// database drops every page above its new size, those of earlier transactions
/// included, so a page that returns later comes only from a later frame.
#[derive(Clone, Debug)]
pub struct PageMap {
    pages: BTreeMap<PageNo, FrameRef>,
    run: Option<Run>,
    end: WalOffset,
}

/// What the first transaction fixes and the latest one sets.
#[derive(Clone, Copy, Debug)]
struct Run {
    start: WalOffset,
    salts: Salts,
    commit: NonZeroU32,
}

impl PageMap {
    /// An empty map for a WAL of `page_size`.
    #[must_use]
    pub const fn new(page_size: PageSize) -> Self {
        Self {
            pages: BTreeMap::new(),
            run: None,
            end: WalOffset::start(page_size),
        }
    }

    /// Applies the next transaction.
    ///
    /// # Errors
    /// [`PageMapError`] when the transaction is for another page size or generation, or
    /// does not start exactly where those applied end.
    pub fn apply(&mut self, tx: &CommittedTx) -> Result<(), PageMapError> {
        if tx.end().next().page_size() != self.end.page_size() {
            return Err(PageMapError::PageSizeMismatch {
                expected: self.end.page_size(),
                found: tx.end().next().page_size(),
            });
        }
        let found = tx.end().salts();
        if let Some(expected) = self.salts().filter(|expected| *expected != found) {
            return Err(PageMapError::Generation { expected, found });
        }
        // A committed transaction always holds its commit frame, so the fallback never applies.
        let first = tx.frames().first().map_or(self.end, FrameRef::offset);
        if self.run.is_some() {
            match first.frame_index().cmp(&self.end.frame_index()) {
                Ordering::Less => {
                    return Err(PageMapError::NotAfterEnd {
                        end: self.end,
                        found: first,
                    });
                }
                Ordering::Greater => {
                    return Err(PageMapError::Gap {
                        expected: self.end,
                        found: first,
                    });
                }
                Ordering::Equal => {}
            }
        }
        for frame in tx.frames() {
            self.pages.insert(frame.pgno(), *frame);
        }
        let commit = tx.commit();
        while let Some(top) = self.pages.last_entry() {
            if top.key().get() <= commit.get() {
                break;
            }
            top.remove();
        }
        self.run = Some(Run {
            start: self.run.map_or(first, |run| run.start),
            salts: found,
            commit,
        });
        self.end = tx.end().next();
        Ok(())
    }

    /// The newest frame of every page, in ascending page order.
    pub fn pages(&self) -> impl Iterator<Item = &FrameRef> {
        self.pages.values()
    }

    /// The newest frame of one page, if the WAL holds it.
    #[must_use]
    pub fn get(&self, pgno: PageNo) -> Option<&FrameRef> {
        self.pages.get(&pgno)
    }

    /// The database size in pages after the last transaction applied; `None` before any.
    #[must_use]
    pub const fn commit(&self) -> Option<NonZeroU32> {
        match self.run {
            Some(run) => Some(run.commit),
            None => None,
        }
    }

    /// The WAL bytes the applied transactions span: the first frame's start and the last commit frame's end.
    /// An empty map spans nothing, from the first frame position to itself.
    #[must_use]
    pub fn range(&self) -> (WalOffset, WalOffset) {
        (
            self.run
                .map_or(WalOffset::start(self.end.page_size()), |run| run.start),
            self.end,
        )
    }

    /// The generation of the applied transactions; `None` before any.
    #[must_use]
    pub const fn salts(&self) -> Option<Salts> {
        match self.run {
            Some(run) => Some(run.salts),
            None => None,
        }
    }

    /// The page size of the WAL this map describes.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.end.page_size()
    }
}

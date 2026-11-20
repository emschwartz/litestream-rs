//! The newest committed version of every page across a run of transactions.

use std::collections::BTreeMap;

use lsr_ltx::{PageNo, PageSize};

use crate::error::PageMapError;
use crate::frame::{FrameRef, WalOffset};
use crate::scan::CommittedTx;

/// Which WAL frame holds each page's newest committed version, and the
/// database size after the last transaction applied.
///
/// Transactions must be applied in WAL order. A transaction that shrinks the
/// database drops every page above its new size, those of earlier transactions
/// included, so a page that returns later comes only from a later frame.
#[derive(Clone, Debug)]
pub struct PageMap {
    page_size: PageSize,
    pages: BTreeMap<PageNo, FrameRef>,
    commit: u32,
    start: Option<WalOffset>,
    end: WalOffset,
}

impl PageMap {
    /// An empty map for a WAL of `page_size`.
    #[must_use]
    pub const fn new(page_size: PageSize) -> Self {
        Self {
            page_size,
            pages: BTreeMap::new(),
            commit: 0,
            start: None,
            end: WalOffset::start(page_size),
        }
    }

    /// Applies the next transaction.
    ///
    /// # Errors
    /// [`PageMapError`] when the transaction is for another page size or starts before the end of those applied.
    pub fn apply(&mut self, tx: &CommittedTx) -> Result<(), PageMapError> {
        if tx.end().next().page_size() != self.page_size {
            return Err(PageMapError::PageSizeMismatch {
                expected: self.page_size,
                found: tx.end().next().page_size(),
            });
        }
        let first = tx.frames().first().map(FrameRef::offset);
        if let Some(found) = first.filter(|f| f.frame_index() < self.end.frame_index()) {
            return Err(PageMapError::NotAfterEnd {
                end: self.end,
                found,
            });
        }
        for frame in tx.frames() {
            self.pages.insert(frame.pgno(), *frame);
        }
        self.commit = tx.commit();
        while let Some(top) = self.pages.last_entry() {
            if top.key().get() <= self.commit {
                break;
            }
            top.remove();
        }
        self.start = self.start.or(first);
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

    /// The database size in pages after the last transaction applied; zero before any.
    #[must_use]
    pub const fn commit(&self) -> u32 {
        self.commit
    }

    /// The WAL bytes the applied transactions span: the first frame's start and the last commit frame's end.
    /// An empty map spans nothing, from the first frame position to itself.
    #[must_use]
    pub fn range(&self) -> (WalOffset, WalOffset) {
        (
            self.start.unwrap_or(WalOffset::start(self.page_size)),
            self.end,
        )
    }

    /// The page size of the WAL this map describes.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.page_size
    }
}

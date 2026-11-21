//! [MemSink]: the reference [DatabaseSink], with fault injection.
//!
//! Pages live in a map and an all-zero page is not stored, so a database that
//! spans the lock page costs only its non-zero pages. Clones share one state:
//! the restore consumes one handle and the test keeps another to ask what was
//! published.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use lsr_ltx::{DbChecksum, DbPages, PageChecksum, PageNo, PageRef, PageSize};

use super::{DatabaseSink, PublishFailure, PublishLeft, SinkOp};

/// A published database: sparse, zero pages implicit.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Database {
    page_size: PageSize,
    page_count: u32,
    pages: BTreeMap<u32, Vec<u8>>,
}

impl Database {
    /// The page size.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// The length in pages.
    #[must_use]
    pub const fn page_count(&self) -> u32 {
        self.page_count
    }

    /// One page, zeros included; None outside 1 to the page count.
    #[must_use]
    pub fn page(&self, pgno: u32) -> Option<Vec<u8>> {
        if pgno == 0 || pgno > self.page_count {
            return None;
        }
        Some(
            self.pages
                .get(&pgno)
                .cloned()
                .unwrap_or_else(|| vec![0; self.page_size.bytes()]),
        )
    }
}

#[derive(Debug, Default)]
struct State {
    page_size: Option<PageSize>,
    page_count: u32,
    pages: BTreeMap<u32, Vec<u8>>,
    calls: BTreeMap<SinkOp, usize>,
    faults: Vec<(SinkOp, usize)>,
    publish_fault: Option<PublishLeft>,
    published: Option<Database>,
    /// One page of zeros to compare against: a std memcmp is far faster than
    /// a byte loop, which matters for a million-page database.
    zeros: Vec<u8>,
}

impl State {
    /// Counts the call and fails it when a fault is armed for this index.
    fn enter(&mut self, op: SinkOp) -> io::Result<()> {
        let nth = self.calls.entry(op).or_default();
        let this = *nth;
        *nth = nth.saturating_add(1);
        if self.faults.contains(&(op, this)) {
            return Err(io::Error::other("injected sink fault"));
        }
        Ok(())
    }

    fn set_page_size(&mut self, size: PageSize) -> io::Result<()> {
        match self.page_size {
            Some(have) if have != size => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "page size changed under the sink",
            )),
            Some(_) | None => {
                self.page_size = Some(size);
                Ok(())
            }
        }
    }
}

/// An in-memory [DatabaseSink].
#[derive(Clone, Debug, Default)]
pub struct MemSink {
    state: Arc<Mutex<State>>,
}

impl MemSink {
    /// An empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing here panics while holding the lock.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes the nth call (0-based) of [op] fail, counted over the sink's life.
    pub fn fail_at(&self, op: SinkOp, nth: usize) {
        self.lock().faults.push((op, nth));
    }

    /// Makes the publish fail, leaving `left` at the target: with
    /// [PublishLeft::VisibleNotDurable] the database is published all the same.
    pub fn fail_publish(&self, left: PublishLeft) {
        self.lock().publish_fault = Some(left);
    }

    /// How many times [op] has been called.
    #[must_use]
    pub fn calls(&self, op: SinkOp) -> usize {
        self.lock().calls.get(&op).copied().unwrap_or(0)
    }

    /// The database, once published; never before.
    #[must_use]
    pub fn published(&self) -> Option<Database> {
        self.lock().published.clone()
    }
}

impl DatabaseSink for MemSink {
    fn write_page(&mut self, page: PageRef<'_>) -> io::Result<()> {
        let (pgno, data) = (page.pgno(), page.data());
        let mut state = self.lock();
        state.enter(SinkOp::WritePage)?;
        state.set_page_size(page.size())?;
        state.page_count = state.page_count.max(pgno.get());
        if state.zeros.len() != data.len() {
            state.zeros = vec![0; data.len()];
        }
        if data == state.zeros.as_slice() {
            state.pages.remove(&pgno.get());
        } else {
            state.pages.insert(pgno.get(), data.to_vec());
        }
        Ok(())
    }

    fn truncate(&mut self, pages: DbPages, page_size: PageSize) -> io::Result<()> {
        let pages = pages.get();
        let mut state = self.lock();
        state.enter(SinkOp::Truncate)?;
        state.set_page_size(page_size)?;
        drop(state.pages.split_off(&pages.saturating_add(1)));
        state.page_count = pages;
        Ok(())
    }

    fn reset(&mut self) -> io::Result<()> {
        let mut state = self.lock();
        state.enter(SinkOp::Reset)?;
        state.page_size = None;
        state.page_count = 0;
        state.pages.clear();
        Ok(())
    }

    fn checksum(&mut self, pages: DbPages, page_size: PageSize) -> io::Result<DbChecksum> {
        let mut state = self.lock();
        state.enter(SinkOp::Checksum)?;
        let zero_page = vec![0; page_size.bytes()];
        let mut total = DbChecksum::EMPTY;
        for n in 1..=pages.get() {
            // The lock page is the one number PageNo refuses.
            let Some(pgno) = PageNo::new(n, page_size) else {
                continue;
            };
            let data = state.pages.get(&n).unwrap_or(&zero_page);
            total = total.toggle(PageChecksum::of(pgno, data));
        }
        Ok(total)
    }

    type PublishError = MemPublishError;

    fn publish(self) -> Result<(), MemPublishError> {
        let mut state = self.lock();
        let fault = state.publish_fault;
        if fault == Some(PublishLeft::Nothing) {
            return Err(MemPublishError(PublishLeft::Nothing));
        }
        let page_size = state
            .page_size
            .ok_or(MemPublishError(PublishLeft::Nothing))?;
        state.published = Some(Database {
            page_size,
            page_count: state.page_count,
            pages: state.pages.clone(),
        });
        fault.map_or(Ok(()), |left| Err(MemPublishError(left)))
    }
}

/// [MemSink]'s publish failure: the one [MemSink::fail_publish] asked for, or
/// a publish with nothing written.
#[derive(Debug, thiserror::Error)]
#[error("publish failed: {0}")]
pub struct MemPublishError(PublishLeft);

impl PublishFailure for MemPublishError {
    fn left(&self) -> PublishLeft {
        self.0
    }
}

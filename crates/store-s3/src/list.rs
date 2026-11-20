//! Listing a level: pages, then one HEAD per file for its stored stamp.
//!
//! The cursor is sent as `start-after`, later pages as `continuation-token`.
//! Pages are gathered until `limit` files have a stamp and one more key proves
//! the listing goes on; the paging and foreign-name rules then belong to
//! [`build_listing`](lsr_core::build_listing), which this module feeds and does not
//! second-guess. Every fault is an error: a page that does not parse, a
//! truncated page without a token, keys out of order or outside the prefix, a
//! repeated token, or a stamp that is not a stamp. Never a shorter listing.
//!
//! The extra key that proves a next page is not stamped, so a page costs at
//! most `limit` HEADs. Its placeholder stamp never leaves this module:
//! `build_listing` cuts it off.

use std::collections::VecDeque;
use std::io;

use lsr_core::{
    FileInfo, Level, ListedStamp, Listing, PageLimit, StampOrigin, StoreError, StoreOp,
    StoredStamp, build_listing, file_name,
};
use lsr_ltx::{Timestamp, TxidRange};

use crate::response::{Head, ListPage, ListedFile, parse_head, parse_list};
use crate::store::{Inner, ListStats};
use crate::transport::Transport;

const OP: StoreOp = StoreOp::List;

fn invalid_data() -> StoreError {
    StoreError::Transient {
        op: OP,
        kind: io::ErrorKind::InvalidData,
    }
}

/// The stamp a HEAD settles: the object's metadata when it has one, else the
/// bucket's `LastModified` marked as the fallback. The size must be the one the
/// caller saw, or the object was overwritten since and a retry reads the new one.
pub(crate) fn stamp_from_head(
    op: StoreOp,
    head: &Head,
    size: u64,
    last_modified: Option<Timestamp>,
) -> Result<StoredStamp, StoreError> {
    if head.size != size {
        return Err(StoreError::Transient {
            op,
            kind: io::ErrorKind::InvalidData,
        });
    }
    match (head.stamp, last_modified) {
        (Some(stamp), _) => Ok(StoredStamp::metadata(stamp)),
        (None, Some(modified)) => Ok(StoredStamp::fallback(modified)),
        (None, None) => Err(StoreError::Transient {
            op,
            kind: io::ErrorKind::InvalidData,
        }),
    }
}

/// The lowest and highest name a page holds, files and foreign names together.
fn name_bounds(page: &ListPage) -> Option<(String, String)> {
    let names: Vec<String> = page
        .files
        .iter()
        .map(|file| file_name(file.key.range()))
        .chain(page.foreign.iter().cloned())
        .collect();
    let low = names.iter().min()?.clone();
    let high = names.iter().max()?.clone();
    Some((low, high))
}

/// One call's state.
struct Run<'a, T> {
    inner: &'a Inner<T>,
    level: Level,
    cursor: Option<TxidRange>,
    page_size: PageLimit,
    start: Timestamp,
    pending: VecDeque<ListedFile>,
    foreign: Vec<String>,
    token: Option<String>,
    first_page_done: bool,
    exhausted: bool,
    last_name: Option<String>,
    stats: ListStats,
}

impl<T: Transport> Run<'_, T> {
    fn fetch_page(&mut self) -> Result<(), StoreError> {
        self.inner.within_budget(OP, self.start)?;
        let (level, size) = (self.level, self.page_size);
        let request = match (&self.token, self.first_page_done) {
            (Some(token), true) => self
                .inner
                .request(OP, |b, now| b.list_continue(now, level, token, size))?,
            _ => self
                .inner
                .request(OP, |b, now| b.list_page(now, level, self.cursor, size))?,
        };
        let response = self.inner.exec(OP, None, request)?;
        let page = parse_list(&response, self.inner.prefix(), level)
            .map_err(|err| err.into_store(OP, None))?;
        if let Some((low, high)) = name_bounds(&page) {
            if self.last_name.as_ref().is_some_and(|last| *last >= low) {
                return Err(invalid_data());
            }
            self.last_name = Some(high);
        }
        if page.next_token.is_some() && page.next_token == self.token {
            return Err(invalid_data());
        }
        self.first_page_done = true;
        self.exhausted = page.next_token.is_none();
        self.token = page.next_token;
        self.pending.extend(page.files);
        self.foreign.extend(page.foreign);
        Ok(())
    }

    fn next_listed(&mut self) -> Result<Option<ListedFile>, StoreError> {
        loop {
            if let Some(file) = self.pending.pop_front() {
                return Ok(Some(file));
            }
            if self.exhausted {
                return Ok(None);
            }
            self.fetch_page()?;
        }
    }

    /// The file with its stored stamp, or `None` when it vanished.
    fn stamp(&mut self, file: ListedFile) -> Result<Option<FileInfo>, StoreError> {
        let key = file.key;
        self.inner.within_budget(OP, self.start)?;
        let request = self.inner.request(OP, |b, now| b.head(now, &key))?;
        let response = match self.inner.exec(OP, Some(&key), request) {
            Ok(response) => response,
            Err(StoreError::NotFound { .. }) => {
                self.stats.vanished = self.stats.vanished.saturating_add(1);
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        let head = parse_head(&response).map_err(|err| err.into_store(OP, Some(key)))?;
        let stamp = stamp_from_head(OP, &head, file.size, file.last_modified)?;
        match stamp.origin() {
            StampOrigin::Fallback => {
                self.stats.fell_back = self.stats.fell_back.saturating_add(1);
            }
            StampOrigin::Metadata | StampOrigin::Mtime => {
                self.stats.stamped = self.stats.stamped.saturating_add(1);
            }
        }
        Ok(Some(FileInfo {
            key,
            size: file.size,
            listed: ListedStamp::Stored(stamp),
        }))
    }

    /// `want` stamped files, plus an unstamped one after them when the listing goes on.
    fn collect(&mut self, want: usize) -> Result<Vec<FileInfo>, StoreError> {
        let mut files = Vec::new();
        while files.len() < want {
            let Some(listed) = self.next_listed()? else {
                return Ok(files);
            };
            files.extend(self.stamp(listed)?);
        }
        if let Some(extra) = self.next_listed()? {
            files.push(FileInfo {
                key: extra.key,
                size: extra.size,
                listed: ListedStamp::LastModified(
                    extra
                        .last_modified
                        .unwrap_or(Timestamp::from_unix_millis(0)),
                ),
            });
        }
        Ok(files)
    }
}

impl<T: Transport> Inner<T> {
    pub(crate) fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let want = limit.get();
        // One key more than the page proves a next page; a bigger LIST only costs bytes.
        let page_size = u16::try_from(want.saturating_add(1))
            .ok()
            .and_then(PageLimit::new)
            .unwrap_or(PageLimit::MAX);
        let mut run = Run {
            inner: self,
            level,
            cursor: after,
            page_size,
            start: self.now(),
            pending: VecDeque::new(),
            foreign: Vec::new(),
            token: None,
            first_page_done: false,
            exhausted: false,
            last_name: None,
            stats: ListStats::default(),
        };
        let collected = run.collect(want);
        self.record(run.stats);
        Ok(build_listing(collected?, &run.foreign, after, limit))
    }
}

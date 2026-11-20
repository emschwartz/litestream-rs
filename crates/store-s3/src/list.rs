//! Listing a level: LIST pages only, never a HEAD (ruling G1).
//!
//! The cursor is sent as `start-after`, later pages as `continuation-token`.
//! Pages are gathered until `limit` files and one more key, which proves the
//! listing goes on, are in hand; the paging and foreign-name rules then belong
//! to [`build_listing`](lsr_core::build_listing), which this module feeds and
//! does not second-guess. Every fault is an error: a page that does not parse,
//! a truncated page without a token, keys out of order or outside the prefix,
//! a repeated token, or an entry with no readable `LastModified`. Never a
//! shorter listing.
//!
//! A listed file carries its `LastModified`, which is not a stamp. The stored
//! stamp comes from [`Inner::stamp`](crate::store), one HEAD for the file a
//! reader's decision turns on, and the HEAD's rules (size check, fallback,
//! `BadStamp`) live there with it.

use std::collections::VecDeque;
use std::io;

use lsr_core::{
    FileInfo, Level, ListedStamp, Listing, PageLimit, StoreError, StoreOp, build_listing, file_name,
};
use lsr_ltx::{Timestamp, TxidRange};

use crate::response::{ListPage, ListedFile, parse_list};
use crate::store::Inner;
use crate::transport::Transport;

const OP: StoreOp = StoreOp::List;

fn invalid_data() -> StoreError {
    StoreError::Transient {
        op: OP,
        kind: io::ErrorKind::InvalidData,
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

    /// `want` files, plus one after them when the listing goes on.
    fn collect(&mut self, want: usize) -> Result<Vec<FileInfo>, StoreError> {
        let mut files = Vec::new();
        while files.len() <= want {
            let Some(listed) = self.next_listed()? else {
                break;
            };
            files.push(FileInfo {
                key: listed.key,
                size: listed.size,
                listed: ListedStamp::LastModified(listed.last_modified),
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
        };
        let collected = run.collect(want)?;
        Ok(build_listing(collected, &run.foreign, after, limit))
    }
}

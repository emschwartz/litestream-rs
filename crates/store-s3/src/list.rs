//! Listing a level: LIST pages only, never a HEAD (ruling G1). Several levels
//! can be listed at once ([`Inner::list_many`]): each round sends the next
//! page of every listing still going as one batch.
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
    FileInfo, Level, ListAt, ListedStamp, Listing, PageLimit, StoreError, StoreOp, build_listing,
    file_name,
};
use lsr_ltx::{Timestamp, TxidRange};

use crate::classify::classify;
use crate::request::HttpRequest;
use crate::response::{ListPage, ListedFile, parse_list};
use crate::store::Inner;
use crate::transport::{HttpResponse, Transport};

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

/// One listing's state.
struct Run<'a, T> {
    inner: &'a Inner<T>,
    level: Level,
    cursor: Option<TxidRange>,
    want: usize,
    page_size: PageLimit,
    pending: VecDeque<ListedFile>,
    foreign: Vec<String>,
    token: Option<String>,
    first_page_done: bool,
    exhausted: bool,
    last_name: Option<String>,
}

impl<'a, T: Transport> Run<'a, T> {
    fn new(inner: &'a Inner<T>, level: Level, after: Option<TxidRange>, want: usize) -> Self {
        // One key more than the page proves a next page; a bigger LIST only costs bytes.
        let page_size = u16::try_from(want.saturating_add(1))
            .ok()
            .and_then(PageLimit::new)
            .unwrap_or(PageLimit::MAX);
        Self {
            inner,
            level,
            cursor: after,
            want,
            page_size,
            pending: VecDeque::new(),
            foreign: Vec::new(),
            token: None,
            first_page_done: false,
            exhausted: false,
            last_name: None,
        }
    }

    /// Whether another page is needed: fewer than `want` files and the one
    /// after them are in hand, and the listing goes on.
    fn needs_page(&self) -> bool {
        !self.exhausted && self.pending.len() <= self.want
    }

    fn page_request(&self) -> Result<HttpRequest, StoreError> {
        let (level, size) = (self.level, self.page_size);
        match (&self.token, self.first_page_done) {
            (Some(token), true) => self
                .inner
                .request(OP, |b, now| b.list_continue(now, level, token, size)),
            _ => self
                .inner
                .request(OP, |b, now| b.list_page(now, level, self.cursor, size)),
        }
    }

    fn accept(&mut self, response: &HttpResponse) -> Result<(), StoreError> {
        classify(OP, None, None, response)?;
        let page = parse_list(response, self.inner.prefix(), self.level)
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

    /// The page: `want` files, plus one after them when the listing goes on.
    fn finish(self, after: Option<TxidRange>, limit: PageLimit) -> Listing {
        let files = self
            .pending
            .into_iter()
            .take(self.want.saturating_add(1))
            .map(|listed| FileInfo {
                key: listed.key,
                size: listed.size,
                listed: ListedStamp::LastModified(listed.last_modified),
            })
            .collect();
        build_listing(files, &self.foreign, after, limit)
    }
}

impl<T: Transport> Inner<T> {
    pub(crate) fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let start = self.now();
        let mut run = Run::new(self, level, after, limit.get());
        while run.needs_page() {
            self.within_budget(OP, start)?;
            let request = run.page_request()?;
            let response = self.send(OP, request)?;
            run.accept(&response)?;
        }
        Ok(run.finish(after, limit))
    }

    /// Several listings at once: each round sends the next page of every
    /// listing that needs one as one batch, and each page is judged exactly
    /// as [`Self::list`] judges it. A failed listing stops; the rest go on.
    pub(crate) fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        let start = self.now();
        let mut runs: Vec<Result<Run<'_, T>, StoreError>> = pages
            .iter()
            .map(|at| Ok(Run::new(self, at.level, at.after, limit.get())))
            .collect();
        while self.list_round(&mut runs, start) {}
        runs.into_iter()
            .zip(pages)
            .map(|(run, at)| run.map(|run| run.finish(at.after, limit)))
            .collect()
    }

    /// Sends the next page of every listing that needs one as one batch and
    /// files each answer; returns whether any page was asked for.
    fn list_round(&self, runs: &mut [Result<Run<'_, T>, StoreError>], start: Timestamp) -> bool {
        let active: Vec<usize> = (0..runs.len())
            .filter(|&at| matches!(runs.get(at), Some(Ok(run)) if run.needs_page()))
            .collect();
        if active.is_empty() {
            return false;
        }
        let budget = self.within_budget(OP, start);
        let (mut requests, mut sent_for, mut failed) = (Vec::new(), Vec::new(), Vec::new());
        for at in active {
            let built = budget.clone().and_then(|()| match runs.get(at) {
                Some(Ok(run)) => run.page_request(),
                Some(Err(_)) | None => Err(invalid_data()),
            });
            match built {
                Ok(request) => {
                    requests.push(request);
                    sent_for.push(at);
                }
                Err(err) => failed.push((at, err)),
            }
        }
        let answers = self.send_many(OP, requests);
        for (at, answer) in sent_for.into_iter().zip(answers) {
            let Some(slot) = runs.get_mut(at) else {
                continue;
            };
            let outcome = match (answer, slot.as_mut()) {
                (Ok(response), Ok(run)) => run.accept(&response),
                (Err(err), _) => Err(err),
                (Ok(_), Err(err)) => Err(err.clone()),
            };
            if let Err(err) = outcome {
                *slot = Err(err);
            }
        }
        for (at, err) in failed {
            if let Some(slot) = runs.get_mut(at) {
                *slot = Err(err);
            }
        }
        true
    }
}

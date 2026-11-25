//! Listing a level: LIST pages only, never a HEAD (a HEAD per file made a listing cost one request per file). Several levels
//! can be listed at once ([`Inner::list_many`]): each round sends the next
//! page of every listing still going as one batch.
//!
//! The cursor is sent as `start-after`, later pages as `continuation-token`.
//! Pages are gathered until `limit` files and one more key, which proves the
//! listing goes on, are in hand; the paging rules then belong to
//! [`build_listing`](lsr_core::build_listing), which this module feeds.
//!
//! Order. A page's keys may come in any order: Cloudflare R2 lists a name after the longer
//! names that extend it (`X.ltx.tmp`, `X.ltx`, `X`), and Go does not look at order at all. An
//! LTX file name has a fixed width and never extends another, so files list ascending on every
//! provider; the store sorts each page's files anyway, and requires each page's files to lie
//! above every file of the pages before it. A stray name is counted on the page whose last
//! file the provider listed it before, by where it was listed, so the next listing (which
//! starts after that file, in the provider's own order) finds the ones left. Every fault is
//! still an error: a page that does not parse, a truncated page without a token, keys outside
//! the prefix, a key listed twice, a file at or before one already passed, a repeated token,
//! or an entry with no readable `LastModified`. Never a shorter listing.
//!
//! A listed file carries its `LastModified`, which is not a created-at time. The stored
//! created-at time comes from [`Inner::created_at`](crate::store), one HEAD for the file a
//! reader's decision turns on, and the HEAD's rules (size check, fallback,
//! `BadCreatedAt`) live there with it.

use std::collections::{BTreeMap, VecDeque};
use std::io;

use lsr_core::{
    BatchKind, FileInfo, FileKey, Level, ListAt, ListedTime, Listing, PageLimit, StoreError,
    StoreOp, build_listing,
};
use lsr_ltx::{Timestamp, TxidRange};

use crate::classify::classify;
use crate::request::HttpRequest;
use crate::response::{ListedFile, parse_list};
use crate::store::Inner;
use crate::transport::{HttpResponse, Transport};

const OP: StoreOp = StoreOp::List;

fn invalid_data() -> StoreError {
    StoreError::Transient {
        op: OP,
        kind: io::ErrorKind::InvalidData,
    }
}

/// One listing's state.
struct Run<'a, T> {
    inner: &'a Inner<T>,
    level: Level,
    cursor: Option<TxidRange>,
    want: usize,
    page_size: PageLimit,
    pending: VecDeque<ListedFile>,
    /// Each unrecognized name with how many files the provider listed before it, over the
    /// whole listing. A name seen twice is refused, so each counts once.
    unrecognized: BTreeMap<String, usize>,
    files_seen: usize,
    last_file: Option<FileKey>,
    token: Option<String>,
    first_page_done: bool,
    exhausted: bool,
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
            unrecognized: BTreeMap::new(),
            files_seen: 0,
            last_file: None,
            token: None,
            first_page_done: false,
            exhausted: false,
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
        // The files of one level have fixed-width names, so they list ascending on every
        // provider and a page that starts at or before a file already passed has gone back (or
        // repeated): the pages cannot be trusted whole. Other names may come in any order.
        let first = page.files.first().map(|file| file.key);
        if first
            .zip(self.last_file)
            .is_some_and(|(first, last)| first <= last)
        {
            return Err(invalid_data());
        }
        if page.next_token.is_some() && page.next_token == self.token {
            return Err(invalid_data());
        }
        self.first_page_done = true;
        self.exhausted = page.next_token.is_none();
        self.token = page.next_token;
        for stray in page.unrecognized {
            let at = self.files_seen.saturating_add(stray.files_before);
            if self.unrecognized.insert(stray.name, at).is_some() {
                return Err(invalid_data());
            }
        }
        self.files_seen = self.files_seen.saturating_add(page.files.len());
        self.last_file = page.files.last().map(|file| file.key).or(self.last_file);
        self.pending.extend(page.files);
        Ok(())
    }

    /// The page: `want` files, plus one after them when the listing goes on.
    fn finish(self, after: Option<TxidRange>, limit: PageLimit) -> Listing {
        let files: Vec<FileInfo> = self
            .pending
            .into_iter()
            .take(self.want.saturating_add(1))
            .map(|listed| FileInfo {
                key: listed.key,
                size: listed.size,
                listed: ListedTime::LastModified(listed.last_modified),
            })
            .collect();
        // A name belongs to this page when the provider listed it before the page's last file
        // (or the listing ended); one listed after it is found by the listing that starts after
        // that file. Counting by where the provider listed a name, never by comparing bytes, is
        // what keeps a stray that R2 lists late from falling between two pages.
        let more = files.len() > self.want;
        let skipped = self
            .unrecognized
            .values()
            .filter(|&&listed_after| !more || listed_after < self.want)
            .count();
        Listing {
            skipped,
            ..build_listing(files, &[], after, limit)
        }
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
        let answers = self.send_many(OP, Self::width(BatchKind::Lists), requests);
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

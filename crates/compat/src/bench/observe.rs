//! Watching a bucket for level-0 files, as fast as the server answers.
//!
//! The observer lists level 0 after the newest file it has seen, back to back with a short
//! pause, stamps a new file with the time its listing returned, reads the file, verifies it
//! and takes the marker off its marker page ([`super::app`]). A file is complete the moment
//! it is listed (a put is atomic), so the stamp is the first moment the commit is durable
//! to within one listing round trip, which [`Observer::resolution`] reports.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use lsr_core::conformance::block_on;
use lsr_core::{Level, ObjectStore, PageLimit, ReadLen, StoreError};
use lsr_ltx::{Event, ReadDecoder, TxidRange};

use super::clock::{now, pause};
use crate::error::CompatError;
use crate::s3::S3Replica;

/// The pause between two listings: well under a listing's round trip.
const BETWEEN_LISTS: Duration = Duration::from_millis(1);

/// A level-0 file first seen, and the last commit it holds.
#[derive(Debug, Clone, Copy)]
pub(super) struct Seen {
    /// When the listing that showed the file returned.
    pub at: Instant,
    /// The marker on the file's marker page.
    pub marker: u64,
}

/// The watcher of one replica.
#[derive(Debug)]
pub(super) struct Observer<'a> {
    replica: &'a S3Replica,
    marker_page: u32,
    after: Option<TxidRange>,
    rtts: Vec<Duration>,
    newest: u64,
}

impl<'a> Observer<'a> {
    pub(super) const fn new(replica: &'a S3Replica, marker_page: u32) -> Self {
        Self {
            replica,
            marker_page,
            after: None,
            rtts: Vec::new(),
            newest: 0,
        }
    }

    /// The median round trip of a level-0 listing: how finely this observer sees the bucket.
    pub(super) fn resolution(&self) -> Duration {
        let mut rtts = self.rtts.clone();
        rtts.sort();
        rtts.get(rtts.len() / 2).copied().unwrap_or_default()
    }

    /// One listing, and every new file read. New markers go to `out`.
    pub(super) fn poll(&mut self, out: &mut Vec<Seen>) -> Result<(), CompatError> {
        let store = self.replica.store();
        let before = now();
        let listing = block_on(store.list(Level::ZERO, self.after, PageLimit::MAX))
            .map_err(|e| CompatError::Fixture(format!("listing level 0 on MinIO: {e}")))?;
        let at = now();
        self.rtts.push(at.duration_since(before));
        for file in listing.files {
            let marker = self.marker_of(&file)?;
            self.after = Some(file.key.range());
            if let Some(marker) = marker {
                self.newest = self.newest.max(marker);
                out.push(Seen { at, marker });
            }
        }
        pause(BETWEEN_LISTS);
        Ok(())
    }

    fn marker_of(&self, file: &lsr_core::FileInfo) -> Result<Option<u64>, CompatError> {
        let mut bytes = Vec::new();
        while u64::try_from(bytes.len()).unwrap_or(u64::MAX) < file.size {
            let offset = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let left = file.size.saturating_sub(offset);
            let len = u32::try_from(left.min(u64::from(ReadLen::MAX_RAW)))
                .ok()
                .and_then(ReadLen::new)
                .ok_or_else(|| CompatError::Fixture("a level-0 file of no length".to_owned()))?;
            match block_on(self.replica.store().read(&file.key, offset, len)) {
                Ok(chunk) if chunk.is_empty() => {
                    return Err(CompatError::Fixture(
                        "a level-0 file ended early".to_owned(),
                    ));
                }
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(StoreError::NotFound { .. }) => return Ok(None),
                Err(e) => {
                    return Err(CompatError::Fixture(format!(
                        "reading level 0 on MinIO: {e}"
                    )));
                }
            }
        }
        let bad = |e: lsr_ltx::DecodeError| CompatError::Fixture(format!("a level-0 file: {e}"));
        let mut decoder = ReadDecoder::new(bytes.as_slice());
        let mut marker = None;
        while let Some(event) = decoder.next_event().map_err(bad)? {
            if let Event::Page(page) = event
                && page.pgno().get() == self.marker_page
            {
                marker = Some(parse_marker(page.data())?);
            }
        }
        decoder.finish().map_err(bad)?;
        Ok(marker)
    }

    /// Polls until a marker of at least `target` has been seen; every new marker goes to
    /// `out`. Gives up at `deadline` or when `cancel` is set (the writer failed).
    pub(super) fn until(
        &mut self,
        what: &str,
        target: u64,
        deadline: Instant,
        cancel: &AtomicBool,
        out: &mut Vec<Seen>,
    ) -> Result<(), CompatError> {
        while self.newest < target {
            if cancel.load(Ordering::Relaxed) {
                return Err(CompatError::Fixture(format!(
                    "cancelled while waiting for {what}"
                )));
            }
            if now() >= deadline {
                return Err(CompatError::Timeout(what.to_owned()));
            }
            self.poll(out)?;
        }
        Ok(())
    }

    /// The newest marker any level-0 file holds, from the whole listing: a replica of thousands
    /// of files is walked a page at a time and only the last few files are read (a file written
    /// after the final commit, by a checkpoint, holds no marker page).
    pub(super) fn tail_marker(&self) -> Result<Option<u64>, CompatError> {
        let store = self.replica.store();
        let mut after = None;
        let mut tail: Vec<lsr_core::FileInfo> = Vec::new();
        loop {
            let listing = block_on(store.list(Level::ZERO, after, PageLimit::MAX))
                .map_err(|e| CompatError::Fixture(format!("listing level 0 on MinIO: {e}")))?;
            tail.extend(listing.files);
            let keep = tail.len().saturating_sub(TAIL_FILES);
            tail.drain(..keep);
            match listing.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        for file in tail.iter().rev() {
            if let Some(marker) = self.marker_of(file)? {
                return Ok(Some(marker));
            }
        }
        Ok(None)
    }
}

const TAIL_FILES: usize = 16;

fn parse_marker(page: &[u8]) -> Result<u64, CompatError> {
    page.get(page.len().saturating_sub(8)..)
        .and_then(|tail| std::str::from_utf8(tail).ok())
        .filter(|text| text.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| {
            CompatError::Fixture("the marker page does not end in the marker digits".to_owned())
        })
}

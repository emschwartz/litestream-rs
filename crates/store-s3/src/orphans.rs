//! The bounded list of multipart uploads that were started and never finished.
//!
//! Dropping an upload does no I/O (the store contract), so a dropped or failed
//! multipart upload's id lands here and is aborted later: at the start of the
//! next `begin_upload`, or by `S3Store::abort_orphans`; a killed process's are found at the next
//! boot by listing the bucket's open uploads (`remove_scratch`). The list holds
//! at most 64 ids; a 65th pushes the oldest off, and the bucket's
//! `AbortIncompleteMultipartUpload` rule is the backstop for what falls off
//! (drop does no I/O, so a failed upload's id waits here). An id whose abort fails goes back, so an outage costs one
//! request per attempt and never grows the list.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

use lsr_core::{FileKey, StoreError, StoreOp};

use crate::classify::{S3Code, classify};
use crate::request::UploadId;
use crate::response::parse_uploads;
use crate::store::Inner;
use crate::transport::Transport;

/// The most ids kept; the oldest is forgotten past this.
pub(crate) const MAX_ORPHANS: usize = 64;

/// The most ListMultipartUploads pages (1000 uploads each) one boot's sweep reads.
pub(crate) const SWEEP_PAGES: usize = 4;

/// Uploads to abort, oldest first.
#[derive(Default)]
pub(crate) struct Orphans(Mutex<VecDeque<(FileKey, UploadId)>>);

impl Orphans {
    fn list(&self) -> MutexGuard<'_, VecDeque<(FileKey, UploadId)>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn push(&self, key: FileKey, id: UploadId) {
        let mut list = self.list();
        list.push_back((key, id));
        while list.len() > MAX_ORPHANS {
            list.pop_front();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.list().len()
    }

    pub(crate) fn take(&self) -> VecDeque<(FileKey, UploadId)> {
        std::mem::take(&mut *self.list())
    }

    /// Puts ids not yet aborted back ahead of any that arrived meanwhile.
    fn restore(&self, rest: VecDeque<(FileKey, UploadId)>) {
        let mut list = self.list();
        for item in rest.into_iter().rev() {
            list.push_front(item);
        }
        while list.len() > MAX_ORPHANS {
            list.pop_front();
        }
    }
}

impl<T: Transport> Inner<T> {
    /// One AbortMultipartUpload. An upload that is already gone is aborted.
    pub(crate) fn abort_upload(&self, key: &FileKey, id: &UploadId) -> Result<(), StoreError> {
        let op = StoreOp::Upload;
        let request = self.request(op, |b, now| b.abort_multipart(now, key, id))?;
        let response = self.send(op, request)?;
        if response.status() == 404
            && S3Code::from_body(response.body()) == Some(S3Code::NoSuchUpload)
        {
            return Ok(());
        }
        classify(op, Some(key), None, &response)
    }

    /// Aborts the multipart uploads of this store's own keys that a killed process left open,
    /// and the ones this process dropped; how many. The ids a process keeps die with it, so
    /// at boot the bucket's own listing is the only record: every upload open under the prefix
    /// whose key is one of this store's object keys is ours. Only the replicating engine
    /// uploads there, and a second engine on one replica is already a stop condition; if one
    /// runs anyway, its upload's Complete finds no upload and fails, and is retried. At most
    /// [`SWEEP_PAGES`] listing pages per call, so a boot's work is bounded; the rest go at
    /// the next. The caller holds no upload of this store: the sweep waits for every part
    /// this process started to end before it lists, and would wait for ever on a part whose
    /// upload is alive and never collected.
    pub(crate) fn remove_scratch(&self) -> Result<u64, StoreError> {
        let op = StoreOp::Upload;
        // An engine rebuilt over this store may have dropped an upload a moment ago: its
        // parts end first, so no upload listed below has a part of this process on the wire.
        self.parts_ended();
        let start = self.now();
        let dropped = self.abort_orphans()?;
        let mut removed = u64::try_from(dropped).unwrap_or(u64::MAX);
        let mut markers: Option<(String, String)> = None;
        for _ in 0..SWEEP_PAGES {
            self.within_budget(op, start)?;
            let at = markers
                .as_ref()
                .map(|(key, id)| (key.as_str(), id.as_str()));
            let request = self.request(op, |b, now| b.list_uploads(now, at))?;
            let response = self.send(op, request)?;
            classify(op, None, None, &response)?;
            let page =
                parse_uploads(&response, self.prefix()).map_err(|err| err.into_store(op, None))?;
            for (key, id) in &page.ours {
                self.within_budget(op, start)?;
                self.abort_upload(key, id)?;
                removed = removed.saturating_add(1);
            }
            match page.next {
                Some(next) if Some(&next) != markers.as_ref() => markers = Some(next),
                Some(_) | None => break,
            }
        }
        Ok(removed)
    }

    /// Aborts every listed upload, oldest first, and returns how many are settled.
    /// The first failure stops the pass and keeps that id and the rest listed.
    pub(crate) fn abort_orphans(&self) -> Result<usize, StoreError> {
        let start = self.now();
        let mut queue = self.orphans.take();
        let mut settled = 0_usize;
        while let Some((key, id)) = queue.pop_front() {
            let outcome = self
                .within_budget(StoreOp::Upload, start)
                .and_then(|()| self.abort_upload(&key, &id));
            if let Err(err) = outcome {
                queue.push_front((key, id));
                self.orphans.restore(queue);
                return Err(err);
            }
            settled = settled.saturating_add(1);
        }
        Ok(settled)
    }
}
#[cfg(test)]
mod tests {
    use lsr_core::Level;
    use lsr_ltx::{Txid, TxidRange};

    use super::*;

    fn key(n: u64) -> FileKey {
        let id = Txid::new(n).unwrap();
        FileKey::new(Level::ZERO, TxidRange::new(id, id).unwrap())
    }

    #[test]
    fn the_list_keeps_the_newest_64() {
        let orphans = Orphans::default();
        for n in 1..=65 {
            orphans.push(key(n), UploadId::new(format!("id{n}")));
        }
        assert_eq!(orphans.len(), MAX_ORPHANS);
        let kept = orphans.take();
        assert_eq!(kept.front().map(|(k, _)| *k), Some(key(2)));
        assert_eq!(kept.back().map(|(k, _)| *k), Some(key(65)));
    }

    #[test]
    fn restored_ids_go_ahead_of_newer_ones_and_stay_bounded() {
        let orphans = Orphans::default();
        orphans.push(key(1), UploadId::new("a".to_owned()));
        let taken = orphans.take();
        orphans.push(key(2), UploadId::new("b".to_owned()));
        orphans.restore(taken);
        let order: Vec<FileKey> = orphans.take().iter().map(|(k, _)| *k).collect();
        assert_eq!(order, [key(1), key(2)]);
    }
}

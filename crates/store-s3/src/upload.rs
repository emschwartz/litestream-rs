//! `S3Upload`: one object being written, as a single PUT or a multipart upload.
//!
//! Writes accumulate in one buffer of at most one part (the memory bound). An
//! object that ends below the part size is sent as one PUT, as Go does (rc.go:814).
//! The write that fills the buffer starts a multipart upload (the created-at time rides on
//! CreateMultipartUpload), and each full buffer becomes the next part. Full
//! parts wait until the `concurrency` setting's worth of them is ready (the buffer
//! filling counts as one), then go out as one batch, so memory is at most
//! that many parts. With a concurrency of one each part is sent by the write
//! that filled it, as before. A failed part fails the upload (at the write or
//! commit that sent its batch) and the caller restarts it; nothing is retried
//! here.
//!
//! A single PUT is judged by the status, and then by the `ETag`: for a PUT
//! without server-side encryption the ETag is the object's MD5, so a different
//! one means the bytes stored are not the bytes sent. The object is already
//! visible then, so it is deleted (best effort) before the error returns, as the
//! file store does for a size mismatch; the uploader writes the same key again
//! without listing. A completed multipart object whose ETag names another
//! part count is treated the same way; a wrong part ETag leaves nothing visible, so the upload
//! id goes on the orphan list instead.
//!
//! A create-only upload ([`WriteMode::New`]) sends `If-None-Match: *` on its
//! PUT, or on its complete (`multipart.rs`). A 412 is
//! [`StoreError::AlreadyExists`]. A provider that answers the condition with
//! `NotImplemented` has no conditional writes: the PUT fails `Transient` (its
//! body is not kept, so the caller writes again), and from then on this store
//! sends create-only writes without the condition, as Go's are, and counts
//! them in [`crate::WriteStats`]. A provider that ignores the header
//! silently cannot be told apart here.
//!
//! **Written from a source.** An upload whose first write is
//! [`Upload::write_from`] keeps no part buffer: of each part it keeps the MD5 and a tally
//! of every 256 KiB (`again.rs`), and when `concurrency` parts are ready it sends them with
//! bodies read again through the caller's [`Reread`], which gives a second read's bytes
//! only when they tally as the first did. An object below one part is sent the same way by
//! [`Upload::commit_from`]. So the upload of a staged file holds `concurrency` reads of
//! 256 KiB while a batch is out and a few kilobytes between batches, whatever the part size
//! (`docs/decisions/D222.md`). The requests, their order and what each answer must prove
//! are the same. A second read that gives nothing fails the upload with the source's own
//! error before anything is published. Such an upload is written and committed through the
//! source only: a plain `write` or `commit` of it is refused, since the store no longer
//! holds what it would send.
//!
//! Dropping a multipart upload does no I/O: parts not yet sent are dropped
//! and its id goes on the orphan list (`orphans.rs`). A batch has finished
//! (every request answered or timed out) before its call returns, so no part
//! is ever in flight when an id is orphaned or aborted.

use std::io;
use std::sync::Arc;

use lsr_core::{
    CorruptCause, CreatedAt, FileInfo, FileKey, ListedTime, PartNumber, Reread, StoreError,
    StoreOp, Upload, WriteMode,
};
use lsr_ltx::Timestamp;

use crate::again::{Run, Sealed};
use crate::classify::{classify, is_not_implemented};
use crate::multipart::{Sent, completed_etag_fits};
use crate::part_plan::{MAX_OBJECT, plan_part_size};
use crate::request::{Precondition, UploadId};
use crate::response::{EtagCheck, check_etag};
use crate::store::Inner;
use crate::transport::{HttpResponse, StreamedRequest, Transport};

/// Where an upload is. `Done` is after a commit, an abort or a failure: the
/// upload id, if there was one, is already with the caller or on the orphan list.
enum UploadState {
    Buffering(Vec<u8>),
    Multipart {
        id: UploadId,
        /// The ETag of part n is the (n - 1)th entry.
        etags: Vec<String>,
        buf: Vec<u8>,
        /// Full parts not yet sent; they follow `etags` in order.
        ready: Vec<Vec<u8>>,
    },
    /// Written from a source: tallies in place of bytes (the module docs).
    Sourced {
        /// The multipart upload and its parts' ETags so far, once the first part was full.
        multipart: Option<(UploadId, Vec<String>)>,
        /// The part being handed over, boxed so an upload that holds bytes is no larger for
        /// it.
        run: Box<Run>,
        /// Full parts not yet sent; they follow the ETags in order.
        ready: Vec<Sealed>,
    },
    Done,
}

/// One object being written. Dropping it does no I/O.
pub struct S3Upload<T> {
    inner: Arc<Inner<T>>,
    key: FileKey,
    created_at: Timestamp,
    mode: WriteMode,
    /// The part size this upload cuts its object into, planned from the object's
    /// length when it was known at the start ([`plan_part_size`]).
    part: usize,
    written: u64,
    state: UploadState,
}

impl<T> std::fmt::Debug for S3Upload<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (multipart, buffered) = match &self.state {
            UploadState::Buffering(buf) => (false, buf.len()),
            UploadState::Multipart { buf, .. } => (true, buf.len()),
            UploadState::Sourced { multipart, .. } => (multipart.is_some(), 0),
            UploadState::Done => (false, 0),
        };
        f.debug_struct("S3Upload")
            .field("key", &self.key)
            .field("multipart", &multipart)
            .field("buffered", &buffered)
            .finish_non_exhaustive()
    }
}

impl<T> S3Upload<T> {
    /// An upload of `key`; `len` is its exact length when the caller knows it.
    ///
    /// # Errors
    /// [`StoreError::ObjectTooLarge`] when no part size can carry `len` bytes,
    /// before any request is sent.
    pub(crate) fn new(
        inner: Arc<Inner<T>>,
        key: FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self, StoreError> {
        let planned = match len {
            None => inner.part_size,
            Some(len) => {
                plan_part_size(inner.part_size, len).ok_or(StoreError::ObjectTooLarge {
                    key,
                    len,
                    max: MAX_OBJECT,
                })?
            }
        };
        Ok(Self {
            inner,
            key,
            created_at,
            mode,
            part: usize::try_from(planned.get()).unwrap_or(usize::MAX),
            written: 0,
            state: UploadState::Buffering(Vec::new()),
        })
    }

    /// The capacity of every buffer held (parts waiting and the one
    /// filling), so a test can prove the memory bound.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn buffer_capacity(&self) -> usize {
        match &self.state {
            UploadState::Buffering(buf) => buf.capacity(),
            UploadState::Multipart { buf, ready, .. } => ready
                .iter()
                .map(Vec::capacity)
                .fold(buf.capacity(), usize::saturating_add),
            UploadState::Sourced { run, ready, .. } => ready
                .iter()
                .map(Sealed::held)
                .fold(run.held(), usize::saturating_add),
            UploadState::Done => 0,
        }
    }

    /// Moves a live multipart id to the orphan list and ends the upload.
    fn orphan(&mut self) {
        match std::mem::replace(&mut self.state, UploadState::Done) {
            UploadState::Multipart { id, .. }
            | UploadState::Sourced {
                multipart: Some((id, _)),
                ..
            } => self.inner.orphans.push(self.key, id),
            UploadState::Buffering(_)
            | UploadState::Sourced {
                multipart: None, ..
            }
            | UploadState::Done => {}
        }
    }

    /// Whether this upload keeps tallies in place of bytes.
    const fn sourced(&self) -> bool {
        matches!(self.state, UploadState::Sourced { .. })
    }

    /// Ends an upload written one way and then used the other: the store does not hold
    /// what the call would send.
    fn mixed(&mut self) -> StoreError {
        self.orphan();
        ended()
    }
}

impl<T> Drop for S3Upload<T> {
    fn drop(&mut self) {
        self.orphan();
    }
}

/// An upload used after it failed or finished: no HTTP exchange happened.
fn ended() -> StoreError {
    StoreError::Refused {
        op: StoreOp::Upload,
        status: 0,
    }
}

/// Buffer size past which an upload reserves a whole part.
const GROW_TO_PART: usize = 256 * 1024;

/// Appends `bytes` (which fit under `part`) growing the buffer geometrically
/// but never past one part, so a part-sized buffer is the most memory held.
fn append_bounded(buf: &mut Vec<u8>, bytes: &[u8], part: usize) {
    let need = buf.len().saturating_add(bytes.len());
    if need > buf.capacity() {
        let grown = need.max(buf.capacity().saturating_mul(2)).max(4096);
        // Past 256 KiB the object is on its way to being a part: take the whole part at once
        // rather than reallocating (and copying) a part-sized buffer a dozen times.
        let target = if grown > GROW_TO_PART { part } else { grown }.min(part);
        buf.reserve_exact(target.saturating_sub(buf.len()));
    }
    buf.extend_from_slice(bytes);
}

impl<T: Transport> Inner<T> {
    /// One PUT of `body` under `key`; see the module docs for the `ETag` rule.
    pub(crate) fn put(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        body: Vec<u8>,
        mode: WriteMode,
    ) -> Result<FileInfo, StoreError> {
        let op = StoreOp::Upload;
        let wrote = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let precondition = self.precondition(mode);
        let request = self.request(op, |b, now| {
            b.put_object_if(now, key, body, created_at, precondition)
        })?;
        let sent = request.md5();
        let response = self.send(op, request.into_request())?;
        self.judge_put(key, created_at, (wrote, sent), precondition, &response)
    }

    /// [`Self::put`] of an object the upload let go of: the same request with the length and
    /// MD5 taken when the bytes were first handed over, its body read again through `again`
    /// while it is sent. A second read that gives nothing is the result, with the source's
    /// own error: the body ended short, so the store holds no new object.
    pub(crate) fn put_from(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        whole: &Sealed,
        mode: WriteMode,
        again: &dyn Reread,
    ) -> Result<FileInfo, StoreError> {
        let op = StoreOp::Upload;
        let sized = (whole.len(), whole.md5());
        let precondition = self.precondition(mode);
        let request = self.request(op, |b, now| {
            b.put_object_streamed_if(now, key, sized, created_at, precondition)
        })?;
        let mut body = whole.body(again);
        let streamed = StreamedRequest::new(request.into_request(), whole.len(), &mut body);
        let sent = self
            .send_streamed(op, std::num::NonZeroUsize::MIN, vec![streamed])
            .pop();
        if let Some(unread) = body.failed() {
            return Err(unread);
        }
        let response = sent.unwrap_or(Err(StoreError::Transient {
            op,
            kind: io::ErrorKind::Other,
        }))?;
        self.judge_put(key, created_at, sized, precondition, &response)
    }

    /// What a PUT's answer must prove, for a body of `wrote` bytes whose MD5 is `sent`.
    fn judge_put(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        (wrote, sent): (u64, [u8; 16]),
        precondition: Precondition,
        response: &HttpResponse,
    ) -> Result<FileInfo, StoreError> {
        let op = StoreOp::Upload;
        if precondition == Precondition::KeyIsFree && is_not_implemented(response) {
            // Nothing published and nothing checked: the caller decides whether to replace
            // (D79). The store never drops the condition itself.
            return Err(StoreError::CreateOnlyUnsupported { op, key: *key });
        }
        if response.status() == 412 {
            self.tally_taken();
        }
        classify(op, Some(key), None, response)?;
        match check_etag(response, sent) {
            EtagCheck::Matches | EtagCheck::NotMd5 => Ok(FileInfo {
                key: *key,
                size: wrote,
                listed: ListedTime::Created(CreatedAt::metadata(created_at)),
            }),
            EtagCheck::Missing => Err(StoreError::Transient {
                op,
                kind: io::ErrorKind::InvalidData,
            }),
            EtagCheck::Mismatch => Err(self.delete_bad_object(key, None)),
        }
    }

    /// Deletes an object the store acknowledged wrongly, best effort, and
    /// returns the error to report. Whether the delete lands changes nothing
    /// the caller can do now.
    fn delete_bad_object(&self, key: &FileKey, part: Option<PartNumber>) -> StoreError {
        match self.delete(&[*key]) {
            Ok(()) | Err(_) => {}
        }
        StoreError::Corrupt {
            key: Some(*key),
            cause: CorruptCause::ChecksumMismatch { part },
        }
    }
}

impl<T: Transport> S3Upload<T> {
    /// Fills the buffer from `chunk`, uploading each part as it fills.
    fn append(&mut self, mut chunk: &[u8]) -> Result<(), StoreError> {
        let start = self.inner.now();
        let part = self.part;
        self.written = self
            .written
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        while !chunk.is_empty() {
            let (UploadState::Buffering(buf) | UploadState::Multipart { buf, .. }) =
                &mut self.state
            else {
                return Err(ended());
            };
            let (head, rest) = chunk.split_at(part.saturating_sub(buf.len()).min(chunk.len()));
            append_bounded(buf, head, part);
            chunk = rest;
            if buf.len() >= part {
                self.start_multipart()?;
                self.queue_part(start)?;
            }
        }
        Ok(())
    }

    /// The first full buffer turns a buffered upload into a multipart one.
    fn start_multipart(&mut self) -> Result<(), StoreError> {
        if !matches!(self.state, UploadState::Buffering(_)) {
            return Ok(());
        }
        let UploadState::Buffering(buf) = std::mem::replace(&mut self.state, UploadState::Done)
        else {
            return Err(ended());
        };
        let id = self.inner.create_upload(&self.key, self.created_at)?;
        self.state = UploadState::Multipart {
            id,
            etags: Vec::new(),
            buf,
            ready: Vec::new(),
        };
        Ok(())
    }

    /// Moves the full buffer to the parts waiting, and sends them once the
    /// store's concurrency of them is ready.
    fn queue_part(&mut self, start: Timestamp) -> Result<(), StoreError> {
        let width = self.inner.concurrency.get();
        let UploadState::Multipart { buf, ready, .. } = &mut self.state else {
            return Ok(());
        };
        ready.push(std::mem::take(buf));
        if ready.len() >= width {
            return self.send_parts(start);
        }
        Ok(())
    }

    /// Uploads every waiting part as one batch. Any failure ends the upload
    /// and hands its id to the orphan list.
    fn send_parts(&mut self, start: Timestamp) -> Result<(), StoreError> {
        let UploadState::Multipart {
            id, etags, ready, ..
        } = &mut self.state
        else {
            return Ok(());
        };
        if ready.is_empty() {
            return Ok(());
        }
        let bodies = std::mem::take(ready);
        let first = etags.len().saturating_add(1);
        let outcome = self
            .inner
            .within_budget(StoreOp::Upload, start)
            .and_then(|()| self.inner.put_parts(&self.key, id, first, bodies));
        match outcome {
            Ok(sent) => {
                etags.extend(sent);
                Ok(())
            }
            Err(err) => {
                self.orphan();
                Err(err)
            }
        }
    }

    /// The last part (when anything is left), then CompleteMultipartUpload and
    /// the ETag check on the finished object.
    fn finish_multipart(&mut self) -> Result<FileInfo, StoreError> {
        let start = self.inner.now();
        if let UploadState::Multipart { buf, ready, .. } = &mut self.state
            && !buf.is_empty()
        {
            ready.push(std::mem::take(buf));
        }
        self.send_parts(start)?;
        self.complete(start)
    }

    /// CompleteMultipartUpload for the parts sent, and the ETag check on the finished
    /// object.
    fn complete(&mut self, start: Timestamp) -> Result<FileInfo, StoreError> {
        let (UploadState::Multipart { id, etags, .. }
        | UploadState::Sourced {
            multipart: Some((id, etags)),
            ..
        }) = &self.state
        else {
            return Err(ended());
        };
        let parts = etags.len();
        let sent = Sent {
            written: self.written,
            created_at: self.created_at,
            parts,
        };
        let completed = self
            .inner
            .within_budget(StoreOp::Upload, start)
            .and_then(|()| {
                self.inner
                    .complete_upload(&self.key, id, etags, self.mode, sent)
            });
        let etag = match completed {
            Ok(etag) => etag,
            Err(err) => {
                self.orphan();
                return Err(err);
            }
        };
        // Completed: the id is spent, so nothing is left to orphan.
        self.state = UploadState::Done;
        if !completed_etag_fits(&etag, parts) {
            return Err(self.inner.delete_bad_object(&self.key, None));
        }
        Ok(FileInfo {
            key: self.key,
            size: self.written,
            listed: ListedTime::Created(CreatedAt::metadata(self.created_at)),
        })
    }

    /// The part size as an object offset.
    fn part_len(&self) -> u64 {
        u64::try_from(self.part).unwrap_or(u64::MAX)
    }

    /// Takes `chunk` into the part being handed over, keeping its digest and tallies and
    /// none of its bytes, and sends the waiting parts once the store's concurrency of them
    /// is ready.
    fn append_from(&mut self, mut chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        let start = self.inner.now();
        let part = self.part_len();
        let width = self.inner.concurrency.get();
        self.written = self
            .written
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        while !chunk.is_empty() {
            let UploadState::Sourced {
                multipart,
                run,
                ready,
            } = &mut self.state
            else {
                return Err(ended());
            };
            let room = usize::try_from(part.saturating_sub(run.len())).unwrap_or(usize::MAX);
            let (head, rest) = chunk.split_at(room.min(chunk.len()));
            run.add(head);
            chunk = rest;
            if run.len() < part {
                continue;
            }
            if multipart.is_none() {
                match self.inner.create_upload(&self.key, self.created_at) {
                    Ok(id) => *multipart = Some((id, Vec::new())),
                    Err(err) => {
                        self.state = UploadState::Done;
                        return Err(err);
                    }
                }
            }
            let next = Run::at(run.end(), part);
            ready.push(std::mem::replace(&mut **run, next).seal());
            if ready.len() >= width {
                self.send_parts_from(start, again)?;
            }
        }
        Ok(())
    }

    /// Uploads every waiting part as one batch, each body read again through `again`. Any
    /// failure ends the upload and hands its id to the orphan list.
    fn send_parts_from(&mut self, start: Timestamp, again: &dyn Reread) -> Result<(), StoreError> {
        let UploadState::Sourced {
            multipart: Some((id, etags)),
            ready,
            ..
        } = &mut self.state
        else {
            return Ok(());
        };
        if ready.is_empty() {
            return Ok(());
        }
        let parts = std::mem::take(ready);
        let first = etags.len().saturating_add(1);
        let outcome = self
            .inner
            .within_budget(StoreOp::Upload, start)
            .and_then(|()| {
                self.inner
                    .put_parts_from(&self.key, id, first, &parts, again)
            });
        match outcome {
            Ok(sent) => {
                etags.extend(sent);
                Ok(())
            }
            Err(err) => {
                self.orphan();
                Err(err)
            }
        }
    }

    /// Commits an upload written from a source: one PUT when it never filled a part, else
    /// the parts still waiting, the last one, and CompleteMultipartUpload.
    fn finish_from(&mut self, again: &dyn Reread) -> Result<FileInfo, StoreError> {
        let start = self.inner.now();
        match &mut self.state {
            UploadState::Sourced {
                multipart: None,
                run,
                ..
            } => {
                let whole = std::mem::replace(&mut **run, Run::at(0, 0)).seal();
                self.state = UploadState::Done;
                return self
                    .inner
                    .put_from(&self.key, self.created_at, &whole, self.mode, again);
            }
            UploadState::Sourced {
                multipart: Some(_),
                run,
                ready,
            } => {
                if run.len() > 0 {
                    let next = Run::at(run.end(), 0);
                    ready.push(std::mem::replace(&mut **run, next).seal());
                }
            }
            UploadState::Buffering(_) | UploadState::Multipart { .. } | UploadState::Done => {
                return Err(ended());
            }
        }
        self.send_parts_from(start, again)?;
        self.complete(start)
    }
}

impl<T: Transport> Upload for S3Upload<T> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        if self.sourced() {
            return Err(self.mixed());
        }
        self.append(chunk)
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        // The first write decides: an upload that already holds bytes goes on holding them.
        if self.written == 0 && matches!(&self.state, UploadState::Buffering(buf) if buf.is_empty())
        {
            self.state = UploadState::Sourced {
                multipart: None,
                run: Box::new(Run::at(0, self.part_len())),
                ready: Vec::new(),
            };
        }
        if self.sourced() {
            self.append_from(chunk, again)
        } else {
            self.append(chunk)
        }
    }

    async fn commit(mut self) -> Result<FileInfo, StoreError> {
        if self.sourced() {
            return Err(self.mixed());
        }
        if let UploadState::Buffering(buf) = &mut self.state {
            let body = std::mem::take(buf);
            self.state = UploadState::Done;
            return self.inner.put(&self.key, self.created_at, body, self.mode);
        }
        self.finish_multipart()
    }

    async fn commit_from(mut self, again: &dyn Reread) -> Result<FileInfo, StoreError> {
        if self.sourced() {
            self.finish_from(again)
        } else {
            self.commit().await
        }
    }

    async fn abort(mut self) -> Result<(), StoreError> {
        match std::mem::replace(&mut self.state, UploadState::Done) {
            UploadState::Multipart { id, .. }
            | UploadState::Sourced {
                multipart: Some((id, _)),
                ..
            } => {
                let aborted = self.inner.abort_upload(&self.key, &id);
                if aborted.is_err() {
                    self.inner.orphans.push(self.key, id);
                }
                aborted
            }
            UploadState::Buffering(_)
            | UploadState::Sourced {
                multipart: None, ..
            }
            | UploadState::Done => Ok(()),
        }
    }
}

//! `S3Upload`: one object being written, as a single PUT or a multipart upload.
//!
//! Writes fill one part at a time. An object that ends below the part size is sent as one
//! PUT, as Go does (rc.go:814). The write that fills the first part starts a multipart upload
//! (the created-at time rides on CreateMultipartUpload).
//!
//! **Parts in flight.** A part is sent the moment it is whole: the upload hands it to the
//! store's lanes (`lanes.rs`) and goes on filling the next one, as Go's uploader feeds its
//! workers. The thread that sends a part computes its SHA-256. The upload and the parts it
//! started share one small state under one lock: each part started leaves exactly one answer
//! there, whether its request was answered, failed, was never sent or its work panicked, and
//! a part that ends wakes whoever waits (a thread on the condition variable, or the task whose
//! waker [`Upload::flight`] left). Answers are judged on the upload's own thread when it
//! collects them, and each ETag is put by its part number, so the order parts end in is no
//! part of the result.
//!
//! Two bounds hold, whatever the lanes do:
//! - **Count.** At most `width` parts of one upload are started and not yet collected: the
//!   `concurrency` setting, or fewer for parts that hold their bytes (below).
//! - **One part waits.** A whole part that finds every lane taken waits in the upload, one at
//!   most. A write that would make a second waits for a part to end, on this thread, helping
//!   the lanes when they have no thread of their own. A buffered write that fills a part with
//!   every lane taken waits too, since the rest of it would go into a new buffer, so
//!   [`Upload::flight`] answers [`Flight::Full`] once the part being filled has less room
//!   than a write of [`ReadLen::CHUNK`]: a caller that asks first never waits.
//!
//! **Parts that hold their bytes.** A part written with [`Upload::write`] is a buffer, so
//! the upload holds `width` parts in flight and one more, filling or waiting: no byte goes
//! into a new buffer while a whole part still waits. `width` is cut so that this fits a fixed
//! budget (`part_plan.rs`, `docs/decisions/D232.md`).
//!
//! **Parts written from a source.** An upload whose first write is [`Upload::write_from`]
//! keeps no part buffer: of each part it keeps what was seen of every 256 KiB and a reader of
//! the caller's source (`again.rs`), and the SHA-256 of what it was handed. The thread that sends
//! the part reads it again as the body, using a cell's bytes only when they are what was first
//! seen; a part of one cell or less keeps its bytes and reads nothing again
//! (`docs/decisions/D270.md`). So a part in flight holds one read of 256 KiB, the HTTP client's buffers (768 KiB a
//! request by the gate in `tests/store-s3/memory.rs`) and one open file, whatever the part
//! size. The engine's staged send asks [`Upload::flight`] before each read and stops when the
//! answer is [`Flight::Full`], so its tick never waits for a part
//! (`docs/decisions/D233.md`). Such an upload is written through the source only: a plain
//! `write` of it is refused, since the store no longer holds what it would send.
//!
//! **Failure.** The first part that fails is the upload's failure, returned by the call that
//! collects it; nothing is retried here. Parts not yet on the wire are then not sent, and the
//! caller restarts the upload. A failed upload sends no AbortMultipartUpload itself: its id
//! goes on the orphan list (`orphans.rs`).
//!
//! **Leaving.** `commit` and `abort` return only when every part started has ended, so
//! neither CompleteMultipartUpload nor AbortMultipartUpload is ever sent beside a part of the
//! same upload. Dropping an upload does no I/O and waits for nothing: parts not yet on the
//! wire are not sent, and the id goes on the orphan list when the last part started lets go
//! of the shared state, so an id is never aborted from that list with a part of it still on
//! the wire. The boot sweep waits for the same thing store-wide (`orphans.rs`).
//!
//! A single PUT is judged by its status. Its body's SHA-256 is signed, so the store refuses a
//! body that differs from it and publishes nothing; what the `ETag` says is not compared, as
//! in Go. A completed multipart object whose ETag names another part count is visible and
//! wrong, so it is deleted (best effort) before the error returns, as the file store does for
//! a size mismatch; the uploader writes the same key again without listing.
//!
//! A create-only upload ([`WriteMode::New`]) sends `If-None-Match: *` on its
//! PUT, or on its complete (`multipart.rs`). A 412 is
//! [`StoreError::AlreadyExists`]. A provider that answers the condition with
//! `NotImplemented` has no conditional writes: the write fails with
//! [`StoreError::CreateOnlyUnsupported`] and publishes nothing, and the caller
//! decides whether to write the key as a replace. The store never drops the
//! condition itself (`docs/decisions/D079.md`). A provider that ignores the
//! header silently cannot be told apart here.

use std::future::poll_fn;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, Waker};

use lsr_core::{
    CorruptCause, CreatedAt, FileInfo, FileKey, Flight, ListedTime, PartNumber, ReadLen, Reread,
    Rest, StoreError, StoreOp, Upload, WriteMode,
};
use lsr_ltx::Timestamp;

use crate::again::{Run, Sealed};
use crate::classify::{classify, transient_other};
use crate::lanes::{Condvar, Mutex, MutexGuard, lock};
use crate::multipart::{PAYLOAD_TOO_LARGE, Sent, completed_etag_fits, judge_part};
use crate::part_plan::{MAX_OBJECT, held_width, plan_part_size};
use crate::request::{HttpRequest, Precondition, RequestBuilder, RequestError, UploadId};
use crate::sigv4::PayloadHash;
use crate::store::Inner;
use crate::transport::{HttpResponse, StreamedRequest, Transport};

/// A part's bytes: a buffer, or what is needed to read them again.
pub(crate) enum Bytes {
    Held(Vec<u8>),
    Again(Box<Sealed>),
}

impl Bytes {
    fn held(&self) -> usize {
        match self {
            Self::Held(buf) => buf.capacity(),
            Self::Again(part) => part.held(),
        }
    }

    /// Sends the write `build` makes for these bytes, with them as its body, and returns the
    /// answer and the length the request declared. A buffer's SHA-256 is taken as the request
    /// is built, after the clock and the credentials are read, and the buffer is moved into it.
    /// A part written from a source carries the SHA-256 of what it was handed and streams its
    /// body from the bytes it kept or from a second read (`again.rs`); a read that gave nothing
    /// is the result, whatever the request came to. Nothing is sent once `stop` is set.
    fn send_with<T: Transport>(
        &mut self,
        inner: &Inner<T>,
        stop: &AtomicBool,
        build: impl FnOnce(
            &RequestBuilder,
            Timestamp,
            (u64, PayloadHash),
        ) -> Result<HttpRequest, RequestError>,
    ) -> Result<(HttpResponse, u64), StoreError> {
        let op = StoreOp::Upload;
        if stop.load(Ordering::Relaxed) {
            return Err(StoreError::Transient {
                op,
                kind: io::ErrorKind::Interrupted,
            });
        }
        match self {
            Self::Held(buf) => {
                let body = std::mem::take(buf);
                let len = u64::try_from(body.len()).unwrap_or(u64::MAX);
                let mut request =
                    inner.request(op, |b, now| build(b, now, (len, PayloadHash::of(&body))))?;
                request.set_body(body);
                Ok((inner.send(op, request)?, len))
            }
            Self::Again(part) => {
                let sized = (part.len(), part.sha256());
                let request = inner.request(op, |b, now| build(b, now, sized))?;
                let mut body = part.body(stop);
                let streamed = StreamedRequest::new(request, sized.0, &mut body);
                let sent = inner.send_streamed(op, streamed);
                if let Some(unread) = body.failed() {
                    return Err(unread);
                }
                Ok((sent?, sized.0))
            }
        }
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "one per upload, moved only when a part is sealed; the size is the checksum state \
              of the cell being filled"
)]
enum Filling {
    Held(Vec<u8>),
    /// `None` until the part's first byte arrives: its reader is asked for then.
    Again(Option<Run>),
}

impl Filling {
    fn len(&self) -> u64 {
        match self {
            Self::Held(buf) => u64::try_from(buf.len()).unwrap_or(u64::MAX),
            Self::Again(run) => run.as_ref().map_or(0, Run::len),
        }
    }

    const fn sourced(&self) -> bool {
        matches!(self, Self::Again(_))
    }

    /// The part as it stands, taken out: the filling is left empty and of the same kind, so an
    /// upload written from a source stays one. `None` for a source not yet read from.
    fn seal(&mut self) -> Option<Bytes> {
        match self {
            Self::Held(buf) => Some(Bytes::Held(std::mem::take(buf))),
            Self::Again(run) => run.take().map(|run| Bytes::Again(Box::new(run.seal()))),
        }
    }

    #[cfg(feature = "testing")]
    fn held(&self) -> usize {
        match self {
            Self::Held(buf) => buf.capacity(),
            Self::Again(run) => run.as_ref().map_or(0, Run::held),
        }
    }
}

/// How one part started ended: its answer, or why there is none.
pub(crate) struct Done {
    number: PartNumber,
    /// The memory the part held, let go of before this was left.
    held: usize,
    sent: Result<HttpResponse, StoreError>,
}

/// The answers not yet collected, and whom a part that ends wakes.
#[derive(Default)]
struct Ends {
    done: Vec<Done>,
    wake: Option<Waker>,
}

/// What a multipart upload and the parts it started share.
pub(crate) struct Shared<T> {
    pub(crate) inner: Arc<Inner<T>>,
    pub(crate) key: FileKey,
    pub(crate) id: UploadId,
    ends: Mutex<Ends>,
    ended: Condvar,
    /// The upload failed, was aborted or was dropped: a part not yet on the wire is not sent.
    pub(crate) stop: AtomicBool,
    /// CompleteMultipartUpload or AbortMultipartUpload succeeded: the id is spent.
    spent: AtomicBool,
}

impl<T> Shared<T> {
    fn ends(&self) -> MutexGuard<'_, Ends> {
        lock(&self.ends)
    }
}

/// The last holder lets go, the upload or the last part it started: an id neither completed
/// nor aborted goes on the orphan list now, when no part of it can still be on the wire.
impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        if !*self.spent.get_mut() {
            self.inner.orphans.push(self.key, self.id.clone());
        }
    }
}

/// One part started: leaves exactly one [`Done`] when it is dropped, however that comes
/// about (the part was sent, was not, its work panicked, or it was never run).
struct Report<T> {
    shared: Arc<Shared<T>>,
    number: PartNumber,
    held: usize,
    bytes: Option<Bytes>,
    sent: Option<Result<HttpResponse, StoreError>>,
    /// Declared after `shared`, and fields drop in order: when this part holds the upload's
    /// last reference, its id is on the orphan list before the store hears the part ended,
    /// so what waits for the parts to end (the boot sweep, `orphans.rs`) finds it there.
    _started: Started<T>,
}

/// One part counted started on its store, until this is dropped.
struct Started<T>(Arc<Inner<T>>);

impl<T> Started<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        inner.part_started();
        Self(inner)
    }
}

impl<T> Drop for Started<T> {
    fn drop(&mut self) {
        self.0.part_ended();
    }
}

impl<T> Drop for Report<T> {
    fn drop(&mut self) {
        // The part's buffer, or its open file, goes before anyone hears the part ended.
        drop(self.bytes.take());
        let sent = self
            .sent
            .take()
            .unwrap_or(Err(transient_other(StoreOp::Upload)));
        let wake = {
            let mut ends = self.shared.ends();
            ends.done.push(Done {
                number: self.number,
                held: self.held,
                sent,
            });
            ends.wake.clone()
        };
        self.shared.ended.notify_all();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl<T: Transport> Report<T> {
    /// The part's work; what it came to is left when this is dropped.
    fn run(mut self) {
        if let Some(mut bytes) = self.bytes.take() {
            let (key, id) = (&self.shared.key, &self.shared.id);
            let number = self.number;
            self.sent = Some(
                bytes
                    .send_with(&self.shared.inner, &self.shared.stop, |b, now, sized| {
                        b.upload_part_of(now, key, id, number, sized)
                    })
                    .map(|(response, _)| response),
            );
        }
    }
}

/// A multipart upload under way.
struct Multi<T> {
    shared: Arc<Shared<T>>,
    /// The ETag of part n is the (n - 1)th entry, once its answer was collected.
    etags: Vec<Option<String>>,
    ready: Option<Bytes>,
    /// Parts started whose answers are not collected, and the memory they hold.
    flying: usize,
    flying_held: usize,
    /// The object's last part was sealed: nothing more may be written.
    tail: bool,
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
    /// Where the part being filled starts in the object.
    sealed: u64,
    filling: Filling,
    /// The multipart upload, once the first part was whole.
    multi: Option<Multi<T>>,
    /// After a commit, an abort or a failure.
    over: bool,
}

impl<T> std::fmt::Debug for S3Upload<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Upload")
            .field("key", &self.key)
            .field("multipart", &self.multi.is_some())
            .field("filling", &self.filling.len())
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
            sealed: 0,
            filling: Filling::Held(Vec::new()),
            multi: None,
            over: false,
        })
    }

    /// The memory every part of this upload holds: the one filling, the one waiting and the
    /// ones in flight, so a test can prove the bound.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn buffer_capacity(&self) -> usize {
        let multi = self.multi.as_ref().map_or(0, |multi| {
            multi
                .ready
                .as_ref()
                .map_or(0, Bytes::held)
                .saturating_add(multi.flying_held)
        });
        self.filling.held().saturating_add(multi)
    }

    /// How many parts are started and not yet collected.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.multi.as_ref().map_or(0, |multi| multi.flying)
    }

    fn part_len(&self) -> u64 {
        u64::try_from(self.part).unwrap_or(u64::MAX)
    }

    fn width(&self) -> usize {
        let most = self.inner.concurrency;
        if self.filling.sourced() {
            most.get()
        } else {
            held_width(most, self.part_len()).get()
        }
    }

    /// Ends the upload on a failure of its own making; parts not yet on the wire are not sent.
    fn fail(&mut self, err: StoreError) -> StoreError {
        self.over = true;
        if let Some(multi) = &self.multi {
            multi.shared.stop.store(true, Ordering::Relaxed);
        }
        err
    }
}

/// Parts not yet on the wire are not sent. The id is orphaned when the last of them lets go.
impl<T> Drop for S3Upload<T> {
    fn drop(&mut self) {
        if let Some(multi) = &self.multi {
            multi.shared.stop.store(true, Ordering::Relaxed);
        }
    }
}

/// An upload used after it failed or finished: no HTTP exchange happened.
fn ended() -> StoreError {
    StoreError::Refused {
        op: StoreOp::Upload,
        status: 0,
    }
}

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
    /// One PUT of `bytes` under `key`; see the module docs for the `ETag` rule. A part read
    /// again lives until the answer is judged, so its reader outlasts the delete of a wrong
    /// object.
    fn put(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mut bytes: Bytes,
        mode: WriteMode,
    ) -> Result<FileInfo, StoreError> {
        let precondition = self.precondition(mode);
        // A commit is never stopped: it is the caller's own call.
        let go = AtomicBool::new(false);
        let (response, wrote) = bytes.send_with(self, &go, |b, now, sized| {
            b.put_object_of(now, key, sized, created_at, precondition)
        })?;
        self.judge_put(key, created_at, wrote, precondition, &response)
    }

    /// What a PUT's answer must prove, for a body of `wrote` bytes.
    fn judge_put(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        wrote: u64,
        precondition: Precondition,
        response: &HttpResponse,
    ) -> Result<FileInfo, StoreError> {
        self.refuse_unsupported_or_tally(precondition, key, response)?;
        classify(StoreOp::Upload, Some(key), None, response)?;
        Ok(FileInfo {
            key: *key,
            size: wrote,
            listed: ListedTime::Created(CreatedAt::metadata(created_at)),
        })
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
    /// Takes the answers of the parts that ended, judging each as a part is judged
    /// (`multipart.rs`) and putting its ETag by its number. The first failure, lowest part
    /// number first among those collected together, ends the upload and is returned.
    fn collect(&mut self) -> Result<(), StoreError> {
        let Some(multi) = &mut self.multi else {
            return Ok(());
        };
        let mut done = std::mem::take(&mut multi.shared.ends().done);
        done.sort_by_key(|part| part.number.get());
        let mut failed = None;
        for part in done {
            multi.flying = multi.flying.saturating_sub(1);
            multi.flying_held = multi.flying_held.saturating_sub(part.held);
            let judged = part
                .sent
                .and_then(|response| judge_part(&self.key, part.number, &response));
            let slot = usize::from(part.number.get()).checked_sub(1);
            match (judged, slot.and_then(|at| multi.etags.get_mut(at))) {
                (Ok(etag), Some(slot)) => *slot = Some(etag),
                (Ok(_), None) => failed = failed.or_else(|| Some(ended())),
                (Err(err), _) => failed = failed.or(Some(err)),
            }
        }
        match failed {
            Some(err) => Err(self.fail(err)),
            None => Ok(()),
        }
    }

    fn start_ready(&mut self) -> Result<bool, StoreError> {
        let width = self.width();
        let Some(multi) = &mut self.multi else {
            return Ok(false);
        };
        if multi.flying >= width {
            return Ok(false);
        }
        let Some(bytes) = multi.ready.take() else {
            return Ok(false);
        };
        let next = u16::try_from(multi.etags.len().saturating_add(1)).unwrap_or(0);
        let Some(number) = PartNumber::new(next) else {
            return Err(self.fail(StoreError::Refused {
                op: StoreOp::Upload,
                status: PAYLOAD_TOO_LARGE,
            }));
        };
        multi.etags.push(None);
        multi.flying = multi.flying.saturating_add(1);
        multi.flying_held = multi.flying_held.saturating_add(bytes.held());
        let report = Report {
            _started: Started::new(Arc::clone(&multi.shared.inner)),
            shared: Arc::clone(&multi.shared),
            number,
            held: bytes.held(),
            bytes: Some(bytes),
            sent: None,
        };
        self.inner.lanes.start(Box::new(move || report.run()));
        Ok(true)
    }

    fn pump(&mut self) -> Result<(), StoreError> {
        loop {
            self.collect()?;
            if !self.start_ready()? {
                return Ok(());
            }
        }
    }

    /// Waits until a part of this upload has an answer to collect. Lanes with no thread run
    /// a part on this one; otherwise the thread sleeps until a part ends.
    fn wait_one(&self) {
        let Some(multi) = &self.multi else {
            return;
        };
        if multi.flying == 0 {
            return;
        }
        loop {
            if !multi.shared.ends().done.is_empty() {
                return;
            }
            if self.inner.lanes.help() {
                continue;
            }
            let ends = multi.shared.ends();
            if ends.done.is_empty() {
                drop(
                    multi
                        .shared
                        .ended
                        .wait(ends)
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                );
            }
        }
    }

    fn until_none_waits(&mut self) -> Result<(), StoreError> {
        loop {
            self.pump()?;
            if self
                .multi
                .as_ref()
                .is_none_or(|multi| multi.ready.is_none())
            {
                return Ok(());
            }
            self.wait_one();
        }
    }

    /// Moves the part being filled to the place of the one that waits, which must be free.
    fn seal_now(&mut self) {
        let Some(multi) = &mut self.multi else {
            return;
        };
        let len = self.filling.len();
        let Some(bytes) = self.filling.seal() else {
            return;
        };
        self.sealed = self.sealed.saturating_add(len);
        multi.ready = Some(bytes);
    }

    /// The part being filled is whole: it waits for a lane, or starts at once. The first one
    /// turns the upload into a multipart one.
    fn seal(&mut self) -> Result<(), StoreError> {
        if self.multi.is_none() {
            let id = match self.inner.create_upload(&self.key, self.created_at) {
                Ok(id) => id,
                Err(err) => return Err(self.fail(err)),
            };
            self.multi = Some(Multi {
                shared: Arc::new(Shared {
                    inner: Arc::clone(&self.inner),
                    key: self.key,
                    id,
                    ends: Mutex::new(Ends::default()),
                    ended: Condvar::new(),
                    stop: AtomicBool::new(false),
                    spent: AtomicBool::new(false),
                }),
                etags: Vec::new(),
                ready: None,
                flying: 0,
                flying_held: 0,
                tail: false,
            });
        }
        self.until_none_waits()?;
        self.seal_now();
        self.pump()
    }

    /// Takes `chunk` into the part being filled, sealing each part as it fills. Written from
    /// a source (`again`), a part keeps what was seen of the bytes and none of them.
    async fn take(
        &mut self,
        mut chunk: &[u8],
        again: Option<&dyn Reread>,
    ) -> Result<(), StoreError> {
        // Over, or its last part sealed, or written from a source and now without one: the
        // store does not hold what the write would send.
        if self.over
            || self.multi.as_ref().is_some_and(|multi| multi.tail)
            || (again.is_none() && self.filling.sourced())
        {
            return Err(self.fail(ended()));
        }
        let part = self.part_len();
        self.written = self
            .written
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        while !chunk.is_empty() {
            // No byte goes into a new buffer while a whole one still waits (the module docs).
            if !self.filling.sourced() && self.filling.len() == 0 {
                self.until_none_waits()?;
            }
            let room = usize::try_from(part.saturating_sub(self.filling.len()));
            let (head, rest) = chunk.split_at(room.unwrap_or(usize::MAX).min(chunk.len()));
            match (&mut self.filling, again) {
                (Filling::Held(buf), _) => append_bounded(buf, head, self.part),
                (Filling::Again(Some(run)), _) => run.add(head),
                (Filling::Again(run @ None), Some(again)) => match again.open().await {
                    Ok(reader) => run
                        .insert(Run::at(self.sealed, part, self.inner.cell, reader))
                        .add(head),
                    Err(err) => return Err(self.fail(err)),
                },
                (Filling::Again(None), None) => return Err(self.fail(ended())),
            }
            chunk = rest;
            if self.filling.len() >= part {
                self.seal()?;
            }
        }
        Ok(())
    }

    /// The last part (when anything is left), every part's answer, then
    /// CompleteMultipartUpload and the ETag check on the finished object.
    fn finish(&mut self) -> Result<FileInfo, StoreError> {
        let start = self.inner.now();
        if self.filling.len() > 0 {
            self.until_none_waits()?;
            self.seal_now();
        }
        loop {
            self.pump()?;
            let Some(multi) = &self.multi else {
                return Err(ended());
            };
            if multi.flying == 0 && multi.ready.is_none() {
                break;
            }
            self.wait_one();
        }
        self.complete(start)
    }

    fn complete(&mut self, start: Timestamp) -> Result<FileInfo, StoreError> {
        let Some(multi) = &self.multi else {
            return Err(ended());
        };
        let Some(etags) = multi.etags.iter().cloned().collect::<Option<Vec<String>>>() else {
            return Err(ended());
        };
        let parts = etags.len();
        let sent = Sent {
            written: self.written,
            created_at: self.created_at,
            parts,
        };
        let id = &multi.shared.id;
        let etag = self
            .inner
            .within_budget(StoreOp::Upload, start)
            .and_then(|()| {
                self.inner
                    .complete_upload(&self.key, id, &etags, self.mode, sent)
            })?;
        // Completed: the id is spent, so nothing is left to orphan.
        multi.shared.spent.store(true, Ordering::Relaxed);
        if !completed_etag_fits(&etag, parts) {
            return Err(self.inner.delete_bad_object(&self.key, None));
        }
        Ok(FileInfo {
            key: self.key,
            size: self.written,
            listed: ListedTime::Created(CreatedAt::metadata(self.created_at)),
        })
    }

    /// Stops what is not yet on the wire and waits until every part started has ended. Their
    /// answers are dropped: the upload is over.
    fn settle(&mut self) {
        loop {
            let Some(multi) = &mut self.multi else {
                return;
            };
            multi.shared.stop.store(true, Ordering::Relaxed);
            multi.ready = None;
            let ended = std::mem::take(&mut multi.shared.ends().done).len();
            multi.flying = multi.flying.saturating_sub(ended);
            if multi.flying == 0 {
                multi.flying_held = 0;
                return;
            }
            self.wait_one();
        }
    }
}

impl<T: Transport> Upload for S3Upload<T> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.take(chunk, None).await
    }

    async fn write_from(&mut self, chunk: &[u8], again: &dyn Reread) -> Result<(), StoreError> {
        // The first write decides: an upload that already holds bytes goes on holding them.
        if self.written == 0 && !self.over {
            self.filling = Filling::Again(None);
        }
        self.take(chunk, Some(again)).await
    }

    /// Never waits for a part (the module docs). The task that asks is woken when a part of
    /// this upload ends.
    async fn flight(&mut self, rest: Rest) -> Result<Flight, StoreError> {
        if self.over {
            return Err(ended());
        }
        let waker = poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
        let Some(multi) = &self.multi else {
            // Below one part: the commit sends it.
            return Ok(Flight::Idle);
        };
        multi.shared.ends().wake = Some(waker);
        self.pump()?;
        if rest == Rest::None
            && self.filling.len() > 0
            && let Some(multi) = &mut self.multi
            && multi.ready.is_none()
        {
            // The object's last part, short or whole.
            multi.tail = true;
            self.seal_now();
            self.pump()?;
        }
        // A buffered part that fills with every lane taken waits, and the rest of the write it
        // filled waits with it (`take`): with less room than one write left, that write could.
        let short = self.part_len().saturating_sub(self.filling.len()) < ReadLen::CHUNK.get();
        let (width, buffered) = (self.width(), !self.filling.sourced());
        Ok(match &self.multi {
            Some(multi) if multi.ready.is_some() => Flight::Full,
            Some(multi) if buffered && short && multi.flying >= width => Flight::Full,
            Some(multi) if multi.flying > 0 => Flight::Room,
            Some(_) | None => Flight::Idle,
        })
    }

    async fn commit(mut self) -> Result<FileInfo, StoreError> {
        if self.over {
            return Err(ended());
        }
        self.over = true;
        if self.multi.is_none() {
            // Nothing written from a source yet is an empty object.
            let whole = self.filling.seal().unwrap_or(Bytes::Held(Vec::new()));
            return self.inner.put(&self.key, self.created_at, whole, self.mode);
        }
        let finished = self.finish();
        if finished.is_err() {
            // No part of a failed upload is on the wire when the caller hears of it.
            self.settle();
        }
        finished
    }

    async fn abort(mut self) -> Result<(), StoreError> {
        let failed = std::mem::replace(&mut self.over, true);
        self.settle();
        let Some(multi) = &self.multi else {
            return Ok(());
        };
        if failed {
            // As a failed upload always was: its id waits on the orphan list.
            return Ok(());
        }
        let aborted = self.inner.abort_upload(&self.key, &multi.shared.id);
        if aborted.is_ok() {
            multi.shared.spent.store(true, Ordering::Relaxed);
        }
        aborted
    }
}

#[cfg(test)]
#[cfg(feature = "testing")]
mod part_cap_tests {
    use lsr_core::{FakeClock, Level};

    use super::*;
    use crate::config::{BucketName, Endpoint, Prefix, S3Config};
    use crate::credentials::{S3Credentials, SecretString};
    use crate::fake::FakeS3;
    use crate::store::S3Store;

    fn store() -> S3Store<FakeS3> {
        let config = S3Config::new(
            Endpoint::new("http://localhost:9000").unwrap(),
            BucketName::new("bucket").unwrap(),
            Prefix::new("db").unwrap(),
        );
        let credentials = S3Credentials::new(
            "AKIAIOSFODNN7EXAMPLE".to_owned(),
            SecretString::new("secret".to_owned()),
        );
        let at = Timestamp::from_unix_millis(1_369_353_600_000);
        let fake = FakeS3::new(&config, FakeClock::new(at));
        S3Store::with_transport(config, credentials, fake, FakeClock::new(at)).unwrap()
    }

    /// A part number past 10000 is refused before any request is sent for it; 10000 is
    /// sent.
    #[test]
    fn part_numbers_stop_at_the_cap() {
        let key = FileKey::new(
            Level::ZERO,
            lsr_ltx::TxidRange::new(
                lsr_ltx::Txid::new(1).unwrap(),
                lsr_ltx::Txid::new(1).unwrap(),
            )
            .unwrap(),
        );
        let at = Timestamp::from_unix_millis(1_369_353_600_000);
        let store = store();
        let mut upload =
            S3Upload::new(Arc::clone(&store.inner), key, at, WriteMode::New, None).unwrap();
        // Part 1, and then as if 9998 more had been sent.
        upload.seal().unwrap();
        let multi = upload.multi.as_mut().unwrap();
        assert_eq!(multi.etags.len(), 1);
        multi.etags.resize(9_999, Some("\"etag\"".to_owned()));
        multi.ready = Some(Bytes::Held(Vec::new()));
        upload.pump().unwrap();
        let multi = upload.multi.as_mut().unwrap();
        assert_eq!(multi.etags.len(), 10_000, "part 10000 is sent");
        assert!(multi.etags.last().unwrap().is_some());

        multi.ready = Some(Bytes::Held(Vec::new()));
        assert_eq!(
            upload.pump(),
            Err(StoreError::Refused {
                op: StoreOp::Upload,
                status: PAYLOAD_TOO_LARGE,
            }),
            "part 10001 is refused"
        );
        let multi = upload.multi.as_ref().unwrap();
        assert_eq!((multi.etags.len(), multi.flying), (10_000, 0));
        assert!(upload.over);
    }

    /// The last part of a dropped upload puts its id on the orphan list before the store
    /// counts the part ended, so what waits for the parts to end (the boot sweep) finds the
    /// id there. The count is held still while the part lets go on another thread: the id
    /// must reach the list first. In the other order the part waits on the count, the id
    /// never arrives, and nextest ends the test.
    #[test]
    fn a_last_part_orphans_its_id_before_the_store_counts_it_ended() {
        let store = store();
        let shared = Arc::new(Shared {
            inner: Arc::clone(&store.inner),
            key: FileKey::new(
                Level::ZERO,
                lsr_ltx::TxidRange::new(
                    lsr_ltx::Txid::new(1).unwrap(),
                    lsr_ltx::Txid::new(1).unwrap(),
                )
                .unwrap(),
            ),
            id: UploadId::new("dropped".to_owned()),
            ends: Mutex::new(Ends::default()),
            ended: Condvar::new(),
            stop: AtomicBool::new(true),
            spent: AtomicBool::new(false),
        });
        let report = Report {
            _started: Started::new(Arc::clone(&store.inner)),
            shared,
            number: PartNumber::new(1).unwrap(),
            held: 0,
            bytes: None,
            sent: None,
        };
        let count = lock(&store.inner.parts);
        assert_eq!(*count, 1);
        std::thread::scope(|scope| {
            drop(scope.spawn(move || drop(report)));
            while store.inner.orphans.len() == 0 {
                std::thread::yield_now();
            }
            drop(count);
        });
        assert_eq!(*lock(&store.inner.parts), 0);
        assert_eq!(store.pending_orphans(), 1);
    }
}

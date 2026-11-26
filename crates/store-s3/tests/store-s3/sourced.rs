//! Uploads written from a source the store reads again (`Upload::write_from`,
//! `docs/decisions/D222.md`): the store keeps no part, sends what a buffered upload sends,
//! and fails with the source's own error, before anything is published, when a later read
//! gives nothing. A part of more than one cell is read again as its body, a cell at a time,
//! through a reader of its own; a part of one cell or less is sent from the bytes it kept and
//! its source is never read again (`docs/decisions/D270.md`). What happens with several parts in flight is in `flight.rs`.

use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use lsr_core::conformance::block_on;
use lsr_core::{
    Again, BoxFuture, CorruptCause, FakeClock, FileKey, ObjectStore, Reread, Seen, StoreError,
    StoreOp, Upload, WriteMode,
};
use lsr_ltx::Timestamp;
use lsr_store_s3::{
    FakeS3, Fault, HttpRequest, HttpResponse, PartSize, PayloadHash, S3Config, S3Store, Shape,
    StreamedRequest, Transport, TransportError, object_key,
};

use crate::common::*;
use crate::rig::{FakeHarness, count, shapes};

const MIB: usize = 1024 * 1024;
pub const CELL: usize = 256 * 1024;
pub const PART: usize = 5 * MIB;

/// Parts of 5 MiB, two at a time.
pub fn small_parts() -> S3Config {
    config()
        .with_part_size(PartSize::MIN)
        .with_concurrency(NonZeroUsize::new(2).unwrap())
}

/// Bytes that never repeat, so a read from another offset is other bytes.
pub fn body(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// What [`Source`] answers when the bytes it holds are not the ones first seen.
pub const fn changed() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Read,
        kind: ErrorKind::InvalidData,
    }
}

/// A read the source's own storage failed.
pub const fn unreadable() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Read,
        kind: ErrorKind::PermissionDenied,
    }
}

/// What the store answers for bytes that are not the ones first seen, when the source
/// handed them over all the same.
pub const fn unseen() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Upload,
        kind: ErrorKind::InvalidData,
    }
}

#[derive(Default)]
struct File {
    bytes: Mutex<Vec<u8>>,
    reads: AtomicU32,
    opens: AtomicU32,
    /// The read, counted from one, that fails.
    fail: Mutex<Option<(u32, StoreError)>>,
    /// Gives whatever it holds, seen or not: a source that breaks its word.
    unchecked: bool,
}

/// The file an upload is written from: its bytes are given back only when they are what was
/// seen when they were first handed over, as the engine's source gives them. Every reader it
/// opens reads the same file.
#[derive(Clone, Default)]
pub struct Source(Arc<File>);

impl Source {
    pub fn of(bytes: &[u8]) -> Self {
        Self(Arc::new(File {
            bytes: Mutex::new(bytes.to_vec()),
            ..File::default()
        }))
    }

    /// A source that gives whatever it holds.
    fn unchecked(bytes: &[u8]) -> Self {
        Self(Arc::new(File {
            bytes: Mutex::new(bytes.to_vec()),
            unchecked: true,
            ..File::default()
        }))
    }

    pub fn reads(&self) -> u32 {
        self.0.reads.load(Ordering::Relaxed)
    }

    /// Readers opened: one for each part.
    pub fn opens(&self) -> u32 {
        self.0.opens.load(Ordering::Relaxed)
    }

    pub fn fail_read(&self, nth: u32, err: StoreError) {
        *self.0.fail.lock().unwrap() = Some((nth, err));
    }

    pub fn flip(&self, at: usize) {
        self.0.bytes.lock().unwrap()[at] ^= 0x40;
    }
}

impl Reread for Source {
    fn open(&self) -> BoxFuture<'_, Result<Box<dyn Again>, StoreError>> {
        self.0.opens.fetch_add(1, Ordering::Relaxed);
        let reader: Box<dyn Again> = Box::new(self.clone());
        Box::pin(std::future::ready(Ok(reader)))
    }
}

impl Again for Source {
    fn read<'a>(
        &'a mut self,
        offset: u64,
        was: &'a Seen,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        let file = &self.0;
        let nth = file.reads.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let failed = file.fail.lock().unwrap().take_if(|(at, _)| *at == nth);
        let len = usize::try_from(was.len()).unwrap();
        assert!(
            (1..=CELL).contains(&len),
            "a read of {len} bytes: one read's worth is asked for at a time"
        );
        let start = usize::try_from(offset).unwrap();
        let bytes = file
            .bytes
            .lock()
            .unwrap()
            .get(start..start.saturating_add(len))
            .map(<[u8]>::to_vec);
        let unchecked = file.unchecked;
        Box::pin(async move {
            if let Some((_, err)) = failed {
                return Err(err);
            }
            let bytes = bytes.ok_or_else(changed)?;
            if unchecked || Seen::of(&bytes) == *was {
                Ok(bytes)
            } else {
                Err(changed())
            }
        })
    }
}

pub fn begin<S: ObjectStore>(store: &S, key: &FileKey) -> S::Upload {
    block_on(store.begin_upload(key, Timestamp::from_unix_millis(5), WriteMode::New)).unwrap()
}

/// Writes `bytes` from `source` in writes of `chunk` bytes and commits.
fn put_from<S: ObjectStore>(
    store: &S,
    key: &FileKey,
    bytes: &[u8],
    chunk: usize,
    source: &Source,
) -> Result<(), StoreError> {
    let mut upload = begin(store, key);
    block_on(async {
        for chunk in bytes.chunks(chunk) {
            upload.write_from(chunk, source).await?;
        }
        upload.commit().await.map(drop)
    })
}

pub fn stored(harness: &FakeHarness, key: &FileKey) -> Option<Vec<u8>> {
    harness
        .wire
        .inner()
        .body_of(&object_key(config().prefix(), key))
}

/// What each request was and how many bytes it carried.
fn sent(harness: &FakeHarness) -> Vec<(Option<Shape>, usize)> {
    harness
        .wire
        .requests()
        .iter()
        .map(|request| (request.shape, request.body_len))
        .collect()
}

/// Two parts, a third that is not full and the commit, written in chunks that align with
/// neither a part nor a cell: the requests, their sizes and the object are a buffered
/// upload's, every cell of a full part was read back once as the body, the last part (of one
/// cell or less) not at all, and each part read through a reader of its own.
#[test]
fn an_upload_from_a_source_sends_what_a_buffered_upload_sends() {
    let bytes = body(2 * PART + 100_000);
    let key = key(0, 1, 1);
    for chunk in [CELL, 100_000, PART + 7, bytes.len()] {
        let plain = FakeHarness::with_config(small_parts());
        let mut upload = begin(&plain.store, &key);
        block_on(async {
            for chunk in bytes.chunks(chunk) {
                upload.write(chunk).await.unwrap();
            }
            upload.commit().await.unwrap();
        });

        let sourced = FakeHarness::with_config(small_parts());
        let source = Source::of(&bytes);
        put_from(&sourced.store, &key, &bytes, chunk, &source).unwrap();
        assert_eq!(sent(&sourced), sent(&plain), "chunks of {chunk}");
        assert!(stored(&sourced, &key).as_deref() == Some(&bytes[..]));
        let cells = u32::try_from(2 * PART.div_ceil(CELL)).unwrap();
        assert_eq!(source.reads(), cells, "chunks of {chunk}");
        assert_eq!(source.opens(), 3, "chunks of {chunk}");
    }
}

/// An object below one part goes up as one PUT at the commit, read back a cell at a time only
/// when it is longer than one cell; an empty one reads nothing.
#[test]
fn an_object_below_one_part_is_one_put_read_back_at_the_commit() {
    let key = key(0, 1, 1);
    for len in [0, 1, CELL, CELL + 1] {
        let bytes = body(len);
        let harness = FakeHarness::with_config(small_parts());
        let source = Source::of(&bytes);
        let mut upload = begin(&harness.store, &key);
        block_on(upload.write_from(&bytes, &source)).unwrap();
        assert_eq!(source.reads(), 0, "nothing is sent before the commit");
        block_on(upload.commit()).unwrap();
        assert_eq!(shapes(&harness.wire), [Some(Shape::Put)], "{len} bytes");
        assert_eq!(stored(&harness, &key), Some(bytes), "{len} bytes");
        let again = if len > CELL { len.div_ceil(CELL) } else { 0 };
        assert_eq!(source.reads(), u32::try_from(again).unwrap(), "{len} bytes");
        assert_eq!(source.opens(), u32::from(len > 0), "{len} bytes");
    }
}

/// Passes every request to the fake and keeps what each streamed one declared and carried.
struct Streams {
    fake: FakeS3,
    seen: Arc<Mutex<Vec<Streamed>>>,
}

#[derive(Debug, PartialEq, Eq)]
struct Streamed {
    shape: Option<Shape>,
    /// The `x-amz-content-sha256` header, the hash the request is signed with.
    sha256: Option<String>,
    /// The `Content-MD5` header, which no write carries.
    md5: Option<String>,
    /// The `Content-Length` header.
    length: Option<String>,
    /// The length the request says its body has.
    len: u64,
    /// The body read from it.
    body: Vec<u8>,
    /// Whether the request held any body of its own.
    own_body: bool,
}

impl Transport for Streams {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.fake.send(request)
    }

    fn send_streamed(&self, streamed: StreamedRequest<'_>) -> Result<HttpResponse, TransportError> {
        let (len, own_body) = (streamed.len(), !streamed.request().body().is_empty());
        let empty = streamed.is_empty();
        let whole = streamed.into_buffered()?;
        self.note(&whole, len, own_body, empty);
        self.fake.send(whole)
    }
}

impl Streams {
    fn note(&self, whole: &HttpRequest, len: u64, own_body: bool, empty: bool) {
        assert_eq!(empty, len == 0);
        self.seen.lock().unwrap().push(Streamed {
            shape: Shape::of(whole),
            sha256: whole.header("x-amz-content-sha256").map(str::to_owned),
            md5: whole.header("content-md5").map(str::to_owned),
            length: whole.header("content-length").map(str::to_owned),
            len,
            body: whole.body().to_vec(),
            own_body,
        });
    }
}

/// Every request whose body is read while it is sent declares the body's length and carries
/// the SHA-256 of the bytes first handed over (taken over a later read that was checked
/// against the first) as the hash it is signed with, so the store checks what it received
/// against the first read.
#[test]
fn every_streamed_request_carries_the_digest_and_length_of_the_first_read() {
    let bytes = body(2 * PART + 100_000);
    for (len, shapes) in [
        (
            bytes.len(),
            vec![Shape::UploadPart, Shape::UploadPart, Shape::UploadPart],
        ),
        (100_000, vec![Shape::Put]),
    ] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let transport = Streams {
            fake: fake(),
            seen: Arc::clone(&seen),
        };
        let store = S3Store::with_transport(
            small_parts(),
            credentials(),
            transport,
            FakeClock::new(now()),
        )
        .unwrap();
        let source = Source::of(&bytes);
        put_from(&store, &key(0, 1, 1), &bytes[..len], CELL, &source).unwrap();
        let seen = seen.lock().unwrap();
        let want: Vec<Streamed> = bytes[..len]
            .chunks(PART)
            .zip(shapes)
            .map(|(part, shape)| Streamed {
                shape: Some(shape),
                sha256: Some(PayloadHash::of(part).to_hex()),
                md5: None,
                length: Some(part.len().to_string()),
                len: u64::try_from(part.len()).unwrap(),
                body: part.to_vec(),
                own_body: false,
            })
            .collect();
        assert!(*seen == want, "{len} bytes");
    }
}

/// The file changes on disk under a part that is handed over and not yet whole: the write
/// that makes it whole fails with the source's error, the part is never sent, the upload is
/// over and nothing is published.
#[test]
fn a_second_read_that_differs_fails_the_write_that_sends_the_part() {
    let bytes = body(2 * PART);
    let harness = FakeHarness::with_config(small_parts());
    let source = Source::of(&bytes);
    let mut upload = begin(&harness.store, &key(0, 1, 1));
    let (first, last) = bytes.split_at(2 * PART - CELL);
    block_on(upload.write_from(first, &source)).unwrap();
    // The first part went when it was whole, read again as its body.
    let cells = u32::try_from(PART / CELL).unwrap();
    assert_eq!(source.reads(), cells);
    assert_eq!(count(&harness.wire, Shape::UploadPart), 1);
    source.flip(PART + 5);
    let failed = block_on(upload.write_from(last, &source));
    assert_eq!(failed, Err(changed()));
    // The second was refused on its first cell: no cell of it was sent.
    assert_eq!(source.reads(), cells + 1);
    assert_eq!(count(&harness.wire, Shape::UploadPart), 1);
    assert_eq!(count(&harness.wire, Shape::Complete), 0);
    assert!(block_on(upload.write_from(last, &source)).is_err(), "over");
    assert!(block_on(upload.commit()).is_err(), "over");
    assert_eq!(stored(&harness, &key(0, 1, 1)), None);
}

/// A read that gives nothing at the commit, on the first cell of the body or on the second,
/// for the one PUT of a small object and for the last part of a large one: the source's
/// error is the commit's, as it is, and nothing is published.
#[test]
fn a_later_read_that_gives_nothing_fails_the_commit_before_anything_is_published() {
    for err in [changed(), unreadable()] {
        for (len, publishing) in [
            (CELL + 100_000, Shape::Put),
            (PART + CELL + 100_000, Shape::Complete),
        ] {
            // The two cells left at the commit are read as the body.
            for pass in [1, 2] {
                let bytes = body(len);
                let harness = FakeHarness::with_config(small_parts());
                let source = Source::of(&bytes);
                let mut upload = begin(&harness.store, &key(0, 1, 1));
                block_on(upload.write_from(&bytes, &source)).unwrap();
                source.fail_read(source.reads() + pass, err.clone());
                let failed = block_on(upload.commit());
                assert_eq!(failed, Err(err.clone()), "{len} bytes, pass {pass}");
                assert_eq!(count(&harness.wire, Shape::Complete), 0, "{len} bytes");
                assert_eq!(count(&harness.wire, Shape::Put), 0, "{len} bytes");
                // Of a large object the full part before the last went; the last did not.
                let parts = usize::from(publishing == Shape::Complete);
                assert_eq!(
                    count(&harness.wire, Shape::UploadPart),
                    parts,
                    "{len} bytes, pass {pass}"
                );
                assert_eq!(stored(&harness, &key(0, 1, 1)), None, "{len} bytes");
            }
        }
    }
}

/// A part's failure is its own: a request that failed is reported as the request's, a read
/// that gave nothing as the source's, and the parts after a failed one are not sent.
#[test]
fn a_parts_failure_is_reported_as_what_it_was() {
    let bytes = body(2 * PART);
    let reset = Fault::Transport {
        on: Shape::UploadPart,
        err: TransportError::Io(ErrorKind::ConnectionReset),
    };
    for (unread, want) in [
        (
            false,
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: ErrorKind::ConnectionReset,
            },
        ),
        (true, unreadable()),
    ] {
        let harness = FakeHarness::with_config(small_parts());
        harness.wire.push(reset.clone());
        let source = Source::of(&bytes);
        if unread {
            source.fail_read(1, unreadable());
        }
        let failed = put_from(&harness.store, &key(0, 1, 1), &bytes, CELL, &source);
        assert_eq!(failed, Err(want), "unread: {unread}");
        assert_eq!(
            count(&harness.wire, Shape::UploadPart),
            usize::from(!unread)
        );
        assert_eq!(stored(&harness, &key(0, 1, 1)), None);
    }
}

/// The store itself uses a cell's bytes only when they are what was first seen, so a source
/// that breaks its word and gives other bytes, or another count of them, has them neither
/// sent: the body ends short and nothing is published.
#[test]
fn a_source_that_breaks_its_word_publishes_nothing() {
    for len in [CELL + 100_000, PART + CELL + 100_000] {
        let bytes = body(len);
        let harness = FakeHarness::with_config(small_parts());
        let source = Source::unchecked(&bytes);
        let mut upload = begin(&harness.store, &key(0, 1, 1));
        block_on(upload.write_from(&bytes, &source)).unwrap();
        source.flip(len - 5);
        assert_eq!(block_on(upload.commit()), Err(unseen()), "{len} bytes");
        assert_eq!(count(&harness.wire, Shape::Put), 0, "{len} bytes");
        assert_eq!(count(&harness.wire, Shape::Complete), 0, "{len} bytes");
        assert_eq!(stored(&harness, &key(0, 1, 1)), None, "{len} bytes");
    }

    #[derive(Clone)]
    struct Short;
    impl Reread for Short {
        fn open(&self) -> BoxFuture<'_, Result<Box<dyn Again>, StoreError>> {
            let reader: Box<dyn Again> = Box::new(Self);
            Box::pin(std::future::ready(Ok(reader)))
        }
    }
    impl Again for Short {
        fn read<'a>(
            &'a mut self,
            _offset: u64,
            _was: &'a Seen,
        ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
            Box::pin(async { Ok(vec![0; 10]) })
        }
    }
    let harness = FakeHarness::with_config(small_parts());
    let mut upload = begin(&harness.store, &key(0, 1, 1));
    block_on(upload.write_from(&body(CELL + 100), &Short)).unwrap();
    assert_eq!(block_on(upload.commit()), Err(unseen()));
    assert_eq!(count(&harness.wire, Shape::Put), 0);
}

/// A source that cannot open its file fails the write that asked, with its own error.
#[test]
fn a_source_that_gives_no_reader_fails_the_write() {
    struct Gone;
    impl Reread for Gone {
        fn open(&self) -> BoxFuture<'_, Result<Box<dyn Again>, StoreError>> {
            Box::pin(std::future::ready(Err(unreadable())))
        }
    }
    let harness = FakeHarness::with_config(small_parts());
    let mut upload = begin(&harness.store, &key(0, 1, 1));
    assert_eq!(
        block_on(upload.write_from(&body(100), &Gone)),
        Err(unreadable())
    );
    assert!(block_on(upload.commit()).is_err(), "over");
    assert!(harness.wire.requests().is_empty());
}

/// The first write decides how an upload holds its bytes. One written from a source holds
/// none, so a plain write of it is refused and ends it; one that began with plain writes
/// goes on holding what it is handed and never reads the source.
#[test]
fn an_upload_is_written_one_way() {
    let bytes = body(PART + 100_000);
    let key = key(0, 1, 1);
    let ended = |err: &StoreError| matches!(err, StoreError::Refused { .. });

    let harness = FakeHarness::with_config(small_parts());
    let source = Source::of(&bytes);
    let mut upload = begin(&harness.store, &key);
    block_on(upload.write_from(&bytes, &source)).unwrap();
    let refused = block_on(upload.write(b"more")).unwrap_err();
    assert!(ended(&refused), "{refused:?}");
    assert!(block_on(upload.commit()).is_err(), "over");
    assert_eq!(stored(&harness, &key), None);
    assert_eq!(count(&harness.wire, Shape::Complete), 0);

    let source = Source::of(&bytes);
    let mut upload = begin(&harness.store, &key);
    block_on(async {
        upload.write(&bytes[..1]).await.unwrap();
        upload.write_from(&bytes[1..], &source).await.unwrap();
        upload.commit().await.unwrap();
    });
    assert_eq!(
        (source.reads(), source.opens()),
        (0, 0),
        "held, not read again"
    );
    assert!(stored(&harness, &key).as_deref() == Some(&bytes[..]));
}

/// A part's checksum failure names the part, for an upload from a source as for any.
#[test]
fn a_streamed_part_is_judged_as_any_part_is() {
    let bytes = body(PART + 100_000);
    let harness = FakeHarness::with_config(small_parts());
    harness.wire.push(Fault::Status {
        on: Shape::UploadPart,
        status: 400,
        code: Some("XAmzContentSHA256Mismatch"),
        retry_after: None,
    });
    let source = Source::of(&bytes);
    let failed = put_from(&harness.store, &key(0, 1, 1), &bytes, CELL, &source).unwrap_err();
    assert!(
        matches!(
            failed,
            StoreError::Corrupt {
                cause: CorruptCause::ChecksumMismatch { part: Some(_) },
                ..
            }
        ),
        "{failed:?}"
    );
    assert_eq!(stored(&harness, &key(0, 1, 1)), None);
}

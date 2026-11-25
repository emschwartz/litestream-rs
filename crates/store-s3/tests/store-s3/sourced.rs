//! Uploads written from a source the store reads again (`Upload::write_from`,
//! `docs/decisions/D222.md`): the store keeps no part, sends what a buffered upload sends,
//! and fails with the source's own error, before anything is published, when a second read
//! gives nothing.

use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use lsr_core::conformance::block_on;
use lsr_core::{
    BoxFuture, ByteTally, CorruptCause, FakeClock, FileKey, ObjectStore, Reread, StoreError,
    StoreOp, Upload, WriteMode,
};
use lsr_ltx::Timestamp;
use lsr_store_s3::{
    FakeS3, Fault, HttpRequest, HttpResponse, PartSize, S3Config, S3Store, Shape, StreamedRequest,
    Transport, TransportError, object_key,
};
use md5::{Digest as _, Md5};

use crate::common::*;
use crate::rig::{FakeHarness, count, shapes};

const MIB: usize = 1024 * 1024;
const CELL: usize = 256 * 1024;
const PART: usize = 5 * MIB;

/// Parts of 5 MiB, two at a time.
fn small_parts() -> S3Config {
    config()
        .with_part_size(PartSize::MIN)
        .with_concurrency(NonZeroUsize::new(2).unwrap())
}

/// Bytes that never repeat, so a read from another offset is other bytes.
fn body(len: usize) -> Vec<u8> {
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

fn tally(bytes: &[u8]) -> ByteTally {
    let mut tally = ByteTally::new();
    tally.add(bytes);
    tally
}

/// What [`Source`] answers when the bytes it holds are not the ones tallied.
const fn changed() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Read,
        kind: ErrorKind::InvalidData,
    }
}

/// A read the source's own storage failed.
const fn unreadable() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Read,
        kind: ErrorKind::PermissionDenied,
    }
}

/// The file an upload is written from: its bytes are given back only when they tally as
/// they did when first handed over, as the engine's source gives them.
#[derive(Default)]
struct Source {
    bytes: Mutex<Vec<u8>>,
    reads: AtomicU32,
    /// The read, counted from one, that fails.
    fail: Mutex<Option<(u32, StoreError)>>,
    /// Gives whatever it holds, tallied or not: a source that breaks its word.
    unchecked: bool,
}

impl Source {
    fn of(bytes: &[u8]) -> Self {
        Self {
            bytes: Mutex::new(bytes.to_vec()),
            ..Self::default()
        }
    }

    fn reads(&self) -> u32 {
        self.reads.load(Ordering::Relaxed)
    }

    fn fail_read(&self, nth: u32, err: StoreError) {
        *self.fail.lock().unwrap() = Some((nth, err));
    }

    fn flip(&self, at: usize) {
        self.bytes.lock().unwrap()[at] ^= 0x40;
    }
}

impl Reread for Source {
    fn read<'a>(
        &'a self,
        offset: u64,
        was: &'a ByteTally,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        let nth = self.reads.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let failed = self.fail.lock().unwrap().take_if(|(at, _)| *at == nth);
        let len = usize::try_from(was.size()).unwrap();
        assert!(
            (1..=CELL).contains(&len),
            "a second read of {len} bytes: one read's worth is asked for at a time"
        );
        let start = usize::try_from(offset).unwrap();
        let bytes = self
            .bytes
            .lock()
            .unwrap()
            .get(start..start.saturating_add(len))
            .map(<[u8]>::to_vec);
        Box::pin(async move {
            if let Some((_, err)) = failed {
                return Err(err);
            }
            let bytes = bytes.ok_or_else(changed)?;
            if self.unchecked || tally(&bytes) == *was {
                Ok(bytes)
            } else {
                Err(changed())
            }
        })
    }
}

fn begin<S: ObjectStore>(store: &S, key: &FileKey) -> S::Upload {
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
        upload.commit_from(source).await.map(drop)
    })
}

fn stored(harness: &FakeHarness, key: &FileKey) -> Option<Vec<u8>> {
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
/// neither a part nor a tally: the requests, their sizes and the object are a buffered
/// upload's, and every byte was read back.
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
        let cells = u32::try_from(2 * PART.div_ceil(CELL) + 1).unwrap();
        assert_eq!(source.reads(), cells, "chunks of {chunk}");
    }
}

/// An object below one part goes up as one PUT read back at the commit, and an empty one
/// reads nothing.
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
        block_on(upload.commit_from(&source)).unwrap();
        assert_eq!(shapes(&harness.wire), [Some(Shape::Put)], "{len} bytes");
        assert_eq!(stored(&harness, &key), Some(bytes), "{len} bytes");
        assert_eq!(
            source.reads(),
            u32::try_from(len.div_ceil(CELL)).unwrap(),
            "{len} bytes"
        );
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
    /// The `Content-MD5` header.
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

    fn send_streamed(
        &self,
        requests: Vec<StreamedRequest<'_>>,
        _limit: NonZeroUsize,
    ) -> Vec<Result<HttpResponse, TransportError>> {
        requests
            .into_iter()
            .map(|streamed| {
                let (len, own_body) = (streamed.len(), !streamed.request().body().is_empty());
                assert_eq!(streamed.is_empty(), len == 0);
                let whole = streamed.into_buffered()?;
                self.seen.lock().unwrap().push(Streamed {
                    shape: Shape::of(&whole),
                    md5: whole.header("content-md5").map(str::to_owned),
                    length: whole.header("content-length").map(str::to_owned),
                    len,
                    body: whole.body().to_vec(),
                    own_body,
                });
                self.fake.send(whole)
            })
            .collect()
    }
}

/// Every request whose body is read while it is sent declares the body's length and carries
/// the MD5 of the bytes first handed over, so the store checks what it received against the
/// first read whatever the second one gave.
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
                md5: Some(STANDARD.encode(Md5::digest(part))),
                length: Some(part.len().to_string()),
                len: u64::try_from(part.len()).unwrap(),
                body: part.to_vec(),
                own_body: false,
            })
            .collect();
        assert!(*seen == want, "{len} bytes");
    }
}

/// The file changes on disk under a part that was handed over and not yet sent: the write
/// that sends the batch fails with the source's error, the part is never sent, the upload is
/// over and nothing is published.
#[test]
fn a_second_read_that_differs_fails_the_write_that_sends_the_part() {
    let bytes = body(2 * PART);
    let harness = FakeHarness::with_config(small_parts());
    let source = Source::of(&bytes);
    let mut upload = begin(&harness.store, &key(0, 1, 1));
    let (first, last) = bytes.split_at(2 * PART - CELL);
    block_on(upload.write_from(first, &source)).unwrap();
    assert_eq!(source.reads(), 0);
    source.flip(PART + 5);
    let failed = block_on(upload.write_from(last, &source));
    assert_eq!(failed, Err(changed()));
    // The first part went; the second was refused on its first tally.
    assert_eq!(count(&harness.wire, Shape::UploadPart), 1);
    assert_eq!(count(&harness.wire, Shape::Complete), 0);
    assert!(block_on(upload.write_from(last, &source)).is_err(), "over");
    assert!(block_on(upload.commit_from(&source)).is_err(), "over");
    assert_eq!(stored(&harness, &key(0, 1, 1)), None);
}

/// A second read that gives nothing at the commit, for the one PUT of a small object and for
/// the last part of a large one: the source's error is the commit's, as it is, and the
/// request that would have published the object is never sent.
#[test]
fn a_second_read_that_gives_nothing_fails_the_commit_before_anything_is_published() {
    for err in [changed(), unreadable()] {
        for (len, publishing) in [(100_000, Shape::Put), (PART + 100_000, Shape::Complete)] {
            let bytes = body(len);
            let harness = FakeHarness::with_config(small_parts());
            let source = Source::of(&bytes);
            let mut upload = begin(&harness.store, &key(0, 1, 1));
            block_on(upload.write_from(&bytes, &source)).unwrap();
            // The last tally's read.
            let nth = u32::try_from(len.div_ceil(CELL)).unwrap();
            source.fail_read(nth, err.clone());
            let failed = block_on(upload.commit_from(&source));
            assert_eq!(failed, Err(err.clone()), "{len} bytes");
            assert_eq!(count(&harness.wire, publishing), 0, "{len} bytes");
            // Of a large object the full part before the last went; the last did not.
            let parts = usize::from(publishing == Shape::Complete);
            assert_eq!(
                count(&harness.wire, Shape::UploadPart),
                parts,
                "{len} bytes"
            );
            assert_eq!(stored(&harness, &key(0, 1, 1)), None, "{len} bytes");
        }
    }
}

/// The first failure in part order is the upload's: a second part's read that gave nothing
/// does not hide the first part's own failure, and the first part's read is not hidden by
/// the second's request.
#[test]
fn the_first_failure_in_part_order_is_the_uploads() {
    let bytes = body(2 * PART);
    let reset = Fault::Transport {
        on: Shape::UploadPart,
        err: TransportError::Io(ErrorKind::ConnectionReset),
    };
    let cells = u32::try_from(PART / CELL).unwrap();
    for (unread_part, want) in [
        (
            2,
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: ErrorKind::ConnectionReset,
            },
        ),
        (1, unreadable()),
    ] {
        let harness = FakeHarness::with_config(small_parts());
        harness.wire.push(reset.clone());
        let source = Source::of(&bytes);
        source.fail_read((unread_part - 1) * cells + 1, unreadable());
        let failed = put_from(&harness.store, &key(0, 1, 1), &bytes, CELL, &source);
        assert_eq!(failed, Err(want), "part {unread_part} unread");
        assert_eq!(stored(&harness, &key(0, 1, 1)), None);
    }
}

/// The store checks what it received against the digest of the first read, so even a source
/// that breaks its word and gives other bytes publishes nothing; and one that gives another
/// count is refused before the body can end short without a reason.
#[test]
fn a_source_that_breaks_its_word_publishes_nothing() {
    for len in [100_000, PART + 100_000] {
        let bytes = body(len);
        let harness = FakeHarness::with_config(small_parts());
        let source = Source {
            unchecked: true,
            ..Source::of(&bytes)
        };
        let mut upload = begin(&harness.store, &key(0, 1, 1));
        block_on(upload.write_from(&bytes, &source)).unwrap();
        source.flip(len - 5);
        let failed = block_on(upload.commit_from(&source)).unwrap_err();
        // 400 BadDigest: what the store received is not what the digest was taken over.
        assert!(
            matches!(
                failed,
                StoreError::Corrupt {
                    cause: CorruptCause::ChecksumMismatch { .. },
                    ..
                }
            ),
            "{len} bytes: {failed:?}"
        );
        assert_eq!(stored(&harness, &key(0, 1, 1)), None, "{len} bytes");
    }

    struct Short;
    impl Reread for Short {
        fn read<'a>(
            &'a self,
            _offset: u64,
            _was: &'a ByteTally,
        ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
            Box::pin(async { Ok(vec![0; 10]) })
        }
    }
    let harness = FakeHarness::with_config(small_parts());
    let mut upload = begin(&harness.store, &key(0, 1, 1));
    block_on(upload.write_from(&body(100), &Short)).unwrap();
    assert_eq!(
        block_on(upload.commit_from(&Short)),
        Err(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::InvalidData,
        })
    );
    assert!(harness.wire.requests().is_empty());
}

/// The first write decides how an upload holds its bytes. One written from a source holds
/// none, so a plain write or commit of it is refused and ends it; one that began with plain
/// writes goes on holding what it is handed and never reads the source.
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
    assert!(block_on(upload.commit_from(&source)).is_err(), "over");

    let mut upload = begin(&harness.store, &key);
    block_on(upload.write_from(&bytes, &source)).unwrap();
    let refused = block_on(upload.commit()).unwrap_err();
    assert!(ended(&refused), "{refused:?}");
    assert_eq!(stored(&harness, &key), None);
    assert_eq!(count(&harness.wire, Shape::Complete), 0);
    assert_eq!(source.reads(), 0);

    let mut upload = begin(&harness.store, &key);
    block_on(async {
        upload.write(&bytes[..1]).await.unwrap();
        upload.write_from(&bytes[1..], &source).await.unwrap();
        upload.commit_from(&source).await.unwrap();
    });
    assert_eq!(source.reads(), 0, "held, not read again");
    assert!(stored(&harness, &key).as_deref() == Some(&bytes[..]));
}

/// A part's checksum failure names the part, for an upload from a source as for any.
#[test]
fn a_streamed_part_is_judged_as_any_part_is() {
    let bytes = body(PART + 100_000);
    let harness = FakeHarness::with_config(small_parts());
    harness.wire.push(Fault::WrongEtag {
        on: Shape::UploadPart,
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

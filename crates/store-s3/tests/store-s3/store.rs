//! The store over FakeS3: the conformance suite (no Docker, in `just check`) and
//! the cases the Transport level could only state at the Transport level because they need the
//! store's five operations or `Retrying`.

use crate::common;
use crate::rig;

// The helpers are test code only by cfg, which is what clippy.toml's allow-*-in-tests reads.
#[cfg(test)]
mod cases {
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use super::common::*;
    use super::rig::*;
    use lsr_core::WriteMode;
    use lsr_core::conformance::{Harness, block_on};
    use lsr_core::{
        CorruptCause, CreatedAtSource, Denied, FileKey, Level, ListedTime, ObjectStore, PartNumber,
        ReadLen, RecordingSleeper, RetryPolicy, Retrying, SeededEntropy, StoreError, StoreOp,
        Upload,
    };
    use lsr_ltx::Timestamp;
    use lsr_store_s3::{
        FakeS3, Fault, FaultTransport, ListStats, S3Store, Shape, WriteStats, object_key,
    };

    lsr_core::conformance_suite!(FakeHarness::new);

    type Retried = Retrying<S3Store<Shared>, RecordingSleeper, SeededEntropy>;

    fn retried(h: FakeHarness) -> (Arc<FaultTransport<FakeS3>>, RecordingSleeper, Retried) {
        let sleeper = RecordingSleeper::new();
        let store = Retrying::new(
            h.store,
            sleeper.clone(),
            SeededEntropy::new(7),
            RetryPolicy::DEFAULT,
        );
        (h.wire, sleeper, store)
    }

    #[test]
    fn throttle_then_success_under_retrying() {
        for (status, code) in [(503, Some("SlowDown")), (429, None)] {
            let (wire, sleeper, store) = retried(FakeHarness::new());
            wire.push(Fault::Status {
                on: Shape::List,
                status,
                code,
                retry_after: Some("1"),
            });
            assert!(list_all(&store).unwrap().files.is_empty());
            assert_eq!(sleeper.waits(), [Duration::from_secs(1)]);
            assert_eq!(count(&wire, Shape::List), 2);
        }
    }

    #[test]
    fn retry_after_past_cap_returns_at_once() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        wire.push(Fault::Status {
            on: Shape::List,
            status: 503,
            code: Some("SlowDown"),
            retry_after: Some("60"),
        });
        assert_eq!(
            list_all(&store).unwrap_err(),
            StoreError::Throttled {
                op: StoreOp::List,
                retry_after: Some(Duration::from_secs(60))
            }
        );
        assert!(sleeper.waits().is_empty());
        assert_eq!(count(&wire, Shape::List), 1);
    }

    #[test]
    fn listing_makes_no_head_requests() {
        let h = FakeHarness::new();
        for n in 1..=5 {
            put_through(&h.store, &key(0, n, n), b"x").unwrap();
        }
        let before = count(&h.wire, Shape::Head);
        let page = block_on(h.store.list(Level::ZERO, None, limit(3))).unwrap();
        assert_eq!(page.files.len(), 3);
        assert_eq!(count(&h.wire, Shape::Head), before);
        assert!(page.files.iter().all(|f| f.created_at().is_none()));
        assert_eq!(h.store.created_at_stats(), ListStats::default());
    }

    #[test]
    fn a_stream_whose_get_is_throttled_is_sent_again_after_the_wait() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        put_through(&store, &k, b"abcdefghi").unwrap();
        wire.push(Fault::Status {
            on: Shape::Get,
            status: 503,
            code: Some("SlowDown"),
            retry_after: Some("1"),
        });
        let got = pieces(&store, k, 0, 3);
        assert_eq!(
            got,
            [
                b"abc".to_vec(),
                b"def".to_vec(),
                b"ghi".to_vec(),
                Vec::new()
            ]
        );
        assert_eq!(sleeper.waits(), [Duration::from_secs(1)]);
        assert_eq!(count(&wire, Shape::Get), 2);
    }

    /// The pieces of a stream, `len` bytes asked each time, until one comes back short.
    fn pieces(store: &impl ObjectStore, k: FileKey, offset: u64, len: u32) -> Vec<Vec<u8>> {
        let mut stream = store.read_stream(k, offset);
        let len = ReadLen::new(len).unwrap();
        let mut out = Vec::new();
        loop {
            let piece = block_on(stream.next(len)).unwrap();
            let short = u64::try_from(piece.len()).unwrap() < len.get();
            out.push(piece);
            if short {
                return out;
            }
        }
    }

    #[test]
    fn a_stream_is_one_get_however_many_pieces_it_hands_out() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abcdefghi").unwrap();
        let before = count(&h.wire, Shape::Get);
        let got = pieces(&h.store, k, 0, 3);
        assert_eq!(got, [&b"abc"[..], b"def", b"ghi", b""]);
        assert_eq!(count(&h.wire, Shape::Get), before + 1);
        assert_eq!(pieces(&h.store, k, 4, 4), [&b"efgh"[..], b"i"]);
    }

    /// A held stream borrows nothing (a compaction keeps it between ticks) and is the same one
    /// GET, however many pieces it hands out.
    #[test]
    fn a_held_stream_is_one_get_however_many_pieces_it_hands_out() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abcdefghi").unwrap();
        let before = count(&h.wire, Shape::Get);
        let mut stream = block_on(h.store.hold_stream(k, 2)).unwrap().unwrap();
        let len = ReadLen::new(3).unwrap();
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(block_on(stream.next(len)).unwrap());
        }
        assert_eq!(got, [&b"cde"[..], b"fgh", b"i"]);
        assert_eq!(count(&h.wire, Shape::Get), before + 1);
    }

    #[test]
    fn a_stream_from_past_the_end_hands_out_nothing() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abc").unwrap();
        assert_eq!(pieces(&h.store, k, 3, 3), [Vec::<u8>::new()]);
        assert_eq!(pieces(&h.store, k, 30, 3), [Vec::<u8>::new()]);
    }

    #[test]
    fn a_stream_of_a_missing_object_is_not_found() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        let mut stream = h.store.read_stream(k, 0);
        assert!(matches!(
            block_on(stream.next(ReadLen::new(3).unwrap())),
            Err(StoreError::NotFound { .. })
        ));
    }

    /// A body that ends before its declared length is a failure, never the end of the object:
    /// taken as the end, it would be a short file.
    #[test]
    fn a_stream_cut_short_fails_where_the_body_ends() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abcdefghi").unwrap();
        h.wire.push(Fault::TruncateBody {
            on: Shape::Get,
            keep: 4,
        });
        let mut stream = h.store.read_stream(k, 0);
        let len = ReadLen::new(3).unwrap();
        assert_eq!(block_on(stream.next(len)).unwrap(), b"abc");
        assert_eq!(
            block_on(stream.next(len)),
            Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::UnexpectedEof,
            })
        );
    }

    /// Through the retry layer a stream cut short goes on with a second GET from the bytes it
    /// handed out, so the pieces are the object's.
    #[test]
    fn a_stream_cut_short_goes_on_from_where_it_stopped_under_retrying() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        put_through(&store, &k, b"abcdefghi").unwrap();
        wire.push(Fault::TruncateBody {
            on: Shape::Get,
            keep: 4,
        });
        let before = count(&wire, Shape::Get);
        assert_eq!(pieces(&store, k, 0, 3), [&b"abc"[..], b"def", b"ghi", b""]);
        assert_eq!(count(&wire, Shape::Get), before + 2);
        assert_eq!(sleeper.waits().len(), 1);
    }

    #[test]
    fn a_failed_listing_in_a_batch_fails_only_its_own_slot() {
        let (wire, store) = store_with(4);
        put_through(&store, &key(0, 1, 1), b"x").unwrap();
        put_through(&store, &key(1, 1, 1), b"y").unwrap();
        wire.push(Fault::Status {
            on: Shape::List,
            status: 500,
            code: Some("InternalError"),
            retry_after: None,
        });
        let pages = [Level::ZERO, Level::new(1).unwrap(), Level::new(2).unwrap()]
            .map(|level| lsr_core::ListAt { level, after: None });
        let got = block_on(store.list_many(&pages, limit(10)));
        assert!(
            matches!(got[0], Err(StoreError::Transient { .. })),
            "{got:?}"
        );
        assert_eq!(got[1].as_ref().unwrap().files.len(), 1);
        assert!(got[2].as_ref().unwrap().files.is_empty());
        assert_eq!(count(&wire, Shape::List), 3);
    }

    #[test]
    fn head_404_between_list_and_created_at() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"x").unwrap();
        let file = list_all(&h.store).unwrap().files[0];
        h.wire.push(Fault::Status {
            on: Shape::Head,
            status: 404,
            code: None,
            retry_after: None,
        });
        assert_eq!(
            block_on(h.store.created_at(&file)),
            Err(StoreError::NotFound { key: k })
        );
        assert_eq!(h.store.created_at_stats().vanished, 1);
    }

    /// A batch delete the store refuses for one key (object lock, a deny on
    /// delete) is that key's access refusal, persistent and named, not a
    /// transient failure retried with no signal.
    #[test]
    fn a_delete_refused_for_one_key_is_a_permission_denial() {
        let h = FakeHarness::new();
        let (kept, gone) = (key(0, 1, 1), key(0, 2, 2));
        for k in [&kept, &gone] {
            h.wire
                .inner()
                .put_raw(&object_key(config().prefix(), k), b"abc".to_vec(), &[]);
        }
        h.wire
            .inner()
            .deny_delete(&object_key(config().prefix(), &kept));
        assert_eq!(
            block_on(h.store.delete(&[kept, gone])),
            Err(StoreError::Denied {
                op: StoreOp::Delete,
                why: Denied::Permission
            })
        );
    }

    #[test]
    fn metadata_less_object_falls_back_on_created_at() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        h.wire
            .inner()
            .put_raw(&object_key(config().prefix(), &k), b"abc".to_vec(), &[]);
        let file = list_all(&h.store).unwrap().files[0];
        let created_at = block_on(h.store.created_at(&file)).unwrap();
        assert_eq!(created_at.origin(), CreatedAtSource::Fallback);
        let ListedTime::LastModified(listed) = file.listed else {
            panic!("a listed file carries LastModified: {file:?}");
        };
        assert_eq!(created_at.at(), listed);
        assert_eq!(h.store.created_at_stats().fell_back, 1);
    }

    #[test]
    fn bad_created_at_fails_the_reader_not_the_listing() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        h.wire.inner().put_raw(
            &object_key(config().prefix(), &k),
            b"abc".to_vec(),
            &[("x-amz-meta-litestream-timestamp", "yesterday")],
        );
        let listed = list_all(&h.store).unwrap();
        let [file] = listed.files.as_slice() else {
            panic!("the listing keeps the file: {listed:?}");
        };
        let bad = StoreError::Corrupt {
            key: Some(k),
            cause: CorruptCause::BadCreatedAt,
        };
        assert_eq!(block_on(h.store.created_at(file)), Err(bad.clone()));
        let read = block_on(h.store.read_with_created_at(file, ReadLen::new(8).unwrap()));
        assert_eq!(read, Err(bad));
    }

    #[test]
    fn read_with_created_at_is_one_get() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abc").unwrap();
        let file = list_all(&h.store).unwrap().files[0];
        let before = shapes(&h.wire).len();
        let (bytes, created_at) = block_on(
            h.store
                .read_with_created_at(&file, ReadLen::new(8).unwrap()),
        )
        .unwrap();
        assert_eq!(bytes, b"abc");
        assert_eq!(created_at.at(), Timestamp::from_unix_millis(5));
        let sent = shapes(&h.wire).split_off(before);
        assert_eq!(sent, [Some(Shape::Get)]);
    }

    #[test]
    fn multipart_created_at_is_the_commit_created_at() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        write_all(&h.store, &k, &body(PART * 2), PART).unwrap();
        let file = list_all(&h.store).unwrap().files[0];
        let created_at = block_on(h.store.created_at(&file)).unwrap();
        assert_eq!(created_at.at(), Timestamp::from_unix_millis(5));
        assert_eq!(created_at.origin(), CreatedAtSource::Metadata);
    }

    #[test]
    fn last_modified_never_moves_an_answer() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abc").unwrap();
        let object = object_key(config().prefix(), &k);
        h.wire
            .inner()
            .set_last_modified(&object, Timestamp::from_unix_millis(86_400_000));
        let file = list_all(&h.store).unwrap().files[0];
        assert_eq!(
            file.listed,
            ListedTime::LastModified(Timestamp::from_unix_millis(86_400_000))
        );
        let created_at = block_on(h.store.created_at(&file)).unwrap();
        assert_eq!(created_at.at(), Timestamp::from_unix_millis(5));
        let (_, read) = block_on(
            h.store
                .read_with_created_at(&file, ReadLen::new(8).unwrap()),
        )
        .unwrap();
        assert_eq!(read.at(), created_at.at());
    }

    #[test]
    fn created_at_is_one_head_and_a_read_with_created_at_is_one_get() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abc").unwrap();
        let listed = list_all(&h.store).unwrap();
        let file = listed.files[0];
        let heads = count(&h.wire, Shape::Head);

        let created_at = block_on(h.store.created_at(&file)).unwrap();
        assert_eq!(count(&h.wire, Shape::Head), heads + 1);

        let gets = count(&h.wire, Shape::Get);
        let (bytes, read_created_at) = block_on(
            h.store
                .read_with_created_at(&file, ReadLen::new(8).unwrap()),
        )
        .unwrap();
        assert_eq!(bytes, b"abc");
        assert_eq!(read_created_at.at(), created_at.at());
        assert_eq!(count(&h.wire, Shape::Get), gets + 1);
        assert_eq!(
            count(&h.wire, Shape::Head),
            heads + 1,
            "the GET carried the created-at time"
        );
    }

    #[test]
    fn a_read_with_created_at_of_an_object_with_no_created_at_falls_back_to_a_head() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        put_through(&h.store, &k, b"abc").unwrap();
        let file = list_all(&h.store).unwrap().files[0];
        h.wire.inner().put_raw(
            &lsr_store_s3::object_key(config().prefix(), &k),
            b"abc".to_vec(),
            &[],
        );
        let heads = count(&h.wire, Shape::Head);
        let (_, created_at) = block_on(
            h.store
                .read_with_created_at(&file, ReadLen::new(8).unwrap()),
        )
        .unwrap();
        assert_eq!(created_at.origin(), lsr_core::CreatedAtSource::Fallback);
        assert_eq!(count(&h.wire, Shape::Head), heads + 1);
    }

    /// A PUT whose body the store refuses against its signed SHA-256 is a checksum mismatch:
    /// not retried here, and nothing to delete, since the store published nothing.
    #[test]
    fn a_put_refused_against_its_signed_hash_is_corrupt() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        wire.push(Fault::Status {
            on: Shape::Put,
            status: 400,
            code: Some("XAmzContentSHA256Mismatch"),
            retry_after: None,
        });
        assert_eq!(
            put_through(&store, &k, b"abc").unwrap_err(),
            StoreError::Corrupt {
                key: Some(k),
                cause: lsr_core::CorruptCause::ChecksumMismatch { part: None }
            }
        );
        assert_eq!(
            count(&wire, Shape::Put),
            1,
            "a corrupt commit is not retried"
        );
        assert_eq!(count(&wire, Shape::DeleteObjects), 0);
        assert!(sleeper.waits().is_empty());
        assert!(wire.inner().keys().is_empty());
    }

    /// A PUT answered with the ETag of other bytes is written: as in Go, the ETag is not
    /// compared with anything (the store checked the body against its signed hash).
    #[test]
    fn a_wrong_etag_on_put_is_taken_as_it_comes() {
        let (wire, _sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        wire.push(Fault::WrongEtag { on: Shape::Put });
        put_through(&store, &k, b"abc").unwrap();
        assert_eq!(wire.inner().keys().len(), 1);
    }

    #[test]
    fn five_hundred_after_publish_leaves_object_visible_with_created_at() {
        let h = FakeHarness::new();
        assert!(h.fail_next_commit_after_publish());
        let k = key(0, 1, 1);
        let err = put_through(&h.store, &k, b"abc").unwrap_err();
        assert_eq!(
            err,
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: io::ErrorKind::Other
            }
        );
        let listed = list_all(&h.store).unwrap();
        let [file] = listed.files.as_slice() else {
            panic!("one visible file expected: {listed:?}");
        };
        assert_eq!((file.key, file.size), (k, 3));
        assert_eq!(
            block_on(h.store.created_at(file)).unwrap().at(),
            Timestamp::from_unix_millis(5)
        );
        let read = block_on(h.store.read(&k, 0, ReadLen::new(16).unwrap())).unwrap();
        assert_eq!(read, b"abc");
    }

    const PART: usize = 5 * 1024 * 1024;

    /// A 251-byte cycle: the part size is no multiple of it, so parts differ and a swapped one shows.
    fn body(len: usize) -> Vec<u8> {
        (0..=250).cycle().take(len).collect()
    }

    fn write_all(
        store: &impl ObjectStore,
        k: &FileKey,
        bytes: &[u8],
        chunk: usize,
    ) -> Result<(), StoreError> {
        block_on(async {
            let mut up = store
                .begin_upload(k, Timestamp::from_unix_millis(5), WriteMode::Replace)
                .await?;
            for piece in bytes.chunks(chunk) {
                up.write(piece).await?;
            }
            up.commit().await.map(drop)
        })
    }

    /// `write_all` as a create-only write.
    fn write_new(
        store: &impl ObjectStore,
        k: &FileKey,
        bytes: &[u8],
        chunk: usize,
    ) -> Result<(), StoreError> {
        block_on(async {
            let mut up = store
                .begin_upload(k, Timestamp::from_unix_millis(5), WriteMode::New)
                .await?;
            for piece in bytes.chunks(chunk) {
                up.write(piece).await?;
            }
            up.commit().await.map(drop)
        })
    }

    fn read_back(store: &impl ObjectStore, k: &FileKey, len: usize) -> Vec<u8> {
        let want = u32::try_from(len).unwrap();
        block_on(store.read(k, 0, ReadLen::new(want).unwrap())).unwrap()
    }

    fn part(n: u16) -> Option<PartNumber> {
        PartNumber::new(n)
    }

    #[test]
    fn exactly_part_size_uses_multipart() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        let bytes = body(PART);
        write_all(&h.store, &k, &bytes, PART).unwrap();
        assert_eq!(count(&h.wire, Shape::Put), 0);
        assert_eq!(count(&h.wire, Shape::CreateMultipart), 1);
        assert_eq!(count(&h.wire, Shape::UploadPart), 1, "no empty last part");
        assert_eq!(count(&h.wire, Shape::Complete), 1);
        assert!(read_back(&h.store, &k, PART) == bytes);
    }

    #[test]
    fn one_below_part_size_uses_put() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        let bytes = body(PART - 1);
        write_all(&h.store, &k, &bytes, PART).unwrap();
        assert_eq!(count(&h.wire, Shape::Put), 1);
        assert_eq!(count(&h.wire, Shape::CreateMultipart), 0);
        assert!(read_back(&h.store, &k, PART - 1) == bytes);
    }

    #[test]
    fn multipart_round_trip_keeps_bytes_and_created_at() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        let len = PART * 5 / 2;
        let bytes = body(len);
        write_all(&h.store, &k, &bytes, 1_500_003).unwrap();
        assert_eq!(
            shapes(&h.wire),
            [
                Some(Shape::CreateMultipart),
                Some(Shape::UploadPart),
                Some(Shape::UploadPart),
                Some(Shape::UploadPart),
                Some(Shape::Complete),
            ]
        );
        assert!(read_back(&h.store, &k, len) == bytes);
        let listed = list_all(&h.store).unwrap();
        let [file] = listed.files.as_slice() else {
            panic!("one file expected: {listed:?}");
        };
        assert_eq!(file.size, u64::try_from(len).unwrap());
        assert_eq!(
            block_on(h.store.created_at(file)).unwrap().at(),
            Timestamp::from_unix_millis(5)
        );
        assert_eq!(h.wire.inner().open_uploads(), 0);
        assert_eq!(h.store.pending_orphans(), 0);
    }

    /// A part answered with the ETag of other bytes is taken as it comes; the store refuses the
    /// completion that names it (`InvalidPart`), which is a checksum mismatch: nothing is
    /// published and the engine redoes the upload.
    #[test]
    fn a_wrong_part_etag_fails_the_completion_as_corrupt() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        wire.push(Fault::WrongEtag {
            on: Shape::UploadPart,
        });
        let err = write_all(&store, &k, &body(PART * 2), PART).unwrap_err();
        assert_eq!(
            err,
            StoreError::Corrupt {
                key: Some(k),
                cause: CorruptCause::ChecksumMismatch { part: None }
            }
        );
        assert_eq!(count(&wire, Shape::Complete), 1, "not retried here");
        assert!(sleeper.waits().is_empty());
        assert!(list_all(&store).unwrap().files.is_empty());
    }

    #[test]
    fn a_part_refused_against_its_signed_hash_is_corrupt_and_not_retried() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        wire.push(Fault::Pass {
            on: Shape::UploadPart,
        });
        wire.push(Fault::Status {
            on: Shape::UploadPart,
            status: 400,
            code: Some("XAmzContentSHA256Mismatch"),
            retry_after: None,
        });
        let err = write_all(&store, &k, &body(PART * 3), PART).unwrap_err();
        assert_eq!(
            err,
            StoreError::Corrupt {
                key: Some(k),
                cause: CorruptCause::ChecksumMismatch { part: part(2) }
            }
        );
        assert_eq!(count(&wire, Shape::UploadPart), 2, "no third follows");
        assert_eq!(count(&wire, Shape::Complete), 0);
        assert!(sleeper.waits().is_empty());
        assert!(list_all(&store).unwrap().files.is_empty());
        for text in [format!("{err:?}"), err.to_string()] {
            assert!(!text.contains(CANARY) && !text.contains(TOKEN));
        }
    }

    #[test]
    fn a_failed_part_leaves_its_id_for_the_next_begin_to_abort() {
        let h = FakeHarness::new();
        for _ in 0..8 {
            h.wire.push(Fault::Status {
                on: Shape::UploadPart,
                status: 500,
                code: Some("InternalError"),
                retry_after: None,
            });
        }
        let err = write_all(&h.store, &key(0, 1, 1), &body(PART), PART).unwrap_err();
        assert!(matches!(err, StoreError::Transient { .. }), "{err:?}");
        assert_eq!(h.store.pending_orphans(), 1);
        assert_eq!(h.wire.inner().open_uploads(), 1);
        block_on(h.store.begin_upload(
            &key(0, 2, 2),
            Timestamp::from_unix_millis(5),
            WriteMode::Replace,
        ))
        .unwrap();
        assert_eq!(h.store.pending_orphans(), 0);
        assert_eq!(h.wire.inner().open_uploads(), 0);
        assert_eq!(count(&h.wire, Shape::Abort), 1);
    }

    #[test]
    fn complete_answering_an_error_in_a_200_is_not_success() {
        let h = FakeHarness::new();
        h.wire.push(Fault::ErrorInOkBody {
            on: Shape::Complete,
            code: "InternalError",
        });
        let err = write_all(&h.store, &key(0, 1, 1), &body(PART), PART).unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::Transient {
                    op: StoreOp::Upload,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(list_all(&h.store).unwrap().files.is_empty());
        assert_eq!(h.store.pending_orphans(), 1);
        assert_eq!(h.store.abort_orphans().unwrap(), 1);
        assert_eq!(h.wire.inner().open_uploads(), 0);
    }

    #[test]
    fn five_hundred_after_complete_leaves_object_visible_and_abort_is_a_no_op() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        h.wire.push(Fault::AfterPublish {
            on: Shape::Complete,
            status: 500,
        });
        let bytes = body(PART);
        let err = write_all(&h.store, &k, &bytes, PART).unwrap_err();
        assert!(matches!(err, StoreError::Transient { .. }), "{err:?}");
        let listed = list_all(&h.store).unwrap();
        let [file] = listed.files.as_slice() else {
            panic!("one visible file expected: {listed:?}");
        };
        assert_eq!(
            block_on(h.store.created_at(file)).unwrap().at(),
            Timestamp::from_unix_millis(5)
        );
        assert!(read_back(&h.store, &k, PART) == bytes);
        // The upload id is spent; its abort answers NoSuchUpload, which is success.
        assert_eq!(h.store.abort_orphans().unwrap(), 1);
    }

    #[test]
    fn dropped_multipart_is_invisible_until_aborted() {
        let h = FakeHarness::new();
        block_on(async {
            let mut up = h
                .store
                .begin_upload(
                    &key(0, 1, 1),
                    Timestamp::from_unix_millis(5),
                    WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&body(PART)).await.unwrap();
        });
        assert_eq!(count(&h.wire, Shape::Abort), 0, "drop does no I/O");
        assert!(list_all(&h.store).unwrap().files.is_empty());
        assert_eq!(h.wire.inner().open_uploads(), 1);
        assert_eq!(h.store.pending_orphans(), 1);
        assert_eq!(h.store.abort_orphans().unwrap(), 1);
        assert_eq!(h.wire.inner().open_uploads(), 0);
        assert_eq!(h.store.pending_orphans(), 0);
    }

    /// A process killed mid-upload takes its ids with it: the next process's boot finds the
    /// upload in the bucket's listing and aborts it. An upload under another key stays.
    #[test]
    fn a_killed_process_s_multipart_upload_is_aborted_at_the_next_boot() {
        let h = FakeHarness::new();
        block_on(async {
            let mut up = h
                .store
                .begin_upload(
                    &key(0, 1, 1),
                    Timestamp::from_unix_millis(5),
                    lsr_core::WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&body(PART)).await.unwrap();
        });
        let builder = lsr_store_s3::RequestBuilder::new(&config(), &credentials()).unwrap();
        let clock = lsr_core::FakeClock::new(now());
        let foreign = format!("{}/notes.ltx", config().prefix().as_str());
        lsr_store_s3::abandon_upload(&*h.wire, &builder, &clock, &foreign, body(PART)).unwrap();
        assert_eq!(h.wire.inner().open_uploads(), 2);
        // The next process over the same bucket knows no ids.
        let next = S3Store::with_transport(
            config(),
            credentials(),
            Shared(Arc::clone(&h.wire)),
            lsr_core::FakeClock::new(now()),
        )
        .unwrap();
        assert_eq!(next.pending_orphans(), 0);
        assert_eq!(block_on(next.remove_scratch()), Ok(1));
        assert_eq!(h.wire.inner().open_uploads(), 1, "the foreign upload stays");
        assert_eq!(count(&h.wire, Shape::ListUploads), 1);
        assert_eq!(block_on(next.remove_scratch()), Ok(0));
    }

    #[test]
    fn abort_removes_the_upload() {
        let h = FakeHarness::new();
        block_on(async {
            let mut up = h
                .store
                .begin_upload(
                    &key(0, 1, 1),
                    Timestamp::from_unix_millis(5),
                    WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&body(PART)).await.unwrap();
            up.abort().await.unwrap();
        });
        assert_eq!(count(&h.wire, Shape::Abort), 1);
        assert_eq!(h.wire.inner().open_uploads(), 0);
        assert_eq!(h.store.pending_orphans(), 0);
        assert!(list_all(&h.store).unwrap().files.is_empty());
    }

    #[test]
    fn a_lost_abort_ack_orphans_the_id_and_the_retry_succeeds() {
        let h = FakeHarness::new();
        h.wire.push(Fault::AfterPublish {
            on: Shape::Abort,
            status: 500,
        });
        let result = block_on(async {
            let mut up = h
                .store
                .begin_upload(
                    &key(0, 1, 1),
                    Timestamp::from_unix_millis(5),
                    WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&body(PART)).await.unwrap();
            up.abort().await
        });
        assert!(result.is_err());
        assert_eq!(h.wire.inner().open_uploads(), 0, "the abort reached S3");
        assert_eq!(h.store.pending_orphans(), 1);
        assert_eq!(
            h.store.abort_orphans().unwrap(),
            1,
            "NoSuchUpload is success"
        );
        assert_eq!(h.store.pending_orphans(), 0);
    }

    /// A store over a fresh fake with this many parts in flight at most.
    fn store_with(concurrency: usize) -> (Arc<FaultTransport<FakeS3>>, S3Store<Shared>) {
        let wire = Arc::new(FaultTransport::new(fake()));
        let config = config().with_concurrency(std::num::NonZeroUsize::new(concurrency).unwrap());
        let store = S3Store::with_transport(
            config,
            credentials(),
            Shared(Arc::clone(&wire)),
            lsr_core::FakeClock::new(now()),
        )
        .unwrap();
        (wire, store)
    }

    #[test]
    fn many_parts_assemble_the_same_object_in_order() {
        let (wire, store) = store_with(3);
        let k = key(0, 1, 1);
        let bytes = body(PART * 7 + 11);
        write_all(&store, &k, &bytes, 1_500_003).unwrap();
        assert_eq!(count(&wire, Shape::UploadPart), 8);
        assert!(read_back(&store, &k, PART * 3)[..] == bytes[..PART * 3]);
        let tail = block_on(store.read(
            &k,
            u64::try_from(PART * 7).unwrap(),
            ReadLen::new(64).unwrap(),
        ))
        .unwrap();
        assert_eq!(tail[..], bytes[PART * 7..]);
        assert_eq!(wire.inner().open_uploads(), 0);
    }

    /// A buffered part is held until its request succeeds, so a request that fails once is sent
    /// again with the same bytes: the upload completes with one extra UploadPart and no restart.
    #[test]
    fn a_buffered_part_that_fails_once_is_sent_again_and_the_upload_completes() {
        let (wire, store) = store_with(3);
        wire.push(Fault::Pass {
            on: Shape::UploadPart,
        });
        wire.push(Fault::Status {
            on: Shape::UploadPart,
            status: 500,
            code: Some("InternalError"),
            retry_after: None,
        });
        let bytes = body(PART * 5);
        write_all(&store, &key(0, 1, 1), &bytes, PART).unwrap();
        assert_eq!(
            count(&wire, Shape::UploadPart),
            6,
            "five parts and one again"
        );
        assert_eq!(count(&wire, Shape::CreateMultipart), 1, "no restart");
        assert_eq!(count(&wire, Shape::Complete), 1);
        assert_eq!(wire.pauses(), [Duration::from_millis(100)]);
        let stored = wire
            .inner()
            .body_of(&object_key(config().prefix(), &key(0, 1, 1)));
        assert_eq!(stored.as_deref(), Some(&bytes[..]));
    }

    #[test]
    fn a_failed_part_fails_the_upload_and_orphans_its_id() {
        let (wire, store) = store_with(3);
        wire.push(Fault::Pass {
            on: Shape::UploadPart,
        });
        for _ in 0..8 {
            wire.push(Fault::Status {
                on: Shape::UploadPart,
                status: 500,
                code: Some("InternalError"),
                retry_after: None,
            });
        }
        let err = write_all(&store, &key(0, 1, 1), &body(PART * 5), PART).unwrap_err();
        assert!(matches!(err, StoreError::Transient { .. }), "{err:?}");
        assert_eq!(
            count(&wire, Shape::UploadPart),
            9,
            "the first part, then the second part's eight tries, then nothing"
        );
        assert_eq!(count(&wire, Shape::Complete), 0);
        assert_eq!(store.pending_orphans(), 1);
        assert_eq!(store.abort_orphans().unwrap(), 1);
        assert_eq!(wire.inner().open_uploads(), 0);
    }

    // ------------------------------------------------ create-only writes

    #[test]
    fn a_create_only_put_into_a_taken_key_is_refused_and_changes_nothing() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        write_new(&h.store, &k, b"first", 64).unwrap();
        let err = write_new(&h.store, &k, b"second", 64).unwrap_err();
        assert_eq!(err, StoreError::AlreadyExists { key: k });
        assert_eq!(read_back(&h.store, &k, 5), b"first");
        assert_eq!(h.store.write_stats(), WriteStats { refused_taken: 1 });
    }

    #[test]
    fn a_create_only_multipart_into_a_taken_key_is_refused_at_the_complete() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        write_new(&h.store, &k, b"first", 64).unwrap();
        let err = write_new(&h.store, &k, &body(PART * 2), PART).unwrap_err();
        assert_eq!(err, StoreError::AlreadyExists { key: k });
        assert_eq!(read_back(&h.store, &k, 5), b"first");
        // The refused upload stays open, as Tigris leaves it; the next begin aborts it.
        assert_eq!(h.store.pending_orphans(), 1);
        assert_eq!(h.store.abort_orphans().unwrap(), 1);
        assert_eq!(h.wire.inner().open_uploads(), 0);
    }

    #[test]
    fn a_taken_key_reported_inside_a_200_is_refused_too() {
        let h = FakeHarness::new();
        h.wire.push(Fault::ErrorInOkBody {
            on: Shape::Complete,
            code: "PreconditionFailed",
        });
        let k = key(0, 1, 1);
        let err = write_new(&h.store, &k, &body(PART), PART).unwrap_err();
        assert_eq!(err, StoreError::AlreadyExists { key: k });
    }

    /// A provider without conditional writes answers the header with 501. The
    /// PUT fails with `CreateOnlyUnsupported`, nothing is published, and every
    /// later create-only write fails the same way: the store never drops the
    /// condition itself, so an object already there is never replaced by a
    /// create-only write (`docs/decisions.md` D79). A replace still goes.
    #[test]
    fn a_provider_without_conditional_writes_fails_every_create_only_write() {
        let h = FakeHarness::new();
        h.wire.inner().refuse_conditional_writes();
        let k = key(0, 1, 1);
        let refused = StoreError::CreateOnlyUnsupported {
            op: StoreOp::Upload,
            key: k,
        };
        assert_eq!(write_new(&h.store, &k, b"first", 64), Err(refused.clone()));
        assert!(list_all(&h.store).unwrap().files.is_empty());
        write_all(&h.store, &k, b"first", 64).unwrap();
        assert_eq!(write_new(&h.store, &k, b"other", 64), Err(refused));
        assert_eq!(read_back(&h.store, &k, 5), b"first", "never replaced");
        assert_eq!(h.store.write_stats(), WriteStats { refused_taken: 0 });
    }

    /// The same at a complete: nothing is published, the open upload goes on
    /// the orphan list, and no complete goes out without the condition.
    #[test]
    fn a_complete_refused_as_not_implemented_publishes_nothing() {
        let h = FakeHarness::new();
        h.wire.inner().refuse_conditional_writes();
        let k = key(0, 1, 1);
        let bytes = body(PART * 2);
        assert_eq!(
            write_new(&h.store, &k, &bytes, PART),
            Err(StoreError::CreateOnlyUnsupported {
                op: StoreOp::Upload,
                key: k,
            })
        );
        assert!(list_all(&h.store).unwrap().files.is_empty());
        assert_eq!(count(&h.wire, Shape::Complete), 1);
        assert_eq!(h.store.abort_orphans().unwrap(), 1);
        assert_eq!(h.wire.inner().open_uploads(), 0);
    }

    /// A complete whose answer was lost, sent again by a client or proxy,
    /// finds its id spent. The object a HEAD finds is this upload's: success,
    /// not a refusal that leaves the engine degraded.
    #[test]
    fn a_retried_complete_finding_its_upload_spent_is_settled_by_a_head() {
        let h = FakeHarness::new();
        h.wire.push(Fault::SpentAfterPublish {
            on: Shape::Complete,
        });
        let k = key(0, 1, 1);
        let bytes = body(PART * 2);
        write_new(&h.store, &k, &bytes, PART).unwrap();
        assert!(read_back(&h.store, &k, PART * 2) == bytes);
        assert_eq!(count(&h.wire, Shape::Head), 1);
    }

    /// The id is spent and the object under the key is not this upload's:
    /// neither a success nor a refusal, so the caller lists and reads back.
    #[test]
    fn a_spent_upload_with_another_object_under_the_key_is_ambiguous() {
        let h = FakeHarness::new();
        h.wire.push(Fault::Status {
            on: Shape::Complete,
            status: 404,
            code: Some("NoSuchUpload"),
            retry_after: None,
        });
        let k = key(0, 1, 1);
        let err = write_new(&h.store, &k, &body(PART * 2), PART).unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: io::ErrorKind::InvalidData
                }
            ),
            "{err:?}"
        );
    }

    /// A completed object's ETag is no MD5 of it, and its `<hex>-<parts>`
    /// form is a convention, not a promise: an ETag in another form is not
    /// taken for damage (the parts were each verified), so the object stays.
    #[test]
    fn a_completed_etag_in_another_form_is_not_taken_for_damage() {
        let h = FakeHarness::new();
        h.wire.push(Fault::WrongEtag {
            on: Shape::Complete,
        });
        let k = key(0, 1, 1);
        let bytes = body(PART * 2);
        write_all(&h.store, &k, &bytes, PART).unwrap();
        assert!(read_back(&h.store, &k, PART * 2) == bytes);
        assert_eq!(count(&h.wire, Shape::DeleteObjects), 0);
    }
}

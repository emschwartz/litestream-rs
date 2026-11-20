//! The store over FakeS3: the conformance suite (no Docker, in `just check`) and
//! the cases 8d could only state at the Transport level because they need the
//! store's five operations or `Retrying`.

#[cfg(test)]
mod common;
#[cfg(test)]
#[path = "common/rig.rs"]
mod rig;

// The helpers are test code only by cfg, which is what clippy.toml's allow-*-in-tests reads.
#[cfg(test)]
mod cases {
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use super::common::*;
    use super::rig::*;
    use lsr_core::conformance::{Harness, block_on};
    use lsr_core::{
        CorruptCause, FileKey, Level, ObjectStore, PartNumber, ReadLen, RecordingSleeper,
        RetryPolicy, Retrying, SeededEntropy, StoreError, StoreOp, Upload,
    };
    use lsr_ltx::Timestamp;
    use lsr_store_s3::{FakeS3, Fault, FaultTransport, S3Store, Shape};

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
    fn head_404_between_list_and_head() {
        let h = FakeHarness::new();
        for n in 1..=3 {
            put_through(&h.store, &key(0, n, n), b"x").unwrap();
        }
        h.wire.push(Fault::Status {
            on: Shape::Head,
            status: 404,
            code: None,
            retry_after: None,
        });
        let page = block_on(h.store.list(Level::ZERO, None, limit(2))).unwrap();
        let got: Vec<FileKey> = page.files.iter().map(|f| f.key).collect();
        assert_eq!(got, [key(0, 2, 2), key(0, 3, 3)]);
        assert_eq!(h.store.last_list_stats().vanished, 1);
    }

    #[test]
    fn wrong_etag_on_put_is_corrupt() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        wire.push(Fault::WrongEtag { on: Shape::Put });
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
        assert!(sleeper.waits().is_empty());
        assert!(wire.inner().keys().is_empty(), "the bad object was deleted");
    }

    #[test]
    fn five_hundred_after_publish_leaves_object_visible_with_stamp() {
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
        assert_eq!(file.created_at, Timestamp::from_unix_millis(5));
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
                .begin_upload(k, Timestamp::from_unix_millis(5))
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
    fn multipart_round_trip_keeps_bytes_and_stamp() {
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
        assert_eq!(file.created_at, Timestamp::from_unix_millis(5));
        assert_eq!(h.wire.inner().open_uploads(), 0);
        assert_eq!(h.store.pending_orphans(), 0);
    }

    #[test]
    fn wrong_part_etag_is_corrupt_and_not_retried() {
        let (wire, sleeper, store) = retried(FakeHarness::new());
        let k = key(0, 1, 1);
        wire.push(Fault::Pass {
            on: Shape::UploadPart,
        });
        wire.push(Fault::WrongEtag {
            on: Shape::UploadPart,
        });
        let err = write_all(&store, &k, &body(PART * 3), PART).unwrap_err();
        assert_eq!(
            err,
            StoreError::Corrupt {
                key: Some(k),
                cause: CorruptCause::ChecksumMismatch { part: part(2) }
            }
        );
        assert_eq!(count(&wire, Shape::UploadPart), 2, "the third never goes");
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
        h.wire.push(Fault::Status {
            on: Shape::UploadPart,
            status: 500,
            code: Some("InternalError"),
            retry_after: None,
        });
        let err = write_all(&h.store, &key(0, 1, 1), &body(PART), PART).unwrap_err();
        assert!(matches!(err, StoreError::Transient { .. }), "{err:?}");
        assert_eq!(h.store.pending_orphans(), 1);
        assert_eq!(h.wire.inner().open_uploads(), 1);
        block_on(
            h.store
                .begin_upload(&key(0, 2, 2), Timestamp::from_unix_millis(5)),
        )
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
        assert_eq!(file.created_at, Timestamp::from_unix_millis(5));
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
                .begin_upload(&key(0, 1, 1), Timestamp::from_unix_millis(5))
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

    #[test]
    fn abort_removes_the_upload() {
        let h = FakeHarness::new();
        block_on(async {
            let mut up = h
                .store
                .begin_upload(&key(0, 1, 1), Timestamp::from_unix_millis(5))
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
                .begin_upload(&key(0, 1, 1), Timestamp::from_unix_millis(5))
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

    #[test]
    fn memory_is_one_part() {
        let h = FakeHarness::new();
        let k = key(0, 1, 1);
        let bytes = body(PART * 4);
        block_on(async {
            let mut up = h
                .store
                .begin_upload(&k, Timestamp::from_unix_millis(5))
                .await
                .unwrap();
            let mut high = 0;
            for piece in bytes.chunks(1_500_003) {
                up.write(piece).await.unwrap();
                high = high.max(up.buffer_capacity());
                assert!(up.buffer_capacity() <= PART);
            }
            assert!(high > 0);
            up.commit().await.unwrap();
        });
        assert_eq!(count(&h.wire, Shape::UploadPart), 4);
        assert!(read_back(&h.store, &k, PART * 2)[..] == bytes[..PART * 2]);
    }
}

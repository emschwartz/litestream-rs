//! The store over FakeS3: the conformance suite (no Docker, in `just check`) and
//! the cases 8d could only state at the Transport level because they need the
//! store's five operations or `Retrying`.

#[cfg(test)]
mod common;

// The helpers are test code only by cfg, which is what clippy.toml's allow-*-in-tests reads.
#[cfg(test)]
mod cases {
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use super::common::*;
    use lsr_core::conformance::{Failure, Harness, block_on};
    use lsr_core::{
        FakeClock, FileKey, Level, ObjectStore, PageLimit, ReadLen, RecordingSleeper, RetryPolicy,
        Retrying, SeededEntropy, StoreError, StoreOp, Upload,
    };
    use lsr_ltx::Timestamp;
    use lsr_store_s3::{
        FakeS3, Fault, FaultTransport, HttpRequest, HttpResponse, S3Store, Shape, Transport,
        TransportError, level_prefix,
    };

    /// The fault layer over the fake, shared between the store that owns a
    /// transport and the test that scripts it.
    struct Shared(Arc<FaultTransport<FakeS3>>);

    impl Transport for Shared {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            self.0.send(request)
        }
    }

    struct FakeHarness {
        wire: Arc<FaultTransport<FakeS3>>,
        store: S3Store<Shared>,
    }

    impl FakeHarness {
        fn new() -> Self {
            let wire = Arc::new(FaultTransport::new(fake()));
            let store = S3Store::with_transport(
                config(),
                credentials(),
                Shared(Arc::clone(&wire)),
                FakeClock::new(now()),
            )
            .unwrap();
            Self { wire, store }
        }
    }

    impl Harness for FakeHarness {
        type Store = S3Store<Shared>;

        fn store(&self) -> &Self::Store {
            &self.store
        }

        fn plant_foreign(&self, level: Level, name: &str) -> Result<(), Failure> {
            let object = format!("{}{name}", level_prefix(config().prefix(), level));
            self.wire.inner().put_raw(&object, b"stray".to_vec(), &[]);
            Ok(())
        }

        fn fail_next_commit_after_publish(&self) -> bool {
            self.wire.push(Fault::AfterPublish {
                on: Shape::Put,
                status: 500,
            });
            true
        }
    }

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

    fn count(wire: &FaultTransport<FakeS3>, shape: Shape) -> usize {
        wire.requests()
            .iter()
            .filter(|r| r.shape == Some(shape))
            .count()
    }

    fn put_through(store: &impl ObjectStore, key: &FileKey, body: &[u8]) -> Result<(), StoreError> {
        block_on(async {
            let mut up = store
                .begin_upload(key, Timestamp::from_unix_millis(5))
                .await?;
            up.write(body).await?;
            up.commit().await.map(drop)
        })
    }

    fn list_all(store: &impl ObjectStore) -> Result<lsr_core::Listing, StoreError> {
        block_on(store.list(Level::ZERO, None, PageLimit::MAX))
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
}

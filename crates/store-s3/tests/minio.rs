//! The S3 store against a real MinIO (`just test-s3`).
//!
//! Every test needs `LSR_MINIO_ENDPOINT` (`http://127.0.0.1:9000`, set by the
//! recipe) and skips with a message when it is unset, so `just check` never
//! needs Docker. The server, its pinned image and its throwaway credentials
//! live in `scripts/minio.sh`; the credential defaults below repeat that script
//! and can be overridden with `LSR_MINIO_ACCESS_KEY` and `LSR_MINIO_SECRET_KEY`.
//! Each test works under its own prefix in the shared bucket, so tests run in
//! parallel and a rerun never sees an earlier run's objects.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod minio {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use lsr_core::conformance::{Failure, Harness, block_on};
    use lsr_core::{
        Clock, CorruptCause, FileKey, Level, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp,
        Upload,
    };
    use lsr_ltx::{Timestamp, Txid, TxidRange};
    use lsr_store_s3::{
        BucketName, Endpoint, HttpRequest, HttpResponse, Prefix, RequestBuilder, S3Code, S3Config,
        S3Credentials, S3Store, SecretString, Transport, TransportError, UploadId, UreqTransport,
        classify, level_prefix, object_key,
    };
    use rusty_s3::S3Action;

    const BUCKET: &str = "lsr-test";
    const STAMP_HEADER: &str = "x-amz-meta-litestream-timestamp";
    const SKIP: &str = "skipped: set LSR_MINIO_ENDPOINT (just minio-up)";

    struct WallClock;

    impl Clock for WallClock {
        fn now(&self) -> Timestamp {
            let millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0));
            Timestamp::from_unix_millis(millis)
        }
    }

    /// Counts what the store sends, by kind.
    #[derive(Default)]
    struct Counts {
        lists: AtomicU64,
        heads: AtomicU64,
    }

    struct Counting {
        inner: UreqTransport,
        counts: std::sync::Arc<Counts>,
    }

    impl Transport for Counting {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            let is_list = request
                .url()
                .query()
                .is_some_and(|q| q.contains("list-type=2"));
            let counter = if is_list {
                &self.counts.lists
            } else if request.method() == lsr_store_s3::Method::Head {
                &self.counts.heads
            } else {
                return self.inner.send(request);
            };
            counter.fetch_add(1, Ordering::Relaxed);
            self.inner.send(request)
        }
    }

    struct MinioHarness {
        store: S3Store<Counting>,
        counts: std::sync::Arc<Counts>,
        endpoint: String,
        prefix: Prefix,
        access: String,
        secret: String,
    }

    fn unique_prefix() -> Prefix {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let text = format!(
            "t{nanos}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        Prefix::new(&text).unwrap()
    }

    impl MinioHarness {
        fn connect() -> Option<Self> {
            let Ok(endpoint) = std::env::var("LSR_MINIO_ENDPOINT") else {
                eprintln!("{SKIP}");
                return None;
            };
            let access =
                std::env::var("LSR_MINIO_ACCESS_KEY").unwrap_or_else(|_| "lsrtestroot".into());
            let secret = std::env::var("LSR_MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "lsrtest-throwaway-0000".into());
            let prefix = unique_prefix();
            let config = S3Config::new(
                Endpoint::new(&endpoint).unwrap(),
                BucketName::new(BUCKET).unwrap(),
                prefix.clone(),
            );
            let counts = std::sync::Arc::new(Counts::default());
            let transport = Counting {
                inner: UreqTransport::new(config.deadline()),
                counts: std::sync::Arc::clone(&counts),
            };
            let credentials = S3Credentials::new(access.clone(), SecretString::new(secret.clone()));
            let store = S3Store::with_transport(config, credentials, transport, WallClock).unwrap();
            Some(Self {
                store,
                counts,
                endpoint,
                prefix,
                access,
                secret,
            })
        }

        /// A PUT that bypasses the store: any key, any headers, so a test can
        /// plant what only another writer would leave.
        fn raw_put(&self, object: &str, body: &[u8], headers: &[(&str, &str)]) {
            let bucket = rusty_s3::Bucket::new(
                url::Url::parse(&self.endpoint).unwrap(),
                rusty_s3::UrlStyle::Path,
                BUCKET,
                "us-east-1",
            )
            .unwrap();
            let credentials = rusty_s3::Credentials::new(&self.access, &self.secret);
            let mut action = bucket.put_object(Some(&credentials), object);
            for (name, value) in headers {
                action.headers_mut().insert(*name, *value);
            }
            let url = action.sign_with_time(Duration::from_secs(60), &jiff::Timestamp::now());
            let mut request = ureq::put(url.as_str());
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            request.send(body).unwrap();
        }

        fn key(&self, level: u8, min: u64, max: u64) -> FileKey {
            let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
            FileKey::new(Level::new(level).unwrap(), range)
        }

        fn put(&self, key: &FileKey, stamp: Timestamp, bytes: &[u8]) {
            block_on(async {
                let mut upload = self.store.begin_upload(key, stamp).await.unwrap();
                upload.write(bytes).await.unwrap();
                upload.commit().await.unwrap();
            });
        }

        fn list_all(&self, level: u8, limit: u16) -> lsr_core::Listing {
            let level = Level::new(level).unwrap();
            block_on(self.store.list(level, None, PageLimit::new(limit).unwrap())).unwrap()
        }
    }

    impl Harness for MinioHarness {
        type Store = S3Store<Counting>;

        fn store(&self) -> &Self::Store {
            &self.store
        }

        fn plant_foreign(&self, level: Level, name: &str) -> Result<(), Failure> {
            let object = format!("{}{name}", level_prefix(&self.prefix, level));
            self.raw_put(&object, b"stray", &[]);
            Ok(())
        }
    }

    /// One test per conformance case, each skipping when no server is configured.
    /// The list is checked against core's by `conformance_list_matches_core`.
    macro_rules! minio_cases {
    ($($case:ident),+ $(,)?) => {
        mod conformance_cases {
            use super::*;
            pub const NAMES: &[&str] = &[$(stringify!($case)),+];
            $(
                #[test]
                fn $case() {
                    let Some(harness) = MinioHarness::connect() else { return };
                    block_on(lsr_core::conformance::$case(&harness)).unwrap();
                }
            )+
        }
    };
}

    minio_cases!(
        list_empty_level,
        paging_at_every_limit,
        cursor_is_exclusive,
        levels_are_separate,
        foreign_names_are_skipped_and_counted,
        scratch_names_are_not_foreign,
        commit_error_may_leave_the_object_visible,
        abort_publishes_nothing,
        pending_and_dropped_uploads_are_invisible,
        read_spans_the_end,
        read_of_missing_key_is_not_found,
        delete_is_idempotent,
        two_uploads_to_one_key,
        chunks_concatenate_and_stamp_survives,
        stamp_returns_the_committed_stamp,
        listed_stamp_is_exact_or_marked,
        stamp_of_a_vanished_file_is_not_found,
        stamp_of_an_overwritten_file_is_refused,
        read_with_stamp_agrees_with_read_and_stamp,
    );

    /// A case added to core's suite and missing from the list above would silently
    /// not run on MinIO.
    #[test]
    fn conformance_list_matches_core() {
        let source = include_str!("../../core/src/conformance.rs");
        let listed = source
            .split("@each $make:expr")
            .next()
            .and_then(|head| head.rsplit("conformance_suite!(@each $make;").next())
            .unwrap();
        let mut core: Vec<&str> = listed
            .split(|c: char| c == ',' || c.is_whitespace() || c == ')' || c == ';')
            .filter(|word| {
                !word.is_empty() && word.chars().all(|c| c.is_ascii_lowercase() || c == '_')
            })
            .collect();
        core.sort_unstable();
        let mut ours: Vec<&str> = conformance_cases::NAMES.to_vec();
        ours.sort_unstable();
        assert_eq!(ours, core);
    }

    fn at(text: &str) -> Timestamp {
        Timestamp::parse_rfc3339(text).unwrap()
    }

    #[test]
    fn stamp_survives_round_trip_on_minio() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        let stamps = [
            Timestamp::from_unix_millis(0),
            Timestamp::from_unix_millis(1),
            at("2024-02-29T12:34:56.789Z"),
            at("9999-12-31T23:59:59.999Z"),
        ];
        for (index, stamp) in stamps.iter().enumerate() {
            let n = u64::try_from(index).unwrap() + 1;
            h.put(&h.key(0, n, n), *stamp, b"x");
        }
        let listing = h.list_all(0, 10);
        let got: Vec<Timestamp> = listing
            .files
            .iter()
            .map(|f| block_on(h.store.stamp(f)).unwrap().at())
            .collect();
        assert_eq!(got, stamps);
    }

    #[test]
    fn go_stamp_parses() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        let key = h.key(0, 5, 5);
        h.raw_put(
            &object_key(&h.prefix, &key),
            b"go wrote this",
            &[(STAMP_HEADER, "2025-01-02T03:04:05.123456789Z")],
        );
        let listing = h.list_all(0, 10);
        let stamps: Vec<Timestamp> = listing
            .files
            .iter()
            .map(|f| block_on(h.store.stamp(f)).unwrap().at())
            .collect();
        assert_eq!(stamps, [at("2025-01-02T03:04:05.123Z")]);
        assert_eq!(h.store.stamp_stats().stamped, 1);
    }

    #[test]
    fn object_without_metadata_falls_back() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        let key = h.key(0, 7, 7);
        h.raw_put(&object_key(&h.prefix, &key), b"no stamp", &[]);
        let listing = h.list_all(0, 10);
        let [file] = listing.files.as_slice() else {
            panic!("one file expected, got {listing:?}");
        };
        let now = WallClock.now().unix_millis();
        assert!((now - block_on(h.store.stamp(file)).unwrap().at().unix_millis()).abs() < 600_000);
        let stats = h.store.stamp_stats();
        assert_eq!((stats.stamped, stats.fell_back), (0, 1));
    }

    #[test]
    fn unparseable_stamp_fails_the_reader_not_the_listing() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        let key = h.key(0, 9, 9);
        h.raw_put(
            &object_key(&h.prefix, &key),
            b"bad",
            &[(STAMP_HEADER, "yesterday")],
        );
        let listing = h.list_all(0, 10);
        let [file] = listing.files.as_slice() else {
            panic!("the listing keeps the file: {listing:?}");
        };
        assert_eq!(
            block_on(h.store.stamp(file)),
            Err(StoreError::Corrupt {
                key: Some(key),
                cause: CorruptCause::BadStamp
            })
        );
    }

    #[test]
    fn listing_makes_no_head_requests() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        for n in 1..=2500_u64 {
            h.put(&h.key(0, n, n), Timestamp::from_unix_millis(1), b"x");
        }
        h.counts.lists.store(0, Ordering::Relaxed);
        h.counts.heads.store(0, Ordering::Relaxed);
        let page = h.list_all(0, 1000);
        assert_eq!(page.files.len(), 1000);
        assert!(page.next.is_some());
        assert!(h.counts.lists.load(Ordering::Relaxed) <= 2);
        assert_eq!(h.counts.heads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn wrong_content_md5_is_refused_by_minio() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        let key = h.key(0, 3, 3);
        let config = S3Config::new(
            Endpoint::new(&h.endpoint).unwrap(),
            BucketName::new(BUCKET).unwrap(),
            h.prefix.clone(),
        );
        let credentials = S3Credentials::new(h.access.clone(), SecretString::new(h.secret.clone()));
        let builder = RequestBuilder::new(&config, &credentials).unwrap();
        // The signed digest is of "hello"; the body that goes out is not.
        let request = builder
            .put_object(
                WallClock.now(),
                &key,
                b"hello".to_vec(),
                Timestamp::from_unix_millis(1),
            )
            .unwrap()
            .tamper_body(b"other".to_vec());
        let judged = UreqTransport::new(config.deadline()).send(request).unwrap();
        assert_eq!(
            classify(StoreOp::Upload, Some(&key), None, &judged),
            Err(StoreError::Corrupt {
                key: Some(key),
                cause: CorruptCause::ChecksumMismatch { part: None }
            })
        );
        // Nothing was stored under the key.
        let read = block_on(h.store.read(&key, 0, ReadLen::new(16).unwrap()));
        assert_eq!(read, Err(StoreError::NotFound { key }));
    }
    const PART: usize = 5 * 1024 * 1024;

    fn body(len: usize) -> Vec<u8> {
        (0..=250).cycle().take(len).collect()
    }

    impl MinioHarness {
        fn write_all(&self, key: &FileKey, stamp: Timestamp, bytes: &[u8], chunk: usize) {
            block_on(async {
                let mut upload = self.store.begin_upload(key, stamp).await.unwrap();
                for piece in bytes.chunks(chunk) {
                    upload.write(piece).await.unwrap();
                }
                upload.commit().await.unwrap();
            });
        }

        fn read_back(&self, key: &FileKey, len: usize) -> Vec<u8> {
            let want = u32::try_from(len).unwrap();
            block_on(self.store.read(key, 0, ReadLen::new(want).unwrap())).unwrap()
        }

        /// ListMultipartUploads under this test's prefix, which no store call makes.
        fn open_uploads(&self) -> usize {
            // rusty-s3 has no ListMultipartUploads action, so the query is signed by hand.
            // MinIO ignores `prefix` here, so the listing is bucket-wide and the
            // count keeps only this test's keys.
            let base = url::Url::parse(&format!("{}/{BUCKET}/", self.endpoint)).unwrap();
            let mine = format!("<Key>{}/", self.prefix.as_str());
            let query = [("uploads", "")];
            let url = rusty_s3::signing::sign(
                &jiff::Timestamp::now(),
                rusty_s3::Method::Get,
                base,
                &self.access,
                &self.secret,
                None,
                "us-east-1",
                60,
                query.into_iter(),
                std::iter::empty(),
            );
            let mut response = ureq::get(url.as_str())
                .config()
                .http_status_as_error(false)
                .build()
                .call()
                .unwrap();
            let status = response.status().as_u16();
            let text = response.body_mut().read_to_string().unwrap();
            assert_eq!(status, 200, "{text}");
            text.matches(mine.as_str()).count()
        }
    }

    #[test]
    fn multipart_round_trip_on_minio() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        let key = h.key(0, 1, 1);
        let len = PART * 5 / 2;
        let bytes = body(len);
        h.write_all(&key, at("2024-05-01T12:00:00.123Z"), &bytes, 1_500_003);
        assert!(h.read_back(&key, len) == bytes);
        let listed = h.list_all(0, 10);
        let [file] = listed.files.as_slice() else {
            panic!("one file expected: {listed:?}");
        };
        assert_eq!(file.size, u64::try_from(len).unwrap());
        assert_eq!(
            block_on(h.store.stamp(file)).unwrap().at(),
            at("2024-05-01T12:00:00.123Z")
        );
        assert_eq!(h.open_uploads(), 0);
        assert_eq!(h.store.pending_orphans(), 0);
    }

    #[test]
    fn part_size_boundary_on_minio() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        for (n, len) in [(1, PART), (2, PART - 1), (3, PART + 1), (4, PART * 2)] {
            let key = h.key(0, n, n);
            let bytes = body(len);
            h.write_all(&key, Timestamp::from_unix_millis(9), &bytes, PART);
            assert!(h.read_back(&key, len) == bytes, "length {len}");
        }
        assert_eq!(h.open_uploads(), 0);
    }

    #[test]
    fn dropped_multipart_is_invisible_then_aborted_on_minio() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        block_on(async {
            let mut upload = h
                .store
                .begin_upload(&h.key(0, 1, 1), Timestamp::from_unix_millis(9))
                .await
                .unwrap();
            upload.write(&body(PART)).await.unwrap();
        });
        assert!(h.list_all(0, 10).files.is_empty());
        assert_eq!(h.open_uploads(), 1);
        assert_eq!(h.store.pending_orphans(), 1);
        assert_eq!(h.store.abort_orphans().unwrap(), 1);
        assert_eq!(h.open_uploads(), 0);
    }

    #[test]
    fn abort_on_minio_leaves_nothing() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        block_on(async {
            let mut upload = h
                .store
                .begin_upload(&h.key(0, 1, 1), Timestamp::from_unix_millis(9))
                .await
                .unwrap();
            upload.write(&body(PART)).await.unwrap();
            upload.abort().await.unwrap();
        });
        assert_eq!(h.open_uploads(), 0);
        assert!(h.list_all(0, 10).files.is_empty());
    }

    #[test]
    fn abort_of_an_unknown_upload_is_not_a_failure_on_minio() {
        let Some(h) = MinioHarness::connect() else {
            return;
        };
        let key = h.key(0, 1, 1);
        let config = S3Config::new(
            Endpoint::new(&h.endpoint).unwrap(),
            BucketName::new(BUCKET).unwrap(),
            h.prefix.clone(),
        );
        let credentials = S3Credentials::new(h.access.clone(), SecretString::new(h.secret.clone()));
        let builder = RequestBuilder::new(&config, &credentials).unwrap();
        let request = builder
            .abort_multipart(
                WallClock.now(),
                &key,
                &UploadId::new("no-such-id".to_owned()),
            )
            .unwrap();
        let response = UreqTransport::new(config.deadline()).send(request).unwrap();
        // MinIO answers 204 where S3 answers 404 NoSuchUpload; the store treats both as aborted.
        let no_such = response.status() == 404
            && S3Code::from_body(response.body()) == Some(S3Code::NoSuchUpload);
        assert!(response.status() == 204 || no_such, "{}", response.status());
        assert_eq!(
            classify(StoreOp::Upload, Some(&key), None, &response),
            Ok(())
        );
    }
}

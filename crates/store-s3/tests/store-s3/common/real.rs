//! What minio.rs and tigris.rs share: a store over a real server, raw signed
//! requests for what the store never sends (signed by the store's own
//! `RequestBuilder::raw`, behind the `testing` feature), a redacting dump, and the case
//! bodies that hold on every provider. Each including file declares it with
//! `#[cfg(test)] #[path = "common/real.rs"] mod real;`.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use lsr_core::conformance::{Failure, Harness, block_on};
use lsr_core::{
    Clock, CorruptCause, FileKey, Level, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp,
    Upload,
};
use lsr_ltx::{Timestamp, Txid, TxidRange};
use lsr_store_s3::{
    BucketName, Endpoint, HttpRequest, HttpResponse, Prefix, Region, RequestBuilder, S3Code,
    S3Config, S3Credentials, S3Store, SecretString, Transport, TransportError, UploadId,
    UreqTransport, classify, level_prefix, object_key,
};

pub const CREATED_AT_HEADER: &str = "x-amz-meta-litestream-timestamp";
pub const PART: usize = 5 * 1024 * 1024;

/// Tigris starts a multipart upload at 16 MiB, so a 5 MiB body there is one PUT and proves nothing.
pub const TIGRIS_PART: usize = 16 * 1024 * 1024;

/// The body size at which this provider's store goes multipart.
pub fn part(h: &RealHarness) -> usize {
    if h.server.name == "tigris" {
        TIGRIS_PART
    } else {
        PART
    }
}

/// The store's own wall clock: these suites sign against a real server's clock.
pub use lsr_store_s3::SystemClock as WallClock;

/// One server and its throwaway credentials. No Debug: the secret must not print.
#[derive(Clone)]
pub struct Server {
    pub name: &'static str,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access: String,
    pub secret: String,
    /// Every test prefix sits under this one (the Tigris run's own prefix).
    pub root: Option<String>,
}

/// Removes what must never reach a log or a document: the key pair, signatures,
/// credentials in a URL and Authorization headers.
pub fn redact(server: &Server, text: &str) -> String {
    let mut out = text.to_owned();
    for secret in [&server.access, &server.secret] {
        if !secret.is_empty() {
            out = out.replace(secret.as_str(), "<redacted>");
        }
    }
    for marker in [
        "X-Amz-Signature=",
        "X-Amz-Credential=",
        "X-Amz-Security-Token=",
        "Authorization:",
    ] {
        let mut pieces = out.split(marker);
        let mut kept = pieces.next().unwrap_or("").to_owned();
        for piece in pieces {
            kept.push_str(marker);
            kept.push_str("<redacted>");
            let end = piece
                .find(['&', '"', '\n', ' ', '<'])
                .unwrap_or(piece.len());
            kept.push_str(piece.split_at(end).1);
        }
        out = kept;
    }
    out
}

/// A response to a raw request.
pub struct Raw {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Raw {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(have, _)| have.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// The text between <tag> and </tag>, first occurrence.
    pub fn tag(&self, tag: &str) -> Option<String> {
        let text = self.text();
        let after = text.split_once(&format!("<{tag}>"))?.1;
        Some(after.split_once(&format!("</{tag}>"))?.0.to_owned())
    }
}

/// What the store sent and saw, by kind.
#[derive(Default)]
pub struct Counts {
    pub lists: AtomicU64,
    pub heads: AtomicU64,
    pub requests: AtomicU64,
    pub header_names: Mutex<BTreeSet<String>>,
    pub statuses: Mutex<BTreeSet<u16>>,
}

pub struct Counting {
    inner: Box<dyn Transport>,
    counts: Arc<Counts>,
}

impl Counting {
    pub fn new(inner: Box<dyn Transport>, counts: Arc<Counts>) -> Self {
        Self { inner, counts }
    }
}

/// The store's client: ureq, or reqwest when `LSR_S3_TRANSPORT=reqwest` (needs
/// the `reqwest` feature), so the same suite runs over both.
fn store_transport(deadline: std::time::Duration) -> Box<dyn Transport> {
    match std::env::var("LSR_S3_TRANSPORT").as_deref() {
        #[cfg(feature = "reqwest")]
        Ok("reqwest") => Box::new(lsr_store_s3::ReqwestTransport::new(deadline).unwrap()),
        Ok("ureq") | Err(_) => Box::new(UreqTransport::new(deadline)),
        Ok(other) => panic!("LSR_S3_TRANSPORT={other}: ureq, or reqwest with the feature"),
    }
}

impl Transport for Counting {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let is_list = request
            .url()
            .query()
            .is_some_and(|q| q.contains("list-type=2"));
        if is_list {
            self.counts.lists.fetch_add(1, Ordering::Relaxed);
        } else if request.method() == lsr_store_s3::Method::Head {
            self.counts.heads.fetch_add(1, Ordering::Relaxed);
        }
        self.counts.requests.fetch_add(1, Ordering::Relaxed);
        let response = self.inner.send(request)?;
        self.counts.note(&response);
        Ok(response)
    }

    /// Batches run concurrently against the real server, each request counted.
    fn send_many(
        &self,
        requests: Vec<HttpRequest>,
        limit: std::num::NonZeroUsize,
    ) -> Vec<Result<HttpResponse, TransportError>> {
        lsr_store_s3::send_on_threads(self, requests, limit)
    }
}

impl Counts {
    fn note(&self, response: &HttpResponse) {
        self.statuses.lock().unwrap().insert(response.status());
    }

    fn note_raw(&self, raw: &Raw) {
        self.statuses.lock().unwrap().insert(raw.status);
        let mut names = self.header_names.lock().unwrap();
        for (name, _) in &raw.headers {
            names.insert(name.to_ascii_lowercase());
        }
    }
}

pub struct RealHarness {
    pub store: S3Store<Counting>,
    pub counts: Arc<Counts>,
    pub server: Server,
    pub config: S3Config,
    pub prefix: Prefix,
}

#[expect(
    clippy::disallowed_methods,
    reason = "a run prefix unique across runs against a real bucket"
)]
fn unique_prefix(server: &Server) -> Prefix {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let leaf = format!(
        "t{nanos}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let text = match &server.root {
        Some(root) => format!("{root}/{leaf}"),
        None => leaf,
    };
    Prefix::new(&text).unwrap()
}

impl Drop for RealHarness {
    fn drop(&mut self) {
        let statuses = self.counts.statuses.lock().unwrap().clone();
        eprintln!(
            "{}-requests: total={} lists={} heads={} statuses={statuses:?}",
            self.server.name,
            self.counts.requests.load(Ordering::Relaxed),
            self.counts.lists.load(Ordering::Relaxed),
            self.counts.heads.load(Ordering::Relaxed),
        );
    }
}

impl RealHarness {
    pub fn connect(server: &Server) -> Self {
        let prefix = unique_prefix(server);
        let config = S3Config::new(
            Endpoint::new(&server.endpoint).unwrap(),
            BucketName::new(&server.bucket).unwrap(),
            prefix.clone(),
        )
        .with_region(Region::new(&server.region).unwrap());
        let counts = Arc::new(Counts::default());
        eprintln!(
            "{}-transport: {}",
            server.name,
            std::env::var("LSR_S3_TRANSPORT").unwrap_or_else(|_| "ureq".into())
        );
        let transport = Counting::new(store_transport(config.deadline()), Arc::clone(&counts));
        let store = S3Store::with_transport(
            config.clone(),
            self_credentials(server),
            transport,
            WallClock,
        )
        .unwrap();
        Self {
            store,
            counts,
            server: server.clone(),
            config,
            prefix,
        }
    }

    pub fn builder(&self) -> RequestBuilder {
        RequestBuilder::new(&self.config, &self_credentials(&self.server)).unwrap()
    }

    /// A request built by the store's builder, sent through a plain client, counted.
    pub fn send(&self, request: impl Into<HttpRequest>) -> HttpResponse {
        let request = request.into();
        let transport = Counting::new(
            store_transport(self.config.deadline()),
            Arc::clone(&self.counts),
        );
        transport.send(request).unwrap()
    }

    /// A signed request the builder cannot make: any method, query, headers, body, signed
    /// by the store's own raw builder (`RequestBuilder::raw`) and sent through a plain client.
    /// `object` is a full key; None addresses the bucket.
    pub fn raw(
        &self,
        method: &str,
        object: Option<&str>,
        query: &[(&str, &str)],
        headers: &[(&'static str, &str)],
        body: &[u8],
    ) -> Raw {
        let verb = match method {
            "GET" => lsr_store_s3::Method::Get,
            "HEAD" => lsr_store_s3::Method::Head,
            "PUT" => lsr_store_s3::Method::Put,
            "POST" => lsr_store_s3::Method::Post,
            "DELETE" => lsr_store_s3::Method::Delete,
            other => panic!("method {other}"),
        };
        let headers = headers.iter().map(|(n, v)| (*n, (*v).to_owned())).collect();
        let request = self
            .builder()
            .raw(WallClock.now(), verb, object, query, headers, body.to_vec())
            .unwrap();
        let response = store_transport(self.config.deadline())
            .send(request)
            .unwrap_or_else(|err| {
                panic!(
                    "{method} failed: {}",
                    redact(&self.server, &err.to_string())
                )
            });
        let raw = Raw {
            status: response.status(),
            headers: response.headers().to_vec(),
            body: response.body().to_vec(),
        };
        self.counts.requests.fetch_add(1, Ordering::Relaxed);
        self.counts.note_raw(&raw);
        raw
    }

    /// A PUT that bypasses the store: any key, any headers.
    pub fn raw_put(&self, object: &str, body: &[u8], headers: &[(&'static str, &str)]) {
        let raw = self.raw("PUT", Some(object), &[], headers, body);
        assert_eq!(raw.status, 200, "{}", self.dump(&raw));
    }

    /// Status, error code, the interesting headers and the start of the body, redacted.
    pub fn dump(&self, raw: &Raw) -> String {
        let mut text = format!("status={}", raw.status);
        if let Some(code) = raw.tag("Code") {
            text.push_str(&format!(" code={code}"));
        }
        for name in [
            "etag",
            "content-length",
            "content-range",
            "last-modified",
            "date",
            "retry-after",
            "server",
            "x-amz-request-id",
            "x-amz-version-id",
            "x-amz-meta-litestream-timestamp",
            "x-tigris-consistent",
            "x-tigris-region",
            "x-amz-checksum-sha256",
            "x-amz-storage-class",
        ] {
            if let Some(value) = raw.header(name) {
                text.push_str(&format!(" {name}={value}"));
            }
        }
        let body: String = raw.text().chars().take(400).collect();
        if !body.is_empty() && raw.status >= 300 {
            text.push_str(&format!(" body={body}"));
        }
        redact(&self.server, &text)
    }

    pub fn key(&self, level: u8, min: u64, max: u64) -> FileKey {
        let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
        FileKey::new(Level::new(level).unwrap(), range)
    }

    pub fn object(&self, key: &FileKey) -> String {
        object_key(&self.prefix, key)
    }

    pub fn put(&self, key: &FileKey, created_at: Timestamp, bytes: &[u8]) {
        block_on(async {
            let mut upload = self.store.begin_upload(key, created_at).await.unwrap();
            upload.write(bytes).await.unwrap();
            upload.commit().await.unwrap();
        });
    }

    pub fn list_all(&self, level: u8, limit: u16) -> lsr_core::Listing {
        let level = Level::new(level).unwrap();
        block_on(self.store.list(level, None, PageLimit::new(limit).unwrap())).unwrap()
    }

    pub fn write_all(&self, key: &FileKey, created_at: Timestamp, bytes: &[u8], chunk: usize) {
        block_on(async {
            let mut upload = self.store.begin_upload(key, created_at).await.unwrap();
            for piece in bytes.chunks(chunk) {
                upload.write(piece).await.unwrap();
            }
            upload.commit().await.unwrap();
        });
    }

    pub fn read_back(&self, key: &FileKey, len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        while out.len() < len {
            let want = u32::try_from(len.saturating_sub(out.len()).min(1 << 20)).unwrap();
            let offset = u64::try_from(out.len()).unwrap();
            let chunk =
                block_on(self.store.read(key, offset, ReadLen::new(want).unwrap())).unwrap();
            assert!(!chunk.is_empty(), "short read at {offset}");
            out.extend_from_slice(&chunk);
        }
        out
    }

    /// ListMultipartUploads under this test's prefix, which no store call makes.
    /// MinIO ignores the prefix parameter, so the count keeps only this test's keys.
    pub fn open_uploads(&self) -> usize {
        let raw = self.raw("GET", None, &[("uploads", "")], &[], b"");
        assert_eq!(raw.status, 200, "{}", self.dump(&raw));
        let mine = format!("<Key>{}/", self.prefix.as_str());
        raw.text().matches(mine.as_str()).count()
    }
}

fn self_credentials(server: &Server) -> S3Credentials {
    S3Credentials::new(
        server.access.clone(),
        SecretString::new(server.secret.clone()),
    )
}

impl Harness for RealHarness {
    type Store = S3Store<Counting>;

    fn store(&self) -> &Self::Store {
        &self.store
    }

    fn plant_unrecognized(&self, level: Level, name: &str) -> Result<(), Failure> {
        let object = format!("{}{name}", level_prefix(&self.prefix, level));
        self.raw_put(&object, b"stray", &[]);
        Ok(())
    }
}

pub fn at(text: &str) -> Timestamp {
    Timestamp::parse_rfc3339(text).unwrap()
}

pub fn body(len: usize) -> Vec<u8> {
    (0..=250).cycle().take(len).collect()
}

/// The conformance case names core's suite lists, to check a provider's list against.
pub fn conformance_names_in_core() -> Vec<&'static str> {
    let source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../core/src/conformance.rs"
    ));
    let listed = source
        .split("@each $make:expr")
        .next()
        .and_then(|head| head.rsplit("conformance_suite!(@each $make;").next())
        .unwrap();
    let mut names: Vec<&str> = listed
        .split(|c: char| c == ',' || c.is_whitespace() || c == ')' || c == ';')
        .filter(|word| !word.is_empty() && word.chars().all(|c| c.is_ascii_lowercase() || c == '_'))
        .collect();
    names.sort_unstable();
    names
}

// The cases below hold on every provider; each provider's file runs them.

pub fn created_at_survives_round_trip(h: &RealHarness) {
    let resolved = [
        Timestamp::from_unix_millis(0),
        Timestamp::from_unix_millis(1),
        at("2024-02-29T12:34:56.789Z"),
        at("9999-12-31T23:59:59.999Z"),
    ];
    for (n, created_at) in (1_u64..).zip(resolved.iter()) {
        h.put(&h.key(0, n, n), *created_at, b"x");
    }
    let listing = h.list_all(0, 10);
    let got: Vec<Timestamp> = listing
        .files
        .iter()
        .map(|f| block_on(h.store.created_at(f)).unwrap().at())
        .collect();
    assert_eq!(got, resolved);
}

pub fn go_created_at_parses(h: &RealHarness) {
    let key = h.key(0, 5, 5);
    h.raw_put(
        &h.object(&key),
        b"go wrote this",
        &[(CREATED_AT_HEADER, "2025-01-02T03:04:05.123456789Z")],
    );
    let listing = h.list_all(0, 10);
    let resolved: Vec<Timestamp> = listing
        .files
        .iter()
        .map(|f| block_on(h.store.created_at(f)).unwrap().at())
        .collect();
    assert_eq!(resolved, [at("2025-01-02T03:04:05.123Z")]);
    assert_eq!(h.store.created_at_stats().from_metadata, 1);
}

pub fn object_without_metadata_falls_back(h: &RealHarness) {
    let key = h.key(0, 7, 7);
    h.raw_put(&h.object(&key), b"no created-at time", &[]);
    let listing = h.list_all(0, 10);
    let [file] = listing.files.as_slice() else {
        panic!("one file expected, got {listing:?}");
    };
    let now = WallClock.now().unix_millis();
    let recorded = block_on(h.store.created_at(file))
        .unwrap()
        .at()
        .unix_millis();
    assert!(now.abs_diff(recorded) < 600_000);
    let stats = h.store.created_at_stats();
    assert_eq!((stats.from_metadata, stats.fell_back), (0, 1));
}

pub fn unparseable_created_at_fails_the_reader_not_the_listing(h: &RealHarness) {
    let key = h.key(0, 9, 9);
    h.raw_put(&h.object(&key), b"bad", &[(CREATED_AT_HEADER, "yesterday")]);
    let listing = h.list_all(0, 10);
    let [file] = listing.files.as_slice() else {
        panic!("the listing keeps the file: {listing:?}");
    };
    assert_eq!(
        block_on(h.store.created_at(file)),
        Err(StoreError::Corrupt {
            key: Some(key),
            cause: CorruptCause::BadCreatedAt
        })
    );
}

/// Objects over one page: a listing page of 1000 sends no HEAD.
pub const PAGED_OBJECTS: u64 = 1100;

pub fn listing_makes_no_head_requests(h: &RealHarness) {
    for n in 1..=PAGED_OBJECTS {
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

pub fn multipart_round_trip(h: &RealHarness) {
    let key = h.key(0, 1, 1);
    let len = part(h).saturating_mul(5) / 2;
    let bytes = body(len);
    h.write_all(&key, at("2024-05-01T12:00:00.123Z"), &bytes, 1_500_003);
    assert!(h.read_back(&key, len) == bytes);
    let listed = h.list_all(0, 10);
    let [file] = listed.files.as_slice() else {
        panic!("one file expected: {listed:?}");
    };
    assert_eq!(file.size, u64::try_from(len).unwrap());
    assert_eq!(
        block_on(h.store.created_at(file)).unwrap().at(),
        at("2024-05-01T12:00:00.123Z")
    );
    assert_eq!(h.open_uploads(), 0);
    assert_eq!(h.store.pending_orphans(), 0);
}

pub fn part_size_boundary(h: &RealHarness) {
    for (n, len) in [
        (1, part(h)),
        (2, part(h).saturating_sub(1)),
        (3, part(h).saturating_add(1)),
        (4, part(h).saturating_mul(2)),
    ] {
        let key = h.key(0, n, n);
        let bytes = body(len);
        h.write_all(&key, Timestamp::from_unix_millis(9), &bytes, part(h));
        assert!(h.read_back(&key, len) == bytes, "length {len}");
    }
    assert_eq!(h.open_uploads(), 0);
}

pub fn dropped_multipart_is_invisible_then_aborted(h: &RealHarness) {
    block_on(async {
        let mut upload = h
            .store
            .begin_upload(&h.key(0, 1, 1), Timestamp::from_unix_millis(9))
            .await
            .unwrap();
        upload.write(&body(part(h))).await.unwrap();
    });
    assert!(h.list_all(0, 10).files.is_empty());
    assert_eq!(h.open_uploads(), 1);
    assert_eq!(h.store.pending_orphans(), 1);
    assert_eq!(h.store.abort_orphans().unwrap(), 1);
    assert_eq!(h.open_uploads(), 0);
}

pub fn abort_leaves_nothing(h: &RealHarness) {
    block_on(async {
        let mut upload = h
            .store
            .begin_upload(&h.key(0, 1, 1), Timestamp::from_unix_millis(9))
            .await
            .unwrap();
        upload.write(&body(part(h))).await.unwrap();
        upload.abort().await.unwrap();
    });
    assert_eq!(h.open_uploads(), 0);
    assert!(h.list_all(0, 10).files.is_empty());
}

/// S3 answers 404 NoSuchUpload to an abort of an unknown upload and MinIO 204;
/// the store treats both as aborted.
pub fn abort_of_an_unknown_upload_is_not_a_failure(h: &RealHarness) {
    let key = h.key(0, 1, 1);
    let request = h
        .builder()
        .abort_multipart(
            WallClock.now(),
            &key,
            &UploadId::new("no-such-id".to_owned()),
        )
        .unwrap();
    let response = h.send(request);
    let no_such = response.status() == 404
        && S3Code::from_body(response.body()) == Some(S3Code::NoSuchUpload);
    assert!(response.status() == 204 || no_such, "{}", response.status());
    if response.status() == 204 {
        assert_eq!(
            classify(StoreOp::Upload, Some(&key), None, &response),
            Ok(())
        );
    }
}

/// One test per conformance case. `$ignored` is a cfg predicate: `all()` ignores every
/// test (Tigris: only `just test-tigris` runs them), `any()` ignores none.
macro_rules! conformance_cases {
    ($connect:path, $ignored:meta; $($case:ident),+ $(,)?) => {
        pub const NAMES: &[&str] = &[$(stringify!($case)),+];
        $(
            #[test]
            #[cfg_attr($ignored, ignore = "needs a real bucket (just test-tigris)")]
            fn $case() {
                let Some(h) = $connect() else { return };
                lsr_core::conformance::block_on(lsr_core::conformance::$case(&h)).unwrap();
            }
        )+
    };
}

macro_rules! plain_cases {
    ($connect:path, $ignored:meta; $($case:ident),+ $(,)?) => {
        $(
            #[test]
            #[cfg_attr($ignored, ignore = "needs a real bucket (just test-tigris)")]
            fn $case() {
                let Some(h) = $connect() else { return };
                $crate::real::$case(&h);
            }
        )+
    };
}

/// Gives a provider's file one test per shared case and per conformance case.
macro_rules! shared_cases {
    ($connect:path, $ignored:meta) => {
        mod shared_cases {
            use super::*;

            $crate::real::conformance_cases!($connect, $ignored;
                list_empty_level,
                paging_at_every_limit,
                cursor_is_exclusive,
                levels_are_separate,
                unrecognized_names_are_skipped_and_counted,
                scratch_names_are_not_unrecognized,
                commit_error_may_leave_the_object_visible,
                abort_publishes_nothing,
                pending_and_dropped_uploads_are_invisible,
                read_spans_the_end,
                read_of_missing_key_is_not_found,
                delete_is_idempotent,
                two_uploads_to_one_key,
                chunks_concatenate_and_created_at_survives,
                created_at_returns_the_committed_created_at,
                listed_created_at_is_exact_or_marked,
                created_at_of_a_vanished_file_is_not_found,
                created_at_of_an_overwritten_file_is_refused,
                read_with_created_at_agrees_with_read_and_created_at,
                read_many_agrees_with_read,
                list_many_agrees_with_list,
                created_at_many_agrees_with_created_at,
            );

            /// A case added to core's suite and missing from the list above would
            /// silently not run on this provider.
            #[test]
            fn conformance_list_matches_core() {
                let mut ours: Vec<&str> = NAMES.to_vec();
                ours.sort_unstable();
                assert_eq!(ours, $crate::real::conformance_names_in_core());
            }

            $crate::real::plain_cases!($connect, $ignored;
                created_at_survives_round_trip,
                go_created_at_parses,
                object_without_metadata_falls_back,
                unparseable_created_at_fails_the_reader_not_the_listing,
                listing_makes_no_head_requests,
                multipart_round_trip,
                part_size_boundary,
                dropped_multipart_is_invisible_then_aborted,
                abort_leaves_nothing,
                abort_of_an_unknown_upload_is_not_a_failure,
            );
        }
    };
}
pub(crate) use {conformance_cases, plain_cases, shared_cases};

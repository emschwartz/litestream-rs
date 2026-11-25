//! Each kind of request has its own limit on requests in flight: multipart
//! parts by `concurrency` (5; R2 2 unless set), creation-time HEADs 16,
//! ranged reads and listing pages 8. A transport that
//! records the limit each batch is handed shows what the store asks for.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::conformance::block_on;
use lsr_core::{BatchKind, FakeClock, FileInfo, Level, ListAt, ListedTime, ObjectStore, Upload};
use lsr_ltx::Timestamp;
use lsr_store_s3::{
    FakeS3, FaultTransport, HttpRequest, HttpResponse, Provider, S3Config, S3Store, Transport,
    TransportError,
};

use crate::common::*;
use crate::real::{PART, body};
use lsr_core::WriteMode;

/// Passes every request to the fake and notes the limit of each batch.
struct Recording {
    inner: Arc<FaultTransport<FakeS3>>,
    limits: Arc<Mutex<Vec<usize>>>,
}

impl Transport for Recording {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.inner.send(request)
    }

    fn send_many(
        &self,
        requests: Vec<HttpRequest>,
        limit: NonZeroUsize,
    ) -> Vec<Result<HttpResponse, TransportError>> {
        self.limits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(limit.get());
        requests.into_iter().map(|r| self.send(r)).collect()
    }
}

fn store_for(config: S3Config) -> (S3Store<Recording>, Arc<Mutex<Vec<usize>>>) {
    let limits = Arc::new(Mutex::new(Vec::new()));
    let transport = Recording {
        inner: Arc::new(FaultTransport::new(fake())),
        limits: Arc::clone(&limits),
    };
    let store =
        S3Store::with_transport(config, credentials(), transport, FakeClock::new(now())).unwrap();
    (store, limits)
}

fn seen(limits: &Mutex<Vec<usize>>) -> Vec<usize> {
    let mut seen = limits
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    seen.dedup();
    seen
}

fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

/// The limit a four-part upload asks for, which is every batch's limit.
fn part_limit(config: S3Config) -> Vec<usize> {
    let (store, limits) = store_for(config);
    block_on(async {
        let mut up = store
            .begin_upload(
                &key(0, 1, 1),
                Timestamp::from_unix_millis(5),
                WriteMode::Replace,
            )
            .await
            .unwrap();
        up.write(&body(PART * 4)).await.unwrap();
        up.commit().await.unwrap();
    });
    seen(&limits)
}

#[test]
fn parts_follow_the_concurrency_setting_per_provider() {
    let generic = config();
    let tigris = config().with_provider(Provider::Tigris);
    let r2 = config().with_provider(Provider::R2);
    assert_eq!(part_limit(generic.clone()), [5]);
    assert_eq!(part_limit(generic.with_concurrency(n(3))), [3]);
    assert_eq!(part_limit(tigris.clone()), [5]);
    assert_eq!(part_limit(tigris.with_concurrency(n(3))), [3]);
    assert_eq!(part_limit(r2.clone()), [2]);
    assert_eq!(part_limit(r2.with_concurrency(n(3))), [3]);
}

/// A setting that is not about parts changes no other kind's limit.
#[test]
fn concurrency_governs_only_parts() {
    let (store, limits) = store_for(config().with_concurrency(n(1)));
    let k = key(0, 1, 1);
    let bytes = body(4);
    block_on(async {
        let mut up = store
            .begin_upload(&k, Timestamp::from_unix_millis(5), WriteMode::Replace)
            .await
            .unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap();
    });
    block_on(store.list_many(&[ListAt::first(Level::ZERO)], limit(10)));
    let files = [FileInfo {
        key: k,
        size: 4,
        listed: ListedTime::LastModified(now()),
    }];
    block_on(store.created_at_many(&files));
    assert_eq!(seen(&limits), [8, 16]);
}

#[test]
fn creation_time_heads_go_sixteen_at_a_time() {
    let (store, limits) = store_for(config());
    let k = key(0, 1, 1);
    block_on(async {
        let mut up = store
            .begin_upload(&k, Timestamp::from_unix_millis(5), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        up.commit().await.unwrap();
    });
    let files = [FileInfo {
        key: k,
        size: 4,
        listed: ListedTime::LastModified(now()),
    }];
    assert_eq!(block_on(store.created_at_many(&files)).len(), 1);
    assert_eq!(seen(&limits), [16]);
    assert_eq!(store.batch_width(BatchKind::ResolvedCreatedAt).get(), 16);
}

#[test]
fn listings_go_eight_at_a_time() {
    let (store, limits) = store_for(config());
    let pages = [Level::ZERO].map(|level| ListAt { level, after: None });
    assert_eq!(block_on(store.list_many(&pages, limit(10))).len(), 1);
    assert_eq!(seen(&limits), [8]);
    assert_eq!(store.batch_width(BatchKind::Lists).get(), 8);
}

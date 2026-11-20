//! The store over FakeS3 behind the fault layer, for the test files that drive
//! the store itself. Declared with `#[path = "common/rig.rs"] mod rig;`.
use std::sync::Arc;

use crate::common::*;
use lsr_core::conformance::{Failure, Harness, block_on};
use lsr_core::{FakeClock, FileKey, Level, ObjectStore, PageLimit, StoreError, Upload};
use lsr_ltx::Timestamp;
use lsr_store_s3::{
    FakeS3, Fault, FaultTransport, HttpRequest, HttpResponse, S3Store, Shape, Transport,
    TransportError, level_prefix,
};

/// The fault layer over the fake, shared between the store that owns a
/// transport and the test that scripts it.
pub struct Shared(pub Arc<FaultTransport<FakeS3>>);

impl Transport for Shared {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.0.send(request)
    }
}

pub struct FakeHarness {
    pub wire: Arc<FaultTransport<FakeS3>>,
    pub store: S3Store<Shared>,
}

impl FakeHarness {
    pub fn new() -> Self {
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

pub fn count(wire: &FaultTransport<FakeS3>, shape: Shape) -> usize {
    wire.requests()
        .iter()
        .filter(|r| r.shape == Some(shape))
        .count()
}

/// The shapes of every request so far, in order.
pub fn shapes(wire: &FaultTransport<FakeS3>) -> Vec<Option<Shape>> {
    wire.requests().iter().map(|r| r.shape).collect()
}

pub fn put_through(store: &impl ObjectStore, key: &FileKey, body: &[u8]) -> Result<(), StoreError> {
    block_on(async {
        let mut up = store
            .begin_upload(key, Timestamp::from_unix_millis(5))
            .await?;
        up.write(body).await?;
        up.commit().await.map(drop)
    })
}

pub fn list_all(store: &impl ObjectStore) -> Result<lsr_core::Listing, StoreError> {
    block_on(store.list(Level::ZERO, None, PageLimit::MAX))
}

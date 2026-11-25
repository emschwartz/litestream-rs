//! A store opened before its credential chain can answer (`S3Store::deferred_over`): each
//! request asks the chain again until it does, and a request made meanwhile fails as any
//! failed credential fetch does (`Transient` when waiting may fix it, `Denied(Credentials)`
//! when not), with nothing sent to the bucket. Go's SDK fetches credentials lazily per request
//! the same way, so a source that is down at start costs it a failed sync, not the database.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use lsr_core::conformance::block_on;
use lsr_core::{FakeClock, Level, ObjectStore, StoreError, StoreOp};
use lsr_store_s3::{
    CredentialChain, FakeS3, FaultTransport, HttpRequest, HttpResponse, S3Store, Transport,
    TransportError,
};

use crate::common::*;

const DOC: &str = r#"{"Code":"Success","AccessKeyId":"id","SecretAccessKey":"secret","Token":"tok","Expiration":"2099-01-01T00:00:00Z"}"#;

/// What the instance metadata service does right now.
const DOWN: u8 = 0;
const FORBIDDEN: u8 = 1;
const UP: u8 = 2;

struct Metadata {
    mode: AtomicU8,
    asked: AtomicUsize,
}

impl Transport for Metadata {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        let answer = |status, headers: Vec<(String, String)>, body: &str| {
            Ok(HttpResponse::new(
                status,
                headers,
                body.as_bytes().to_vec(),
                true,
            ))
        };
        match self.mode.load(Ordering::SeqCst) {
            DOWN => Err(TransportError::Connect),
            FORBIDDEN => answer(403, Vec::new(), ""),
            _ => match request.url().path() {
                "/latest/api/token" => answer(
                    200,
                    vec![(
                        "X-Aws-Ec2-Metadata-Token-Ttl-Seconds".to_owned(),
                        "300".to_owned(),
                    )],
                    "tok",
                ),
                "/latest/meta-data/iam/security-credentials/" => answer(200, Vec::new(), "role"),
                "/latest/meta-data/iam/security-credentials/role" => answer(200, Vec::new(), DOC),
                _ => answer(404, Vec::new(), ""),
            },
        }
    }
}

/// The bucket's side, counting what it is sent.
struct Bucket(Arc<FaultTransport<FakeS3>>);

impl Transport for Bucket {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.0.send(request)
    }
}

struct Rig {
    store: S3Store<Bucket>,
    metadata: Arc<Metadata>,
    bucket: Arc<FaultTransport<FakeS3>>,
}

/// A store whose chain has nothing but the instance metadata service, in `mode`.
fn rig(mode: u8) -> Rig {
    let metadata = Arc::new(Metadata {
        mode: AtomicU8::new(mode),
        asked: AtomicUsize::new(0),
    });
    let vars: HashMap<&str, &str> = HashMap::from([
        ("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:9"),
        ("AWS_CONFIG_FILE", "/nonexistent/config"),
        ("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials"),
    ]);
    let clock: Arc<dyn lsr_core::Clock> = Arc::new(FakeClock::new(now()));
    let shared = Arc::clone(&metadata);
    let transport: Arc<dyn Transport> = shared;
    let chain = CredentialChain::with_env(move |name| vars.get(name).map(|v| (*v).to_owned()))
        .with_transport(transport, clock)
        .with_region("us-east-1");
    let bucket = Arc::new(FaultTransport::new(fake()));
    let store = S3Store::deferred_over(
        config(),
        chain,
        Bucket(Arc::clone(&bucket)),
        FakeClock::new(now()),
    )
    .unwrap();
    Rig {
        store,
        metadata,
        bucket,
    }
}

fn list(rig: &Rig) -> Result<usize, StoreError> {
    block_on(rig.store.list(Level::ZERO, None, limit(10))).map(|listing| listing.files.len())
}

#[test]
fn a_chain_that_cannot_be_reached_fails_each_request_transient_and_sends_nothing() {
    let rig = rig(DOWN);
    for _ in 0..2 {
        assert_eq!(
            list(&rig),
            Err(StoreError::Transient {
                op: StoreOp::List,
                kind: std::io::ErrorKind::NotConnected,
            })
        );
    }
    assert!(rig.bucket.requests().is_empty());
}

#[test]
fn a_chain_the_service_refuses_is_a_credentials_refusal_and_sends_nothing() {
    let rig = rig(FORBIDDEN);
    assert_eq!(
        list(&rig),
        Err(StoreError::Denied {
            op: StoreOp::List,
            why: lsr_core::Denied::Credentials,
        })
    );
    assert!(rig.bucket.requests().is_empty());
}

/// The point of the store: the service comes up after the process did, and replication starts
/// with no restart, then stops asking.
#[test]
fn the_store_signs_once_the_chain_answers_and_asks_no_more() {
    let rig = rig(DOWN);
    list(&rig).unwrap_err();
    rig.metadata.mode.store(UP, Ordering::SeqCst);
    assert_eq!(list(&rig), Ok(0));
    assert_eq!(rig.bucket.requests().len(), 1);
    let asked = rig.metadata.asked.load(Ordering::SeqCst);
    assert_eq!(list(&rig), Ok(0));
    assert_eq!(rig.metadata.asked.load(Ordering::SeqCst), asked);
}

//! The S3 store against a real MinIO (`just test-s3`).
//!
//! Every test needs `LSR_MINIO_ENDPOINT` (`http://127.0.0.1:9000`, set by the
//! recipe) and a server that answers; without both it fails (`lsr_core::needs`), unless the
//! runner opted out with `LSR_SKIP=minio`. The tests are `#[ignore]`d out of `just check`
//! (which never needs Docker); `just test-s3` runs them. The server, its pinned image and its throwaway credentials
//! live in `scripts/minio.sh`; the credential defaults in `lsr_core::needs::minio` repeat that
//! script and can be overridden with `LSR_MINIO_ACCESS_KEY` and `LSR_MINIO_SECRET_KEY`.
//! Each test works under its own prefix in the shared bucket, so tests run in
//! parallel and a rerun never sees an earlier run's objects. The cases every
//! provider must pass are shared with tigris.rs through `common/real.rs`.

use lsr_core::conformance::block_on;
use lsr_core::{Clock, CorruptCause, ReadLen, StoreError, StoreOp};
use lsr_ltx::Timestamp;
use lsr_store_s3::classify;

use crate::real::{RealHarness, Server, WallClock};

/// The local MinIO of scripts/minio.sh, or None when the runner opted out with `LSR_SKIP=minio`.
fn connect() -> Option<RealHarness> {
    let minio = lsr_core::needs::minio()?;
    Some(RealHarness::connect(&Server {
        name: "minio",
        endpoint: minio.endpoint,
        bucket: "lsr-test".into(),
        region: "us-east-1".into(),
        access: minio.access,
        secret: minio.secret,
        root: None,
    }))
}

crate::real::shared_cases!(connect, all());

#[test]
#[ignore = "needs MinIO; just test-s3"]
fn a_body_other_than_the_signed_one_is_refused_by_minio() {
    let Some(h) = connect() else {
        return;
    };
    let key = h.key(0, 3, 3);
    // The signed SHA-256 is of "hello"; the body that goes out is not.
    let request = h
        .builder()
        .put_object(
            WallClock.now(),
            &key,
            b"hello".to_vec(),
            Timestamp::from_unix_millis(1),
        )
        .unwrap()
        .tamper_body(b"other".to_vec());
    let judged = h.send(request);
    assert_eq!(
        classify(StoreOp::Upload, Some(&key), None, &judged),
        Err(StoreError::Corrupt {
            key: Some(key),
            cause: CorruptCause::ChecksumMismatch { part: None }
        })
    );
    // Nothing was stored under the key.
    let read = block_on(lsr_core::ObjectStore::read(
        &h.store,
        &key,
        0,
        ReadLen::new(16).unwrap(),
    ));
    assert_eq!(read, Err(StoreError::NotFound { key }));
}

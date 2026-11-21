//! The S3 store against a real MinIO (`just test-s3`).
//!
//! Every test needs `LSR_MINIO_ENDPOINT` (`http://127.0.0.1:9000`, set by the
//! recipe) and skips with a message when it is unset, so `just check` never
//! needs Docker. With `LSR_REQUIRE_MINIO` set (the recipe sets it under CI) a missing or
//! unreachable server fails the test instead: a green run must mean the server answered. The server, its pinned image and its throwaway credentials
//! live in `scripts/minio.sh`; the credential defaults in `server()` repeat that script
//! and can be overridden with `LSR_MINIO_ACCESS_KEY` and `LSR_MINIO_SECRET_KEY`.
//! Each test works under its own prefix in the shared bucket, so tests run in
//! parallel and a rerun never sees an earlier run's objects. The cases every
//! provider must pass are shared with tigris.rs through `common/real.rs`.

use lsr_core::conformance::block_on;
use lsr_core::{Clock, CorruptCause, ReadLen, StoreError, StoreOp};
use lsr_ltx::Timestamp;
use lsr_store_s3::classify;

use crate::real::{RealHarness, Server, WallClock};

/// The local MinIO of scripts/minio.sh, or None when no endpoint is set.
fn server() -> Option<Server> {
    let endpoint = std::env::var("LSR_MINIO_ENDPOINT").ok()?;
    Some(Server {
        name: "minio",
        endpoint,
        bucket: "lsr-test".into(),
        region: "us-east-1".into(),
        access: std::env::var("LSR_MINIO_ACCESS_KEY").unwrap_or_else(|_| "lsrtestroot".into()),
        secret: std::env::var("LSR_MINIO_SECRET_KEY")
            .unwrap_or_else(|_| "lsrtest-throwaway-0000".into()),
        root: None,
    })
}

/// Whether CI (or the developer) demands the server: no skipping, no running without it.
fn required() -> bool {
    std::env::var_os("LSR_REQUIRE_MINIO").is_some_and(|v| !v.is_empty())
}

/// Panics unless the endpoint accepts a TCP connection, so a test that expects an error
/// cannot pass on a server that is simply absent.
fn require_reachable(endpoint: &str) {
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default();
    let addr = std::net::ToSocketAddrs::to_socket_addrs(authority)
        .ok()
        .and_then(|mut addrs| addrs.next());
    let reached = addr.is_some_and(|addr| {
        std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(5)).is_ok()
    });
    assert!(
        reached,
        "LSR_REQUIRE_MINIO is set but MinIO is unreachable at {endpoint} (just minio-up); \
         refusing to pass without running"
    );
}

fn connect() -> Option<RealHarness> {
    let Some(server) = server() else {
        assert!(
            !required(),
            "LSR_REQUIRE_MINIO is set but LSR_MINIO_ENDPOINT is not; refusing to skip (just minio-up)"
        );
        eprintln!("skipped: set LSR_MINIO_ENDPOINT (just minio-up)");
        return None;
    };
    if required() {
        require_reachable(&server.endpoint);
    }
    Some(RealHarness::connect(&server))
}

crate::real::shared_cases!(connect, any());

#[test]
fn wrong_content_md5_is_refused_by_minio() {
    let Some(h) = connect() else {
        return;
    };
    let key = h.key(0, 3, 3);
    // The signed digest is of "hello"; the body that goes out is not.
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

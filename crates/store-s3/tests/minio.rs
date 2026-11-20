//! The S3 store against a real MinIO (`just test-s3`).
//!
//! Every test needs `LSR_MINIO_ENDPOINT` (`http://127.0.0.1:9000`, set by the
//! recipe) and skips with a message when it is unset, so `just check` never
//! needs Docker. The server, its pinned image and its throwaway credentials
//! live in `scripts/minio.sh`; the credential defaults in `server()` repeat that script
//! and can be overridden with `LSR_MINIO_ACCESS_KEY` and `LSR_MINIO_SECRET_KEY`.
//! Each test works under its own prefix in the shared bucket, so tests run in
//! parallel and a rerun never sees an earlier run's objects. The cases every
//! provider must pass are shared with tigris.rs through `common/real.rs`.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
#[path = "common/real.rs"]
mod real;

#[cfg(test)]
mod minio {
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

    fn connect() -> Option<RealHarness> {
        let Some(server) = server() else {
            eprintln!("skipped: set LSR_MINIO_ENDPOINT (just minio-up)");
            return None;
        };
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
}

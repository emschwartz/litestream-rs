//! One short end-to-end run on a real provider (`just test-provider <tigris|aws|r2>`): the built
//! `lsr replicate -once` copies a small database to the run's prefix, `lsr verify` reads the
//! replica back, `lsr restore` rebuilds it, and the restored pages equal the source's
//! (never compared on LTX bytes). Ignored: the recipe supplies the credentials and the prefix,
//! and deletes the prefix afterwards.

use lsr_compat::{hash_pages, integrity_check};
use lsr_core::needs::real_bucket;

use crate::common::{database, lsr, write};

/// The replica URL with the endpoint and region as Go's query parameters.
fn replica_url(bucket: &lsr_core::needs::RealBucket) -> String {
    let escaped = bucket.endpoint.replace(':', "%3A").replace('/', "%2F");
    format!(
        "s3://{}/{}/e2e?endpoint={escaped}&region={}",
        bucket.bucket, bucket.prefix, bucket.region
    )
}

fn run(bucket: &lsr_core::needs::RealBucket, args: &[&str]) -> std::process::Output {
    lsr()
        .args(args)
        .env("AWS_ACCESS_KEY_ID", &bucket.access)
        .env("AWS_SECRET_ACCESS_KEY", &bucket.secret)
        .env("AWS_REGION", &bucket.region)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .output()
        .unwrap()
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn replicate_a_small_database_restore_it_and_compare_pages() {
    let bucket = real_bucket();
    let url = replica_url(&bucket);
    let dir = tempfile::TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let app = database(&db);
    write(&app, 40);
    app.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
        .unwrap();
    let replicated = run(&bucket, &["replicate", "-once", db.to_str().unwrap(), &url]);
    let log = String::from_utf8_lossy(&replicated.stderr).into_owned();
    assert_eq!(replicated.status.code(), Some(0), "{log}");
    let verified = run(&bucket, &["verify", "--replica", &url]);
    let log = String::from_utf8_lossy(&verified.stderr).into_owned();
    assert_eq!(verified.status.code(), Some(0), "{log}");
    let restored = dir.path().join("restored.db");
    let out = run(
        &bucket,
        &[
            "restore",
            "--replica",
            &url,
            "--output",
            restored.to_str().unwrap(),
        ],
    );
    let log = String::from_utf8_lossy(&out.stderr).into_owned();
    assert_eq!(out.status.code(), Some(0), "{log}");
    integrity_check(&restored).unwrap();
    drop(app);
    assert_eq!(
        hash_pages(&restored).unwrap(),
        hash_pages(&db).unwrap(),
        "restored pages equal the source's"
    );
}

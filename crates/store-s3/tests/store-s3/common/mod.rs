//! Shared setup: a fake bucket, a request builder for it, and the helpers both
//! test files use. Each test file uses a subset of these.
use lsr_core::{FakeClock, FileKey, Level, PageLimit};
use lsr_ltx::{Timestamp, Txid, TxidRange};
use lsr_store_s3::{BucketName, Endpoint, FakeS3, Prefix, S3Config, S3Credentials, SecretString};

/// The AWS documentation's example instant.
pub const NOW: i64 = 1_369_353_600_000;

pub const CANARY: &str = "CANARY-7f3a91";
pub const TOKEN: &str = "CANARY-TOKEN-55c2";

pub fn now() -> Timestamp {
    Timestamp::from_unix_millis(NOW)
}

pub fn credentials() -> S3Credentials {
    S3Credentials::new(
        "AKIAIOSFODNN7EXAMPLE".to_owned(),
        SecretString::new(CANARY.to_owned()),
    )
    .with_session_token(SecretString::new(TOKEN.to_owned()))
}

pub fn config() -> S3Config {
    S3Config::new(
        Endpoint::new("http://localhost:9000").unwrap(),
        BucketName::new("bucket").unwrap(),
        Prefix::new("db").unwrap(),
    )
}

pub fn fake() -> FakeS3 {
    FakeS3::new(&config(), FakeClock::new(now()))
}

pub fn range(min: u64, max: u64) -> TxidRange {
    TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap()
}

pub fn key(level: u8, min: u64, max: u64) -> FileKey {
    FileKey::new(Level::new(level).unwrap(), range(min, max))
}

pub fn limit(raw: u16) -> PageLimit {
    PageLimit::new(raw).unwrap()
}

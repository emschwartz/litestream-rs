//! The secrets proof: nothing a caller can format holds a credential, a
//! signature or a URL.

use crate::{
    BucketName, ConfigError, Endpoint, HttpResponse, Prefix, RequestBuilder, S3Config,
    S3Credentials, SecretString, TransportError, classify, classify_transport,
};
use lsr_core::{FileKey, Level, PageLimit, StoreOp};
use lsr_ltx::{Timestamp, Txid, TxidRange};

const CANARY: &str = "CANARY-7f3a91";
const TOKEN: &str = "CANARY-TOKEN-55c2";

fn credentials() -> S3Credentials {
    S3Credentials::new(
        "AKIAIOSFODNN7EXAMPLE".to_owned(),
        SecretString::new(CANARY.to_owned()),
    )
    .with_session_token(SecretString::new(TOKEN.to_owned()))
}

fn config() -> S3Config {
    S3Config::new(
        Endpoint::new("http://localhost:9000").unwrap(),
        BucketName::new("bucket").unwrap(),
        Prefix::new("db").unwrap(),
    )
}

fn assert_clean(what: &str, shown: &str) {
    for bad in ["CANARY", "wJalr", "Signature", "Security-Token"] {
        assert!(!shown.contains(bad), "{what} printed {bad}: {shown}");
    }
}

#[test]
fn credentials_never_print() {
    let creds = credentials();
    let builder = RequestBuilder::new(&config(), &creds).unwrap();
    let id = Txid::new(1).unwrap();
    let key = FileKey::new(Level::ZERO, TxidRange::new(id, id).unwrap());
    let now = Timestamp::from_unix_millis(1_369_353_600_000);

    assert_clean(
        "secret",
        &format!("{:?}", SecretString::new(CANARY.to_owned())),
    );
    assert_clean("credentials", &format!("{creds:?}"));
    assert_clean("config", &format!("{:?}", config()));
    assert_clean("builder", &format!("{builder:?}"));

    let requests = [
        builder
            .list_page(now, Level::ZERO, None, PageLimit::MAX)
            .unwrap(),
        builder.head(now, &key).unwrap(),
        builder.put_object(now, &key, vec![1], now).unwrap(),
        builder.delete_objects(now, &[key]).unwrap(),
    ];
    for request in &requests {
        assert_clean("request", &format!("{request:?}"));
        assert!(
            request.url().as_str().contains("X-Amz-Security-Token"),
            "the token must be signed in"
        );
    }

    let errors = [
        ConfigError::BadUrl,
        ConfigError::UnsupportedScheme,
        ConfigError::EmptyPrefix,
        ConfigError::BadPrefix,
        ConfigError::MissingBucket,
        ConfigError::BadBucket,
        ConfigError::BadRegion,
        ConfigError::UnknownParameter,
        ConfigError::PartSizeTooSmall { bytes: 1 },
    ];
    for error in errors {
        assert_clean("config error", &format!("{error:?} {error}"));
    }
    let secret_url = format!("s3://user:{CANARY}@bucket/p?x={CANARY}");
    let refused = S3Config::from_url(&secret_url).unwrap_err();
    assert_clean("url error", &format!("{refused:?} {refused}"));

    let body = format!("<Error><Code>AccessDenied</Code><Message>{CANARY}</Message></Error>");
    for status in [200, 400, 403, 404, 429, 500, 503] {
        let response = HttpResponse::new(
            status,
            vec![("Retry-After".to_owned(), CANARY.to_owned())],
            body.clone().into_bytes(),
            status != 200,
        );
        assert_clean("response", &format!("{response:?}"));
        if let Err(error) = classify(StoreOp::Read, Some(&key), None, &response) {
            assert_clean("store error", &format!("{error:?} {error}"));
        }
    }
    for error in [
        TransportError::Timeout,
        TransportError::Tls,
        TransportError::Connect,
    ] {
        let mapped = classify_transport(StoreOp::List, &error);
        assert_clean(
            "transport",
            &format!("{error:?} {error} {mapped:?} {mapped}"),
        );
    }
}

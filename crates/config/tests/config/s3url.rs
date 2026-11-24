//! The one `s3://` URL parser (`S3Url`, `s3_config`) on its own: the documented form,
//! the defaults, every refusal, the overrides' precedence, and no secret in a problem.
//! The daemon's mapping of a `url:` key (`map`) and the command line's `--replica`
//! (`lsr-cli`'s args tests) both run through it.

use lsr_config::{ReplicaProblem, S3Overrides, S3Url, s3_config};
use lsr_store_s3::{ConfigError, PartSize, Provider, S3Config, UrlStyle};

fn read(text: &str) -> Result<S3Config, ReplicaProblem> {
    s3_config(Some(S3Url::parse(text)?), &S3Overrides::default())
}

#[test]
fn reads_the_documented_form() {
    let config = read(
        "s3://my-bucket/db/one?endpoint=http%3A%2F%2Flocalhost%3A9000&region=eu-west-1&partSize=8388608",
    )
    .unwrap();
    assert_eq!(config.bucket().as_str(), "my-bucket");
    assert_eq!(config.prefix().as_str(), "db/one");
    assert_eq!(config.region().as_str(), "eu-west-1");
    assert_eq!(config.provider(), Provider::Minio);
    assert_eq!(config.style(), UrlStyle::Path);
    assert_eq!(config.part_size().get(), 8_388_608);
}

#[test]
fn a_bare_url_is_aws_in_virtual_host_style() {
    let config = read("s3://b/p").unwrap();
    assert_eq!(
        config.endpoint().url().as_str(),
        "https://s3.us-east-1.amazonaws.com/"
    );
    assert_eq!(config.provider(), Provider::Aws);
    assert_eq!(config.style(), UrlStyle::VirtualHost);
    assert_eq!(config.part_size(), PartSize::MIN);
    let tigris = read("s3://b/p?endpoint=https%3A%2F%2Ffly.storage.tigris.dev").unwrap();
    assert_eq!(tigris.provider(), Provider::Tigris);
    assert_eq!(tigris.part_size(), PartSize::TIGRIS);
}

/// Go cleans the URL's path (`path.Clean`) and trims one leading slash, so `s3://b/` has an empty
/// path and its keys start with a slash. `s3://b` cleans to `.`, which is a different prefix (its
/// keys are `./0000/<file>`) that the `url` type cannot carry, so it is refused, not read as the bare bucket.
#[test]
fn a_bucket_with_no_path_is_a_replica_at_the_root_of_the_bucket() {
    for text in ["s3://b/", "s3://b/.", "s3://b//", "s3://b/a/.."] {
        let config = read(text).unwrap();
        assert_eq!(config.bucket().as_str(), "b", "{text}");
        assert_eq!(config.prefix().as_str(), "", "{text}");
    }
    let url = S3Url::parse("s3://b/p").unwrap();
    let over = S3Overrides {
        path: Some(""),
        ..S3Overrides::default()
    };
    assert_eq!(
        s3_config(Some(url), &over).unwrap().prefix().as_str(),
        "p",
        "an empty override counts as unset"
    );
    let bucket_only = S3Overrides {
        bucket: Some("b"),
        ..S3Overrides::default()
    };
    assert_eq!(s3_config(None, &bucket_only).unwrap().prefix().as_str(), "");
}

#[test]
fn each_bad_shape_is_refused() {
    let s3 = ReplicaProblem::S3;
    let cases = [
        ("not a url", ReplicaProblem::BadUrl),
        ("s3:///p", ReplicaProblem::BucketRequired),
        ("s3://b", s3(ConfigError::BadPrefix)),
        ("s3://b?region=eu-west-1", s3(ConfigError::BadPrefix)),
        ("s3://b/a%0Ab", s3(ConfigError::BadPrefix)),
        ("s3://b%20c/p", s3(ConfigError::BadBucket)),
        (
            "s3://b/p?partSize=1024",
            s3(ConfigError::PartSizeTooSmall { bytes: 1024 }),
        ),
        (
            "s3://b/p?partSize=big",
            ReplicaProblem::QueryNotNumber {
                name: "partSize".to_owned(),
            },
        ),
        (
            "s3://b/p?skipVerify=true",
            ReplicaProblem::QueryNotInV1 {
                name: "skipVerify".to_owned(),
                why: "litestream-rs always verifies TLS certificates",
            },
        ),
        (
            "s3://b/p?forcePathstyle=true",
            ReplicaProblem::QueryUnknown {
                name: "forcePathstyle".to_owned(),
            },
        ),
        ("s3://user:pw@b/p", ReplicaProblem::UserInfo),
        (
            "s3://arn:aws:s3:us-east-1:123:accesspoint/ap/db",
            ReplicaProblem::AccessPoint,
        ),
        (
            "s3://b.nyc3.digitaloceanspaces.com/p",
            ReplicaProblem::ProviderHost {
                provider: "DigitalOcean Spaces",
            },
        ),
    ];
    for (text, want) in cases {
        assert_eq!(read(text), Err(want), "{text}");
    }
}

/// Go's precedence: a set override wins over the URL's host and query; an empty one
/// counts as unset; an endpoint given either way turns path style on unless it is set.
#[test]
fn overrides_win_over_the_url() {
    let url = S3Url::parse("s3://b/p?region=us-west-2&endpoint=http://a:1").unwrap();
    let over = S3Overrides {
        region: Some("eu-west-1"),
        endpoint: Some("fly.storage.tigris.dev"),
        path: Some(""),
        ..S3Overrides::default()
    };
    let config = s3_config(Some(url), &over).unwrap();
    assert_eq!(config.region().as_str(), "eu-west-1");
    assert_eq!(
        config.endpoint().url().as_str(),
        "https://fly.storage.tigris.dev/"
    );
    assert_eq!(config.prefix().as_str(), "p");
    assert_eq!(config.style(), UrlStyle::Path);
    let set = S3Overrides {
        force_path_style: Some(false),
        ..over
    };
    let url = S3Url::parse("s3://b/p?endpoint=http://a:1").unwrap();
    let config = s3_config(Some(url), &set).unwrap();
    assert_eq!(config.style(), UrlStyle::VirtualHost);
    let none = s3_config(None, &S3Overrides::default());
    assert_eq!(none, Err(ReplicaProblem::BucketRequired));
}

#[test]
fn no_problem_shows_the_url_s_secrets() {
    let canary = "hunter2-canary";
    for text in [
        format!("s3://user:{canary}@bucket/p"),
        format!("s3://bucket/p?x={canary}"),
        format!("s3://bucket/p?partSize={canary}"),
    ] {
        let refused = S3Url::parse(&text).unwrap_err();
        let shown = format!("{refused:?} {refused}");
        assert!(!shown.contains(canary), "{shown}");
    }
}

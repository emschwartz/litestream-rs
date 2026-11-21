//! ObjectStore on an S3-compatible bucket, sans-IO half: configuration,
//! credentials that cannot print, the credential chain that finds them (explicit
//! values, the environment, the AWS shared files; see the `chain` module for
//! what it does not cover), signed requests as data, response parsers and
//! the error classification. The blocking HTTP client and the store itself come
//! after this layer; everything here is pure and testable without a network.
//!
//! The HTTP client is a feature: `ureq` (the default, no async runtime) or
//! `reqwest`. An application that already builds reqwest depends on this crate with
//! `default-features = false, features = ["reqwest"]`, so ureq and its copy of
//! the TLS stack stay out of its build. Either client, when the store builds
//! it, trusts the certificate authorities Go's `crypto/x509` trusts (the
//! `trust` module).

mod chain;
mod classify;
mod config;
mod credentials;
#[cfg(feature = "testing")]
mod fake;
#[cfg(feature = "testing")]
mod fault;
mod list;
mod multipart;
mod orphans;
mod request;
#[cfg(feature = "reqwest")]
mod reqwest_transport;
mod response;
#[cfg(test)]
mod secrets;
mod store;
mod transport;
#[cfg(any(feature = "ureq", feature = "reqwest"))]
mod trust;
mod upload;
#[cfg(feature = "ureq")]
mod ureq_transport;

pub use chain::{CredentialChain, CredentialError, EnvGap, FileOutcome, Resolved, Source, Tried};
pub use classify::{S3Code, classify, classify_transport, is_range_past_end};
pub use config::{
    BucketName, CREATION_TIME_CONCURRENCY, ConfigError, DEFAULT_REGION, Endpoint, PartSize, Prefix,
    Provider, READ_CONCURRENCY, Region, S3Config, UrlStyle,
};
pub use credentials::{S3Credentials, SecretString};
#[cfg(feature = "testing")]
pub use fake::FakeS3;
#[cfg(feature = "testing")]
pub use fault::{Fault, FaultTransport, RequestShape, Shape};
pub use request::{
    HttpRequest, Method, RequestBuilder, RequestError, UploadId, WriteRequest, level_prefix,
    object_key,
};
#[cfg(feature = "testing")]
pub use request::{OpenUpload, abandon_upload, abort_upload, open_uploads};
#[cfg(feature = "reqwest")]
pub use reqwest_transport::{ReqwestSetupError, ReqwestTransport};
pub use response::{
    EtagCheck, Head, ListPage, ListedFile, ResponseError, check_etag, hex, parse_complete,
    parse_create, parse_delete_failures, parse_head, parse_list, quoted_md5,
};
pub use store::{ListStats, S3Store, SystemClock};
pub use transport::{HttpResponse, Transport, TransportError, send_on_threads};
pub use upload::S3Upload;
#[cfg(feature = "ureq")]
pub use ureq_transport::UreqTransport;

//! ObjectStore on an S3-compatible bucket, sans-IO half: configuration,
//! credentials that cannot print, signed requests as data, response parsers and
//! the error classification. The blocking HTTP client and the store itself come
//! after this layer; everything here is pure and testable without a network.

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
mod upload;
#[cfg(feature = "ureq")]
mod ureq_transport;

pub use classify::{S3Code, classify, classify_transport, is_range_past_end};
pub use config::{
    BucketName, ConfigError, DEFAULT_REGION, Endpoint, PartSize, Prefix, Provider, Region,
    S3Config, UrlStyle,
};
pub use credentials::{S3Credentials, SecretString};
#[cfg(feature = "testing")]
pub use fake::FakeS3;
#[cfg(feature = "testing")]
pub use fault::{Fault, FaultTransport, RequestShape, Shape};
pub use request::{
    HttpRequest, MAX_DELETE_KEYS, Method, RequestBuilder, RequestError, UploadId, level_prefix,
    object_key, range_header, start_after,
};
#[cfg(feature = "reqwest")]
pub use reqwest_transport::{ReqwestSetupError, ReqwestTransport};
pub use response::{
    EtagCheck, Head, ListPage, ListedFile, ResponseError, check_etag, hex, parse_complete,
    parse_create, parse_delete_failures, parse_head, parse_list, quoted_md5,
};
pub use store::{ListStats, S3Store};
pub use transport::{HttpResponse, Transport, TransportError};
pub use upload::S3Upload;
#[cfg(feature = "ureq")]
pub use ureq_transport::UreqTransport;

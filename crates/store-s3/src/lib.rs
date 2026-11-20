//! ObjectStore on an S3-compatible bucket, sans-IO half: configuration,
//! credentials that cannot print, signed requests as data, response parsers and
//! the error classification. The blocking HTTP client and the store itself come
//! after this layer; everything here is pure and testable without a network.

mod classify;
mod config;
mod credentials;
mod request;
mod response;
#[cfg(test)]
mod secrets;
mod transport;

pub use classify::{S3Code, classify, classify_transport, is_range_past_end};
pub use config::{
    BucketName, ConfigError, DEFAULT_REGION, Endpoint, PartSize, Prefix, Provider, Region,
    S3Config, UrlStyle,
};
pub use credentials::{S3Credentials, SecretString};
pub use request::{
    HttpRequest, MAX_DELETE_KEYS, Method, RequestBuilder, RequestError, UploadId, level_prefix,
    object_key, range_header, start_after,
};
pub use response::{
    EtagCheck, Head, ListPage, ListedFile, ResponseError, check_etag, hex, parse_complete,
    parse_create, parse_delete_failures, parse_head, parse_list, quoted_md5,
};
pub use transport::{HttpResponse, Transport, TransportError};

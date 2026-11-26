//! Transport-level helpers: only the test files that build requests by hand
//! declare this module (`#[path = "common/wire.rs"] mod wire;`).
use lsr_core::FileKey;
use lsr_ltx::Timestamp;
use lsr_store_s3::{HttpRequest, HttpResponse, RequestBuilder, Transport};

use crate::common::*;

pub fn builder() -> RequestBuilder {
    RequestBuilder::new(&config(), &credentials()).unwrap()
}

pub fn text(response: &HttpResponse) -> String {
    String::from_utf8_lossy(response.body()).into_owned()
}

/// PUTs `body` at `key` with created-at time `created_at` through `transport`.
pub fn put(
    transport: &impl Transport,
    key: &FileKey,
    body: &[u8],
    created_at: i64,
) -> HttpResponse {
    let request = builder()
        .put_object(
            now(),
            key,
            body.to_vec(),
            Timestamp::from_unix_millis(created_at),
        )
        .unwrap();
    transport.send(request).unwrap()
}

pub fn send(transport: &impl Transport, request: HttpRequest) -> HttpResponse {
    transport.send(request).unwrap()
}

//! The part size is chosen per upload from the object's length (`part_plan.rs`), so an
//! object that no 10000 parts of the configured size could carry is neither sent whole and
//! refused at the end, nor sent again for ever: the part grows to fit, and an object no part
//! size can carry is refused before a byte is sent.

use std::sync::Arc;

use lsr_core::conformance::block_on;
use lsr_core::{FakeClock, ObjectStore, StoreError, Upload, WriteMode};
use lsr_ltx::Timestamp;
use lsr_store_s3::{
    FakeS3, FaultTransport, HttpRequest, HttpResponse, S3Store, Transport, TransportError,
};

use crate::common::*;
use crate::real::PART;

const GIB: u64 = 1024 * 1024 * 1024;
const TIB: u64 = 1024 * GIB;

/// Passes every request to the fake, which the test can ask what it was sent.
struct Seen(Arc<FaultTransport<FakeS3>>);

impl Transport for Seen {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.0.send(request)
    }
}

fn store() -> (S3Store<Seen>, Arc<FaultTransport<FakeS3>>) {
    let wire = Arc::new(FaultTransport::new(fake()));
    let store = S3Store::with_transport(
        config(),
        credentials(),
        Seen(Arc::clone(&wire)),
        FakeClock::new(now()),
    )
    .unwrap();
    (store, wire)
}

fn begin(
    store: &S3Store<Seen>,
    len: Option<u64>,
) -> Result<lsr_store_s3::S3Upload<Seen>, StoreError> {
    block_on(store.begin_sized_upload(
        &key(0, 1, 1),
        Timestamp::from_unix_millis(5),
        WriteMode::Replace,
        len,
    ))
}

/// An object over 5 TiB is S3's to refuse, and every retry would send it all again; the
/// refusal names the object, its size and the limit, and nothing was sent.
#[test]
fn an_object_over_the_store_limit_is_refused_before_a_byte_is_sent() {
    let (store, wire) = store();
    let err = begin(&store, Some(5 * TIB + 1)).unwrap_err();
    assert_eq!(
        err,
        StoreError::ObjectTooLarge {
            key: key(0, 1, 1),
            len: 5 * TIB + 1,
            max: 5 * TIB,
        }
    );
    assert!(wire.requests().is_empty(), "{:?}", wire.requests());
}

/// The largest object S3 holds is carried: it is not refused.
#[test]
fn an_object_of_exactly_the_store_limit_begins() {
    let (store, wire) = store();
    begin(&store, Some(5 * TIB)).unwrap();
    assert!(wire.requests().is_empty());
}

/// A 60 GiB object needs parts larger than the 5 MiB it was configured with (a 5 MiB part
/// stops at 48.8 GiB): a write of one configured part starts no multipart upload, because the
/// part it is filling is larger.
#[test]
fn an_object_over_ten_thousand_configured_parts_fills_a_larger_part() {
    let (store, wire) = store();
    let mut upload = begin(&store, Some(60 * GIB)).unwrap();
    block_on(upload.write(&vec![0_u8; PART])).unwrap();
    assert!(wire.requests().is_empty(), "{:?}", wire.requests());
    drop(upload);
}

/// With no length given, or one that fits, the configured part is the part: a write of one
/// part starts the multipart upload.
#[test]
fn an_object_that_fits_is_cut_at_the_configured_part() {
    for len in [None, Some(60 * 1024 * 1024)] {
        let (store, wire) = store();
        let mut upload = begin(&store, len).unwrap();
        block_on(upload.write(&vec![0_u8; PART])).unwrap();
        // The create, and the part, sent as soon as it was whole.
        assert_eq!(wire.requests().len(), 2, "{len:?}: {:?}", wire.requests());
        drop(upload);
    }
}

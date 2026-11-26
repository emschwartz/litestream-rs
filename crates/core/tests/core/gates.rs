//! A restore's streams survive the store wrappers.
//!
//! A restore reads each file through one stream, one request on a bucket, only if every layer
//! between it and the store forwards the stream. A wrapper that forgets turns each 4 MiB chunk
//! into a request of its own without changing a single answer, so only the bill shows it. Here
//! a store that counts requests sits under the retry layer, and a restore of one file is held to
//! one request, not one per chunk (11 here).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use lsr_core::conformance::block_on;
use lsr_core::{
    MemSink, RecordingSleeper, RestoreTarget, RetryPolicy, Retrying, SeededEntropy, restore,
};

use crate::waves::{RESTORE_CHUNK, Waves, put_increment, snapshot_store};

#[test]
fn restore_through_the_retry_layer_reads_a_file_in_one_request() {
    let (inner, size) = snapshot_store();
    let waves = Waves::new(inner);
    let (requests, pieces) = (Arc::clone(&waves.requests), Arc::clone(&waves.pieces));
    let store = Retrying::new(
        waves,
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let restored = block_on(restore(&store, RestoreTarget::Latest, MemSink::new())).unwrap();
    assert_eq!(restored.files(), 1);
    let chunks = size.div_ceil(RESTORE_CHUNK);
    assert_eq!(pieces.load(Ordering::SeqCst), chunks, "one piece per chunk");
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "one request for the file"
    );
}

/// A replica of a snapshot and three level-0 files: a restore sends one request per file (four),
/// where reading a request per chunk sent one for each of the snapshot's 11 chunks.
#[test]
fn restore_of_a_chain_sends_one_request_per_file() {
    let (inner, size) = snapshot_store();
    for (n, pages) in [(2, 300), (3, 10), (4, 1)] {
        put_increment(&inner, n, pages);
    }
    let waves = Waves::new(inner);
    let (requests, pieces) = (Arc::clone(&waves.requests), Arc::clone(&waves.pieces));
    let store = Retrying::new(
        waves,
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let restored = block_on(restore(&store, RestoreTarget::Latest, MemSink::new())).unwrap();
    assert_eq!(restored.files(), 4);
    assert!(pieces.load(Ordering::SeqCst) > size.div_ceil(RESTORE_CHUNK));
    assert_eq!(requests.load(Ordering::SeqCst), 4, "one request per file");
}

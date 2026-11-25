//! Read-ahead survives the store wrappers.
//!
//! A restore reads a file's chunks side by side only if every layer between it and
//! the store forwards the store's batch width and its batch read. A wrapper that
//! forgets either one turns a restore serial without changing a single answer, so
//! nothing but the critical-path depth shows it. Here a store that models a link on a
//! virtual clock sits under the retry layer, and the restore's waves are held to
//! `ceil(chunks / width)`. A serial restore is one wave per chunk (11 here).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use lsr_core::conformance::block_on;
use lsr_core::{MemSink, RecordingSleeper, RestoreTarget, RetryPolicy, Retrying, SeededEntropy, restore};

use crate::waves::{RESTORE_CHUNK, WIDTH, Waves, snapshot_store};

#[test]
fn restore_through_the_retry_layer_reads_side_by_side() {
    let (inner, size) = snapshot_store();
    let waves = Waves::new(inner);
    let (rounds, reads) = (Arc::clone(&waves.rounds), Arc::clone(&waves.reads));
    let store = Retrying::new(
        waves,
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let restored = block_on(restore(&store, RestoreTarget::Latest, MemSink::new())).unwrap();
    assert_eq!(restored.files(), 1);
    let chunks = size.div_ceil(RESTORE_CHUNK);
    assert_eq!(reads.load(Ordering::SeqCst), chunks, "one read per chunk");
    let bound = chunks.div_ceil(WIDTH);
    let seen = rounds.load(Ordering::SeqCst);
    assert!(
        seen <= bound,
        "{seen} waves, bound {bound} for {chunks} chunks"
    );
}

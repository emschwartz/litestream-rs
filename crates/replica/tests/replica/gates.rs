//! A restore's streams survive the public restore entry point.
//!
//! `lsr_replica::restore` hands the store to core behind a trait object. If that erasure
//! dropped the store's stream, each 4 MiB chunk of a file would be a request of its own without
//! changing a single answer, and only the bill would show it. A store that counts requests is
//! restored from, and a restore of a snapshot and three level-0 files is held to one request per
//! file, not one per chunk (the snapshot alone is 11).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use lsr_replica::{Restore, restore};

use crate::waves::{RESTORE_CHUNK, Waves, put_increment, snapshot_store};

#[test]
fn restore_through_the_public_entry_reads_each_file_in_one_request() {
    let (inner, size) = snapshot_store();
    for (n, pages) in [(2, 300), (3, 10), (4, 1)] {
        put_increment(&inner, n, pages);
    }
    let waves = Waves::new(inner);
    let (requests, pieces) = (Arc::clone(&waves.requests), Arc::clone(&waves.pieces));
    let dir = tempfile::tempdir().unwrap();
    let restored = restore(&waves, &dir.path().join("db"), &Restore::default()).unwrap();
    assert_eq!(restored.files(), 4);
    assert!(pieces.load(Ordering::SeqCst) > size.div_ceil(RESTORE_CHUNK));
    assert_eq!(requests.load(Ordering::SeqCst), 4, "one request per file");
}

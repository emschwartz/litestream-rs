//! Read-ahead survives the public restore entry point.
//!
//! `lsr_replica::restore` hands the store to core behind a trait object. If that
//! erasure dropped the store's batch width or its batch read, a restore would turn
//! serial without changing a single answer, and only the critical-path depth would show
//! it. A store that models a link on a virtual clock is restored from, and the waves
//! are held to `ceil(chunks / width)`. A serial restore is one wave per chunk (11 here).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use lsr_replica::{RestoreTarget, restore};

use crate::waves::{RESTORE_CHUNK, WIDTH, Waves, snapshot_store};

#[test]
fn restore_through_the_public_entry_reads_side_by_side() {
    let (inner, size) = snapshot_store();
    let waves = Waves::new(inner);
    let (rounds, reads) = (Arc::clone(&waves.rounds), Arc::clone(&waves.reads));
    let dir = tempfile::tempdir().unwrap();
    let restored = restore(&waves, RestoreTarget::Latest, &dir.path().join("db")).unwrap();
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

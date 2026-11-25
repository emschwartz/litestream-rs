//! The counting allocator's process-wide mode, which the store-s3 memory gates rest on: a
//! gate that cannot see a worker thread's buffer, or that counts the test server's, would
//! pass for the wrong reason.
//!
//! Each test is its own process under nextest, so nothing else allocates meanwhile.

use std::thread;

use crate::heap::{LARGE_BLOCK, exclude_thread, large_allocs_of, process_peak_of};

const BIG: usize = 64 * 1024 * 1024;

#[test]
fn the_process_peak_sees_a_buffer_held_on_another_thread() {
    let ((), peak) = process_peak_of(|| {
        thread::scope(|scope| {
            scope.spawn(|| {
                let held = vec![1_u8; BIG];
                assert_eq!(held.len(), BIG);
            });
        });
    });
    assert!(peak.extra >= BIG, "saw {} bytes", peak.extra);
    assert!(peak.kept < BIG, "kept {} bytes", peak.kept);
}

#[test]
fn the_process_peak_skips_an_excluded_thread() {
    let ((), peak) = process_peak_of(|| {
        thread::scope(|scope| {
            scope.spawn(|| {
                exclude_thread();
                let held = vec![1_u8; BIG];
                assert_eq!(held.len(), BIG);
            });
        });
    });
    assert!(peak.extra < BIG / 2, "counted {} bytes", peak.extra);
}

/// The large-block count the compaction gate rests on sees whole blocks and growth, and
/// skips small ones: a gate that counted nothing would pass for the wrong reason.
#[test]
fn large_allocations_are_counted_by_block_and_by_growth() {
    let ((), large) = large_allocs_of(|| {
        let whole = vec![1_u8; LARGE_BLOCK];
        let mut grown: Vec<u8> = Vec::with_capacity(16);
        grown.extend_from_slice(&whole);
        let small = vec![1_u8; LARGE_BLOCK - 1];
        assert_eq!(grown.len() + small.len(), 2 * LARGE_BLOCK - 1);
    });
    // The block itself and the growth of the small buffer past the threshold.
    assert_eq!(large, 2);
}

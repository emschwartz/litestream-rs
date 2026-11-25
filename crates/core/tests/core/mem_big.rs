//! The memory gates at the sizes where a leak is unmistakable: databases of 1 to 4 GiB
//! and an inventory of a million files. They are `#[ignore]`d out of `just check`
//! (minutes in a debug build, gigabytes of disk); `just mem-big` runs them in release
//! and the nightly workflow runs that recipe. The laws are the ones `mem.rs` holds at
//! small sizes, with the read width the S3 store really uses.

use crate::mem::{
    assert_planning_borrows, assert_restore_flat_in_size, assert_retention_borrows,
    assert_snapshot_law,
};

/// The read width of the S3 store, so the window is the real one: sixteen 1 MiB chunks.
const S3_WIDTH: usize = 16;

/// 4 KiB pages in a mebibyte.
const PAGES_PER_MIB: u32 = 256;

/// The smaller database of the restore gate; the larger is four times it.
const RESTORE_BASE_MIB: u32 = 1024;

#[test]
#[ignore = "large; just mem-big"]
fn restore_peak_heap_is_flat_from_one_to_four_gib() {
    assert_restore_flat_in_size(RESTORE_BASE_MIB * PAGES_PER_MIB, S3_WIDTH);
}

#[test]
#[ignore = "large; just mem-big"]
fn snapshot_law_holds_from_256_mib_to_one_gib() {
    assert_snapshot_law(256 * PAGES_PER_MIB);
}

#[test]
#[ignore = "large; just mem-big"]
fn planning_a_million_files_copies_none_of_them() {
    assert_planning_borrows(1_000_000);
}

#[test]
#[ignore = "large; just mem-big"]
fn retention_over_a_million_files_copies_none_of_them() {
    assert_retention_borrows(1_000_000);
}

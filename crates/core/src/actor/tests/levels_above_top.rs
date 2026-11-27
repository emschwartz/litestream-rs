//! The levels above the configured top (4 to 8 by default) hold a file only when an earlier
//! configuration or another process left one there. The boot's listing of every level shows
//! which do; a whole maintenance listing lists only those, with the configured levels and the
//! snapshots, so retention still sees a file an earlier configuration stranded there (Go never
//! lists those levels, `store.go:871-879`).

use super::*;

/// A replica whose listings carry only a last-modified time, as S3's do.
fn s3_like(rig: &mut Rig) {
    rig.remote = Counting {
        inner: MemStore::with_listed_last_modified(),
        commit_says_created: true,
        ..Counting::default()
    };
}

fn lists_at(rig: &Rig, from: usize, level: u8) -> usize {
    rig.remote
        .requests_from(from)
        .get(&(Request::List, level))
        .copied()
        .unwrap_or_default()
}

/// A first start lists each level above the top once, at boot, not once per whole listing.
/// The snapshot is planned from the boot's listing
/// (`docs/decisions/D287.md`), so only two whole listings remain, the boot's and the one after
/// the snapshot: the levels it lists (0 to 3 and 9) twice.
#[test]
fn a_first_start_lists_the_levels_above_the_top_once() {
    let mut rig = super::first_start::quiet();
    s3_like(&mut rig);
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT).len(), 1);
    for level in [0, 1, 2, 3, 9] {
        assert_eq!(lists_at(&rig, 0, level), 2, "level {level}");
    }
    for level in 4..=8 {
        assert_eq!(lists_at(&rig, 0, level), 1, "level {level}");
    }
}

/// A file an earlier configuration left at level 5 keeps level 5 in every whole listing, so
/// retention and the anchor see it; level 4, empty at boot, is listed by the boot alone.
#[test]
fn a_level_above_the_top_that_holds_a_file_is_listed_by_every_whole_listing() {
    let mut rig = super::first_start::quiet();
    s3_like(&mut rig);
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    let zero = Rig::files(&rig.remote, Level::ZERO);
    let [first] = zero.as_slice() else {
        panic!("one level-0 file expected: {zero:?}");
    };
    let bytes = super::second_writer::read_whole(&rig.remote.inner, &first.key);
    let stray = FileKey::new(Level::new(5).unwrap(), first.key.range());
    super::second_writer::put_whole(&rig.remote.inner, &stray, &bytes);
    let mark = rig.remote.requests_made();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0 + 1_000);
    let whole = lists_at(&rig, mark, 0);
    assert!(
        whole >= 2,
        "the boot and a maintenance pass list level 0: {whole}"
    );
    assert_eq!(lists_at(&rig, mark, 5), whole);
    assert_eq!(lists_at(&rig, mark, 4), 1);
}

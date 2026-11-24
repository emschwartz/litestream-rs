//! The level-0 rule of the short view against the whole rules: whatever it deletes from the
//! files a short listing returned, the rules on the whole listing delete too, and the newest
//! restore still plans without it.

use super::*;
use crate::maintain::retain::short_level_zero;

/// What `short_level_zero` says about `view`, from a fresh state.
fn short(
    view: &Inventory,
    now: Timestamp,
    cfg: &Maintenance,
    published: Option<Txid>,
) -> ShortZero {
    match short_level_zero(
        view,
        now,
        cfg,
        published,
        &MaintainState::default(),
        &ResolvedCreatedAt::default(),
    ) {
        Resolve::Done(zero) => zero,
        Resolve::Need(file) => panic!("a stored listing asked for a created-at time: {file:?}"),
    }
}

/// The files a short listing returns: level 0 and level 9 whole, and at each level 1 to 8 the
/// files after a cursor `b` picks anywhere in the level.
fn view_of(inventory: &Inventory, b: &mut Bytes) -> Inventory {
    let mut files: Vec<FileInfo> = Vec::new();
    files.extend_from_slice(inventory.level(Level::ZERO));
    files.extend_from_slice(inventory.level(Level::SNAPSHOT));
    for n in 1..=8 {
        let all = inventory.level(level(n));
        let cut = b.index(all.len().saturating_add(1));
        files.extend_from_slice(all.get(cut..).unwrap_or_default());
    }
    inv(files)
}

/// One generated listing: the short rule's deletes are the whole rule's, or fewer, and
/// restoring the newest state still works without them. Only a listing the whole gate lets
/// through counts: the short rule runs only while the last whole listing passed it.
fn check_short(data: &[u8], weird: bool, which: usize) {
    let listing = build(data, weird);
    if listing.files.is_empty() {
        return;
    }
    let mut inventory = inv(listing.files.clone());
    if let Some((l, count)) = listing.skipped {
        inventory = inventory.with_skipped(l, count);
    }
    let mut b = Bytes { data, at: 7 };
    let now = at(pick_now(&mut b, inventory.files()));
    let cfg = &configs()[which];
    let allowed: Vec<FileKey> = match retention(&inventory, now, cfg, listing.published) {
        Retention::Delete(job) => job.keys().to_vec(),
        Retention::Wait { .. } => Vec::new(),
        Retention::Held(_) => return,
    };
    let view = view_of(&inventory, &mut b);
    let zero = short(&view, now, cfg, listing.published);
    for key in &zero.keys {
        assert!(
            key.level() == Level::ZERO && allowed.contains(key),
            "the short view deletes {key}, the whole rules keep it: {allowed:?}"
        );
    }
    let Some(newest) = inventory.newest() else {
        return;
    };
    let outcome = plan_without(&inventory, &zero.keys, KeyTarget::Latest);
    assert!(
        restores(&outcome, newest),
        "the newest state does not plan without {:?}: {outcome:?}",
        zero.keys
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(600))]

    #[test]
    fn the_short_views_deletes_are_a_subset_of_the_whole_rules(
        data in prop::collection::vec(any::<u8>(), 96..400),
        weird in any::<bool>(),
        which in 0usize..4,
    ) {
        check_short(&data, weird, which);
    }
}

fn ladder_now() -> Timestamp {
    at(newest_created_at(&ladder()))
}

/// The ladder as a whole listing and as a short one with the same files: the same level-0
/// deletes.
#[test]
fn a_short_view_of_everything_deletes_what_the_whole_rules_delete_from_level_zero() {
    let zero = short(&inv(ladder()), ladder_now(), &defaults(), None);
    assert_eq!(
        zero.keys,
        (25..=29).map(|n| key(0, n, n)).collect::<Vec<_>>()
    );
}

/// A level-0 file counts as covered only by a file the pass listed. The level 1 file over
/// 21 to 30 is not in this view, so nothing above the floor is covered.
#[test]
fn a_cover_that_was_not_listed_covers_nothing() {
    let files: Vec<FileInfo> = ladder()
        .into_iter()
        .filter(|f| f.key != key(1, 21, 30))
        .collect();
    let zero = short(&inv(files), ladder_now(), &defaults(), None);
    assert_eq!(zero.keys, Vec::<FileKey>::new());
}

/// The whole rules skip an uncovered level-0 file; the short rule stops at it when it is
/// young, which only deletes less.
#[test]
fn the_scan_stops_at_the_first_young_file_covered_or_not() {
    let mut files = ladder();
    // Level 0 file 27 is young: created at the newest time. Files after it are covered too.
    let newest = at(newest_created_at(&files));
    for f in &mut files {
        if f.key == key(0, 27, 27) {
            set_created_at(f, newest);
        }
    }
    let zero = short(&inv(files), ladder_now(), &defaults(), None);
    assert_eq!(zero.keys, vec![key(0, 25, 25), key(0, 26, 26)]);
    assert_eq!(zero.settled_min, Some(txid(27)));
}

/// Nothing is deleted while a restore point is pinned or an output is distrusted: those
/// decisions read the whole history.
#[test]
fn a_pinned_restore_point_leaves_level_zero_to_the_whole_listing() {
    let mut state = MaintainState::default();
    state.set_kept_restore_point(Some(txid(30)));
    let got = short_level_zero(
        &inv(ladder()),
        ladder_now(),
        &defaults(),
        None,
        &state,
        &ResolvedCreatedAt::default(),
    );
    let Resolve::Done(zero) = got else {
        panic!("asked for a created-at time");
    };
    assert!(zero.keys.is_empty());
}

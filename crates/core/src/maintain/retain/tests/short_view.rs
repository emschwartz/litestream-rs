//! The level-0 rule of the short view against the whole rules: whatever it deletes from the
//! files a short listing returned, the rules on the whole listing delete too, and the newest
//! restore still plans without it.

use super::*;
use crate::maintain::retain::short_level_zero;

/// What `short_level_zero` says about `view`, from a fresh state.
fn short(
    view: &Inventory,
    known: &[FileInfo],
    now: Timestamp,
    cfg: &Maintenance,
    published: Option<Txid>,
) -> ShortZero {
    match short_level_zero(
        view,
        known,
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
    // Half the time the newest file of each level is the one the tail did not return: what the
    // engine knows of it is a remembered record.
    let hide_newest = b.percent(50);
    let mut files: Vec<FileInfo> = Vec::new();
    files.extend_from_slice(inventory.level(Level::ZERO));
    files.extend_from_slice(inventory.level(Level::SNAPSHOT));
    for n in 1..=8 {
        let all = inventory.level(level(n));
        let cut = b.index(all.len().saturating_add(1));
        let tail = all.get(cut..).unwrap_or_default();
        let tail = if hide_newest {
            tail.split_last().map_or(tail, |(_, rest)| rest)
        } else {
            tail
        };
        files.extend_from_slice(tail);
    }
    inv(files)
}

/// The newest file of each level 1 to 8: what the engine remembers from its whole listing.
fn newest_per_level(inventory: &Inventory) -> Vec<FileInfo> {
    (1..=8)
        .filter_map(|n| inventory.level(level(n)).last().copied())
        .collect()
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
    // The shape the first version of this test never drew: the newest file of a level 1 to 8 is
    // younger than the newest level-0 file (a Go compaction stamped at compaction time), which
    // lifts the whole rules' anchor above what level 0 and the snapshots alone show.
    let mut sane = listing.sane;
    if b.percent(40) {
        let n = u8::try_from(1 + b.below(8)).unwrap_or(1);
        let young = newest_created_at(inventory.files())
            + i64::try_from(b.below(3 * 24)).unwrap_or(0) * HOUR;
        let mut files = inventory.files().to_vec();
        if let Some(last) = inventory.level(level(n)).last()
            && let Some(f) = files.iter_mut().find(|f| f.key == last.key)
        {
            set_created_at(f, at_created_at(young));
            inventory = inv(files);
            // Created-at times no longer follow the transactions, so instants are not judged.
            sane = false;
        }
    }
    let now = at(pick_now(&mut b, inventory.files()));
    let cfg = &configs()[which];
    // A listing the whole gate holds is the stale-check case: the short rule runs on the last
    // clear check, and the listing may have become one the gate would hold since. There the
    // property is the weaker one every delete must keep: what restored before restores after.
    let (allowed, stale): (Vec<FileKey>, bool) =
        match retention(&inventory, now, cfg, listing.published) {
            Retention::Delete(job) => (job.keys().to_vec(), false),
            Retention::Wait { .. } => (Vec::new(), false),
            Retention::Held(_) => (Vec::new(), true),
        };
    let view = view_of(&inventory, &mut b);
    let known = newest_per_level(&inventory);
    let zero = short(&view, &known, now, cfg, listing.published);
    for key in &zero.keys {
        assert!(
            key.level() == Level::ZERO && (stale || allowed.contains(key)),
            "the short view deletes {key}, the whole rules keep it: {allowed:?}"
        );
    }
    let Some(newest) = inventory.newest() else {
        return;
    };
    // A listing that never planned the newest state (a hole, a gap) restores nothing before or after.
    if !restores(&plan(&inventory, KeyTarget::Latest), newest) {
        return;
    }
    if !zero.keys.is_empty()
        && retained_from(&inventory, now, cfg).is_some()
        && let Ok(job) = DeleteJob::new(zero.keys.clone())
    {
        check_delete(&inventory, &job, now, cfg, sane);
    }
    let outcome = plan_without(&inventory, &zero.keys, KeyTarget::Latest);
    assert!(
        restores(&outcome, newest),
        "the newest state does not plan without {:?}: {outcome:?}",
        zero.keys
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

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
    let zero = short(&inv(ladder()), &[], ladder_now(), &defaults(), None);
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
    let zero = short(&inv(files), &[], ladder_now(), &defaults(), None);
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
    let zero = short(&inv(files), &[], ladder_now(), &defaults(), None);
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
        &[],
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

/// The reviewer's case: the newest level 1 file (a Go compaction stamped at compaction time) is
/// younger than the newest level-0 file, so the whole rules' anchor is that file's time and the
/// floor is 20. A short view that anchors on the level-0 file and the snapshots alone reads an
/// earlier time, a floor of 10, and deletes level-0 files 18 to 21, which the whole rules
/// keep. Reading the remembered newest level 1 file for its age makes the two agree.
#[test]
fn the_anchor_reads_the_newest_known_file_of_each_level_so_the_short_view_never_deletes_more() {
    let t = crate::maintain::tests::T0;
    let files = vec![
        crate::maintain::tests::file(9, 1, 10, t),
        crate::maintain::tests::file(9, 1, 20, t + 2 * DAY),
        crate::maintain::tests::file(9, 1, 30, t + 2 * DAY + 12 * HOUR),
        crate::maintain::tests::file(1, 11, 14, t + 3 * DAY + HOUR),
        crate::maintain::tests::file(1, 15, 19, t + 3 * DAY + HOUR),
        crate::maintain::tests::file(1, 20, 30, t + 3 * DAY + HOUR),
        crate::maintain::tests::file(0, 18, 21, t + 2 * DAY + HOUR),
        crate::maintain::tests::file(0, 31, 31, t + 2 * DAY + 13 * HOUR),
    ];
    let now = at(t + 3 * DAY + 2 * HOUR);
    let cfg = defaults();
    let whole = match retention(&inv(files.clone()), now, &cfg, None) {
        Retention::Delete(job) => job.keys().to_vec(),
        Retention::Wait { .. } => Vec::new(),
        Retention::Held(hold) => panic!("held: {hold:?}"),
    };
    assert!(
        !whole.contains(&key(0, 18, 21)),
        "the whole rules keep level 0 18 to 21: {whole:?}"
    );
    // The short listing returns every file here, covers included. The anchor must still read
    // the newest level 1 file, which the pass knows whether it was listed or remembered.
    let view = inv(files.clone());
    let known = [crate::maintain::tests::file(1, 20, 30, t + 3 * DAY + HOUR)];
    let zero = short(&view, &known, now, &cfg, None);
    assert!(
        !zero.keys.contains(&key(0, 18, 21)),
        "the short view deletes what the whole rules keep: {:?}",
        zero.keys
    );
}
/// A listing behind what was published is a hold, and a hold leaves level 0 to the whole
/// listing: the same files are deleted once the listing has caught up.
#[test]
fn a_listing_behind_what_was_published_deletes_nothing_from_level_zero() {
    let behind = short(
        &inv(ladder()),
        &[],
        ladder_now(),
        &defaults(),
        Some(txid(31)),
    );
    assert!(behind.keys.is_empty(), "{:?}", behind.keys);
    let caught_up = short(
        &inv(ladder()),
        &[],
        ladder_now(),
        &defaults(),
        Some(txid(30)),
    );
    assert_eq!(caught_up.keys.len(), 5);
}

/// A covered level-0 file too young to delete wakes the pass when it ages out, if the clock reads
/// earlier than the newest created-at time so that the file ages out before it.
#[test]
fn a_young_covered_file_wakes_the_pass_when_it_ages_out() {
    let mut files = ladder();
    let now = newest_created_at(&files) - HOUR;
    for f in &mut files {
        if f.key == key(0, 29, 29) {
            set_created_at(f, at(now - 100_000));
        }
    }
    let zero = short(&inv(files), &[], at(now), &defaults(), None);
    assert_eq!(
        zero.keys,
        (25..=28).map(|n| key(0, n, n)).collect::<Vec<_>>()
    );
    // Created 100 s before the clock, and level 0 is kept 300 s.
    assert_eq!(zero.wake, Some(at(now + 200_000)));
    assert_eq!(zero.settled_min, Some(txid(29)));
}

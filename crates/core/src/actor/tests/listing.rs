//! How often maintenance lists the replica under a steady writer.
//!
//! A listing is one LIST call per level (ten), and maintenance plans every
//! job from one. An upload must not make the next listing due at once: a
//! database that commits every second would list ten times a second. Go lists
//! on each compaction level's window and on its level-0 retention check
//! interval (`store.go:591-680`), and so does the actor.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test clocks and counts are small bounded values"
)]

use super::*;

/// What one tick did, as the store saw it.
struct Seen {
    at: i64,
    event: Option<Event>,
    lists: u32,
    /// Keys the replica was asked to delete during the tick.
    deleted: usize,
}

/// A host that commits once a second for `seconds` and drives the actor as
/// a daemon does: again at once on `Next::Now`, at the asked time when that
/// comes before the next commit, else at the next commit (the WAL poll).
fn write_every_second(rig: &mut Rig, actor: &mut Actor, start: i64, seconds: i64) -> Vec<Seen> {
    let mut seen = Vec::new();
    for s in 0..seconds {
        let second = start + s * 1000;
        rig.commit(u8::try_from(s % 250 + 1).unwrap());
        let mut now = second;
        for _ in 0..100 {
            let (lists, deleted) = (rig.remote.lists(), rig.remote.deleted().len());
            let tick = rig.tick(actor, now);
            seen.push(Seen {
                at: now,
                event: event(&tick).cloned(),
                lists: rig.remote.lists() - lists,
                deleted: rig.remote.deleted().len() - deleted,
            });
            match next_of(&tick) {
                Some(Next::Now) => {}
                Some(Next::At(at)) if at.unix_millis() < second + 1000 => {
                    now = now.max(at.unix_millis());
                }
                Some(Next::At(_) | Next::OnWalChange) | None => break,
            }
        }
    }
    seen
}

fn level(n: u8) -> Level {
    Level::new(n).unwrap()
}

/// When each compaction into `dst` was published.
fn compactions(seen: &[Seen], dst: Level) -> Vec<i64> {
    seen.iter()
        .filter_map(|s| match &s.event {
            Some(Event::Maintained {
                event: MaintainedEvent::Compacted(info),
                ..
            }) if info.key.level() == dst => Some(s.at),
            _ => None,
        })
        .collect()
}

const SECONDS: i64 = 600;
/// Go's level-1 window and level-0 retention check interval.
const WINDOW: i64 = 30;
const CHECK: i64 = 15;

/// Listing calls a steady writer may cause in [`SECONDS`], counted in
/// listings of every level. Each check interval wakes one listing, and the
/// level-0 delete it may plan spends it, so one more follows; each window
/// boundary that comes before the interval is up wakes one, and the
/// compaction it plans is followed by one more. The constant covers the
/// other levels' compactions and the snapshot. Listing once per upload makes
/// about one a second: 600 here, 9450 calls in all on the old code.
fn listing_bound() -> u32 {
    let passes = 2 * (SECONDS / CHECK) + 2 * (SECONDS / WINDOW) + 8;
    u32::try_from(passes).unwrap() * u32::try_from(Level::ALL.len()).unwrap()
}

/// The defect: every upload made maintenance list the whole replica on the
/// next tick, ten LIST calls a second for a writer committing once a second.
/// Now an upload makes the next listing due at the earlier of the next
/// compaction window and the level-0 retention check interval, and
/// compaction and retention still run on their schedule.
#[test]
fn a_steady_writer_lists_once_per_check_interval_not_per_upload() {
    let (mut rig, mut actor) = caught_up();
    let lists_before = rig.remote.lists();
    let seen = write_every_second(&mut rig, &mut actor, T0 + 1000, SECONDS);
    let lists = rig.remote.lists() - lists_before;
    assert!(
        lists <= listing_bound(),
        "{lists} listing calls in {SECONDS} s, bound {}",
        listing_bound()
    );
    // Compaction keeps its window: a level-1 output in every 30 s window,
    // never more than a window and a check interval after the last one.
    let l1 = compactions(&seen, level(1));
    assert!(
        l1.len() >= usize::try_from(SECONDS / WINDOW - 1).unwrap(),
        "{l1:?}"
    );
    for pair in l1.windows(2) {
        assert!(pair[1] - pair[0] <= (WINDOW + CHECK) * 1000, "{l1:?}");
    }
    assert!(!compactions(&seen, level(2)).is_empty(), "level 2 compacts");
    // Retention keeps its schedule: level 0 holds about its 5 minutes and no
    // more, and every delete was planned from a listing made in its own tick.
    let deleting: Vec<&Seen> = seen.iter().filter(|s| s.deleted > 0).collect();
    assert!(!deleting.is_empty(), "level-0 retention deletes");
    for s in &deleting {
        assert!(
            s.lists >= u32::try_from(Level::ALL.len()).unwrap(),
            "a delete at {} without a full listing first ({} calls)",
            s.at,
            s.lists
        );
    }
    let l0 = Rig::files(&rig.remote, Level::ZERO).len();
    assert!(l0 <= 300 + 2 * 30 + 15, "{l0} level-0 files kept");
}

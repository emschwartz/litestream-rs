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
use crate::L0Retention;
use crate::maintain::FULL_LISTING_INTERVAL;

/// What one tick did, as the store saw it.
struct Seen {
    at: i64,
    event: Option<Event>,
    lists: u32,
    /// Keys the replica was asked to delete during the tick.
    deleted: usize,
}

/// An application that commits once a second for `seconds` and drives the actor as
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
    let deleting: Vec<usize> = (0..seen.len()).filter(|i| seen[*i].deleted > 0).collect();
    assert!(!deleting.is_empty(), "level-0 retention deletes");
    // A delete may be planned from the listing the pass before it carried over.
    drop(deleting);
    let l0 = Rig::files(&rig.remote, Level::ZERO).len();
    assert!(l0 <= 300 + 2 * 30 + 15, "{l0} level-0 files kept");
}
// A replica with a long history: what a steady writer's listings cost.

/// Go's default levels with the snapshot retention of `days` days.
fn retained(days: u64) -> Maintenance {
    Maintenance::new(
        vec![
            Duration::from_secs(30),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        ],
        Duration::from_secs(86_400),
        Duration::from_secs(days * 86_400),
        L0Retention::After(Duration::from_secs(300)),
    )
    .unwrap()
}

/// A valid file covering `range` at `level`, with a page-1 image of `n`.
fn ltx_file(range: TxidRange, n: u8) -> Vec<u8> {
    use lsr_ltx::{Encoder, Header, HeaderFields};
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: PS,
        commit: 3,
        txids: range,
        timestamp: ts(T0),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for page in 1..=3 {
        encoder.write_page(pg(page), &vec![n; PS.bytes()]).unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// Moves every level-0 file of the replica but the newest `keep` to level 1 under the same
/// range, and drops the same staged files. The last transaction moved.
fn turn_into_level_one(rig: &Rig, keep: usize) -> Option<Txid> {
    let files = Rig::files(&rig.remote, Level::ZERO);
    let old = files.get(..files.len().saturating_sub(keep))?;
    for info in old {
        let bytes = super::second_writer::read_whole(&rig.remote.inner, &info.key);
        let key = FileKey::new(level(1), info.key.range());
        super::second_writer::put_whole(&rig.remote.inner, &key, &bytes);
    }
    let keys: Vec<FileKey> = old.iter().map(|info| info.key).collect();
    block_on(rig.remote.inner.delete(&keys)).unwrap();
    block_on(rig.staging.inner.delete(&keys)).unwrap();
    old.last().map(|info| info.key.range().max())
}

/// A booted actor over a replica whose level 1 holds one file per transaction for `files`
/// transactions, as a week of retention leaves it, with levels 2 and 3 and the snapshot
/// covering them. Built directly in the store: only the last transactions go through the
/// actor, so the history costs no ticks.
fn long_history(files: u64, maintenance: Maintenance) -> (Rig, Actor, i64) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(ActorConfig {
        maintenance,
        ..cfg()
    });
    rig.settle(&mut actor, T0);
    let mut through = Txid::MIN;
    for n in 2..=files {
        rig.commit(u8::try_from(n % 250 + 1).unwrap());
        rig.settle(&mut actor, T0);
        // Every few hundred transactions level 0 becomes level 1, one file per
        // transaction but for the newest few, and staging lets go of what was uploaded:
        // what maintenance does over time, here without a tick for each file.
        if n % 500 == 0 || n == files {
            through = turn_into_level_one(&rig, 10).unwrap_or(through);
        }
    }
    // The higher levels and the snapshot cover them, each one file over everything.
    let whole = TxidRange::new(Txid::MIN, through).unwrap();
    for l in [2, 3, 9] {
        let key = FileKey::new(level(l), whole);
        let bytes = ltx_file(whole, 1);
        super::second_writer::put_whole(&rig.remote.inner, &key, &bytes);
    }
    // A listing sees them: the actor lists whole at its next pass.
    let mut now = T0 + 1000;
    for _ in 0..200 {
        let tick = rig.tick(&mut actor, now);
        match next_of(&tick) {
            Some(Next::At(at)) if at.unix_millis() <= T0 + 3_600_000 => {
                now = now.max(at.unix_millis());
            }
            Some(Next::Now) => {}
            _ => break,
        }
    }
    (rig, actor, now)
}

/// LIST requests and listed entries an hour of a steady writer may cost over a long history.
/// Before maintenance listed only what a check reads, this hour cost 10,773 requests and
/// 2.95 million entries at 2,900 level-1 files, and 25,410 requests and 17.9 million at
/// 20,000 (issue 324). After: about 3,200 requests and 230,000 to 300,000 entries at either
/// size. A pass lists level 0 whole, level 9 whole and a tail of each compaction level (five
/// requests with the default three levels); whole listings stay to boot, faults, retention
/// and one every [`FULL_LISTING_INTERVAL`], about 30 requests each at 20,000 files.
const REQUESTS_PER_HOUR: u32 = 600;
const ENTRIES_PER_HOUR: u64 = 230_000;

/// Runs the hour a minute at a time, stopping as soon as a bound is passed: a replica that
/// lists everything at every pass would otherwise take minutes to prove it.
fn an_hour_stays_within_the_bounds(files: u64, days: u64) {
    let (mut rig, mut actor, mut now) = long_history(files, retained(days));
    let (lists, entries) = (rig.remote.lists(), rig.remote.listed());
    for minute in 0..60 {
        write_every_second(&mut rig, &mut actor, now + 1000, 60);
        now += 60_000;
        let (lists, entries) = (rig.remote.lists() - lists, rig.remote.listed() - entries);
        assert!(
            lists <= REQUESTS_PER_HOUR && entries <= ENTRIES_PER_HOUR,
            "{lists} LIST requests and {entries} entries after {} minutes at {files} level-1 files, \
             bounds {REQUESTS_PER_HOUR} and {ENTRIES_PER_HOUR} an hour",
            minute + 1
        );
    }
}

/// The defect (issue 324): with a week of retention, 20,000 level-1 files, every check listed
/// all of them again, 21 requests a pass, 14 passes a minute.
#[test]
fn an_hour_of_uploads_over_20000_level_one_files_lists_a_bounded_number_of_requests() {
    an_hour_stays_within_the_bounds(20_000, 7);
}

/// The same with a day of retention, as an upstream user reported.
#[test]
fn an_hour_of_uploads_over_2900_level_one_files_lists_a_bounded_number_of_requests() {
    an_hour_stays_within_the_bounds(2_900, 1);
}

// What short listings still catch, and what waits for the whole one.

/// Runs one second of the writer from `at` and says what the pass saw: whether any maintenance
/// pass listed the whole replica (every level), only a tail, and whether the engine stopped.
struct Second {
    whole: bool,
    short: bool,
    stopped: bool,
}

fn one_second(rig: &mut Rig, actor: &mut Actor, at: i64) -> Second {
    let seen = write_every_second(rig, actor, at, 1);
    let all = u32::try_from(Level::ALL.len()).unwrap();
    Second {
        whole: seen.iter().any(|s| s.lists >= all),
        short: seen.iter().any(|s| s.lists > 0 && s.lists < all),
        stopped: seen.iter().any(|s| s.event.is_none()),
    }
}

/// Runs the writer until a pass lists the whole replica; returns the next second to run.
fn until_a_whole_listing(rig: &mut Rig, actor: &mut Actor, mut at: i64) -> i64 {
    for _ in 0..2_000 {
        let second = one_second(rig, actor, at);
        at += 1000;
        if second.whole {
            return at;
        }
    }
    panic!("no whole listing in 2000 s");
}

/// A level-2 file over `1..=1_000_000`: another writer's, past this engine's position, and
/// sorted before any level-2 file the engine writes after its first.
fn put_foreign_level_two(rig: &Rig) {
    let range = TxidRange::new(Txid::MIN, Txid::new(1_000_000).unwrap()).unwrap();
    let key = FileKey::new(level(2), range);
    super::second_writer::put_whole(&rig.remote.inner, &key, &ltx_file(range, 9));
}

/// Another writer's file at level 0 is the first thing a live second writer leaves, and
/// level 0 is listed whole by every pass: the next pass stops the engine, without a whole
/// listing.
#[test]
fn another_writers_level_zero_file_is_caught_by_the_next_short_pass() {
    let (mut rig, mut actor) = caught_up();
    let mut at = until_a_whole_listing(&mut rig, &mut actor, T0 + 1000);
    for _ in 0..90 {
        let second = one_second(&mut rig, &mut actor, at);
        at += 1000;
        assert!(!second.stopped && !second.whole);
    }
    super::second_writer::put_foreign(&rig, 1_000_000);
    let mut caught_after = None;
    for s in 0..60 {
        let second = one_second(&mut rig, &mut actor, at);
        at += 1000;
        assert!(!second.whole, "a whole listing at {s} s");
        if second.stopped {
            caught_after = Some(s);
            break;
        }
    }
    assert!(caught_after.is_some(), "the engine never stopped");
}

/// What a short listing cannot see: another writer's compaction output that sorts before the
/// newest file this engine knows at its level. It is found by the next whole listing, at most
/// [`FULL_LISTING_INTERVAL`] after the last one, and the engine then stops.
#[test]
fn another_writers_compaction_output_below_the_newest_known_waits_for_the_whole_listing() {
    // Levels 2 and 3 already hold files, as on a replica that has run for a while: with
    // none at level 3 the engine lists all of level 2 for its compaction.
    let (mut rig, mut actor, mut at) = long_history(300, retained(1));
    at += 1000;
    // Two level-2 outputs: the second is the newest this engine knows, and the foreign file
    // sorts before it.
    for _ in 0..1_000 {
        one_second(&mut rig, &mut actor, at);
        at += 1000;
    }
    assert!(Rig::files(&rig.remote, level(2)).len() >= 2);
    at = until_a_whole_listing(&mut rig, &mut actor, at);
    put_foreign_level_two(&rig);
    let put_at = at;
    let (mut short_passes, mut stopped_at) = (0, None);
    while stopped_at.is_none()
        && at < put_at + 2 * i64::try_from(FULL_LISTING_INTERVAL.as_millis()).unwrap()
    {
        let second = one_second(&mut rig, &mut actor, at);
        short_passes += i32::from(second.short);
        if second.stopped {
            stopped_at = Some(at);
        }
        at += 1000;
    }
    let stopped_at = stopped_at.expect("the whole listing never saw the file");
    let waited = stopped_at - put_at;
    let longest = i64::try_from(FULL_LISTING_INTERVAL.as_millis()).unwrap() + 31_000;
    assert!(
        waited <= longest,
        "caught {waited} ms after, at most {longest}"
    );
    // Short passes ran meanwhile and did not see it: it was not found at once.
    assert!(short_passes >= 10, "{short_passes} short passes");
    assert!(waited >= 30_000, "found after {waited} ms by a short pass");
}

/// A fault drops what the engine remembers of the replica: the next maintenance pass lists
/// every level.
#[test]
fn a_fault_drops_the_remembered_files_and_the_next_pass_lists_the_whole_replica() {
    let (mut rig, mut actor) = caught_up();
    let at = until_a_whole_listing(&mut rig, &mut actor, T0 + 1000);
    let mut at = at;
    // Short passes now.
    let mut short = false;
    for _ in 0..120 {
        short |= one_second(&mut rig, &mut actor, at).short;
        at += 1000;
    }
    assert!(short, "no short pass before the fault");
    // The replica refuses uploads for a while: the upload task faults.
    rig.remote
        .refuse_uploads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for _ in 0..5 {
        one_second(&mut rig, &mut actor, at);
        at += 1000;
    }
    rig.remote
        .refuse_uploads
        .store(false, std::sync::atomic::Ordering::SeqCst);
    // The first maintenance listing after it covers the whole replica.
    let all = u32::try_from(Level::ALL.len()).unwrap();
    for _ in 0..200 {
        let seen = write_every_second(&mut rig, &mut actor, at, 1);
        at += 1000;
        if let Some(first) = seen.iter().find(|s| s.lists > 0) {
            assert!(
                first.lists >= all,
                "a pass of {} requests after the fault",
                first.lists
            );
            return;
        }
    }
    panic!("no listing after the fault");
}

/// An input that is gone when a compaction runs means another writer is changing the replica:
/// the next pass lists the whole replica instead of a tail.
#[test]
fn a_compaction_that_finds_an_input_gone_owes_a_whole_listing() {
    use crate::maintain::{DeleteJob, Job};
    let (mut rig, mut actor) = caught_up();
    let mut at = until_a_whole_listing(&mut rig, &mut actor, T0 + 1000);
    one_second(&mut rig, &mut actor, at);
    at += 1000;
    assert!(actor.short_pass_ok(ts(at)), "short passes are on");
    let range = TxidRange::new(Txid::MIN, Txid::MIN).unwrap();
    let job = Job::Delete(DeleteJob::new(vec![FileKey::new(level(1), range)]).unwrap());
    let tick = Tick::Ran {
        event: Event::Maintained {
            event: MaintainedEvent::Replan,
            created_at_read: 0,
        },
        next: Next::Now,
    };
    actor.after_job(&job, &tick, None, ts(at));
    assert!(
        !actor.short_pass_ok(ts(at)),
        "the next pass is a whole listing"
    );
}

/// The backstop that bounds how long a foreign file can go unseen is measured on the engine's
/// clock, which holds after a step back: a step back forces a whole listing at once instead of
/// stretching the backstop by the size of the step.
#[test]
fn a_clock_stepped_back_forces_a_whole_listing() {
    let (mut rig, mut actor) = caught_up();
    let mut at = until_a_whole_listing(&mut rig, &mut actor, T0 + 1000);
    for _ in 0..60 {
        let second = one_second(&mut rig, &mut actor, at);
        at += 1000;
        assert!(!second.whole);
    }
    // The wall clock steps back 20 minutes, more than the backstop.
    let back = at - 1_200_000;
    let mut whole = false;
    for s in 0..120 {
        whole |= one_second(&mut rig, &mut actor, back + s * 1000).whole;
    }
    assert!(
        whole,
        "no whole listing within two minutes of the step back"
    );
}

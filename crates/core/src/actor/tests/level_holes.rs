//! Levels above 1 have no hole when level 1 gains two files in one window.
//!
//! A level-1 compaction's output carries its last input's time, so a run whose last input
//! was committed before the window opened leaves the window open, and a commit that lands
//! while that compaction runs becomes a second level-1 file in the same window. The trial
//! drill saw level 2 then take the files before the first of the two and the second alone,
//! never the first: a hole Go's compaction refuses to read across
//! (`non-contiguous transaction ids in input files`, ltx `compactor.go:125`).

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test clocks and counts are small bounded values"
)]

use super::*;
use crate::L0Retention;

/// The trial drill's levels: 10, 20 and 40 seconds.
fn drill_levels() -> Maintenance {
    Maintenance::new(
        vec![
            Duration::from_secs(10),
            Duration::from_secs(20),
            Duration::from_secs(40),
        ],
        Duration::from_secs(86_400),
        Duration::from_secs(86_400),
        L0Retention::After(Duration::from_secs(300)),
    )
    .unwrap()
}

fn is_level_one_output(tick: &Tick) -> bool {
    matches!(
        event(tick),
        Some(Event::Maintained {
            event: MaintainedEvent::Compacted(info),
            ..
        }) if info.key.level() == Level::new(1).unwrap()
    )
}

/// A writer that commits half a second into every second from `from` to `until` (seconds
/// after `T0`), and once more while each window's first level-1 compaction runs (the tick
/// after it sees the commit). Returns every tick.
fn write_through_compactions(rig: &mut Rig, actor: &mut Actor, from: i64, until: i64) -> Vec<Tick> {
    let mut seen = Vec::new();
    for s in from..until {
        let second = T0 + 500 + s * 1000;
        rig.commit(u8::try_from(s % 250 + 1).unwrap());
        let mut now = second;
        let mut landed = false;
        for _ in 0..100 {
            let tick = rig.tick(actor, now);
            let (first_output, next) = (is_level_one_output(&tick) && !landed, next_of(&tick));
            seen.push(tick);
            if first_output {
                landed = true;
                rig.commit(u8::try_from(s % 250 + 2).unwrap());
                now += 1;
                continue;
            }
            match next {
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

/// The keys of the compaction outputs published at `level`, in order.
fn outputs_at(seen: &[Tick], level: u8) -> Vec<FileKey> {
    seen.iter()
        .filter_map(|tick| match event(tick) {
            Some(Event::Maintained {
                event: MaintainedEvent::Compacted(info),
                ..
            }) if info.key.level() == Level::new(level).unwrap() => Some(info.key),
            _ => None,
        })
        .collect()
}

/// Levels of 10 and 20 seconds under an hour-long level 3: level 2 keeps its files above
/// level 3's newest for the length of a test.
fn slow_level_three() -> ActorConfig {
    ActorConfig {
        maintenance: Maintenance::new(
            vec![
                Duration::from_secs(10),
                Duration::from_secs(20),
                Duration::from_secs(3600),
            ],
            Duration::from_secs(86_400),
            Duration::from_secs(86_400),
            L0Retention::After(Duration::from_secs(300)),
        )
        .unwrap(),
        ..cfg()
    }
}

/// The transaction ranges of `level` on the replica, oldest first.
fn ranges_at(rig: &Rig, level: u8) -> Vec<(u64, u64)> {
    Rig::files(&rig.remote, Level::new(level).unwrap())
        .iter()
        .map(|f| (f.key.range().min().get(), f.key.range().max().get()))
        .collect()
}

/// The first pair of neighbours in `ranges` where the later does not start right after the
/// earlier ends.
fn first_hole(ranges: &[(u64, u64)]) -> Option<((u64, u64), (u64, u64))> {
    ranges
        .iter()
        .zip(ranges.iter().skip(1))
        .find_map(|(a, b)| (b.0 != a.1 + 1).then_some((*a, *b)))
}

#[test]
fn two_level_one_files_in_one_window_leave_no_hole_in_the_levels_above() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(ActorConfig {
        maintenance: drill_levels(),
        ..cfg()
    });
    rig.settle(&mut actor, T0);
    write_through_compactions(&mut rig, &mut actor, 0, 130);
    let one = ranges_at(&rig, 1);
    // The scenario is the one meant: some window gave level 1 a single-transaction file
    // right after a longer one.
    assert!(
        one.iter()
            .zip(one.iter().skip(1))
            .any(|(a, b)| a.0 < a.1 && b.0 == b.1),
        "level 1 never gained two files in one window: {one:?}"
    );
    for level in 1..=3 {
        let ranges = ranges_at(&rig, level);
        assert!(ranges.len() >= 2, "level {level} holds {ranges:?}");
        assert_eq!(
            first_hole(&ranges),
            None,
            "level {level} has a hole: {ranges:?}"
        );
    }
}

/// A replica an earlier version left with holes in level 2 (level 1 whole, level 3 behind):
/// the first listing closes each from level 1, one after the other, without waiting for any
/// window. Until then Go's compaction into level 3 fails at every tick.
#[test]
fn holes_already_on_the_replica_are_closed_at_the_first_listing() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(slow_level_three());
    rig.settle(&mut actor, T0);
    write_through_compactions(&mut rig, &mut actor, 0, 95);
    let two = Rig::files(&rig.remote, Level::new(2).unwrap());
    assert!(two.len() >= 5, "level 2 holds {two:?}");
    let gone = [two[1].key, two[3].key];
    block_on(rig.remote.inner.delete(&gone)).unwrap();
    assert!(first_hole(&ranges_at(&rig, 2)).is_some());
    // A new process: it lists the whole replica at its start.
    let mut actor = Actor::new(slow_level_three());
    let seen = rig.settle(&mut actor, T0 + 95_600);
    no_stop(&seen);
    assert_eq!(outputs_at(&seen, 2), gone);
    assert_eq!(first_hole(&ranges_at(&rig, 2)), None);
    assert_eq!(first_hole(&ranges_at(&rig, 1)), None);
}

/// A level-1 file removed from the replica while the engine runs (not by it): the short pass
/// that next plans level 2 sees that level 1 no longer follows it, plans nothing from its
/// tails and lists the whole replica, which writes the file again from level 0.
#[test]
fn a_level_one_file_deleted_under_the_engine_is_written_again_from_level_zero() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(slow_level_three());
    rig.settle(&mut actor, T0);
    write_through_compactions(&mut rig, &mut actor, 0, 35);
    let two_max = ranges_at(&rig, 2).last().unwrap().1;
    let first_above = Rig::files(&rig.remote, Level::new(1).unwrap())
        .into_iter()
        .find(|f| f.key.range().min().get() > two_max)
        .unwrap()
        .key;
    block_on(rig.remote.inner.delete(&[first_above])).unwrap();
    let seen = write_through_compactions(&mut rig, &mut actor, 35, 45);
    no_stop(&seen);
    assert!(
        outputs_at(&seen, 1).contains(&first_above),
        "level 1 never regained {first_above:?}: {:?}",
        outputs_at(&seen, 1)
    );
    assert_eq!(first_hole(&ranges_at(&rig, 1)), None);
    let two = ranges_at(&rig, 2);
    assert_eq!(first_hole(&two), None, "{two:?}");
    assert!(two.last().unwrap().1 > first_above.range().max().get());
}

/// A short pass runs `due` as the engine does.
fn act_on_short(rig: &mut Rig, actor: &mut Actor, due: Due, now: i64) -> Tick {
    let (staging, remote) = (Staging(&rig.staging), Replica(&rig.remote));
    let mut env = Env {
        wal: &mut rig.wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &staging,
        remote: &remote,
        clock: &FakeClock::new(ts(now)),
        notice: &mut |_| {},
    };
    block_on(actor.act_on(&mut env, due, Some(None), 0, ts(now)))
}

/// The check before the write: a short pass's compaction that would not start right after
/// its level's newest file is a bug, reported before anything is written. The planner never
/// plans one (it asks for a whole listing instead), so this is what a listing that lost a
/// file runs into.
#[test]
fn a_short_pass_compaction_that_would_leave_a_hole_is_a_bug_and_writes_nothing() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(slow_level_three());
    rig.settle(&mut actor, T0);
    // A level-1 file above the first snapshot, then three files in the window after it.
    rig.commit(1);
    rig.settle(&mut actor, T0 + 1000);
    rig.settle(&mut actor, T0 + 11_000);
    for n in 2..=4 {
        rig.commit(n);
        rig.settle(&mut actor, T0 + 11_000 + 1000 * i64::from(n));
    }
    let one = ranges_at(&rig, 1);
    let newest = one.last().unwrap().1;
    let zero: Vec<FileInfo> = Rig::files(&rig.remote, Level::ZERO)
        .into_iter()
        .filter(|f| f.key.range().min().get() > newest)
        .collect();
    assert_eq!(zero.len(), 3, "{zero:?}");
    let job = |files: &[FileInfo]| {
        let inputs = crate::CompactionInputs::new(files.to_vec()).unwrap();
        Job::Compact(crate::maintain::CompactJob::new(inputs, Level::new(1).unwrap()).unwrap())
    };
    // The second and third files alone: the first would be in no level-1 file.
    let skipping = job(&zero[1..]);
    let tick = crate::fault::reporting_bugs(|| {
        assert_eq!(
            actor.follows_its_level(&skipping),
            Err(Fault::Bug(crate::BugSite::CompactionLeavesAHole))
        );
        act_on_short(&mut rig, &mut actor, Due::Run(skipping), T0 + 4000)
    });
    assert!(matches!(tick, Tick::Stopped(_)), "{tick:?}");
    assert_eq!(ranges_at(&rig, 1), one, "the job ran");
    // All three files start right after level 1's newest: that job follows.
    assert_eq!(actor.follows_its_level(&job(&zero)), Ok(()));
}

/// The same pass with the job the planner gives: it follows, and runs.
#[test]
fn a_short_pass_compaction_that_follows_its_level_runs() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(slow_level_three());
    rig.settle(&mut actor, T0);
    rig.commit(1);
    rig.settle(&mut actor, T0 + 1000);
    // Level 1 holds nothing: it starts above the first snapshot, which ends at 1.
    let newest = ranges_at(&rig, 1).last().map_or(1, |range| range.1);
    let zero: Vec<FileInfo> = Rig::files(&rig.remote, Level::ZERO)
        .into_iter()
        .filter(|f| f.key.range().min().get() > newest)
        .collect();
    let inputs = crate::CompactionInputs::new(zero).unwrap();
    let job = crate::maintain::CompactJob::new(inputs, Level::new(1).unwrap()).unwrap();
    let key = job.output_key();
    let tick = act_on_short(&mut rig, &mut actor, Due::Run(Job::Compact(job)), T0 + 2000);
    assert_eq!(outputs_at(&[tick], 1), vec![key]);
}

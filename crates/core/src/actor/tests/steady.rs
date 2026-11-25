//! A steady writer's request bill: what one minute of 20 commits a second costs at the
//! replica, counted request by request.
//!
//! The driver is the replicator thread's loop in small (`lsr-replica`'s `thread.rs`): a tick
//! that did work and says `Next::Now` is followed by another at once, anything else parks
//! until the engine's own wake or one poll interval after the last tick. Requests take time
//! (a put longer than a sync), and the writer commits on its own clock, so commits land while
//! a put is on its way.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test clocks and counts are small bounded values"
)]

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use super::*;

/// The writer commits this often: 20 commits a second.
const COMMIT_EVERY: i64 = 50;
/// The replicator thread's poll interval, Go's monitor interval.
const POLL: i64 = 1000;

/// How long the tick that ended in `event` took, in milliseconds: a request to the replica
/// costs a round trip, and a sync its fsyncs (20 ms each in the daemon's debug log of a run
/// against a file replica).
fn cost(event: Option<&Event>) -> i64 {
    match event {
        Some(Event::Uploaded(_) | Event::Sending { .. }) => 20,
        Some(Event::Maintained { .. }) => 30,
        Some(Event::Synced { .. } | Event::Booted { .. }) => 20,
        Some(_) | None => 1,
    }
}

/// A replica whose listings carry only a last-modified time, as S3's do, and whose answer to
/// a put carries the created-at time it stored.
fn s3_like() -> Rig {
    let mut rig = Rig::new();
    rig.remote = Counting {
        inner: MemStore::with_listed_last_modified(),
        commit_says_created: true,
        ..Counting::default()
    };
    rig
}

/// Runs `actor` under a writer that commits every [`COMMIT_EVERY`] milliseconds from `from`
/// until `until`, as the replicator thread would drive it. Returns the time it stopped at.
fn drive(rig: &mut Rig, actor: &mut Actor, from: i64, until: i64) -> i64 {
    let mut now = from;
    let mut commit_at = from;
    let mut n = 0u8;
    while now < until {
        while commit_at <= now {
            n = n % 250 + 1;
            rig.commit(n);
            commit_at += COMMIT_EVERY;
        }
        let tick = rig.tick(actor, now);
        let poll = now + POLL;
        let event = event(&tick);
        let idle = event == Some(&Event::Idle);
        now += cost(event);
        match next_of(&tick) {
            Some(Next::Now) if !idle => {}
            Some(Next::At(at)) => now = now.max(at.unix_millis().min(poll)),
            Some(Next::Now | Next::OnWalChange) | None => now = now.max(poll),
        }
    }
    now
}

/// The requests the replica took from the `from`th on, by kind and level.
fn bill(rig: &Rig, from: usize) -> BTreeMap<(Request, u8), usize> {
    rig.remote.requests_from(from)
}

/// The configuration the replicator thread gives the engine: a sync interval of one poll.
fn paced() -> ActorConfig {
    ActorConfig {
        sync_interval: Duration::from_millis(1000),
        ..go_like()
    }
}

/// The replica's bill for two minutes of a steady writer under `config`, counted once
/// levels 1 and 2 hold files and level-0 retention has begun (six minutes in).
fn two_steady_minutes(config: ActorConfig) -> BTreeMap<(Request, u8), usize> {
    let mut rig = s3_like();
    let mut actor = Actor::new(config);
    let start = 600_000_000;
    let now = drive(&mut rig, &mut actor, start, start + 360_000);
    let mark = rig.remote.requests_made();
    drive(&mut rig, &mut actor, now, now + 120_000);
    bill(&rig, mark)
}

/// A level-1 compaction reads its level-0 inputs from staging, and what it must know of the
/// replica's copies (that they are the files it holds) it remembers from its own puts: no
/// level-0 file is read back. Go reads its local copies and asks the replica nothing
/// (`db.go`, `Compactor.Compact` over the local level-0 directory).
#[test]
fn a_steady_writer_reads_no_level_zero_file_back() {
    let bill = two_steady_minutes(paced());
    assert!(
        bill.get(&(Request::Put, 1)).is_some_and(|puts| *puts >= 2),
        "{bill:?}"
    );
    assert_eq!(bill.get(&(Request::Get, 0)), None, "{bill:?}");
}

/// A token is held only while its staged file is: the prune that deletes the file lets the
/// token go, so the set stays a compaction window's worth however long the engine runs.
#[test]
fn sent_tokens_are_let_go_when_their_staged_files_are_pruned() {
    let mut rig = s3_like();
    let mut actor = Actor::new(paced());
    let start = 600_000_000;
    drive(&mut rig, &mut actor, start, start + 180_000);
    let staged = Rig::files(&rig.staging, Level::ZERO);
    let held = actor.engine.sent.len();
    assert!(
        held > 0 && held <= staged.len(),
        "{held} of {}",
        staged.len()
    );
    assert!(staged.len() < 100, "{}", staged.len());
}

/// A new process holds no token for the files the one before it sent: each of those still
/// staged is compared with the replica's object by one read, once, and the writer's own
/// files after that cost none.
#[test]
fn after_a_restart_only_the_files_left_staged_are_read_back() {
    let mut rig = s3_like();
    let mut actor = Actor::new(paced());
    let start = 600_000_000;
    let now = drive(&mut rig, &mut actor, start, start + 100_000);
    let left = Rig::files(&rig.staging, Level::ZERO).len();
    let mark = rig.remote.requests_made();
    let mut restarted = Actor::new(paced());
    let now = drive(&mut rig, &mut restarted, now, now + 60_000);
    let first = bill(&rig, mark);
    let mark = rig.remote.requests_made();
    drive(&mut rig, &mut restarted, now, now + 60_000);
    let second = bill(&rig, mark);
    let read_back = first.get(&(Request::Get, 0)).copied().unwrap_or(0);
    // The boot reads the replica's newest file once more, to compare it with the staged one.
    assert!(
        left > 1 && read_back >= 1 && read_back <= left + 1,
        "{left} left: {first:?}"
    );
    assert_eq!(second.get(&(Request::Get, 0)), None, "{second:?}");
}
/// How many files the sync steps of `ticks` staged.
fn synced(ticks: &[Tick]) -> u32 {
    ticks
        .iter()
        .map(|tick| match event(tick) {
            Some(Event::Synced { files }) => *files,
            Some(_) | None => 0,
        })
        .sum()
}

/// A booted engine with a sync interval, its first file on the replica, and one more
/// commit synced and uploaded at the time it returns.
fn paced_and_synced() -> (Rig, Actor, i64) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(paced());
    rig.settle(&mut actor, T0);
    let at = T0 + 5_000;
    rig.commit(2);
    assert_eq!(synced(&rig.settle(&mut actor, at)), 1);
    (rig, actor, at)
}

/// A steady writer costs one level-0 file per poll interval, as under Go, whose monitor
/// syncs once per tick of its ticker (`db.go`, `monitor`). Without the interval every tick
/// stages what was committed since the last one, and the engine ticks again at once
/// whenever a file waits to be uploaded or maintenance has work: a commit that lands while
/// the poll's file is written becomes a second file in the same second.
#[test]
fn a_steady_writer_stages_one_file_per_poll_interval() {
    let bill = two_steady_minutes(paced());
    let puts = bill.get(&(Request::Put, 0)).copied().unwrap_or(0);
    assert!((118..=121).contains(&puts), "{bill:?}");
    let unpaced = two_steady_minutes(go_like());
    let puts = unpaced.get(&(Request::Put, 0)).copied().unwrap_or(0);
    assert!(puts > 150, "{unpaced:?}");
}

/// A commit that lands inside the interval is staged when the interval ends, and the tick
/// that held it names that time as its wake.
#[test]
fn a_commit_inside_the_interval_waits_for_it_and_the_tick_names_the_wake() {
    let (mut rig, mut actor, at) = paced_and_synced();
    rig.commit(3);
    let held = rig.settle(&mut actor, at);
    assert_eq!(synced(&held), 0, "{held:?}");
    assert_eq!(
        held.last().and_then(next_of),
        Some(Next::At(ts(at + 1_000)))
    );
    let still = rig.settle(&mut actor, at + 999);
    assert_eq!(synced(&still), 0, "{still:?}");
    assert_eq!(
        still.last().and_then(next_of),
        Some(Next::At(ts(at + 1_000)))
    );
    let due = rig.settle(&mut actor, at + 1_000);
    assert_eq!(synced(&due), 1, "{due:?}");
    // The file is on the replica, and nothing is held any longer.
    assert_eq!(Rig::files(&rig.remote, Level::ZERO).len(), 3);
    assert_ne!(due.last().and_then(next_of), Some(Next::At(ts(at + 1_000))));
}

/// An operator's snapshot is staged at once, inside the interval.
#[test]
fn a_snapshot_asked_for_does_not_wait_for_the_interval() {
    let (mut rig, mut actor, at) = paced_and_synced();
    actor.request_snapshot();
    assert_eq!(synced(&rig.settle(&mut actor, at + 1)), 1);
}

/// A sync the byte cap cut short goes on at once: the cap is a bound on one file, and the
/// commits behind it are not new.
#[test]
fn the_rest_of_a_capped_sync_follows_at_once() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(ActorConfig {
        sync: SyncConfig {
            cap: NonZeroU64::new(1),
            ..SyncConfig::default()
        },
        ..paced()
    });
    rig.settle(&mut actor, T0);
    for n in 2..5 {
        rig.commit(n);
    }
    // One commit a file at this cap, all three in one instant.
    assert_eq!(synced(&rig.settle(&mut actor, T0 + 5_000)), 3);
}

/// A checkpoint that is due takes its proof inside the interval, and the proof is a sync
/// of everything committed: the commit made since the last file does not wait.
#[test]
fn a_due_checkpoint_syncs_inside_the_interval() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(ActorConfig {
        checkpoint: CheckpointConfig {
            interval: Some(Duration::from_millis(2_000)),
            min_pages: None,
            truncate_pages: None,
        },
        ..paced()
    });
    rig.settle(&mut actor, T0);
    rig.commit(2);
    assert_eq!(synced(&rig.settle(&mut actor, T0 + 1_500)), 1);
    rig.commit(3);
    // Held: no checkpoint is due yet.
    assert_eq!(synced(&rig.settle(&mut actor, T0 + 1_600)), 0);
    // The checkpoint interval ends before the sync interval does.
    let seen = rig.settle(&mut actor, T0 + 2_000);
    assert!(
        seen.iter()
            .any(|tick| matches!(event(tick), Some(Event::Checkpointed { .. }))),
        "{seen:?}"
    );
    // The third commit's file and the one the checkpoint staged after it.
    assert_eq!(Rig::files(&rig.remote, Level::ZERO).len(), 4, "{seen:?}");
}

/// A drain stages what is left at once: a stop does not wait out the interval.
#[test]
fn a_drain_does_not_wait_for_the_interval() {
    let (mut rig, mut actor, at) = paced_and_synced();
    rig.commit(3);
    actor.begin_shutdown(ts(at + 10_000));
    let seen = rig.settle(&mut actor, at + 1);
    assert_eq!(synced(&seen), 1, "{seen:?}");
}
/// How many requests of `kind` a bill holds, at every level.
fn count(bill: &BTreeMap<(Request, u8), usize>, kind: Request) -> usize {
    bill.iter()
        .filter(|((request, _), _)| *request == kind)
        .map(|(_, n)| *n)
        .sum()
}

/// Level-0 retention ages each file it deletes by the created-at time the file's put was
/// answered with, and its anchors by those of the newest files this process put: no HEAD.
/// A listing with only a last-modified time once cost one HEAD per level-0 file deleted.
/// Go ages by the listed last-modified time and asks nothing.
#[test]
fn retention_reads_no_created_at_time_of_a_file_this_process_put() {
    let mut rig = s3_like();
    let mut actor = Actor::new(paced());
    let start = 600_000_000;
    // Seven minutes in, level-0 retention (five minutes) has made its first delete.
    let now = drive(&mut rig, &mut actor, start, start + 420_000);
    let mark = rig.remote.requests_made();
    drive(&mut rig, &mut actor, now, now + 240_000);
    let bill = bill(&rig, mark);
    assert!(bill.contains_key(&(Request::Delete, 0)), "{bill:?}");
    assert_eq!(count(&bill, Request::Head), 0, "{bill:?}");
    // What is remembered is let go with the files: about five minutes of them.
    assert!(
        actor.engine.known.len() < 450,
        "{}",
        actor.engine.known.len()
    );
}

/// A new process knows no created-at time of the files the one before it put: each of
/// those costs one HEAD when retention reaches it, and the files it puts itself none.
#[test]
fn after_a_restart_retention_reads_the_created_at_times_it_does_not_know() {
    let mut rig = s3_like();
    let mut actor = Actor::new(paced());
    let start = 600_000_000;
    let now = drive(&mut rig, &mut actor, start, start + 420_000);
    let left = Rig::files(&rig.remote, Level::ZERO).len();
    let mark = rig.remote.requests_made();
    let mut restarted = Actor::new(paced());
    // Long enough for retention to pass every file the first process left.
    let now = drive(&mut rig, &mut restarted, now, now + 480_000);
    let first = bill(&rig, mark);
    let heads = first.get(&(Request::Head, 0)).copied().unwrap_or(0);
    assert!(heads > 0 && heads <= left + 8, "{left} left: {first:?}");
    let mark = rig.remote.requests_made();
    drive(&mut rig, &mut restarted, now, now + 240_000);
    let second = bill(&rig, mark);
    assert!(second.contains_key(&(Request::Delete, 0)), "{second:?}");
    // One level-0 file the first process left stays (the first file, which level 1 starts
    // above), and each retention check reads its time again: two checks in four minutes.
    let again = second.get(&(Request::Head, 0)).copied().unwrap_or(0);
    assert!(again <= 2, "{second:?}");
}

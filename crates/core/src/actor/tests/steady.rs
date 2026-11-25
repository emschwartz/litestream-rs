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

use super::*;

/// The writer commits this often: 20 commits a second.
const COMMIT_EVERY: i64 = 50;
/// The replicator thread's poll interval, Go's monitor interval.
const POLL: i64 = 1000;

/// How long the tick that ended in `event` took, in milliseconds: a request to the replica
/// costs a round trip, and a sync an fsync.
fn cost(event: Option<&Event>) -> i64 {
    match event {
        Some(Event::Uploaded(_) | Event::Sending { .. }) => 20,
        Some(Event::Maintained { .. }) => 30,
        Some(Event::Synced { .. } | Event::Booted { .. }) => 5,
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

/// The replica's bill for two minutes of a steady writer, counted once levels 1 and 2 hold
/// files and level-0 retention has begun (six minutes in).
fn two_steady_minutes() -> BTreeMap<(Request, u8), usize> {
    let mut rig = s3_like();
    let mut actor = Actor::new(go_like());
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
    let bill = two_steady_minutes();
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
    let mut actor = Actor::new(go_like());
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
    let mut actor = Actor::new(go_like());
    let start = 600_000_000;
    let now = drive(&mut rig, &mut actor, start, start + 100_000);
    let left = Rig::files(&rig.staging, Level::ZERO).len();
    let mark = rig.remote.requests_made();
    let mut restarted = Actor::new(go_like());
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

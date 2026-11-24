use proptest::prelude::*;

use lsr_wal::Salts;

use super::*;
use crate::fault::StopCause;
use crate::store::Denied;

const BOUND: MaxUnreplicatedAge = MaxUnreplicatedAge::DEFAULT;

fn ts(millis: i64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

fn bound_ms() -> i64 {
    i64::try_from(BOUND.get().as_millis()).unwrap()
}

const MODES: [IfReplicationStops; 3] = [
    IfReplicationStops::Report,
    IfReplicationStops::HoldStart,
    IfReplicationStops::StopApp,
];

fn trips() -> [Trip; 3] {
    [
        Trip::Stopped(StopCause::OtherWriter),
        Trip::UnreplicatedTooLong { since: ts(1) },
        Trip::RefusedAtStart(Denied::Credentials),
    ]
}

/// The design's table, every cell.
#[test]
fn the_gate_follows_the_table() {
    let stop = Trip::Stopped(StopCause::OtherWriter);
    let age = Trip::UnreplicatedTooLong { since: ts(1) };
    let refused = Trip::RefusedAtStart(Denied::Permission);
    let held = |trip| Gate::Held(trip);
    let cases = [
        // (mode, released, trip, gate)
        (IfReplicationStops::Report, false, stop, Gate::Allowed),
        (IfReplicationStops::Report, true, stop, Gate::Allowed),
        (IfReplicationStops::Report, false, age, Gate::Allowed),
        (IfReplicationStops::Report, false, refused, Gate::Allowed),
        (IfReplicationStops::HoldStart, false, stop, held(stop)),
        (IfReplicationStops::HoldStart, true, stop, Gate::Allowed),
        (IfReplicationStops::HoldStart, false, age, Gate::Allowed),
        (IfReplicationStops::HoldStart, true, age, Gate::Allowed),
        (IfReplicationStops::HoldStart, false, refused, held(refused)),
        (IfReplicationStops::HoldStart, true, refused, Gate::Allowed),
        (IfReplicationStops::StopApp, false, stop, held(stop)),
        (IfReplicationStops::StopApp, true, stop, held(stop)),
        (IfReplicationStops::StopApp, false, age, held(age)),
        (IfReplicationStops::StopApp, true, age, held(age)),
        (IfReplicationStops::StopApp, false, refused, held(refused)),
        (IfReplicationStops::StopApp, true, refused, Gate::Allowed),
    ];
    for (mode, released, trip, want) in cases {
        assert_eq!(
            gate(mode, released, Some(trip)),
            want,
            "{mode:?} released={released} {trip:?}"
        );
    }
    for mode in MODES {
        assert_eq!(gate(mode, false, None), Gate::Allowed);
        assert_eq!(gate(mode, true, None), Gate::Allowed);
    }
}

/// Writes are held while the gate is held, and while the boot is still being judged under
/// `hold-start` and `stop-app`; `report` never holds them, and an open gate never does.
#[test]
fn writes_are_blocked_by_a_held_gate_and_by_a_pending_boot_except_under_report() {
    let stop = Trip::Stopped(StopCause::OtherWriter);
    for mode in MODES {
        assert_eq!(blocks_writes(mode, Gate::Allowed), None, "{mode:?}");
        assert_eq!(
            blocks_writes(mode, Gate::Held(stop)),
            Some(WhyBlocked::Tripped(stop)),
            "{mode:?}"
        );
        let pending = match mode {
            IfReplicationStops::Report => None,
            IfReplicationStops::HoldStart | IfReplicationStops::StopApp => {
                Some(WhyBlocked::Booting)
            }
        };
        assert_eq!(blocks_writes(mode, Gate::Pending), pending, "{mode:?}");
    }
}

#[test]
fn the_default_mode_is_hold_start() {
    assert_eq!(IfReplicationStops::default(), IfReplicationStops::HoldStart);
    assert_eq!(MaxUnreplicatedAge::default().get().as_secs(), 600);
}

/// A released application is never held again under `hold-start`; under `stop-app` it is.
#[test]
fn the_release_latches() {
    let stop = Some(Trip::Stopped(StopCause::RemoteCorrupt));
    let mut app = AppGate::default();
    assert_eq!(
        app.judge(IfReplicationStops::HoldStart, None, false),
        Gate::Pending,
        "nothing is released before the boot is judged"
    );
    assert!(!app.released());
    assert_eq!(
        app.judge(IfReplicationStops::HoldStart, stop, false),
        Gate::Held(Trip::Stopped(StopCause::RemoteCorrupt))
    );
    assert!(!app.released());
    assert_eq!(
        app.judge(IfReplicationStops::HoldStart, None, true),
        Gate::Allowed
    );
    assert!(app.released());
    assert_eq!(
        app.judge(IfReplicationStops::HoldStart, stop, true),
        Gate::Allowed
    );
    assert!(matches!(
        app.judge(IfReplicationStops::StopApp, stop, true),
        Gate::Held(_)
    ));
}

/// Every stop cause but the end of a drain is a trip, and a stop wins over the others.
#[test]
fn a_stop_trips_and_wins() {
    let causes = [
        StopCause::StoreDenied(Denied::Tls),
        StopCause::StoreRefused,
        StopCause::RemoteCorrupt,
        StopCause::OtherWriter,
        StopCause::RemoteRegressed,
        StopCause::WalCorrupt,
        StopCause::WalTruncated,
        StopCause::WalUnlinked,
        StopCause::WalSyncFailed,
        StopCause::Bug,
    ];
    for cause in causes {
        assert_eq!(
            trip(Some(cause), Some(Denied::Credentials), Some(ts(1))),
            Some(Trip::Stopped(cause))
        );
        assert!(!Trip::Stopped(cause).clears_by_itself());
    }
    assert_eq!(trip(Some(StopCause::Shutdown), None, None), None);
    assert_eq!(
        trip(Some(StopCause::Shutdown), Some(Denied::Tls), Some(ts(1))),
        Some(Trip::RefusedAtStart(Denied::Tls))
    );
    assert_eq!(
        trip(None, None, Some(ts(5))),
        Some(Trip::UnreplicatedTooLong { since: ts(5) })
    );
    assert!(Trip::UnreplicatedTooLong { since: ts(5) }.clears_by_itself());
    assert!(Trip::RefusedAtStart(Denied::Tls).clears_by_itself());
}

#[test]
fn every_trip_is_held_by_stop_app_before_release() {
    for trip in trips() {
        assert_eq!(
            gate(IfReplicationStops::StopApp, false, Some(trip)),
            Gate::Held(trip)
        );
    }
}

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn mark(frames: u32) -> WalMark {
    WalMark::new(Salts { salt1: 1, salt2: 2 }, frames)
}

/// File `pending` holds a commit the replica (at `pending - 1`) lacks, stamped `stamp`.
fn file(pending: u64, stamp: i64) -> Observed {
    Observed {
        staged: Some(txid(pending)),
        pending: Some(txid(pending)),
        replicated: Txid::new(pending.saturating_sub(1)),
        oldest: Some(ts(stamp)),
        ..Observed::default()
    }
}

/// Lag with no date of its own: a pending file whose stamp is unknown.
fn undated() -> Observed {
    Observed {
        staged: Some(txid(2)),
        pending: Some(txid(2)),
        replicated: Some(txid(1)),
        ..Observed::default()
    }
}

/// Frames up to `frames` past a synced position at `synced`, nothing pending past file 1.
fn frames(synced: u32, frames: u32) -> Observed {
    Observed {
        ahead: Some(mark(frames)),
        synced: Some(mark(synced)),
        staged: Some(txid(1)),
        replicated: Some(txid(1)),
        ..Observed::default()
    }
}

/// The clock starts at the oldest date, never ahead of now; undated lag dates from its first
/// sighting while it lasts.
#[test]
fn the_clock_takes_the_older_start() {
    let mut lag = Lag::default();
    lag.observe(file(2, 50), BOUND, ts(100));
    assert_eq!(lag.since(), Some(ts(50)));
    let mut lag = Lag::default();
    lag.observe(file(2, 500), BOUND, ts(100));
    assert_eq!(lag.since(), Some(ts(100)), "a stamp ahead of now");
    let mut lag = Lag::default();
    lag.observe(undated(), BOUND, ts(100));
    lag.observe(undated(), BOUND, ts(200));
    assert_eq!(lag.since(), Some(ts(100)), "the first sighting holds");
    lag.observe(Observed::default(), BOUND, ts(300));
    assert_eq!(lag.since(), None);
}

/// Frames date from the observation that first saw the WAL end where it did, and that date
/// holds while the synced position has not passed it, though more frames arrive.
#[test]
fn unstaged_frames_date_from_their_first_sighting() {
    let mut lag = Lag::default();
    lag.observe(frames(0, 3), BOUND, ts(100));
    lag.observe(frames(2, 5), BOUND, ts(200));
    assert_eq!(lag.since(), Some(ts(100)), "frame 3 is not staged yet");
    lag.observe(frames(3, 5), BOUND, ts(300));
    assert_eq!(
        lag.since(),
        Some(ts(300)),
        "frames 4 and 5 date from this sighting: the files frame 3 went into hold no commit"
    );
}

/// Staged frames carry their date into the files they went into until the replica holds
/// them, then drop it, though newer commits are pending: the clock never dates a run of lag.
#[test]
fn staged_frames_carry_their_date_until_the_replica_holds_them() {
    let mut lag = Lag::default();
    lag.observe(frames(0, 3), BOUND, ts(100));
    let staged = Observed {
        synced: Some(mark(3)),
        ..file(2, 150)
    };
    lag.observe(staged, BOUND, ts(200));
    assert_eq!(lag.since(), Some(ts(100)), "carried into file 2");
    let newer = Observed {
        ahead: Some(mark(6)),
        synced: Some(mark(4)),
        ..file(3, 250)
    };
    lag.observe(newer, BOUND, ts(300));
    assert_eq!(lag.since(), Some(ts(250)), "the replica holds file 2");
}

/// Frames staged in the same tick as a newer commit carry their date to the first file staged
/// after them, never to the newest: once the replica holds that file the date goes, though the
/// newer commit's file is pending.
#[test]
fn staged_frames_never_date_a_newer_file() {
    let mut lag = Lag::default();
    lag.observe(frames(0, 3), BOUND, ts(100));
    let staged = Observed {
        synced: Some(WalMark::new(Salts { salt1: 3, salt2: 4 }, 1)),
        staged: Some(txid(4)),
        pending: Some(txid(4)),
        replicated: Some(txid(1)),
        oldest: Some(ts(900)),
        ..Observed::default()
    };
    lag.observe(staged, BOUND, ts(1_000));
    assert_eq!(
        lag.since(),
        Some(ts(100)),
        "file 2 holds frames seen at 100"
    );
    let landed = Observed {
        replicated: Some(txid(3)),
        ..staged
    };
    lag.observe(landed, BOUND, ts(1_100));
    assert_eq!(lag.since(), Some(ts(900)), "only file 4 is pending");
}

/// A snapshot owed for commits only in the database file keeps the date those frames had,
/// though the WAL restarted.
#[test]
fn an_owed_snapshot_keeps_the_date_of_its_frames() {
    let mut lag = Lag::default();
    lag.observe(frames(0, 3), BOUND, ts(100));
    let owed = Observed {
        synced: Some(WalMark::new(Salts { salt1: 3, salt2: 4 }, 0)),
        owed: true,
        ..Observed::default()
    };
    lag.observe(owed, BOUND, ts(200));
    assert_eq!(lag.since(), Some(ts(100)));
    lag.observe(Observed::default(), BOUND, ts(300));
    assert_eq!(lag.since(), None);
}

/// The probe is due once per bound instant, at or after it.
#[test]
fn the_probe_is_due_once_at_the_bound() {
    let mut lag = Lag::default();
    lag.observe(undated(), BOUND, ts(0));
    assert_eq!(lag.next_probe(BOUND), Some(ts(bound_ms())));
    assert!(!lag.probe_due(BOUND, ts(bound_ms() - 1)));
    assert!(lag.probe_due(BOUND, ts(bound_ms())));
    assert!(!lag.probe_due(BOUND, ts(bound_ms() + 1)), "once");
    assert_eq!(lag.next_probe(BOUND), None);
    assert!(!Lag::default().probe_due(BOUND, ts(i64::MAX)));
}

/// A failure after the bound trips; the trip holds until the age is zero.
#[test]
fn a_failure_after_the_bound_trips_until_caught_up() {
    let mut lag = Lag::default();
    lag.observe(undated(), BOUND, ts(0));
    lag.failed(ts(bound_ms() - 1));
    lag.observe(undated(), BOUND, ts(bound_ms()));
    assert_eq!(lag.tripped(), None, "a failure before the bound");
    lag.failed(ts(bound_ms()));
    lag.observe(undated(), BOUND, ts(bound_ms()));
    assert_eq!(lag.tripped(), Some(ts(0)));
    lag.observe(undated(), BOUND, ts(bound_ms() + 10));
    assert_eq!(lag.tripped(), Some(ts(0)), "latched");
    assert_eq!(lag.next_probe(BOUND), None, "no probe while tripped");
    lag.observe(Observed::default(), BOUND, ts(bound_ms() + 20));
    assert_eq!(lag.tripped(), None);
}

/// A trip for age clears once the commits it tripped on reach the replica, though newer ones
/// keep the age above zero: a busy database may never be caught up.
#[test]
fn a_trip_for_age_clears_once_its_commits_land() {
    let mut lag = Lag::default();
    lag.observe(file(2, 0), BOUND, ts(0));
    lag.failed(ts(bound_ms()));
    lag.observe(file(2, 0), BOUND, ts(bound_ms()));
    assert_eq!(lag.tripped(), Some(ts(0)));
    lag.observe(file(3, bound_ms() - 5), BOUND, ts(bound_ms() + 10));
    assert_eq!(lag.tripped(), None, "file 2 landed; file 3 is 15 ms old");
    assert_eq!(lag.since(), Some(ts(bound_ms() - 5)));
}

/// One step of what the actor feeds the clock.
#[derive(Clone, Copy, Debug)]
enum Step {
    Observe(Observed),
    Fail,
    Probe,
}

fn observed() -> impl Strategy<Value = Observed> {
    let wal = proptest::option::of((0_u32..3, 0_u32..40));
    let mark = |(salt, frames)| {
        WalMark::new(
            Salts {
                salt1: salt,
                salt2: 0,
            },
            frames,
        )
    };
    (
        proptest::option::of((wal.clone(), 1_u32..20)),
        wal,
        proptest::option::of((1_u64..20, 0_u64..20)),
        proptest::option::of(0_i64..2_000_000),
        any::<bool>(),
    )
        .prop_map(move |(ahead, synced, files, oldest, owed)| Observed {
            ahead: ahead.map(|(at, more)| {
                let (salt, frames) = at.unwrap_or((0, 0));
                mark((salt, frames.saturating_add(more)))
            }),
            synced: synced.map(mark),
            staged: files.and_then(|(pending, _)| Txid::new(pending)),
            pending: files.and_then(|(pending, _)| Txid::new(pending)),
            replicated: files.and_then(|(pending, back)| Txid::new(pending.saturating_sub(back))),
            oldest: oldest.map(ts),
            owed,
        })
}

fn step() -> impl Strategy<Value = (u32, Step)> {
    let kind = prop_oneof![
        observed().prop_map(Step::Observe),
        Just(Step::Fail),
        Just(Step::Probe),
    ];
    (0_u32..200_000, kind)
}

proptest! {
    /// The clock's laws over any sequence: it trips only with the age at or past the bound and
    /// a failure at or past the bound's instant, it holds no start while caught up, its start
    /// is never ahead of now, and a probe is never due before the bound.
    #[test]
    fn the_clock_never_trips_early(steps in proptest::collection::vec(step(), 1..60)) {
        let mut lag = Lag::default();
        let mut now = 0_i64;
        let mut failures: Vec<i64> = Vec::new();
        for (dt, step) in steps {
            now = now.saturating_add(i64::from(dt));
            match step {
                Step::Observe(seen) => {
                    let was = lag.tripped();
                    lag.observe(seen, BOUND, ts(now));
                    if seen == Observed::default() {
                        prop_assert_eq!(lag.since(), None);
                        prop_assert_eq!(lag.tripped(), None);
                        failures.clear();
                    }
                    if let (None, Some(since)) = (was, lag.tripped()) {
                        let at = since.unix_millis().saturating_add(bound_ms());
                        prop_assert!(now >= at);
                        prop_assert!(failures.iter().any(|f| *f >= at), "{failures:?} {at}");
                    }
                    if let Some(since) = lag.since() {
                        prop_assert!(since.unix_millis() <= now);
                    }
                }
                Step::Fail => {
                    lag.failed(ts(now));
                    failures.push(now);
                }
                Step::Probe => {
                    if lag.probe_due(BOUND, ts(now)) {
                        let since = lag.since().unwrap();
                        prop_assert!(now >= since.unix_millis().saturating_add(bound_ms()));
                    }
                }
            }
        }
    }

    /// Under `report` nothing is ever held; under every mode no trip means open.
    #[test]
    fn report_never_holds(released in any::<bool>(), which in 0_usize..3) {
        prop_assert_eq!(
            gate(IfReplicationStops::Report, released, trips().get(which).copied()),
            Gate::Allowed
        );
    }
}

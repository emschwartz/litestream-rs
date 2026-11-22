use proptest::prelude::*;

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
        Trip::NotYetReachable(Denied::Credentials),
    ]
}

/// The design's table (`docs/proposals/fail-stop.md`, "Decision"), every cell.
#[test]
fn the_gate_follows_the_table() {
    let stop = Trip::Stopped(StopCause::OtherWriter);
    let age = Trip::UnreplicatedTooLong { since: ts(1) };
    let refused = Trip::NotYetReachable(Denied::Permission);
    let held = |trip| Gate::Held(trip);
    let cases = [
        // (mode, released, trip, gate)
        (IfReplicationStops::Report, false, stop, Gate::Open),
        (IfReplicationStops::Report, true, stop, Gate::Open),
        (IfReplicationStops::Report, false, age, Gate::Open),
        (IfReplicationStops::Report, false, refused, Gate::Open),
        (IfReplicationStops::HoldStart, false, stop, held(stop)),
        (IfReplicationStops::HoldStart, true, stop, Gate::Open),
        (IfReplicationStops::HoldStart, false, age, Gate::Open),
        (IfReplicationStops::HoldStart, true, age, Gate::Open),
        (IfReplicationStops::HoldStart, false, refused, held(refused)),
        (IfReplicationStops::HoldStart, true, refused, Gate::Open),
        (IfReplicationStops::StopApp, false, stop, held(stop)),
        (IfReplicationStops::StopApp, true, stop, held(stop)),
        (IfReplicationStops::StopApp, false, age, held(age)),
        (IfReplicationStops::StopApp, true, age, held(age)),
        (IfReplicationStops::StopApp, false, refused, held(refused)),
        (IfReplicationStops::StopApp, true, refused, Gate::Open),
    ];
    for (mode, released, trip, want) in cases {
        assert_eq!(
            gate(mode, released, Some(trip)),
            want,
            "{mode:?} released={released} {trip:?}"
        );
    }
    for mode in MODES {
        assert_eq!(gate(mode, false, None), Gate::Open);
        assert_eq!(gate(mode, true, None), Gate::Open);
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
        Gate::Open
    );
    assert!(app.released());
    assert_eq!(
        app.judge(IfReplicationStops::HoldStart, stop, true),
        Gate::Open
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
        Some(Trip::NotYetReachable(Denied::Tls))
    );
    assert_eq!(
        trip(None, None, Some(ts(5))),
        Some(Trip::UnreplicatedTooLong { since: ts(5) })
    );
    assert!(Trip::UnreplicatedTooLong { since: ts(5) }.clears_by_itself());
    assert!(Trip::NotYetReachable(Denied::Tls).clears_by_itself());
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

/// The clock starts at the older of the stamp and the first sighting, and never ahead of now.
#[test]
fn the_clock_takes_the_older_start() {
    let mut lag = Lag::default();
    lag.observe(true, Some(ts(50)), BOUND, ts(100));
    assert_eq!(lag.since(), Some(ts(50)));
    let mut lag = Lag::default();
    lag.observe(true, Some(ts(500)), BOUND, ts(100));
    assert_eq!(lag.since(), Some(ts(100)), "a stamp ahead of now");
    lag.observe(true, None, BOUND, ts(200));
    assert_eq!(lag.since(), Some(ts(100)), "the first sighting holds");
    lag.observe(false, None, BOUND, ts(300));
    assert_eq!(lag.since(), None);
}

/// The probe is due once per bound instant, at or after it.
#[test]
fn the_probe_is_due_once_at_the_bound() {
    let mut lag = Lag::default();
    lag.observe(true, None, BOUND, ts(0));
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
    lag.observe(true, None, BOUND, ts(0));
    lag.failed(ts(bound_ms() - 1));
    lag.observe(true, None, BOUND, ts(bound_ms()));
    assert_eq!(lag.tripped(), None, "a failure before the bound");
    lag.failed(ts(bound_ms()));
    lag.observe(true, None, BOUND, ts(bound_ms()));
    assert_eq!(lag.tripped(), Some(ts(0)));
    lag.observe(true, None, BOUND, ts(bound_ms() + 10));
    assert_eq!(lag.tripped(), Some(ts(0)), "latched");
    assert_eq!(lag.next_probe(BOUND), None, "no probe while tripped");
    lag.observe(false, None, BOUND, ts(bound_ms() + 20));
    assert_eq!(lag.tripped(), None);
}

/// One step of what the actor feeds the clock.
#[derive(Clone, Copy, Debug)]
enum Step {
    Observe { behind: bool, stamp: Option<i64> },
    Fail,
    Probe,
}

fn step() -> impl Strategy<Value = (u32, Step)> {
    let kind = prop_oneof![
        (any::<bool>(), proptest::option::of(0_i64..2_000_000))
            .prop_map(|(behind, stamp)| Step::Observe { behind, stamp }),
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
                Step::Observe { behind, stamp } => {
                    let was = lag.tripped();
                    lag.observe(behind, stamp.map(ts), BOUND, ts(now));
                    if !behind {
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
            Gate::Open
        );
    }
}

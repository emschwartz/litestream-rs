//! One scenario per fault: each drives the engine into that fault and checks
//! what was seen, with the oracle judging the replica.

use lsr_core::{Event, HaltCause, Responses};
use lsr_sim::Op;
use lsr_sim::run::Seen;
use lsr_sim::scenario::{
    self, FaultKind, Scenario, app_scenarios, clock_scenarios, fault_reason, fault_scenarios,
    halt_of, names_fault, persistent_of, run_scenario, seal_scenarios, window_scenarios,
};

fn run(make: fn() -> Scenario) {
    let result = run_scenario(&make());
    assert!(result.is_ok(), "{}", result.err().unwrap_or_default());
}

/// A scenario's operations are fixed, so the seed's random history is not
/// its replay: a failure must name the test, not `just sim-replay`.
#[test]
fn a_failing_scenario_names_its_own_replay() {
    let mut broken = scenario::clock_steps_back_10s();
    broken.name = "a_broken_scenario";
    broken
        .ops
        .insert(0, Op::Meta(lsr_sim::generate::MetaOp::Restore));
    let message = run_scenario(&broken).unwrap_err();
    assert!(message.contains("a_broken_scenario"), "{message}");
    assert!(message.contains("failed at step 0"), "{message}");
    assert!(!message.contains("sim-replay"), "{message}");
    assert!(
        message.contains("cargo nextest run -p lsr-sim --run-ignored all a_broken_scenario"),
        "{message}"
    );
}

#[test]
fn every_fault_has_a_scenario_or_a_reason() {
    for kind in FaultKind::ALL {
        assert!(
            !fault_scenarios(kind).is_empty() || fault_reason(kind).is_some(),
            "{kind:?} has no scenario and no reason"
        );
    }
}

/// The scenarios of every fault whose policy can stop the engine or degrade
/// a task, the end of a drain included (it stops under every response).
fn persistent_scenarios() -> Vec<(FaultKind, fn() -> Scenario)> {
    FaultKind::ALL
        .into_iter()
        .filter(|kind| halt_of(*kind).is_some())
        .flat_map(|kind| fault_scenarios(kind).iter().map(move |make| (kind, *make)))
        .collect()
}

/// Under the strict responses every persistent fault's scenario stops the
/// engine with its cause, as before the responses were configurable.
#[test]
fn under_strict_every_persistent_scenario_stops_with_its_cause() {
    for (kind, make) in persistent_scenarios() {
        let scenario = make().under(Responses::STRICT);
        assert!(!scenario.keeps_trying(), "{}", scenario.name);
        let report = run_scenario(&scenario);
        let report = report.unwrap_or_else(|why| panic!("{why}"));
        let want = halt_of(kind).unwrap();
        assert!(
            report.seen.iter().any(|seen| matches!(
                seen,
                Seen::Halt(cause) if std::mem::discriminant(cause) == std::mem::discriminant(&want)
            )),
            "{} did not stop with {want:?}",
            scenario.name
        );
    }
}

/// Under the default responses every persistent fault but another writer
/// (and the end of a drain) degrades a task instead, the oracle holds, and
/// no engine halts (a drain may end at its deadline while the task is
/// degraded). A scenario whose fault clears expects the task to
/// recover; each such fault has at least one that does, except the ones the
/// simulator cannot undo (a WAL cut short or unlinked stays so, and a page
/// size change clears only through a restart).
#[test]
fn under_go_like_every_persistent_scenario_degrades_and_the_oracle_holds() {
    let mut recovers = std::collections::BTreeSet::new();
    for (kind, make) in persistent_scenarios() {
        let scenario = make().under(Responses::GO_LIKE);
        let report = run_scenario(&scenario).unwrap_or_else(|why| panic!("{why}"));
        let Some(cause) = persistent_of(kind).filter(|c| !Responses::GO_LIKE.stops(*c)) else {
            continue;
        };
        assert!(scenario.keeps_trying(), "{}", scenario.name);
        assert!(
            !report
                .seen
                .iter()
                .any(|seen| matches!(seen, Seen::Halt(cause) if *cause != HaltCause::Shutdown)),
            "{} halted",
            scenario.name
        );
        assert!(
            report.seen.iter().any(|seen| matches!(
                seen,
                Seen::Event(Event::Degraded { cause: got, .. })
                    if std::mem::discriminant(got) == std::mem::discriminant(&cause)
            )),
            "{} degraded no task by {cause:?}",
            scenario.name
        );
        assert!(report.seen.contains(&Seen::Oracle), "{}", scenario.name);
        if report
            .seen
            .iter()
            .any(|seen| matches!(seen, Seen::Recovered(_)))
        {
            recovers.insert(format!("{kind:?}"));
        }
    }
    for kind in [
        FaultKind::StoreAuth,
        FaultKind::StoreRefused,
        FaultKind::RemoteCorrupt,
        FaultKind::RemoteRegressed,
        FaultKind::WalCorruptBelowCommitted,
    ] {
        assert!(
            recovers.contains(&format!("{kind:?}")),
            "{kind:?} never recovers"
        );
    }
}

/// Another writer stops the engine under the default responses too, and
/// the cause stays: no later tick uploads or maintains.
#[test]
fn under_go_like_a_second_writer_stops_uploads_and_maintenance() {
    let scenario = scenario::second_writer_mid_run().under(Responses::GO_LIKE);
    assert!(!scenario.keeps_trying());
    let report = run_scenario(&scenario).unwrap_or_else(|why| panic!("{why}"));
    assert!(report.seen.contains(&Seen::Halt(HaltCause::OtherWriter)));
}

#[test]
fn every_scenario_names_its_fault() {
    for kind in FaultKind::ALL {
        for make in fault_scenarios(kind) {
            let scenario = make();
            assert!(
                names_fault(&scenario, kind),
                "{} does not expect {kind:?}",
                scenario.name
            );
        }
    }
}

#[test]
fn every_scenario_is_tested_here() {
    let source = include_str!("faults.rs");
    let mut all: Vec<fn() -> Scenario> = clock_scenarios().to_vec();
    all.extend_from_slice(window_scenarios());
    all.extend_from_slice(app_scenarios());
    all.extend_from_slice(seal_scenarios());
    for kind in FaultKind::ALL {
        all.extend_from_slice(fault_scenarios(kind));
    }
    for make in all {
        let name = make().name;
        assert!(
            source.contains(&format!("fn {name}()")),
            "no test for {name}"
        );
    }
}

#[test]
fn vacuum_page_size_change() {
    run(scenario::vacuum_page_size_change);
}

#[test]
fn store_flaky_5pct() {
    run(scenario::store_flaky_5pct);
}

#[test]
fn store_429_retry_after() {
    run(scenario::store_429_retry_after);
}

#[test]
fn creds_revoked_mid_run() {
    run(scenario::creds_revoked_mid_run);
}

#[test]
fn store_refuses_upload() {
    run(scenario::store_refuses_upload);
}

#[test]
fn remote_file_deleted_externally() {
    run(scenario::remote_file_deleted_externally);
}

#[test]
fn commit_fails_after_publish() {
    run(scenario::commit_fails_after_publish);
}

#[test]
fn size_mismatch_on_commit() {
    run(scenario::size_mismatch_on_commit);
}

#[test]
fn etag_mismatch_on_put() {
    run(scenario::etag_mismatch_on_put);
}

#[test]
fn remote_bitrot_on_list_read() {
    run(scenario::remote_bitrot_on_list_read);
}

#[test]
fn second_writer_mid_run() {
    run(scenario::second_writer_mid_run);
}

#[test]
fn bucket_rolled_back() {
    run(scenario::bucket_rolled_back);
}

#[test]
fn staged_byte_flip_before_upload() {
    run(scenario::staged_byte_flip_before_upload);
}

#[test]
fn meta_dir_deleted_mid_run() {
    run(scenario::meta_dir_deleted_mid_run);
}

#[test]
fn disk_full_on_staging() {
    run(scenario::disk_full_on_staging);
}

#[test]
fn eio_on_wal_read() {
    run(scenario::eio_on_wal_read);
}

#[test]
fn fsync_fails_once() {
    run(scenario::fsync_fails_once);
}

#[test]
fn busy_on_barrier() {
    run(scenario::busy_on_barrier);
}

#[test]
fn readers_hold_the_wal() {
    run(scenario::readers_hold_the_wal);
}

#[test]
fn shm_header_mid_update() {
    run(scenario::shm_header_mid_update);
}

#[test]
fn restart_between_steps() {
    run(scenario::restart_between_steps);
}

#[test]
fn wal_header_torn_while_running() {
    run(scenario::wal_header_torn_while_running);
}

#[test]
fn shm_end_inside_tx() {
    run(scenario::shm_end_inside_tx);
}

#[test]
fn frame_overwritten_between_scan_and_read() {
    run(scenario::frame_overwritten_between_scan_and_read);
}

#[test]
fn db_created_no_pages() {
    run(scenario::db_created_no_pages);
}

#[test]
fn reacquire_fails_after_checkpoint() {
    run(scenario::reacquire_fails_after_checkpoint);
}

#[test]
fn bit_flip_in_committed_frame() {
    run(scenario::bit_flip_in_committed_frame);
}

#[test]
fn wal_truncated_externally_under_lock() {
    run(scenario::wal_truncated_externally_under_lock);
}

#[test]
fn wal_unlinked_mid_run() {
    run(scenario::wal_unlinked_mid_run);
}

#[test]
fn db_page_flipped_under_sync() {
    run(scenario::db_page_flipped_under_sync);
}

#[test]
fn sigterm_during_upload() {
    run(scenario::sigterm_during_upload);
}

#[test]
fn clock_steps_back_10s() {
    run(scenario::clock_steps_back_10s);
}

#[test]
fn app_autocheckpoints() {
    run(scenario::app_autocheckpoints);
}

#[test]
fn sealed_then_checkpoint_busy() {
    run(scenario::sealed_then_checkpoint_busy);
}

#[test]
fn sealed_then_checkpoint_io() {
    run(scenario::sealed_then_checkpoint_io);
}

#[test]
fn sealed_then_bump_busy() {
    run(scenario::sealed_then_bump_busy);
}

#[test]
fn sealed_then_bump_io() {
    run(scenario::sealed_then_bump_io);
}

#[test]
fn corrupt_l1_publish_then_power_loss() {
    run(scenario::corrupt_l1_publish_then_power_loss);
}

#[test]
fn corrupt_l2_publish_then_power_loss() {
    run(scenario::corrupt_l2_publish_then_power_loss);
}

// The damaged snapshot is listed after the power loss; the read-back after a
// boot deletes it, so restores no longer refuse on it.
#[test]
fn corrupt_l9_publish_then_power_loss() {
    run(scenario::corrupt_l9_publish_then_power_loss);
}

#[test]
fn corrupt_l0_publish_then_power_loss() {
    run(scenario::corrupt_l0_publish_then_power_loss);
}

#[test]
fn vacuum_page_size_change_and_back() {
    run(scenario::vacuum_page_size_change_and_back);
}

#[test]
fn bit_flip_in_committed_frame_healed() {
    run(scenario::bit_flip_in_committed_frame_healed);
}

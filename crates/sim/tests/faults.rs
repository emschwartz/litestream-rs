//! One scenario per fault: each drives the engine into that fault and checks
//! what was seen, with the oracle judging the replica.

use lsr_sim::Op;
use lsr_sim::scenario::{
    self, FaultKind, Scenario, clock_scenarios, fault_reason, fault_scenarios, names_fault,
    run_scenario, window_scenarios,
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
fn shm_header_mid_update() {
    run(scenario::shm_header_mid_update);
}

#[test]
fn restart_between_steps() {
    run(scenario::restart_between_steps);
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
fn sigterm_during_upload() {
    run(scenario::sigterm_during_upload);
}

#[test]
fn clock_steps_back_10s() {
    run(scenario::clock_steps_back_10s);
}

// Finding against core: after the lost delete and the power loss, the next boot
// halts with RemoteCorrupt instead of repairing level 1.
// Replay: just sim-replay 0x0000000000000006 17
#[test]
#[ignore = "review: boot halts RemoteCorrupt on a wrong-bytes right-size level 1 object"]
fn corrupt_l1_publish_then_power_loss() {
    run(scenario::corrupt_l1_publish_then_power_loss);
}

// Finding against core: the damaged snapshot is listed after the power loss,
// the engine does not halt, and every restore refuses on it (checksum
// mismatch) until the next snapshot. The read-back after a boot (issue 1,
// item 1) repairs it. Replay: this test; the operations are fixed.
#[test]
#[ignore = "review: a damaged level 9 snapshot stays listed after power loss and restores refuse on it"]
fn corrupt_l9_publish_then_power_loss() {
    run(scenario::corrupt_l9_publish_then_power_loss);
}

#[test]
fn corrupt_l0_publish_then_power_loss() {
    run(scenario::corrupt_l0_publish_then_power_loss);
}

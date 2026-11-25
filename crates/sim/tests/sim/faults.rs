//! One scenario per fault: each drives the engine into that fault and checks
//! what was seen, with the oracle judging the replica.

use lsr_core::{Event, Responses, StopCause};
use lsr_sim::Op;
use lsr_sim::run::Seen;
use lsr_sim::scenario::{
    self, FaultKind, Scenario, app_scenarios, clock_scenarios, fault_reason, fault_scenarios,
    names_fault, persistent_of, run_scenario, seal_scenarios, stop_of, window_scenarios,
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
        .filter(|kind| stop_of(*kind).is_some())
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
        let want = stop_of(kind).unwrap();
        assert!(
            report.seen.iter().any(|seen| matches!(
                seen,
                Seen::Stop(cause) if std::mem::discriminant(cause) == std::mem::discriminant(&want)
            )),
            "{} did not stop with {want:?}",
            scenario.name
        );
    }
}

/// Under the default responses every persistent fault but another writer
/// (and the end of a drain) degrades a task instead, the oracle holds, and
/// no engine stops (a drain may end at its deadline while the task is
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
                .any(|seen| matches!(seen, Seen::Stop(cause) if *cause != StopCause::Shutdown)),
            "{} stopped",
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
        FaultKind::StoreCredentials,
        FaultKind::StorePermission,
        FaultKind::StoreClockSkew,
        FaultKind::StoreBucketMissing,
        FaultKind::StoreTls,
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
    assert!(report.seen.contains(&Seen::Stop(StopCause::OtherWriter)));
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
fn wal_fsync_fails() {
    let scenario = scenario::wal_fsync_fails();
    assert!(scenario.keeps_trying());
    run(scenario::wal_fsync_fails);
}

/// A power loss after the retried sync went on: the database is behind what was staged and the
/// boot snapshots it, as for any rewound database (`docs/decisions/D173.md`).
#[test]
fn wal_fsync_fails_then_power_loss() {
    run(scenario::wal_fsync_fails_then_power_loss);
}

/// A restart between a failed WAL sync and its retry remembers nothing of it and boots as after
/// any crash: no debt, no TRUNCATE of the engine's own.
#[test]
fn wal_fsync_fails_then_restart() {
    run(scenario::wal_fsync_fails_then_restart);
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
fn clock_steps_back_an_hour_mid_retry() {
    run(scenario::clock_steps_back_an_hour_mid_retry);
}

#[test]
fn clock_jumps_forward_an_hour() {
    run(scenario::clock_jumps_forward_an_hour);
}

#[test]
fn creds_revoked_mid_run() {
    run(scenario::creds_revoked_mid_run);
}

#[test]
fn permission_revoked_mid_run() {
    run(scenario::permission_revoked_mid_run);
}

#[test]
fn clock_skewed_against_the_store_mid_run() {
    run(scenario::clock_skewed_against_the_store_mid_run);
}

#[test]
fn bucket_deleted_mid_run() {
    run(scenario::bucket_deleted_mid_run);
}

#[test]
fn tls_fails_mid_run() {
    run(scenario::tls_fails_mid_run);
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
fn older_copy_on_a_new_host() {
    run(scenario::older_copy_on_a_new_host);
}

#[test]
fn older_copy_taken_after_the_last_counter_bump() {
    run(scenario::older_copy_taken_after_the_last_counter_bump);
}

#[test]
fn second_writer_mid_run() {
    run(scenario::second_writer_mid_run);
}

#[test]
fn second_writer_between_whole_listings() {
    run(scenario::second_writer_between_whole_listings);
}

#[test]
fn fault_drops_what_the_engine_remembers_of_the_replica() {
    run(scenario::fault_drops_what_the_engine_remembers_of_the_replica);
}

#[test]
fn second_writer_survives_restart() {
    run(scenario::second_writer_survives_restart);
}

#[test]
fn foreign_file_while_down_stops() {
    run(scenario::foreign_file_while_down_stops);
}

#[test]
fn newest_staged_file_lost() {
    run(scenario::newest_staged_file_lost);
}

#[test]
fn a_running_drop_of_an_unconfirmed_upload_keeps_a_takeover_through_it() {
    run(scenario::a_running_drop_of_an_unconfirmed_upload_keeps_a_takeover_through_it);
}

#[test]
fn restored_old_meta() {
    run(scenario::restored_old_meta);
}

#[test]
fn staging_put_back_with_other_bytes_is_not_written_over_the_replica() {
    run(scenario::staging_put_back_with_other_bytes_is_not_written_over_the_replica);
}

#[test]
fn vm_restore_stops_until_trust_local() {
    run(scenario::vm_restore_stops_until_trust_local);
}

#[test]
fn deploy_cycle_after_sigterm() {
    run(scenario::deploy_cycle_after_sigterm);
}

#[test]
fn deploy_cycle_after_sigkill() {
    run(scenario::deploy_cycle_after_sigkill);
}

#[test]
fn deploy_cycle_sigkill_after_a_wal_restart_continues() {
    run(scenario::deploy_cycle_sigkill_after_a_wal_restart_continues);
}

#[test]
fn deploy_cycle_with_the_wal_deleted_at_close() {
    run(scenario::deploy_cycle_with_the_wal_deleted_at_close);
}

#[test]
fn bucket_rolled_back() {
    run(scenario::bucket_rolled_back);
}

#[test]
fn bucket_rolled_back_while_idle() {
    run(scenario::bucket_rolled_back_while_idle);
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
fn writes_held_through_a_checkpoint() {
    run(scenario::writes_held_through_a_checkpoint);
}

#[test]
fn writes_held_while_the_boot_is_pending() {
    run(scenario::writes_held_while_the_boot_is_pending);
}

#[test]
fn writes_allowed_after_the_boot() {
    run(scenario::writes_allowed_after_the_boot);
}

#[test]
fn writes_held_after_a_stop() {
    run(scenario::writes_held_after_a_stop);
}

#[test]
fn writes_never_held_under_report() {
    run(scenario::writes_never_held_under_report);
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
fn create_only_refused_mid_run() {
    run(scenario::create_only_refused_mid_run);
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

/// A WAL header written again with the same salts and another seed. With our read lock lapsed,
/// another application connection restarts the WAL once (its checkpoint count becomes 1), the
/// app restarts it twice more by its commits and a third time by a spill it rolls back, which
/// leaves the wal-index at no frame of the newest generation under the app's header. The engine
/// takes its position at the start of that generation; then the other connection commits frame
/// 1 and writes the header again, same salts, its own count. The engine follows the new header
/// and replicates the commit (it called the WAL corrupt for good while it kept the old seed).
#[test]
fn a_header_written_again_by_another_connection_is_followed() {
    let commit = || Op::Commit {
        pages: 2,
        grow: false,
    };
    let other = || Op::OtherConnectionCommit { pages: 2 };
    let backfill = || Op::OtherProcessCheckpoint(lsr_sim::OtherProcessMode::Passive);
    let ticks = |n| vec![Op::Tick; n];
    let ops: Vec<Op> = [
        vec![commit(), commit()],
        ticks(6),
        vec![Op::OurReader { begin: false }],
        vec![
            backfill(),
            other(),
            backfill(),
            commit(),
            backfill(),
            commit(),
        ],
        vec![backfill(), Op::Spill, Op::Rollback],
        ticks(6),
        vec![other()],
        ticks(12),
        vec![Op::Restore],
    ]
    .concat();
    if let Err(failure) =
        lsr_sim::run::run_ops(lsr_sim::Seed::new(1), &ops, &lsr_sim::Opts::default())
    {
        panic!("{}", failure.message(failure.steps));
    }
}

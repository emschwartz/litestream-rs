//! Recorded regression histories: each file is one seed's operations, written by
//! `lsr_sim::record` when the history was pinned, so no change to the generator reshapes them.
//!
//! A recorded history can still stop reaching its bug when the runner or the engine around it
//! changes, and then it passes for nothing. So every history names, in [`GUARDS`], the planted
//! bug in `docs/planted-bugs/entries/` its test must fail under (`just planted-bugs` runs each one
//! with its patch applied and fails when the test survives), or says why it has none.
//! [`every_recorded_history_names_its_planted_bug`] holds the table to the files and the ledger.

pub mod seed_0x126d68ed250e;
pub mod seed_0x126d68ef1c07;
pub mod seed_0x126d68ef545f;
pub mod seed_0x126d68fac4b6;
pub mod seed_0x126d68fac789;
pub mod seed_0x126d68facf21;
pub mod seed_0x126d68facf74;
pub mod seed_0x126d68fad061;
pub mod seed_0x126d68fae16b;
pub mod seed_0x1919f;
pub mod seed_0x25;
pub mod seed_0x2c;
pub mod seed_0x2d;
pub mod seed_0x31c;
pub mod seed_0x330;
pub mod seed_0x46;
pub mod seed_0x4c;
pub mod seed_0x585;
pub mod seed_0x753;
pub mod seed_0x80e;
pub mod seed_0x82;
pub mod seed_0x8ef;
pub mod seed_0x92d;
pub mod seed_0x9f9;
pub mod seed_0xa4d;
pub mod seed_0xb6b;
pub mod seed_0xb9e;
pub mod seed_0xe9;
pub mod seed_0xf;
pub mod seed_0xf45b5;
pub mod seed_0xf488c;

/// What guards a recorded history against passing for nothing.
#[derive(Clone, Copy, Debug)]
pub enum Guard {
    /// The planted bug of the defect the test was written for: its ledger entry lists the test,
    /// so the planted-bug runner requires the test to fail with it planted.
    Own(&'static str),
    /// No plant of the test's own defect exists (its fix predates the ledger, or was in a
    /// checker), so a plant on the path the history takes stands in: it shows the history still
    /// runs that path, not that it still reaches its own defect.
    OnPath(&'static str),
    /// No planted bug kills it, for the reason given.
    Unguarded(&'static str),
}

/// Every recorded history: its module, the test that runs it, and its guard.
pub const GUARDS: &[(&str, &str, Guard)] = &[
    (
        "seed_0x82",
        "a_snapshot_a_lagging_listing_hides_is_not_written_again_seed_0x82",
        Guard::Own("written-snapshot-unwatched"),
    ),
    (
        "seed_0x80e",
        "an_as_of_restore_through_a_lagging_listing_is_held_to_the_as_of_bound_seed_0x80e",
        Guard::Own("hold-as-of-demands-the-raw-answer-sim"),
    ),
    (
        "seed_0x25",
        "a_torn_staged_file_is_dropped_seed_0x25",
        Guard::OnPath("always-snapshot-at-boot"),
    ),
    (
        "seed_0x2d",
        "a_torn_staged_file_is_dropped_seed_0x2d",
        Guard::OnPath("always-snapshot-at-boot"),
    ),
    (
        "seed_0x46",
        "a_torn_staged_file_is_dropped_seed_0x46",
        Guard::OnPath("always-snapshot-at-boot"),
    ),
    (
        "seed_0x4c",
        "a_short_object_a_delete_could_not_remove_is_damage_seed_0x4c",
        Guard::Own("sim-short-object-damage"),
    ),
    (
        "seed_0x126d68ef1c07",
        "page_size_vacuum_and_staging_wipe_seed_0x126d68ef1c07",
        Guard::OnPath("wal-barrier-sim"),
    ),
    (
        "seed_0x126d68ed250e",
        "page_size_vacuum_and_staging_wipe_seed_0x126d68ed250e",
        Guard::Unguarded(
            "its defect was the oracle pairing an id with its first note (issue #39), which has \
             no plant, and no plant in the ledger kills the history",
        ),
    ),
    (
        "seed_0x126d68ef545f",
        "page_size_vacuum_and_staging_wipe_seed_0x126d68ef545f",
        Guard::OnPath("always-snapshot-at-boot"),
    ),
    (
        "seed_0x1919f",
        "a_restored_staging_backup_is_not_uploaded_seed_0x1919f",
        Guard::OnPath("inside-file-sole-base-sim"),
    ),
    (
        "seed_0x126d68fac4b6",
        "a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fac4b6",
        Guard::OnPath("recover-before-trusting-snapshot"),
    ),
    (
        "seed_0x126d68fac789",
        "a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fac789",
        Guard::OnPath("maintenance-another-writer"),
    ),
    (
        "seed_0x126d68facf21",
        "a_put_back_staged_file_never_sends_latest_back_seed_0x126d68facf21",
        Guard::OnPath("maintenance-another-writer"),
    ),
    (
        "seed_0x126d68fad061",
        "a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fad061",
        Guard::OnPath("maintenance-another-writer"),
    ),
    (
        "seed_0x126d68fae16b",
        "a_put_back_staged_file_never_sends_latest_back_seed_0x126d68fae16b",
        Guard::OnPath("always-snapshot-at-boot"),
    ),
    (
        "seed_0x126d68facf74",
        "another_writer_s_files_fit_the_level_bytes_budget_seed_0x126d68facf74",
        Guard::OnPath("maintenance-another-writer"),
    ),
    (
        "seed_0x2c",
        "swarm_seed_44_restores_only_committed_states",
        Guard::Own("anchor-any-tail"),
    ),
    (
        "seed_0x31c",
        "a_stall_ends_with_the_fault_that_caused_it_seed_0x31c",
        Guard::Own("success-keeps-a-stall"),
    ),
    (
        "seed_0xb6b",
        "an_id_the_replica_holds_is_not_taken_again_after_a_lying_read_seed_0xb6b",
        Guard::Unguarded(
            "conditional level-0 writes (#96) fixed it, and no plant in the ledger lists it yet: \
             their plants are judged by core tests",
        ),
    ),
    (
        "seed_0x753",
        "swarm_seed_0x753_a_redo_that_rebuilds_the_restored_state_is_not_a_loss",
        Guard::Own("latest-judged-against-old-nodes-sim"),
    ),
    (
        "seed_0xf",
        "a_restart_after_a_failed_wal_sync_owes_the_truncate_seed_0xf",
        Guard::Unguarded(
            "it passes with journal-debt-not-recorded-sim planted too, so it no longer reaches the \
             restart that would owe the TRUNCATE: an other-process RESTART checkpoint follows the \
             failed sync before any crash (histories/seed_0xf.rs lines 284 and 288), which may start a new WAL \
             generation first; the cause is not yet confirmed",
        ),
    ),
    (
        "seed_0x585",
        "a_power_loss_after_a_failed_wal_sync_leaves_a_database_ahead_of_its_wal_seed_0x585",
        Guard::Unguarded(
            "it pins an open defect (KnownWrong::DatabaseAheadOfWal) and passes only while main \
             fails with it, so no plant can be judged by it",
        ),
    ),
    (
        "seed_0xe9",
        "a_halt_across_restarts_still_goes_unreported_seed_0xe9",
        Guard::Unguarded(
            "it pins an open defect (KnownWrong::HaltForgottenAtRestart) and passes only while \
             main fails with it, so no plant can be judged by it",
        ),
    ),
    (
        "seed_0xf488c",
        "swarm_seed_0xf488c_takes_no_commit_before_a_truncate_for_damage",
        Guard::Own("boundary-snapshot-keeps-page-table-sim"),
    ),
    (
        "seed_0xb9e",
        "compaction_takes_a_staging_denial_as_staging_seed_0xb9e",
        Guard::Own("compact-local-read-as-remote-sim"),
    ),
    (
        "seed_0x92d",
        "seed_0x92d_trips_on_frames_it_never_read_and_is_excused",
        Guard::Unguarded(
            "the history checks a judge exemption, not an engine fix: no engine plant can make \
             the judge excuse a trip it would otherwise call false",
        ),
    ),
    (
        "seed_0x9f9",
        "seed_0x9f9_does_not_trip",
        Guard::Unguarded(
            "the runner changes merged with #188 stopped this history from reaching the false trip \
             even on the engine before the fail-stop fix, so no plant can be judged by it; \
             seed_0x330 pins the same cause",
        ),
    ),
    (
        "seed_0x330",
        "seed_0x330_does_not_trip",
        Guard::Own("failstop-dates-the-run-sim"),
    ),
    (
        "seed_0x8ef",
        "seed_0x8ef_does_not_trip",
        Guard::Own("failstop-checkpoint-quiet-forgotten"),
    ),
    (
        "seed_0xa4d",
        "seed_0xa4d_does_not_trip",
        Guard::Own("failstop-trip-held-to-zero-age-sim"),
    ),
    (
        "seed_0xf45b5",
        "a_running_drop_records_past_an_unconfirmed_upload_seed_0xf45b5",
        Guard::Own("running-drop-record-capped-at-confirmed"),
    ),
];

/// The ledger's entries, by name, with the test-name substrings each lists.
fn ledger() -> Vec<(String, Vec<String>)> {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/planted-bugs/entries");
    let entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .map(|entry| {
            let quoted = |line: &str| -> Vec<String> {
                line.split('"')
                    .skip(1)
                    .step_by(2)
                    .map(str::to_owned)
                    .collect()
            };
            let name = entry
                .lines()
                .find(|line| line.starts_with("name = "))
                .map(|line| quoted(line).concat())
                .unwrap_or_default();
            let tests = entry
                .split_once("tests = [")
                .and_then(|(_, rest)| rest.split_once(']'))
                .map(|(list, _)| quoted(list))
                .unwrap_or_default();
            (name, tests)
        })
        .collect();
    assert!(
        !entries.is_empty(),
        "no ledger entries under {}",
        dir.display()
    );
    entries
}

#[test]
fn every_recorded_history_names_its_planted_bug() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sim/histories");
    let mut files: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            name.trim_end_matches(".rs").to_owned()
        })
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no recorded histories in {}",
        dir.display()
    );
    let mut listed: Vec<String> = GUARDS
        .iter()
        .map(|(module, _, _)| (*module).to_owned())
        .collect();
    listed.sort();
    assert_eq!(
        files, listed,
        "every history file has exactly one row in GUARDS"
    );
    let ledger = ledger();
    for (module, test, guard) in GUARDS {
        match guard {
            Guard::Own(plant) | Guard::OnPath(plant) => {
                let entry = ledger.iter().find(|(name, _)| name == plant);
                let Some((_, tests)) = entry else {
                    panic!("{module}: no ledger entry named {plant}");
                };
                assert!(
                    tests
                        .iter()
                        .any(|listed| listed.ends_with(test) || test.contains(listed.as_str())),
                    "{module}: ledger entry {plant} does not list {test}"
                );
            }
            Guard::Unguarded(why) => assert!(!why.is_empty(), "{module}: say why it is unguarded"),
        }
    }
}

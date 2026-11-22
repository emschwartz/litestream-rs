//! Recorded regression histories: each file is one seed's operations, written by
//! `lsr_sim::record` when the history was pinned, so no change to the generator reshapes them.
//!
//! A recorded history can still stop reaching its bug when the runner or the engine around it
//! changes, and then it passes for nothing. So every history names, in [`GUARDS`], the planted
//! bug in `docs/planted-bugs.toml` its test must fail under (`just planted-bugs` runs each one
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
pub mod seed_0x46;
pub mod seed_0xa2;
pub mod seed_0xb6b;

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
        "seed_0xa2",
        "a_stall_ends_with_the_fault_that_caused_it_seed_0xa2",
        Guard::Own("success-keeps-a-stall"),
    ),
    (
        "seed_0xb6b",
        "an_id_the_replica_holds_is_not_taken_again_after_a_lying_read_seed_0xb6b",
        Guard::Unguarded(
            "it is known wrong and ignored until conditional level-0 writes (#96) land: it fails \
             on main, so no plant can be judged by it",
        ),
    ),
];

/// The ledger's entries, by name, with the test-name substrings each lists.
fn ledger() -> Vec<(String, Vec<String>)> {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/planted-bugs.toml");
    let text = std::fs::read_to_string(&path).unwrap();
    text.split("[[bug]]")
        .skip(1)
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
        .collect()
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

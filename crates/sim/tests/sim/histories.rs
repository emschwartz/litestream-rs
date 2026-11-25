//! The one recorded history left: its operations were written by `lsr_sim::record`, so no
//! change to the generator reshapes them. Every other recorded history was shrunk into an explicit
//! scenario or deleted (`docs/decisions/D165.md`).
//!
//! The history stays because it checks a judge exemption, which no engine plant can make
//! fail: the judge would have to excuse a trip it would otherwise call false. Replacing it needs
//! a scenario of its own that reaches the same exemption.

pub mod seed_0x734;

/// Every recorded history: its module, the test that runs it, and why no planted bug kills it.
pub const GUARDS: &[(&str, &str, &str)] = &[(
    "seed_0x734",
    "seed_0x734_uploads_the_files_a_rollback_left_and_never_trips",
    "the history checks a judge exemption, not an engine fix: no engine plant can make the judge \
     excuse a trip it would otherwise call false",
)];

#[test]
fn every_recorded_history_is_listed() {
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
    for (module, _, why) in GUARDS {
        assert!(!why.is_empty(), "{module}: say why it is unguarded");
    }
}

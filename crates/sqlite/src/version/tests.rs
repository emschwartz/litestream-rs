use super::*;

#[test]
fn the_linked_sqlite_is_the_one_the_layouts_were_written_for() {
    check_linked().unwrap();
}

#[test]
fn another_version_or_build_of_sqlite_is_refused() {
    let other_version = check(3_054_000, WRITTEN_FOR_SOURCE_ID);
    assert!(
        matches!(
            other_version,
            Err(OpenError::SqliteBuild {
                found: 3_054_000,
                ..
            })
        ),
        "{other_version:?}"
    );
    let other_build = check(WRITTEN_FOR_NUMBER, "2026-10-16 14:45:20 0000");
    assert!(
        matches!(other_build, Err(OpenError::SqliteBuild { ref source_id, .. }) if source_id == "2026-10-16 14:45:20 0000"),
        "{other_build:?}"
    );
    check(WRITTEN_FOR_NUMBER, WRITTEN_FOR_SOURCE_ID).unwrap();
}

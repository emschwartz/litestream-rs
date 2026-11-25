use super::*;

/// The SQLite this crate links is accepted exactly when it is on the list: the bundled builds of
/// every libsqlite3-sys version in the range are, and a system SQLite (`just sqlite-system`) is
/// refused with its own version and source id unless it is one of them.
#[test]
fn the_linked_build_is_accepted_exactly_when_it_is_listed() {
    let found = crate::capi::version_number();
    let source_id = crate::capi::source_id();
    let listed = VERIFIED
        .iter()
        .any(|build| build.number == found && build.source_id == source_id);
    match check_linked() {
        Ok(()) => assert!(listed, "{found} {source_id} is not listed"),
        Err(OpenError::SqliteBuild {
            found: refused,
            source_id: refused_id,
            verified,
        }) => {
            assert!(!listed, "{found} {source_id} is listed but refused");
            assert_eq!((refused, refused_id.as_str()), (found, source_id.as_str()));
            assert_eq!(verified, VERIFIED_NUMBERS);
        }
        Err(other) => panic!("not a build refusal: {other:?}"),
    }
}

#[test]
fn every_listed_build_is_accepted() {
    for build in VERIFIED {
        check(build.number, build.source_id).unwrap();
    }
    assert_eq!(VERIFIED.map(|build| build.number), VERIFIED_NUMBERS);
    assert!(
        VERIFIED_NUMBERS
            .iter()
            .zip(VERIFIED_NUMBERS.iter().skip(1))
            .all(|(a, b)| a < b),
        "oldest first, one entry per version"
    );
    assert!(
        VERIFIED_NUMBERS.first() >= Some(&3_051_003),
        "nothing older than 3.51.3, whose predecessor shipped a WAL-reset bug"
    );
}

#[test]
fn another_version_or_build_of_sqlite_is_refused() {
    let [_, _, _, _, newest] = VERIFIED;
    let other_version = check(3_051_002, newest.source_id);
    assert!(
        matches!(
            other_version,
            Err(OpenError::SqliteBuild {
                found: 3_051_002,
                verified,
                ..
            }) if verified == VERIFIED_NUMBERS
        ),
        "{other_version:?}"
    );
    let other_build = check(newest.number, "2026-10-16 14:45:20 0000");
    assert!(
        matches!(other_build, Err(OpenError::SqliteBuild { ref source_id, .. }) if source_id == "2026-10-16 14:45:20 0000"),
        "{other_build:?}"
    );
    let [oldest, ..] = VERIFIED;
    let crossed = check(oldest.number, newest.source_id);
    assert!(crossed.is_err(), "a listed version with another build's id");
}

#[test]
fn the_refusal_names_the_build_found_and_the_builds_verified() {
    let message = check(3_045_001, "2024-01-30 16:01:20 e876e51a")
        .unwrap_err()
        .to_string();
    assert!(message.contains("3.45.1"), "{message}");
    assert!(
        message.contains("2024-01-30 16:01:20 e876e51a"),
        "{message}"
    );
    assert!(
        message.contains("3.51.3, 3.53.1, 3.53.2, 3.54.0, 3.54.1"),
        "{message}"
    );
}

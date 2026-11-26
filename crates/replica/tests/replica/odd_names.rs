//! A database whose file name holds `#`, `?` or `%`, which SQLite reads as syntax in a `file:` URI.
//!
//! Go opened its databases as `file:<path>?...`, so `app#1.db` made it open a sibling called `app`
//! and fail on `app#1.db-wal` (upstream #1647); its restore check then checked an empty sibling and
//! passed. Ours must replicate the file named, restore to a name with the same characters, leave
//! no sibling, and hold the same pages.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::common::{Site, assert_same, config, stopped_on_system_clock};
use lsr_replica::{IntegrityCheck, Replicator, Restore, StartOptions, StopOutcome, restore};

/// The names in the site's directory.
fn names(site: &Site) -> BTreeSet<String> {
    std::fs::read_dir(site.dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

/// Asserts that every entry in the directory is the database, the restored file, one of their
/// sidecars or the replica.
fn only_files_of(site: &Site, db_name: &str, restored: &str) {
    for name in names(site) {
        // The meta directory is the file's name behind a dot.
        let name = name.trim_start_matches('.');
        assert!(
            name == "replica" || name.starts_with(db_name) || name.starts_with(restored),
            "a stray file {name:?} beside {db_name:?}"
        );
    }
}

#[test]
fn a_database_named_with_uri_syntax_replicates_and_restores_under_such_a_name() {
    for (name, restored) in [
        ("a#1?b%41.db", "c#2?d%42.db"),
        ("one#two.db", "r#s.db"),
        ("one?two.db", "r?s.db"),
        ("one%41two.db", "r%42s.db"),
    ] {
        let site = Site::named(name);
        let store = site.store();
        let replicator = Replicator::start(
            &site.db,
            Arc::clone(&store),
            StartOptions {
                config: config(),
                ..StartOptions::default()
            },
        )
        .unwrap();
        site.commit(3);
        let stopped = stopped_on_system_clock(&replicator);
        assert!(
            matches!(stopped, StopOutcome::Drained { txid: Some(_) }),
            "{name}: {stopped:?}"
        );
        drop(replicator);
        only_files_of(&site, name, restored);

        let to = site.dir.path().join(restored);
        let options = Restore {
            integrity: IntegrityCheck::Full,
            ..Restore::default()
        };
        restore(&*store, &to, &options).unwrap();
        assert_same(&site.app, &to);
        only_files_of(&site, name, restored);
    }
}

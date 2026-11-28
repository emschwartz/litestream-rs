//! `Config::snapshot_read_limit` (issue 315): the engine's whole-database reads proceed at the
//! limit. On the released fake clock a wait costs no real time, and the clock shows how long the
//! reads would have taken.

use std::num::NonZeroU64;

use lsr_replica::{Config, Replicator, StartOptions, StopOutcome};

use crate::common::{Site, config, fake, stopped};

/// Rows of 3000 bytes in the database file: about 3 MiB of pages.
const ROWS: u32 = 1000;

/// How far the fake clock got between the start of a replicator over a fresh replica (so a boot
/// copy of the whole database file) and the end of its stop, under `limit`.
fn clock_taken(limit: Option<u64>) -> i64 {
    let site = Site::new();
    site.commit(ROWS);
    site.app
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
        .unwrap();
    let (runtime, clock) = fake();
    clock.release();
    let began = clock.now().unix_millis();
    let replicator = Replicator::start(
        &site.db,
        site.store(),
        StartOptions {
            config: Config {
                snapshot_read_limit: limit.and_then(NonZeroU64::new),
                ..config()
            },
            runtime: Some(runtime),
            ..StartOptions::default()
        },
    )
    .unwrap();
    let outcome = stopped(&replicator, &clock);
    assert!(
        matches!(outcome, StopOutcome::Drained { txid: Some(_) }),
        "{outcome:?}"
    );
    site.assert_restores(&*site.store());
    clock.now().unix_millis().saturating_sub(began)
}

/// A boot copy of about 3 MiB at 1 MiB a second takes seconds of the clock more than the same
/// copy with no limit, and the replica it leaves restores the same database.
#[test]
fn a_read_limit_holds_the_boot_copy_to_its_rate() {
    let free = clock_taken(None);
    let paced = clock_taken(Some(1 << 20));
    assert!(
        paced - free >= 2_500,
        "no limit {free} ms, a limit of 1 MiB/s {paced} ms"
    );
}

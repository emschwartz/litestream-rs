//! Power loss at each sync of the WAL under `synchronous = NORMAL`: every write since each
//! file's last sync is gone, every connection dies, and the next boot snapshots
//! (`LocalRewound`) or continues; no transaction the replica acknowledged stops restoring to
//! what was committed.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use lsr_core::{Event, Fault as EngineFault, SnapshotCause, Tick};
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{Backend, Effect, Fault, FileKind, Op, Rig, RigConfig, RigError};
use tempfile::TempDir;

fn config() -> RigConfig {
    RigConfig {
        power: true,
        synchronous: "NORMAL",
        ..RigConfig::default()
    }
}

/// The cycle, stopping at the first settle after the power failed.
fn cycle_until_dark<B: Backend>(rig: &mut Rig<B>) -> Result<(), RigError> {
    rig.cycle_with(|rig| {
        if !rig.vfs().powered() {
            return Err(RigError::State("run without power"));
        }
        rig.settle()?;
        if rig.vfs().powered() {
            Ok(())
        } else {
            Err(RigError::State("run without power"))
        }
    })
}

/// What a loss at one WAL sync led to.
#[derive(Debug)]
struct Outcome {
    /// Why the next boot snapshots, if it does.
    cause: Option<SnapshotCause>,
    /// The boot settled only after the application's next commit (see
    /// `a_boot_after_a_loss_that_left_a_header_only_wal_settles`).
    needed_a_write: bool,
}

/// The snapshot cause a boot named, for a tick that is a boot.
fn boot_cause(tick: &Tick) -> Option<Option<SnapshotCause>> {
    match tick {
        Tick::Ran {
            event: Event::Booted { cause },
            ..
        } => Some(*cause),
        Tick::Ran { .. } | Tick::Halted(_) | Tick::Drained { .. } | Tick::DeadlineReached(_) => {
            None
        }
    }
}

/// The outcome of a loss at each WAL sync, keyed by sync number. `strict`: the boot must settle
/// before the application writes again.
fn each_wal_sync<B: Backend>(strict: bool) -> BTreeMap<u32, Outcome> {
    let dir = TempDir::new().unwrap();
    let mut dry = Rig::<B>::new(dir.path(), config()).unwrap();
    dry.faults_everywhere(true);
    dry.cycle().unwrap();
    let syncs = dry.vfs().calls(FileKind::Wal, Op::Sync);
    assert!(syncs > 3, "the cycle syncs the WAL: {syncs}");
    let mut causes = BTreeMap::new();
    for nth in 1..=syncs {
        let dir = TempDir::new().unwrap();
        let mut rig = Rig::<B>::new(dir.path(), config()).unwrap();
        rig.faults_everywhere(true);
        rig.vfs().arm(Fault {
            kind: FileKind::Wal,
            op: Op::Sync,
            nth: NonZeroU32::new(nth).unwrap(),
            effect: Effect::PowerLoss,
        });
        let at = format!("{} power loss at WAL sync {nth}", B::NAME);
        let done = cycle_until_dark(&mut rig);
        assert!(!rig.vfs().powered(), "{at}: the loss happened ({done:?})");
        rig.faults_everywhere(false);
        let boots = rig.ticks().len();
        rig.reboot_after_power_loss()
            .unwrap_or_else(|err| panic!("{at}: reboot: {err}"));
        // The boot sees the WAL as the loss left it; the application writes only after.
        let needed_a_write = match rig.settle() {
            Err(RigError::Unsettled(_))
                if !strict && rig.faults().last() == Some(&EngineFault::WalMoved) =>
            {
                rig.insert(100).unwrap();
                rig.settle().unwrap_or_else(|err| panic!("{at}: {err}"));
                true
            }
            other => {
                other.unwrap_or_else(|err| panic!("{at}: {err}; faults {:?}", rig.faults()));
                false
            }
        };
        for _ in 0..4 {
            rig.insert(2500).unwrap();
            rig.settle().unwrap_or_else(|err| panic!("{at}: {err}"));
        }
        let cause = rig
            .ticks()
            .iter()
            .skip(boots)
            .find_map(boot_cause)
            .unwrap_or_else(|| panic!("{at}: booted"));
        assert!(
            matches!(
                cause,
                None | Some(SnapshotCause::LocalRewound { .. } | SnapshotCause::GenerationChanged)
            ),
            "{at}: the boot continues or snapshots what the loss took, got {cause:?}"
        );
        rig.check().unwrap_or_else(|err| panic!("{at}: {err}"));
        causes.insert(
            nth,
            Outcome {
                cause,
                needed_a_write,
            },
        );
    }
    causes
}

/// SQLite belief: under `synchronous = NORMAL` SQLite syncs the WAL only before a backfill and
/// after writing a restarted log's header, so a loss can take committed transactions the engine
/// already shipped. The replica keeps them (upstream #875 was a belief about what the WAL keeps).
/// The boot then continues (nothing it synced was lost), snapshots a rewind (`LocalRewound`: the
/// WAL is shorter than what was synced), or snapshots a restart it cannot prove (the restarted
/// header was synced, the frames after it were not: `GenerationChanged`), and every transaction
/// on the replica still restores to what was committed.
#[test]
fn power_loss_at_each_wal_sync_loses_no_acknowledged_transaction() {
    let outcomes = each_wal_sync::<Sqlite>(false);
    let seen = |want: fn(&Option<SnapshotCause>) -> bool| outcomes.values().any(|o| want(&o.cause));
    assert!(seen(Option::is_none), "{outcomes:?}");
    assert!(
        seen(|c| matches!(c, Some(SnapshotCause::LocalRewound { .. }))),
        "{outcomes:?}"
    );
    assert!(
        seen(|c| matches!(c, Some(SnapshotCause::GenerationChanged))),
        "{outcomes:?}"
    );
    // Only a boot with a snapshot pending waits for the application's next commit.
    assert!(
        outcomes
            .values()
            .filter(|o| o.needed_a_write)
            .all(|o| o.cause.is_some()),
        "{outcomes:?}"
    );
}

/// Found here: a loss right after a TRUNCATE checkpoint and its new header's sync leaves a WAL
/// of exactly 32 bytes, and SQLite's recovery reads no header from a file that short
/// (`walIndexRecover` only reads one past `WAL_HDRSIZE`), so the wal-index names salts 0 while
/// the file names the new ones. The boot's pending snapshot then retries "WAL moved" until the
/// application commits again; an idle database never settles after such a loss, though nothing is lost.
#[test]
fn a_boot_after_a_loss_that_left_a_header_only_wal_settles() {
    each_wal_sync::<Sqlite>(true);
}

//! Power loss at each sync of the WAL under `synchronous = NORMAL`: every write since each
//! file's last sync is gone, every connection dies, and the next boot snapshots
//! (`LocalRewound`) or continues; no transaction the replica acknowledged stops restoring to
//! what was committed.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use lsr_core::{Event, Fault as EngineFault, SnapshotCause, Tick};
use lsr_ltx::PageSize;
use lsr_sim::RecorderEvent;
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::{Backend, Effect, Fault, FileKind, Op, Rig, RigConfig, RigError};
use lsr_wal::WalOffset;
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
        Tick::Ran { .. } | Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_) => {
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
                None | Some(
                    SnapshotCause::FirstSync
                        | SnapshotCause::LocalRewound { .. }
                        | SnapshotCause::GenerationChanged
                        | SnapshotCause::StagedUnwitnessed { .. }
                )
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
/// after writing a restarted log's header, so a loss can take committed transactions. The engine
/// syncs the WAL itself before it stages, so a loss never takes one it shipped: the boot
/// continues (nothing it synced was lost), snapshots a restart it cannot prove (the restarted
/// header was synced, the frames after it were not: `GenerationChanged`, or `StagedUnwitnessed`
/// when files staged in the old generation were still unpublished, which the boot then deletes
/// rather than uploads, since nothing live vouches for them), or snapshots from
/// scratch (the loss came before the first file was staged: `FirstSync`), and never finds the
/// WAL rewound below what it synced (`LocalRewound`, which only a disk that lies about fsync can
/// now produce). Every transaction on the replica still restores to what was committed.
#[test]
fn power_loss_at_each_wal_sync_loses_no_acknowledged_transaction() {
    let outcomes = each_wal_sync::<Sqlite>(false);
    let seen = |want: fn(&Option<SnapshotCause>) -> bool| outcomes.values().any(|o| want(&o.cause));
    assert!(seen(Option::is_none), "{outcomes:?}");
    assert!(
        seen(|c| matches!(
            c,
            Some(SnapshotCause::GenerationChanged | SnapshotCause::StagedUnwitnessed { .. })
        )),
        "{outcomes:?}"
    );
    assert!(
        !seen(|c| matches!(c, Some(SnapshotCause::LocalRewound { .. }))),
        "the WAL lost frames the engine had synced: {outcomes:?}"
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

/// Where the newest staged level-0 file's WAL frames end: the salts they were read under and
/// the byte offset just past them.
fn staged_end<B: Backend>(rig: &Rig<B>) -> Option<((u32, u32), u64)> {
    let (_remote, staging) = rig.store_events();
    staging.iter().rev().find_map(|event| match event {
        RecorderEvent::Put {
            key,
            origin: Some(origin),
            ..
        } if key.level().get() == 0 && origin.size() > 0 => Some((
            origin.salts(),
            origin.offset().saturating_add(origin.size()),
        )),
        RecorderEvent::Put { .. } | RecorderEvent::Delete { .. } => None,
    })
}

/// The application's WAL after the reboot: its header's salts and the byte offset just past
/// the frames SQLite's recovery kept.
fn recovered_end<B: Backend>(rig: &mut Rig<B>) -> ((u32, u32), u64) {
    let frames = u32::try_from(rig.query("PRAGMA wal_checkpoint(NOOP)").unwrap()[1]).unwrap();
    let page_size = u32::try_from(rig.query("PRAGMA page_size").unwrap()[0]).unwrap();
    #[expect(clippy::disallowed_methods, reason = "the WAL carries no POSIX lock")]
    let bytes = std::fs::read(rig.wal()).unwrap();
    let word = |at: usize| u32::from_be_bytes(bytes[at..][..4].try_into().unwrap());
    let end = WalOffset::from_frame_index(frames, PageSize::new(page_size).unwrap());
    ((word(16), word(20)), end.get())
}

/// SQLite belief: under `synchronous = NORMAL` no commit syncs the WAL, so between two
/// checkpoints every committed frame is in the page cache only, and a power cut takes it. The
/// engine syncs the WAL itself before it stages, so a cut right after a settle (everything
/// staged and uploaded, nothing checkpointed) keeps every frame the replica holds: the
/// database never comes back behind its replica, and the boot continues instead of
/// snapshotting a rewind. Without that sync the cut takes the shipped commits, the database
/// rewinds below the bucket, and the application's next commits fork history under the same
/// salts.
#[test]
fn a_power_cut_after_a_settle_leaves_the_database_at_least_as_far_as_the_replica() {
    let dir = TempDir::new().unwrap();
    let mut rig = Rig::<Sqlite>::new(dir.path(), config()).unwrap();
    rig.settle().unwrap();
    for _ in 0..3 {
        rig.insert(2500).unwrap();
        rig.settle().unwrap();
    }
    let (salts, staged) = staged_end(&rig).expect("the settles staged WAL frames");
    rig.vfs().cut_power();
    rig.reboot_after_power_loss().unwrap();
    let (recovered_salts, recovered) = recovered_end(&mut rig);
    assert_eq!(recovered_salts, salts, "the cut took the WAL header");
    assert!(
        recovered >= staged,
        "the replica holds WAL bytes up to {staged}, the database recovered only {recovered}"
    );
    let boots = rig.ticks().len();
    rig.insert(2500).unwrap();
    rig.settle().unwrap();
    let cause = rig.ticks().iter().skip(boots).find_map(boot_cause);
    assert_eq!(cause, Some(None), "the boot continues");
    rig.check().unwrap();
}

/// Linux reports a failed writeback once, to each descriptor, then calls the pages clean, so the
/// next fsync succeeds over bytes that never reached the disk. A failed sync of the WAL before
/// staging stages nothing, and the backend keeps refusing afterwards instead of trusting a sync
/// that succeeds: the frames written before the failure are not known to be durable.
#[test]
fn a_failed_wal_sync_stages_nothing_and_a_later_sync_does_not_clear_it() {
    let dir = TempDir::new().unwrap();
    let mut rig = Rig::<Sqlite>::new(dir.path(), config()).unwrap();
    rig.settle().unwrap();
    rig.insert(2500).unwrap();
    rig.settle().unwrap();
    let before = staged_end(&rig);
    rig.vfs().arm(Fault {
        kind: FileKind::Wal,
        op: Op::Sync,
        nth: NonZeroU32::MIN,
        effect: Effect::Error(Op::Sync.error()),
    });
    rig.insert(2500).unwrap();
    let settled = rig.settle();
    assert!(
        matches!(settled, Err(RigError::Unsettled(_))),
        "{settled:?}"
    );
    assert_eq!(rig.vfs().fired().len(), 1, "the engine's sync failed once");
    assert!(rig.faults().len() > 1, "it retried: {:?}", rig.faults());
    assert!(
        rig.faults()
            .iter()
            .all(|f| *f == EngineFault::LocalFsyncFailed),
        "{:?}",
        rig.faults()
    );
    assert_eq!(
        staged_end(&rig),
        before,
        "nothing staged past the failed sync"
    );
}

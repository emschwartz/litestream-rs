//! One scenario per [`Fault`], and the scenarios of faults that are not
//! faults: the clock stepping back.
//!
//! A persistent fault's scenarios hold under every set of responses: each
//! names its cause with [`Expect::Stopped`], which is a stop where the
//! responses stop and a degraded task where they keep trying, and the ones
//! whose fault clears also expect [`Expect::Recovered`].

use std::time::Duration;

use lsr_core::{
    Denied, Did, Event, Fault, FileKey, Level, MaintainedEvent, PersistentCause, SnapshotCause,
    SourceError, SourceOp, StopCause, StoreError, StoreOp,
};
use lsr_ltx::PageSize;

use super::{
    CauseKind, EventMatch, Expect, SEED, SETTLE, Scenario, build, commit, ticks,
    vacuum_page_size_change, vacuum_page_size_change_and_back,
};
use crate::generate::{EnvFault, MetaOp, Op, RemoteOp, Target, WalOp};
use crate::run::{RunReport, Seen};
use crate::seed::Seed;
use crate::sqlite::OtherProcessMode;
use crate::store::{FaultShape, RecorderEvent};

/// A [`Fault`] without its payload: one variant per fault, so a new fault
/// does not compile until [`fault_scenarios`] names its scenarios.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    /// [`Fault::StoreTransient`].
    StoreTransient,
    /// [`Fault::StoreThrottled`].
    StoreThrottled,
    /// [`Fault::StoreDenied`] for [`Denied::Credentials`].
    StoreCredentials,
    /// [`Fault::StoreDenied`] for [`Denied::Permission`].
    StorePermission,
    /// [`Fault::StoreDenied`] for [`Denied::ClockSkew`].
    StoreClockSkew,
    /// [`Fault::StoreDenied`] for [`Denied::BucketMissing`].
    StoreBucketMissing,
    /// [`Fault::StoreDenied`] for [`Denied::Tls`].
    StoreTls,
    /// [`Fault::RemoteObjectMissing`].
    RemoteObjectMissing,
    /// [`Fault::UploadAmbiguous`].
    UploadAmbiguous,
    /// [`Fault::UploadChecksumMismatch`].
    UploadChecksumMismatch,
    /// [`Fault::StoreRefused`].
    StoreRefused,
    /// [`Fault::RemoteCorrupt`].
    RemoteCorrupt,
    /// [`Fault::RemoteAheadOfLocal`].
    RemoteAheadOfLocal,
    /// [`Fault::RemoteRegressed`].
    RemoteRegressed,
    /// [`Fault::StagedLost`].
    StagedLost,
    /// [`Fault::LocalDiskFull`].
    LocalDiskFull,
    /// [`Fault::LocalIo`].
    LocalIo,
    /// [`Fault::LocalFsyncFailed`].
    LocalFsyncFailed,
    /// [`Fault::SqliteBusy`].
    SqliteBusy,
    /// [`Fault::ReadersHeld`].
    ReadersHeld,
    /// [`Fault::WritesHeld`].
    WritesHeld,
    /// [`Fault::WalMoved`].
    WalMoved,
    /// [`Fault::WalIndexInconsistent`].
    WalIndexInconsistent,
    /// [`Fault::WalChangedUnderRead`].
    WalChangedUnderRead,
    /// [`Fault::NothingToSnapshot`].
    NothingToSnapshot,
    /// [`Fault::ReadTxLost`].
    ReadTxLost,
    /// [`Fault::PageSizeChanged`].
    PageSizeChanged,
    /// [`Fault::WalCorruptBelowCommitted`].
    WalCorruptBelowCommitted,
    /// [`Fault::WalTruncatedBelowCommitted`].
    WalTruncatedBelowCommitted,
    /// [`Fault::WalUnlinked`].
    WalUnlinked,
    /// [`Fault::WalSyncFailed`].
    WalSyncFailed,
    /// [`Fault::DatabasePageMismatch`].
    DatabasePageMismatch,
    /// [`Fault::CreateOnlyUnsupported`].
    CreateOnlyUnsupported,
    /// [`Fault::ShutdownDeadline`].
    ShutdownDeadline,
    /// [`Fault::Bug`].
    Bug,
}

impl FaultKind {
    /// Every fault, in the order of [`Fault`].
    pub const ALL: [Self; 35] = [
        Self::StoreTransient,
        Self::StoreThrottled,
        Self::StoreCredentials,
        Self::StorePermission,
        Self::StoreClockSkew,
        Self::StoreBucketMissing,
        Self::StoreTls,
        Self::RemoteObjectMissing,
        Self::UploadAmbiguous,
        Self::UploadChecksumMismatch,
        Self::StoreRefused,
        Self::RemoteCorrupt,
        Self::RemoteAheadOfLocal,
        Self::RemoteRegressed,
        Self::StagedLost,
        Self::LocalDiskFull,
        Self::LocalIo,
        Self::LocalFsyncFailed,
        Self::SqliteBusy,
        Self::ReadersHeld,
        Self::WritesHeld,
        Self::WalMoved,
        Self::WalIndexInconsistent,
        Self::WalChangedUnderRead,
        Self::NothingToSnapshot,
        Self::ReadTxLost,
        Self::PageSizeChanged,
        Self::WalCorruptBelowCommitted,
        Self::WalTruncatedBelowCommitted,
        Self::WalUnlinked,
        Self::WalSyncFailed,
        Self::DatabasePageMismatch,
        Self::CreateOnlyUnsupported,
        Self::ShutdownDeadline,
        Self::Bug,
    ];
}

impl FaultKind {
    /// The kind for one access refusal.
    #[must_use]
    pub const fn denied(why: Denied) -> Self {
        match why {
            Denied::Credentials => Self::StoreCredentials,
            Denied::Permission => Self::StorePermission,
            Denied::ClockSkew => Self::StoreClockSkew,
            Denied::BucketMissing => Self::StoreBucketMissing,
            Denied::Tls => Self::StoreTls,
        }
    }
}

impl From<&Fault> for FaultKind {
    fn from(fault: &Fault) -> Self {
        match fault {
            Fault::StoreTransient => Self::StoreTransient,
            Fault::StoreThrottled { .. } => Self::StoreThrottled,
            Fault::StoreDenied(why) => Self::denied(*why),
            Fault::RemoteObjectMissing => Self::RemoteObjectMissing,
            Fault::UploadAmbiguous => Self::UploadAmbiguous,
            Fault::UploadChecksumMismatch => Self::UploadChecksumMismatch,
            Fault::StoreRefused => Self::StoreRefused,
            Fault::RemoteCorrupt => Self::RemoteCorrupt,
            Fault::RemoteAheadOfLocal => Self::RemoteAheadOfLocal,
            Fault::RemoteRegressed => Self::RemoteRegressed,
            Fault::StagedLost => Self::StagedLost,
            Fault::LocalDiskFull => Self::LocalDiskFull,
            Fault::LocalIo => Self::LocalIo,
            Fault::LocalFsyncFailed => Self::LocalFsyncFailed,
            Fault::SqliteBusy => Self::SqliteBusy,
            Fault::ReadersHeld => Self::ReadersHeld,
            Fault::WritesHeld => Self::WritesHeld,
            Fault::WalMoved => Self::WalMoved,
            Fault::WalIndexInconsistent => Self::WalIndexInconsistent,
            Fault::WalChangedUnderRead => Self::WalChangedUnderRead,
            Fault::NothingToSnapshot => Self::NothingToSnapshot,
            Fault::ReadTxLost => Self::ReadTxLost,
            Fault::PageSizeChanged { .. } => Self::PageSizeChanged,
            Fault::WalCorruptBelowCommitted => Self::WalCorruptBelowCommitted,
            Fault::WalTruncatedBelowCommitted => Self::WalTruncatedBelowCommitted,
            Fault::WalUnlinked => Self::WalUnlinked,
            Fault::WalSyncFailed => Self::WalSyncFailed,
            Fault::DatabasePageMismatch { .. } => Self::DatabasePageMismatch,
            Fault::CreateOnlyUnsupported => Self::CreateOnlyUnsupported,
            Fault::ShutdownDeadline => Self::ShutdownDeadline,
            Fault::Bug(_) => Self::Bug,
        }
    }
}

/// The persistent cause a fault's policy names, if it has one.
#[must_use]
pub fn persistent_of(kind: FaultKind) -> Option<PersistentCause> {
    stop_of(kind).and_then(PersistentCause::of)
}

/// The stop cause a fault's policy names, if its policy can stop the engine.
#[must_use]
pub fn stop_of(kind: FaultKind) -> Option<StopCause> {
    let page = PageSize::MIN;
    match kind {
        FaultKind::StoreCredentials => Some(StopCause::StoreDenied(Denied::Credentials)),
        FaultKind::StorePermission => Some(StopCause::StoreDenied(Denied::Permission)),
        FaultKind::StoreClockSkew => Some(StopCause::StoreDenied(Denied::ClockSkew)),
        FaultKind::StoreBucketMissing => Some(StopCause::StoreDenied(Denied::BucketMissing)),
        FaultKind::StoreTls => Some(StopCause::StoreDenied(Denied::Tls)),
        FaultKind::StoreRefused => Some(StopCause::StoreRefused),
        FaultKind::RemoteCorrupt => Some(StopCause::RemoteCorrupt),
        FaultKind::RemoteAheadOfLocal => Some(StopCause::OtherWriter),
        FaultKind::RemoteRegressed => Some(StopCause::RemoteRegressed),
        FaultKind::PageSizeChanged => Some(StopCause::PageSizeChanged {
            was: page,
            now: page,
        }),
        FaultKind::WalCorruptBelowCommitted => Some(StopCause::WalCorrupt),
        FaultKind::WalTruncatedBelowCommitted => Some(StopCause::WalTruncated),
        FaultKind::WalUnlinked => Some(StopCause::WalUnlinked),
        FaultKind::WalSyncFailed => Some(StopCause::WalSyncFailed),
        FaultKind::DatabasePageMismatch => {
            lsr_ltx::PageNo::new(1, page).map(|pgno| StopCause::DatabasePageMismatch { pgno })
        }
        FaultKind::CreateOnlyUnsupported => Some(StopCause::ConditionalWritesUnsupported),
        FaultKind::ShutdownDeadline => Some(StopCause::Shutdown),
        FaultKind::Bug => Some(StopCause::Bug),
        FaultKind::StoreTransient
        | FaultKind::StoreThrottled
        | FaultKind::RemoteObjectMissing
        | FaultKind::UploadAmbiguous
        | FaultKind::UploadChecksumMismatch
        | FaultKind::StagedLost
        | FaultKind::LocalDiskFull
        | FaultKind::LocalIo
        | FaultKind::LocalFsyncFailed
        | FaultKind::SqliteBusy
        | FaultKind::ReadersHeld
        | FaultKind::WritesHeld
        | FaultKind::WalMoved
        | FaultKind::WalIndexInconsistent
        | FaultKind::WalChangedUnderRead
        | FaultKind::NothingToSnapshot
        | FaultKind::ReadTxLost => None,
    }
}

/// Whether a scenario's expectations name this fault: the retry it ends in,
/// or the stop cause its policy names.
#[must_use]
pub fn names_fault(scenario: &Scenario, kind: FaultKind) -> bool {
    scenario
        .expect
        .iter()
        .any(|want| match (want, stop_of(kind)) {
            (Expect::Event(EventMatch::Retrying(got)), _) => *got == kind,
            (Expect::Event(EventMatch::WritesHeld), _) => kind == FaultKind::WritesHeld,
            // The lost file's policy is a snapshot at the first transaction the
            // replica lacks, announced as this resnapshot.
            (Expect::Event(EventMatch::Resnapshot(CauseKind::NewestStagedFileUnreadable)), _) => {
                kind == FaultKind::StagedLost
            }
            (Expect::Stopped(got @ StopCause::StoreDenied(_)), Some(stop)) => *got == stop,
            (Expect::Stopped(got), Some(stop)) => {
                std::mem::discriminant(got) == std::mem::discriminant(&stop)
            }
            _ => false,
        })
}

/// The scenarios that make the engine meet [`kind`].
#[must_use]
pub fn fault_scenarios(kind: FaultKind) -> &'static [fn() -> Scenario] {
    match kind {
        FaultKind::StoreTransient => &[store_flaky_5pct, commit_fails_after_publish],
        FaultKind::StoreThrottled => &[store_429_retry_after],
        FaultKind::StoreCredentials => &[creds_revoked_mid_run],
        FaultKind::StorePermission => &[permission_revoked_mid_run],
        FaultKind::StoreClockSkew => &[clock_skewed_against_the_store_mid_run],
        FaultKind::StoreBucketMissing => &[bucket_deleted_mid_run],
        FaultKind::StoreTls => &[tls_fails_mid_run],
        FaultKind::RemoteObjectMissing => &[remote_file_deleted_externally],
        FaultKind::UploadAmbiguous => &[size_mismatch_on_commit],
        FaultKind::UploadChecksumMismatch => &[etag_mismatch_on_put],
        FaultKind::StoreRefused => &[store_refuses_upload],
        FaultKind::RemoteCorrupt => &[remote_bitrot_on_list_read],
        FaultKind::RemoteAheadOfLocal => &[
            second_writer_mid_run,
            second_writer_survives_restart,
            super::foreign_file_while_down_stops,
        ],
        FaultKind::RemoteRegressed => &[bucket_rolled_back, bucket_rolled_back_while_idle],
        FaultKind::StagedLost => &[staged_byte_flip_before_upload, meta_dir_deleted_mid_run],
        FaultKind::LocalDiskFull => &[disk_full_on_staging],
        FaultKind::LocalIo => &[eio_on_wal_read],
        FaultKind::LocalFsyncFailed => &[fsync_fails_once],
        FaultKind::SqliteBusy => &[busy_on_barrier, shm_header_mid_update],
        FaultKind::ReadersHeld => &[readers_hold_the_wal],
        FaultKind::WritesHeld => &[writes_held_through_a_checkpoint],
        FaultKind::WalMoved => &[wal_header_torn_while_running],
        FaultKind::WalIndexInconsistent => &[shm_end_inside_tx],
        FaultKind::WalChangedUnderRead => &[frame_overwritten_between_scan_and_read],
        FaultKind::NothingToSnapshot => &[db_created_no_pages],
        FaultKind::ReadTxLost => &[reacquire_fails_after_checkpoint],
        FaultKind::PageSizeChanged => &[vacuum_page_size_change, vacuum_page_size_change_and_back],
        FaultKind::WalCorruptBelowCommitted => &[
            bit_flip_in_committed_frame,
            bit_flip_in_committed_frame_healed,
        ],
        FaultKind::WalTruncatedBelowCommitted => &[wal_truncated_externally_under_lock],
        FaultKind::WalUnlinked => &[wal_unlinked_mid_run],
        FaultKind::WalSyncFailed => &[
            wal_fsync_fails,
            wal_fsync_fails_then_power_loss,
            wal_fsync_fails_then_restart,
        ],
        FaultKind::DatabasePageMismatch => &[db_page_flipped_under_sync],
        FaultKind::CreateOnlyUnsupported => &[create_only_refused_mid_run],
        FaultKind::ShutdownDeadline => &[sigterm_during_upload],
        FaultKind::Bug => &[],
    }
}

/// Why a fault has no scenario that holds under every response set. [`FaultKind::Bug`]: the
/// oracle's bug-fault violation fails every run that meets it, so every scenario and every
/// random history asserts it never fires.
#[must_use]
pub const fn fault_reason(kind: FaultKind) -> Option<&'static str> {
    if matches!(kind, FaultKind::Bug) {
        Some("every run fails on it: the oracle's BugFault violation")
    } else {
        None
    }
}

/// Scenarios that are not tied to one fault: the clock steps back.
#[must_use]
pub fn clock_scenarios() -> &'static [fn() -> Scenario] {
    &[
        clock_steps_back_10s,
        clock_steps_back_an_hour_mid_retry,
        clock_jumps_forward_an_hour,
    ]
}

/// Scenarios that are not tied to one fault: the application checkpoints on its own.
#[must_use]
pub fn app_scenarios() -> &'static [fn() -> Scenario] {
    &[app_autocheckpoints]
}

/// A checkpoint whose seal staged the app's commit inside the tick, and
/// whose next step then failed: the scenarios that are not tied to one fault.
/// A failed barrier release has none: the rules keep a barrier whose release
/// failed held, so the app never writes again (core's actor tests cover it).
#[must_use]
pub fn seal_scenarios() -> &'static [fn() -> Scenario] {
    &[
        sealed_then_checkpoint_busy,
        sealed_then_checkpoint_io,
        sealed_then_bump_busy,
        sealed_then_bump_io,
    ]
}

/// The crash windows of a compaction's output: the scenarios that are not tied
/// to one fault.
#[must_use]
pub fn window_scenarios() -> &'static [fn() -> Scenario] {
    &[
        corrupt_l1_publish_then_power_loss,
        corrupt_l2_publish_then_power_loss,
        corrupt_l9_publish_then_power_loss,
        corrupt_l0_publish_then_power_loss,
    ]
}

fn remote_fault(op: StoreOp, ahead: u16, shape: FaultShape) -> Op {
    Op::StoreFault {
        target: Target::Remote,
        op,
        ahead,
        shape,
    }
}

fn staging_fault(op: StoreOp, ahead: u16, shape: FaultShape) -> Op {
    Op::StoreFault {
        target: Target::Staging,
        op,
        ahead,
        shape,
    }
}

fn transient(op: StoreOp) -> StoreError {
    StoreError::Transient {
        op,
        kind: std::io::ErrorKind::ConnectionReset,
    }
}

/// A snapshot and its upload, with the engine up.
fn live() -> Vec<Op> {
    let mut ops = vec![commit(3, false)];
    ops.extend(ticks(4));
    ops
}

/// [`live`], then `fault`, then more work and the oracle.
fn injected(name: &'static str, fault: Vec<Op>, expect: Vec<Expect>) -> Scenario {
    let mut expect = expect;
    expect.push(Expect::Oracle);
    build(
        name,
        vec![
            live(),
            fault,
            vec![commit(2, false)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        expect,
    )
}

fn retries(kind: FaultKind) -> Expect {
    Expect::Event(EventMatch::Retrying(kind))
}

/// Every call to the replica fails now and then, at 5 in 100.
#[must_use]
pub fn store_flaky_5pct() -> Scenario {
    let mut s = build(
        "store_flaky_5pct",
        vec![
            live(),
            vec![
                remote_fault(
                    StoreOp::Upload,
                    0,
                    FaultShape::Rate {
                        per_mille: 50,
                        err: transient(StoreOp::Upload),
                    },
                ),
                remote_fault(
                    StoreOp::List,
                    0,
                    FaultShape::Rate {
                        per_mille: 50,
                        err: transient(StoreOp::List),
                    },
                ),
            ],
            (0..40)
                .flat_map(|_| [commit(2, false), Op::Tick, Op::Tick])
                .collect(),
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![retries(FaultKind::StoreTransient), Expect::Oracle],
    );
    s.seed = Seed::new(SEED);
    s
}

/// The store throttles one upload and names a wait longer than the first
/// backoff. The retry waits for the later of the two, and a tick that comes
/// early makes no call to the replica.
#[must_use]
pub fn store_429_retry_after() -> Scenario {
    let throttled = StoreError::Throttled {
        op: StoreOp::Upload,
        retry_after: Some(THROTTLE),
    };
    let mut s = build(
        "store_429_retry_after",
        vec![
            live(),
            vec![
                remote_fault(StoreOp::Upload, 0, FaultShape::Error(throttled)),
                commit(2, false),
                Op::Tick,
                Op::Tick,
                // The clock is at the retry time now; one second before the
                // wait ends is later than the tick that failed.
                Op::ClockJump(-1_500),
                Op::Tick,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![retries(FaultKind::StoreThrottled), Expect::Oracle],
    );
    s.check = Some(throttle_waits);
    s
}

const THROTTLE: Duration = Duration::from_secs(2);

fn throttle_waits(report: &RunReport) -> Result<(), String> {
    use lsr_core::{Backoff, backoff_wait};
    let mut began = None;
    let mut waiting: Option<lsr_ltx::Timestamp> = None;
    let mut early = 0_u32;
    for seen in &report.seen {
        match seen {
            Seen::Tick {
                at, remote_calls, ..
            } => {
                began = Some(*at);
                if let Some(until) = waiting {
                    if *at < until {
                        early = early.saturating_add(1);
                        if *remote_calls != 0 {
                            return Err(format!(
                                "{remote_calls} calls to the replica at {at:?}, before {until:?}"
                            ));
                        }
                    } else {
                        waiting = None;
                    }
                }
            }
            Seen::Event(Event::Retrying {
                fault:
                    Fault::StoreThrottled {
                        retry_after: Some(named),
                    },
                until,
            }) => {
                let at = began.ok_or("a retry with no tick before it")?;
                let wait = backoff_wait(
                    Backoff::After(*named),
                    std::num::NonZeroU32::MIN,
                    std::time::Duration::from_secs(1),
                );
                let want = at
                    .unix_millis()
                    .saturating_add(i64::try_from(wait.as_millis()).unwrap_or(i64::MAX));
                if until.unix_millis() != want {
                    return Err(format!(
                        "waits until {until:?}, the later of the backoff and the named wait is {want}"
                    ));
                }
                waiting = Some(*until);
            }
            Seen::Event(_)
            | Seen::Stop(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle => {}
        }
    }
    if early == 0 {
        return Err("no tick came before the wait ended".to_owned());
    }
    Ok(())
}

/// A stop for the scenario's fault, not the end of a drain whose deadline a
/// degraded task outlasted.
fn stopped_by_fault(seen: &Seen) -> bool {
    matches!(seen, Seen::Stop(cause) if *cause != StopCause::Shutdown)
}

/// No later tick calls the replica: the stop is final until a restart. Where
/// the responses keep trying there is no stop to be quiet after: a task was
/// degraded instead, which the expectations check.
fn quiet_after_stop(report: &RunReport) -> Result<(), String> {
    let degraded = report
        .seen
        .iter()
        .any(|seen| matches!(seen, Seen::Event(Event::Degraded { .. })));
    if degraded && !report.seen.iter().any(stopped_by_fault) {
        return Ok(());
    }
    let mut stopped = false;
    for (at, seen) in report.seen.iter().enumerate() {
        match seen {
            Seen::Stop(_) if stopped_by_fault(seen) => stopped = true,
            // A stopped engine only reports its stop: a boot asking for a
            // takeover record is a new process's, as a boot is.
            Seen::Event(Event::Booted { .. } | Event::TakeoverWanted(_)) => stopped = false,
            // A restore's reboot lists the replica in its first tick, and the
            // oracle has just judged: that tick is not the stopped engine's.
            Seen::Tick { remote_calls, .. }
                if stopped
                    && *remote_calls != 0
                    && !matches!(
                        at.checked_sub(1).and_then(|p| report.seen.get(p)),
                        Some(Seen::Oracle)
                    ) =>
            {
                return Err(format!(
                    "{remote_calls} calls to the replica after the stop"
                ));
            }
            Seen::Event(_)
            | Seen::Stop(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    if report.seen.iter().any(stopped_by_fault) {
        Ok(())
    } else {
        Err("the engine never stopped".to_owned())
    }
}

/// The store refuses an upload for `why` in the middle of the run: a stop
/// under that kind of refusal and no other, and no further calls.
fn store_denies_upload(name: &'static str, why: Denied) -> Scenario {
    let mut s = injected(
        name,
        vec![remote_fault(
            StoreOp::Upload,
            0,
            FaultShape::Error(StoreError::Denied {
                op: StoreOp::Upload,
                why,
            }),
        )],
        vec![
            Expect::Stopped(StopCause::StoreDenied(why)),
            Expect::Recovered,
        ],
    );
    s.check = Some(quiet_after_stop);
    s
}

/// The credentials stop working in the middle of the run.
#[must_use]
pub fn creds_revoked_mid_run() -> Scenario {
    store_denies_upload("creds_revoked_mid_run", Denied::Credentials)
}

/// A policy starts refusing uploads in the middle of the run.
#[must_use]
pub fn permission_revoked_mid_run() -> Scenario {
    store_denies_upload("permission_revoked_mid_run", Denied::Permission)
}

/// The host clock drifts too far from the store's in the middle of the run.
#[must_use]
pub fn clock_skewed_against_the_store_mid_run() -> Scenario {
    store_denies_upload("clock_skewed_against_the_store_mid_run", Denied::ClockSkew)
}

/// The bucket is deleted in the middle of the run.
#[must_use]
pub fn bucket_deleted_mid_run() -> Scenario {
    store_denies_upload("bucket_deleted_mid_run", Denied::BucketMissing)
}

/// The store's certificate stops verifying in the middle of the run.
#[must_use]
pub fn tls_fails_mid_run() -> Scenario {
    store_denies_upload("tls_fails_mid_run", Denied::Tls)
}

/// The backend refuses an upload for a reason a retry does not change.
#[must_use]
pub fn store_refuses_upload() -> Scenario {
    let mut s = injected(
        "store_refuses_upload",
        vec![remote_fault(
            StoreOp::Upload,
            0,
            FaultShape::Error(StoreError::Refused {
                op: StoreOp::Upload,
                status: 403,
            }),
        )],
        vec![Expect::Stopped(StopCause::StoreRefused), Expect::Recovered],
    );
    s.check = Some(quiet_after_stop);
    s
}

fn some_key() -> lsr_core::FileKey {
    lsr_core::FileKey::new(
        lsr_core::Level::ZERO,
        lsr_ltx::TxidRange::single(lsr_ltx::Txid::MIN),
    )
}

/// A file the listing named is gone when boot reads it back: another process
/// deleted it (the first read after the listing). A compaction that loses an input replans
/// instead, so boot is where this fault reaches the engine.
#[must_use]
pub fn remote_file_deleted_externally() -> Scenario {
    build(
        "remote_file_deleted_externally",
        vec![
            vec![commit(3, false)],
            ticks(4),
            vec![
                remote_fault(
                    StoreOp::Read,
                    0,
                    FaultShape::Error(StoreError::NotFound { key: some_key() }),
                ),
                Op::Restart,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![retries(FaultKind::RemoteObjectMissing), Expect::Oracle],
    )
}

/// The commit publishes the object and then fails: the response was lost.
#[must_use]
pub fn commit_fails_after_publish() -> Scenario {
    injected(
        "commit_fails_after_publish",
        vec![remote_fault(
            StoreOp::Upload,
            0,
            FaultShape::ErrorAfterPublish(transient(StoreOp::Upload)),
        )],
        vec![retries(FaultKind::StoreTransient)],
    )
}

/// The commit reports a size that is not the size written.
#[must_use]
pub fn size_mismatch_on_commit() -> Scenario {
    injected(
        "size_mismatch_on_commit",
        vec![remote_fault(StoreOp::Upload, 0, FaultShape::SizeMismatch)],
        vec![retries(FaultKind::UploadAmbiguous)],
    )
}

/// The store says the stored bytes are not the ones sent. The uploader deletes
/// the key and writes it again with no listing between.
#[must_use]
pub fn etag_mismatch_on_put() -> Scenario {
    let mut s = injected(
        "etag_mismatch_on_put",
        vec![remote_fault(
            StoreOp::Upload,
            0,
            FaultShape::ChecksumMismatch,
        )],
        vec![retries(FaultKind::UploadChecksumMismatch)],
    );
    s.check = Some(deleted_then_put_again);
    s
}

fn deleted_then_put_again(report: &RunReport) -> Result<(), String> {
    let mut deleted = None;
    let mut written_again = false;
    for event in &report.remote_events {
        match event {
            RecorderEvent::Put { key, .. } if deleted == Some(*key) => {
                written_again = true;
                break;
            }
            RecorderEvent::Delete { keys, .. } if keys.len() == 1 => {
                deleted = keys.first().copied()
            }
            RecorderEvent::Put { .. } | RecorderEvent::Delete { .. } => {}
        }
    }
    if !written_again {
        return Err("the key was not deleted and then written again".to_owned());
    }
    // From the rejection to the next upload, nothing lists the replica.
    let mut rejected = false;
    for seen in &report.seen {
        match seen {
            Seen::Event(Event::Retrying {
                fault: Fault::UploadChecksumMismatch,
                ..
            }) => rejected = true,
            Seen::Event(Event::Uploaded(_)) if rejected => break,
            Seen::Tick { remote_lists, .. } if rejected && *remote_lists != 0 => {
                return Err(
                    "the replica was listed between the rejection and the rewrite".to_owned(),
                );
            }
            Seen::Event(_)
            | Seen::Stop(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    Ok(())
}

/// The staging directory is gone and the replica is read back at boot (the first read
/// after the listing): one byte of the newest object has rotted, there is no staged copy to compare with, so
/// the object must decode, and it does not.
#[must_use]
pub fn remote_bitrot_on_list_read() -> Scenario {
    build(
        "remote_bitrot_on_list_read",
        vec![
            live(),
            vec![
                remote_fault(StoreOp::Read, 0, FaultShape::FlipByteOnRead { at: 100 }),
                Op::EnvFault(EnvFault::WipeStaging),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Stopped(StopCause::RemoteCorrupt),
            Expect::Recovered,
            Expect::Oracle,
        ],
    )
}

/// Another writer put two transactions on the replica while the engine staged
/// one. The upload's reply is lost, so the engine lists, and finds the replica
/// ahead of everything it staged.
#[must_use]
pub fn second_writer_mid_run() -> Scenario {
    let mut s = build(
        "second_writer_mid_run",
        vec![
            live(),
            vec![
                remote_fault(
                    StoreOp::Upload,
                    0,
                    FaultShape::ErrorAfterPublish(transient(StoreOp::Upload)),
                ),
                commit(2, false),
                Op::Remote(RemoteOp::PutUnrecognized),
                Op::Remote(RemoteOp::PutUnrecognized),
            ],
            ticks(SETTLE),
            vec![Op::Remote(RemoteOp::RemoveUnrecognized), Op::Restore],
        ],
        vec![Expect::Stopped(StopCause::OtherWriter), Expect::Oracle],
    );
    s.check = Some(quiet_after_stop);
    s
}

/// As [`second_writer_mid_run`], then a supervisor restarts the engine while
/// the other writer's files still stand: the restart must stop again, not take
/// them as history and snapshot above them. Once an operator removes them and
/// restarts it once more, the engine replicates again.
#[must_use]
pub fn second_writer_survives_restart() -> Scenario {
    let mut s = build(
        "second_writer_survives_restart",
        vec![
            live(),
            vec![
                remote_fault(
                    StoreOp::Upload,
                    0,
                    FaultShape::ErrorAfterPublish(transient(StoreOp::Upload)),
                ),
                commit(2, false),
                Op::Remote(RemoteOp::PutUnrecognized),
                Op::Remote(RemoteOp::PutUnrecognized),
            ],
            ticks(SETTLE),
            vec![Op::Restart, commit(1, false)],
            ticks(SETTLE),
            vec![
                Op::Remote(RemoteOp::RemoveUnrecognized),
                Op::Remote(RemoteOp::RemoveUnrecognized),
                Op::Restart,
                commit(1, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Stopped(StopCause::OtherWriter), Expect::Oracle],
    );
    s.check = Some(stop_survives_restart);
    s
}

/// The first stop for another writer is followed, at the restart's boot (the
/// first tick after it that calls the replica), by another stop and nothing
/// else: no boot, no snapshot above the other writer's files. Later a boot
/// replicates and no stop follows it.
fn stop_survives_restart(report: &RunReport) -> Result<(), String> {
    let seen = &report.seen;
    let other = |seen: &Seen| matches!(seen, Seen::Stop(StopCause::OtherWriter));
    let first = seen
        .iter()
        .position(other)
        .ok_or("the engine never stopped for another writer")?;
    let calls = |seen: &Seen| matches!(seen, Seen::Tick { remote_calls, .. } if *remote_calls > 0);
    let boot = seen
        .iter()
        .skip(first)
        .position(calls)
        .map(|at| at.saturating_add(first))
        .ok_or("no tick after the stop called the replica")?;
    let verdict = seen
        .iter()
        .skip(boot.saturating_add(1))
        .find(|seen| !matches!(seen, Seen::Tick { .. }));
    if !verdict.is_some_and(other) {
        return Err(format!(
            "the restart with the other writer's files still there did {verdict:?}, not stop"
        ));
    }
    let booted = seen
        .iter()
        .rposition(|seen| matches!(seen, Seen::Event(Event::Booted { .. })))
        .filter(|at| *at > boot)
        .ok_or("the engine never booted once the other writer's files were gone")?;
    let after = seen.get(booted..).unwrap_or_default();
    if after.iter().any(other) {
        return Err("the engine still stopped once the other writer's files were gone".to_owned());
    }
    let replicated = seen
        .iter()
        .skip(boot)
        .any(|seen| matches!(seen, Seen::Event(Event::Uploaded(_))));
    if !replicated {
        return Err("the engine never replicated again after the operator's restart".to_owned());
    }
    Ok(())
}

/// The replica lost its newest files after the engine had confirmed them: the
/// next listing is behind what was confirmed.
#[must_use]
pub fn bucket_rolled_back() -> Scenario {
    let mut s = build(
        "bucket_rolled_back",
        vec![
            live(),
            vec![
                commit(2, false),
                Op::Tick,
                Op::Tick,
                remote_fault(
                    StoreOp::Upload,
                    0,
                    FaultShape::ErrorAfterPublish(transient(StoreOp::Upload)),
                ),
                commit(2, false),
                Op::Tick,
                Op::Tick,
                Op::Remote(RemoteOp::RollBack),
                Op::Remote(RemoteOp::RollBack),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Stopped(StopCause::RemoteRegressed),
            Expect::Recovered,
            Expect::Oracle,
        ],
    );
    s.check = Some(quiet_after_stop);
    s
}

/// The replica loses its newest file while the engine is idle, after its upload and before
/// the maintenance listing that follows it: the listing ends below what the engine confirmed,
/// the regression is reported and, as configured, replication goes on. The staged copy is
/// still there, so the engine sends it again, and a restore finds the replica whole.
#[must_use]
pub fn bucket_rolled_back_while_idle() -> Scenario {
    build(
        "bucket_rolled_back_while_idle",
        vec![
            live(),
            vec![
                commit(2, false),
                Op::Tick,
                Op::Tick,
                Op::Remote(RemoteOp::RollBack),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Stopped(StopCause::RemoteRegressed),
            Expect::Recovered,
            Expect::Oracle,
        ],
    )
}

/// One staged file does not verify at upload, and a later one is staged after
/// it.
#[must_use]
pub fn staged_byte_flip_before_upload() -> Scenario {
    let mut s = build(
        "staged_byte_flip_before_upload",
        vec![
            live(),
            vec![
                remote_fault(
                    StoreOp::Upload,
                    0,
                    FaultShape::Error(transient(StoreOp::Upload)),
                ),
                commit(2, false),
                Op::Tick,
                commit(2, false),
                Op::Tick,
                Op::Meta(MetaOp::FlipNewestByte),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Resnapshot(
                CauseKind::NewestStagedFileUnreadable,
            )),
            Expect::Event(EventMatch::Snapshotted),
            Expect::Oracle,
        ],
    );
    s.check = Some(staged_files_dropped);
    s
}

/// The staging directory is deleted while the engine runs.
#[must_use]
pub fn meta_dir_deleted_mid_run() -> Scenario {
    let mut s = build(
        "meta_dir_deleted_mid_run",
        vec![
            live(),
            vec![
                remote_fault(
                    StoreOp::Upload,
                    0,
                    FaultShape::Error(transient(StoreOp::Upload)),
                ),
                commit(2, false),
                Op::Tick,
                commit(2, false),
                Op::Tick,
                Op::Meta(MetaOp::DeleteAll),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Resnapshot(
                CauseKind::NewestStagedFileUnreadable,
            )),
            Expect::Event(EventMatch::Snapshotted),
            Expect::Oracle,
        ],
    );
    s.check = Some(staged_files_dropped);
    s
}

fn staged_files_dropped(report: &RunReport) -> Result<(), String> {
    let mut lost = false;
    for seen in &report.seen {
        match seen {
            Seen::Stop(cause) => return Err(format!("stopped: {cause:?}")),
            Seen::Event(Event::Degraded { cause, .. }) => {
                return Err(format!("degraded: {cause:?}"));
            }
            Seen::Event(Event::Resnapshot(SnapshotCause::NewestStagedFileUnreadable)) => {
                lost = true
            }
            Seen::Event(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    if lost {
        Ok(())
    } else {
        Err("the lost staged file did not turn into a snapshot".to_owned())
    }
}

/// Staging has no room for the next file.
#[must_use]
pub fn disk_full_on_staging() -> Scenario {
    let full = StoreError::ScratchExhausted { key: some_key() };
    injected(
        "disk_full_on_staging",
        vec![staging_fault(StoreOp::Upload, 0, FaultShape::Error(full))],
        vec![retries(FaultKind::LocalDiskFull)],
    )
}

/// Reading the WAL fails with EIO.
#[must_use]
pub fn eio_on_wal_read() -> Scenario {
    injected(
        "eio_on_wal_read",
        vec![
            commit(2, false),
            Op::EnvFault(EnvFault::Source(
                SourceOp::Open,
                SourceError::Io(std::io::ErrorKind::Other),
            )),
        ],
        vec![retries(FaultKind::LocalIo)],
    )
}

/// The staging directory's fsync fails once: the written size differs.
#[must_use]
pub fn fsync_fails_once() -> Scenario {
    injected(
        "fsync_fails_once",
        vec![staging_fault(
            StoreOp::Upload,
            0,
            FaultShape::Error(StoreError::Corrupt {
                key: Some(some_key()),
                cause: lsr_core::CorruptCause::CommitSizeMismatch {
                    wrote: 100,
                    stored: 99,
                    removed: true,
                },
            }),
        )],
        vec![retries(FaultKind::LocalFsyncFailed)],
    )
}

/// SQLite is busy when the checkpoint takes its barrier.
#[must_use]
pub fn busy_on_barrier() -> Scenario {
    injected(
        "busy_on_barrier",
        vec![
            Op::EnvFault(EnvFault::Source(SourceOp::BeginBarrier, SourceError::Busy)),
            commit(9, true),
        ],
        vec![retries(FaultKind::SqliteBusy)],
    )
}

/// The application's readers hold the WAL when a checkpoint is asked for: it is
/// refused before SQLite is asked, and retried.
#[must_use]
pub fn readers_hold_the_wal() -> Scenario {
    injected(
        "readers_hold_the_wal",
        vec![
            Op::EnvFault(EnvFault::Source(
                SourceOp::Checkpoint,
                SourceError::ReadersHeld,
            )),
            commit(9, true),
        ],
        vec![retries(FaultKind::ReadersHeld)],
    )
}

/// The replicator holds the application's writes when a checkpoint comes due: the checkpoint
/// waits, uncounted and never degraded, and runs once writes go through again.
#[must_use]
pub fn writes_held_through_a_checkpoint() -> Scenario {
    injected(
        "writes_held_through_a_checkpoint",
        vec![
            Op::EnvFault(EnvFault::Source(
                SourceOp::Checkpoint,
                SourceError::WritesHeld,
            )),
            commit(9, true),
        ],
        vec![Expect::Event(EventMatch::WritesHeld)],
    )
}

/// A writer is mid-update in the wal-index header.
#[must_use]
pub fn shm_header_mid_update() -> Scenario {
    injected(
        "shm_header_mid_update",
        vec![commit(2, false), Op::EnvFault(EnvFault::ShmMidUpdate)],
        vec![retries(FaultKind::SqliteBusy)],
    )
}

/// A TRUNCATE from outside empties the WAL between the engine's steps, while
/// its read was lapsed. The next sync is a `WalMissing` snapshot, which writes
/// the WAL header itself rather than wait for the app's next write.
#[must_use]
pub fn restart_between_steps() -> Scenario {
    build(
        "restart_between_steps",
        vec![
            live(),
            vec![
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Resnapshot(CauseKind::WalMissing)),
            Expect::Event(EventMatch::Snapshotted),
            Expect::Oracle,
        ],
    )
}

/// The WAL header is zeroed under a running engine. The snapshot that follows
/// writes a frame to get a header, but SQLite writes a header only at the
/// start of a log, so the header stays torn and the step retries as the WAL
/// having moved, on the task's backoff, until a TRUNCATE and a commit heal it.
#[must_use]
pub fn wal_header_torn_while_running() -> Scenario {
    build(
        "wal_header_torn_while_running",
        vec![
            live(),
            vec![Op::Wal(WalOp::TearHeader)],
            ticks(SETTLE),
            vec![
                Op::OurReader { begin: false },
                Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
                commit(1, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Resnapshot(CauseKind::WalMissing)),
            retries(FaultKind::WalMoved),
            Expect::Event(EventMatch::Snapshotted),
            Expect::Oracle,
        ],
    )
}

/// The wal-index end falls inside a transaction.
#[must_use]
pub fn shm_end_inside_tx() -> Scenario {
    injected(
        "shm_end_inside_tx",
        vec![commit(3, false), Op::EnvFault(EnvFault::ShmEndInsideTx)],
        vec![retries(FaultKind::WalIndexInconsistent)],
    )
}

/// A frame is overwritten after the scan verified it and before its page is
/// read.
#[must_use]
pub fn frame_overwritten_between_scan_and_read() -> Scenario {
    injected(
        "frame_overwritten_between_scan_and_read",
        vec![commit(3, false), Op::EnvFault(EnvFault::OverwriteLastFrame)],
        vec![retries(FaultKind::WalChangedUnderRead)],
    )
}

/// The database exists and holds no page yet.
#[must_use]
pub fn db_created_no_pages() -> Scenario {
    build(
        "db_created_no_pages",
        vec![
            vec![Op::EnvFault(EnvFault::EmptyDatabase)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![retries(FaultKind::NothingToSnapshot), Expect::Oracle],
    )
}

/// The engine's read transaction is gone after a checkpoint.
#[must_use]
pub fn reacquire_fails_after_checkpoint() -> Scenario {
    injected(
        "reacquire_fails_after_checkpoint",
        vec![
            Op::EnvFault(EnvFault::Source(
                SourceOp::Checkpoint,
                SourceError::ReadTxLost,
            )),
            commit(9, true),
        ],
        vec![retries(FaultKind::ReadTxLost)],
    )
}

/// One bit of a committed frame that is not yet synced flips on disk, and
/// stays flipped. Where the responses keep trying, the sync task stays
/// degraded to the end: nothing past the frame is staged, and no snapshot is.
#[must_use]
pub fn bit_flip_in_committed_frame() -> Scenario {
    let mut s = injected(
        "bit_flip_in_committed_frame",
        vec![
            commit(3, false),
            Op::Wal(WalOp::FlipLastFrameBit),
            Op::RequestSnapshot,
        ],
        vec![Expect::Stopped(StopCause::WalCorrupt)],
    );
    s.check = Some(nothing_staged_past_the_corrupt_frame);
    s
}

/// As [`bit_flip_in_committed_frame`], and the bit flips back before the
/// next commit: the frame verifies again and the task recovers.
#[must_use]
pub fn bit_flip_in_committed_frame_healed() -> Scenario {
    let mut s = injected(
        "bit_flip_in_committed_frame_healed",
        vec![
            commit(3, false),
            Op::Wal(WalOp::FlipLastFrameBit),
            Op::Tick,
            Op::Tick,
            Op::Wal(WalOp::FlipLastFrameBit),
        ],
        vec![Expect::Stopped(StopCause::WalCorrupt), Expect::Recovered],
    );
    s.check = Some(quiet_after_stop);
    s
}

/// The stop is quiet; or, where the responses keep trying, from the first
/// degraded sync to the end no file is staged at all: not the corrupt
/// frame's transaction, not one after it, and not a snapshot.
fn nothing_staged_past_the_corrupt_frame(report: &RunReport) -> Result<(), String> {
    if report.seen.iter().any(stopped_by_fault) {
        return quiet_after_stop(report);
    }
    let mut degraded = false;
    for seen in &report.seen {
        match seen {
            Seen::Event(Event::Degraded {
                cause: PersistentCause::WalCorrupt,
                ..
            }) => degraded = true,
            Seen::Event(Event::Synced { files }) if degraded && *files > 0 => {
                return Err("a file was staged past the corrupt frame".to_owned());
            }
            Seen::Snapshot(txid) if degraded => {
                return Err(format!(
                    "a snapshot at {txid} was staged past the corrupt frame"
                ));
            }
            Seen::Recovered(task) if degraded => {
                return Err(format!("{task:?} recovered while the frame is corrupt"));
            }
            Seen::Event(_)
            | Seen::Stop(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    if degraded {
        Ok(())
    } else {
        Err("the sync task was never degraded".to_owned())
    }
}

/// The WAL file is cut short below the committed end while the engine holds
/// the lock.
#[must_use]
pub fn wal_truncated_externally_under_lock() -> Scenario {
    let mut expect = vec![Expect::Stopped(StopCause::WalTruncated), Expect::Oracle];
    let mut s = build(
        "wal_truncated_externally_under_lock",
        vec![
            live(),
            vec![
                commit(3, false),
                commit(3, false),
                Op::Wal(WalOp::TruncateAtFrame(5)),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        std::mem::take(&mut expect),
    );
    s.check = Some(quiet_after_stop);
    s
}

/// The WAL file is unlinked while SQLite keeps writing it.
#[must_use]
pub fn wal_unlinked_mid_run() -> Scenario {
    let mut s = injected(
        "wal_unlinked_mid_run",
        vec![Op::EnvFault(EnvFault::UnlinkWal), commit(2, false)],
        vec![Expect::Stopped(StopCause::WalUnlinked)],
    );
    s.check = Some(quiet_after_stop);
    s
}

/// The engine's sync of the WAL fails once, and the kernel keeps the pages it covered off the
/// disk for good (the simulator's lost writeback): nothing from that generation is staged until
/// a sync succeeds, the sync task is degraded at `wal_sync_failed` meanwhile, and the next try
/// syncs again and goes on, as Go does. Where the responses stop instead, the run ends there.
#[must_use]
pub fn wal_fsync_fails() -> Scenario {
    build(
        "wal_fsync_fails",
        vec![
            live(),
            vec![
                Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
                commit(2, false),
                commit(2, false),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Stopped(StopCause::WalSyncFailed),
            Expect::Recovered,
            Expect::Oracle,
        ],
    )
}

/// [`wal_fsync_fails`], then a power loss: the retried sync went on and staged frames the
/// kernel may have dropped, so the database can come back behind what was staged. How much of
/// the unsynced tail survives is drawn, so the boot may or may not snapshot (`LocalRewound`);
/// either way the oracle restores the replica (`docs/decisions/D173.md`).
#[must_use]
pub fn wal_fsync_fails_then_power_loss() -> Scenario {
    build(
        "wal_fsync_fails_then_power_loss",
        vec![
            live(),
            vec![
                Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
                commit(2, false),
            ],
            ticks(SETTLE),
            vec![commit(2, false)],
            ticks(SETTLE),
            vec![Op::PowerLoss],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Stopped(StopCause::WalSyncFailed), Expect::Oracle],
    )
}

/// The engine's sync of the WAL fails, and the process restarts before its retry. The new
/// process knows nothing of the failure and syncs as any boot does: nothing is remembered across
/// a restart, as in Go.
#[must_use]
pub fn wal_fsync_fails_then_restart() -> Scenario {
    build(
        "wal_fsync_fails_then_restart",
        vec![
            live(),
            vec![
                Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
                commit(2, false),
                Op::Tick,
                Op::Restart,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Stopped(StopCause::WalSyncFailed), Expect::Oracle],
    )
}

/// The database page a stray write flips in [`db_page_flipped_under_sync`]:
/// one the database file alone holds after the scenario's first commit.
const FLIPPED: u32 = 3;

/// A byte of a database page flips between two syncs, outside SQLite, and an
/// operator's snapshot then reads the page from the file. The page checksum
/// table recorded it at the first snapshot, so the read is a mismatch naming
/// the page: a stop under strict responses, before the page is shipped; under
/// the default a degraded sync task that ships the page as the file holds it
/// (the database the application serves, which the oracle then judges by), keeps
/// syncing and pins retention.
#[must_use]
pub fn db_page_flipped_under_sync() -> Scenario {
    let pgno = lsr_ltx::PageNo::new(FLIPPED, PageSize::MIN);
    let flipped = pgno.map_or(StopCause::Bug, |pgno| StopCause::DatabasePageMismatch {
        pgno,
    });
    injected(
        "db_page_flipped_under_sync",
        vec![
            Op::EnvFault(EnvFault::FlipDatabasePage(FLIPPED)),
            Op::RequestSnapshot,
        ],
        vec![Expect::Stopped(flipped)],
    )
}

/// The replica's store turns out to have no conditional writes: the next level-0 upload is
/// refused with nothing published, and under responses that stop the engine stops on it; under
/// keep-trying responses uploads replace from then on, with the upload task degraded (D79).
#[must_use]
pub fn create_only_refused_mid_run() -> Scenario {
    build(
        "create_only_refused_mid_run",
        vec![
            live(),
            vec![
                Op::Remote(RemoteOp::RefuseCreateOnly),
                commit(2, false),
                Op::Tick,
            ],
            ticks(SETTLE),
        ],
        vec![
            Expect::Stopped(StopCause::ConditionalWritesUnsupported),
            Expect::Oracle,
        ],
    )
}

/// SIGTERM while the replica keeps failing: the retries outlast the grace.
#[must_use]
pub fn sigterm_during_upload() -> Scenario {
    let failures = (0..6)
        .map(|ahead| {
            remote_fault(
                StoreOp::Upload,
                ahead,
                FaultShape::Error(transient(StoreOp::Upload)),
            )
        })
        .collect();
    build(
        "sigterm_during_upload",
        vec![
            live(),
            failures,
            vec![commit(2, false), Op::Tick, Op::Shutdown],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Stopped(StopCause::Shutdown), Expect::Oracle],
    )
}

/// The clock reads ten seconds earlier than the last tick: one event, and the
/// engine goes on with the later time.
#[must_use]
pub fn clock_steps_back_10s() -> Scenario {
    let mut s = build(
        "clock_steps_back_10s",
        vec![
            live(),
            vec![commit(2, false), Op::ClockJump(-10_000), Op::Tick],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Event(EventMatch::Clamped), Expect::Oracle],
    );
    s.check = Some(one_clamp_no_fault);
    s
}

/// The clock steps back an hour while an upload waits out its one-second
/// retry: the retry still runs within seconds of wall clock, never an hour.
#[must_use]
pub fn clock_steps_back_an_hour_mid_retry() -> Scenario {
    let transient = StoreError::Transient {
        op: StoreOp::Upload,
        kind: std::io::ErrorKind::TimedOut,
    };
    let mut s = build(
        "clock_steps_back_an_hour_mid_retry",
        vec![
            live(),
            vec![
                remote_fault(StoreOp::Upload, 0, FaultShape::Error(transient)),
                commit(2, false),
                Op::Tick,
                Op::Tick,
                Op::ClockJump(-3_600_000),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Retrying(FaultKind::StoreTransient)),
            Expect::Event(EventMatch::Clamped),
            Expect::Oracle,
        ],
    );
    s.check = Some(uploads_soon_after_the_clamp);
    s
}

/// The upload after the clamp ran within a minute of wall clock of it.
fn uploads_soon_after_the_clamp(report: &RunReport) -> Result<(), String> {
    let mut at = None;
    let mut clamped_at = None;
    for seen in &report.seen {
        match seen {
            Seen::Tick { at: began, .. } => at = Some(*began),
            Seen::Event(Event::Clamped(_)) => clamped_at = at,
            Seen::Event(Event::Uploaded(Did::Put(_))) => {
                if let (Some(clamped), Some(now)) = (clamped_at, at) {
                    let waited = now.saturating_duration_since(clamped);
                    return if waited < Duration::from_secs(60) {
                        Ok(())
                    } else {
                        Err(format!("the upload waited {waited:?} of wall clock"))
                    };
                }
            }
            Seen::Event(_)
            | Seen::Stop(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle => {}
        }
    }
    Err("no upload after the clamp".to_owned())
}

/// The clock jumps an hour forward: timers that pass early run, nothing
/// faults, and every acknowledged commit restores.
#[must_use]
pub fn clock_jumps_forward_an_hour() -> Scenario {
    let mut s = build(
        "clock_jumps_forward_an_hour",
        vec![
            live(),
            vec![commit(2, false), Op::ClockJump(3_600_000), Op::Tick],
            vec![commit(3, false)],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Oracle],
    );
    s.check = Some(no_fault);
    s
}

fn no_fault(report: &RunReport) -> Result<(), String> {
    let faulted = report.seen.iter().any(|seen| {
        matches!(
            seen,
            Seen::Event(Event::Retrying { .. } | Event::Degraded { .. } | Event::Clamped(_))
                | Seen::Stop(_)
        )
    });
    if faulted {
        return Err("a fault, a stop or a clamp followed a forward jump".to_owned());
    }
    Ok(())
}

fn one_clamp_no_fault(report: &RunReport) -> Result<(), String> {
    let mut clamps = 0_u32;
    let mut faulted = false;
    for seen in &report.seen {
        match seen {
            Seen::Event(Event::Clamped(_)) => clamps = clamps.saturating_add(1),
            Seen::Event(Event::Retrying { .. } | Event::Degraded { .. })
            | Seen::Stop(_)
            | Seen::Recovered(_) => faulted = true,
            Seen::Event(_) | Seen::Snapshot(_) | Seen::Oracle | Seen::Tick { .. } => {}
        }
    }
    if clamps != 1 {
        return Err(format!("{clamps} clamp events, want 1"));
    }
    if faulted {
        return Err("a fault or a stop followed the clamp".to_owned());
    }
    Ok(())
}

/// The store acknowledges the next upload to the replica with one byte
/// changed, and the delete that would remove the object fails.
fn corrupt_publish() -> Vec<Op> {
    vec![
        remote_fault(StoreOp::Upload, 0, FaultShape::CorruptPublish),
        remote_fault(
            StoreOp::Delete,
            0,
            FaultShape::Error(transient(StoreOp::Delete)),
        ),
    ]
}

/// A compaction publishes an object whose bytes are wrong and whose size is
/// right; the delete that would remove it fails, and power is lost before a
/// retry. The next process has no memory of the object.
fn corrupt_publish_then_power_loss(name: &'static str, jump: Option<i64>) -> Scenario {
    let mut fault = corrupt_publish();
    fault.extend(jump.map(Op::ClockJump));
    build(
        name,
        vec![
            live(),
            vec![commit(3, false), Op::Tick, Op::Tick],
            fault,
            vec![Op::Tick, Op::Tick, Op::PowerLoss],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![Expect::Oracle],
    )
}

/// What the next boot must do about the object: read it back, delete it
/// because the level below still covers it, and build the level again.
fn repaired(mut scenario: Scenario, level: u8, check: super::Check) -> Scenario {
    let level = Level::new(level).unwrap_or(Level::ZERO);
    scenario.expect = vec![
        retries(FaultKind::UploadChecksumMismatch),
        Expect::Event(EventMatch::Repaired(level)),
        Expect::Oracle,
    ];
    scenario.check = Some(check);
    scenario
}

/// The key the read-back deleted, when it deleted exactly one.
fn repaired_key(report: &RunReport) -> Result<FileKey, String> {
    let mut keys = report.seen.iter().filter_map(|seen| match seen {
        Seen::Event(Event::Maintained {
            event: MaintainedEvent::Repaired(key),
            ..
        }) => Some(*key),
        Seen::Event(_)
        | Seen::Stop(_)
        | Seen::Recovered(_)
        | Seen::Snapshot(_)
        | Seen::Oracle
        | Seen::Tick { .. } => None,
    });
    match (keys.next(), keys.next()) {
        (Some(key), None) => Ok(key),
        (first, second) => Err(format!("repaired {first:?} and {second:?}, want one key")),
    }
}

/// The damaged object was published at `level`, the read-back deleted that
/// key and nothing else, and a later compaction published the level again
/// through the same last transaction.
fn rebuilt_at(report: &RunReport, level: u8) -> Result<(), String> {
    let key = repaired_key(report)?;
    if key.level().get() != level {
        return Err(format!("{key} is not at level {level}"));
    }
    let events = &report.remote_events;
    let through = key.range().max();
    let builds = |event: &RecorderEvent| match event {
        RecorderEvent::Put { key: put, .. } => {
            put.level() == key.level() && put.range().max() >= through
        }
        RecorderEvent::Delete { .. } => false,
    };
    let published = events
        .iter()
        .position(|event| matches!(event, RecorderEvent::Put { key: put, .. } if *put == key))
        .ok_or_else(|| format!("{key} was never published"))?;
    let mut later = events.iter().skip(published.saturating_add(1));
    if !later.any(|event| matches!(event, RecorderEvent::Delete { keys, .. } if *keys == [key])) {
        return Err(format!("{key} was never deleted alone"));
    }
    // `later` now starts after the delete.
    if later.any(builds) {
        Ok(())
    } else {
        Err(format!("level {level} was not built again through {key}"))
    }
}

fn rebuilt_at_one(report: &RunReport) -> Result<(), String> {
    rebuilt_at(report, 1)
}

fn rebuilt_at_two(report: &RunReport) -> Result<(), String> {
    rebuilt_at(report, 2)
}

/// The level 1 compaction's output rots in the window: the first compaction
/// of the run, right after the first upload.
#[must_use]
pub fn corrupt_l1_publish_then_power_loss() -> Scenario {
    let scenario = build(
        "corrupt_l1_publish_then_power_loss",
        vec![
            vec![commit(3, false)],
            ticks(3),
            corrupt_publish(),
            vec![Op::Tick, Op::PowerLoss],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        Vec::new(),
    );
    repaired(scenario, 1, rebuilt_at_one)
}

/// The level 2 compaction's output rots in the window: level 1 is inside its
/// interval after [`live`], so the next upload to the replica is level 2's.
#[must_use]
pub fn corrupt_l2_publish_then_power_loss() -> Scenario {
    let scenario = corrupt_publish_then_power_loss("corrupt_l2_publish_then_power_loss", None);
    repaired(scenario, 2, rebuilt_at_two)
}

/// The level 9 compaction's output rots in the window.
#[must_use]
pub fn corrupt_l9_publish_then_power_loss() -> Scenario {
    snapshot_window("corrupt_l9_publish_then_power_loss")
}

/// The first snapshot rots in the window. Six ticks boot, sync, upload and
/// compact levels 1 to 3, so the next upload to the replica is level 9's.
/// The snapshot job deletes what the store rejected; that delete fails, and
/// power is lost before the retry.
fn snapshot_window(name: &'static str) -> Scenario {
    let mut s = build(
        name,
        vec![
            vec![commit(3, false)],
            ticks(6),
            vec![
                remote_fault(StoreOp::Upload, 0, FaultShape::CorruptPublish),
                remote_fault(
                    StoreOp::Delete,
                    0,
                    FaultShape::Error(transient(StoreOp::Delete)),
                ),
                Op::Tick,
                Op::PowerLoss,
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![retries(FaultKind::UploadChecksumMismatch), Expect::Oracle],
    );
    s.check = Some(damaged_a_snapshot);
    s
}

/// The one object the corrupt publish changed, when it is at `level`.
fn damaged_at(report: &RunReport, level: u8) -> Result<lsr_core::FileKey, String> {
    match report.remote_damaged.as_slice() {
        [key] if key.level().get() == level => Ok(*key),
        other => Err(format!(
            "the corrupt publish changed {other:?}, want one object at level {level}"
        )),
    }
}

fn damaged_a_snapshot(report: &RunReport) -> Result<(), String> {
    damaged_at(report, 9).map(drop)
}

/// The level 0 upload's object rots in the window: after a day's clock jump
/// the passive checkpoint stages one more file, and its upload is the next
/// to the replica. The uploader's delete fails and power is lost. The next
/// boot reads the replica's newest level 0 file back against the staged one
/// (a listing confirms a file only by content), finds the staged copy whole
/// and uploads it over the torn object: the key is written twice, the second
/// time with the bytes the engine staged for it.
#[must_use]
pub fn corrupt_l0_publish_then_power_loss() -> Scenario {
    let mut s =
        corrupt_publish_then_power_loss("corrupt_l0_publish_then_power_loss", Some(93_600_000));
    s.expect = vec![
        retries(FaultKind::UploadChecksumMismatch),
        Expect::Event(EventMatch::Booted(None)),
        Expect::Oracle,
    ];
    s.check = Some(rewritten_from_staging);
    s
}

/// The damaged object is a level 0 file the replica accepted twice: the corrupt publish and
/// the boot's upload of the staged copy.
fn rewritten_from_staging(report: &RunReport) -> Result<(), String> {
    let key = damaged_at(report, 0)?;
    let puts = report
        .remote_events
        .iter()
        .filter(|event| matches!(event, RecorderEvent::Put { key: put, .. } if *put == key))
        .count();
    if puts == 2 {
        Ok(())
    } else {
        Err(format!("{key} was uploaded {puts} times, want 2"))
    }
}

/// The app's automatic checkpoint threshold in [`app_autocheckpoints`], in frames: small,
/// so a few commits cross it, and below the engine's own PASSIVE threshold.
const APP_THRESHOLD: u32 = 4;

/// The app runs SQLite's automatic checkpoint (at [`APP_THRESHOLD`] frames) and the engine is
/// told so. With the read lock above read mark 0 (the driver's first frame), the app's first checkpoint
/// past the threshold copies the frames below the read lock and its next attempts copy nothing; a
/// another process's read stops the engine's own checkpoint short, so the read lock is retaken above read mark 0
/// and the app backfills once more; after the engine's full checkpoints the read lock is on read mark 0
/// and every attempt copies nothing. The engine counts each backfill the app made and none of
/// its own, infers the attempts that copied nothing on both marks, and the replica restores
/// every commit.
#[must_use]
pub fn app_autocheckpoints() -> Scenario {
    let step = |pages| vec![commit(pages, false), Op::Tick];
    let ones = |n| (0..n).flat_map(|_| step(1)).collect::<Vec<_>>();
    let mut s = build(
        "app_autocheckpoints",
        vec![
            vec![Op::AppAutocheckpoint(Some(APP_THRESHOLD))],
            ticks(4),
            step(3),
            step(1),
            step(1),
            step(1),
            vec![Op::OtherProcessReader { begin: true }],
            step(4),
            ticks(2),
            vec![Op::OtherProcessReader { begin: false }],
            ones(6),
            ticks(SETTLE),
            ones(8),
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![
            Expect::Event(EventMatch::Checkpointed(lsr_core::CheckpointMode::Passive)),
            Expect::Oracle,
        ],
    );
    s.check = Some(app_counted);
    s
}

/// Every backfill the app's automatic checkpoints made was counted, nothing else was counted
/// as a checkpoint that ran, and the attempts that copied nothing were inferred.
fn app_counted(report: &RunReport) -> Result<(), String> {
    let (seen, app) = (report.app_checkpoints, report.app_autocheckpoints);
    if app.backfilled < 2 || app.attempts <= app.backfilled {
        return Err(format!("the scenario did not run both halves: {app:?}"));
    }
    if seen.backfilled != app.backfilled || seen.restarted != 0 {
        return Err(format!("counted {seen:?}, the app ran {app:?}"));
    }
    if seen.stalled < 2 {
        return Err(format!(
            "a stall not inferred: {seen:?}, the app ran {app:?}"
        ));
    }
    Ok(())
}

/// The app commits inside a tick, after the sync read the committed end and
/// before the PASSIVE checkpoint's barrier (nine frames make one due), so the
/// checkpoint's seal stages that commit; then `op` fails with `err`. The
/// engine stops before any other commit: the drain must upload the sealed
/// file on its own and name its transaction, and the oracle restore it.
fn sealed_then(name: &'static str, op: SourceOp, err: SourceError) -> Scenario {
    let mut s = build(
        name,
        vec![
            live(),
            vec![
                commit(9, false),
                Op::EnvFault(EnvFault::Source(op, err)),
                Op::CommitInTick { pages: 2 },
                Op::Shutdown,
                Op::Restore,
            ],
        ],
        vec![Expect::Oracle],
    );
    s.check = Some(sealed_then_failed);
    s
}

/// The commit happened inside the tick and the checkpoint after it failed.
fn sealed_then_failed(report: &RunReport) -> Result<(), String> {
    if report.commits_in_tick != 1 {
        return Err(format!(
            "{} commits inside a tick, not one",
            report.commits_in_tick
        ));
    }
    let failed = report
        .seen
        .iter()
        .any(|seen| matches!(seen, Seen::Event(Event::Retrying { .. })));
    if failed {
        Ok(())
    } else {
        Err("no checkpoint step failed".to_owned())
    }
}

/// [`sealed_then`]: the checkpoint is busy.
#[must_use]
pub fn sealed_then_checkpoint_busy() -> Scenario {
    sealed_then(
        "sealed_then_checkpoint_busy",
        SourceOp::Checkpoint,
        SourceError::Busy,
    )
}

/// [`sealed_then`]: the checkpoint fails with an I/O error.
#[must_use]
pub fn sealed_then_checkpoint_io() -> Scenario {
    sealed_then(
        "sealed_then_checkpoint_io",
        SourceOp::Checkpoint,
        SourceError::Io(std::io::ErrorKind::Other),
    )
}

/// [`sealed_then`]: the sequence bump is busy.
#[must_use]
pub fn sealed_then_bump_busy() -> Scenario {
    sealed_then(
        "sealed_then_bump_busy",
        SourceOp::BumpSeq,
        SourceError::Busy,
    )
}

/// [`sealed_then`]: the sequence bump fails with an I/O error.
#[must_use]
pub fn sealed_then_bump_io() -> Scenario {
    sealed_then(
        "sealed_then_bump_io",
        SourceOp::BumpSeq,
        SourceError::Io(std::io::ErrorKind::Other),
    )
}

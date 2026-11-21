//! One scenario per [`Fault`], and the scenarios of faults that are not
//! faults: the clock stepping back.
//!
//! A persistent fault's scenarios hold under every set of responses: each
//! names its cause with [`Expect::Stopped`], which is a halt where the
//! responses stop and a degraded lane where they keep trying, and the ones
//! whose fault clears also expect [`Expect::Recovered`].

use std::time::Duration;

use lsr_core::{
    Event, Fault, FileKey, HaltCause, Level, MaintainedEvent, PersistentCause, SnapshotCause,
    SourceError, SourceOp, StoreError, StoreOp,
};
use lsr_ltx::PageSize;

use super::{
    CauseKind, EventMatch, Expect, SEED, SETTLE, Scenario, build, commit, ticks,
    vacuum_page_size_change, vacuum_page_size_change_and_back,
};
use crate::generate::{EnvFault, MetaOp, Op, RemoteOp, Target, WalOp};
use crate::run::{RunReport, Seen};
use crate::seed::Seed;
use crate::sqlite::StrangerMode;
use crate::store::{FaultShape, TapEvent};

/// A [`Fault`] without its payload: one variant per fault, so a new fault
/// does not compile until [`fault_scenarios`] names its scenarios.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    /// [`Fault::StoreTransient`].
    StoreTransient,
    /// [`Fault::StoreThrottled`].
    StoreThrottled,
    /// [`Fault::StoreAuth`].
    StoreAuth,
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
    /// [`Fault::ShutdownDeadline`].
    ShutdownDeadline,
    /// [`Fault::Bug`].
    Bug,
}

impl FaultKind {
    /// Every fault, in the order of [`Fault`].
    pub const ALL: [Self; 26] = [
        Self::StoreTransient,
        Self::StoreThrottled,
        Self::StoreAuth,
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
        Self::WalMoved,
        Self::WalIndexInconsistent,
        Self::WalChangedUnderRead,
        Self::NothingToSnapshot,
        Self::ReadTxLost,
        Self::PageSizeChanged,
        Self::WalCorruptBelowCommitted,
        Self::WalTruncatedBelowCommitted,
        Self::WalUnlinked,
        Self::ShutdownDeadline,
        Self::Bug,
    ];
}

impl From<&Fault> for FaultKind {
    fn from(fault: &Fault) -> Self {
        match fault {
            Fault::StoreTransient => Self::StoreTransient,
            Fault::StoreThrottled { .. } => Self::StoreThrottled,
            Fault::StoreAuth => Self::StoreAuth,
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
            Fault::WalMoved => Self::WalMoved,
            Fault::WalIndexInconsistent => Self::WalIndexInconsistent,
            Fault::WalChangedUnderRead => Self::WalChangedUnderRead,
            Fault::NothingToSnapshot => Self::NothingToSnapshot,
            Fault::ReadTxLost => Self::ReadTxLost,
            Fault::PageSizeChanged { .. } => Self::PageSizeChanged,
            Fault::WalCorruptBelowCommitted => Self::WalCorruptBelowCommitted,
            Fault::WalTruncatedBelowCommitted => Self::WalTruncatedBelowCommitted,
            Fault::WalUnlinked => Self::WalUnlinked,
            Fault::ShutdownDeadline => Self::ShutdownDeadline,
            Fault::Bug => Self::Bug,
        }
    }
}

/// The persistent cause a fault's policy names, if it has one.
#[must_use]
pub fn persistent_of(kind: FaultKind) -> Option<PersistentCause> {
    halt_of(kind).and_then(PersistentCause::of)
}

/// The stop cause a fault's policy names, if its policy can stop the engine.
#[must_use]
pub fn halt_of(kind: FaultKind) -> Option<HaltCause> {
    let page = PageSize::MIN;
    match kind {
        FaultKind::StoreAuth => Some(HaltCause::Credentials),
        FaultKind::StoreRefused => Some(HaltCause::StoreRefused),
        FaultKind::RemoteCorrupt => Some(HaltCause::RemoteCorrupt),
        FaultKind::RemoteAheadOfLocal => Some(HaltCause::ForeignWriter),
        FaultKind::RemoteRegressed => Some(HaltCause::RemoteRegressed),
        FaultKind::PageSizeChanged => Some(HaltCause::PageSizeChanged {
            was: page,
            now: page,
        }),
        FaultKind::WalCorruptBelowCommitted => Some(HaltCause::WalCorrupt),
        FaultKind::WalTruncatedBelowCommitted => Some(HaltCause::WalTruncated),
        FaultKind::WalUnlinked => Some(HaltCause::WalUnlinked),
        FaultKind::ShutdownDeadline => Some(HaltCause::Shutdown),
        FaultKind::Bug => Some(HaltCause::Bug),
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
        .any(|want| match (want, halt_of(kind)) {
            (Expect::Event(EventMatch::Retrying(got)), _) => *got == kind,
            // The lost file's policy is a snapshot at the first transaction the
            // replica lacks, announced as this resnapshot.
            (Expect::Event(EventMatch::Resnapshot(CauseKind::LocalTipUnreadable)), _) => {
                kind == FaultKind::StagedLost
            }
            (Expect::Stopped(got), Some(halt)) => {
                std::mem::discriminant(got) == std::mem::discriminant(&halt)
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
        FaultKind::StoreAuth => &[creds_revoked_mid_run],
        FaultKind::RemoteObjectMissing => &[remote_file_deleted_externally],
        FaultKind::UploadAmbiguous => &[size_mismatch_on_commit],
        FaultKind::UploadChecksumMismatch => &[etag_mismatch_on_put],
        FaultKind::StoreRefused => &[store_refuses_upload],
        FaultKind::RemoteCorrupt => &[remote_bitrot_on_list_read],
        FaultKind::RemoteAheadOfLocal => &[second_writer_mid_run],
        FaultKind::RemoteRegressed => &[bucket_rolled_back],
        FaultKind::StagedLost => &[staged_byte_flip_before_upload, meta_dir_deleted_mid_run],
        FaultKind::LocalDiskFull => &[disk_full_on_staging],
        FaultKind::LocalIo => &[eio_on_wal_read],
        FaultKind::LocalFsyncFailed => &[fsync_fails_once],
        FaultKind::SqliteBusy => &[busy_on_barrier, shm_header_mid_update],
        FaultKind::WalMoved => &[restart_between_steps],
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
        FaultKind::ShutdownDeadline => &[sigterm_during_upload],
        FaultKind::Bug => &[],
    }
}

/// Why a fault has no scenario of its own. [`FaultKind::Bug`] is the only one:
/// the oracle's bug-fault violation fails every run that meets it, so every
/// scenario and every random history asserts it never fires.
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
    &[clock_steps_back_10s]
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

fn remote_fault(op: StoreOp, ahead: u8, shape: FaultShape) -> Op {
    Op::StoreFault {
        target: Target::Remote,
        op,
        ahead,
        shape,
    }
}

fn staging_fault(op: StoreOp, ahead: u8, shape: FaultShape) -> Op {
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
            | Seen::Halt(_)
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

/// A halt for the scenario's fault, not the end of a drain whose deadline a
/// degraded lane outlasted.
fn stopped(seen: &Seen) -> bool {
    matches!(seen, Seen::Halt(cause) if *cause != HaltCause::Shutdown)
}

/// No later tick calls the replica: the halt is final until a restart. Where
/// the responses keep trying there is no halt to be quiet after: a lane was
/// degraded instead, which the expectations check.
fn quiet_after_halt(report: &RunReport) -> Result<(), String> {
    let degraded = report
        .seen
        .iter()
        .any(|seen| matches!(seen, Seen::Event(Event::Degraded { .. })));
    if degraded && !report.seen.iter().any(stopped) {
        return Ok(());
    }
    let mut halted = false;
    for (at, seen) in report.seen.iter().enumerate() {
        match seen {
            Seen::Halt(_) if stopped(seen) => halted = true,
            Seen::Event(Event::Booted { .. }) => halted = false,
            // A restore's reboot lists the replica in its first tick, and the
            // oracle has just judged: that tick is not the halted engine's.
            Seen::Tick { remote_calls, .. }
                if halted
                    && *remote_calls != 0
                    && !matches!(
                        at.checked_sub(1).and_then(|p| report.seen.get(p)),
                        Some(Seen::Oracle)
                    ) =>
            {
                return Err(format!(
                    "{remote_calls} calls to the replica after the halt"
                ));
            }
            Seen::Event(_)
            | Seen::Halt(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    if report.seen.iter().any(stopped) {
        Ok(())
    } else {
        Err("the engine never halted".to_owned())
    }
}

/// The credentials stop working in the middle of the run: a halt, and no
/// further calls.
#[must_use]
pub fn creds_revoked_mid_run() -> Scenario {
    let mut s = injected(
        "creds_revoked_mid_run",
        vec![remote_fault(
            StoreOp::Upload,
            0,
            FaultShape::Error(StoreError::Auth {
                op: StoreOp::Upload,
            }),
        )],
        vec![Expect::Stopped(HaltCause::Credentials), Expect::Recovered],
    );
    s.check = Some(quiet_after_halt);
    s
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
        vec![Expect::Stopped(HaltCause::StoreRefused), Expect::Recovered],
    );
    s.check = Some(quiet_after_halt);
    s
}

fn some_key() -> lsr_core::FileKey {
    lsr_core::FileKey::new(
        lsr_core::Level::ZERO,
        lsr_ltx::TxidRange::single(lsr_ltx::Txid::MIN),
    )
}

/// A file the listing named is gone when boot reads it back: another process
/// deleted it. A compaction that loses an input replans instead, so boot is
/// where this fault reaches the engine.
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
            TapEvent::Put { key, .. } if deleted == Some(*key) => {
                written_again = true;
                break;
            }
            TapEvent::Delete { keys } if keys.len() == 1 => deleted = keys.first().copied(),
            TapEvent::Put { .. } | TapEvent::Delete { .. } => {}
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
            | Seen::Halt(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    Ok(())
}

/// The staging directory is gone and the replica is read back at boot: one byte
/// of the newest object has rotted, there is no staged copy to compare with, so
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
            Expect::Stopped(HaltCause::RemoteCorrupt),
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
                Op::Remote(RemoteOp::PutForeign),
                Op::Remote(RemoteOp::PutForeign),
            ],
            ticks(SETTLE),
            vec![Op::Remote(RemoteOp::RemoveForeign), Op::Restore],
        ],
        vec![Expect::Stopped(HaltCause::ForeignWriter), Expect::Oracle],
    );
    s.check = Some(quiet_after_halt);
    s
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
            Expect::Stopped(HaltCause::RemoteRegressed),
            Expect::Recovered,
            Expect::Oracle,
        ],
    );
    s.check = Some(quiet_after_halt);
    s
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
            Expect::Event(EventMatch::Resnapshot(CauseKind::LocalTipUnreadable)),
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
            Expect::Event(EventMatch::Resnapshot(CauseKind::LocalTipUnreadable)),
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
            Seen::Halt(cause) => return Err(format!("halted: {cause:?}")),
            Seen::Event(Event::Degraded { cause, .. }) => {
                return Err(format!("degraded: {cause:?}"));
            }
            Seen::Event(Event::Resnapshot(SnapshotCause::LocalTipUnreadable)) => lost = true,
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

/// A writer is mid-update in the wal-index header.
#[must_use]
pub fn shm_header_mid_update() -> Scenario {
    injected(
        "shm_header_mid_update",
        vec![commit(2, false), Op::EnvFault(EnvFault::ShmMidUpdate)],
        vec![retries(FaultKind::SqliteBusy)],
    )
}

/// The WAL file restarts between the engine's reads.
#[must_use]
pub fn restart_between_steps() -> Scenario {
    build(
        "restart_between_steps",
        vec![
            live(),
            vec![
                Op::OurReader { begin: false },
                Op::StrangerCheckpoint(StrangerMode::Truncate),
            ],
            ticks(SETTLE),
            vec![Op::Restore],
        ],
        vec![retries(FaultKind::WalMoved), Expect::Oracle],
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
/// stays flipped. Where the responses keep trying, the sync lane stays
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
        vec![Expect::Stopped(HaltCause::WalCorrupt)],
    );
    s.check = Some(nothing_staged_past_the_corrupt_frame);
    s
}

/// As [`bit_flip_in_committed_frame`], and the bit flips back before the
/// next commit: the frame verifies again and the lane recovers.
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
        vec![Expect::Stopped(HaltCause::WalCorrupt), Expect::Recovered],
    );
    s.check = Some(quiet_after_halt);
    s
}

/// The halt is quiet; or, where the responses keep trying, from the first
/// degraded sync to the end no file is staged at all: not the corrupt
/// frame's transaction, not one after it, and not a snapshot.
fn nothing_staged_past_the_corrupt_frame(report: &RunReport) -> Result<(), String> {
    if report.seen.iter().any(stopped) {
        return quiet_after_halt(report);
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
            Seen::Recovered(lane) if degraded => {
                return Err(format!("{lane:?} recovered while the frame is corrupt"));
            }
            Seen::Event(_)
            | Seen::Halt(_)
            | Seen::Recovered(_)
            | Seen::Snapshot(_)
            | Seen::Oracle
            | Seen::Tick { .. } => {}
        }
    }
    if degraded {
        Ok(())
    } else {
        Err("the sync lane was never degraded".to_owned())
    }
}

/// The WAL file is cut short below the committed end while the engine holds
/// the lock.
#[must_use]
pub fn wal_truncated_externally_under_lock() -> Scenario {
    let mut expect = vec![Expect::Stopped(HaltCause::WalTruncated), Expect::Oracle];
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
    s.check = Some(quiet_after_halt);
    s
}

/// The WAL file is unlinked while SQLite keeps writing it.
#[must_use]
pub fn wal_unlinked_mid_run() -> Scenario {
    let mut s = injected(
        "wal_unlinked_mid_run",
        vec![Op::EnvFault(EnvFault::UnlinkWal), commit(2, false)],
        vec![Expect::Stopped(HaltCause::WalUnlinked)],
    );
    s.check = Some(quiet_after_halt);
    s
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
        vec![Expect::Stopped(HaltCause::Shutdown), Expect::Oracle],
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

fn one_clamp_no_fault(report: &RunReport) -> Result<(), String> {
    let mut clamps = 0_u32;
    let mut faulted = false;
    for seen in &report.seen {
        match seen {
            Seen::Event(Event::Clamped(_)) => clamps = clamps.saturating_add(1),
            Seen::Event(Event::Retrying { .. } | Event::Degraded { .. })
            | Seen::Halt(_)
            | Seen::Recovered(_) => faulted = true,
            Seen::Event(_) | Seen::Snapshot(_) | Seen::Oracle | Seen::Tick { .. } => {}
        }
    }
    if clamps != 1 {
        return Err(format!("{clamps} clamp events, want 1"));
    }
    if faulted {
        return Err("a fault or a halt followed the clamp".to_owned());
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
        | Seen::Halt(_)
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
    let builds = |event: &TapEvent| match event {
        TapEvent::Put { key: put, .. } => {
            put.level() == key.level() && put.range().max() >= through
        }
        TapEvent::Delete { .. } => false,
    };
    let published = events
        .iter()
        .position(|event| matches!(event, TapEvent::Put { key: put, .. } if *put == key))
        .ok_or_else(|| format!("{key} was never published"))?;
    let mut later = events.iter().skip(published.saturating_add(1));
    if !later.any(|event| *event == TapEvent::Delete { keys: vec![key] }) {
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
/// (R1) and uploads it again over the same key.
#[must_use]
pub fn corrupt_l0_publish_then_power_loss() -> Scenario {
    let mut s =
        corrupt_publish_then_power_loss("corrupt_l0_publish_then_power_loss", Some(93_600_000));
    s.expect = vec![retries(FaultKind::UploadChecksumMismatch), Expect::Oracle];
    s.check = Some(uploaded_again);
    s
}

/// The damaged object is a level 0 file and the replica accepted that key
/// twice: the damaged upload, and the one after the read-back.
fn uploaded_again(report: &RunReport) -> Result<(), String> {
    let key = damaged_at(report, 0)?;
    let puts = report
        .remote_events
        .iter()
        .filter(|event| matches!(event, TapEvent::Put { key: put, .. } if *put == key))
        .count();
    if puts == 2 {
        Ok(())
    } else {
        Err(format!("{key} was uploaded {puts} times, want 2"))
    }
}

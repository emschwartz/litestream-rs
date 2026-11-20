use std::io::ErrorKind;
use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{PageNo, PageSize};
use lsr_wal::{FrameFault, PageMapError, ReadPageError, ResumeError, Salts, WalError, WalOffset};
use proptest::prelude::*;

use super::*;
use crate::layout::{FileKey, Level};
use crate::store::StoreOp;
use lsr_ltx::{Txid, TxidRange};

fn ps() -> PageSize {
    PageSize::new(4096).unwrap()
}

fn page() -> PageNo {
    PageNo::new(2, ps()).unwrap()
}

fn at() -> WalOffset {
    WalOffset::start(ps())
}

fn salts() -> Salts {
    Salts { salt1: 1, salt2: 2 }
}

fn key() -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(Txid::new(1).unwrap()))
}

fn halt(cause: HaltCause) -> Policy {
    Policy::Halt(cause)
}

fn doubling() -> Policy {
    Policy::Retry(Backoff::Doubling)
}

macro_rules! policy_is {
    ($name:ident, $fault:expr, $policy:expr) => {
        #[test]
        fn $name() {
            assert_eq!(policy(&$fault), $policy);
        }
    };
}

policy_is!(
    store_transient_retries_doubling,
    Fault::StoreTransient,
    doubling()
);
policy_is!(
    store_throttled_with_a_wait_retries_after_it,
    Fault::StoreThrottled {
        retry_after: Some(Duration::from_secs(7))
    },
    Policy::Retry(Backoff::After(Duration::from_secs(7)))
);
policy_is!(
    store_throttled_without_a_wait_retries_doubling,
    Fault::StoreThrottled { retry_after: None },
    doubling()
);
policy_is!(
    store_auth_halts_on_credentials,
    Fault::StoreAuth,
    halt(HaltCause::Credentials)
);
policy_is!(
    remote_object_missing_retries,
    Fault::RemoteObjectMissing,
    doubling()
);
policy_is!(upload_ambiguous_retries, Fault::UploadAmbiguous, doubling());
policy_is!(
    upload_checksum_mismatch_retries,
    Fault::UploadChecksumMismatch,
    doubling()
);
policy_is!(
    store_refused_halts,
    Fault::StoreRefused,
    halt(HaltCause::StoreRefused)
);
policy_is!(
    remote_corrupt_halts,
    Fault::RemoteCorrupt,
    halt(HaltCause::RemoteCorrupt)
);
policy_is!(
    remote_ahead_of_local_halts_as_foreign_writer,
    Fault::RemoteAheadOfLocal,
    halt(HaltCause::ForeignWriter)
);
policy_is!(
    remote_regressed_halts,
    Fault::RemoteRegressed,
    halt(HaltCause::RemoteRegressed)
);
policy_is!(staged_lost_retries, Fault::StagedLost, doubling());
policy_is!(local_disk_full_retries, Fault::LocalDiskFull, doubling());
policy_is!(local_io_retries, Fault::LocalIo, doubling());
policy_is!(
    local_fsync_failed_retries,
    Fault::LocalFsyncFailed,
    doubling()
);
policy_is!(sqlite_busy_retries, Fault::SqliteBusy, doubling());
policy_is!(wal_moved_retries, Fault::WalMoved, doubling());
policy_is!(
    wal_index_inconsistent_retries,
    Fault::WalIndexInconsistent,
    doubling()
);
policy_is!(
    wal_changed_under_read_retries,
    Fault::WalChangedUnderRead,
    doubling()
);
policy_is!(
    nothing_to_snapshot_retries,
    Fault::NothingToSnapshot,
    doubling()
);
policy_is!(read_tx_lost_retries, Fault::ReadTxLost, doubling());
policy_is!(
    page_size_changed_halts_naming_both_sizes,
    Fault::PageSizeChanged {
        was: PageSize::MIN,
        now: ps()
    },
    halt(HaltCause::PageSizeChanged {
        was: PageSize::MIN,
        now: ps()
    })
);
policy_is!(
    wal_corrupt_below_committed_halts,
    Fault::WalCorruptBelowCommitted,
    halt(HaltCause::WalCorrupt)
);
policy_is!(
    wal_truncated_below_committed_halts,
    Fault::WalTruncatedBelowCommitted,
    halt(HaltCause::WalTruncated)
);
policy_is!(
    wal_unlinked_halts,
    Fault::WalUnlinked,
    halt(HaltCause::WalUnlinked)
);
policy_is!(
    shutdown_deadline_halts,
    Fault::ShutdownDeadline,
    halt(HaltCause::Shutdown)
);
policy_is!(bug_halts, Fault::Bug, halt(HaltCause::Bug));

/// Ties every variant to a test above: a new variant breaks this match first.
#[test]
fn every_fault_has_a_named_policy_test() {
    let tested = |fault: &Fault| match fault {
        Fault::StoreTransient
        | Fault::StoreThrottled { .. }
        | Fault::StoreAuth
        | Fault::RemoteObjectMissing
        | Fault::UploadAmbiguous
        | Fault::UploadChecksumMismatch
        | Fault::StoreRefused
        | Fault::RemoteCorrupt
        | Fault::RemoteAheadOfLocal
        | Fault::RemoteRegressed
        | Fault::StagedLost
        | Fault::LocalDiskFull
        | Fault::LocalIo
        | Fault::LocalFsyncFailed
        | Fault::SqliteBusy
        | Fault::WalMoved
        | Fault::WalIndexInconsistent
        | Fault::WalChangedUnderRead
        | Fault::NothingToSnapshot
        | Fault::ReadTxLost
        | Fault::PageSizeChanged { .. }
        | Fault::WalCorruptBelowCommitted
        | Fault::WalTruncatedBelowCommitted
        | Fault::WalUnlinked
        | Fault::ShutdownDeadline
        | Fault::Bug => true,
    };
    assert!(tested(&Fault::Bug));
}

fn store_fault(err: &StoreError) -> Fault {
    Fault::from_remote(err)
}

#[test]
fn from_remote_transient() {
    let err = StoreError::Transient {
        op: StoreOp::Read,
        kind: ErrorKind::TimedOut,
    };
    assert_eq!(store_fault(&err), Fault::StoreTransient);
}

#[test]
fn from_remote_throttled_keeps_a_named_wait() {
    let err = StoreError::Throttled {
        op: StoreOp::List,
        retry_after: Some(Duration::from_secs(3)),
    };
    assert_eq!(
        store_fault(&err),
        Fault::StoreThrottled {
            retry_after: Some(Duration::from_secs(3))
        }
    );
}

#[test]
fn from_remote_throttled_without_or_with_a_zero_wait_names_none() {
    for retry_after in [None, Some(Duration::ZERO)] {
        let err = StoreError::Throttled {
            op: StoreOp::List,
            retry_after,
        };
        assert_eq!(
            store_fault(&err),
            Fault::StoreThrottled { retry_after: None }
        );
    }
}

#[test]
fn from_remote_auth() {
    assert_eq!(
        store_fault(&StoreError::Auth {
            op: StoreOp::Upload
        }),
        Fault::StoreAuth
    );
}

#[test]
fn from_remote_not_found_is_missing() {
    let err = StoreError::NotFound { key: key() };
    assert_eq!(store_fault(&err), Fault::RemoteObjectMissing);
}

#[test]
fn from_staging_names_the_local_fault_for_every_store_error() {
    let corrupt = |cause| StoreError::Corrupt {
        key: Some(key()),
        cause,
    };
    let mismatch = |removed| CorruptCause::CommitSizeMismatch {
        wrote: 10,
        stored: 9,
        removed,
    };
    let cases = [
        (
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: ErrorKind::TimedOut,
            },
            Fault::LocalIo,
        ),
        (
            StoreError::Throttled {
                op: StoreOp::List,
                retry_after: Some(Duration::from_secs(3)),
            },
            Fault::LocalIo,
        ),
        (StoreError::Auth { op: StoreOp::Read }, Fault::LocalIo),
        (StoreError::NotFound { key: key() }, Fault::StagedLost),
        (
            StoreError::ScratchExhausted { key: key() },
            Fault::LocalDiskFull,
        ),
        (corrupt(mismatch(true)), Fault::LocalFsyncFailed),
        (corrupt(mismatch(false)), Fault::LocalFsyncFailed),
        (corrupt(CorruptCause::NotARegularFile), Fault::StagedLost),
        (
            corrupt(CorruptCause::TimestampOutOfRange),
            Fault::StagedLost,
        ),
    ];
    for (err, want) in cases {
        assert_eq!(Fault::from_staging(&err), want, "{err}");
    }
}

#[test]
fn no_staging_error_is_blamed_on_the_replica() {
    let remote_only = [
        Fault::StoreTransient,
        Fault::StoreAuth,
        Fault::RemoteObjectMissing,
        Fault::UploadAmbiguous,
        Fault::UploadChecksumMismatch,
        Fault::StoreRefused,
        Fault::RemoteCorrupt,
    ];
    let errs = [
        StoreError::Auth { op: StoreOp::Read },
        StoreError::Refused {
            op: StoreOp::Read,
            status: 501,
        },
        StoreError::NotFound { key: key() },
        StoreError::Corrupt {
            key: None,
            cause: CorruptCause::NotARegularFile,
        },
        StoreError::Corrupt {
            key: None,
            cause: CorruptCause::ChecksumMismatch { part: None },
        },
    ];
    for err in errs {
        assert!(!remote_only.contains(&Fault::from_staging(&err)), "{err}");
    }
}

#[test]
fn from_remote_scratch_exhausted_is_local_disk_full() {
    assert_eq!(
        store_fault(&StoreError::ScratchExhausted { key: key() }),
        Fault::LocalDiskFull
    );
}

#[test]
fn from_remote_commit_size_mismatch_is_upload_ambiguous() {
    for removed in [true, false] {
        let err = StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 10,
                stored: 9,
                removed,
            },
        };
        assert_eq!(store_fault(&err), Fault::UploadAmbiguous);
    }
}

#[test]
fn from_remote_checksum_mismatch_and_refusal_have_their_own_faults() {
    let mismatch = StoreError::Corrupt {
        key: Some(key()),
        cause: CorruptCause::ChecksumMismatch { part: None },
    };
    assert_eq!(store_fault(&mismatch), Fault::UploadChecksumMismatch);
    let refused = StoreError::Refused {
        op: StoreOp::Upload,
        status: 501,
    };
    assert_eq!(store_fault(&refused), Fault::StoreRefused);
}

#[test]
fn from_staging_refused_is_local_io_and_checksum_is_fsync() {
    let refused = StoreError::Refused {
        op: StoreOp::Upload,
        status: 501,
    };
    assert_eq!(Fault::from_staging(&refused), Fault::LocalIo);
    let mismatch = StoreError::Corrupt {
        key: None,
        cause: CorruptCause::ChecksumMismatch { part: None },
    };
    assert_eq!(Fault::from_staging(&mismatch), Fault::LocalFsyncFailed);
    let stamp = StoreError::Corrupt {
        key: None,
        cause: CorruptCause::BadStamp,
    };
    assert_eq!(Fault::from_staging(&stamp), Fault::StagedLost);
}

#[test]
fn from_remote_any_other_corrupt_is_remote_corrupt() {
    for cause in [
        CorruptCause::NotARegularFile,
        CorruptCause::TimestampOutOfRange,
        CorruptCause::BadStamp,
    ] {
        let err = StoreError::Corrupt { key: None, cause };
        assert_eq!(store_fault(&err), Fault::RemoteCorrupt);
    }
}

fn io() -> std::io::Error {
    std::io::Error::from(ErrorKind::UnexpectedEof)
}

#[test]
fn from_wal_maps_every_variant() {
    let cases = [
        (
            WalError::CorruptBelowCommitted {
                offset: at(),
                fault: FrameFault::ChecksumMismatch,
            },
            Fault::WalCorruptBelowCommitted,
        ),
        (
            WalError::TruncatedBelowCommitted { offset: at() },
            Fault::WalTruncatedBelowCommitted,
        ),
        (
            WalError::Restarted {
                cursor: salts(),
                header: salts(),
            },
            Fault::WalMoved,
        ),
        (
            WalError::CursorPastEnd {
                cursor: at(),
                end: at(),
            },
            Fault::WalMoved,
        ),
        (
            WalError::PageSizeMismatch {
                expected: ps(),
                found: PageSize::MIN,
            },
            Fault::PageSizeChanged {
                was: PageSize::MIN,
                now: ps(),
            },
        ),
        (
            WalError::EndNotAtCommit { end: at() },
            Fault::WalIndexInconsistent,
        ),
        (WalError::TooManyFrames, Fault::LocalIo),
        (WalError::Io(io()), Fault::LocalIo),
    ];
    for (err, fault) in cases {
        let shown = format!("{err:?}");
        assert_eq!(Fault::from_wal(err), fault, "{shown}");
    }
}

#[test]
fn from_resume_maps_every_variant() {
    let cases = [
        (ResumeError::Io(io()), Fault::LocalIo),
        (
            ResumeError::PageSizeMismatch {
                expected: ps(),
                found: PageSize::MIN,
            },
            Fault::PageSizeChanged {
                was: PageSize::MIN,
                now: ps(),
            },
        ),
        (ResumeError::Truncated { at: at() }, Fault::WalMoved),
        (
            ResumeError::Restarted {
                expected: salts(),
                found: salts(),
            },
            Fault::WalMoved,
        ),
        (
            ResumeError::PrevFrame {
                at: at(),
                fault: FrameFault::ChecksumMismatch,
            },
            Fault::Bug,
        ),
        (ResumeError::NotAtCommit { at: at() }, Fault::Bug),
    ];
    for (err, fault) in cases {
        let shown = format!("{err:?}");
        assert_eq!(Fault::from_resume(err), fault, "{shown}");
    }
}

#[test]
fn from_read_page_maps_every_variant() {
    let cases = [
        (ReadPageError::Io(io()), Fault::LocalIo),
        (
            ReadPageError::BufferSize {
                expected: 4096,
                found: 1,
            },
            Fault::Bug,
        ),
        (
            ReadPageError::PageSizeMismatch {
                expected: ps(),
                found: PageSize::MIN,
            },
            Fault::Bug,
        ),
        (
            ReadPageError::Truncated {
                pgno: page(),
                offset: at(),
            },
            Fault::WalTruncatedBelowCommitted,
        ),
        (
            ReadPageError::ChangedSinceScan {
                pgno: page(),
                offset: at(),
            },
            Fault::WalChangedUnderRead,
        ),
    ];
    for (err, fault) in cases {
        let shown = format!("{err:?}");
        assert_eq!(Fault::from_read_page(err), fault, "{shown}");
    }
}

#[test]
fn from_page_map_is_always_a_bug() {
    let cases = [
        PageMapError::PageSizeMismatch {
            expected: ps(),
            found: PageSize::MIN,
        },
        PageMapError::NotAfterEnd {
            end: at(),
            found: at(),
        },
        PageMapError::Gap {
            expected: at(),
            found: at(),
        },
        PageMapError::Generation {
            expected: salts(),
            found: salts(),
        },
    ];
    for err in cases {
        assert_eq!(Fault::from_page_map(err), Fault::Bug, "{err:?}");
    }
}

#[test]
fn from_source_maps_every_variant() {
    let cases = [
        (SourceError::Io(ErrorKind::BrokenPipe), Fault::LocalIo),
        (SourceError::DiskFull, Fault::LocalDiskFull),
        (SourceError::FsyncFailed, Fault::LocalFsyncFailed),
        (SourceError::Busy, Fault::SqliteBusy),
        (SourceError::Short { pgno: page() }, Fault::LocalIo),
        (SourceError::Unlinked, Fault::WalUnlinked),
        (SourceError::ReadTxLost, Fault::ReadTxLost),
    ];
    for (err, fault) in cases {
        assert_eq!(Fault::from_source(err), fault, "{err:?}");
    }
}

#[test]
fn backoff_wait_is_the_later_of_doubling_and_the_named_wait() {
    let interval = Duration::from_secs(1);
    let third = NonZeroU32::new(3).unwrap();
    let wait = |backoff| backoff_wait(backoff, third, interval);
    assert_eq!(wait(Backoff::Doubling), Duration::from_secs(4));
    assert_eq!(
        wait(Backoff::After(Duration::from_secs(1))),
        Duration::from_secs(4)
    );
    assert_eq!(
        wait(Backoff::After(Duration::from_secs(600))),
        Duration::from_secs(600)
    );
}

#[test]
fn retry_wait_doubles_from_the_interval() {
    let interval = Duration::from_secs(1);
    let wait = |n: u32| retry_wait(NonZeroU32::new(n).unwrap(), interval);
    assert_eq!(wait(1), Duration::from_secs(1));
    assert_eq!(wait(2), Duration::from_secs(2));
    assert_eq!(wait(4), Duration::from_secs(8));
    assert_eq!(wait(10), MAX_BACKOFF);
    assert_eq!(wait(u32::MAX), MAX_BACKOFF);
}

#[test]
fn retry_wait_never_exceeds_the_cap_even_for_a_huge_interval() {
    let wait = retry_wait(NonZeroU32::MIN, Duration::MAX);
    assert_eq!(wait, MAX_BACKOFF);
}

proptest! {
    #[test]
    fn retry_wait_is_non_decreasing_and_capped(
        interval_ms in 0u64..=600_000,
        attempt in 1u32..=200,
    ) {
        let interval = Duration::from_millis(interval_ms);
        let now = retry_wait(NonZeroU32::new(attempt).unwrap(), interval);
        let next = retry_wait(NonZeroU32::new(attempt.saturating_add(1)).unwrap(), interval);
        prop_assert!(now <= next);
        prop_assert!(next <= MAX_BACKOFF);
    }
}

#[test]
fn fault_rs_has_no_wildcard_arm() {
    let source = include_str!("../fault.rs");
    let needle = ["_", " =", ">"].concat();
    let hits: Vec<_> = source
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains(&needle))
        .map(|(n, _)| n + 1)
        .collect();
    assert!(hits.is_empty(), "wildcard arms at lines {hits:?}");
}

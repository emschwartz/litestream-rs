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

fn persistent(cause: PersistentCause) -> Policy {
    Policy::Persistent(cause)
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
    store_auth_is_persistent_on_credentials,
    Fault::StoreAuth,
    persistent(PersistentCause::Credentials)
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
    store_refused_is_persistent,
    Fault::StoreRefused,
    persistent(PersistentCause::StoreRefused)
);
policy_is!(
    remote_corrupt_is_persistent,
    Fault::RemoteCorrupt,
    persistent(PersistentCause::RemoteCorrupt)
);
policy_is!(
    remote_ahead_of_local_is_another_writer,
    Fault::RemoteAheadOfLocal,
    persistent(PersistentCause::OtherWriter)
);
policy_is!(
    remote_regressed_is_persistent,
    Fault::RemoteRegressed,
    persistent(PersistentCause::RemoteRegressed)
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
policy_is!(readers_held_retries, Fault::ReadersHeld, doubling());
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
    page_size_changed_is_persistent_naming_both_sizes,
    Fault::PageSizeChanged {
        was: PageSize::MIN,
        now: ps()
    },
    persistent(PersistentCause::PageSizeChanged {
        was: PageSize::MIN,
        now: ps()
    })
);
policy_is!(
    wal_corrupt_below_committed_is_persistent,
    Fault::WalCorruptBelowCommitted,
    persistent(PersistentCause::WalCorrupt)
);
policy_is!(
    wal_truncated_below_committed_is_persistent,
    Fault::WalTruncatedBelowCommitted,
    persistent(PersistentCause::WalTruncated)
);
policy_is!(
    wal_unlinked_is_persistent,
    Fault::WalUnlinked,
    persistent(PersistentCause::WalUnlinked)
);
policy_is!(
    shutdown_deadline_halts,
    Fault::ShutdownDeadline,
    Policy::Shutdown
);
policy_is!(
    a_database_page_mismatch_is_persistent,
    Fault::DatabasePageMismatch { pgno: page_two() },
    persistent(PersistentCause::DatabasePageMismatch { pgno: page_two() })
);
policy_is!(
    bug_is_persistent,
    Fault::Bug(BugSite::PageMap),
    persistent(PersistentCause::Bug)
);

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
        | Fault::ReadersHeld
        | Fault::WalMoved
        | Fault::WalIndexInconsistent
        | Fault::WalChangedUnderRead
        | Fault::NothingToSnapshot
        | Fault::ReadTxLost
        | Fault::PageSizeChanged { .. }
        | Fault::WalCorruptBelowCommitted
        | Fault::WalTruncatedBelowCommitted
        | Fault::WalUnlinked
        | Fault::DatabasePageMismatch { .. }
        | Fault::ShutdownDeadline
        | Fault::Bug(_) => true,
    };
    assert!(tested(&Fault::Bug(BugSite::PageMap)));
}

fn page_two() -> PageNo {
    PageNo::new(2, PageSize::MIN).unwrap()
}

fn every_persistent_cause() -> [PersistentCause; 11] {
    [
        PersistentCause::Credentials,
        PersistentCause::StoreRefused,
        PersistentCause::RemoteCorrupt,
        PersistentCause::OtherWriter,
        PersistentCause::RemoteRegressed,
        PersistentCause::WalCorrupt,
        PersistentCause::WalTruncated,
        PersistentCause::WalUnlinked,
        PersistentCause::PageSizeChanged {
            was: PageSize::MIN,
            now: ps(),
        },
        PersistentCause::DatabasePageMismatch { pgno: page_two() },
        PersistentCause::Bug,
    ]
}

/// Ties the list above to the enum: a new cause breaks this match first.
#[test]
fn every_persistent_cause_is_listed() {
    let listed = |cause: PersistentCause| match cause {
        PersistentCause::Credentials
        | PersistentCause::StoreRefused
        | PersistentCause::RemoteCorrupt
        | PersistentCause::OtherWriter
        | PersistentCause::RemoteRegressed
        | PersistentCause::WalCorrupt
        | PersistentCause::WalTruncated
        | PersistentCause::WalUnlinked
        | PersistentCause::PageSizeChanged { .. }
        | PersistentCause::DatabasePageMismatch { .. }
        | PersistentCause::Bug => true,
    };
    assert!(every_persistent_cause().into_iter().all(listed));
}

#[test]
fn go_like_keeps_trying_on_every_cause_but_a_second_writer() {
    for cause in every_persistent_cause() {
        assert_eq!(
            Responses::GO_LIKE.stops(cause),
            cause == PersistentCause::OtherWriter,
            "{cause:?}"
        );
    }
    assert_eq!(Responses::default(), Responses::GO_LIKE);
}

#[test]
fn strict_stops_on_every_cause() {
    for cause in every_persistent_cause() {
        assert!(Responses::STRICT.stops(cause), "{cause:?}");
    }
}

/// Keeping trying on a bug, or past another writer, never resumes the state
/// the failure left: both rebuild. A replica that went backwards is listed
/// again; every other cause retries the same step.
#[test]
fn keeping_trying_rebuilds_where_the_state_cannot_be_trusted() {
    let all = Responses {
        other_writer: OtherWriter::ContinueAndInterleaveHistories,
        ..Responses::GO_LIKE
    };
    for cause in every_persistent_cause() {
        let want = match cause {
            PersistentCause::Bug | PersistentCause::OtherWriter => Action::Rebuild,
            PersistentCause::RemoteRegressed => Action::Relist,
            PersistentCause::Credentials
            | PersistentCause::StoreRefused
            | PersistentCause::RemoteCorrupt
            | PersistentCause::WalCorrupt
            | PersistentCause::WalTruncated
            | PersistentCause::WalUnlinked
            | PersistentCause::PageSizeChanged { .. }
            | PersistentCause::DatabasePageMismatch { .. } => Action::Retry,
        };
        assert_eq!(all.action(cause), want, "{cause:?}");
    }
}

/// Each field answers for its own cause only.
#[test]
fn each_field_names_one_cause() {
    for cause in every_persistent_cause() {
        let mut one = Responses::GO_LIKE;
        match cause {
            PersistentCause::Credentials => one.credentials = Response::Stop,
            PersistentCause::StoreRefused => one.store_refused = Response::Stop,
            PersistentCause::RemoteCorrupt => one.remote_corrupt = Response::Stop,
            PersistentCause::OtherWriter => {
                one.other_writer = OtherWriter::ContinueAndInterleaveHistories;
            }
            PersistentCause::RemoteRegressed => one.remote_regressed = Response::Stop,
            PersistentCause::WalCorrupt => one.wal_corrupt = Response::Stop,
            PersistentCause::WalTruncated => one.wal_truncated = Response::Stop,
            PersistentCause::WalUnlinked => one.wal_unlinked = Response::Stop,
            PersistentCause::PageSizeChanged { .. } => one.page_size_changed = Response::Stop,
            PersistentCause::DatabasePageMismatch { .. } => {
                one.database_page_mismatch = Response::Stop;
            }
            PersistentCause::Bug => one.bug = Response::Stop,
        }
        for other in every_persistent_cause() {
            let flipped = Responses::GO_LIKE.stops(other) != one.stops(other);
            assert_eq!(flipped, other == cause, "{cause:?} changed {other:?}");
        }
    }
}

#[test]
fn a_persistent_cause_and_its_stop_cause_round_trip() {
    for cause in every_persistent_cause() {
        assert_eq!(PersistentCause::of(HaltCause::from(cause)), Some(cause));
    }
    assert_eq!(PersistentCause::of(HaltCause::Shutdown), None);
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
    let created_at = StoreError::Corrupt {
        key: None,
        cause: CorruptCause::BadCreatedAt,
    };
    assert_eq!(Fault::from_staging(&created_at), Fault::StagedLost);
}

#[test]
fn from_remote_any_other_corrupt_is_remote_corrupt() {
    for cause in [
        CorruptCause::NotARegularFile,
        CorruptCause::TimestampOutOfRange,
        CorruptCause::BadCreatedAt,
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
            Fault::Bug(BugSite::WalResume),
        ),
        (
            ResumeError::NotAtCommit { at: at() },
            Fault::Bug(BugSite::WalResume),
        ),
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
            Fault::Bug(BugSite::WalPageRead),
        ),
        (
            ReadPageError::PageSizeMismatch {
                expected: ps(),
                found: PageSize::MIN,
            },
            Fault::Bug(BugSite::WalPageRead),
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
        assert_eq!(
            Fault::from_page_map(err),
            Fault::Bug(BugSite::PageMap),
            "{err:?}"
        );
    }
}

#[test]
fn from_source_maps_every_variant() {
    let cases = [
        (SourceError::Io(ErrorKind::BrokenPipe), Fault::LocalIo),
        (SourceError::DiskFull, Fault::LocalDiskFull),
        (SourceError::FsyncFailed, Fault::LocalFsyncFailed),
        (SourceError::Busy, Fault::SqliteBusy),
        (SourceError::ReadersHeld, Fault::ReadersHeld),
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

/// Go's `DefaultSyncBackoffMax`: the ceiling is a named number, not whatever
/// the arithmetic that spells it gives.
#[test]
fn the_longest_backoff_is_five_minutes() {
    assert_eq!(MAX_BACKOFF, Duration::from_secs(300));
}

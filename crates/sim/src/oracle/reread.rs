//! Second reads of a staged file: what a store that lets go of the bytes it was handed is
//! given when it reads them back (`docs/decisions/D222.md`).
//!
//! The replica's recorder stands between the engine and the store and sees both reads: the
//! bytes the engine handed over (its first read, which it verified) and what the engine's
//! source answered when the store asked for them again
//! ([`RecorderCounts::rereads_given`](crate::store::RecorderCounts::rereads_given) and the
//! two beside it). Ground truth is the first read, kept by the recorder; nothing here asks
//! the engine what it compared. A staged file may change between the two reads (the file
//! system gives a reader no isolation from a writer of the same file), and a read may fail.
//! The judge holds, tick by tick:
//!
//! - a second read never gives the store bytes other than the first read's, whatever became
//!   of the staged file in between;
//! - a tick in which a second read was refused publishes nothing;
//! - and the engine does not report that tick's failure as the replica's, unless a fault of
//!   the replica's fired in it (a store reports the first failure in part order, which may be
//!   its own). The fault that fired is the simulator's record; the report is the engine's.

use lsr_core::{Did, Event, Fault};

use super::Violation;
use crate::reach::Reach;
use crate::store::RecorderCounts;

/// The second reads of one tick, as the replica's recorder counted them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SecondReads {
    /// Reads that gave the first read's bytes.
    pub given: u64,
    /// Reads that gave other bytes.
    pub differing: u64,
    /// Reads the source refused.
    pub refused: u64,
}

impl SecondReads {
    /// The second reads among one tick's counts.
    #[must_use]
    pub const fn of(tick: &RecorderCounts) -> Self {
        Self {
            given: tick.rereads_given,
            differing: tick.rereads_differing,
            refused: tick.rereads_refused,
        }
    }
}

/// Whether `fault` tells the operator the replica is at fault.
const fn blames_replica(fault: &Fault) -> bool {
    match fault {
        Fault::StoreTransient
        | Fault::StoreThrottled { .. }
        | Fault::StoreDenied(_)
        | Fault::RemoteObjectMissing
        | Fault::UploadAmbiguous
        | Fault::UploadChecksumMismatch
        | Fault::StoreRefused
        | Fault::RemoteCorrupt
        | Fault::RemoteAheadOfLocal
        | Fault::RemoteRegressed
        | Fault::CreateOnlyUnsupported => true,
        Fault::StagedLost
        | Fault::LocalDiskFull
        | Fault::LocalIo
        | Fault::LocalFsyncFailed
        | Fault::WalSyncFailed
        | Fault::SqliteBusy
        | Fault::ReadersHeld
        | Fault::WritesHeld
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
        | Fault::Bug(_) => false,
    }
}

/// Judges the second reads of one tick that ended with `ended` (`None` for a tick that
/// crashed, stopped or ended a drain). `replica_fault_fired` says a fault of the replica's
/// fired in the tick. Counts what it judged in `reach`.
///
/// # Errors
/// [`Violation::SecondReadDiffers`], [`Violation::PublishedPastRefusedRead`] or
/// [`Violation::RefusedReadBlamedOnReplica`].
pub fn judge_rereads(
    seen: SecondReads,
    ended: Option<&Event>,
    replica_fault_fired: bool,
    reach: &mut Reach,
) -> Result<(), Violation> {
    if seen.differing > 0 {
        return Err(Violation::SecondReadDiffers);
    }
    if seen.given > 0 {
        reach.bump("reread.given");
    }
    if seen.refused == 0 {
        return Ok(());
    }
    reach.bump("reread.refused");
    match ended {
        Some(Event::Uploaded(Did::Put(_))) => Err(Violation::PublishedPastRefusedRead),
        Some(Event::Retrying { fault, .. }) if blames_replica(fault) => {
            if replica_fault_fired {
                reach.bump("reread.refused.beside_a_replica_fault");
                Ok(())
            } else {
                Err(Violation::RefusedReadBlamedOnReplica {
                    fault: format!("{fault:?}"),
                })
            }
        }
        Some(Event::Retrying { .. }) => {
            reach.bump("reread.refused.reported");
            Ok(())
        }
        Some(_) | None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use lsr_core::{CreatedAt, FileInfo, FileKey, Level, ListedTime};
    use lsr_ltx::{Timestamp, Txid, TxidRange};

    use super::*;

    fn retrying(fault: Fault) -> Event {
        Event::Retrying {
            fault,
            until: Timestamp::from_unix_millis(0),
        }
    }

    const REFUSED: SecondReads = SecondReads {
        given: 0,
        differing: 0,
        refused: 1,
    };

    #[test]
    fn a_second_read_that_gave_other_bytes_is_a_violation_however_the_tick_ended() {
        let seen = SecondReads {
            given: 3,
            differing: 1,
            refused: 0,
        };
        for ended in [None, Some(Event::Idle), Some(retrying(Fault::StagedLost))] {
            let mut reach = Reach::default();
            assert_eq!(
                judge_rereads(seen, ended.as_ref(), true, &mut reach),
                Err(Violation::SecondReadDiffers)
            );
        }
    }

    #[test]
    fn second_reads_that_gave_the_first_bytes_are_counted_and_pass() {
        let seen = SecondReads {
            given: 2,
            ..SecondReads::default()
        };
        let mut reach = Reach::default();
        assert_eq!(
            judge_rereads(seen, Some(&Event::Idle), false, &mut reach),
            Ok(())
        );
        assert_eq!(reach.get("reread.given"), 1);
        assert_eq!(reach.get("reread.refused"), 0);
        // A tick with none counts nothing.
        let mut none = Reach::default();
        assert_eq!(
            judge_rereads(SecondReads::default(), None, false, &mut none),
            Ok(())
        );
        assert_eq!(none, Reach::default());
    }

    #[test]
    fn a_refused_second_read_reported_as_the_staged_files_passes_and_is_counted() {
        for fault in [Fault::StagedLost, Fault::LocalIo] {
            let mut reach = Reach::default();
            let ended = retrying(fault);
            assert_eq!(
                judge_rereads(REFUSED, Some(&ended), false, &mut reach),
                Ok(())
            );
            assert_eq!(reach.get("reread.refused"), 1);
            assert_eq!(reach.get("reread.refused.reported"), 1);
        }
    }

    #[test]
    fn a_refused_second_read_blamed_on_the_replica_needs_a_replica_fault_that_fired() {
        let ended = retrying(Fault::UploadChecksumMismatch);
        let mut reach = Reach::default();
        assert_eq!(
            judge_rereads(REFUSED, Some(&ended), false, &mut reach),
            Err(Violation::RefusedReadBlamedOnReplica {
                fault: "UploadChecksumMismatch".to_owned()
            })
        );
        assert_eq!(
            judge_rereads(REFUSED, Some(&ended), true, &mut reach),
            Ok(())
        );
        assert_eq!(reach.get("reread.refused.beside_a_replica_fault"), 1);
        assert_eq!(reach.get("reread.refused.reported"), 0);
    }

    #[test]
    fn a_tick_that_refused_a_second_read_publishes_nothing() {
        let one = Txid::new(1).unwrap();
        let info = FileInfo {
            key: FileKey::new(Level::new(0).unwrap(), TxidRange::new(one, one).unwrap()),
            size: 1,
            listed: ListedTime::Created(CreatedAt::metadata(Timestamp::from_unix_millis(0))),
        };
        let ended = Event::Uploaded(Did::Put(info));
        let mut reach = Reach::default();
        assert_eq!(
            judge_rereads(REFUSED, Some(&ended), true, &mut reach),
            Err(Violation::PublishedPastRefusedRead)
        );
        // A tick that ended without an event (a crash) is counted and owes no report.
        assert_eq!(judge_rereads(REFUSED, None, false, &mut reach), Ok(()));
        assert_eq!(reach.get("reread.refused"), 2);
    }

    #[test]
    fn the_seconds_reads_of_a_tick_are_read_from_its_counts() {
        let tick = RecorderCounts {
            rereads_given: 4,
            rereads_differing: 5,
            rereads_refused: 6,
            ..RecorderCounts::default()
        };
        assert_eq!(
            SecondReads::of(&tick),
            SecondReads {
                given: 4,
                differing: 5,
                refused: 6
            }
        );
    }
}

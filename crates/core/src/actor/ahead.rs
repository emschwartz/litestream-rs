//! What the actor does with the check a boot owes after a failed WAL sync (`crate::ahead`,
//! `docs/decisions.md` D103).
//!
//! A boot that restores a WAL-sync debt from the engine journal, while the WAL is still the
//! generation of the newest staged file, owes the check before the TRUNCATE that pays the debt:
//! the TRUNCATE would copy the WAL into the database file and the evidence with it. The check is
//! the first thing the next sync step does. A clean file goes on to the TRUNCATE. A file ahead
//! of its WAL is a verdict:
//!
//! 1. It is held in `Kept` and written to the journal at the end of the tick, before the tick
//!    that runs the TRUNCATE, so a restart neither checks again nor forgets it.
//! 2. It is raised as [`StopCause::DatabaseAheadOfWal`] through the configured response: a stop
//!    under `STRICT`, else the sync task degraded until the cure's next step succeeds.
//! 3. It keeps the newest staged transaction at the check, the state before the damage,
//!    restorable for [`AHEAD_KEPT_FOR`] from when it was made, and reports it ([`Actor::ahead`]) for as
//!    long. Then the restore point goes back to normal retention. No operator action ends or
//!    extends it, and a verdict already recorded is not raised again at a later boot.
//!
//! A second verdict while one stands keeps the older restore point (the newer one sits on the
//! damaged file) and restarts the window. An engine clock earlier than the verdict's time (a
//! journal from a clock that ran ahead) restarts the window once, from now.
//!
//! The images the check compares against are the replica's state at its newest confirmed
//! transaction. A replica that cannot give it because a call failed is retried, as any store
//! fault is. One that cannot because it is damaged (a gap a bucket rollback left, a file that
//! does not verify) would hold the check, and with it the TRUNCATE, for ever. So damage is
//! reported as `RemoteCorrupt` once, and when it recurs at a later attempt the check is given up
//! (`docs/decisions.md` D106):
//!
//! 1. The notice is held in `Kept` and journalled as the verdict is, and raised as
//!    [`StopCause::DatabaseAheadOfWalUnchecked`] through the same response: a stop under `STRICT`, else
//!    the cure runs unchecked.
//! 2. It stands for [`AHEAD_KEPT_FOR`] ([`Actor::ahead_of_wal_unchecked`]), and keeps no restore point:
//!    the state the check would have kept is the one the replica could not give.
//! 3. A second one while one stands restarts the window.

use std::time::Duration;

use lsr_journal::{Ahead, AheadKind, Unchecked};
use lsr_ltx::{Timestamp, Txid};

use super::parts::Course;
use super::{Actor, Env, Stepped};
use crate::ahead::{CheckError, Found};
use crate::fault::role::Role;
use crate::fault::{Fault, StopCause};
use crate::source::{Checkpointer, PageSource, WalSource};

/// How long a verdict keeps the state before the damage restorable: long enough for an operator
/// to notice the log line over a weekend and a few working days. The kept chain is about one
/// copy of the database and does not grow.
pub const AHEAD_KEPT_FOR: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Whether the next sync step owes the check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AheadCheck {
    /// Nothing to check.
    No,
    /// A boot restored a debt over the WAL of the newest staged file.
    Owed,
    /// Owed still, after an attempt found the replica unable to give the images: the next
    /// attempt that finds the same gives the check up.
    OwedAgain,
}

impl Actor {
    /// After boot adopted the journal: the check is owed when the WAL is still the newest
    /// staged file's generation (`continued`), a debt is owed, and no verdict stands already.
    pub(super) fn owe_ahead_check(&mut self, continued: bool) {
        let owed = continued
            && self.kept.unproven != super::unproven::Unproven::No
            && self.kept.ahead.is_none();
        self.kept.ahead_check = if owed {
            AheadCheck::Owed
        } else {
            AheadCheck::No
        };
    }

    /// Whether the next sync step is the check.
    pub(super) const fn owes_ahead_check(&self) -> bool {
        match self.kept.ahead_check {
            AheadCheck::Owed | AheadCheck::OwedAgain => true,
            AheadCheck::No => false,
        }
    }

    /// The check, in place of a sync step.
    pub(super) async fn check_ahead<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<Stepped, Fault>
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: Role,
        R: Role,
    {
        let Course::At { position, .. } = &self.engine.course else {
            // A snapshot is owed already: the WAL is not the newest staged file's.
            self.kept.ahead_check = AheadCheck::No;
            return Ok(Stepped::Synced {
                files: 0,
                proof: None,
            });
        };
        let checked = crate::ahead::check(
            position,
            env.wal,
            env.db,
            &self.engine.pages,
            env.remote,
            self.engine.upload.confirmed(),
        )
        .await;
        let found = match checked {
            Ok(found) => found,
            Err(CheckError::Fault(fault)) => return Err(fault),
            Err(CheckError::Unrestorable) => return self.unrestorable(now),
        };
        let restore_point = position.txid();
        self.kept.ahead_check = AheadCheck::No;
        let Some(found) = found else {
            // A step cut short: the sync runs again at once, and the TRUNCATE comes next.
            self.engine.course.set_tail(super::parts::Tail::More);
            return Ok(Stepped::Synced {
                files: 0,
                proof: None,
            });
        };
        Ok(self.found(AheadKind::AheadOfWal, Some(restore_point), found, now))
    }

    /// A verdict that the database file holds a state no commit made: kept, with the older of
    /// its restore point and a standing verdict's, and raised as its stop cause. A verdict with
    /// no restore point (nothing was ever staged) keeps nothing and is raised all the same.
    pub(super) fn found(
        &mut self,
        kind: AheadKind,
        restore_point: Option<Txid>,
        found: Found,
        now: Timestamp,
    ) -> Stepped {
        let held = self.kept.ahead.map(|old| old.restore_point);
        if let Some(restore_point) = held.into_iter().chain(restore_point).min() {
            self.kept.ahead = Some(Ahead {
                kind,
                restore_point,
                pages: found.pages,
                first: found.first,
                made: now,
            });
        }
        let (pages, first) = (found.pages, found.first);
        Stepped::Stop(match kind {
            AheadKind::AheadOfWal => StopCause::DatabaseAheadOfWal { pages, first },
            AheadKind::WalLostCommits => StopCause::WalLostCommits { pages, first },
        })
    }

    /// The replica could not give the images because it is damaged: reported as such once, and
    /// the check given up when it recurs.
    fn unrestorable(&mut self, now: Timestamp) -> Result<Stepped, Fault> {
        match self.kept.ahead_check {
            AheadCheck::Owed => {
                self.kept.ahead_check = AheadCheck::OwedAgain;
                Err(Fault::RemoteCorrupt)
            }
            AheadCheck::OwedAgain | AheadCheck::No => {
                self.kept.ahead_check = AheadCheck::No;
                self.kept.unchecked = Some(Unchecked { made: now });
                Ok(Stepped::Stop(StopCause::DatabaseAheadOfWalUnchecked))
            }
        }
    }

    /// Restores a notice a journal recorded that a check could not be made, keeping the newer
    /// of two, and tells the operator at the next tick how long it stands.
    pub(super) fn adopt_unchecked(&mut self, recorded: Option<Unchecked>) {
        self.kept.unchecked = newer(self.kept.unchecked, recorded);
        if recorded.is_some()
            && let Some(unchecked) = self.kept.unchecked
        {
            self.kept.ahead_notice = Some(super::AheadNotice::UncheckedStands {
                until: unchecked_until(unchecked),
            });
        }
    }

    /// Ends a notice whose window has passed; re-dates one made after `now`, once. Returns
    /// whether it ended.
    pub(super) fn expire_unchecked(&mut self, now: Timestamp) -> bool {
        let Some(unchecked) = self.kept.unchecked else {
            return false;
        };
        self.kept.unchecked = stands(unchecked, now);
        self.kept.unchecked.is_none()
    }

    /// The standing notice that a boot owed the check for a database file ahead of its WAL and
    /// could not make it, so the cure ran unchecked (`docs/decisions.md` D106).
    #[must_use]
    pub const fn ahead_of_wal_unchecked(&self) -> Option<Unchecked> {
        self.kept.unchecked
    }

    /// Restores a verdict a journal recorded, keeping the older restore point of two, and tells
    /// the operator at the next tick how long its restore point is still kept.
    pub(super) fn adopt_ahead(&mut self, recorded: Option<Ahead>) {
        self.kept.ahead = merged(self.kept.ahead, recorded);
        if recorded.is_some()
            && let Some(ahead) = self.kept.ahead
        {
            self.kept.ahead_notice = Some(super::AheadNotice::Kept {
                kind: ahead.kind,
                restore_point: ahead.restore_point,
                until: kept_until(ahead),
            });
        }
    }

    /// Ends a verdict whose window has passed; re-dates one made after `now` (a clock that ran
    /// ahead), once. Returns a verdict that ended, so retention may release the chain it kept
    /// and the operator hears of it.
    pub(super) fn expire_ahead(&mut self, now: Timestamp) -> Option<Ahead> {
        let ahead = self.kept.ahead?;
        match expired(ahead, now) {
            Ok(kept) => {
                self.kept.ahead = Some(kept);
                None
            }
            Err(_) => {
                self.kept.ahead = None;
                Some(ahead)
            }
        }
    }

    /// The standing verdict that the database file was found ahead of its WAL, while the state
    /// before it is kept restorable (`docs/decisions.md` D103).
    #[must_use]
    pub const fn ahead(&self) -> Option<Ahead> {
        self.kept.ahead
    }
}

/// The verdict a process holds after reading `read` beside `held`: of two, the one with the older
/// restore point, the state before any damage; the one held when they name the same point.
fn merged(held: Option<Ahead>, read: Option<Ahead>) -> Option<Ahead> {
    match (held, read) {
        (Some(held), Some(read)) if read.restore_point < held.restore_point => Some(read),
        (Some(held), _) => Some(held),
        (None, read) => read,
    }
}

/// The verdict at `now`: still kept (re-dated to `now` if it was made later, by a clock that ran
/// ahead), or ended, with the restore point it kept.
fn expired(ahead: Ahead, now: Timestamp) -> Result<Ahead, Txid> {
    let ahead = Ahead {
        made: ahead.made.min(now),
        ..ahead
    };
    if now >= kept_until(ahead) {
        Err(ahead.restore_point)
    } else {
        Ok(ahead)
    }
}

/// When a verdict's restore point goes back to normal retention.
#[must_use]
pub fn kept_until(ahead: Ahead) -> Timestamp {
    ahead.made.saturating_add(AHEAD_KEPT_FOR)
}

/// Of a notice held and one read, the newer: a later check given up restarts the window.
fn newer(held: Option<Unchecked>, read: Option<Unchecked>) -> Option<Unchecked> {
    match (held, read) {
        (Some(held), Some(read)) => Some(Unchecked {
            made: held.made.max(read.made),
        }),
        (held, read) => held.or(read),
    }
}

/// The notice at `now`: still standing (re-dated to `now` if it was made later, by a clock that
/// ran ahead), or `None` once its window has passed.
fn stands(unchecked: Unchecked, now: Timestamp) -> Option<Unchecked> {
    let unchecked = Unchecked {
        made: unchecked.made.min(now),
    };
    (now < unchecked_until(unchecked)).then_some(unchecked)
}

/// When a notice that a check could not be made ends.
#[must_use]
pub fn unchecked_until(unchecked: Unchecked) -> Timestamp {
    unchecked.made.saturating_add(AHEAD_KEPT_FOR)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    fn at(millis: i64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    fn verdict(restore_point: u64, made: i64) -> Ahead {
        Ahead {
            kind: AheadKind::AheadOfWal,
            restore_point: Txid::new(restore_point).unwrap(),
            pages: NonZeroU32::MIN,
            first: NonZeroU32::MIN,
            made: at(made),
        }
    }

    /// Of two verdicts, the older restore point wins; on the same point, the one held stays.
    #[test]
    fn the_older_restore_point_wins() {
        let (held, older, newer) = (verdict(5, 1), verdict(3, 2), verdict(7, 3));
        assert_eq!(merged(Some(held), Some(older)), Some(older));
        assert_eq!(merged(Some(held), Some(newer)), Some(held));
        assert_eq!(merged(Some(held), Some(verdict(5, 9))), Some(held));
        assert_eq!(merged(Some(held), None), Some(held));
        assert_eq!(merged(None, Some(newer)), Some(newer));
        assert_eq!(merged(None, None), None);
    }

    /// Kept one millisecond before the window's end, released at it and after it.
    #[test]
    fn the_window_ends_exactly_seven_days_after_the_verdict() {
        let ahead = verdict(5, 1_000);
        let end = 1_000 + 7 * 24 * 60 * 60 * 1_000;
        assert_eq!(expired(ahead, at(end - 1)), Ok(ahead));
        assert_eq!(expired(ahead, at(end)), Err(ahead.restore_point));
        assert_eq!(expired(ahead, at(end + 1)), Err(ahead.restore_point));
    }

    /// A verdict dated after `now` (a journal from a clock that ran ahead) is re-dated to `now`,
    /// so its window runs from now, never from the future.
    #[test]
    fn a_verdict_from_the_future_is_re_dated() {
        let ahead = verdict(5, 50_000);
        assert_eq!(expired(ahead, at(1_000)), Ok(verdict(5, 1_000)));
        assert_eq!(expired(ahead, at(50_000)), Ok(ahead));
    }

    fn notice(made: i64) -> Unchecked {
        Unchecked { made: at(made) }
    }

    /// Of two notices the newer stands: a later check given up restarts the window.
    #[test]
    fn the_newer_notice_wins() {
        assert_eq!(newer(Some(notice(5)), Some(notice(9))), Some(notice(9)));
        assert_eq!(newer(Some(notice(9)), Some(notice(5))), Some(notice(9)));
        assert_eq!(newer(Some(notice(5)), Some(notice(5))), Some(notice(5)));
        assert_eq!(newer(None, Some(notice(5))), Some(notice(5)));
        assert_eq!(newer(Some(notice(5)), None), Some(notice(5)));
        assert_eq!(newer(None, None), None);
    }

    /// The notice stands until exactly seven days after it was made, and one made after now is
    /// re-dated to now.
    #[test]
    fn the_notice_ends_exactly_seven_days_after_it_was_made() {
        let window = i64::try_from(AHEAD_KEPT_FOR.as_millis()).unwrap();
        assert_eq!(stands(notice(1_000), at(1_000)), Some(notice(1_000)));
        assert_eq!(
            stands(notice(1_000), at(1_000 + window - 1)),
            Some(notice(1_000))
        );
        assert_eq!(stands(notice(1_000), at(1_000 + window)), None);
        assert_eq!(stands(notice(5_000), at(2_000)), Some(notice(2_000)));
        assert_eq!(unchecked_until(notice(1_000)), at(1_000 + window));
    }
}

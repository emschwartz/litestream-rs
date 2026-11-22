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

use std::time::Duration;

use lsr_journal::Ahead;
use lsr_ltx::{Timestamp, Txid};

use super::parts::Course;
use super::{Actor, Env, Stepped};
use crate::fault::{Fault, StopCause};
use crate::source::{Checkpointer, PageSource, WalSource};
use crate::store::ObjectStore;

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
    pub(super) fn owes_ahead_check(&self) -> bool {
        self.kept.ahead_check == AheadCheck::Owed
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
        L: ObjectStore,
        R: ObjectStore,
    {
        let Course::At { position, .. } = &self.engine.course else {
            // A snapshot is owed already: the WAL is not the newest staged file's.
            self.kept.ahead_check = AheadCheck::No;
            return Ok(Stepped::Synced {
                files: 0,
                proof: None,
            });
        };
        let found = crate::ahead::check(
            position,
            env.wal,
            env.db,
            &self.engine.pages,
            env.remote,
            self.engine.upload.confirmed(),
        )
        .await?;
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
        self.kept.ahead = Some(Ahead {
            restore_point: self
                .kept
                .ahead
                .map_or(restore_point, |old| old.restore_point.min(restore_point)),
            pages: found.pages,
            first: found.first,
            made: now,
        });
        Ok(Stepped::Stop(StopCause::DatabaseAheadOfWal {
            pages: found.pages,
            first: found.first,
        }))
    }

    /// Restores a verdict a journal recorded, keeping the older restore point of two, and tells
    /// the operator at the next tick how long its restore point is still kept.
    pub(super) fn adopt_ahead(&mut self, recorded: Option<Ahead>) {
        self.kept.ahead = merged(self.kept.ahead, recorded);
        if recorded.is_some()
            && let Some(ahead) = self.kept.ahead
        {
            self.kept.ahead_notice = Some(super::AheadNotice::Kept {
                restore_point: ahead.restore_point,
                until: kept_until(ahead),
            });
        }
    }

    /// Ends a verdict whose window has passed; re-dates one made after `now` (a clock that ran
    /// ahead), once. Returns the restore point a verdict that ended kept, so retention may
    /// release its chain and the operator hears of it.
    pub(super) fn expire_ahead(&mut self, now: Timestamp) -> Option<Txid> {
        match expired(self.kept.ahead?, now) {
            Ok(kept) => {
                self.kept.ahead = Some(kept);
                None
            }
            Err(released) => {
                self.kept.ahead = None;
                Some(released)
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    fn at(millis: i64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    fn verdict(restore_point: u64, made: i64) -> Ahead {
        Ahead {
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
}

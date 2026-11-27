//! The read limit on whole-database reads (`snapshot-read-limit`, issue 315).
//!
//! The engine reads every page of the database for a boot copy, a level-9 snapshot and the
//! scheduled verify. In-process those reads share the page cache and the disk with the
//! application, and no cgroup stands between them. A [`Paced`] page source spends a byte budget
//! as it reads and waits on the runtime's clock between pages, so those reads proceed at the
//! limit and no faster. It never charges anything when no limit is set.
//!
//! The wait sits inside `read_page`. A boot copy and a verify read are one unit of work of the
//! engine (nothing else could run meanwhile), so the wait stalls nothing. A level-9 snapshot is
//! sliced: [`slice_bytes`] shrinks the bytes of a tick to a tenth of a second of reading, so a
//! slice waits about that long and the engine's WAL step still has its turn every few slices
//! (`docs/decisions/D217.md`, `docs/decisions/D255.md`).
//!
//! The budget is a due time computed from one epoch, never from the last read: the clock's
//! millisecond grain then never adds up across thousands of pages. A reader more than a quarter
//! second behind its budget starts a new one, so credit is not banked and an idle stretch is
//! not repaid with a burst. Go has no such limit; the bench arm that measured it is
//! `crates/compat/bench-patches/read-pace.patch`.

use std::num::NonZeroU64;
use std::time::Duration;

use lsr_core::{PageSource, SourceError};
use lsr_ltx::{DbPages, PageNo, Timestamp};

use crate::runtime::{Runtime, later};

/// A read within this much of its due time does not wait: a page at a time costs no wait, and
/// the long-run rate stays exact.
const SLACK: Duration = Duration::from_millis(1);

/// A reader this far behind its budget starts a new one.
const BANK: Duration = Duration::from_millis(250);

/// The part of a second of reading one engine tick may hand over under a limit.
const TICK_SHARE: u64 = 10;

/// The smallest slice a limit shrinks a tick to: one read of a staged file's chunk.
const MIN_SLICE: u64 = 256 * 1024;

/// The bytes one upload tick hands the replica under `limit` (bytes a second): a tenth of a
/// second of reading, at least [`MIN_SLICE`] and never more than the engine's own bound.
pub(crate) fn slice_bytes(limit: NonZeroU64, bound: NonZeroU64) -> NonZeroU64 {
    let share = limit.get().checked_div(TICK_SHARE).unwrap_or(MIN_SLICE);
    NonZeroU64::new(share.max(MIN_SLICE).min(bound.get())).unwrap_or(bound)
}

/// The budget of one limited read: what has been read since its epoch, and when it is due.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Pace {
    bytes_per_s: NonZeroU64,
    /// When the current budget began.
    epoch: Option<Timestamp>,
    /// The time the bytes charged since the epoch ought to have taken.
    spent: Duration,
}

impl Pace {
    pub(crate) fn new(bytes_per_s: NonZeroU64) -> Self {
        Self {
            bytes_per_s,
            epoch: None,
            spent: Duration::ZERO,
        }
    }

    /// Charges `bytes` read as of `now`. The time to wait until, when the reader is more than
    /// [`SLACK`] ahead of its budget.
    pub(crate) fn charge(&mut self, now: Timestamp, bytes: usize) -> Option<Timestamp> {
        let nanos = u128::from(u64::try_from(bytes).unwrap_or(u64::MAX))
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(self.bytes_per_s.get()))
            .unwrap_or(0);
        let epoch = *self.epoch.get_or_insert(now);
        self.spent = self.spent.saturating_add(Duration::from_nanos(
            u64::try_from(nanos).unwrap_or(u64::MAX),
        ));
        let due = later(epoch, self.spent);
        if due > later(now, SLACK) {
            return Some(due);
        }
        if later(due, BANK) < now {
            self.epoch = Some(now);
            self.spent = Duration::ZERO;
        }
        None
    }
}

/// A page source whose reads proceed at a limit, or at the inner source's own speed when there
/// is none.
pub(crate) struct Paced<S> {
    inner: S,
    runtime: Runtime,
    pace: Option<Pace>,
}

impl<S> Paced<S> {
    pub(crate) fn new(inner: S, runtime: Runtime, limit: Option<NonZeroU64>) -> Self {
        Self {
            inner,
            runtime,
            pace: limit.map(Pace::new),
        }
    }
}

impl<S: PageSource> PageSource for Paced<S> {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        self.inner.page_count()
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        let read = self.inner.read_page(pgno, buf);
        if let Some(pace) = &mut self.pace {
            let due = pace.charge(self.runtime.clock.now(), buf.len());
            while let Some(due) = due.filter(|due| self.runtime.clock.now() < *due) {
                self.runtime.park.park(Some(due));
            }
        }
        read
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const START: Timestamp = Timestamp::from_unix_millis(1_700_000_000_000);
    const PAGE: usize = 4096;

    fn rate(bytes: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes).unwrap()
    }

    /// Reads `pages` pages the way a reader does: wait out what `charge` asks, then go on.
    /// The time the run took, in milliseconds.
    fn run(pace: &mut Pace, mut now: Timestamp, pages: u64) -> i64 {
        let began = now;
        for _ in 0..pages {
            if let Some(due) = pace.charge(now, PAGE) {
                now = due;
            }
        }
        now.unix_millis().saturating_sub(began.unix_millis())
    }

    #[test]
    fn a_run_of_pages_takes_what_the_limit_says() {
        let mut pace = Pace::new(rate(1 << 20));
        // 16 MiB at 1 MiB a second.
        assert!((15_950..=16_050).contains(&run(&mut pace, START, 4096)));
    }

    #[test]
    fn an_idle_stretch_is_not_repaid_with_a_burst() {
        let mut pace = Pace::new(rate(1 << 20));
        run(&mut pace, START, 256);
        let later_on = Timestamp::from_unix_millis(START.unix_millis() + 60_000);
        // 4 MiB after a minute of nothing still takes four seconds.
        assert!((3_950..=4_050).contains(&run(&mut pace, later_on, 1024)));
    }

    #[test]
    fn a_reader_a_little_behind_keeps_its_credit() {
        let mut pace = Pace::new(rate(1 << 20));
        run(&mut pace, START, 64);
        // 100 ms late (inside the quarter second): the next second's bytes are still due at the
        // time the whole run's rate gives, so the late reader catches up without waiting.
        let late = Timestamp::from_unix_millis(START.unix_millis() + 350);
        assert_eq!(pace.charge(late, PAGE), None);
    }

    #[test]
    fn a_page_inside_the_slack_does_not_wait() {
        let mut pace = Pace::new(rate(1 << 30));
        assert_eq!(pace.charge(START, PAGE), None);
    }

    #[test]
    fn a_tick_under_a_limit_is_a_tenth_of_a_second_of_reading_within_bounds() {
        let bound = rate(8 << 20);
        assert_eq!(slice_bytes(rate(20 << 20), bound).get(), 2 << 20);
        assert_eq!(slice_bytes(rate(1 << 20), bound).get(), MIN_SLICE);
        assert_eq!(slice_bytes(rate(1 << 40), bound), bound);
    }

    proptest! {
        /// Over any limit and run, the time taken is the bytes over the limit, to a millisecond
        /// of grain a page.
        #[test]
        fn the_rate_holds_over_any_run(limit in 65_536_u64..(1 << 30), pages in 1_u64..4096) {
            let mut pace = Pace::new(rate(limit));
            let took = run(&mut pace, START, pages);
            let bytes = pages.saturating_mul(u64::try_from(PAGE).unwrap());
            let want = i64::try_from(bytes.saturating_mul(1000).checked_div(limit).unwrap()).unwrap();
            prop_assert!(took.abs_diff(want) <= 2, "took {took} ms, want {want}");
        }
    }
}

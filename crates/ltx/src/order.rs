//! Which page numbers may follow which, shared by the decoder, the page index
//! and the encoder.
//!
//! These are the Go encoder's rules at ltx v0.5.1 and v0.5.3, so every file Go
//! wrote follows them: no page above the commit size, never the lock page, a
//! snapshot's pages run 1, 2, 3, ... skipping only the lock page, and other
//! files' pages strictly ascend. A snapshot must also end at the commit size
//! (or the page below it, when the commit size is the lock page). Go's encoder
//! does not check that last rule, but Litestream only writes whole snapshots
//! and Go's own restore (`DecodeDatabaseTo`) fails on a short one, so a short
//! snapshot is corrupt here too.

use std::num::NonZeroU32;

use thiserror::Error;

use crate::page::lock_page;
use crate::{DbPages, Header, PageNo, PageSize};

/// A page number out of place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum OrderError {
    /// The page is past the end of the database.
    #[error("page {pgno} is above the commit size {commit}")]
    AboveCommit {
        /// The page number.
        pgno: u32,
        /// The header's commit size.
        commit: u32,
    },
    /// The page is the lock page, which SQLite never writes.
    #[error("page {pgno} is the lock page")]
    LockPage {
        /// The page number.
        pgno: u32,
    },
    /// A snapshot skipped or repeated a page.
    #[error("snapshot page {pgno} where page {expected} belongs")]
    SnapshotGap {
        /// The page number.
        pgno: u32,
        /// The page the snapshot needed next.
        expected: u32,
    },
    /// A page number did not exceed the one before it.
    #[error("page {pgno} after page {prev}")]
    NotAscending {
        /// The page number.
        pgno: u32,
        /// The page before it.
        prev: u32,
    },
    /// A snapshot ended before the database did.
    #[error("snapshot ends at page {last} but the commit size is {commit}")]
    SnapshotShort {
        /// The last page in the snapshot, zero for none.
        last: u32,
        /// The header's commit size.
        commit: u32,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PageSequence {
    snapshot: bool,
    commit: u32,
    lock: u32,
    page_size: PageSize,
    /// The last page accepted, zero before the first.
    last: u32,
}

impl PageSequence {
    pub(crate) fn new(header: &Header) -> Self {
        Self::raw(header.is_snapshot(), header.commit(), header.page_size())
    }

    pub(crate) const fn raw(snapshot: bool, commit: u32, page_size: PageSize) -> Self {
        Self {
            snapshot,
            commit,
            lock: lock_page(page_size),
            page_size,
            last: 0,
        }
    }

    pub(crate) fn push(&mut self, pgno: NonZeroU32) -> Result<PageNo, OrderError> {
        let n = pgno.get();
        if n > self.commit {
            return Err(OrderError::AboveCommit {
                pgno: n,
                commit: self.commit,
            });
        }
        if n == self.lock {
            return Err(OrderError::LockPage { pgno: n });
        }
        if self.snapshot {
            let expected = self.after(self.last);
            if n != expected {
                return Err(OrderError::SnapshotGap { pgno: n, expected });
            }
        } else if n <= self.last {
            return Err(OrderError::NotAscending {
                pgno: n,
                prev: self.last,
            });
        }
        self.last = n;
        Ok(PageNo::checked(pgno))
    }

    pub(crate) fn finish(&self) -> Result<(), OrderError> {
        let end = DbPages::new(self.commit)
            .last_page(self.page_size)
            .map_or(0, PageNo::get);
        if self.snapshot && self.last != end {
            return Err(OrderError::SnapshotShort {
                last: self.last,
                commit: self.commit,
            });
        }
        Ok(())
    }

    const fn after(&self, pgno: u32) -> u32 {
        let next = pgno.saturating_add(1);
        if next == self.lock {
            next.saturating_add(1)
        } else {
            next
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    fn sizes() -> impl Iterator<Item = PageSize> {
        (9..=16).map(|shift| PageSize::new(1 << shift).unwrap())
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "pushes about a million pages one at a time; native runs cover it"
    )]
    fn snapshot_crosses_the_lock_page_at_every_size() {
        for size in sizes() {
            let lock = lock_page(size);
            for commit in [lock - 1, lock, lock + 1, lock + 2] {
                let mut seq = PageSequence::raw(true, commit, size);
                for pgno in (1..=commit).filter(|p| *p != lock) {
                    seq.push(nz(pgno)).unwrap();
                }
                seq.finish().unwrap();
            }
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "pushes about a million pages one at a time; native runs cover it"
    )]
    fn lock_page_is_rejected_at_every_size() {
        for size in sizes() {
            let lock = lock_page(size);
            let mut incremental = PageSequence::raw(false, lock + 5, size);
            incremental.push(nz(3)).unwrap();
            assert_eq!(
                incremental.push(nz(lock)),
                Err(OrderError::LockPage { pgno: lock })
            );
            let mut snapshot = PageSequence::raw(true, lock + 5, size);
            for pgno in 1..lock {
                snapshot.push(nz(pgno)).unwrap();
            }
            assert_eq!(
                snapshot.push(nz(lock)),
                Err(OrderError::LockPage { pgno: lock })
            );
        }
    }

    #[test]
    fn page_above_commit_is_rejected() {
        let mut seq = PageSequence::raw(false, 4, PageSize::MIN);
        assert_eq!(
            seq.push(nz(5)),
            Err(OrderError::AboveCommit { pgno: 5, commit: 4 })
        );
    }

    #[test]
    fn snapshot_gap_is_rejected() {
        let mut seq = PageSequence::raw(true, 4, PageSize::MIN);
        assert_eq!(
            seq.push(nz(2)),
            Err(OrderError::SnapshotGap {
                pgno: 2,
                expected: 1
            })
        );
        seq.push(nz(1)).unwrap();
        assert_eq!(
            seq.push(nz(1)),
            Err(OrderError::SnapshotGap {
                pgno: 1,
                expected: 2
            })
        );
    }

    #[test]
    fn non_ascending_is_rejected() {
        let mut seq = PageSequence::raw(false, 9, PageSize::MIN);
        seq.push(nz(4)).unwrap();
        assert_eq!(
            seq.push(nz(4)),
            Err(OrderError::NotAscending { pgno: 4, prev: 4 })
        );
        assert_eq!(
            seq.push(nz(2)),
            Err(OrderError::NotAscending { pgno: 2, prev: 4 })
        );
    }

    #[test]
    fn short_snapshot_is_rejected() {
        let mut seq = PageSequence::raw(true, 3, PageSize::MIN);
        assert_eq!(
            seq.finish(),
            Err(OrderError::SnapshotShort { last: 0, commit: 3 })
        );
        seq.push(nz(1)).unwrap();
        seq.push(nz(2)).unwrap();
        assert_eq!(
            seq.finish(),
            Err(OrderError::SnapshotShort { last: 2, commit: 3 })
        );
        PageSequence::raw(true, 0, PageSize::MIN).finish().unwrap();
        PageSequence::raw(false, 3, PageSize::MIN).finish().unwrap();
    }
}

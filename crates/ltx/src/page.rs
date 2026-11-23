//! Page sizes and page numbers.
//!
//! SQLite never writes the page that holds byte offset 2^30 (the "pending
//! byte" its file locks use), so that page, the lock page, never appears in an
//! LTX file. Which page it is depends on the page size.

use std::fmt;
use std::num::NonZeroU32;

use crate::PageChecksum;
use crate::conv::usize_from;

/// Byte offset of SQLite's pending byte.
const PENDING_BYTE: u32 = 0x4000_0000;

/// A database page size: a power of two from 512 to 65536 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PageSize(u32);

impl PageSize {
    /// The smallest page size SQLite allows.
    pub const MIN: Self = Self(512);
    /// The largest page size SQLite allows.
    pub const MAX: Self = Self(65536);

    /// Returns `None` unless `bytes` is a power of two in 512..=65536.
    #[must_use]
    pub const fn new(bytes: u32) -> Option<Self> {
        if bytes.is_power_of_two() && bytes >= Self::MIN.0 && bytes <= Self::MAX.0 {
            Some(Self(bytes))
        } else {
            None
        }
    }

    /// The size in bytes, as stored in an LTX header.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The size in bytes, for buffer lengths.
    #[must_use]
    pub const fn bytes(self) -> usize {
        usize_from(self.0)
    }
}

/// The page that holds SQLite's pending byte at this page size.
#[must_use]
pub const fn lock_page(page_size: PageSize) -> u32 {
    PENDING_BYTE
        .wrapping_shr(page_size.0.trailing_zeros())
        .wrapping_add(1)
}

/// A page number that can appear in an LTX file of some page size: never
/// zero and never that size's lock page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageNo(NonZeroU32);

impl PageNo {
    /// Returns `None` for zero and for the lock page of `page_size`.
    #[must_use]
    pub const fn new(pgno: u32, page_size: PageSize) -> Option<Self> {
        match NonZeroU32::new(pgno) {
            Some(nonzero) if pgno != lock_page(page_size) => Some(Self(nonzero)),
            Some(_) | None => None,
        }
    }

    /// The page number, 1-based as in SQLite.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    pub(crate) const fn nonzero(self) -> NonZeroU32 {
        self.0
    }

    /// For callers that already checked the lock page against their header.
    pub(crate) const fn checked(pgno: NonZeroU32) -> Self {
        Self(pgno)
    }
}

impl fmt::Display for PageNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One page's bytes with the page size they are a page of: `data` is exactly
/// one page long and `pgno` is a page a database of that size can hold, so a
/// writer places it from the page size, never from the length of a slice.
///
/// ```
/// # use lsr_ltx::{PageNo, PageRef, PageSize};
/// let size = PageSize::new(4096).unwrap();
/// let pgno = PageNo::new(2, size).unwrap();
/// assert!(PageRef::new(pgno, size, &[0; 512]).is_none());
/// assert_eq!(PageRef::new(pgno, size, &[0; 4096]).unwrap().offset(), 4096);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRef<'a> {
    pgno: PageNo,
    size: PageSize,
    data: &'a [u8],
}

impl<'a> PageRef<'a> {
    /// Returns `None` unless `data` is exactly one page of `size` and `pgno`
    /// is not the lock page at `size`.
    #[must_use]
    pub fn new(pgno: PageNo, size: PageSize, data: &'a [u8]) -> Option<Self> {
        let fits = data.len() == size.bytes() && PageNo::new(pgno.get(), size).is_some();
        fits.then_some(Self { pgno, size, data })
    }

    /// The page number.
    #[must_use]
    pub const fn pgno(&self) -> PageNo {
        self.pgno
    }

    /// The page size.
    #[must_use]
    pub const fn size(&self) -> PageSize {
        self.size
    }

    /// Exactly one page of bytes.
    #[must_use]
    pub const fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Where the page starts in a database file: (pgno - 1) times the page size.
    #[must_use]
    pub fn offset(&self) -> u64 {
        // At most (2^32 - 2) * 2^16, far inside u64.
        u64::from(self.pgno.get().wrapping_sub(1)).wrapping_mul(u64::from(self.size.0))
    }

    /// The page's checksum.
    #[must_use]
    pub fn checksum(&self) -> PageChecksum {
        PageChecksum::of(self.pgno, self.data)
    }
}

/// A database's length in pages, as an LTX header's commit field stores it.
///
/// The lock page counts toward the length but is never held, so the last page
/// a file of the database holds is [`DbPages::last_page`], not the length.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DbPages(u32);

impl DbPages {
    /// A database of no pages.
    pub const EMPTY: Self = Self(0);

    /// A database of `pages` pages.
    #[must_use]
    pub const fn new(pages: u32) -> Self {
        Self(pages)
    }

    /// The length in pages.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Whether `pgno` is inside the database.
    #[must_use]
    pub const fn contains(self, pgno: PageNo) -> bool {
        pgno.get() <= self.0
    }

    /// The highest page a file of this database holds: the last page, or the
    /// one below it when the last page is the lock page. `None` when empty.
    #[must_use]
    pub const fn last_page(self, page_size: PageSize) -> Option<PageNo> {
        match PageNo::new(self.0, page_size) {
            Some(pgno) => Some(pgno),
            None => PageNo::new(self.0.saturating_sub(1), page_size),
        }
    }

    /// The database's length in bytes.
    #[must_use]
    pub fn bytes(self, page_size: PageSize) -> u64 {
        u64::from(self.0).wrapping_mul(u64::from(page_size.0))
    }
}

impl fmt::Display for DbPages {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_numbers_print_as_decimal() {
        let page = PageNo::new(4242, PageSize::new(4096).unwrap()).unwrap();
        assert_eq!(page.to_string(), "4242");
    }

    #[test]
    fn page_size_accepts_only_sqlite_sizes() {
        let valid: Vec<u32> = (0..=17)
            .map(|shift| 1 << shift)
            .filter(|n| PageSize::new(*n).is_some())
            .collect();
        assert_eq!(valid, [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]);
        assert_eq!(PageSize::new(0), None);
        assert_eq!(PageSize::new(4097), None);
        assert_eq!(PageSize::new(u32::MAX), None);
    }

    #[test]
    fn lock_page_holds_the_pending_byte() {
        for shift in 9..=16 {
            let size = PageSize::new(1 << shift).unwrap();
            let lock = lock_page(size);
            let start = u64::from(lock - 1) * u64::from(size.get());
            assert!(start <= 0x4000_0000 && 0x4000_0000 < start + u64::from(size.get()));
        }
        assert_eq!(lock_page(PageSize::new(4096).unwrap()), 262_145);
        assert_eq!(lock_page(PageSize::MAX), 16_385);
    }

    #[test]
    fn page_number_rejects_zero_and_the_lock_page_at_every_size() {
        for shift in 9..=16 {
            let size = PageSize::new(1 << shift).unwrap();
            let lock = lock_page(size);
            assert_eq!(PageNo::new(0, size), None);
            assert_eq!(PageNo::new(lock, size), None);
            assert_eq!(PageNo::new(lock - 1, size).map(PageNo::get), Some(lock - 1));
            assert_eq!(PageNo::new(lock + 1, size).map(PageNo::get), Some(lock + 1));
        }
    }

    #[test]
    fn a_page_ref_is_exactly_one_page_of_its_size() {
        let small = PageSize::MIN;
        let big = PageSize::new(4096).unwrap();
        let pgno = PageNo::new(3, big).unwrap();
        for len in [0, 511, 513, 4095, 4097] {
            assert_eq!(PageRef::new(pgno, big, &vec![0; len]), None, "{len}");
        }
        let data = vec![7; 4096];
        let page = PageRef::new(pgno, big, &data).unwrap();
        assert_eq!(
            (page.pgno(), page.size(), page.data()),
            (pgno, big, &data[..])
        );
        assert_eq!(page.offset(), 2 * 4096);
        assert_eq!(page.checksum(), PageChecksum::of(pgno, &data));
        // A page number checked at one size can be another size's lock page.
        let lock = PageNo::new(lock_page(small), big).unwrap();
        assert_eq!(PageRef::new(lock, small, &[0; 512]), None);
        let top = PageNo::new(u32::MAX, PageSize::MAX).unwrap();
        let max = vec![0; 65536];
        let last = PageRef::new(top, PageSize::MAX, &max).unwrap();
        assert_eq!(last.offset(), u64::from(u32::MAX - 1) * 65536);
    }

    #[test]
    fn the_last_page_of_a_database_skips_the_lock_page() {
        for shift in 9..=16 {
            let size = PageSize::new(1 << shift).unwrap();
            let lock = lock_page(size);
            let last = |n: u32| DbPages::new(n).last_page(size).map(PageNo::get);
            assert_eq!(last(0), None);
            assert_eq!(last(1), Some(1));
            assert_eq!(last(lock - 1), Some(lock - 1));
            assert_eq!(last(lock), Some(lock - 1));
            assert_eq!(last(lock + 1), Some(lock + 1));
        }
    }

    #[test]
    fn a_database_holds_the_pages_up_to_its_length() {
        let size = PageSize::new(4096).unwrap();
        let pages = DbPages::new(5);
        assert!(pages.contains(PageNo::new(1, size).unwrap()));
        assert!(pages.contains(PageNo::new(5, size).unwrap()));
        assert!(!pages.contains(PageNo::new(6, size).unwrap()));
        assert!(!DbPages::EMPTY.contains(PageNo::new(1, size).unwrap()));
        assert_eq!(pages.bytes(size), 5 * 4096);
        assert_eq!(
            DbPages::new(u32::MAX).bytes(PageSize::MAX),
            u64::from(u32::MAX) * 65536
        );
        assert_eq!((pages.get(), pages.to_string()), (5, "5".to_owned()));
    }
}

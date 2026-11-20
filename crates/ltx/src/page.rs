//! Page sizes and page numbers.
//!
//! SQLite never writes the page that holds byte offset 2^30 (the "pending
//! byte" its file locks use), so that page, the lock page, never appears in an
//! LTX file. Which page it is depends on the page size.

use std::fmt;
use std::num::NonZeroU32;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}

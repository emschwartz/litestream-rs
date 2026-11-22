//! Content addresses: a node of the history is named by what the database holds, never by
//! anything the engine wrote about it.

use std::fmt;

use sha2::{Digest as _, Sha256};

/// A SHA-256 of a database state.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    /// The chain workload's state before its first row.
    #[must_use]
    pub fn genesis() -> Self {
        Self(Sha256::digest(b"lsr-history chain genesis").into())
    }

    /// The chain workload's row `k`: it certifies `prev`, the row before it, and its own payload.
    #[must_use]
    pub fn link(k: u64, prev: &Self, payload: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update(k.to_be_bytes());
        hash.update(prev.0);
        hash.update(payload);
        Self(hash.finalize().into())
    }

    /// A database image, page by page: the page size, the page count, then every page's bytes.
    /// Two images are equal exactly when their digests are, short of a SHA-256 collision.
    #[must_use]
    pub fn of_pages<'a>(page_size: u32, pages: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut hash = Sha256::new();
        hash.update(page_size.to_be_bytes());
        let mut count = 0_u64;
        for page in pages {
            hash.update(page);
            count = count.saturating_add(1);
        }
        hash.update(count.to_be_bytes());
        Self(hash.finalize().into())
    }

    /// The raw bytes.
    #[must_use]
    pub const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// From raw bytes, as a chain row stores them; `None` unless exactly 32.
    #[must_use]
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        <[u8; 32]>::try_from(bytes).ok().map(Self)
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Digest {
    /// The first eight bytes in hex: enough to tell states apart in a message.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.iter().take(8) {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

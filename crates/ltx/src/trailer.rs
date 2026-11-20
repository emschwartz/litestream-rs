//! The 16-byte LTX trailer: post-apply checksum, then file checksum, big-endian.
//!
//! Go's decoder never validates the trailer, but both Go encoders this crate
//! reads (ltx v0.5.1 and v0.5.3) run `Trailer.Validate` and the commit-0 rule
//! before writing one, so no Go-written file breaks these rules and a file that
//! does is corrupt: an untracked file has a zero post-apply checksum, a tracked
//! one a flagged checksum, `Checksum::EMPTY` when the database is empty.

use thiserror::Error;

use crate::Header;
use crate::checksum::Checksum;
use crate::conv::Cursor;

/// Bytes in an encoded trailer.
pub const TRAILER_LEN: usize = 16;

/// Why a trailer failed validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum TrailerError {
    /// The post-apply checksum does not fit the header: present on an
    /// untracked file, absent or unflagged on a tracked one, or not the empty
    /// checksum on a tracked file with commit 0.
    #[error("invalid post-apply checksum {raw:#x} for this header")]
    PostApply {
        /// The post-apply field.
        raw: u64,
    },
    /// The file checksum lacks the checksum flag.
    #[error("invalid file checksum {raw:#x}")]
    FileChecksum {
        /// The file checksum field.
        raw: u64,
    },
}

/// A validated LTX trailer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trailer {
    post_apply: Option<Checksum>,
    file_checksum: Checksum,
}

impl Trailer {
    /// The database checksum after this file applies, when tracked.
    #[must_use]
    pub const fn post_apply(&self) -> Option<Checksum> {
        self.post_apply
    }

    /// The CRC-64 over the file's contents.
    #[must_use]
    pub const fn file_checksum(&self) -> Checksum {
        self.file_checksum
    }

    /// Decodes and validates a trailer against its header.
    ///
    /// # Errors
    ///
    /// The first [`TrailerError`] the bytes violate.
    pub fn decode(bytes: &[u8; TRAILER_LEN], header: &Header) -> Result<Self, TrailerError> {
        let mut cursor = Cursor::new(bytes);
        let post_apply = match cursor.u64() {
            0 => None,
            raw => Some(Checksum::from_raw(raw).ok_or(TrailerError::PostApply { raw })?),
        };
        check_post_apply(header, post_apply)?;
        let raw = cursor.u64();
        let file_checksum = Checksum::from_raw(raw).ok_or(TrailerError::FileChecksum { raw })?;
        Ok(Self {
            post_apply,
            file_checksum,
        })
    }

    pub(crate) const fn new(post_apply: Option<Checksum>, file_checksum: Checksum) -> Self {
        Self {
            post_apply,
            file_checksum,
        }
    }

    /// Encodes the trailer.
    #[must_use]
    pub fn encode(&self) -> [u8; TRAILER_LEN] {
        let mut out = [0; TRAILER_LEN];
        out[0..8].copy_from_slice(&self.post_apply.map_or(0, Checksum::get).to_be_bytes());
        out[8..16].copy_from_slice(&self.file_checksum.get().to_be_bytes());
        out
    }
}

/// The post-apply rules, shared by the decoder and the encoder.
pub(crate) fn check_post_apply(
    header: &Header,
    post_apply: Option<Checksum>,
) -> Result<(), TrailerError> {
    match (header.tracked(), post_apply) {
        (false, None) => Ok(()),
        (true, Some(sum)) if header.commit() != 0 || sum == Checksum::EMPTY => Ok(()),
        (false | true, other) => Err(TrailerError::PostApply {
            raw: other.map_or(0, Checksum::get),
        }),
    }
}

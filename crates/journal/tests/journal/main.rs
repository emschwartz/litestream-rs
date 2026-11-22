//! Integration tests for lsr-journal: one binary, one module per file.

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "record offsets within one 4096-byte slot"
)]
mod codec;
#[cfg(test)]
#[expect(
    clippy::unwrap_in_result,
    reason = "fixtures build floors from literal nonzero bounds"
)]
mod facts;
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "sequence numbers drawn below u64::MAX / 2"
)]
mod read;

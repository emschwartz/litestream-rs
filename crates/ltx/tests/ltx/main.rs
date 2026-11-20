//! Integration tests for lsr-ltx, one binary so the helpers are shared.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

// `cfg(test)` is always set here; it marks the helpers as test code, so
// clippy.toml's allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod common;
#[cfg(test)]
mod corruption;
#[cfg(test)]
mod errors;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod roundtrip;

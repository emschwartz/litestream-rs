//! Integration tests for lsr-spec: one binary, one module per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod manifest;
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the sweep does arithmetic on tiny bounded transaction ids and indexes"
)]
mod restore_plan;

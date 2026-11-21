//! Integration tests for lsr-core: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod plan_adversarial;
#[cfg(test)]
mod plan_lazy;
#[cfg(test)]
mod secret_shapes;
#[cfg(test)]
mod stamp_sources;

//! Integration tests for lsr-core: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod created_at_sources;
#[cfg(test)]
mod gates;
#[cfg(test)]
mod plan_adversarial;
#[cfg(test)]
mod plan_exhaustive;
#[cfg(test)]
mod plan_lazy;
#[cfg(test)]
mod secret_shapes;

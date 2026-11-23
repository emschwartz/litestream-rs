//! Integration tests for lsr-core: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod created_at_sources;
#[cfg(test)]
mod gates;
#[cfg(test)]
mod heap;
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    clippy::wildcard_enum_match_arm,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::large_enum_variant,
    reason = "the differential walk: slot offsets of at most 8192, a fake file that panics on a \
              call the model did not make, catch-all arms over the model's labels that panic, and \
              pages held by value in the walk's frames"
)]
mod journal_model;
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the fixtures size databases and inventories from small bounded counts"
)]
mod mem;
#[cfg(test)]
mod mem_big;
#[cfg(test)]
mod plan_adversarial;
#[cfg(test)]
mod plan_exhaustive;
#[cfg(test)]
mod plan_lazy;
#[cfg(test)]
mod secret_shapes;
#[cfg(test)]
mod skip_guard;
#[cfg(test)]
mod walk;

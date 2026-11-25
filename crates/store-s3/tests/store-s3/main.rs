//! Integration tests for lsr-store-s3: one binary, one module per former test file, so the
//! crate's tests link once instead of once per file.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod bare_bucket;
#[cfg(test)]
mod common;
#[cfg(test)]
mod fake;
#[cfg(test)]
mod faults;
#[cfg(test)]
mod gates;
#[cfg(test)]
mod latency;
#[cfg(test)]
mod limits;
#[cfg(test)]
mod memory;
#[cfg(test)]
mod minio;
#[cfg(test)]
mod part_size;
#[cfg(test)]
mod provider;
#[cfg(test)]
#[path = "common/real.rs"]
mod real;
#[cfg(test)]
mod request_counts;
#[cfg(test)]
#[path = "common/rig.rs"]
mod rig;
#[cfg(test)]
mod store;
#[cfg(test)]
mod tigris;
#[cfg(test)]
#[path = "common/wire.rs"]
mod wire;

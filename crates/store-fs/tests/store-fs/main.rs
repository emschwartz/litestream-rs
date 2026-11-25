//! Integration tests for lsr-store-fs: one binary, one module per file, so the crate's tests
//! link once instead of once per file.

#[cfg(test)]
mod memory;
#[cfg(test)]
mod opens;
#[cfg(test)]
mod review;

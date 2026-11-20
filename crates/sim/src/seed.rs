//! The seed every simulator run replays from, and the line that replays it.

use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// One run's seed: the whole history, every fault and every crash point follow
/// from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seed(u64);

/// Text that is not a seed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("not a seed: expected up to 16 hex digits, optionally after 0x, got {0:?}")]
pub struct ParseSeedError(String);

impl Seed {
    /// The seed with this raw value.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// An independent stream for one purpose (the wrapper schedule, the crash
    /// points, the page bytes), so adding a draw to one never shifts another's.
    #[must_use]
    pub fn stream(self, purpose: u64) -> u64 {
        let mixed = (self.0 ^ purpose.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for Seed {
    type Err = ParseSeedError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = text.strip_prefix("0x").unwrap_or(text);
        if digits.is_empty() || digits.len() > 16 {
            return Err(ParseSeedError(text.to_owned()));
        }
        u64::from_str_radix(digits, 16)
            .map(Self)
            .map_err(|_err| ParseSeedError(text.to_owned()))
    }
}

/// The command that replays a run: `just sim-replay 0x<seed> [steps]`.
#[must_use]
pub fn replay_line(seed: Seed, steps: Option<u32>) -> String {
    match steps {
        Some(steps) => format!("just sim-replay 0x{seed} {steps}"),
        None => format!("just sim-replay 0x{seed}"),
    }
}

#[cfg(test)]
mod tests;

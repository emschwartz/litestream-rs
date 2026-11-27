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

/// How many seeds one night of the rotating set owns.
pub const SEEDS_PER_NIGHT: u64 = 1_000_000;

/// Text that is not a UTC date written `YYYYMMDD`.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("not a date: expected YYYYMMDD, got {0:?}")]
pub struct ParseDayError(String);

/// The first seed of the nightly rotating set for a UTC date written
/// `YYYYMMDD`: the date as a number times [`SEEDS_PER_NIGHT`]. No two nights
/// share a seed, and a night is run again from its date alone.
///
/// # Errors
/// [`ParseDayError`] unless `day` is eight digits with a month of 1 to 12 and
/// a day of 1 to 31 (the calendar is not checked further: the date only names
/// a range).
pub fn nightly_base(day: &str) -> Result<u64, ParseDayError> {
    let bad = || ParseDayError(day.to_owned());
    if day.len() != 8 || !day.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let number: u64 = day.parse().map_err(|_err| bad())?;
    let (year, month, date) = (number / 10_000, number / 100 % 100, number % 100);
    if year == 0 || !(1..=12).contains(&month) || !(1..=31).contains(&date) {
        return Err(bad());
    }
    number.checked_mul(SEEDS_PER_NIGHT).ok_or_else(bad)
}

/// The first seed of the nightly's large-profile set (`docs/decisions/D277.md`) for a UTC date:
/// the upper half of the night's range, so it never meets the rotating set, which counts up from
/// [`nightly_base`] and stays below this by far (a night runs thousands of seeds, not
/// [`SEEDS_PER_NIGHT`] / 2).
///
/// # Errors
/// [`ParseDayError`] as [`nightly_base`].
pub fn long_base(day: &str) -> Result<u64, ParseDayError> {
    let base = nightly_base(day)?;
    base.checked_add(SEEDS_PER_NIGHT / 2)
        .ok_or_else(|| ParseDayError(day.to_owned()))
}

/// Text that is not a shard, `INDEX/COUNT` with `INDEX < COUNT`.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("not a shard: expected INDEX/COUNT with INDEX < COUNT, got {0:?}")]
pub struct ParseShardError(String);

/// One of `count` parallel jobs that split a seed range between them: shard `index` runs the
/// seeds at offsets `index`, `index + count`, `index + 2 * count` and so on from the base, so
/// no two shards run a seed and together they run every one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shard {
    index: u64,
    count: u64,
}

impl Shard {
    /// Parses `INDEX/COUNT`, as `LSR_SIM_SHARD` holds it.
    ///
    /// # Errors
    /// [`ParseShardError`] unless both are numbers, the count is not 0 and the index is below it.
    pub fn parse(text: &str) -> Result<Self, ParseShardError> {
        let bad = || ParseShardError(text.to_owned());
        let (index, count) = text.split_once('/').ok_or_else(bad)?;
        let (index, count): (u64, u64) = (
            index.parse().map_err(|_err| bad())?,
            count.parse().map_err(|_err| bad())?,
        );
        if index >= count {
            return Err(bad());
        }
        Ok(Self { index, count })
    }

    /// The seeds this shard runs from `base`, in order, without end.
    pub fn seeds(self, base: u64) -> impl Iterator<Item = Seed> {
        (0..)
            .map_while(move |k: u64| k.checked_mul(self.count)?.checked_add(self.index))
            .map_while(move |offset| base.checked_add(offset))
            .map(Seed::new)
    }
}

#[cfg(test)]
mod tests;

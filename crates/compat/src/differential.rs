//! Generated differentials against Go, run the one way that cannot flake on its own draw.
//!
//! A differential is generated cases given to the pinned Go build and to litestream-rs,
//! with a tally of how each case ended, and floors on the tally: a run that never reached
//! an ending shows nothing about it. Two things made such runs flake: an unseeded runner
//! draws different cases every run, and an ending reached only by chance falls under its
//! floor now and then. So every run here has a fixed seed, derived from the differential's
//! name and the part, so a run is the same cases every time and a new seed is an edit; and
//! an ending a floor names is reached by an aimed part, cases built to end that way and
//! required to, so the floor holds whatever the free part draws.
//!
//! Every compat differential runs through [`Differential`]; the `seeded` test in the compat
//! suite refuses a `TestRunner` built anywhere else and a `ProptestConfig` without a fixed
//! seed.

use std::collections::BTreeMap;
use std::sync::Mutex;

use proptest::strategy::Strategy;
use proptest::test_runner::{Config, RngSeed, TestCaseError, TestRunner};

/// Why a differential failed.
#[derive(Debug, thiserror::Error)]
pub enum DifferentialError {
    /// A case disagreed, or an aimed case ended otherwise; the message is proptest's, with
    /// the shrunk case.
    #[error("{name}, part {part}: {message}")]
    Failed {
        /// The differential.
        name: &'static str,
        /// `free`, or the ending an aimed part aims at.
        part: String,
        /// What proptest reported.
        message: String,
    },
    /// An ending reached fewer times than its floor.
    #[error("{name}: {ending} ended only {seen} cases, fewer than {floor}: {tally:?}")]
    Underreached {
        /// The differential.
        name: &'static str,
        /// The ending.
        ending: String,
        /// How often it was reached.
        seen: u32,
        /// The floor.
        floor: u32,
        /// Every ending's count.
        tally: BTreeMap<String, u32>,
    },
}

/// One differential's runs and the tally of their endings.
#[derive(Debug)]
pub struct Differential {
    name: &'static str,
    tally: Mutex<BTreeMap<String, u32>>,
}

impl Differential {
    /// A differential named `name`; the name seeds its runs.
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            tally: Mutex::new(BTreeMap::new()),
        }
    }

    /// `cases` free cases from `strategy`; `test` compares one and names its ending.
    ///
    /// # Errors
    /// [`DifferentialError::Failed`] with the shrunk case that failed.
    pub fn free<S: Strategy>(
        &self,
        cases: u32,
        strategy: &S,
        test: impl Fn(S::Value) -> Result<String, TestCaseError>,
    ) -> Result<(), DifferentialError> {
        self.part("free", cases, strategy, |value| {
            let ending = test(value)?;
            self.count(ending);
            Ok(())
        })
    }

    /// `cases` cases from `strategy`, each built to end as `ending`; `test` compares one and
    /// names its ending, which must be `ending`.
    ///
    /// # Errors
    /// [`DifferentialError::Failed`] with the shrunk case that failed or ended otherwise.
    pub fn aimed<S: Strategy>(
        &self,
        ending: &str,
        cases: u32,
        strategy: &S,
        test: impl Fn(S::Value) -> Result<String, TestCaseError>,
    ) -> Result<(), DifferentialError> {
        self.part(ending, cases, strategy, |value| {
            let ended = test(value)?;
            if ended != ending {
                return Err(TestCaseError::fail(format!(
                    "a case aimed at {ending} ended as {ended}"
                )));
            }
            self.count(ended);
            Ok(())
        })
    }

    /// The tally, after checking every ending in `endings` was reached at least `floor`
    /// times.
    ///
    /// # Errors
    /// [`DifferentialError::Underreached`] for the first ending under its floor.
    pub fn require(
        self,
        endings: &[&str],
        floor: u32,
    ) -> Result<BTreeMap<String, u32>, DifferentialError> {
        let tally = self
            .tally
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for ending in endings {
            let seen = tally.get(*ending).copied().unwrap_or(0);
            if seen < floor {
                return Err(DifferentialError::Underreached {
                    name: self.name,
                    ending: (*ending).to_owned(),
                    seen,
                    floor,
                    tally,
                });
            }
        }
        Ok(tally)
    }

    fn count(&self, ending: String) {
        let mut tally = self
            .tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = tally.entry(ending).or_default();
        *count = count.saturating_add(1);
    }

    fn part<S: Strategy>(
        &self,
        part: &str,
        cases: u32,
        strategy: &S,
        test: impl Fn(S::Value) -> Result<(), TestCaseError>,
    ) -> Result<(), DifferentialError> {
        let mut runner = TestRunner::new(Config {
            cases,
            failure_persistence: None,
            rng_seed: RngSeed::Fixed(seed(self.name, part)),
            ..Config::default()
        });
        runner
            .run(strategy, test)
            .map_err(|error| DifferentialError::Failed {
                name: self.name,
                part: part.to_owned(),
                message: error.to_string(),
            })
    }
}

/// FNV-1a over the name and the part: a fixed seed per run that no edit elsewhere moves.
fn seed(name: &str, part: &str) -> u64 {
    name.bytes()
        .chain(std::iter::once(0))
        .chain(part.bytes())
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn seeds_differ_by_name_and_part_and_stay_put() {
        assert_ne!(seed("a", "free"), seed("b", "free"));
        assert_ne!(seed("a", "free"), seed("a", "Usage"));
        assert_ne!(seed("ab", "c"), seed("a", "bc"));
        assert_eq!(seed("a", "free"), seed("a", "free"));
    }

    #[test]
    fn a_run_draws_the_same_cases_every_time() {
        let draws = || {
            let seen = Mutex::new(Vec::new());
            let run = Differential::new("same");
            run.free(16, &(0_u64..u64::MAX), |n| {
                seen.lock().unwrap().push(n);
                Ok("drawn".to_owned())
            })
            .unwrap();
            seen.into_inner().unwrap()
        };
        assert_eq!(draws(), draws());
    }

    #[test]
    fn an_aimed_case_that_ends_otherwise_fails() {
        let run = Differential::new("aimed");
        let failed = run.aimed("even", 8, &(0_u32..100), |n| {
            Ok(if n % 2 == 0 { "even" } else { "odd" }.to_owned())
        });
        assert!(
            matches!(failed, Err(DifferentialError::Failed { .. })),
            "{failed:?}"
        );
    }

    #[test]
    fn a_floor_counts_every_part() {
        let run = Differential::new("floors");
        run.free(8, &Just(()), |()| Ok("free".to_owned())).unwrap();
        run.aimed("aimed", 3, &Just(()), |()| Ok("aimed".to_owned()))
            .unwrap();
        let tally = run.require(&["free", "aimed"], 3).unwrap();
        assert_eq!(tally.get("free"), Some(&8));
        let run = Differential::new("short");
        run.aimed("aimed", 2, &Just(()), |()| Ok("aimed".to_owned()))
            .unwrap();
        assert!(matches!(
            run.require(&["aimed"], 3),
            Err(DifferentialError::Underreached { seen: 2, .. })
        ));
    }
}

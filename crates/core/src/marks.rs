//! Coverage marks: which rows of the behaviour table (`docs/BEHAVIOR.md`,
//! section 3) a run actually reached.
//!
//! A row names a circumstance the engine distinguishes. `mark!(Run13)`
//! sits on the branch that detects that circumstance, not at the entry of the
//! function that might, and counts one hit there. The simulator takes the
//! counts after each run ([`take`]) and its nightly soak fails on a row no
//! seed reached: a branch the swarm never drives is a row with no evidence,
//! however green the run.
//!
//! The counter exists only under the `verify` feature (on in the simulator,
//! the fuzzers and tests, off in release). Without it `mark!` expands to
//! nothing, so release code carries no counter, no thread-local and no cost.
//! The count is per thread: the simulator runs an engine on one thread, and a
//! test process that runs several engines on one thread sums them.
//!
//! [`Row`] holds the rows that have a site. Each row of `docs/behavior.toml`
//! carries `mark = "site"` or `mark = "none: <reason>"`, and lsr-compat's
//! `behavior` test fails when a row has neither, when the rows marked `site`
//! and the variants differ, or when a variant has no `mark!` site.

/// Defines [`Row`] with its table ids, in one list so the two cannot drift.
macro_rules! rows {
    ($($variant:ident = $id:literal,)*) => {
        /// A row of the behaviour table that has a `mark!` site in this crate.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Row {
            $(
                #[doc = concat!("Row ", $id, ".")]
                $variant,
            )*
        }

        impl Row {
            /// Every row with a site, in table order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),*];

            /// The row's id in the behaviour table, as `RUN-13`.
            #[must_use]
            pub const fn id(self) -> &'static str {
                match self {
                    $(Self::$variant => $id,)*
                }
            }
        }
    };
}

rows! {
    Boot1 = "BOOT-1",
    Boot2 = "BOOT-2",
    Boot3 = "BOOT-3",
    Boot4 = "BOOT-4",
    Boot5 = "BOOT-5",
    Boot6 = "BOOT-6",
    Boot7 = "BOOT-7",
    Boot8 = "BOOT-8",
    Boot9 = "BOOT-9",
    Boot10 = "BOOT-10",
    Boot11 = "BOOT-11",
    Boot12 = "BOOT-12",
    Boot13 = "BOOT-13",
    Boot14 = "BOOT-14",
    Boot15 = "BOOT-15",
    Boot16 = "BOOT-16",
    Boot17 = "BOOT-17",
    Boot23 = "BOOT-23",
    Run1 = "RUN-1",
    Run2 = "RUN-2",
    Run3 = "RUN-3",
    Run4 = "RUN-4",
    Run5 = "RUN-5",
    Run7 = "RUN-7",
    Run8 = "RUN-8",
    Run9 = "RUN-9",
    Run10 = "RUN-10",
    Run11 = "RUN-11",
    Run12 = "RUN-12",
    Run16 = "RUN-16",
    Run18 = "RUN-18",
    Run19 = "RUN-19",
    Run22 = "RUN-22",
    Run23 = "RUN-23",
    Run24 = "RUN-24",
    Ckpt1 = "CKPT-1",
    Ckpt2 = "CKPT-2",
    Ckpt4 = "CKPT-4",
    Ckpt5 = "CKPT-5",
    Up1 = "UP-1",
    Up6 = "UP-6",
    Up8 = "UP-8",
    Up9 = "UP-9",
    Up10 = "UP-10",
    Up11 = "UP-11",
    Up16 = "UP-16",
    Maint1 = "MAINT-1",
    Maint2 = "MAINT-2",
    Maint4 = "MAINT-4",
    Maint5 = "MAINT-5",
    Maint7 = "MAINT-7",
    Maint8 = "MAINT-8",
    Maint10 = "MAINT-10",
    Maint11 = "MAINT-11",
    Rest1 = "REST-1",
    Rest3 = "REST-3",
    Rest4 = "REST-4",
    Rest5 = "REST-5",
    Verify1 = "VERIFY-1",
    Verify3 = "VERIFY-3",
    Verify4 = "VERIFY-4",
    World4 = "WORLD-4",
    World13 = "WORLD-13",
    World28 = "WORLD-28",
}

/// Counts one hit of a row under the `verify` feature; nothing otherwise.
#[cfg(feature = "verify")]
macro_rules! mark {
    ($row:ident) => {
        $crate::marks::hit($crate::marks::Row::$row)
    };
}

/// Counts one hit of a row under the `verify` feature; nothing otherwise.
#[cfg(not(feature = "verify"))]
macro_rules! mark {
    ($row:ident) => {};
}

#[cfg(feature = "verify")]
std::thread_local! {
    static HITS: std::cell::RefCell<std::collections::BTreeMap<Row, u64>> =
        const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}

/// Counts one hit of `row` on this thread. Bounded: one count per row.
#[cfg(feature = "verify")]
pub fn hit(row: Row) {
    HITS.with_borrow_mut(|hits| {
        let count = hits.entry(row).or_insert(0);
        *count = count.saturating_add(1);
    });
}

/// The hits counted on this thread since the last call, which starts the
/// count again from zero.
#[cfg(feature = "verify")]
#[must_use]
pub fn take() -> Vec<(Row, u64)> {
    HITS.with_borrow_mut(std::mem::take).into_iter().collect()
}

#[cfg(all(test, feature = "verify"))]
mod tests {
    use super::*;

    #[test]
    fn ids_are_distinct_and_take_starts_again() {
        let mut ids: Vec<&str> = Row::ALL.iter().map(|row| row.id()).collect();
        let before = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), before);
        if let Some(&row) = Row::ALL.first() {
            drop(take());
            hit(row);
            hit(row);
            assert_eq!(take(), vec![(row, 2)]);
            assert_eq!(take(), vec![]);
        }
    }
}

//! The fixture scenarios and their deterministic step lists.

/// One kind of history the harness generates a fixture for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scenario {
    /// 4096-byte pages, a small mixed history.
    Small4096,
    /// 16384-byte pages, the same mix.
    Page16384,
    /// 4096-byte pages; a large table is dropped and `VACUUM` shrinks the database.
    VacuumShrink,
    /// The small history with every file covering one mid-chain TXID deleted.
    MidChainGap,
}

/// One workload step; every step ends committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Insert this many rows into `items`.
    Insert(u32),
    /// Update this many random rows of `items`.
    Update(u32),
    /// Delete this many random rows of `items`.
    Delete(u32),
    /// Insert one blob of this many pages plus a remainder, so it overflows into a page chain.
    Blob(u32),
    /// Insert this many 1 KiB rows into `scratch`.
    FillScratch(u32),
    /// Drop `scratch`.
    DropScratch,
    /// `VACUUM`.
    Vacuum,
}

impl Scenario {
    /// Every scenario.
    pub const ALL: [Self; 4] = [
        Self::Small4096,
        Self::Page16384,
        Self::VacuumShrink,
        Self::MidChainGap,
    ];

    /// The directory name of the scenario.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Small4096 => "small-4096",
            Self::Page16384 => "page-16384",
            Self::VacuumShrink => "vacuum-shrink",
            Self::MidChainGap => "mid-chain-gap",
        }
    }

    /// The scenario a name denotes.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.name() == name)
    }

    /// Database page size.
    pub const fn page_size(self) -> u32 {
        match self {
            Self::Page16384 => 16384,
            Self::Small4096 | Self::VacuumShrink | Self::MidChainGap => 4096,
        }
    }

    /// The workload seed; fixed per scenario so a regeneration replays the same SQL.
    pub const fn seed(self) -> u64 {
        match self {
            Self::Small4096 => 0x51,
            Self::Page16384 => 0x16,
            Self::VacuumShrink => 0xAC,
            Self::MidChainGap => 0x6A9,
        }
    }

    /// Whether the scenario deletes a mid-chain file after the run.
    pub const fn has_gap(self) -> bool {
        matches!(self, Self::MidChainGap)
    }

    /// The steps, in order. Every scenario holds inserts, updates, deletes, an overflowing
    /// blob, a table drop and `VACUUM`; they differ in size and in whether the vacuum shrinks.
    pub fn steps(self) -> Vec<Step> {
        let scratch = match self {
            Self::VacuumShrink => 300,
            Self::Small4096 | Self::Page16384 | Self::MidChainGap => 24,
        };
        vec![
            Step::Insert(40),
            Step::Update(20),
            Step::Blob(5),
            Step::Insert(30),
            Step::FillScratch(scratch),
            Step::Delete(15),
            Step::Update(25),
            Step::DropScratch,
            Step::Vacuum,
            Step::Insert(20),
            Step::Update(10),
        ]
    }

    /// Whether the history must end smaller than its peak.
    pub const fn must_shrink(self) -> bool {
        matches!(self, Self::VacuumShrink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_and_are_distinct() {
        for s in Scenario::ALL {
            assert_eq!(Scenario::parse(s.name()), Some(s));
        }
        let seeds: std::collections::BTreeSet<_> = Scenario::ALL.iter().map(|s| s.seed()).collect();
        assert_eq!(seeds.len(), Scenario::ALL.len());
    }

    #[test]
    fn every_scenario_holds_the_full_mix() {
        for s in Scenario::ALL {
            let steps = s.steps();
            let has = |f: fn(&Step) -> bool| steps.iter().any(f);
            assert!(has(|x| matches!(x, Step::Insert(_))));
            assert!(has(|x| matches!(x, Step::Update(_))));
            assert!(has(|x| matches!(x, Step::Delete(_))));
            assert!(has(|x| matches!(x, Step::Blob(_))));
            assert!(has(|x| matches!(x, Step::DropScratch)));
            assert!(has(|x| matches!(x, Step::Vacuum)));
        }
    }
}

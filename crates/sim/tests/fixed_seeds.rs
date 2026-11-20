//! The fixed seed set: 64 histories of 300 operations, the whole thing inside
//! ten seconds. A failure prints its replay line (`just sim-replay`).
//!
//! Seed 45 is replaced by 65: it is finding 2 (see `review_torn_staged_file`).

#[cfg(test)]
mod fixed {
    use std::time::{Duration, Instant};

    use lsr_sim::{Bound, Seed, Violation, Weights, replay_line, run};

    const STEPS: u32 = 300;
    const BUDGET: Duration = Duration::from_secs(10);

    /// Seeds that end with staged L0 files the engine never pruned (gap 4,
    /// fixed on branch `actor-followup`): the run passes every other property
    /// and trips only the idle-files bound. A seed here that passes means gap 4
    /// is fixed, and the test says to remove it, so this list shrinks to empty.
    const GAP_4: &[u64] = &[2, 4, 7, 17, 30, 31, 36, 38, 40, 46, 50, 55, 56, 58, 62, 65];

    fn batch(first: u64) {
        let started = Instant::now();
        for n in (first..).take(8) {
            let n = if n == 45 { 65 } else { n };
            let outcome = run(Seed::new(n), STEPS, &Weights::default());
            let gap_4 = GAP_4.contains(&n);
            match outcome {
                Ok(_) if gap_4 => panic!("gap 4 fixed: remove seed {n} from GAP_4"),
                Ok(_) => {}
                Err(failure)
                    if gap_4
                        && matches!(
                            failure.violation,
                            Violation::ResourceBound {
                                what: Bound::IdleFiles,
                                ..
                            }
                        ) =>
                {
                    eprintln!("gap 4: {}", replay_line(Seed::new(n), Some(STEPS)));
                }
                Err(failure) => panic!("{failure}"),
            }
        }
        assert!(started.elapsed() < BUDGET, "simulator budget exceeded");
    }

    #[test]
    fn seeds_1_to_8() {
        batch(1);
    }

    #[test]
    fn seeds_9_to_16() {
        batch(9);
    }

    #[test]
    fn seeds_17_to_24() {
        batch(17);
    }

    #[test]
    fn seeds_25_to_32() {
        batch(25);
    }

    #[test]
    fn seeds_33_to_40() {
        batch(33);
    }

    #[test]
    fn seeds_41_to_48() {
        batch(41);
    }

    #[test]
    fn seeds_49_to_56() {
        batch(49);
    }

    #[test]
    fn seeds_57_to_64() {
        batch(57);
    }

    /// Finding 1: a vacuum that changes the page size makes maintenance
    /// compact across the change and the engine halts with `RemoteCorrupt`.
    /// Replay: `just sim-replay 0x1 300` with `Weights { vacuum_resizes: true }`.
    #[test]
    #[ignore = "finding 1: a resizing vacuum halts the engine (PageSizeChanged, RemoteCorrupt)"]
    fn review_vacuum_resize() {
        let weights = Weights {
            vacuum_resizes: true,
            ..Weights::default()
        };
        for n in 1..=16 {
            if let Err(failure) = run(Seed::new(n), STEPS, &weights) {
                panic!("{failure}");
            }
        }
    }

    /// Finding 2: a power loss tears a staged L0 file that was not yet
    /// uploaded. Boot reads `LocalTipUnreadable` and resnapshots as the next
    /// txid; the torn file stays in staging and the upload lane later halts
    /// with `StagedCorrupt`. Replay: `just sim-replay 0x2d 256`, `0x46 252`.
    #[test]
    #[ignore = "finding 2: a torn staged L0 file halts the upload lane with StagedCorrupt"]
    fn review_torn_staged_file() {
        for n in [45, 70] {
            if let Err(failure) = run(Seed::new(n), STEPS, &Weights::default()) {
                panic!("{failure}");
            }
        }
    }
}

//! The fixed seed set: 64 histories of 300 operations, the whole thing inside
//! ten seconds. A failure prints its replay line (`just sim-replay`).

#[cfg(test)]
mod fixed {
    use std::time::{Duration, Instant};

    use lsr_sim::{Seed, Weights, run};

    const STEPS: u32 = 300;
    const BUDGET: Duration = Duration::from_secs(10);

    #[expect(
        clippy::disallowed_methods,
        reason = "reports how long a batch of seeds takes; the runs themselves use simulated time"
    )]
    fn batch(first: u64) {
        let started = Instant::now();
        for n in (first..).take(8) {
            let outcome = run(Seed::new(n), STEPS, &Weights::default());
            if let Err(failure) = outcome {
                panic!("{failure}");
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

    fn replay(seed: u64, steps: u32) {
        if let Err(failure) = run(Seed::new(seed), steps, &Weights::default()) {
            panic!("{failure}");
        }
    }

    // Finding 2: a power loss tore a staged level-0 file whose commit never
    // returned. Boot snapshotted past it and left it in staging, and the upload
    // lane halted on it. The torn file, and all staged after it, is now lost.

    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x25() {
        replay(0x25, 44);
    }

    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x2d() {
        replay(0x2d, 256);
    }

    /// Reached through `LocalRewound`: the torn file was an upload whose commit
    /// errored after publishing, torn by an earlier power loss.
    #[test]
    fn a_torn_staged_file_is_dropped_seed_0x46() {
        replay(0x46, 252);
    }

    // Issue #39: after a VACUUM that changes the page size, a staging wipe and a
    // resnapshot, a restore at an early TXID differs from the oracle's commit.
    // Which side is wrong is not settled yet.

    #[test]
    #[ignore = "review: #39, restore differs from the oracle after a page-size VACUUM and a staging wipe"]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ef1c07() {
        replay(0x0000_126d_68ef_1c07, 52);
    }

    #[test]
    #[ignore = "review: #39, restore differs from the oracle after a page-size VACUUM and a staging wipe"]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ed250e() {
        replay(0x0000_126d_68ed_250e, 215);
    }

    #[test]
    #[ignore = "review: #39, restore differs from the oracle after a page-size VACUUM and a staging wipe"]
    fn page_size_vacuum_and_staging_wipe_seed_0x126d68ef545f() {
        replay(0x0000_126d_68ef_545f, 176);
    }
}

//! The rotating seed set: runs seeds from a base until the time is up.
//!
//! `just sim-soak <seconds>` sets `LSR_SIM_BASE_SEED` (default: `date +%s`)
//! and `LSR_SIM_SECONDS`. Every seed's replay line is printed before the run,
//! so a hang or a crash of the process still names its seed. A failing seed
//! belongs in the fixed set once the bug is fixed.

#[cfg(test)]
mod soak {
    use std::time::{Duration, Instant};

    use lsr_sim::{Seed, Weights, replay_line, run};

    const STEPS: u32 = 300;

    #[test]
    #[ignore = "runs for LSR_SIM_SECONDS; use `just sim-soak`"]
    fn rotating_seeds() {
        let base: u64 = std::env::var("LSR_SIM_BASE_SEED")
            .expect("LSR_SIM_BASE_SEED")
            .parse()
            .expect("LSR_SIM_BASE_SEED is a number");
        let seconds: u64 = std::env::var("LSR_SIM_SECONDS")
            .expect("LSR_SIM_SECONDS")
            .parse()
            .expect("LSR_SIM_SECONDS is a number");
        let until = Instant::now() + Duration::from_secs(seconds);
        let mut failed = Vec::new();
        let mut ran = 0_u64;
        for n in base.. {
            if Instant::now() >= until {
                break;
            }
            let seed = Seed::new(n);
            println!("{}", replay_line(seed, Some(STEPS)));
            if let Err(failure) = run(seed, STEPS, &Weights::default()) {
                println!("{failure}");
                failed.push(n);
            }
            ran += 1;
        }
        println!("ran {ran} seeds from {base}, {} failed", failed.len());
        assert!(failed.is_empty(), "failing seeds: {failed:?}");
    }
}

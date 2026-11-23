//! The simulator's drawn histories with the engine on real SQLite ([`RealSide`]) instead of
//! the simulator's rule model: the same generator, oracle and history judge, with every commit,
//! checkpoint, reader, crash and power loss made on a real database through the fault VFS, and
//! the engine reading it through this crate's own `Sqlite`. A defect in this crate's doors, which
//! the model stands in for in `lsr-sim`'s own runs, is in reach here.

#[cfg(test)]
mod swarm {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use lsr_ltx::PageSize;
    use lsr_sim::domain::{KnownWrong, known_in};
    use lsr_sim::side::{SideMaker, SqliteSide};
    use lsr_sim::{Failure, Opts, Seed, Weights, history, replay_line, run_with};
    use lsr_sqlite::testing::RealSide;
    use tempfile::TempDir;

    /// Real backends, each in a directory of its own under `root`: the shrinker builds a new
    /// world for every history it tries.
    fn real(root: &Arc<TempDir>) -> SideMaker {
        let root = Arc::clone(root);
        let next = Arc::new(AtomicU32::new(0));
        SideMaker::new(Arc::new(
            move |seed: Seed, size: PageSize, pages: u32| -> Arc<dyn SqliteSide> {
                let n = next.fetch_add(1, Ordering::Relaxed);
                let dir = root.path().join(format!("{:x}-{n}", seed.get()));
                std::fs::create_dir(&dir).unwrap();
                let side = RealSide::create(&dir, size, pages, seed.get()).unwrap();
                #[expect(
                    clippy::arc_with_non_send_sync,
                    reason = "the simulator runs a world on one thread"
                )]
                Arc::new(side)
            },
        ))
    }

    fn run(
        root: &Arc<TempDir>,
        seed: Seed,
        steps: u32,
        weights: &Weights,
    ) -> Result<lsr_sim::RunReport, Box<Failure>> {
        let opts = Opts {
            sqlite: real(root),
            ..Opts::default()
        };
        run_with(seed, steps, weights, &opts).map_err(Box::new)
    }

    /// Swarm seeds pass with the engine on real SQLite, and they reach it: the WAL grew under
    /// real commits and the oracle judged restores of them.
    #[test]
    fn drawn_histories_pass_on_real_sqlite() {
        let (mut checks, mut frames) = (0, 0);
        for n in 1..=12 {
            let seed = Seed::new(n);
            let root = Arc::new(TempDir::new().unwrap());
            let report = run(&root, seed, 120, &Weights::swarm(seed))
                .unwrap_or_else(|failure| panic!("{failure}\n{}", replay_line(seed, None)));
            checks += report.checks;
            frames = frames.max(report.max_wal_frames);
        }
        assert!(
            checks > 0 && frames > 0,
            "{checks} checks, at most {frames} WAL frames"
        );
    }

    /// Seeds to the first failure with the engine on real SQLite, as `lsr-sim`'s
    /// `unaided::seeds_to_first_failure` counts them on the model: the mixed weights, 300 steps,
    /// `LSR_SIM_BASE_SEED` and `LSR_SIM_SEEDS`, every failing seed with `LSR_SIM_ALL`. A failure
    /// whose signature matches an open defect its history drew is counted as known.
    #[test]
    #[ignore = "runs LSR_SIM_SEEDS seeds on real SQLite"]
    fn real_seeds_to_first_failure() {
        let env = |name: &str, default: u64| {
            std::env::var(name).map_or(default, |v| v.parse().expect("a number"))
        };
        let (base, seeds) = (env("LSR_SIM_BASE_SEED", 1), env("LSR_SIM_SEEDS", 3000));
        let all = std::env::var("LSR_SIM_ALL").is_ok();
        let mut failed = Vec::new();
        for n in base..base.saturating_add(seeds) {
            let seed = Seed::new(n);
            let weights = Weights::mixed(seed);
            let root = Arc::new(TempDir::new().unwrap());
            let Err(failure) = run(&root, seed, 300, &weights) else {
                continue;
            };
            let drawn = known_in(&history(seed, 300, &weights), &weights);
            if let Some(defect) = KnownWrong::of(&failure.violation, &drawn) {
                println!("KNOWN {defect:?}: {failure}");
                continue;
            }
            println!("FAILED after {} seeds: {failure}", n - base + 1);
            println!("{}", replay_line(seed, Some(failure.steps)));
            failed.push(n);
            if !all {
                break;
            }
        }
        println!(
            "real: {} failed of seeds from {base}: {failed:?}",
            failed.len()
        );
    }
}

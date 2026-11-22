//! Histories in which a bucket copy drops some files' stored created-at
//! times (`RemoteOp::DropCreatedAt`), so the store reports their upload time
//! instead. The `AsOf` oracle must judge such a time as bounding the commit
//! from above only, as the planner must: it never dates the next transaction.

#[cfg(test)]
mod dropped {
    use lsr_sim::{Seed, Weights, run};

    #[test]
    fn as_of_never_takes_an_upload_time_for_a_commit_time() {
        let weights = Weights {
            drop_created_at: 4,
            ..Weights::default()
        };
        let mut fell_back = 0_u64;
        for n in 1..=16 {
            let seed = Seed::new(n);
            match run(seed, 300, &weights) {
                Ok(report) => fell_back += report.reach.get("check.asof.fallback"),
                Err(failure) => panic!("{failure}"),
            }
        }
        assert!(fell_back > 0, "no AsOf check saw a dropped created-at time");
    }
}

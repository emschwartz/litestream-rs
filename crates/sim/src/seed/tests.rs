use proptest::prelude::*;

use super::*;

proptest! {
    #[test]
    fn seed_display_round_trips(raw in any::<u64>()) {
        let seed = Seed::new(raw);
        let text = seed.to_string();
        prop_assert_eq!(text.len(), 16);
        prop_assert_eq!(text.parse::<Seed>(), Ok(seed));
        prop_assert_eq!(format!("0x{text}").parse::<Seed>(), Ok(seed));
    }
}

#[test]
fn seed_parse_rejects_what_is_not_hex() {
    for bad in ["", "0x", "xyz", "0x1_2", "12345678901234567", " 1", "-1"] {
        assert!(bad.parse::<Seed>().is_err(), "{bad:?}");
    }
}

#[test]
fn replay_line_names_the_seed_and_the_step_count() {
    let seed = Seed::new(0xabc);
    assert_eq!(
        replay_line(seed, None),
        "just sim-replay 0x0000000000000abc"
    );
    assert_eq!(
        replay_line(seed, Some(117)),
        "just sim-replay 0x0000000000000abc 117"
    );
}

#[test]
fn streams_differ_per_purpose_and_per_seed() {
    let a = Seed::new(1);
    assert_ne!(a.stream(0), a.stream(1));
    assert_ne!(a.stream(0), Seed::new(2).stream(0));
    assert_eq!(a.stream(5), a.stream(5));
}

#[test]
fn a_night_owns_a_million_seeds_of_its_own() {
    assert_eq!(nightly_base("20261120"), Ok(20_261_120_000_000));
    let tonight = nightly_base("20261231").unwrap();
    let tomorrow = nightly_base("20270101").unwrap();
    assert!(tomorrow - tonight >= SEEDS_PER_NIGHT);
    assert_eq!(
        nightly_base("20261121").unwrap() - nightly_base("20261120").unwrap(),
        SEEDS_PER_NIGHT
    );
}

#[test]
fn text_that_is_not_a_date_is_no_base() {
    for text in [
        "",
        "2026112",
        "202611200",
        "2026-11-20",
        "20261320",
        "20261100",
        "20261132",
        "00001120",
        "2026112x",
        "+0261120",
        " 20261120",
    ] {
        assert_eq!(
            nightly_base(text),
            Err(ParseDayError(text.to_owned())),
            "{text:?}"
        );
    }
}

#[test]
fn shards_are_disjoint_and_cover_the_range() {
    for count in [1_u64, 2, 4, 7] {
        let base = 1_000;
        let shards: Vec<Vec<u64>> = (0..count)
            .map(|index| {
                let shard = Shard::parse(&format!("{index}/{count}")).unwrap();
                shard.seeds(base).take(25).map(Seed::get).collect()
            })
            .collect();
        let mut seen = std::collections::BTreeSet::new();
        for seeds in &shards {
            assert_eq!(seeds.len(), 25);
            for seed in seeds {
                assert!(seen.insert(*seed), "{seed} runs in two shards of {count}");
            }
        }
        // Every seed below the shortest shard's last is somebody's.
        let covered_to = base + 25 * count;
        assert!(
            (base..covered_to).all(|seed| seen.contains(&seed)),
            "{count}"
        );
        assert_eq!(u64::try_from(seen.len()).unwrap(), 25 * count);
    }
}

#[test]
fn text_that_is_no_shard_is_refused() {
    for text in [
        "", "1", "1/", "/4", "4/4", "5/4", "0/0", "-1/4", "a/b", "1/4/2", " 1/4",
    ] {
        assert_eq!(
            Shard::parse(text),
            Err(ParseShardError(text.to_owned())),
            "{text:?}"
        );
    }
    Shard::parse("3/4").unwrap();
}

#[test]
fn the_long_range_is_the_upper_half_of_the_night() {
    let rotating = nightly_base("20261125").unwrap();
    let long = long_base("20261125").unwrap();
    let next = nightly_base("20261126").unwrap();
    assert_eq!(long - rotating, SEEDS_PER_NIGHT / 2);
    assert!(long < next);
    // A shard's seeds stay inside the night: the first thousand of the busiest shard layout.
    let last = Shard::parse("0/4").unwrap().seeds(long).nth(1_000).unwrap();
    assert!(last.get() > long && last.get() < next);
    assert_eq!(
        long_base("2026-11-25"),
        Err(ParseDayError("2026-11-25".to_owned()))
    );
}

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

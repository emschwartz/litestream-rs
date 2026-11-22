//! The read rule over two slots: every verdict, and the winner is the higher sequence number
//! whatever the other slot holds.

use lsr_journal::{Journal, Opened, SLOT, Seq, Slot, SlotRead, Verdict, Why, encode, parse};
use proptest::prelude::*;

use crate::codec::journal;

fn seq(raw: u64) -> Seq {
    Seq::new(raw).unwrap()
}

#[test]
fn absent_is_missing_and_an_open_error_is_unreadable() {
    assert_eq!(parse(Opened::Absent).verdict, Verdict::Missing);
    assert_eq!(parse(Opened::Io).verdict, Verdict::Unreadable(Why::Io));
}

#[test]
fn no_valid_slot_is_unreadable_and_says_why() {
    let zeros = [0; SLOT];
    let invalid = parse(Opened::Slots(SlotRead::Bytes(&zeros), SlotRead::Short));
    assert_eq!(invalid.verdict, Verdict::Unreadable(Why::Invalid));
    assert!(!invalid.slot_failed);
    let io = parse(Opened::Slots(SlotRead::Io, SlotRead::Bytes(&zeros)));
    assert_eq!(io.verdict, Verdict::Unreadable(Why::Io));
    assert!(io.slot_failed);
}

#[test]
fn an_error_in_one_slot_keeps_the_other_and_is_reported() {
    let one = encode(&Journal::ABSENT, seq(1));
    let parsed = parse(Opened::Slots(SlotRead::Bytes(&one), SlotRead::Io));
    let read = Verdict::Read {
        slot: Slot::S0,
        seq: seq(1),
        journal: Journal::ABSENT,
    };
    assert_eq!(parsed.verdict, read);
    assert!(parsed.slot_failed);
}

#[test]
fn two_slots_with_one_sequence_number_are_a_tie() {
    let one = encode(&Journal::ABSENT, seq(3));
    let parsed = parse(Opened::Slots(SlotRead::Bytes(&one), SlotRead::Bytes(&one)));
    assert_eq!(parsed.verdict, Verdict::Unreadable(Why::Tie));
}

proptest! {
    #[test]
    fn the_higher_sequence_number_wins(a in journal(), b in journal(), low in 1..u64::MAX / 2, gap in 1..1000_u64, swap: bool) {
        let older = encode(&a, seq(low));
        let newer = encode(&b, seq(low + gap));
        let (s0, s1, slot) = if swap { (&newer, &older, Slot::S0) } else { (&older, &newer, Slot::S1) };
        let parsed = parse(Opened::Slots(SlotRead::Bytes(s0), SlotRead::Bytes(s1)));
        prop_assert_eq!(parsed.verdict, Verdict::Read { slot, seq: seq(low + gap), journal: b });
    }

    #[test]
    fn a_valid_slot_wins_over_anything_in_the_other(a in journal(), raw in 1..u64::MAX, junk in any::<[u8; 64]>()) {
        let good = encode(&a, seq(raw));
        let mut bad = [0; SLOT];
        bad[..64].copy_from_slice(&junk);
        let parsed = parse(Opened::Slots(SlotRead::Bytes(&bad), SlotRead::Bytes(&good)));
        prop_assert_eq!(parsed.verdict, Verdict::Read { slot: Slot::S1, seq: seq(raw), journal: a });
    }
}

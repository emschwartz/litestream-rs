//! The codec: a round trip for every record, one test per decoder error, and a changed byte
//! anywhere in a record invalidates it.

use std::num::NonZeroU32;

use lsr_journal::{
    Ahead, AheadKind, Decoded, Floor, Invalid, Journal, MAGIC, Proof, RECORD_MAX, ReplicaId, SLOT,
    Seq, StoredBudget, Triple, Unchecked, Unproven, WalChecksum, decode, encode,
};
use lsr_ltx::{FileChecksum, Timestamp, Txid};
use proptest::prelude::*;

fn checksum() -> impl Strategy<Value = FileChecksum> {
    any::<u64>().prop_map(|raw| FileChecksum::from_raw(raw | 1 << 63).unwrap())
}

fn txid() -> impl Strategy<Value = Txid> {
    (1..=u64::MAX).prop_map(|raw| Txid::new(raw).unwrap())
}

fn triple() -> impl Strategy<Value = Triple> {
    (any::<u64>(), any::<u64>(), checksum()).prop_map(|(len, digest, checksum)| Triple {
        len,
        digest,
        checksum,
    })
}

pub fn journal() -> impl Strategy<Value = Journal> {
    let floor = (any::<[u8; 8]>(), txid(), proptest::option::of(triple()))
        .prop_map(|(replica, bound, anchor)| Floor::new(ReplicaId(replica), bound, anchor));
    let unproven = prop_oneof![
        Just(Unproven::No),
        Just(Unproven::Truncate),
        Just(Unproven::Snapshot)
    ];
    let budget = (any::<u8>(), any::<i64>()).prop_map(|(tokens, millis)| StoredBudget {
        tokens,
        as_of: Timestamp::from_unix_millis(millis),
    });
    let proof = (txid(), triple(), any::<(u32, u32)>()).prop_map(|(txid, triple, (a, b))| Proof {
        txid,
        triple,
        wal: WalChecksum(a, b),
    });
    let kind = prop_oneof![Just(AheadKind::AheadOfWal), Just(AheadKind::WalLostCommits)];
    let ahead = (kind, txid(), 1..=u32::MAX, 1..=u32::MAX, any::<i64>()).prop_map(
        |(kind, restore_point, pages, first, millis)| Ahead {
            kind,
            restore_point,
            pages: NonZeroU32::new(pages).unwrap(),
            first: NonZeroU32::new(first).unwrap(),
            made: Timestamp::from_unix_millis(millis),
        },
    );
    let unchecked = any::<i64>().prop_map(|millis| Unchecked {
        made: Timestamp::from_unix_millis(millis),
    });
    (
        proptest::option::of(floor),
        unproven,
        proptest::option::of(budget),
        proptest::option::of(proof),
        proptest::option::of(ahead),
        proptest::option::of(unchecked),
    )
        .prop_map(
            |(floor, unproven, budget, pending, ahead, unchecked)| Journal {
                floor,
                unproven,
                budget,
                pending,
                ahead,
                unchecked,
            },
        )
}

fn seq() -> impl Strategy<Value = Seq> {
    (1..=u64::MAX).prop_map(|raw| Seq::new(raw).unwrap())
}

/// The record's length in its slot: everything up to and including the checksum.
fn record_len(slot: &[u8; SLOT]) -> usize {
    20 + usize::from(u16::from_le_bytes([slot[10], slot[11]])) + 4
}

proptest! {
    #[test]
    fn every_record_round_trips(journal in journal(), seq in seq()) {
        let slot = encode(&journal, seq);
        prop_assert_eq!(decode(&slot), Ok(Decoded::Ours { seq, journal }));
        prop_assert!(record_len(&slot) <= RECORD_MAX);
        prop_assert!(slot[record_len(&slot)..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn a_changed_byte_in_the_record_invalidates_it(
        journal in journal(),
        seq in seq(),
        at in any::<prop::sample::Index>(),
        flip in 1..=u8::MAX,
    ) {
        let mut slot = encode(&journal, seq);
        let at = at.index(record_len(&slot));
        slot[at] ^= flip;
        prop_assert!(decode(&slot).is_err(), "byte {} changed and the slot still decodes", at);
    }

    #[test]
    fn a_changed_byte_after_the_record_changes_nothing(
        journal in journal(),
        seq in seq(),
        at in any::<prop::sample::Index>(),
        flip in 1..=u8::MAX,
    ) {
        let mut slot = encode(&journal, seq);
        let len = record_len(&slot);
        let at = len + at.index(SLOT - len);
        slot[at] ^= flip;
        prop_assert_eq!(decode(&slot), Ok(Decoded::Ours { seq, journal }));
    }
}

/// A slot whose header and body are `bytes`, with a correct checksum after them.
fn sealed(bytes: &[u8]) -> [u8; SLOT] {
    let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(bytes);
    let mut slot = [0; SLOT];
    slot[..bytes.len()].copy_from_slice(bytes);
    slot[bytes.len()..bytes.len() + 4].copy_from_slice(&crc.to_le_bytes());
    slot
}

fn head(version: u16, len: u16, seq: u64) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    out.extend(version.to_le_bytes());
    out.extend(len.to_le_bytes());
    out.extend(seq.to_le_bytes());
    out
}

fn record(version: u16, seq: u64, body: &[u8]) -> [u8; SLOT] {
    let mut bytes = head(version, u16::try_from(body.len()).unwrap(), seq);
    bytes.extend(body);
    sealed(&bytes)
}

#[test]
fn zeros_have_no_magic() {
    assert_eq!(decode(&[0; SLOT]), Err(Invalid::Magic));
}

#[test]
fn version_0_is_refused() {
    assert_eq!(decode(&record(0, 1, &[0, 0])), Err(Invalid::Version));
}

#[test]
fn a_body_past_the_record_is_refused() {
    let mut slot = record(1, 1, &[0, 0]);
    slot[10..12].copy_from_slice(&489_u16.to_le_bytes());
    assert_eq!(decode(&slot), Err(Invalid::Length(489)));
}

#[test]
fn a_wrong_checksum_is_refused() {
    let mut slot = record(1, 1, &[0, 0]);
    slot[22] ^= 1;
    assert_eq!(decode(&slot), Err(Invalid::Checksum));
}

#[test]
fn sequence_0_is_refused() {
    assert_eq!(decode(&record(1, 0, &[0, 0])), Err(Invalid::Seq));
}

#[test]
fn unknown_flags_and_an_anchor_without_a_floor_are_refused() {
    assert_eq!(decode(&record(1, 1, &[0x10, 0])), Err(Invalid::Flags(0x10)));
    assert_eq!(decode(&record(1, 1, &[2, 0])), Err(Invalid::Flags(2)));
}

#[test]
fn a_body_whose_length_disagrees_with_its_flags_is_refused() {
    assert_eq!(
        decode(&record(1, 1, &[0, 0, 0])),
        Err(Invalid::BodyLength { len: 3, want: 2 })
    );
}

#[test]
fn a_zero_bound_is_refused() {
    let mut body = vec![1];
    body.extend([7; 8]);
    body.extend(0_u64.to_le_bytes());
    body.push(0);
    assert_eq!(decode(&record(1, 1, &body)), Err(Invalid::Txid));
}

#[test]
fn a_file_checksum_without_its_flag_bit_is_refused() {
    let mut body = vec![3];
    body.extend([7; 8]);
    body.extend(5_u64.to_le_bytes());
    body.extend([0; 16]);
    body.extend(9_u64.to_le_bytes());
    body.push(0);
    assert_eq!(decode(&record(1, 1, &body)), Err(Invalid::FileChecksum(9)));
}

#[test]
fn an_unknown_debt_code_is_refused() {
    assert_eq!(decode(&record(1, 1, &[0, 3])), Err(Invalid::Unproven(3)));
}

#[test]
fn a_newer_version_is_valid_on_magic_length_and_checksum() {
    let slot = record(5, 9, &[0xff; 40]);
    assert_eq!(
        decode(&slot),
        Ok(Decoded::Newer {
            seq: Seq::new(9).unwrap(),
            version: 5
        })
    );
}

/// A binary that predates a fact reads every record written while it does not stand: only a
/// record holding a verdict is version 2, only one holding an unchecked notice version 3, and
/// only one holding a lost-commits verdict version 4.
#[test]
fn each_fact_raises_the_version_only_while_it_stands() {
    let version = |journal: &Journal| {
        let slot = encode(journal, Seq::new(1).unwrap());
        u16::from_le_bytes([slot[8], slot[9]])
    };
    assert_eq!(version(&Journal::ABSENT), 1);
    let ahead = Journal {
        ahead: Some(Ahead {
            kind: AheadKind::AheadOfWal,
            restore_point: Txid::new(4).unwrap(),
            pages: NonZeroU32::new(2).unwrap(),
            first: NonZeroU32::new(3).unwrap(),
            made: Timestamp::from_unix_millis(5),
        }),
        ..Journal::ABSENT
    };
    assert_eq!(version(&ahead), 2);
    let unchecked = Some(Unchecked {
        made: Timestamp::from_unix_millis(6),
    });
    assert_eq!(version(&Journal { unchecked, ..ahead }), 3);
    assert_eq!(
        version(&Journal {
            unchecked,
            ..Journal::ABSENT
        }),
        3
    );
    let lost = ahead.ahead.map(|ahead| Ahead {
        kind: AheadKind::WalLostCommits,
        ..ahead
    });
    assert_eq!(
        version(&Journal {
            ahead: lost,
            ..ahead
        }),
        4
    );
    assert_eq!(
        version(&Journal {
            ahead: lost,
            unchecked,
            ..Journal::ABSENT
        }),
        4
    );
}

/// The lost-commits kind is version 4's: a version-3 record carrying it is invalid, as a flag no
/// writer of that version sets, and so is the kind without a verdict. A version-4 record reads
/// it back; the same verdict without the bit reads as the older kind.
#[test]
fn a_lost_commits_verdict_is_version_4_s_and_needs_a_verdict() {
    let verdict = |flags: u8| {
        let mut body = vec![flags];
        body.extend(4_u64.to_le_bytes());
        body.extend(2_u32.to_le_bytes());
        body.extend(3_u32.to_le_bytes());
        body.extend(5_i64.to_le_bytes());
        body.push(0);
        body
    };
    let kind = |version: u16, flags: u8| match decode(&record(version, 1, &verdict(flags))) {
        Ok(Decoded::Ours { journal, .. }) => journal.ahead.map(|ahead| ahead.kind),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        decode(&record(3, 1, &verdict(0x50))),
        Err(Invalid::Flags(0x50))
    );
    assert_eq!(kind(4, 0x50), Some(AheadKind::WalLostCommits));
    assert_eq!(kind(4, 0x10), Some(AheadKind::AheadOfWal));
    assert_eq!(kind(3, 0x10), Some(AheadKind::AheadOfWal));
    assert_eq!(decode(&record(4, 1, &[0x40, 0])), Err(Invalid::Flags(0x40)));
}

/// The unchecked notice's flag is version 3's: a version-2 record carrying it is invalid, as a
/// flag no writer of that version sets.
#[test]
fn an_unchecked_notice_in_a_version_2_record_is_refused() {
    let mut body = vec![0x20];
    body.extend(7_i64.to_le_bytes());
    body.push(1);
    assert_eq!(decode(&record(2, 1, &body)), Err(Invalid::Flags(0x20)));
    assert_eq!(
        decode(&record(3, 1, &body)),
        Ok(Decoded::Ours {
            seq: Seq::new(1).unwrap(),
            journal: Journal {
                unproven: Unproven::Truncate,
                unchecked: Some(Unchecked {
                    made: Timestamp::from_unix_millis(7),
                }),
                ..Journal::ABSENT
            },
        })
    );
    assert_eq!(decode(&record(3, 1, &[0x40, 0])), Err(Invalid::Flags(0x40)));
}

/// The verdict's flag is version 2's: a version-1 record carrying it is invalid, as a flag no
/// writer of that version sets.
#[test]
fn a_verdict_in_a_version_1_record_is_refused() {
    let mut body = vec![0x10];
    body.extend(4_u64.to_le_bytes());
    body.extend(2_u32.to_le_bytes());
    body.extend(3_u32.to_le_bytes());
    body.extend(5_i64.to_le_bytes());
    body.push(0);
    assert_eq!(decode(&record(1, 1, &body)), Err(Invalid::Flags(0x10)));
    assert!(matches!(
        decode(&record(2, 1, &body)),
        Ok(Decoded::Ours { journal, .. }) if journal.ahead.is_some()
    ));
    assert_eq!(decode(&record(2, 1, &[0x20, 0])), Err(Invalid::Flags(0x20)));
}

#[test]
fn a_verdict_of_no_pages_or_from_page_zero_is_refused() {
    for (pages, first) in [(0_u32, 3_u32), (2, 0)] {
        let mut body = vec![0x10];
        body.extend(4_u64.to_le_bytes());
        body.extend(pages.to_le_bytes());
        body.extend(first.to_le_bytes());
        body.extend(5_i64.to_le_bytes());
        body.push(0);
        assert_eq!(
            decode(&record(2, 1, &body)),
            Err(Invalid::Ahead { pages, first })
        );
    }
}

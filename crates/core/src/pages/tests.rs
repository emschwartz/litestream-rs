use proptest::prelude::*;

use super::*;

const PS: PageSize = PageSize::MIN;

fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn page(fill: u8) -> Vec<u8> {
    vec![fill; PS.bytes()]
}

fn digest(n: u32, fill: u8) -> PageChecksum {
    PageChecksum::of(pg(n), &page(fill))
}

fn table() -> PageChecksumTable {
    let mut table = PageChecksumTable::new();
    table.fit(PS);
    table
}

#[test]
fn a_page_never_seen_is_recorded_by_its_first_read_and_checked_by_every_read_after() {
    let mut table = table();
    assert_eq!(table.check(pg(3), &page(7)), Ok(()));
    assert_eq!(table.get(pg(3)), Some(digest(3, 7)));
    assert_eq!(table.check(pg(3), &page(7)), Ok(()));
    assert_eq!(
        table.check(pg(3), &page(8)),
        Err(Mismatch {
            pgno: pg(3),
            recorded: digest(3, 7),
            found: digest(3, 8),
        })
    );
}

#[test]
fn a_page_a_frame_recorded_is_checked_on_its_first_read() {
    let mut table = table();
    table.record(pg(2), digest(2, 1));
    assert!(table.check(pg(2), &page(9)).is_err());
    assert_eq!(table.mismatch(), Some(pg(2)));
}

/// The response that keeps trying ships the file's bytes: the same bytes read
/// again pass, other bytes are a new mismatch, and a frame that rewrites the
/// page ends the report.
#[test]
fn a_mismatched_page_passes_with_the_bytes_it_was_reported_with_until_a_frame_rewrites_it() {
    let mut table = table();
    table.record(pg(2), digest(2, 1));
    assert!(table.check(pg(2), &page(9)).is_err());
    assert_eq!(table.check(pg(2), &page(9)), Ok(()));
    assert!(table.check(pg(2), &page(10)).is_err());
    assert_eq!(table.check(pg(2), &page(10)), Ok(()));
    assert_eq!(table.mismatch(), Some(pg(2)));
    table.record(pg(2), digest(2, 4));
    assert_eq!(table.mismatch(), None);
    assert!(table.check(pg(2), &page(10)).is_err());
}

#[test]
fn an_operator_accepting_the_file_trusts_the_bytes_found() {
    let mut table = table();
    table.record(pg(2), digest(2, 1));
    assert!(table.check(pg(2), &page(9)).is_err());
    table.accept();
    assert_eq!(table.mismatch(), None);
    assert_eq!(table.get(pg(2)), Some(digest(2, 9)));
    assert_eq!(table.check(pg(2), &page(9)), Ok(()));
}

#[test]
fn a_shrink_forgets_the_pages_past_the_end_so_a_regrown_page_is_read_afresh() {
    let mut table = table();
    for n in 1..=5 {
        table.record(pg(n), digest(n, 1));
    }
    assert!(table.check(pg(5), &page(2)).is_err());
    table.truncate(nz(3));
    assert_eq!(table.len(), 3);
    assert_eq!(table.mismatch(), None);
    assert_eq!(table.check(pg(5), &page(2)), Ok(()));
    assert_eq!(table.check(pg(4), &page(3)), Ok(()));
}

#[test]
fn a_page_size_change_empties_the_table() {
    let mut table = table();
    table.record(pg(1), digest(1, 1));
    table.fit(PS);
    assert_eq!(table.len(), 1);
    table.fit(PageSize::new(1024).unwrap());
    assert!(table.is_empty());
}

#[test]
fn only_the_checkpoint_boundary_and_an_operator_keep_the_table_at_a_snapshot() {
    let at = lsr_wal::WalOffset::start(PS);
    let causes = [
        (SnapshotCause::FirstSync, true),
        (
            SnapshotCause::BehindRemote {
                local: lsr_ltx::Txid::MIN,
            },
            true,
        ),
        (SnapshotCause::NewestStagedFileUnreadable, true),
        (SnapshotCause::WalMissing, true),
        (SnapshotCause::GenerationChanged, true),
        (SnapshotCause::PrevFrameMismatch, true),
        (SnapshotCause::LastPageMismatch, true),
        (
            SnapshotCause::LocalRewound {
                synced: at,
                committed: at,
            },
            true,
        ),
        (SnapshotCause::CheckpointBoundary, true),
        (SnapshotCause::OperatorRequested, false),
    ];
    for (cause, clears) in causes {
        assert_eq!(snapshot_clears(&cause), clears, "{cause:?}");
    }
}

proptest! {
    /// Whatever the frames and reads, a read of the bytes last recorded for a
    /// page passes, and the table never holds a page past the last truncation.
    #[test]
    fn the_newest_record_always_passes(
        ops in prop::collection::vec((1_u32..12, any::<u8>(), 0_u8..3), 1..60),
    ) {
        let mut table = table();
        let mut newest = std::collections::BTreeMap::new();
        let mut size = 11_u32;
        for (n, fill, kind) in ops {
            match kind {
                0 if n <= size => {
                    table.record(pg(n), digest(n, fill));
                    newest.insert(n, fill);
                }
                1 => {
                    size = n;
                    table.truncate(nz(n));
                    newest.retain(|page, _| *page <= n);
                }
                _ => {}
            }
            for (page_no, fill) in &newest {
                prop_assert_eq!(table.check(pg(*page_no), &page(*fill)), Ok(()));
            }
            prop_assert!(table.len() <= usize::try_from(size).unwrap());
        }
    }
}

#[test]
fn a_staging_listing_whose_cursor_never_advances_is_a_fault_not_a_loop() {
    let staging = crate::stuck::StuckListing::new(crate::MemStore::new());
    let got = crate::conformance::block_on(seeded(&staging, None, nz(1)));
    assert_eq!(got.err(), Some(Fault::LocalIo));
}

/// A level-0 file for transaction `n` holding `pages` filled with `fill`.
fn staged_file(n: u64, pages: &[u32], fill: u8) -> (FileKey, Vec<u8>) {
    use lsr_ltx::{Encoder, Header, HeaderFields, Timestamp, TxidRange};
    let txid = Txid::new(n).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: PageSize::MIN,
        commit: 4,
        txids: TxidRange::single(txid),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for n in pages {
        encoder.write_page(pg(*n), &page(fill)).unwrap();
    }
    let key = FileKey::new(Level::ZERO, TxidRange::single(txid));
    (key, encoder.finish(None).unwrap().0)
}

/// Only staged files from the replica's newest on seed the table: an older
/// one nothing vouched for at boot leaves its pages unknown, to be read
/// afresh, rather than checked against bytes it may never have held.
#[test]
fn only_vouched_staged_files_seed_the_table() {
    use crate::store::Upload;
    let staging = crate::MemStore::new();
    for (n, pages, fill) in [(2, &[1, 2][..], 1), (3, &[1][..], 2), (4, &[3][..], 3)] {
        let (key, bytes) = staged_file(n, pages, fill);
        crate::conformance::block_on(async {
            let mut up = staging
                .begin_upload(&key, lsr_ltx::Timestamp::from_unix_millis(1))
                .await
                .unwrap();
            up.write(&bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }
    let all = crate::conformance::block_on(seeded(&staging, None, nz(4))).unwrap();
    assert_eq!(all.len(), 3);
    let vouched = crate::conformance::block_on(seeded(&staging, Txid::new(3), nz(4))).unwrap();
    assert_eq!(vouched.len(), 2);
    let mut vouched = vouched;
    // Page 2 was only in file 2: unknown, so any bytes pass and are recorded.
    assert_eq!(vouched.check(pg(2), &page(9)), Ok(()));
    assert!(vouched.check(pg(1), &page(1)).is_err());
}

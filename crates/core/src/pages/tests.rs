use proptest::prelude::*;

use super::*;
use crate::fault::role::{Replica, Staging};
use crate::store::ObjectStore;

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
    assert_eq!((table.len(), table.is_empty()), (1, false));
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
    let got = crate::conformance::block_on(seeded(&Staging(&staging), None, nz(1)));
    assert_eq!(got.err(), Some(Fault::LocalIo));
}

/// A level-0 file for transaction `n` holding `pages` filled with `fill`.
fn staged_file(n: u64, pages: &[u32], fill: u8) -> (FileKey, Vec<u8>) {
    staged_file_at(n, pages, fill, 4)
}

/// [`staged_file`] at a database size of `commit` pages.
fn staged_file_at(n: u64, pages: &[u32], fill: u8, commit: u32) -> (FileKey, Vec<u8>) {
    use lsr_ltx::{Encoder, Header, HeaderFields, Timestamp, TxidRange};
    let txid = Txid::new(n).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: PageSize::MIN,
        commit,
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
                .begin_upload(
                    &key,
                    lsr_ltx::Timestamp::from_unix_millis(1),
                    crate::store::WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }
    let all = crate::conformance::block_on(seeded(&Staging(&staging), None, nz(4)))
        .unwrap()
        .table;
    assert_eq!(all.len(), 3);
    let vouched = crate::conformance::block_on(seeded(&Staging(&staging), Txid::new(3), nz(4)))
        .unwrap()
        .table;
    assert_eq!(vouched.len(), 2);
    let mut vouched = vouched;
    // Page 2 was only in file 2: unknown, so any bytes pass and are recorded.
    assert_eq!(vouched.check(pg(2), &page(9)), Ok(()));
    assert!(vouched.check(pg(1), &page(1)).is_err());
}

/// Seeding finds the pending files that change nothing against the files before them: the
/// same page with the same bytes at the same size. A new size, or other bytes, is a change
/// (`crate::failstop`, the quiet files).
#[test]
fn seeding_finds_the_pending_files_that_change_nothing() {
    use crate::store::Upload;
    let staging = crate::MemStore::new();
    let files = [
        (1, &[1, 2, 3, 4][..], 1, 4),
        // Pending from here: the same page 1 again, unchanged.
        (2, &[1][..], 1, 4),
        // The same bytes at another size.
        (3, &[1][..], 1, 5),
        // Other bytes.
        (4, &[2][..], 7, 5),
    ];
    for (n, pages, fill, commit) in files {
        let (key, bytes) = staged_file_at(n, pages, fill, commit);
        crate::conformance::block_on(async {
            let mut up = staging
                .begin_upload(
                    &key,
                    lsr_ltx::Timestamp::from_unix_millis(1),
                    crate::store::WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }
    let seeded =
        crate::conformance::block_on(seeded(&Staging(&staging), Txid::new(1), nz(5))).unwrap();
    let quiet: Vec<u64> = seeded.quiet.iter().map(|txid| txid.get()).collect();
    assert_eq!(quiet, vec![2]);
}

/// An untracked file of `pages` (page, fill) whose frames came from the WAL of salts `(1, 1)`
/// between `offset` and `end`.
fn file_from_wal(n: u64, pages: &[(u32, u8)], offset: u64, end: u64) -> (FileKey, Vec<u8>) {
    use lsr_ltx::{Encoder, Header, HeaderFields, Timestamp, TxidRange, WalOrigin};
    let txid = Txid::new(n).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: PageSize::MIN,
        commit: 3,
        txids: TxidRange::single(txid),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal: WalOrigin::new(offset, end.saturating_sub(offset), 1, 1),
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for (n, fill) in pages {
        encoder.write_page(pg(*n), &page(*fill)).unwrap();
    }
    let key = FileKey::new(Level::ZERO, TxidRange::single(txid));
    (key, encoder.finish(None).unwrap().0)
}

/// A pending file whose WAL span ends where the replica's newest file's ended holds no commit
/// the replica lacks, though it rewrites pages the table seeded from that file never saw (a
/// snapshot staged for a doubt on a quiet WAL). Without this a restarted engine dates the file
/// by its stamp and, after a long pause, trips for age over a replica that holds everything.
#[test]
fn a_pending_file_that_ends_where_the_replicas_newest_ended_in_the_wal_is_quiet() {
    use crate::store::Upload;
    let staging = crate::MemStore::new();
    let files = [
        // The replica's newest: one page, WAL bytes 100 to 200.
        file_from_wal(2, &[(1, 2)], 100, 200),
        // Pending: every page again, the WAL from its start to the same end.
        file_from_wal(3, &[(1, 2), (2, 1), (3, 1)], 32, 200),
        // Pending: the same again, ending where the quiet one did.
        file_from_wal(4, &[(1, 2), (2, 1), (3, 1)], 32, 200),
        // Pending: frames past the end, other bytes: a commit.
        file_from_wal(5, &[(1, 3)], 200, 300),
    ];
    for (key, bytes) in files {
        crate::conformance::block_on(async {
            let mut up = staging
                .begin_upload(
                    &key,
                    lsr_ltx::Timestamp::from_unix_millis(1),
                    crate::store::WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }
    let seeded =
        crate::conformance::block_on(seeded(&Staging(&staging), Txid::new(2), nz(3))).unwrap();
    let quiet: Vec<u64> = seeded.quiet.iter().map(|txid| txid.get()).collect();
    assert_eq!(quiet, vec![3, 4]);
}
/// Puts `files` in `store`.
fn put_all(store: &crate::MemStore, files: Vec<(FileKey, Vec<u8>)>) {
    use crate::store::Upload;
    for (key, bytes) in files {
        crate::conformance::block_on(async {
            let mut up = store
                .begin_upload(
                    &key,
                    lsr_ltx::Timestamp::from_unix_millis(1),
                    crate::store::WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }
}

/// The replica's newest file is no longer staged (it was pruned after its upload, or staging
/// was lost), so nothing staged stands before the first pending file. A pending snapshot that
/// ends where the replica's newest file ends in the WAL holds no commit the replica lacks, and
/// a restart that forgot it would date it by its stamp and trip the age bound over a replica
/// that holds everything. One that ends elsewhere may hold a commit and stays counted.
#[test]
fn a_first_pending_file_ending_where_the_replicas_newest_ended_is_quiet() {
    let replica = crate::MemStore::new();
    put_all(&replica, vec![file_from_wal(2, &[(1, 2)], 100, 200)]);
    let at_the_end = crate::MemStore::new();
    put_all(
        &at_the_end,
        vec![file_from_wal(3, &[(1, 2), (2, 1), (3, 1)], 32, 200)],
    );
    let past_it = crate::MemStore::new();
    put_all(&past_it, vec![file_from_wal(3, &[(1, 3)], 200, 300)]);
    let quiet = |staging: &crate::MemStore| {
        let mut seeded =
            crate::conformance::block_on(seeded(&Staging(staging), Txid::new(2), nz(3))).unwrap();
        crate::conformance::block_on(seeded.place(&Replica(&replica), Txid::new(2))).unwrap();
        seeded
            .quiet
            .iter()
            .map(|txid| txid.get())
            .collect::<Vec<u64>>()
    };
    assert_eq!(quiet(&at_the_end), vec![3]);
    assert_eq!(quiet(&past_it), Vec::<u64>::new());
}

/// The engine believes the replica holds a newest file it does not (an upload whose answer was
/// lost): placing the first pending file leaves it counted and does not fail the boot, which
/// would read the same missing file again for as long as the belief lasts.
#[test]
fn a_replica_missing_the_believed_newest_file_leaves_the_first_pending_file_counted() {
    let replica = crate::MemStore::new();
    let staging = crate::MemStore::new();
    put_all(
        &staging,
        vec![file_from_wal(3, &[(1, 2), (2, 1), (3, 1)], 32, 200)],
    );
    let mut seeded =
        crate::conformance::block_on(seeded(&Staging(&staging), Txid::new(2), nz(3))).unwrap();
    crate::conformance::block_on(seeded.place(&Replica(&replica), Txid::new(2))).unwrap();
    assert!(seeded.quiet.is_empty());
}

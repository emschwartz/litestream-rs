//! Request counts over a writer-shaped replica of 10^4 files.
//!
//! The fixture is committed through the store into FakeS3, which lists
//! LastModified only, so every exact created-at time an operation needs is a HEAD and the
//! wire shows it. Layout: 9000 transactions, one per second; level 1 holds one
//! file per transaction, level 2 files of 15, level 3 files of 100, level 0 the
//! newest 300, and eight snapshots (every 1000 transactions, recorded as created 5 s after
//! their last). Nothing here asserts a wall-clock time. The table that
//! [request_count_table] prints is the record: LIST, HEAD and GET per
//! operation, and what a listing that read a HEAD per file would have sent.

use crate::common;

// The helpers are test code only by cfg, which is what clippy.toml's allow-*-in-tests reads.
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "fixture arithmetic on small bounded counts and instants"
)]
mod cases {
    use std::collections::BTreeSet;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::time::Duration;

    use super::common::*;
    use lsr_core::conformance::block_on;
    use lsr_core::{
        ContiguousChain, Depth, Did, Due, FakeClock, FileKey, Inventory, Job, L0Retention, Level,
        MaintainState, Maintenance, MemSink, MemStore, NewestStagedFile, ObjectStore, PlanOutcome,
        ResolvedCreatedAt, RestoreError, RestoreTarget, Restored, Upload, UploadState, due,
        plan_reference, resolve, restore, upload_next, verify,
    };
    use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};
    use lsr_store_s3::{
        FakeS3, FaultTransport, HttpRequest, HttpResponse, S3Store, Shape, Transport,
        TransportError,
    };
    use lsr_wal::WalReader;

    const EPOCH_MS: i64 = 1_700_000_000_000;
    const TXNS: u64 = 9000;
    const LEVEL_ZERO_FILES: u64 = 300;
    const SNAPSHOT_EVERY: u64 = 1000;
    const SNAPSHOT_LAG_MS: i64 = 5000;
    const FILES: usize = 9998;
    const PAGE_BYTES: u32 = 512;

    /// Transaction n's created-at time: one second apart.
    fn created_at(n: u64) -> Timestamp {
        Timestamp::from_unix_millis(EPOCH_MS + i64::try_from(n).unwrap() * 1000)
    }

    fn snapshot_created_at(max: u64) -> Timestamp {
        Timestamp::from_unix_millis(created_at(max).unix_millis() + SNAPSHOT_LAG_MS)
    }

    /// The page every file ends with: its last transaction's seed repeated.
    fn seed(n: u64) -> u8 {
        u8::try_from(n % 250).unwrap() + 1
    }

    fn txid(n: u64) -> Txid {
        Txid::new(n).unwrap()
    }

    /// One writer-shaped replica file: level, first and last transaction, stored created-at time.
    type Planned = (u8, u64, u64, Timestamp);

    fn layout() -> Vec<Planned> {
        let mut files = Vec::new();
        for n in TXNS - LEVEL_ZERO_FILES + 1..=TXNS {
            files.push((0, n, n, created_at(n)));
        }
        for n in 1..=TXNS {
            files.push((1, n, n, created_at(n)));
        }
        for j in 0..TXNS / 15 {
            files.push((2, 15 * j + 1, 15 * j + 15, created_at(15 * j + 15)));
        }
        for j in 0..TXNS / 100 {
            files.push((3, 100 * j + 1, 100 * j + 100, created_at(100 * j + 100)));
        }
        for k in 1..=TXNS / SNAPSHOT_EVERY - 1 {
            let max = k * SNAPSHOT_EVERY;
            files.push((9, 1, max, snapshot_created_at(max)));
        }
        files
    }

    /// A one-page database whose only page ends as seed(max). Untracked, so no
    /// checksum chain; the header created-at time equals the stored one, as the writer's does.
    fn ltx_bytes(min: u64, max: u64, at: Timestamp) -> Vec<u8> {
        let page_size = PageSize::new(PAGE_BYTES).unwrap();
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size,
            commit: 1,
            txids: TxidRange::new(txid(min), txid(max)).unwrap(),
            timestamp: at,
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        let page = vec![seed(max); page_size.bytes()];
        encoder
            .write_page(PageNo::new(1, page_size).unwrap(), &page)
            .unwrap();
        encoder.finish(None).unwrap().0
    }

    /// The fault layer shared between the store and the test that counts.
    struct Shared(Arc<FaultTransport<FakeS3>>);

    impl Transport for Shared {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            self.0.send(request)
        }
    }

    type Store = S3Store<Shared>;

    struct Rig {
        wire: Arc<FaultTransport<FakeS3>>,
    }

    impl Rig {
        /// The 10^4-file replica, committed through a store.
        fn build() -> Self {
            let wire = Arc::new(FaultTransport::new(fake()));
            let rig = Self { wire };
            let store = rig.store();
            for (level, min, max, at) in layout() {
                let key = key(level, min, max);
                block_on(async {
                    let mut up = store.begin_upload(&key, at).await.unwrap();
                    up.write(&ltx_bytes(min, max, at)).await.unwrap();
                    up.commit().await.unwrap();
                });
            }
            rig
        }

        /// A store with clean created-at time stats over the same bucket.
        fn store(&self) -> Store {
            S3Store::with_transport(
                config(),
                credentials(),
                Shared(Arc::clone(&self.wire)),
                FakeClock::new(now()),
            )
            .unwrap()
        }

        fn counted<T>(&self, op: impl FnOnce() -> T) -> (T, Counts) {
            let mark = self.wire.requests().len();
            let out = op();
            let seen = self.wire.requests();
            (out, Counts::of(seen.get(mark..).unwrap_or_default()))
        }

        /// Every file with its created-at time, for the reference planner: 10^4 HEADs, uncounted.
        fn inventory_with_created_at(&self) -> Inventory {
            let store = self.store();
            block_on(async {
                Inventory::load(&store)
                    .await
                    .unwrap()
                    .with_created_at(&store)
                    .await
            })
            .unwrap()
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
    struct Counts {
        list: usize,
        head: usize,
        get: usize,
        other: usize,
    }

    impl Counts {
        fn of(seen: &[lsr_store_s3::RequestShape]) -> Self {
            let mut counts = Self::default();
            for request in seen {
                match request.shape {
                    Some(Shape::List) => counts.list += 1,
                    Some(Shape::Head) => counts.head += 1,
                    Some(Shape::Get) => counts.get += 1,
                    _ => counts.other += 1,
                }
            }
            counts
        }
    }

    /// One table row; [per_file_heads] is what a HEAD per listed file would have added.
    struct Row {
        what: String,
        counts: Counts,
        per_file_heads: usize,
        note: String,
    }

    impl Row {
        fn print(&self) {
            println!(
                "| {:<48} | {:>4} | {:>5} | {:>6} | {:>7} | {} |",
                self.what,
                self.counts.list,
                self.counts.head,
                self.counts.get,
                self.per_file_heads,
                self.note
            );
        }
    }

    fn restore_at(
        rig: &Rig,
        target: RestoreTarget,
    ) -> (Result<Restored, RestoreError>, Counts, MemSink) {
        let store = rig.store();
        let sink = MemSink::new();
        let (result, counts) = rig.counted(|| block_on(restore(&store, target, sink.clone())));
        (result, counts, sink)
    }

    fn assert_page(sink: &MemSink, reached: Txid) {
        let page = sink.published().unwrap().page(1).unwrap();
        assert_eq!(
            page,
            vec![seed(reached.get()); usize::try_from(PAGE_BYTES).unwrap()]
        );
    }

    fn restore_row(rig: &Rig, what: &str, target: RestoreTarget, wants: u64) -> Row {
        let (result, counts, sink) = restore_at(rig, target);
        let restored = result.unwrap();
        assert_eq!(restored.reached(), txid(wants), "{what}");
        assert_page(&sink, restored.reached());
        assert_eq!(
            counts.head,
            usize::try_from(restored.created_at_read()).unwrap(),
            "{what}"
        );
        Row {
            what: what.to_owned(),
            counts,
            per_file_heads: FILES,
            note: format!("{} files applied", restored.files()),
        }
    }

    fn now_ms() -> i64 {
        created_at(TXNS).unix_millis() + 500
    }

    fn instant_back(age: Duration) -> Timestamp {
        Timestamp::from_unix_millis(now_ms() - i64::try_from(age.as_millis()).unwrap())
    }

    /// The files at levels 0 to 8 that touch and pass the reach at some step of the chain.
    fn touching(inventory: &Inventory, chain: &ContiguousChain) -> usize {
        let mut seen = BTreeSet::new();
        for step in chain
            .files()
            .iter()
            .map(|file| file.key.range().max().get())
        {
            for file in inventory
                .files()
                .iter()
                .filter(|f| !f.key.level().is_snapshot())
            {
                let span = file.key.range();
                if span.min().get() <= step + 1 && span.max().get() > step {
                    seen.insert(file.key);
                }
            }
        }
        seen.len()
    }

    fn starts_at_one(inventory: &Inventory) -> usize {
        inventory
            .files()
            .iter()
            .filter(|f| f.key.range().min().get() == 1)
            .count()
    }

    fn plan_of(inventory: &Inventory, at: Timestamp) -> ContiguousChain {
        let outcome = plan_reference(inventory, RestoreTarget::AsOf(at));
        let PlanOutcome::Plan(chain) = outcome else {
            panic!("the reference planner gave {outcome:?}");
        };
        chain
    }

    /// The as-of rows: back by each age, then the oldest snapshot's instant.
    fn as_of_rows(rig: &Rig) -> Vec<Row> {
        let inventory = rig.inventory_with_created_at();
        let s = starts_at_one(&inventory);
        let minute = Duration::from_secs(60);
        let cases = [
            (
                "restore AsOf 1 minute back",
                instant_back(minute),
                TXNS - 60,
            ),
            (
                "restore AsOf 1 hour back",
                instant_back(minute * 60),
                TXNS - 3600,
            ),
            (
                "restore AsOf 2 hours back",
                instant_back(minute * 120),
                TXNS - 7200,
            ),
            (
                "restore AsOf at the oldest snapshot",
                Timestamp::from_unix_millis(snapshot_created_at(SNAPSHOT_EVERY).unix_millis() + 1),
                SNAPSHOT_EVERY + 5,
            ),
        ];
        let mut rows = Vec::new();
        for (what, at, wants) in cases {
            let row = restore_row(rig, what, RestoreTarget::AsOf(at), wants);
            let chain = plan_of(&inventory, at);
            let (e, c) = (
                touching(&inventory, &chain),
                chain
                    .files()
                    .iter()
                    .filter(|f| !f.key.level().is_snapshot())
                    .count(),
            );
            assert!(
                row.counts.head <= s + e,
                "{what}: {} HEADs, S {s} + E {e}",
                row.counts.head
            );
            if what.ends_with("oldest snapshot") {
                assert!(
                    row.counts.head < 2 * c + s + 10,
                    "{what}: {} HEADs, C {c}",
                    row.counts.head
                );
            }
            rows.push(Row {
                note: format!("{}; S {s}, E {e}, C {c}", row.note),
                ..row
            });
        }
        rows
    }

    fn verify_row(rig: &Rig, depth: Depth) -> Row {
        verify_counted(rig, depth).0
    }

    /// The row and the number of files verify says it opened.
    fn verify_counted(rig: &Rig, depth: Depth) -> (Row, usize) {
        let store = rig.store();
        let (report, counts) = rig.counted(|| block_on(verify(&store, depth)));
        let report = report.unwrap();
        assert!(report.findings().is_empty(), "{:?}", report.findings());
        let row = Row {
            what: format!("verify {depth:?}"),
            counts,
            per_file_heads: FILES,
            note: format!("{} files read", report.files_read()),
        };
        (row, report.files_read())
    }

    fn boot_row(rig: &Rig) -> Row {
        let store = rig.store();
        let staging = MemStore::new();
        let (done, counts) = rig.counted(|| {
            block_on(upload_next(
                UploadState::Unknown,
                &staging,
                &store,
                Some(txid(TXNS)),
            ))
        });
        let done = done.unwrap();
        assert_eq!(
            done.did,
            Did::Listed {
                newest: Some(txid(TXNS))
            }
        );
        Row {
            what: "uploader boot inventory".to_owned(),
            counts,
            per_file_heads: FILES,
            note: "list, then the tail read back".to_owned(),
        }
    }

    fn recover_row(rig: &Rig) -> Row {
        let store = rig.store();
        let (newest_staged, counts) = rig.counted(|| {
            block_on(NewestStagedFile::load(
                &store,
                None::<&mut WalReader<Cursor<Vec<u8>>>>,
            ))
        });
        assert!(newest_staged.unwrap().is_some());
        Row {
            what: "recover level-0 listing".to_owned(),
            counts,
            per_file_heads: usize::try_from(LEVEL_ZERO_FILES).unwrap(),
            note: "newest level-0 file, header read".to_owned(),
        }
    }

    fn files_rows(rig: &Rig) -> Vec<Row> {
        let store = rig.store();
        let (inventory, plain) = rig.counted(|| block_on(Inventory::load(&store)).unwrap());
        assert_eq!(inventory.files().len(), FILES);
        let (with_created_at, counts) = rig.counted(|| {
            block_on(async {
                Inventory::load(&store)
                    .await
                    .unwrap()
                    .with_created_at(&store)
                    .await
                    .unwrap()
            })
        });
        assert_eq!(with_created_at.files().len(), FILES);
        assert_eq!(store.created_at_stats().fell_back, 0);
        vec![
            Row {
                what: "lsr files".to_owned(),
                counts: plain,
                per_file_heads: FILES,
                note: "listed times, no created-at time read".to_owned(),
            },
            Row {
                what: "lsr files --created-at".to_owned(),
                counts,
                per_file_heads: FILES,
                note: "one HEAD per file, by request".to_owned(),
            },
        ]
    }

    /// What a tick's HEAD bound is made of: non-empty levels, S, and the level-0 deletes.
    fn tick_bound(deleted_level_zero: usize) -> usize {
        let planned = layout();
        let levels: BTreeSet<u8> = planned.iter().map(|f| f.0).collect();
        let s = planned.iter().filter(|f| f.1 == 1).count();
        levels.len() + s + deleted_level_zero + 1
    }

    fn tick_row(rig: &Rig, what: &str, config: &Maintenance) -> (Row, Due) {
        let store = rig.store();
        let now = Timestamp::from_unix_millis(now_ms());
        let (resolved, counts) = rig.counted(|| {
            block_on(async {
                let inventory = Inventory::load(&store).await.unwrap();
                let mut state = MaintainState::default();
                let mut step = |resolved: &ResolvedCreatedAt| {
                    due(
                        &inventory,
                        now,
                        config,
                        Some(txid(TXNS)),
                        &mut state,
                        resolved,
                    )
                };
                resolve(&store, &mut step).await.unwrap()
            })
        });
        assert_eq!(
            counts.head,
            usize::try_from(resolved.created_at_read).unwrap(),
            "{what}"
        );
        let row = Row {
            what: what.to_owned(),
            counts,
            per_file_heads: FILES,
            note: format!("{} created-at times read", resolved.created_at_read),
        };
        (row, resolved.value)
    }

    fn retention_config(l0: L0Retention) -> Maintenance {
        let levels = vec![
            Duration::from_secs(30),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        ];
        let (snapshot, kept) = (Duration::from_secs(5400), Duration::from_secs(5400));
        Maintenance::new(levels, snapshot, kept, l0).unwrap()
    }

    fn deletes(due: &Due) -> Vec<FileKey> {
        let Due::Run(Job::Delete(job)) = due else {
            panic!("expected a delete job, got {due:?}");
        };
        job.keys().to_vec()
    }

    fn tick_rows(rig: &Rig) -> Vec<Row> {
        let (idle, nothing) = tick_row(
            rig,
            "maintenance tick, nothing due",
            &Maintenance::go_default(),
        );
        assert!(
            matches!(nothing, Due::Wait { held: None, .. }),
            "{nothing:?}"
        );
        assert!(
            idle.counts.head <= tick_bound(0),
            "{} HEADs",
            idle.counts.head
        );
        let after_five = L0Retention::After(Duration::from_secs(300));
        let (job, planned) = tick_row(
            rig,
            "maintenance tick, retention job",
            &retention_config(after_five),
        );
        let gone = deletes(&planned);
        let level_zero = gone.iter().filter(|k| k.level() == Level::ZERO).count();
        assert!(
            job.counts.head <= tick_bound(level_zero),
            "{} HEADs",
            job.counts.head
        );
        let after_one = L0Retention::After(Duration::from_secs(1));
        let (job_l0, planned) = tick_row(
            rig,
            "maintenance tick, retention with level 0",
            &retention_config(after_one),
        );
        let level_zero = deletes(&planned)
            .iter()
            .filter(|k| k.level() == Level::ZERO)
            .count();
        assert!(level_zero > 0, "the aggressive schedule deletes level 0");
        assert!(
            job_l0.counts.head <= tick_bound(level_zero),
            "{} HEADs",
            job_l0.counts.head
        );
        let note = |row: Row, n: usize| Row {
            note: format!("{}; {n} level-0 deletes", row.note),
            ..row
        };
        vec![
            idle,
            note(job, level_zero_of(&gone)),
            note(job_l0, level_zero),
        ]
    }

    fn level_zero_of(keys: &[FileKey]) -> usize {
        keys.iter().filter(|k| k.level() == Level::ZERO).count()
    }

    fn print_table(rows: &[Row]) {
        println!(
            "| {:<48} | LIST | HEAD  | GET    | per-file HEAD (est.) | note |",
            "operation (10^4 files)"
        );
        for row in rows {
            row.print();
        }
    }

    #[test]
    fn the_fixture_is_writer_shaped() {
        let rig = Rig::build();
        let store = rig.store();
        let inventory = block_on(Inventory::load(&store)).unwrap();
        let per_level = |n: u8| inventory.level(Level::new(n).unwrap()).len();
        assert_eq!(
            [
                per_level(0),
                per_level(1),
                per_level(2),
                per_level(3),
                per_level(9)
            ],
            [300, 9000, 600, 90, 8]
        );
        assert_eq!(inventory.files().len(), FILES);
        let page = block_on(store.list(Level::new(1).unwrap(), None, limit(1000))).unwrap();
        assert_eq!((page.files.len(), page.next.is_some()), (1000, true));
    }

    #[test]
    fn latest_and_txid_restore_send_no_head() {
        let rig = Rig::build();
        let rows = [
            restore_row(&rig, "restore Latest", RestoreTarget::Latest, TXNS),
            restore_row(
                &rig,
                "restore Txid 4500",
                RestoreTarget::Txid(txid(4500)),
                4500,
            ),
        ];
        for row in rows {
            row.print();
            assert_eq!(row.counts.head, 0, "{}", row.what);
        }
    }

    #[test]
    fn verify_sends_no_head() {
        let rig = Rig::build();
        let names = verify_row(&rig, Depth::Names);
        let contents = verify_row(&rig, Depth::Contents);
        for row in [&names, &contents] {
            row.print();
            assert_eq!(row.counts.head, 0, "{}", row.what);
        }
        assert_eq!(names.counts.get, 0, "Names reads no object");
    }

    /// Every file is one GET (a short chunk is the end of the
    /// object, no empty read follows), and one listing serves the walk and the
    /// dry run. The latest plan's files are opened twice by design (the walk,
    /// then the dry run), so GETs equal the files verify reports opening.
    #[test]
    fn verify_reads_each_file_once() {
        let rig = Rig::build();
        let names = verify_row(&rig, Depth::Names);
        let (contents, opened) = verify_counted(&rig, Depth::Contents);
        contents.print();
        let plan_files = opened - FILES;
        assert_eq!(contents.counts.get, opened, "one GET per file opened");
        assert_eq!(
            contents.counts.get,
            FILES + plan_files,
            "every listed file once, the latest plan's files once more"
        );
        assert_eq!(contents.counts.list, names.counts.list, "one listing");
    }

    #[test]
    fn boot_and_recover_send_no_head() {
        let rig = Rig::build();
        for row in [boot_row(&rig), recover_row(&rig)] {
            row.print();
            assert_eq!(row.counts.head, 0, "{}", row.what);
        }
    }

    #[test]
    fn as_of_heads_are_bounded() {
        let rig = Rig::build();
        as_of_rows(&rig).iter().for_each(Row::print);
    }

    #[test]
    fn maintenance_tick_heads_are_bounded() {
        let rig = Rig::build();
        tick_rows(&rig).iter().for_each(Row::print);
    }

    #[test]
    fn files_reads_no_created_at_unless_asked() {
        let rig = Rig::build();
        let rows = files_rows(&rig);
        rows.iter().for_each(Row::print);
        let heads: Vec<usize> = rows.iter().map(|row| row.counts.head).collect();
        assert_eq!(heads, [0, FILES]);
    }

    /// The record: run with --no-capture to read the table.
    #[test]
    fn request_count_table() {
        let rig = Rig::build();
        let mut rows = vec![
            restore_row(&rig, "restore Latest", RestoreTarget::Latest, TXNS),
            restore_row(
                &rig,
                "restore Txid 4500",
                RestoreTarget::Txid(txid(4500)),
                4500,
            ),
        ];
        rows.extend(as_of_rows(&rig));
        rows.extend(tick_rows(&rig));
        rows.extend([
            verify_row(&rig, Depth::Names),
            verify_row(&rig, Depth::Contents),
        ]);
        rows.extend([boot_row(&rig), recover_row(&rig)]);
        rows.extend(files_rows(&rig));
        print_table(&rows);
    }

    /// A full scan sends one LIST per page of keys: 9000 level-1 files are ten
    /// requests, not the nineteen a page of exactly 1000 cost (one more key
    /// proved the page was not the last, and the next page fetched it again).
    #[test]
    fn a_full_scan_sends_one_list_per_page() {
        let rig = Rig::build();
        let store = rig.store();
        let (loaded, counts) = rig.counted(|| block_on(Inventory::load(&store)));
        assert_eq!(loaded.unwrap().files().len(), FILES);
        // Levels 0, 2, 3, 9 and the five empty ones take one request each.
        assert_eq!(counts.list, 9 + 10);
    }

    /// A transport that sends every batch one request at a time (the trait's
    /// default), the baseline side-by-side sending is compared with.
    struct OneByOne(Shared);

    impl Transport for OneByOne {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            self.0.send(request)
        }
    }

    /// Listing the levels side by side changes nothing but the wall time: the
    /// same inventory from the same requests as one at a time.
    #[test]
    fn levels_listed_at_once_give_the_same_inventory() {
        let rig = Rig::build();
        let (eight, eight_counts) =
            rig.counted(|| block_on(Inventory::load(&rig.store())).unwrap());
        let sequential = S3Store::with_transport(
            config(),
            credentials(),
            OneByOne(Shared(Arc::clone(&rig.wire))),
            FakeClock::new(now()),
        )
        .unwrap();
        let (one, one_counts) = rig.counted(|| block_on(Inventory::load(&sequential)).unwrap());
        assert_eq!(one, eight);
        assert_eq!(one_counts, eight_counts);
    }
}

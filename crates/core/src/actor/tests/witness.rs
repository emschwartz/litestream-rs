//! A staged file reaches the replica only with a witness bound to its bytes
//! (`crate::witness`): made by this process, or matched against the live WAL
//! at boot. A whole, valid file put in its place is refused.

use lsr_ltx::Encoder;

use super::*;

/// Rewrites staged `n` as a whole, valid LTX file under the same key and
/// header, with page 1 filled with `fill`: what a staging backup put back,
/// or any replacement that verifies, leaves. Returns its bytes.
fn replace_staged(rig: &Rig, n: u64, fill: u8) -> Vec<u8> {
    replace_staged_page(rig, n, 1, fill)
}

/// As [`replace_staged`], with page `pgno` filled instead.
fn replace_staged_page(rig: &Rig, n: u64, pgno: u32, fill: u8) -> Vec<u8> {
    let store = &rig.staging.inner;
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(n).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let bytes = block_on(store.read(&key, 0, all)).unwrap();
    let mut decoder = ReadDecoder::new(&bytes[..]);
    let mut pages = Vec::new();
    while let Some(event) = decoder.next_event().unwrap() {
        if let lsr_ltx::Event::Page(page) = event {
            let mut data = page.data().to_vec();
            if page.pgno() == pg(pgno) {
                data.fill(fill);
            }
            pages.push((page.pgno(), data));
        }
    }
    let header = *decoder.finish().unwrap().header();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for (pgno, data) in &pages {
        encoder.write_page(*pgno, data).unwrap();
    }
    let other = encoder.finish(None).unwrap().0;
    assert_ne!(other, bytes);
    block_on(async {
        let mut up = store
            .begin_upload(&key, ts(T0), crate::store::WriteMode::Replace)
            .await
            .unwrap();
        up.write(&other).await.unwrap();
        up.commit().await.unwrap();
    });
    other
}

/// Puts staged 2 under key 3: a whole, valid file, but one that starts where the file before
/// transaction 2 ended, not where the replica's file for 2 ended, so it does not chain.
fn misplace_staged_tail(rig: &Rig) {
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(2).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let two = block_on(rig.staging.inner.read(&key, 0, all)).unwrap();
    put_l0(&rig.staging.inner, 3, &relabelled(&two, 3));
}

/// Whether the replica's level-0 object for `n` is `bytes`.
fn replica_holds(rig: &Rig, n: u64, bytes: &[u8]) -> bool {
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(n).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    block_on(rig.remote.inner.read(&key, 0, all)).is_ok_and(|held| held == bytes)
}

/// A file staged by this engine and replaced, while it waits for its upload,
/// by another valid file under the same key: the upload's own read does not
/// match the witness, the file is dropped like a lost one, and a snapshot
/// takes its place.
#[test]
fn a_staged_file_replaced_under_a_running_engine_is_never_uploaded() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2], T0 + 1);
    assert_eq!(ranges(&rig.remote), vec![(1, 1)]);
    let other = replace_staged(&rig, 2, 0xEE);
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    no_stop(&seen);
    assert!(!replica_holds(&rig, 2, &other), "{seen:?}");
    assert!(seen.iter().any(|tick| matches!(
        event(tick),
        Some(Event::Resnapshot(SnapshotCause::NewestStagedFileUnreadable))
    )));
    // Maintenance may have compacted level 0 meanwhile; the newest state is 2.
    assert_eq!(restored(&rig), 2);
}

/// A plain crash with files pending in the live WAL: every one is witnessed
/// and uploaded with the bytes the engine before the crash staged.
#[test]
fn pending_files_in_the_live_wal_are_uploaded_as_staged() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let before: Vec<Vec<u8>> = Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .map(|info| block_on(rig.staging.inner.read(&info.key, 0, all)).unwrap())
        .collect();
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
    assert!(replica_holds(&rig, 2, &before[0]));
    assert!(replica_holds(&rig, 3, &before[1]));
    assert_eq!(snapshots(&rig), vec![1]);
}

/// A pending file that is torn, with a whole file after it, is never uploaded: the boot
/// reads it whole, finds it does not verify, and drops it with the file after it. The
/// WAL, which holds their frames, stages them again.
#[test]
fn a_torn_pending_file_is_not_uploaded_and_is_staged_again() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(2).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let mut torn = block_on(rig.staging.inner.read(&key, 0, all)).unwrap();
    torn.truncate(torn.len() / 2);
    put_l0(&rig.staging.inner, 2, &torn);
    let mut actor = Actor::new(cfg());
    // The boot's sweep meets the torn file first and doubts that one read, so the boot
    // retries before it goes on.
    rig.tick(&mut actor, T0 + 2);
    let seen = rig.settle(&mut actor, T0 + 1_002);
    no_stop(&seen);
    assert!(!replica_holds(&rig, 2, &torn));
    assert_eq!(restored(&rig), 3);
}

/// The replica's tail and the pending file were staged in a WAL generation
/// the application has since checkpointed away. The pending file holds
/// commits the application made, so it is uploaded as staged, as Go uploads
/// it, and the boot's snapshot (the generation changed) follows it.
#[test]
fn pending_files_from_a_gone_generation_are_uploaded_before_the_snapshot() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2], T0 + 1);
    rig.wal.force_restart();
    rig.commit(3);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::GenerationChanged)
        })
    );
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert!(ranges(&rig.remote).contains(&(2, 2)));
    assert_eq!(restored(&rig), 3);
}

/// The replica's newest level-0 object is torn and its staged copy, which
/// boot's listing would upload over it, does not chain to the file before it: the object stays as
/// listed and is confirmed, and the snapshot takes the transaction after it,
/// never the listed one.
#[test]
fn a_refused_copy_of_a_torn_tail_leaves_the_listed_key_to_the_replica() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    assert_eq!(staged(&rig), vec![2, 3]);
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(3).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let mut torn = block_on(rig.remote.inner.read(&key, 0, all)).unwrap();
    torn.truncate(torn.len() / 2);
    block_on(async {
        let mut up = rig
            .remote
            .inner
            .begin_upload(&key, ts(T0), crate::store::WriteMode::Replace)
            .await
            .unwrap();
        up.write(&torn).await.unwrap();
        up.commit().await.unwrap();
    });
    misplace_staged_tail(&rig);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::StagedUnverified {
                from: Txid::new(3).unwrap()
            })
        })
    );
    rig.settle(&mut actor, T0 + 2);
    assert!(replica_holds(&rig, 3, &torn));
    // The snapshot took 4 (the torn object keeps a full decode of every
    // level-0 file from running here).
    assert!(
        ranges(&rig.remote).contains(&(4, 4)),
        "{:?}",
        ranges(&rig.remote)
    );
}

/// A torn tail behind a listing that lags and leaves its key out, its staged copy not
/// chaining to the file before it, so the boot refuses that copy. Only a read can tell the key is still on the replica; a
/// listing that left it out freed the key for the transactions committed since
/// ("transaction 16 covers commit 45 and then commit 57", which the simulator first found
/// through a forged staged copy).
#[test]
fn a_tail_checkpointed_away_behind_a_lagging_listing_leaves_the_key_to_the_replica() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(3).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let mut torn = block_on(rig.remote.inner.read(&key, 0, all)).unwrap();
    torn.truncate(torn.len() / 2);
    block_on(async {
        let mut up = rig
            .remote
            .inner
            .begin_upload(&key, ts(T0), crate::store::WriteMode::Replace)
            .await
            .unwrap();
        up.write(&torn).await.unwrap();
        up.commit().await.unwrap();
    });
    rig.remote.hidden.lock().unwrap().push(key);
    misplace_staged_tail(&rig);
    rig.wal.force_restart();
    rig.commit(4);
    let mut actor = Actor::new(cfg());
    rig.tick(&mut actor, T0 + 2);
    rig.settle(&mut actor, T0 + 2);
    assert!(
        replica_holds(&rig, 3, &torn),
        "new content took the replica's key"
    );
}

/// The takeover kept before the boot drops a refused staged copy covers the replica's
/// newest key: reads that came back wrong judged that tail torn, so it is not the confirmed
/// position, and a record capped at the confirmed transaction stopped one short of it. Once
/// reads were honest, a later boot refused the engine's own upload (seed 0x3ed in the
/// simulator: "a boot refused replica files through 0x26 with staging intact and no other
/// writer").
#[test]
fn the_takeover_before_a_refused_copy_drop_covers_a_torn_looking_tail() {
    // Enough wrong reads of the tail that every read the boot makes of it comes back wrong.
    const FLIPS: usize = 10;
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(3).unwrap()));
    for _ in 0..FLIPS {
        rig.remote.flip_next_read(key);
    }
    misplace_staged_tail(&rig);
    rig.wal.force_restart();
    rig.commit(4);
    let mut actor = Actor::new(go_like());
    rig.tick(&mut actor, T0 + 2);
    rig.settle(&mut actor, T0 + 2);
    let record = rig.takeover.expect("a takeover was kept before the drop");
    assert!(
        record.covers(Txid::new(3).unwrap()),
        "the record {record:?} stops short of the replica's newest key"
    );
}

/// Every staged file's bytes, by transaction.
fn staged_bytes(rig: &Rig) -> Vec<(u64, Vec<u8>)> {
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .map(|info| {
            let bytes = block_on(rig.staging.inner.read(&info.key, 0, all)).unwrap();
            (info.key.range().max().get(), bytes)
        })
        .collect()
}

/// A crash with the first snapshot staged and not yet uploaded: the boot
/// checks it whole against the live WAL and database file and uploads it
/// as staged, with no snapshot of its own.
#[test]
fn a_pending_snapshot_the_wal_proves_is_uploaded_as_staged() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.settle(&mut actor, T0);
    assert_eq!(ranges(&rig.remote), vec![]);
    let before = staged_bytes(&rig);
    assert_eq!(before.len(), 1);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 1);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, T0 + 1);
    no_stop(&seen);
    assert_eq!(ranges(&rig.remote), vec![(1, 1)]);
    assert!(replica_holds(&rig, 1, &before[0].1));
    assert_eq!(restored(&rig), 1);
}

/// The same crash, but the application then commits a page the snapshot
/// took from the database file: the snapshot is still uploaded as staged,
/// the replica gets exactly one, and the commit follows it.
#[test]
fn a_pending_snapshot_is_uploaded_as_staged_when_the_application_has_committed_since() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.settle(&mut actor, T0);
    let before = staged_bytes(&rig);
    rig.wal.append_tx(&[(pg(2), vec![9; PS.bytes()])], nz(3));
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 1);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, T0 + 1);
    no_stop(&seen);
    assert!(replica_holds(&rig, 1, &before[0].1));
    assert_eq!(snapshots(&rig), vec![1]);
    assert_eq!(ranges(&rig.remote).first(), Some(&(1, 1)));
    assert_eq!(restored_page(&rig, 2), 9);
}

/// `bytes` re-encoded as the file for transaction `n`, pages and the rest of
/// the header unchanged.
fn relabelled(bytes: &[u8], n: u64) -> Vec<u8> {
    let mut decoder = ReadDecoder::new(bytes);
    let mut pages = Vec::new();
    while let Some(event) = decoder.next_event().unwrap() {
        if let lsr_ltx::Event::Page(page) = event {
            pages.push((page.pgno(), page.data().to_vec()));
        }
    }
    let mut fields = decoder.finish().unwrap().header().fields();
    fields.txids = TxidRange::single(Txid::new(n).unwrap());
    let mut encoder = Encoder::new(Vec::new(), lsr_ltx::Header::new(fields).unwrap()).unwrap();
    for (pgno, data) in &pages {
        encoder.write_page(*pgno, data).unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// A power loss under `synchronous=NORMAL` dropped the frames of a staged,
/// unpublished transaction: SQLite's committed end is back before them,
/// although they still sit in the file and verify. The staged file holds a
/// commit the application made and the disk lost; it is uploaded as staged,
/// as Go uploads it, and the boot's snapshot of the database follows.
#[test]
fn a_staged_file_the_disk_lost_the_frames_of_is_uploaded_before_the_snapshot() {
    let (mut rig, mut actor) = caught_up();
    let before = rig.wal.committed_end();
    stage_held(&mut rig, &mut actor, &[2], T0 + 1);
    let pending = staged_bytes(&rig);
    let two = pending.iter().find(|(n, _)| *n == 2).unwrap().1.clone();
    // The wal-index after recovery: the commit of transaction 2 is gone.
    let read = CommittedRead::new(before, 0, rig.wal.epoch()).unwrap();
    rig.wal.script_committed(Some(read));
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert!(
        matches!(
            event(&boot),
            Some(Event::Booted {
                cause: Some(SnapshotCause::LocalRewound { .. })
            })
        ),
        "{boot:?}"
    );
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert!(replica_holds(&rig, 2, &two));
    assert_eq!(snapshots(&rig), vec![1, 3]);
}

/// Writes `bytes` under level-0 key `n` in `store`, replacing what is there.
fn put_l0(store: &MemStore, n: u64, bytes: &[u8]) {
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(n).unwrap()));
    block_on(async {
        let mut up = store
            .begin_upload(&key, ts(T0), crate::store::WriteMode::Replace)
            .await
            .unwrap();
        up.write(bytes).await.unwrap();
        up.commit().await.unwrap();
    });
}

/// A pending file must start where the file before it ended. One that
/// names the next transaction but whose range starts further on (here the
/// file for transaction 4, put back under transaction 3) skips a commit,
/// though the WAL holds every page of its own range: it is refused, and the
/// WAL stages the skipped commit again.
#[test]
fn a_pending_file_that_skips_a_range_is_refused() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    stage_held(&mut rig, &mut actor, &[3, 4], T0 + 1);
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let four = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(4).unwrap()));
    let bytes = block_on(rig.staging.inner.read(&four, 0, all)).unwrap();
    block_on(rig.staging.inner.delete(&[four])).unwrap();
    let skipping = relabelled(&bytes, 3);
    put_l0(&rig.staging.inner, 3, &skipping);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert!(!replica_holds(&rig, 3, &skipping));
    assert_eq!(restored(&rig), 4);
}

/// A pending snapshot that reaches exactly as far as the replica's newest
/// file (an operator's snapshot with nothing committed since) is the same
/// state, not an earlier one: it is proven and uploaded as staged.
#[test]
fn a_pending_snapshot_as_far_as_the_replicas_newest_is_uploaded() {
    let (mut rig, mut actor) = caught_up();
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    actor.request_snapshot();
    rig.settle(&mut actor, T0 + 1);
    let pending = staged_bytes(&rig);
    let (n, snapshot) = pending.last().unwrap().clone();
    assert_eq!(n, 2);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert!(replica_holds(&rig, 2, &snapshot));
}

/// `bytes` re-encoded with `edit` applied to every page and then `fields`.
fn reencoded(
    bytes: &[u8],
    mut fields_edit: impl FnMut(&mut lsr_ltx::HeaderFields),
    extra: &[(PageNo, Vec<u8>)],
) -> Vec<u8> {
    let mut decoder = ReadDecoder::new(bytes);
    let mut pages = Vec::new();
    while let Some(event) = decoder.next_event().unwrap() {
        if let lsr_ltx::Event::Page(page) = event {
            pages.push((page.pgno(), page.data().to_vec()));
        }
    }
    pages.extend_from_slice(extra);
    pages.sort_by_key(|(pgno, _)| *pgno);
    let mut fields = decoder.finish().unwrap().header().fields();
    fields_edit(&mut fields);
    let mut encoder = Encoder::new(Vec::new(), lsr_ltx::Header::new(fields).unwrap()).unwrap();
    for (pgno, data) in &pages {
        encoder.write_page(*pgno, data).unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// A pending file that neither starts where the file before it ended nor
/// at the live generation's first frame is refused, whatever it holds: here
/// an incremental filled out to every page, each the database's.
#[test]
fn a_full_file_that_neither_follows_nor_starts_the_generation_is_refused() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    stage_held(&mut rig, &mut actor, &[3, 4], T0 + 1);
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let four = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(4).unwrap()));
    let bytes = block_on(rig.staging.inner.read(&four, 0, all)).unwrap();
    block_on(rig.staging.inner.delete(&[four])).unwrap();
    let rest: Vec<(PageNo, Vec<u8>)> = (2..=3)
        .map(|n| (pg(n), vec![u8::try_from(n).unwrap(); PS.bytes()]))
        .collect();
    let full = reencoded(
        &bytes,
        |fields| fields.txids = TxidRange::single(Txid::new(3).unwrap()),
        &rest,
    );
    put_l0(&rig.staging.inner, 3, &full);
    let mut actor = Actor::new(cfg());
    rig.tick(&mut actor, T0 + 2);
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert!(!replica_holds(&rig, 3, &full));
    assert_eq!(restored(&rig), 4);
}

/// The first byte of page `pgno` in the replica's newest state.
fn restored_page(rig: &Rig, pgno: u32) -> u8 {
    let sink = MemSink::new();
    block_on(restore(
        &rig.remote.inner,
        RestoreTarget::Latest,
        sink.clone(),
    ))
    .unwrap();
    sink.published().unwrap().page(pgno).unwrap()[0]
}

/// A power loss on a disk that lies about the engine's WAL barrier drops the frames of the
/// replica's newest file (still in staging) and of the pending file after it. The application
/// comes back and writes other bytes into the first file's frames, then the second job again
/// verbatim. The newest staged file matches the WAL, but the file below it does not: the
/// boot compares it too, takes a snapshot, and a restore returns what the database holds.
#[test]
fn a_staged_file_below_the_newest_that_the_wal_rewrote_costs_a_snapshot() {
    let (mut rig, mut actor) = caught_up();
    let kept = rig.wal.committed_end().frame_count();
    rig.wal.append_tx(&[(pg(2), vec![0xA0; PS.bytes()])], nz(3));
    rig.settle(&mut actor, T0 + 1);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2)]);
    let job = [(pg(1), vec![0xC0; PS.bytes()])];
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.wal.append_tx(&job, nz(3));
    rig.settle(&mut actor, T0 + 1);
    assert_eq!(staged(&rig).last(), Some(&3));
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2)]);
    rig.wal.lose_frames_from(kept);
    rig.wal.append_tx(&[(pg(2), vec![0xB0; PS.bytes()])], nz(3));
    rig.wal.append_tx(&job, nz(3));
    let mut actor = Actor::new(cfg());
    rig.tick(&mut actor, T0 + 2);
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert_eq!(
        (restored_page(&rig, 1), restored_page(&rig, 2)),
        (0xC0, 0xB0)
    );
}

/// A replica outage longer than the witness cap: the engine stops staging at
/// the cap and holds its position (the WAL keeps the frames) instead of
/// staging a file no token vouches for, which the uploader would refuse and a
/// snapshot would replace. When the replica returns, the pending files drain,
/// staging resumes from the held position, and nothing is snapshotted.
#[test]
fn a_replica_outage_past_the_witness_cap_holds_the_position_and_costs_no_snapshot() {
    let (mut rig, mut actor) = caught_up();
    let cap = crate::witness::MAX_WITNESSED;
    // The last commit's byte appears nowhere else, so restoring it means it
    // reached the replica.
    let mut commits: Vec<u8> = (0..cap.saturating_add(5))
        .map(|n| u8::try_from(n % 200).unwrap().saturating_add(2))
        .collect();
    commits.push(255);
    stage_held(&mut rig, &mut actor, &commits, T0 + 1);
    assert!(staged(&rig).len() <= cap, "{} staged", staged(&rig).len());
    assert!(actor.engine.witnesses.len() <= cap);
    let last = *commits.last().unwrap();
    let mut seen = Vec::new();
    let mut now = T0 + 10_000;
    while restored(&rig) != last {
        let more = rig.follow(&mut actor, now, 20_000);
        assert!(!more.is_empty());
        seen.extend(more);
        now = now.saturating_add(10_000);
        assert!(now < T0 + 10_000_000, "never drained: {:?}", seen.last());
    }
    no_stop(&seen);
    assert_eq!(restored(&rig), last);
    assert!(
        !seen.iter().any(|tick| matches!(
            event(tick),
            Some(Event::Resnapshot(_) | Event::Booted { .. })
        )),
        "{:?}",
        seen.iter()
            .filter(|tick| matches!(event(tick), Some(Event::Resnapshot(_))))
            .collect::<Vec<_>>()
    );
}

/// A staging directory holding more pending files than the witness cap (left
/// by Go, which has no cap, or by an older build): boot witnesses up to the
/// cap and deletes the rest as it deletes a file the WAL does not vouch for.
/// The WAL still holds their frames, so they are staged again once uploads
/// make room, and nothing is snapshotted.
#[test]
fn pending_files_past_the_cap_at_boot_are_regenerated_from_the_wal() {
    let (mut rig, mut actor) = caught_up();
    let cap = crate::witness::MAX_WITNESSED;
    let fill = |n: usize| u8::try_from(n % 200).unwrap().saturating_add(2);
    let first: Vec<u8> = (0..cap).map(fill).collect();
    stage_held(&mut rig, &mut actor, &first, T0 + 1);
    // Forget the tokens, so this engine stages past the cap as one without
    // one would.
    actor.engine.witnesses = Witnesses::new();
    let more: Vec<u8> = (0..10).map(fill).chain([255]).collect();
    stage_held(&mut rig, &mut actor, &more, T0 + 1);
    let pending = staged(&rig).len().saturating_sub(1);
    assert!(pending > cap, "{pending} pending");
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    assert!(actor.engine.witnesses.len() <= cap);
    assert!(staged(&rig).len() <= cap.saturating_add(1));
    let mut seen = Vec::new();
    let mut now = T0 + 10_000;
    while restored(&rig) != 255 {
        seen.extend(rig.follow(&mut actor, now, 20_000));
        now = now.saturating_add(10_000);
        assert!(now < T0 + 10_000_000, "never drained: {:?}", seen.last());
    }
    no_stop(&seen);
    assert!(
        !seen
            .iter()
            .any(|tick| matches!(event(tick), Some(Event::Resnapshot(_)))),
        "a snapshot was taken"
    );
}

/// One read of a pending file that answers with torn bytes (a lying read; the stored bytes
/// are the engine's): the first pass refuses the file and the second accepts it. Passes that
/// disagree prove nothing, so the boot deletes nothing and retries (`LocalIo`); the next
/// boot reads the stored bytes and uploads them.
#[test]
fn witness_passes_that_disagree_retry_the_boot() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(3).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let stored = block_on(rig.staging.inner.read(&key, 0, all)).unwrap();
    let torn = stored[..stored.len() / 2].to_vec();
    // The boot reads the newest staged file once before it checks, so the second read
    // from its start is the first pass's.
    rig.staging.other_bytes_on_read(key, 2, torn);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert!(
        matches!(
            event(&boot),
            Some(Event::Retrying {
                fault: Fault::LocalIo,
                ..
            })
        ),
        "{boot:?}"
    );
    assert_eq!(
        rig.staging.reads_of(&key),
        3,
        "the boot ran two witness passes"
    );
    assert_eq!(staged(&rig), vec![2, 3], "the boot deleted nothing");
    let seen = rig.settle(&mut actor, T0 + 1_002);
    no_stop(&seen);
    assert!(replica_holds(&rig, 3, &stored));
}

/// Pending files 2 and 3 staged and held; the `nth` read of staged 3 from its start answers
/// with another valid file, the stored bytes untouched. Returns the rig, the boot's first
/// tick, the boot's later ticks and the stored bytes of 3.
fn boot_with_one_lying_read(nth: usize) -> (Rig, Tick, Vec<Tick>, Vec<u8>) {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(3).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let stored = block_on(rig.staging.inner.read(&key, 0, all)).unwrap();
    let other = replace_staged(&rig, 3, 0xEE);
    put_l0(&rig.staging.inner, 3, &stored);
    rig.staging.other_bytes_on_read(key, nth, other);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(staged(&rig), vec![2, 3], "the boot deleted nothing");
    let seen = rig.settle(&mut actor, T0 + 1_002);
    (rig, boot, seen, stored)
}

/// The boot's first two reads of the newest staged file are the sweep's and the witness
/// passes'; its third is recovery's read of the file's header. One lie there made the file
/// look unreadable, and the boot snapshotted (`NewestStagedFileUnreadable`) on one failed
/// read. Now that read is doubted: the boot fails with `StagedLost`, the next boot reads the
/// stored bytes, continues without a snapshot and uploads them.
#[test]
fn one_lying_read_of_the_newest_staged_header_retries_the_boot() {
    let (rig, boot, seen, stored) = boot_with_one_lying_read(3);
    let retried = matches!(
        event(&boot),
        Some(Event::Retrying {
            fault: Fault::StagedLost,
            ..
        })
    );
    assert!(retried, "{boot:?}");
    no_stop(&seen);
    let booted = seen.iter().find_map(|tick| match event(tick) {
        Some(Event::Booted { cause }) => Some(*cause),
        Some(_) | None => None,
    });
    assert_eq!(booted, Some(None), "the next boot continues: {seen:?}");
    assert!(replica_holds(&rig, 3, &stored));
}

/// The fourth read is recovery's scan of the file's pages: one lie there made its last page
/// disagree with the WAL's, and the boot snapshotted (`LastPageMismatch`). It is doubted
/// the same way.
#[test]
fn one_lying_read_of_the_newest_staged_pages_retries_the_boot() {
    let (rig, boot, seen, stored) = boot_with_one_lying_read(4);
    let retried = matches!(
        event(&boot),
        Some(Event::Retrying {
            fault: Fault::StagedLost,
            ..
        })
    );
    assert!(retried, "{boot:?}");
    no_stop(&seen);
    let booted = seen.iter().find_map(|tick| match event(tick) {
        Some(Event::Booted { cause }) => Some(*cause),
        Some(_) | None => None,
    });
    assert_eq!(booted, Some(None), "the next boot continues: {seen:?}");
    assert!(replica_holds(&rig, 3, &stored));
}

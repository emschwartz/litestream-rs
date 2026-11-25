//! A replica store that lets go of what it is handed and reads the staged file again
//! (`docs/decisions/D222.md`): what a second read must give before a byte of it is used,
//! and whose fault a second read that gives nothing is.

use super::slices::{QUARTER_MIB, finish, staged_big, step};
use super::*;
use crate::mem::reread::{Rereads, When};

/// The reads that take `bytes` a chunk at a time.
fn chunks(bytes: &[u8]) -> u32 {
    u32::try_from(bytes.len().div_ceil(256 * 1024)).unwrap()
}

/// The big staged file behind a probe that counts and scripts its reads.
fn probed() -> (Probe, Witnesses, Vec<u8>) {
    let (inner, witnesses, bytes) = staged_big();
    let staging = Probe {
        inner,
        ..Probe::default()
    };
    (staging, witnesses, bytes)
}

/// A read the staging directory refuses: its permissions, where the same refusal from the
/// replica would be a credential.
fn refused_read() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Read,
        kind: ErrorKind::PermissionDenied,
    }
}

#[test]
fn a_store_that_reads_the_staged_file_again_publishes_what_the_send_verified() {
    for when in [When::Commit, When::Write] {
        let (staging, witnesses, bytes) = probed();
        let remote = Rereads::reading_at(when);
        let put = next_with(UploadState::At(None), &staging, &remote, 1, &witnesses).unwrap();
        assert!(matches!(put.did, Did::Put(_)), "{when:?}");
        assert_eq!(read_all(&remote.inner, &key(1)), bytes, "{when:?}");
        // Every chunk once to verify it and once more for the store.
        assert_eq!(staging.log().reads, 2 * chunks(&bytes), "{when:?}");
    }
}

/// The staged file changes on disk behind the send: a chunk already read, verified and
/// handed over. The first reads still add up to the witnessed file, so only the second read
/// can tell, and it must: the changed bytes never reach the replica.
#[test]
fn a_staged_file_changed_behind_the_send_is_lost_and_nothing_is_published() {
    let (staging, witnesses, bytes) = staged_big();
    let remote = Rereads::default();
    let stores = (&staging, &remote);
    let state = UploadState::At(None);
    let mut in_flight = InFlight::default();
    let (first, _) = step(state, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    assert_eq!(first, Ok(None));
    let mut changed = bytes;
    changed[1000] ^= 0x40;
    put_bytes(&staging, key(1), &changed);
    let (ended, _) = finish(state, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    let failed = ended.unwrap_err();
    assert_eq!(failed.fault, Fault::StagedLost);
    // Nothing was published, so no listing is owed and nothing is deleted.
    assert_eq!(failed.state, state);
    assert!(files(&remote.inner).is_empty());
    assert_eq!(in_flight.sent(), None);
}

/// The same inside a write: a store whose batch of parts filled reads back at once.
#[test]
fn a_second_read_that_differs_inside_a_write_ends_the_send_as_a_lost_staged_file() {
    let (staging, witnesses, _) = probed();
    // The second read from the file's start is the store's.
    staging.log().flip_whole_reads = vec![2];
    let remote = Rereads::reading_at(When::Write);
    let failed = failure(next_with(
        UploadState::At(None),
        &staging,
        &remote,
        1,
        &witnesses,
    ));
    assert_eq!(failed.fault, Fault::StagedLost);
    assert_eq!(failed.state, UploadState::At(None));
    assert!(files(&remote.inner).is_empty());
}

/// A second read the staging directory fails is the staging directory's fault, though the
/// replica's store is what reports it, and it costs no listing: nothing was published.
#[test]
fn a_second_read_that_fails_is_the_staging_directorys_fault() {
    let err = refused_read();
    let ours = Staging::<MemStore>::fault(&err);
    assert_ne!(ours, Replica::<MemStore>::fault(&err), "the test's error");
    for when in [When::Commit, When::Write] {
        let (staging, witnesses, bytes) = probed();
        let nth = match when {
            When::Commit => chunks(&bytes) + 1,
            When::Write => 2,
        };
        staging.log().fail_read = Some((nth, err.clone()));
        let remote = Rereads::reading_at(when);
        let failed = failure(next_with(
            UploadState::At(None),
            &staging,
            &remote,
            1,
            &witnesses,
        ));
        assert_eq!(failed.fault, ours, "{when:?}");
        assert_eq!(failed.state, UploadState::At(None), "{when:?}");
        assert!(files(&remote.inner).is_empty(), "{when:?}");
    }
}

/// The store reports the first failure in part order. When that is its own (an earlier
/// part's request), a second read that also gave nothing does not make it staging's: the
/// commit's outcome is unknown, as any failed commit's is, and a listing settles it.
#[test]
fn a_commit_the_store_failed_for_its_own_reason_is_the_replicas_fault() {
    let (staging, witnesses, bytes) = probed();
    staging.log().fail_read = Some((chunks(&bytes) + 1, refused_read()));
    let remote = Rereads::default();
    remote.asked().own = Some(transient());
    let failed = failure(next_with(
        UploadState::At(None),
        &staging,
        &remote,
        1,
        &witnesses,
    ));
    assert_eq!(failed.fault, Replica::<MemStore>::fault(&transient()));
    assert_eq!(failed.state, UploadState::ListFirst { confirmed: None });
}

/// The source itself: one bounded read of the staging directory, given back only when it
/// tallies, and a memory of what it refused.
#[test]
fn a_second_read_is_one_bounded_read_that_must_tally() {
    let (staging, _, bytes) = staged_big();
    let role = Staging(&staging);
    let again = Staged::of(&role, key(1));
    let tally = |bytes: &[u8]| {
        let mut tally = ByteTally::new();
        tally.add(bytes);
        tally
    };
    let was = tally(&bytes[..1000]);
    assert_eq!(block_on(again.read(0, &was)).unwrap(), bytes[..1000]);
    assert!(!again.gave(&Unread::Changed.error()), "nothing refused yet");

    // The same count from another offset is other bytes.
    assert_eq!(block_on(again.read(1, &was)), Err(Unread::Changed.error()));
    assert!(again.gave(&Unread::Changed.error()));
    assert!(!again.gave(&transient()));
    let fault = |err: &StoreError| again.fault::<Replica<'_, MemStore>>(err);
    assert_eq!(fault(&Unread::Changed.error()), Fault::StagedLost);
    assert_eq!(
        fault(&transient()),
        Replica::<MemStore>::fault(&transient())
    );

    // A tally of no bytes, or of more than one read may ask for, is not a read at all.
    let asked_wrong = StoreError::Refused {
        op: StoreOp::Read,
        status: 0,
    };
    let most = usize::try_from(ReadLen::MAX_RAW).unwrap();
    assert_eq!(
        block_on(again.read(0, &ByteTally::new())),
        Err(asked_wrong.clone())
    );
    assert_eq!(
        block_on(again.read(0, &tally(&vec![0; most + 1]))),
        Err(asked_wrong.clone())
    );
    assert_eq!(
        fault(&asked_wrong),
        Staging::<MemStore>::fault(&asked_wrong)
    );
    // The largest read is one: the file is shorter, so what it gives does not tally.
    assert_eq!(
        block_on(again.read(0, &tally(&vec![0; most]))),
        Err(Unread::Changed.error())
    );
}

#[test]
fn the_reads_that_gave_nothing_are_remembered_up_to_a_bound() {
    let (staging, _, _) = staged_big();
    let role = Staging(&staging);
    let again = Staged::of(&role, key(1));
    for _ in 0..UNREAD_KEPT + 5 {
        block_on(again.read(0, &ByteTally::new())).unwrap_err();
    }
    assert_eq!(again.unread.lock().unwrap().len(), UNREAD_KEPT);
}

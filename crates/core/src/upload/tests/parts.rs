//! A replica store whose upload keeps parts in flight between two steps
//! (`docs/decisions/D233.md`): what a step does when the upload has no room, what the end
//! of the file waits for, and where a part's late failure comes out.

use super::reread::{chunks, probed, refused_read};
use super::slices::{QUARTER_MIB, finish, step};
use super::*;
use crate::mem::reread::{Rereads, When};
use crate::store::{Flight, Rest};

/// A step's bytes that never run out.
const WHOLE: u64 = u64::MAX;

/// An upload with a whole part waiting takes nothing: the step that finds it so reads no
/// byte of the staged file and hands its bytes for the tick back unspent, and the send says
/// it waits. A step that read before the upload filled is a slice like any other.
#[test]
fn a_step_whose_upload_has_no_room_reads_nothing_and_keeps_its_bytes() {
    let (staging, witnesses, bytes) = probed();
    assert!(chunks(&bytes) >= 4, "the test's file");
    let remote = Rereads::default();
    let stores = (&staging, &remote);
    let state = UploadState::At(None);
    let mut in_flight = InFlight::default();
    // The first read begins the upload; nothing is asked of an upload not begun.
    let (first, left) = step(state, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    assert_eq!((first, left), (Ok(None), 0));
    assert!(remote.asked().rests.is_empty());
    assert!(!in_flight.waits());

    remote
        .asked()
        .flights
        .extend([Ok(Flight::Full), Ok(Flight::Room), Ok(Flight::Full)]);
    let read = staging.log().held_reads;
    let two = 2 * QUARTER_MIB;
    let (full, left) = step(state, stores, &witnesses, &mut in_flight, two);
    assert_eq!((full, left), (Ok(None), two));
    assert_eq!(staging.log().held_reads, read);
    assert_eq!(in_flight.sent(), Some(QUARTER_MIB));
    assert!(in_flight.waits());

    // Room for one read, and then none: the step read what there was room for.
    let (some, left) = step(state, stores, &witnesses, &mut in_flight, two);
    assert_eq!((some, left), (Ok(None), QUARTER_MIB));
    assert_eq!(staging.log().held_reads, read + 1);
    assert_eq!(in_flight.sent(), Some(two));
    assert!(in_flight.waits());

    // Its parts answered, the file goes on to its end.
    let (ended, _) = finish(state, stores, &witnesses, &mut in_flight, WHOLE);
    assert!(matches!(ended.unwrap().did, Did::Put(_)));
    assert!(!in_flight.waits());
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    // Asked before every read but the first, and once at the end.
    let rests = remote.asked().rests.clone();
    assert_eq!(rests.last(), Some(&Rest::None));
    assert_eq!(rests.iter().filter(|rest| **rest == Rest::None).count(), 1);
}

/// The staged file is read to its end and verified with parts still in flight. Nothing is
/// committed until the upload says every part is answered, and the steps that wait for that
/// read nothing more.
#[test]
fn a_file_read_to_its_end_is_committed_only_when_every_part_is_answered() {
    let (staging, witnesses, bytes) = probed();
    let remote = Rereads::default();
    let stores = (&staging, &remote);
    let state = UploadState::At(None);
    let mut in_flight = InFlight::default();
    // One question before each read after the first, then the ones at the end: parts in
    // flight with room, and a part that waits, are both not yet.
    let before_reads = usize::try_from(chunks(&bytes)).unwrap() - 1;
    {
        let mut asked = remote.asked();
        asked
            .flights
            .extend(std::iter::repeat_n(Ok(Flight::Room), before_reads));
        asked.flights.extend([Ok(Flight::Room), Ok(Flight::Full)]);
    }
    let len = u64::try_from(bytes.len()).unwrap();
    for _ in 0..2 {
        let (waiting, _) = step(state, stores, &witnesses, &mut in_flight, WHOLE);
        assert_eq!(waiting, Ok(None));
        assert_eq!(in_flight.sent(), Some(len));
        assert!(in_flight.waits());
        assert!(files(&remote.inner).is_empty(), "nothing is published yet");
        // Each chunk once: the file is not read again while its parts are waited for.
        assert_eq!(staging.log().held_reads, chunks(&bytes));
    }
    let (ended, _) = step(state, stores, &witnesses, &mut in_flight, WHOLE);
    assert!(matches!(ended.unwrap().unwrap().did, Did::Put(_)));
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    assert_eq!(in_flight.sent(), None);
    let rests = remote.asked().rests.clone();
    assert_eq!(rests.len(), before_reads + 3);
    assert_eq!(rests[before_reads..], [Rest::None; 3]);
}

/// A part's request that failed while the engine did other work comes out of the next
/// question, between two reads or at the file's end: the replica's fault, the upload
/// aborted, nothing published and the state as it stood.
#[test]
fn a_part_that_failed_in_flight_fails_the_send_at_the_next_question() {
    for at_the_end in [false, true] {
        let (staging, witnesses, bytes) = probed();
        let remote = Rereads::default();
        let stores = (&staging, &remote);
        let state = UploadState::At(None);
        let mut in_flight = InFlight::default();
        let before = if at_the_end {
            usize::try_from(chunks(&bytes)).unwrap() - 1
        } else {
            1
        };
        {
            let mut asked = remote.asked();
            asked
                .flights
                .extend(std::iter::repeat_n(Ok(Flight::Room), before));
            asked.flights.push_back(Err(transient()));
        }
        let (ended, _) = finish(state, stores, &witnesses, &mut in_flight, WHOLE);
        let failed = ended.unwrap_err();
        assert_eq!(failed.fault, Fault::StoreTransient, "{at_the_end}");
        assert_eq!(failed.state, state);
        assert!(files(&remote.inner).is_empty());
        assert_eq!(remote.inner.scratch(), 0, "the upload was aborted");
        assert_eq!(in_flight.sent(), None);
        assert!(!in_flight.waits());
        let last = if at_the_end { Rest::None } else { Rest::More };
        assert_eq!(remote.asked().rests.last(), Some(&last));
    }
}

/// A part read back while the engine did other work, and the staged file had changed or
/// the staging directory refused the read: the store reports it at the next question, and
/// the fault is the staged file's or the staging directory's, never the replica's.
#[test]
fn a_part_whose_second_read_gave_nothing_in_flight_is_not_the_replicas_fault() {
    let err = refused_read();
    let ours = Staging::<MemStore>::fault(&err);
    assert_ne!(ours, Replica::<MemStore>::fault(&err), "the test's error");
    for (changed, fault) in [(true, Fault::StagedLost), (false, ours)] {
        let (staging, witnesses, _) = probed();
        // The second read through a held file is the store's, of the first chunk.
        if changed {
            staging.log().flip_held_read = Some(2);
        } else {
            staging.log().fail_held_read = Some((2, err.clone()));
        }
        let remote = Rereads::reading_at(When::Flight);
        let stores = (&staging, &remote);
        let state = UploadState::At(None);
        let mut in_flight = InFlight::default();
        let (ended, slices) = finish(state, stores, &witnesses, &mut in_flight, WHOLE);
        let failed = ended.unwrap_err();
        assert_eq!(failed.fault, fault);
        assert_eq!(slices, 0);
        assert_eq!(failed.state, state);
        assert!(files(&remote.inner).is_empty());
        assert_eq!(remote.inner.scratch(), 0);
    }
}

/// A staging store with nothing to hold open hands the upload its bytes to keep: no source
/// to read again is given, and the file goes up whole.
#[test]
fn a_staging_store_that_holds_nothing_open_hands_the_upload_its_bytes() {
    let (staging, witnesses, bytes) = probed();
    staging.log().hold_open = false;
    let remote = Rereads::reading_at(When::Flight);
    let stores = (&staging, &remote);
    let mut in_flight = InFlight::default();
    let (ended, _) = finish(
        UploadState::At(None),
        stores,
        &witnesses,
        &mut in_flight,
        QUARTER_MIB,
    );
    assert!(matches!(ended.unwrap().did, Did::Put(_)));
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    assert_eq!(staging.log().opens, 0);
    assert_eq!(staging.log().held_reads, 0);
    // Asked about its parts all the same.
    assert_eq!(remote.asked().rests.last(), Some(&Rest::None));
}

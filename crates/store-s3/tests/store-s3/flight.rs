//! Parts in flight (`docs/decisions/D233.md`): an upload starts each part when it is whole
//! and goes on building the next, bounded by a count; the order parts end in is no part of
//! the result; a part that fails with others in flight ends the upload and the rest are not
//! sent; and no upload id is completed, aborted or swept with a part of it still started.
//!
//! The lanes here are [`Manual`]: a part started is in flight until the test lands it, or
//! until the upload has to wait for one and lands the one the test's pick names.

use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use lsr_core::conformance::block_on;
use lsr_core::{FakeClock, Flight, ObjectStore, Rest, StoreError, StoreOp, Upload};
use lsr_store_s3::{
    FakeS3, Fault, FaultTransport, HttpRequest, HttpResponse, Lanes, Manual, PartSize, S3Store,
    S3Upload, Shape, Transport, TransportError, object_key,
};

use crate::common::*;
use crate::rig::{count, shapes};
use crate::sourced::{CELL, PART, Source, begin, body};

/// The fault layer over the fake, with lanes the test drives.
struct Laned {
    wire: Arc<FaultTransport<FakeS3>>,
    lanes: Arc<Manual>,
}

impl Transport for Laned {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.wire.send(request)
    }

    fn lanes(&self, _most: NonZeroUsize) -> Arc<dyn Lanes> {
        Arc::<Manual>::clone(&self.lanes)
    }
}

struct Rig {
    wire: Arc<FaultTransport<FakeS3>>,
    lanes: Arc<Manual>,
    store: S3Store<Laned>,
}

/// A store of 5 MiB parts, `width` in flight at most, whose waiting callers land the part
/// `pick` names.
fn rig(width: usize, pick: impl FnMut(usize) -> usize + Send + 'static) -> Rig {
    let config = config()
        .with_part_size(PartSize::MIN)
        .with_concurrency(NonZeroUsize::new(width).unwrap());
    let wire = Arc::new(FaultTransport::new(FakeS3::new(
        &config,
        FakeClock::new(now()),
    )));
    let lanes = Arc::new(Manual::new(pick));
    let transport = Laned {
        wire: Arc::clone(&wire),
        lanes: Arc::clone(&lanes),
    };
    let store =
        S3Store::with_transport(config, credentials(), transport, FakeClock::new(now())).unwrap();
    Rig { wire, lanes, store }
}

/// The oldest part started lands first.
fn oldest(_started: usize) -> usize {
    0
}

/// The newest part started lands first.
fn newest(started: usize) -> usize {
    started.saturating_sub(1)
}

impl Rig {
    /// An upload of `parts` whole parts of `bytes`, written from `source` a cell at a time
    /// while the upload says a write will not wait.
    fn started(&self, bytes: &[u8], source: &Source, parts: usize) -> S3Upload<Laned> {
        let mut upload = begin(&self.store, &key(0, 1, 1));
        for chunk in bytes[..parts.saturating_mul(PART)].chunks(CELL) {
            assert_ne!(block_on(upload.flight(Rest::More)), Ok(Flight::Full));
            block_on(upload.write_from(chunk, source)).unwrap();
        }
        upload
    }

    fn stored(&self) -> Option<Vec<u8>> {
        self.wire
            .inner()
            .body_of(&object_key(config().prefix(), &key(0, 1, 1)))
    }

    fn parts_sent(&self) -> usize {
        count(&self.wire, Shape::UploadPart)
    }
}

/// A request that fails.
fn reset() -> Fault {
    Fault::Transport {
        on: Shape::UploadPart,
        err: TransportError::Io(ErrorKind::ConnectionReset),
    }
}

fn was_reset() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Upload,
        kind: ErrorKind::ConnectionReset,
    }
}

/// A part is started the moment it is whole, and the write that made it whole does not wait
/// for it: nothing of it is on the wire when the write returns, and the upload goes on.
#[test]
fn a_part_is_started_when_it_is_whole_and_the_write_does_not_wait() {
    let rig = rig(3, oldest);
    let bytes = body(2 * PART);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 2);
    assert_eq!((upload.in_flight(), rig.lanes.started()), (2, 2));
    assert_eq!(rig.parts_sent(), 0);
    assert_eq!(source.reads(), 0, "the sender reads a part, not the write");
    assert_eq!(block_on(upload.flight(Rest::More)), Ok(Flight::Room));

    // An answer is collected by the next call, whichever part it is of.
    assert!(rig.lanes.land(1));
    assert_eq!(rig.parts_sent(), 1);
    assert_eq!(upload.in_flight(), 2, "not collected yet");
    assert_eq!(block_on(upload.flight(Rest::More)), Ok(Flight::Room));
    assert_eq!(upload.in_flight(), 1);
    block_on(upload.commit()).unwrap();
    assert!(rig.stored().as_deref() == Some(&bytes[..]));
}

/// The count bound: no more parts of an upload are started than the setting, one whole part
/// more waits, and the upload says so before the write that would have to wait. A write made
/// all the same waits for a part to end, and lands it.
#[test]
fn no_more_parts_are_in_flight_than_the_setting_and_one_waits() {
    let most = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&most);
    let rig = rig(2, move |started| {
        seen.fetch_max(started, Ordering::Relaxed);
        0
    });
    let bytes = body(5 * PART);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 3);
    assert_eq!((upload.in_flight(), rig.lanes.started()), (2, 2));
    assert_eq!(block_on(upload.flight(Rest::More)), Ok(Flight::Full));
    assert_eq!(rig.parts_sent(), 0, "asking waits for nothing");

    // A fourth part's worth all the same: the write lands a part to make room.
    block_on(upload.write_from(&bytes[3 * PART..4 * PART], &source)).unwrap();
    assert_eq!(rig.parts_sent(), 1);
    assert_eq!((upload.in_flight(), rig.lanes.started()), (2, 2));
    block_on(upload.write_from(&bytes[4 * PART..], &source)).unwrap();
    block_on(upload.commit()).unwrap();
    assert_eq!(most.load(Ordering::Relaxed), 2, "the most ever started");
    assert_eq!(rig.parts_sent(), 5);
    assert!(rig.stored().as_deref() == Some(&bytes[..]));
    assert_eq!(source.opens(), 5, "a reader for each part");
}

/// A buffered upload (a level-9 snapshot's) asked before each write says it is full before the
/// write that would fill a part with every lane taken, so no write it let through waits for a
/// part. Such a write used to wait for one on the engine's thread, and the engine synced and
/// uploaded nothing of its database until the part ended.
#[test]
fn a_buffered_write_let_through_never_waits_for_a_part() {
    let rig = rig(1, oldest);
    let bytes = body(3 * PART + 100_000);
    let mut upload = begin(&rig.store, &key(0, 1, 1));
    let mut landed = 0;
    for chunk in bytes.chunks(300_000) {
        while block_on(upload.flight(Rest::More)) == Ok(Flight::Full) {
            assert!(rig.lanes.land(0));
            landed += 1;
        }
        block_on(upload.write(chunk)).unwrap();
        assert_eq!(rig.parts_sent(), landed, "a write waited for a part");
    }
    assert_eq!(
        landed, 2,
        "the upload said full before each part with a part in flight"
    );
    block_on(upload.commit()).unwrap();
    assert!(rig.stored().as_deref() == Some(&bytes[..]));
}

/// Parts that end newest first, or in any order a test picks, make the object the parts in
/// order make: each ETag goes by its part number.
#[test]
fn parts_that_end_out_of_order_make_the_same_object() {
    let bytes = body(4 * PART + 100_000);
    let picks: [fn(usize) -> usize; 2] = [newest, |started| started / 2];
    for pick in picks {
        let rig = rig(4, pick);
        let source = Source::of(&bytes);
        let mut upload = rig.started(&bytes, &source, 4);
        assert_eq!(upload.in_flight(), 4);
        block_on(upload.write_from(&bytes[4 * PART..], &source)).unwrap();
        // The second part ends first of all.
        assert!(rig.lanes.land(1));
        block_on(upload.commit()).unwrap();
        assert!(rig.stored().as_deref() == Some(&bytes[..]));
        assert_eq!(rig.wire.inner().open_uploads(), 0);
        assert_eq!(rig.store.pending_orphans(), 0);
    }
}

/// A part fails while three others are in flight. The call that collects its answer fails
/// with it, the others are not sent, and an abort then returns with nothing in flight and
/// leaves the id to the orphan list, as a failed upload always did.
#[test]
fn a_part_that_fails_with_others_in_flight_ends_the_upload_and_the_rest_are_not_sent() {
    let rig = rig(4, oldest);
    let bytes = body(4 * PART);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 4);
    rig.wire.push(reset());
    // The second part is the one whose request is reset.
    assert!(rig.lanes.land(1));
    assert_eq!(block_on(upload.flight(Rest::More)), Err(was_reset()));
    assert_eq!((upload.in_flight(), rig.lanes.started()), (3, 3));
    assert!(block_on(upload.flight(Rest::More)).is_err(), "over");
    assert!(
        block_on(upload.write_from(&bytes[..1], &source)).is_err(),
        "over"
    );

    let reads = source.reads();
    block_on(upload.abort()).unwrap();
    assert_eq!(rig.lanes.started(), 0, "the abort waited for every part");
    assert_eq!(rig.parts_sent(), 1, "the rest were not sent");
    assert_eq!(source.reads(), reads, "nor read");
    assert_eq!(count(&rig.wire, Shape::Abort), 0);
    assert_eq!(rig.store.pending_orphans(), 1);
    assert_eq!(rig.store.abort_orphans().unwrap(), 1);
    assert_eq!(rig.wire.inner().open_uploads(), 0);
}

/// The same failure met by a commit: it fails with the part's error, having waited for
/// every part started, and sends no CompleteMultipartUpload.
#[test]
fn a_commit_that_meets_a_failed_part_waits_for_the_rest_and_completes_nothing() {
    let rig = rig(3, newest);
    let bytes = body(3 * PART);
    let source = Source::of(&bytes);
    let upload = rig.started(&bytes, &source, 3);
    rig.wire.push(reset());
    assert_eq!(block_on(upload.commit()), Err(was_reset()));
    assert_eq!(rig.lanes.started(), 0);
    assert_eq!(rig.parts_sent(), 1);
    assert_eq!(count(&rig.wire, Shape::Complete), 0);
    assert_eq!(rig.stored(), None);
    assert_eq!(rig.store.pending_orphans(), 1);
}

/// An abort with parts in flight: they end unsent, and only then is the abort sent.
#[test]
fn an_abort_waits_for_the_parts_in_flight_and_they_are_not_sent() {
    let rig = rig(3, oldest);
    let bytes = body(3 * PART);
    let source = Source::of(&bytes);
    let upload = rig.started(&bytes, &source, 3);
    block_on(upload.abort()).unwrap();
    assert_eq!(rig.lanes.started(), 0);
    assert_eq!(
        shapes(&rig.wire),
        [Some(Shape::CreateMultipart), Some(Shape::Abort)]
    );
    assert_eq!(source.reads(), 0);
    assert_eq!(rig.wire.inner().open_uploads(), 0);
    assert_eq!(rig.store.pending_orphans(), 0);
}

/// A part already on the wire when the abort comes is waited for too: the abort follows
/// its answer.
#[test]
fn an_abort_follows_the_answer_of_a_part_that_was_sent() {
    let rig = rig(2, oldest);
    let bytes = body(2 * PART);
    let source = Source::of(&bytes);
    let upload = rig.started(&bytes, &source, 2);
    assert!(rig.lanes.land(0));
    block_on(upload.abort()).unwrap();
    assert_eq!(
        shapes(&rig.wire),
        [
            Some(Shape::CreateMultipart),
            Some(Shape::UploadPart),
            Some(Shape::Abort)
        ]
    );
    assert_eq!(rig.wire.inner().open_uploads(), 0);
}

/// Dropping an upload does no I/O and waits for nothing. Its id goes on the orphan list only
/// when the last part it started has ended, so the list never aborts an upload with a part
/// of it still to be sent; the parts themselves are not sent.
#[test]
fn a_dropped_uploads_id_is_orphaned_when_its_last_part_ends() {
    let rig = rig(2, oldest);
    let bytes = body(2 * PART);
    let source = Source::of(&bytes);
    let upload = rig.started(&bytes, &source, 2);
    drop(upload);
    assert_eq!(rig.lanes.started(), 2, "the drop waited for nothing");
    assert_eq!(rig.store.pending_orphans(), 0);
    assert_eq!(rig.store.abort_orphans().unwrap(), 0);
    assert!(rig.lanes.land(0));
    assert_eq!(rig.store.pending_orphans(), 0, "a part is still started");
    assert!(rig.lanes.land(0));
    assert_eq!(rig.store.pending_orphans(), 1);
    assert_eq!(shapes(&rig.wire), [Some(Shape::CreateMultipart)]);
    assert_eq!(rig.store.abort_orphans().unwrap(), 1);
    assert_eq!(rig.wire.inner().open_uploads(), 0);
}

/// The boot sweep aborts every upload open under the prefix. An engine rebuilt over the
/// same store may have dropped an upload a moment before: the sweep waits for that upload's
/// parts to end, so it never aborts an id with a part of this process still started.
#[test]
fn the_boot_sweep_waits_for_the_parts_a_dropped_upload_started() {
    let rig = rig(2, oldest);
    let bytes = body(2 * PART);
    let source = Source::of(&bytes);
    drop(rig.started(&bytes, &source, 2));
    assert_eq!(rig.lanes.started(), 2);
    assert_eq!(block_on(rig.store.remove_scratch()), Ok(1));
    assert_eq!(rig.lanes.started(), 0);
    assert_eq!(rig.parts_sent(), 0);
    assert_eq!(rig.wire.inner().open_uploads(), 0);
    assert_eq!(rig.store.pending_orphans(), 0);
}

/// The object's last part is sealed when the caller says nothing more comes, short or not,
/// and the upload is idle only when every answer is in. Nothing may be written after.
#[test]
fn the_last_part_is_sealed_when_told_and_the_upload_is_idle_when_every_answer_is_in() {
    let rig = rig(2, newest);
    let bytes = body(PART + 100_000);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 1);
    block_on(upload.write_from(&bytes[PART..], &source)).unwrap();
    assert_eq!(upload.in_flight(), 1, "the tail is not a part yet");
    assert_eq!(block_on(upload.flight(Rest::More)), Ok(Flight::Room));

    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Room));
    assert_eq!((upload.in_flight(), rig.lanes.started()), (2, 2));
    assert!(block_on(upload.write_from(b"more", &source)).is_err());
}

#[test]
fn an_upload_whose_parts_all_ended_is_idle_and_its_commit_only_completes() {
    let rig = rig(2, newest);
    let bytes = body(PART + 100_000);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 1);
    block_on(upload.write_from(&bytes[PART..], &source)).unwrap();
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Room));
    assert!(rig.lanes.land(1));
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Room));
    assert!(rig.lanes.land(0));
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Idle));
    let before = rig.wire.requests().len();
    block_on(upload.commit()).unwrap();
    assert_eq!(
        shapes(&rig.wire)[before..],
        [Some(Shape::Complete)],
        "nothing was left to send or wait for"
    );
    assert!(rig.stored().as_deref() == Some(&bytes[..]));
}

/// With every lane taken the last part waits like any other, and the answer says so.
#[test]
fn a_last_part_with_no_lane_free_waits_and_the_upload_is_full() {
    let rig = rig(1, oldest);
    let bytes = body(PART + 100_000);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 1);
    block_on(upload.write_from(&bytes[PART..], &source)).unwrap();
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Full));
    assert_eq!((upload.in_flight(), rig.lanes.started()), (1, 1));
    assert!(rig.lanes.land(0));
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Room));
    assert!(rig.lanes.land(0));
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Idle));
    block_on(upload.commit()).unwrap();
    assert!(rig.stored().as_deref() == Some(&bytes[..]));
}

/// The last part fails: the question "has it all landed" is answered with the failure, so
/// a caller that only asks never waits for ever.
#[test]
fn a_last_part_that_fails_is_the_answer_to_whether_all_landed() {
    let rig = rig(2, oldest);
    let bytes = body(PART + 100_000);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 1);
    block_on(upload.write_from(&bytes[PART..], &source)).unwrap();
    assert!(rig.lanes.land(0));
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Room));
    rig.wire.push(reset());
    assert!(rig.lanes.land(0));
    assert_eq!(block_on(upload.flight(Rest::None)), Err(was_reset()));
    assert_eq!(rig.stored(), None);
}

/// An object below one part has nothing in flight: it is idle, and its commit sends it.
#[test]
fn an_object_below_one_part_is_idle_until_its_commit() {
    let rig = rig(2, oldest);
    let bytes = body(100_000);
    let source = Source::of(&bytes);
    let mut upload = begin(&rig.store, &key(0, 1, 1));
    block_on(upload.write_from(&bytes, &source)).unwrap();
    assert_eq!(block_on(upload.flight(Rest::More)), Ok(Flight::Idle));
    assert_eq!(block_on(upload.flight(Rest::None)), Ok(Flight::Idle));
    assert!(rig.wire.requests().is_empty());
    block_on(upload.commit()).unwrap();
    assert_eq!(shapes(&rig.wire), [Some(Shape::Put)]);
    assert_eq!(rig.lanes.started(), 0);
}

struct Woken(AtomicUsize);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// The task that asked is woken when a part ends, each time one does, and asking never
/// leaves the task pending.
#[test]
fn a_part_that_ends_wakes_the_task_that_asked() {
    let rig = rig(2, oldest);
    let bytes = body(2 * PART);
    let source = Source::of(&bytes);
    let mut upload = rig.started(&bytes, &source, 2);
    let woken = Arc::new(Woken(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&woken));
    let asked = {
        let mut asking = std::pin::pin!(upload.flight(Rest::More));
        asking.as_mut().poll(&mut Context::from_waker(&waker))
    };
    assert_eq!(asked, Poll::Ready(Ok(Flight::Room)));
    assert_eq!(woken.0.load(Ordering::Relaxed), 0);
    assert!(rig.lanes.land(0));
    assert_eq!(woken.0.load(Ordering::Relaxed), 1);
    assert!(rig.lanes.land(0));
    assert_eq!(woken.0.load(Ordering::Relaxed), 2);
}

/// Parts that are buffers: the upload holds the parts in flight and one more, which fills
/// or waits, and never a byte past that. It reaches the bound, so the bound is what holds it.
#[test]
fn an_upload_that_holds_its_parts_holds_those_in_flight_and_one_more() {
    for width in [1, 3] {
        let rig = rig(width, oldest);
        let bytes = body(8 * PART);
        let mut upload = begin(&rig.store, &key(0, 1, 1));
        let high = Mutex::new(0);
        block_on(async {
            for piece in bytes.chunks(1_500_003) {
                upload.write(piece).await.unwrap();
                let held = upload.buffer_capacity();
                assert!(held <= (width + 1) * PART, "{width}: {held} held");
                assert!(upload.in_flight() <= width, "{width}");
                assert!(rig.lanes.started() <= width, "{width}");
                let mut high = high.lock().unwrap();
                *high = (*high).max(held);
            }
        });
        let high = *high.lock().unwrap();
        assert!(high > width * PART, "{width}: {high}");
        block_on(upload.commit()).unwrap();
        assert_eq!(rig.parts_sent(), 8);
        assert!(rig.stored().as_deref() == Some(&bytes[..]));
    }
}

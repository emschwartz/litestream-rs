//! A file sent a slice a step: what one step may hand the replica, what is kept between two
//! steps, and what ends a send part-way.

use super::*;

pub(super) const QUARTER_MIB: u64 = 256 * 1024;

/// One step that may read `budget` bytes; also the bytes of the budget it left.
pub(super) fn step<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    (staging, remote): (&L, &S),
    witnesses: &Witnesses,
    in_flight: &mut InFlight,
    budget: u64,
) -> (Result<Option<Uploaded>, UploadFailed>, u64) {
    let mut left = budget;
    let stepped = block_on(upload_step(
        state,
        (&Staging(staging), &Replica(remote)),
        Some(txid(2)),
        (witnesses, Guard::BucketRefuses),
        (in_flight, &mut left),
    ));
    (stepped, left)
}

/// Steps until a step ends; returns it and how many steps left the file in flight.
pub(super) fn finish<L: ObjectStore, S: ObjectStore>(
    state: UploadState,
    stores: (&L, &S),
    witnesses: &Witnesses,
    in_flight: &mut InFlight,
    budget: u64,
) -> (Result<Uploaded, UploadFailed>, u32) {
    let mut slices = 0_u32;
    loop {
        match step(state, stores, witnesses, in_flight, budget).0 {
            Ok(None) => slices = slices.saturating_add(1),
            Ok(Some(uploaded)) => return (Ok(uploaded), slices),
            Err(failed) => return (Err(failed), slices),
        }
    }
}

/// Transaction 1 staged as a file of several read chunks, with its token.
pub(super) fn staged_big() -> (MemStore, Witnesses, Vec<u8>) {
    let staging = MemStore::new();
    let bytes = big_file();
    put_bytes(&staging, key(1), &bytes);
    let witnesses = Witnesses::trust_all(&staging);
    (staging, witnesses, bytes)
}

#[test]
fn a_step_hands_the_replica_its_budget_and_keeps_the_rest_of_the_file_in_flight() {
    let (staging, witnesses, bytes) = staged_big();
    let remote = Probe::default();
    let stores = (&staging, &remote);
    let state = UploadState::At(None);
    let mut in_flight = InFlight::default();
    assert_eq!(in_flight.sent(), None);
    // Not a multiple of the read chunk: the last read of a step is cut to what is left.
    let budget = QUARTER_MIB + 1000;
    let (first, left) = step(state, stores, &witnesses, &mut in_flight, budget);
    assert_eq!(first, Ok(None));
    assert_eq!(left, 0);
    assert_eq!(remote.log().written, budget);
    assert_eq!(in_flight.sent(), Some(budget));
    assert!(files(&remote.inner).is_empty(), "nothing is published yet");
    let (put, slices) = finish(state, stores, &witnesses, &mut in_flight, budget);
    assert!(matches!(put.unwrap().did, Did::Put(_)));
    let len = u64::try_from(bytes.len()).unwrap();
    assert_eq!(u64::from(slices) + 1, len / budget);
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    assert_eq!(remote.log().written, len);
    assert_eq!(remote.log().begins, [key(1)], "one upload for every slice");
    assert_eq!(remote.log().aborts, 0);
    assert_eq!(in_flight.sent(), None);
}

/// A step whose budget outlasts the file leaves the rest of it for the tick's next file.
#[test]
fn a_file_that_ends_inside_the_budget_leaves_the_rest_of_it() {
    let (staging, remote) = (MemStore::new(), MemStore::new());
    let len = u64::try_from(stage(&staging, 1).len()).unwrap();
    let witnesses = Witnesses::trust_all(&staging);
    let mut in_flight = InFlight::default();
    let state = UploadState::At(None);
    let (put, left) = step(
        state,
        (&staging, &remote),
        &witnesses,
        &mut in_flight,
        len + 7,
    );
    assert!(matches!(put.unwrap().unwrap().did, Did::Put(_)));
    assert_eq!(left, 7);
}

proptest! {
    /// Whatever the budget, down to a byte a step, the replica gets exactly the staged bytes,
    /// and no step reads more than the budget or hands the replica more than it read. Below
    /// the length of a file header the bytes read wait for the header to be whole, since the
    /// upload is begun from it: the step that completes it hands them over with its own.
    #[test]
    fn any_budget_sends_the_staged_bytes(budget in 1_u64..400) {
        let (staging, remote) = (MemStore::new(), Probe::default());
        let bytes = stage(&staging, 1);
        let witnesses = Witnesses::trust_all(&staging);
        let mut in_flight = InFlight::default();
        let state = UploadState::At(None);
        let mut sent = 0_u64;
        let put = loop {
            let before = remote.log().written;
            let (stepped, _) = step(state, (&staging, &remote), &witnesses, &mut in_flight, budget);
            let held = u64::try_from(lsr_ltx::HEADER_LEN).unwrap();
            let written = remote.log().written.saturating_sub(before);
            prop_assert!(written < budget.saturating_add(held));
            match stepped.unwrap() {
                Some(uploaded) => break uploaded,
                None => {
                    let now = in_flight.sent().unwrap();
                    prop_assert_eq!(now, sent.saturating_add(budget));
                    sent = now;
                }
            }
        };
        prop_assert!(matches!(put.did, Did::Put(_)));
        prop_assert_eq!(read_all(&remote.inner, &key(1)), bytes);
    }
}

/// The bytes sent are verified on the reads that produced them, through the commit: a file
/// replaced under a send, with another whole file of the same key, is refused by the reads
/// after it, and the upload begun for it is aborted unpublished.
#[test]
fn a_file_replaced_between_two_slices_is_refused_and_its_upload_aborted() {
    let (staging, witnesses, _) = staged_big();
    let remote = Probe::default();
    let state = UploadState::At(None);
    let mut in_flight = InFlight::default();
    let (first, _) = step(
        state,
        (&staging, &remote),
        &witnesses,
        &mut in_flight,
        QUARTER_MIB,
    );
    assert_eq!(first, Ok(None));
    put_bytes(&staging, key(1), &big_file_for(1, 77));
    let (refused, _) = finish(
        state,
        (&staging, &remote),
        &witnesses,
        &mut in_flight,
        QUARTER_MIB,
    );
    assert_eq!(refused, Err(fail(state, Fault::StagedLost)));
    assert!(files(&remote.inner).is_empty());
    assert_eq!(remote.log().aborts, 1);
    assert_eq!(in_flight.sent(), None);
}

/// Nothing of a send is kept past a failure: the retry reads the file from its first byte
/// into a new upload.
#[test]
fn a_failed_slice_leaves_nothing_in_flight_and_the_retry_starts_at_the_first_byte() {
    let (staging, witnesses, bytes) = staged_big();
    let staging = Probe {
        inner: staging,
        log: Arc::default(),
    };
    let remote = Probe::default();
    let stores = (&staging, &remote);
    let state = UploadState::At(None);
    let mut in_flight = InFlight::default();
    let (first, _) = step(state, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    assert_eq!(first, Ok(None));
    remote.log().write_fail.push_back(transient());
    let (failed, _) = step(state, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    assert_eq!(failed, Err(fail(state, Fault::StoreTransient)));
    assert_eq!(remote.log().aborts, 1);
    assert_eq!(in_flight.sent(), None);
    assert_eq!(staging.log().whole_reads, 1);
    let (put, _) = finish(state, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    assert!(matches!(put.unwrap().did, Did::Put(_)));
    assert_eq!(
        staging.log().whole_reads,
        2,
        "read again from the first byte"
    );
    assert_eq!(remote.log().begins, [key(1), key(1)]);
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
}

/// A send is of the planned file, as witnessed. When the plan moves on (or there is nothing
/// left to plan), a send of another file is ended and its upload aborted.
#[test]
fn a_send_of_a_file_the_plan_no_longer_names_is_ended() {
    let (staging, _, _) = staged_big();
    put_bytes(&staging, key(2), &big_file_for(2, 5));
    let witnesses = Witnesses::trust_all(&staging);
    let remote = Probe::default();
    let stores = (&staging, &remote);
    let mut in_flight = InFlight::default();
    let (first, _) = step(
        UploadState::At(None),
        stores,
        &witnesses,
        &mut in_flight,
        QUARTER_MIB,
    );
    assert_eq!(first, Ok(None));
    let after_one = UploadState::At(Some(txid(1)));
    let (second, _) = step(after_one, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    assert_eq!(second, Ok(None));
    assert_eq!(remote.log().aborts, 1);
    assert_eq!(remote.log().begins, [key(1), key(2)]);
    assert_eq!(in_flight.sent(), Some(QUARTER_MIB));
    let after_two = UploadState::At(Some(txid(2)));
    let (idle, _) = step(after_two, stores, &witnesses, &mut in_flight, QUARTER_MIB);
    assert_eq!(
        idle.map(|step| step.map(|up| up.did)),
        Ok(Some(Did::Nothing))
    );
    assert_eq!(remote.log().aborts, 2);
    assert_eq!(in_flight.sent(), None);
}

/// Staged files are deleted only with the upload task's word that no send is reading them:
/// given for files below the one in flight, and for any file once the send is stopped.
#[test]
fn no_staged_file_at_or_past_the_one_in_flight_may_be_deleted_until_the_send_is_stopped() {
    let (staging, _, _) = staged_big();
    put_bytes(&staging, key(2), &big_file_for(2, 5));
    let witnesses = Witnesses::trust_all(&staging);
    let remote = Probe::default();
    let mut in_flight = InFlight::default();
    assert!(
        in_flight.unsent_through(txid(9)).is_some(),
        "nothing is in flight"
    );
    let after_one = UploadState::At(Some(txid(1)));
    let (sent, _) = step(
        after_one,
        (&staging, &remote),
        &witnesses,
        &mut in_flight,
        QUARTER_MIB,
    );
    assert_eq!(sent, Ok(None));
    assert!(in_flight.unsent_through(txid(1)).is_some());
    assert!(in_flight.unsent_through(txid(2)).is_none());
    assert!(in_flight.unsent_through(txid(3)).is_none());
    block_on(async {
        let unsent = in_flight.stop().await;
        drop_staged_from(&Staging(&staging), txid(2), &unsent).await
    })
    .unwrap();
    assert_eq!(remote.log().aborts, 1);
    assert_eq!(in_flight.sent(), None);
    assert!(in_flight.unsent_through(txid(3)).is_some());
    assert_eq!(files(&staging).len(), 1);
}

/// The file that replaces a torn object under its key is sent a slice a step as well: the
/// refused commit starts a second send, which replaces.
#[test]
fn a_torn_object_under_the_key_is_replaced_a_slice_a_step() {
    let (staging, witnesses, bytes) = staged_big();
    let remote = Probe::default();
    put_bytes(&remote.inner, key(1), b"torn");
    let stores = (&staging, &remote);
    let state = UploadState::At(None);
    let mut in_flight = InFlight::default();
    let mut restarts = 0;
    let put = loop {
        let was = in_flight.sent();
        match step(state, stores, &witnesses, &mut in_flight, QUARTER_MIB)
            .0
            .unwrap()
        {
            Some(uploaded) => break uploaded,
            None if in_flight.sent() < was => restarts += 1,
            None => {}
        }
    };
    assert!(matches!(put.did, Did::Put(_)), "{:?}", put.did);
    assert_eq!(restarts, 1, "the refusal started the file again, once");
    assert_eq!(remote.log().modes, [WriteMode::New, WriteMode::Replace]);
    assert_eq!(read_all(&remote.inner, &key(1)), bytes);
}

#[test]
fn a_tick_hands_the_replica_eight_mebibytes_unless_the_configuration_says_otherwise() {
    // The default is the bound `docs/decisions/D217.md` states: 32 reads of a quarter mebibyte.
    assert_eq!(
        UploadConfig::default().bytes_per_tick.get(),
        QUARTER_MIB.saturating_mul(32)
    );
}

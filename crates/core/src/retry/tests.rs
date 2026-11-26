use std::io::ErrorKind;

use lsr_ltx::{Timestamp, Txid, TxidRange};
use proptest::prelude::*;

use super::*;
use crate::conformance::{self, Harness};
use crate::env::{RecordingSleeper, SeededEntropy};
use crate::layout::CreatedAt;
use crate::mem::MemStore;
use crate::mem::reread::{Handed, Rereads, lost, mixed};
use crate::mem::short::{LyingUpload, SizeLiar};
use crate::store::{CorruptCause, PartNumber, StoreOp};

fn key() -> FileKey {
    let id = Txid::new(1).unwrap();
    FileKey::new(Level::ZERO, TxidRange::single(id))
}

fn created_at() -> Timestamp {
    Timestamp::from_unix_millis(1)
}

fn limit() -> PageLimit {
    PageLimit::new(10).unwrap()
}

fn transient(kind: ErrorKind) -> StoreError {
    StoreError::Transient {
        op: StoreOp::List,
        kind,
    }
}

fn throttled(after: Option<Duration>) -> StoreError {
    StoreError::Throttled {
        op: StoreOp::List,
        retry_after: after,
    }
}

/// An entropy that always draws the top of the range, so waits equal their bound.
#[derive(Debug)]
struct MaxEntropy;

impl Entropy for MaxEntropy {
    fn below(&self, upper: u64) -> u64 {
        upper.saturating_sub(1)
    }
}

/// An entropy that always draws zero.
#[derive(Debug)]
struct ZeroEntropy;

impl Entropy for ZeroEntropy {
    fn below(&self, _upper: u64) -> u64 {
        0
    }
}

type Rig<E> = (
    MemStore,
    RecordingSleeper,
    Retrying<MemStore, RecordingSleeper, E>,
);

fn rig<E: Entropy>(entropy: E, policy: RetryPolicy) -> Rig<E> {
    let mem = MemStore::new();
    let sleeper = RecordingSleeper::new();
    let store = Retrying::new(mem.clone(), sleeper.clone(), entropy, policy);
    (mem, sleeper, store)
}

fn policy(attempts: u8) -> RetryPolicy {
    RetryPolicy {
        attempts: NonZeroU8::new(attempts).unwrap(),
        ..RetryPolicy::DEFAULT
    }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// classify: one test per StoreError variant (and per Throttled shape)

#[test]
fn transient_is_retried() {
    assert_eq!(classify(&transient(ErrorKind::TimedOut)), Class::Retry);
}

#[test]
fn throttled_with_a_named_wait_is_retry_after() {
    assert_eq!(
        classify(&throttled(Some(ms(300)))),
        Class::RetryAfter(ms(300))
    );
}

#[test]
fn throttled_without_a_wait_is_retried() {
    assert_eq!(classify(&throttled(None)), Class::Retry);
}

#[test]
fn throttled_with_a_zero_wait_is_retried_with_jitter() {
    assert_eq!(classify(&throttled(Some(Duration::ZERO))), Class::Retry);
}

#[test]
fn not_found_is_missing() {
    assert_eq!(
        classify(&StoreError::NotFound { key: key() }),
        Class::Missing
    );
}

#[test]
fn auth_is_permanent() {
    assert_eq!(
        classify(&StoreError::Denied {
            op: StoreOp::Read,
            why: crate::store::Denied::Credentials
        }),
        Class::Permanent
    );
}

#[test]
fn scratch_exhausted_is_permanent() {
    assert_eq!(
        classify(&StoreError::ScratchExhausted { key: key() }),
        Class::Permanent
    );
}

/// Sending the same too-large object again cannot make the store take it.
#[test]
fn an_object_over_the_store_limit_is_permanent() {
    let err = StoreError::ObjectTooLarge {
        key: key(),
        len: 6,
        max: 5,
    };
    assert_eq!(classify(&err), Class::Permanent);
}

#[test]
fn corrupt_is_permanent() {
    let err = StoreError::Corrupt {
        key: None,
        cause: CorruptCause::NotARegularFile,
    };
    assert_eq!(classify(&err), Class::Permanent);
}

#[test]
fn new_errors_are_permanent() {
    let corrupt = |cause| StoreError::Corrupt { key: None, cause };
    for err in [
        corrupt(CorruptCause::ChecksumMismatch { part: None }),
        corrupt(CorruptCause::ChecksumMismatch {
            part: PartNumber::new(3),
        }),
        corrupt(CorruptCause::BadCreatedAt),
        StoreError::Refused {
            op: StoreOp::Upload,
            status: 501,
        },
    ] {
        assert_eq!(classify(&err), Class::Permanent, "{err}");
    }
}

// the wrapper

#[test]
fn success_after_transient_failures_waits_within_each_bound() {
    for k in 1..=4_u32 {
        let (mem, sleeper, store) = rig(SeededEntropy::new(u64::from(k)), policy(5));
        for _ in 0..k {
            mem.inject_failure(StoreOp::List, transient(ErrorKind::TimedOut));
        }
        let listing = conformance::block_on(store.list(Level::ZERO, None, limit()));
        assert_eq!(listing.map(|l| l.files.len()), Ok(0));
        let waits = sleeper.waits();
        assert_eq!(waits.len(), usize::try_from(k).unwrap());
        for (i, wait) in waits.iter().enumerate() {
            let failures = u32::try_from(i).unwrap() + 1;
            assert!(!wait.is_zero());
            assert!(*wait <= RetryPolicy::DEFAULT.bound(failures));
        }
        let stats = store.stats();
        assert_eq!(
            (stats.calls, stats.retries, stats.gave_up),
            (u64::from(k) + 1, u64::from(k), 0)
        );
        assert_eq!(stats.waited, sleeper.total());
    }
}

#[test]
fn bounds_double_then_stop_at_the_cap() {
    let (mem, sleeper, store) = rig(MaxEntropy, policy(8));
    for _ in 0..7 {
        mem.inject_failure(StoreOp::List, transient(ErrorKind::TimedOut));
    }
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap();
    assert_eq!(
        sleeper.waits(),
        [
            ms(100),
            ms(200),
            ms(400),
            ms(800),
            ms(1600),
            ms(3200),
            ms(5000)
        ]
    );
}

#[test]
fn a_zero_draw_still_waits() {
    let (mem, sleeper, store) = rig(ZeroEntropy, policy(2));
    mem.inject_failure(StoreOp::List, transient(ErrorKind::TimedOut));
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap();
    assert_eq!(sleeper.waits(), [Duration::from_nanos(1)]);
}

#[test]
fn attempts_failures_return_the_last_error_without_a_final_wait() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(3));
    let kinds = [
        ErrorKind::TimedOut,
        ErrorKind::ConnectionReset,
        ErrorKind::BrokenPipe,
    ];
    for kind in kinds {
        mem.inject_failure(StoreOp::List, transient(kind));
    }
    mem.inject_failure(StoreOp::List, transient(ErrorKind::Other));
    let got = conformance::block_on(store.list(Level::ZERO, None, limit()));
    assert_eq!(got, Err(transient(ErrorKind::BrokenPipe)));
    assert_eq!(sleeper.waits().len(), 2);
    let stats = store.stats();
    assert_eq!((stats.calls, stats.retries, stats.gave_up), (3, 2, 1));
}

#[test]
fn not_found_makes_no_wait_and_one_call() {
    let (_mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    let got = conformance::block_on(store.read(&key(), 0, ReadLen::new(1).unwrap()));
    assert_eq!(got, Err(StoreError::NotFound { key: key() }));
    assert!(sleeper.waits().is_empty());
    assert_eq!(store.stats().calls, 1);
}

#[test]
fn auth_makes_no_wait_and_one_call() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    mem.inject_failure(
        StoreOp::Delete,
        StoreError::Denied {
            op: StoreOp::Delete,
            why: crate::store::Denied::Credentials,
        },
    );
    let got = conformance::block_on(store.delete(&[key()]));
    assert_eq!(
        got,
        Err(StoreError::Denied {
            op: StoreOp::Delete,
            why: crate::store::Denied::Credentials
        })
    );
    assert!(sleeper.waits().is_empty());
    assert_eq!(store.stats().calls, 1);
}

#[test]
fn retry_after_over_the_cap_returns_at_once() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    let err = throttled(Some(RetryPolicy::DEFAULT.cap + ms(1)));
    mem.inject_failure(StoreOp::List, err.clone());
    let got = conformance::block_on(store.list(Level::ZERO, None, limit()));
    assert_eq!(got, Err(err));
    assert!(sleeper.waits().is_empty());
    let stats = store.stats();
    assert_eq!((stats.calls, stats.gave_up), (1, 1));
}

#[test]
fn retry_after_within_the_cap_is_slept_exactly() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    mem.inject_failure(StoreOp::List, throttled(Some(RetryPolicy::DEFAULT.cap)));
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap();
    assert_eq!(sleeper.waits(), [RetryPolicy::DEFAULT.cap]);
}

#[test]
fn read_delete_and_begin_upload_are_retried_too() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(3), policy(5));
    conformance::block_on(async {
        mem.inject_failure(StoreOp::Delete, transient(ErrorKind::TimedOut));
        store.delete(&[key()]).await.unwrap();
        mem.inject_failure(StoreOp::Upload, transient(ErrorKind::TimedOut));
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abc").await.unwrap();
        up.commit().await.unwrap();
        mem.inject_failure(StoreOp::Read, transient(ErrorKind::TimedOut));
        let bytes = store
            .read(&key(), 0, ReadLen::new(8).unwrap())
            .await
            .unwrap();
        assert_eq!(bytes, b"abc");
    });
    assert_eq!(sleeper.waits().len(), 3);
}

/// Stores "abcdefghi" under [`key`] and takes pieces of 3 bytes from a stream of it, the
/// read failing once with `err` after the first piece.
fn stream_failing_after_one_piece(
    err: StoreError,
) -> (
    RecordingSleeper,
    RetryStats,
    Vec<Result<Vec<u8>, StoreError>>,
) {
    let (mem, sleeper, store) = rig(SeededEntropy::new(3), policy(5));
    let pieces = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abcdefghi").await.unwrap();
        up.commit().await.unwrap();
        let mut stream = store.read_stream(key(), 0);
        let len = ReadLen::new(3).unwrap();
        let mut pieces = vec![stream.next(len).await];
        mem.inject_failure(StoreOp::Read, err);
        for _ in 0..3 {
            pieces.push(stream.next(len).await);
        }
        pieces
    });
    let stats = store.stats();
    (sleeper, stats, pieces)
}

#[test]
fn a_stream_that_fails_goes_on_from_the_bytes_it_handed_out() {
    let (sleeper, stats, pieces) =
        stream_failing_after_one_piece(transient(ErrorKind::ConnectionReset));
    assert_eq!(
        pieces,
        [
            Ok(b"abc".to_vec()),
            Ok(b"def".to_vec()),
            Ok(b"ghi".to_vec()),
            Ok(Vec::new())
        ]
    );
    assert_eq!(sleeper.waits().len(), 1);
    assert_eq!(stats.retries, 1);
    // The upload's begin, write and commit, the stream once, and the one reopening.
    assert_eq!(stats.calls, 5);
}

#[test]
fn a_stream_that_fails_for_good_is_not_reopened() {
    let (sleeper, stats, pieces) =
        stream_failing_after_one_piece(StoreError::NotFound { key: key() });
    assert_eq!(pieces.first(), Some(&Ok(b"abc".to_vec())));
    assert_eq!(
        pieces.get(1),
        Some(&Err(StoreError::NotFound { key: key() }))
    );
    assert!(sleeper.waits().is_empty());
    assert_eq!(stats.retries, 0);
}

#[test]
fn a_stream_from_an_offset_starts_there() {
    let (_mem, _sleeper, store) = rig(SeededEntropy::new(3), policy(5));
    conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abcdefghi").await.unwrap();
        up.commit().await.unwrap();
        let mut stream = store.read_stream(key(), 4);
        let len = ReadLen::new(3).unwrap();
        assert_eq!(stream.next(len).await.unwrap(), b"efg");
        assert_eq!(stream.next(len).await.unwrap(), b"hi");
    });
    assert_eq!(store.stats().calls, 4);
}

#[test]
fn upload_write_failure_passes_through_with_one_call() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        mem.inject_failure(StoreOp::Upload, transient(ErrorKind::TimedOut));
        let got = up.write(b"x").await;
        assert_eq!(got, Err(transient(ErrorKind::TimedOut)));
    });
    assert!(sleeper.waits().is_empty());
    assert_eq!(store.stats().calls, 2);
}

#[test]
fn upload_commit_failure_passes_through_with_one_call() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"x").await.unwrap();
        mem.inject_failure(StoreOp::Upload, transient(ErrorKind::TimedOut));
        let got = up.commit().await;
        assert_eq!(got, Err(transient(ErrorKind::TimedOut)));
    });
    assert!(sleeper.waits().is_empty());
    assert_eq!(store.stats().calls, 3);
}

/// An upload written from a source reaches the store with it, once for each call, and its
/// commit is checked against the bytes written as any commit is.
#[test]
fn an_upload_written_from_a_source_reaches_the_store_with_it() {
    let inner = Rereads::default();
    let store = Retrying::new(
        inner.clone(),
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        policy(5),
    );
    let handed = Handed::of(b"abcd");
    let info = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::New)
            .await
            .unwrap();
        up.write_from(b"ab", &handed).await.unwrap();
        up.write_from(b"cd", &handed).await.unwrap();
        up.commit().await
    })
    .unwrap();
    assert_eq!(info.size, 4);
    assert_eq!(handed.reads(), 2, "the store read back each write");
    // The begin, two writes and the commit.
    assert_eq!(store.stats().calls, 4);
    let read = conformance::block_on(inner.read(&key(), 0, ReadLen::CHUNK)).unwrap();
    assert_eq!(read, b"abcd");
}

/// A source that no longer holds the bytes fails the commit with its own error, passed
/// through as it is, and a plain write to an upload written from a source is the store's
/// refusal: neither is retried, and nothing is published.
#[test]
fn a_source_that_gives_nothing_fails_the_commit_once() {
    let inner = Rereads::default();
    let sleeper = RecordingSleeper::new();
    let store = Retrying::new(
        inner.clone(),
        sleeper.clone(),
        SeededEntropy::new(1),
        policy(5),
    );
    let at = key();
    let begin = || store.begin_upload(&at, created_at(), WriteMode::New);
    let (gone, plain) = conformance::block_on(async {
        let handed = Handed::of(b"abcd");
        let other = Handed::of(b"abxd");
        let mut up = begin().await.unwrap();
        up.write_from(b"abcd", &other).await.unwrap();
        let gone = up.commit().await;
        let mut up = begin().await.unwrap();
        up.write_from(b"abcd", &handed).await.unwrap();
        (gone, up.write(b"ef").await)
    });
    assert_eq!(gone, Err(lost()));
    assert_eq!(plain, Err(mixed()));
    assert!(sleeper.waits().is_empty());
    let listed = conformance::block_on(inner.list(Level::ZERO, None, limit())).unwrap();
    assert!(listed.files.is_empty());
}

#[test]
fn a_commit_that_failed_after_publishing_is_not_repeated() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    mem.inject_failure_after_publish(transient(ErrorKind::TimedOut));
    conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"x").await.unwrap();
        up.commit().await.unwrap_err();
        let listing = store.list(Level::ZERO, None, limit()).await.unwrap();
        assert_eq!(listing.files.len(), 1);
    });
    assert!(sleeper.waits().is_empty());
}

#[test]
fn a_commit_reporting_another_size_is_corrupt() {
    let store = Retrying::new(
        SizeLiar(MemStore::new()),
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let got = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::New)
            .await
            .unwrap();
        up.write(b"ab").await.unwrap();
        up.write(b"cd").await.unwrap();
        up.commit().await
    });
    assert_eq!(
        got,
        Err(StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 4,
                stored: 3,
                removed: true
            },
        })
    );
}

#[test]
fn a_matching_commit_size_is_passed_through() {
    let (_mem, _sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    let info = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        up.commit().await.unwrap()
    });
    assert_eq!(info.size, 4);
}

#[test]
fn abort_passes_through() {
    let (_mem, _sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        up.abort().await.unwrap();
        // Begin, write and abort are one call each, like a commit.
        assert_eq!(store.stats().calls, 3);
        let listing = store.list(Level::ZERO, None, limit()).await.unwrap();
        assert!(listing.files.is_empty());
    });
}

#[test]
fn the_default_policy_is_five_attempts_of_100ms_capped_at_5s() {
    let p = RetryPolicy::default();
    assert_eq!((p.attempts.get(), p.base, p.cap), (5, ms(100), ms(5000)));
    assert_eq!(p.bound(1_000), ms(5000));
    assert_eq!(p.bound(0), ms(100));
}

fn any_error() -> impl Strategy<Value = StoreError> {
    prop_oneof![
        Just(transient(ErrorKind::TimedOut)),
        Just(throttled(None)),
        (1_u64..9_000).prop_map(|n| throttled(Some(ms(n)))),
        Just(StoreError::Denied {
            op: StoreOp::List,
            why: crate::store::Denied::Credentials
        }),
        Just(StoreError::NotFound { key: key() }),
    ]
}

proptest! {
    #[test]
    fn calls_and_waits_stay_within_the_policy(
        schedule in proptest::collection::vec(any_error(), 0..12),
        seed in any::<u64>(),
        attempts in 1_u8..8,
    ) {
        let policy = policy(attempts);
        let (mem, sleeper, store) = rig(SeededEntropy::new(seed), policy);
        for err in &schedule {
            mem.inject_failure(StoreOp::List, err.clone());
        }
        drop(conformance::block_on(store.list(Level::ZERO, None, limit())));
        let stats = store.stats();
        prop_assert!(stats.calls <= u64::from(attempts));
        let waits = sleeper.waits();
        prop_assert_eq!(u64::try_from(waits.len()).unwrap(), stats.retries);
        let mut bounds = Duration::ZERO;
        for (i, err) in schedule.iter().take(waits.len()).enumerate() {
            let failures = u32::try_from(i).unwrap().saturating_add(1);
            bounds = bounds.saturating_add(match classify(err) {
                Class::RetryAfter(named) => named,
                Class::Retry => policy.bound(failures),
                Class::Missing | Class::Permanent => Duration::ZERO,
            });
        }
        prop_assert!(stats.waited <= bounds);
        prop_assert_eq!(stats.waited, sleeper.total());
    }
}

// the conformance suite over the wrapper

struct RetryingHarness {
    mem: MemStore,
    store: Retrying<MemStore, RecordingSleeper, SeededEntropy>,
}

impl Default for RetryingHarness {
    fn default() -> Self {
        let (mem, _sleeper, store) = rig(SeededEntropy::new(9), RetryPolicy::DEFAULT);
        Self { mem, store }
    }
}

impl Harness for RetryingHarness {
    type Store = Retrying<MemStore, RecordingSleeper, SeededEntropy>;

    fn store(&self) -> &Self::Store {
        &self.store
    }

    fn plant_unrecognized(&self, level: Level, name: &str) -> Result<(), conformance::Failure> {
        self.mem.plant_unrecognized(level, name);
        Ok(())
    }

    fn fail_next_commit_after_publish(&self) -> bool {
        self.mem
            .inject_failure_after_publish(transient(ErrorKind::TimedOut));
        true
    }
}

mod conformance_over_retrying {
    use super::*;

    crate::conformance_suite!(RetryingHarness::default);
}

#[test]
fn a_create_only_commit_size_mismatch_leaves_no_object_visible() {
    // The wrapper knows the stored object is not what was sent, reports it as
    // Corrupt, and deletes it: a create-only write found the key free, so the
    // object is its own and a caller that lists after the failed commit must
    // not find it.
    let inner = MemStore::new();
    let store = Retrying::new(
        SizeLiar(inner.clone()),
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let got = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::New)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        up.commit().await
    });
    assert!(matches!(got, Err(StoreError::Corrupt { .. })));
    let listed = conformance::block_on(store.list(Level::ZERO, None, limit()))
        .unwrap()
        .files;
    assert!(
        listed.is_empty(),
        "an object the wrapper called corrupt is still visible: {listed:?}"
    );
}

/// A replace whose commit is acknowledged short keeps the key: it may have held an acknowledged
/// object before, and a delete would end the replica a transaction early with no error.
#[test]
fn a_replace_commit_size_mismatch_never_deletes_the_key() {
    let inner = MemStore::new();
    let store = Retrying::new(
        SizeLiar(inner.clone()),
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let got = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        up.commit().await
    });
    assert_eq!(
        got,
        Err(StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 4,
                stored: 3,
                removed: false
            },
        })
    );
    let listed = conformance::block_on(store.list(Level::ZERO, None, limit()))
        .unwrap()
        .files;
    assert_eq!(listed.len(), 1, "a replace's short commit deleted the key");
}

/// A store whose deletes are refused.
#[derive(Debug)]
struct DeleteRefuser(SizeLiar);

impl ObjectStore for DeleteRefuser {
    type Upload = LyingUpload;

    /// Leaves no scratch.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        Ok(0)
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.0.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.0.read(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.0.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.0.read_with_created_at(file, len).await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<LyingUpload, StoreError> {
        self.0.begin_sized_upload(key, created_at, mode, len).await
    }

    async fn delete(&self, _keys: &[FileKey]) -> Result<(), StoreError> {
        Err(StoreError::Denied {
            op: StoreOp::Delete,
            why: crate::store::Denied::Credentials,
        })
    }
}

#[test]
fn a_mismatch_whose_delete_fails_says_the_object_was_not_removed() {
    let store = Retrying::new(
        DeleteRefuser(SizeLiar(MemStore::new())),
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let got = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::New)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        up.commit().await
    });
    assert_eq!(
        got,
        Err(StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 4,
                stored: 3,
                removed: false
            },
        })
    );
}

#[test]
fn a_mismatch_on_an_object_already_gone_counts_as_removed() {
    // MemStore deletes a missing key as Ok, so the object vanishing between
    // commit and delete cannot turn into removed: false.
    let mem = MemStore::new();
    let store = Retrying::new(
        SizeLiar(mem.clone()),
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let got = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::New)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        conformance::block_on(mem.delete(&[key()])).unwrap();
        up.commit().await
    });
    assert!(matches!(
        got,
        Err(StoreError::Corrupt {
            cause: CorruptCause::CommitSizeMismatch { removed: true, .. },
            ..
        })
    ));
}

/// The boot's scratch removal reaches the store under the retry wrapper and returns its count.
#[test]
fn remove_scratch_is_forwarded() {
    let (mem, _sleeper, store) = rig(SeededEntropy::new(1), policy(3));
    mem.plant_scratch(3);
    mem.plant_scratch(4);
    assert_eq!(mem.scratch(), 7, "planted scratch is counted");
    assert_eq!(conformance::block_on(store.remove_scratch()), Ok(7));
    assert_eq!(mem.scratch(), 0);
}
// the listing budget

/// A sleeper that moves a fake clock by what it was asked to wait, so time passes as it would.
#[derive(Clone, Debug)]
struct Ticking(Arc<crate::env::FakeClock>);

impl Sleeper for Ticking {
    async fn sleep(&self, wait: Duration) {
        self.0.advance(wait);
    }
}

type Budgeted = (
    MemStore,
    Arc<crate::env::FakeClock>,
    Retrying<MemStore, Ticking, MaxEntropy>,
);

/// Waits of 100, 200, 400 ms after the first, second and third failure, with a listing budget.
fn budgeted(budget: Duration) -> Budgeted {
    let mem = MemStore::new();
    let clock = Arc::new(crate::env::FakeClock::new(created_at()));
    let shared = Arc::clone(&clock);
    let as_clock: Arc<dyn Clock> = shared;
    let store = Retrying::with_listing_budget(
        mem.clone(),
        Ticking(Arc::clone(&clock)),
        MaxEntropy,
        policy(5),
        as_clock,
        budget,
    );
    (mem, clock, store)
}

fn fail_lists(mem: &MemStore, times: usize) {
    for _ in 0..times {
        mem.inject_failure(StoreOp::List, transient(ErrorKind::TimedOut));
    }
}

#[test]
fn a_spent_listing_budget_stops_the_retries_and_says_what_was_tried() {
    let (mem, _clock, store) = budgeted(ms(500));
    fail_lists(&mem, 9);
    let got = conformance::block_on(store.list(Level::ZERO, None, limit()));
    assert_eq!(got, Err(transient(ErrorKind::TimedOut)));
    assert_eq!(
        store.listing_expired(),
        Some(ListingExpired {
            budget: ms(500),
            listings: 3,
            last: transient(ErrorKind::TimedOut),
        })
    );
    assert_eq!(store.stats().calls, 3);
}

/// A retry that would begin exactly when the budget ends is not made; one millisecond sooner is.
#[test]
fn the_budget_ends_a_retry_that_would_begin_at_its_end() {
    for (budget, listings) in [(300, 2), (301, 3)] {
        let (mem, _clock, store) = budgeted(ms(budget));
        fail_lists(&mem, 9);
        conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap_err();
        let expired = store.listing_expired().unwrap();
        assert_eq!(expired.listings, listings, "budget {budget} ms");
    }
}

/// The budget runs from the first listing, not from when the store was made.
#[test]
fn the_budget_starts_at_the_first_listing() {
    let (mem, clock, store) = budgeted(ms(500));
    clock.advance(Duration::from_secs(3600));
    fail_lists(&mem, 2);
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap();
    assert_eq!(store.listing_expired(), None);
    assert_eq!(store.stats().retries, 2);
}

/// One budget covers every listing: after it is spent, the next listing is tried once.
#[test]
fn a_spent_budget_leaves_later_listings_one_attempt_each() {
    let (mem, clock, store) = budgeted(ms(500));
    fail_lists(&mem, 9);
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap_err();
    clock.advance(Duration::from_secs(1));
    let before = store.stats().calls;
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap_err();
    assert_eq!(store.stats().calls, before + 1);
    assert_eq!(store.listing_expired().unwrap().listings, 3);
}

#[test]
fn a_batch_listing_is_held_to_the_budget_too() {
    let (mem, _clock, store) = budgeted(ms(500));
    fail_lists(&mem, 9);
    let at = ListAt {
        level: Level::ZERO,
        after: None,
    };
    let got = conformance::block_on(store.list_many(&[at, at], limit()));
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], Err(transient(ErrorKind::TimedOut)));
    let expired = store.listing_expired().unwrap();
    assert_eq!(expired.listings, 2 + 2);
}

/// Only listings are bounded: reads keep the policy after the budget is spent.
#[test]
fn reads_keep_the_policys_attempts_after_the_budget_is_spent() {
    let (mem, _clock, store) = budgeted(ms(500));
    fail_lists(&mem, 9);
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap_err();
    for _ in 0..4 {
        mem.inject_failure(StoreOp::Read, transient(ErrorKind::TimedOut));
    }
    let before = store.stats().calls;
    let got = conformance::block_on(store.read(&key(), 0, ReadLen::or_chunk(16)));
    assert_eq!(got, Err(StoreError::NotFound { key: key() }));
    assert_eq!(store.stats().calls, before + 5);
}

#[test]
fn a_store_without_a_budget_retries_listings_by_the_policy_alone() {
    let (mem, sleeper, store) = rig(MaxEntropy, policy(5));
    fail_lists(&mem, 9);
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap_err();
    assert_eq!(sleeper.waits().len(), 4);
    assert_eq!(store.listing_expired(), None);
}

#[test]
fn a_budget_that_is_never_spent_reports_nothing() {
    let (mem, _clock, store) = budgeted(Duration::from_secs(3600));
    fail_lists(&mem, 9);
    conformance::block_on(store.list(Level::ZERO, None, limit())).unwrap_err();
    assert_eq!(store.listing_expired(), None);
    assert_eq!(store.stats().calls, 5);
}

/// The budget shows in a store's debug output, so a log of a restore's store says it is bounded.
#[test]
fn a_store_with_a_budget_shows_it_in_debug_output() {
    let (_mem, _clock, store) = budgeted(ms(500));
    let shown = format!("{store:?}");
    assert!(shown.contains("ListingBudget"), "{shown}");
    assert!(shown.contains("500ms"), "{shown}");
}

/// A held stream through the retrying store is the inner store's held one, and a piece that
/// fails is asked again of a new held stream from the bytes handed out, under the policy.
#[test]
fn a_held_stream_that_fails_goes_on_from_the_bytes_it_handed_out() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(3), policy(5));
    let pieces = conformance::block_on(async {
        let mut up = store
            .begin_upload(&key(), created_at(), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"abcdefghi").await.unwrap();
        up.commit().await.unwrap();
        let mut stream = store.hold_stream(key(), 0).await.unwrap().unwrap();
        let len = ReadLen::new(3).unwrap();
        let mut pieces = vec![stream.next(len).await];
        mem.inject_failure(StoreOp::Read, transient(ErrorKind::ConnectionReset));
        for _ in 0..3 {
            pieces.push(stream.next(len).await);
        }
        pieces
    });
    assert_eq!(
        pieces,
        [
            Ok(b"abc".to_vec()),
            Ok(b"def".to_vec()),
            Ok(b"ghi".to_vec()),
            Ok(Vec::new())
        ]
    );
    assert_eq!(sleeper.waits().len(), 1);
    assert_eq!(store.stats().retries, 1);
}

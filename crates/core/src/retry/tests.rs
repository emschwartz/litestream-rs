use std::io::ErrorKind;

use lsr_ltx::{Timestamp, Txid, TxidRange};
use proptest::prelude::*;

use super::*;
use crate::conformance::{self, Harness};
use crate::env::{RecordingSleeper, SeededEntropy};
use crate::layout::CreatedAt;
use crate::mem::MemStore;
use crate::store::{PartNumber, StoreOp};

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
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
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

#[test]
fn upload_write_failure_passes_through_with_one_call() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    conformance::block_on(async {
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
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
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
        up.write(b"x").await.unwrap();
        mem.inject_failure(StoreOp::Upload, transient(ErrorKind::TimedOut));
        let got = up.commit().await;
        assert_eq!(got, Err(transient(ErrorKind::TimedOut)));
    });
    assert!(sleeper.waits().is_empty());
    assert_eq!(store.stats().calls, 3);
}

#[test]
fn a_commit_that_failed_after_publishing_is_not_repeated() {
    let (mem, sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    mem.inject_failure_after_publish(transient(ErrorKind::TimedOut));
    conformance::block_on(async {
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
        up.write(b"x").await.unwrap();
        up.commit().await.unwrap_err();
        let listing = store.list(Level::ZERO, None, limit()).await.unwrap();
        assert_eq!(listing.files.len(), 1);
    });
    assert!(sleeper.waits().is_empty());
}

/// A store whose commits report a size other than the one written.
#[derive(Debug)]
struct SizeLiar(MemStore);

struct LyingUpload(<MemStore as ObjectStore>::Upload);

impl ObjectStore for SizeLiar {
    type Upload = LyingUpload;

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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<LyingUpload, StoreError> {
        Ok(LyingUpload(self.0.begin_upload(key, created_at).await?))
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.0.delete(keys).await
    }
}

impl Upload for LyingUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.0.write(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let mut info = self.0.commit().await?;
        info.size = info.size.saturating_sub(1);
        Ok(info)
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.0.abort().await
    }
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
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
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
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
        up.write(b"abcd").await.unwrap();
        up.commit().await.unwrap()
    });
    assert_eq!(info.size, 4);
}

#[test]
fn abort_passes_through() {
    let (_mem, _sleeper, store) = rig(SeededEntropy::new(1), policy(5));
    conformance::block_on(async {
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
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
fn a_commit_size_mismatch_leaves_no_object_visible() {
    // The wrapper knows the stored object is not what was sent, reports it as
    // Corrupt, and leaves it published: a caller that lists after a failed
    // commit (as the compactor's contract asks) finds the key and trusts it.
    let inner = MemStore::new();
    let store = Retrying::new(
        SizeLiar(inner.clone()),
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let got = conformance::block_on(async {
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
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

/// A store whose deletes are refused.
#[derive(Debug)]
struct DeleteRefuser(SizeLiar);

impl ObjectStore for DeleteRefuser {
    type Upload = LyingUpload;

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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<LyingUpload, StoreError> {
        self.0.begin_upload(key, created_at).await
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
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
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
        let mut up = store.begin_upload(&key(), created_at()).await.unwrap();
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

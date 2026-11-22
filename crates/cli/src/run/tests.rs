//! The S3 path of the commands, in process: `FakeS3` behind the real
//! `S3Store` and the retry layer, holding the same files as a fixture's
//! directory replica.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use lsr_compat::{Manifest, Staged, all_fixtures, stage};
use lsr_core::conformance::block_on;
use lsr_core::{Depth, FakeClock, FileInfo, Inventory, RestoreTarget, verify};
use lsr_ltx::Timestamp;
use lsr_store_fs::FsStore;
use lsr_store_s3::{
    FakeS3, Fault, FaultTransport, S3Config, S3Credentials, S3Store, SecretString, Shape,
    Transport, object_key,
};

use super::*;
use crate::replica::remote;

const NOW: i64 = 1_700_000_000_000;

fn config() -> S3Config {
    let url = "s3://lsr-test/p?endpoint=http%3A%2F%2F127.0.0.1%3A9000&forcePathStyle=true";
    lsr_config::s3_config(
        Some(lsr_config::S3Url::parse(url).unwrap()),
        &lsr_config::S3Overrides::default(),
    )
    .unwrap()
}

fn creds() -> S3Credentials {
    S3Credentials::new(
        "CANARYKEYID".to_owned(),
        SecretString::new("CANARY-SECRET".to_owned()),
    )
}

fn clock() -> FakeClock {
    FakeClock::new(Timestamp::from_unix_millis(NOW))
}

fn staged_fixtures() -> Vec<Staged> {
    let list = all_fixtures().unwrap();
    assert!(!list.is_empty());
    list.iter()
        .map(|fixture| stage(fixture, &Manifest::load(fixture).unwrap()).unwrap())
        .collect()
}

/// The fixture's files in a bucket, each with its stored created-at time unless `with_created_at` is false.
fn bucket_of(staged: &Staged, with_created_at: bool) -> (FakeS3, Vec<FileInfo>) {
    let dir = FsStore::new(staged.replica());
    let infos = block_on(Inventory::load(&dir)).unwrap().files().to_vec();
    let config = config();
    let fake = FakeS3::new(&config, clock());
    for info in &infos {
        let path = staged
            .replica()
            .join("ltx")
            .join(info.key.level().to_string())
            .join(info.key.file_name());
        let created_at = info.created_at().unwrap().at().to_string();
        let meta: &[(&str, &str)] = if with_created_at {
            &[("x-amz-meta-litestream-timestamp", created_at.as_str())]
        } else {
            &[]
        };
        fake.put_raw(
            &object_key(config.prefix(), &info.key),
            fs::read(path).unwrap(),
            meta,
        );
    }
    (fake, infos)
}

/// The files by key, size and time: the origin of a created-at time differs by store.
fn by_time(files: &[FileInfo]) -> Vec<(lsr_core::FileKey, u64, Option<Timestamp>)> {
    files
        .iter()
        .map(|f| (f.key, f.size, f.created_at().map(|s| s.at())))
        .collect()
}

/// The fault layer shared between the store and the test that counts its requests.
struct Shared(Arc<FaultTransport<FakeS3>>);

impl Transport for Shared {
    fn send(
        &self,
        request: lsr_store_s3::HttpRequest,
    ) -> Result<lsr_store_s3::HttpResponse, lsr_store_s3::TransportError> {
        self.0.send(request)
    }
}

fn s3_store<T: Transport>(transport: T) -> Remote<T> {
    remote(S3Store::with_transport(config(), creds(), transport, clock()).unwrap())
}

#[test]
fn files_over_s3_list_what_the_directory_lists() {
    for staged in staged_fixtures() {
        let (fake, infos) = bucket_of(&staged, true);
        let Done::Files {
            inventory,
            created_at_fallbacks,
            ..
        } = block_on(files_on_s3(&s3_store(fake), false, true)).unwrap()
        else {
            panic!("not files");
        };
        assert_eq!(by_time(inventory.files()), by_time(&infos));
        assert_eq!(created_at_fallbacks, Some(0));
    }
}

#[test]
fn files_over_s3_count_every_created_at_taken_from_last_modified() {
    let staged = staged_fixtures().remove(0);
    let (fake, infos) = bucket_of(&staged, false);
    let Done::Files {
        created_at_fallbacks,
        ..
    } = block_on(files_on_s3(&s3_store(fake), false, true)).unwrap()
    else {
        panic!("not files");
    };
    assert_eq!(
        created_at_fallbacks,
        Some(u64::try_from(infos.len()).unwrap())
    );
}

#[test]
fn files_prints_listed_created_at_without_heads() {
    let staged = staged_fixtures().remove(0);
    let (fake, infos) = bucket_of(&staged, true);
    let wire = Arc::new(FaultTransport::new(fake));
    let done = block_on(files_on_s3(
        &s3_store(Shared(Arc::clone(&wire))),
        false,
        false,
    ))
    .unwrap();
    let heads = wire
        .requests()
        .iter()
        .filter(|r| r.shape == Some(Shape::Head))
        .count();
    assert_eq!(heads, 0);
    let Done::Files {
        created_at_fallbacks,
        ..
    } = &done
    else {
        panic!("not files");
    };
    assert_eq!(*created_at_fallbacks, None);
    let text = crate::render::done(&done);
    assert_eq!(text.matches("(listed)").count(), infos.len(), "{text}");
    assert!(!text.contains("LastModified"), "{text}");
}

#[test]
fn files_with_created_at_sends_one_head_per_file() {
    let staged = staged_fixtures().remove(0);
    let (fake, infos) = bucket_of(&staged, true);
    let wire = Arc::new(FaultTransport::new(fake));
    block_on(files_on_s3(
        &s3_store(Shared(Arc::clone(&wire))),
        false,
        true,
    ))
    .unwrap();
    let heads = wire
        .requests()
        .iter()
        .filter(|r| r.shape == Some(Shape::Head))
        .count();
    assert_eq!(heads, infos.len());
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Database(Vec<u8>),
    Refused(String),
}

fn outcome(output: &Path, done: Result<Finished, CliError>) -> Outcome {
    match done {
        Ok(Finished::Restored { .. }) => Outcome::Database(fs::read(output).unwrap()),
        Ok(other) => panic!("not a restore: {other:?}"),
        Err(err) => Outcome::Refused(err.to_string()),
    }
}

fn how(target: RestoreTarget) -> How {
    How {
        target,
        as_of_inside: AsOfInside::Refuse,
        clear_journal_files_first: false,
        integrity_check: IntegrityCheck::Off,
        if_replica_exists: false,
    }
}

/// The latest state, with the full SQLite check: the fixtures are real databases.
fn latest_checked() -> How {
    How {
        integrity_check: IntegrityCheck::Full,
        ..how(RestoreTarget::Latest)
    }
}

#[test]
fn restore_and_verify_over_s3_equal_the_directory() {
    for staged in staged_fixtures() {
        let (fake, _) = bucket_of(&staged, true);
        let store = s3_store(fake);
        let dir = FsStore::new(staged.replica());
        let (dir_db, s3_db) = (staged.output("dir.db"), staged.output("s3.db"));
        let from_dir = outcome(
            &dir_db,
            block_on(restore_on(&dir, &dir_db, latest_checked())),
        );
        let from_s3 = outcome(
            &s3_db,
            block_on(restore_on(&store, &s3_db, latest_checked())),
        );
        assert_eq!(from_s3, from_dir);
        let want = block_on(verify(&dir, Depth::Contents)).unwrap();
        let got = block_on(verify(&store, Depth::Contents)).unwrap();
        assert_eq!(got.worst(), want.worst());
    }
}

#[test]
fn a_403_is_an_environment_error_that_names_no_secret() {
    let staged = staged_fixtures().remove(0);
    let (fake, _) = bucket_of(&staged, true);
    let faults = FaultTransport::new(fake);
    faults.push(Fault::Status {
        on: Shape::List,
        status: 403,
        code: Some("AccessDenied"),
        retry_after: None,
    });
    let err = block_on(files_on_s3(&s3_store(faults), false, false)).unwrap_err();
    assert!(matches!(&err, CliError::Inventory(_)), "{err:?}");
    assert_eq!(exit_for_error(&err), Exit::Environment);
    let line = crate::render::error_line(&err);
    assert!(
        !line.contains("CANARY") && !line.contains("127.0.0.1"),
        "{line}"
    );
}

#[test]
fn a_throttled_listing_is_retried() {
    let staged = staged_fixtures().remove(0);
    let (fake, infos) = bucket_of(&staged, true);
    let faults = FaultTransport::new(fake);
    faults.push(Fault::Status {
        on: Shape::List,
        status: 503,
        code: Some("SlowDown"),
        retry_after: None,
    });
    let Done::Files { inventory, .. } =
        block_on(files_on_s3(&s3_store(faults), false, true)).unwrap()
    else {
        panic!("not files");
    };
    assert_eq!(by_time(inventory.files()), by_time(&infos));
}

/// A dry run over a bucket without stored created-at times reads the exact created-at times an
/// as-of plan needs, as the restore does, and names the chain the restore
/// applies; it writes nothing.
#[test]
fn dry_run_over_s3_plans_what_the_restore_applies() {
    let mut read_total = 0_u64;
    for fixture in all_fixtures().unwrap() {
        let manifest = Manifest::load(&fixture).unwrap();
        let staged = stage(&fixture, &manifest).unwrap();
        for entry in &manifest.targets {
            let Some(target) = entry.target.restore_target() else {
                continue;
            };
            let (fake, _) = bucket_of(&staged, false);
            let store = s3_store(fake);
            let output = staged.output("out.db");
            let planned = block_on(plan_on(&store, &output, how(target)));
            assert!(!output.exists());
            let restored = block_on(restore_on(&store, &output, how(target)));
            match (planned, restored) {
                (
                    Ok(Finished::Planned {
                        chain,
                        created_at_read,
                        ..
                    }),
                    Ok(Finished::Restored { restored, .. }),
                ) => {
                    assert_eq!(chain.reaches(), restored.reached(), "{target:?}");
                    assert_eq!(chain.files().len(), restored.files(), "{target:?}");
                    assert_eq!(created_at_read, restored.created_at_read(), "{target:?}");
                    read_total += created_at_read;
                }
                (Err(plan), Err(done)) => assert_eq!(plan.to_string(), done.to_string()),
                (plan, done) => panic!("{target:?}: dry run {plan:?}, restore {done:?}"),
            }
            if output.exists() {
                fs::remove_file(&output).unwrap();
            }
        }
    }
    assert!(read_total > 0, "an as-of target read created-at times");
}

/// A publish that failed says whether the database is at the output: before the link it
/// is not there; after it, it is there (complete and verified) but may not be durable.
#[test]
fn a_publish_failure_says_whether_the_database_is_visible() {
    let output = Path::new("/restore/app.db");
    for (error, visible) in [
        (
            lsr_local::PublishError::NotVisible {
                target: output.to_owned(),
                step: lsr_local::PublishStep::Link,
                source: std::io::Error::other("disk full"),
            },
            false,
        ),
        (
            lsr_local::PublishError::VisibleNotDurable {
                target: output.to_owned(),
                step: lsr_local::PublishStep::SyncName,
                source: std::io::Error::other("fsync failed"),
            },
            true,
        ),
    ] {
        let failed = RestoreError::Publish {
            left: lsr_core::PublishFailure::left(&error),
            source: Box::new(error),
        };
        let cli = CliError::from(failed);
        assert!(matches!(cli, CliError::Publish(_)), "{cli:?}");
        assert_eq!(exit_for_error(&cli), Exit::Environment);
        let line = crate::render::error_line(&cli);
        assert!(line.contains("/restore/app.db"), "{line}");
        let says_visible = line.contains("is published but may not survive a crash");
        let says_absent = line.contains("was not published");
        assert_eq!((says_visible, says_absent), (visible, !visible), "{line}");
    }
}

/// A store whose listings leave out one object that reads still find: a listing that lags.
struct Unlisting<S> {
    inner: S,
    hidden: Vec<lsr_core::FileKey>,
}

impl<S: ObjectStore> ObjectStore for Unlisting<S> {
    type Upload = S::Upload;

    async fn list(
        &self,
        level: lsr_core::Level,
        after: Option<lsr_ltx::TxidRange>,
        limit: lsr_core::PageLimit,
    ) -> Result<lsr_core::Listing, lsr_core::StoreError> {
        let mut listing = self.inner.list(level, after, limit).await?;
        listing
            .files
            .retain(|file| !self.hidden.contains(&file.key));
        Ok(listing)
    }

    async fn read(
        &self,
        key: &lsr_core::FileKey,
        offset: u64,
        len: lsr_core::ReadLen,
    ) -> Result<Vec<u8>, lsr_core::StoreError> {
        self.inner.read(key, offset, len).await
    }

    async fn created_at(
        &self,
        file: &FileInfo,
    ) -> Result<lsr_core::CreatedAt, lsr_core::StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: lsr_core::ReadLen,
    ) -> Result<(Vec<u8>, lsr_core::CreatedAt), lsr_core::StoreError> {
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &lsr_core::FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, lsr_core::StoreError> {
        self.inner.begin_upload(key, created_at).await
    }

    async fn delete(&self, keys: &[lsr_core::FileKey]) -> Result<(), lsr_core::StoreError> {
        self.inner.delete(keys).await
    }
}

/// A dry run over a listing that hides the newest level-0 file plans nothing and fails as the
/// restore would: printing the older chain as the plan for `Latest` would be another answer.
#[test]
fn a_dry_run_over_a_lagging_listing_fails_like_the_restore() {
    let mut planned = 0;
    for staged in staged_fixtures() {
        let dir = FsStore::new(staged.replica());
        let files = block_on(Inventory::load(&dir)).unwrap().files().to_vec();
        let newest = files.iter().map(|f| f.key.range().max()).max().unwrap();
        let Some(hidden) = files
            .iter()
            .map(|f| f.key)
            .find(|key| key.level().get() == 0 && key.range().max() == newest)
        else {
            continue;
        };
        let store = Unlisting {
            inner: dir,
            hidden: vec![hidden],
        };
        let output = staged.output("dry.db");
        let got = block_on(plan_on(&store, &output, how(RestoreTarget::Latest)));
        assert!(
            matches!(
                got,
                Err(CliError::Restore(RestoreError::ListingBehind { found, .. })) if found == hidden
            ),
            "{got:?}"
        );
        let real = block_on(restore_on(&store, &output, how(RestoreTarget::Latest)));
        assert!(
            real.is_err(),
            "the restore published over a lagging listing"
        );
        planned += 1;
    }
    assert!(planned > 0, "no fixture ends on a level-0 file");
}

/// A listing that shows nothing over a replica whose first file exists is not an empty replica:
/// `--if-replica-exists` must not skip the restore with success, so the application never
/// starts on an empty database over a replica that holds its history.
#[test]
fn if_replica_exists_over_a_listing_that_shows_nothing_skips_nothing() {
    let first = lsr_core::FileKey::new(
        lsr_core::Level::ZERO,
        lsr_ltx::TxidRange::single(lsr_ltx::Txid::new(1).unwrap()),
    );
    let mut judged = 0;
    for staged in staged_fixtures() {
        let dir = FsStore::new(staged.replica());
        let files = block_on(Inventory::load(&dir)).unwrap().files().to_vec();
        if !files.iter().any(|f| f.key == first) {
            continue;
        }
        let store = Unlisting {
            inner: dir,
            hidden: files.iter().map(|f| f.key).collect(),
        };
        let output = staged.output("lagging.db");
        let latest = How {
            if_replica_exists: true,
            ..how(RestoreTarget::Latest)
        };
        let got = block_on(restore_on(&store, &output, latest));
        assert!(
            matches!(
                got,
                Err(CliError::Restore(RestoreError::ListingBehind { found, .. })) if found == first
            ),
            "{got:?}"
        );
        let first_txid = How {
            if_replica_exists: true,
            ..how(RestoreTarget::Txid(lsr_ltx::Txid::new(1).unwrap()))
        };
        let planned = block_on(plan_on(&store, &output, first_txid));
        assert!(
            matches!(
                planned,
                Err(CliError::Restore(RestoreError::ListingBehind { .. }))
            ),
            "{planned:?}"
        );
        judged += 1;
    }
    assert!(
        judged > 0,
        "no fixture starts with a level-0 file at transaction 1"
    );
}

//! The S3 path of the commands, in process: `FakeS3` behind the real
//! `S3Store` and the retry layer, holding the same files as a fixture's
//! directory replica.

use std::fs;
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
    S3Config::from_url("s3://lsr-test/p?endpoint=http%3A%2F%2F127.0.0.1%3A9000&forcePathStyle=true")
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

/// The fixture's files in a bucket, each with its stored stamp unless `stamped` is false.
fn bucket_of(staged: &Staged, stamped: bool) -> (FakeS3, Vec<FileInfo>) {
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
        let stamp = info.exact_stamp().unwrap().at().to_string();
        let meta: &[(&str, &str)] = if stamped {
            &[("x-amz-meta-litestream-timestamp", stamp.as_str())]
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

/// The files by key, size and time: the origin of a stamp differs by store.
fn by_time(files: &[FileInfo]) -> Vec<(lsr_core::FileKey, u64, Option<Timestamp>)> {
    files
        .iter()
        .map(|f| (f.key, f.size, f.exact_stamp().map(|s| s.at())))
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
            stamp_fallbacks,
            ..
        } = block_on(files_on_s3(&s3_store(fake), false, true)).unwrap()
        else {
            panic!("not files");
        };
        assert_eq!(by_time(inventory.files()), by_time(&infos));
        assert_eq!(stamp_fallbacks, Some(0));
    }
}

#[test]
fn files_over_s3_count_every_stamp_taken_from_last_modified() {
    let staged = staged_fixtures().remove(0);
    let (fake, infos) = bucket_of(&staged, false);
    let Done::Files {
        stamp_fallbacks, ..
    } = block_on(files_on_s3(&s3_store(fake), false, true)).unwrap()
    else {
        panic!("not files");
    };
    assert_eq!(stamp_fallbacks, Some(u64::try_from(infos.len()).unwrap()));
}

#[test]
fn files_prints_listed_stamps_without_heads() {
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
        stamp_fallbacks, ..
    } = &done
    else {
        panic!("not files");
    };
    assert_eq!(*stamp_fallbacks, None);
    let text = crate::render::done(&done);
    assert_eq!(text.matches("(listed)").count(), infos.len(), "{text}");
    assert!(!text.contains("LastModified"), "{text}");
}

#[test]
fn files_with_stamps_sends_one_head_per_file() {
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

fn outcome(done: Result<Done, CliError>) -> Outcome {
    match done {
        Ok(Done::Restored { output, .. }) => Outcome::Database(fs::read(output).unwrap()),
        Ok(_) => panic!("not a restore"),
        Err(err) => Outcome::Refused(err.to_string()),
    }
}

#[test]
fn restore_and_verify_over_s3_equal_the_directory() {
    for staged in staged_fixtures() {
        let (fake, _) = bucket_of(&staged, true);
        let store = s3_store(fake);
        let dir = FsStore::new(staged.replica());
        let from_dir = outcome(block_on(restore_on(
            &dir,
            staged.output("dir.db"),
            RestoreTarget::Latest,
            false,
        )));
        let from_s3 = outcome(block_on(restore_on(
            &store,
            staged.output("s3.db"),
            RestoreTarget::Latest,
            false,
        )));
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

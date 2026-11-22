//! A fixture's replica on a prefix of a local MinIO bucket, so Go's AWS SDK and Rust's S3 store
//! meet on one bucket (`just compat-s3`).
//!
//! These checks must never reach a real object store, and the machines that run them may hold
//! real keys in `AWS_*` variables. So: the endpoint, bucket and the throwaway credentials come
//! only from the `LSR_COMPAT_S3_*` variables the recipe sets ([`MinioEnv::from_env`]), never
//! from `AWS_*`; the endpoint is refused unless it is plain `http` to a loopback host
//! ([`loopback_endpoint`]), before any request; the config files both programs read name that
//! endpoint, path style and the credentials explicitly; and every child process the harness
//! starts gets an environment with every `AWS_*` and `LITESTREAM_*` variable removed and an
//! empty home directory ([`scrub_env`]), so neither Go's SDK nor Rust's credential chain can
//! find a key, a profile or an endpoint of its own.
//!
//! The harness reads the bucket with `lsr-store-s3` directly, credentials passed in. Listings
//! page two files at a time through the store's cursor, so every poll also exercises listing
//! across pages and the start-after keys on whatever the writer named.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use lsr_core::conformance::block_on;
use lsr_core::{
    FileInfo, FileKey, Level, ObjectStore, PageLimit, ReadLen, RestoreError, RestoreTarget,
    Restored, StoreError, cmp_range, restore,
};
use lsr_local::FileSink;
use lsr_ltx::Txid;
use lsr_store_s3::{BucketName, Endpoint, Prefix, S3Config, S3Credentials, S3Store, SecretString};

use crate::error::CompatError;
use crate::replica::{Summary, rel_of, summarize_bytes};

/// The variable that names the MinIO endpoint; unset, the S3 checks fail (`LSR_SKIP=minio` opts out).
pub const ENDPOINT_VAR: &str = "LSR_COMPAT_S3_ENDPOINT";
/// The bucket, which must exist.
pub const BUCKET_VAR: &str = "LSR_COMPAT_S3_BUCKET";
/// The throwaway access key of the MinIO container.
pub const ACCESS_KEY_VAR: &str = "LSR_COMPAT_S3_ACCESS_KEY";
/// The throwaway secret of the MinIO container.
pub const SECRET_KEY_VAR: &str = "LSR_COMPAT_S3_SECRET_KEY";

/// Variable prefixes no child process may inherit: Go's SDK reads `AWS_*` (keys, profiles,
/// endpoints, regions) and Go copies `LITESTREAM_*` keys into them; Rust's chain reads both.
const SCRUBBED_PREFIXES: [&str; 2] = ["AWS_", "LITESTREAM_"];

/// Files per listing page, so even a short run lists across several pages.
const LIST_PAGE: u16 = 2;

/// Checks that `text` is an `http` URL to a loopback host (`127.0.0.1`, `::1` or `localhost`)
/// with no path, and returns it normalised (`http://<host>:<port>`), the one form written to
/// config files.
///
/// # Errors
/// [`CompatError::Fixture`] for anything else: these checks never talk to a real store.
pub fn loopback_endpoint(text: &str) -> Result<String, CompatError> {
    let refuse = |why: &str| {
        CompatError::Fixture(format!(
            "{ENDPOINT_VAR} must be http:// to 127.0.0.1, ::1 or localhost (a local MinIO): {why}"
        ))
    };
    let endpoint = Endpoint::new(text).map_err(|e| refuse(&format!("does not parse: {e}")))?;
    let url = endpoint.url();
    if url.scheme() != "http" {
        return Err(refuse("not http"));
    }
    let host = url.host_str().unwrap_or("");
    if !matches!(host, "127.0.0.1" | "[::1]" | "localhost") {
        return Err(refuse(&format!("host {host:?} is not loopback")));
    }
    if url.path() != "/" {
        return Err(refuse("it has a path"));
    }
    let port = url.port_or_known_default().unwrap_or(80);
    Ok(format!("http://{host}:{port}"))
}

/// Where the S3 checks run: a loopback endpoint, a bucket and the MinIO container's
/// throwaway credentials, all from the `LSR_COMPAT_S3_*` variables.
#[derive(Clone)]
pub struct MinioEnv {
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
}

impl std::fmt::Debug for MinioEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MinioEnv")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl MinioEnv {
    /// Reads the `LSR_COMPAT_S3_*` variables.
    ///
    /// # Errors
    /// [`CompatError::Fixture`] when the endpoint is unset or not loopback `http`, or the
    /// bucket or a credential is missing.
    pub fn from_env() -> Result<Self, CompatError> {
        let endpoint = std::env::var_os(ENDPOINT_VAR)
            .ok_or_else(|| CompatError::Fixture(format!("{ENDPOINT_VAR} is unset")))?;
        let endpoint = loopback_endpoint(&endpoint.to_string_lossy())?;
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    CompatError::Fixture(format!("{name} is unset ({ENDPOINT_VAR} is set)"))
                })
        };
        Ok(Self {
            endpoint,
            bucket: var(BUCKET_VAR)?,
            access_key: var(ACCESS_KEY_VAR)?,
            secret_key: var(SECRET_KEY_VAR)?,
        })
    }

    /// The normalised loopback endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// A replica under one prefix of the MinIO bucket.
pub struct S3Replica {
    env: MinioEnv,
    prefix: String,
    store: S3Store,
}

impl std::fmt::Debug for S3Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Replica")
            .field("endpoint", &self.env.endpoint)
            .field("bucket", &self.env.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

fn store_error(what: &str) -> impl FnOnce(StoreError) -> CompatError + '_ {
    move |e| CompatError::Fixture(format!("{what} on MinIO: {e}"))
}

impl S3Replica {
    /// A replica under `prefix`, which the caller makes unique to the run.
    ///
    /// # Errors
    /// [`CompatError::Fixture`] when the bucket, prefix or endpoint cannot make a store.
    pub fn new(env: &MinioEnv, prefix: &str) -> Result<Self, CompatError> {
        let bad = |e: lsr_store_s3::ConfigError| CompatError::Fixture(format!("S3 config: {e}"));
        let config = S3Config::new(
            Endpoint::new(&env.endpoint).map_err(bad)?,
            BucketName::new(&env.bucket).map_err(bad)?,
            Prefix::new(prefix).map_err(bad)?,
        )
        .with_style(lsr_store_s3::UrlStyle::Path);
        let credentials = S3Credentials::new(
            env.access_key.clone(),
            SecretString::new(env.secret_key.clone()),
        );
        Ok(Self {
            env: env.clone(),
            prefix: prefix.to_owned(),
            store: S3Store::new(config, credentials).map_err(bad)?,
        })
    }

    /// The key prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The `replica:` block both Go and `lsr replicate -config` read, indented for a `dbs`
    /// entry: the endpoint, path style, region and credentials all explicit.
    pub(crate) fn yaml(&self) -> String {
        format!(
            "    replica:
      type: s3
      bucket: {bucket}
      path: {prefix:?}
      endpoint: {endpoint}
      region: us-east-1
      force-path-style: true
      access-key-id: {access:?}
      secret-access-key: {secret:?}
      sync-interval: 100ms
",
            bucket = self.env.bucket,
            prefix = self.prefix,
            endpoint = self.env.endpoint,
            access = self.env.access_key,
            secret = self.env.secret_key,
        )
    }

    /// Every file of every level, listed [`LIST_PAGE`] at a time.
    ///
    /// # Errors
    /// [`CompatError::Fixture`] when a listing fails or its cursor does not advance.
    pub fn files(&self) -> Result<Vec<FileInfo>, CompatError> {
        let page = PageLimit::new(LIST_PAGE)
            .ok_or_else(|| CompatError::Fixture("listing page size".to_owned()))?;
        let mut out = Vec::new();
        for level in Level::ALL {
            let mut after = None;
            loop {
                let listing =
                    block_on(self.store.list(level, after, page)).map_err(store_error("list"))?;
                out.extend(listing.files);
                match listing.next {
                    Some(next) if after.is_none_or(|prev| cmp_range(next, prev).is_gt()) => {
                        after = Some(next);
                    }
                    Some(next) => {
                        return Err(CompatError::Fixture(format!(
                            "the listing of level {level} did not advance past {next:?}"
                        )));
                    }
                    None => break,
                }
            }
        }
        Ok(out)
    }

    /// The newest transaction any file covers.
    ///
    /// # Errors
    /// As [`S3Replica::files`].
    pub fn newest_txid(&self) -> Result<Option<Txid>, CompatError> {
        Ok(self.files()?.iter().map(|f| f.key.range().max()).max())
    }

    /// The level-9 files, by the path a file replica would give them.
    ///
    /// # Errors
    /// As [`S3Replica::files`].
    pub fn snapshots(&self) -> Result<BTreeSet<String>, CompatError> {
        Ok(self
            .files()?
            .iter()
            .filter(|f| f.key.level() == Level::SNAPSHOT)
            .map(|f| rel_of(&f.key))
            .collect())
    }

    /// The size of the largest object, to show an upload went past the multipart part size.
    ///
    /// # Errors
    /// As [`S3Replica::files`].
    pub fn largest(&self) -> Result<u64, CompatError> {
        Ok(self.files()?.iter().map(|f| f.size).max().unwrap_or(0))
    }

    /// Reads one object whole and verifies it; `None` when it vanished first.
    fn summarize(&self, file: &FileInfo) -> Result<Option<Summary>, CompatError> {
        let len = u32::try_from(file.size)
            .ok()
            .and_then(ReadLen::new)
            .ok_or_else(|| CompatError::Fixture(format!("{} is too large", rel_of(&file.key))))?;
        match block_on(self.store.read(&file.key, 0, len)) {
            Ok(bytes) => summarize_bytes(&bytes, &rel_of(&file.key)).map(Some),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(store_error("read")(e)),
        }
    }

    /// The level-0 file whose transactions start at `min`, read whole.
    ///
    /// # Errors
    /// As [`S3Replica::files`], or when the file does not decode.
    pub fn l0_starting_at(&self, min: Txid) -> Result<Option<Summary>, CompatError> {
        let found = self
            .files()?
            .into_iter()
            .find(|f| f.key.level() == Level::ZERO && f.key.range().min() == min);
        match found {
            Some(file) => self.summarize(&file),
            None => Ok(None),
        }
    }

    /// Restores `target` with the Rust executor over this store into a new database at `out`.
    ///
    /// # Errors
    /// [`CompatError`] when the sink cannot be created; the executor's refusal is the inner `Err`.
    pub fn restore(
        &self,
        target: RestoreTarget,
        out: &Path,
    ) -> Result<Result<Restored, RestoreError>, CompatError> {
        let sink = FileSink::create(out)
            .map_err(|e| CompatError::Fixture(format!("creating the restore sink: {e}")))?;
        Ok(block_on(restore(&self.store, target, sink)))
    }

    /// Deletes every file under the prefix, so a run leaves the bucket as it found it.
    ///
    /// # Errors
    /// As [`S3Replica::files`], or when the delete fails.
    pub fn delete_all(&self) -> Result<(), CompatError> {
        let keys: Vec<FileKey> = self.files()?.iter().map(|f| f.key).collect();
        for chunk in keys.chunks(500) {
            block_on(self.store.delete(chunk)).map_err(store_error("delete"))?;
        }
        Ok(())
    }
}

/// Removes every `AWS_*` and `LITESTREAM_*` variable from what `command` inherits or was given, and points
/// its home directory at `home` (created empty if missing), so no credential, profile, shared
/// config file or endpoint of the machine's own reaches a child. Applied to every Go and `lsr`
/// process the harness starts, directory replicas included.
///
/// # Errors
/// [`CompatError::Io`] when `home` cannot be created.
pub fn scrub_env(command: &mut Command, home: &Path) -> Result<(), CompatError> {
    std::fs::create_dir_all(home).map_err(CompatError::io(home))?;
    let set: Vec<_> = command
        .get_envs()
        .map(|(name, _)| name.to_owned())
        .collect();
    let inherited = std::env::vars_os().map(|(name, _)| name);
    for name in inherited.chain(set) {
        if is_scrubbed(&name) {
            command.env_remove(&name);
        }
    }
    command.env("HOME", home);
    Ok(())
}

fn is_scrubbed(name: &OsStr) -> bool {
    let name = name.to_string_lossy();
    SCRUBBED_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// The empty home directory a child started beside `file` gets.
pub(crate) fn home_beside(file: &Path) -> PathBuf {
    file.parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".scrubbed-home")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_http_endpoints_pass_normalised() {
        for (text, want) in [
            ("http://127.0.0.1:9000", "http://127.0.0.1:9000"),
            ("http://127.0.0.1:9000/", "http://127.0.0.1:9000"),
            ("http://localhost:9100", "http://localhost:9100"),
            ("http://[::1]:9000", "http://[::1]:9000"),
            ("http://127.0.0.1", "http://127.0.0.1:80"),
        ] {
            assert_eq!(loopback_endpoint(text).expect(text), want);
        }
    }

    #[test]
    fn anything_but_loopback_http_is_refused() {
        for text in [
            "https://127.0.0.1:9000",
            "http://fly.storage.tigris.dev",
            "https://s3.us-east-1.amazonaws.com",
            "http://localhost.example.com:9000",
            "http://127.0.0.1.nip.io:9000",
            "http://10.0.0.1:9000",
            "http://192.168.1.2:9000",
            "http://user:pw@127.0.0.1:9000",
            "http://127.0.0.1:9000/bucket",
            "http://127.0.0.1:9000?x=1",
            "s3://127.0.0.1:9000",
            "127.0.0.1:9000",
            "",
        ] {
            let got = loopback_endpoint(text);
            assert!(
                matches!(&got, Err(CompatError::Fixture(why)) if why.contains(ENDPOINT_VAR)),
                "{text:?} was accepted: {got:?}"
            );
        }
    }

    #[test]
    fn credential_variables_are_scrubbed_and_others_kept() {
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
            "AWS_PROFILE",
            "AWS_CONFIG_FILE",
            "LITESTREAM_ACCESS_KEY_ID",
            "LITESTREAM_SECRET_ACCESS_KEY",
        ] {
            assert!(is_scrubbed(OsStr::new(name)), "{name}");
        }
        for name in ["PATH", "LSR_COMPAT_S3_ENDPOINT", "TMPDIR", "XAWS_KEY"] {
            assert!(!is_scrubbed(OsStr::new(name)), "{name}");
        }
    }

    #[test]
    fn a_scrubbed_child_sees_no_aws_variable() {
        let home = tempfile::tempdir().expect("home");
        let mut command = Command::new("env");
        command
            .env("AWS_ACCESS_KEY_ID", "CANARY")
            .env("LITESTREAM_SECRET_ACCESS_KEY", "CANARY");
        scrub_env(&mut command, home.path()).expect("scrub");
        let out = command.output().expect("env runs");
        let text = String::from_utf8_lossy(&out.stdout);
        // The messages never print the environment: it may hold real keys.
        assert!(
            !text.contains("CANARY"),
            "a variable set on the command reached the child"
        );
        // Whatever this process inherited (real keys, on some machines) is gone too.
        assert!(
            text.lines()
                .all(|l| !l.starts_with("AWS_") && !l.starts_with("LITESTREAM_")),
            "a credential variable reached the child"
        );
        assert!(text.contains(&format!("HOME={}", home.path().display())));
    }
}

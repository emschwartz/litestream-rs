//! Where S3 credentials come from when the caller does not hand them over:
//! the parts of the AWS default credential chain that need no network and no
//! subprocess, as Go Litestream (041a8c3, with the AWS SDK for Go v2, config
//! v1.32.6) runs them.
//!
//! 0. **The shared files, first, whatever source wins.** The SDK loads the shared
//!    config file (`AWS_CONFIG_FILE`, else `~/.aws/config`) and credentials file
//!    (`AWS_SHARED_CREDENTIALS_FILE`, else `~/.aws/credentials`), combines them
//!    (`shared` module) and picks the profile `AWS_PROFILE`, else
//!    `AWS_DEFAULT_PROFILE`, else `default`, before it looks at any credentials. So
//!    these stop every resolution, explicit and environment keys included, as they
//!    stop Go's start: a profile a non-empty `AWS_PROFILE` names that no file has
//!    ([`CredentialError::ProfileNotFound`]); a chosen profile holding only one of
//!    the key id and secret ([`CredentialError::PartialProfile`]); a file that opens
//!    but cannot be read ([`CredentialError::ReadFailed`]). A file that cannot be
//!    opened is skipped, as in Go.
//! 1. Explicit values (a library caller's [`CredentialChain::with_explicit`], and
//!    the daemon's `access-key-id` and `secret-access-key`).
//! 2. The environment: the key id from `AWS_ACCESS_KEY_ID`, else
//!    `AWS_ACCESS_KEY`; the secret from `AWS_SECRET_ACCESS_KEY`, else
//!    `AWS_SECRET_KEY`; with both, `AWS_SESSION_TOKEN` if set. An empty
//!    variable counts as unset. `LITESTREAM_ACCESS_KEY_ID` and
//!    `LITESTREAM_SECRET_ACCESS_KEY` stand in for their `AWS_*` names only when those
//!    are unset, as Go copies them; an empty `AWS_*` variable blocks the copy.
//! 3. The chosen profile's static keys, when it holds a non-empty key id and secret,
//!    with its session token.
//!
//! When none has them, [`CredentialError::Missing`] names every source and why it
//! gave nothing; no error or `Debug` output holds a value. A line in a shared file
//! that is not INI is refused, where Go skips it (see the `ini` module and
//! `docs/decisions.md` D43).
//!
//! Not built, because each needs a network service or a subprocess: the EC2
//! instance metadata service (IMDS), ECS and EKS container credentials
//! (`AWS_CONTAINER_CREDENTIALS_*`), web-identity tokens
//! (`AWS_WEB_IDENTITY_TOKEN_FILE`, `web_identity_token_file`), assumed roles
//! (`role_arn`, `source_profile`, `credential_source`), IAM Identity Center
//! (`sso_*`, `login_session`) and `credential_process`. A profile that holds
//! only those settings reads as having no keys.
//!
//! The process environment is read only by [`CredentialChain::from_process`];
//! tests hand the resolver an environment of their own, so they never mutate
//! the process's and can run in parallel.

mod ini;
mod process;
mod remote;
mod shared;
#[cfg(test)]
mod sources_tests;
#[cfg(test)]
mod tests;

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;
use zeroize::Zeroizing;

use crate::credentials::{S3Credentials, SecretString};
use crate::source::{CredentialSource, Expiring};
use crate::transport::Transport;
use ini::Sections;
use lsr_core::Clock;
use shared::Profile;

/// The variables tried for the access key id, in order.
pub(crate) const ACCESS_KEY_ID_VARS: [&str; 3] = [
    "AWS_ACCESS_KEY_ID",
    "LITESTREAM_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY",
];
/// The variables tried for the secret access key, in order.
pub(crate) const SECRET_ACCESS_KEY_VARS: [&str; 3] = [
    "AWS_SECRET_ACCESS_KEY",
    "LITESTREAM_SECRET_ACCESS_KEY",
    "AWS_SECRET_KEY",
];
/// The variable holding a session token, read with environment keys only.
pub(crate) const SESSION_TOKEN_VAR: &str = "AWS_SESSION_TOKEN";
/// The variable naming the profile; a profile it names must exist.
pub(crate) const PROFILE_VAR: &str = "AWS_PROFILE";
/// The profile variable read when [`PROFILE_VAR`] is unset; not checked.
pub(crate) const DEFAULT_PROFILE_VAR: &str = "AWS_DEFAULT_PROFILE";
/// The variable overriding the shared credentials file's path.
pub(crate) const SHARED_CREDENTIALS_FILE_VAR: &str = "AWS_SHARED_CREDENTIALS_FILE";
/// The variable overriding the shared config file's path.
pub(crate) const CONFIG_FILE_VAR: &str = "AWS_CONFIG_FILE";
/// The variable holding the home directory the default paths sit under.
#[cfg(windows)]
pub const HOME_VAR: &str = "USERPROFILE";
/// The variable holding the home directory the default paths sit under.
#[cfg(not(windows))]
pub(crate) const HOME_VAR: &str = "HOME";
/// The profile read when no variable names one.
pub(crate) const DEFAULT_PROFILE: &str = "default";
/// The largest shared file read; real ones are a few hundred bytes.
pub(crate) const MAX_SHARED_FILE_BYTES: u64 = 1024 * 1024;

type Env = dyn Fn(&str) -> Option<String> + Send + Sync;

/// The sources credentials are looked for in, and the environment they read.
pub struct CredentialChain {
    explicit: Option<S3Credentials>,
    env: Box<Env>,
    /// The client the sources that need a network call use; without one they are skipped.
    http: Option<remote::Http>,
    /// The signing region, for STS's regional endpoint.
    region: Option<String>,
}

impl fmt::Debug for CredentialChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialChain")
            .field("explicit", &self.explicit)
            .finish_non_exhaustive()
    }
}

impl CredentialChain {
    /// The chain over this process's environment and files.
    #[must_use]
    pub fn from_process() -> Self {
        Self {
            explicit: None,
            env: Box::new(|var| std::env::var(var).ok()),
            http: default_http(),
            region: None,
        }
    }

    /// The chain over an environment the caller supplies, for tests.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn with_env(env: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        Self {
            explicit: None,
            env: Box::new(env),
            http: None,
            region: None,
        }
    }

    /// The client the web-identity, container and instance-metadata sources use, and the
    /// clock that caps an instance-metadata expiry.
    #[must_use]
    pub fn with_transport(mut self, transport: Arc<dyn Transport>, clock: Arc<dyn Clock>) -> Self {
        self.http = Some(remote::Http { transport, clock });
        self
    }

    /// The replica's signing region, which picks STS's regional endpoint.
    #[must_use]
    pub fn with_region(mut self, region: &str) -> Self {
        self.region = Some(region.to_owned());
        self
    }

    /// Credentials that win over every other source.
    #[must_use]
    pub fn with_explicit(mut self, credentials: S3Credentials) -> Self {
        self.explicit = Some(credentials);
        self
    }

    /// The first source's credentials.
    ///
    /// # Errors
    /// [`CredentialError::Missing`] when no source has both a key id and a
    /// secret; the other variants for a shared file that cannot be used.
    pub fn resolve(&self) -> Result<Resolved, CredentialError> {
        let (profile, named) = self.profile();
        let environment = self.env_keys();
        let files = self.shared_files(&profile)?;
        let gap = match &environment {
            Ok(_) => EnvGap::Both,
            Err(gap) => *gap,
        };
        let tried = Tried {
            environment: gap,
            profile: profile.clone(),
            credentials_file: files.credentials_look,
            config_file: files.config_look,
        };
        let section = match shared::choose(files.merged, &profile) {
            Profile::Absent if named => return Err(CredentialError::ProfileNotFound(tried)),
            Profile::Absent => None,
            Profile::Partial(path) => {
                return Err(CredentialError::PartialProfile { path, profile });
            }
            Profile::Present(section) => Some(section),
        };
        if let Some(section) = &section {
            validate(section, &profile)?;
        }
        if let Some(credentials) = &self.explicit {
            return Ok(Resolved::fixed(credentials.clone(), Source::Explicit));
        }
        if let Ok(credentials) = environment {
            return Ok(Resolved::fixed(credentials, Source::Environment));
        }
        if let Some(file) = self.var("AWS_WEB_IDENTITY_TOKEN_FILE") {
            let role = self.var("AWS_ROLE_ARN");
            return self.web_identity(file, role, self.var("AWS_ROLE_SESSION_NAME"));
        }
        if let Some(section) = &section
            && let Some(resolved) =
                self.profile_source(section, &files.credentials_path, &profile)?
        {
            return Ok(resolved);
        }
        self.host_source(tried)
    }

    /// Go's `resolveCredsFromProfile` for the chosen profile, up to the sources that
    /// belong to the host (container and instance metadata).
    fn profile_source(
        &self,
        section: &ini::Section,
        credentials_path: &Option<PathBuf>,
        profile: &str,
    ) -> Result<Option<Resolved>, CredentialError> {
        let key = |name: &str| nonempty(Some(section.string(name))).map(str::to_owned);
        if key("source_profile").is_some() {
            return Err(not_built("an assumed role (source_profile)"));
        }
        if let Some((credentials, from_credentials, path)) = profile_keys(section, credentials_path)
        {
            let profile = profile.to_owned();
            let source = if from_credentials {
                Source::SharedCredentialsFile { path, profile }
            } else {
                Source::SharedConfigFile { path, profile }
            };
            return Ok(Some(Resolved::fixed(credentials, source)));
        }
        if key("credential_source").is_some() {
            return Err(not_built("an assumed role (credential_source)"));
        }
        if let Some(file) = key("web_identity_token_file") {
            let resolved = self.web_identity(file, key("role_arn"), key("role_session_name"))?;
            return Ok(Some(resolved));
        }
        if SSO_KEYS.iter().any(|name| key(name).is_some()) {
            return Err(not_built("IAM Identity Center (sso_*)"));
        }
        if key("login_session").is_some() {
            return Err(not_built("a login session (login_session)"));
        }
        match key("credential_process") {
            Some(command) => Ok(Some(remote_resolved(Arc::new(process::Process {
                command,
            }))?)),
            None => Ok(None),
        }
    }

    /// The container credentials, then the instance metadata service, as Go tries them
    /// last; without an HTTP client, neither.
    fn host_source(&self, tried: Tried) -> Result<Resolved, CredentialError> {
        let Some(http) = self.http.clone() else {
            return Err(CredentialError::Missing(tried));
        };
        let relative = self.var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
        let full = self.var("AWS_CONTAINER_CREDENTIALS_FULL_URI");
        if let Some(url) = remote::Container::uri(relative, full) {
            return remote_resolved(Arc::new(remote::Container {
                http,
                url: url?,
                token: self.var("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
                token_file: self.var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"),
            }));
        }
        if self
            .var(IMDS_DISABLED_VAR)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
        {
            return Err(CredentialError::Missing(tried));
        }
        let endpoint = remote::InstanceMetadata::endpoint(
            self.var("AWS_EC2_METADATA_SERVICE_ENDPOINT"),
            self.var("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE"),
        );
        let v1_disabled = self
            .var("AWS_EC2_METADATA_V1_DISABLED")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));
        remote_resolved(Arc::new(remote::InstanceMetadata {
            http,
            endpoint,
            v1_disabled,
        }))
    }

    fn web_identity(
        &self,
        token_file: String,
        role_arn: Option<String>,
        session_name: Option<String>,
    ) -> Result<Resolved, CredentialError> {
        let config = |why: &str| {
            CredentialError::Source(SourceError::Config {
                provider: "web identity",
                why: why.to_owned(),
            })
        };
        let role_arn = role_arn.ok_or_else(|| config("role ARN is not set"))?;
        let http = self.http.clone().ok_or_else(|| config("no HTTP client"))?;
        let region = self.region.clone().unwrap_or_default();
        let endpoint = self
            .var("AWS_ENDPOINT_URL_STS")
            .or_else(|| self.var("AWS_ENDPOINT_URL"))
            .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"));
        remote_resolved(Arc::new(remote::WebIdentity {
            http,
            endpoint,
            token_file,
            role_arn,
            session_name,
        }))
    }

    fn var(&self, name: &str) -> Option<String> {
        (self.env)(name).filter(|value| !value.is_empty())
    }

    /// An `AWS_*` variable as the SDK sees it in Go Litestream, whose `Run` first
    /// copies `LITESTREAM_*` into it only when it is unset (an empty `AWS_*` stays).
    fn aws_var(&self, aws: &str, litestream: &str) -> Option<String> {
        (self.env)(aws)
            .or_else(|| (self.env)(litestream))
            .filter(|value| !value.is_empty())
    }

    fn env_keys(&self) -> Result<S3Credentials, EnvGap> {
        let [id_var, litestream_id, short_id] = ACCESS_KEY_ID_VARS;
        let [secret_var, litestream_secret, short_secret] = SECRET_ACCESS_KEY_VARS;
        let id = self
            .aws_var(id_var, litestream_id)
            .or_else(|| self.var(short_id));
        let secret = self
            .aws_var(secret_var, litestream_secret)
            .or_else(|| self.var(short_secret));
        let (id, secret) = match (id, secret) {
            (Some(id), Some(secret)) => (id, SecretString::new(secret)),
            (None, Some(_)) => return Err(EnvGap::KeyId),
            (Some(_), None) => return Err(EnvGap::Secret),
            (None, None) => return Err(EnvGap::Both),
        };
        let credentials = S3Credentials::new(id, secret);
        Ok(match self.var(SESSION_TOKEN_VAR) {
            Some(token) => credentials.with_session_token(SecretString::new(token)),
            None => credentials,
        })
    }

    /// The profile and whether [`PROFILE_VAR`] named it.
    fn profile(&self) -> (String, bool) {
        match self.var(PROFILE_VAR) {
            Some(profile) => (profile, true),
            None => (
                self.var(DEFAULT_PROFILE_VAR)
                    .unwrap_or_else(|| DEFAULT_PROFILE.to_owned()),
                false,
            ),
        }
    }

    fn path_of(&self, kind: FileKind) -> Option<PathBuf> {
        match self.var(kind.override_var()) {
            Some(path) => Some(PathBuf::from(path)),
            None => self
                .var(HOME_VAR)
                .map(|home| Path::new(&home).join(".aws").join(kind.file_name())),
        }
    }

    /// Both files, read and combined as the SDK does at load.
    fn shared_files(&self, profile: &str) -> Result<Files, CredentialError> {
        let (config, config_look) = self.one_file(FileKind::Config, profile)?;
        let (credentials, credentials_look) = self.one_file(FileKind::Credentials, profile)?;
        let credentials_path = match &credentials_look {
            FileLook::At { path, .. } => Some(path.clone()),
            FileLook::NoHome => None,
        };
        Ok(Files {
            merged: shared::merge(config, credentials),
            credentials_path,
            credentials_look,
            config_look,
        })
    }

    fn one_file(
        &self,
        kind: FileKind,
        profile: &str,
    ) -> Result<(Sections, FileLook), CredentialError> {
        let Some(path) = self.path_of(kind) else {
            return Ok((Sections::new(), FileLook::NoHome));
        };
        let text = match read_bounded(&path)? {
            Ok(text) => text,
            Err(outcome) => return Ok((Sections::new(), FileLook::At { path, outcome })),
        };
        let parsed = match kind {
            FileKind::Config => shared::config_file(&text, &path),
            FileKind::Credentials => shared::credentials_file(&text, &path),
        };
        let sections = parsed.map_err(|bad| CredentialError::MalformedLine {
            path: path.clone(),
            line: bad.line,
        })?;
        let outcome = if sections.contains_key(profile) {
            FileOutcome::NoKeys
        } else {
            FileOutcome::NoProfile
        };
        Ok((sections, FileLook::At { path, outcome }))
    }
}

/// The variable that turns the instance metadata service off.
const IMDS_DISABLED_VAR: &str = "AWS_EC2_METADATA_DISABLED";
/// The keys that make a profile an IAM Identity Center one (`hasSSOConfiguration`).
const SSO_KEYS: [&str; 5] = [
    "sso_session",
    "sso_account_id",
    "sso_region",
    "sso_role_name",
    "sso_start_url",
];

/// The HTTP client the remote sources use by default: a short deadline, since a host
/// without the instance metadata service should fail fast.
fn default_http() -> Option<remote::Http> {
    #[cfg(feature = "ureq")]
    {
        let transport: Arc<dyn Transport> =
            Arc::new(crate::UreqTransport::new(std::time::Duration::from_secs(5)));
        Some(remote::Http {
            transport,
            clock: Arc::new(crate::SystemClock),
        })
    }
    #[cfg(not(feature = "ureq"))]
    {
        None
    }
}

fn not_built(what: &'static str) -> CredentialError {
    CredentialError::NotBuilt { what }
}

/// Fetches from a source and keeps it for refreshes.
fn remote_resolved(source: Arc<dyn CredentialSource>) -> Result<Resolved, CredentialError> {
    let fetched = source.fetch()?;
    Ok(Resolved {
        credentials: fetched.credentials().clone(),
        source: Source::Remote(source.name()),
        expires: fetched.expires(),
        refresh: Some(source),
    })
}

/// Go's load-time checks of the chosen profile (`validateCredentialsRequireARN`,
/// `validateCredentialType`), which stop the load whatever source would win.
fn validate(section: &ini::Section, profile: &str) -> Result<(), CredentialError> {
    let has = |name: &str| !section.string(name).is_empty();
    let kinds = [
        "source_profile",
        "credential_source",
        "credential_process",
        "web_identity_token_file",
    ];
    if kinds.iter().filter(|kind| has(kind)).count() > 1 {
        return Err(CredentialError::ProfileInvalid {
            profile: profile.to_owned(),
            why: "only one credential type may be specified per profile: source profile, credential source, credential process, web identity token",
        });
    }
    let needs_arn =
        has("source_profile") || has("credential_source") || has("web_identity_token_file");
    if needs_arn && !has("role_arn") {
        return Err(CredentialError::ProfileInvalid {
            profile: profile.to_owned(),
            why: "this credential type requires role_arn",
        });
    }
    Ok(())
}

/// The two files, combined, and what each gave.
struct Files {
    merged: Sections,
    credentials_path: Option<PathBuf>,
    credentials_look: FileLook,
    config_look: FileLook,
}

/// A profile's static keys when it holds both, with whether they came from the
/// credentials file, and that file's path.
fn profile_keys(
    section: &ini::Section,
    credentials_path: &Option<PathBuf>,
) -> Option<(S3Credentials, bool, PathBuf)> {
    let id = nonempty(Some(section.string(ini::ACCESS_KEY_ID)))?;
    let secret = nonempty(Some(section.string(ini::SECRET_ACCESS_KEY)))?;
    let credentials = S3Credentials::new(id.to_owned(), SecretString::new(secret.to_owned()));
    let credentials = match nonempty(Some(section.string(ini::SESSION_TOKEN))) {
        Some(token) => credentials.with_session_token(SecretString::new(token.to_owned())),
        None => credentials,
    };
    let path = section.keys_from.clone().unwrap_or_default();
    let from_credentials = credentials_path.as_ref() == Some(&path);
    Some((credentials, from_credentials, path))
}

/// The file's text in a buffer zeroed on drop, or why there is none.
fn read_bounded(path: &Path) -> Result<Result<Zeroizing<String>, FileOutcome>, CredentialError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Err(FileOutcome::NotFound)),
        Err(err) => return Ok(Err(FileOutcome::Unreadable(err.kind()))),
    };
    let too_large = || CredentialError::FileTooLarge {
        path: path.to_owned(),
        limit: MAX_SHARED_FILE_BYTES,
    };
    let size = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    if size > MAX_SHARED_FILE_BYTES {
        return Err(too_large());
    }
    // Sized up front so a growing buffer leaves no unzeroed copy behind.
    let capacity = usize::try_from(size.saturating_add(1)).unwrap_or(0);
    let mut bytes = Zeroizing::new(Vec::with_capacity(capacity));
    let read = (&mut file)
        .take(MAX_SHARED_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes);
    match read {
        Ok(_) if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SHARED_FILE_BYTES => {
            Err(too_large())
        }
        Ok(_) => Ok(Ok(Zeroizing::new(
            String::from_utf8_lossy(&bytes).into_owned(),
        ))),
        // Go skips a file it cannot open, but a read that fails after the open
        // (a directory) fails its load.
        Err(err) => Err(CredentialError::ReadFailed {
            path: path.to_owned(),
            kind: err.kind(),
        }),
    }
}

/// One of the two shared files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Credentials,
    Config,
}

impl FileKind {
    fn override_var(self) -> &'static str {
        match self {
            Self::Credentials => SHARED_CREDENTIALS_FILE_VAR,
            Self::Config => CONFIG_FILE_VAR,
        }
    }

    fn file_name(self) -> &'static str {
        match self {
            Self::Credentials => "credentials",
            Self::Config => "config",
        }
    }
}

/// Credentials and the source that gave them.
#[derive(Debug, Clone)]
pub struct Resolved {
    credentials: S3Credentials,
    source: Source,
    expires: Option<lsr_ltx::Timestamp>,
    refresh: Option<Arc<dyn CredentialSource>>,
}

impl Resolved {
    fn fixed(credentials: S3Credentials, source: Source) -> Self {
        Self {
            credentials,
            source,
            expires: None,
            refresh: None,
        }
    }

    /// The credentials with their expiry, for a store to sign with.
    #[must_use]
    pub fn expiring(&self) -> Expiring {
        match self.expires {
            Some(at) => Expiring::until(self.credentials.clone(), at),
            None => Expiring::forever(self.credentials.clone()),
        }
    }

    /// The source to fetch from again before they expire; `None` for keys that never do.
    #[must_use]
    pub fn refresh(&self) -> Option<Arc<dyn CredentialSource>> {
        self.refresh.clone()
    }

    /// The credentials.
    #[must_use]
    pub fn credentials(&self) -> &S3Credentials {
        &self.credentials
    }

    /// The credentials, for a store to take.
    #[must_use]
    pub fn into_credentials(self) -> S3Credentials {
        self.credentials
    }

    /// Where they came from, for a log line.
    #[must_use]
    pub fn source(&self) -> &Source {
        &self.source
    }
}

/// A source that gave credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// The caller's explicit values.
    Explicit,
    /// The environment variables.
    Environment,
    /// A profile in the shared credentials file.
    SharedCredentialsFile {
        /// The file read.
        path: PathBuf,
        /// The profile used.
        profile: String,
    },
    /// A profile in the shared config file.
    SharedConfigFile {
        /// The file read.
        path: PathBuf,
        /// The profile used.
        profile: String,
    },
    /// A source that fetches temporary credentials: `web identity`, `container`,
    /// `instance metadata` or `credential_process`.
    Remote(&'static str),
}

/// Which environment variables were missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvGap {
    /// No key id variable was set.
    KeyId,
    /// No secret variable was set.
    Secret,
    /// Neither was set.
    Both,
}

/// What looking in one shared file found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileLook {
    /// No override variable and no home directory, so no path to try.
    NoHome,
    /// The file at `path`.
    At {
        /// The path tried.
        path: PathBuf,
        /// Why it gave nothing.
        outcome: FileOutcome,
    },
}

/// Why a shared file gave no credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOutcome {
    /// There is no such file.
    NotFound,
    /// The file exists but could not be read; skipped, as Go skips it.
    Unreadable(io::ErrorKind),
    /// No section names the profile.
    NoProfile,
    /// The profile's section has no key id and no secret.
    NoKeys,
}

/// Every source tried and why each gave nothing. Explicit values are always
/// first, and were not given, or the chain would have stopped there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tried {
    environment: EnvGap,
    profile: String,
    credentials_file: FileLook,
    config_file: FileLook,
}

impl Tried {
    /// What the environment lacked.
    #[must_use]
    pub fn environment(&self) -> EnvGap {
        self.environment
    }

    /// The profile looked for in the shared files.
    #[must_use]
    pub fn profile(&self) -> &str {
        &self.profile
    }

    /// What the shared credentials file gave.
    #[must_use]
    pub fn credentials_file(&self) -> &FileLook {
        &self.credentials_file
    }

    /// What the shared config file gave.
    #[must_use]
    pub fn config_file(&self) -> &FileLook {
        &self.config_file
    }
}

impl fmt::Display for Tried {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |names: &[&str]| names.join(", ");
        let unset = match self.environment {
            EnvGap::KeyId => list(&ACCESS_KEY_ID_VARS),
            EnvGap::Secret => list(&SECRET_ACCESS_KEY_VARS),
            EnvGap::Both => format!(
                "{}, {}",
                list(&ACCESS_KEY_ID_VARS),
                list(&SECRET_ACCESS_KEY_VARS)
            ),
        };
        write!(
            f,
            "explicit values (none given); environment ({unset} unset); "
        )?;
        let profile = &self.profile;
        let credentials = ("shared credentials file", SHARED_CREDENTIALS_FILE_VAR);
        write_look(f, credentials, &self.credentials_file, profile)?;
        f.write_str("; ")?;
        let config = ("shared config file", CONFIG_FILE_VAR);
        write_look(f, config, &self.config_file, profile)
    }
}

fn write_look(
    f: &mut fmt::Formatter<'_>,
    (name, var): (&str, &str),
    look: &FileLook,
    profile: &str,
) -> fmt::Result {
    match look {
        FileLook::NoHome => write!(f, "{name} (neither {var} nor {HOME_VAR} set)"),
        FileLook::At { path, outcome } => {
            let why = match outcome {
                FileOutcome::NotFound => "not found".to_owned(),
                FileOutcome::Unreadable(kind) => format!("unreadable: {kind}"),
                FileOutcome::NoProfile => format!("no profile {profile:?}"),
                FileOutcome::NoKeys => format!("profile {profile:?} has no static keys"),
            };
            write!(f, "{name} {} ({why})", path.display())
        }
    }
}

/// Why the chain gave no credentials. No variant holds a value read from a
/// source.
#[derive(Debug, Error)]
pub enum CredentialError {
    /// No source had both a key id and a secret.
    #[error("no S3 credentials found; looked at {0}")]
    Missing(Tried),
    /// `AWS_PROFILE` names a profile neither shared file has.
    #[error("profile {:?} named by {PROFILE_VAR} not found; looked at {}", .0.profile, .0)]
    ProfileNotFound(Tried),
    /// A profile has a key id without a secret or a secret without a key id.
    #[error(
        "profile {profile:?} in {} has only one of aws_access_key_id and aws_secret_access_key",
        path.display()
    )]
    PartialProfile {
        /// The file.
        path: PathBuf,
        /// The profile.
        profile: String,
    },
    /// A line of a shared file is not INI. The line's text is not kept.
    #[error("{}: line {line} is not a [section], a key = value pair or a comment", path.display())]
    MalformedLine {
        /// The file.
        path: PathBuf,
        /// The line, counting from 1.
        line: usize,
    },
    /// A source the chain does not build yet (#176): an assumed role, IAM Identity Center
    /// or a login session. Go's SDK resolves these.
    #[error("{what} is not built in litestream-rs yet; Go resolves it (issue #176)")]
    NotBuilt {
        /// Which source.
        what: &'static str,
    },
    /// The chosen profile fails one of the SDK's load-time checks.
    #[error("profile {profile:?}: {why}")]
    ProfileInvalid {
        /// The profile.
        profile: String,
        /// Which check.
        why: &'static str,
    },
    /// A source that fetches credentials gave none.
    #[error(transparent)]
    Source(SourceError),
    /// A shared file opened but could not be read (it is a directory, say). Go
    /// skips a file it cannot open but fails on one it cannot read.
    #[error("{} could not be read: {kind}", path.display())]
    ReadFailed {
        /// The file.
        path: PathBuf,
        /// What the file system said.
        kind: io::ErrorKind,
    },
    /// A shared file is larger than `MAX_SHARED_FILE_BYTES`.
    #[error("{} is larger than {limit} bytes", path.display())]
    FileTooLarge {
        /// The file.
        path: PathBuf,
        /// The limit.
        limit: u64,
    },
}

/// Why a source that fetches credentials gave none. No variant holds a credential.
#[derive(Debug, Error)]
pub enum SourceError {
    /// The source did not answer.
    #[error("{provider} could not be reached: {why}")]
    Unreachable {
        /// Which source.
        provider: &'static str,
        /// What the transport said.
        why: String,
    },
    /// The source answered with an error status.
    #[error("{provider} answered {status} {code}")]
    Status {
        /// Which source.
        provider: &'static str,
        /// The HTTP status.
        status: u16,
        /// The error code it gave, if any.
        code: String,
    },
    /// The answer holds no credentials.
    #[error("{provider} answered something that is not credentials")]
    Malformed {
        /// Which source.
        provider: &'static str,
    },
    /// The source refused, with its code.
    #[error("{provider} refused: {code}")]
    Refused {
        /// Which source.
        provider: &'static str,
        /// Its code.
        code: String,
    },
    /// The source cannot be asked as configured.
    #[error("{provider}: {why}")]
    Config {
        /// Which source.
        provider: &'static str,
        /// Why.
        why: String,
    },
}

impl SourceError {
    /// Whether waiting may fix it: no answer, or a server-side status.
    #[must_use]
    pub fn transient(&self) -> bool {
        match self {
            Self::Unreachable { .. } => true,
            Self::Status { status, .. } => *status >= 500 || *status == 429,
            Self::Malformed { .. } | Self::Refused { .. } | Self::Config { .. } => false,
        }
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

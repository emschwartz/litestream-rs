//! Where S3 credentials come from when the caller does not hand them over:
//! the parts of the AWS default credential chain that need no network and no
//! subprocess, in the order Go litestream (with the AWS SDK for Go v2) tries
//! them.
//!
//! 1. Explicit values (a library caller's [`CredentialChain::with_explicit`]).
//! 2. The environment: the key id from `AWS_ACCESS_KEY_ID`, else
//!    `LITESTREAM_ACCESS_KEY_ID`, else `AWS_ACCESS_KEY`; the secret from
//!    `AWS_SECRET_ACCESS_KEY`, else `LITESTREAM_SECRET_ACCESS_KEY`, else
//!    `AWS_SECRET_KEY`; with both, `AWS_SESSION_TOKEN` if set. An empty
//!    variable counts as unset. With only one of the two, Go moves on to the
//!    files, and so does this.
//! 3. The shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, else
//!    `~/.aws/credentials`), then the shared config file (`AWS_CONFIG_FILE`,
//!    else `~/.aws/config`), for the profile `AWS_PROFILE`, else
//!    `AWS_DEFAULT_PROFILE`, else `default`. Static keys only. A session token
//!    is taken from the same file as the keys. A file that is missing or
//!    cannot be opened is skipped, as in Go.
//!
//! The first source with both a key id and a secret wins. When none has them,
//! [`CredentialError::Missing`] names every source and why it gave nothing, so
//! an operator knows where the library looked; no error or `Debug` output holds
//! a value. As in Go, a profile with one of the key id and secret but not the
//! other is an error rather than a fall-through, and so is a profile that
//! `AWS_PROFILE` names but neither file has (Go fails on that at load; here
//! only once the files are consulted, so explicit or environment credentials
//! still win). A line in a shared file that is not INI is refused, where Go
//! skips it (see the `ini` module).
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
#[cfg(test)]
mod tests;

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use thiserror::Error;
use zeroize::Zeroizing;

use crate::credentials::{S3Credentials, SecretString};
use ini::Flavor;

/// The variables tried for the access key id, in order.
pub const ACCESS_KEY_ID_VARS: [&str; 3] = [
    "AWS_ACCESS_KEY_ID",
    "LITESTREAM_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY",
];
/// The variables tried for the secret access key, in order.
pub const SECRET_ACCESS_KEY_VARS: [&str; 3] = [
    "AWS_SECRET_ACCESS_KEY",
    "LITESTREAM_SECRET_ACCESS_KEY",
    "AWS_SECRET_KEY",
];
/// The variable holding a session token, read with environment keys only.
pub const SESSION_TOKEN_VAR: &str = "AWS_SESSION_TOKEN";
/// The variable naming the profile; a profile it names must exist.
pub const PROFILE_VAR: &str = "AWS_PROFILE";
/// The profile variable read when [`PROFILE_VAR`] is unset; not checked.
pub const DEFAULT_PROFILE_VAR: &str = "AWS_DEFAULT_PROFILE";
/// The variable overriding the shared credentials file's path.
pub const SHARED_CREDENTIALS_FILE_VAR: &str = "AWS_SHARED_CREDENTIALS_FILE";
/// The variable overriding the shared config file's path.
pub const CONFIG_FILE_VAR: &str = "AWS_CONFIG_FILE";
/// The variable holding the home directory the default paths sit under.
#[cfg(windows)]
pub const HOME_VAR: &str = "USERPROFILE";
/// The variable holding the home directory the default paths sit under.
#[cfg(not(windows))]
pub const HOME_VAR: &str = "HOME";
/// The profile read when no variable names one.
pub const DEFAULT_PROFILE: &str = "default";
/// The largest shared file read; real ones are a few hundred bytes.
pub const MAX_SHARED_FILE_BYTES: u64 = 1024 * 1024;

type Env = dyn Fn(&str) -> Option<String> + Send + Sync;

/// The sources credentials are looked for in, and the environment they read.
pub struct CredentialChain {
    explicit: Option<S3Credentials>,
    env: Box<Env>,
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
        }
    }

    /// The chain over an environment the caller supplies, for tests.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn with_env(env: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        Self {
            explicit: None,
            env: Box::new(env),
        }
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
        if let Some(credentials) = &self.explicit {
            return Ok(Resolved {
                credentials: credentials.clone(),
                source: Source::Explicit,
            });
        }
        let environment = match self.env_keys() {
            Ok(credentials) => {
                return Ok(Resolved {
                    credentials,
                    source: Source::Environment,
                });
            }
            Err(gap) => gap,
        };
        let (profile, named) = self.profile();
        let credentials_file = match self.file_keys(FileKind::Credentials, &profile)? {
            Ok(resolved) => return Ok(resolved),
            Err(look) => look,
        };
        let config_file = match self.file_keys(FileKind::Config, &profile)? {
            Ok(resolved) => return Ok(resolved),
            Err(look) => look,
        };
        let tried = Tried {
            environment,
            profile,
            credentials_file,
            config_file,
        };
        if named && !tried.profile_seen() {
            return Err(CredentialError::ProfileNotFound(tried));
        }
        Err(CredentialError::Missing(tried))
    }

    fn var(&self, name: &str) -> Option<String> {
        (self.env)(name).filter(|value| !value.is_empty())
    }

    fn first(&self, names: &[&str]) -> Option<String> {
        names.iter().find_map(|name| self.var(name))
    }

    fn env_keys(&self) -> Result<S3Credentials, EnvGap> {
        let id = self.first(&ACCESS_KEY_ID_VARS);
        let secret = self.first(&SECRET_ACCESS_KEY_VARS);
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

    /// The outer error stops the chain; the inner one is a source that gave
    /// nothing.
    fn file_keys(
        &self,
        kind: FileKind,
        profile: &str,
    ) -> Result<Result<Resolved, FileLook>, CredentialError> {
        let path = match self.var(kind.override_var()) {
            Some(path) => PathBuf::from(path),
            None => match self.var(HOME_VAR) {
                Some(home) => Path::new(&home).join(".aws").join(kind.file_name()),
                None => return Ok(Err(FileLook::NoHome)),
            },
        };
        let text = match read_bounded(&path)? {
            Ok(text) => text,
            Err(outcome) => return Ok(Err(FileLook::At { path, outcome })),
        };
        let keys = match ini::lookup(&text, kind.flavor(), profile) {
            Ok(Some(keys)) => keys,
            Ok(None) => {
                return Ok(Err(FileLook::At {
                    path,
                    outcome: FileOutcome::NoProfile,
                }));
            }
            Err(bad) => {
                return Err(CredentialError::MalformedLine {
                    path,
                    line: bad.line,
                });
            }
        };
        let (id, secret) = match (nonempty(keys.id), nonempty(keys.secret)) {
            (Some(id), Some(secret)) => (id, secret),
            (None, None) => {
                return Ok(Err(FileLook::At {
                    path,
                    outcome: FileOutcome::NoKeys,
                }));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(CredentialError::PartialProfile {
                    path,
                    profile: profile.to_owned(),
                });
            }
        };
        let credentials = S3Credentials::new(id.to_owned(), SecretString::new(secret.to_owned()));
        let credentials = match nonempty(keys.token) {
            Some(token) => credentials.with_session_token(SecretString::new(token.to_owned())),
            None => credentials,
        };
        Ok(Ok(Resolved {
            credentials,
            source: kind.source(path, profile.to_owned()),
        }))
    }
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
        Err(err) => Ok(Err(FileOutcome::Unreadable(err.kind()))),
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

    fn flavor(self) -> Flavor {
        match self {
            Self::Credentials => Flavor::Credentials,
            Self::Config => Flavor::Config,
        }
    }

    fn source(self, path: PathBuf, profile: String) -> Source {
        match self {
            Self::Credentials => Source::SharedCredentialsFile { path, profile },
            Self::Config => Source::SharedConfigFile { path, profile },
        }
    }
}

/// Credentials and the source that gave them.
#[derive(Debug, Clone)]
pub struct Resolved {
    credentials: S3Credentials,
    source: Source,
}

impl Resolved {
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

    fn profile_seen(&self) -> bool {
        [&self.credentials_file, &self.config_file]
            .iter()
            .any(|look| {
                matches!(
                    look,
                    FileLook::At {
                        outcome: FileOutcome::NoKeys,
                        ..
                    }
                )
            })
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
    /// A shared file is larger than [`MAX_SHARED_FILE_BYTES`].
    #[error("{} is larger than {limit} bytes", path.display())]
    FileTooLarge {
        /// The file.
        path: PathBuf,
        /// The limit.
        limit: u64,
    },
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

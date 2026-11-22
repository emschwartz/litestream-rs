//! The one rule for a test that needs something outside the process: fail closed.
//!
//! A test that cannot get its MinIO, its Go builds, its C compiler or its disk space does not
//! pass quietly. [`need`] returns the resource or panics with what is missing and how to get it.
//! The only way to skip is for whoever runs the tests to say so: `LSR_SKIP=minio` (a comma
//! separated list of [`Resource::name`]s). The default is never to skip, and under CI (the `CI`
//! variable GitHub sets) a non-empty `LSR_SKIP` is itself a failure, so a green CI run means the
//! tests ran against the real thing.
//!
//! [`must`] is the same check for a resource whose opt-out is the test's `#[ignore]` (Tigris):
//! whoever runs those asked for them by name, so there is nothing to skip.

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// The variable naming what the runner opts out of; unset or empty, nothing is skipped.
pub const SKIP_VAR: &str = "LSR_SKIP";

/// An external thing a test needs. Closed, so a typo in `LSR_SKIP` is an error, not a no-op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resource {
    /// A MinIO server (`just minio-up`).
    Minio,
    /// A Toxiproxy server in front of MinIO (`scripts/toxiproxy.sh`).
    Toxiproxy,
    /// The pinned Go builds (`scripts/build-go-litestream.sh`).
    GoBuilds,
    /// The Tigris bucket and its credentials (`scripts/tigris.sh`).
    Tigris,
    /// A C compiler and the SQLite headers.
    CCompiler,
    /// Free disk space for the large cases.
    Disk,
}

impl Resource {
    /// Every resource, for the parser of [`SKIP_VAR`].
    pub const ALL: [Self; 6] = [
        Self::Minio,
        Self::Toxiproxy,
        Self::GoBuilds,
        Self::Tigris,
        Self::CCompiler,
        Self::Disk,
    ];

    /// The word that opts out of this resource in [`SKIP_VAR`].
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Minio => "minio",
            Self::Toxiproxy => "toxiproxy",
            Self::GoBuilds => "go",
            Self::Tigris => "tigris",
            Self::CCompiler => "cc",
            Self::Disk => "disk",
        }
    }

    /// How to get it, for the failure message.
    const fn how(self) -> &'static str {
        match self {
            Self::Minio => "start MinIO and run through `just test-s3` or `just compat-s3`",
            Self::Toxiproxy => "run through `just test-s3`, which starts Toxiproxy before MinIO",
            Self::GoBuilds => "build the Go binaries with `just compat`",
            Self::Tigris => {
                "run through `just test-tigris` or `just tigris-restore` with credentials"
            }
            Self::CCompiler => "install a C compiler and the SQLite headers (see `just compat`)",
            Self::Disk => "free the disk space the case needs",
        }
    }
}

/// Whether the runner opted out of `resource`, from the raw values of [`SKIP_VAR`] and `CI`.
///
/// # Errors
/// A word in `skip` that is no [`Resource::name`], or any opt-out while `ci` is set.
fn opted_out(skip: Option<&str>, ci: bool, resource: Resource) -> Result<bool, String> {
    let words: Vec<&str> = skip
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|w| !w.is_empty())
        .collect();
    if words.is_empty() {
        return Ok(false);
    }
    if ci {
        return Err(format!("{SKIP_VAR} is set under CI; CI never skips"));
    }
    if let Some(unknown) = words
        .iter()
        .find(|w| !Resource::ALL.iter().any(|r| r.name() == **w))
    {
        let known: Vec<&str> = Resource::ALL.iter().map(|r| r.name()).collect();
        return Err(format!(
            "{SKIP_VAR} names {unknown:?}, which is not one of {known:?}"
        ));
    }
    Ok(words.contains(&resource.name()))
}

fn env_text(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// The resource `probe` finds, or a panic saying what is missing; `None` only when the runner
/// opted out with `LSR_SKIP`, in which case the caller returns. `probe` does not run then.
///
/// ```ignore
/// let Some(minio) = need(Resource::Minio, probe_minio) else { return };
/// ```
#[expect(
    clippy::panic,
    reason = "a missing resource must fail the test itself; returning would pass it"
)]
pub fn need<T>(resource: Resource, probe: impl FnOnce() -> Result<T, String>) -> Option<T> {
    let skip = env_text(SKIP_VAR);
    match opted_out(skip.as_deref(), env_text("CI").is_some(), resource) {
        Err(why) => panic!("{why}"),
        Ok(true) => {
            eprintln!("SKIPPED by {SKIP_VAR}={0}: no {0}", resource.name());
            None
        }
        Ok(false) => match probe() {
            Ok(found) => Some(found),
            Err(why) => missing(resource, &why, true),
        },
    }
}

/// The resource `probe` finds, or a panic; there is no opt-out, because the test is `#[ignore]`d
/// and whoever runs it asked for it.
pub fn must<T>(resource: Resource, probe: impl FnOnce() -> Result<T, String>) -> T {
    match probe() {
        Ok(found) => found,
        Err(why) => missing(resource, &why, false),
    }
}

#[expect(
    clippy::panic,
    reason = "a missing resource must fail the test itself; returning would pass it"
)]
fn missing(resource: Resource, why: &str, can_skip: bool) -> ! {
    let opt_out = if can_skip {
        format!(
            " To opt out on purpose (never under CI): {SKIP_VAR}={}",
            resource.name()
        )
    } else {
        String::new()
    };
    panic!(
        "this test needs {}: {why}. To fix: {}.{opt_out}",
        resource.name(),
        resource.how()
    )
}

/// A MinIO server and its throwaway credentials (`scripts/minio.sh`).
#[derive(Clone, Debug)]
pub struct Minio {
    /// `http://127.0.0.1:9000`.
    pub endpoint: String,
    /// The root user.
    pub access: String,
    /// The root password.
    pub secret: String,
}

/// The MinIO named by `LSR_MINIO_ENDPOINT` (credentials from `LSR_MINIO_ACCESS_KEY` and
/// `LSR_MINIO_SECRET_KEY`, defaulting to the ones in `scripts/minio.sh`), once it accepts a
/// connection; see [`need`].
pub fn minio() -> Option<Minio> {
    need(Resource::Minio, || {
        let endpoint = env_text("LSR_MINIO_ENDPOINT").ok_or("LSR_MINIO_ENDPOINT is unset")?;
        reachable(&endpoint)?;
        Ok(Minio {
            endpoint,
            access: env_text("LSR_MINIO_ACCESS_KEY").unwrap_or_else(|| "lsrtestroot".into()),
            secret: env_text("LSR_MINIO_SECRET_KEY")
                .unwrap_or_else(|| "lsrtest-throwaway-0000".into()),
        })
    })
}

/// A Toxiproxy server's HTTP API (`scripts/toxiproxy.sh`).
#[derive(Clone, Debug)]
pub struct Toxiproxy {
    /// `http://127.0.0.1:8474`.
    pub api: String,
}

/// The Toxiproxy whose API `LSR_TOXIPROXY_API` names, once it accepts a connection; see
/// [`need`].
pub fn toxiproxy() -> Option<Toxiproxy> {
    need(Resource::Toxiproxy, || {
        let api = env_text("LSR_TOXIPROXY_API").ok_or("LSR_TOXIPROXY_API is unset")?;
        reachable(&api)?;
        Ok(Toxiproxy { api })
    })
}

/// Whether `endpoint` (`http://host:port`) accepts a TCP connection within five seconds, so a
/// test that expects an error cannot pass on a server that is simply absent.
///
/// # Errors
/// What failed: the address did not resolve, or the connection was refused or timed out.
pub fn reachable(endpoint: &str) -> Result<(), String> {
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default();
    let addr = authority
        .to_socket_addrs()
        .map_err(|e| format!("{endpoint} does not resolve: {e}"))?
        .next()
        .ok_or_else(|| format!("{endpoint} resolves to no address"))?;
    TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map(drop)
        .map_err(|e| format!("{endpoint} is unreachable: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_skipped_by_default() {
        for r in Resource::ALL {
            assert_eq!(opted_out(None, false, r), Ok(false));
            assert_eq!(opted_out(Some(""), false, r), Ok(false));
            assert_eq!(opted_out(Some(" , "), true, r), Ok(false));
        }
    }

    #[test]
    fn only_the_named_resources_are_skipped() {
        assert_eq!(opted_out(Some("minio"), false, Resource::Minio), Ok(true));
        assert_eq!(opted_out(Some("minio"), false, Resource::Tigris), Ok(false));
        assert_eq!(
            opted_out(Some("go, minio"), false, Resource::Minio),
            Ok(true)
        );
        assert_eq!(
            opted_out(Some("go, minio"), false, Resource::GoBuilds),
            Ok(true)
        );
    }

    #[test]
    fn a_typo_or_ci_is_an_error_not_a_skip() {
        assert!(
            opted_out(Some("minoi"), false, Resource::Minio)
                .unwrap_err()
                .contains("minoi")
        );
        assert!(
            opted_out(Some("minio"), true, Resource::Minio)
                .unwrap_err()
                .contains("CI")
        );
    }

    #[test]
    #[should_panic(expected = "this test needs cc")]
    fn a_missing_resource_panics() {
        must(Resource::CCompiler, || Err::<(), _>("no cc".to_owned()));
    }

    /// What a probe must do in this environment: `None` only when the runner opted out, a
    /// panic when the resource is required and its variable is unset, and never `None` when
    /// the variable names it (found, or a panic that it is unreachable). A probe that returned
    /// `None` on its own would let every test that needs the resource skip and pass.
    fn fails_closed<T>(resource: Resource, var: &str, probe: fn() -> Option<T>) {
        let opted = opted_out(
            env_text(SKIP_VAR).as_deref(),
            env_text("CI").is_some(),
            resource,
        );
        let named = env_text(var).is_some();
        let got = std::panic::catch_unwind(probe);
        match (opted, named) {
            (Ok(true), _) => assert!(matches!(got, Ok(None)), "an opt-out returns None"),
            (Err(_), _) | (Ok(false), false) => assert!(
                got.is_err(),
                "a required {} that is missing must fail the test, not skip it",
                resource.name()
            ),
            (Ok(false), true) => assert!(
                !matches!(got, Ok(None)),
                "a named {} is found or fails, never skipped",
                resource.name()
            ),
        }
    }

    #[test]
    fn the_toxiproxy_probe_fails_closed() {
        fails_closed(Resource::Toxiproxy, "LSR_TOXIPROXY_API", toxiproxy);
    }

    #[test]
    fn the_minio_probe_fails_closed() {
        fails_closed(Resource::Minio, "LSR_MINIO_ENDPOINT", minio);
    }

    #[test]
    fn a_refused_connection_is_unreachable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(reachable(&format!("http://{addr}")), Ok(()));
        drop(listener);
        assert!(
            reachable(&format!("http://{addr}"))
                .unwrap_err()
                .contains("unreachable")
        );
    }
}

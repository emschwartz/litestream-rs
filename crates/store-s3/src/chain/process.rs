//! `credential_process`, as the AWS SDK for Go v2 runs it (`credentials/processcreds`
//! v1.19.6): `sh -c <command>` with the process's environment, standard output read as JSON
//! with `Version` 1, `AccessKeyId` and `SecretAccessKey` required and `SessionToken` and
//! `Expiration` optional, keys matched as Go's `encoding/json` matches them. The SDK allows
//! one minute; so does this, after which the child is killed.

use std::io::Read as _;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use super::remote::document_credentials;
use super::{CredentialError, SourceError};
use crate::source::{CredentialSource, Expiring};

/// The SDK's `processcreds.DefaultTimeout`.
const TIMEOUT: Duration = Duration::from_secs(60);
/// The most of the command's output read; real ones are a few hundred bytes.
const MAX_OUTPUT: u64 = 64 * 1024;

const PROCESS: &str = "credential_process";

/// A `credential_process` command line.
#[derive(Debug)]
pub(crate) struct Process {
    pub(crate) command: String,
}

fn failed(why: String) -> CredentialError {
    CredentialError::Source(SourceError::Config {
        provider: PROCESS,
        why,
    })
}

impl Process {
    /// Runs the command and returns its standard output.
    fn output(&self) -> Result<Vec<u8>, CredentialError> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|err| failed(format!("error starting the process: {}", err.kind())))?;
        let Some(stdout) = child.stdout.take() else {
            return Err(failed("no standard output".to_owned()));
        };
        let (send, receive) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let mut out = Vec::new();
            let read = stdout.take(MAX_OUTPUT).read_to_end(&mut out).map(|_| out);
            // The receiver may have given up on a slow process; nothing to report then.
            drop(send.send(read));
        });
        match receive.recv_timeout(TIMEOUT) {
            Ok(read) => {
                let status = child.wait().map_err(|err| {
                    failed(format!("error waiting for the process: {}", err.kind()))
                })?;
                let out =
                    read.map_err(|err| failed(format!("error reading output: {}", err.kind())))?;
                if status.success() {
                    Ok(out)
                } else {
                    Err(failed(format!("the process exited with {status}")))
                }
            }
            Err(_) => {
                // Best effort: the child may have exited between the timeout and the kill.
                drop(child.kill());
                drop(child.wait());
                Err(failed(
                    "the process did not finish within a minute".to_owned(),
                ))
            }
        }
    }
}

impl CredentialSource for Process {
    fn fetch(&self) -> Result<Expiring, CredentialError> {
        let out = self.output()?;
        let value: serde_json::Value = serde_json::from_slice(&out)
            .map_err(|_unparsed| failed("parse failed of process output".to_owned()))?;
        let version = value
            .as_object()
            .and_then(|o| {
                o.get("Version").or_else(|| {
                    o.iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case("Version"))
                        .map(|(_, v)| v)
                })
            })
            .and_then(serde_json::Value::as_i64);
        if version != Some(1) {
            return Err(failed("wrong version in process output (not 1)".to_owned()));
        }
        document_credentials(&value, "SessionToken", None).ok_or_else(|| {
            failed("missing AccessKeyId or SecretAccessKey in process output".to_owned())
        })
    }

    fn name(&self) -> &'static str {
        PROCESS
    }
}

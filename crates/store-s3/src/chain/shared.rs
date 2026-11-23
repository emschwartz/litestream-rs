//! The shared config and credentials files combined as the AWS SDK for Go v2
//! (config v1.32.6) combines them before it looks at any credential source
//! (`LoadSharedConfigProfile` and `mergeSections` in `config/shared_config.go`):
//!
//! - in the config file, `[profile x]` becomes `x` and replaces any bare `[x]`; a
//!   bare section other than `default` (in any case), `sso-session ...` and
//!   `services ...` is dropped;
//! - in the credentials file, a `[profile x]` section is dropped;
//! - the credentials file's sections are laid over the config file's: a new
//!   section is taken whole; a shared one takes the credentials file's section
//!   error (which may clear the config file's), its key id, secret and session
//!   token only when the credentials file holds both the id and the secret, and
//!   each other key the chain reads (`role_arn`, `credential_process`, ...) that
//!   it holds.
//!
//! The SDK does this at load, so its failures stop the load whatever source would
//! have given credentials: a profile named by a non-empty `AWS_PROFILE` that no
//! file has, and a chosen profile whose section error stands.

use std::path::Path;

use super::ini::{
    self, ACCESS_KEY_ID, Malformed, SECRET_ACCESS_KEY, SESSION_TOKEN, Section, Sections,
};

/// What the combined files hold for one profile.
pub(super) enum Profile {
    /// No file has it.
    Absent,
    /// The chosen section holds only one of the key id and secret, in this file.
    Partial(std::path::PathBuf),
    /// The section, which may or may not hold both keys.
    Present(Section),
}

/// The config file's sections, renamed and filtered as the SDK does.
pub(super) fn config_file(text: &str, path: &Path) -> Result<Sections, Malformed> {
    let parsed = ini::parse(text, path)?;
    let mut out = Sections::new();
    let mut renamed = Vec::new();
    for (name, section) in parsed {
        if let Some(bare) = name.strip_prefix("profile ") {
            renamed.push((bare.to_owned(), section));
        } else if name.eq_ignore_ascii_case("default")
            || name.starts_with("sso-session ")
            || name.starts_with("services ")
        {
            out.insert(name, section);
        }
    }
    for (name, section) in renamed {
        out.insert(name, section);
    }
    Ok(out)
}

/// The credentials file's sections, `[profile x]` dropped.
pub(super) fn credentials_file(text: &str, path: &Path) -> Result<Sections, Malformed> {
    let mut parsed = ini::parse(text, path)?;
    parsed.retain(|name, _| !name.starts_with("profile "));
    Ok(parsed)
}

/// The SDK's `mergeSections(config, credentials)`.
pub(super) fn merge(mut config: Sections, credentials: Sections) -> Sections {
    for (name, src) in credentials {
        let Some(dst) = config.get_mut(&name) else {
            config.insert(name, src);
            continue;
        };
        dst.partial = src.partial.clone();
        if src.has(ACCESS_KEY_ID) && src.has(SECRET_ACCESS_KEY) {
            dst.set(ACCESS_KEY_ID, src.string(ACCESS_KEY_ID));
            dst.set(SECRET_ACCESS_KEY, src.string(SECRET_ACCESS_KEY));
            if src.has(SESSION_TOKEN) {
                dst.set(SESSION_TOKEN, src.string(SESSION_TOKEN));
            }
            dst.keys_from = src.keys_from.clone();
        }
        for key in MERGED_KEYS {
            if src.has(key) {
                dst.set(key, src.string(key));
            }
        }
    }
    config
}

/// The keys `mergeSections` copies one by one from the credentials file over the config
/// file's (`stringKeys` in `config/shared_config.go`), those the chain reads.
const MERGED_KEYS: [&str; 13] = [
    "role_arn",
    "source_profile",
    "credential_source",
    "external_id",
    "role_session_name",
    "region",
    "credential_process",
    "web_identity_token_file",
    "sso_session",
    "sso_account_id",
    "sso_region",
    "sso_role_name",
    "sso_start_url",
];

/// The chosen profile in the merged sections.
pub(super) fn choose(mut merged: Sections, profile: &str) -> Profile {
    match merged.remove(profile) {
        None => Profile::Absent,
        Some(section) => match section.partial.clone() {
            Some(path) => Profile::Partial(path),
            None => Profile::Present(section),
        },
    }
}

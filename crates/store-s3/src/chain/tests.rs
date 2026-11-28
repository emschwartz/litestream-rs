use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use super::*;

const CANARY: &str = "CANARY-chain-5e1d";

/// A chain over exactly these variables; nothing from the process.
fn chain(vars: &[(&str, &str)]) -> CredentialChain {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    CredentialChain::with_env(move |name| vars.get(name).cloned())
}

fn write(dir: &TempDir, name: &str, text: &str) -> PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, text).unwrap();
    path
}

fn s(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// The key id, secret and token the signer would use.
fn parts(resolved: &Resolved) -> (String, String, Option<String>) {
    let signer = resolved.credentials().to_signer();
    (
        signer.key().to_owned(),
        signer.secret().to_owned(),
        signer.token().map(str::to_owned),
    )
}

fn keys(id: &str, secret: &str) -> (String, String, Option<String>) {
    (id.to_owned(), secret.to_owned(), None)
}

fn missing(err: CredentialError) -> Tried {
    match err {
        CredentialError::Missing(tried) => tried,
        other @ (CredentialError::ProfileNotFound(_)
        | CredentialError::PartialProfile { .. }
        | CredentialError::MalformedLine { .. }
        | CredentialError::ReadFailed { .. }
        | CredentialError::FileTooLarge { .. }
        | CredentialError::NotBuilt { .. }
        | CredentialError::ProfileInvalid { .. }
        | CredentialError::Source(_)) => panic!("expected Missing, got {other:?}"),
    }
}

const CREDS_FILE: &str = "[default]\naws_access_key_id = FILEID\naws_secret_access_key = filesecret\naws_session_token = filetoken\n";
const CONFIG_FILE: &str = "[default]\nregion = eu-west-1\naws_access_key_id = CONFID\naws_secret_access_key = confsecret\n";

#[test]
fn explicit_values_alone() {
    let explicit = S3Credentials::new("EXPID".to_owned(), SecretString::new("exps".to_owned()));
    let resolved = chain(&[]).with_explicit(explicit).resolve().unwrap();
    assert_eq!(resolved.source(), &Source::Explicit);
    assert_eq!(parts(&resolved), keys("EXPID", "exps"));
}

#[test]
fn environment_alone() {
    let resolved = chain(&[
        ("AWS_ACCESS_KEY_ID", "ENVID"),
        ("AWS_SECRET_ACCESS_KEY", "envs"),
        ("AWS_SESSION_TOKEN", "envt"),
    ])
    .resolve()
    .unwrap();
    assert_eq!(resolved.source(), &Source::Environment);
    assert_eq!(
        parts(&resolved),
        (
            "ENVID".to_owned(),
            "envs".to_owned(),
            Some("envt".to_owned())
        )
    );
}

#[test]
fn litestream_variables_alone() {
    let resolved = chain(&[
        ("LITESTREAM_ACCESS_KEY_ID", "LSID"),
        ("LITESTREAM_SECRET_ACCESS_KEY", "lss"),
    ])
    .resolve()
    .unwrap();
    assert_eq!(resolved.source(), &Source::Environment);
    assert_eq!(parts(&resolved), keys("LSID", "lss"));
}

#[test]
fn the_sdks_short_variable_names_alone() {
    let resolved = chain(&[("AWS_ACCESS_KEY", "SHORTID"), ("AWS_SECRET_KEY", "shorts")])
        .resolve()
        .unwrap();
    assert_eq!(parts(&resolved), keys("SHORTID", "shorts"));
}

#[test]
fn aws_variables_win_field_by_field_over_litestream_ones() {
    let resolved = chain(&[
        ("AWS_ACCESS_KEY_ID", "AWSID"),
        ("LITESTREAM_ACCESS_KEY_ID", "LSID"),
        ("AWS_ACCESS_KEY", "SHORTID"),
        ("LITESTREAM_SECRET_ACCESS_KEY", "lss"),
        ("AWS_SECRET_KEY", "shorts"),
    ])
    .resolve()
    .unwrap();
    assert_eq!(parts(&resolved), keys("AWSID", "lss"));
}

#[test]
fn shared_credentials_file_alone() {
    let dir = TempDir::new().unwrap();
    let path = write(&dir, "creds", CREDS_FILE);
    let resolved = chain(&[("AWS_SHARED_CREDENTIALS_FILE", s(&path))])
        .resolve()
        .unwrap();
    assert_eq!(
        resolved.source(),
        &Source::SharedCredentialsFile {
            path,
            profile: "default".to_owned()
        }
    );
    assert_eq!(
        parts(&resolved),
        (
            "FILEID".to_owned(),
            "filesecret".to_owned(),
            Some("filetoken".to_owned())
        )
    );
}

#[test]
fn shared_config_file_alone() {
    let dir = TempDir::new().unwrap();
    let path = write(&dir, "config", CONFIG_FILE);
    let resolved = chain(&[("AWS_CONFIG_FILE", s(&path))]).resolve().unwrap();
    assert_eq!(
        resolved.source(),
        &Source::SharedConfigFile {
            path,
            profile: "default".to_owned()
        }
    );
    assert_eq!(parts(&resolved), keys("CONFID", "confsecret"));
}

#[test]
fn default_paths_sit_under_the_home_variable() {
    let home = TempDir::new().unwrap();
    fs::create_dir(home.path().join(".aws")).unwrap();
    fs::write(home.path().join(".aws/config"), CONFIG_FILE).unwrap();
    let resolved = chain(&[(HOME_VAR, s(home.path()))]).resolve().unwrap();
    assert_eq!(
        resolved.source(),
        &Source::SharedConfigFile {
            path: home.path().join(".aws").join("config"),
            profile: "default".to_owned()
        }
    );
    let dir = TempDir::new().unwrap();
    let other = write(&dir, "config", &CONFIG_FILE.replace("CONFID", "OVERID"));
    let resolved = chain(&[(HOME_VAR, s(home.path())), ("AWS_CONFIG_FILE", s(&other))])
        .resolve()
        .unwrap();
    assert_eq!(parts(&resolved).0, "OVERID");
}

#[test]
fn precedence_is_explicit_environment_credentials_file_config_file() {
    let dir = TempDir::new().unwrap();
    let creds = write(&dir, "creds", CREDS_FILE);
    let config = write(&dir, "config", CONFIG_FILE);
    let env = [
        ("AWS_ACCESS_KEY_ID", "ENVID"),
        ("AWS_SECRET_ACCESS_KEY", "envs"),
        ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
        ("AWS_CONFIG_FILE", s(&config)),
    ];
    let explicit = S3Credentials::new("EXPID".to_owned(), SecretString::new("exps".to_owned()));
    let id = |chain: CredentialChain| parts(&chain.resolve().unwrap()).0;
    assert_eq!(id(chain(&env).with_explicit(explicit)), "EXPID");
    assert_eq!(id(chain(&env)), "ENVID");
    assert_eq!(id(chain(&env[2..])), "FILEID");
    assert_eq!(id(chain(&env[3..])), "CONFID");
}

#[test]
fn half_an_environment_falls_through_to_the_files() {
    let dir = TempDir::new().unwrap();
    let creds = write(&dir, "creds", CREDS_FILE);
    let resolved = chain(&[
        ("AWS_ACCESS_KEY_ID", "ENVID"),
        ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
    ])
    .resolve()
    .unwrap();
    // The file's keys whole, never the environment's id with the file's secret.
    assert_eq!(parts(&resolved).0, "FILEID");
    assert!(matches!(
        resolved.source(),
        Source::SharedCredentialsFile { .. }
    ));
}

#[test]
fn a_named_profile_is_read_from_its_own_sections() {
    let dir = TempDir::new().unwrap();
    let creds = write(
        &dir,
        "creds",
        "[default]\naws_access_key_id = DEFID\naws_secret_access_key = d\n\
         [profile ci]\naws_access_key_id = WRONG1\naws_secret_access_key = w\n\
         [ci]\naws_access_key_id = CIID\naws_secret_access_key = cis\n",
    );
    let config = write(
        &dir,
        "config",
        "[ci]\naws_access_key_id = WRONG2\naws_secret_access_key = w\n\
         [profile ci]\naws_access_key_id = CONFCI\naws_secret_access_key = c\n",
    );
    let creds_env = ("AWS_SHARED_CREDENTIALS_FILE", s(&creds));
    let config_env = ("AWS_CONFIG_FILE", s(&config));
    let id = |vars: &[(&str, &str)]| parts(&chain(vars).resolve().unwrap()).0;
    assert_eq!(id(&[creds_env, config_env, ("AWS_PROFILE", "ci")]), "CIID");
    assert_eq!(id(&[config_env, ("AWS_PROFILE", "ci")]), "CONFCI");
    assert_eq!(id(&[creds_env, ("AWS_DEFAULT_PROFILE", "ci")]), "CIID");
    assert_eq!(
        id(&[
            creds_env,
            ("AWS_PROFILE", "default"),
            ("AWS_DEFAULT_PROFILE", "ci")
        ]),
        "DEFID"
    );
    let resolved = chain(&[creds_env, ("AWS_PROFILE", "ci")])
        .resolve()
        .unwrap();
    assert_eq!(
        resolved.source(),
        &Source::SharedCredentialsFile {
            path: creds.clone(),
            profile: "ci".to_owned()
        }
    );
}

#[test]
fn config_profile_default_wins_over_a_bare_default_section() {
    let dir = TempDir::new().unwrap();
    let config = write(
        &dir,
        "config",
        "[default]\naws_access_key_id = BARE\naws_secret_access_key = b\n\
         [profile default]\naws_access_key_id = PREFIXED\naws_secret_access_key = p\n",
    );
    let resolved = chain(&[("AWS_CONFIG_FILE", s(&config))]).resolve().unwrap();
    assert_eq!(parts(&resolved).0, "PREFIXED");
}

#[test]
fn a_profile_without_keys_gives_nothing() {
    let dir = TempDir::new().unwrap();
    let creds = write(&dir, "creds", "[default]\n");
    let config = write(
        &dir,
        "config",
        "[default]\nregion = us-east-1\nrole_arn = arn:aws:iam::1:role/r\n",
    );
    let err = chain(&[
        ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
        ("AWS_CONFIG_FILE", s(&config)),
    ])
    .resolve()
    .unwrap_err();
    let tried = missing(err);
    for look in [tried.credentials_file(), tried.config_file()] {
        assert!(
            matches!(
                look,
                FileLook::At {
                    outcome: FileOutcome::NoKeys,
                    ..
                }
            ),
            "{look:?}"
        );
    }
}

#[test]
fn a_missing_file_gives_nothing() {
    let dir = TempDir::new().unwrap();
    let absent = dir.path().join("absent");
    let err = chain(&[
        ("AWS_SHARED_CREDENTIALS_FILE", s(&absent)),
        ("AWS_CONFIG_FILE", s(&absent)),
    ])
    .resolve()
    .unwrap_err();
    let tried = missing(err);
    let not_found = FileLook::At {
        path: absent,
        outcome: FileOutcome::NotFound,
    };
    assert_eq!(tried.credentials_file(), &not_found);
    assert_eq!(tried.config_file(), &not_found);
}

#[test]
fn a_file_that_opens_but_cannot_be_read_stops_the_chain_as_in_go() {
    let dir = TempDir::new().unwrap();
    let config = write(&dir, "config", CONFIG_FILE);
    // A directory opens on Unix and fails on read: the SDK's load fails, so Go
    // does not start, though the config file has keys.
    let err = chain(&[
        ("AWS_SHARED_CREDENTIALS_FILE", s(dir.path())),
        ("AWS_CONFIG_FILE", s(&config)),
        ("AWS_ACCESS_KEY_ID", "ENVID"),
        ("AWS_SECRET_ACCESS_KEY", "e"),
    ])
    .resolve()
    .unwrap_err();
    assert!(matches!(err, CredentialError::ReadFailed { .. }), "{err:?}");
}

#[test]
fn a_malformed_line_is_refused_by_number_never_by_text() {
    let dir = TempDir::new().unwrap();
    for (text, line) in [
        (format!("[default]\naws_access_key_id = A\n{CANARY}\n"), 3),
        // An unterminated header would otherwise file the keys under [default].
        (
            format!(
                "[default]\naws_access_key_id = A\naws_secret_access_key = a\n[ci\naws_secret_access_key = {CANARY}\n"
            ),
            4,
        ),
    ] {
        let path = write(&dir, "creds", &text);
        let err = chain(&[("AWS_SHARED_CREDENTIALS_FILE", s(&path))])
            .resolve()
            .unwrap_err();
        assert!(
            matches!(&err, CredentialError::MalformedLine { path: p, line: l } if *p == path && *l == line),
            "{err:?}"
        );
        assert!(!format!("{err} {err:?}").contains("CANARY"), "{err}");
    }
}

#[test]
fn a_profile_with_half_its_keys_is_an_error() {
    let dir = TempDir::new().unwrap();
    let config = write(&dir, "config", CONFIG_FILE);
    for half in [
        "[default]\naws_access_key_id = A\n",
        "[default]\naws_secret_access_key = CANARY-half\n",
    ] {
        let creds = write(&dir, "creds", half);
        let err = chain(&[
            ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
            ("AWS_CONFIG_FILE", s(&config)),
        ])
        .resolve()
        .unwrap_err();
        assert!(
            matches!(&err, CredentialError::PartialProfile { profile, .. } if profile == "default"),
            "{err:?}"
        );
        assert!(!err.to_string().contains("CANARY"));
    }
}

#[test]
fn a_profile_named_by_aws_profile_must_exist() {
    let dir = TempDir::new().unwrap();
    let creds = write(&dir, "creds", CREDS_FILE);
    let vars = [
        ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
        ("AWS_PROFILE", "gone"),
    ];
    let err = chain(&vars).resolve().unwrap_err();
    assert!(
        matches!(&err, CredentialError::ProfileNotFound(tried) if tried.profile() == "gone"),
        "{err:?}"
    );
    assert!(err.to_string().contains("\"gone\""), "{err}");
    // AWS_DEFAULT_PROFILE is a preference, not a requirement, as in Go.
    let err = chain(&[vars[0], ("AWS_DEFAULT_PROFILE", "gone")])
        .resolve()
        .unwrap_err();
    assert!(matches!(err, CredentialError::Missing(_)), "{err:?}");
    // The SDK loads the profile before any source, so environment and explicit
    // keys do not save a start from a missing named profile, as in Go.
    let err = chain(&[
        vars[0],
        vars[1],
        ("AWS_ACCESS_KEY_ID", "ENVID"),
        ("AWS_SECRET_ACCESS_KEY", "e"),
    ])
    .resolve()
    .unwrap_err();
    assert!(
        matches!(err, CredentialError::ProfileNotFound(_)),
        "{err:?}"
    );
    let explicit = S3Credentials::new("X".to_owned(), SecretString::new("y".to_owned()));
    let err = chain(&vars).with_explicit(explicit).resolve().unwrap_err();
    assert!(
        matches!(err, CredentialError::ProfileNotFound(_)),
        "{err:?}"
    );
}

#[test]
fn the_missing_error_names_every_source_tried() {
    let dir = TempDir::new().unwrap();
    let creds = dir.path().join("no-creds");
    let config = write(&dir, "config", "[profile other]\n");
    let err = chain(&[
        ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
        ("AWS_CONFIG_FILE", s(&config)),
        ("AWS_DEFAULT_PROFILE", "ci"),
    ])
    .resolve()
    .unwrap_err();
    let shown = err.to_string();
    for needle in [
        "explicit values (none given)",
        "AWS_ACCESS_KEY_ID",
        "LITESTREAM_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY,",
        "AWS_SECRET_ACCESS_KEY",
        "LITESTREAM_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        &format!("shared credentials file {} (not found)", creds.display()),
        &format!(
            "shared config file {} (no profile \"ci\")",
            config.display()
        ),
    ] {
        assert!(shown.contains(needle), "{needle} not in {shown}");
    }
    assert_eq!(shown.lines().count(), 1, "{shown}");
    let tried = missing(err);
    assert_eq!(tried.environment(), EnvGap::Both);

    // Only the secret's variables when the key id is there, and no home at all.
    let shown = chain(&[("AWS_ACCESS_KEY_ID", "id")])
        .resolve()
        .unwrap_err()
        .to_string();
    assert!(
        shown.contains(
            "(AWS_SECRET_ACCESS_KEY, LITESTREAM_SECRET_ACCESS_KEY, AWS_SECRET_KEY unset)"
        ),
        "{shown}"
    );
    assert!(!shown.contains("LITESTREAM_ACCESS_KEY_ID"), "{shown}");
    assert!(
        shown.contains(&format!(
            "shared credentials file (neither AWS_SHARED_CREDENTIALS_FILE nor {HOME_VAR} set)"
        )),
        "{shown}"
    );
    assert!(
        shown.contains(&format!(
            "shared config file (neither AWS_CONFIG_FILE nor {HOME_VAR} set)"
        )),
        "{shown}"
    );
}

#[test]
fn values_never_print() {
    let dir = TempDir::new().unwrap();
    let creds = write(
        &dir,
        "creds",
        &format!(
            "[default]\naws_access_key_id = AK{CANARY}\naws_secret_access_key = {CANARY}\naws_session_token = {CANARY}\n"
        ),
    );
    let chain = chain(&[("AWS_SHARED_CREDENTIALS_FILE", s(&creds))]);
    let resolved = chain.resolve().unwrap();
    for shown in [format!("{chain:?}"), format!("{resolved:?}")] {
        assert!(!shown.contains("CANARY"), "{shown}");
    }
}

#[test]
fn a_file_over_the_limit_is_refused() {
    let dir = TempDir::new().unwrap();
    let limit = usize::try_from(MAX_SHARED_FILE_BYTES).unwrap();
    let path = write(&dir, "creds", &"#".repeat(limit + 1));
    let err = chain(&[("AWS_SHARED_CREDENTIALS_FILE", s(&path))])
        .resolve()
        .unwrap_err();
    assert!(
        matches!(err, CredentialError::FileTooLarge { .. }),
        "{err:?}"
    );
    let path = write(&dir, "creds", &"#".repeat(limit));
    let err = chain(&[("AWS_SHARED_CREDENTIALS_FILE", s(&path))])
        .resolve()
        .unwrap_err();
    assert!(matches!(err, CredentialError::Missing(_)), "{err:?}");
}

#[test]
fn the_ini_dialect_matches_go() {
    let text = "\
aws_access_key_id = BEFORE_ANY_HEADER
# comment
; comment
  [ default ] ; trailing comment
AWS_Access_Key_ID: \"QUOTED\" # inline comment
aws_secret_access_key = 'sec#ret' ;inline
s3 =
  aws_session_token = nested-not-a-token
[other]
aws_session_token = elsewhere
[default]
aws_session_token = tok
  aws_secret_access_key = promoted
";
    let path = Path::new("/f");
    let credentials = shared::credentials_file(text, path).unwrap();
    let default = &credentials["default"];
    assert_eq!(default.string(ini::ACCESS_KEY_ID), "QUOTED");
    assert_eq!(default.string(ini::SECRET_ACCESS_KEY), "promoted");
    assert_eq!(default.string(ini::SESSION_TOKEN), "tok");
    assert!(!credentials.contains_key("absent"));
    // `[ default ]` is the bare default, which the config file keeps; a bare
    // `[other]` it drops.
    let config = shared::config_file(text, path).unwrap();
    assert_eq!(config["default"].string(ini::ACCESS_KEY_ID), "QUOTED");
    assert!(!config.contains_key("other"));
}

/// The SDK loads the chosen profile before any source: a section with a key id and
/// no secret stops Go's start even with complete environment keys.
#[test]
fn a_partial_profile_stops_the_chain_even_with_environment_keys() {
    let dir = TempDir::new().unwrap();
    let config = write(&dir, "config", "[default]\nAWS_ACCESS_KEY_ID = lonely\n");
    let err = chain(&[
        ("AWS_CONFIG_FILE", s(&config)),
        ("AWS_ACCESS_KEY_ID", "ENVID"),
        ("AWS_SECRET_ACCESS_KEY", "e"),
    ])
    .resolve()
    .unwrap_err();
    assert!(
        matches!(err, CredentialError::PartialProfile { .. }),
        "{err:?}"
    );
}

/// Go copies `LITESTREAM_*` into an `AWS_*` variable only when that is unset: an
/// empty `AWS_ACCESS_KEY_ID` blocks the copy, so the SDK reads `AWS_ACCESS_KEY`.
#[test]
fn an_empty_aws_variable_blocks_the_litestream_copy() {
    let resolved = chain(&[
        ("AWS_ACCESS_KEY_ID", ""),
        ("LITESTREAM_ACCESS_KEY_ID", "LSID"),
        ("AWS_ACCESS_KEY", "SHORTID"),
        ("AWS_SECRET_ACCESS_KEY", "s"),
    ])
    .resolve()
    .unwrap();
    assert_eq!(parts(&resolved), keys("SHORTID", "s"));
    let resolved = chain(&[
        ("LITESTREAM_ACCESS_KEY_ID", "LSID"),
        ("AWS_ACCESS_KEY", "SHORTID"),
        ("AWS_SECRET_ACCESS_KEY", "s"),
    ])
    .resolve()
    .unwrap();
    assert_eq!(parts(&resolved), keys("LSID", "s"));
}

/// The SDK's merge: the credentials file's copy of a section replaces the config
/// file's section error, and its keys replace the config file's only as a pair.
#[test]
fn the_credentials_file_is_merged_over_the_config_file_as_the_sdk_merges() {
    let dir = TempDir::new().unwrap();
    let config = write(&dir, "config", "[default]\naws_access_key_id = CONFID\n");
    let creds = write(&dir, "creds", "[default]\nregion = x\n");
    let resolved = chain(&[
        ("AWS_CONFIG_FILE", s(&config)),
        ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
        ("AWS_ACCESS_KEY_ID", "ENVID"),
        ("AWS_SECRET_ACCESS_KEY", "e"),
    ])
    .resolve()
    .unwrap();
    assert_eq!(parts(&resolved), keys("ENVID", "e"));
    let config = write(&dir, "config2", CONFIG_FILE);
    let creds = write(&dir, "creds2", "[default]\naws_secret_access_key = only\n");
    let err = chain(&[
        ("AWS_CONFIG_FILE", s(&config)),
        ("AWS_SHARED_CREDENTIALS_FILE", s(&creds)),
    ])
    .resolve()
    .unwrap_err();
    assert!(
        matches!(err, CredentialError::PartialProfile { .. }),
        "{err:?}"
    );
}

/// An indented line with no separator continues the value above it with a newline.
#[test]
fn an_indented_line_without_a_separator_continues_the_value() {
    let dir = TempDir::new().unwrap();
    let creds = write(
        &dir,
        "creds",
        "[default]\naws_access_key_id = ID\n  more\naws_secret_access_key = s\n",
    );
    let resolved = chain(&[("AWS_SHARED_CREDENTIALS_FILE", s(&creds))])
        .resolve()
        .unwrap();
    assert_eq!(parts(&resolved), keys("ID\nmore", "s"));
}

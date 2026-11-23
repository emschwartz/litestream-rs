//! S3 credentials against Go: generated environments, shared credential and config files and
//! config-file keys, each resolved by Go's own path (the replica client's keys from
//! `NewS3ReplicaClientFromConfig`, then `config.LoadDefaultConfig` as its `Init` calls it,
//! through the dumper) and by litestream-rs's (`lsr_config::plan`, then
//! `lsr_store_s3::CredentialChain`). Both must sign with the same key id, secret and session
//! token, or both refuse.
//!
//! Every key is fake: the values name where they came from (`env-AWS_ACCESS_KEY_ID`,
//! `cred-p1-aws_access_key_id`), so a disagreement shows which source each side took.
//!
//! The generator covers the static sources both chains build: the eight credential
//! variables set, empty or unset; `AWS_PROFILE` and `AWS_DEFAULT_PROFILE` naming a profile
//! that exists in one file, both or neither; the two file-path variables, pointing at a file
//! or at nothing; and files with key spellings in any case, spaces, comments, blank values,
//! repeated keys and sections, `[profile x]` in either file, and lines that are not INI.
//! Profiles that need a network service or a subprocess are the roles differential's
//! (`dropin_roles`); the ones litestream-rs does not build yet (an assumed role, SSO) are the
//! known gap of CFG-7, asserted as a gap in `go_resolves_what_rust_does_not_build`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use lsr_compat::{
    Answer, Answers, Differential, Dump, DumpEnv, FakeAws, Roles, TokenAnswer, go_dump,
    go_dump_binary, rust_dump,
};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;

const PROFILES: &[&str] = &["default", "p1", "p2", "nope", ""];

/// The variables that hold keys, in no order: the generator sets each independently.
const KEY_VARS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY",
    "LITESTREAM_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SECRET_KEY",
    "LITESTREAM_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
];

#[derive(Clone, Debug)]
enum Var {
    Unset,
    Empty,
    Set,
}

#[derive(Clone, Debug)]
enum FileVar {
    Unset,
    Empty,
    /// The generated file, at a path of its own.
    Moved,
    Missing,
    /// A directory: it opens and fails on read.
    Directory,
}

#[derive(Clone, Debug)]
struct Line {
    /// Index into the key spellings, or a non-key line.
    what: LineKind,
    value: Value,
}

#[derive(Clone, Debug)]
enum LineKind {
    Key(&'static str),
    Comment,
    Blank,
    NotIni,
    Continuation,
}

#[derive(Clone, Copy, Debug)]
enum Value {
    Plain,
    Spaced,
    Empty,
    TrailingComment,
    Quoted,
}

#[derive(Clone, Debug)]
struct Section {
    header: Header,
    profile: &'static str,
    lines: Vec<Line>,
}

#[derive(Clone, Copy, Debug)]
enum Header {
    Bare,
    Prefixed,
    Padded,
    Commented,
}

#[derive(Clone, Debug)]
struct SharedFile {
    present: bool,
    sections: Vec<Section>,
}

#[derive(Clone, Debug)]
enum ConfigKey {
    Unset,
    Empty,
    Set,
}

#[derive(Clone, Debug)]
struct Case {
    vars: Vec<Var>,
    profile: Option<&'static str>,
    default_profile: Option<&'static str>,
    credentials_var: FileVar,
    config_var: FileVar,
    credentials: SharedFile,
    config: SharedFile,
    global_id: ConfigKey,
    global_secret: ConfigKey,
    replica_id: ConfigKey,
    replica_secret: ConfigKey,
}

const KEYS: &[&str] = &[
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "AWS_ACCESS_KEY_ID",
    "Aws_Secret_Access_Key",
    "region",
];

fn line() -> impl Strategy<Value = Line> {
    let what = prop_oneof![
        8 => proptest::sample::select(KEYS).prop_map(LineKind::Key),
        1 => Just(LineKind::Comment),
        1 => Just(LineKind::Blank),
        1 => Just(LineKind::NotIni),
        1 => Just(LineKind::Continuation),
    ];
    let value = prop_oneof![
        6 => Just(Value::Plain),
        1 => Just(Value::Spaced),
        1 => Just(Value::Empty),
        1 => Just(Value::TrailingComment),
        1 => Just(Value::Quoted),
    ];
    (what, value).prop_map(|(what, value)| Line { what, value })
}

fn section() -> impl Strategy<Value = Section> {
    (
        prop_oneof![
            6 => Just(Header::Bare),
            3 => Just(Header::Prefixed),
            1 => Just(Header::Padded),
            1 => Just(Header::Commented),
        ],
        proptest::sample::select(&PROFILES[..4]),
        prop::collection::vec(line(), 0..5),
    )
        .prop_map(|(header, profile, lines)| Section {
            header,
            profile,
            lines,
        })
}

fn shared_file() -> impl Strategy<Value = SharedFile> {
    (
        prop::bool::weighted(0.8),
        prop::collection::vec(section(), 0..4),
    )
        .prop_map(|(present, sections)| SharedFile { present, sections })
}

fn var() -> impl Strategy<Value = Var> {
    prop_oneof![5 => Just(Var::Unset), 1 => Just(Var::Empty), 2 => Just(Var::Set)]
}

fn file_var() -> impl Strategy<Value = FileVar> {
    prop_oneof![
        4 => Just(FileVar::Unset),
        1 => Just(FileVar::Empty),
        2 => Just(FileVar::Moved),
        1 => Just(FileVar::Missing),
        1 => Just(FileVar::Directory),
    ]
}

fn config_key() -> impl Strategy<Value = ConfigKey> {
    prop_oneof![
        6 => Just(ConfigKey::Unset),
        1 => Just(ConfigKey::Empty),
        1 => Just(ConfigKey::Set),
    ]
}

fn profile() -> impl Strategy<Value = Option<&'static str>> {
    prop::option::weighted(0.4, proptest::sample::select(PROFILES))
}

fn case() -> impl Strategy<Value = Case> {
    (
        prop::collection::vec(var(), KEY_VARS.len()),
        profile(),
        profile(),
        file_var(),
        file_var(),
        shared_file(),
        shared_file(),
        (config_key(), config_key(), config_key(), config_key()),
    )
        .prop_map(
            |(
                vars,
                profile,
                default_profile,
                credentials_var,
                config_var,
                credentials,
                config,
                keys,
            )| {
                let (global_id, global_secret, replica_id, replica_secret) = keys;
                Case {
                    vars,
                    profile,
                    default_profile,
                    credentials_var,
                    config_var,
                    credentials,
                    config,
                    global_id,
                    global_secret,
                    replica_id,
                    replica_secret,
                }
            },
        )
}

fn render(file: &SharedFile, tag: &str) -> String {
    let mut text = String::new();
    for section in &file.sections {
        let name = match section.header {
            Header::Bare => format!("[{}]", section.profile),
            Header::Prefixed => format!("[profile {}]", section.profile),
            Header::Padded => format!("[ {} ]", section.profile),
            Header::Commented => format!("[{}] # note", section.profile),
        };
        writeln!(text, "{name}").unwrap();
        for line in &section.lines {
            let value = format!("{tag}-{}-{}", section.profile, line_key(line));
            match line.what {
                LineKind::Key(key) => {
                    let rendered = match line.value {
                        Value::Plain => format!("{key}={value}"),
                        Value::Spaced => format!("  {key} =  {value}  "),
                        Value::Empty => format!("{key} ="),
                        Value::TrailingComment => format!("{key} = {value} # trailing"),
                        Value::Quoted => format!("{key} = \"{value}\""),
                    };
                    writeln!(text, "{rendered}").unwrap();
                }
                LineKind::Comment => text.push_str("# a comment = not a key\n; another\n"),
                LineKind::Blank => text.push('\n'),
                LineKind::NotIni => text.push_str("this line is not ini\n"),
                LineKind::Continuation => text.push_str("    continued-value\n"),
            }
        }
    }
    text
}

fn line_key(line: &Line) -> String {
    match line.what {
        LineKind::Key(key) => key.to_ascii_lowercase(),
        LineKind::Comment | LineKind::Blank | LineKind::NotIni | LineKind::Continuation => {
            String::new()
        }
    }
}

fn config_line(text: &mut String, indent: &str, key: &str, value: &ConfigKey, tag: &str) {
    match value {
        ConfigKey::Unset => {}
        ConfigKey::Empty => writeln!(text, "{indent}{key}: \"\"").unwrap(),
        ConfigKey::Set => writeln!(text, "{indent}{key}: {tag}-{key}").unwrap(),
    }
}

/// Writes the case's files under `base` and returns the environment both sides see.
fn setup(base: &Path, case: &Case) -> (DumpEnv, String) {
    let home = base.join("home");
    std::fs::create_dir_all(home.join(".aws")).unwrap();
    let mut vars = BTreeMap::new();
    for (name, var) in KEY_VARS.iter().zip(&case.vars) {
        match var {
            Var::Unset => {}
            Var::Empty => drop(vars.insert((*name).to_owned(), String::new())),
            Var::Set => drop(vars.insert((*name).to_owned(), format!("env-{name}"))),
        }
    }
    for (name, profile) in [
        ("AWS_PROFILE", case.profile),
        ("AWS_DEFAULT_PROFILE", case.default_profile),
    ] {
        if let Some(profile) = profile {
            vars.insert(name.to_owned(), profile.to_owned());
        }
    }
    let mut report = String::new();
    for (var, file, default_name, tag, setting) in [
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            &case.credentials,
            "credentials",
            "cred",
            &case.credentials_var,
        ),
        (
            "AWS_CONFIG_FILE",
            &case.config,
            "config",
            "conf",
            &case.config_var,
        ),
    ] {
        let text = render(file, tag);
        let path = match setting {
            FileVar::Unset | FileVar::Empty => home.join(".aws").join(default_name),
            FileVar::Moved => base.join(format!("moved-{default_name}")),
            FileVar::Missing => base.join(format!("missing-{default_name}")),
            FileVar::Directory => base.join(format!("dir-{default_name}")),
        };
        match setting {
            FileVar::Unset => {}
            FileVar::Empty => drop(vars.insert(var.to_owned(), String::new())),
            FileVar::Moved | FileVar::Missing | FileVar::Directory => {
                drop(vars.insert(var.to_owned(), path.display().to_string()));
            }
        }
        match setting {
            FileVar::Directory => std::fs::create_dir_all(&path).unwrap(),
            FileVar::Missing => {}
            FileVar::Unset | FileVar::Empty | FileVar::Moved if file.present => {
                std::fs::write(&path, &text).unwrap();
            }
            FileVar::Unset | FileVar::Empty | FileVar::Moved => {}
        }
        writeln!(report, "{} ({}):\n{text}", path.display(), file.present).unwrap();
    }
    let env = DumpEnv {
        vars,
        home,
        cwd: base.to_owned(),
        credentials: true,
    };
    (env, report)
}

fn config_text(case: &Case) -> String {
    let mut text = String::new();
    config_line(&mut text, "", "access-key-id", &case.global_id, "global");
    config_line(
        &mut text,
        "",
        "secret-access-key",
        &case.global_secret,
        "global",
    );
    text.push_str("dbs:\n  - path: /data/app.db\n    replica:\n      url: s3://bucket/prefix\n");
    config_line(
        &mut text,
        "      ",
        "access-key-id",
        &case.replica_id,
        "replica",
    );
    config_line(
        &mut text,
        "      ",
        "secret-access-key",
        &case.replica_secret,
        "replica",
    );
    text
}

fn answers(go: &Path, case: &Case) -> (Dump, Dump, String) {
    let base = tempfile::tempdir().unwrap();
    let base = base.path().canonicalize().unwrap();
    let (env, files) = setup(&base, case);
    let text = config_text(case);
    let config = base.join("litestream.yml");
    std::fs::write(&config, &text).unwrap();
    let go_out = go_dump(go, &config, &env).unwrap();
    let rust_out = rust_dump(&config, &env);
    let report = format!("env: {:?}\nconfig:\n{text}\n{files}", env.vars);
    (go_out, rust_out, report)
}

/// Whether the config names only one of the key id and secret, after Go copies each global
/// key into a replica that leaves it empty. Go ignores a lone key and signs with whatever the
/// default chain finds; litestream-rs refuses the file (D42).
fn half_keys_in_config(case: &Case) -> bool {
    let given = |replica: &ConfigKey, global: &ConfigKey| {
        matches!(replica, ConfigKey::Set) || matches!(global, ConfigKey::Set)
    };
    given(&case.replica_id, &case.global_id) != given(&case.replica_secret, &case.global_secret)
}

/// Whether a file both sides read holds a line that is not INI, which Go skips and
/// litestream-rs refuses (D43).
fn not_ini_in_a_read_file(case: &Case) -> bool {
    [
        (&case.credentials, &case.credentials_var),
        (&case.config, &case.config_var),
    ]
    .into_iter()
    .any(|(file, setting)| {
        file.present
            && !matches!(setting, FileVar::Missing | FileVar::Directory)
            && file.sections.iter().any(|section| {
                section
                    .lines
                    .iter()
                    .any(|line| matches!(line.what, LineKind::NotIni))
            })
    })
}

/// Whether one replica's credentials are a refusal.
fn credentials_refused(dump: &Dump) -> bool {
    match dump {
        Dump::Refused(_) => true,
        Dump::Runs(dbs) => dbs.iter().all(|db| {
            db.replica
                .credentials
                .as_ref()
                .is_some_and(|credentials| !credentials.error.is_empty())
        }),
    }
}

fn compare(go: &Path, case: &Case) -> Result<(), TestCaseError> {
    let (go_out, rust_out, report) = answers(go, case);
    if not_ini_in_a_read_file(case) {
        prop_assert!(
            credentials_refused(&rust_out),
            "a line that is not INI must be refused (D43)\n{report}\nRust: {rust_out:?}"
        );
        return Ok(());
    }
    if half_keys_in_config(case) {
        prop_assert!(
            matches!(rust_out, Dump::Refused(_)),
            "a lone config key must be refused (D42)\n{report}\nRust: {rust_out:?}"
        );
        return Ok(());
    }
    prop_assert!(
        go_out.agrees_with(&rust_out),
        "Go and Rust disagree\n{report}\nGo:   {go_out:?}\nRust: {rust_out:?}"
    );
    Ok(())
}

/// The differential: 1024 generated cases. A disagreement prints the environment, the config
/// and both files, shrunk, with both answers.
#[test]
#[ignore = "runs the pinned Go dumper; just compat"]
fn go_and_rust_resolve_the_same_credentials() {
    let go = must(Resource::GoBuilds, || {
        go_dump_binary().map_err(|e| e.to_string())
    });
    let run = Differential::new("static credentials");
    let compared = run.free(1024, &case(), |case| {
        compare(&go, &case).map(|()| "compared".to_owned())
    });
    if let Err(error) = compared {
        panic!("{error}");
    }
}

/// The known gap (CFG-7): a profile whose credentials come from an assumed role over another
/// profile's keys. Go's SDK calls STS `AssumeRole`; litestream-rs refuses the profile as not
/// built yet (issue #176). Asserted as a difference, so closing the gap shows here.
#[test]
#[ignore = "runs the pinned Go dumper; just compat"]
fn go_resolves_what_rust_does_not_build() {
    let go = must(Resource::GoBuilds, || {
        go_dump_binary().map_err(|e| e.to_string())
    });
    let base = tempfile::tempdir().unwrap();
    let base = base.path().canonicalize().unwrap();
    let home = base.join("home");
    std::fs::create_dir_all(home.join(".aws")).unwrap();
    std::fs::write(
        home.join(".aws").join("config"),
        "[default]\nrole_arn = arn:aws:iam::000000000000:role/fake\nsource_profile = base\n\
         [profile base]\naws_access_key_id = base-id\naws_secret_access_key = base-secret\n",
    )
    .unwrap();
    let config = base.join("litestream.yml");
    std::fs::write(
        &config,
        "dbs:\n  - path: /data/app.db\n    replica:\n      url: s3://bucket/prefix\n      region: us-east-1\n",
    )
    .unwrap();
    let fake = FakeAws::start(Answers {
        sts: Answer::Credentials,
        container: Answer::Credentials,
        container_token: None,
        imds_token: TokenAnswer::Token,
        imds_roles: Roles::One,
        imds: Answer::Credentials,
    })
    .unwrap();
    let env = DumpEnv {
        vars: BTreeMap::from([
            ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
            ("AWS_ENDPOINT_URL_STS".to_owned(), fake.url()),
        ]),
        home,
        cwd: base.clone(),
        credentials: true,
    };
    let go_out = go_dump(&go, &config, &env).unwrap();
    let rust_out = rust_dump(&config, &env);
    let key = |dump: &Dump| match dump {
        Dump::Runs(dbs) => dbs
            .first()
            .and_then(|db| db.replica.credentials.clone())
            .map(|c| (c.access_key_id, c.error)),
        Dump::Refused(_) => None,
    };
    let go_key = key(&go_out);
    assert!(
        go_key
            .as_ref()
            .is_some_and(|(id, error)| id == "sts-id" && error.is_empty()),
        "{go_out:?}"
    );
    let rust_key = key(&rust_out);
    assert!(
        rust_key.as_ref().is_some_and(
            |(id, error)| id.is_empty() && error.contains("not built in litestream-rs yet")
        ),
        "{rust_out:?}"
    );
}

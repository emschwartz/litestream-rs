//! Temporary S3 credentials against Go: web identity, container credentials, the EC2
//! instance metadata service and `credential_process`, each resolved by Go's SDK (through
//! the dumper, as the replica client's `Init` loads it) and by litestream-rs's chain, both
//! pointed at one [`FakeAws`] through the SDK's endpoint variables. Both must sign with the
//! same key id, secret and session token, or both refuse.
//!
//! The generator covers which sources are configured (the environment's web identity, a
//! profile of each kind, a container URL allowed or not, with or without its token, the
//! metadata service on or off) and what each service answers: credentials, a 4xx, a body
//! that is not its format, or one without keys; for the metadata service also a token
//! refused with each status the SDK distinguishes and a role list of none, one or two.
//! Server errors (5xx) are left out: Go's SDK retries them with backoff, which would make each
//! case take seconds, and both sides end in a refusal anyway. Their classification is a unit
//! test in `lsr-store-s3`.
//!
//! Each source must be shown deciding, so the run is two parts. The free part draws every
//! field as above. The aimed part draws a case for one source at a time, with every source
//! before it in the chain left out, the source itself ready and its service answering
//! credentials, so a fixed share of cases is decided by each source whatever the free part
//! draws, and each must be decided by the source it aims at. Both run through
//! [`Differential`], with fixed seeds.
//!
//! The profiles litestream-rs does not build yet (an assumed role through `source_profile`
//! or `credential_source`, IAM Identity Center, a login session) are asserted refused with
//! the not-built error, not compared: issue #176 tracks them.

use std::collections::BTreeMap;
use std::path::Path;

use lsr_compat::{
    Answer, Answers, CompatError, Differential, Dump, DumpEnv, FakeAws, Roles, TokenAnswer,
    go_dump, go_dump_binary, rust_dump,
};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;

#[derive(Clone, Copy, Debug)]
enum WebIdentity {
    None,
    /// The token file exists and the role ARN is set.
    Ready,
    /// The role ARN is unset.
    NoRole,
    /// The token file does not exist.
    NoFile,
}

#[derive(Clone, Copy, Debug)]
enum Process {
    Keys,
    WrongVersion,
    NoKeys,
    Fails,
    NotJson,
}

#[derive(Clone, Copy, Debug)]
enum Profile {
    None,
    Keys,
    WebIdentity,
    /// `web_identity_token_file` without `role_arn`: a load-time refusal in Go.
    WebIdentityNoRole,
    Process(Process),
    /// Two credential kinds in one profile: a load-time refusal in Go.
    TwoKinds,
    RoleFromBase,
    CredentialSource,
    Sso,
}

#[derive(Clone, Copy, Debug)]
enum Container {
    None,
    /// The fake, on loopback.
    Fake,
    /// A plain-HTTP host that is neither loopback nor a container address.
    Disallowed,
}

#[derive(Clone, Copy, Debug)]
enum ContainerToken {
    None,
    Right,
    Wrong,
    /// The right token, from `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`.
    File,
}

#[derive(Clone, Debug)]
struct Case {
    env_keys: bool,
    web_identity: WebIdentity,
    profile: Profile,
    container: Container,
    container_token: ContainerToken,
    imds: bool,
    answers: Answers,
}

fn answer() -> impl Strategy<Value = Answer> {
    prop_oneof![
        6 => Just(Answer::Credentials),
        1 => prop::sample::select(vec![400_u16, 403, 404]).prop_map(Answer::Status),
        1 => Just(Answer::Malformed),
        1 => Just(Answer::Empty),
    ]
}

fn answers() -> impl Strategy<Value = Answers> {
    (
        answer(),
        answer(),
        prop::option::weighted(0.3, Just("container-auth")),
        prop_oneof![
            4 => Just(TokenAnswer::Token),
            1 => Just(TokenAnswer::NoTtl),
            2 => prop::sample::select(vec![400_u16, 401, 403, 404, 405]).prop_map(TokenAnswer::Status),
        ],
        prop_oneof![1 => Just(Roles::None), 4 => Just(Roles::One), 1 => Just(Roles::Two)],
        answer(),
    )
        .prop_map(
            |(sts, container, container_token, imds_token, imds_roles, imds)| Answers {
                sts,
                container,
                container_token,
                imds_token,
                imds_roles,
                imds,
            },
        )
}

fn profile() -> impl Strategy<Value = Profile> {
    let process = prop_oneof![
        3 => Just(Process::Keys),
        1 => Just(Process::WrongVersion),
        1 => Just(Process::NoKeys),
        1 => Just(Process::Fails),
        1 => Just(Process::NotJson),
    ];
    prop_oneof![
        8 => Just(Profile::None),
        1 => Just(Profile::Keys),
        2 => Just(Profile::WebIdentity),
        1 => Just(Profile::WebIdentityNoRole),
        3 => process.prop_map(Profile::Process),
        1 => Just(Profile::TwoKinds),
        1 => Just(Profile::RoleFromBase),
        1 => Just(Profile::CredentialSource),
        1 => Just(Profile::Sso),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    (
        prop::bool::weighted(0.1),
        prop_oneof![
            8 => Just(WebIdentity::None),
            2 => Just(WebIdentity::Ready),
            1 => Just(WebIdentity::NoRole),
            1 => Just(WebIdentity::NoFile),
        ],
        profile(),
        prop_oneof![
            5 => Just(Container::None),
            3 => Just(Container::Fake),
            1 => Just(Container::Disallowed),
        ],
        prop_oneof![
            Just(ContainerToken::None),
            Just(ContainerToken::Right),
            Just(ContainerToken::Wrong),
            Just(ContainerToken::File),
        ],
        prop::bool::weighted(0.8),
        answers(),
    )
        .prop_map(
            |(env_keys, web_identity, profile, container, container_token, imds, answers)| Case {
                env_keys,
                web_identity,
                profile,
                container,
                container_token,
                imds,
                answers,
            },
        )
}

/// A source the aimed part makes decide, by the key id prefix the fake gives it.
#[derive(Clone, Copy, Debug)]
enum Aim {
    Env,
    Sts,
    File,
    Proc,
    Container,
    Imds,
}

impl Aim {
    const ALL: [Self; 6] = [
        Self::Env,
        Self::Sts,
        Self::File,
        Self::Proc,
        Self::Container,
        Self::Imds,
    ];

    const fn prefix(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::Sts => "sts",
            Self::File => "file",
            Self::Proc => "proc",
            Self::Container => "container",
            Self::Imds => "imds",
        }
    }

    /// `case` with every source before this one in the chain left out (environment keys,
    /// the environment's web identity, the profile, the container URL, the metadata service),
    /// no profile Go refuses at load, and this one ready, its service answering credentials.
    fn shape(self, mut case: Case) -> Case {
        case.env_keys = matches!(self, Self::Env);
        if !matches!(self, Self::Env) {
            case.web_identity = match self {
                Self::Sts => WebIdentity::Ready,
                Self::Env | Self::File | Self::Proc | Self::Container | Self::Imds => {
                    WebIdentity::None
                }
            };
        }
        match self {
            // A profile both sides refuse at load ends the chain before any source runs.
            Self::Env | Self::Sts | Self::Container | Self::Imds => case.profile = Profile::None,
            Self::File => case.profile = Profile::Keys,
            Self::Proc => case.profile = Profile::Process(Process::Keys),
        }
        case.answers.sts = Answer::Credentials;
        match self {
            Self::Env | Self::Sts | Self::File | Self::Proc => {}
            Self::Container => {
                case.container = Container::Fake;
                case.container_token = ContainerToken::None;
                case.answers.container_token = None;
                case.answers.container = Answer::Credentials;
            }
            Self::Imds => {
                case.container = Container::None;
                case.imds = true;
                case.answers.imds_token = TokenAnswer::Token;
                case.answers.imds_roles = Roles::One;
                case.answers.imds = Answer::Credentials;
            }
        }
        case
    }
}

/// Cases per source in the aimed part.
const AIMED: u32 = 32;

const ROLE: &str = "arn:aws:iam::000000000000:role/fake";

fn process_command(process: Process) -> &'static str {
    match process {
        Process::Keys => {
            r#"printf '{"Version":1,"AccessKeyId":"proc-id","SecretAccessKey":"proc-secret","SessionToken":"proc-token","Expiration":"2099-01-01T00:00:00Z"}'"#
        }
        Process::WrongVersion => {
            r#"printf '{"Version":2,"AccessKeyId":"proc-id","SecretAccessKey":"proc-secret"}'"#
        }
        Process::NoKeys => r#"printf '{"Version":1}'"#,
        Process::Fails => "exit 3",
        Process::NotJson => "printf 'not json'",
    }
}

fn profile_text(profile: Profile, token_file: &Path) -> String {
    let token = token_file.display();
    let body = match profile {
        Profile::None => String::new(),
        Profile::Keys => "aws_access_key_id = file-id\naws_secret_access_key = file-secret\n".to_owned(),
        Profile::WebIdentity => {
            format!("web_identity_token_file = {token}\nrole_arn = {ROLE}\nrole_session_name = s\n")
        }
        Profile::WebIdentityNoRole => format!("web_identity_token_file = {token}\n"),
        Profile::Process(process) => format!("credential_process = {}\n", process_command(process)),
        Profile::TwoKinds => format!(
            "web_identity_token_file = {token}\nrole_arn = {ROLE}\ncredential_process = {}\n",
            process_command(Process::Keys)
        ),
        Profile::RoleFromBase => format!("role_arn = {ROLE}\nsource_profile = base\n"),
        Profile::CredentialSource => format!("role_arn = {ROLE}\ncredential_source = Environment\n"),
        Profile::Sso => "sso_session = s\nsso_account_id = 000000000000\nsso_role_name = r\n\
                         [sso-session s]\nsso_region = us-east-1\nsso_start_url = https://fake.invalid/start\n"
            .to_owned(),
    };
    format!(
        "[default]\nregion = us-east-1\n{body}\n[profile base]\naws_access_key_id = base-id\naws_secret_access_key = base-secret\n"
    )
}

/// The environment and files for a case; the fake must outlive the run.
fn setup(base: &Path, case: &Case, fake: &FakeAws) -> DumpEnv {
    let home = base.join("home");
    std::fs::create_dir_all(home.join(".aws")).unwrap();
    let token_file = base.join("web-identity-token");
    std::fs::write(&token_file, "fake-web-identity-token").unwrap();
    std::fs::write(
        home.join(".aws").join("config"),
        profile_text(case.profile, &token_file),
    )
    .unwrap();
    let mut vars = BTreeMap::from([
        ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
        ("AWS_ENDPOINT_URL_STS".to_owned(), fake.url()),
    ]);
    let mut set = |name: &str, value: String| drop(vars.insert(name.to_owned(), value));
    if case.env_keys {
        set("AWS_ACCESS_KEY_ID", "env-id".to_owned());
        set("AWS_SECRET_ACCESS_KEY", "env-secret".to_owned());
    }
    match case.web_identity {
        WebIdentity::None => {}
        WebIdentity::Ready | WebIdentity::NoRole | WebIdentity::NoFile => {
            let file = match case.web_identity {
                WebIdentity::NoFile => base.join("no-such-token"),
                WebIdentity::None | WebIdentity::Ready | WebIdentity::NoRole => token_file.clone(),
            };
            set("AWS_WEB_IDENTITY_TOKEN_FILE", file.display().to_string());
            if !matches!(case.web_identity, WebIdentity::NoRole) {
                set("AWS_ROLE_ARN", ROLE.to_owned());
            }
            set("AWS_ROLE_SESSION_NAME", "env-session".to_owned());
        }
    }
    match case.container {
        Container::None => {}
        Container::Fake => set("AWS_CONTAINER_CREDENTIALS_FULL_URI", fake.container_url()),
        Container::Disallowed => set(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://192.0.2.1/container-credentials".to_owned(),
        ),
    }
    match case.container_token {
        ContainerToken::None => {}
        ContainerToken::Right => set(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
            "container-auth".to_owned(),
        ),
        ContainerToken::Wrong => set("AWS_CONTAINER_AUTHORIZATION_TOKEN", "wrong".to_owned()),
        ContainerToken::File => {
            let file = base.join("container-token");
            std::fs::write(&file, "container-auth").unwrap();
            set(
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                file.display().to_string(),
            );
        }
    }
    if case.imds {
        set("AWS_EC2_METADATA_SERVICE_ENDPOINT", fake.url());
    }
    DumpEnv {
        vars,
        home,
        cwd: base.to_path_buf(),
        credentials: true,
    }
}

const CONFIG: &str = "dbs:\n  - path: /data/app.db\n    replica:\n      url: s3://bucket/prefix\n      region: us-east-1\n";

fn answers_for(go: &Path, case: &Case) -> (Dump, Dump, String) {
    let base = tempfile::tempdir().unwrap();
    let base = base.path().canonicalize().unwrap();
    let fake = FakeAws::start(case.answers).unwrap();
    let env = setup(&base, case, &fake);
    let config = base.join("litestream.yml");
    std::fs::write(&config, CONFIG).unwrap();
    let go_out = match go_dump(go, &config, &env) {
        Ok(out) => out,
        // The SDK's STS providers dereference the answer's `Credentials` and `Expiration`
        // without a check (`stscreds/web_identity_provider.go:152`,
        // `stscreds/assume_role_provider.go:321`), so an STS answer without them crashes Go's
        // dumper, as it would crash litestream. Counted as a refusal: litestream-rs must
        // refuse there too.
        Err(CompatError::Process { detail, .. })
            if detail.contains("aws-sdk-go-v2/credentials@v1.19.6/stscreds/") =>
        {
            Dump::Refused(format!(
                "go panicked: {}",
                detail.lines().next().unwrap_or_default()
            ))
        }
        Err(error) => panic!("{error}"),
    };
    let rust_out = rust_dump(&config, &env);
    let shared = std::fs::read_to_string(env.home.join(".aws").join("config")).unwrap();
    let report = format!(
        "case: {case:?}\nenv: {:?}\nconfig file:\n{shared}",
        env.vars
    );
    (go_out, rust_out, report)
}

/// The error text of the one replica's credentials, if they are an error.
fn credential_error(dump: &Dump) -> Option<String> {
    match dump {
        Dump::Refused(error) => Some(error.clone()),
        Dump::Runs(dbs) => dbs
            .first()
            .and_then(|db| db.replica.credentials.as_ref())
            .filter(|credentials| !credentials.error.is_empty())
            .map(|credentials| credentials.error.clone()),
    }
}

/// The keys one side signs with, or `None` for a refusal (the other differentials compare
/// the rest of the plan). An empty key id counts as a refusal: Go's SDK takes a source's
/// answer without keys as credentials and signs with an empty key, which every bucket
/// refuses; litestream-rs refuses the answer when it arrives (`docs/decisions.md`, D49).
fn outcome(dump: &Dump) -> Option<(String, String, String)> {
    match dump {
        Dump::Refused(_) => None,
        Dump::Runs(dbs) => dbs
            .first()
            .and_then(|db| db.replica.credentials.as_ref())
            .filter(|credentials| {
                credentials.error.is_empty() && !credentials.access_key_id.is_empty()
            })
            .map(|c| {
                (
                    c.access_key_id.clone(),
                    c.secret_access_key.clone(),
                    c.session_token.clone(),
                )
            }),
    }
}

/// Whether the chain reaches a profile litestream-rs does not build: nothing before the
/// profile gives credentials and the profile is one of the kinds issue #176 tracks.
fn reaches_unbuilt(case: &Case) -> bool {
    !case.env_keys
        && matches!(case.web_identity, WebIdentity::None)
        && matches!(
            case.profile,
            Profile::RoleFromBase | Profile::CredentialSource | Profile::Sso
        )
}

/// Compares one case and names its ending: the source whose key id both sides signed with,
/// or `refused`.
fn compare(go: &Path, case: &Case) -> Result<String, TestCaseError> {
    let (go_out, rust_out, report) = answers_for(go, case);
    let won = outcome(&rust_out).map_or_else(
        || "refused".to_owned(),
        |(id, _, _)| id.split('-').next().unwrap_or_default().to_owned(),
    );
    if reaches_unbuilt(case) {
        let error = credential_error(&rust_out).unwrap_or_default();
        prop_assert!(
            error.contains("not built in litestream-rs yet"),
            "an unbuilt source must be refused as such\n{report}\nRust: {rust_out:?}"
        );
        return Ok(won);
    }
    prop_assert!(
        outcome(&go_out) == outcome(&rust_out),
        "Go and Rust disagree\n{report}\nGo:   {go_out:?}\nRust: {rust_out:?}"
    );
    Ok(won)
}

/// The differential: 1024 free cases and [`AIMED`] cases aimed at each source, against one
/// fake per case. A disagreement prints the case, the environment and the shared config
/// file, shrunk, with both answers.
#[test]
#[ignore = "runs the pinned Go dumper; just compat"]
fn go_and_rust_fetch_the_same_temporary_credentials() {
    let go = must(Resource::GoBuilds, || {
        go_dump_binary().map_err(|e| e.to_string())
    });
    let run = Differential::new("temporary credentials");
    let mut parts = vec![run.free(1024, &case(), |case| compare(&go, &case))];
    for aim in Aim::ALL {
        let cases = case().prop_map(move |case| aim.shape(case));
        parts.push(run.aimed(aim.prefix(), AIMED, &cases, |case| compare(&go, &case)));
    }
    if let Some(Err(error)) = parts.into_iter().find(Result::is_err) {
        panic!("{error}");
    }
    let sources = ["sts", "container", "imds", "proc", "env", "file", "refused"];
    match run.require(&sources, 20) {
        Ok(tally) => eprintln!("decided by: {tally:?}"),
        Err(error) => panic!("{error}"),
    }
}

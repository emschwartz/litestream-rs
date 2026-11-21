//! Environment expansion over the whole file before parsing, as Go's
//! `ParseConfig` runs `os.Expand` (each expansion rule is checked against Go
//! in `expand.rs`'s unit tests).

use std::path::Path;

use lsr_config::{Expand, GoConfig, WarningKind};

use crate::common::{TestEnv, refused_with};

fn load(text: &str, env: &TestEnv) -> GoConfig {
    match GoConfig::parse(text, Expand::Env, env) {
        Ok(loaded) => loaded.config,
        Err(error) => panic!("{}", error.report()),
    }
}

#[test]
fn dollar_name_braces_and_pid() {
    let env = TestEnv::with(&[("DB_DIR", "/data"), ("BUCKET", "backups")]);
    let config = load(
        "dbs:\n  - path: $DB_DIR/app.db\n    meta-path: /meta/$PID\n    replica:\n      url: s3://${BUCKET}/app\n",
        &env,
    );
    let db = &config.dbs()[0];
    assert_eq!(db.path(), Some(Path::new("/data/app.db")));
    assert_eq!(db.meta_path(), Some("/meta/4242"));
    assert_eq!(db.replica().unwrap().url(), Some("s3://backups/app"));
}

#[test]
fn an_unset_variable_is_empty_and_warned_once() {
    let loaded = GoConfig::parse(
        "addr: \":$NOPE\"\nexec: run $NOPE\n",
        Expand::Env,
        &TestEnv::default(),
    )
    .unwrap();
    assert_eq!(loaded.config.addr(), Some(":"));
    assert_eq!(loaded.config.exec(), Some("run"));
    let unset: Vec<_> = loaded
        .report
        .warnings
        .iter()
        .filter(|w| matches!(w.kind, WarningKind::UnsetVariable { .. }))
        .collect();
    assert_eq!(unset.len(), 1);
}

#[test]
fn no_expand_env_reads_dollars_literally() {
    let env = TestEnv::with(&[("A", "x")]);
    let loaded = GoConfig::parse("exec: echo $A ${A} $$\n", Expand::Off, &env).unwrap();
    assert_eq!(loaded.config.exec(), Some("echo $A ${A} $$"));
}

#[test]
fn expansion_happens_before_parsing_as_in_go() {
    // A value holding YAML syntax changes the document, exactly as in Go.
    let env = TestEnv::with(&[("INJECT", "x\nmcp-addr: \":3001\"")]);
    let problems = refused_with("exec: $INJECT\n", Expand::Env, &env);
    assert!(
        problems.iter().any(|p| p.path.to_string() == "mcp-addr"),
        "{problems:#?}"
    );
    // Comments are expanded too, and a `$$` there is harmless.
    let config = load("# cost: $$5\nexec: ok\n", &TestEnv::default());
    assert_eq!(config.exec(), Some("ok"));
}

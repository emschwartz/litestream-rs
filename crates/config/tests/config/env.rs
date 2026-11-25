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
/// The names the load warned of as unset, in order; a config the load refuses still reports
/// its warnings.
fn unset_names(text: &str) -> Vec<String> {
    let warnings = match GoConfig::parse(text, Expand::Env, &TestEnv::default()) {
        Ok(loaded) => loaded.report.warnings,
        Err(error) => error.report().warnings.clone(),
    };
    let mut names = Vec::new();
    for warning in &warnings {
        if let WarningKind::UnsetVariable { name } = &warning.kind {
            names.push(name.clone());
        }
    }
    names
}

/// Go expands the whole file and says nothing about a variable that is not set (`ParseConfig`
/// has no warning at all). The warning here is for a reference that changes what a value reads:
/// one in a comment, which changes nothing, is as silent as Go's.
#[test]
fn an_unset_reference_in_a_comment_is_silent() {
    for text in [
        "# the price is $9 a month\nexec: ok\n",
        "exec: ok # not $NOPE\n",
        "dbs:\n  # path: ${NOPE}/app.db\n  - path: /d/app.db\n    replica:\n      url: file:///r\n",
        "# $A $B $A\n# ${C}\nexec: ok\n",
    ] {
        assert_eq!(unset_names(text), Vec::<String>::new(), "{text}");
    }
}

/// Every place a value can read a variable still warns: a plain, a quoted and a block scalar, a
/// key, and a reference beside a comment that is not one.
#[test]
fn an_unset_reference_that_changes_a_value_still_warns() {
    for (text, name) in [
        ("exec: run $NOPE\n", "NOPE"),
        ("exec: \"run $NOPE\"\n", "NOPE"),
        ("exec: 'run $NOPE'\n", "NOPE"),
        ("exec: |\n  run $NOPE\n  # still text\n", "NOPE"),
        ("exec: |\n  # $NOPE is text here\n", "NOPE"),
        ("addr: \":$NOPE\"\n", "NOPE"),
        ("exec: a#$NOPE\n", "NOPE"),
        ("$NOPE: 1\n", "NOPE"),
    ] {
        // Some of these are not valid configs: only whether the load warned is looked at.
        assert_eq!(unset_names(text), [name.to_owned()], "{text}");
    }
}

/// Of several names only the ones a value reads are named, each once, in file order.
#[test]
fn only_the_names_a_value_reads_are_warned() {
    assert_eq!(
        unset_names("# $A\nexec: $B $C\naddr: \":$B\" # $D\n"),
        ["B", "C"]
    );
}

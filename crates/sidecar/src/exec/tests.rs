use super::*;

/// What go-shellwords v1.0.12's `Parse` returned for each line, run with Go 1.24 against the
/// module Litestream 041a8c3 pins (`go.mod`): its own `testcases` table and `TestError`, the
/// operator and redirection cases of `TestHaveMore` and `TestHaveRedirect` (with its default
/// `ParseEnv` off), and lines an `exec:` value plausibly holds. `None` is its error; the
/// position is `Parser.Position`, `-1` when it read the whole line.
const GO: &[(&str, Option<&[&str]>, i64)] = &[
    ("", Some(&[]), -1),
    ("\"\"", Some(&[""]), -1),
    ("''", Some(&[""]), -1),
    ("var --bar=baz", Some(&["var", "--bar=baz"]), -1),
    ("var --bar=\"baz\"", Some(&["var", "--bar=baz"]), -1),
    ("var \"--bar=baz\"", Some(&["var", "--bar=baz"]), -1),
    ("var \"--bar='baz'\"", Some(&["var", "--bar='baz'"]), -1),
    ("var --bar=`baz`", Some(&["var", "--bar=`baz`"]), -1),
    ("var \"--bar=\\\"baz'\"", Some(&["var", "--bar=\"baz'"]), -1),
    ("var \"--bar=\\'baz\\'\"", Some(&["var", "--bar='baz'"]), -1),
    ("var --bar='\\'", Some(&["var", "--bar=\\"]), -1),
    ("var \"--bar baz\"", Some(&["var", "--bar baz"]), -1),
    ("var --\"bar baz\"", Some(&["var", "--bar baz"]), -1),
    ("var  --\"bar baz\"", Some(&["var", "--bar baz"]), -1),
    ("a \"b\"", Some(&["a", "b"]), -1),
    ("a \" b \"", Some(&["a", " b "]), -1),
    ("a \"   \"", Some(&["a", "   "]), -1),
    ("a 'b'", Some(&["a", "b"]), -1),
    ("a ' b '", Some(&["a", " b "]), -1),
    ("a '   '", Some(&["a", "   "]), -1),
    ("foo bar\\  ", Some(&["foo", "bar "]), -1),
    ("foo \"\" bar ''", Some(&["foo", "", "bar", ""]), -1),
    ("foo \\\\", Some(&["foo", "\\"]), -1),
    ("foo \\& bar", Some(&["foo", "&", "bar"]), -1),
    ("foo '", None, 0),
    ("foo \"", None, 0),
    ("foo `", None, 0),
    ("echo 🍺; seq 1 10", Some(&["echo", "🍺"]), 6),
    ("ls -la 2>foo", Some(&["ls", "-la"]), 7),
    ("echo $FOO ${BAR}", Some(&["echo", "$FOO", "${BAR}"]), -1),
    ("echo $(date) x", Some(&["echo", "$(date)", "x"]), -1),
    ("echo (x)", None, 0),
    ("echo )", None, 0),
    ("a|b", Some(&["a"]), 1),
    ("a && b", Some(&["a"]), 2),
    ("a < in", Some(&["a"]), 2),
    ("a >out", Some(&["a"]), 2),
    ("\tmyapp\t-x \r\n -y\n", Some(&["myapp", "-x", "-y"]), -1),
    ("   ", Some(&[]), -1),
    (
        "myapp -addr :8080 -name \"my app\"",
        Some(&["myapp", "-addr", ":8080", "-name", "my app"]),
        -1,
    ),
    ("sh -c 'exit 3'", Some(&["sh", "-c", "exit 3"]), -1),
    (
        "sh -c \"echo \\\"hi\\\"; exit 0\"",
        Some(&["sh", "-c", "echo \"hi\"; exit 0"]),
        -1,
    ),
    ("a\\", None, 0),
    ("a 'it''s'", Some(&["a", "its"]), -1),
    ("a \"x\\ny\"", Some(&["a", "xny"]), -1),
    ("a \\'b\\'", Some(&["a", "'b'"]), -1),
    ("a `b c` d", Some(&["a", "`b c`", "d"]), -1),
    ("a \"$(b c)\" d", Some(&["a", "$(b c)", "d"]), -1),
    ("a ';' \";\" \\;", Some(&["a", ";", ";", ";"]), -1),
    ("x=1 myapp", Some(&["x=1", "myapp"]), -1),
    (
        "/usr/bin/env FOO=bar app --flag",
        Some(&["/usr/bin/env", "FOO=bar", "app", "--flag"]),
        -1,
    ),
    ("a \"\\\\\"", Some(&["a", "\\"]), -1),
    ("a 'b\\c'", Some(&["a", "b\\c"]), -1),
    ("é \"ü ö\"", Some(&["é", "ü ö"]), -1),
];

#[test]
fn split_matches_go_shellwords() {
    for &(line, want, position) in GO {
        let got = split(line);
        match want {
            None => assert_eq!(got, Err(Unbalanced), "{line:?}"),
            Some(words) => {
                let got = got.unwrap_or_else(|e| panic!("{line:?}: {e}"));
                assert_eq!(got.args, words, "{line:?}");
                let at = got.stop.map_or(-1, |s| i64::try_from(s.position).unwrap());
                assert_eq!(at, position, "{line:?}");
            }
        }
    }
}

/// Go runs the words before an unquoted operator; the daemon refuses the line instead, and
/// an empty one, which Go indexes and panics on.
#[test]
fn a_command_go_would_run_differently_is_refused() {
    let command = ExecCommand::parse("sh -c 'echo a; exit 3'").unwrap();
    assert_eq!(command.program(), "sh");
    assert_eq!(command.args(), ["-c", "echo a; exit 3"]);
    assert_eq!(
        ExecCommand::parse("myapp; rm -rf /"),
        Err(ExecError::Operator {
            line: "myapp; rm -rf /".to_owned(),
            operator: ';',
            position: 5,
        })
    );
    assert!(matches!(
        ExecCommand::parse("myapp 2>log"),
        Err(ExecError::Operator { operator: '>', .. })
    ));
    assert!(matches!(
        ExecCommand::parse(" \t"),
        Err(ExecError::Empty { .. })
    ));
    assert!(matches!(
        ExecCommand::parse("myapp 'x"),
        Err(ExecError::Unbalanced { .. })
    ));
}

/// Go's rule (`replicate.go:92`, `:152`): an exit of 0 is clean, any other code is not; a
/// death by any signal is clean only in a shutdown begun by a signal, never one the daemon
/// had to kill.
#[test]
fn a_child_end_is_clean_as_go_counts_it() {
    assert!(ChildEnd::Exited(0).clean());
    for code in [1, 3, 7, 255] {
        assert!(!ChildEnd::Exited(code).clean(), "{code}");
    }
    for signal in [6, 9, 15] {
        let end = |during| ChildEnd::Signalled { signal, during };
        assert!(end(During::Shutdown).clean(), "{signal}");
        assert!(!end(During::Run).clean(), "{signal}");
        assert!(!end(During::Kill).clean(), "{signal}");
    }
    assert!(!ChildEnd::DidNotStart.clean());
}

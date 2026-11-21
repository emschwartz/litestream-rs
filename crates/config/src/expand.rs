//! Go's two expansions: environment variables over the raw file text before
//! YAML parsing, and `~` plus `sqlite://` stripping on paths.
//!
//! **Environment.** Litestream runs Go's `os.Expand` over the whole file,
//! comments included, before parsing (`ParseConfig`), with `$PID` mapped to
//! the process id and every other name to `os.Getenv`. Ported exactly:
//! - `$NAME` takes the longest run of ASCII letters, digits and `_`;
//!   `${NAME}` takes everything up to the next `}`.
//! - One shell-special character is a name of its own: `$$`, `$1`, `$?`,
//!   `${1}` all look up that one character, which is never set, so `$$`
//!   becomes nothing (not `$`).
//! - `${}` and an unclosed `${` are dropped; a `$` followed by anything else
//!   (a space, `}`, `-`) or at the very end is kept.
//! - There is no default syntax: `${A:-b}` looks up the name `A:-b`.
//!
//! An unset variable expands to nothing, as in Go; the loader warns once per
//! name. The expansion happens before parsing, so a value that holds `:`, `#`
//! or a newline changes the YAML around it, exactly as it does in Go.
//!
//! **Paths.** Go's `expand` (`cmd/litestream/main.go`): strip one `sqlite3://`
//! or `sqlite://` prefix, replace a leading `~` or `~/` with the home
//! directory, and otherwise make the path absolute against the current
//! directory and clean it (`filepath.Abs`).

use std::path::{Path, PathBuf};

/// Whether environment variables are expanded (Go's `-no-expand-env` turns it off).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expand {
    /// Expand `$NAME`, `${NAME}` and `$PID`, as Go does by default.
    Env,
    /// Read the text as written.
    Off,
}

/// The process facts a load reads; [`ProcessEnv`] is the real one.
pub trait Env {
    /// The variable's value, `None` when unset.
    fn var(&self, name: &str) -> Option<String>;
    /// The process id, for `$PID`.
    fn pid(&self) -> u32;
    /// The home directory, for `~`.
    fn home_dir(&self) -> Option<PathBuf>;
    /// The current directory, for relative paths.
    fn current_dir(&self) -> Option<PathBuf>;
}

/// The running process's environment.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessEnv;

impl Env for ProcessEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }

    fn pid(&self) -> u32 {
        std::process::id()
    }

    fn home_dir(&self) -> Option<PathBuf> {
        std::env::home_dir()
    }

    fn current_dir(&self) -> Option<PathBuf> {
        std::env::current_dir().ok()
    }
}

/// Text after expansion, and the names that were not set (each once, in order).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Expanded {
    pub(crate) text: String,
    pub(crate) unset: Vec<String>,
}

/// Go's `os.Expand(text, mapping)` with Litestream's mapping.
pub(crate) fn expand_env(text: &str, env: &dyn Env) -> Expanded {
    let mut out = String::with_capacity(text.len());
    let mut unset: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find('$') {
        let (before, from) = rest.split_at(at);
        out.push_str(before);
        let after = from.get(1..).unwrap_or("");
        if after.is_empty() {
            // Go expands only a `$` with something after it.
            out.push('$');
            rest = after;
            break;
        }
        let (name, width) = shell_name(after);
        match name {
            None if width > 0 => {}
            None => out.push('$'),
            Some("PID") => out.push_str(&env.pid().to_string()),
            Some(name) => match env.var(name) {
                Some(value) => out.push_str(&value),
                None if unset.iter().any(|seen| seen == name) => {}
                None => unset.push(name.to_owned()),
            },
        }
        rest = after.get(width..).unwrap_or("");
    }
    out.push_str(rest);
    Expanded { text: out, unset }
}

/// Go's `getShellName`: the name after a `$` and how many bytes it spans.
/// `(None, 0)` keeps the `$`; `(None, n)` drops `n` bytes as bad syntax.
fn shell_name(s: &str) -> (Option<&str>, usize) {
    let bytes = s.as_bytes();
    match bytes {
        [b'{', special, b'}', ..] if is_shell_special(*special) => (s.get(1..2), 3),
        [b'{', ..] => match s.find('}') {
            Some(1) => (None, 2),
            Some(close) => (s.get(1..close), close.saturating_add(1)),
            None => (None, 1),
        },
        [first, ..] if is_shell_special(*first) => (s.get(..1), 1),
        _ => {
            let len = bytes
                .iter()
                .take_while(|&&b| b == b'_' || b.is_ascii_alphanumeric())
                .count();
            if len == 0 {
                (None, 0)
            } else {
                (s.get(..len), len)
            }
        }
    }
}

fn is_shell_special(b: u8) -> bool {
    matches!(
        b,
        b'*' | b'#' | b'$' | b'@' | b'!' | b'?' | b'-' | b'0'..=b'9'
    )
}

/// Why a path cannot be expanded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    /// `~` with no home directory to put there.
    #[error("cannot expand ~: no home directory available")]
    NoHomeDirectory,
    /// A relative path with no current directory to resolve it against.
    #[error("cannot make a relative path absolute: no current directory")]
    NoCurrentDirectory,
}

/// Go's `StripSQLitePrefix`: one leading `sqlite3://` or `sqlite://` removed.
#[must_use]
pub fn strip_sqlite_prefix(path: &str) -> &str {
    path.strip_prefix("sqlite3://")
        .or_else(|| path.strip_prefix("sqlite://"))
        .unwrap_or(path)
}

/// Go's `expand`: the prefix stripped, `~` replaced, the result absolute and clean.
///
/// # Errors
/// A [`PathError`] when the home or current directory is needed and unknown.
pub fn expand_path(path: &str, env: &dyn Env) -> Result<PathBuf, PathError> {
    let path = strip_sqlite_prefix(path);
    if path == "~" {
        return env.home_dir().ok_or(PathError::NoHomeDirectory);
    }
    if let Some(rest) = path.strip_prefix("~/") {
        let home = env.home_dir().ok_or(PathError::NoHomeDirectory)?;
        return Ok(PathBuf::from(clean(&join(&home, rest))));
    }
    if path.starts_with('/') {
        return Ok(PathBuf::from(clean(path)));
    }
    let cwd = env.current_dir().ok_or(PathError::NoCurrentDirectory)?;
    Ok(PathBuf::from(clean(&join(&cwd, path))))
}

fn join(base: &Path, rest: &str) -> String {
    format!("{}/{rest}", base.display())
}

/// Go's `filepath.Clean` on Unix: the shortest equivalent path, lexically.
#[must_use]
pub fn clean(path: &str) -> String {
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => match parts.last() {
                Some(&last) if last != ".." => {
                    parts.pop();
                }
                _ if rooted => {}
                _ => parts.push(".."),
            },
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_owned(),
        (false, false) => joined,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Fake(HashMap<&'static str, &'static str>);

    impl Env for Fake {
        fn var(&self, name: &str) -> Option<String> {
            self.0.get(name).map(|v| (*v).to_owned())
        }
        fn pid(&self) -> u32 {
            4242
        }
        fn home_dir(&self) -> Option<PathBuf> {
            Some(PathBuf::from("/home/u"))
        }
        fn current_dir(&self) -> Option<PathBuf> {
            Some(PathBuf::from("/work"))
        }
    }

    fn env() -> Fake {
        Fake(HashMap::from([("A", "x"), ("B_2", "y"), ("EMPTY", "")]))
    }

    /// Each case was run through Go 1.24's `os.Expand` with the same mapping.
    #[test]
    fn expands_as_go_os_expand() {
        let cases = [
            ("$A", "x"),
            ("${A}", "x"),
            ("$A$B_2", "xy"),
            ("${A}b", "xb"),
            ("$Ab", ""),
            ("$PID", "4242"),
            ("${PID}", "4242"),
            ("$$", ""),
            ("$1", ""),
            ("${1}", ""),
            ("${}", ""),
            ("${A", "A"),
            ("$", "$"),
            ("a$", "a$"),
            ("$ x", "$ x"),
            ("$}", "$}"),
            ("${A:-b}", ""),
            ("pa$word", "pa"),
            ("$EMPTY|", "|"),
            ("caf\u{e9} $A", "caf\u{e9} x"),
            ("# $A in a comment", "# x in a comment"),
        ];
        for (text, want) in cases {
            assert_eq!(expand_env(text, &env()).text, want, "{text:?}");
        }
    }

    #[test]
    fn unset_names_are_listed_once() {
        let expanded = expand_env("$NOPE ${NOPE} $A $OTHER $$", &env());
        assert_eq!(expanded.unset, ["NOPE", "OTHER", "$"]);
    }

    #[test]
    fn go_strip_sqlite_prefix_table() {
        let cases = [
            ("sqlite3:///path/to/db.sqlite", "/path/to/db.sqlite"),
            ("sqlite:///path/to/db.sqlite", "/path/to/db.sqlite"),
            ("sqlite3://./data/db.sqlite", "./data/db.sqlite"),
            ("sqlite://./data/db.sqlite", "./data/db.sqlite"),
            ("sqlite3://~/db.sqlite", "~/db.sqlite"),
            ("sqlite://~/db.sqlite", "~/db.sqlite"),
            ("/path/to/db.sqlite", "/path/to/db.sqlite"),
            ("./data/db.sqlite", "./data/db.sqlite"),
            ("~/db.sqlite", "~/db.sqlite"),
            ("", ""),
            ("sqlite3://C:/data/db.sqlite", "C:/data/db.sqlite"),
        ];
        for (input, want) in cases {
            assert_eq!(strip_sqlite_prefix(input), want, "{input}");
        }
    }

    #[test]
    fn paths_expand_as_go() {
        let cases = [
            ("~", "/home/u"),
            ("~/litestream-meta", "/home/u/litestream-meta"),
            ("sqlite://~/db.sqlite", "/home/u/db.sqlite"),
            ("db.sqlite", "/work/db.sqlite"),
            ("./data/../db", "/work/db"),
            ("/path/to/../to/db.sqlite", "/path/to/db.sqlite"),
            ("//a//b/", "/a/b"),
            ("~user/db", "/work/~user/db"),
        ];
        for (input, want) in cases {
            assert_eq!(
                expand_path(input, &env()),
                Ok(PathBuf::from(want)),
                "{input}"
            );
        }
    }

    #[test]
    fn clean_matches_go() {
        let cases = [
            ("", "."),
            ("..", ".."),
            ("../..", "../.."),
            ("a/../..", ".."),
            ("/..", "/"),
            ("/../a", "/a"),
            ("a/./b/", "a/b"),
        ];
        for (input, want) in cases {
            assert_eq!(clean(input), want, "{input}");
        }
    }
}

//! The slice of the AWS shared-file INI dialect the credential chain needs:
//! one profile's `aws_access_key_id`, `aws_secret_access_key` and
//! `aws_session_token`, as the AWS SDK for Go v2 (`internal/ini`) reads them.
//!
//! Like Go: `#` and `;` start a comment line; a `[header]` may carry a trailing
//! comment; a property is `key = value` or `key: value`, its key compared
//! without case and its value trimmed, cut at a ` #` or ` ;` comment and
//! stripped of one pair of matching quotes; repeated sections merge and the
//! last value wins; an indented line is a nested property (ignored) unless the
//! key before it already has a value, when it counts as a property of its own;
//! properties before the first header are ignored.
//!
//! Unlike Go, a line that is none of those is refused rather than skipped. Go
//! skips it, so a header missing its `]` silently files the keys below it under
//! the profile above; the AWS CLI's parser refuses such a file too.
//!
//! The text is borrowed: values are slices of the caller's (zeroized) buffer,
//! so no secret is copied here.

/// Which file the text came from; the two name their sections differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Flavor {
    /// `~/.aws/credentials`: `[name]`; a `[profile name]` section is dropped.
    Credentials,
    /// `~/.aws/config`: `[profile name]`, and `[default]` for the default
    /// profile unless a `[profile default]` exists.
    Config,
}

/// The keys one profile holds, as written (possibly empty).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Keys<'a> {
    pub(super) id: Option<&'a str>,
    pub(super) secret: Option<&'a str>,
    pub(super) token: Option<&'a str>,
}

/// A line that is not blank, a comment, a header or a property; 1-based. The
/// line's text is never kept: it may hold a secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Malformed {
    pub(super) line: usize,
}

/// Which accumulator a section's properties go to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// `[profile name]` in the config file, `[name]` in the credentials file.
    Named,
    /// `[default]` in the config file, used only when no `[profile default]`.
    BareDefault,
    /// Any other section, or before the first header.
    Other,
}

/// What the scan has seen of the wanted profile.
#[derive(Debug, Default)]
struct Found<'a> {
    named: Option<Keys<'a>>,
    bare: Option<Keys<'a>>,
}

impl<'a> Found<'a> {
    fn slot(&mut self, target: Target) -> Option<&mut Keys<'a>> {
        match target {
            Target::Named => Some(self.named.get_or_insert_with(Keys::default)),
            Target::BareDefault => Some(self.bare.get_or_insert_with(Keys::default)),
            Target::Other => None,
        }
    }
}

/// The keys of `profile` in `text`, or `None` when no section names it.
pub(super) fn lookup<'a>(
    text: &'a str,
    flavor: Flavor,
    profile: &str,
) -> Result<Option<Keys<'a>>, Malformed> {
    let mut found = Found::default();
    let mut target = Target::Other;
    // Whether the last property had a value: `None` right after a header.
    let mut last_filled: Option<bool> = None;
    for (index, line) in text.lines().enumerate() {
        let number = index.saturating_add(1);
        let lead = line.trim_start_matches([' ', '\t']);
        if lead.is_empty() || lead.starts_with(['#', ';']) {
            continue;
        }
        if let Some(inner) = header(line) {
            target = classify(inner, flavor, profile);
            // A section that names the profile counts even with no keys.
            found.slot(target);
            last_filled = None;
            continue;
        }
        if lead.len() != line.len() {
            // Indented: a nested property or a continuation, unless orphaned.
            if last_filled != Some(false)
                && let Some((key, value)) = split_property(lead)
            {
                let value = cut_comment(value).trim();
                last_filled = Some(!value.is_empty());
                assign(found.slot(target), key, unquote(value));
            }
            continue;
        }
        let (key, value) = split_property(cut_comment(line)).ok_or(Malformed { line: number })?;
        last_filled = Some(!value.is_empty());
        assign(found.slot(target), key, unquote(value));
    }
    Ok(found.named.or(found.bare))
}

fn assign<'a>(slot: Option<&mut Keys<'a>>, key: &str, value: &'a str) {
    let Some(keys) = slot else { return };
    if key.eq_ignore_ascii_case("aws_access_key_id") {
        keys.id = Some(value);
    } else if key.eq_ignore_ascii_case("aws_secret_access_key") {
        keys.secret = Some(value);
    } else if key.eq_ignore_ascii_case("aws_session_token") {
        keys.token = Some(value);
    }
}

/// The trimmed inside of a `[header]` line, its trailing comment cut.
fn header(line: &str) -> Option<&str> {
    let line = line.split(['#', ';']).next().unwrap_or(line).trim();
    Some(line.strip_prefix('[')?.strip_suffix(']')?.trim())
}

fn classify(inner: &str, flavor: Flavor, profile: &str) -> Target {
    // Go splits a header at its first run of blanks into a type and a name.
    let (kind, name) = match inner.split_once([' ', '\t']) {
        Some((kind, rest)) => (Some(kind), rest.trim_start_matches([' ', '\t'])),
        None => (None, inner),
    };
    match (flavor, kind) {
        (Flavor::Config, Some("profile")) if name == profile => Target::Named,
        (Flavor::Config, None) if name == "default" && profile == "default" => Target::BareDefault,
        (Flavor::Credentials, None) if name == profile => Target::Named,
        (Flavor::Credentials, Some(kind)) if kind != "profile" => {
            let joined = profile
                .strip_prefix(kind)
                .and_then(|rest| rest.strip_prefix(' '));
            if joined == Some(name) {
                Target::Named
            } else {
                Target::Other
            }
        }
        (Flavor::Config | Flavor::Credentials, Some(_) | None) => Target::Other,
    }
}

/// `key = value` or `key: value`, whichever separator comes first, both
/// sides trimmed.
fn split_property(line: &str) -> Option<(&str, &str)> {
    let at = line.find(['=', ':'])?;
    let (key, rest) = line.split_at(at);
    Some((key.trim(), rest.get(1..)?.trim()))
}

/// The text before a comment that a blank introduces.
fn cut_comment(text: &str) -> &str {
    [" #", " ;", "\t#", "\t;"]
        .iter()
        .fold(text, |text, mark| text.split(mark).next().unwrap_or(text))
}

fn unquote(value: &str) -> &str {
    ['"', '\'']
        .iter()
        .find_map(|quote| value.strip_prefix(*quote)?.strip_suffix(*quote))
        .unwrap_or(value)
}

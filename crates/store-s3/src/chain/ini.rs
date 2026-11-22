//! The AWS shared-file INI dialect as the AWS SDK for Go v2 reads it
//! (`internal/ini` v1.8.4, the version Go Litestream 041a8c3 links), line for line:
//!
//! - lines are split at `\n` only; a blank line or one whose first non-blank
//!   character is `#` or `;` is skipped;
//! - a header is a line that, cut at its first `#` and then its first `;` and
//!   trimmed, is `[...]`; its inside, trimmed, splits at the first run of blanks
//!   into a type and a name, rejoined with one space (`[ profile   a ]` is
//!   `profile a`); a header resets the current key and creates its section once,
//!   so repeated sections merge;
//! - a line that starts with neither a space nor a tab is a property: cut at the
//!   first ` #`, ` ;`, tab-`#` or tab-`;`, right-trimmed, split at `=` or `:`,
//!   whichever comes first, both sides trimmed, the key lower-cased and the value
//!   stripped of one pair of matching quotes; it replaces an earlier value;
//! - an indented line with a separator is a sub-property of the current key when
//!   that key's value is empty (its comment is part of its value), and otherwise is
//!   promoted to a property of its own, comment cut;
//! - an indented line without one continues the current key's value with a
//!   newline, when that value is a non-empty string;
//! - properties before the first header, or under `[]`, are dropped.
//!
//! Unlike Go, a line that is none of those is refused rather than skipped
//! (`docs/decisions.md` D43). Go skips it, so a header missing its `]` silently
//! files the keys below it under the profile above; the AWS CLI's parser refuses
//! such a file too.
//!
//! Each section also records whether it holds exactly one of the key id and the
//! secret: the SDK marks that "partial credentials" error on the section when it
//! loads a file (`mergeSections` in `config/shared_config.go`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use zeroize::Zeroizing;

/// The key id's key.
pub(super) const ACCESS_KEY_ID: &str = "aws_access_key_id";
/// The secret's key.
pub(super) const SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
/// The session token's key.
pub(super) const SESSION_TOKEN: &str = "aws_session_token";

/// A line that is not blank, a comment, a header or a property; 1-based. The
/// line's text is never kept: it may hold a secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Malformed {
    pub(super) line: usize,
}

/// One property's value: a string, or sub-properties (whose string is empty).
#[derive(Debug, Default)]
struct Value {
    text: Zeroizing<String>,
    map: bool,
}

/// One section's properties, keys lower-cased.
#[derive(Debug, Default)]
pub(super) struct Section {
    values: BTreeMap<String, Value>,
    /// The file whose copy of this section holds only one of the key id and
    /// secret, when the copy that counts does (the SDK's section error).
    pub(super) partial: Option<PathBuf>,
    /// The file the key id and secret were taken from.
    pub(super) keys_from: Option<PathBuf>,
}

impl Section {
    /// Whether the key is present, even with an empty value.
    pub(super) fn has(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    /// The key's string value; empty when absent or holding sub-properties.
    pub(super) fn string(&self, key: &str) -> &str {
        self.values.get(key).map_or("", |value| value.text.as_str())
    }

    /// Sets a key to a string value, as the SDK's `UpdateValue`.
    pub(super) fn set(&mut self, key: &str, text: &str) {
        let value = Value {
            text: Zeroizing::new(text.to_owned()),
            map: false,
        };
        self.values.insert(key.to_owned(), value);
    }
}

/// The sections of one file, by name.
pub(super) type Sections = BTreeMap<String, Section>;

/// The parser's state: Go's `csection` and `ckey`.
#[derive(Default)]
struct Parser {
    sections: Sections,
    section: Option<String>,
    key: Option<String>,
}

/// Parses one file's text.
pub(super) fn parse(text: &str, path: &Path) -> Result<Sections, Malformed> {
    let mut parser = Parser::default();
    for (index, line) in text.split('\n').enumerate() {
        if line.trim().is_empty() || is_comment(line) {
            continue;
        }
        if let Some(name) = as_profile(line) {
            parser.profile(name);
        } else if !starts_blank(line) {
            let (key, value) = as_property(line).ok_or(Malformed {
                line: index.saturating_add(1),
            })?;
            parser.property(key, value);
        } else {
            let lead = line.trim_start_matches([' ', '\t']);
            match split_property(lead) {
                Some((key, value)) => parser.sub_property(key, unquote(value)),
                None => parser.continuation(lead),
            }
        }
    }
    for section in parser.sections.values_mut() {
        if section.has(ACCESS_KEY_ID) != section.has(SECRET_ACCESS_KEY) {
            section.partial = Some(path.to_owned());
        }
        if section.has(ACCESS_KEY_ID) && section.has(SECRET_ACCESS_KEY) {
            section.keys_from = Some(path.to_owned());
        }
    }
    Ok(parser.sections)
}

impl Parser {
    fn profile(&mut self, name: String) {
        self.key = None;
        self.sections.entry(name.clone()).or_default();
        self.section = Some(name);
    }

    fn current(&mut self) -> Option<&mut Section> {
        let name = self.section.as_ref().filter(|name| !name.is_empty())?;
        self.sections.get_mut(name)
    }

    fn property(&mut self, key: String, value: &str) {
        let Some(section) = self.current() else {
            return;
        };
        section.set(&key, value);
        // Go's empty `ckey` means no current key.
        self.key = (!key.is_empty()).then_some(key);
    }

    fn sub_property(&mut self, key: String, value: &str) {
        let current = self.key.clone();
        let Some(section) = self.current() else {
            return;
        };
        let parent = current
            .as_deref()
            .filter(|parent| section.string(parent).is_empty());
        match parent.and_then(|parent| section.values.get_mut(parent)) {
            Some(parent) => {
                // The sub-property's own key and value are never read here.
                parent.text = Zeroizing::new(String::new());
                parent.map = true;
            }
            None => {
                let promoted = trim_property_comment(value).trim().to_owned();
                self.property(key, &promoted);
            }
        }
    }

    fn continuation(&mut self, text: &str) {
        let Some(key) = self.key.clone() else {
            return;
        };
        let Some(section) = self.current() else {
            return;
        };
        if let Some(value) = section.values.get_mut(&key)
            && !value.text.is_empty()
            && !value.map
        {
            let joined = format!("{}\n{text}", value.text.as_str());
            value.text = Zeroizing::new(joined);
        }
    }
}

fn starts_blank(line: &str) -> bool {
    line.starts_with([' ', '\t'])
}

fn is_comment(line: &str) -> bool {
    line.trim_start_matches([' ', '\t']).starts_with(['#', ';'])
}

/// Go's `asProfile`: the section name a header line gives.
fn as_profile(line: &str) -> Option<String> {
    let cut = line.split('#').next().unwrap_or(line);
    let cut = cut.split(';').next().unwrap_or(cut).trim();
    let inner = cut.strip_prefix('[')?.strip_suffix(']')?.trim();
    Some(match inner.split_once([' ', '\t']) {
        Some((kind, rest)) => format!("{kind} {}", rest.trim_start_matches([' ', '\t'])),
        None => inner.to_owned(),
    })
}

/// Go's `asProperty` on a line that does not start blank.
fn as_property(line: &str) -> Option<(String, &str)> {
    let trimmed = trim_property_comment(line).trim_end_matches([' ', '\t']);
    let (key, value) = split_property(trimmed)?;
    Some((key, unquote(value)))
}

/// Go's `splitProperty`: at `=`, or at `:` when it comes first; the key
/// lower-cased.
fn split_property(line: &str) -> Option<(String, &str)> {
    let at = match (line.find('='), line.find(':')) {
        (Some(equals), Some(colon)) => equals.min(colon),
        (Some(at), None) | (None, Some(at)) => at,
        (None, None) => return None,
    };
    let (key, rest) = line.split_at(at);
    Some((key.trim().to_lowercase(), rest.get(1..)?.trim()))
}

fn trim_property_comment(text: &str) -> &str {
    [" #", " ;", "\t#", "\t;"]
        .iter()
        .fold(text, |text, mark| text.split(mark).next().unwrap_or(text))
}

/// Go's `legacyStrconv`: one pair of matching quotes removed.
fn unquote(value: &str) -> &str {
    ['"', '\'']
        .iter()
        .find_map(|quote| value.strip_prefix(*quote)?.strip_suffix(*quote))
        .unwrap_or(value)
}

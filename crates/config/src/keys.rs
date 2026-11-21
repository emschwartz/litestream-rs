//! Every key Go's `Config` defines at 041a8c3 (`cmd/litestream/main.go`,
//! `server.go`, `internal/log.go`), section by section, with what Go reads it
//! as and how v1 treats it (docs/plans/phase-4-sidecar.md, the config ruling):
//! implemented, inert unless asked for, or a knob with no effect. One table
//! per Go struct; `ReplicaSettings` is inlined both at the top level and in
//! each replica, as Go inlines it.
//!
//! Classification calls beyond the plan's list, each with its reason:
//! - `sse-*` and `storage-class` are inert (refused when set), not no-effect:
//!   ignoring them would write unencrypted objects, or objects in another
//!   storage class, where the operator asked otherwise (Go itself refuses
//!   `age` for the same reason).
//! - Keys only another backend reads (`account-name`, `host`, `jwt`, ...) are
//!   accepted at any value: on an `s3` or `file` replica they do nothing in
//!   Go either, and every other replica type is refused.
//! - `socket.path`, `socket.permissions` and `heartbeat-interval` are
//!   accepted at any value: they matter only with the socket or a heartbeat
//!   URL, which are refused.
//! - Top-level `part-size` has no effect: Go's `SetDefaults` does not copy it
//!   into replicas, so it does nothing in Go either.

use crate::error::KeyClass;

/// A Go struct with keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Section {
    Top,
    Snapshot,
    Retention,
    Socket,
    Validation,
    Logging,
    Level,
    Db,
    Replica,
    Age,
    Rust,
    Responses,
}

/// What Go reads a key's value as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Read {
    Str,
    /// A string that is never shown in a message.
    Secret,
    Bool,
    Duration,
    Int,
    Uint32,
    Bytes,
    List,
    /// Go's `interface{}`: anything.
    Any,
    Map(Section),
    Maps(Section),
}

/// The values at which an inert key asks for nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Inert {
    /// `false` or null.
    False,
    /// Empty or null.
    Empty,
    /// Null only.
    Null,
    /// An empty list or null.
    EmptyList,
    /// Any value: the key matters only with something else that is refused.
    Any,
}

/// How v1 treats a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rule {
    Implemented,
    /// Inert at these values; otherwise this feature is not in v1.
    Inert(Inert, &'static str),
    /// Accepted with a warning saying why it does nothing.
    NoEffect(&'static str),
}

/// One key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Key {
    pub(crate) name: &'static str,
    pub(crate) read: Read,
    pub(crate) rule: Rule,
}

impl Key {
    pub(crate) fn class(&self) -> KeyClass {
        match self.rule {
            Rule::Implemented => KeyClass::Implemented,
            Rule::Inert(..) => KeyClass::Inert,
            Rule::NoEffect(_) => KeyClass::NoEffect,
        }
    }

    pub(crate) fn secret(&self) -> bool {
        self.read == Read::Secret
    }
}

const fn key(name: &'static str, read: Read, rule: Rule) -> Key {
    Key { name, read, rule }
}

use Read::{Any, Bool, Bytes, Duration, Int, List, Map, Maps, Secret, Str, Uint32};
use Rule::{Implemented, NoEffect};

const fn inert(name: &'static str, read: Read, at: Inert, feature: &'static str) -> Key {
    key(name, read, Rule::Inert(at, feature))
}

const fn other_backend(name: &'static str, read: Read) -> Key {
    inert(
        name,
        read,
        Inert::Any,
        "a replica backend other than s3 and file",
    )
}

const MD5: &str = "v1 always sends Content-MD5 on uploads";
const SIGNING: &str = "v1's S3 client signs requests one fixed way";
const SSE: &str = "server-side encryption";

/// Go's `ReplicaSettings`, inlined at the top level and in every replica.
const SETTINGS: &[Key] = &[
    key("sync-interval", Duration, Implemented),
    inert(
        "validation-interval",
        Duration,
        Inert::Null,
        "periodic validation",
    ),
    key("max-sync-ltx-files", Int, Implemented),
    inert("auto-recover", Bool, Inert::False, "auto-recover"),
    key("access-key-id", Secret, Implemented),
    key("secret-access-key", Secret, Implemented),
    key("region", Str, Implemented),
    key("bucket", Str, Implemented),
    key("endpoint", Str, Implemented),
    key("force-path-style", Bool, Implemented),
    key("sign-payload", Bool, NoEffect(SIGNING)),
    key("sign-accept-encoding", Bool, NoEffect(SIGNING)),
    key("require-content-md5", Bool, NoEffect(MD5)),
    key("put-object-content-md5", Bool, NoEffect(MD5)),
    key(
        "skip-verify",
        Bool,
        NoEffect("v1 always verifies TLS certificates"),
    ),
    inert("storage-class", Str, Inert::Empty, "an S3 storage class"),
    key("part-size", Bytes, Implemented),
    key(
        "concurrency",
        Int,
        NoEffect("v1 uploads one part at a time"),
    ),
    inert("sse-customer-algorithm", Str, Inert::Empty, SSE),
    inert("sse-customer-key", Secret, Inert::Empty, SSE),
    inert("sse-customer-key-path", Str, Inert::Empty, SSE),
    inert("sse-kms-key-id", Str, Inert::Empty, SSE),
    other_backend("account-name", Str),
    other_backend("account-key", Secret),
    other_backend("sas-token", Secret),
    other_backend("host", Str),
    other_backend("user", Str),
    other_backend("password", Secret),
    other_backend("key-path", Str),
    other_backend("concurrent-writes", Bool),
    other_backend("host-key", Str),
    other_backend("webdav-url", Str),
    other_backend("webdav-username", Str),
    other_backend("webdav-password", Secret),
    other_backend("jwt", Secret),
    other_backend("seed", Secret),
    other_backend("creds", Str),
    other_backend("nkey", Secret),
    other_backend("username", Str),
    other_backend("token", Secret),
    other_backend("tls", Bool),
    other_backend("root-cas", List),
    other_backend("client-cert", Str),
    other_backend("client-key", Str),
    other_backend("max-reconnects", Int),
    other_backend("reconnect-wait", Duration),
    other_backend("timeout", Duration),
    inert("age", Map(Section::Age), Inert::Any, "age encryption"),
];

/// Go's `Config`, before the inlined settings; searched first, so its
/// `part-size` wins over the settings' one.
const TOP: &[Key] = &[
    key("addr", Str, Implemented),
    inert(
        "socket",
        Map(Section::Socket),
        Inert::Any,
        "the control socket",
    ),
    key("levels", Maps(Section::Level), Implemented),
    key("snapshot", Map(Section::Snapshot), Implemented),
    inert(
        "validation",
        Map(Section::Validation),
        Inert::Any,
        "periodic validation",
    ),
    key("l0-retention", Duration, Implemented),
    key("l0-retention-check-interval", Duration, Implemented),
    inert(
        "verify-compaction",
        Bool,
        Inert::False,
        "compaction verification",
    ),
    key("retention", Map(Section::Retention), Implemented),
    inert("heartbeat-url", Str, Inert::Empty, "heartbeats"),
    inert("heartbeat-interval", Duration, Inert::Any, "heartbeats"),
    key("dbs", Maps(Section::Db), Implemented),
    key("exec", Str, Implemented),
    key("logging", Map(Section::Logging), Implemented),
    inert("mcp-addr", Str, Inert::Empty, "the MCP server"),
    key("shutdown-sync-timeout", Duration, Implemented),
    key(
        "shutdown-sync-interval",
        Duration,
        NoEffect("v1's final sync retries on its own schedule"),
    ),
    key(
        "shutdown-timeout",
        Any,
        NoEffect("v1 bounds shutdown by shutdown-sync-timeout alone"),
    ),
    key(
        "part-size",
        Bytes,
        NoEffect("Go does not copy a top-level part-size into replicas"),
    ),
    key("litestream-rs", Map(Section::Rust), Implemented),
];

const SNAPSHOT: &[Key] = &[
    key("interval", Duration, Implemented),
    key("retention", Duration, Implemented),
];

const RETENTION: &[Key] = &[key("enabled", Bool, Implemented)];

const SOCKET: &[Key] = &[
    inert("enabled", Bool, Inert::False, "the control socket"),
    inert("path", Str, Inert::Any, "the control socket"),
    inert("permissions", Uint32, Inert::Any, "the control socket"),
];

const VALIDATION: &[Key] = &[inert(
    "interval",
    Duration,
    Inert::Null,
    "periodic validation",
)];

const LOGGING: &[Key] = &[
    key("level", Str, Implemented),
    key("type", Str, Implemented),
    key("stderr", Bool, Implemented),
    key(
        "source",
        Bool,
        NoEffect("v1's log lines carry no source location"),
    ),
];

const LEVEL: &[Key] = &[key("interval", Duration, Implemented)];

const DB: &[Key] = &[
    key("path", Str, Implemented),
    key("dir", Str, Implemented),
    key("pattern", Str, Implemented),
    key("recursive", Bool, Implemented),
    inert(
        "watch",
        Bool,
        Inert::False,
        "watching a directory for new databases",
    ),
    key("snapshot", Map(Section::Snapshot), Implemented),
    key("meta-path", Str, Implemented),
    key("meta-dir", Str, Implemented),
    key("monitor-interval", Duration, Implemented),
    key("checkpoint-interval", Duration, Implemented),
    key(
        "busy-timeout",
        Duration,
        NoEffect("v1's connections run with busy timeout zero"),
    ),
    key("min-checkpoint-page-count", Int, Implemented),
    key("truncate-page-n", Int, Implemented),
    key("max-sync-wal-bytes", Int, Implemented),
    key("restore-if-db-not-exists", Bool, Implemented),
    key("replica", Map(Section::Replica), Implemented),
    key("replicas", Maps(Section::Replica), Implemented),
];

/// Go's `ReplicaConfig`, before the inlined settings.
const REPLICA: &[Key] = &[
    key("type", Str, Implemented),
    key(
        "name",
        Str,
        NoEffect("replica names were for v0.3's multiple replicas"),
    ),
    key("path", Str, Implemented),
    key("url", Str, Implemented),
];

const AGE: &[Key] = &[
    inert("identities", List, Inert::EmptyList, "age encryption"),
    inert("recipients", List, Inert::EmptyList, "age encryption"),
];

/// The one key Go does not define (docs/plans/phase-4-sidecar.md, ruling on
/// Rust-only settings): Go ignores it, so the same file still starts Go.
const RUST: &[Key] = &[
    key("host-readers", Str, Implemented),
    key("host-autocheckpoint", Str, Implemented),
    key("responses", Map(Section::Responses), Implemented),
];

/// One per field of the replicator's responses.
#[cfg(test)]
pub(crate) const RESPONSE_KEYS: &[&str] = &[
    "credentials",
    "store-refused",
    "remote-corrupt",
    "remote-regressed",
    "wal-corrupt",
    "wal-truncated",
    "wal-unlinked",
    "page-size-changed",
    "bug",
    "foreign-writer",
];

const RESPONSES: &[Key] = &[
    key("credentials", Str, Implemented),
    key("store-refused", Str, Implemented),
    key("remote-corrupt", Str, Implemented),
    key("remote-regressed", Str, Implemented),
    key("wal-corrupt", Str, Implemented),
    key("wal-truncated", Str, Implemented),
    key("wal-unlinked", Str, Implemented),
    key("page-size-changed", Str, Implemented),
    key("bug", Str, Implemented),
    key("foreign-writer", Str, Implemented),
];

/// The tables a section's keys come from, searched in order.
fn tables(section: Section) -> [&'static [Key]; 2] {
    match section {
        Section::Top => [TOP, SETTINGS],
        Section::Replica => [REPLICA, SETTINGS],
        Section::Snapshot => [SNAPSHOT, &[]],
        Section::Retention => [RETENTION, &[]],
        Section::Socket => [SOCKET, &[]],
        Section::Validation => [VALIDATION, &[]],
        Section::Logging => [LOGGING, &[]],
        Section::Level => [LEVEL, &[]],
        Section::Db => [DB, &[]],
        Section::Age => [AGE, &[]],
        Section::Rust => [RUST, &[]],
        Section::Responses => [RESPONSES, &[]],
    }
}

/// The key `name` in `section`.
pub(crate) fn find(section: Section, name: &str) -> Option<&'static Key> {
    tables(section)
        .into_iter()
        .flatten()
        .find(|key| key.name == name)
}

/// The section's key nearest `name`, within two edits.
pub(crate) fn suggest(section: Section, name: &str) -> Option<&'static str> {
    let mut best: Option<(usize, &'static str)> = None;
    for key in tables(section).into_iter().flatten() {
        let distance = edit_distance(name, key.name);
        if distance <= 2 && best.is_none_or(|(d, _)| distance < d) {
            best = Some((distance, key.name));
        }
    }
    best.map(|(_, name)| name)
}

/// Levenshtein distance over characters, ignoring ASCII case.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().map(|c| c.to_ascii_lowercase()).collect();
    let b: Vec<char> = b.chars().map(|c| c.to_ascii_lowercase()).collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut current = vec![i.saturating_add(1)];
        for (j, cb) in b.iter().enumerate() {
            let substitute = previous
                .get(j)
                .copied()
                .unwrap_or(usize::MAX)
                .saturating_add(usize::from(ca != cb));
            let delete = previous
                .get(j.saturating_add(1))
                .copied()
                .unwrap_or(usize::MAX)
                .saturating_add(1);
            let insert = current
                .last()
                .copied()
                .unwrap_or(usize::MAX)
                .saturating_add(1);
            current.push(substitute.min(delete).min(insert));
        }
        previous = current;
    }
    previous.last().copied().unwrap_or(0)
}

/// Every section, for tests that walk them all.
#[cfg(test)]
pub(crate) const SECTIONS: &[Section] = &[
    Section::Top,
    Section::Snapshot,
    Section::Retention,
    Section::Socket,
    Section::Validation,
    Section::Logging,
    Section::Level,
    Section::Db,
    Section::Replica,
    Section::Age,
    Section::Rust,
    Section::Responses,
];

/// The keys of a section, for tests.
#[cfg(test)]
pub(crate) fn names(section: Section) -> Vec<&'static Key> {
    tables(section).into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggestions() {
        assert_eq!(
            suggest(Section::Replica, "sync_interval"),
            Some("sync-interval")
        );
        assert_eq!(suggest(Section::Top, "addres"), Some("addr"));
        assert_eq!(suggest(Section::Logging, "lvl"), Some("level"));
        assert_eq!(suggest(Section::Snapshot, "intervall"), Some("interval"));
        assert_eq!(suggest(Section::Top, "definitely-not-a-setting"), None);
        assert_eq!(suggest(Section::Db, "Path"), Some("path"));
    }

    #[test]
    fn response_keys_are_the_responses_table() {
        let table: Vec<&str> = RESPONSES.iter().map(|k| k.name).collect();
        assert_eq!(table, RESPONSE_KEYS);
    }

    #[test]
    fn no_section_names_a_key_twice_except_the_top_level_part_size() {
        for &section in SECTIONS {
            let keys = names(section);
            for (at, key) in keys.iter().enumerate() {
                let again = keys.iter().skip(at + 1).any(|k| k.name == key.name);
                let allowed = section == Section::Top && key.name == "part-size";
                assert!(!again || allowed, "{section:?} {}", key.name);
            }
        }
    }
}

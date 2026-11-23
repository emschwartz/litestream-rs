//! The obligation ledger: what each field of the engine's long-lived state is, with respect to
//! a restart of the process (issue #171, "an obligation held only in memory").
//!
//! A field is one of four kinds:
//! - [`Keep::Derived`]: rebuilt at every start from something durable (staging, the replica, the
//!   WAL, the caller's configuration), so a restart loses nothing by dropping it.
//! - [`Keep::Journalled`]: an obligation the process must not forget, held in a durable local
//!   record ([`Record`]) and read back at every start.
//! - [`Keep::Ephemeral`]: a report, a schedule or a request about this process alone, whose loss
//!   at a restart costs nothing the reason does not name.
//! - [`Keep::Owed`]: an obligation that a restart forgets today, with the work that will journal
//!   it. A known gap, named rather than hidden among the ephemeral ones.
//!
//! Each struct's ledger is a closed world: [`unclassified`] holds it to the struct's source, so a
//! field added without a ledger entry, or an entry left after its field is gone, fails a test in
//! the crate that owns the struct.

/// A durable local record a journalled field is held in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Record {
    /// The engine journal (`docs/journal.md`): written by the actor's end-of-tick reconcile and
    /// read by its boot, so the guard requires both to name the field.
    EngineJournal,
    /// The takeover record in the meta directory, written and read by the driver
    /// (`crate::takeover`).
    TakeoverRecord,
}

/// What a field is to a restart, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keep {
    /// Rebuilt at every start from something durable.
    Derived(&'static str),
    /// Held in a durable record and read back at every start.
    Journalled(Record, &'static str),
    /// About this process alone; its loss costs what the reason says.
    Ephemeral(&'static str),
    /// An obligation a restart forgets today, and the work that will journal it.
    Owed(&'static str),
}

impl Keep {
    /// The reason given.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Derived(why)
            | Self::Journalled(_, why)
            | Self::Ephemeral(why)
            | Self::Owed(why) => why,
        }
    }
}

/// The field names of `struct name { .. }` in `source`, in order: the lines between the item's
/// opening line and its closing brace that declare a field, comments and attributes skipped.
/// Empty when the item is not found, which a caller's test treats as a failure.
#[must_use]
pub fn fields_of(source: &str, name: &str) -> Vec<String> {
    let opening = format!("struct {name} {{");
    let mut lines = source.lines();
    if !lines.any(|line| line.trim_start().contains(&opening)) {
        return Vec::new();
    }
    lines
        .take_while(|line| line.trim_end() != "}")
        .filter_map(|line| field_name(line.trim()))
        .collect()
}

/// The name a field line declares: `name: Type,` after an optional visibility. A comment or an
/// attribute declares none, since what comes before its first colon is not one identifier.
fn field_name(line: &str) -> Option<String> {
    let line = line
        .strip_prefix("pub(crate) ")
        .or_else(|| line.strip_prefix("pub(super) "))
        .or_else(|| line.strip_prefix("pub "))
        .unwrap_or(line);
    let (name, _ty) = line.split_once(':')?;
    let ident = name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    (ident && !name.is_empty() && line.ends_with(',')).then(|| name.to_owned())
}

/// What is wrong with `ledger` against the fields of `struct name` in `source`: fields without
/// an entry, entries without a field, entries named twice, and empty reasons. Empty when the
/// ledger is the struct's closed world.
#[must_use]
pub fn unclassified(source: &str, name: &str, ledger: &[(&str, Keep)]) -> Vec<String> {
    let fields = fields_of(source, name);
    let mut wrong = Vec::new();
    if fields.is_empty() {
        wrong.push(format!("struct {name}: not found, or has no fields"));
    }
    for field in &fields {
        if !ledger.iter().any(|(entry, _)| entry == field) {
            wrong.push(format!("{name}.{field}: no ledger entry"));
        }
    }
    for (index, (entry, keep)) in ledger.iter().enumerate() {
        if !fields.iter().any(|field| field == entry) {
            wrong.push(format!("{name}.{entry}: an entry with no such field"));
        }
        if ledger
            .iter()
            .take(index)
            .any(|(earlier, _)| earlier == entry)
        {
            wrong.push(format!("{name}.{entry}: named twice"));
        }
        if keep.reason().trim().is_empty() {
            wrong.push(format!("{name}.{entry}: no reason"));
        }
    }
    wrong
}

#[cfg(test)]
mod tests;

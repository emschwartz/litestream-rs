//! The walk over the node tree: every key looked up in its section's table,
//! its value read as Go reads it, and the key's class applied. Unknown keys,
//! type errors and off-only keys asked to do something become problems; no-effect
//! keys and bare-number durations become warnings. What survives is a
//! [`Fields`] per mapping, typed but not yet interpreted.

use crate::duration::GoDuration;
use crate::error::{Expected, KeyClass, KeyPath, Problem, ProblemKind, Warning, WarningKind};
use crate::keys::{self, Key, OffValue, Read, Rule, Section};
use crate::value::{self, found};
use crate::yaml::{Node, NodeKind};

/// A value as Go's type for its key reads it. `Null` is the key present with
/// a null value, which differs from the key absent only for `levels`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    Null,
    Str(String),
    Bool(bool),
    Int(i64),
    Uint(u32),
    Duration(GoDuration),
    Bytes(u64),
    List(Vec<String>),
    Any,
    Map(Fields),
    Maps(Vec<Fields>),
}

/// One key read from a mapping.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Field {
    pub(crate) name: &'static str,
    pub(crate) line: usize,
    pub(crate) value: Value,
}

/// A mapping's keys, read.
#[derive(Clone, Debug, PartialEq, Default)]
pub(crate) struct Fields {
    pub(crate) path: KeyPath,
    pub(crate) line: Option<usize>,
    pub(crate) fields: Vec<Field>,
}

/// What `levels` and `dbs` hold, where absent and null differ.
pub(crate) enum Listed<'a> {
    Absent,
    Null,
    Items(&'a [Fields]),
}

impl Fields {
    pub(crate) fn get(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub(crate) fn line(&self, name: &str) -> Option<usize> {
        self.get(name).map(|f| f.line).or(self.line)
    }

    pub(crate) fn string(&self, name: &str) -> Option<String> {
        match self.get(name).map(|f| &f.value) {
            Some(Value::Str(text)) => Some(text.clone()),
            _ => None,
        }
    }

    pub(crate) fn boolean(&self, name: &str) -> Option<bool> {
        match self.get(name).map(|f| &f.value) {
            Some(Value::Bool(value)) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn int(&self, name: &str) -> Option<i64> {
        match self.get(name).map(|f| &f.value) {
            Some(Value::Int(value)) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn duration(&self, name: &str) -> Option<GoDuration> {
        match self.get(name).map(|f| &f.value) {
            Some(Value::Duration(value)) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn bytes(&self, name: &str) -> Option<u64> {
        match self.get(name).map(|f| &f.value) {
            Some(Value::Bytes(value)) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn map(&self, name: &str) -> Option<&Fields> {
        match self.get(name).map(|f| &f.value) {
            Some(Value::Map(fields)) => Some(fields),
            _ => None,
        }
    }

    pub(crate) fn maps(&self, name: &str) -> Listed<'_> {
        match self.get(name).map(|f| &f.value) {
            None => Listed::Absent,
            Some(Value::Maps(items)) => Listed::Items(items),
            Some(_) => Listed::Null,
        }
    }
}

/// The walk's findings.
#[derive(Default)]
pub(crate) struct Walk {
    pub(crate) problems: Vec<Problem>,
    pub(crate) warnings: Vec<Warning>,
    pub(crate) keys: Vec<(KeyPath, KeyClass)>,
}

impl Walk {
    pub(crate) fn problem(&mut self, path: KeyPath, line: Option<usize>, kind: ProblemKind) {
        self.problems.push(Problem { path, line, kind });
    }

    pub(crate) fn warn(&mut self, path: KeyPath, line: Option<usize>, kind: WarningKind) {
        self.warnings.push(Warning { path, line, kind });
    }

    /// The top-level mapping; `None` (an empty file) is an empty one.
    pub(crate) fn top(&mut self, root: Option<&Node>) -> Fields {
        match root {
            None => Fields::default(),
            Some(node) => self.mapping(node, Section::Top, &KeyPath::root()),
        }
    }

    fn mapping(&mut self, node: &Node, section: Section, path: &KeyPath) -> Fields {
        let mut fields = Fields {
            path: path.clone(),
            line: Some(node.line),
            fields: Vec::new(),
        };
        let entries = match &node.kind {
            NodeKind::Map(entries) => entries,
            NodeKind::Scalar { .. } | NodeKind::Seq(_) => {
                if !node.is_null() {
                    let found = found(node, false);
                    self.problem(
                        path.clone(),
                        Some(node.line),
                        ProblemKind::WrongType {
                            expected: Expected::Mapping,
                            found,
                        },
                    );
                }
                return fields;
            }
        };
        for entry in entries {
            let key_path = path.key(&entry.key);
            let Some(key) = keys::find(section, &entry.key) else {
                self.keys.push((key_path.clone(), KeyClass::Unknown));
                let suggestion = keys::suggest(section, &entry.key);
                self.problem(
                    key_path,
                    Some(entry.line),
                    ProblemKind::Unknown { suggestion },
                );
                continue;
            };
            self.keys.push((key_path.clone(), key.class()));
            if let Some(value) = self.value(key, &entry.value, &key_path) {
                self.apply_rule(key, &value, &key_path, entry.line);
                fields.fields.push(Field {
                    name: key.name,
                    line: entry.line,
                    value,
                });
            }
        }
        fields
    }

    fn value(&mut self, key: &Key, node: &Node, path: &KeyPath) -> Option<Value> {
        let secret = key.secret();
        let line = Some(node.line);
        let read = match key.read {
            Read::Str | Read::Secret => value::read_str(node, secret).map(|v| v.map(Value::Str)),
            Read::StrPtr => value::read_str_ptr(node, secret).map(|v| v.map(Value::Str)),
            Read::Bool => value::read_bool(node, secret).map(|v| v.map(Value::Bool)),
            Read::Int => value::read_int(node, secret).map(|v| v.map(Value::Int)),
            Read::Uint32 => value::read_u32(node, secret).map(|v| v.map(Value::Uint)),
            Read::Bytes => value::read_bytes(node, secret).map(|v| v.map(Value::Bytes)),
            Read::List => value::read_list(node, secret).map(|v| v.map(Value::List)),
            Read::Any => Ok(Some(Value::Any)),
            Read::Duration => value::read_duration(node, secret).map(|v| {
                v.map(|(d, bare)| {
                    if bare && d != GoDuration::ZERO {
                        self.warn(
                            path.clone(),
                            line,
                            WarningKind::BareNumberDuration { value: d },
                        );
                    }
                    Value::Duration(d)
                })
            }),
            Read::Map(section) => Ok(Some(Value::Map(self.mapping(node, section, path)))),
            Read::Maps(section) => Ok(self.mappings(node, section, path)),
        };
        match read {
            Ok(value) => Some(value.unwrap_or(Value::Null)),
            Err(kind) => {
                self.problem(path.clone(), line, kind);
                None
            }
        }
    }

    fn mappings(&mut self, node: &Node, section: Section, path: &KeyPath) -> Option<Value> {
        if node.is_null() {
            return None;
        }
        let NodeKind::Seq(items) = &node.kind else {
            let found = found(node, false);
            self.problem(
                path.clone(),
                Some(node.line),
                ProblemKind::WrongType {
                    expected: Expected::ListOfMappings,
                    found,
                },
            );
            return Some(Value::Maps(Vec::new()));
        };
        let mut out = Vec::new();
        for (index, item) in items.iter().enumerate() {
            let item_path = path.index(index);
            if item.is_null() {
                self.problem(item_path, Some(item.line), ProblemKind::EmptyItem);
                continue;
            }
            out.push(self.mapping(item, section, &item_path));
        }
        Some(Value::Maps(out))
    }

    fn apply_rule(&mut self, key: &Key, value: &Value, path: &KeyPath, line: usize) {
        match key.rule {
            Rule::Implemented => {}
            Rule::NoEffect(why) => {
                self.warn(path.clone(), Some(line), WarningKind::NoEffect { why })
            }
            Rule::OffOnly(at, feature) => {
                if !is_off_value(at, value) {
                    self.problem(path.clone(), Some(line), ProblemKind::NotInV1 { feature });
                }
            }
        }
    }
}

fn is_off_value(at: OffValue, value: &Value) -> bool {
    match at {
        OffValue::Any => true,
        OffValue::False => matches!(value, Value::Null | Value::Bool(false)),
        OffValue::Empty => matches!(value, Value::Null),
        OffValue::EmptyList => match value {
            Value::Null => true,
            Value::List(items) => items.is_empty(),
            Value::Str(_)
            | Value::Bool(_)
            | Value::Int(_)
            | Value::Uint(_)
            | Value::Duration(_)
            | Value::Bytes(_)
            | Value::Any
            | Value::Map(_)
            | Value::Maps(_) => false,
        },
    }
}

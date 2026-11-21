//! The YAML node tree, built from `saphyr-parser`'s events, with the places
//! where yaml.v2 and the YAML spec disagree settled here.
//!
//! The tree keeps each scalar's text and whether it was plain; what it means
//! (yaml.v2's YAML 1.1 resolution: `yes` is true, `0600` is octal) is decided
//! by `value.rs` from the type Go reads the key as. Settled here:
//! - Aliases are replaced by a copy of their anchor's node, with at most
//!   [`ALIAS_NODES`] nodes added in all, and nesting is capped at
//!   [`MAX_DEPTH`], so no file costs more than its size suggests.
//! - `<<` merges apply in file order as yaml.v2 applies them: keys after the
//!   merge win over it, and in a list of merged mappings the first wins. A key
//!   *before* the merge that the merge also sets is refused: yaml.v2 lets the
//!   merge win, the spec lets the key win.
//! - A key set twice in one mapping is refused (yaml.v2 keeps the last; Go's
//!   strict pass only warns).
//! - A second document is refused (yaml.v2 reads only the first); an empty
//!   one after `---` is allowed, since there is nothing to ignore.
//! - Explicit tags are refused.
//! - A tab after `:` is reported as such: yaml.v2 reads it as a space, the
//!   parser refuses it.

use std::collections::HashMap;

use saphyr_parser::{Event, Parser, ScalarStyle, ScanError, Span, StrInput};

use crate::error::{KeyPath, Problem, ProblemKind};

/// Most nodes aliases may add to the tree.
pub(crate) const ALIAS_NODES: usize = 10_000;
/// Deepest nesting read.
pub(crate) const MAX_DEPTH: usize = 64;

/// A node and the line it starts on (from 1).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Node {
    pub(crate) line: usize,
    pub(crate) kind: NodeKind,
}

/// What a node is.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NodeKind {
    /// A scalar's text; `plain` unless it was quoted or a block.
    Scalar { text: String, plain: bool },
    /// A mapping, merges applied, in file order.
    Map(Vec<Entry>),
    /// A list.
    Seq(Vec<Node>),
}

/// One key of a mapping.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Entry {
    pub(crate) key: String,
    pub(crate) line: usize,
    pub(crate) value: Node,
}

impl Node {
    fn count(&self) -> usize {
        match &self.kind {
            NodeKind::Scalar { .. } => 1,
            NodeKind::Map(entries) => entries.iter().fold(1_usize, |n, e| {
                n.saturating_add(e.value.count()).saturating_add(1)
            }),
            NodeKind::Seq(items) => items
                .iter()
                .fold(1_usize, |n, item| n.saturating_add(item.count())),
        }
    }

    /// Whether this is yaml.v2's null: a plain `~`, `null`, `Null`, `NULL` or nothing.
    pub(crate) fn is_null(&self) -> bool {
        matches!(&self.kind, NodeKind::Scalar { text, plain: true }
            if matches!(text.as_str(), "" | "~" | "null" | "Null" | "NULL"))
    }
}

/// The first document's root (`None` for an empty file), and the problems
/// that do not stop the parse (duplicates, merge conflicts).
pub(crate) fn parse(text: &str) -> Result<(Option<Node>, Vec<Problem>), Problem> {
    let mut builder = Builder {
        text,
        events: Parser::new_from_str(text),
        anchors: HashMap::new(),
        alias_budget: ALIAS_NODES,
        problems: Vec::new(),
    };
    let root = builder.stream()?;
    Ok((root, builder.problems))
}

struct Builder<'a> {
    text: &'a str,
    events: Parser<'a, StrInput<'a>>,
    anchors: HashMap<usize, Node>,
    alias_budget: usize,
    problems: Vec<Problem>,
}

fn at(line: usize, kind: ProblemKind) -> Problem {
    Problem {
        path: KeyPath::root(),
        line: Some(line),
        kind,
    }
}

/// How a mapping entry came to hold its value.
#[derive(Clone, Copy)]
enum Origin {
    Explicit,
    Merged,
}

impl<'a> Builder<'a> {
    fn next(&mut self) -> Result<(Event<'a>, Span), Problem> {
        match self.events.next_event() {
            Some(Ok(event)) => Ok(event),
            Some(Err(error)) => Err(self.syntax(&error)),
            None => Err(at(
                1,
                ProblemKind::Syntax {
                    message: "unexpected end of input".to_owned(),
                    column: 1,
                },
            )),
        }
    }

    fn syntax(&self, error: &ScanError) -> Problem {
        let mark = error.marker();
        let column = mark.col().saturating_add(1);
        let char_at = |index: usize| self.text.chars().nth(index);
        // The parser marks the tab itself or the character after it.
        let tab_column = if char_at(mark.index()) == Some('\t') {
            Some(column)
        } else if mark.index() > 0 && char_at(mark.index().saturating_sub(1)) == Some('\t') {
            Some(mark.col())
        } else {
            None
        };
        let kind = if let Some(column) = tab_column.filter(|_| {
            error
                .info()
                .contains("must be followed by a valid YAML whitespace")
        }) {
            ProblemKind::TabAfterColon { column }
        } else {
            ProblemKind::Syntax {
                message: error.info().to_owned(),
                column,
            }
        };
        at(mark.line(), kind)
    }

    fn stream(&mut self) -> Result<Option<Node>, Problem> {
        let mut root: Option<Node> = None;
        let mut documents = 0_usize;
        loop {
            let (event, span) = self.next()?;
            match event {
                Event::StreamEnd => return Ok(root),
                Event::DocumentStart(_) => {
                    let (event, span) = self.next()?;
                    let node = self.node(event, span, 0)?;
                    documents = documents.saturating_add(1);
                    if documents == 1 {
                        root = (!node.is_null()).then_some(node);
                    } else if !node.is_null() {
                        return Err(at(node.line, ProblemKind::MultipleDocuments));
                    }
                }
                Event::DocumentEnd | Event::StreamStart | Event::Nothing => {}
                Event::Alias(_)
                | Event::Scalar(..)
                | Event::SequenceStart(..)
                | Event::SequenceEnd
                | Event::MappingStart(..)
                | Event::MappingEnd => {
                    return Err(at(
                        span.start.line(),
                        ProblemKind::Syntax {
                            message: "unexpected event outside a document".to_owned(),
                            column: span.start.col().saturating_add(1),
                        },
                    ));
                }
            }
        }
    }

    fn node(&mut self, event: Event<'a>, span: Span, depth: usize) -> Result<Node, Problem> {
        let line = span.start.line();
        if depth > MAX_DEPTH {
            return Err(at(line, ProblemKind::TooDeep { limit: MAX_DEPTH }));
        }
        let (node, anchor) = match event {
            Event::Scalar(text, style, anchor, tag) => {
                if tag.is_some() {
                    return Err(at(line, ProblemKind::Tag));
                }
                let plain = matches!(style, ScalarStyle::Plain);
                let kind = NodeKind::Scalar {
                    text: text.into_owned(),
                    plain,
                };
                (Node { line, kind }, anchor)
            }
            Event::MappingStart(anchor, tag) => {
                if tag.is_some() {
                    return Err(at(line, ProblemKind::Tag));
                }
                (self.mapping(line, depth)?, anchor)
            }
            Event::SequenceStart(anchor, tag) => {
                if tag.is_some() {
                    return Err(at(line, ProblemKind::Tag));
                }
                (self.sequence(line, depth)?, anchor)
            }
            Event::Alias(id) => return self.alias(id, line),
            Event::Nothing
            | Event::StreamStart
            | Event::StreamEnd
            | Event::DocumentStart(_)
            | Event::DocumentEnd
            | Event::SequenceEnd
            | Event::MappingEnd => {
                return Err(at(
                    line,
                    ProblemKind::Syntax {
                        message: "expected a value".to_owned(),
                        column: span.start.col().saturating_add(1),
                    },
                ));
            }
        };
        if anchor > 0 {
            self.anchors.insert(anchor, node.clone());
        }
        Ok(node)
    }

    fn alias(&mut self, id: usize, line: usize) -> Result<Node, Problem> {
        let Some(node) = self.anchors.get(&id) else {
            return Err(at(
                line,
                ProblemKind::Syntax {
                    message: "alias to an unknown anchor".to_owned(),
                    column: 1,
                },
            ));
        };
        let cost = node.count();
        if cost > self.alias_budget {
            return Err(at(line, ProblemKind::AliasExpansion { limit: ALIAS_NODES }));
        }
        self.alias_budget = self.alias_budget.saturating_sub(cost);
        Ok(node.clone())
    }

    fn sequence(&mut self, line: usize, depth: usize) -> Result<Node, Problem> {
        let mut items = Vec::new();
        loop {
            let (event, span) = self.next()?;
            if matches!(event, Event::SequenceEnd) {
                return Ok(Node {
                    line,
                    kind: NodeKind::Seq(items),
                });
            }
            items.push(self.node(event, span, depth.saturating_add(1))?);
        }
    }

    fn mapping(&mut self, line: usize, depth: usize) -> Result<Node, Problem> {
        let mut entries: Vec<(Entry, Origin)> = Vec::new();
        loop {
            let (event, span) = self.next()?;
            let key_line = span.start.line();
            let (key, plain) = match event {
                Event::MappingEnd => break,
                Event::Scalar(_, _, _, Some(_)) => return Err(at(key_line, ProblemKind::Tag)),
                Event::Scalar(text, style, _, None) => {
                    (text.into_owned(), matches!(style, ScalarStyle::Plain))
                }
                Event::Alias(_)
                | Event::MappingStart(..)
                | Event::SequenceStart(..)
                | Event::Nothing
                | Event::StreamStart
                | Event::StreamEnd
                | Event::DocumentStart(_)
                | Event::DocumentEnd
                | Event::SequenceEnd => return Err(at(key_line, ProblemKind::ComplexKey)),
            };
            let (event, span) = self.next()?;
            let value = self.node(event, span, depth.saturating_add(1))?;
            if plain && key == "<<" {
                self.merge(&mut entries, value, key_line);
            } else {
                self.explicit(
                    &mut entries,
                    Entry {
                        key,
                        line: key_line,
                        value,
                    },
                );
            }
        }
        let entries = entries.into_iter().map(|(entry, _)| entry).collect();
        Ok(Node {
            line,
            kind: NodeKind::Map(entries),
        })
    }

    fn explicit(&mut self, entries: &mut Vec<(Entry, Origin)>, entry: Entry) {
        match entries.iter_mut().find(|(e, _)| e.key == entry.key) {
            Some((first, Origin::Explicit)) => self.problems.push(at(
                entry.line,
                ProblemKind::Duplicate {
                    first_line: first.line,
                },
            )),
            Some(slot) => *slot = (entry, Origin::Explicit),
            None => entries.push((entry, Origin::Explicit)),
        }
    }

    /// yaml.v2's `merge`: a mapping, or a list of mappings applied last to
    /// first so the first wins.
    fn merge(&mut self, entries: &mut Vec<(Entry, Origin)>, value: Node, line: usize) {
        let sources = match value.kind {
            NodeKind::Map(source) => vec![source],
            NodeKind::Seq(items) => {
                let mut sources = Vec::new();
                for item in items {
                    match item.kind {
                        NodeKind::Map(source) => sources.push(source),
                        NodeKind::Scalar { .. } | NodeKind::Seq(_) => {
                            self.problems
                                .push(at(item.line, ProblemKind::MergeNotMapping));
                        }
                    }
                }
                sources
            }
            NodeKind::Scalar { .. } => {
                self.problems.push(at(line, ProblemKind::MergeNotMapping));
                Vec::new()
            }
        };
        for source in sources.into_iter().rev() {
            for entry in source {
                match entries.iter_mut().find(|(e, _)| e.key == entry.key) {
                    Some((first, Origin::Explicit)) => self.problems.push(at(
                        line,
                        ProblemKind::MergeOverridesKey {
                            key_line: first.line,
                        },
                    )),
                    Some(slot) => *slot = (entry, Origin::Merged),
                    None => entries.push((entry, Origin::Merged)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar(node: &Node) -> &str {
        match &node.kind {
            NodeKind::Scalar { text, .. } => text,
            NodeKind::Map(_) | NodeKind::Seq(_) => panic!("not a scalar"),
        }
    }

    fn get<'n>(node: &'n Node, key: &str) -> &'n Node {
        match &node.kind {
            NodeKind::Map(entries) => &entries.iter().find(|e| e.key == key).unwrap().value,
            NodeKind::Scalar { .. } | NodeKind::Seq(_) => panic!("not a map"),
        }
    }

    fn root(text: &str) -> Node {
        let (root, problems) = parse(text).unwrap();
        assert!(problems.is_empty(), "{problems:?}");
        root.unwrap()
    }

    fn fails(text: &str) -> ProblemKind {
        match parse(text) {
            Err(problem) => problem.kind,
            Ok((_, problems)) => problems.into_iter().next().unwrap().kind,
        }
    }

    #[test]
    fn merges_follow_yaml_v2_order() {
        let tree = root("base: &b\n  d: 5s\n  s: base\nin:\n  <<: *b\n  s: own\n");
        assert_eq!(scalar(get(get(&tree, "in"), "s")), "own");
        assert_eq!(scalar(get(get(&tree, "in"), "d")), "5s");
        let tree = root("x1: &p\n  s: one\nx2: &q\n  s: two\n  d: 1s\nin:\n  <<: [*p, *q]\n");
        assert_eq!(scalar(get(get(&tree, "in"), "s")), "one");
        assert_eq!(scalar(get(get(&tree, "in"), "d")), "1s");
    }

    #[test]
    fn a_key_before_a_merge_that_sets_it_is_refused() {
        let kind = fails("base: &b\n  s: base\nin:\n  s: own\n  <<: *b\n");
        assert_eq!(kind, ProblemKind::MergeOverridesKey { key_line: 4 });
    }

    #[test]
    fn duplicates_are_refused_with_both_lines() {
        assert_eq!(
            fails("s: a\ns: b\n"),
            ProblemKind::Duplicate { first_line: 1 }
        );
    }

    #[test]
    fn documents() {
        assert_eq!(parse("").unwrap().0, None);
        assert_eq!(parse("# only\n").unwrap().0, None);
        assert_eq!(parse("---\n").unwrap().0, None);
        assert_eq!(scalar(get(&root("s: v\n---\n"), "s")), "v");
        assert_eq!(scalar(get(&root("%YAML 1.1\n---\ns: v\n"), "s")), "v");
        assert_eq!(
            fails("s: first\n---\ns: second\n"),
            ProblemKind::MultipleDocuments
        );
    }

    #[test]
    fn tabs() {
        assert_eq!(fails("s:\tv"), ProblemKind::TabAfterColon { column: 3 });
        assert!(matches!(
            fails("in:\n\td: 5s\n"),
            ProblemKind::Syntax { .. }
        ));
        assert_eq!(scalar(get(&root("s: a\tb"), "s")), "a\tb");
    }

    #[test]
    fn tags_are_refused() {
        assert_eq!(fails("s: !!str 5\n"), ProblemKind::Tag);
        assert_eq!(fails("s: !foo {a: b}\n"), ProblemKind::Tag);
    }

    #[test]
    fn aliases_are_bounded() {
        let mut text = String::from("a0: &a0 [x, x, x, x, x, x, x, x, x, x]\n");
        for level in 1..6 {
            let prev = level - 1;
            text.push_str(&format!(
                "a{level}: &a{level} [*a{prev}, *a{prev}, *a{prev}, *a{prev}, *a{prev}, *a{prev}, *a{prev}, *a{prev}, *a{prev}, *a{prev}]\n"
            ));
        }
        assert_eq!(
            fails(&text),
            ProblemKind::AliasExpansion { limit: ALIAS_NODES }
        );
    }

    #[test]
    fn depth_is_bounded() {
        let text = format!("{}{}", "[".repeat(MAX_DEPTH + 2), "]".repeat(MAX_DEPTH + 2));
        assert_eq!(fails(&text), ProblemKind::TooDeep { limit: MAX_DEPTH });
    }
}

//! The commit log: every state the application's database was in, as a tree.
//!
//! The log is written beside the database by whoever drives the application, on storage no fault
//! touches, and only from what the application itself saw: a commit SQLite acknowledged, a commit
//! whose outcome is unknown (the statement failed as the power went), and the state the database
//! holds when it comes back after a crash. It is a tree, not a line, because a crash can rewind
//! the database to an earlier node and the application then commits a different history from
//! there.
//!
//! Each node is named by a [`Digest`] of the database's content at that node. Content can repeat
//! (a commit that writes the bytes already there), so a digest names a set of nodes.
//!
//! Times are whatever clock the driver keeps, in milliseconds. The local head's history over time
//! is kept, so the judge can ask which node was live at an instant.

use std::collections::BTreeMap;

use crate::Digest;

/// A node of the history tree, by position in the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u32);

impl NodeId {
    /// The root: the database before the first logged commit.
    pub const ROOT: Self = Self(0);

    /// The position in the log.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    fn index(self) -> usize {
        usize::try_from(self.0).unwrap_or(usize::MAX)
    }
}

#[derive(Clone, Debug)]
struct Node {
    parent: Option<NodeId>,
    digest: Digest,
    /// When the driver began the statement that made it.
    begun: i64,
    /// When SQLite acknowledged it (or, unacknowledged, when the driver gave up on it).
    at: i64,
    depth: u32,
    acknowledged: bool,
}

/// A crash took the database from `cut` back to `to`, an ancestor of it, at log position `seq`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rewind {
    /// The node the database holds afterwards.
    pub to: NodeId,
    /// The head it was at before.
    pub cut: NodeId,
    /// The driver's clock.
    pub at: i64,
    /// How many nodes the log held when it happened.
    pub seq: u32,
}

/// The tree of histories, its local head and the head's history over time.
#[derive(Clone, Debug)]
pub struct Log {
    nodes: Vec<Node>,
    by_digest: BTreeMap<Digest, Vec<NodeId>>,
    head: NodeId,
    heads: Vec<(i64, NodeId)>,
    rewinds: Vec<Rewind>,
    latest_time: i64,
    clock_went_back: bool,
}

impl Log {
    /// A log whose database holds `root` at `at`.
    #[must_use]
    pub fn new(root: Digest, at: i64) -> Self {
        let node = Node {
            parent: None,
            digest: root,
            begun: at,
            at,
            depth: 0,
            acknowledged: true,
        };
        Self {
            nodes: vec![node],
            by_digest: BTreeMap::from([(root, vec![NodeId::ROOT])]),
            head: NodeId::ROOT,
            heads: vec![(at, NodeId::ROOT)],
            rewinds: Vec::new(),
            latest_time: at,
            clock_went_back: false,
        }
    }

    fn push(&mut self, digest: Digest, begun: i64, at: i64, acknowledged: bool) -> NodeId {
        self.note_time(begun);
        self.note_time(at);
        let id = NodeId(u32::try_from(self.nodes.len()).unwrap_or(u32::MAX));
        let depth = self
            .node(self.head)
            .map_or(0, |n| n.depth.saturating_add(1));
        self.nodes.push(Node {
            parent: Some(self.head),
            digest,
            begun,
            at,
            depth,
            acknowledged,
        });
        self.by_digest.entry(digest).or_default().push(id);
        id
    }

    fn move_head(&mut self, to: NodeId, at: i64) {
        self.head = to;
        self.heads.push((at, to));
    }

    /// SQLite acknowledged a commit on the local head that leaves the database at `digest`, and
    /// the driver read one time for it: [`Self::commit_between`] with both ends at `at`.
    pub fn commit(&mut self, digest: Digest, at: i64) -> NodeId {
        self.commit_between(digest, at, at)
    }

    /// A commit is an interval: the driver began the statement at `begun` and SQLite
    /// acknowledged it at `acknowledged`. Other connections may see it anywhere in between, so
    /// `AsOf` judges presence by `begun` (no commit begun at or after the instant may be there)
    /// and the head moves at `acknowledged` (a lag bound counts from there).
    pub fn commit_between(&mut self, digest: Digest, begun: i64, acknowledged: i64) -> NodeId {
        let id = self.push(digest, begun, acknowledged, true);
        self.move_head(id, acknowledged);
        id
    }

    /// A commit on the local head whose outcome is unknown: the statement failed in a way that
    /// may or may not have committed. The head stays; [`Self::recover`] says which it was.
    pub fn pending(&mut self, digest: Digest, at: i64) -> NodeId {
        self.push(digest, at, at, false)
    }

    /// After a crash the database holds `to`. A descendant of the head (a pending commit that
    /// landed) moves the head forward; anything else is a rewind and forks the tree there.
    pub fn recover(&mut self, to: NodeId, at: i64) {
        self.note_time(at);
        if to == self.head {
            return;
        }
        if !self.is_ancestor(self.head, to) {
            let seq = u32::try_from(self.nodes.len()).unwrap_or(u32::MAX);
            self.rewinds.push(Rewind {
                to,
                cut: self.head,
                at,
                seq,
            });
        }
        self.move_head(to, at);
    }

    /// Tells the log the driver's clock reads `now`. A reading earlier than one before means the
    /// clock went back, and instants can no longer be compared with commit times.
    pub fn note_time(&mut self, now: i64) {
        if now < self.latest_time {
            self.clock_went_back = true;
        }
        self.latest_time = self.latest_time.max(now);
    }

    /// Whether the driver's clock ever went back.
    #[must_use]
    pub const fn clock_went_back(&self) -> bool {
        self.clock_went_back
    }

    /// The node the local database holds now.
    #[must_use]
    pub const fn head(&self) -> NodeId {
        self.head
    }

    /// Every node whose content is `digest`, oldest first.
    #[must_use]
    pub fn find(&self, digest: &Digest) -> &[NodeId] {
        self.by_digest.get(digest).map_or(&[], Vec::as_slice)
    }

    /// The nodes whose content is `digest` that are descendants of `of` (or `of` itself): where a
    /// driver that saw `of` before a crash finds what the database holds after it.
    #[must_use]
    pub fn find_from(&self, digest: &Digest, of: NodeId) -> Option<NodeId> {
        let found = self.find(digest);
        found
            .iter()
            .rev()
            .find(|n| self.is_ancestor(of, **n))
            .or_else(|| found.iter().rev().find(|n| self.is_ancestor(**n, of)))
            .copied()
    }

    /// A node's content.
    #[must_use]
    pub fn digest(&self, id: NodeId) -> Option<Digest> {
        self.node(id).map(|n| n.digest)
    }

    /// When a node's commit was acknowledged.
    #[must_use]
    pub fn committed_at(&self, id: NodeId) -> Option<i64> {
        self.node(id).map(|n| n.at)
    }

    /// When the driver began the statement that made a node: the earliest instant any reader
    /// could see it.
    #[must_use]
    pub fn begun_at(&self, id: NodeId) -> Option<i64> {
        self.node(id).map(|n| n.begun)
    }

    /// Whether SQLite acknowledged the node's commit (the root counts as acknowledged).
    #[must_use]
    pub fn acknowledged(&self, id: NodeId) -> bool {
        self.node(id).is_some_and(|n| n.acknowledged)
    }

    /// Every rewind, oldest first.
    #[must_use]
    pub fn rewinds(&self) -> &[Rewind] {
        &self.rewinds
    }

    /// How many nodes the log holds.
    #[must_use]
    pub fn len(&self) -> u32 {
        u32::try_from(self.nodes.len()).unwrap_or(u32::MAX)
    }

    /// Never: the root is always there.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(id.index())
    }

    /// Whether `a` is `b` or one of its ancestors.
    #[must_use]
    pub fn is_ancestor(&self, a: NodeId, b: NodeId) -> bool {
        let Some(depth) = self.node(a).map(|n| n.depth) else {
            return false;
        };
        let mut at = b;
        loop {
            let Some(node) = self.node(at) else {
                return false;
            };
            if node.depth <= depth {
                return at == a;
            }
            let Some(parent) = node.parent else {
                return false;
            };
            at = parent;
        }
    }

    /// The node the local database held at `instant`: the last head change at or before it.
    #[must_use]
    pub fn head_at(&self, instant: i64) -> NodeId {
        self.heads
            .iter()
            .take_while(|(at, _)| *at <= instant)
            .last()
            .map_or(NodeId::ROOT, |(_, id)| *id)
    }

    /// Whether `id` was the local head at some instant from `from` to `to`, both included.
    #[must_use]
    pub fn live_within(&self, id: NodeId, from: i64, to: i64) -> bool {
        self.head_at(from) == id
            || self
                .heads
                .iter()
                .any(|(at, head)| *head == id && from <= *at && *at <= to)
    }
}

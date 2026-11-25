//! Core value types of the Raft domain.

use std::collections::BTreeSet;

/// Identifier of a server in the cluster. `0` is reserved as "none".
pub type NodeId = u64;
/// Raft term. Monotonically increasing logical clock.
pub type Term = u64;
/// 1-based position in the replicated log. `0` means "before the first entry".
pub type Index = u64;

/// Payload kinds carried by log entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryKind {
    /// Appended by every new leader so it can commit entries of earlier terms.
    Noop,
    /// Opaque command for the replicated state machine.
    Command(Vec<u8>),
    /// New cluster configuration (single-server change).
    Config(Configuration),
}

/// One replicated log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub kind: EntryKind,
}

/// State that must be durable before answering any RPC.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    /// `None` when the node has not voted in `term`.
    pub voted_for: Option<NodeId>,
    pub commit: Index,
}

/// Set of voting members.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Configuration {
    pub voters: BTreeSet<NodeId>,
}

impl Configuration {
    pub fn new<I: IntoIterator<Item = NodeId>>(voters: I) -> Self {
        Self {
            voters: voters.into_iter().collect(),
        }
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }

    /// Smallest number of voters that forms a majority.
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

/// Single-server membership change (Raft thesis §4.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfChange {
    AddVoter(NodeId),
    RemoveVoter(NodeId),
}

impl ConfChange {
    /// Applies the change, returning `None` if it would be a no-op or would
    /// leave the cluster without voters.
    pub fn apply_to(&self, cfg: &Configuration) -> Option<Configuration> {
        let mut next = cfg.clone();
        let changed = match *self {
            ConfChange::AddVoter(id) => id != 0 && next.voters.insert(id),
            ConfChange::RemoveVoter(id) => next.voters.remove(&id),
        };
        if !changed || next.voters.is_empty() {
            return None;
        }
        Some(next)
    }
}

/// Metadata describing the prefix of the log a snapshot replaces.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub index: Index,
    pub term: Term,
    pub config: Configuration,
}

/// Snapshot of the state machine plus its metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

/// Role of a node (State pattern; transitions live in `RaftCore`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

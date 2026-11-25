use super::types::{Index, NodeId};
use std::fmt;

/// Errors produced by the domain. They never carry I/O causes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftError {
    /// Operation requires leadership; carries the known leader if any.
    NotLeader { leader: Option<NodeId> },
    /// A previous membership change is still uncommitted.
    ConfChangePending { index: Index },
    /// The membership change is a no-op or would leave no voters.
    InvalidConfChange,
    /// A peer tried to overwrite an already committed entry.
    CommittedConflict { index: Index, commit: Index },
    /// Requested compaction beyond what has been applied.
    CompactBeyondApplied { requested: Index, applied: Index },
    /// Requested index is not available (compacted or not yet appended).
    Unavailable { index: Index },
    /// Invalid configuration passed at construction.
    InvalidConfig(&'static str),
}

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftError::NotLeader { leader } => write!(f, "not leader (leader hint: {leader:?})"),
            RaftError::ConfChangePending { index } => {
                write!(f, "membership change at index {index} not yet committed")
            }
            RaftError::InvalidConfChange => write!(f, "invalid membership change"),
            RaftError::CommittedConflict { index, commit } => {
                write!(f, "conflict at index {index} below commit {commit}")
            }
            RaftError::CompactBeyondApplied { requested, applied } => {
                write!(f, "cannot compact to {requested}: applied is {applied}")
            }
            RaftError::Unavailable { index } => write!(f, "index {index} unavailable"),
            RaftError::InvalidConfig(why) => write!(f, "invalid config: {why}"),
        }
    }
}

impl std::error::Error for RaftError {}

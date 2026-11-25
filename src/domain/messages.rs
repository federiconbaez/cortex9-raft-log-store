//! RPCs exchanged between Raft peers. Transport is out of scope.

use super::types::{Entry, Index, NodeId, Snapshot, Term};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub term: Term,
    pub body: Body,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    AppendEntries {
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
    AppendEntriesResp {
        success: bool,
        /// On success: highest index known to match the leader.
        /// On failure: hint of the follower's last plausible index.
        index: Index,
    },
    RequestVote {
        last_index: Index,
        last_term: Term,
    },
    RequestVoteResp {
        granted: bool,
    },
    InstallSnapshot {
        snapshot: Snapshot,
    },
    InstallSnapshotResp {
        /// Index the follower now has (its commit after installing).
        index: Index,
    },
}

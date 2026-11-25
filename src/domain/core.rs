//! `RaftCore`: the Raft protocol as a deterministic state machine.
//!
//! Inputs: `tick`, `step`, `propose`, `propose_conf_change`, `compact`.
//! Outputs: `Effect`s (Command pattern) drained with `take_effects`, ordered
//! so that durable writes precede the messages that depend on them.

use std::collections::{BTreeMap, BTreeSet};

use super::error::RaftError;
use super::log::RaftLog;
use super::messages::{Body, Message};
use super::types::*;
use crate::ports::Rand;

/// Static parameters of a node.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub id: NodeId,
    /// Base election timeout in ticks; the effective one is randomized in
    /// `[election_tick, 2 * election_tick)`.
    pub election_tick: u32,
    pub heartbeat_tick: u32,
    pub max_entries_per_msg: usize,
}

impl RaftConfig {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            election_tick: 10,
            heartbeat_tick: 2,
            max_entries_per_msg: 64,
        }
    }

    fn validate(&self) -> Result<(), RaftError> {
        if self.id == 0 {
            return Err(RaftError::InvalidConfig("node id 0 is reserved"));
        }
        if self.heartbeat_tick == 0 || self.election_tick <= self.heartbeat_tick {
            return Err(RaftError::InvalidConfig(
                "require 0 < heartbeat_tick < election_tick",
            ));
        }
        if self.max_entries_per_msg == 0 {
            return Err(RaftError::InvalidConfig("max_entries_per_msg must be > 0"));
        }
        Ok(())
    }
}

/// Side effects requested by the domain, executed by the application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Save the snapshot; if `retain_log_suffix` is false also drop every
    /// stored entry after it. Then compact the log up to `snapshot.meta.index`.
    PersistSnapshot {
        snapshot: Snapshot,
        retain_log_suffix: bool,
    },
    /// Truncate-then-append.
    PersistEntries(Vec<Entry>),
    PersistHardState(HardState),
    Send(Message),
    /// Replace the state machine with this snapshot.
    RestoreSnapshot(Snapshot),
    /// Apply committed entries in order.
    Apply(Vec<Entry>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    pub next: Index,
    pub matched: Index,
}

pub struct RaftCore<R: Rand> {
    cfg: RaftConfig,
    rand: R,
    term: Term,
    voted_for: Option<NodeId>,
    role: Role,
    leader: Option<NodeId>,
    log: RaftLog,
    snapshot: Option<Snapshot>,
    /// Configuration covered by the snapshot (or bootstrap configuration).
    base_config: Configuration,
    config: Configuration,
    progress: BTreeMap<NodeId, Progress>,
    votes: BTreeSet<NodeId>,
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    randomized_timeout: u32,
    // Effect buffers.
    persisted_hs: HardState,
    snapshot_to_persist: Option<(Snapshot, bool)>,
    snapshot_to_restore: Option<Snapshot>,
    outbox: Vec<Message>,
}

impl<R: Rand> RaftCore<R> {
    /// Creates a node from durable state. `bootstrap` is the initial
    /// configuration used when neither the snapshot nor the log carries one.
    pub fn new(
        cfg: RaftConfig,
        rand: R,
        bootstrap: Configuration,
        hard_state: HardState,
        snapshot: Option<Snapshot>,
        entries: Vec<Entry>,
    ) -> Result<Self, RaftError> {
        cfg.validate()?;
        let log = RaftLog::restore(snapshot.as_ref().map(|s| &s.meta), entries, &hard_state)?;
        let base_config = snapshot
            .as_ref()
            .map(|s| s.meta.config.clone())
            .unwrap_or(bootstrap);
        let mut core = Self {
            cfg,
            rand,
            term: hard_state.term,
            voted_for: hard_state.voted_for,
            role: Role::Follower,
            leader: None,
            log,
            snapshot_to_restore: snapshot.clone(),
            snapshot,
            base_config,
            config: Configuration::default(),
            progress: BTreeMap::new(),
            votes: BTreeSet::new(),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_timeout: 0,
            persisted_hs: HardState::default(),
            snapshot_to_persist: None,
            outbox: Vec::new(),
        };
        core.persisted_hs = core.hard_state();
        core.recompute_config();
        core.reset_randomized_timeout();
        Ok(core)
    }

    // ----- queries -------------------------------------------------------

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn log(&self) -> &RaftLog {
        &self.log
    }
    pub fn config(&self) -> &Configuration {
        &self.config
    }
    pub fn progress(&self, peer: NodeId) -> Option<Progress> {
        self.progress.get(&peer).copied()
    }
    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            voted_for: self.voted_for,
            commit: self.log.commit(),
        }
    }

    // ----- inputs --------------------------------------------------------

    /// Advances logical time by one tick.
    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.cfg.heartbeat_tick {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append();
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.randomized_timeout
                    && self.config.contains(self.cfg.id)
                {
                    self.campaign();
                }
            }
        }
    }

    /// Leader-only: appends a command. Returns its index.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<Index, RaftError> {
        self.ensure_leader()?;
        let idx = self
            .log
            .append_local(self.term, EntryKind::Command(command));
        self.after_local_append();
        Ok(idx)
    }

    /// Leader-only: single-server membership change.
    pub fn propose_conf_change(&mut self, change: ConfChange) -> Result<Index, RaftError> {
        self.ensure_leader()?;
        if let Some((idx, _)) = self.log.last_config() {
            if idx > self.log.commit() {
                return Err(RaftError::ConfChangePending { index: idx });
            }
        }
        let next = change
            .apply_to(&self.config)
            .ok_or(RaftError::InvalidConfChange)?;
        let idx = self.log.append_local(self.term, EntryKind::Config(next));
        self.recompute_config();
        self.after_local_append();
        Ok(idx)
    }

    /// Replaces the applied prefix up to `index` with a snapshot of `data`.
    pub fn compact(&mut self, index: Index, data: Vec<u8>) -> Result<(), RaftError> {
        if index <= self.log.snapshot_index() {
            return Ok(());
        }
        let config = self
            .log
            .config_at(index)
            .map(|(_, c)| c.clone())
            .unwrap_or_else(|| self.base_config.clone());
        let term = self.log.compact(index)?;
        let snap = Snapshot {
            meta: SnapshotMeta {
                index,
                term,
                config: config.clone(),
            },
            data,
        };
        self.base_config = config;
        self.snapshot = Some(snap.clone());
        self.snapshot_to_persist = Some((snap, true));
        Ok(())
    }

    /// Handles a message from a peer.
    pub fn step(&mut self, m: Message) -> Result<(), RaftError> {
        if m.to != self.cfg.id {
            return Ok(());
        }
        if m.term > self.term {
            let leader = match m.body {
                Body::AppendEntries { .. } | Body::InstallSnapshot { .. } => Some(m.from),
                _ => None,
            };
            self.become_follower(m.term, leader);
        } else if m.term < self.term {
            // Tell stale leaders/candidates about the newer term.
            match m.body {
                Body::AppendEntries { .. } | Body::InstallSnapshot { .. } => self.send(
                    m.from,
                    Body::AppendEntriesResp {
                        success: false,
                        index: 0,
                    },
                ),
                Body::RequestVote { .. } => {
                    self.send(m.from, Body::RequestVoteResp { granted: false })
                }
                _ => {}
            }
            return Ok(());
        }

        match m.body {
            Body::RequestVote {
                last_index,
                last_term,
            } => self.on_request_vote(m.from, last_index, last_term),
            Body::RequestVoteResp { granted } => self.on_vote_resp(m.from, granted),
            Body::AppendEntries {
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => self.on_append(m.from, prev_index, prev_term, entries, leader_commit)?,
            Body::AppendEntriesResp { success, index } => {
                self.on_append_resp(m.from, success, index)
            }
            Body::InstallSnapshot { snapshot } => self.on_install_snapshot(m.from, snapshot),
            Body::InstallSnapshotResp { index } => self.on_install_snapshot_resp(m.from, index),
        }
        Ok(())
    }

    /// Drains pending effects in a safe order: persistence, then messages,
    /// then state-machine application of committed data.
    pub fn take_effects(&mut self) -> Vec<Effect> {
        let mut out = Vec::new();
        if let Some((snapshot, retain_log_suffix)) = self.snapshot_to_persist.take() {
            out.push(Effect::PersistSnapshot {
                snapshot,
                retain_log_suffix,
            });
        }
        let unstable = self.log.take_unstable();
        if !unstable.is_empty() {
            out.push(Effect::PersistEntries(unstable));
        }
        let hs = self.hard_state();
        if hs != self.persisted_hs {
            self.persisted_hs = hs;
            out.push(Effect::PersistHardState(hs));
        }
        out.extend(self.outbox.drain(..).map(Effect::Send));
        if let Some(s) = self.snapshot_to_restore.take() {
            out.push(Effect::RestoreSnapshot(s));
        }
        let committed = self.log.take_committed();
        if !committed.is_empty() {
            out.push(Effect::Apply(committed));
        }
        out
    }

    // ----- role transitions (State pattern) -------------------------------

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.progress.clear();
        self.election_elapsed = 0;
        self.reset_randomized_timeout();
    }

    fn campaign(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.cfg.id);
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id);
        self.election_elapsed = 0;
        self.reset_randomized_timeout();
        if self.has_quorum(&self.votes) {
            self.become_leader();
            return;
        }
        let (li, lt) = (self.log.last_index(), self.log.last_term());
        for peer in self.peers() {
            self.send(
                peer,
                Body::RequestVote {
                    last_index: li,
                    last_term: lt,
                },
            );
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.heartbeat_elapsed = 0;
        self.progress.clear();
        self.sync_progress();
        self.log.append_local(self.term, EntryKind::Noop);
        self.after_local_append();
    }

    // ----- handlers ------------------------------------------------------

    fn on_request_vote(&mut self, from: NodeId, last_index: Index, last_term: Term) {
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(from);
        let granted = can_vote && self.log.is_up_to_date(last_index, last_term);
        if granted {
            self.voted_for = Some(from);
            self.election_elapsed = 0;
        }
        self.send(from, Body::RequestVoteResp { granted });
    }

    fn on_vote_resp(&mut self, from: NodeId, granted: bool) {
        if self.role != Role::Candidate || !granted {
            return;
        }
        self.votes.insert(from);
        if self.has_quorum(&self.votes) {
            self.become_leader();
        }
    }

    fn on_append(
        &mut self,
        from: NodeId,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) -> Result<(), RaftError> {
        debug_assert!(
            self.role != Role::Leader,
            "two leaders in term {}",
            self.term
        );
        if self.role == Role::Candidate {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.election_elapsed = 0;

        if prev_index < self.log.commit() {
            let commit = self.log.commit();
            self.send(
                from,
                Body::AppendEntriesResp {
                    success: true,
                    index: commit,
                },
            );
            return Ok(());
        }
        match self.log.maybe_append(prev_index, prev_term, &entries)? {
            Some(last_new) => {
                self.recompute_config();
                self.log.commit_to(leader_commit.min(last_new));
                self.send(
                    from,
                    Body::AppendEntriesResp {
                        success: true,
                        index: last_new,
                    },
                );
            }
            None => {
                let hint = self.log.reject_hint(prev_index);
                self.send(
                    from,
                    Body::AppendEntriesResp {
                        success: false,
                        index: hint,
                    },
                );
            }
        }
        Ok(())
    }

    fn on_append_resp(&mut self, from: NodeId, success: bool, index: Index) {
        if self.role != Role::Leader {
            return;
        }
        let Some(pr) = self.progress.get_mut(&from) else {
            return;
        };
        if success {
            if index > pr.matched {
                pr.matched = index;
            }
            pr.next = pr.next.max(pr.matched + 1);
            let more = pr.next <= self.log.last_index();
            self.maybe_commit();
            if more && self.role == Role::Leader {
                self.send_append(from);
            }
        } else {
            let floor = pr.matched + 1;
            pr.next = (index + 1).min(pr.next.saturating_sub(1)).max(floor).max(1);
            self.send_append(from);
        }
    }

    fn on_install_snapshot(&mut self, from: NodeId, snapshot: Snapshot) {
        debug_assert!(self.role != Role::Leader);
        if self.role == Role::Candidate {
            self.become_follower(self.term, Some(from));
        }
        self.leader = Some(from);
        self.election_elapsed = 0;
        if snapshot.meta.index <= self.log.commit() {
            let commit = self.log.commit();
            self.send(from, Body::InstallSnapshotResp { index: commit });
            return;
        }
        let retained = self.log.install_snapshot(&snapshot.meta);
        self.base_config = snapshot.meta.config.clone();
        self.snapshot = Some(snapshot.clone());
        self.snapshot_to_persist = Some((snapshot.clone(), retained));
        let idx = snapshot.meta.index;
        self.snapshot_to_restore = Some(snapshot);
        self.recompute_config();
        self.send(from, Body::InstallSnapshotResp { index: idx });
    }

    fn on_install_snapshot_resp(&mut self, from: NodeId, index: Index) {
        if self.role != Role::Leader {
            return;
        }
        let Some(pr) = self.progress.get_mut(&from) else {
            return;
        };
        pr.matched = pr.matched.max(index);
        pr.next = pr.matched + 1;
        let more = pr.next <= self.log.last_index();
        self.maybe_commit();
        if more && self.role == Role::Leader {
            self.send_append(from);
        }
    }

    // ----- replication ---------------------------------------------------

    fn after_local_append(&mut self) {
        self.maybe_commit();
        if self.role == Role::Leader {
            self.broadcast_append();
        }
    }

    fn broadcast_append(&mut self) {
        for peer in self.peers() {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let Some(pr) = self.progress.get(&to).copied() else {
            return;
        };
        let prev_index = pr.next - 1;
        let body = match self.log.term_at(prev_index) {
            Some(prev_term) => {
                let entries = self
                    .log
                    .entries_from(pr.next, self.cfg.max_entries_per_msg)
                    .unwrap_or_default();
                Body::AppendEntries {
                    prev_index,
                    prev_term,
                    entries,
                    leader_commit: self.log.commit(),
                }
            }
            None => match &self.snapshot {
                Some(s) => Body::InstallSnapshot {
                    snapshot: s.clone(),
                },
                None => return,
            },
        };
        self.send(to, body);
    }

    /// Advances commit to the highest index replicated on a quorum of voters,
    /// only for entries of the current term (Raft §5.4.2).
    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut matched: Vec<Index> = self
            .config
            .voters
            .iter()
            .map(|&v| {
                if v == self.cfg.id {
                    self.log.last_index()
                } else {
                    self.progress.get(&v).map(|p| p.matched).unwrap_or(0)
                }
            })
            .collect();
        if matched.is_empty() {
            return;
        }
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let n = matched[self.config.quorum() - 1];
        if n > self.log.commit() && self.log.term_at(n) == Some(self.term) {
            self.log.commit_to(n);
        }
        // A leader removed from the configuration steps down once the
        // configuration that removes it is committed (thesis §4.2.2).
        if !self.config.contains(self.cfg.id) {
            if let Some((idx, _)) = self.log.last_config() {
                if idx <= self.log.commit() {
                    self.broadcast_append(); // propagate the final commit index
                    self.become_follower(self.term, None);
                }
            }
        }
    }

    // ----- helpers -------------------------------------------------------

    fn ensure_leader(&self) -> Result<(), RaftError> {
        if self.role == Role::Leader {
            Ok(())
        } else {
            Err(RaftError::NotLeader {
                leader: self.leader,
            })
        }
    }

    fn peers(&self) -> Vec<NodeId> {
        self.config
            .voters
            .iter()
            .copied()
            .filter(|&v| v != self.cfg.id)
            .collect()
    }

    fn has_quorum(&self, set: &BTreeSet<NodeId>) -> bool {
        self.config
            .voters
            .iter()
            .filter(|v| set.contains(v))
            .count()
            >= self.config.quorum()
    }

    /// Configuration = latest config entry in the log, else the base one.
    /// Called after every log mutation so a truncated config is rolled back.
    fn recompute_config(&mut self) {
        self.config = self
            .log
            .last_config()
            .map(|(_, c)| c.clone())
            .unwrap_or_else(|| self.base_config.clone());
        if self.role == Role::Leader {
            self.sync_progress();
        }
    }

    fn sync_progress(&mut self) {
        let next = self.log.last_index() + 1;
        let peers = self.peers();
        self.progress.retain(|id, _| peers.contains(id));
        for p in peers {
            self.progress
                .entry(p)
                .or_insert(Progress { next, matched: 0 });
        }
    }

    fn reset_randomized_timeout(&mut self) {
        let base = self.cfg.election_tick;
        self.randomized_timeout = base + (self.rand.next_u64() % base as u64) as u32;
    }

    fn send(&mut self, to: NodeId, body: Body) {
        self.outbox.push(Message {
            from: self.cfg.id,
            to,
            term: self.term,
            body,
        });
    }
}

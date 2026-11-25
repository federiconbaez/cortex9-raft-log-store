#![allow(dead_code)]
//! Deterministic cluster simulator used by the invariant tests.

use cortex9_raft_log_store::adapters::memory::{MemLogStore, MemSnapshotStore};
use cortex9_raft_log_store::adapters::rand::SeededRand;
use cortex9_raft_log_store::application::RaftNode;
use cortex9_raft_log_store::domain::*;
use cortex9_raft_log_store::ports::{Rand, StateMachine};
use std::collections::{BTreeMap, BTreeSet};

/// State machine that records every applied (index, command) pair so the
/// simulator can check State Machine Safety. Its snapshot is the full list.
#[derive(Default, Clone, Debug)]
pub struct RecordingSm {
    pub applied: Vec<(Index, Vec<u8>)>,
}

impl StateMachine for RecordingSm {
    fn apply(&mut self, index: Index, command: &[u8]) {
        if let Some((last, _)) = self.applied.last() {
            assert!(index > *last, "applied out of order: {index} after {last}");
        }
        self.applied.push((index, command.to_vec()));
    }
    fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (i, c) in &self.applied {
            out.extend_from_slice(&i.to_le_bytes());
            out.extend_from_slice(&(c.len() as u32).to_le_bytes());
            out.extend_from_slice(c);
        }
        out
    }
    fn restore(&mut self, data: &[u8]) {
        self.applied.clear();
        let mut p = 0;
        while p < data.len() {
            let i = u64::from_le_bytes(data[p..p + 8].try_into().unwrap());
            let n = u32::from_le_bytes(data[p + 8..p + 12].try_into().unwrap()) as usize;
            self.applied.push((i, data[p + 12..p + 12 + n].to_vec()));
            p += 12 + n;
        }
    }
}

type AppliedLog = Vec<(Index, Vec<u8>)>;

pub type SimNode = RaftNode<MemLogStore, MemSnapshotStore, RecordingSm, SeededRand>;

pub struct Faults {
    pub drop_pct: u64,
    pub dup_pct: u64,
    pub max_delay: u64,
    pub crash_pct: u64,
    pub partition_pct: u64,
    pub snapshot_threshold: u64,
}

impl Default for Faults {
    fn default() -> Self {
        Self {
            drop_pct: 10,
            dup_pct: 5,
            max_delay: 3,
            crash_pct: 1,
            partition_pct: 2,
            snapshot_threshold: 8,
        }
    }
}

pub struct Sim {
    pub ids: Vec<NodeId>,
    pub nodes: BTreeMap<NodeId, Option<SimNode>>,
    stores: BTreeMap<NodeId, (MemLogStore, MemSnapshotStore)>,
    bootstrap: Configuration,
    pub rng: SeededRand,
    pub now: u64,
    queue: Vec<(u64, Message)>,
    /// Nodes cut off from everyone else.
    pub isolated: BTreeSet<NodeId>,
    pub faults: Faults,
    leaders_by_term: BTreeMap<Term, NodeId>,
    checked_leaders: BTreeSet<(Term, NodeId)>,
    committed: BTreeMap<Index, Entry>,
    applied_global: BTreeMap<Index, Vec<u8>>,
    next_cmd: u64,
    pub stats: Stats,
}

#[derive(Default, Debug)]
pub struct Stats {
    pub proposals_accepted: u64,
    pub max_commit: Index,
    pub elections: u64,
    pub crashes: u64,
    pub snapshots_installed: u64,
}

impl Sim {
    pub fn new(n: u64, seed: u64, faults: Faults) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let bootstrap = Configuration::new(ids.clone());
        let mut sim = Self {
            ids: ids.clone(),
            nodes: BTreeMap::new(),
            stores: BTreeMap::new(),
            bootstrap,
            rng: SeededRand::new(seed),
            now: 0,
            queue: Vec::new(),
            isolated: BTreeSet::new(),
            faults,
            leaders_by_term: BTreeMap::new(),
            checked_leaders: BTreeSet::new(),
            committed: BTreeMap::new(),
            applied_global: BTreeMap::new(),
            next_cmd: 0,
            stats: Stats::default(),
        };
        for id in ids {
            sim.stores.insert(id, Default::default());
            sim.start(id);
        }
        sim
    }

    fn pct(&mut self, p: u64) -> bool {
        p > 0 && self.rng.next_u64() % 100 < p
    }

    pub fn start(&mut self, id: NodeId) {
        let (l, s) = self.stores[&id].clone();
        let seed = self.rng.next_u64();
        let node = RaftNode::open(
            RaftConfig::new(id),
            SeededRand::new(seed),
            self.bootstrap.clone(),
            l,
            s,
            RecordingSm::default(),
            self.faults.snapshot_threshold,
        )
        .expect("node opens from its stores");
        self.nodes.insert(id, Some(node));
    }

    pub fn crash(&mut self, id: NodeId) {
        self.nodes.insert(id, None);
        self.stats.crashes += 1;
    }

    fn enqueue(&mut self, msgs: Vec<Message>) {
        for m in msgs {
            if self.pct(self.faults.drop_pct) {
                continue;
            }
            let copies = if self.pct(self.faults.dup_pct) { 2 } else { 1 };
            for _ in 0..copies {
                let d = if self.faults.max_delay == 0 {
                    0
                } else {
                    self.rng.next_u64() % (self.faults.max_delay + 1)
                };
                self.queue.push((self.now + d, m.clone()));
            }
        }
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(id, n)| n.as_ref().map(|n| (id, n)))
            .filter(|(_, n)| n.core().role() == Role::Leader)
            .max_by_key(|(_, n)| n.core().term())
            .map(|(id, _)| *id)
    }

    pub fn propose(&mut self) -> Option<Index> {
        self.propose_cmd().map(|(i, _)| i)
    }

    /// Proposes a fresh command to the highest-term leader. The command may
    /// still be lost if that leader is stale; callers re-propose.
    pub fn propose_cmd(&mut self) -> Option<(Index, Vec<u8>)> {
        let leader = self.leader()?;
        self.next_cmd += 1;
        let cmd = format!("cmd-{}", self.next_cmd).into_bytes();
        let node = self.nodes.get_mut(&leader)?.as_mut()?;
        match node.propose(cmd.clone()) {
            Ok((idx, msgs)) => {
                self.stats.proposals_accepted += 1;
                self.enqueue(msgs);
                Some((idx, cmd))
            }
            Err(_) => None,
        }
    }

    /// True if every live node's state machine applied `cmd`.
    pub fn applied_everywhere(&self, cmd: &[u8]) -> bool {
        self.live()
            .all(|(_, n)| n.state_machine().applied.iter().any(|(_, c)| c == cmd))
    }

    pub fn conf_change(&mut self, cc: ConfChange) -> Option<Index> {
        let leader = self.leader()?;
        let node = self.nodes.get_mut(&leader)?.as_mut()?;
        match node.propose_conf_change(cc) {
            Ok((idx, msgs)) => {
                self.enqueue(msgs);
                Some(idx)
            }
            Err(_) => None,
        }
    }

    /// One unit of simulated time: deliver due messages, tick every node,
    /// inject faults, check invariants.
    pub fn step(&mut self, inject_faults: bool) {
        self.now += 1;
        // Deliver.
        let now = self.now;
        let mut due: Vec<Message> = Vec::new();
        let mut keep = Vec::new();
        for (t, m) in self.queue.drain(..) {
            if t <= now {
                due.push(m);
            } else {
                keep.push((t, m));
            }
        }
        self.queue = keep;
        // Shuffle delivery order deterministically (reordering).
        for i in (1..due.len()).rev() {
            let j = (self.rng.next_u64() % (i as u64 + 1)) as usize;
            due.swap(i, j);
        }
        for m in due {
            if self.isolated.contains(&m.from) || self.isolated.contains(&m.to) {
                continue;
            }
            if let Some(Some(node)) = self.nodes.get_mut(&m.to) {
                let is_snap = matches!(m.body, Body::InstallSnapshot { .. });
                let before = node.core().log().snapshot_index();
                let out = node.step(m).expect("step never fails in a correct cluster");
                if is_snap && node.core().log().snapshot_index() > before {
                    self.stats.snapshots_installed += 1;
                }
                self.enqueue(out);
            }
        }
        // Tick.
        for id in self.ids.clone() {
            if let Some(Some(node)) = self.nodes.get_mut(&id) {
                let out = node.tick().expect("tick never fails");
                self.enqueue(out);
            }
        }
        if inject_faults {
            for id in self.ids.clone() {
                let crashed = matches!(self.nodes.get(&id), Some(None));
                if crashed {
                    if self.pct(20) {
                        self.start(id);
                    }
                } else if self.pct(self.faults.crash_pct) {
                    self.crash(id);
                }
            }
            if self.pct(self.faults.partition_pct) {
                self.isolated.clear();
                let k = self.rng.next_u64() % 3; // isolate 0..=2 nodes
                for _ in 0..k {
                    let id = self.ids[(self.rng.next_u64() % self.ids.len() as u64) as usize];
                    self.isolated.insert(id);
                }
            }
        }
        self.check_invariants();
    }

    pub fn heal(&mut self) {
        self.isolated.clear();
        for id in self.ids.clone() {
            if matches!(self.nodes.get(&id), Some(None)) {
                self.start(id);
            }
        }
    }

    pub fn live(&self) -> impl Iterator<Item = (&NodeId, &SimNode)> {
        self.nodes
            .iter()
            .filter_map(|(id, n)| n.as_ref().map(|n| (id, n)))
    }

    pub fn check_invariants(&mut self) {
        // Election Safety.
        let leaders: Vec<(Term, NodeId)> = self
            .live()
            .filter(|(_, n)| n.core().role() == Role::Leader)
            .map(|(id, n)| (n.core().term(), *id))
            .collect();
        for (term, id) in leaders {
            let prev = *self.leaders_by_term.entry(term).or_insert(id);
            assert_eq!(prev, id, "Election Safety violated in term {term}");
        }

        let views: Vec<(NodeId, RaftLog)> = self
            .live()
            .map(|(id, n)| (*id, n.core().log().clone()))
            .collect();

        // Log Matching (pairwise, over the overlapping suffixes).
        for (a_i, (a_id, a)) in views.iter().enumerate() {
            for (b_id, b) in views.iter().skip(a_i + 1) {
                let lo = a.first_index().max(b.first_index());
                let hi = a.last_index().min(b.last_index());
                let mut top = None;
                let mut j = hi;
                while j >= lo && j > 0 {
                    if a.term_at(j) == b.term_at(j) {
                        top = Some(j);
                        break;
                    }
                    j -= 1;
                }
                if let Some(top) = top {
                    for k in lo..=top {
                        let ea = &a.entries()[(k - a.first_index()) as usize];
                        let eb = &b.entries()[(k - b.first_index()) as usize];
                        assert_eq!(
                            ea, eb,
                            "Log Matching violated at {k} between {a_id} and {b_id}"
                        );
                    }
                }
            }
        }

        // Committed entries never change (and feed Leader Completeness).
        for (id, log) in &views {
            for e in log.entries() {
                if e.index > log.commit() {
                    break;
                }
                match self.committed.get(&e.index) {
                    Some(prev) => {
                        assert_eq!(prev, e, "committed entry {} changed on {id}", e.index)
                    }
                    None => {
                        self.committed.insert(e.index, e.clone());
                    }
                }
                self.stats.max_commit = self.stats.max_commit.max(e.index);
            }
        }

        // Leader Completeness: a newly elected leader holds every entry
        // committed so far. (A stale, partitioned leader of an older term
        // legitimately lacks later commits, so it is checked only once, at
        // election time.)
        let fresh: Vec<NodeId> = self
            .live()
            .filter(|(id, n)| {
                n.core().role() == Role::Leader
                    && !self.checked_leaders.contains(&(n.core().term(), **id))
            })
            .map(|(id, _)| *id)
            .collect();
        self.stats.elections += fresh.len() as u64;
        for id in fresh {
            let n = self.nodes[&id].as_ref().unwrap();
            self.checked_leaders.insert((n.core().term(), id));
            let log = n.core().log();
            for (idx, e) in self.committed.range(log.first_index()..) {
                assert!(
                    *idx <= log.last_index(),
                    "leader {id} misses committed index {idx}"
                );
                let mine = &log.entries()[(idx - log.first_index()) as usize];
                assert_eq!(
                    mine, e,
                    "leader {id} has a different committed entry at {idx}"
                );
            }
        }

        // State Machine Safety.
        let applied: Vec<(NodeId, AppliedLog)> = self
            .live()
            .map(|(id, n)| (*id, n.state_machine().applied.clone()))
            .collect();
        for (id, list) in applied {
            for (idx, cmd) in list {
                match self.applied_global.get(&idx) {
                    Some(prev) => {
                        assert_eq!(prev, &cmd, "node {id} applied a different command at {idx}")
                    }
                    None => {
                        self.applied_global.insert(idx, cmd);
                    }
                }
            }
        }
    }
}

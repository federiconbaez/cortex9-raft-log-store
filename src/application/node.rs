//! `RaftNode`: composes the pure core with storage and the state machine.

use crate::domain::{
    ConfChange, Configuration, Effect, EntryKind, Index, Message, RaftConfig, RaftCore, RaftError,
};
use crate::ports::{LogStore, Rand, SnapshotStore, StateMachine};
use std::fmt;

#[derive(Debug)]
pub enum NodeError {
    Raft(RaftError),
    Storage(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::Raft(e) => write!(f, "raft: {e}"),
            NodeError::Storage(e) => write!(f, "storage: {e}"),
        }
    }
}
impl std::error::Error for NodeError {}
impl From<RaftError> for NodeError {
    fn from(e: RaftError) -> Self {
        NodeError::Raft(e)
    }
}

fn storage<E: std::error::Error + Send + Sync + 'static>(e: E) -> NodeError {
    NodeError::Storage(Box::new(e))
}

pub struct RaftNode<L, S, M, R>
where
    L: LogStore,
    S: SnapshotStore,
    M: StateMachine,
    R: Rand,
{
    core: RaftCore<R>,
    log_store: L,
    snap_store: S,
    sm: M,
    /// Take a snapshot when this many applied entries are beyond the last one.
    snapshot_threshold: u64,
}

impl<L, S, M, R> RaftNode<L, S, M, R>
where
    L: LogStore,
    S: SnapshotStore,
    M: StateMachine,
    R: Rand,
{
    /// Opens a node, recovering whatever its stores contain.
    pub fn open(
        cfg: RaftConfig,
        rand: R,
        bootstrap: Configuration,
        mut log_store: L,
        mut snap_store: S,
        sm: M,
        snapshot_threshold: u64,
    ) -> Result<Self, NodeError> {
        let persisted = log_store.load().map_err(storage)?;
        let snapshot = snap_store.load().map_err(storage)?;
        let core = RaftCore::new(
            cfg,
            rand,
            bootstrap,
            persisted.hard_state,
            snapshot,
            persisted.entries,
        )?;
        let mut node = Self {
            core,
            log_store,
            snap_store,
            sm,
            snapshot_threshold,
        };
        node.drive()?;
        Ok(node)
    }

    pub fn core(&self) -> &RaftCore<R> {
        &self.core
    }
    pub fn state_machine(&self) -> &M {
        &self.sm
    }

    pub fn tick(&mut self) -> Result<Vec<Message>, NodeError> {
        self.core.tick();
        self.drive()
    }

    pub fn step(&mut self, m: Message) -> Result<Vec<Message>, NodeError> {
        self.core.step(m)?;
        self.drive()
    }

    pub fn propose(&mut self, cmd: Vec<u8>) -> Result<(Index, Vec<Message>), NodeError> {
        let idx = self.core.propose(cmd)?;
        Ok((idx, self.drive()?))
    }

    pub fn propose_conf_change(
        &mut self,
        cc: ConfChange,
    ) -> Result<(Index, Vec<Message>), NodeError> {
        let idx = self.core.propose_conf_change(cc)?;
        Ok((idx, self.drive()?))
    }

    /// Executes effects until the core is quiescent. Persistence effects are
    /// executed before `Send`s are released to the caller.
    fn drive(&mut self) -> Result<Vec<Message>, NodeError> {
        let mut out = Vec::new();
        loop {
            let effects = self.core.take_effects();
            if effects.is_empty() {
                break;
            }
            for eff in effects {
                match eff {
                    Effect::PersistSnapshot {
                        snapshot,
                        retain_log_suffix,
                    } => {
                        self.snap_store.save(&snapshot).map_err(storage)?;
                        if !retain_log_suffix {
                            self.log_store
                                .truncate_from(snapshot.meta.index + 1)
                                .map_err(storage)?;
                        }
                        self.log_store
                            .compact(snapshot.meta.index)
                            .map_err(storage)?;
                    }
                    Effect::PersistEntries(es) => self.log_store.append(&es).map_err(storage)?,
                    Effect::PersistHardState(hs) => {
                        self.log_store.save_hard_state(&hs).map_err(storage)?
                    }
                    Effect::Send(m) => out.push(m),
                    Effect::RestoreSnapshot(s) => self.sm.restore(&s.data),
                    Effect::Apply(entries) => {
                        for e in entries {
                            if let EntryKind::Command(c) = &e.kind {
                                self.sm.apply(e.index, c);
                            }
                        }
                    }
                }
            }
            self.maybe_snapshot()?;
        }
        Ok(out)
    }

    fn maybe_snapshot(&mut self) -> Result<(), NodeError> {
        if self.snapshot_threshold == 0 {
            return Ok(());
        }
        let log = self.core.log();
        let applied = log.applied();
        if applied.saturating_sub(log.snapshot_index()) >= self.snapshot_threshold {
            let data = self.sm.snapshot();
            self.core.compact(applied, data)?;
        }
        Ok(())
    }
}

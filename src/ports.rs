//! Ports: the only way the core talks to the outside world.

use crate::domain::types::{Entry, HardState, Index, Snapshot};

/// Source of randomness for election timeouts (driven port used by the domain).
pub trait Rand {
    fn next_u64(&mut self) -> u64;
}

/// Monotonic clock in milliseconds (used by the application driver only).
pub trait Clock {
    fn now_millis(&self) -> u64;
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now_millis(&self) -> u64 {
        (**self).now_millis()
    }
}

/// Durable state recovered at start-up.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PersistedLog {
    pub hard_state: HardState,
    pub entries: Vec<Entry>,
}

/// Repository for log entries and hard state.
pub trait LogStore {
    type Error: std::error::Error + Send + Sync + 'static;

    fn load(&mut self) -> Result<PersistedLog, Self::Error>;
    /// Truncate-then-append: the first entry's index replaces every stored
    /// entry with index >= it. Must be durable when it returns.
    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error>;
    /// Drop every stored entry with index >= `index`.
    fn truncate_from(&mut self, index: Index) -> Result<(), Self::Error>;
    /// Drop every stored entry with index <= `index` (covered by a snapshot).
    fn compact(&mut self, index: Index) -> Result<(), Self::Error>;
    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error>;
}

/// Repository for the latest snapshot.
pub trait SnapshotStore {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Atomically replaces the stored snapshot.
    fn save(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error>;
    fn load(&mut self) -> Result<Option<Snapshot>, Self::Error>;
}

/// The replicated application.
pub trait StateMachine {
    fn apply(&mut self, index: Index, command: &[u8]);
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&mut self, data: &[u8]);
}

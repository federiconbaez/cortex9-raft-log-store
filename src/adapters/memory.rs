//! In-memory stores. They model durable storage for simulations: the data
//! survives a "crash" of the `RaftNode` that owns a clone of the handle.

use crate::domain::types::{Entry, HardState, Index, Snapshot};
use crate::ports::{LogStore, PersistedLog, SnapshotStore};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub struct MemLogStore {
    inner: Arc<Mutex<PersistedLog>>,
}

impl MemLogStore {
    pub fn snapshot_of_contents(&self) -> PersistedLog {
        self.inner.lock().expect("poisoned").clone()
    }
}

impl LogStore for MemLogStore {
    type Error = Infallible;

    fn load(&mut self) -> Result<PersistedLog, Infallible> {
        Ok(self.snapshot_of_contents())
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), Infallible> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let mut g = self.inner.lock().expect("poisoned");
        g.entries.retain(|e| e.index < first.index);
        g.entries.extend_from_slice(entries);
        Ok(())
    }

    fn truncate_from(&mut self, index: Index) -> Result<(), Infallible> {
        self.inner
            .lock()
            .expect("poisoned")
            .entries
            .retain(|e| e.index < index);
        Ok(())
    }

    fn compact(&mut self, index: Index) -> Result<(), Infallible> {
        self.inner
            .lock()
            .expect("poisoned")
            .entries
            .retain(|e| e.index > index);
        Ok(())
    }

    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Infallible> {
        self.inner.lock().expect("poisoned").hard_state = *hs;
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemSnapshotStore {
    inner: Arc<Mutex<Option<Snapshot>>>,
}

impl SnapshotStore for MemSnapshotStore {
    type Error = Infallible;

    fn save(&mut self, snapshot: &Snapshot) -> Result<(), Infallible> {
        *self.inner.lock().expect("poisoned") = Some(snapshot.clone());
        Ok(())
    }

    fn load(&mut self) -> Result<Option<Snapshot>, Infallible> {
        Ok(self.inner.lock().expect("poisoned").clone())
    }
}
